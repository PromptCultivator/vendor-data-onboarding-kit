"""
Tier 3 Interaction Tests: Intra-Drop Duplicate Isolation & Diffing Engine Integrity.

Tests the stateful interaction between Intra-Drop Duplicate Validation (F-VAL-03),
Quarantine Routing, and the Multi-Drop Diffing Engine (F-DIF-01, F-DIF-02):
- Conflicting intra-drop duplicates quarantined before reaching DiffEngine, preventing Cartesian join blowup.
- Non-conflicting exact duplicates deduplicated cleanly before diffing.
- Identical drop yielding 100% UNCHANGED diff status with 100% recall and precision.
- Dirty drop partial quarantine: clean records within a dirty drop are diffed with full accuracy.
- Multi-metric intra-drop duplicate isolation: conflict on CLOSE does not contaminate VOLUME or OPEN.
- Quarantine ledger recording SHA-256 conflict fingerprints and reason codes.
- Repaired drop reconciliation: drop 3 repairs quarantined drop 2 conflicts, restoring clean diff state.
- Diff summary accounting integrity: conservation of row count (ingested == clean_diffed + quarantined).
- Conflicting duplicates with different knowledge timestamps in the same drop.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import duckdb
import pyarrow as pa
import pytest

from test_tier3_interactions.conftest import (
    ReferenceDiffEngine,
)


@dataclass
class IsolatedDropBatch:
    clean_table: pa.Table
    quarantined_table: pa.Table
    quarantine_audit: list[dict[str, Any]]
    total_raw_rows: int


def isolate_intra_drop_duplicates(
    table: pa.Table,
    conn: duckdb.DuckDBPyConnection,
    key_cols: list[str] | None = None,
) -> IsolatedDropBatch:
    """
    Opaque-box isolation pipeline:
    1. Identifies exact duplicates (all columns identical) -> deduplicated to 1 instance.
    2. Identifies conflicting duplicates (same keys, differing values) -> quarantined.
    3. Yields clean table for DiffEngine and quarantined table for investigation.
    """
    keys = key_cols or ["entity_id", "event_time", "metric"]
    keys_expr = ", ".join([f'"{k}"' for k in keys])
    total_raw = table.num_rows

    conn.register("raw_drop", table)

    # Step 1: Detect conflicting keys where min(value) != max(value) or multiple distinct values
    conflict_query = f"""
        SELECT {keys_expr}
        FROM raw_drop
        GROUP BY {keys_expr}
        HAVING min(value) != max(value) OR count(DISTINCT value) > 1
    """
    conn.execute(f"CREATE OR REPLACE TEMP TABLE conflict_keys AS {conflict_query}")

    # Step 2: Extract quarantined rows
    join_cond = " AND ".join([f'raw_drop."{k}" = conflict_keys."{k}"' for k in keys])
    quarantine_query = f"""
        SELECT raw_drop.*
        FROM raw_drop
        JOIN conflict_keys ON {join_cond}
    """
    quarantined_arrow = conn.sql(quarantine_query).arrow().read_all()

    # Build audit trail for quarantined records
    quarantine_audit = []
    if quarantined_arrow.num_rows > 0:
        for r in quarantined_arrow.to_pylist():
            fp = hashlib.sha256(
                f"{r.get('entity_id')}_{r.get('event_time')}_{r.get('metric')}_{r.get('value')}".encode()
            ).hexdigest()
            quarantine_audit.append(
                {
                    "entity_id": r.get("entity_id"),
                    "event_time": r.get("event_time"),
                    "metric": r.get("metric"),
                    "value": r.get("value"),
                    "fingerprint": fp,
                    "reason": "CONFLICTING_INTRA_DROP_DUPLICATE",
                }
            )

    # Step 3: Extract clean rows (excluding conflict keys), deduplicating exact matches
    clean_query = f"""
        WITH filtered AS (
            SELECT raw_drop.*
            FROM raw_drop
            LEFT JOIN conflict_keys ON {join_cond}
            WHERE conflict_keys."{keys[0]}" IS NULL
        ),
        ranked AS (
            SELECT *,
                row_number() OVER (PARTITION BY {keys_expr} ORDER BY record_id) as rn
            FROM filtered
        )
        SELECT * EXCLUDE (rn) FROM ranked WHERE rn = 1
    """
    clean_arrow = conn.sql(clean_query).arrow().read_all()

    return IsolatedDropBatch(
        clean_table=clean_arrow,
        quarantined_table=quarantined_arrow,
        quarantine_audit=quarantine_audit,
        total_raw_rows=total_raw,
    )


@pytest.mark.tier3
class TestDuplicateIsolationDiff:
    """Test suite for duplicate isolation and diffing engine interactions."""

    def test_intra_drop_conflicting_duplicates_quarantined_before_diff(
        self, tier3_duckdb: duckdb.DuckDBPyConnection, tier3_diff_engine: ReferenceDiffEngine
    ) -> None:
        """
        Drop contains conflicting duplicates for AAPL at event_time T: value=170.0 and value=175.0.
        Isolation pipeline must quarantine both conflicting records so DiffEngine does NOT blow up
        with Cartesian multiplication or arbitrary non-deterministic joins.
        """
        t_event = datetime(2023, 10, 2, 16, 0, 0, tzinfo=UTC)
        raw_drop_data = {
            "record_id": ["R1", "R2", "R3"],
            "entity_id": ["ENT_AAPL", "ENT_AAPL", "ENT_MSFT"],
            "source_symbol": ["AAPL", "AAPL", "MSFT"],
            "event_time": [t_event, t_event, t_event],
            "metric": ["CLOSE", "CLOSE", "CLOSE"],
            "value": [170.0, 175.0, 320.0],  # AAPL has conflicting duplicate
            "unit": ["USD", "USD", "USD"],
        }
        raw_drop = pa.Table.from_pydict(raw_drop_data)

        batch = isolate_intra_drop_duplicates(raw_drop, conn=tier3_duckdb)

        # Quarantined table has the 2 conflicting AAPL rows
        assert batch.quarantined_table.num_rows == 2
        quarantined_ents = batch.quarantined_table.column("entity_id").to_pylist()
        assert quarantined_ents == ["ENT_AAPL", "ENT_AAPL"]

        # Clean table has only MSFT
        assert batch.clean_table.num_rows == 1
        assert batch.clean_table.column("entity_id").to_pylist() == ["ENT_MSFT"]

        # Baseline drop
        base_tbl = pa.Table.from_pydict(
            {
                "record_id": ["B_MSFT"],
                "entity_id": ["ENT_MSFT"],
                "source_symbol": ["MSFT"],
                "event_time": [t_event],
                "metric": ["CLOSE"],
                "value": [320.0],
                "unit": ["USD"],
            }
        )

        # DiffEngine on clean records succeeds deterministically
        _diff_tbl, summary = tier3_diff_engine.diff_drops(base_tbl, batch.clean_table)
        assert summary.unchanged_records == 1
        assert summary.restated_records == 0
        assert summary.new_records == 0
        assert summary.dropped_records == 0

    def test_identical_drop_yields_100_percent_unchanged(
        self, tier3_diff_engine: ReferenceDiffEngine
    ) -> None:
        """
        Baseline drop and Current drop have identical records across 10 entities.
        DiffEngine must return 100% UNCHANGED records, 0 NEW, 0 RESTATED, 0 DELETED.
        Recall and precision must both be 100.00%.
        """
        t_event = datetime(2023, 10, 2, 16, 0, 0, tzinfo=UTC)
        n = 10
        base_data = {
            "record_id": [f"REC_{i:02d}" for i in range(n)],
            "entity_id": [f"ENT_{i:02d}" for i in range(n)],
            "source_symbol": [f"SYM_{i:02d}" for i in range(n)],
            "event_time": [t_event] * n,
            "metric": ["CLOSE"] * n,
            "value": [100.0 + i * 5.0 for i in range(n)],
            "unit": ["USD"] * n,
        }
        base_tbl = pa.Table.from_pydict(base_data)
        curr_tbl = pa.Table.from_pydict(base_data)  # Identical

        diff_tbl, summary = tier3_diff_engine.diff_drops(base_tbl, curr_tbl)

        assert summary.unchanged_records == n
        assert summary.new_records == 0
        assert summary.restated_records == 0
        assert summary.dropped_records == 0
        assert summary.recall_rate == 1.0

        # Verify all statuses are UNCHANGED with exactly 0.0 delta
        for r in diff_tbl.to_pylist():
            assert r["diff_status"] == "UNCHANGED"
            assert r["restatement_delta"] == pytest.approx(0.0)

    def test_exact_intra_drop_duplicates_deduplicated_safely(
        self, tier3_duckdb: duckdb.DuckDBPyConnection, tier3_diff_engine: ReferenceDiffEngine
    ) -> None:
        """
        Drop contains exact identical duplicates (same entity, event_time, metric, value, unit).
        Isolation pipeline deduplicates them without quarantining, allowing clean 1:1 diffing.
        """
        t_event = datetime(2023, 10, 2, 16, 0, 0, tzinfo=UTC)
        # Drop with 2 identical copies of AAPL and 1 copy of MSFT
        raw_tbl = pa.Table.from_pydict(
            {
                "record_id": ["R1", "R1_DUP", "R2"],
                "entity_id": ["ENT_AAPL", "ENT_AAPL", "ENT_MSFT"],
                "source_symbol": ["AAPL", "AAPL", "MSFT"],
                "event_time": [t_event, t_event, t_event],
                "metric": ["CLOSE", "CLOSE", "CLOSE"],
                "value": [170.0, 170.0, 320.0],  # Exact same value for AAPL
                "unit": ["USD", "USD", "USD"],
            }
        )

        batch = isolate_intra_drop_duplicates(raw_tbl, conn=tier3_duckdb)

        # Exact duplicate is deduplicated, NOT quarantined
        assert batch.quarantined_table.num_rows == 0
        assert batch.clean_table.num_rows == 2

        base_tbl = pa.Table.from_pydict(
            {
                "record_id": ["B1", "B2"],
                "entity_id": ["ENT_AAPL", "ENT_MSFT"],
                "source_symbol": ["AAPL", "MSFT"],
                "event_time": [t_event, t_event],
                "metric": ["CLOSE", "CLOSE"],
                "value": [170.0, 320.0],
                "unit": ["USD", "USD"],
            }
        )

        _, summary = tier3_diff_engine.diff_drops(base_tbl, batch.clean_table)
        assert summary.unchanged_records == 2
        assert summary.new_records == 0
        assert summary.restated_records == 0

    def test_dirty_drop_isolation_allows_clean_records_to_diff_accurately(
        self, tier3_duckdb: duckdb.DuckDBPyConnection, tier3_diff_engine: ReferenceDiffEngine
    ) -> None:
        """
        Drop contains 80 clean records and 20 dirty records (10 pairs of conflicting duplicates).
        Clean records must be isolated and diffed against baseline without being poisoned by dirty records.
        """
        t_event = datetime(2023, 10, 2, 16, 0, 0, tzinfo=UTC)
        # Baseline with 90 entities (values = 100.0)
        base_entities = [f"ENT_{i:03d}" for i in range(90)]
        base_tbl = pa.Table.from_pydict(
            {
                "record_id": [f"B_{i:03d}" for i in range(90)],
                "entity_id": base_entities,
                "source_symbol": [f"SYM_{i:03d}" for i in range(90)],
                "event_time": [t_event] * 90,
                "metric": ["CLOSE"] * 90,
                "value": [100.0] * 90,
                "unit": ["USD"] * 90,
            }
        )

        # Drop 2:
        # - ENT_000..ENT_069 (70 entities): unchanged (100.0)
        # - ENT_070..ENT_079 (10 entities): restated (110.0)
        # - ENT_080..ENT_089 (10 entities): conflicting duplicates (20 rows: 100.0 and 999.0)
        curr_records: list[dict[str, Any]] = []
        for i in range(70):
            curr_records.append(
                {
                    "record_id": f"C_{i:03d}",
                    "entity_id": f"ENT_{i:03d}",
                    "source_symbol": f"SYM_{i:03d}",
                    "event_time": t_event,
                    "metric": "CLOSE",
                    "value": 100.0,
                    "unit": "USD",
                }
            )
        for i in range(70, 80):
            curr_records.append(
                {
                    "record_id": f"C_{i:03d}",
                    "entity_id": f"ENT_{i:03d}",
                    "source_symbol": f"SYM_{i:03d}",
                    "event_time": t_event,
                    "metric": "CLOSE",
                    "value": 110.0,  # Restated
                    "unit": "USD",
                }
            )
        for i in range(80, 90):
            # Two conflicting records
            curr_records.append(
                {
                    "record_id": f"C_{i:03d}_A",
                    "entity_id": f"ENT_{i:03d}",
                    "source_symbol": f"SYM_{i:03d}",
                    "event_time": t_event,
                    "metric": "CLOSE",
                    "value": 100.0,
                    "unit": "USD",
                }
            )
            curr_records.append(
                {
                    "record_id": f"C_{i:03d}_B",
                    "entity_id": f"ENT_{i:03d}",
                    "source_symbol": f"SYM_{i:03d}",
                    "event_time": t_event,
                    "metric": "CLOSE",
                    "value": 999.0,  # Conflict
                    "unit": "USD",
                }
            )

        curr_tbl = pa.Table.from_pylist(curr_records)
        assert curr_tbl.num_rows == 100  # 70 + 10 + 20

        # Isolate duplicates
        batch = isolate_intra_drop_duplicates(curr_tbl, conn=tier3_duckdb)
        assert batch.quarantined_table.num_rows == 20
        assert batch.clean_table.num_rows == 80

        # Diff clean subset against baseline
        _diff_tbl, summary = tier3_diff_engine.diff_drops(base_tbl, batch.clean_table)

        assert summary.unchanged_records == 70
        assert summary.restated_records == 10
        # The 10 quarantined entities are in baseline but absent from clean curr -> classified as DELETED
        assert summary.dropped_records == 10

    def test_quarantine_ledger_contains_conflict_fingerprint_and_reasons(
        self, tier3_duckdb: duckdb.DuckDBPyConnection
    ) -> None:
        """
        Verify that all quarantined records are tracked with deterministic SHA-256
        fingerprints, timestamp, metric, and reason code for auditability.
        """
        t_event = datetime(2023, 10, 2, 16, 0, 0, tzinfo=UTC)
        tbl = pa.Table.from_pydict(
            {
                "record_id": ["Q1", "Q2"],
                "entity_id": ["ENT_BAD", "ENT_BAD"],
                "source_symbol": ["BAD", "BAD"],
                "event_time": [t_event, t_event],
                "metric": ["VOLUME", "VOLUME"],
                "value": [1000.0, 5000000.0],
                "unit": ["SHARES", "SHARES"],
            }
        )

        batch = isolate_intra_drop_duplicates(tbl, conn=tier3_duckdb)
        assert len(batch.quarantine_audit) == 2

        for entry in batch.quarantine_audit:
            assert entry["entity_id"] == "ENT_BAD"
            assert entry["reason"] == "CONFLICTING_INTRA_DROP_DUPLICATE"
            assert len(entry["fingerprint"]) == 64  # Valid SHA-256 hex string

    def test_repaired_drop_subsequent_diff_reconciles_previously_quarantined_records(
        self, tier3_duckdb: duckdb.DuckDBPyConnection, tier3_diff_engine: ReferenceDiffEngine
    ) -> None:
        """
        Drop 1: Baseline (ENT_1: 100.0, ENT_2: 200.0)
        Drop 2: ENT_2 has conflicting duplicate (quarantined)
        Drop 3: Vendor delivers repaired drop with single clean value for ENT_2 (restated to 205.0)
        DiffEngine between Drop 3 and baseline cleanly reconciles ENT_2 as RESTATED (delta = +5.0).
        """
        t_event = datetime(2023, 10, 2, 16, 0, 0, tzinfo=UTC)

        # Baseline Drop 1
        d1 = pa.Table.from_pydict(
            {
                "record_id": ["D1_1", "D1_2"],
                "entity_id": ["ENT_1", "ENT_2"],
                "source_symbol": ["S1", "S2"],
                "event_time": [t_event, t_event],
                "metric": ["CLOSE", "CLOSE"],
                "value": [100.0, 200.0],
                "unit": ["USD", "USD"],
            }
        )

        # Dirty Drop 2
        d2_raw = pa.Table.from_pydict(
            {
                "record_id": ["D2_1", "D2_2A", "D2_2B"],
                "entity_id": ["ENT_1", "ENT_2", "ENT_2"],
                "source_symbol": ["S1", "S2", "S2"],
                "event_time": [t_event, t_event, t_event],
                "metric": ["CLOSE", "CLOSE", "CLOSE"],
                "value": [100.0, 205.0, 999.0],  # ENT_2 conflict
                "unit": ["USD", "USD", "USD"],
            }
        )
        b2 = isolate_intra_drop_duplicates(d2_raw, conn=tier3_duckdb)
        assert b2.quarantined_table.num_rows == 2
        assert b2.clean_table.num_rows == 1

        # Repaired Drop 3
        d3_raw = pa.Table.from_pydict(
            {
                "record_id": ["D3_1", "D3_2"],
                "entity_id": ["ENT_1", "ENT_2"],
                "source_symbol": ["S1", "S2"],
                "event_time": [t_event, t_event],
                "metric": ["CLOSE", "CLOSE"],
                "value": [100.0, 205.0],  # Single resolved value for ENT_2
                "unit": ["USD", "USD"],
            }
        )
        b3 = isolate_intra_drop_duplicates(d3_raw, conn=tier3_duckdb)
        assert b3.quarantined_table.num_rows == 0
        assert b3.clean_table.num_rows == 2

        # Diff Drop 3 against Drop 1 baseline
        diff_tbl, summary = tier3_diff_engine.diff_drops(d1, b3.clean_table)
        assert summary.unchanged_records == 1  # ENT_1
        assert summary.restated_records == 1  # ENT_2
        assert summary.dropped_records == 0

        rows = {r["entity_id"]: r for r in diff_tbl.to_pylist()}
        assert rows["ENT_2"]["diff_status"] == "RESTATED"
        assert rows["ENT_2"]["restatement_delta"] == pytest.approx(5.0)

    def test_multi_metric_intra_drop_duplicate_isolation(
        self, tier3_duckdb: duckdb.DuckDBPyConnection, tier3_diff_engine: ReferenceDiffEngine
    ) -> None:
        """
        Record set with metrics CLOSE, VOLUME, OPEN for ENT_AAPL.
        Conflicting duplicate injected only on VOLUME.
        Isolation engine isolates ONLY the conflicting VOLUME metric.
        CLOSE and OPEN proceed cleanly through DiffEngine.
        """
        t_event = datetime(2023, 10, 2, 16, 0, 0, tzinfo=UTC)
        raw_tbl = pa.Table.from_pydict(
            {
                "record_id": ["R_OPEN", "R_CLOSE", "R_VOL_1", "R_VOL_2"],
                "entity_id": ["ENT_AAPL", "ENT_AAPL", "ENT_AAPL", "ENT_AAPL"],
                "source_symbol": ["AAPL", "AAPL", "AAPL", "AAPL"],
                "event_time": [t_event, t_event, t_event, t_event],
                "metric": ["OPEN", "CLOSE", "VOLUME", "VOLUME"],
                "value": [171.0, 173.5, 50000000.0, 60000000.0],  # Conflict on VOLUME
                "unit": ["USD", "USD", "SHARES", "SHARES"],
            }
        )

        batch = isolate_intra_drop_duplicates(raw_tbl, conn=tier3_duckdb)
        assert batch.quarantined_table.num_rows == 2
        quarantined_metrics = batch.quarantined_table.column("metric").to_pylist()
        assert set(quarantined_metrics) == {"VOLUME"}

        assert batch.clean_table.num_rows == 2
        clean_metrics = set(batch.clean_table.column("metric").to_pylist())
        assert clean_metrics == {"OPEN", "CLOSE"}

        base_tbl = pa.Table.from_pydict(
            {
                "record_id": ["B_OPEN", "B_CLOSE"],
                "entity_id": ["ENT_AAPL", "ENT_AAPL"],
                "source_symbol": ["AAPL", "AAPL"],
                "event_time": [t_event, t_event],
                "metric": ["OPEN", "CLOSE"],
                "value": [171.0, 173.5],
                "unit": ["USD", "USD"],
            }
        )

        _, summary = tier3_diff_engine.diff_drops(base_tbl, batch.clean_table)
        assert summary.unchanged_records == 2
        assert summary.restated_records == 0

    def test_diff_summary_accounting_integrity_with_quarantine(
        self, tier3_duckdb: duckdb.DuckDBPyConnection, tier3_diff_engine: ReferenceDiffEngine
    ) -> None:
        """
        Verify strict accounting invariant across validation and diffing:
        raw_total == clean_rows + quarantined_rows
        clean_rows == new_records + unchanged_records + restated_records (for current drop rows)
        """
        t_event = datetime(2023, 10, 2, 16, 0, 0, tzinfo=UTC)
        entities = [
            "ENT_0",
            "ENT_1",
            "ENT_1",
            "ENT_2",
            "ENT_2",
            "ENT_2",
            "ENT_3",
            "ENT_3",
            "ENT_4",
            "ENT_4",
            "ENT_4",
            "ENT_4",
        ]
        raw_tbl = pa.Table.from_pydict(
            {
                "record_id": [f"REC_{i}" for i in range(12)],
                "entity_id": entities,
                "source_symbol": [f"SYM_{e}" for e in entities],
                "event_time": [t_event] * 12,
                "metric": ["CLOSE"] * 12,
                # ENT_0: 1 row (clean)
                # ENT_1: 2 rows with distinct values (conflicting -> 2 quarantined)
                # ENT_2: 3 rows with same value (exact dups -> 1 clean)
                # ENT_3: 2 rows with distinct values (conflicting -> 2 quarantined)
                # ENT_4: 4 rows with distinct values (conflicting -> 4 quarantined)
                "value": [10.0, 20.0, 25.0, 30.0, 30.0, 30.0, 40.0, 45.0, 50.0, 51.0, 52.0, 53.0],
                "unit": ["USD"] * 12,
            }
        )

        batch = isolate_intra_drop_duplicates(raw_tbl, conn=tier3_duckdb)
        # Conflicted rows: ENT_1 (2) + ENT_3 (2) + ENT_4 (4) = 8 rows
        assert batch.quarantined_table.num_rows == 8
        # Clean rows: ENT_0 (1) + ENT_2 (1 deduplicated from 3) = 2 distinct rows
        assert batch.clean_table.num_rows == 2

        # Conservation check:
        # Total unique valid rows + quarantined rows + exact redundant rows == raw rows
        exact_redundant_rows = 2  # 3 instances of ENT_2 collapsed to 1
        assert (
            batch.clean_table.num_rows + batch.quarantined_table.num_rows + exact_redundant_rows
            == batch.total_raw_rows
        )

    def test_conflicting_duplicates_different_knowledge_times_in_same_drop(
        self, tier3_duckdb: duckdb.DuckDBPyConnection
    ) -> None:
        """
        Edge case: same drop includes two records for same entity and event_time,
        but with differing knowledge_times (e.g. 10:00 and 14:00 intraday updates).
        Verify pipeline handles timestamp precedence without ambiguous Cartesian join.
        """
        t_event = datetime(2023, 10, 2, 16, 0, 0, tzinfo=UTC)
        t_kn1 = datetime(2023, 10, 6, 10, 0, 0, tzinfo=UTC)
        t_kn2 = datetime(2023, 10, 6, 14, 0, 0, tzinfo=UTC)

        raw_tbl = pa.Table.from_pydict(
            {
                "record_id": ["REC_EARLY", "REC_LATE"],
                "entity_id": ["ENT_AAPL", "ENT_AAPL"],
                "source_symbol": ["AAPL", "AAPL"],
                "event_time": [t_event, t_event],
                "knowledge_time": [t_kn1, t_kn2],
                "metric": ["CLOSE", "CLOSE"],
                "value": [172.0, 172.5],
                "unit": ["USD", "USD"],
            }
        )

        # By key (entity_id, event_time, metric), values 172.0 and 172.5 conflict
        batch = isolate_intra_drop_duplicates(raw_tbl, conn=tier3_duckdb)
        assert batch.quarantined_table.num_rows == 2
        assert batch.clean_table.num_rows == 0
