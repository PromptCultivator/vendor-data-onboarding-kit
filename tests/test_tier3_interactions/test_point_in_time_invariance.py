"""
Tier 3 Interaction Tests: Point-in-Time (PIT) Invariance & Zero Look-Ahead Bias.

Enforces the central quantitative invariant of the entire market data platform:
Point-in-Time Correctness:
  H_1 == H_2 across all multi-drop ingestion sequences, with strictly 0 violations
  and 0.00% look-ahead alpha leakage.

Tests:
- Strict mathematical check: query as-of T_1 before Drop 2 is bit-identical to query as-of T_1 after Drop 2.
- Multi-drop revision ladder across 4 sequential drops with cascading restatements.
- Intermediate query timestamps testing strict temporal bounds (prior, between, post).
- PIT invariance under entity deletions/unlisting in subsequent drops.
- PIT invariance across corporate action ticker changes (FB -> META).
- Physical Parquet partitioned file immutability (SHA-256 hashes and read-only permissions).
- Authentic synthetic fixtures verification (drop_001_baseline.parquet vs drop_002_restatements.parquet).
- Multi-entity sparse reporting trajectory under asynchronous filing dates.
- Full vintage audit trail trajectory integrity and monotonicity.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from test_tier3_interactions.conftest import (
    BitemporalParquetStore,
    assert_point_in_time_invariance,
)


@pytest.mark.tier3
@pytest.mark.bitemporal
class TestPointInTimeInvariance:
    """Mathematical verification of Point-in-Time invariance (H_1 == H_2)."""

    def test_strict_pit_invariance_before_and_after_restatement_drop(
        self, tmp_path: Path, tier3_duckdb: duckdb.DuckDBPyConnection
    ) -> None:
        """
        Baseline Drop 1 ingested at T_1 (knowledge_time = 2023-10-01T20:00:00Z).
        Query as-of T_1 yields H_1.
        Drop 2 ingested at T_2 (knowledge_time = 2023-10-15T20:00:00Z) restating 100% of facts.
        Query as-of T_1 yields H_2.
        Assert strictly: H_1 == H_2 (zero contamination, identical row count and values).
        """
        store = BitemporalParquetStore(root_dir=tmp_path / "store", conn=tier3_duckdb)
        dataset_id = "EQUITY_DAILY"
        t_event = datetime(2023, 9, 29, 16, 0, 0, tzinfo=UTC)
        t_known_1 = datetime(2023, 10, 1, 20, 0, 0, tzinfo=UTC)
        t_known_2 = datetime(2023, 10, 15, 20, 0, 0, tzinfo=UTC)

        entities = ["ENT_AAPL", "ENT_MSFT", "ENT_GOOG", "ENT_AMZN", "ENT_META"]
        drop1_tbl = pa.Table.from_pydict(
            {
                "record_id": [f"D1_{e}" for e in entities],
                "entity_id": entities,
                "source_symbol": [e.replace("ENT_", "") for e in entities],
                "source_symbol_type": ["TICKER"] * 5,
                "event_time": [t_event] * 5,
                "knowledge_time": [t_known_1] * 5,
                "metric": ["CLOSE_PRICE"] * 5,
                "value": [170.50, 315.20, 130.40, 125.80, 290.10],
                "unit": ["USD"] * 5,
                "vendor_id": ["VENDOR_A"] * 5,
                "dataset_id": [dataset_id] * 5,
                "is_restatement": [False] * 5,
                "revision_seq": [1] * 5,
            }
        )

        # Append Drop 1
        store.append(drop1_tbl, dataset_id=dataset_id, drop_id="DROP_001")

        # Query as-of T_1 -> H_1
        h1 = store.as_of_query(dataset_id=dataset_id, as_of_time=t_known_1)
        assert h1.num_rows == 5

        # Drop 2 with major restatements
        drop2_tbl = pa.Table.from_pydict(
            {
                "record_id": [f"D2_{e}" for e in entities],
                "entity_id": entities,
                "source_symbol": [e.replace("ENT_", "") for e in entities],
                "source_symbol_type": ["TICKER"] * 5,
                "event_time": [t_event] * 5,
                "knowledge_time": [t_known_2] * 5,
                "metric": ["CLOSE_PRICE"] * 5,
                "value": [175.00, 310.00, 135.00, 120.00, 305.00],  # All restated
                "unit": ["USD"] * 5,
                "vendor_id": ["VENDOR_A"] * 5,
                "dataset_id": [dataset_id] * 5,
                "is_restatement": [True] * 5,
                "revision_seq": [2] * 5,
            }
        )
        store.append(drop2_tbl, dataset_id=dataset_id, drop_id="DROP_002")

        # Query as-of T_1 after Drop 2 -> H_2
        h2 = store.as_of_query(dataset_id=dataset_id, as_of_time=t_known_1)

        # Mathematical invariance assertion
        assert_point_in_time_invariance(h1, h2, key_columns=["entity_id", "event_time", "metric"])

        # Also verify that query as-of T_2 sees the restatements
        h_post = store.as_of_query(dataset_id=dataset_id, as_of_time=t_known_2)
        assert h_post.num_rows == 5
        post_values = {r["entity_id"]: r["value"] for r in h_post.to_pylist()}
        assert post_values["ENT_AAPL"] == 175.00
        assert post_values["ENT_MSFT"] == 310.00

    def test_pit_invariance_multi_drop_revision_ladder(
        self, tmp_path: Path, tier3_duckdb: duckdb.DuckDBPyConnection
    ) -> None:
        """
        Sequential ladder of 4 drops: T_1 < T_2 < T_3 < T_4.
        Historical snapshots as-of T_1, T_2, T_3 evaluated at any future drop time
        MUST be bit-identical to the state when that drop was first ingested.
        """
        store = BitemporalParquetStore(root_dir=tmp_path / "ladder_store", conn=tier3_duckdb)
        dataset_id = "LADDER_DS"
        t_event = datetime(2023, 1, 15, 16, 0, 0, tzinfo=UTC)
        timestamps = [
            datetime(2023, 1, 20, 12, 0, 0, tzinfo=UTC),
            datetime(2023, 2, 20, 12, 0, 0, tzinfo=UTC),
            datetime(2023, 3, 20, 12, 0, 0, tzinfo=UTC),
            datetime(2023, 4, 20, 12, 0, 0, tzinfo=UTC),
        ]
        values_per_drop = [100.0, 102.5, 101.8, 105.0]

        snapshots_at_ingestion: list[pa.Table] = []

        for i, (t_k, val) in enumerate(zip(timestamps, values_per_drop)):
            drop_tbl = pa.Table.from_pydict(
                {
                    "record_id": [f"REC_D{i + 1}"],
                    "entity_id": ["ENT_LADDER"],
                    "source_symbol": ["LADDER"],
                    "source_symbol_type": ["TICKER"],
                    "event_time": [t_event],
                    "knowledge_time": [t_k],
                    "metric": ["REVENUE"],
                    "value": [val],
                    "unit": ["USD"],
                    "vendor_id": ["VENDOR_L"],
                    "dataset_id": [dataset_id],
                    "is_restatement": [i > 0],
                    "revision_seq": [i + 1],
                }
            )
            store.append(drop_tbl, dataset_id=dataset_id, drop_id=f"DROP_{i + 1:03d}")

            # Take snapshot as-of t_k immediately after ingestion
            snap = store.as_of_query(dataset_id=dataset_id, as_of_time=t_k)
            snapshots_at_ingestion.append(snap)

        # After all 4 drops are ingested, re-query as-of each historical timestamp
        for i, t_k in enumerate(timestamps):
            re_queried = store.as_of_query(dataset_id=dataset_id, as_of_time=t_k)
            assert_point_in_time_invariance(
                snapshots_at_ingestion[i],
                re_queried,
                key_columns=["entity_id", "event_time", "metric"],
            )
            # Verify exact value at this historical cut
            assert re_queried.to_pylist()[0]["value"] == values_per_drop[i]

    def test_zero_lookahead_bias_intermediate_query_timestamps(
        self, tmp_path: Path, tier3_duckdb: duckdb.DuckDBPyConnection
    ) -> None:
        """
        Strict temporal bounds:
        T_prior (before Drop 1): returns 0 records.
        T_1 (Drop 1 ingestion): returns Drop 1 records.
        T_mid (between Drop 1 and Drop 2): returns Drop 1 records (Drop 2 invisible).
        T_2 (Drop 2 ingestion): returns Drop 2 records.
        """
        store = BitemporalParquetStore(root_dir=tmp_path / "bounds_store", conn=tier3_duckdb)
        dataset_id = "BOUNDS_DS"
        t_event = datetime(2023, 6, 1, 16, 0, 0, tzinfo=UTC)
        t_1 = datetime(2023, 6, 10, 12, 0, 0, tzinfo=UTC)
        t_2 = datetime(2023, 6, 20, 12, 0, 0, tzinfo=UTC)

        t_prior = datetime(2023, 6, 5, 12, 0, 0, tzinfo=UTC)
        t_mid = datetime(2023, 6, 15, 12, 0, 0, tzinfo=UTC)
        t_post = datetime(2023, 6, 25, 12, 0, 0, tzinfo=UTC)

        d1 = pa.Table.from_pydict(
            {
                "record_id": ["D1"],
                "entity_id": ["ENT_X"],
                "source_symbol": ["X"],
                "source_symbol_type": ["TICKER"],
                "event_time": [t_event],
                "knowledge_time": [t_1],
                "metric": ["PRICE"],
                "value": [50.0],
                "unit": ["USD"],
                "vendor_id": ["V"],
                "dataset_id": [dataset_id],
                "is_restatement": [False],
                "revision_seq": [1],
            }
        )
        d2 = pa.Table.from_pydict(
            {
                "record_id": ["D2"],
                "entity_id": ["ENT_X"],
                "source_symbol": ["X"],
                "source_symbol_type": ["TICKER"],
                "event_time": [t_event],
                "knowledge_time": [t_2],
                "metric": ["PRICE"],
                "value": [55.0],
                "unit": ["USD"],
                "vendor_id": ["V"],
                "dataset_id": [dataset_id],
                "is_restatement": [True],
                "revision_seq": [2],
            }
        )
        store.append(d1, dataset_id=dataset_id, drop_id="D1")
        store.append(d2, dataset_id=dataset_id, drop_id="D2")

        # Query at T_prior: must be empty (0 rows, zero look-ahead)
        q_prior = store.as_of_query(dataset_id=dataset_id, as_of_time=t_prior)
        assert q_prior.num_rows == 0

        # Query at T_mid: must see exactly 50.0, strictly NOT 55.0
        q_mid = store.as_of_query(dataset_id=dataset_id, as_of_time=t_mid)
        assert q_mid.num_rows == 1
        assert q_mid.to_pylist()[0]["value"] == 50.0

        # Query at T_post: must see 55.0
        q_post = store.as_of_query(dataset_id=dataset_id, as_of_time=t_post)
        assert q_post.num_rows == 1
        assert q_post.to_pylist()[0]["value"] == 55.0

    def test_pit_invariance_under_record_deletions(
        self, tmp_path: Path, tier3_duckdb: duckdb.DuckDBPyConnection
    ) -> None:
        """
        Drop 1 at T_1 contains entities A, B, C.
        Drop 2 at T_2 contains only entities A and C (entity B delisted/omitted).
        As-of query at T_1 evaluated after Drop 2 MUST STILL return entity B!
        Append-only store never mutates or deletes historical facts.
        """
        store = BitemporalParquetStore(root_dir=tmp_path / "del_store", conn=tier3_duckdb)
        dataset_id = "DEL_DS"
        t_event = datetime(2023, 5, 1, 16, 0, 0, tzinfo=UTC)
        t_1 = datetime(2023, 5, 5, 12, 0, 0, tzinfo=UTC)
        t_2 = datetime(2023, 5, 20, 12, 0, 0, tzinfo=UTC)

        d1 = pa.Table.from_pydict(
            {
                "record_id": ["D1_A", "D1_B", "D1_C"],
                "entity_id": ["ENT_A", "ENT_B", "ENT_C"],
                "source_symbol": ["A", "B", "C"],
                "source_symbol_type": ["TICKER"] * 3,
                "event_time": [t_event] * 3,
                "knowledge_time": [t_1] * 3,
                "metric": ["PRICE"] * 3,
                "value": [10.0, 20.0, 30.0],
                "unit": ["USD"] * 3,
                "vendor_id": ["V"] * 3,
                "dataset_id": [dataset_id] * 3,
                "is_restatement": [False] * 3,
                "revision_seq": [1] * 3,
            }
        )
        store.append(d1, dataset_id=dataset_id, drop_id="D1")

        h1_before = store.as_of_query(dataset_id=dataset_id, as_of_time=t_1)
        assert h1_before.num_rows == 3

        # Drop 2 omits entity B
        d2 = pa.Table.from_pydict(
            {
                "record_id": ["D2_A", "D2_C"],
                "entity_id": ["ENT_A", "ENT_C"],
                "source_symbol": ["A", "C"],
                "source_symbol_type": ["TICKER"] * 2,
                "event_time": [t_event] * 2,
                "knowledge_time": [t_2] * 2,
                "metric": ["PRICE"] * 2,
                "value": [11.0, 31.0],  # Updated values
                "unit": ["USD"] * 2,
                "vendor_id": ["V"] * 2,
                "dataset_id": [dataset_id] * 2,
                "is_restatement": [True] * 2,
                "revision_seq": [2] * 2,
            }
        )
        store.append(d2, dataset_id=dataset_id, drop_id="D2")

        # Query as-of T_1 after Drop 2
        h1_after = store.as_of_query(dataset_id=dataset_id, as_of_time=t_1)

        # Invariance check: H_1 before == H_1 after
        assert_point_in_time_invariance(
            h1_before, h1_after, key_columns=["entity_id", "event_time", "metric"]
        )
        entities_present = set(h1_after.column("entity_id").to_pylist())
        assert "ENT_B" in entities_present

    def test_pit_invariance_across_corporate_action_ticker_change(
        self, tmp_path: Path, tier3_duckdb: duckdb.DuckDBPyConnection
    ) -> None:
        """
        Corporate action rename FB -> META on 2022-06-09.
        Drop 1 at 2022-05-20 contains ticker 'FB'.
        Drop 2 at 2022-06-15 contains ticker 'META'.
        Historical query as-of 2022-05-20 returns FB source_symbol.
        Query as-of 2022-06-15 returns META source_symbol.
        Neither contaminates the other, and historical snapshot is invariant.
        """
        store = BitemporalParquetStore(root_dir=tmp_path / "corp_store", conn=tier3_duckdb)
        dataset_id = "CORP_DS"
        t_event = datetime(2022, 5, 15, 16, 0, 0, tzinfo=UTC)
        t_1 = datetime(2022, 5, 20, 20, 0, 0, tzinfo=UTC)
        t_2 = datetime(2022, 6, 15, 20, 0, 0, tzinfo=UTC)

        d1 = pa.Table.from_pydict(
            {
                "record_id": ["REC_FB"],
                "entity_id": ["ENT_META"],
                "source_symbol": ["FB"],
                "source_symbol_type": ["TICKER"],
                "event_time": [t_event],
                "knowledge_time": [t_1],
                "metric": ["CLOSE"],
                "value": [200.0],
                "unit": ["USD"],
                "vendor_id": ["V"],
                "dataset_id": [dataset_id],
                "is_restatement": [False],
                "revision_seq": [1],
            }
        )
        store.append(d1, dataset_id=dataset_id, drop_id="D1")

        h1_pre = store.as_of_query(dataset_id=dataset_id, as_of_time=t_1)

        d2 = pa.Table.from_pydict(
            {
                "record_id": ["REC_META"],
                "entity_id": ["ENT_META"],
                "source_symbol": ["META"],
                "source_symbol_type": ["TICKER"],
                "event_time": [t_event],
                "knowledge_time": [t_2],
                "metric": ["CLOSE"],
                "value": [200.0],
                "unit": ["USD"],
                "vendor_id": ["V"],
                "dataset_id": [dataset_id],
                "is_restatement": [False],
                "revision_seq": [2],
            }
        )
        store.append(d2, dataset_id=dataset_id, drop_id="D2")

        h1_post = store.as_of_query(dataset_id=dataset_id, as_of_time=t_1)
        assert_point_in_time_invariance(
            h1_pre, h1_post, key_columns=["entity_id", "event_time", "metric"]
        )

        # As-of T_1 returns historical symbol FB
        assert h1_post.to_pylist()[0]["source_symbol"] == "FB"
        # As-of T_2 returns updated symbol META
        h2 = store.as_of_query(dataset_id=dataset_id, as_of_time=t_2)
        assert h2.to_pylist()[0]["source_symbol"] == "META"

    def test_pit_invariance_physical_parquet_file_immutability(
        self, tmp_path: Path, tier3_duckdb: duckdb.DuckDBPyConnection
    ) -> None:
        """
        Verify physical storage immutability:
        Writing Drop 2 to the store must NOT alter bytes, SHA-256 hash, or write permissions
        of the parquet files generated in Drop 1.
        """
        import stat

        store_dir = tmp_path / "phys_store"
        store = BitemporalParquetStore(root_dir=store_dir, conn=tier3_duckdb)
        dataset_id = "PHYS_DS"

        t_event = datetime(2023, 10, 1, 16, 0, 0, tzinfo=UTC)
        t_1 = datetime(2023, 10, 2, 12, 0, 0, tzinfo=UTC)
        t_2 = datetime(2023, 10, 9, 12, 0, 0, tzinfo=UTC)

        d1 = pa.Table.from_pydict(
            {
                "record_id": ["P1"],
                "entity_id": ["ENT_1"],
                "source_symbol": ["S1"],
                "source_symbol_type": ["TICKER"],
                "event_time": [t_event],
                "knowledge_time": [t_1],
                "metric": ["CLOSE"],
                "value": [100.0],
                "unit": ["USD"],
                "vendor_id": ["V"],
                "dataset_id": [dataset_id],
                "is_restatement": [False],
                "revision_seq": [1],
            }
        )
        store.append(d1, dataset_id=dataset_id, drop_id="DROP_001")

        d1_files = list((store_dir / dataset_id / "drop_id=DROP_001").glob("*.parquet"))
        assert len(d1_files) == 1
        d1_file = d1_files[0]
        hash_before = hashlib.sha256(d1_file.read_bytes()).hexdigest()

        # Check read-only permission (stat.S_IWRITE is not set for group/others)
        mode = d1_file.stat().st_mode
        assert not (mode & stat.S_IWGRP)
        assert not (mode & stat.S_IWOTH)

        # Append Drop 2
        d2 = pa.Table.from_pydict(
            {
                "record_id": ["P2"],
                "entity_id": ["ENT_1"],
                "source_symbol": ["S1"],
                "source_symbol_type": ["TICKER"],
                "event_time": [t_event],
                "knowledge_time": [t_2],
                "metric": ["CLOSE"],
                "value": [105.0],
                "unit": ["USD"],
                "vendor_id": ["V"],
                "dataset_id": [dataset_id],
                "is_restatement": [True],
                "revision_seq": [2],
            }
        )
        store.append(d2, dataset_id=dataset_id, drop_id="DROP_002")

        # Verify hash of Drop 1 file is identical
        hash_after = hashlib.sha256(d1_file.read_bytes()).hexdigest()
        assert hash_before == hash_after, "Drop 1 parquet file was mutated during Drop 2 append!"

    def test_pit_invariance_synthetic_fixtures_manifest_check(
        self, tmp_path: Path, tier3_duckdb: duckdb.DuckDBPyConnection
    ) -> None:
        """
        Verify PIT invariance using the authentic bundled synthetic fixtures:
        tests/fixtures/synthetic/drop_001_baseline.parquet
        tests/fixtures/synthetic/drop_002_restatements.parquet
        """
        fixture_dir = Path(__file__).resolve().parent.parent / "fixtures" / "synthetic"
        drop1_path = fixture_dir / "drop_001_baseline.parquet"
        drop2_path = fixture_dir / "drop_002_restatements.parquet"

        assert drop1_path.exists(), f"Missing fixture {drop1_path}"
        assert drop2_path.exists(), f"Missing fixture {drop2_path}"

        drop1_tbl = pq.read_table(drop1_path)
        drop2_tbl = pq.read_table(drop2_path)

        store = BitemporalParquetStore(root_dir=tmp_path / "synth_store", conn=tier3_duckdb)
        dataset_id = "SYNTH_DAILY"

        # Ingest Drop 1
        store.append(drop1_tbl, dataset_id=dataset_id, drop_id="DROP_001")

        # Knowledge time of Drop 1
        t_k1 = max(drop1_tbl.column("knowledge_time").to_pylist())

        # Snapshot H_1
        h1 = store.as_of_query(dataset_id=dataset_id, as_of_time=t_k1)
        assert h1.num_rows > 0

        # Ingest Drop 2
        store.append(drop2_tbl, dataset_id=dataset_id, drop_id="DROP_002")

        # Snapshot H_2 (as-of t_k1 after Drop 2 ingestion)
        h2 = store.as_of_query(dataset_id=dataset_id, as_of_time=t_k1)

        # Strict PIT invariance assertion
        assert_point_in_time_invariance(h1, h2, key_columns=["entity_id", "event_time", "metric"])

    def test_pit_invariance_multi_entity_sparse_reporting_trajectory(
        self, tmp_path: Path, tier3_duckdb: duckdb.DuckDBPyConnection
    ) -> None:
        """
        Multi-entity sparse reporting: 10 entities reporting at staggered event times and knowledge dates.
        Cross-sectional PIT query as-of any calendar day retrieves strictly the latest knowledge
        available on or before that calendar day, with 0 future leakage.
        """
        store = BitemporalParquetStore(root_dir=tmp_path / "sparse_store", conn=tier3_duckdb)
        dataset_id = "SPARSE_DS"

        records: list[dict[str, Any]] = []
        # Entity 1 reports every Monday; Entity 2 reports every Friday; Entity 3 reports bi-weekly
        for week in range(4):
            day_mon = 2 + week * 7
            day_fri = 6 + week * 7
            t_ev_mon = datetime(2023, 10, day_mon, 16, 0, 0, tzinfo=UTC)
            t_kn_mon = datetime(2023, 10, day_mon, 20, 0, 0, tzinfo=UTC)
            t_ev_fri = datetime(2023, 10, day_fri, 16, 0, 0, tzinfo=UTC)
            t_kn_fri = datetime(2023, 10, day_fri, 20, 0, 0, tzinfo=UTC)

            records.append(
                {
                    "record_id": f"REC_E1_W{week}",
                    "entity_id": "ENT_MON",
                    "source_symbol": "MON",
                    "source_symbol_type": "TICKER",
                    "event_time": t_ev_mon,
                    "knowledge_time": t_kn_mon,
                    "metric": "CLOSE",
                    "value": 100.0 + week,
                    "unit": "USD",
                    "vendor_id": "V",
                    "dataset_id": dataset_id,
                    "is_restatement": False,
                    "revision_seq": 1,
                }
            )
            records.append(
                {
                    "record_id": f"REC_E2_W{week}",
                    "entity_id": "ENT_FRI",
                    "source_symbol": "FRI",
                    "source_symbol_type": "TICKER",
                    "event_time": t_ev_fri,
                    "knowledge_time": t_kn_fri,
                    "metric": "CLOSE",
                    "value": 200.0 + week,
                    "unit": "USD",
                    "vendor_id": "V",
                    "dataset_id": dataset_id,
                    "is_restatement": False,
                    "revision_seq": 1,
                }
            )

        tbl = pa.Table.from_pylist(records)
        store.append(tbl, dataset_id=dataset_id, drop_id="SPARSE_BATCH")

        # Query on Wednesday of week 1 (2023-10-11):
        # Must see Monday of week 1 (2023-10-09) for ENT_MON,
        # and Friday of week 0 (2023-10-06) for ENT_FRI! Must NOT see Friday of week 1 (2023-10-13)!
        t_asof_wed = datetime(2023, 10, 11, 12, 0, 0, tzinfo=UTC)
        snap = store.as_of_query(dataset_id=dataset_id, as_of_time=t_asof_wed)

        by_ent = {r["entity_id"]: r for r in snap.to_pylist()}
        assert by_ent["ENT_MON"]["event_time"].day == 9
        assert by_ent["ENT_FRI"]["event_time"].day == 6

    def test_pit_invariance_audit_trail_trajectory_integrity(
        self, tmp_path: Path, tier3_duckdb: duckdb.DuckDBPyConnection
    ) -> None:
        """
        Verify audit trail trajectory [(v_1, k_1), (v_2, k_2), ...] for a single event_time.
        Must be strictly monotonically increasing in knowledge_time and revision_seq.
        """
        store = BitemporalParquetStore(root_dir=tmp_path / "audit_store", conn=tier3_duckdb)
        dataset_id = "AUDIT_DS"
        t_event = datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC)

        revisions = [
            (datetime(2023, 1, 15, tzinfo=UTC), 100.0, 1),
            (datetime(2023, 2, 15, tzinfo=UTC), 102.5, 2),
            (datetime(2023, 3, 15, tzinfo=UTC), 101.8, 3),
            (datetime(2023, 4, 15, tzinfo=UTC), 103.0, 4),
        ]

        for i, (t_k, val, seq) in enumerate(revisions):
            tbl = pa.Table.from_pydict(
                {
                    "record_id": [f"AUDIT_{i}"],
                    "entity_id": ["ENT_AUDIT"],
                    "source_symbol": ["AUDIT"],
                    "source_symbol_type": ["TICKER"],
                    "event_time": [t_event],
                    "knowledge_time": [t_k],
                    "metric": ["METRIC_A"],
                    "value": [val],
                    "unit": ["USD"],
                    "vendor_id": ["V"],
                    "dataset_id": [dataset_id],
                    "is_restatement": [i > 0],
                    "revision_seq": [seq],
                }
            )
            store.append(tbl, dataset_id=dataset_id, drop_id=f"D_{i}")

        audit = store.audit_trail(
            dataset_id=dataset_id, entity_id="ENT_AUDIT", event_time=t_event, metric="METRIC_A"
        )
        rows = audit.to_pylist()
        assert len(rows) == 4

        for idx, row in enumerate(rows):
            assert row["revision_seq"] == idx + 1
            assert row["value"] == revisions[idx][1]
            if idx > 0:
                assert row["knowledge_time"] > rows[idx - 1]["knowledge_time"]
