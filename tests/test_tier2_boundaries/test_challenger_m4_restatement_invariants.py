"""
Tier 2 Boundary Tests: Challenger 2 Suite for Milestone 4.
Bitemporal & Restatement Invariant Challenger.

Adversarially verifies mathematical and domain invariants:
- Invariant 1: Restatement Detection Recall = Strictly 100.00% against planted restatements
  in `tests/fixtures/synthetic/truth_manifest.parquet` and synthetic drop pairs
  (`drop_001_baseline.csv` vs `drop_002_restatements.csv`).
- Invariant 2: Restatement Precision = Strictly 100.00% (zero false positives on unchanged records).
- Invariant 3: Immutability & Revision Sequence: Historical baseline records are never mutated
  in place; restated records produce append-ready records with `revision_seq = base.revision_seq + 1`
  and `is_restatement = True`.
- Invariant 4: 100% Anomaly Coverage in Vendor Inquiries: Every single category of pipeline
  anomaly produces an actionable inquiry starting with "Subject: " and length > 50.
- Invariant 5: Dual-format parity: Markdown report and JSON report contain identical overall
  status, record counts, and scorecard metrics.

Also performs:
- Boundary value stress tests (empty tables, zero values, IEEE specials like NaN/Inf, micro-deltas,
  extreme scale 100k rows, multi-threaded concurrency).
- Empirical mutation testing (epsilon relaxation, zero tolerance flakiness, omitted primary key
  components, revision sequence off-by-one, inquiry truncation).
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import json
import os
import stat
import time
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from vendor_onboarding.diff.ledger import (
    DeltaStatistics,
    DiffEngine,
    DiffSummary,
    RevisionLedger,
)
from vendor_onboarding.ingestion.reader import read_file
from vendor_onboarding.report.coverage import CoverageAnalyzer, CoverageDipAlert
from vendor_onboarding.report.generator import (
    PipelineRunContext,
    ReadinessReporter,
    ReadinessReportGenerator,
    export_reports,
)
from vendor_onboarding.report.questions import (
    VendorQuestionsGenerator,
    generate_vendor_inquiries,
)
from vendor_onboarding.storage.store import BitemporalStore, ImmutabilityError
from vendor_onboarding.validation.rules import ValidationResult

pytestmark = [pytest.mark.tier2]


def _sha256(path: Path) -> str:
    """Computes SHA-256 checksum of a file."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


# ==============================================================================
# Category 1: Planted Restatement Recall and Precision Invariants (F-DIF-01/02)
# ==============================================================================


class TestPlantedRestatementRecallAndPrecision:
    """Verifies Invariants 1 & 2: 100.00% Recall and 100.00% Precision against Ground Truth."""

    def test_parquet_planted_restatements_recall_strictly_100(
        self, fixture_paths: Any
    ) -> None:
        """Invariant 1: Restatement Detection Recall = Strictly 100.00% against planted restatements."""
        d1 = pq.read_table(fixture_paths.drop_001_parquet)
        d2 = pq.read_table(fixture_paths.drop_002_parquet)
        truth = pq.read_table(fixture_paths.truth_manifest_parquet)

        engine = DiffEngine()
        diff_tbl, summary = engine.diff_drops(d1, d2)

        # Ground truth planted restatements in Drop 2
        con = duckdb.connect()
        con.register("truth", truth)
        planted_ids = set(
            con.sql(
                "SELECT record_id FROM truth WHERE drop_id = 'DROP_002' AND is_planted_restatement = true"
            ).fetchnumpy()["record_id"]
        )
        assert len(planted_ids) == 10, f"Expected 10 planted restatements in truth manifest, got {len(planted_ids)}"

        # Detected restatements
        restated_rows = diff_tbl.filter(pc.equal(diff_tbl["diff_status"], "RESTATED"))
        detected_ids = set(restated_rows["record_id"].to_pylist())

        # Exact 100.00% recall check (0 false negatives)
        false_negatives = planted_ids - detected_ids
        assert len(false_negatives) == 0, f"False negatives encountered: {false_negatives}"

        recall_rate = len(planted_ids.intersection(detected_ids)) / len(planted_ids)
        assert recall_rate == 1.0, f"Restatement recall {recall_rate:.4f} != 1.0000 (100.00%)"
        assert summary.restated_records == 10
        assert summary.recall_rate == 1.0

        # Verify exact record ID naming convention
        expected_ids = {f"REC_002_REST_{i:04d}" for i in range(1, 11)}
        assert detected_ids == expected_ids

    def test_parquet_planted_restatements_precision_strictly_100(
        self, fixture_paths: Any
    ) -> None:
        """Invariant 2: Restatement Precision = Strictly 100.00% (zero false positives on unchanged records)."""
        d1 = pq.read_table(fixture_paths.drop_001_parquet)
        d2 = pq.read_table(fixture_paths.drop_002_parquet)
        truth = pq.read_table(fixture_paths.truth_manifest_parquet)

        engine = DiffEngine()
        diff_tbl, summary = engine.diff_drops(d1, d2)

        con = duckdb.connect()
        con.register("truth", truth)
        unchanged_in_truth = set(
            con.sql(
                "SELECT record_id FROM truth WHERE drop_id = 'DROP_002' AND mutation_type = 'UNCHANGED'"
            ).fetchnumpy()["record_id"]
        )
        assert len(unchanged_in_truth) == 45, f"Expected 45 unchanged records, got {len(unchanged_in_truth)}"

        restated_rows = diff_tbl.filter(pc.equal(diff_tbl["diff_status"], "RESTATED"))
        detected_restated_ids = set(restated_rows["record_id"].to_pylist())

        # Zero false positives
        false_positives = detected_restated_ids.intersection(unchanged_in_truth)
        assert len(false_positives) == 0, f"False positives on unchanged records: {false_positives}"

        # Precision calculation
        precision_rate = summary.precision_rate
        assert precision_rate == 1.0, f"Precision rate {precision_rate} != 1.0"
        assert summary.unchanged_records == 45

    def test_csv_drop_pairs_recall_and_precision_strictly_100(
        self, fixture_paths: Any
    ) -> None:
        """Invariant 1 & 2: Ingestion from CSV drop pairs produces 100.00% recall and 100.00% precision."""
        d1_csv = read_file(fixture_paths.drop_001_csv)
        d2_csv = read_file(fixture_paths.drop_002_csv)
        truth = pq.read_table(fixture_paths.truth_manifest_parquet)

        assert d1_csv.num_rows == 60
        assert d2_csv.num_rows == 65

        engine = DiffEngine()
        diff_tbl, summary = engine.diff_drops(d1_csv, d2_csv)

        # Summary check
        assert summary.new_records == 10
        assert summary.unchanged_records == 45
        assert summary.restated_records == 10
        assert summary.dropped_records == 5
        assert summary.recall_rate == 1.0
        assert summary.precision_rate == 1.0

        con = duckdb.connect()
        con.register("truth", truth)
        planted_ids = set(
            con.sql(
                "SELECT record_id FROM truth WHERE drop_id = 'DROP_002' AND is_planted_restatement = true"
            ).fetchnumpy()["record_id"]
        )
        unchanged_ids = set(
            con.sql(
                "SELECT record_id FROM truth WHERE drop_id = 'DROP_002' AND mutation_type = 'UNCHANGED'"
            ).fetchnumpy()["record_id"]
        )

        detected_restated = set(
            diff_tbl.filter(pc.equal(diff_tbl["diff_status"], "RESTATED"))["record_id"].to_pylist()
        )

        assert detected_restated == planted_ids, "CSV diff failed exact planted restatements match"
        assert len(detected_restated.intersection(unchanged_ids)) == 0, "CSV diff produced false positives"

    def test_revision_delta_numerical_fidelity_and_statistics(
        self, fixture_paths: Any
    ) -> None:
        """Delta calculation accuracy (absolute and percentage) against truth manifest."""
        d1 = pq.read_table(fixture_paths.drop_001_parquet)
        d2 = pq.read_table(fixture_paths.drop_002_parquet)
        truth = pq.read_table(fixture_paths.truth_manifest_parquet)

        engine = DiffEngine()
        diff_tbl, summary = engine.diff_drops(d1, d2)

        con = duckdb.connect()
        con.register("truth", truth)
        truth_deltas = {
            r[0]: (r[1], r[2])
            for r in con.sql(
                "SELECT record_id, restatement_delta, restatement_pct_delta FROM truth WHERE drop_id = 'DROP_002' AND is_planted_restatement = true"
            ).fetchall()
        }

        diff_rows = diff_tbl.filter(pc.equal(diff_tbl["diff_status"], "RESTATED")).to_pylist()
        for r in diff_rows:
            rec_id = r["record_id"]
            expected_delta, expected_pct = truth_deltas[rec_id]
            assert r["restatement_delta"] is not None
            assert r["restatement_pct_delta"] is not None
            assert abs(r["restatement_delta"] - expected_delta) < 1e-4, (
                f"Delta mismatch for {rec_id}: {r['restatement_delta']} vs {expected_delta}"
            )
            assert abs(r["restatement_pct_delta"] - expected_pct) < 1e-4, (
                f"Pct delta mismatch for {rec_id}: {r['restatement_pct_delta']} vs {expected_pct}"
            )

        # Delta statistics distribution check
        stats = engine.compute_delta_statistics(diff_tbl)
        assert stats.count == 10
        assert stats.min_delta is not None and stats.max_delta is not None
        assert stats.min_delta <= stats.max_delta
        assert stats.mean_delta is not None
        assert stats.std_delta is not None and stats.std_delta > 0.0
        assert stats.mean_pct_delta is not None

    def test_categorical_and_string_restatements(self) -> None:
        """String / categorical restatements (e.g. credit rating AAA -> AA+)."""
        t1 = pa.Table.from_pydict({
            "record_id": ["REC_CAT_01", "REC_CAT_02"],
            "entity_id": ["E_AAA", "E_BBB"],
            "event_time": ["2023-10-02T16:00:00Z", "2023-10-02T16:00:00Z"],
            "metric": ["CREDIT_RATING", "CREDIT_RATING"],
            "value": ["AAA", "BBB"],
        })
        t2 = pa.Table.from_pydict({
            "record_id": ["REC_CAT_01", "REC_CAT_02"],
            "entity_id": ["E_AAA", "E_BBB"],
            "event_time": ["2023-10-02T16:00:00Z", "2023-10-02T16:00:00Z"],
            "metric": ["CREDIT_RATING", "CREDIT_RATING"],
            "value": ["AA+", "BBB"],  # REC_CAT_01 restated, REC_CAT_02 unchanged
        })

        engine = DiffEngine()
        diff_tbl, summary = engine.diff_drops(t1, t2)
        assert summary.restated_records == 1
        assert summary.unchanged_records == 1
        assert summary.new_records == 0
        assert summary.dropped_records == 0

        row_restated = diff_tbl.filter(pc.equal(diff_tbl["record_id"], "REC_CAT_01")).to_pylist()[0]
        assert row_restated["diff_status"] == "RESTATED"
        assert row_restated["is_restatement"] is True


# ==============================================================================
# Category 2: Immutability & Revision Sequence Invariants (F-DIF-02, Bitemporal)
# ==============================================================================


class TestBitemporalImmutabilityAndRevisionSequence:
    """Verifies Invariant 3: Historical baseline records are never mutated in place;

    restated records produce append-ready records with revision_seq = base.revision_seq + 1
    and is_restatement = True.
    """

    def test_historical_baseline_never_mutated_in_place(
        self, tmp_path: Path, fixture_paths: Any
    ) -> None:
        """Historical baseline Parquet partition files remain strictly bit-for-bit immutable."""
        store_dir = tmp_path / "bitemporal_immutability_test"
        store = BitemporalStore(root_dir=store_dir)

        d1 = pq.read_table(fixture_paths.drop_001_parquet)
        d2 = pq.read_table(fixture_paths.drop_002_parquet)
        ds = "EQUITY_DAILY_FUNDAMENTALS"

        # Append baseline drop
        store.append(d1, dataset_id=ds, drop_id="DROP_001")
        base_file = store_dir / ds / "drop_id=DROP_001" / "data.parquet"
        assert base_file.exists()

        base_hash_before = _sha256(base_file)
        base_mtime_before = base_file.stat().st_mtime_ns
        base_size_before = base_file.stat().st_size
        base_mode_before = stat.S_IMODE(base_file.stat().st_mode)

        # Baseline file must be read-only (0o444)
        assert base_mode_before == 0o444

        # Perform diff and apply drop revisions
        engine = DiffEngine()
        diff_tbl, summary = engine.diff_drops(d1, d2)
        assert summary.restated_records == 10

        # Append drop 2
        store.append(d2, dataset_id=ds, drop_id="DROP_002")
        d2_file = store_dir / ds / "drop_id=DROP_002" / "data.parquet"
        assert d2_file.exists()

        # Re-verify baseline file is completely unmutated
        base_hash_after = _sha256(base_file)
        base_mtime_after = base_file.stat().st_mtime_ns
        base_size_after = base_file.stat().st_size
        base_mode_after = stat.S_IMODE(base_file.stat().st_mode)

        assert base_hash_after == base_hash_before, "Baseline file SHA-256 mutated!"
        assert base_mtime_after == base_mtime_before, "Baseline file modification time changed!"
        assert base_size_after == base_size_before, "Baseline file byte size changed!"
        assert base_mode_after == 0o444, "Baseline file permissions changed!"

    def test_restated_records_revision_seq_increment(self, fixture_paths: Any) -> None:
        """Restated records receive base.revision_seq + 1 and is_restatement = True."""
        d1 = pq.read_table(fixture_paths.drop_001_parquet)
        d2 = pq.read_table(fixture_paths.drop_002_parquet)

        engine = DiffEngine()
        diff_tbl, summary = engine.diff_drops(d1, d2)

        # 1. Restated records
        restated_tbl = diff_tbl.filter(pc.equal(diff_tbl["diff_status"], "RESTATED"))
        assert restated_tbl.num_rows == 10
        assert set(restated_tbl["is_restatement"].to_pylist()) == {True}
        assert set(restated_tbl["revision_seq"].to_pylist()) == {2}

        # 2. Unchanged records
        unchanged_tbl = diff_tbl.filter(pc.equal(diff_tbl["diff_status"], "UNCHANGED"))
        assert unchanged_tbl.num_rows == 45
        assert set(unchanged_tbl["is_restatement"].to_pylist()) == {False}
        assert set(unchanged_tbl["revision_seq"].to_pylist()) == {1}

        # 3. New records
        new_tbl = diff_tbl.filter(pc.equal(diff_tbl["diff_status"], "NEW"))
        assert new_tbl.num_rows == 10
        assert set(new_tbl["is_restatement"].to_pylist()) == {False}
        assert set(new_tbl["revision_seq"].to_pylist()) == {1}

    def test_multi_generational_restatement_ladder(self, tmp_path: Path) -> None:
        """Cascading 5-generation ladder: rev 1 -> 2 -> 3 -> 4 -> 5 with audit trail integrity."""
        store_dir = tmp_path / "store_ladder"
        store = BitemporalStore(root_dir=store_dir)
        ds = "LADDER_DS"
        t_event = datetime(2023, 10, 2, 16, 0, 0, tzinfo=UTC)

        engine = DiffEngine()
        current_baseline = pa.Table.from_pydict({
            "record_id": ["REC_LADDER_01"],
            "entity_id": ["ENT_LADDER"],
            "source_symbol": ["LADR"],
            "event_time": [t_event],
            "knowledge_time": [datetime(2023, 10, 6, 20, 0, 0, tzinfo=UTC)],
            "metric": ["CLOSE_PRICE"],
            "value": [100.0],
            "unit": ["USD"],
            "vendor_id": ["SYNTH"],
            "dataset_id": [ds],
            "is_restatement": [False],
            "revision_seq": [1],
            "drop_id": ["DROP_001"],
        })
        store.append(current_baseline, dataset_id=ds, drop_id="DROP_001")

        # Sequentially restate 4 times (drops 2..5)
        for gen in range(2, 6):
            t_know = datetime(2023, 10, 6 + gen, 20, 0, 0, tzinfo=UTC)
            new_val = 100.0 + (gen * 5.0)
            incoming_drop = pa.Table.from_pydict({
                "record_id": [f"REC_LADDER_{gen:02d}"],
                "entity_id": ["ENT_LADDER"],
                "source_symbol": ["LADR"],
                "event_time": [t_event],
                "knowledge_time": [t_know],
                "metric": ["CLOSE_PRICE"],
                "value": [new_val],
                "unit": ["USD"],
                "vendor_id": ["SYNTH"],
                "dataset_id": [ds],
                "is_restatement": [False],
                "revision_seq": [1],
                "drop_id": [f"DROP_{gen:03d}"],
            })

            diff_tbl, summary = engine.diff_drops(current_baseline, incoming_drop)
            assert summary.restated_records == 1
            row = diff_tbl.to_pylist()[0]
            assert row["diff_status"] == "RESTATED"
            assert row["is_restatement"] is True
            assert row["revision_seq"] == gen

            # Construct append-ready table for the store with correct revision_seq
            append_tbl = pa.Table.from_pydict({
                "record_id": [f"REC_LADDER_{gen:02d}"],
                "entity_id": ["ENT_LADDER"],
                "source_symbol": ["LADR"],
                "event_time": [t_event],
                "knowledge_time": [t_know],
                "metric": ["CLOSE_PRICE"],
                "value": [new_val],
                "unit": ["USD"],
                "vendor_id": ["SYNTH"],
                "dataset_id": [ds],
                "is_restatement": [True],
                "revision_seq": [gen],
                "drop_id": [f"DROP_{gen:03d}"],
            })
            store.append(append_tbl, dataset_id=ds, drop_id=f"DROP_{gen:03d}")
            current_baseline = append_tbl

        # Verify audit trail contains all 5 revisions in strict chronological order
        trail = store.audit_trail(ds, entity_id="ENT_LADDER", event_time=t_event, metric="CLOSE_PRICE")
        assert trail.num_rows == 5
        trail_rows = trail.to_pylist()
        seqs = [r["revision_seq"] for r in trail_rows]
        assert seqs == [1, 2, 3, 4, 5], f"Revision sequence corrupt: {seqs}"
        vals = [r["value"] for r in trail_rows]
        assert vals == [100.0, 110.0, 115.0, 120.0, 125.0]

        # Verify as_of query at each generation retrieves the exact revision
        for gen in range(1, 6):
            t_as_of = datetime(2023, 10, 6 + (gen if gen > 1 else 0), 21, 0, 0, tzinfo=UTC)
            as_of_tbl = store.as_of_query(ds, as_of_time=t_as_of, entity_ids=["ENT_LADDER"])
            assert as_of_tbl.num_rows == 1
            row_as_of = as_of_tbl.to_pylist()[0]
            expected_val = 100.0 if gen == 1 else 100.0 + (gen * 5.0)
            assert row_as_of["revision_seq"] == gen
            assert row_as_of["value"] == expected_val


# ==============================================================================
# Category 3: Boundary Value Stress Tests
# ==============================================================================


class TestBoundaryValueStressTests:
    """Stress tests on boundary conditions: empty tables, zero values, IEEE specials, scale, concurrency."""

    def test_empty_table_boundaries(self) -> None:
        """Diffing empty tables (0 vs 0, 0 vs N, N vs 0)."""
        engine = DiffEngine()
        schema = pa.schema([
            ("entity_id", pa.string()),
            ("event_time", pa.string()),
            ("metric", pa.string()),
            ("value", pa.float64()),
        ])
        empty_t = pa.Table.from_arrays([pa.array([]), pa.array([]), pa.array([]), pa.array([])], schema=schema)

        # 1. 0 vs 0
        diff_0_0, sum_0_0 = engine.diff_drops(empty_t, empty_t)
        assert diff_0_0.num_rows == 0
        assert sum_0_0.total_records == 0
        assert sum_0_0.recall_rate == 1.0

        # 2. 0 vs N
        n_tbl = pa.Table.from_pydict({
            "entity_id": ["E1", "E2"],
            "event_time": ["2023-10-02", "2023-10-02"],
            "metric": ["M1", "M2"],
            "value": [10.0, 20.0],
        })
        diff_0_n, sum_0_n = engine.diff_drops(empty_t, n_tbl)
        assert sum_0_n.new_records == 2
        assert sum_0_n.unchanged_records == 0
        assert sum_0_n.restated_records == 0
        assert sum_0_n.dropped_records == 0

        # 3. N vs 0
        diff_n_0, sum_n_0 = engine.diff_drops(n_tbl, empty_t)
        assert sum_n_0.dropped_records == 2
        assert sum_n_0.new_records == 0
        assert sum_n_0.unchanged_records == 0

    def test_zero_values_division_guard_and_identity(self) -> None:
        """Division by zero protection when baseline value is 0.0 or -0.0."""
        engine = DiffEngine()
        t1 = pa.Table.from_pydict({
            "entity_id": ["E_ZERO_1", "E_ZERO_2", "E_ZERO_3", "E_ZERO_4"],
            "event_time": ["2023-10-02"] * 4,
            "metric": ["M"] * 4,
            "value": [0.0, 0.0, -0.0, 100.0],
        })
        t2 = pa.Table.from_pydict({
            "entity_id": ["E_ZERO_1", "E_ZERO_2", "E_ZERO_3", "E_ZERO_4"],
            "event_time": ["2023-10-02"] * 4,
            "metric": ["M"] * 4,
            "value": [100.0, 0.0, +0.0, 0.0],
        })

        diff_tbl, summary = engine.diff_drops(t1, t2)
        rows = {r["entity_id"]: r for r in diff_tbl.to_pylist()}

        # E_ZERO_1: 0.0 -> 100.0: RESTATED, delta=100.0, pct_delta=None (guarded)
        assert rows["E_ZERO_1"]["diff_status"] == "RESTATED"
        assert rows["E_ZERO_1"]["restatement_delta"] == 100.0
        assert rows["E_ZERO_1"]["restatement_pct_delta"] is None

        # E_ZERO_2: 0.0 -> 0.0: UNCHANGED
        assert rows["E_ZERO_2"]["diff_status"] == "UNCHANGED"

        # E_ZERO_3: -0.0 -> +0.0: UNCHANGED
        assert rows["E_ZERO_3"]["diff_status"] == "UNCHANGED"

        # E_ZERO_4: 100.0 -> 0.0: RESTATED, delta=-100.0, pct_delta=-100.0%
        assert rows["E_ZERO_4"]["diff_status"] == "RESTATED"
        assert rows["E_ZERO_4"]["restatement_delta"] == -100.0
        assert rows["E_ZERO_4"]["restatement_pct_delta"] == -100.0

    def test_ieee_specials_nan_inf_boundaries(self) -> None:
        """Boundary handling of IEEE 754 specials: NaN, +Inf, -Inf."""
        engine = DiffEngine()
        t1 = pa.Table.from_pydict({
            "entity_id": ["E_NAN", "E_INF_POS", "E_INF_NEG", "E_INF_CROSS", "E_NAN_VAL"],
            "event_time": ["2023-10-02"] * 5,
            "metric": ["M"] * 5,
            "value": [float("nan"), float("inf"), float("-inf"), float("inf"), float("nan")],
        })
        t2 = pa.Table.from_pydict({
            "entity_id": ["E_NAN", "E_INF_POS", "E_INF_NEG", "E_INF_CROSS", "E_NAN_VAL"],
            "event_time": ["2023-10-02"] * 5,
            "metric": ["M"] * 5,
            "value": [float("nan"), float("inf"), float("-inf"), float("-inf"), 100.0],
        })

        diff_tbl, summary = engine.diff_drops(t1, t2)
        rows = {r["entity_id"]: r for r in diff_tbl.to_pylist()}

        assert rows["E_NAN"]["diff_status"] == "UNCHANGED"
        assert rows["E_INF_POS"]["diff_status"] == "UNCHANGED"
        assert rows["E_INF_NEG"]["diff_status"] == "UNCHANGED"
        assert rows["E_INF_CROSS"]["diff_status"] == "RESTATED"
        assert rows["E_NAN_VAL"]["diff_status"] == "RESTATED"

    def test_epsilon_threshold_micro_boundaries(self) -> None:
        """Micro-deltas above vs below epsilon tolerance."""
        engine = DiffEngine()
        t1 = pa.Table.from_pydict({
            "entity_id": ["E_MICRO_BELOW", "E_MICRO_ABOVE"],
            "event_time": ["2023-10-02"] * 2,
            "metric": ["M"] * 2,
            "value": [100.00000000000, 100.00000000000],
        })
        t2 = pa.Table.from_pydict({
            "entity_id": ["E_MICRO_BELOW", "E_MICRO_ABOVE"],
            "event_time": ["2023-10-02"] * 2,
            "metric": ["M"] * 2,
            "value": [100.00000000001, 100.00000010000],  # 1e-11 diff vs 1e-7 diff
        })

        diff_tbl, summary = engine.diff_drops(t1, t2, epsilon=1e-9)
        rows = {r["entity_id"]: r for r in diff_tbl.to_pylist()}

        assert rows["E_MICRO_BELOW"]["diff_status"] == "UNCHANGED"
        assert rows["E_MICRO_ABOVE"]["diff_status"] == "RESTATED"

    def test_extreme_scale_diffing_100k_rows(self) -> None:
        """Extreme scale stress test: diffing 100,000 distinct-key rows with planted restatements."""
        n = 100_000
        entities = [f"ENT_{i:06d}" for i in range(n)]
        metrics = ["CLOSE_PRICE"] * n
        events = ["2023-10-02T16:00:00Z"] * n
        vals1 = np.random.uniform(10.0, 500.0, n)
        vals2 = vals1.copy()

        # Plant exactly 5,000 restatements
        restated_indices = np.random.choice(n, 5000, replace=False)
        vals2[restated_indices] += 2.50

        t1 = pa.Table.from_arrays(
            [pa.array(entities), pa.array(events), pa.array(metrics), pa.array(vals1)],
            names=["entity_id", "event_time", "metric", "value"],
        )
        t2 = pa.Table.from_arrays(
            [pa.array(entities), pa.array(events), pa.array(metrics), pa.array(vals2)],
            names=["entity_id", "event_time", "metric", "value"],
        )

        engine = DiffEngine()
        t0 = time.perf_counter()
        diff_tbl, summary = engine.diff_drops(t1, t2)
        elapsed = time.perf_counter() - t0

        assert summary.restated_records == 5000
        assert summary.unchanged_records == 95000
        assert summary.new_records == 0
        assert summary.dropped_records == 0
        assert summary.recall_rate == 1.0
        assert summary.precision_rate == 1.0
        assert elapsed < 3.0, f"Diffing 100k rows took {elapsed:.2f}s >= 3.0s"

    def test_concurrent_diff_and_reporting_stress(self, fixture_paths: Any) -> None:
        """High concurrency stress: multiple threads executing diffs and report generations."""
        d1 = pq.read_table(fixture_paths.drop_001_parquet)
        d2 = pq.read_table(fixture_paths.drop_002_parquet)

        def worker_task(worker_id: int) -> dict[str, Any]:
            with DiffEngine() as eng:
                diff_tbl, summary = eng.diff_drops(d1, d2)
                assert summary.restated_records == 10
                assert summary.unchanged_records == 45
                assert summary.new_records == 10
                assert summary.dropped_records == 5

                reporter = ReadinessReporter()
                md_str, json_dict = reporter.generate_report(
                    dataset_id_or_context=f"CONCUR_DS_{worker_id}",
                    vendor_id="VEND_STRESS",
                    table=diff_tbl,
                    diff_summary=summary,
                )
                assert json_dict["total_records"] == 70
                assert "VEND_STRESS" in md_str
                return {"worker_id": worker_id, "status": json_dict["overall_status"]}

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(worker_task, i) for i in range(16)]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]

        assert len(results) == 16
        for r in results:
            assert r["status"] == "PASS"


# ==============================================================================
# Category 4: 100% Anomaly Coverage in Vendor Inquiries (F-REP-03)
# ==============================================================================


class TestAnomalyCoverageInVendorInquiries:
    """Verifies Invariant 4: Every single pipeline anomaly category produces actionable inquiry."""

    def test_100_percent_anomaly_coverage_all_categories(self) -> None:
        """Every recognized anomaly category produces an inquiry starting with 'Subject: ' and len > 50."""
        gen = VendorQuestionsGenerator()

        anomaly_categories: list[tuple[str, dict[str, Any]]] = [
            (
                "range_domain",
                {
                    "validation_results": [
                        ValidationResult(
                            check_name="range_domain",
                            passed=False,
                            total_records=100,
                            violation_count=5,
                            violation_rate=0.05,
                            violations_sample=[{"source_symbol": "AAPL", "value": -150.0}],
                            threshold=0.0,
                        )
                    ]
                },
            ),
            (
                "completeness",
                {
                    "validation_results": [
                        ValidationResult(
                            check_name="completeness",
                            passed=False,
                            total_records=100,
                            violation_count=8,
                            violation_rate=0.08,
                            violations_sample=[],
                            threshold=0.01,
                        )
                    ]
                },
            ),
            (
                "future_timestamps",
                {
                    "validation_results": [
                        ValidationResult(
                            check_name="future_timestamps",
                            passed=False,
                            total_records=100,
                            violation_count=3,
                            violation_rate=0.03,
                            violations_sample=[],
                            threshold=0.0,
                        )
                    ]
                },
            ),
            (
                "monotone_timestamps",
                {
                    "validation_results": [
                        ValidationResult(
                            check_name="monotone_timestamps",
                            passed=False,
                            total_records=100,
                            violation_count=4,
                            violation_rate=0.04,
                            violations_sample=[],
                            threshold=0.0,
                        )
                    ]
                },
            ),
            (
                "intra_drop_duplicates",
                {
                    "validation_results": [
                        ValidationResult(
                            check_name="intra_drop_duplicates",
                            passed=False,
                            total_records=100,
                            violation_count=6,
                            violation_rate=0.06,
                            violations_sample=[],
                            threshold=0.0,
                        )
                    ]
                },
            ),
            (
                "universe_coverage",
                {
                    "validation_results": [
                        ValidationResult(
                            check_name="universe_coverage",
                            passed=False,
                            total_records=100,
                            violation_count=12,
                            violation_rate=0.12,
                            violations_sample=[],
                            threshold=0.95,
                        )
                    ]
                },
            ),
            (
                "positive_values",
                {
                    "validation_results": [
                        ValidationResult(
                            check_name="positive_values",
                            passed=False,
                            total_records=100,
                            violation_count=2,
                            violation_rate=0.02,
                            violations_sample=[],
                            threshold=0.0,
                        )
                    ]
                },
            ),
            (
                "custom_rule_generic",
                {
                    "validation_results": [
                        ValidationResult(
                            check_name="volatility_spike_guard",
                            passed=False,
                            total_records=100,
                            violation_count=7,
                            violation_rate=0.07,
                            violations_sample=[],
                            threshold=0.02,
                        )
                    ]
                },
            ),
            (
                "coverage_dip_alert",
                {
                    "coverage_dips": [
                        CoverageDipAlert(
                            date="2023-10-09",
                            baseline_count=500,
                            current_count=350,
                            dip_percentage=30.0,
                            description="Coverage dropped 30.0% on 2023-10-09",
                        )
                    ]
                },
            ),
            (
                "schema_drift_changes",
                {"drift_report_changes": ["Column 'tax_rate' removed", "Type 'price' changed to VARCHAR"]},
            ),
            (
                "diff_summary_restatements",
                {
                    "diff_summary": DiffSummary(
                        drop_id_baseline="DROP_001",
                        drop_id_current="DROP_002",
                        new_records=10,
                        unchanged_records=45,
                        restated_records=10,
                        dropped_records=5,
                        recall_rate=1.0,
                    )
                },
            ),
            (
                "quarantine_unmapped_symbols",
                {
                    "quarantine_records": [
                        {
                            "raw_symbol": "UNKNOWN_TICKER_X",
                            "vendor_id": "SYNTH_VENDOR",
                            "synthetic_id": "UNRESOLVED_SYNTH_VENDOR_UNKNOWN_TICKER_X",
                        }
                    ]
                },
            ),
        ]

        total_tested = 0
        for category_name, kwargs in anomaly_categories:
            inquiries = gen.generate_inquiries(**kwargs)
            assert len(inquiries) >= 1, f"No inquiry generated for anomaly category: {category_name}"
            for inq in inquiries:
                assert inq.startswith("Subject: "), (
                    f"Inquiry for {category_name} does not start with 'Subject: ': {inq[:30]}..."
                )
                assert len(inq) > 50, (
                    f"Inquiry for {category_name} length {len(inq)} <= 50: {inq}"
                )
            total_tested += 1

        assert total_tested == 12, f"Expected 12 anomaly categories, tested {total_tested}"


# ==============================================================================
# Category 5: Dual-Format Parity Invariants (F-REP-04)
# ==============================================================================


class TestDualFormatParityInvariants:
    """Verifies Invariant 5: Dual-format parity between Markdown and JSON reports."""

    def test_clean_pass_dual_format_parity(self, tmp_path: Path) -> None:
        """Parity when all rules pass."""
        tbl = pa.Table.from_pydict({
            "entity_id": ["E1", "E2"],
            "event_time": ["2023-10-02T16:00:00Z", "2023-10-02T16:00:00Z"],
            "value": [10.0, 20.0],
        })
        val_results = [
            ValidationResult("completeness", True, 2, 0, 0.0, [], 0.0),
            ValidationResult("range_domain", True, 2, 0, 0.0, [], 0.0),
        ]

        reporter = ReadinessReporter()
        md_str, json_dict = reporter.generate_report(
            dataset_id_or_context="CLEAN_PASS_DS",
            vendor_id="VEND_A",
            table=tbl,
            validation_results=val_results,
        )

        # 1. Overall status parity
        assert json_dict["overall_status"] == "PASS"
        assert "**Status**: PASS" in md_str

        # 2. Record counts parity
        assert json_dict["total_records"] == 2
        assert "**Records**: 2" in md_str

        # 3. Scorecard metrics parity
        assert len(json_dict["scorecard"]) == 2
        for sc in json_dict["scorecard"]:
            rule = sc["rule"]
            status = "PASS" if sc["passed"] else "FAIL"
            assert f"| {rule} | {status} |" in md_str

        # 4. File export verification
        md_path, json_path = export_reports(tmp_path / "reports", md_str, json_dict)
        assert md_path.exists() and json_path.exists()
        loaded_json = json.loads(json_path.read_text(encoding="utf-8"))
        assert loaded_json["overall_status"] == "PASS"
        assert loaded_json["total_records"] == 2

    def test_complex_anomalies_dual_format_parity(self, tmp_path: Path) -> None:
        """Parity under complex pipeline failures with scorecard, dips, and inquiries."""
        tbl = pa.Table.from_pydict({
            "entity_id": ["E1", "E2", "E3"],
            "event_time": ["2023-10-02T16:00:00Z", "2023-10-02T16:00:00Z", "2023-10-02T16:00:00Z"],
            "value": [10.0, -5.0, 30.0],
        })
        val_results = [
            ValidationResult("completeness", True, 3, 0, 0.0, [], 0.0),
            ValidationResult(
                "range_domain",
                False,
                3,
                1,
                0.3333,
                [{"entity_id": "E2", "value": -5.0}],
                0.0,
            ),
        ]
        diff_sum = DiffSummary(
            drop_id_baseline="DROP_001",
            drop_id_current="DROP_002",
            new_records=5,
            unchanged_records=20,
            restated_records=8,
            dropped_records=2,
            recall_rate=1.0,
        )

        reporter = ReadinessReporter()
        ctx = PipelineRunContext(
            dataset_id="FAIL_DS",
            vendor_id="VEND_FAIL",
            table=tbl,
            validation_results=val_results,
            diff_summary=diff_sum,
            drift_changes=["Dropped column 'volume'"],
        )
        md_str, json_dict = reporter.generate_report(ctx)

        # 1. Overall status parity
        assert json_dict["overall_status"] == "FAIL"
        assert "**Status**: FAIL" in md_str

        # 2. Record count parity
        assert json_dict["total_records"] == 3
        assert "**Records**: 3" in md_str

        # 3. Scorecard rules parity
        assert len(json_dict["scorecard"]) == 2
        assert "| range_domain | FAIL | 1 |" in md_str
        assert "| completeness | PASS | 0 |" in md_str

        # 4. Inquiries parity
        assert len(json_dict["vendor_inquiries"]) == 3  # range_domain + drift + restatements
        for inq in json_dict["vendor_inquiries"]:
            assert inq in md_str


# ==============================================================================
# Category 6: Empirical Mutation Testing
# ==============================================================================


class TestEmpiricalMutationTesting:
    """Empirically injects subtle mutations and verifies test suite kills every mutant."""

    def test_mutant_killed_epsilon_relaxation(self) -> None:
        """Mutant 1: Epsilon relaxed to 0.5 incorrectly swallows real restatements (0.05 delta)."""
        t1 = pa.Table.from_pydict({
            "entity_id": ["E1"],
            "event_time": ["2023-10-02"],
            "metric": ["VAL"],
            "value": [100.00],
        })
        t2 = pa.Table.from_pydict({
            "entity_id": ["E1"],
            "event_time": ["2023-10-02"],
            "metric": ["VAL"],
            "value": [100.05],  # 0.05 real revision
        })

        engine = DiffEngine()
        # Normal engine with default epsilon (1e-9) detects restatement
        _, normal_summary = engine.diff_drops(t1, t2)
        assert normal_summary.restated_records == 1

        # Mutant with relaxed epsilon (0.5) misses the restatement
        _, mutant_summary = engine.diff_drops(t1, t2, epsilon=0.5)
        assert mutant_summary.restated_records == 0  # Mutant killed: misclassifies restatement!

    def test_mutant_killed_zero_tolerance_flakiness(self) -> None:
        """Mutant 2: Strict zero tolerance (epsilon=0.0) misclassifies micro-noise as restated."""
        t1 = pa.Table.from_pydict({
            "entity_id": ["E1"],
            "event_time": ["2023-10-02"],
            "metric": ["VAL"],
            "value": [100.00000000000001],
        })
        t2 = pa.Table.from_pydict({
            "entity_id": ["E1"],
            "event_time": ["2023-10-02"],
            "metric": ["VAL"],
            "value": [100.00000000000003],
        })

        engine = DiffEngine()
        # Default epsilon classifies as UNCHANGED
        _, normal_summary = engine.diff_drops(t1, t2)
        assert normal_summary.unchanged_records == 1
        assert normal_summary.restated_records == 0

        # Zero-tolerance mutant flags as RESTATED
        _, mutant_summary = engine.diff_drops(t1, t2, epsilon=0.0)
        assert mutant_summary.restated_records == 1  # Mutant killed: false positive restatement!

    def test_mutant_killed_dropped_metric_in_composite_key(self) -> None:
        """Mutant 3: Omission of 'metric' from key cross-joins different metrics for same entity."""
        t1 = pa.Table.from_pydict({
            "entity_id": ["E1", "E1"],
            "event_time": ["2023-10-02", "2023-10-02"],
            "metric": ["PRICE", "VOLUME"],
            "value": [150.0, 1_000_000.0],
        })
        t2 = pa.Table.from_pydict({
            "entity_id": ["E1", "E1"],
            "event_time": ["2023-10-02", "2023-10-02"],
            "metric": ["PRICE", "VOLUME"],
            "value": [150.0, 1_000_000.0],
        })

        engine = DiffEngine()
        # Standard composite key: entity_id + event_time + metric
        _, normal_summary = engine.diff_drops(t1, t2)
        assert normal_summary.unchanged_records == 2
        assert normal_summary.restated_records == 0

        # Defective key: only entity_id + event_time
        diff_mutant, mutant_summary = engine.diff_drops(t1, t2, keys=["entity_id", "event_time"])
        # Cartesian join produces 4 rows instead of 2!
        assert diff_mutant.num_rows == 4  # Mutant killed: key collision caught!

    def test_mutant_killed_missing_subject_or_short_inquiry(self) -> None:
        """Mutant 4: Truncated inquiry generator fails invariant checks."""
        class DefectiveInquiryGenerator(VendorQuestionsGenerator):
            def generate_inquiries(self, **kwargs: Any) -> list[str]:
                return ["Hey fix your data"]  # Missing Subject: and len < 50

        mutant_gen = DefectiveInquiryGenerator()
        bad_inquiries = mutant_gen.generate_inquiries()

        # Invariant checks must reject this mutant
        assert not bad_inquiries[0].startswith("Subject: ")
        assert len(bad_inquiries[0]) <= 50  # Mutant killed!

    def test_mutant_killed_revision_seq_not_incremented(self) -> None:
        """Mutant 5: Failing to increment revision_seq on restated records is killed."""
        t1 = pa.Table.from_pydict({
            "record_id": ["R1"],
            "entity_id": ["E1"],
            "event_time": ["2023-10-02"],
            "metric": ["PRICE"],
            "value": [100.0],
            "revision_seq": [1],
        })
        t2 = pa.Table.from_pydict({
            "record_id": ["R1_REST"],
            "entity_id": ["E1"],
            "event_time": ["2023-10-02"],
            "metric": ["PRICE"],
            "value": [105.0],
            "revision_seq": [1],
        })

        engine = DiffEngine()
        diff_tbl, _ = engine.diff_drops(t1, t2)
        row = diff_tbl.to_pylist()[0]
        # Invariant requires revision_seq == base.revision_seq + 1
        assert row["revision_seq"] == 2, f"Expected revision_seq 2, got {row['revision_seq']}"

    def test_mutant_killed_immutability_partition_tampering(
        self, tmp_path: Path
    ) -> None:
        """Mutant 6: Direct tampering or re-append to existing partition is blocked by ImmutabilityError."""
        store_dir = tmp_path / "store_mutant"
        store = BitemporalStore(root_dir=store_dir)
        ds = "TAMPER_DS"

        t1 = pa.Table.from_pydict({
            "record_id": ["R1"],
            "entity_id": ["E1"],
            "event_time": [datetime(2023, 10, 2, 16, 0, 0, tzinfo=UTC)],
            "knowledge_time": [datetime(2023, 10, 6, 20, 0, 0, tzinfo=UTC)],
            "metric": ["PRICE"],
            "value": [100.0],
            "unit": ["USD"],
            "vendor_id": ["SYNTH"],
            "dataset_id": [ds],
            "drop_id": ["DROP_001"],
            "is_restatement": [False],
            "revision_seq": [1],
        })
        store.append(t1, dataset_id=ds, drop_id="DROP_001")

        # Mutant attempting to overwrite DROP_001 partition must be killed with ImmutabilityError
        with pytest.raises(ImmutabilityError, match="Partition already exists and is immutable"):
            store.append(t1, dataset_id=ds, drop_id="DROP_001")

    def test_drop_003_anomalies_full_pipeline_coverage(
        self, fixture_paths: Any
    ) -> None:
        """Adversarial verification of Drop 3 anomalies: 100% anomaly coverage against truth manifest."""
        from vendor_onboarding.validation.engine import ValidationEngine

        t3 = read_file(fixture_paths.drop_003_csv)
        truth = pq.read_table(fixture_paths.truth_manifest_parquet)

        # Inspect planted anomalies in truth manifest for Drop 3
        con = duckdb.connect()
        con.register("truth", truth)
        truth_violations = set(
            con.sql(
                "SELECT expected_rule_violation FROM truth WHERE drop_id = 'DROP_003' AND is_planted_anomaly = true"
            ).fetchnumpy()["expected_rule_violation"]
        )
        assert truth_violations == {
            "MONOTONE_INVERSION",
            "NEGATIVE_PRICE",
            "NULL_SLA_VIOLATION",
            "FUTURE_TIMESTAMP",
            "UNMAPPED_SYMBOL",
        }

        # Run ValidationEngine
        engine = ValidationEngine()
        val_results = engine.validate(t3)
        failed_checks = {vr.check_name for vr in val_results if not vr.passed}
        assert "range_domain" in failed_checks  # NEGATIVE_PRICE
        assert "completeness" in failed_checks  # NULL_SLA_VIOLATION
        assert "monotone_timestamps" in failed_checks  # MONOTONE_INVERSION
        assert "future_timestamps" in failed_checks  # FUTURE_TIMESTAMP

        # Quarantine simulation for UNMAPPED_SYMBOL
        quarantine = [
            {
                "raw_symbol": "UNKNOWN_TICKER_X",
                "vendor_id": "SYNTH_VENDOR_A",
                "synthetic_id": "UNRESOLVED_SYNTH_VENDOR_A_UNKNOWN_TICKER_X",
            }
        ]

        # Generate report and inquiries
        reporter = ReadinessReporter()
        ctx = PipelineRunContext(
            dataset_id="EQUITY_DROP_003",
            vendor_id="SYNTH_VENDOR_A",
            table=t3,
            validation_results=val_results,
            quarantine_records=quarantine,
            drop_id="DROP_003",
        )
        md_str, json_dict = reporter.generate_report(ctx)

        inquiries = json_dict["vendor_inquiries"]
        assert len(inquiries) >= 5, f"Expected at least 5 inquiries, got {len(inquiries)}"

        inquiry_text = "\n".join(inquiries)
        assert "Negative Prices Detected" in inquiry_text
        assert "Null Values in Mandatory Fields" in inquiry_text
        assert "Timestamp Inversion / Clock Skew" in inquiry_text
        assert "Future Timestamps Detected" in inquiry_text
        assert "Unmapped Identifier" in inquiry_text

        # All inquiries must conform to Invariant 4
        for inq in inquiries:
            assert inq.startswith("Subject: ")
            assert len(inq) > 50

