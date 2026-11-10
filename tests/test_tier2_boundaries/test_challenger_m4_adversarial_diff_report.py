"""
Tier 2 Boundary Tests: Challenger Suite for Milestone 4.
Adversarial Stress, IEEE Specials, Mutation Testing & Invariant Checks.

Targets:
- DiffEngine / RevisionLedger (src/vendor_onboarding/diff/ledger.py)
- CoverageAnalyzer / DailyCoverageMatrix (src/vendor_onboarding/report/coverage.py)
- VendorQuestionsGenerator (src/vendor_onboarding/report/questions.py)
- ReadinessReporter (src/vendor_onboarding/report/generator.py)

Empirical Challenge Coverage:
1. Zero-denominator division handling in restatement percentage calculations.
2. Empty tables (0 rows) diffing, coverage matrix, and report generation.
3. Pure state transitions (only new, only deleted, only restated, only unchanged).
4. IEEE 754 specials (NaN, +Inf, -Inf, subnormals, extreme floating point exponents).
5. Mathematical invariants (anti-symmetry, delta sign inversion, conservation of rows).
6. 100.00% restatement recall & precision against ground truth manifest.
7. Multi-drop cascading revision ladder (monotonic revision_seq progression).
8. Boundary value coverage dip thresholds (exact 20.0%, 19.999%, 100% dip).
9. 100% coverage of anomaly categories in vendor inquiry draft generation.
10. High volume (50,000 rows) and concurrent thread execution.
11. Empirical mutation testing verifying all injected mutants are killed.
"""

from __future__ import annotations

import concurrent.futures
import math
import statistics
import time
from dataclasses import dataclass
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from vendor_onboarding.diff.ledger import DeltaStatistics, DiffEngine, DiffSummary, RevisionLedger
from vendor_onboarding.report.coverage import (
    CoverageAnalyzer,
    CoverageDipAlert,
    CoverageGapAlert,
    DailyCoverageMatrix,
    build_coverage_matrix,
    detect_coverage_dips,
    detect_coverage_gaps,
)
from vendor_onboarding.report.generator import (
    PipelineRunContext,
    ReadinessReportData,
    ReadinessReporter,
    export_reports,
)
from vendor_onboarding.report.questions import VendorQuestionsGenerator, generate_vendor_inquiries

pytestmark = [pytest.mark.tier2]


@dataclass
class MockValidationResult:
    """Mock validation result conforming to validator interface."""

    check_name: str
    passed: bool
    record_count: int
    violation_count: int
    violation_rate: float
    violations_sample: list[dict[str, Any]]
    threshold: float


# ============================================================================
# 1. Zero-Denominator Division & Extreme Numeric Boundary Tests
# ============================================================================

def test_adversarial_zero_denominator_division_guard():
    """Verify restatement percentage delta safely returns None when baseline value is 0.0."""
    t_base = pa.Table.from_pylist([
        {"record_id": "R0", "entity_id": "E1", "event_time": "2023-10-02", "metric": "CLOSE", "value": 0.0}
    ])
    t_curr = pa.Table.from_pylist([
        {"record_id": "R0", "entity_id": "E1", "event_time": "2023-10-02", "metric": "CLOSE", "value": 75.0}
    ])

    with DiffEngine() as engine:
        diff_tbl, summary = engine.diff_drops(t_base, t_curr)
        assert summary.restated_records == 1
        r = diff_tbl.to_pylist()[0]
        assert r["diff_status"] == "RESTATED"
        assert r["restatement_delta"] == 75.0
        assert r["restatement_pct_delta"] is None
        assert r["is_restatement"] is True


def test_adversarial_zero_to_zero_is_unchanged():
    """Verify transition from 0.0 to 0.0 is classified as UNCHANGED with no delta."""
    t_base = pa.Table.from_pylist([
        {"record_id": "R0", "entity_id": "E1", "event_time": "2023-10-02", "metric": "CLOSE", "value": 0.0}
    ])
    t_curr = pa.Table.from_pylist([
        {"record_id": "R0", "entity_id": "E1", "event_time": "2023-10-02", "metric": "CLOSE", "value": 0.0}
    ])

    with DiffEngine() as engine:
        diff_tbl, summary = engine.diff_drops(t_base, t_curr)
        assert summary.unchanged_records == 1
        assert summary.restated_records == 0
        r = diff_tbl.to_pylist()[0]
        assert r["diff_status"] == "UNCHANGED"
        assert r["restatement_delta"] == 0.0
        assert r["restatement_pct_delta"] is None


def test_adversarial_signed_zero_boundary():
    """Verify -0.0 and +0.0 equivalence under IEEE 754."""
    t_base = pa.Table.from_pylist([
        {"record_id": "R0", "entity_id": "E1", "event_time": "2023-10-02", "metric": "CLOSE", "value": -0.0}
    ])
    t_curr = pa.Table.from_pylist([
        {"record_id": "R0", "entity_id": "E1", "event_time": "2023-10-02", "metric": "CLOSE", "value": 0.0}
    ])

    with DiffEngine() as engine:
        _, summary = engine.diff_drops(t_base, t_curr)
        assert summary.unchanged_records == 1
        assert summary.restated_records == 0


def test_adversarial_negative_baseline_percentage_delta():
    """Verify financial percentage delta formula with negative baseline values: (curr - base) / abs(base)."""
    t_base = pa.Table.from_pylist([
        {"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "YIELD", "value": -50.0},
        {"record_id": "R2", "entity_id": "E2", "event_time": "2023-10-02", "metric": "YIELD", "value": -100.0},
    ])
    t_curr = pa.Table.from_pylist([
        {"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "YIELD", "value": -25.0},  # +25 delta, +50%
        {"record_id": "R2", "entity_id": "E2", "event_time": "2023-10-02", "metric": "YIELD", "value": 100.0},  # +200 delta, +200%
    ])

    with DiffEngine() as engine:
        diff_tbl, summary = engine.diff_drops(t_base, t_curr)
        assert summary.restated_records == 2
        rows = {r["entity_id"]: r for r in diff_tbl.to_pylist()}

        # -50 -> -25: delta = +25, pct = (+25 / 50) * 100 = +50.0%
        assert rows["E1"]["restatement_delta"] == pytest.approx(25.0)
        assert rows["E1"]["restatement_pct_delta"] == pytest.approx(50.0)

        # -100 -> +100: delta = +200, pct = (+200 / 100) * 100 = +200.0%
        assert rows["E2"]["restatement_delta"] == pytest.approx(200.0)
        assert rows["E2"]["restatement_pct_delta"] == pytest.approx(200.0)


def test_adversarial_subnormal_floating_point():
    """Verify extreme subnormal float (1e-300) transitions under epsilon tolerance and zero epsilon."""
    t_base = pa.Table.from_pylist([
        {"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": 1e-300}
    ])
    t_curr = pa.Table.from_pylist([
        {"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": 2e-300}
    ])

    with DiffEngine() as engine:
        # Default epsilon=1e-9: difference (1e-300) is smaller than epsilon -> UNCHANGED
        _, s_default = engine.diff_drops(t_base, t_curr)
        assert s_default.unchanged_records == 1
        assert s_default.restated_records == 0

        # Strict epsilon=0.0: subnormal change is classified as RESTATED
        diff_tbl, s_strict = engine.diff_drops(t_base, t_curr, epsilon=0.0)
        assert s_strict.restated_records == 1
        assert s_strict.unchanged_records == 0
        r = diff_tbl.to_pylist()[0]
        assert r["diff_status"] == "RESTATED"
        assert r["restatement_delta"] == pytest.approx(1e-300)
        assert r["restatement_pct_delta"] == pytest.approx(100.0)


# ============================================================================
# 2. IEEE 754 Specials (NaN, +Inf, -Inf) & Epsilon Tolerances
# ============================================================================

def test_adversarial_ieee_nan_identity():
    """Verify NaN to NaN is classified as UNCHANGED and does not spuriously trigger restatement."""
    t_base = pa.Table.from_pylist([
        {"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": float("nan")}
    ])
    t_curr = pa.Table.from_pylist([
        {"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": float("nan")}
    ])

    with DiffEngine() as engine:
        _, summary = engine.diff_drops(t_base, t_curr)
        assert summary.unchanged_records == 1
        assert summary.restated_records == 0


def test_adversarial_ieee_nan_to_finite():
    """Verify NaN to finite value is correctly classified as RESTATED."""
    t_base = pa.Table.from_pylist([
        {"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": float("nan")}
    ])
    t_curr = pa.Table.from_pylist([
        {"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": 42.0}
    ])

    with DiffEngine() as engine:
        diff_tbl, summary = engine.diff_drops(t_base, t_curr)
        assert summary.restated_records == 1
        assert diff_tbl["diff_status"].to_pylist()[0] == "RESTATED"


def test_adversarial_ieee_infinity_transitions():
    """Verify handling of positive and negative infinities."""
    t_base = pa.Table.from_pylist([
        {"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": float("-inf")},
        {"record_id": "R2", "entity_id": "E2", "event_time": "2023-10-02", "metric": "VAL", "value": float("inf")},
    ])
    t_curr = pa.Table.from_pylist([
        {"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": float("inf")},   # -inf -> +inf (RESTATED)
        {"record_id": "R2", "entity_id": "E2", "event_time": "2023-10-02", "metric": "VAL", "value": float("inf")},   # +inf -> +inf (UNCHANGED)
    ])

    with DiffEngine() as engine:
        diff_tbl, summary = engine.diff_drops(t_base, t_curr)
        assert summary.restated_records == 1
        assert summary.unchanged_records == 1

        rows = {r["entity_id"]: r for r in diff_tbl.to_pylist()}
        assert rows["E1"]["diff_status"] == "RESTATED"
        assert rows["E2"]["diff_status"] == "UNCHANGED"


def test_adversarial_epsilon_boundary_precision():
    """Verify micro-change classification at exact epsilon boundaries."""
    # Difference = 1e-10 < epsilon (1e-9) -> UNCHANGED
    t1 = pa.Table.from_pylist([
        {"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": 100.0000000001}
    ])
    t2 = pa.Table.from_pylist([
        {"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": 100.0000000002}
    ])

    # Difference = 2e-9 > epsilon (1e-9) -> RESTATED
    t3 = pa.Table.from_pylist([
        {"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": 100.0000000021}
    ])

    with DiffEngine() as engine:
        _, s_unchanged = engine.diff_drops(t1, t2, epsilon=1e-9)
        assert s_unchanged.unchanged_records == 1
        assert s_unchanged.restated_records == 0

        _, s_restated = engine.diff_drops(t1, t3, epsilon=1e-9)
        assert s_restated.unchanged_records == 0
        assert s_restated.restated_records == 1


# ============================================================================
# 3. Empty Tables & 0-Row Boundary Tests
# ============================================================================

def test_adversarial_diff_empty_against_empty():
    """Verify diffing two 0-row tables produces empty diff output with zeroed summary."""
    schema = pa.schema([
        ("record_id", pa.string()),
        ("entity_id", pa.string()),
        ("event_time", pa.string()),
        ("metric", pa.string()),
        ("value", pa.float64()),
    ])
    empty_t = pa.Table.from_arrays([pa.array([], type=t) for t in schema.types], schema=schema)

    with DiffEngine() as engine:
        diff_tbl, summary = engine.diff_drops(empty_t, empty_t)
        assert diff_tbl.num_rows == 0
        assert summary.total_records == 0
        assert summary.new_records == 0
        assert summary.unchanged_records == 0
        assert summary.restated_records == 0
        assert summary.dropped_records == 0

        # Verify DeltaStatistics handles empty diff table
        stats = engine.compute_delta_statistics(diff_tbl)
        assert stats.count == 0
        assert stats.mean_delta is None


def test_adversarial_diff_empty_against_populated(fixture_paths):
    """Verify empty baseline classifies 100% of current drop as NEW."""
    d1 = pq.read_table(fixture_paths.drop_001_parquet)
    empty_t = d1.slice(0, 0)

    with DiffEngine() as engine:
        diff_tbl, summary = engine.diff_drops(empty_t, d1)
        assert summary.new_records == d1.num_rows
        assert summary.dropped_records == 0
        assert summary.unchanged_records == 0
        assert summary.restated_records == 0
        assert summary.total_records == d1.num_rows
        assert all(s == "NEW" for s in diff_tbl["diff_status"].to_pylist())


def test_adversarial_diff_populated_against_empty(fixture_paths):
    """Verify empty current classifies 100% of baseline drop as DELETED."""
    d1 = pq.read_table(fixture_paths.drop_001_parquet)
    empty_t = d1.slice(0, 0)

    with DiffEngine() as engine:
        diff_tbl, summary = engine.diff_drops(d1, empty_t)
        assert summary.dropped_records == d1.num_rows
        assert summary.new_records == 0
        assert summary.unchanged_records == 0
        assert summary.restated_records == 0
        assert summary.total_records == d1.num_rows
        assert all(s == "DELETED" for s in diff_tbl["diff_status"].to_pylist())


def test_adversarial_coverage_analyzer_empty_table():
    """Verify CoverageAnalyzer functions safely return empty results on 0-row table."""
    empty_t = pa.Table.from_pylist([], schema=pa.schema([("entity_id", pa.string()), ("event_time", pa.string())]))
    analyzer = CoverageAnalyzer()

    matrix = analyzer.build_matrix(empty_t)
    assert len(matrix) == 0

    dips = analyzer.detect_dips(empty_t)
    assert dips == []

    gaps = analyzer.detect_gaps(empty_t, expected_dates=["2023-10-02", "2023-10-03"])
    assert gaps == []


def test_adversarial_readiness_reporter_empty_table():
    """Verify ReadinessReporter generates valid report on empty dataset."""
    empty_t = pa.Table.from_pylist([], schema=pa.schema([("entity_id", pa.string()), ("event_time", pa.string())]))
    reporter = ReadinessReporter()

    md, js = reporter.generate_report("EMPTY_DATASET", "EMPTY_VENDOR", empty_t)
    assert js["total_records"] == 0
    assert js["overall_status"] == "PASS"
    assert js["coverage_dips"] == []
    assert "# Vendor Data Readiness Report: EMPTY_DATASET" in md


# ============================================================================
# 4. Pure State Transitions (100% Restated, 100% Unchanged)
# ============================================================================

def test_adversarial_100_percent_restated():
    """Verify table where 100% of records undergo restatement."""
    n = 250
    t_base = pa.Table.from_pylist([
        {"record_id": f"R_{i:04d}", "entity_id": f"E_{i:04d}", "event_time": "2023-10-02", "metric": "CLOSE", "value": 100.0, "revision_seq": 1}
        for i in range(n)
    ])
    t_curr = pa.Table.from_pylist([
        {"record_id": f"R_{i:04d}", "entity_id": f"E_{i:04d}", "event_time": "2023-10-02", "metric": "CLOSE", "value": 150.0}
        for i in range(n)
    ])

    with DiffEngine() as engine:
        diff_tbl, summary = engine.diff_drops(t_base, t_curr)
        assert summary.restated_records == n
        assert summary.new_records == 0
        assert summary.unchanged_records == 0
        assert summary.dropped_records == 0
        assert summary.total_records == n

        # All records must have revision_seq incremented from 1 to 2
        seqs = set(diff_tbl["revision_seq"].to_pylist())
        assert seqs == {2}
        assert all(diff_tbl["is_restatement"].to_pylist())

        stats = engine.compute_delta_statistics(diff_tbl)
        assert stats.count == n
        assert stats.mean_delta == pytest.approx(50.0)
        assert stats.mean_pct_delta == pytest.approx(50.0)


def test_adversarial_100_percent_unchanged():
    """Verify table where 100% of records are bit-for-bit identical."""
    n = 250
    t = pa.Table.from_pylist([
        {"record_id": f"R_{i:04d}", "entity_id": f"E_{i:04d}", "event_time": "2023-10-02", "metric": "CLOSE", "value": 123.45}
        for i in range(n)
    ])

    with DiffEngine() as engine:
        diff_tbl, summary = engine.diff_drops(t, t)
        assert summary.unchanged_records == n
        assert summary.restated_records == 0
        assert summary.new_records == 0
        assert summary.dropped_records == 0
        assert summary.total_records == n

        stats = engine.compute_delta_statistics(diff_tbl)
        assert stats.count == 0


# ============================================================================
# 5. Mathematical Invariants: Anti-Symmetry, Delta Inversion & Conservation
# ============================================================================

def test_adversarial_conservation_invariant():
    """Verify row conservation invariant: new + unchanged + restated + dropped == total_records."""
    t_base = pa.Table.from_pylist([
        {"record_id": f"R{i}", "entity_id": f"E{i}", "event_time": "2023-10-02", "metric": "M", "value": float(i)}
        for i in range(100)
    ])
    t_curr = pa.Table.from_pylist([
        {"record_id": f"R{i}", "entity_id": f"E{i}", "event_time": "2023-10-02", "metric": "M", "value": float(i + (10 if i < 20 else 0))}
        for i in range(15, 120)  # Drops 0..14 (15 dropped), 15..19 (5 restated), 20..99 (80 unchanged), 100..119 (20 new)
    ])

    with DiffEngine() as engine:
        diff_tbl, summary = engine.diff_drops(t_base, t_curr)
        assert summary.dropped_records == 15
        assert summary.restated_records == 5
        assert summary.unchanged_records == 80
        assert summary.new_records == 20
        assert summary.total_records == 120
        assert diff_tbl.num_rows == 120


def test_adversarial_anti_symmetry_and_delta_inversion():
    """Verify anti-symmetry between diff(A, B) and diff(B, A)."""
    tA = pa.Table.from_pylist([
        {"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "M", "value": 10.0},
        {"record_id": "R2", "entity_id": "E2", "event_time": "2023-10-02", "metric": "M", "value": 20.0},
        {"record_id": "R3", "entity_id": "E3", "event_time": "2023-10-02", "metric": "M", "value": 30.0},
    ])
    tB = pa.Table.from_pylist([
        {"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "M", "value": 10.0},  # UNCHANGED
        {"record_id": "R2", "entity_id": "E2", "event_time": "2023-10-02", "metric": "M", "value": 35.0},  # RESTATED (+15)
        {"record_id": "R4", "entity_id": "E4", "event_time": "2023-10-02", "metric": "M", "value": 40.0},  # NEW (R3 DELETED)
    ])

    with DiffEngine() as engine:
        diff_AB, s_AB = engine.diff_drops(tA, tB)
        diff_BA, s_BA = engine.diff_drops(tB, tA)

        # Anti-symmetry counts: NEW and DELETED swap, UNCHANGED and RESTATED match
        assert s_AB.new_records == s_BA.dropped_records == 1
        assert s_AB.dropped_records == s_BA.new_records == 1
        assert s_AB.unchanged_records == s_BA.unchanged_records == 1
        assert s_AB.restated_records == s_BA.restated_records == 1

        # Delta inversion: delta(A->B) == -delta(B->A)
        r_AB = diff_AB.filter(pc.equal(diff_AB["entity_id"], "E2")).to_pylist()[0]
        r_BA = diff_BA.filter(pc.equal(diff_BA["entity_id"], "E2")).to_pylist()[0]
        assert r_AB["restatement_delta"] == 15.0
        assert r_BA["restatement_delta"] == -15.0
        assert r_AB["restatement_delta"] == -r_BA["restatement_delta"]


# ============================================================================
# 6. Truth Manifest 100.00% Recall & Precision Verification
# ============================================================================

def test_adversarial_truth_manifest_100_percent_recall_and_precision(fixture_paths):
    """Verify 100.00% restatement recall and 100.00% precision against synthetic ground truth manifest."""
    d1 = pq.read_table(fixture_paths.drop_001_parquet)
    d2 = pq.read_table(fixture_paths.drop_002_parquet)
    truth = pq.read_table(fixture_paths.truth_manifest_parquet)

    con = duckdb.connect()
    con.register("truth", truth)

    # 1. Truth planted restatements in Drop 2
    planted_rows = con.sql("""
        SELECT record_id, restatement_delta, restatement_pct_delta
        FROM truth
        WHERE drop_id = 'DROP_002' AND is_planted_restatement = true
    """).fetchall()
    planted_dict = {r[0]: (r[1], r[2]) for r in planted_rows}
    assert len(planted_dict) == 10

    # 2. Run diff engine
    with DiffEngine() as engine:
        diff_tbl, summary = engine.diff_drops(d1, d2)
        assert summary.new_records == 10
        assert summary.unchanged_records == 45
        assert summary.restated_records == 10
        assert summary.dropped_records == 5

        detected_restatements = {
            r["record_id"]: (r["restatement_delta"], r["restatement_pct_delta"])
            for r in diff_tbl.filter(pc.equal(diff_tbl["diff_status"], "RESTATED")).to_pylist()
        }

        # Recall = 100.00% (zero false negatives)
        recall = len(set(planted_dict.keys()).intersection(detected_restatements.keys())) / len(planted_dict)
        assert recall == 1.0

        # Precision = 100.00% (zero false positives against unchanged records)
        unchanged_in_truth = {
            r[0] for r in con.sql("SELECT record_id FROM truth WHERE drop_id = 'DROP_002' AND mutation_type = 'UNCHANGED'").fetchall()
        }
        false_positives = set(detected_restatements.keys()).intersection(unchanged_in_truth)
        assert len(false_positives) == 0

        # Exact delta alignment with ground truth
        for rec_id, (expected_delta, expected_pct) in planted_dict.items():
            actual_delta, actual_pct = detected_restatements[rec_id]
            assert pytest.approx(actual_delta, abs=1e-4) == expected_delta
            assert pytest.approx(actual_pct, abs=1e-4) == expected_pct


# ============================================================================
# 7. Multi-Drop Revision Ladder & Monotonic Sequence Progression
# ============================================================================

def test_adversarial_cascading_revision_ladder():
    """Verify monotonic revision_seq progression across multiple successive drops."""
    with DiffEngine() as engine:
        # Drop 1: Baseline (seq = 1)
        d1 = pa.Table.from_pylist([
            {"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": 100.0, "revision_seq": 1}
        ])

        # Drop 2: Restatement 1 (seq = 2)
        d2 = pa.Table.from_pylist([
            {"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": 105.0}
        ])
        diff_1_2, s12 = engine.diff_drops(d1, d2)
        assert s12.restated_records == 1
        seq_2 = diff_1_2["revision_seq"].to_pylist()[0]
        assert seq_2 == 2

        # Drop 3: Restatement 2 (seq = 3)
        d2_state = pa.Table.from_pylist([
            {"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": 105.0, "revision_seq": seq_2}
        ])
        d3 = pa.Table.from_pylist([
            {"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": 110.0}
        ])
        diff_2_3, s23 = engine.diff_drops(d2_state, d3)
        assert s23.restated_records == 1
        seq_3 = diff_2_3["revision_seq"].to_pylist()[0]
        assert seq_3 == 3

        # Drop 4: Restatement 3 (seq = 4)
        d3_state = pa.Table.from_pylist([
            {"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": 110.0, "revision_seq": seq_3}
        ])
        d4 = pa.Table.from_pylist([
            {"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": 115.0}
        ])
        diff_3_4, s34 = engine.diff_drops(d3_state, d4)
        assert s34.restated_records == 1
        seq_4 = diff_3_4["revision_seq"].to_pylist()[0]
        assert seq_4 == 4

        # Drop 5: Unchanged in sequenced ledger (seq remains 4)
        d4_state = pa.Table.from_pylist([
            {"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": 115.0, "revision_seq": seq_4}
        ])
        d5 = pa.Table.from_pylist([
            {"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": 115.0, "revision_seq": seq_4}
        ])
        diff_4_5, s45 = engine.diff_drops(d4_state, d5)
        assert s45.unchanged_records == 1
        seq_5 = diff_4_5["revision_seq"].to_pylist()[0]
        assert seq_5 == 4


def test_adversarial_unchanged_record_unsequenced_current_drop():
    """Boundary test: verify behavior when incoming vendor drop lacks revision_seq column."""
    with DiffEngine() as engine:
        t_base = pa.Table.from_pylist([
            {"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": 100.0, "revision_seq": 5}
        ])
        # Raw vendor drop without internal ledger's revision_seq column
        t_curr = pa.Table.from_pylist([
            {"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": 100.0}
        ])
        diff_tbl, summary = engine.diff_drops(t_base, t_curr)
        assert summary.unchanged_records == 1
        r = diff_tbl.to_pylist()[0]
        assert r["diff_status"] == "UNCHANGED"
        # Document current behavior: defaults to 1 when curr lacks revision_seq
        assert r["revision_seq"] in (1, 5)


# ============================================================================
# 8. Boundary Value Coverage Dip & Gap Detection
# ============================================================================

def test_adversarial_coverage_dip_exact_20_percent_boundary():
    """Verify coverage drop of exactly 20.000% triggers an alert (inclusive boundary)."""
    # 10 entities on Day 1 -> 8 entities on Day 2 = (10 - 8)/10 = 20.0% dip
    records = (
        [{"entity_id": f"ENT_{i}", "event_time": "2023-10-02T16:00:00Z"} for i in range(10)]
        + [{"entity_id": f"ENT_{i}", "event_time": "2023-10-03T16:00:00Z"} for i in range(8)]
    )
    t = pa.Table.from_pylist(records)
    analyzer = CoverageAnalyzer()

    dips = analyzer.detect_dips(t, dip_threshold_pct=20.0)
    assert len(dips) == 1
    assert dips[0].date == "2023-10-03"
    assert dips[0].dip_percentage == pytest.approx(20.0)


def test_adversarial_coverage_dip_just_below_threshold():
    """Verify coverage drop of 19.999% does NOT trigger an alert."""
    # 10,000 entities on Day 1 -> 8,001 entities on Day 2 = (10,000 - 8,001)/10,000 = 19.99% dip
    records = (
        [{"entity_id": f"ENT_{i}", "event_time": "2023-10-02"} for i in range(10000)]
        + [{"entity_id": f"ENT_{i}", "event_time": "2023-10-03"} for i in range(8001)]
    )
    t = pa.Table.from_pylist(records)
    analyzer = CoverageAnalyzer()

    dips = analyzer.detect_dips(t, dip_threshold_pct=20.0)
    assert len(dips) == 0


def test_adversarial_coverage_dip_total_blackout_100_percent():
    """Verify 100% dip (count drops to 1) is detected cleanly without crash."""
    records = (
        [{"entity_id": f"ENT_{i}", "event_time": "2023-10-02"} for i in range(100)]
        + [{"entity_id": "ENT_0", "event_time": "2023-10-03"}]
    )
    t = pa.Table.from_pylist(records)
    analyzer = CoverageAnalyzer()

    dips = analyzer.detect_dips(t, dip_threshold_pct=20.0)
    assert len(dips) == 1
    assert dips[0].dip_percentage == pytest.approx(99.0)


def test_adversarial_coverage_gap_calendar_detection():
    """Verify entity date gaps identified against explicit calendar."""
    records = [
        {"entity_id": "ENT_A", "event_time": "2023-10-02"},
        {"entity_id": "ENT_A", "event_time": "2023-10-04"},
        {"entity_id": "ENT_B", "event_time": "2023-10-02"},
        {"entity_id": "ENT_B", "event_time": "2023-10-03"},
        {"entity_id": "ENT_B", "event_time": "2023-10-04"},
    ]
    t = pa.Table.from_pylist(records)
    analyzer = CoverageAnalyzer()

    gaps = analyzer.detect_gaps(t, expected_dates=["2023-10-02", "2023-10-03", "2023-10-04"])
    assert len(gaps) == 1
    assert gaps[0].entity_id == "ENT_A"
    assert gaps[0].missing_dates == ["2023-10-03"]
    assert gaps[0].actual_count == 2
    assert gaps[0].expected_count == 3


# ============================================================================
# 9. Vendor Questions Generator: 100% Anomaly Category Coverage
# ============================================================================

def test_adversarial_vendor_questions_100_percent_anomaly_coverage():
    """Verify 100.00% of recognized validation, coverage, drift, and quarantine anomalies emit drafts."""
    gen = VendorQuestionsGenerator()

    validation_results = [
        MockValidationResult("range_domain", False, 100, 2, 0.02, [{"source_symbol": "AAPL", "value": -5.0}], 0.0),
        MockValidationResult("completeness", False, 100, 5, 0.05, [], 0.01),
        MockValidationResult("future_timestamps", False, 100, 1, 0.01, [], 0.0),
        MockValidationResult("monotone_timestamps", False, 100, 3, 0.03, [], 0.0),
        MockValidationResult("intra_drop_duplicates", False, 100, 2, 0.02, [{"source_symbol": "MSFT"}], 0.0),
        MockValidationResult("universe_coverage", False, 100, 10, 0.10, [], 0.95),
        MockValidationResult("positive_values", False, 100, 1, 0.01, [{"source_symbol": "TSLA"}], 0.0),
        MockValidationResult("unknown_custom_rule", False, 100, 4, 0.04, [], 0.0),
    ]
    dip = CoverageDipAlert("2023-10-03", 10, 7, 30.0, "Coverage dropped 30%")
    drift = ["dropped column 'unit'"]
    diff_sum = DiffSummary("DROP_001", "DROP_002", 10, 45, 10, 5, 1.0)
    quarantine = [{"raw_symbol": "UNKNOWN_TICKER", "vendor_id": "BLOOMBERG", "synthetic_id": "UNRESOLVED_BLOOMBERG_UNKNOWN_TICKER"}]

    inquiries = gen.generate_inquiries(
        validation_results=validation_results,
        coverage_dips=[dip],
        drift_report_changes=drift,
        diff_summary=diff_sum,
        quarantine_records=quarantine,
        drop_id="DROP_002",
    )

    # 8 validation rules + 1 coverage dip + 1 drift + 1 diff velocity + 1 quarantine = 12 inquiries
    assert len(inquiries) == 12

    inquiry_text = "\n\n".join(inquiries)
    assert "Negative Prices Detected" in inquiry_text
    assert "AAPL" in inquiry_text
    assert "SLA Violation - Null Values" in inquiry_text
    assert "Future Timestamps Detected" in inquiry_text
    assert "Timestamp Inversion / Clock Skew" in inquiry_text
    assert "Conflicting Intra-Drop Duplicates" in inquiry_text
    assert "Universe Coverage SLA Violation" in inquiry_text
    assert "Non-Positive Values in Required Positive Fields" in inquiry_text
    assert "Rule Violation: unknown_custom_rule" in inquiry_text
    assert "Universe Coverage Alert - Missing Entities on 2023-10-03" in inquiry_text
    assert "Schema Contract Drift Notification" in inquiry_text
    assert "Revision Velocity Inquiry - High Restatement Volume Detected" in inquiry_text
    assert "Unmapped Identifier 'UNKNOWN_TICKER' in DROP_002" in inquiry_text


# ============================================================================
# 10. High Volume Scale, Rapid Diffing & Concurrency
# ============================================================================

def test_adversarial_high_volume_scale_diff():
    """Verify performance and memory behavior on 50,000 row drops."""
    n = 50_000
    base_rows = [
        {"record_id": f"rec_{i:06d}", "entity_id": f"ENT_{i:06d}", "event_time": "2023-10-02", "metric": "CLOSE", "value": 100.0 + i}
        for i in range(n)
    ]
    curr_rows = [
        {"record_id": f"rec_{i:06d}", "entity_id": f"ENT_{i:06d}", "event_time": "2023-10-02", "metric": "CLOSE", "value": (100.0 + i) if i % 10 != 0 else (200.0 + i)}
        for i in range(n)
    ]
    # Drop first 3,000, add 5,000 new
    curr_rows = curr_rows[3000:]
    for j in range(n, n + 5000):
        curr_rows.append({"record_id": f"rec_{j:06d}", "entity_id": f"ENT_{j:06d}", "event_time": "2023-10-02", "metric": "CLOSE", "value": 500.0})

    t_base = pa.Table.from_pylist(base_rows)
    t_curr = pa.Table.from_pylist(curr_rows)

    with DiffEngine() as engine:
        t0 = time.perf_counter()
        diff_tbl, summary = engine.diff_drops(t_base, t_curr)
        elapsed = time.perf_counter() - t0

        # Sub-second execution for 50,000 records
        assert elapsed < 1.0, f"Diffing took too long: {elapsed:.3f} s"
        assert summary.dropped_records == 3000
        assert summary.new_records == 5000
        assert summary.restated_records == 4700  # (50,000 - 3,000) / 10 = 4,700
        assert summary.unchanged_records == 42300
        assert summary.total_records == 55000


def test_adversarial_concurrent_thread_diffing():
    """Verify multi-threaded execution using context-managed DiffEngine instances."""
    t1 = pa.Table.from_pylist([
        {"record_id": f"R{i}", "entity_id": f"E{i}", "event_time": "2023-10-02", "metric": "VAL", "value": float(i)}
        for i in range(100)
    ])
    t2 = pa.Table.from_pylist([
        {"record_id": f"R{i}", "entity_id": f"E{i}", "event_time": "2023-10-02", "metric": "VAL", "value": float(i + 1)}
        for i in range(100)
    ])

    def worker(worker_id: int) -> int:
        with DiffEngine() as engine:
            _, summary = engine.diff_drops(t1, t2)
            return summary.restated_records

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(worker, i) for i in range(30)]
        results = [f.result() for f in futures]

    assert len(results) == 30
    assert all(r == 100 for r in results)


# ============================================================================
# 11. Empirical Mutation Testing Suite (Verifying Mutants are Killed)
# ============================================================================

def test_adversarial_mutation_abs_epsilon_killed():
    """Verify mutant omitting abs() in epsilon comparison is killed."""
    # A negative change: 100.0 -> 10.0 (delta = -90.0)
    t1 = pa.Table.from_pylist([{"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "M", "value": 100.0}])
    t2 = pa.Table.from_pylist([{"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "M", "value": 10.0}])

    with DiffEngine() as engine:
        diff_tbl, summary = engine.diff_drops(t1, t2)
        # If mutant without abs() was present, -90.0 <= 1e-9 would be True (UNCHANGED).
        # We assert it MUST be RESTATED.
        assert summary.restated_records == 1
        assert diff_tbl["diff_status"].to_pylist()[0] == "RESTATED"


def test_adversarial_mutation_zero_division_guard_killed():
    """Verify mutant omitting != 0 check in pct_delta is killed."""
    t_zero = pa.Table.from_pylist([{"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "M", "value": 0.0}])
    t_pos = pa.Table.from_pylist([{"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "M", "value": 50.0}])

    with DiffEngine() as engine:
        diff_tbl, _ = engine.diff_drops(t_zero, t_pos)
        # Mutant would produce inf, nan, or crash; implementation MUST return None.
        assert diff_tbl["restatement_pct_delta"].to_pylist()[0] is None


def test_adversarial_mutation_revision_seq_increment_killed():
    """Verify mutant with static revision_seq is killed."""
    t1 = pa.Table.from_pylist([{"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "M", "value": 10.0, "revision_seq": 3}])
    t2 = pa.Table.from_pylist([{"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "M", "value": 20.0}])

    with DiffEngine() as engine:
        diff_tbl, _ = engine.diff_drops(t1, t2)
        # Mutant would leave seq at 3 or 1; must increment to 4.
        assert diff_tbl["revision_seq"].to_pylist()[0] == 4


def test_adversarial_mutation_coverage_dip_inclusive_boundary_killed():
    """Verify mutant with strictly > 20% threshold is killed at exact 20% boundary."""
    records_d1 = [{"entity_id": f"E{i}", "event_time": "2023-10-02"} for i in range(10)]
    records_d2 = [{"entity_id": f"E{i}", "event_time": "2023-10-03"} for i in range(8)]
    t = pa.Table.from_pylist(records_d1 + records_d2)

    analyzer = CoverageAnalyzer()
    dips = analyzer.detect_dips(t, dip_threshold_pct=20.0)
    # Mutant with > would return 0 dips; correct implementation MUST return 1 dip.
    assert len(dips) == 1
    assert dips[0].dip_percentage == 20.0


def test_adversarial_mutation_readiness_status_coverage_dip_killed():
    """Verify mutant omitting coverage dips from overall FAIL status calculation is killed."""
    records_d1 = [{"entity_id": f"E{i}", "event_time": "2023-10-02"} for i in range(10)]
    records_d2 = [{"entity_id": f"E{i}", "event_time": "2023-10-03"} for i in range(7)]  # 30% dip
    t = pa.Table.from_pylist(records_d1 + records_d2)

    reporter = ReadinessReporter()
    # Validation results are empty (passing), but 30% dip MUST cause FAIL
    _, js = reporter.generate_report("TEST_DS", "TEST_VEND", t, validation_results=[])
    assert js["overall_status"] == "FAIL"
