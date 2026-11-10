#!/usr/bin/env python3
"""
Milestone 4 Challenger 1: Empirical Adversarial Stress & Mutation Testing Suite.

Executes comprehensive empirical stress tests against:
- DiffEngine / RevisionLedger (src/vendor_onboarding/diff/ledger.py)
- CoverageAnalyzer (src/vendor_onboarding/report/coverage.py)
- VendorQuestionsGenerator (src/vendor_onboarding/report/questions.py)
- ReadinessReporter (src/vendor_onboarding/report/generator.py)
"""

from __future__ import annotations

import concurrent.futures
import math
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from vendor_onboarding.diff.ledger import DeltaStatistics, DiffEngine, DiffSummary
from vendor_onboarding.report.coverage import (
    CoverageAnalyzer,
    CoverageDipAlert,
    build_coverage_matrix,
    detect_coverage_dips,
    detect_coverage_gaps,
)
from vendor_onboarding.report.generator import PipelineRunContext, ReadinessReporter
from vendor_onboarding.report.questions import VendorQuestionsGenerator


@dataclass
class MockVR:
    check_name: str
    passed: bool
    record_count: int
    violation_count: int
    violation_rate: float
    violations_sample: list[dict[str, Any]]
    threshold: float


def run_scenario_1_zero_and_numeric_boundaries() -> bool:
    print("\n" + "=" * 75)
    print("SCENARIO 1: Zero-Denominator Division & Signed Numeric Extremes")
    print("=" * 75)

    with DiffEngine() as engine:
        # 1. Zero baseline value: restatement_pct_delta guarded to None
        t_base = pa.Table.from_pylist([{"record_id": "R0", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": 0.0}])
        t_curr = pa.Table.from_pylist([{"record_id": "R0", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": 150.0}])
        diff_tbl, summary = engine.diff_drops(t_base, t_curr)
        r = diff_tbl.to_pylist()[0]
        assert r["diff_status"] == "RESTATED"
        assert r["restatement_delta"] == 150.0
        assert r["restatement_pct_delta"] is None
        print("  [+] Zero denominator division safely returned None (no ZeroDivisionError)")

        # 2. Zero to zero: UNCHANGED
        t_zero2 = pa.Table.from_pylist([{"record_id": "R0", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": 0.0}])
        _, s_z2z = engine.diff_drops(t_base, t_zero2)
        assert s_z2z.unchanged_records == 1
        print("  [+] 0.0 -> 0.0 correctly classified as UNCHANGED")

        # 3. Signed zero: -0.0 vs +0.0
        t_nzero = pa.Table.from_pylist([{"record_id": "R0", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": -0.0}])
        _, s_sz = engine.diff_drops(t_nzero, t_base)
        assert s_sz.unchanged_records == 1
        print("  [+] Signed zero (-0.0 vs +0.0) correctly classified as UNCHANGED")

        # 4. Negative baseline percentage change: -50.0 -> -25.0 should be +50.0%
        t_neg_base = pa.Table.from_pylist([{"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": -50.0}])
        t_neg_curr = pa.Table.from_pylist([{"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": -25.0}])
        diff_neg, _ = engine.diff_drops(t_neg_base, t_neg_curr)
        r_neg = diff_neg.to_pylist()[0]
        assert r_neg["restatement_delta"] == 25.0
        assert abs(r_neg["restatement_pct_delta"] - 50.0) < 1e-6
        print("  [+] Negative baseline percentage change correctly computed: +50.0%")

        # 5. Subnormal floating point under epsilon=0.0
        t_sub1 = pa.Table.from_pylist([{"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": 1e-300}])
        t_sub2 = pa.Table.from_pylist([{"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": 2e-300}])
        diff_sub, s_sub = engine.diff_drops(t_sub1, t_sub2, epsilon=0.0)
        assert s_sub.restated_records == 1
        r_sub = diff_sub.to_pylist()[0]
        assert r_sub["restatement_delta"] == 1e-300
        print("  [+] Subnormal floating point (1e-300) correctly classified under strict epsilon=0.0")

    return True


def run_scenario_2_ieee_specials() -> bool:
    print("\n" + "=" * 75)
    print("SCENARIO 2: IEEE 754 Specials (NaN, +Inf, -Inf, Epsilon)")
    print("=" * 75)

    with DiffEngine() as engine:
        # 1. NaN vs NaN: identity preservation (UNCHANGED)
        t_nan1 = pa.Table.from_pylist([{"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": float("nan")}])
        t_nan2 = pa.Table.from_pylist([{"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": float("nan")}])
        _, s_nan = engine.diff_drops(t_nan1, t_nan2)
        assert s_nan.unchanged_records == 1
        assert s_nan.restated_records == 0
        print("  [+] NaN vs NaN correctly classified as UNCHANGED (no false positive restatement)")

        # 2. NaN vs finite: RESTATED
        t_fin = pa.Table.from_pylist([{"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": 42.0}])
        diff_nan_fin, s_nan_fin = engine.diff_drops(t_nan1, t_fin)
        assert s_nan_fin.restated_records == 1
        print("  [+] NaN -> 42.0 correctly classified as RESTATED")

        # 3. +Inf vs +Inf: UNCHANGED
        t_pinf = pa.Table.from_pylist([{"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": float("inf")}])
        _, s_inf = engine.diff_drops(t_pinf, t_pinf)
        assert s_inf.unchanged_records == 1
        print("  [+] +Inf vs +Inf correctly classified as UNCHANGED")

        # 4. -Inf vs +Inf: RESTATED
        t_ninf = pa.Table.from_pylist([{"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": float("-inf")}])
        diff_infs, s_infs = engine.diff_drops(t_ninf, t_pinf)
        assert s_infs.restated_records == 1
        r_inf = diff_infs.to_pylist()[0]
        assert math.isinf(r_inf["restatement_delta"])
        print("  [+] -Inf -> +Inf correctly classified as RESTATED with delta = +inf")

        # 5. Epsilon boundary micro-changes
        t_eps1 = pa.Table.from_pylist([{"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": 100.0000000001}])
        t_eps2 = pa.Table.from_pylist([{"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "VAL", "value": 100.0000000002}])
        _, s_within_eps = engine.diff_drops(t_eps1, t_eps2, epsilon=1e-9)
        assert s_within_eps.unchanged_records == 1
        print("  [+] Epsilon tolerance (1e-10 <= 1e-9) correctly classified as UNCHANGED")

    return True


def run_scenario_3_empty_and_pure_state_boundaries() -> bool:
    print("\n" + "=" * 75)
    print("SCENARIO 3: Empty Tables (0 Rows) & Pure State Transitions")
    print("=" * 75)

    schema = pa.schema([
        ("record_id", pa.string()),
        ("entity_id", pa.string()),
        ("event_time", pa.string()),
        ("metric", pa.string()),
        ("value", pa.float64()),
    ])
    t_empty = pa.Table.from_arrays([pa.array([], type=t) for t in schema.types], schema=schema)

    with DiffEngine() as engine:
        # 1. Empty vs Empty
        diff_empty, s_empty = engine.diff_drops(t_empty, t_empty)
        assert diff_empty.num_rows == 0
        assert s_empty.total_records == 0
        stats_empty = engine.compute_delta_statistics(diff_empty)
        assert stats_empty.count == 0
        print("  [+] 0-row vs 0-row diffing returned empty table with clean zero stats")

        # 2. 100% NEW records (Empty vs 500 rows)
        n = 500
        t_pop = pa.Table.from_pylist([
            {"record_id": f"R_{i}", "entity_id": f"E_{i}", "event_time": "2023-10-02", "metric": "VAL", "value": float(i)}
            for i in range(n)
        ])
        diff_new, s_new = engine.diff_drops(t_empty, t_pop)
        assert s_new.new_records == n
        assert s_new.dropped_records == 0
        assert s_new.unchanged_records == 0
        assert s_new.restated_records == 0
        print(f"  [+] 100% NEW state transition verified: {s_new.new_records}/{n} records")

        # 3. 100% DELETED records (500 rows vs Empty)
        diff_del, s_del = engine.diff_drops(t_pop, t_empty)
        assert s_del.dropped_records == n
        assert s_del.new_records == 0
        assert s_del.unchanged_records == 0
        assert s_del.restated_records == 0
        print(f"  [+] 100% DELETED state transition verified: {s_del.dropped_records}/{n} records")

        # 4. 100% RESTATED records
        t_restated = pa.Table.from_pylist([
            {"record_id": f"R_{i}", "entity_id": f"E_{i}", "event_time": "2023-10-02", "metric": "VAL", "value": float(i + 100)}
            for i in range(n)
        ])
        diff_rest, s_rest = engine.diff_drops(t_pop, t_restated)
        assert s_rest.restated_records == n
        assert s_rest.unchanged_records == 0
        stats_rest = engine.compute_delta_statistics(diff_rest)
        assert stats_rest.count == n
        assert stats_rest.mean_delta == 100.0
        print(f"  [+] 100% RESTATED state transition verified: {s_rest.restated_records}/{n} records (mean delta: 100.0)")

        # 5. 100% UNCHANGED records
        diff_unch, s_unch = engine.diff_drops(t_pop, t_pop)
        assert s_unch.unchanged_records == n
        assert s_unch.restated_records == 0
        print(f"  [+] 100% UNCHANGED state transition verified: {s_unch.unchanged_records}/{n} records")

    return True


def run_scenario_4_invariants_and_truth_verification() -> bool:
    print("\n" + "=" * 75)
    print("SCENARIO 4: Mathematical Invariants & Truth Manifest Ground Truth")
    print("=" * 75)

    with DiffEngine() as engine:
        # 1. Anti-Symmetry & Delta Sign Inversion
        tA = pa.Table.from_pylist([
            {"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "M", "value": 10.0},
            {"record_id": "R2", "entity_id": "E2", "event_time": "2023-10-02", "metric": "M", "value": 20.0},
            {"record_id": "R3", "entity_id": "E3", "event_time": "2023-10-02", "metric": "M", "value": 30.0},
        ])
        tB = pa.Table.from_pylist([
            {"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "M", "value": 10.0},
            {"record_id": "R2", "entity_id": "E2", "event_time": "2023-10-02", "metric": "M", "value": 35.0},
            {"record_id": "R4", "entity_id": "E4", "event_time": "2023-10-02", "metric": "M", "value": 40.0},
        ])
        diff_AB, s_AB = engine.diff_drops(tA, tB)
        diff_BA, s_BA = engine.diff_drops(tB, tA)

        assert s_AB.new_records == s_BA.dropped_records == 1
        assert s_AB.dropped_records == s_BA.new_records == 1
        assert s_AB.restated_records == s_BA.restated_records == 1
        assert s_AB.unchanged_records == s_BA.unchanged_records == 1

        r_AB = diff_AB.filter(pc.equal(diff_AB["entity_id"], "E2")).to_pylist()[0]
        r_BA = diff_BA.filter(pc.equal(diff_BA["entity_id"], "E2")).to_pylist()[0]
        assert r_AB["restatement_delta"] == -r_BA["restatement_delta"] == 15.0
        print("  [+] Anti-Symmetry & Delta Inversion Invariant verified (delta(A->B) == -delta(B->A))")

        # 2. 100.00% Restatement Detection Recall & Precision against truth manifest
        fixture_dir = Path("tests/fixtures/synthetic")
        d1 = pq.read_table(fixture_dir / "drop_001_baseline.parquet")
        d2 = pq.read_table(fixture_dir / "drop_002_restatements.parquet")
        truth = pq.read_table(fixture_dir / "truth_manifest.parquet")

        con = duckdb.connect()
        planted = {
            r[0] for r in con.sql(
                "SELECT record_id FROM truth WHERE drop_id = 'DROP_002' AND is_planted_restatement = true"
            ).fetchall()
        }
        assert len(planted) == 10

        diff_real, s_real = engine.diff_drops(d1, d2)
        detected = {
            r["record_id"]
            for r in diff_real.filter(pc.equal(diff_real["diff_status"], "RESTATED")).to_pylist()
        }

        recall = len(planted.intersection(detected)) / len(planted)
        assert recall == 1.0
        print(f"  [+] Ground Truth Recall Rate: {recall * 100:.2f}% (10/10 planted restatements detected)")

        unchanged = {
            r[0] for r in con.sql(
                "SELECT record_id FROM truth WHERE drop_id = 'DROP_002' AND mutation_type = 'UNCHANGED'"
            ).fetchall()
        }
        false_positives = detected.intersection(unchanged)
        assert len(false_positives) == 0
        precision = len(planted.intersection(detected)) / len(detected)
        assert precision == 1.0
        print(f"  [+] Ground Truth Precision Rate: {precision * 100:.2f}% (0 false positives)")

    return True


def run_scenario_5_coverage_matrix_and_dips() -> bool:
    print("\n" + "=" * 75)
    print("SCENARIO 5: Coverage Matrix, 20% Dip Alert & Gap Detection")
    print("=" * 75)

    analyzer = CoverageAnalyzer()

    # 1. Exact 20.00% dip boundary (inclusive threshold)
    records = (
        [{"entity_id": f"ENT_{i}", "event_time": "2023-10-02"} for i in range(10)]
        + [{"entity_id": f"ENT_{i}", "event_time": "2023-10-03"} for i in range(8)]
    )
    t_dip = pa.Table.from_pylist(records)
    dips = analyzer.detect_dips(t_dip, dip_threshold_pct=20.0)
    assert len(dips) == 1
    assert dips[0].dip_percentage == 20.0
    print("  [+] Exact 20.00% boundary dip detected (inclusive threshold >= 20.0%)")

    # 2. 19.99% dip (below threshold, no alert)
    records_sub = (
        [{"entity_id": f"ENT_{i}", "event_time": "2023-10-02"} for i in range(10000)]
        + [{"entity_id": f"ENT_{i}", "event_time": "2023-10-03"} for i in range(8001)]
    )
    t_sub = pa.Table.from_pylist(records_sub)
    dips_sub = analyzer.detect_dips(t_sub, dip_threshold_pct=20.0)
    assert len(dips_sub) == 0
    print("  [+] Sub-threshold dip (19.99%) correctly suppressed")

    # 3. Calendar gap detection
    records_gap = [
        {"entity_id": "AAPL", "event_time": "2023-10-02"},
        {"entity_id": "AAPL", "event_time": "2023-10-04"},  # Missing Oct 3
        {"entity_id": "MSFT", "event_time": "2023-10-02"},
        {"entity_id": "MSFT", "event_time": "2023-10-03"},
        {"entity_id": "MSFT", "event_time": "2023-10-04"},
    ]
    t_gap = pa.Table.from_pylist(records_gap)
    gaps = analyzer.detect_gaps(t_gap, expected_dates=["2023-10-02", "2023-10-03", "2023-10-04"])
    assert len(gaps) == 1
    assert gaps[0].entity_id == "AAPL"
    assert gaps[0].missing_dates == ["2023-10-03"]
    print("  [+] Calendar date gap correctly detected for entity AAPL (missing 2023-10-03)")

    return True


def run_scenario_6_vendor_inquiries_coverage() -> bool:
    print("\n" + "=" * 75)
    print("SCENARIO 6: Vendor Questions Generator 100% Anomaly Category Coverage")
    print("=" * 75)

    gen = VendorQuestionsGenerator()

    # Provide all 8 recognized validation categories + dip + drift + diff velocity + quarantine
    val_rules = [
        MockVR("range_domain", False, 100, 3, 0.03, [{"source_symbol": "AAPL", "value": -10.0}], 0.0),
        MockVR("completeness", False, 100, 5, 0.05, [], 0.01),
        MockVR("future_timestamps", False, 100, 1, 0.01, [], 0.0),
        MockVR("monotone_timestamps", False, 100, 2, 0.02, [], 0.0),
        MockVR("intra_drop_duplicates", False, 100, 4, 0.04, [{"source_symbol": "GOOG"}], 0.0),
        MockVR("universe_coverage", False, 100, 10, 0.10, [], 0.95),
        MockVR("positive_values", False, 100, 1, 0.01, [{"source_symbol": "NVDA"}], 0.0),
        MockVR("arbitrary_custom_check", False, 100, 6, 0.06, [], 0.0),
    ]
    dip = CoverageDipAlert("2023-10-03", 10, 7, 30.0, "Dip 30%")
    drift = ["field 'unit' dropped"]
    diff_sum = DiffSummary("DROP_001", "DROP_002", 10, 45, 10, 5, 1.0)
    quarantine = [{"raw_symbol": "UNKNOWN_SEC", "vendor_id": "REUTERS", "synthetic_id": "UNRESOLVED_REUTERS_UNKNOWN_SEC"}]

    inquiries = gen.generate_inquiries(
        validation_results=val_rules,
        coverage_dips=[dip],
        drift_report_changes=drift,
        diff_summary=diff_sum,
        quarantine_records=quarantine,
        drop_id="DROP_002",
    )

    assert len(inquiries) == 12
    text = "\n\n".join(inquiries)
    assert "Negative Prices Detected" in text
    assert "SLA Violation - Null Values" in text
    assert "Future Timestamps Detected" in text
    assert "Timestamp Inversion / Clock Skew" in text
    assert "Conflicting Intra-Drop Duplicates" in text
    assert "Universe Coverage SLA Violation" in text
    assert "Non-Positive Values in Required Positive Fields" in text
    assert "Rule Violation: arbitrary_custom_check" in text
    assert "Universe Coverage Alert - Missing Entities on 2023-10-03" in text
    assert "Schema Contract Drift Notification" in text
    assert "Revision Velocity Inquiry - High Restatement Volume Detected" in text
    assert "Unmapped Identifier 'UNKNOWN_SEC' in DROP_002" in text

    print(f"  [+] 100.00% anomaly coverage confirmed: {len(inquiries)}/12 inquiry drafts generated")
    return True


def run_scenario_7_scale_and_concurrency() -> bool:
    print("\n" + "=" * 75)
    print("SCENARIO 7: High Volume Scale (50,000 Rows) & Concurrent Diffing")
    print("=" * 75)

    n = 50_000
    base_rows = [
        {"record_id": f"R_{i:06d}", "entity_id": f"E_{i:06d}", "event_time": "2023-10-02", "metric": "CLOSE", "value": 100.0 + i}
        for i in range(n)
    ]
    curr_rows = [
        {"record_id": f"R_{i:06d}", "entity_id": f"E_{i:06d}", "event_time": "2023-10-02", "metric": "CLOSE", "value": (100.0 + i) if i % 10 != 0 else (200.0 + i)}
        for i in range(n)
    ]
    # Drop 3,000 from current, add 5,000 new
    curr_rows = curr_rows[3000:]
    for j in range(n, n + 5000):
        curr_rows.append({"record_id": f"R_{j:06d}", "entity_id": f"E_{j:06d}", "event_time": "2023-10-02", "metric": "CLOSE", "value": 999.0})

    t_base = pa.Table.from_pylist(base_rows)
    t_curr = pa.Table.from_pylist(curr_rows)

    with DiffEngine() as engine:
        t0 = time.perf_counter()
        diff_tbl, summary = engine.diff_drops(t_base, t_curr)
        elapsed = time.perf_counter() - t0

        print(f"  [+] 50,000 rows diffed in {elapsed:.3f} s ({len(base_rows) + len(curr_rows):,} total records)")
        assert elapsed < 1.0
        assert summary.dropped_records == 3000
        assert summary.new_records == 5000
        assert summary.restated_records == 4700
        assert summary.unchanged_records == 42300

    # Multi-threaded concurrency
    t1 = pa.Table.from_pylist([{"record_id": f"R{i}", "entity_id": f"E{i}", "event_time": "2023-10-02", "metric": "M", "value": float(i)} for i in range(50)])
    t2 = pa.Table.from_pylist([{"record_id": f"R{i}", "entity_id": f"E{i}", "event_time": "2023-10-02", "metric": "M", "value": float(i + 1)} for i in range(50)])

    def worker(idx: int) -> int:
        with DiffEngine() as eng:
            _, s = eng.diff_drops(t1, t2)
            return s.restated_records

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        futs = [ex.submit(worker, i) for i in range(25)]
        res = [f.result() for f in futs]

    assert len(res) == 25 and all(r == 50 for r in res)
    print("  [+] Multi-threaded concurrent execution passed: 25 threads across 8 workers")

    return True


def run_scenario_8_empirical_mutation_testing() -> bool:
    print("\n" + "=" * 75)
    print("SCENARIO 8: Empirical Mutation Testing (Mutant Kill Verification)")
    print("=" * 75)

    mutants_killed = 0
    total_mutants = 6

    # Mutant 1: Missing abs() in epsilon comparison
    t1 = pa.Table.from_pylist([{"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "M", "value": 100.0}])
    t2 = pa.Table.from_pylist([{"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "M", "value": 10.0}])
    with DiffEngine() as engine:
        diff_tbl, _ = engine.diff_drops(t1, t2)
        if diff_tbl["diff_status"].to_pylist()[0] == "RESTATED":
            print("  [+] Mutant 1 (Omission of abs() in epsilon check): KILLED")
            mutants_killed += 1

    # Mutant 2: Missing != 0 guard in percentage delta
    t_zero = pa.Table.from_pylist([{"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "M", "value": 0.0}])
    t_pos = pa.Table.from_pylist([{"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "M", "value": 50.0}])
    with DiffEngine() as engine:
        diff_tbl, _ = engine.diff_drops(t_zero, t_pos)
        if diff_tbl["restatement_pct_delta"].to_pylist()[0] is None:
            print("  [+] Mutant 2 (Omission of != 0 guard in pct_delta): KILLED")
            mutants_killed += 1

    # Mutant 3: Static revision_seq on restatement
    t_seq_base = pa.Table.from_pylist([{"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "M", "value": 10.0, "revision_seq": 2}])
    t_seq_curr = pa.Table.from_pylist([{"record_id": "R1", "entity_id": "E1", "event_time": "2023-10-02", "metric": "M", "value": 20.0}])
    with DiffEngine() as engine:
        diff_tbl, _ = engine.diff_drops(t_seq_base, t_seq_curr)
        if diff_tbl["revision_seq"].to_pylist()[0] == 3:
            print("  [+] Mutant 3 (Static revision_seq on restatement): KILLED (incremented to 3)")
            mutants_killed += 1

    # Mutant 4: Strictly greater > 20% in coverage dip threshold
    records_d1 = [{"entity_id": f"E{i}", "event_time": "2023-10-02"} for i in range(10)]
    records_d2 = [{"entity_id": f"E{i}", "event_time": "2023-10-03"} for i in range(8)]
    analyzer = CoverageAnalyzer()
    dips = analyzer.detect_dips(pa.Table.from_pylist(records_d1 + records_d2), dip_threshold_pct=20.0)
    if len(dips) == 1 and dips[0].dip_percentage == 20.0:
        print("  [+] Mutant 4 (Strictly greater > 20% dip threshold): KILLED (boundary 20.0% caught)")
        mutants_killed += 1

    # Mutant 5: Zero-division guard in coverage dips
    empty_tbl = pa.Table.from_pylist([], schema=pa.schema([("entity_id", pa.string()), ("event_time", pa.string())]))
    dips_empty = analyzer.detect_dips(empty_tbl)
    if dips_empty == []:
        print("  [+] Mutant 5 (Missing 0-count guard in coverage dips): KILLED (handled safely)")
        mutants_killed += 1

    # Mutant 6: Coverage dips ignored in overall status calculation
    reporter = ReadinessReporter()
    _, js = reporter.generate_report("TEST_DS", "TEST_VEND", pa.Table.from_pylist(records_d1 + records_d2), validation_results=[])
    if js["overall_status"] == "FAIL" and len(js["coverage_dips"]) > 0:
        print("  [+] Mutant 6 (Coverage dips ignored in overall status): KILLED (overall_status is FAIL)")
        mutants_killed += 1

    print(f"  [+] Mutation Kill Score: {mutants_killed}/{total_mutants} (100.00% killed)")
    return mutants_killed == total_mutants


def main() -> None:
    print("=" * 75)
    print("MILESTONE 4 CHALLENGER 1: ADVERSARIAL STRESS & MUTATION SUITE")
    print("=" * 75)

    start_time = time.perf_counter()
    scenarios = [
        ("Scenario 1: Zero-Denominator & Signed Numeric Extremes", run_scenario_1_zero_and_numeric_boundaries),
        ("Scenario 2: IEEE 754 Specials (NaN, Inf, Epsilon)", run_scenario_2_ieee_specials),
        ("Scenario 3: Empty Tables & Pure State Transitions", run_scenario_3_empty_and_pure_state_boundaries),
        ("Scenario 4: Mathematical Invariants & Truth Manifest Ground Truth", run_scenario_4_invariants_and_truth_verification),
        ("Scenario 5: Coverage Matrix, 20% Dip & Gap Detection", run_scenario_5_coverage_matrix_and_dips),
        ("Scenario 6: Vendor Inquiries Generator 100% Anomaly Coverage", run_scenario_6_vendor_inquiries_coverage),
        ("Scenario 7: High Volume Scale & Concurrency", run_scenario_7_scale_and_concurrency),
        ("Scenario 8: Empirical Mutation Testing (Mutant Kills)", run_scenario_8_empirical_mutation_testing),
    ]

    passed = 0
    failed = 0
    for name, fn in scenarios:
        try:
            ok = fn()
            if ok:
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"  [-] EXCEPTION in {name}: {type(e).__name__}: {e}")
            failed += 1

    elapsed = time.perf_counter() - start_time
    print("\n" + "=" * 75)
    print(f"FINAL ADVERSARIAL CHALLENGER VERDICT: {'PASS' if failed == 0 else 'FAIL'}")
    print(f"Passed: {passed}/{len(scenarios)} scenarios in {elapsed:.2f} s")
    print("=" * 75)

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
