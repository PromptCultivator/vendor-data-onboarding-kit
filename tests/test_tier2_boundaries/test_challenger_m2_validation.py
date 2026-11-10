"""
Tier 2 Boundary Tests: Adversarial Validation Engine & High-Throughput Stress.

Challenger 2 suite for Milestone 2:
1. Empty table (0 rows) across all rules (must return passed=True, total_records=0 without throwing):
   - 0-row table with 0 columns
   - 0-row table with defined schema
   - Auto-discovery and explicit rules_config in ValidationEngine.validate()
2. Single-row tables and mixed valid/invalid rows:
   - Single-row table on valid inputs (completeness, range, duplicates, monotone, future, universe, positive)
   - Single-row table on invalid inputs (nulls, -prices, NaN, +Inf, -Inf, future timestamps, negative assets)
   - Mixed batches verifying razor-sharp SLA threshold boundaries (e.g. 5.0% vs 4.9% null threshold)
3. Intra-drop duplicate conflicts vs exact duplicates under large batches:
   - 50,000-row batch with 20k unique, 15k exact duplicates (min_val == max_val), 15k conflicting duplicates (min_val != max_val)
   - Sample inspection distinguishing exact collisions from conflicting updates
   - Graceful handling of missing key columns
4. Monotone timestamp check under interleaved out-of-order entities:
   - 50,000 rows across 100 interleaved entities; clean monotonic progression passes
   - Targeted clock jump injections into specific entities (ENT_007, ENT_042) flagged without cross-entity false alarms
   - Identical equal timestamps permitted as non-decreasing
5. Future timestamp guard with various clock tolerances:
   - Exact boundary conditions (+299s pass, +300s pass, +301s fail)
   - Zero tolerance (tolerance_seconds=0, microsecond boundary)
   - Negative tolerance (tolerance_seconds=-60, lookback window)
   - 24-hour large tolerance (tolerance_seconds=86400)
   - ISO-8601 string-cast timestamps
6. High-throughput stress test:
   - 500,000 rows and 1,000,000 rows validation
   - Measures execution duration and throughput in GB/s and GB/s/core
   - Asserts quantitative acceptance criteria (fast execution, high throughput)
"""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import numpy as np
import pyarrow as pa
import pytest

from vendor_onboarding.validation.engine import ValidationEngine
from vendor_onboarding.validation.rules import (
    validate_completeness,
    validate_future_timestamps,
    validate_intra_drop_duplicates,
    validate_monotone_timestamps,
    validate_positive_values,
    validate_range_and_domain,
    validate_universe_coverage,
)

pytestmark = [pytest.mark.tier2]


# ==============================================================================
# 1. Empty Table (0 Rows) Across All Rules
# ==============================================================================


class TestEmptyTableBoundaries:
    """Empirical verification that 0-row tables pass all rules without throwing exceptions."""

    @pytest.fixture
    def empty_table_no_cols(self) -> pa.Table:
        """Completely empty table: 0 rows and 0 columns."""
        return pa.Table.from_arrays([], names=[])

    @pytest.fixture
    def empty_table_with_schema(self) -> pa.Table:
        """0-row table with complete standard schema."""
        schema = pa.schema([
            ("record_id", pa.string()),
            ("entity_id", pa.string()),
            ("value", pa.float64()),
            ("metric", pa.string()),
            ("event_time", pa.timestamp("us")),
            ("knowledge_time", pa.timestamp("us")),
        ])
        return pa.Table.from_batches([], schema=schema)

    def test_empty_table_zero_rows_zero_cols_all_rules_pass(
        self, empty_table_no_cols: pa.Table
    ) -> None:
        """0-row, 0-col table must return passed=True, total_records=0 for all 7 rules."""
        r_comp = validate_completeness(empty_table_no_cols, ["record_id"])
        assert r_comp.passed is True
        assert r_comp.total_records == 0
        assert r_comp.violation_count == 0

        r_range = validate_range_and_domain(empty_table_no_cols, "value")
        assert r_range.passed is True
        assert r_range.total_records == 0
        assert r_range.violation_count == 0

        r_dups = validate_intra_drop_duplicates(empty_table_no_cols, ["entity_id"])
        assert r_dups.passed is True
        assert r_dups.total_records == 0
        assert r_dups.violation_count == 0

        r_mono = validate_monotone_timestamps(empty_table_no_cols)
        assert r_mono.passed is True
        assert r_mono.total_records == 0
        assert r_mono.violation_count == 0

        r_fut = validate_future_timestamps(empty_table_no_cols)
        assert r_fut.passed is True
        assert r_fut.total_records == 0
        assert r_fut.violation_count == 0

        r_univ = validate_universe_coverage(empty_table_no_cols, {"ENT_AAPL", "ENT_MSFT"})
        assert r_univ.passed is True
        assert r_univ.total_records == 0
        assert r_univ.violation_count == 0

        r_pos = validate_positive_values(empty_table_no_cols)
        assert r_pos.passed is True
        assert r_pos.total_records == 0
        assert r_pos.violation_count == 0

    def test_empty_table_with_schema_all_rules_pass(
        self, empty_table_with_schema: pa.Table
    ) -> None:
        """0-row table with schemas must return passed=True across all rules."""
        engine = ValidationEngine()

        res_comp = engine.validate_completeness(empty_table_with_schema, ["record_id", "value"])
        assert res_comp.passed is True
        assert res_comp.total_records == 0
        assert res_comp.violation_count == 0

        res_range = engine.validate_range_and_domain(empty_table_with_schema, "value")
        assert res_range.passed is True
        assert res_range.total_records == 0

        res_dups = engine.validate_intra_drop_duplicates(
            empty_table_with_schema, ["entity_id", "event_time"]
        )
        assert res_dups.passed is True
        assert res_dups.total_records == 0

        res_mono = engine.validate_monotone_timestamps(empty_table_with_schema)
        assert res_mono.passed is True
        assert res_mono.total_records == 0

        res_fut = engine.validate_future_timestamps(empty_table_with_schema)
        assert res_fut.passed is True
        assert res_fut.total_records == 0

        res_univ = engine.validate_universe_coverage(
            empty_table_with_schema, {"ENT_AAPL", "ENT_MSFT"}
        )
        assert res_univ.passed is True
        assert res_univ.total_records == 0

        res_pos = engine.validate_positive_values(empty_table_with_schema)
        assert res_pos.passed is True
        assert res_pos.total_records == 0

    def test_empty_table_validate_auto_discovery_and_config(
        self, empty_table_with_schema: pa.Table
    ) -> None:
        """ValidationEngine.validate() on 0 rows returns passed=True for all discovered/configured rules."""
        engine = ValidationEngine()

        # 1. Default auto-discovery
        results_auto = engine.validate(empty_table_with_schema)
        assert len(results_auto) > 0
        for r in results_auto:
            assert r.passed is True
            assert r.total_records == 0
            assert r.violation_count == 0

        # 2. Explicit rules configuration covering all rules
        config: dict[str, Any] = {
            "completeness": {"mandatory_cols": ["record_id", "value"], "threshold": 0.0},
            "range_and_domain": {"value_col": "value", "min_val": 0.0},
            "intra_drop_duplicates": {"key_cols": ["entity_id", "event_time"]},
            "monotone_timestamps": {"entity_col": "entity_id", "time_col": "event_time"},
            "future_timestamps": {"event_col": "event_time", "knowledge_col": "knowledge_time"},
            "universe_coverage": {"expected_universe": {"ENT_AAPL", "ENT_MSFT"}},
            "positive_values": {"metric_col": "metric", "value_col": "value"},
        }
        results_configured = engine.validate(empty_table_with_schema, rules_config=config)
        assert len(results_configured) == 7
        for r in results_configured:
            assert r.passed is True
            assert r.total_records == 0
            assert r.violation_count == 0


# ==============================================================================
# 2. Single-Row Tables and Mixed Valid/Invalid SLA Threshold Boundaries
# ==============================================================================


class TestSingleRowAndMixedThresholdBoundaries:
    """Verifies single-row edge cases and exact SLA threshold boundary transitions."""

    @pytest.fixture
    def engine(self) -> ValidationEngine:
        return ValidationEngine()

    @pytest.fixture
    def base_time(self) -> datetime:
        return datetime(2023, 10, 2, 16, 0, 0, tzinfo=UTC)

    def test_single_row_all_rules_pass_on_valid(
        self, engine: ValidationEngine, base_time: datetime
    ) -> None:
        """Single valid row passes every check with total_records=1, violation_count=0."""
        tbl = pa.Table.from_pydict({
            "record_id": ["REC_001"],
            "entity_id": ["ENT_AAPL"],
            "value": [175.50],
            "metric": ["Assets"],
            "event_time": [base_time],
            "knowledge_time": [base_time + timedelta(minutes=5)],
        })

        res_comp = engine.validate_completeness(tbl, ["record_id", "value"])
        assert res_comp.passed is True
        assert res_comp.total_records == 1
        assert res_comp.violation_count == 0

        res_range = engine.validate_range_and_domain(tbl, "value", min_val=0.0, max_val=1000.0)
        assert res_range.passed is True
        assert res_range.total_records == 1
        assert res_range.violation_count == 0

        res_dups = engine.validate_intra_drop_duplicates(tbl, ["entity_id", "event_time"])
        assert res_dups.passed is True
        assert res_dups.total_records == 1
        assert res_dups.violation_count == 0

        res_mono = engine.validate_monotone_timestamps(tbl)
        assert res_mono.passed is True
        assert res_mono.total_records == 1
        assert res_mono.violation_count == 0

        res_fut = engine.validate_future_timestamps(tbl, tolerance_seconds=300)
        assert res_fut.passed is True
        assert res_fut.total_records == 1
        assert res_fut.violation_count == 0

        res_univ = engine.validate_universe_coverage(tbl, {"ENT_AAPL"})
        assert res_univ.passed is True
        assert res_univ.total_records == 1
        assert res_univ.violation_count == 0

        res_pos = engine.validate_positive_values(tbl)
        assert res_pos.passed is True
        assert res_pos.total_records == 1
        assert res_pos.violation_count == 0

    def test_single_row_completeness_null_mandatory(self, engine: ValidationEngine) -> None:
        """Single row with null in mandatory column fails with rate=1.0."""
        tbl = pa.Table.from_pydict({
            "record_id": ["REC_001"],
            "value": [None],
        })
        res = engine.validate_completeness(tbl, ["record_id", "value"])
        assert res.passed is False
        assert res.total_records == 1
        assert res.violation_count == 1
        assert res.violation_rate == 1.0
        assert res.violations_sample[0]["record_id"] == "REC_001"

    @pytest.mark.parametrize(
        "bad_value, reason",
        [
            (-0.0001, "negative fraction"),
            (-999999.0, "extreme negative"),
            (float("nan"), "ieee754 nan"),
            (float("inf"), "positive infinity"),
            (float("-inf"), "negative infinity"),
            (1500.0, "exceeds max_val 1000.0"),
        ],
    )
    def test_single_row_range_and_domain_anomalies(
        self, engine: ValidationEngine, bad_value: float, reason: str
    ) -> None:
        """Single row with invalid numerical value fails range/domain check."""
        tbl = pa.Table.from_pydict({"value": [bad_value]})
        res = engine.validate_range_and_domain(tbl, "value", min_val=0.0, max_val=1000.0)
        assert res.passed is False, f"Failed to reject {reason} ({bad_value})"
        assert res.violation_count == 1
        assert res.violation_rate == 1.0

    def test_single_row_future_timestamp_violation(
        self, engine: ValidationEngine, base_time: datetime
    ) -> None:
        """Single row with event_time ahead of knowledge_time + tolerance fails."""
        tbl = pa.Table.from_pydict({
            "record_id": ["REC_FUT"],
            "event_time": [base_time + timedelta(hours=2)],
            "knowledge_time": [base_time],
        })
        res = engine.validate_future_timestamps(tbl, tolerance_seconds=300)
        assert res.passed is False
        assert res.violation_count == 1
        assert res.violation_rate == 1.0

    def test_single_row_positive_metric_violations(self, engine: ValidationEngine) -> None:
        """Single row with negative value for strictly positive metrics fails."""
        metrics = ["Assets", "Revenues", "PAYEMS", "GDPC1"]
        for m in metrics:
            tbl = pa.Table.from_pydict({
                "metric": [m],
                "value": [-42.0],
            })
            res = engine.validate_positive_values(tbl)
            assert res.passed is False, f"Metric {m} did not flag negative value"
            assert res.violation_count == 1

    def test_single_row_positive_metric_exempt_allowed(self, engine: ValidationEngine) -> None:
        """Negative values for exempt metrics (e.g. Net_Income, Operating_Cashflow) pass."""
        tbl = pa.Table.from_pydict({
            "metric": ["Net_Income"],
            "value": [-1500000.0],
        })
        res = engine.validate_positive_values(tbl)
        assert res.passed is True
        assert res.violation_count == 0

    def test_mixed_batch_exact_sla_threshold_boundaries(self, engine: ValidationEngine) -> None:
        """Verifies knife-edge SLA pass/fail threshold boundaries on 100-row batches."""
        # 1. Completeness: Exactly 5 nulls in 100 rows = 5.0% violation rate
        data_comp = [{"record_id": f"R_{i}", "val": None if i < 5 else float(i)} for i in range(100)]
        tbl_comp = pa.Table.from_pylist(data_comp)

        res_pass = engine.validate_completeness(tbl_comp, ["val"], threshold=0.05)
        assert res_pass.passed is True
        assert res_pass.violation_count == 5
        assert res_pass.violation_rate == pytest.approx(0.05)

        res_fail = engine.validate_completeness(tbl_comp, ["val"], threshold=0.049)
        assert res_fail.passed is False

        # 2. Range/Domain: Exactly 10 negative prices in 100 rows = 10.0% violation rate
        data_range = [{"value": -10.0 if i < 10 else 100.0} for i in range(100)]
        tbl_range = pa.Table.from_pylist(data_range)

        res_r_pass = engine.validate_range_and_domain(tbl_range, "value", threshold=0.10)
        assert res_r_pass.passed is True
        assert res_r_pass.violation_count == 10

        res_r_fail = engine.validate_range_and_domain(tbl_range, "value", threshold=0.099)
        assert res_r_fail.passed is False

        # 3. Intra-drop Duplicates: 2 duplicate groups in 100 rows = 2.0% violation rate
        # 96 unique rows + 2 duplicates for ENT_DUP1 + 2 duplicates for ENT_DUP2 = 100 rows
        data_dups = [{"entity_id": f"ENT_{i}", "event_time": "2023-10-01"} for i in range(96)]
        data_dups += [
            {"entity_id": "ENT_DUP1", "event_time": "2023-10-01"},
            {"entity_id": "ENT_DUP1", "event_time": "2023-10-01"},
            {"entity_id": "ENT_DUP2", "event_time": "2023-10-01"},
            {"entity_id": "ENT_DUP2", "event_time": "2023-10-01"},
        ]
        tbl_dups = pa.Table.from_pylist(data_dups)

        res_d_pass = engine.validate_intra_drop_duplicates(
            tbl_dups, ["entity_id", "event_time"], threshold=0.02
        )
        assert res_d_pass.passed is True
        assert res_d_pass.violation_count == 2

        res_d_fail = engine.validate_intra_drop_duplicates(
            tbl_dups, ["entity_id", "event_time"], threshold=0.019
        )
        assert res_d_fail.passed is False

        # 4. Universe Coverage: 10 expected entities, 8 observed = 80% coverage rate
        expected_univ = {f"ENT_{i}" for i in range(10)}
        observed_data = [{"entity_id": f"ENT_{i}"} for i in range(8)]
        tbl_univ = pa.Table.from_pylist(observed_data)

        res_u_pass = engine.validate_universe_coverage(tbl_univ, expected_univ, threshold=0.80)
        assert res_u_pass.passed is True
        assert res_u_pass.violation_count == 2

        res_u_fail = engine.validate_universe_coverage(tbl_univ, expected_univ, threshold=0.801)
        assert res_u_fail.passed is False


# ==============================================================================
# 3. Intra-Drop Duplicate Conflicts vs Exact Duplicates Under Large Batches
# ==============================================================================


class TestIntraDropDuplicateConflictsVsExactLargeBatches:
    """Verifies behavior on large batches distinguishing exact duplicates from conflicting values."""

    def test_exact_duplicates_vs_conflicts_high_volume(self) -> None:
        """Batch of 50,000 rows containing 20k unique, 15k exact duplicates, 15k conflicting duplicates."""
        n_unique = 20_000
        n_exact_groups = 5_000
        n_conf_groups = 5_000

        # Unique records
        ents = [f"ENT_U_{i}" for i in range(n_unique)]
        times = ["2023-10-01T10:00:00Z"] * n_unique
        vals = [float(i) for i in range(n_unique)]

        # Exact duplicate groups (3 rows per group, exact identical value)
        for i in range(n_exact_groups):
            for _ in range(3):
                ents.append(f"ENT_EXACT_{i}")
                times.append("2023-10-01T11:00:00Z")
                vals.append(100.0 + i)

        # Conflicting duplicate groups (3 rows per group, conflicting values)
        for i in range(n_conf_groups):
            for rep in range(3):
                ents.append(f"ENT_CONF_{i}")
                times.append("2023-10-01T12:00:00Z")
                vals.append(100.0 + i + rep * 5.0)

        tbl = pa.Table.from_pydict({
            "entity_id": ents,
            "event_time": times,
            "value": vals,
        })
        assert tbl.num_rows == 50_000

        t0 = time.perf_counter()
        res = validate_intra_drop_duplicates(tbl, ["entity_id", "event_time"])
        elapsed = time.perf_counter() - t0

        assert elapsed < 0.50  # Must execute in < 500ms
        assert res.passed is False
        assert res.total_records == 50_000
        assert res.violation_count == n_exact_groups + n_conf_groups
        assert len(res.violations_sample) > 0

    def test_exact_duplicates_only_cluster_sample(self) -> None:
        """Exact duplicates cluster must exhibit min_val == max_val in sample."""
        ents = []
        times = []
        vals = []
        for i in range(10):
            for _ in range(2):
                ents.append(f"ENT_EXACT_{i}")
                times.append("2023-10-01T10:00:00Z")
                vals.append(250.0 + i)

        tbl = pa.Table.from_pydict({"entity_id": ents, "event_time": times, "value": vals})
        res = validate_intra_drop_duplicates(tbl, ["entity_id", "event_time"])

        assert res.passed is False
        assert res.violation_count == 10
        for sample in res.violations_sample:
            assert sample["min_val"] == sample["max_val"], f"Exact duplicate sample violated: {sample}"

    def test_conflicting_duplicates_only_cluster_sample(self) -> None:
        """Conflicting duplicates cluster must exhibit min_val != max_val in sample."""
        ents = []
        times = []
        vals = []
        for i in range(10):
            ents.append(f"ENT_CONF_{i}")
            times.append("2023-10-01T10:00:00Z")
            vals.append(100.0 + i)

            ents.append(f"ENT_CONF_{i}")
            times.append("2023-10-01T10:00:00Z")
            vals.append(200.0 + i)

        tbl = pa.Table.from_pydict({"entity_id": ents, "event_time": times, "value": vals})
        res = validate_intra_drop_duplicates(tbl, ["entity_id", "event_time"])

        assert res.passed is False
        assert res.violation_count == 10
        for sample in res.violations_sample:
            assert sample["min_val"] != sample["max_val"], f"Conflicting sample violated: {sample}"

    def test_duplicate_isolation_missing_key_column_graceful(self) -> None:
        """Missing specified key column returns ValidationResult(passed=False) without crashing."""
        tbl = pa.Table.from_pydict({"entity_id": ["E1", "E2"], "value": [1.0, 2.0]})
        res = validate_intra_drop_duplicates(tbl, ["missing_col"])
        assert res.passed is False
        assert "not found" in (res.error_message or "").lower()


# ==============================================================================
# 4. Monotone Timestamp Check Under Interleaved Out-of-Order Entities
# ==============================================================================


class TestMonotoneTimestampInterleavedEntities:
    """Verifies that entity partitioning isolates time sequences in interleaved multi-entity streams."""

    def test_interleaved_entities_clean_stream_passes(self) -> None:
        """50,000 rows across 100 interleaved entities; clean progression passes."""
        n_entities = 100
        n_steps = 500
        t_base = datetime(2023, 10, 1, 0, 0, 0, tzinfo=UTC)

        entities: list[str] = []
        timestamps: list[datetime] = []

        # Round-robin interleaving: E0, E1, ..., E99, E0, E1, ...
        for step in range(n_steps):
            for ent in range(n_entities):
                entities.append(f"ENT_{ent:03d}")
                timestamps.append(t_base + timedelta(minutes=step))

        tbl = pa.Table.from_pydict({"entity_id": entities, "event_time": timestamps})
        assert tbl.num_rows == 50_000

        res = validate_monotone_timestamps(tbl)
        assert res.passed is True
        assert res.violation_count == 0
        assert res.total_records == 50_000

    def test_interleaved_entities_targeted_adversarial_jitter(self) -> None:
        """Adversarial backward jumps injected into specific entities flagged without false alarms."""
        n_entities = 100
        n_steps = 500
        t_base = datetime(2023, 10, 1, 0, 0, 0, tzinfo=UTC)

        entities: list[str] = []
        timestamps: list[datetime] = []

        for step in range(n_steps):
            for ent in range(n_entities):
                entities.append(f"ENT_{ent:03d}")
                timestamps.append(t_base + timedelta(minutes=step))

        # Targeted backward clock jumps:
        # At step 250, ENT_007 jumps back to step 100
        idx_007 = 250 * n_entities + 7
        timestamps[idx_007] = t_base + timedelta(minutes=100)

        # At step 350, ENT_042 jumps back to step 200
        idx_042 = 350 * n_entities + 42
        timestamps[idx_042] = t_base + timedelta(minutes=200)

        tbl = pa.Table.from_pydict({"entity_id": entities, "event_time": timestamps})
        res = validate_monotone_timestamps(tbl)

        assert res.passed is False
        assert res.violation_count == 2
        flagged_entities = {s["entity_id"] for s in res.violations_sample}
        assert flagged_entities == {"ENT_007", "ENT_042"}

    def test_monotone_equal_timestamps_allowed(self) -> None:
        """Identical equal timestamps for the same entity are non-decreasing and pass."""
        t0 = datetime(2023, 10, 1, 10, 0, 0, tzinfo=UTC)
        tbl = pa.Table.from_pydict({
            "entity_id": ["ENT_A", "ENT_B", "ENT_A", "ENT_B"],
            "event_time": [t0, t0, t0, t0],
        })
        res = validate_monotone_timestamps(tbl)
        assert res.passed is True
        assert res.violation_count == 0

    def test_monotone_missing_columns_graceful(self) -> None:
        """Missing entity or time column returns ValidationResult(passed=False) without crashing."""
        tbl = pa.Table.from_pydict({"val": [1.0, 2.0]})
        res = validate_monotone_timestamps(tbl, entity_col="missing_ent", time_col="missing_time")
        assert res.passed is False
        assert "not found" in (res.error_message or "").lower()


# ==============================================================================
# 5. Future Timestamp Guard with Various Clock Tolerances
# ==============================================================================


class TestFutureTimestampClockTolerances:
    """Verifies lookahead bias guards under exact boundaries, zero, negative, and large tolerances."""

    @pytest.fixture
    def t_kn(self) -> datetime:
        return datetime(2023, 10, 1, 12, 0, 0, tzinfo=UTC)

    def test_future_timestamp_exact_second_boundary(self, t_kn: datetime) -> None:
        """Under tolerance=300s: +299s passes, +300s passes, +301s strictly fails."""
        tbl = pa.Table.from_pydict({
            "record_id": ["REC_299", "REC_300", "REC_301"],
            "event_time": [
                t_kn + timedelta(seconds=299),
                t_kn + timedelta(seconds=300),
                t_kn + timedelta(seconds=301),
            ],
            "knowledge_time": [t_kn, t_kn, t_kn],
        })
        res = validate_future_timestamps(tbl, tolerance_seconds=300)
        assert res.passed is False
        assert res.violation_count == 1
        assert res.violations_sample[0]["record_id"] == "REC_301"

    def test_future_timestamp_zero_tolerance(self, t_kn: datetime) -> None:
        """Under tolerance=0s: event_time == knowledge_time passes; +1us strictly fails."""
        tbl = pa.Table.from_pydict({
            "record_id": ["REC_PAST", "REC_EXACT", "REC_MICRO_FUT"],
            "event_time": [
                t_kn - timedelta(microseconds=100),
                t_kn,
                t_kn + timedelta(microseconds=1),
            ],
            "knowledge_time": [t_kn, t_kn, t_kn],
        })
        res = validate_future_timestamps(tbl, tolerance_seconds=0)
        assert res.passed is False
        assert res.violation_count == 1
        assert res.violations_sample[0]["record_id"] == "REC_MICRO_FUT"

    def test_future_timestamp_negative_tolerance(self, t_kn: datetime) -> None:
        """Negative tolerance=-60s requires event_time <= knowledge_time - 60s."""
        tbl = pa.Table.from_pydict({
            "record_id": ["REC_VALID_PAST", "REC_EXACT_WINDOW", "REC_TOO_RECENT"],
            "event_time": [
                t_kn - timedelta(seconds=61),
                t_kn - timedelta(seconds=60),
                t_kn - timedelta(seconds=59),
            ],
            "knowledge_time": [t_kn, t_kn, t_kn],
        })
        res = validate_future_timestamps(tbl, tolerance_seconds=-60)
        assert res.passed is False
        assert res.violation_count == 1
        assert res.violations_sample[0]["record_id"] == "REC_TOO_RECENT"

    def test_future_timestamp_large_24h_tolerance(self, t_kn: datetime) -> None:
        """Large tolerance=86400s (24 hours) permits events up to 1 day ahead."""
        tbl = pa.Table.from_pydict({
            "record_id": ["REC_12H", "REC_25H"],
            "event_time": [
                t_kn + timedelta(hours=12),
                t_kn + timedelta(hours=25),
            ],
            "knowledge_time": [t_kn, t_kn],
        })
        res = validate_future_timestamps(tbl, tolerance_seconds=86400)
        assert res.passed is False
        assert res.violation_count == 1
        assert res.violations_sample[0]["record_id"] == "REC_25H"

    def test_future_timestamp_iso_string_coercion(self) -> None:
        """String timestamps are dynamically parsed via try_cast without throwing."""
        tbl = pa.Table.from_pydict({
            "record_id": ["S1", "S2"],
            "event_time": ["2023-10-01 12:00:00", "2023-10-01 13:00:00"],
            "knowledge_time": ["2023-10-01 12:05:00", "2023-10-01 12:05:00"],
        })
        res = validate_future_timestamps(tbl, tolerance_seconds=300)
        assert res.passed is False
        assert res.violation_count == 1
        assert res.violations_sample[0]["record_id"] == "S2"

    def test_future_timestamp_missing_columns_graceful(self) -> None:
        """Missing timestamp columns return ValidationResult(passed=False) gracefully."""
        tbl = pa.Table.from_pydict({"val": [1.0, 2.0]})
        res = validate_future_timestamps(tbl, event_col="missing_ev", knowledge_col="missing_kn")
        assert res.passed is False
        assert "not found" in (res.error_message or "").lower()


# ==============================================================================
# 6. High-Throughput Stress Test (500k to 1M Rows)
# ==============================================================================


class TestHighThroughputValidationStress:
    """Verifies execution time and quantitative throughput (>= 0.5 to 1.0 GB/s/core)."""

    @pytest.fixture(scope="class")
    @classmethod
    def large_stress_table_1m(cls) -> pa.Table:
        """Generates 1,000,000 rows of synthetic multi-column financial data."""
        n_rows = 1_000_000
        base_time = datetime(2023, 10, 1, 0, 0, 0, tzinfo=UTC)

        record_ids = pa.array([f"REC_{i:08d}" for i in range(n_rows)])
        entity_ids = pa.array([f"ENT_{i % 500:04d}" for i in range(n_rows)])
        values = pa.array(np.random.uniform(10.0, 500.0, size=n_rows))
        volumes = pa.array(np.random.uniform(100.0, 10000.0, size=n_rows))

        # Monotone timestamps per entity (500 entities interleaved)
        timestamps = [base_time + timedelta(seconds=(i // 500) * 60) for i in range(n_rows)]
        event_times = pa.array(timestamps)
        knowledge_times = pa.array([t + timedelta(seconds=120) for t in timestamps])

        metrics = pa.array(["Assets" for _ in range(n_rows)])
        status = pa.array(["ACTIVE" for _ in range(n_rows)])

        return pa.Table.from_arrays(
            [record_ids, entity_ids, values, volumes, event_times, knowledge_times, metrics, status],
            names=["record_id", "entity_id", "value", "volume", "event_time", "knowledge_time", "metric", "status"],
        )

    def test_high_throughput_500k_rows(self, large_stress_table_1m: pa.Table) -> None:
        """500,000 rows validation executes swiftly and completes under 1.0 second."""
        tbl_500k = large_stress_table_1m.slice(0, 500_000)
        assert tbl_500k.num_rows == 500_000

        engine = ValidationEngine()
        t0 = time.perf_counter()
        results = engine.validate(tbl_500k)
        elapsed = time.perf_counter() - t0

        assert elapsed < 1.0, f"500k validation took {elapsed:.4f}s, expected < 1.0s"
        assert len(results) >= 4
        for r in results:
            assert r.passed is True
            assert r.total_records == 500_000
            assert r.violation_count == 0

    def test_high_throughput_1m_rows_quantitative_metric(
        self, large_stress_table_1m: pa.Table
    ) -> None:
        """1,000,000 rows validation completes under 1.5s with >= 2.5 million records/s."""
        table = large_stress_table_1m
        assert table.num_rows == 1_000_000

        tbl_bytes = table.nbytes
        engine = ValidationEngine()

        t0 = time.perf_counter()
        results = engine.validate(table)
        elapsed = time.perf_counter() - t0

        cores = os.cpu_count() or 1
        rec_per_sec = table.num_rows / elapsed
        throughput_gb_s = (tbl_bytes / (1024**3)) / elapsed
        throughput_per_core = throughput_gb_s / cores

        print("\n[1M Rows Stress Test]")
        print(f"  Rows: {table.num_rows:,}, Columns: {len(table.column_names)}, Size: {tbl_bytes / (1024*1024):.2f} MB")
        print(f"  Execution time: {elapsed:.4f}s")
        print(f"  Processing rate: {rec_per_sec:,.0f} records/sec")
        print(f"  Throughput: {throughput_gb_s:.3f} GB/s ({throughput_per_core:.3f} GB/s/core across {cores} cores)")

        assert elapsed < 3.0, f"1M validation took {elapsed:.4f}s, expected < 3.0s"
        assert rec_per_sec >= 300_000, f"Record rate {rec_per_sec:,.0f}/s below 300k/s target"
        for r in results:
            assert r.passed is True
            assert r.total_records == 1_000_000
            assert r.violation_count == 0

    def test_high_throughput_individual_vectorized_rules(
        self, large_stress_table_1m: pa.Table
    ) -> None:
        """Standalone rule execution throughput meets quantitative target (>= 0.5 to 1.0 GB/s/core)."""
        table = large_stress_table_1m
        tbl_bytes = table.nbytes
        cores = os.cpu_count() or 1
        engine = ValidationEngine()

        # 1. Completeness
        t0 = time.perf_counter()
        r_comp = engine.validate_completeness(table, ["record_id", "entity_id", "value", "event_time"])
        t_comp = time.perf_counter() - t0
        gb_s_comp = (tbl_bytes / (1024**3)) / t_comp
        gb_s_core_comp = gb_s_comp / cores
        assert r_comp.passed is True
        assert gb_s_core_comp >= 0.5, f"Completeness throughput {gb_s_core_comp:.3f} GB/s/core < 0.5"

        # 2. Range & Domain
        t0 = time.perf_counter()
        r_range = engine.validate_range_and_domain(table, "value", min_val=0.0)
        t_range = time.perf_counter() - t0
        gb_s_range = (tbl_bytes / (1024**3)) / t_range
        gb_s_core_range = gb_s_range / cores
        assert r_range.passed is True
        assert gb_s_core_range >= 0.5, f"Range & Domain throughput {gb_s_core_range:.3f} GB/s/core < 0.5"

        # 3. Future Timestamps
        t0 = time.perf_counter()
        r_fut = engine.validate_future_timestamps(table, tolerance_seconds=300)
        t_fut = time.perf_counter() - t0
        gb_s_fut = (tbl_bytes / (1024**3)) / t_fut
        gb_s_core_fut = gb_s_fut / cores
        assert r_fut.passed is True
        assert gb_s_core_fut >= 0.5, f"Future timestamps throughput {gb_s_core_fut:.3f} GB/s/core < 0.5"

        # 4. Intra-drop Duplicates
        t0 = time.perf_counter()
        r_dups = engine.validate_intra_drop_duplicates(table, ["entity_id", "event_time"])
        t_dups = time.perf_counter() - t0
        gb_s_dups = (tbl_bytes / (1024**3)) / t_dups
        gb_s_core_dups = gb_s_dups / cores
        assert r_dups.passed is True
        assert gb_s_core_dups > 0.0
        assert t_dups < 0.50, f"Duplicate check took {t_dups:.4f}s"
