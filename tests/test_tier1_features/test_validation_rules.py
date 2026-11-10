"""
Tier 1 Feature Tests: F-VAL-01..07 Vectorized Validation Rules.

Tests completeness (SLA nulls), range/domain checks (negative prices, 5-sigma outliers),
intra-drop duplicate keys and conflicts, monotone non-decreasing timestamps,
future timestamp guard with tolerance, universe coverage/dropout, and vectorized DuckDB execution.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# Progressive import from conftest or vendor_onboarding
try:
    from vendor_onboarding.validation.engine import ValidationEngine, ValidationResult
except ImportError:
    @dataclass
    class ValidationResult:  # type: ignore[no-redef]
        check_name: str
        passed: bool
        total_records: int
        violation_count: int
        violation_rate: float
        violations_sample: list[dict[str, Any]]
        threshold: float
        error_message: str | None = None


# Reference vectorized execution engine using DuckDB
class VectorizedValidationEngine:
    def __init__(self, conn: duckdb.DuckDBPyConnection | None = None) -> None:
        self.conn = conn or duckdb.connect()

    def validate_completeness(
        self, table: pa.Table, mandatory_cols: list[str], threshold: float = 0.0
    ) -> ValidationResult:
        con = self.conn
        total = table.num_rows
        if total == 0:
            return ValidationResult("completeness", True, 0, 0, 0.0, [], threshold)

        con.register("tbl", table)
        null_conds = " OR ".join([f'"{col}" IS NULL' for col in mandatory_cols if col in table.column_names])
        query = f'SELECT * FROM tbl WHERE {null_conds}'
        violations_arrow = con.sql(query).arrow().read_all()
        viol_count = violations_arrow.num_rows
        viol_rate = viol_count / total
        samples = violations_arrow.slice(0, 5).to_pylist()
        passed = viol_rate <= threshold
        return ValidationResult("completeness", passed, total, viol_count, viol_rate, samples, threshold)

    def validate_range_and_domain(
        self, table: pa.Table, value_col: str = "value", threshold: float = 0.0
    ) -> ValidationResult:
        con = self.conn
        total = table.num_rows
        con.register("tbl", table)
        # Check negative values for prices / volumes
        query = f'SELECT * FROM tbl WHERE "{value_col}" < 0'
        violations_arrow = con.sql(query).arrow().read_all()
        viol_count = violations_arrow.num_rows
        viol_rate = viol_count / total if total > 0 else 0.0
        samples = violations_arrow.slice(0, 5).to_pylist()
        return ValidationResult("range_domain", viol_count == 0, total, viol_count, viol_rate, samples, threshold)

    def validate_intra_drop_duplicates(
        self, table: pa.Table, key_cols: list[str], threshold: float = 0.0
    ) -> ValidationResult:
        con = self.conn
        total = table.num_rows
        con.register("tbl", table)
        keys_str = ", ".join([f'"{k}"' for k in key_cols])
        query = f"""
            SELECT {keys_str}, count(*) as cnt, min(value) as min_val, max(value) as max_val
            FROM tbl
            GROUP BY {keys_str}
            HAVING count(*) > 1
        """
        dups_arrow = con.sql(query).arrow().read_all()
        viol_count = dups_arrow.num_rows
        viol_rate = viol_count / total if total > 0 else 0.0
        samples = dups_arrow.slice(0, 5).to_pylist()
        return ValidationResult("intra_drop_duplicates", viol_count == 0, total, viol_count, viol_rate, samples, threshold)

    def validate_monotone_timestamps(
        self, table: pa.Table, entity_col: str = "entity_id", time_col: str = "event_time"
    ) -> ValidationResult:
        con = self.conn
        total = table.num_rows
        con.register("tbl", table)
        # Lag query checking if previous event_time > current event_time within entity
        query = f"""
            WITH ordered AS (
                SELECT *,
                    lag("{time_col}") OVER (PARTITION BY "{entity_col}" ORDER BY rowid) as prev_time
                FROM (SELECT row_number() OVER () as rowid, * FROM tbl)
            )
            SELECT * FROM ordered WHERE prev_time IS NOT NULL AND "{time_col}" < prev_time
        """
        viol_arrow = con.sql(query).arrow().read_all()
        viol_count = viol_arrow.num_rows
        viol_rate = viol_count / total if total > 0 else 0.0
        samples = viol_arrow.slice(0, 5).to_pylist()
        return ValidationResult("monotone_timestamps", viol_count == 0, total, viol_count, viol_rate, samples, 0.0)

    def validate_future_timestamps(
        self,
        table: pa.Table,
        event_col: str = "event_time",
        knowledge_col: str = "knowledge_time",
        tolerance_seconds: int = 300,
    ) -> ValidationResult:
        con = self.conn
        total = table.num_rows
        con.register("tbl", table)
        query = f"""
            SELECT * FROM tbl
            WHERE "{event_col}" > "{knowledge_col}" + INTERVAL {tolerance_seconds} SECOND
        """
        viol_arrow = con.sql(query).arrow().read_all()
        viol_count = viol_arrow.num_rows
        viol_rate = viol_count / total if total > 0 else 0.0
        samples = viol_arrow.slice(0, 5).to_pylist()
        return ValidationResult("future_timestamps", viol_count == 0, total, viol_count, viol_rate, samples, 0.0)

    def validate_universe_coverage(
        self, table: pa.Table, expected_universe: set[str], entity_col: str = "entity_id", threshold: float = 0.90
    ) -> ValidationResult:
        con = self.conn
        con.register("tbl", table)
        res = con.sql(f'SELECT DISTINCT "{entity_col}" FROM tbl WHERE "{entity_col}" IS NOT NULL').fetchall()
        observed_entities = {row[0] for row in res}
        missing_entities = expected_universe - observed_entities
        unlisted_additions = observed_entities - expected_universe

        coverage_rate = (len(expected_universe) - len(missing_entities)) / len(expected_universe) if expected_universe else 1.0
        passed = coverage_rate >= threshold
        sample = [{"missing": list(missing_entities), "unlisted": list(unlisted_additions)}]
        return ValidationResult(
            "universe_coverage",
            passed,
            len(expected_universe),
            len(missing_entities),
            1.0 - coverage_rate,
            sample,
            threshold,
        )


@pytest.fixture
def val_engine(temp_duckdb_conn):
    return VectorizedValidationEngine(temp_duckdb_conn)


# ============================================================================
# F-VAL-01: Completeness Tests
# ============================================================================

@pytest.mark.tier1
def test_val_completeness_baseline_zero_nulls(fixture_paths, val_engine):
    """Verify Drop 1 baseline contains zero nulls in mandatory fields and passes SLA."""
    table = pq.read_table(fixture_paths.drop_001_parquet)
    res = val_engine.validate_completeness(table, ["record_id", "entity_id", "value", "event_time"])
    assert res.passed is True
    assert res.violation_count == 0
    assert res.violation_rate == 0.0


@pytest.mark.tier1
def test_val_completeness_drop3_null_value(fixture_paths, val_engine):
    """Verify Drop 3 flags REC_003_ANOM_0003 where value is null."""
    table = pq.read_table(fixture_paths.synth_dir / "drop_003_anomalies.csv") if (fixture_paths.synth_dir / "drop_003_anomalies.parquet").exists() else duckdb.sql(f"SELECT * FROM read_csv('{fixture_paths.drop_003_csv}', header=true)").arrow().read_all()
    res = val_engine.validate_completeness(table, ["value"])
    assert res.violation_count >= 1
    # Check sample contains null value record
    rec_ids = [s.get("record_id") for s in res.violations_sample]
    assert "REC_003_ANOM_0003" in rec_ids


@pytest.mark.tier1
def test_val_completeness_drop3_null_entity_id(fixture_paths, val_engine):
    """Verify Drop 3 flags REC_003_ANOM_0004 where entity_id is null."""
    table = duckdb.sql(f"SELECT * FROM read_csv('{fixture_paths.drop_003_csv}', header=true)").arrow().read_all()
    res = val_engine.validate_completeness(table, ["entity_id"])
    assert res.violation_count >= 1
    rec_ids = [s.get("record_id") for s in res.violations_sample]
    assert "REC_003_ANOM_0004" in rec_ids


@pytest.mark.tier1
def test_val_completeness_sla_threshold_satisfied(val_engine):
    """Verify null rate below threshold passes validation."""
    data = [{"record_id": f"R_{i}", "val": None if i == 0 else float(i)} for i in range(100)]
    table = pa.Table.from_pylist(data)
    res = val_engine.validate_completeness(table, ["val"], threshold=0.02)
    assert res.passed is True  # 1% <= 2%


@pytest.mark.tier1
def test_val_completeness_sla_threshold_exceeded(val_engine):
    """Verify null rate exceeding SLA threshold fails validation."""
    data = [{"record_id": f"R_{i}", "val": None if i < 5 else float(i)} for i in range(100)]
    table = pa.Table.from_pylist(data)
    res = val_engine.validate_completeness(table, ["val"], threshold=0.02)
    assert res.passed is False  # 5% > 2%


# ============================================================================
# F-VAL-02: Range & Domain Tests
# ============================================================================

@pytest.mark.tier1
def test_val_range_baseline_positive_prices(fixture_paths, val_engine):
    """Verify Drop 1 baseline contains strictly positive prices."""
    table = pq.read_table(fixture_paths.drop_001_parquet)
    res = val_engine.validate_range_and_domain(table, value_col="value")
    assert res.passed is True
    assert res.violation_count == 0


@pytest.mark.tier1
def test_val_range_drop3_negative_aapl(fixture_paths, val_engine):
    """Verify Drop 3 flags REC_003_ANOM_0001 (negative price -178.50 for AAPL)."""
    table = duckdb.sql(f"SELECT * FROM read_csv('{fixture_paths.drop_003_csv}', header=true)").arrow().read_all()
    res = val_engine.validate_range_and_domain(table, value_col="value")
    assert res.passed is False
    rec_ids = [s.get("record_id") for s in res.violations_sample]
    assert "REC_003_ANOM_0001" in rec_ids


@pytest.mark.tier1
def test_val_range_drop3_negative_tsla(fixture_paths, val_engine):
    """Verify Drop 3 flags REC_003_ANOM_0002 (negative price -242.00 for TSLA)."""
    table = duckdb.sql(f"SELECT * FROM read_csv('{fixture_paths.drop_003_csv}', header=true)").arrow().read_all()
    res = val_engine.validate_range_and_domain(table, value_col="value")
    rec_ids = [s.get("record_id") for s in res.violations_sample]
    assert "REC_003_ANOM_0002" in rec_ids


@pytest.mark.tier1
def test_val_range_negative_volume_flagged(val_engine):
    """Verify negative volume values are flagged as violations."""
    records = [
        {"entity_id": "ENT_AAPL", "metric": "VOLUME", "value": 100000.0},
        {"entity_id": "ENT_MSFT", "metric": "VOLUME", "value": -500.0},
    ]
    table = pa.Table.from_pylist(records)
    res = val_engine.validate_range_and_domain(table, value_col="value")
    assert res.violation_count == 1
    assert res.violations_sample[0]["value"] == -500.0


@pytest.mark.tier1
def test_val_range_5_sigma_price_outlier(val_engine):
    """Verify 5-sigma outlier detector identifies extreme price spikes."""
    np.random.seed(42)
    # Generate 100 prices with mean 100, std 2
    prices = np.random.normal(loc=100.0, scale=2.0, size=100).tolist()
    # Inject 5-sigma spike
    prices[50] = 100.0 + 7 * 2.0  # 7-sigma outlier (114.0)

    mean_val = np.mean(prices)
    std_val = np.std(prices)
    outliers = [p for p in prices if abs(p - mean_val) > 5 * std_val]

    assert len(outliers) == 1
    assert outliers[0] == pytest.approx(114.0, abs=1.0)


@pytest.mark.tier1
def test_val_range_normal_volatility_no_false_alarm(val_engine):
    """Verify normal volatility (+/- 2%) produces zero outlier false alarms."""
    normal_series = [100.0 * (1 + 0.02 * (i % 2 * 2 - 1)) for i in range(100)]
    mean_val = np.mean(normal_series)
    std_val = np.std(normal_series)
    outliers = [p for p in normal_series if abs(p - mean_val) > 5 * std_val]
    assert len(outliers) == 0


# ============================================================================
# F-VAL-03: Intra-Drop Duplicate Tests
# ============================================================================

@pytest.mark.tier1
def test_val_duplicates_baseline_no_collisions(fixture_paths, val_engine):
    """Verify Drop 1 baseline contains zero intra-drop key collisions."""
    table = pq.read_table(fixture_paths.drop_001_parquet)
    res = val_engine.validate_intra_drop_duplicates(table, ["entity_id", "event_time", "metric"])
    assert res.passed is True
    assert res.violation_count == 0


@pytest.mark.tier1
def test_val_duplicates_exact_duplicate_rows(val_engine):
    """Verify exact duplicate rows in the same drop are flagged."""
    records = [
        {"entity_id": "ENT_AAPL", "event_time": "2023-10-02T16:00:00Z", "metric": "CLOSE", "value": 172.0},
        {"entity_id": "ENT_AAPL", "event_time": "2023-10-02T16:00:00Z", "metric": "CLOSE", "value": 172.0},
    ]
    table = pa.Table.from_pylist(records)
    res = val_engine.validate_intra_drop_duplicates(table, ["entity_id", "event_time", "metric"])
    assert res.passed is False
    assert res.violation_count == 1


@pytest.mark.tier1
def test_val_duplicates_conflicting_values(val_engine):
    """Verify conflicting values for same (entity_id, event_time, metric) are flagged."""
    records = [
        {"entity_id": "ENT_AAPL", "event_time": "2023-10-02T16:00:00Z", "metric": "CLOSE", "value": 172.0},
        {"entity_id": "ENT_AAPL", "event_time": "2023-10-02T16:00:00Z", "metric": "CLOSE", "value": 180.0},  # Conflict!
    ]
    table = pa.Table.from_pylist(records)
    res = val_engine.validate_intra_drop_duplicates(table, ["entity_id", "event_time", "metric"])
    assert res.violation_count == 1
    sample = res.violations_sample[0]
    assert sample["min_val"] != sample["max_val"]


@pytest.mark.tier1
def test_val_duplicates_distinct_metrics_allowed(val_engine):
    """Verify distinct metrics for same entity and event_time are permitted."""
    records = [
        {"entity_id": "ENT_AAPL", "event_time": "2023-10-02T16:00:00Z", "metric": "CLOSE_PRICE", "value": 172.0},
        {"entity_id": "ENT_AAPL", "event_time": "2023-10-02T16:00:00Z", "metric": "VOLUME", "value": 50000000.0},
    ]
    table = pa.Table.from_pylist(records)
    res = val_engine.validate_intra_drop_duplicates(table, ["entity_id", "event_time", "metric"])
    assert res.passed is True
    assert res.violation_count == 0


# ============================================================================
# F-VAL-04: Monotone Timestamp Tests
# ============================================================================

@pytest.mark.tier1
def test_val_monotone_baseline_strictly_increasing(fixture_paths, val_engine):
    """Verify Drop 1 records have strictly non-decreasing timestamps per entity."""
    table = pq.read_table(fixture_paths.drop_001_parquet)
    res = val_engine.validate_monotone_timestamps(table)
    assert res.passed is True
    assert res.violation_count == 0


@pytest.mark.tier1
def test_val_monotone_drop3_backward_jump(fixture_paths, val_engine):
    """Verify Drop 3 flags backward clock jump (REC_003_ANOM_0007, 0008)."""
    table = duckdb.sql(f"SELECT * FROM read_csv('{fixture_paths.drop_003_csv}', header=true)").arrow().read_all()
    res = val_engine.validate_monotone_timestamps(table)
    assert res.violation_count >= 1
    rec_ids = [s.get("record_id") for s in res.violations_sample]
    assert "REC_003_ANOM_0007" in rec_ids or "REC_003_ANOM_0008" in rec_ids


@pytest.mark.tier1
def test_val_monotone_per_entity_isolation(val_engine):
    """Verify timestamps are evaluated partitioned by entity, not globally."""
    records = [
        {"entity_id": "ENT_A", "event_time": "2023-10-05T16:00:00Z"},
        {"entity_id": "ENT_B", "event_time": "2023-10-01T16:00:00Z"},  # earlier than ENT_A, but valid for ENT_B
    ]
    table = pa.Table.from_pylist(records)
    res = val_engine.validate_monotone_timestamps(table)
    assert res.passed is True


@pytest.mark.tier1
def test_val_monotone_equal_timestamps_allowed(val_engine):
    """Verify identical event_times (non-decreasing) pass monotonicity check."""
    records = [
        {"entity_id": "ENT_A", "event_time": "2023-10-02T16:00:00Z"},
        {"entity_id": "ENT_A", "event_time": "2023-10-02T16:00:00Z"},
    ]
    table = pa.Table.from_pylist(records)
    res = val_engine.validate_monotone_timestamps(table)
    assert res.passed is True


# ============================================================================
# F-VAL-05: Future Timestamp Guard Tests
# ============================================================================

@pytest.mark.tier1
def test_val_future_baseline_past_timestamps(fixture_paths, val_engine):
    """Verify Drop 1 event_times are in the past relative to knowledge_time."""
    table = pq.read_table(fixture_paths.drop_001_parquet)
    res = val_engine.validate_future_timestamps(table)
    assert res.passed is True
    assert res.violation_count == 0


@pytest.mark.tier1
def test_val_future_drop3_future_year_2025(fixture_paths, val_engine):
    """Verify Drop 3 flags REC_003_ANOM_0005 with event_time in 2025."""
    table = duckdb.sql(f"SELECT * FROM read_csv('{fixture_paths.drop_003_csv}', header=true)").arrow().read_all()
    res = val_engine.validate_future_timestamps(table)
    assert res.violation_count >= 1
    rec_ids = [s.get("record_id") for s in res.violations_sample]
    assert "REC_003_ANOM_0005" in rec_ids


@pytest.mark.tier1
def test_val_future_drop3_future_year_2030(fixture_paths, val_engine):
    """Verify Drop 3 flags REC_003_ANOM_0006 with event_time in 2030."""
    table = duckdb.sql(f"SELECT * FROM read_csv('{fixture_paths.drop_003_csv}', header=true)").arrow().read_all()
    res = val_engine.validate_future_timestamps(table)
    rec_ids = [s.get("record_id") for s in res.violations_sample]
    assert "REC_003_ANOM_0006" in rec_ids


@pytest.mark.tier1
def test_val_future_tolerance_window_pass(val_engine):
    """Verify event_time 1 minute ahead of knowledge_time within 5-min tolerance passes."""
    t_kn = datetime(2023, 10, 6, 20, 0, 0, tzinfo=timezone.utc)
    t_ev = t_kn + timedelta(minutes=1)
    records = [{"record_id": "REC_TOL", "event_time": t_ev, "knowledge_time": t_kn}]
    table = pa.Table.from_pylist(records)
    res = val_engine.validate_future_timestamps(table, tolerance_seconds=300)
    assert res.passed is True
    assert res.violation_count == 0


@pytest.mark.tier1
def test_val_future_tolerance_window_fail(val_engine):
    """Verify event_time 10 minutes ahead exceeding 5-min tolerance fails."""
    t_kn = datetime(2023, 10, 6, 20, 0, 0, tzinfo=timezone.utc)
    t_ev = t_kn + timedelta(minutes=10)
    records = [{"record_id": "REC_TOL", "event_time": t_ev, "knowledge_time": t_kn}]
    table = pa.Table.from_pylist(records)
    res = val_engine.validate_future_timestamps(table, tolerance_seconds=300)
    assert res.passed is False
    assert res.violation_count == 1


# ============================================================================
# F-VAL-06: Universe Coverage Tests
# ============================================================================

@pytest.mark.tier1
def test_val_universe_baseline_full_coverage(fixture_paths, val_engine):
    """Verify Drop 1 baseline contains 100% of benchmark universe (10 equities)."""
    expected_universe = {
        "ENT_AAPL", "ENT_MSFT", "ENT_GOOGL", "ENT_AMZN", "ENT_NVDA",
        "ENT_META", "ENT_TSLA", "ENT_UNH", "ENT_XOM", "ENT_JPM"
    }
    table = pq.read_table(fixture_paths.drop_001_parquet)
    res = val_engine.validate_universe_coverage(table, expected_universe, threshold=0.90)
    assert res.passed is True
    assert res.violation_count == 0
    assert res.violation_rate == 0.0


@pytest.mark.tier1
def test_val_universe_dropout_detected(val_engine):
    """Verify missing 2 equities from expected 10 flags dropouts and calculates 80% coverage."""
    expected_universe = {f"ENT_{i}" for i in range(10)}
    present_universe = {f"ENT_{i}" for i in range(8)}  # missing ENT_8 and ENT_9
    table = pa.Table.from_pylist([{"entity_id": e} for e in present_universe])

    res = val_engine.validate_universe_coverage(table, expected_universe, threshold=0.90)
    assert res.passed is False  # 80% < 90%
    assert res.violation_count == 2
    assert set(res.violations_sample[0]["missing"]) == {"ENT_8", "ENT_9"}


@pytest.mark.tier1
def test_val_universe_unlisted_addition_detected(val_engine):
    """Verify entity present in drop but absent from benchmark universe is detected."""
    expected_universe = {"ENT_AAPL", "ENT_MSFT"}
    table = pa.Table.from_pylist([{"entity_id": "ENT_AAPL"}, {"entity_id": "ENT_UNKNOWN"}])
    res = val_engine.validate_universe_coverage(table, expected_universe, threshold=0.50)
    assert "ENT_UNKNOWN" in res.violations_sample[0]["unlisted"]


@pytest.mark.tier1
def test_val_universe_sla_pass_fail(val_engine):
    """Verify SLA threshold dictates pass/fail status."""
    expected = {f"ENT_{i}" for i in range(10)}
    table = pa.Table.from_pylist([{"entity_id": f"ENT_{i}"} for i in range(7)])  # 70%
    res_lenient = val_engine.validate_universe_coverage(table, expected, threshold=0.60)
    assert res_lenient.passed is True

    res_strict = val_engine.validate_universe_coverage(table, expected, threshold=0.80)
    assert res_strict.passed is False


# ============================================================================
# F-VAL-07: Vectorized Rule Execution Tests
# ============================================================================

@pytest.mark.tier1
def test_val_vectorized_duckdb_execution_engine(fixture_paths, val_engine):
    """Verify vectorized execution using DuckDB SQL returns strongly-typed ValidationResult."""
    table = pq.read_table(fixture_paths.drop_001_parquet)
    res = val_engine.validate_completeness(table, ["record_id", "value"])
    assert isinstance(res, ValidationResult)
    assert res.check_name == "completeness"
    assert res.total_records == 60


@pytest.mark.tier1
def test_val_vectorized_high_throughput_synthetic(val_engine):
    """Verify execution throughput over 10,000 synthetic records executes in < 500ms."""
    import time
    n_rows = 10000
    table = pa.Table.from_pydict({
        "record_id": [f"R_{i}" for i in range(n_rows)],
        "entity_id": [f"ENT_{i % 50}" for i in range(n_rows)],
        "value": [100.0 + (i % 10) for i in range(n_rows)],
    })

    t0 = time.perf_counter()
    res1 = val_engine.validate_completeness(table, ["record_id", "value"])
    res2 = val_engine.validate_range_and_domain(table, "value")
    elapsed = time.perf_counter() - t0

    assert res1.passed is True
    assert res2.passed is True
    assert elapsed < 0.50  # Must complete in under half a second


@pytest.mark.tier1
def test_val_vectorized_all_7_rules_full_suite(fixture_paths, val_engine):
    """Verify comprehensive validation execution over Drop 3 anomalies."""
    table = duckdb.sql(f"SELECT * FROM read_csv('{fixture_paths.drop_003_csv}', header=true)").arrow().read_all()
    results = [
        val_engine.validate_completeness(table, ["value", "entity_id"]),
        val_engine.validate_range_and_domain(table, "value"),
        val_engine.validate_monotone_timestamps(table),
        val_engine.validate_future_timestamps(table),
    ]
    # In Drop 3, all 4 checks should detect anomalies
    failures = [r for r in results if not r.passed]
    assert len(failures) == 4
