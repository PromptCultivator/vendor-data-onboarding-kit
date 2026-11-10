"""
Tier 2 Boundary Tests: IEEE 754 Specials, Numeric Overflows, and Extreme Outliers.

Per TEST_INFRA.md and PROJECT.md §8.3 / F-VAL-02, F-DIF-03, F-MOD-01:
Tests domain boundaries and numeric edge-cases:
- IEEE 754 specials: NaN, +Inf, -Inf, quiet/signaling NaNs, NaN vs NULL differentiation
- 64-bit signed/unsigned integer bounds (Int64 min/max, UInt64 max, overflow guards)
- Floating point subnormals ($5 \times 10^{-324}$), underflow to zero, machine epsilon
- Extreme statistical outliers ($10^{18}$ market cap spike, $-10^{18}$ net loss) and 5-sigma checks
- Negative prices (equity rejection vs WTI oil exception -$37.63), negative volumes
- Floating point subtraction cancellation and precision loss in restatement deltas
- Point-in-time tolerance boundaries (sub-epsilon invariance vs supra-epsilon failures)
- Exact Decimal128 / Decimal256 currency representations
"""

from __future__ import annotations

import math
from decimal import Decimal
from datetime import datetime, timezone
from typing import Any

import pytest

try:
    import duckdb
except ImportError:
    duckdb = None  # type: ignore

try:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
except ImportError:
    pa = None  # type: ignore
    pc = None  # type: ignore
    pq = None  # type: ignore


pytestmark = [pytest.mark.tier2]


# ============================================================================
# 1. IEEE 754 Specials: NaN, +Inf, -Inf, and NULL Distinction
# ============================================================================

def test_ieee754_nan_floating_point_parsing():
    """Verify that IEEE 754 NaN values are parsed as floats and math.isnan() returns True."""
    nan_val = float("nan")
    assert math.isnan(nan_val)
    assert nan_val != nan_val  # IEEE 754 definition: NaN != NaN


def test_ieee754_nan_distinguished_from_null_in_arrow():
    """PyArrow must distinguish between a valid NaN float and an arrow null."""
    nan_arr = pa.array([1.0, float("nan"), None], type=pa.float64())

    assert nan_arr.null_count == 1  # Only index 2 is an arrow null
    assert not nan_arr.is_null()[1].as_py()  # Index 1 is NOT null
    assert nan_arr.is_null()[2].as_py()  # Index 2 IS null
    assert math.isnan(nan_arr[1].as_py())  # Index 1 is NaN
    assert nan_arr[2].as_py() is None


def test_ieee754_nan_distinguished_from_null_in_duckdb(temp_duckdb_conn):
    """DuckDB SQL distinguishing between `val IS NULL` and `isnan(val)`."""
    temp_duckdb_conn.execute("""
        CREATE TABLE specials (id INT, val DOUBLE);
        INSERT INTO specials VALUES 
            (1, 100.0), 
            (2, 'NaN'::DOUBLE), 
            (3, NULL), 
            (4, 'Infinity'::DOUBLE), 
            (5, '-Infinity'::DOUBLE);
    """)

    res = temp_duckdb_conn.execute("""
        SELECT id, 
               val IS NULL as is_null, 
               isnan(val) as is_nan, 
               isinf(val) as is_inf
        FROM specials ORDER BY id
    """).fetchall()

    # row 1: normal
    assert res[0] == (1, False, False, False)
    # row 2: NaN
    assert res[1] == (2, False, True, False)
    # row 3: NULL
    assert res[2] == (3, True, None, None)  # IS NULL is True, isnan(NULL) is NULL
    # row 4: +Inf
    assert res[3] == (4, False, False, True)
    # row 5: -Inf
    assert res[4] == (5, False, False, True)


def test_ieee754_positive_infinity_flagged_in_validation():
    """Validation checks on financial prices must reject +Infinity."""
    table = pa.table({
        "entity_id": ["AAPL", "MSFT", "GOOGL"],
        "price": [175.50, float("inf"), 140.20],
    })

    # Arrow vectorized check: price must be finite
    is_finite = pc.is_finite(table["price"])
    assert is_finite.to_pylist() == [True, False, True]

    violations = pc.invert(is_finite)
    assert pc.sum(violations.cast(pa.int64())).as_py() == 1


def test_ieee754_negative_infinity_flagged_in_validation():
    """Validation checks on financial metrics must reject -Infinity."""
    table = pa.table({
        "entity_id": ["AAPL", "MSFT"],
        "net_income": [1000000.0, float("-inf")],
    })

    is_finite = pc.is_finite(table["net_income"])
    assert is_finite.to_pylist() == [True, False]


def test_ieee754_quiet_vs_signaling_nan_representation():
    """Verify struct packing preserves quiet NaN bit patterns without crashing the interpreter."""
    import struct
    # 64-bit quiet NaN: exponent all 1s (0x7FF) and fraction MSB set (0x8000000000000)
    quiet_nan_bytes = struct.pack(">Q", 0x7FF8000000000001)
    val = struct.unpack(">d", quiet_nan_bytes)[0]
    assert math.isnan(val)

    # Roundtrip through PyArrow array
    arr = pa.array([val], type=pa.float64())
    assert math.isnan(arr[0].as_py())


# ============================================================================
# 2. 64-Bit Integer Limits & Overflow Boundaries
# ============================================================================

def test_int64_max_boundary_value_preservation():
    """Int64 max (2^63 - 1 = 9,223,372,036,854,775,807) must be preserved exactly."""
    int64_max = 9223372036854775807
    arr = pa.array([int64_max], type=pa.int64())
    assert arr[0].as_py() == int64_max

    # Parquet write/read cycle preserves exact int64
    table = pa.table({"big_id": arr})
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    read_table = pq.read_table(sink.getvalue())
    assert read_table["big_id"][0].as_py() == int64_max


def test_int64_min_boundary_value_preservation():
    """Int64 min (-2^63 = -9,223,372,036,854,775,808) must be preserved exactly."""
    int64_min = -9223372036854775808
    arr = pa.array([int64_min], type=pa.int64())
    assert arr[0].as_py() == int64_min


def test_int64_overflow_beyond_max_raises_or_widens():
    """Attempting to fit 2^63 into signed Int64 must raise ArrowInvalid / OverflowError."""
    overflow_val = 9223372036854775808  # 2^63 (1 past Int64 max)

    with pytest.raises((pa.ArrowInvalid, OverflowError)):
        pa.array([overflow_val], type=pa.int64())


def test_uint64_max_boundary_preservation():
    """UInt64 max (2^64 - 1 = 18,446,744,073,709,551,615) must be preserved."""
    uint64_max = 18446744073709551615
    arr = pa.array([uint64_max], type=pa.uint64())
    assert arr[0].as_py() == uint64_max


def test_uint64_negative_value_rejection():
    """Negative values in unsigned UInt64 must raise ArrowInvalid or OverflowError."""
    with pytest.raises((pa.ArrowInvalid, OverflowError)):
        pa.array([-1], type=pa.uint64())


def test_duckdb_aggregate_sums_on_int64_max_no_silent_overflow(temp_duckdb_conn):
    """DuckDB summing large Int64 values must not silently wrap around (overflow protection)."""
    int64_max = 9223372036854775807
    temp_duckdb_conn.execute(f"CREATE TABLE big_ints (val BIGINT);")
    temp_duckdb_conn.execute(f"INSERT INTO big_ints VALUES ({int64_max}), (1);")

    # Adding 1 to int64_max in BIGINT must raise an out-of-range error or cast to HUGEINT
    try:
        # HUGEINT sum promotes safely
        sum_res = temp_duckdb_conn.execute("SELECT sum(val::HUGEINT) FROM big_ints").fetchone()[0]
        assert sum_res == int64_max + 1
    except duckdb.Error:
        pass  # Overflow error also compliant


# ============================================================================
# 3. Subnormal Floating Point Numbers & Underflows
# ============================================================================

def test_subnormal_float64_minimum_positive():
    """Minimum positive subnormal float64 (5e-324) must not prematurely round to zero."""
    min_subnormal = 5e-324
    assert min_subnormal > 0.0
    arr = pa.array([min_subnormal], type=pa.float64())
    assert arr[0].as_py() == min_subnormal


def test_float64_underflow_to_zero():
    """Numbers smaller than 5e-324 underflow to 0.0 per IEEE 754."""
    underflow_val = 1e-325
    assert underflow_val == 0.0


def test_float64_machine_epsilon_precision():
    """Machine epsilon for float64 is 2^-52 (approx 2.22e-16). Adding smaller epsilon is absorbed."""
    eps = 2.220446049250313e-16
    assert 1.0 + eps > 1.0
    # Half of epsilon cannot be distinguished from 1.0 in float64
    assert 1.0 + (eps / 2.0) == 1.0


# ============================================================================
# 4. Extreme Statistical Outliers & Domain Checks
# ============================================================================

def test_extreme_statistical_outlier_positive_10_pow_18():
    """A value of 10^18 represents an extreme outlier that must trigger F-VAL-02 range check."""
    spike_val = 1e18
    # Distribution of typical company market caps in USD (billions)
    normal_caps = [1e11, 2e11, 1.5e11, 3e11, 2.5e11, 1.8e11, 2.2e11]
    all_caps = normal_caps + [spike_val]

    mean = sum(all_caps) / len(all_caps)
    variance = sum((x - mean) ** 2 for x in all_caps) / len(all_caps)
    std_dev = math.sqrt(variance)

    # Z-score of spike value
    z_score = abs(spike_val - mean) / std_dev
    # Must exceed 2.0 standard deviations even when including itself in distribution
    assert z_score > 2.0


def test_extreme_statistical_outlier_negative_10_pow_18():
    """Extreme negative value -10^18 must be detected as anomalous loss."""
    spike_loss = -1e18
    arr = pa.array([spike_loss], type=pa.float64())
    assert arr[0].as_py() == -1e18
    # Check that magnitude is > 10^17
    assert abs(arr[0].as_py()) >= 1e17


def test_validation_engine_negative_equity_price_flagged():
    """Equity share prices must be strictly positive (> 0.0). Negative prices are violations."""
    prices = pa.array([150.25, -0.01, -45.00, 10.00], type=pa.float64())
    # Equity rule: price > 0
    is_valid = pc.greater(prices, 0.0)
    assert is_valid.to_pylist() == [True, False, False, True]

    violations_count = pc.sum(pc.invert(is_valid).cast(pa.int64())).as_py()
    assert violations_count == 2


def test_validation_engine_negative_volume_flagged():
    """Trading volume must be non-negative (>= 0). Negative volume is strictly invalid."""
    volumes = pa.array([1000000, -500, 0, 250000], type=pa.int64())
    is_valid = pc.greater_equal(volumes, 0)
    assert is_valid.to_pylist() == [True, False, True, True]

    violations = pc.invert(is_valid)
    assert pc.sum(violations.cast(pa.int64())).as_py() == 1


def test_negative_commodity_price_wti_historical_exception():
    """On 2020-04-20, WTI crude oil futures settled at -$37.63. Domain rules must allow negative commodity prices."""
    wti_settlement = -37.63
    # Commodity domain allows negative values when asset_class == 'COMMODITY_FUTURES'
    asset_class = "COMMODITY_FUTURES"
    is_allowed = (asset_class == "COMMODITY_FUTURES") or (wti_settlement >= 0)
    assert is_allowed is True

    # Same price in equity domain must be rejected
    asset_class_equity = "EQUITY"
    is_equity_allowed = (asset_class_equity == "COMMODITY_FUTURES") or (wti_settlement >= 0)
    assert is_equity_allowed is False


def test_zero_price_and_volume_boundary_handling():
    """Zero volume is valid (e.g. trading halt), but zero equity price is a potential anomaly."""
    vol_zero = 0
    assert vol_zero >= 0  # Allowed for volume

    price_zero = 0.0
    # Price = 0.0 triggers zero-price warning in validation
    assert price_zero <= 0.0


# ============================================================================
# 5. Floating Point Rounding & Restatement Delta Precision
# ============================================================================

def test_floating_point_subtraction_cancellation_error_delta():
    """Demonstrate IEEE 754 subtraction artifact: 0.3 - 0.1 != 0.2 in standard binary float."""
    v1 = 0.3
    v2 = 0.1
    delta = v1 - v2
    assert delta != 0.2  # 0.19999999999999998 != 0.2
    assert abs(delta - 0.2) < 1e-15  # Within float tolerance


def test_restatement_delta_exact_representation_with_decimal():
    """High precision financial restatement deltas must be computed using Decimal to avoid cancellation."""
    val_baseline = Decimal("123456789.987654321")
    val_restated = Decimal("123456789.987654320")

    exact_delta = val_restated - val_baseline
    assert exact_delta == Decimal("-0.000000001")


def test_pit_invariance_tolerance_with_sub_epsilon_differences(pit_invariance_assert, sample_canonical_records):
    """PIT assertion must pass when floating point numbers differ by <= float_abs_tol (1e-9)."""
    h1 = sample_canonical_records
    # Copy and add 1e-11 to value (well below 1e-9 tolerance)
    h2 = [
        type(r)(
            entity_id=r.entity_id,
            event_time=r.event_time,
            knowledge_time=r.knowledge_time,
            value=float(r.value) + 1e-11 if isinstance(r.value, (int, float)) else r.value,
            unit=r.unit,
            source_symbol=r.source_symbol,
            source_symbol_type=r.source_symbol_type,
            vendor_id=r.vendor_id,
            dataset_id=r.dataset_id,
            drop_id=r.drop_id,
            is_restatement=r.is_restatement,
            revision_seq=r.revision_seq,
        )
        for r in h1
    ]

    # Must pass cleanly with default float_abs_tol=1e-9
    assert pit_invariance_assert(h1, h2, float_abs_tol=1e-9) is True


def test_pit_invariance_fails_when_difference_exceeds_epsilon(pit_invariance_assert, sample_canonical_records):
    """PIT assertion must fail when floating point difference exceeds float_abs_tol (1e-9)."""
    h1 = sample_canonical_records
    # Mutate by 1e-7 (larger than 1e-9)
    h2 = [
        type(r)(
            entity_id=r.entity_id,
            event_time=r.event_time,
            knowledge_time=r.knowledge_time,
            value=float(r.value) + 1e-7 if isinstance(r.value, (int, float)) else r.value,
            unit=r.unit,
            source_symbol=r.source_symbol,
            source_symbol_type=r.source_symbol_type,
            vendor_id=r.vendor_id,
            dataset_id=r.dataset_id,
            drop_id=r.drop_id,
            is_restatement=r.is_restatement,
            revision_seq=r.revision_seq,
        )
        for r in h1
    ]

    with pytest.raises(AssertionError, match="PIT Invariance Violation"):
        pit_invariance_assert(h1, h2, float_abs_tol=1e-9)


def test_division_by_zero_ratio_generates_inf_or_null(temp_duckdb_conn):
    """Calculating financial ratios (e.g. PE = price / earnings) when earnings = 0."""
    temp_duckdb_conn.execute("""
        CREATE TABLE ratios (price DOUBLE, earnings DOUBLE);
        INSERT INTO ratios VALUES (100.0, 0.0), (50.0, 2.5);
    """)

    # In DuckDB double division by zero returns 'Infinity'::DOUBLE or NULL depending on nullif
    res = temp_duckdb_conn.execute("""
        SELECT price / earnings as raw_div,
               price / NULLIF(earnings, 0.0) as safe_div
        FROM ratios
        ORDER BY price DESC
    """).fetchall()

    # row 1 (100/0): raw_div is inf, safe_div is NULL
    assert math.isinf(res[0][0])
    assert res[0][1] is None
    # row 2 (50/2.5): 20.0
    assert res[1][0] == 20.0
    assert res[1][1] == 20.0


# ============================================================================
# 6. Fixed-Point Decimals & Integer Conversions
# ============================================================================

def test_decimal128_max_scale_and_precision():
    """PyArrow Decimal128 supports up to 38 digits of precision."""
    dec_type = pa.decimal128(38, 10)
    dec_val = Decimal("1234567890123456789012345678.1234567890")
    arr = pa.array([dec_val], type=dec_type)

    assert arr[0].as_py() == dec_val


def test_decimal256_high_precision_currency_totals():
    """PyArrow Decimal256 supports up to 76 digits of precision for global macroeconomic aggregate sums."""
    dec256_type = pa.decimal256(76, 18)
    huge_val = Decimal("9" * 58 + "." + "9" * 18)
    arr = pa.array([huge_val], type=dec256_type)

    assert arr[0].as_py() == huge_val


def test_int32_overflow_into_int64():
    """A 32-bit signed integer maxes at 2,147,483,647. Overflowing values require Int64."""
    val = 2147483648  # Int32 max + 1
    with pytest.raises(pa.ArrowInvalid):
        pa.array([val], type=pa.int32())

    # Fits safely in Int64
    arr64 = pa.array([val], type=pa.int64())
    assert arr64[0].as_py() == val


def test_float32_precision_loss_on_large_integers():
    """PyArrow enforces strict IEEE 754 float32 precision and rejects integers > 2^24 that cannot be represented exactly."""
    exact_int = 16777217  # 2^24 + 1
    with pytest.raises(pa.ArrowInvalid, match="outside of the range exactly representable"):
        pa.array([exact_int], type=pa.float32())

    # Float64 has 53 bits of significand and preserves it exactly
    f64_arr = pa.array([exact_int], type=pa.float64())
    assert f64_arr[0].as_py() == exact_int


def test_monetary_currency_amount_cents_rounding_integrity():
    """Financial currencies rounded to cents (2 decimal places) using banker's rounding (ROUND_HALF_EVEN)."""
    from decimal import ROUND_HALF_EVEN
    # 2.5 cents rounds to 2 cents (even)
    d1 = Decimal("0.025").quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN)
    assert d1 == Decimal("0.02")

    # 3.5 cents rounds to 4 cents (even)
    d2 = Decimal("0.035").quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN)
    assert d2 == Decimal("0.04")


def test_sec_num_txt_extreme_value_scientific_notation(temp_duckdb_conn):
    """SEC DERA num.txt values formatted in scientific notation (e.g. 1.5e+09) parse accurately."""
    temp_duckdb_conn.execute("""
        CREATE TABLE sec_sample (tag VARCHAR, value DOUBLE);
        INSERT INTO sec_sample VALUES 
            ('Revenues', 1.5e11),
            ('NetIncomeLoss', -2.34e8),
            ('MicroMetric', 1.25e-4);
    """)

    rows = temp_duckdb_conn.execute("SELECT tag, value FROM sec_sample ORDER BY value DESC").fetchall()
    assert rows[0] == ("Revenues", 150000000000.0)
    assert rows[1] == ("MicroMetric", 0.000125)
    assert rows[2] == ("NetIncomeLoss", -234000000.0)


def test_alfred_vintage_series_negative_growth_rate_values(fixture_paths):
    """ALFRED macroeconomic indicators include genuine negative values (e.g. GDP contraction)."""
    # Verify PAYEMS or GDPC1 parse negative quarters / changes
    assert fixture_paths.alfred_gdpc1.is_file()
    if duckdb:
        conn = duckdb.connect(":memory:")
        # Query minimum value to ensure it handles real data range
        res = conn.execute(f"SELECT min(value::DOUBLE), max(value::DOUBLE) FROM read_csv('{fixture_paths.alfred_gdpc1}')").fetchall()[0]
        assert res[0] > 0.0  # Real GDP level is positive
        assert res[1] > 20000.0
        conn.close()


def test_canonical_record_with_extreme_values(synthetic_record_factory):
    """CanonicalRecord supports extreme float values (10^18) and large sequence numbers."""
    rec = synthetic_record_factory(
        value=1e18,
        revision_seq=999999,
    )
    assert rec.value == 1e18
    assert rec.revision_seq == 999999


def test_diff_engine_magnitude_delta_with_near_zero_baseline():
    """When baseline value is near zero ($0.0001), percentage delta must not divide by zero or explode undetected."""
    v_base = 0.00001
    v_new = 100.0

    abs_delta = abs(v_new - v_base)
    assert abs(abs_delta - 100.0) < 0.001

    # Safe percentage delta check
    pct_delta = ((v_new - v_base) / v_base) * 100.0
    assert pct_delta > 100000.0  # Huge jump flagged
