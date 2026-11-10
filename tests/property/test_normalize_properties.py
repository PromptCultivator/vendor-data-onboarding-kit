"""Hypothesis property-based tests for pure normalizers and canonical record invariants."""

from __future__ import annotations

import datetime
import zoneinfo
from datetime import timezone
from decimal import Decimal

import pyarrow as pa
import pytest
from hypothesis import given
from hypothesis import strategies as st

from vdok.normalize import (
    CANONICAL_ARROW_SCHEMA,
    CanonicalRow,
    compute_row_hash,
    normalize_identifier,
    normalize_unit,
    normalize_unit_and_value,
    parse_decimal_exact,
    parse_timestamp_utc,
)


# 1. Decimal exactness and float rejection properties
@given(st.floats(allow_nan=True, allow_infinity=True))
def test_decimal_strictly_rejects_all_floats(val: float) -> None:
    """parse_decimal_exact raises TypeError for every binary float instance."""
    with pytest.raises(TypeError, match="Binary floating-point value"):
        parse_decimal_exact(val)


@given(
    st.decimals(
        min_value=Decimal("-1e28"),
        max_value=Decimal("1e28"),
        allow_nan=False,
        allow_infinity=False,
        places=9,
    )
)
def test_decimal_exact_scale_preservation(d: Decimal) -> None:
    """Exact decimal scale is preserved through string representation without precision drift."""
    parsed_from_str = parse_decimal_exact(str(d))
    parsed_from_dec = parse_decimal_exact(d)
    assert parsed_from_str == d
    assert parsed_from_dec == d


@given(
    st.decimals(
        min_value=Decimal("-1e20"),
        max_value=Decimal("1e20"),
        allow_nan=False,
        allow_infinity=False,
        places=6,
    ),
    st.text(alphabet=" \t\r\n", min_size=1, max_size=4),
    st.text(alphabet=" \t\r\n", min_size=1, max_size=4),
)
def test_decimal_whitespace_invariance(d: Decimal, pad_l: str, pad_r: str) -> None:
    """Whitespace padding does not affect the parsed Decimal value."""
    raw = f"{pad_l}{d}{pad_r}"
    assert parse_decimal_exact(raw) == d


@given(st.sampled_from([None, "", "  ", "\t\n", "   \r\n  "]))
def test_decimal_empty_none_returns_none(val: str | None) -> None:
    """Empty strings and None strictly return None."""
    assert parse_decimal_exact(val) is None


# 2. Timestamp standardization properties
@given(
    st.datetimes(
        min_value=datetime.datetime(1990, 1, 1),
        max_value=datetime.datetime(2040, 12, 31),
    )
)
def test_timestamp_utc_roundtrip(dt: datetime.datetime) -> None:
    """Naive datetimes converted to UTC and formatted to ISO roundtrip accurately."""
    utc_dt = parse_timestamp_utc(dt, source_tz="UTC")
    iso_str = utc_dt.isoformat()
    parsed_back = parse_timestamp_utc(iso_str)
    assert parsed_back == utc_dt
    assert parsed_back.tzinfo == timezone.utc


@given(
    st.datetimes(
        min_value=datetime.datetime(1990, 1, 1),
        max_value=datetime.datetime(2040, 12, 31),
    ),
    st.sampled_from(["America/New_York", "Europe/London", "Asia/Tokyo", "UTC"]),
)
def test_timestamp_timezone_conversion_invariants(dt: datetime.datetime, tz_name: str) -> None:
    """Any naive datetime localized to source timezone converts to UTC datetime."""
    utc_dt = parse_timestamp_utc(dt, source_tz=tz_name)
    assert utc_dt.tzinfo == timezone.utc

    # Comparing timestamp seconds
    tz = zoneinfo.ZoneInfo(tz_name)
    expected_ts = dt.replace(tzinfo=tz).timestamp()
    assert utc_dt.timestamp() == pytest.approx(expected_ts, abs=1e-6)


# 3. Unit normalization and scaling properties
@given(st.sampled_from(["USD", "shares", "pure", "ratio"]))
def test_unit_canonical_idempotence(unit: str) -> None:
    """Canonical units are invariant fixed points under normalization."""
    assert normalize_unit(unit) == unit
    assert normalize_unit(normalize_unit(unit)) == unit


@given(
    st.sampled_from(
        [
            ("usd", "USD"),
            ("US$", "USD"),
            ("dollars", "USD"),
            ("shares", "shares"),
            ("pure", "pure"),
            ("%", "ratio"),
            ("pct", "ratio"),
            ("percent", "ratio"),
        ]
    ),
    st.text(alphabet=" \t", min_size=0, max_size=3),
)
def test_unit_case_whitespace_invariance(pair: tuple[str, str], pad: str) -> None:
    """Unit aliases resolve to expected canonical codes regardless of whitespace or casing."""
    variant, expected = pair
    raw = f"{pad}{variant}{pad}"
    assert normalize_unit(raw) == expected


@given(
    st.decimals(
        min_value=Decimal("-1000000"),
        max_value=Decimal("1000000"),
        allow_nan=False,
        allow_infinity=False,
        places=4,
    ),
    st.sampled_from(["%", "percent", "pct", "percentage"]),
)
def test_unit_percentage_scaling_exactness(val: Decimal, unit: str) -> None:
    """Percentage units scale value by 1/100 and set unit to ratio."""
    scaled_val, mapped_unit = normalize_unit_and_value(val, unit)
    assert mapped_unit == "ratio"
    assert scaled_val == val / Decimal(100)


# 4. Identifier folding properties
@given(st.integers(min_value=1, max_value=9999999999))
def test_identifier_cik_invariants(cik_int: int) -> None:
    """Normalized CIK is 10 digits, idempotent, and preserves numeric value."""
    s = str(cik_int)
    norm = normalize_identifier(s, id_type="cik")
    assert len(norm) == 10
    assert norm.isdigit()
    assert int(norm) == cik_int
    assert normalize_identifier(norm, id_type="cik") == norm


@given(
    st.text(
        alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-/",
        min_size=1,
        max_size=10,
    )
)
def test_identifier_ticker_invariants(raw_ticker: str) -> None:
    """Normalized ticker is uppercase, stripped, and idempotent."""
    norm = normalize_identifier(raw_ticker, id_type="ticker")
    assert norm == raw_ticker.strip().upper()
    assert normalize_identifier(norm, id_type="ticker") == norm


# 5. Row hash fingerprint properties
@given(
    st.text(alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ", min_size=1, max_size=5),
    st.text(alphabet="abcdefghijklm", min_size=1, max_size=8),
)
def test_row_hash_determinism_and_sensitivity(entity: str, field_name: str) -> None:
    """Identical rows yield identical hash; 1-character modification alters hash."""
    ts = datetime.datetime(2024, 1, 15, 14, 30, 0, tzinfo=timezone.utc)
    h1 = compute_row_hash(
        entity_id=entity,
        field=field_name,
        event_time=ts,
        value_num=Decimal("100.50"),
        value_str=None,
        unit="USD",
    )
    h2 = compute_row_hash(
        entity_id=entity,
        field=field_name,
        event_time=ts,
        value_num=Decimal("100.50"),
        value_str=None,
        unit="USD",
    )
    assert h1 == h2

    # Modified field must produce different hash
    h_diff = compute_row_hash(
        entity_id=entity,
        field=field_name + "X",
        event_time=ts,
        value_num=Decimal("100.50"),
        value_str=None,
        unit="USD",
    )
    assert h1 != h_diff


# 6. CanonicalRow Arrow roundtrip property
@given(
    st.decimals(
        min_value=Decimal("-1000000"),
        max_value=Decimal("1000000"),
        allow_nan=False,
        allow_infinity=False,
        places=4,
    )
)
def test_canonical_row_arrow_roundtrip(val: Decimal) -> None:
    """CanonicalRow models convert to PyArrow Table and back without data loss."""
    now = datetime.datetime(2024, 1, 15, 14, 30, 0, 100000, tzinfo=timezone.utc)
    row = CanonicalRow(
        entity_id="0000320193",
        entity_id_type="cik",
        field="revenues",
        event_time=now,
        knowledge_time=now,
        value_num=val,
        unit="USD",
        drop_id="0123456789abcdef",
        source_file="data.parquet",
        source_row=1,
    )
    record_dict = row.model_dump()
    tbl = pa.Table.from_pylist([record_dict], schema=CANONICAL_ARROW_SCHEMA)
    assert len(tbl) == 1
    recovered_dict = tbl.to_pylist()[0]
    recovered_row = CanonicalRow(**recovered_dict)
    assert recovered_row.entity_id == row.entity_id
    assert recovered_row.row_hash == row.row_hash
