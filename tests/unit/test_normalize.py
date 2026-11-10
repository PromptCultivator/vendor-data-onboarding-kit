"""Unit tests for canonical data models and pure normalizers."""

from __future__ import annotations

import datetime
import zoneinfo
from datetime import timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest
from pydantic import ValidationError

from vdok.manifest import EntityIdType, VendorManifest
from vdok.normalize import (
    CANONICAL_ARROW_SCHEMA,
    CanonicalRow,
    compute_row_hash,
    normalize_drop,
    normalize_identifier,
    normalize_unit,
    normalize_unit_and_value,
    parse_decimal_exact,
    parse_timestamp_utc,
)
from vdok.symbols import SymbolMaster, build_symbol_master


# 1. Tests for parse_decimal_exact
def test_parse_decimal_exact_string_and_int() -> None:
    """Parse string and integer numeric values to exact Decimals."""
    assert parse_decimal_exact("182.52") == Decimal("182.52")
    assert parse_decimal_exact("  -0.00500  ") == Decimal("-0.00500")
    assert parse_decimal_exact(42) == Decimal("42")
    assert parse_decimal_exact(0) == Decimal("0")
    assert parse_decimal_exact(Decimal("999.99")) == Decimal("999.99")


def test_parse_decimal_exact_commas() -> None:
    """Parse numbers with comma thousands separators."""
    assert parse_decimal_exact("1,234,567.89") == Decimal("1234567.89")


def test_parse_decimal_exact_none_and_empty() -> None:
    """Empty strings and None return None."""
    assert parse_decimal_exact(None) is None
    assert parse_decimal_exact("") is None
    assert parse_decimal_exact("   \t\n") is None


def test_parse_decimal_exact_strictly_rejects_floats() -> None:
    """Strictly reject binary floating-point inputs with TypeError."""
    with pytest.raises(TypeError, match="Binary floating-point value"):
        parse_decimal_exact(182.52)

    with pytest.raises(TypeError, match="Binary floating-point value"):
        parse_decimal_exact(0.0)

    with pytest.raises(TypeError, match="Binary floating-point value"):
        parse_decimal_exact(float("nan"))

    with pytest.raises(TypeError, match="Binary floating-point value"):
        parse_decimal_exact(float("inf"))


def test_parse_decimal_exact_rejects_non_finite_decimals() -> None:
    """Reject NaN and infinite Decimals."""
    with pytest.raises(ValueError, match="Non-finite decimal"):
        parse_decimal_exact(Decimal("nan"))

    with pytest.raises(ValueError, match="Non-finite decimal"):
        parse_decimal_exact(Decimal("Infinity"))

    with pytest.raises(ValueError, match="Non-finite decimal"):
        parse_decimal_exact(Decimal("-Infinity"))


def test_parse_decimal_exact_malformed_string_raises() -> None:
    """Malformed numeric strings raise ValueError."""
    with pytest.raises(ValueError, match="Malformed decimal"):
        parse_decimal_exact("not_a_number")

    with pytest.raises(TypeError, match="Unsupported type"):
        parse_decimal_exact([])


# 2. Tests for parse_timestamp_utc
def test_parse_timestamp_utc_naive_datetime() -> None:
    """Naive datetime has source timezone attached and converts to UTC."""
    dt_naive = datetime.datetime(2024, 1, 15, 9, 30, 0)
    utc_dt = parse_timestamp_utc(dt_naive, source_tz="America/New_York")
    assert utc_dt.tzinfo == timezone.utc
    assert utc_dt.hour == 14
    assert utc_dt.minute == 30


def test_parse_timestamp_utc_aware_datetime() -> None:
    """Aware datetime converts directly to UTC."""
    ny_tz = zoneinfo.ZoneInfo("America/New_York")
    dt_aware = datetime.datetime(2024, 1, 15, 9, 30, 0, tzinfo=ny_tz)
    utc_dt = parse_timestamp_utc(dt_aware)
    assert utc_dt.tzinfo == timezone.utc
    assert utc_dt.hour == 14


def test_parse_timestamp_utc_date() -> None:
    """Date object combines with midnight and converts to UTC."""
    d = datetime.date(2024, 1, 15)
    utc_dt = parse_timestamp_utc(d, source_tz="UTC")
    assert utc_dt == datetime.datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)


def test_parse_timestamp_utc_string_iso() -> None:
    """ISO format strings parse directly."""
    utc_dt = parse_timestamp_utc("2024-01-15T14:30:00Z")
    assert utc_dt == datetime.datetime(2024, 1, 15, 14, 30, 0, tzinfo=timezone.utc)


def test_parse_timestamp_utc_string_with_format() -> None:
    """Custom format strings parse accurately with source timezone."""
    utc_dt = parse_timestamp_utc("20240115", fmt="%Y%m%d", source_tz="America/New_York")
    assert utc_dt.tzinfo == timezone.utc
    assert utc_dt.year == 2024
    assert utc_dt.month == 1
    assert utc_dt.day == 15
    assert utc_dt.hour == 5


def test_parse_timestamp_utc_integer_date() -> None:
    """Integer dates parse when format is provided."""
    utc_dt = parse_timestamp_utc(20240115, fmt="%Y%m%d", source_tz="UTC")
    assert utc_dt == datetime.datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)


def test_parse_timestamp_utc_errors() -> None:
    """Invalid timestamp inputs raise descriptive exceptions."""
    with pytest.raises(ValueError, match="Cannot parse None"):
        parse_timestamp_utc(None)

    with pytest.raises(TypeError, match="Binary floating-point"):
        parse_timestamp_utc(1234567.89)

    with pytest.raises(ValueError, match="cannot be empty"):
        parse_timestamp_utc("   ")


# 3. Tests for normalize_unit and normalize_unit_and_value
def test_normalize_unit_standard_mappings() -> None:
    """Map standard variations of units to canonical tokens."""
    for variant in ["USD", "usd", "US$", "dollar", "dollars", "$"]:
        assert normalize_unit(variant) == "USD"

    for variant in ["shares", "Shares", "share", "shs"]:
        assert normalize_unit(variant) == "shares"

    for variant in ["pure", "scalar", "dimensionless", "none", "count"]:
        assert normalize_unit(variant) == "pure"

    for variant in ["%", "percent", "pct", "percentage", "ratio"]:
        assert normalize_unit(variant) == "ratio"


def test_normalize_unit_unknown_and_strict() -> None:
    """Preserve unknown unit in non-strict mode; raise in strict mode."""
    assert normalize_unit("barrel") == "barrel"
    assert normalize_unit(None) is None
    assert normalize_unit("   ") is None

    with pytest.raises(ValueError, match="Unrecognized unit"):
        normalize_unit("barrel", strict=True)


def test_normalize_unit_and_value_percentage_scaling() -> None:
    """Scale percentage values by 1/100 and set unit to ratio."""
    val, unit = normalize_unit_and_value(Decimal("15.5"), "%")
    assert val == Decimal("0.155")
    assert unit == "ratio"

    val2, unit2 = normalize_unit_and_value(Decimal("100"), "percent")
    assert val2 == Decimal("1")
    assert unit2 == "ratio"

    val_none, unit_pct = normalize_unit_and_value(None, "pct")
    assert val_none is None
    assert unit_pct == "ratio"


def test_normalize_unit_and_value_non_percentage() -> None:
    """Non-percentage units preserve numeric value."""
    val, unit = normalize_unit_and_value(Decimal("182.52"), "USD")
    assert val == Decimal("182.52")
    assert unit == "USD"

    val_no_unit, unit_none = normalize_unit_and_value(Decimal("42"), None)
    assert val_no_unit == Decimal("42")
    assert unit_none is None


# 4. Tests for normalize_identifier
def test_normalize_identifier_cik() -> None:
    """Zero-pad CIK to exactly 10 digits."""
    assert normalize_identifier("320193", id_type="cik") == "0000320193"
    assert normalize_identifier("0000320193", id_type="cik") == "0000320193"
    assert normalize_identifier(" CIK 320193 ", id_type=EntityIdType.CIK) == "0000320193"


def test_normalize_identifier_ticker_and_codes() -> None:
    """Convert ticker, ISIN, and CUSIP to uppercase stripped strings."""
    assert normalize_identifier("  aapl  ", id_type="ticker") == "AAPL"
    assert normalize_identifier("us0378331005", id_type=EntityIdType.ISIN) == "US0378331005"
    assert normalize_identifier("037833100", id_type=EntityIdType.CUSIP) == "037833100"


def test_normalize_identifier_errors() -> None:
    """Empty identifiers and invalid CIKs raise ValueError."""
    with pytest.raises(ValueError, match="cannot be empty"):
        normalize_identifier("   ")

    with pytest.raises(ValueError, match="no digits found"):
        normalize_identifier("NO_DIGITS", id_type="cik")


# 5. Tests for compute_row_hash
def test_compute_row_hash_determinism_and_delimiter_safety() -> None:
    """Row hash is deterministic and resists delimiter collision."""
    now = datetime.datetime(2024, 1, 15, 14, 30, 0, 123456, tzinfo=timezone.utc)
    h1 = compute_row_hash(
        entity_id="AAPL",
        field="price",
        event_time=now,
        value_num=Decimal("182.52"),
        value_str=None,
        unit="USD",
    )
    h2 = compute_row_hash(
        entity_id="AAPL",
        field="price",
        event_time=now,
        value_num=Decimal("182.52"),
        value_str=None,
        unit="USD",
    )
    assert h1 == h2
    assert len(h1) == 64

    # Delimiter collision resistance
    h_col1 = compute_row_hash(
        entity_id="A|B",
        field="C",
        event_time=now,
        value_num=None,
        value_str="val",
        unit="USD",
    )
    h_col2 = compute_row_hash(
        entity_id="A",
        field="B|C",
        event_time=now,
        value_num=None,
        value_str="val",
        unit="USD",
    )
    assert h_col1 != h_col2


def test_compute_row_hash_naive_datetime_raises() -> None:
    """Naive datetime raises ValueError."""
    naive = datetime.datetime(2024, 1, 15, 14, 30, 0)
    with pytest.raises(ValueError, match="must be timezone-aware"):
        compute_row_hash("AAPL", "price", naive, Decimal("100"), None, "USD")


# 6. Tests for CanonicalRow Pydantic model
def test_canonical_row_automatic_hash_and_validation() -> None:
    """CanonicalRow computes row_hash automatically and validates constraints."""
    now = datetime.datetime(2024, 1, 15, 14, 30, 0, tzinfo=timezone.utc)
    row = CanonicalRow(
        entity_id="0000320193",
        entity_id_type="cik",
        field="price",
        event_time=now,
        knowledge_time=now,
        value_num=Decimal("182.52"),
        unit="USD",
        drop_id="0123456789abcdef",
        source_file="data.csv",
        source_row=1,
    )
    assert len(row.row_hash) == 64
    assert not row.unmapped


def test_canonical_row_rejects_float_value_num() -> None:
    """CanonicalRow rejects float value_num inputs via field validator."""
    now = datetime.datetime(2024, 1, 15, 14, 30, 0, tzinfo=timezone.utc)
    with pytest.raises(TypeError, match="Binary floating-point value"):
        CanonicalRow(
            entity_id="0000320193",
            entity_id_type="cik",
            field="price",
            event_time=now,
            knowledge_time=now,
            value_num=182.52,  # type: ignore
            drop_id="0123456789abcdef",
            source_file="data.csv",
            source_row=1,
        )


def test_canonical_row_hash_mismatch_raises() -> None:
    """CanonicalRow raises if provided row_hash conflicts with calculated hash."""
    now = datetime.datetime(2024, 1, 15, 14, 30, 0, tzinfo=timezone.utc)
    with pytest.raises(ValidationError, match="row_hash mismatch"):
        CanonicalRow(
            entity_id="0000320193",
            entity_id_type="cik",
            field="price",
            event_time=now,
            knowledge_time=now,
            value_num=Decimal("182.52"),
            drop_id="0123456789abcdef",
            source_file="data.csv",
            source_row=1,
            row_hash="0" * 64,
        )


# 7. Tests for CANONICAL_ARROW_SCHEMA
def test_canonical_arrow_schema_structure() -> None:
    """Canonical Arrow schema contains exactly 13 fields with matching types."""
    assert len(CANONICAL_ARROW_SCHEMA) == 13
    assert CANONICAL_ARROW_SCHEMA.field("entity_id").type == pa.string()
    assert CANONICAL_ARROW_SCHEMA.field("value_num").type == pa.decimal128(38, 9)
    assert CANONICAL_ARROW_SCHEMA.field("unmapped").type == pa.bool_()
    assert not CANONICAL_ARROW_SCHEMA.field("entity_id").nullable
    assert CANONICAL_ARROW_SCHEMA.field("value_num").nullable


# 8. Tests for normalize_drop
def test_normalize_drop_baseline_csv(tmp_path: Path) -> None:
    """Normalize baseline CSV drop with symbol resolution."""
    manifest_yaml = """
vendor: test_vendor
entity_columns: [entity_id]
entity_id_type: ticker
event_time_column: event_time
event_time_format: "%Y-%m-%dT%H:%M:%SZ"
knowledge_time:
  source: drop_time
timezone: UTC
value_columns: [price]
rules: null
"""
    manifest = VendorManifest.from_yaml(manifest_yaml)
    sm_path = build_symbol_master(
        "tests/fixtures/symbols/company_tickers.json", output_dir=tmp_path
    )
    sm = SymbolMaster.load(sm_path)

    table = normalize_drop(
        drop_dir="tests/fixtures/drops/baseline_csv",
        manifest=manifest,
        drop_id="0123456789abcdef",
        symbol_master=sm,
    )

    assert len(table) == 10
    assert table.schema == CANONICAL_ARROW_SCHEMA

    # Check that AAPL mapped to 0000320193
    rows = table.to_pylist()
    aapl_row = rows[0]
    assert aapl_row["entity_id"] == "0000320193"
    assert aapl_row["field"] == "price"
    assert aapl_row["value_num"] == Decimal("182.520000000")
    assert not aapl_row["unmapped"]

    # Check unmapped row: row 6 had entity_id "0000320193" when id_type was ticker
    c_row = rows[4]  # row 5 in 0-indexed list
    assert c_row["entity_id"] == "0000320193"
    assert c_row["unmapped"]


def test_parse_decimal_exact_bytes() -> None:
    """Parse UTF-8 encoded bytes to exact Decimal."""
    assert parse_decimal_exact(b"182.52") == Decimal("182.52")
    assert parse_decimal_exact(b"  -99.9  ") == Decimal("-99.9")


def test_parse_timestamp_utc_dateutil_fallback() -> None:
    """Non-ISO formatted timestamps fall back to dateutil parser."""
    utc_dt = parse_timestamp_utc("Jan 15, 2024 14:30:00", source_tz="America/New_York")
    assert utc_dt.tzinfo == timezone.utc
    assert utc_dt.year == 2024
    assert utc_dt.month == 1
    assert utc_dt.day == 15
    assert utc_dt.hour == 19
    assert utc_dt.minute == 30


def test_canonical_row_matching_precomputed_hash() -> None:
    """Providing the identical expected row_hash succeeds without error."""
    now = datetime.datetime(2024, 1, 15, 14, 30, 0, tzinfo=timezone.utc)
    expected_hash = compute_row_hash(
        entity_id="AAPL",
        field="price",
        event_time=now,
        value_num=Decimal("182.52"),
        value_str=None,
        unit="USD",
    )
    row = CanonicalRow(
        entity_id="AAPL",
        entity_id_type="ticker",
        field="price",
        event_time=now,
        knowledge_time=now,
        value_num=Decimal("182.52"),
        unit="USD",
        drop_id="0123456789abcdef",
        source_file="data.csv",
        source_row=1,
        row_hash=expected_hash,
    )
    assert row.row_hash == expected_hash


def test_normalize_drop_jsonl_and_jsonl_gz() -> None:
    """Normalize plain and gzipped JSON Lines drops."""
    manifest_yaml = """
vendor: test_vendor
entity_columns: [entity_id]
entity_id_type: ticker
event_time_column: event_time
event_time_format: "%Y-%m-%dT%H:%M:%SZ"
knowledge_time:
  source: drop_time
timezone: UTC
value_columns: [price]
rules: null
"""
    manifest = VendorManifest.from_yaml(manifest_yaml)

    table_plain = normalize_drop(
        drop_dir="tests/fixtures/drops/baseline_jsonl",
        manifest=manifest,
        drop_id="0123456789abcdef",
        symbol_master=None,
    )
    assert len(table_plain) == 10
    assert table_plain.schema == CANONICAL_ARROW_SCHEMA

    table_gz = normalize_drop(
        drop_dir="tests/fixtures/drops/baseline_jsonl_gz",
        manifest=manifest,
        drop_id="0123456789abcdef",
        symbol_master=None,
    )
    assert len(table_gz) == 10
    assert table_gz.schema == CANONICAL_ARROW_SCHEMA


def test_normalize_drop_parquet(tmp_path: Path) -> None:
    """Normalize Parquet drop batches."""
    manifest_yaml = """
vendor: test_vendor
entity_columns: [entity_id]
entity_id_type: ticker
event_time_column: event_time
event_time_format: "%Y-%m-%dT%H:%M:%SZ"
knowledge_time:
  source: drop_time
timezone: UTC
value_columns: [price]
rules: null
"""
    manifest = VendorManifest.from_yaml(manifest_yaml)

    table = normalize_drop(
        drop_dir="tests/fixtures/drops/baseline_parquet",
        manifest=manifest,
        drop_id="0123456789abcdef",
        symbol_master=None,
    )
    assert len(table) == 10
    assert table.schema == CANONICAL_ARROW_SCHEMA


def test_normalize_drop_with_field_columns_and_string_values() -> None:
    """Normalize drop with field columns, multiple value columns, and string metrics."""
    manifest_yaml = """
vendor: test_vendor
entity_columns: [entity_id]
entity_id_type: ticker
event_time_column: event_time
event_time_format: "%Y-%m-%dT%H:%M:%SZ"
knowledge_time:
  source: column
  column: trade_date
  format: "%Y-%m-%d"
timezone: UTC
field_columns: [status]
value_columns: [price, notes]
rules: null
"""
    manifest = VendorManifest.from_yaml(manifest_yaml)

    table = normalize_drop(
        drop_dir="tests/fixtures/drops/baseline_csv",
        manifest=manifest,
        drop_id="0123456789abcdef",
        symbol_master=None,
    )
    # 10 rows x 2 value columns = 20 canonical records
    assert len(table) == 20
    rows = table.to_pylist()
    price_rows = [r for r in rows if r["value_num"] is not None]
    notes_rows = [r for r in rows if r["value_str"] is not None]
    assert len(price_rows) == 10
    assert len(notes_rows) > 0
