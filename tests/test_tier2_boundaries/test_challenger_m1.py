"""
Tier 2 Boundary Tests: Adversarial Schema Drift, Inference Extremes, and Pinning Contract.

Challenger 2 suite for Milestone 1:
1. Adversarial Schema Drift:
   - Case change in columns (`id` -> `Id`, `ticker` -> `TICKER`) -> strictly BREAKING (CASE_MISMATCH).
   - Type narrowing (`int64` -> `int32`, `float64` -> `int64`, `timestamp_tz` -> `timestamp`, etc.) -> strictly BREAKING (TYPE_INCOMPATIBLE).
   - Non-null violated with string nulls (`""`, `"N/A"`, `None`, `"-"`, etc.) -> strictly BREAKING (NULLABILITY_VIOLATION).
   - Safe widening (`int32` -> `int64`, `float32` -> `float64`, `date` -> `timestamp_tz`, etc.) -> strictly WARNING (TYPE_WIDENING).
   - New columns added -> WARNING (NEW_COLUMN); removed required columns -> BREAKING (MISSING_COLUMN).
2. Schema inference on:
   - Empty tables (header-only CSV, empty Parquet, 0-byte files, empty JSON).
   - Single-row tables (CSV, Parquet).
   - 100% null columns (empty strings, N/A, None, dashes).
3. Primary key candidacy under duplicate insertions:
   - Clean candidacy for unique single columns.
   - Immediate exclusion from primary key candidates upon duplicate insertion.
   - Detection of duplicate primary key values in drift detection.
4. Round-trip pinning and loading of complex schemas:
   - Full schema with all 14 DataType enum members, composite PK, nested metadata, categories.
   - Pinned to disk, reloaded, zero drift verified, idempotent serialization.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from vendor_onboarding.ingestion.infer import infer_schema
from vendor_onboarding.models.schema import (
    ColumnDefinition,
    DataType,
    DriftSeverity,
    DriftType,
    SchemaDefinition,
    detect_drift,
    load_schema,
    pin_schema,
)

pytestmark = [pytest.mark.tier2]


# ============================================================================
# 1. Adversarial Schema Drift Tests
# ============================================================================


def test_adversarial_casing_changes_are_breaking() -> None:
    """Column casing mutations ('id' -> 'Id', 'ticker' -> 'TICKER') must be BREAKING with CASE_MISMATCH."""
    pinned = SchemaDefinition(
        dataset_id="FINANCIAL_SERIES",
        vendor_id="VENDOR_MARKET",
        columns={
            "id": ColumnDefinition(name="id", data_type=DataType.STRING, nullable=False),
            "ticker": ColumnDefinition(name="ticker", data_type=DataType.STRING, nullable=False),
            "price": ColumnDefinition(name="price", data_type=DataType.FLOAT64, nullable=False),
        },
        primary_key=["id"],
    )

    current = SchemaDefinition(
        dataset_id="FINANCIAL_SERIES",
        vendor_id="VENDOR_MARKET",
        columns={
            "Id": ColumnDefinition(name="Id", data_type=DataType.STRING, nullable=False),
            "TICKER": ColumnDefinition(name="TICKER", data_type=DataType.STRING, nullable=False),
            "price": ColumnDefinition(name="price", data_type=DataType.FLOAT64, nullable=False),
        },
        primary_key=["Id"],
    )

    report = detect_drift(current, pinned)
    assert report.is_breaking is True
    case_issues = [i for i in report.issues if i.drift_type == DriftType.CASE_MISMATCH]
    assert len(case_issues) == 2
    for issue in case_issues:
        assert issue.severity == DriftSeverity.BREAKING
    assert {i.column_name for i in case_issues} == {"id", "ticker"}
    # Verify no spurious NEW_COLUMN or MISSING_COLUMN was emitted for these columns
    non_case_drift = [i for i in report.issues if i.column_name in ("id", "ticker", "Id", "TICKER") and i.drift_type != DriftType.CASE_MISMATCH]
    assert len(non_case_drift) == 0


@pytest.mark.parametrize(
    ("pinned_type", "current_type"),
    [
        (DataType.INT64, DataType.INT32),
        (DataType.FLOAT64, DataType.INT64),
        (DataType.TIMESTAMP_TZ, DataType.TIMESTAMP),
        (DataType.INT32, DataType.INT16),
        (DataType.INT16, DataType.INT8),
        (DataType.FLOAT64, DataType.FLOAT32),
        (DataType.DECIMAL, DataType.INT64),
        (DataType.STRING, DataType.CATEGORICAL),
        (DataType.TIMESTAMP, DataType.DATE),
    ],
)
def test_adversarial_type_narrowing_is_breaking(
    pinned_type: DataType, current_type: DataType
) -> None:
    """Type narrowing destroys data range or precision and must strictly produce BREAKING drift."""
    pinned = SchemaDefinition(
        dataset_id="PRICE_DATA",
        vendor_id="VENDOR_X",
        columns={"metric": ColumnDefinition(name="metric", data_type=pinned_type, nullable=True)},
    )
    current = SchemaDefinition(
        dataset_id="PRICE_DATA",
        vendor_id="VENDOR_X",
        columns={"metric": ColumnDefinition(name="metric", data_type=current_type, nullable=True)},
    )

    report = detect_drift(current, pinned)
    assert report.is_breaking is True
    assert report.breaking_count == 1
    issue = report.issues[0]
    assert issue.drift_type == DriftType.TYPE_INCOMPATIBLE
    assert issue.severity == DriftSeverity.BREAKING
    assert issue.expected == pinned_type.value
    assert issue.actual == current_type.value


@pytest.mark.parametrize("null_str", ["", "N/A", "None", "-", "null", "NULL", "NA", "#N/A", "NaN", "nan"])
def test_adversarial_non_null_violation_with_string_nulls(
    tmp_path: Path, null_str: str
) -> None:
    """Incoming drop containing common string representations of nulls in non-nullable column must be BREAKING."""
    pinned = SchemaDefinition(
        dataset_id="VENDOR_DATA",
        vendor_id="VENDOR_A",
        columns={
            "id": ColumnDefinition(name="id", data_type=DataType.INT64, nullable=False),
            "code": ColumnDefinition(name="code", data_type=DataType.STRING, nullable=False),
        },
        primary_key=["id"],
    )

    csv_file = tmp_path / f"null_{hash(null_str)}.csv"
    csv_file.write_text(f"id,code\n1,VALID\n2,{null_str}\n3,OK\n", encoding="utf-8")

    inferred = infer_schema(csv_file)
    assert inferred.columns["code"].null_count is not None and inferred.columns["code"].null_count > 0
    assert inferred.columns["code"].nullable is True

    report = detect_drift(inferred, pinned)
    assert report.is_breaking is True
    null_issues = [
        i for i in report.issues
        if i.drift_type == DriftType.NULLABILITY_VIOLATION and i.column_name == "code"
    ]
    assert len(null_issues) == 1
    assert null_issues[0].severity == DriftSeverity.BREAKING


@pytest.mark.parametrize(
    ("pinned_type", "current_type"),
    [
        (DataType.INT32, DataType.INT64),
        (DataType.FLOAT32, DataType.FLOAT64),
        (DataType.DATE, DataType.TIMESTAMP_TZ),
        (DataType.INT8, DataType.INT16),
        (DataType.INT16, DataType.INT32),
        (DataType.INT32, DataType.DECIMAL),
        (DataType.CATEGORICAL, DataType.STRING),
        (DataType.DATE, DataType.TIMESTAMP),
        (DataType.TIMESTAMP, DataType.TIMESTAMP_TZ),
    ],
)
def test_adversarial_safe_widening_is_warning(
    pinned_type: DataType, current_type: DataType
) -> None:
    """Safe widening transitions preserve semantics and must strictly be classified as WARNING, not BREAKING."""
    pinned = SchemaDefinition(
        dataset_id="DATASET",
        vendor_id="VENDOR",
        columns={"col": ColumnDefinition(name="col", data_type=pinned_type, nullable=True)},
    )
    current = SchemaDefinition(
        dataset_id="DATASET",
        vendor_id="VENDOR",
        columns={"col": ColumnDefinition(name="col", data_type=current_type, nullable=True)},
    )

    report = detect_drift(current, pinned)
    assert report.is_breaking is False
    assert report.warning_count == 1
    issue = report.issues[0]
    assert issue.drift_type == DriftType.TYPE_WIDENING
    assert issue.severity == DriftSeverity.WARNING


def test_adversarial_column_addition_and_removal() -> None:
    """New column added -> WARNING; required column removed -> BREAKING."""
    pinned = SchemaDefinition(
        dataset_id="CORE_FEED",
        vendor_id="VENDOR_B",
        columns={
            "req1": ColumnDefinition(name="req1", data_type=DataType.INT64, nullable=False),
            "req2": ColumnDefinition(name="req2", data_type=DataType.STRING, nullable=False),
            "opt": ColumnDefinition(name="opt", data_type=DataType.FLOAT64, nullable=True),
        },
        primary_key=["req1"],
    )

    # Added column
    added = SchemaDefinition(
        dataset_id="CORE_FEED",
        vendor_id="VENDOR_B",
        columns={
            "req1": ColumnDefinition(name="req1", data_type=DataType.INT64, nullable=False),
            "req2": ColumnDefinition(name="req2", data_type=DataType.STRING, nullable=False),
            "opt": ColumnDefinition(name="opt", data_type=DataType.FLOAT64, nullable=True),
            "bonus_metric": ColumnDefinition(name="bonus_metric", data_type=DataType.FLOAT64, nullable=True),
        },
        primary_key=["req1"],
    )
    rep_add = detect_drift(added, pinned)
    assert rep_add.is_breaking is False
    assert rep_add.warning_count == 1
    assert rep_add.issues[0].drift_type == DriftType.NEW_COLUMN
    assert rep_add.issues[0].column_name == "bonus_metric"

    # Removed column (req2 dropped)
    removed = SchemaDefinition(
        dataset_id="CORE_FEED",
        vendor_id="VENDOR_B",
        columns={
            "req1": ColumnDefinition(name="req1", data_type=DataType.INT64, nullable=False),
            "opt": ColumnDefinition(name="opt", data_type=DataType.FLOAT64, nullable=True),
        },
        primary_key=["req1"],
    )
    rep_rem = detect_drift(removed, pinned)
    assert rep_rem.is_breaking is True
    assert any(
        i.drift_type == DriftType.MISSING_COLUMN
        and i.severity == DriftSeverity.BREAKING
        and i.column_name == "req2"
        for i in rep_rem.issues
    )


# ============================================================================
# 2. Schema Inference on Edge Cases (Empty, Single-Row, 100% Nulls)
# ============================================================================


def test_schema_inference_empty_header_csv(tmp_path: Path) -> None:
    """Header-only CSV (0 rows) profiles columns accurately without nulls or primary key candidates."""
    csv_file = tmp_path / "empty_header.csv"
    csv_file.write_text("entity_id,account_name,balance\n", encoding="utf-8")

    schema = infer_schema(csv_file)
    assert schema.total_rows_sampled == 0
    assert len(schema.columns) == 3
    assert set(schema.columns.keys()) == {"entity_id", "account_name", "balance"}
    assert all(c.null_count == 0 for c in schema.columns.values())
    assert all(c.is_primary_key_candidate is False for c in schema.columns.values())
    assert schema.primary_key_candidates == []


def test_schema_inference_zero_byte_file(tmp_path: Path) -> None:
    """Zero-byte files produce a schema with 0 columns and 0 rows without phantom columns."""
    zero_file = tmp_path / "zero_byte.csv"
    zero_file.write_bytes(b"")

    schema = infer_schema(zero_file)
    assert schema.total_rows_sampled == 0
    assert len(schema.columns) == 0


def test_schema_inference_empty_parquet(tmp_path: Path) -> None:
    """Parquet file with typed schema but 0 rows preserves exact column types and 0 sampled rows."""
    pq_path = tmp_path / "empty.parquet"
    arrow_schema = pa.schema([
        pa.field("sec_id", pa.int64()),
        pa.field("vol", pa.float64()),
        pa.field("as_of", pa.timestamp("us")),
    ])
    tbl = pa.Table.from_arrays(
        [pa.array([], type=pa.int64()), pa.array([], type=pa.float64()), pa.array([], type=pa.timestamp("us"))],
        schema=arrow_schema,
    )
    pq.write_table(tbl, pq_path)

    schema = infer_schema(pq_path)
    assert schema.total_rows_sampled == 0
    assert len(schema.columns) == 3
    assert schema.columns["sec_id"].data_type == DataType.INT64
    assert schema.columns["vol"].data_type == DataType.FLOAT64
    assert schema.columns["as_of"].data_type == DataType.TIMESTAMP
    assert all(c.is_primary_key_candidate is False for c in schema.columns.values())


def test_schema_inference_single_row_tables(tmp_path: Path) -> None:
    """Single-row table correctly identifies unique integer/string columns as PK candidates."""
    csv_path = tmp_path / "single_row.csv"
    csv_path.write_text("id,symbol,ratio\n501,NVDA,1.85\n", encoding="utf-8")

    schema = infer_schema(csv_path)
    assert schema.total_rows_sampled == 1
    assert schema.columns["id"].is_primary_key_candidate is True
    assert schema.columns["symbol"].is_primary_key_candidate is True
    assert schema.columns["ratio"].is_primary_key_candidate is False  # float excluded from PK candidate
    assert "id" in schema.primary_key_candidates


def test_schema_inference_100pct_null_columns(tmp_path: Path) -> None:
    """100% null columns correctly report null_percentage=1.0, nullable=True, and is_pk_candidate=False."""
    csv_path = tmp_path / "all_nulls.csv"
    csv_path.write_text("id,empty_col,na_col,mixed_nulls\n1,,NA,None\n2,,NA,-\n3,,NA,null\n", encoding="utf-8")

    schema = infer_schema(csv_path)
    assert schema.total_rows_sampled == 3
    for col_name in ["empty_col", "na_col", "mixed_nulls"]:
        col = schema.columns[col_name]
        assert col.null_count == 3
        assert col.null_percentage == 1.0
        assert col.nullable is True
        assert col.is_primary_key_candidate is False


# ============================================================================
# 3. Primary Key Candidacy Under Duplicate Insertions
# ============================================================================


def test_primary_key_candidacy_under_duplicate_insertions(tmp_path: Path) -> None:
    """Duplicate values disqualify a column from PK candidacy and cause BREAKING drift if pinned."""
    # 1. Unique table: 'id' is candidate
    unique_csv = tmp_path / "unique.csv"
    unique_csv.write_text("id,name\n10,Alpha\n20,Beta\n30,Gamma\n", encoding="utf-8")
    sch_uniq = infer_schema(unique_csv)
    assert sch_uniq.columns["id"].is_primary_key_candidate is True
    assert "id" in sch_uniq.primary_key_candidates

    # 2. Duplicate table: duplicate '10' inserted
    dup_csv = tmp_path / "dup.csv"
    dup_csv.write_text("id,name\n10,Alpha\n20,Beta\n10,Gamma\n", encoding="utf-8")
    sch_dup = infer_schema(dup_csv)
    assert sch_dup.columns["id"].is_primary_key_candidate is False
    assert "id" not in sch_dup.primary_key_candidates
    assert sch_dup.columns["id"].distinct_count == 2
    assert sch_dup.total_rows_sampled == 3

    # 3. Pinned PK with duplicates in incoming schema causes BREAKING PRIMARY_KEY_VIOLATION
    pinned = SchemaDefinition(
        dataset_id="FEED",
        vendor_id="VENDOR",
        columns={"id": ColumnDefinition(name="id", data_type=DataType.INT64, nullable=False)},
        primary_key=["id"],
    )
    dup_schema = SchemaDefinition(
        dataset_id="FEED",
        vendor_id="VENDOR",
        total_rows_sampled=3,
        columns={"id": ColumnDefinition(name="id", data_type=DataType.INT64, nullable=False, distinct_count=2, null_count=0)},
        primary_key=["id"],
    )
    report = detect_drift(dup_schema, pinned)
    assert report.is_breaking is True
    pk_violations = [i for i in report.issues if i.drift_type == DriftType.PRIMARY_KEY_VIOLATION]
    assert len(pk_violations) == 1
    assert pk_violations[0].severity == DriftSeverity.BREAKING
    assert "duplicate values" in pk_violations[0].message


# ============================================================================
# 4. Round-Trip Pinning and Loading of Complex Schemas
# ============================================================================


def test_complex_schema_pin_and_load_roundtrip(tmp_path: Path) -> None:
    """Complex enterprise schema with all 14 DataType enum values round-trips with zero drift."""
    complex_columns = {
        "int8_c": ColumnDefinition(name="int8_c", data_type=DataType.INT8, physical_type="TINYINT", nullable=False),
        "int16_c": ColumnDefinition(name="int16_c", data_type=DataType.INT16, physical_type="SMALLINT", nullable=True),
        "int32_c": ColumnDefinition(name="int32_c", data_type=DataType.INT32, physical_type="INTEGER", nullable=False, is_primary_key_candidate=True),
        "int64_c": ColumnDefinition(name="int64_c", data_type=DataType.INT64, physical_type="BIGINT", nullable=False, is_primary_key_candidate=True),
        "float32_c": ColumnDefinition(name="float32_c", data_type=DataType.FLOAT32, physical_type="FLOAT", nullable=True),
        "float64_c": ColumnDefinition(name="float64_c", data_type=DataType.FLOAT64, physical_type="DOUBLE", nullable=True),
        "decimal_c": ColumnDefinition(name="decimal_c", data_type=DataType.DECIMAL, physical_type="DECIMAL(18,4)", nullable=True),
        "bool_c": ColumnDefinition(name="bool_c", data_type=DataType.BOOLEAN, physical_type="BOOLEAN", nullable=False),
        "str_c": ColumnDefinition(name="str_c", data_type=DataType.STRING, physical_type="VARCHAR", nullable=True),
        "date_c": ColumnDefinition(name="date_c", data_type=DataType.DATE, physical_type="DATE", nullable=False, format="%Y-%m-%d"),
        "ts_c": ColumnDefinition(name="ts_c", data_type=DataType.TIMESTAMP, physical_type="TIMESTAMP", nullable=False, format="%Y-%m-%dT%H:%M:%SZ"),
        "tstz_c": ColumnDefinition(name="tstz_c", data_type=DataType.TIMESTAMP_TZ, physical_type="TIMESTAMPTZ", nullable=False, timezone="UTC", format="%Y-%m-%dT%H:%M:%SZ"),
        "cat_c": ColumnDefinition(name="cat_c", data_type=DataType.CATEGORICAL, physical_type="VARCHAR", nullable=False, is_categorical=True, categories=["BUY", "HOLD", "SELL"]),
        "json_c": ColumnDefinition(name="json_c", data_type=DataType.JSON, physical_type="JSON", nullable=True, description="Audit attributes"),
    }

    schema = SchemaDefinition(
        schema_version="2.0.0",
        dataset_id="ENTERPRISE_GLOBAL_EQUITY",
        vendor_id="VENDOR_REFINITIV",
        file_format="PARQUET",
        total_rows_sampled=500000,
        columns=complex_columns,
        primary_key=["int64_c", "date_c"],
        primary_key_candidates=["int32_c", "int64_c"],
        metadata={"retention_days": 365, "data_tier": "gold", "tags": ["trading", "realtime"]},
    )

    pin_file = tmp_path / "pinned_schema.json"
    pin_schema(schema, pin_file)
    assert pin_file.is_file()

    loaded = load_schema(pin_file)

    # Full structural assertion
    assert loaded.schema_version == schema.schema_version
    assert loaded.dataset_id == schema.dataset_id
    assert loaded.vendor_id == schema.vendor_id
    assert loaded.file_format == schema.file_format
    assert loaded.total_rows_sampled == schema.total_rows_sampled
    assert loaded.primary_key == schema.primary_key
    assert loaded.primary_key_candidates == schema.primary_key_candidates
    assert loaded.metadata == schema.metadata
    assert len(loaded.columns) == 14

    for name, orig_col in schema.columns.items():
        loaded_col = loaded.columns[name]
        assert orig_col.data_type == loaded_col.data_type
        assert orig_col.nullable == loaded_col.nullable
        assert orig_col.format == loaded_col.format
        assert orig_col.timezone == loaded_col.timezone
        assert orig_col.categories == loaded_col.categories

    # Drift assertion: exactly zero drift
    drift = detect_drift(loaded, schema)
    assert drift.is_breaking is False
    assert drift.breaking_count == 0
    assert drift.warning_count == 0
    assert len(drift.issues) == 0

    # Idempotent re-serialization check
    assert json.loads(loaded.to_json()) == json.loads(schema.to_json())
