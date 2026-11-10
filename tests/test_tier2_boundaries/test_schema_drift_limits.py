"""
Tier 2 Boundary Tests: Schema Drift Limits, Extreme Widening, and Massive Columns.

Per TEST_INFRA.md and PROJECT.md §8.1 / F-SCH-01, F-SCH-02, F-SCH-03:
Tests schema drift boundary limits:
- Massive 1000-column wide tables with sparse nulls (inference, drift comparison, storage)
- Type narrowing breaking changes (float64 -> int32, int64 -> int32, string -> int32)
- Type widening backwards-compatible warnings (int32 -> int64, float32 -> float64)
- Complex types & nested structures (JSON/struct definitions and mutations)
- Primary key mutations (dropped PK columns, added PK columns, PK made nullable)
- Column order swapping (INFO drift in name-based systems, positional dangers)
- Case-sensitivity boundaries (case mismatch breaking changes, collisions)
- Pinned schema fixture verification (breaking vs warning fixtures from test fixtures)
- Reserved SQL keyword and special character column names
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

try:
    import duckdb
except ImportError:
    duckdb = None  # type: ignore

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    pa = None  # type: ignore
    pq = None  # type: ignore

from vendor_onboarding.models.schema import (
    ColumnDefinition,
    DataType,
    DriftSeverity,
    DriftType,
    SchemaDefinition,
    detect_drift,
)

pytestmark = [pytest.mark.tier2]


def _make_schema(
    cols: dict[str, ColumnDefinition],
    pk: list[str] | None = None,
    dataset_id: str = "TEST_DATASET",
    vendor_id: str = "TEST_VENDOR",
) -> SchemaDefinition:
    """Helper to instantiate SchemaDefinition."""
    return SchemaDefinition(
        schema_version="1.0.0",
        dataset_id=dataset_id,
        vendor_id=vendor_id,
        primary_key=pk or [],
        columns=cols,
    )


# ============================================================================
# 1. Safe Type Widening Rules
# ============================================================================

def test_safe_type_widening_rules() -> None:
    """Safe type widening (e.g. int32->int64, float32->float64) must be WARNING, not BREAKING."""
    pinned = _make_schema({
        "c_int": ColumnDefinition(name="c_int", data_type=DataType.INT32, nullable=False),
        "c_float": ColumnDefinition(name="c_float", data_type=DataType.FLOAT32, nullable=False),
        "c_date": ColumnDefinition(name="c_date", data_type=DataType.DATE, nullable=False),
        "c_cat": ColumnDefinition(name="c_cat", data_type=DataType.CATEGORICAL, nullable=False),
    })

    current = _make_schema({
        "c_int": ColumnDefinition(name="c_int", data_type=DataType.INT64, nullable=False),
        "c_float": ColumnDefinition(name="c_float", data_type=DataType.FLOAT64, nullable=False),
        "c_date": ColumnDefinition(name="c_date", data_type=DataType.TIMESTAMP, nullable=False),
        "c_cat": ColumnDefinition(name="c_cat", data_type=DataType.STRING, nullable=False),
    })

    report = detect_drift(current, pinned)
    assert report.is_breaking is False
    assert report.breaking_count == 0
    assert report.warning_count == 4
    for issue in report.issues:
        assert issue.severity == DriftSeverity.WARNING
        assert issue.drift_type == DriftType.TYPE_WIDENING


def test_type_widening_int8_to_int64() -> None:
    """Int8 widened to Int64 is backward-compatible widening (WARNING)."""
    pinned = _make_schema({"code": ColumnDefinition(name="code", data_type=DataType.INT8, nullable=True)})
    current = _make_schema({"code": ColumnDefinition(name="code", data_type=DataType.INT64, nullable=True)})

    report = detect_drift(current, pinned)
    assert report.is_breaking is False
    assert report.warning_count == 1
    assert report.issues[0].drift_type == DriftType.TYPE_WIDENING


def test_type_widening_float32_to_float64() -> None:
    """Float32 widened to Float64 is backward-compatible widening (WARNING)."""
    pinned = _make_schema({"metric": ColumnDefinition(name="metric", data_type=DataType.FLOAT32, nullable=True)})
    current = _make_schema({"metric": ColumnDefinition(name="metric", data_type=DataType.FLOAT64, nullable=True)})

    report = detect_drift(current, pinned)
    assert report.is_breaking is False
    assert report.warning_count == 1
    assert report.issues[0].drift_type == DriftType.TYPE_WIDENING


def test_type_widening_date_to_timestamp_tz() -> None:
    """Date widened to Timestamp with timezone is compatible widening."""
    pinned = _make_schema({"pub_date": ColumnDefinition(name="pub_date", data_type=DataType.DATE, nullable=True)})
    current = _make_schema({"pub_date": ColumnDefinition(name="pub_date", data_type=DataType.TIMESTAMP_TZ, nullable=True)})

    report = detect_drift(current, pinned)
    assert report.is_breaking is False
    assert report.warning_count == 1


# ============================================================================
# 2. Narrowing Type Changes are Breaking
# ============================================================================

def test_narrowing_type_changes_are_breaking() -> None:
    """Narrowing conversions (e.g. int64->int32, float64->float32) must be classified as BREAKING."""
    pinned = _make_schema({
        "c_bigint": ColumnDefinition(name="c_bigint", data_type=DataType.INT64, nullable=False),
        "c_double": ColumnDefinition(name="c_double", data_type=DataType.FLOAT64, nullable=False),
    })

    current = _make_schema({
        "c_bigint": ColumnDefinition(name="c_bigint", data_type=DataType.INT32, nullable=False),
        "c_double": ColumnDefinition(name="c_double", data_type=DataType.FLOAT32, nullable=False),
    })

    report = detect_drift(current, pinned)
    assert report.is_breaking is True
    assert report.breaking_count == 2
    for issue in report.issues:
        assert issue.severity == DriftSeverity.BREAKING
        assert issue.drift_type == DriftType.TYPE_INCOMPATIBLE


def test_narrowing_float64_to_int32_is_breaking() -> None:
    """Narrowing float64 to int32 destroys decimal precision and must be BREAKING."""
    pinned = _make_schema({"price": ColumnDefinition(name="price", data_type=DataType.FLOAT64, nullable=True)})
    current = _make_schema({"price": ColumnDefinition(name="price", data_type=DataType.INT32, nullable=True)})

    report = detect_drift(current, pinned)
    assert report.is_breaking is True
    assert any(i.drift_type == DriftType.TYPE_INCOMPATIBLE for i in report.issues)


def test_narrowing_timestamp_to_date_is_breaking() -> None:
    """Narrowing timestamp to date discards intraday time precision and is BREAKING."""
    pinned = _make_schema({"event_time": ColumnDefinition(name="event_time", data_type=DataType.TIMESTAMP, nullable=False)})
    current = _make_schema({"event_time": ColumnDefinition(name="event_time", data_type=DataType.DATE, nullable=False)})

    report = detect_drift(current, pinned)
    assert report.is_breaking is True
    assert report.issues[0].severity == DriftSeverity.BREAKING


def test_incompatible_type_changes_are_breaking() -> None:
    """Completely incompatible types (string -> int, double -> string) must be BREAKING."""
    pinned = _make_schema({
        "price": ColumnDefinition(name="price", data_type=DataType.FLOAT64, nullable=False),
        "ticker": ColumnDefinition(name="ticker", data_type=DataType.STRING, nullable=False),
    })

    current = _make_schema({
        "price": ColumnDefinition(name="price", data_type=DataType.STRING, nullable=False),
        "ticker": ColumnDefinition(name="ticker", data_type=DataType.INT32, nullable=False),
    })

    report = detect_drift(current, pinned)
    assert report.is_breaking is True
    assert report.breaking_count == 2
    for issue in report.issues:
        assert issue.severity == DriftSeverity.BREAKING
        assert issue.drift_type == DriftType.TYPE_INCOMPATIBLE


# ============================================================================
# 3. Nullability Violations
# ============================================================================

def test_nullability_violation_with_incoming_nulls() -> None:
    """Incoming null values on a column pinned as non-nullable must be BREAKING."""
    pinned = _make_schema({
        "entity_id": ColumnDefinition(name="entity_id", data_type=DataType.STRING, nullable=False),
    })

    current = _make_schema({
        "entity_id": ColumnDefinition(
            name="entity_id",
            data_type=DataType.STRING,
            nullable=True,
            null_count=5,
            null_percentage=0.05,
        ),
    })

    report = detect_drift(current, pinned)
    assert report.is_breaking is True
    assert report.breaking_count == 1
    issue = report.issues[0]
    assert issue.severity == DriftSeverity.BREAKING
    assert issue.drift_type == DriftType.NULLABILITY_VIOLATION
    assert "contains 5 null values" in issue.message


def test_nullable_column_made_strictly_non_nullable() -> None:
    """Changing a column from nullable=True to nullable=False is a constraint tightening (INFO/NULLABILITY_CHANGED)."""
    pinned = _make_schema({
        "notes": ColumnDefinition(name="notes", data_type=DataType.STRING, nullable=True),
    })
    current = _make_schema({
        "notes": ColumnDefinition(name="notes", data_type=DataType.STRING, nullable=False),
    })

    report = detect_drift(current, pinned)
    assert len(report.issues) >= 1
    assert any(i.drift_type in (DriftType.NULLABILITY_CHANGED, DriftType.NULLABILITY_VIOLATION) for i in report.issues)


# ============================================================================
# 4. Primary Key Mutation Limits
# ============================================================================

def test_primary_key_modified_or_made_nullable() -> None:
    """Altering primary key definition or making a PK column nullable must be BREAKING."""
    pinned = _make_schema(
        cols={
            "id": ColumnDefinition(name="id", data_type=DataType.INT64, nullable=False),
            "date": ColumnDefinition(name="date", data_type=DataType.DATE, nullable=False),
        },
        pk=["id", "date"],
    )

    current1 = _make_schema(
        cols={
            "id": ColumnDefinition(name="id", data_type=DataType.INT64, nullable=True),
            "date": ColumnDefinition(name="date", data_type=DataType.DATE, nullable=False),
        },
        pk=["id", "date"],
    )
    report1 = detect_drift(current1, pinned)
    assert report1.is_breaking is True
    pk_issues = [i for i in report1.issues if i.drift_type == DriftType.NULLABILITY_VIOLATION]
    assert len(pk_issues) == 1

    current2 = _make_schema(
        cols={
            "id": ColumnDefinition(name="id", data_type=DataType.INT64, nullable=False),
            "date": ColumnDefinition(name="date", data_type=DataType.DATE, nullable=False),
        },
        pk=["id"],
    )
    report2 = detect_drift(current2, pinned)
    assert report2.is_breaking is True
    assert any(i.drift_type == DriftType.PRIMARY_KEY_CHANGED for i in report2.issues)


def test_dropped_primary_key_column_is_breaking() -> None:
    """Dropping a primary key column from current schema is strictly BREAKING."""
    pinned = _make_schema(
        cols={
            "entity_id": ColumnDefinition(name="entity_id", data_type=DataType.STRING, nullable=False),
            "event_time": ColumnDefinition(name="event_time", data_type=DataType.TIMESTAMP, nullable=False),
            "val": ColumnDefinition(name="val", data_type=DataType.FLOAT64, nullable=True),
        },
        pk=["entity_id", "event_time"],
    )
    # current dropped entity_id completely
    current = _make_schema(
        cols={
            "event_time": ColumnDefinition(name="event_time", data_type=DataType.TIMESTAMP, nullable=False),
            "val": ColumnDefinition(name="val", data_type=DataType.FLOAT64, nullable=True),
        },
        pk=["event_time"],
    )

    report = detect_drift(current, pinned)
    assert report.is_breaking is True
    assert any(i.drift_type == DriftType.MISSING_COLUMN for i in report.issues)


def test_added_column_to_composite_primary_key_is_breaking() -> None:
    """Expanding primary key from (entity_id) to (entity_id, sub_account) alters entity resolution."""
    pinned = _make_schema(
        cols={
            "entity_id": ColumnDefinition(name="entity_id", data_type=DataType.STRING, nullable=False),
            "sub_id": ColumnDefinition(name="sub_id", data_type=DataType.STRING, nullable=False),
        },
        pk=["entity_id"],
    )
    current = _make_schema(
        cols={
            "entity_id": ColumnDefinition(name="entity_id", data_type=DataType.STRING, nullable=False),
            "sub_id": ColumnDefinition(name="sub_id", data_type=DataType.STRING, nullable=False),
        },
        pk=["entity_id", "sub_id"],
    )

    report = detect_drift(current, pinned)
    assert report.is_breaking is True
    assert any(i.drift_type == DriftType.PRIMARY_KEY_CHANGED for i in report.issues)


# ============================================================================
# 5. Column Ordering & Case Sensitivity
# ============================================================================

def test_column_name_casing_mismatch() -> None:
    """Column name casing mismatch (e.g. 'entity_id' vs 'Entity_Id') must be BREAKING."""
    pinned = _make_schema({
        "entity_id": ColumnDefinition(name="entity_id", data_type=DataType.STRING, nullable=False),
        "event_time": ColumnDefinition(name="event_time", data_type=DataType.TIMESTAMP, nullable=False),
    })

    current = _make_schema({
        "Entity_Id": ColumnDefinition(name="Entity_Id", data_type=DataType.STRING, nullable=False),
        "event_time": ColumnDefinition(name="event_time", data_type=DataType.TIMESTAMP, nullable=False),
    })

    report = detect_drift(current, pinned)
    assert report.is_breaking is True
    case_issues = [i for i in report.issues if i.drift_type == DriftType.CASE_MISMATCH]
    assert len(case_issues) == 1
    assert case_issues[0].severity == DriftSeverity.BREAKING
    assert case_issues[0].expected == "entity_id"
    assert case_issues[0].actual == "Entity_Id"


def test_column_reordering_detected_as_info() -> None:
    """Reordering of identical columns must produce an INFO issue."""
    pinned = _make_schema({
        "col_a": ColumnDefinition(name="col_a", data_type=DataType.INT32, nullable=False),
        "col_b": ColumnDefinition(name="col_b", data_type=DataType.INT32, nullable=False),
    })

    current = _make_schema({
        "col_b": ColumnDefinition(name="col_b", data_type=DataType.INT32, nullable=False),
        "col_a": ColumnDefinition(name="col_a", data_type=DataType.INT32, nullable=False),
    })

    report = detect_drift(current, pinned)
    assert report.is_breaking is False
    assert report.breaking_count == 0
    assert report.warning_count == 0
    assert report.info_count == 1
    assert report.issues[0].drift_type == DriftType.COLUMN_REORDERED
    assert report.issues[0].severity == DriftSeverity.INFO


def test_swapped_column_order_in_parquet_resolved_by_name(tmp_path: Path):
    """Parquet columns can be read by name regardless of physical order in the file."""
    fpath = tmp_path / "swapped.parquet"
    # Write table with order: [col_b, col_a]
    t = pa.table({
        "col_b": [20, 21],
        "col_a": [10, 11],
    })
    pq.write_table(t, fpath)

    # Read specific column order by projection
    read_t = pq.read_table(fpath, columns=["col_a", "col_b"])
    assert read_t.column_names == ["col_a", "col_b"]
    assert read_t["col_a"].to_pylist() == [10, 11]
    assert read_t["col_b"].to_pylist() == [20, 21]


def test_swapped_column_order_in_csv_with_header_resolved_by_name(tmp_path: Path):
    """CSV with header row allows columnar readers to map columns by header name."""
    fpath = tmp_path / "swapped.csv"
    fpath.write_text("value,entity_id\n150.0,AAPL\n250.0,MSFT\n", encoding="utf-8")

    import pyarrow.csv as pcsv
    tab = pcsv.read_csv(fpath)
    assert tab.column_names == ["value", "entity_id"]
    assert tab["entity_id"].to_pylist() == ["AAPL", "MSFT"]


# ============================================================================
# 6. Categorical Domain Extensions
# ============================================================================

def test_new_categorical_values_detected_as_warning() -> None:
    """New categorical domain values detected must produce a WARNING."""
    pinned = _make_schema({
        "rating": ColumnDefinition(
            name="rating",
            data_type=DataType.CATEGORICAL,
            nullable=False,
            categories=["AAA", "AA", "A"],
        ),
    })

    current = _make_schema({
        "rating": ColumnDefinition(
            name="rating",
            data_type=DataType.CATEGORICAL,
            nullable=False,
            categories=["AAA", "AA", "A", "BBB"],
        ),
    })

    report = detect_drift(current, pinned)
    assert report.is_breaking is False
    assert report.warning_count == 1
    cat_issues = [i for i in report.issues if i.drift_type == DriftType.NEW_CATEGORICAL_VALUES]
    assert len(cat_issues) == 1
    assert cat_issues[0].severity == DriftSeverity.WARNING
    assert "BBB" in cat_issues[0].message


# ============================================================================
# 7. Massive 1,000-Column Wide Tables & Sparse Nulls
# ============================================================================

def test_massive_1000_column_schema_inference_and_drift():
    """Verify that a schema with 1,000 columns can be constructed, serialized, and diffed without bottleneck."""
    col_dict = {
        f"feature_{i:04d}": ColumnDefinition(
            name=f"feature_{i:04d}",
            data_type=DataType.FLOAT64 if i % 2 == 0 else DataType.INT32,
            nullable=True,
        )
        for i in range(1000)
    }

    pinned = _make_schema(col_dict, pk=["feature_0000"])
    assert len(pinned.columns) == 1000

    # Current schema alters 1 column (feature_0500 from INT32 to INT64) -> 1 WARNING
    col_dict_mod = dict(col_dict)
    col_dict_mod["feature_0501"] = ColumnDefinition(
        name="feature_0501",
        data_type=DataType.INT64,
        nullable=True,
    )
    current = _make_schema(col_dict_mod, pk=["feature_0000"])

    report = detect_drift(current, pinned)
    assert report.is_breaking is False
    assert report.warning_count == 1
    assert report.issues[0].column_name == "feature_0501"


def test_massive_1000_column_sparse_nulls_storage_and_query(tmp_path: Path):
    """Parquet and DuckDB handling of 1,000 columns where 99% of values are null."""
    num_cols = 1000
    num_rows = 10
    # Build 1000 sparse columns
    data = {}
    for i in range(num_cols):
        # 9 rows null, 1 row non-null
        vals = [None] * (num_rows - 1) + [float(i)]
        data[f"c_{i:04d}"] = pa.array(vals, type=pa.float64())

    table = pa.Table.from_arrays(list(data.values()), names=list(data.keys()))
    assert table.num_columns == 1000
    assert table.num_rows == 10

    pq_path = tmp_path / "sparse_1000.parquet"
    pq.write_table(table, pq_path)

    if duckdb:
        conn = duckdb.connect(":memory:")
        # Query non-null count of column c_0999
        res = conn.execute(f"SELECT count(c_0999), max(c_0999) FROM read_parquet('{pq_path}')").fetchall()[0]
        assert res[0] == 1
        assert res[1] == 999.0
        conn.close()


def test_massive_column_drift_with_100_missing_columns():
    """Detecting drift when 100 columns are dropped simultaneously."""
    col_dict = {f"col_{i:03d}": ColumnDefinition(name=f"col_{i:03d}", data_type=DataType.STRING) for i in range(500)}
    pinned = _make_schema(col_dict)

    # Current has only 400 columns (100 dropped)
    current_dict = {f"col_{i:03d}": col_dict[f"col_{i:03d}"] for i in range(400)}
    current = _make_schema(current_dict)

    report = detect_drift(current, pinned)
    assert report.is_breaking is True
    assert report.breaking_count == 100
    missing_cols = [i.column_name for i in report.issues if i.drift_type == DriftType.MISSING_COLUMN]
    assert len(missing_cols) == 100


# ============================================================================
# 8. Added Columns (INFO vs WARNING)
# ============================================================================

def test_added_nullable_column_classified_warning_or_info() -> None:
    """Adding a new column to incoming drop is detected with NEW_COLUMN drift."""
    pinned = _make_schema({
        "id": ColumnDefinition(name="id", data_type=DataType.INT64, nullable=False),
    })
    current = _make_schema({
        "id": ColumnDefinition(name="id", data_type=DataType.INT64, nullable=False),
        "new_metric": ColumnDefinition(name="new_metric", data_type=DataType.FLOAT64, nullable=True),
    })

    report = detect_drift(current, pinned)
    assert report.is_breaking is False
    assert any(i.drift_type == DriftType.NEW_COLUMN and i.column_name == "new_metric" for i in report.issues)


def test_added_non_nullable_column_without_default() -> None:
    """Adding a new required (non-nullable) column with incoming nulls or missing in past data."""
    pinned = _make_schema({
        "id": ColumnDefinition(name="id", data_type=DataType.INT64, nullable=False),
    })
    current = _make_schema({
        "id": ColumnDefinition(name="id", data_type=DataType.INT64, nullable=False),
        "required_flag": ColumnDefinition(name="required_flag", data_type=DataType.BOOLEAN, nullable=False),
    })

    report = detect_drift(current, pinned)
    assert any(i.column_name == "required_flag" for i in report.issues)


# ============================================================================
# 9. Complex Types: Structs, JSON, and Arrays
# ============================================================================

def test_nested_struct_schema_parsing():
    """Schemas containing nested JSON or struct fields."""
    col = ColumnDefinition(
        name="payload",
        data_type=DataType.JSON,
        nullable=True,
        description="Nested metadata struct",
    )
    assert col.data_type == DataType.JSON
    assert col.nullable is True


def test_nested_struct_subfield_removal_breaking_drift():
    """Altering a column from JSON structure to primitive String is an incompatible change."""
    pinned = _make_schema({"metadata": ColumnDefinition(name="metadata", data_type=DataType.JSON)})
    current = _make_schema({"metadata": ColumnDefinition(name="metadata", data_type=DataType.INT32)})

    report = detect_drift(current, pinned)
    assert report.is_breaking is True
    assert any(i.drift_type == DriftType.TYPE_INCOMPATIBLE for i in report.issues)


# ============================================================================
# 10. Real Bundled Fixture Schema Drifts
# ============================================================================

def test_schema_drift_breaking_fixture_detection(fixture_paths):
    """Verify that schema_drift_breaking.json against schema_baseline.json triggers BREAKING drift."""
    with open(fixture_paths.schema_baseline, "r", encoding="utf-8") as f:
        baseline_data = json.load(f)
    with open(fixture_paths.schema_drift_breaking, "r", encoding="utf-8") as f:
        breaking_data = json.load(f)

    pinned = SchemaDefinition.from_dict(baseline_data)
    current = SchemaDefinition.from_dict(breaking_data)

    report = detect_drift(current, pinned)
    assert report.is_breaking is True
    assert report.breaking_count >= 1


def test_schema_drift_warning_fixture_detection(fixture_paths):
    """Verify that schema_drift_warning.json against schema_baseline.json produces WARNING drift."""
    with open(fixture_paths.schema_baseline, "r", encoding="utf-8") as f:
        baseline_data = json.load(f)
    with open(fixture_paths.schema_drift_warning, "r", encoding="utf-8") as f:
        warning_data = json.load(f)

    pinned = SchemaDefinition.from_dict(baseline_data)
    current = SchemaDefinition.from_dict(warning_data)

    report = detect_drift(current, pinned)
    assert report.warning_count >= 1


# ============================================================================
# 11. Schema Serialization and Edge Cases
# ============================================================================

def test_empty_schema_no_columns():
    """Schema with 0 columns has empty column names list."""
    empty_s = _make_schema({})
    assert len(empty_s.column_names) == 0
    assert empty_s.to_dict()["columns"] == {}


def test_reserved_sql_keyword_column_names(temp_duckdb_conn):
    """Column names matching reserved SQL keywords (SELECT, WHERE, FROM, ORDER) handled with escaping."""
    cols = {
        "select": ColumnDefinition(name="select", data_type=DataType.INT32),
        "where": ColumnDefinition(name="where", data_type=DataType.STRING),
        "order": ColumnDefinition(name="order", data_type=DataType.FLOAT64),
    }
    schema = _make_schema(cols)
    assert "select" in schema.column_names

    # DuckDB SQL handles with double quotes
    temp_duckdb_conn.execute("""
        CREATE TABLE keywords ("select" INT, "where" VARCHAR, "order" DOUBLE);
        INSERT INTO keywords VALUES (1, 'NY', 15.5);
    """)
    res = temp_duckdb_conn.execute('SELECT "select", "where", "order" FROM keywords').fetchall()[0]
    assert res == (1, "NY", 15.5)


def test_special_characters_in_column_names():
    """Column names with spaces, percentages, slashes, or dots."""
    cols = {
        "Price ($)": ColumnDefinition(name="Price ($)", data_type=DataType.FLOAT64),
        "Growth %": ColumnDefinition(name="Growth %)", data_type=DataType.FLOAT64),
        "entity.dot.name": ColumnDefinition(name="entity.dot.name", data_type=DataType.STRING),
    }
    schema = _make_schema(cols)
    assert "Price ($)" in schema.column_names
    assert schema.get_column("Price ($)") is not None


def test_schema_json_roundtrip_idempotence():
    """Serializing schema to JSON and deserializing restores identical object."""
    cols = {
        "entity_id": ColumnDefinition(name="entity_id", data_type=DataType.STRING, nullable=False),
        "value": ColumnDefinition(name="value", data_type=DataType.FLOAT64, nullable=True),
    }
    s1 = _make_schema(cols, pk=["entity_id"])
    j_str = s1.to_json()

    s2 = SchemaDefinition.from_json(j_str)
    assert s1.dataset_id == s2.dataset_id
    assert s1.primary_key == s2.primary_key
    assert s1.columns["entity_id"].data_type == s2.columns["entity_id"].data_type


def test_type_widening_int32_to_decimal() -> None:
    """Widening Int32 to high-precision Decimal is allowed with WARNING."""
    pinned = _make_schema({"amount": ColumnDefinition(name="amount", data_type=DataType.INT32)})
    current = _make_schema({"amount": ColumnDefinition(name="amount", data_type=DataType.DECIMAL)})

    report = detect_drift(current, pinned)
    assert report.is_breaking is False
    assert report.warning_count == 1
    assert report.issues[0].drift_type == DriftType.TYPE_WIDENING


def test_case_sensitivity_duplicate_differing_only_by_case_in_csv(tmp_path: Path):
    """CSV containing two columns differing only in case (e.g. 'Value' and 'value') must be caught."""
    fpath = tmp_path / "case_dup.csv"
    fpath.write_text("entity_id,Value,value\nAAPL,100.0,200.0\n", encoding="utf-8")

    import pyarrow.csv as pcsv
    tab = pcsv.read_csv(fpath)
    # PyArrow preserves both names, but lowercasing detects collision
    lowered = [c.lower() for c in tab.column_names]
    assert len(lowered) != len(set(lowered)), "Duplicate column name collision under case-insensitivity"


def test_schema_hash_differs_on_column_type_change():
    """Modifying a column data type changes the serialized JSON representation."""
    import hashlib
    s1 = _make_schema({"price": ColumnDefinition(name="price", data_type=DataType.FLOAT64)})
    s2 = _make_schema({"price": ColumnDefinition(name="price", data_type=DataType.STRING)})

    h1 = hashlib.sha256(s1.to_json().encode("utf-8")).hexdigest()
    h2 = hashlib.sha256(s2.to_json().encode("utf-8")).hexdigest()
    assert h1 != h2


def test_swapped_column_order_without_headers_causes_type_inversion(tmp_path: Path):
    """When CSV lacks headers and is parsed by position, swapped columns result in type parse errors."""
    fpath = tmp_path / "no_header_swapped.csv"
    # Row expects (int_id, string_name), but file has (string_name, int_id)
    fpath.write_text("AAPL,12345\nMSFT,67890\n", encoding="utf-8")

    if duckdb:
        conn = duckdb.connect(":memory:")
        # Reading expecting col0 as INT and col1 as VARCHAR
        with pytest.raises(duckdb.Error):
            conn.execute(f"SELECT * FROM read_csv('{fpath}', header=false, columns={{'id': 'INT', 'name': 'VARCHAR'}})")
        conn.close()
