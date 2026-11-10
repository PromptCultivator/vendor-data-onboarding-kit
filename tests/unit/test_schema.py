"""Unit tests for schema inference, contract pinning, and drift detection."""

from __future__ import annotations

import gzip
from pathlib import Path

import pytest

from vdok.manifest import VendorManifest
from vdok.schema import (
    DriftCategory,
    DriftSeverity,
    InferredType,
    VendorSchema,
    accept_schema_changes,
    check_schema,
    compare_schemas,
    infer_schema,
    pin_schema,
)

FIXTURES_DIR = Path("tests/fixtures")


def test_schema_inference_baseline_all_formats() -> None:
    csv_schema = infer_schema(FIXTURES_DIR / "drops" / "baseline_csv", vendor="test_vendor")
    jsonl_schema = infer_schema(FIXTURES_DIR / "drops" / "baseline_jsonl", vendor="test_vendor")
    pq_schema = infer_schema(FIXTURES_DIR / "drops" / "baseline_parquet", vendor="test_vendor")

    expected_types = {
        "record_id": InferredType.INTEGER,
        "entity_id": InferredType.STRING,
        "price": InferredType.DECIMAL,
        "weight": InferredType.FLOAT,
        "status": InferredType.STRING,
        "trade_date": InferredType.DATE,
        "event_time": InferredType.DATETIME,
        "is_primary": InferredType.BOOLEAN,
        "notes": InferredType.STRING,
    }

    for col, exp_type in expected_types.items():
        assert csv_schema.columns[col].type == exp_type, f"CSV type mismatch for {col}"
        assert jsonl_schema.columns[col].type == exp_type, f"JSONL type mismatch for {col}"
        assert pq_schema.columns[col].type == exp_type, f"Parquet type mismatch for {col}"

    # Verify exact decimal enforcement on monetary field
    assert csv_schema.columns["price"].scale == 4
    assert csv_schema.columns["price"].precision is not None

    # Nullability assertions
    assert csv_schema.columns["notes"].nullable is True
    assert csv_schema.columns["record_id"].nullable is False


def test_schema_serialization_roundtrip(tmp_path: Path) -> None:
    schema = infer_schema(FIXTURES_DIR / "drops" / "baseline_csv", vendor="sample_vendor")
    target_file = tmp_path / "schemas" / "sample_vendor.yaml"
    schema.to_file(target_file)
    assert target_file.exists()

    loaded = VendorSchema.from_file(target_file)
    assert loaded.vendor == "sample_vendor"
    assert loaded.version == 1
    assert len(loaded.columns) == len(schema.columns)
    for col_name, col_spec in schema.columns.items():
        assert loaded.columns[col_name].type == col_spec.type
        assert loaded.columns[col_name].nullable == col_spec.nullable


def test_schema_pin_and_check(tmp_path: Path) -> None:
    schema_dir = tmp_path / "schemas"
    drop_path = FIXTURES_DIR / "drops" / "baseline_csv"

    pinned_path = pin_schema("vendor_a", drop_path=drop_path, schema_dir=schema_dir)
    assert pinned_path.exists()

    # Check identical drop is compliant
    diff = check_schema("vendor_a", drop_path=drop_path, schema_dir=schema_dir)
    assert diff.has_hold is False
    assert len(diff.differences) == 0


@pytest.mark.parametrize("fmt", ["csv", "jsonl", "parquet"])
def test_drift_added_column(fmt: str) -> None:
    base = infer_schema(FIXTURES_DIR / "drops" / f"baseline_{fmt}")
    drift = infer_schema(FIXTURES_DIR / "drift" / fmt / "added_column")

    diff = compare_schemas(base, drift)
    assert diff.has_hold is False
    added = [d for d in diff.differences if d.category == DriftCategory.ADDED_COLUMN]
    assert len(added) == 1
    assert added[0].column == "sector"
    assert added[0].severity == DriftSeverity.WARN
    assert "Add column 'sector'" in added[0].proposed_change


@pytest.mark.parametrize("fmt", ["csv", "jsonl", "parquet"])
def test_drift_removed_column(fmt: str) -> None:
    base = infer_schema(FIXTURES_DIR / "drops" / f"baseline_{fmt}")
    drift = infer_schema(FIXTURES_DIR / "drift" / fmt / "removed_column")

    diff = compare_schemas(base, drift)
    assert diff.has_hold is True
    removed = [d for d in diff.differences if d.category == DriftCategory.REMOVED_COLUMN]
    assert len(removed) == 1
    assert removed[0].column == "weight"
    assert removed[0].severity == DriftSeverity.HOLD
    assert "was removed" in removed[0].proposed_change


@pytest.mark.parametrize("fmt", ["csv", "jsonl", "parquet"])
def test_drift_type_narrowed(fmt: str) -> None:
    base = infer_schema(FIXTURES_DIR / "drops" / f"baseline_{fmt}")
    drift = infer_schema(FIXTURES_DIR / "drift" / fmt / "type_narrowed")

    diff = compare_schemas(base, drift)
    assert diff.has_hold is False
    narrowed = [d for d in diff.differences if d.category == DriftCategory.TYPE_NARROWED]
    assert len(narrowed) == 1
    assert narrowed[0].column == "weight"
    assert narrowed[0].severity == DriftSeverity.WARN
    assert "narrowed from" in narrowed[0].proposed_change


@pytest.mark.parametrize("fmt", ["csv", "jsonl", "parquet"])
def test_drift_type_widened(fmt: str) -> None:
    base = infer_schema(FIXTURES_DIR / "drops" / f"baseline_{fmt}")
    drift = infer_schema(FIXTURES_DIR / "drift" / fmt / "type_widened")

    diff = compare_schemas(base, drift)
    assert diff.has_hold is True
    widened = [d for d in diff.differences if d.category == DriftCategory.TYPE_WIDENED]
    assert len(widened) == 1
    assert widened[0].column == "record_id"
    assert widened[0].severity == DriftSeverity.HOLD
    assert "widened from" in widened[0].proposed_change


@pytest.mark.parametrize("fmt", ["csv", "jsonl", "parquet"])
def test_drift_nullability_change(fmt: str) -> None:
    base = infer_schema(FIXTURES_DIR / "drops" / f"baseline_{fmt}")
    drift = infer_schema(FIXTURES_DIR / "drift" / fmt / "nullability_change")

    diff = compare_schemas(base, drift)
    assert diff.has_hold is False
    null_diff = [d for d in diff.differences if d.category == DriftCategory.NULLABILITY_CHANGE]
    assert len(null_diff) == 1
    assert null_diff[0].column == "price"
    assert null_diff[0].severity == DriftSeverity.WARN
    assert "nullable: true" in null_diff[0].proposed_change


def test_accept_schema_changes_workflow(tmp_path: Path) -> None:
    schema_dir = tmp_path / "schemas"
    base_drop = FIXTURES_DIR / "drops" / "baseline_csv"
    drift_drop = FIXTURES_DIR / "drift" / "csv" / "added_column"

    # Initial pin: version 1
    pin_schema("vendor_update", drop_path=base_drop, schema_dir=schema_dir)
    pinned_v1 = VendorSchema.from_file(schema_dir / "vendor_update.yaml")
    assert pinned_v1.version == 1
    assert "sector" not in pinned_v1.columns

    # Accept changes from drift drop: updates to version 2 with added column
    updated_v2 = accept_schema_changes(
        "vendor_update",
        drop_path=drift_drop,
        schema_dir=schema_dir,
    )
    assert updated_v2.version == 2
    assert "sector" in updated_v2.columns
    assert updated_v2.columns["sector"].type == InferredType.STRING

    # Accepting when no drift exists returns unchanged
    unchanged = accept_schema_changes(
        "vendor_update",
        drop_path=drift_drop,
        schema_dir=schema_dir,
    )
    assert unchanged.version == 2

    # Accepting without drop_path increments version
    bumped = accept_schema_changes("vendor_update", schema_dir=schema_dir)
    assert bumped.version == 3


def test_schema_missing_files_raise(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        check_schema(
            "missing_vendor",
            drop_path=FIXTURES_DIR / "drops" / "baseline_csv",
            schema_dir=tmp_path,
        )

    with pytest.raises(FileNotFoundError):
        accept_schema_changes("missing_vendor", schema_dir=tmp_path)


def test_custom_manifest_and_money_columns(tmp_path: Path) -> None:
    manifest_yaml = """
vendor: custom_vendor
entity_columns: [entity_id]
entity_id_type: ticker
event_time_column: event_time
knowledge_time: {source: drop_time}
value_columns: [weight]
"""
    m = VendorManifest.from_yaml(manifest_yaml)
    # When manifest declares 'weight' as value_column, it is inferred as decimal
    schema = infer_schema(
        FIXTURES_DIR / "drops" / "baseline_csv",
        manifest=m,
    )
    assert schema.columns["weight"].type == InferredType.DECIMAL


def test_schema_infer_path_with_single_quotes_csv(tmp_path: Path) -> None:
    """Verify schema inference succeeds on CSV paths containing single quotes."""
    quote_dir = tmp_path / "vendor's_drop"
    quote_dir.mkdir()
    f = quote_dir / "data'with'quote.csv"
    f.write_text("record_id,amount,description\n1,123.45,item_a\n2,678.90,item_b\n", encoding="utf-8")

    schema = infer_schema(quote_dir, vendor="quoted_vendor")
    assert "record_id" in schema.columns
    assert "amount" in schema.columns
    assert "description" in schema.columns
    assert schema.columns["record_id"].type == InferredType.INTEGER
    assert schema.columns["amount"].type == InferredType.DECIMAL
    assert schema.columns["description"].type == InferredType.STRING


def test_schema_infer_path_with_single_quotes_jsonl(tmp_path: Path) -> None:
    """Verify schema inference succeeds on JSONL paths containing single quotes."""
    quote_dir = tmp_path / "vendor's_jsonl_drop"
    quote_dir.mkdir()
    f = quote_dir / "records'data.jsonl"
    f.write_text(
        '{"record_id": 1, "ticker": "AAPL", "price": 180.50}\n'
        '{"record_id": 2, "ticker": "MSFT", "price": 375.25}\n',
        encoding="utf-8",
    )

    schema = infer_schema(quote_dir, vendor="quoted_jsonl_vendor")
    assert schema.columns["record_id"].type == InferredType.INTEGER
    assert schema.columns["ticker"].type == InferredType.STRING
    assert schema.columns["price"].type == InferredType.DECIMAL


def test_schema_infer_path_with_single_quotes_csv_gz(tmp_path: Path) -> None:
    """Verify schema inference succeeds on gzipped CSV paths containing single quotes."""
    quote_dir = tmp_path / "vendor's_gz_drop"
    quote_dir.mkdir()
    f = quote_dir / "compressed'data.csv.gz"
    csv_bytes = b"record_id,flag\n100,true\n200,false\n"
    f.write_bytes(gzip.compress(csv_bytes, mtime=0))

    schema = infer_schema(quote_dir, vendor="quoted_gz_vendor")
    assert schema.columns["record_id"].type == InferredType.INTEGER
    assert schema.columns["flag"].type == InferredType.BOOLEAN


def test_schema_infer_single_column_csv(tmp_path: Path) -> None:
    """Verify schema inference accurately profiles single-column CSV datasets."""
    drop_dir = tmp_path / "single_col_drop"
    drop_dir.mkdir()
    f = drop_dir / "universe.csv"
    f.write_text("ticker\nAAPL\nMSFT\nGOOGL\nAMZN\n", encoding="utf-8")

    schema = infer_schema(drop_dir, vendor="universe_vendor")
    assert len(schema.columns) == 1
    assert "ticker" in schema.columns
    assert schema.columns["ticker"].type == InferredType.STRING
    assert schema.columns["ticker"].distinct_count == 4
    assert schema.columns["ticker"].nullable is False


def test_schema_pin_and_check_path_with_quotes(tmp_path: Path) -> None:
    """Verify schema pinning and drift checking operate as expected on paths with single quotes."""
    quote_dir = tmp_path / "quote's_check_drop"
    quote_dir.mkdir()
    f = quote_dir / "quote'data.csv"
    f.write_text("col_a,col_b\n1,alpha\n2,beta\n", encoding="utf-8")

    schema_dir = tmp_path / "schemas'dir"
    pinned_path = pin_schema("quote_vendor", drop_path=quote_dir, schema_dir=schema_dir)
    assert pinned_path.exists()

    diff = check_schema("quote_vendor", drop_path=quote_dir, schema_dir=schema_dir)
    assert diff.has_hold is False
    assert len(diff.differences) == 0
