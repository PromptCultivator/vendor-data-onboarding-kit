"""Tier 1 Feature Tests: Schema inference, contract pinning, and drift detection."""

import json
from pathlib import Path

import pytest

from vendor_onboarding.ingestion.infer import infer_schema
from vendor_onboarding.models.schema import (
    ColumnDefinition,
    DataType,
    DriftSeverity,
    DriftType,
    detect_drift,
    load_schema,
    pin_schema,
)


def test_infer_schema_csv_profiling(sample_csv_path: Path) -> None:
    """Test schema inference on raw CSV with single-pass statistics."""
    schema = infer_schema(sample_csv_path)
    assert schema.dataset_id == "EQUITY_DAILY_FUNDAMENTALS"
    assert schema.vendor_id == "SYNTH_VENDOR_A"
    assert schema.total_rows_sampled == 60
    assert len(schema.columns) == 13

    # Check specific inferred column types
    assert schema.columns["record_id"].data_type == DataType.STRING
    assert schema.columns["record_id"].nullable is False
    assert schema.columns["record_id"].is_primary_key_candidate is True

    assert schema.columns["value"].data_type == DataType.FLOAT64
    assert schema.columns["event_time"].data_type == DataType.TIMESTAMP
    assert schema.columns["is_restatement"].data_type == DataType.BOOLEAN
    assert schema.columns["revision_seq"].data_type == DataType.INT64


def test_infer_schema_parquet_and_json(sample_parquet_path: Path, sample_json_path: Path) -> None:
    """Test schema inference across columnar Parquet and JSON files."""
    s_pq = infer_schema(sample_parquet_path)
    assert s_pq.total_rows_sampled == 60
    assert s_pq.file_format == "PARQUET"
    assert "entity_id" in s_pq.columns

    s_json = infer_schema(sample_json_path)
    assert s_json.total_rows_sampled == 60
    assert s_json.file_format == "JSON"
    assert s_json.columns["value"].data_type == DataType.FLOAT64


def test_categorical_column_profiling(sample_csv_path: Path) -> None:
    """Test automatic categorical classification and distinct categories extraction."""
    schema = infer_schema(sample_csv_path)

    # Unit and metric have very low cardinality
    assert schema.columns["unit"].is_categorical is True
    assert sorted(schema.columns["unit"].categories or []) == ["SHARES", "USD"]

    assert schema.columns["source_symbol_type"].is_categorical is True
    assert schema.columns["source_symbol_type"].categories == ["TICKER"]

    assert schema.columns["metric"].is_categorical is True
    assert "CLOSE_PRICE" in (schema.columns["metric"].categories or [])


def test_primary_key_candidate_detection(sample_csv_path: Path) -> None:
    """Test primary key candidate detection (null_count == 0, unique values)."""
    schema = infer_schema(sample_csv_path)
    assert "record_id" in schema.primary_key_candidates
    assert schema.primary_key == ["record_id"]
    assert schema.columns["record_id"].null_count == 0
    assert schema.columns["record_id"].distinct_count == 60


def test_pin_and_load_schema_contract(schema_baseline_path: Path, tmp_path: Path) -> None:
    """Test pinning schema to disk and loading it back."""
    schema = load_schema(schema_baseline_path)
    assert schema.schema_version == "1.0.0"
    assert schema.dataset_id == "EQUITY_DAILY_FUNDAMENTALS"
    assert schema.primary_key == ["entity_id", "event_time", "metric"]

    out_file = tmp_path / "pinned_schema.json"
    pin_schema(schema, out_file)
    assert out_file.is_file()

    reloaded = load_schema(out_file)
    assert reloaded.dataset_id == schema.dataset_id
    assert reloaded.primary_key == schema.primary_key
    assert len(reloaded.columns) == len(schema.columns)


def test_detect_drift_zero_drift(schema_baseline_path: Path) -> None:
    """Test drift detection on identical schema produces zero issues."""
    baseline = load_schema(schema_baseline_path)
    report = detect_drift(baseline, baseline)

    assert report.is_breaking is False
    assert report.breaking_count == 0
    assert report.warning_count == 0
    assert len(report.issues) == 0


def test_detect_drift_breaking_changes(
    schema_baseline_path: Path,
    schema_breaking_path: Path,
) -> None:
    """Test classification of BREAKING schema drift: missing column and incompatible type."""
    baseline = load_schema(schema_baseline_path)
    breaking = load_schema(schema_breaking_path)

    report = detect_drift(breaking, baseline)
    assert report.is_breaking is True
    assert report.breaking_count >= 2

    # Check for missing column issue ('unit' is missing in breaking)
    missing_issues = [
        i for i in report.issues if i.drift_type == DriftType.MISSING_COLUMN and i.column_name == "unit"
    ]
    assert len(missing_issues) == 1
    assert missing_issues[0].severity == DriftSeverity.BREAKING

    # Check for incompatible type change ('value' changed from double to string)
    type_issues = [
        i for i in report.issues if i.drift_type == DriftType.TYPE_INCOMPATIBLE and i.column_name == "value"
    ]
    assert len(type_issues) == 1
    assert type_issues[0].severity == DriftSeverity.BREAKING


def test_detect_drift_warning_changes(
    schema_baseline_path: Path,
    schema_warning_path: Path,
) -> None:
    """Test classification of WARNING schema drift: new column and relaxed nullability."""
    baseline = load_schema(schema_baseline_path)
    warning = load_schema(schema_warning_path)

    report = detect_drift(warning, baseline)
    assert report.is_breaking is False
    assert report.breaking_count == 0
    assert report.warning_count >= 2

    # Check new column ('currency_code' was added)
    new_col_issues = [
        i for i in report.issues if i.drift_type == DriftType.NEW_COLUMN and i.column_name == "currency_code"
    ]
    assert len(new_col_issues) == 1
    assert new_col_issues[0].severity == DriftSeverity.WARNING

    # Check relaxed nullability ('source_symbol' nullable: false -> true)
    null_issues = [
        i for i in report.issues if i.column_name == "source_symbol"
    ]
    assert len(null_issues) == 1
    assert null_issues[0].severity == DriftSeverity.WARNING


def test_drift_report_summary_and_markdown(schema_baseline_path: Path, schema_breaking_path: Path) -> None:
    """Test DriftReport summary dictionary and Markdown table formatting."""
    baseline = load_schema(schema_baseline_path)
    breaking = load_schema(schema_breaking_path)
    report = detect_drift(breaking, baseline)

    summary = report.summary()
    assert summary["is_breaking"] is True
    assert summary["breaking_count"] >= 2
    assert summary["pinned_schema_version"] == "1.0.0"

    md = report.to_markdown_table()
    assert "| Column | Severity | Drift Type | Message | Expected | Actual |" in md
    assert "BREAKING" in md
    assert "`unit`" in md
    assert "`value`" in md


def test_infer_schema_sample_rows_limit(sample_csv_path: Path) -> None:
    """Verify sample_rows limit restricts sampling during inference."""
    schema = infer_schema(sample_csv_path, sample_rows=10)
    assert schema.total_rows_sampled == 10
    assert len(schema.columns) == 13


def test_infer_schema_temporal_formats(sample_csv_path: Path) -> None:
    """Verify event_time and knowledge_time are correctly inferred as TIMESTAMP."""
    schema = infer_schema(sample_csv_path)
    assert schema.columns["event_time"].data_type == DataType.TIMESTAMP
    assert schema.columns["knowledge_time"].data_type == DataType.TIMESTAMP


def test_load_schema_invalid_json_raises(tmp_path: Path) -> None:
    """Verify load_schema raises appropriate errors on invalid or missing file."""
    with pytest.raises(FileNotFoundError):
        load_schema(tmp_path / "nonexistent.json")

    bad_json = tmp_path / "bad.json"
    bad_json.write_text("{not valid json", encoding="utf-8")
    with pytest.raises((ValueError, json.JSONDecodeError)):
        load_schema(bad_json)


def test_pin_schema_version_validation(schema_baseline_path: Path) -> None:
    """Verify schema contract specifies semantic version format X.Y.Z."""
    schema = load_schema(schema_baseline_path)
    parts = schema.schema_version.split(".")
    assert len(parts) == 3
    assert all(p.isdigit() for p in parts)


def test_detect_drift_primary_key_modified(schema_baseline_path: Path) -> None:
    """Verify modifying compound primary key definition classifies as BREAKING."""
    baseline = load_schema(schema_baseline_path)
    mutated = baseline.model_copy(deep=True)
    mutated.primary_key = ["entity_id", "event_time"]  # Dropped metric

    report = detect_drift(mutated, baseline)
    assert report.is_breaking is True
    pk_issues = [i for i in report.issues if i.drift_type == DriftType.PRIMARY_KEY_CHANGED]
    assert len(pk_issues) == 1
    assert pk_issues[0].severity == DriftSeverity.BREAKING


def test_detect_drift_nullability_tightened_info(schema_baseline_path: Path) -> None:
    """Verify tightening nullability (nullable -> non-null) classifies as INFO."""
    baseline = load_schema(schema_baseline_path)
    mutated_base = baseline.model_copy(deep=True)
    mutated_base.columns["unit"].nullable = True
    report = detect_drift(baseline, mutated_base)
    null_issues = [i for i in report.issues if i.drift_type == DriftType.NULLABILITY_CHANGED]
    assert len(null_issues) == 1
    assert null_issues[0].severity == DriftSeverity.INFO


def test_detect_drift_added_column_warning(schema_baseline_path: Path) -> None:
    """Verify adding a new column to dataset classifies as WARNING."""
    baseline = load_schema(schema_baseline_path)
    mutated = baseline.model_copy(deep=True)
    mutated.columns["new_column_x"] = ColumnDefinition(
        name="new_column_x",
        data_type=DataType.STRING,
        nullable=True,
    )

    report = detect_drift(mutated, baseline)
    assert report.is_breaking is False
    new_col_issues = [i for i in report.issues if i.drift_type == DriftType.NEW_COLUMN and i.column_name == "new_column_x"]
    assert len(new_col_issues) == 1
    assert new_col_issues[0].severity == DriftSeverity.WARNING


def test_detect_drift_safe_widening_warning(schema_baseline_path: Path) -> None:
    """Verify safe type widening (e.g. int32 -> int64) is WARNING rather than BREAKING."""
    baseline = load_schema(schema_baseline_path)
    base_with_int32 = baseline.model_copy(deep=True)
    base_with_int32.columns["revision_seq"].data_type = DataType.INT32

    # Widening from INT32 in baseline to INT64 in current
    report = detect_drift(baseline, base_with_int32)
    assert report.is_breaking is False
    widening_issues = [i for i in report.issues if i.column_name == "revision_seq"]
    if widening_issues:
        assert widening_issues[0].severity == DriftSeverity.WARNING


def test_column_definition_serialization() -> None:
    """Verify ColumnDefinition to_dict preserves logical type and nullability."""
    col = ColumnDefinition(
        name="price",
        data_type=DataType.FLOAT64,
        nullable=False,
        null_count=0,
    )
    d = col.to_dict()
    assert d["name"] == "price"
    assert d["type"] == "float64"
    assert d["nullable"] is False
    assert d["null_count"] == 0


def test_drift_report_json_roundtrip(schema_baseline_path: Path, schema_warning_path: Path) -> None:
    """Verify DriftReport summary serializes to JSON without exceptions."""
    import json
    baseline = load_schema(schema_baseline_path)
    warning = load_schema(schema_warning_path)
    report = detect_drift(warning, baseline)

    summary = report.summary()
    serialized = json.dumps(summary, default=str)
    deserialized = json.loads(serialized)
    assert deserialized["is_breaking"] is False
    assert deserialized["warning_count"] >= 2

