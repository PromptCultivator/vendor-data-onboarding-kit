"""Programmatic Python API and Domain Exception Hierarchy.

Provides high-level, strongly-typed workflow entry points for schema inference,
data quality validation, append-only bitemporal storage, diffing, and end-to-end pipelines.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa

from vendor_onboarding.diff.ledger import DiffEngine, DiffSummary
from vendor_onboarding.ingestion.infer import infer_schema
from vendor_onboarding.ingestion.reader import read_file
from vendor_onboarding.models.schema import (
    DriftSeverity,
    SchemaDefinition,
    detect_drift,
    load_schema,
    pin_schema,
)
from vendor_onboarding.storage.store import BitemporalParquetStore
from vendor_onboarding.validation.engine import VectorizedValidationEngine
from vendor_onboarding.validation.rules import ValidationResult

# ============================================================================
# Domain Exception Hierarchy
# ============================================================================


class VendorKitError(Exception):
    """Base domain exception for vendor-data-onboarding-kit."""


class SchemaValidationError(VendorKitError):
    """Raised when data violates pinned schema contract or drift bounds."""


class IngestionError(VendorKitError):
    """Raised on corrupted archive, unrecognized format, or decompression error."""


class SymbologyError(VendorKitError):
    """Raised on symbology mapping or unresolved entity error."""


class StorageError(VendorKitError):
    """Raised on append-only violation or storage corruption."""


class DiffError(VendorKitError):
    """Raised on diff or restatement engine calculation failure."""


# ============================================================================
# High-Level Programmatic Workflows
# ============================================================================


def run_onboarding_pipeline(
    file_path: Path | str,
    dataset_id: str = "EQUITY_DAILY",
    vendor_id: str = "SYNTH_VENDOR_A",
    drop_id: str = "DROP_001",
    store_dir: Path | str | None = None,
) -> dict[str, Any]:
    """Executes programmatic end-to-end pipeline: ingestion, validation, diffing, and storage.

    Args:
        file_path: Path to the raw vendor drop file.
        dataset_id: Identifier of the dataset family.
        vendor_id: Identifier of the vendor.
        drop_id: Identifier of this specific drop batch.
        store_dir: Optional root directory for the append-only bitemporal store.

    Returns:
        Structured execution summary dictionary containing status, validation results,
        and restatements detected.
    """
    path_obj = Path(file_path)
    if not path_obj.exists():
        raise IngestionError(f"Input file not found: {file_path}")

    try:
        table = read_file(path_obj)
    except Exception as e:
        raise IngestionError(f"Failed to read file {file_path}: {e}") from e

    # Vectorized validation
    validator = VectorizedValidationEngine()
    val_results = validator.validate(table)
    validation_passed = all(r.passed for r in val_results)

    # Restatements count
    restatements_detected = 0
    if "is_restatement" in table.column_names:
        is_restat_col = table["is_restatement"].to_pylist()
        restatements_detected = sum(1 for x in is_restat_col if bool(x))

    # Append to bitemporal store if store_dir is provided
    if store_dir is not None:
        try:
            store = BitemporalParquetStore(store_dir)
            store.append(table, dataset_id=dataset_id, drop_id=drop_id)
        except Exception as e:
            raise StorageError(f"Failed to persist drop to bitemporal store: {e}") from e

    return {
        "status": "SUCCESS" if validation_passed else "FAILED",
        "dataset_id": dataset_id,
        "vendor_id": vendor_id,
        "drop_id": drop_id,
        "input_file": str(file_path),
        "validation_passed": validation_passed,
        "restatements_detected": restatements_detected,
        "record_count": table.num_rows,
        "validation_results": [
            {
                "check_name": r.check_name,
                "passed": r.passed,
                "violation_count": r.violation_count,
            }
            for r in val_results
        ],
    }


def infer_and_pin(
    file_path: Path | str,
    output_schema_path: Path | str,
    sample_rows: int = 10000,
) -> SchemaDefinition:
    """Infers schema definition from data file and pins it as an immutable contract."""
    path_obj = Path(file_path)
    if not path_obj.exists():
        raise IngestionError(f"File not found: {file_path}")

    schema = infer_schema(str(path_obj), sample_rows=sample_rows)
    pin_schema(schema, str(output_schema_path))
    return schema


def validate_drop(
    file_path: Path | str,
    schema_path: Path | str | None = None,
) -> list[ValidationResult]:
    """Validates raw data file against vectorized quality rules and optional schema contract."""
    path_obj = Path(file_path)
    if not path_obj.exists():
        raise IngestionError(f"File not found: {file_path}")

    if schema_path is not None:
        p_schema = Path(schema_path)
        if not p_schema.exists():
            raise SchemaValidationError(f"Schema contract not found: {schema_path}")

        _ = load_schema(p_schema)
        current_schema = infer_schema(str(path_obj))
        drift_report = detect_drift(current_schema, p_schema)
        if drift_report.is_breaking:
            breaking_msgs = [
                i.message for i in drift_report.issues if i.severity == DriftSeverity.BREAKING
            ]
            raise SchemaValidationError(
                f"Validation failed due to breaking schema drift: {'; '.join(breaking_msgs)}"
            )

    table = read_file(path_obj)
    validator = VectorizedValidationEngine()
    return validator.validate(table)


def query_as_of(
    dataset_id: str,
    as_of_time: datetime | str,
    store_dir: Path | str,
    entity_ids: list[str] | None = None,
    metric: str | None = None,
) -> pa.Table:
    """Executes zero look-ahead point-in-time query against bitemporal store."""
    if isinstance(as_of_time, datetime):
        as_of_dt = (
            as_of_time.replace(tzinfo=UTC)
            if as_of_time.tzinfo is None
            else as_of_time.astimezone(UTC)
        )
    else:
        clean_str = str(as_of_time).replace("Z", "+00:00")
        as_of_dt = datetime.fromisoformat(clean_str)
        if as_of_dt.tzinfo is None:
            as_of_dt = as_of_dt.replace(tzinfo=UTC)
        else:
            as_of_dt = as_of_dt.astimezone(UTC)

    store = BitemporalParquetStore(store_dir)
    return store.as_of_query(
        dataset_id=dataset_id,
        as_of_time=as_of_dt,
        entity_ids=entity_ids,
        metric=metric,
    )


def diff_drops(
    baseline_path: Path | str,
    current_path: Path | str,
    keys: list[str] | None = None,
) -> tuple[pa.Table, DiffSummary]:
    """Diffs two vendor drops and calculates restatement recall, precision, and deltas."""
    p_base = Path(baseline_path)
    p_curr = Path(current_path)

    if not p_base.exists():
        raise IngestionError(f"Baseline file not found: {baseline_path}")
    if not p_curr.exists():
        raise IngestionError(f"Current file not found: {current_path}")

    tbl_base = read_file(p_base)
    tbl_curr = read_file(p_curr)

    diff_engine = DiffEngine()
    return diff_engine.diff_drops(tbl_base, tbl_curr, keys=keys)
