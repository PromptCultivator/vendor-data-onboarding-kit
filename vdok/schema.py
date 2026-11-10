"""Schema inference, contract pinning, and schema drift detection.

Provides statistical row sampling up to 200,000 rows per file, seven-category type
inference with exact decimal enforcement for monetary columns, YAML contract pinning
to schemas/<vendor>.yaml, and five-category drift detection with hold/warn classification.
"""

from __future__ import annotations

import datetime
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from pydantic import BaseModel, Field

from vdok.discover import Compression, FileFormat, scan
from vdok.manifest import VendorManifest


class InferredType(str, Enum):
    """The seven authoritative data types supported by the kit."""

    INTEGER = "integer"
    DECIMAL = "decimal"
    FLOAT = "float"
    STRING = "string"
    DATE = "date"
    DATETIME = "datetime"
    BOOLEAN = "boolean"


class DriftSeverity(str, Enum):
    """Operational severity tier for schema differences."""

    WARN = "warn"
    HOLD = "hold"


class DriftCategory(str, Enum):
    """Classification of schema modifications between drop and contract."""

    ADDED_COLUMN = "added_column"
    REMOVED_COLUMN = "removed_column"
    TYPE_NARROWED = "type_narrowed"
    TYPE_WIDENED = "type_widened"
    NULLABILITY_CHANGE = "nullability_change"


class ColumnSchema(BaseModel):
    """Specification of an individual column contract."""

    type: InferredType
    nullable: bool = True
    distinct_count: int | None = None
    min: Any | None = None
    max: Any | None = None
    precision: int | None = None
    scale: int | None = None


class VendorSchema(BaseModel):
    """Versioned schema contract for a vendor dataset."""

    vendor: str
    version: int = 1
    pinned_at: str
    columns: dict[str, ColumnSchema] = Field(default_factory=dict)

    @classmethod
    def from_yaml(cls, content: str) -> VendorSchema:
        data: Any = yaml.safe_load(content)
        if not isinstance(data, dict):
            raise ValueError("Schema content must be a YAML mapping.")
        return cls.model_validate(data)

    @classmethod
    def from_file(cls, path: Path | str) -> VendorSchema:
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Pinned schema file not found: {p}")
        return cls.from_yaml(p.read_text(encoding="utf-8"))

    def to_yaml(self) -> str:
        dumped = self.model_dump(mode="json", exclude_none=True)
        return yaml.safe_dump(dumped, sort_keys=False)

    def to_file(self, path: Path | str) -> None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(self.to_yaml(), encoding="utf-8")


@dataclass(frozen=True)
class DriftDifference:
    """An individual difference between an incoming drop and the pinned contract."""

    column: str
    category: DriftCategory
    severity: DriftSeverity
    pinned_value: Any
    drop_value: Any
    proposed_change: str


@dataclass(frozen=True)
class SchemaDiff:
    """Complete difference evaluation between drop and pinned schema."""

    vendor: str
    pinned_version: int
    has_hold: bool
    differences: list[DriftDifference]


# Heuristic patterns for monetary fields requiring exact decimal representations
_MONEY_PATTERNS = re.compile(
    r"(price|value|val|cost|fee|revenue|nav|pnl|balance|amount|cash|dividend)",
    re.IGNORECASE,
)

_DATE_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DATETIME_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}")
_INT_PATTERN = re.compile(r"^-?\d+$")
_FLOAT_PATTERN = re.compile(r"^-?\d*(\.\d+)?([eE][-+]?\d+)?$")


def _is_money_column(col_name: str, money_columns: set[str] | None = None) -> bool:
    if money_columns and col_name in money_columns:
        return True
    return bool(_MONEY_PATTERNS.search(col_name))


def _decimal_prec_scale(d: Decimal) -> tuple[int, int]:
    tup = d.as_tuple()
    scale = -tup.exponent if isinstance(tup.exponent, int) and tup.exponent < 0 else 0
    prec = len(tup.digits)
    return prec, scale


def _infer_column_from_values(
    col_name: str,
    values: list[Any],
    money_columns: set[str] | None = None,
) -> ColumnSchema:
    """Infer column type and statistics from sample Python values."""
    total_count = len(values)
    non_null = [v for v in values if v is not None and v != ""]
    null_count = total_count - len(non_null)
    nullable = null_count > 0

    if not non_null:
        return ColumnSchema(
            type=InferredType.STRING,
            nullable=True,
            distinct_count=0,
            min=None,
            max=None,
        )

    distinct_count = len(set(non_null))
    is_money = _is_money_column(col_name, money_columns)

    # 1. Check boolean
    if all(isinstance(v, bool) or str(v).lower() in ("true", "false") for v in non_null):
        return ColumnSchema(
            type=InferredType.BOOLEAN,
            nullable=nullable,
            distinct_count=distinct_count,
            min=None,
            max=None,
        )

    # Allow up to 1% cell-level corruptions on large (>= 100 non-null) numeric columns
    # so row-level type_flip quarantine isolates cell defects rather than batch-holding
    max_numeric_anomalies = int(len(non_null) * 0.01) if len(non_null) >= 100 else 0

    # 2. Check integer
    int_vals: list[int] = []
    non_int_count = 0
    for v in non_null:
        if isinstance(v, int) and not isinstance(v, bool):
            int_vals.append(v)
        elif isinstance(v, str) and _INT_PATTERN.match(v.strip()):
            int_vals.append(int(v.strip()))
        else:
            non_int_count += 1
            if non_int_count > max_numeric_anomalies:
                break

    if non_int_count <= max_numeric_anomalies and int_vals:
        return ColumnSchema(
            type=InferredType.INTEGER,
            nullable=nullable,
            distinct_count=distinct_count,
            min=min(int_vals),
            max=max(int_vals),
        )

    # 3. Check date
    all_date = True
    for v in non_null:
        if isinstance(v, datetime.date) and not isinstance(v, datetime.datetime):
            continue
        if isinstance(v, str) and _DATE_ISO.match(v.strip()):
            try:
                datetime.date.fromisoformat(v.strip())
                continue
            except ValueError:
                pass
        all_date = False
        break

    if all_date:
        date_strs = [
            v.isoformat() if isinstance(v, datetime.date) else str(v).strip() for v in non_null
        ]
        return ColumnSchema(
            type=InferredType.DATE,
            nullable=nullable,
            distinct_count=distinct_count,
            min=min(date_strs),
            max=max(date_strs),
        )

    # 4. Check datetime
    all_datetime = True
    for v in non_null:
        if isinstance(v, datetime.datetime):
            continue
        if isinstance(v, str) and _DATETIME_ISO.match(v.strip()):
            try:
                # normalize Z to +00:00 for ISO parsing
                clean_s = v.strip().replace("Z", "+00:00")
                datetime.datetime.fromisoformat(clean_s)
                continue
            except ValueError:
                pass
        all_datetime = False
        break

    if all_datetime:
        dt_strs = [
            v.isoformat() if isinstance(v, datetime.datetime) else str(v).strip() for v in non_null
        ]
        return ColumnSchema(
            type=InferredType.DATETIME,
            nullable=nullable,
            distinct_count=distinct_count,
            min=min(dt_strs),
            max=max(dt_strs),
        )

    # 5. Check decimal and float
    non_num_count = 0
    valid_numeric_raw: list[Any] = []
    max_scale = 0
    max_precision = 0
    for v in non_null:
        if isinstance(v, Decimal):
            cur_prec, cur_scale = _decimal_prec_scale(v)
            max_scale = max(max_scale, cur_scale)
            max_precision = max(max_precision, cur_prec)
            valid_numeric_raw.append(v)
            continue
        if isinstance(v, (int, float)):
            s = f"{v:.9f}".rstrip("0").rstrip(".")
            try:
                cur_prec, cur_scale = _decimal_prec_scale(Decimal(s))
                max_scale = max(max_scale, cur_scale)
                max_precision = max(max_precision, cur_prec)
                valid_numeric_raw.append(v)
                continue
            except InvalidOperation:
                pass
        if isinstance(v, str) and _FLOAT_PATTERN.match(v.strip()):
            try:
                cur_prec, cur_scale = _decimal_prec_scale(Decimal(v.strip()))
                max_scale = max(max_scale, cur_scale)
                max_precision = max(max_precision, cur_prec)
                valid_numeric_raw.append(v)
                continue
            except InvalidOperation:
                pass
        non_num_count += 1
        if non_num_count > max_numeric_anomalies:
            break

    if non_num_count <= max_numeric_anomalies and valid_numeric_raw:
        # Enforce exact decimal for money fields; allow float for non-money continuous fields
        if is_money or any(isinstance(v, Decimal) for v in valid_numeric_raw):
            dec_vals = [
                v if isinstance(v, Decimal) else Decimal(str(v).strip()) for v in valid_numeric_raw
            ]
            final_precision = max(max_precision, 18)
            final_scale = max(max_scale, 4)
            return ColumnSchema(
                type=InferredType.DECIMAL,
                nullable=nullable,
                distinct_count=distinct_count,
                min=str(min(dec_vals)),
                max=str(max(dec_vals)),
                precision=final_precision,
                scale=final_scale,
            )
        float_vals = [float(v) for v in valid_numeric_raw]
        return ColumnSchema(
            type=InferredType.FLOAT,
            nullable=nullable,
            distinct_count=distinct_count,
            min=min(float_vals),
            max=max(float_vals),
        )

    # 6. Fallback string
    str_vals = [str(v) for v in non_null]
    return ColumnSchema(
        type=InferredType.STRING,
        nullable=nullable,
        distinct_count=distinct_count,
        min=min(str_vals),
        max=max(str_vals),
    )


def _infer_parquet_file(
    file_path: Path,
    max_sample_rows: int,
    money_columns: set[str] | None = None,
) -> dict[str, ColumnSchema]:
    """Infer schema directly from Parquet metadata and table sample."""
    table = pq.read_table(file_path)
    if table.num_rows > max_sample_rows:
        table = table.slice(0, max_sample_rows)

    columns: dict[str, ColumnSchema] = {}
    for name in table.schema.names:
        field = table.schema.field(name)
        col = table.column(name)
        null_count = col.null_count
        nullable = null_count > 0
        total_rows = len(col)
        non_null_count = total_rows - null_count

        arr_type = field.type
        is_money = _is_money_column(name, money_columns)

        if pa.types.is_decimal(arr_type):
            prec = arr_type.precision
            scale = arr_type.scale
            pydict_vals = [v.as_py() for v in col if v.is_valid]
            columns[name] = ColumnSchema(
                type=InferredType.DECIMAL,
                nullable=nullable,
                distinct_count=len(set(pydict_vals)) if pydict_vals else 0,
                min=str(min(pydict_vals)) if pydict_vals else None,
                max=str(max(pydict_vals)) if pydict_vals else None,
                precision=prec,
                scale=scale,
            )
        elif pa.types.is_integer(arr_type):
            pydict_vals = [v.as_py() for v in col if v.is_valid]
            columns[name] = ColumnSchema(
                type=InferredType.INTEGER,
                nullable=nullable,
                distinct_count=len(set(pydict_vals)) if pydict_vals else 0,
                min=min(pydict_vals) if pydict_vals else None,
                max=max(pydict_vals) if pydict_vals else None,
            )
        elif pa.types.is_floating(arr_type):
            if is_money:
                # If monetary, convert to exact decimal
                pydict_vals = [v.as_py() for v in col if v.is_valid]
                dec_vals = [Decimal(f"{v:.9f}".rstrip("0").rstrip(".")) for v in pydict_vals]
                columns[name] = ColumnSchema(
                    type=InferredType.DECIMAL,
                    nullable=nullable,
                    distinct_count=len(set(pydict_vals)) if pydict_vals else 0,
                    min=str(min(dec_vals)) if dec_vals else None,
                    max=str(max(dec_vals)) if dec_vals else None,
                    precision=18,
                    scale=4,
                )
            else:
                pydict_vals = [v.as_py() for v in col if v.is_valid]
                columns[name] = ColumnSchema(
                    type=InferredType.FLOAT,
                    nullable=nullable,
                    distinct_count=len(set(pydict_vals)) if pydict_vals else 0,
                    min=min(pydict_vals) if pydict_vals else None,
                    max=max(pydict_vals) if pydict_vals else None,
                )
        elif pa.types.is_boolean(arr_type):
            columns[name] = ColumnSchema(
                type=InferredType.BOOLEAN,
                nullable=nullable,
                distinct_count=2 if non_null_count > 1 else non_null_count,
            )
        elif pa.types.is_date(arr_type):
            pydict_vals = [v.as_py() for v in col if v.is_valid]
            date_strs = [v.isoformat() for v in pydict_vals]
            columns[name] = ColumnSchema(
                type=InferredType.DATE,
                nullable=nullable,
                distinct_count=len(set(date_strs)) if date_strs else 0,
                min=min(date_strs) if date_strs else None,
                max=max(date_strs) if date_strs else None,
            )
        elif pa.types.is_timestamp(arr_type):
            pydict_vals = [v.as_py() for v in col if v.is_valid]
            dt_strs = [v.isoformat() for v in pydict_vals]
            columns[name] = ColumnSchema(
                type=InferredType.DATETIME,
                nullable=nullable,
                distinct_count=len(set(dt_strs)) if dt_strs else 0,
                min=min(dt_strs) if dt_strs else None,
                max=max(dt_strs) if dt_strs else None,
            )
        else:
            pydict_vals = [v.as_py() for v in col if v.is_valid]
            str_vals = [str(v) for v in pydict_vals]
            columns[name] = ColumnSchema(
                type=InferredType.STRING,
                nullable=nullable,
                distinct_count=len(set(str_vals)) if str_vals else 0,
                min=min(str_vals) if str_vals else None,
                max=max(str_vals) if str_vals else None,
            )
    return columns


def _infer_text_file(
    file_path: Path,
    fmt: FileFormat,
    compression: Compression,
    max_sample_rows: int,
    money_columns: set[str] | None = None,
) -> dict[str, ColumnSchema]:
    """Sample CSV or JSONL using DuckDB and infer strongly typed column schemas."""
    con = duckdb.connect()
    try:
        path_str = str(file_path)
        if fmt == FileFormat.CSV:
            # Read as all_varchar to preserve exact numeric formatting and decimal digits
            df = (
                con.read_csv(
                    path_str,
                    all_varchar=True,
                )
                .limit(max_sample_rows)
                .to_arrow_table()
            )
        else:
            # JSONL: read with read_json
            df = (
                con.read_json(
                    path_str,
                    format="newline_delimited",
                )
                .limit(max_sample_rows)
                .to_arrow_table()
            )

        data_dict = df.to_pydict()
        columns: dict[str, ColumnSchema] = {}
        for col_name, val_list in data_dict.items():
            columns[col_name] = _infer_column_from_values(
                col_name,
                val_list,
                money_columns=money_columns,
            )
        return columns
    finally:
        con.close()


def infer_schema(
    drop_path: Path | str,
    vendor: str = "default",
    max_sample_rows: int = 200000,
    manifest: VendorManifest | None = None,
    money_columns: list[str] | None = None,
) -> VendorSchema:
    """Infer typed schema contract from a vendor drop path."""
    meta = scan(drop_path)
    active_money_cols: set[str] = set()
    if manifest and manifest.value_columns:
        active_money_cols.update(manifest.value_columns)
    if money_columns:
        active_money_cols.update(money_columns)

    all_file_schemas: list[dict[str, ColumnSchema]] = []
    for discovered_file in meta.files:
        if discovered_file.format == FileFormat.PARQUET:
            f_schema = _infer_parquet_file(
                discovered_file.path,
                max_sample_rows=max_sample_rows,
                money_columns=active_money_cols,
            )
        else:
            f_schema = _infer_text_file(
                discovered_file.path,
                fmt=discovered_file.format,
                compression=discovered_file.compression,
                max_sample_rows=max_sample_rows,
                money_columns=active_money_cols,
            )
        all_file_schemas.append(f_schema)

    # Merge column definitions across multi-file drops
    merged_columns: dict[str, ColumnSchema] = {}
    all_col_names: list[str] = []
    for s in all_file_schemas:
        for c in s:
            if c not in all_col_names:
                all_col_names.append(c)

    for col_name in all_col_names:
        per_file = [s[col_name] for s in all_file_schemas if col_name in s]
        primary = per_file[0]
        # Any file with nullable=True makes overall nullable=True
        is_nullable = any(pf.nullable for pf in per_file)
        # Combine distinct counts
        total_distinct = max((pf.distinct_count or 0) for pf in per_file)

        # Merge min and max
        mins = [pf.min for pf in per_file if pf.min is not None]
        maxs = [pf.max for pf in per_file if pf.max is not None]
        min_val = min(mins) if mins else None
        max_val = max(maxs) if maxs else None

        merged_columns[col_name] = ColumnSchema(
            type=primary.type,
            nullable=is_nullable,
            distinct_count=total_distinct,
            min=min_val,
            max=max_val,
            precision=primary.precision,
            scale=primary.scale,
        )

    now_utc = datetime.datetime.now(datetime.timezone.utc).isoformat()
    return VendorSchema(
        vendor=vendor,
        version=1,
        pinned_at=now_utc,
        columns=merged_columns,
    )


# Hierarchy mappings for type widening vs type narrowing
_WIDENING_TRANSITIONS: set[tuple[InferredType, InferredType]] = {
    (InferredType.INTEGER, InferredType.FLOAT),
    (InferredType.INTEGER, InferredType.DECIMAL),
    (InferredType.INTEGER, InferredType.STRING),
    (InferredType.FLOAT, InferredType.STRING),
    (InferredType.DECIMAL, InferredType.STRING),
    (InferredType.DECIMAL, InferredType.FLOAT),
    (InferredType.DATE, InferredType.STRING),
    (InferredType.DATE, InferredType.DATETIME),
    (InferredType.DATETIME, InferredType.STRING),
    (InferredType.BOOLEAN, InferredType.INTEGER),
    (InferredType.BOOLEAN, InferredType.STRING),
}

_NARROWING_TRANSITIONS: set[tuple[InferredType, InferredType]] = {
    (InferredType.FLOAT, InferredType.INTEGER),
    (InferredType.FLOAT, InferredType.DECIMAL),
    (InferredType.DECIMAL, InferredType.INTEGER),
    (InferredType.STRING, InferredType.DATE),
    (InferredType.STRING, InferredType.DATETIME),
    (InferredType.STRING, InferredType.INTEGER),
    (InferredType.STRING, InferredType.FLOAT),
    (InferredType.STRING, InferredType.DECIMAL),
    (InferredType.STRING, InferredType.BOOLEAN),
    (InferredType.DATETIME, InferredType.DATE),
    (InferredType.INTEGER, InferredType.BOOLEAN),
}


def compare_schemas(
    pinned: VendorSchema,
    drop: VendorSchema,
) -> SchemaDiff:
    """Compare an incoming drop schema against the pinned contract and classify drift."""
    differences: list[DriftDifference] = []

    # 1. Check removed columns (hold)
    for col_name, pinned_col in pinned.columns.items():
        if col_name not in drop.columns:
            differences.append(
                DriftDifference(
                    column=col_name,
                    category=DriftCategory.REMOVED_COLUMN,
                    severity=DriftSeverity.HOLD,
                    pinned_value=pinned_col.type.value,
                    drop_value=None,
                    proposed_change=(
                        f"Column '{col_name}' was removed from vendor feed; "
                        "update contract to drop column if intentional."
                    ),
                )
            )

    # 2. Check added columns (warn)
    for col_name, drop_col in drop.columns.items():
        if col_name not in pinned.columns:
            differences.append(
                DriftDifference(
                    column=col_name,
                    category=DriftCategory.ADDED_COLUMN,
                    severity=DriftSeverity.WARN,
                    pinned_value=None,
                    drop_value=drop_col.type.value,
                    proposed_change=(
                        f"Add column '{col_name}' of type {drop_col.type.value} to schema contract."
                    ),
                )
            )

    # 3. Check modifications to existing columns
    for col_name in pinned.columns:
        if col_name not in drop.columns:
            continue
        p_col = pinned.columns[col_name]
        d_col = drop.columns[col_name]

        # Type changes
        if p_col.type != d_col.type:
            pair = (p_col.type, d_col.type)
            if pair in _WIDENING_TRANSITIONS:
                differences.append(
                    DriftDifference(
                        column=col_name,
                        category=DriftCategory.TYPE_WIDENED,
                        severity=DriftSeverity.HOLD,
                        pinned_value=p_col.type.value,
                        drop_value=d_col.type.value,
                        proposed_change=(
                            f"Column '{col_name}' widened from {p_col.type.value} to "
                            f"{d_col.type.value}; update schema contract to widen type."
                        ),
                    )
                )
            elif pair in _NARROWING_TRANSITIONS:
                differences.append(
                    DriftDifference(
                        column=col_name,
                        category=DriftCategory.TYPE_NARROWED,
                        severity=DriftSeverity.WARN,
                        pinned_value=p_col.type.value,
                        drop_value=d_col.type.value,
                        proposed_change=(
                            f"Column '{col_name}' narrowed from {p_col.type.value} to "
                            f"{d_col.type.value}; tighten schema contract or keep current type."
                        ),
                    )
                )
            else:
                # Other type changes default to hold
                differences.append(
                    DriftDifference(
                        column=col_name,
                        category=DriftCategory.TYPE_WIDENED,
                        severity=DriftSeverity.HOLD,
                        pinned_value=p_col.type.value,
                        drop_value=d_col.type.value,
                        proposed_change=(
                            f"Column '{col_name}' changed type from {p_col.type.value} to "
                            f"{d_col.type.value}; update contract."
                        ),
                    )
                )

        # Nullability changes: pinned false -> drop true
        if not p_col.nullable and d_col.nullable:
            differences.append(
                DriftDifference(
                    column=col_name,
                    category=DriftCategory.NULLABILITY_CHANGE,
                    severity=DriftSeverity.WARN,
                    pinned_value=False,
                    drop_value=True,
                    proposed_change=(
                        f"Column '{col_name}' was non-nullable but contains nulls in drop; "
                        "update contract to nullable: true."
                    ),
                )
            )

    has_hold = any(diff.severity == DriftSeverity.HOLD for diff in differences)
    return SchemaDiff(
        vendor=pinned.vendor,
        pinned_version=pinned.version,
        has_hold=has_hold,
        differences=differences,
    )


def pin_schema(
    vendor: str,
    drop_path: Path | str,
    schema_dir: Path | str = "schemas",
    manifest_path: Path | str | None = None,
    money_columns: list[str] | None = None,
) -> Path:
    """Infer schema from drop and persist authoritative contract to schemas/<vendor>.yaml."""
    manifest: VendorManifest | None = None
    if manifest_path is not None:
        manifest = VendorManifest.from_file(manifest_path)
    else:
        std_manifest = Path(f"manifests/{vendor}.yaml")
        if std_manifest.exists():
            manifest = VendorManifest.from_file(std_manifest)

    inferred = infer_schema(
        drop_path=drop_path,
        vendor=vendor,
        manifest=manifest,
        money_columns=money_columns,
    )
    target = Path(schema_dir) / f"{vendor}.yaml"
    inferred.to_file(target)
    return target


def check_schema(
    vendor: str,
    drop_path: Path | str,
    schema_dir: Path | str = "schemas",
    manifest_path: Path | str | None = None,
    money_columns: list[str] | None = None,
) -> SchemaDiff:
    """Compare incoming drop against schemas/<vendor>.yaml and return classified diff."""
    schema_file = Path(schema_dir) / f"{vendor}.yaml"
    if not schema_file.exists():
        raise FileNotFoundError(f"Pinned schema not found for vendor: {vendor} at {schema_file}")

    pinned = VendorSchema.from_file(schema_file)
    manifest: VendorManifest | None = None
    if manifest_path is not None:
        manifest = VendorManifest.from_file(manifest_path)
    else:
        std_manifest = Path(f"manifests/{vendor}.yaml")
        if std_manifest.exists():
            manifest = VendorManifest.from_file(std_manifest)

    drop_schema = infer_schema(
        drop_path=drop_path,
        vendor=vendor,
        manifest=manifest,
        money_columns=money_columns,
    )
    return compare_schemas(pinned, drop_schema)


def accept_schema_changes(
    vendor: str,
    drop_path: Path | str | None = None,
    schema_dir: Path | str = "schemas",
    manifest_path: Path | str | None = None,
    money_columns: list[str] | None = None,
) -> VendorSchema:
    """Accept schema adjustments, updating schemas/<vendor>.yaml and incrementing version."""
    schema_file = Path(schema_dir) / f"{vendor}.yaml"
    if not schema_file.exists():
        raise FileNotFoundError(f"Pinned schema not found for vendor: {vendor} at {schema_file}")

    current = VendorSchema.from_file(schema_file)

    if drop_path is not None:
        manifest: VendorManifest | None = None
        if manifest_path is not None:
            manifest = VendorManifest.from_file(manifest_path)
        else:
            std_manifest = Path(f"manifests/{vendor}.yaml")
            if std_manifest.exists():
                manifest = VendorManifest.from_file(std_manifest)

        drop_schema = infer_schema(
            drop_path=drop_path,
            vendor=vendor,
            manifest=manifest,
            money_columns=money_columns,
        )
        diff = compare_schemas(current, drop_schema)
        if not diff.differences:
            return current

        # Apply differences
        new_columns = dict(current.columns)
        for d in diff.differences:
            if d.category == DriftCategory.ADDED_COLUMN:
                new_columns[d.column] = drop_schema.columns[d.column]
            elif d.category == DriftCategory.REMOVED_COLUMN:
                new_columns.pop(d.column, None)
            elif d.category in (DriftCategory.TYPE_WIDENED, DriftCategory.TYPE_NARROWED):
                new_columns[d.column] = drop_schema.columns[d.column]
            elif d.category == DriftCategory.NULLABILITY_CHANGE:
                existing = new_columns[d.column]
                new_columns[d.column] = ColumnSchema(
                    type=existing.type,
                    nullable=True,
                    distinct_count=existing.distinct_count,
                    min=existing.min,
                    max=existing.max,
                    precision=existing.precision,
                    scale=existing.scale,
                )

        updated = VendorSchema(
            vendor=vendor,
            version=current.version + 1,
            pinned_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
            columns=new_columns,
        )
        updated.to_file(schema_file)
        return updated

    # If no drop_path provided, directly increment version and update timestamp
    updated = VendorSchema(
        vendor=vendor,
        version=current.version + 1,
        pinned_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        columns=current.columns,
    )
    updated.to_file(schema_file)
    return updated
