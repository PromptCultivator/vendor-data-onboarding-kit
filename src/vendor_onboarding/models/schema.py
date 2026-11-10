"""Schema definition, contract pinning, and drift detection engine."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class DataType(str, Enum):
    """Normalized logical data types for vendor onboarding schemas."""

    INT8 = "int8"
    INT16 = "int16"
    INT32 = "int32"
    INT64 = "int64"
    FLOAT32 = "float32"
    FLOAT64 = "float64"
    DECIMAL = "decimal"
    BOOLEAN = "boolean"
    STRING = "string"
    DATE = "date"
    TIMESTAMP = "timestamp"
    TIMESTAMP_TZ = "timestamp_tz"
    CATEGORICAL = "categorical"
    JSON = "json"
    UNKNOWN = "unknown"

    @classmethod
    def from_str(cls, val: str | DataType) -> DataType:
        """Parse raw string or enum into canonical DataType."""
        if isinstance(val, DataType):
            return val
        if not isinstance(val, str):
            return cls.UNKNOWN

        norm = val.strip().lower()
        mapping = {
            "int8": cls.INT8,
            "tinyint": cls.INT8,
            "int1": cls.INT8,
            "byte": cls.INT8,
            "int16": cls.INT16,
            "smallint": cls.INT16,
            "int2": cls.INT16,
            "short": cls.INT16,
            "int32": cls.INT32,
            "integer": cls.INT32,
            "int": cls.INT32,
            "int4": cls.INT32,
            "int64": cls.INT64,
            "bigint": cls.INT64,
            "long": cls.INT64,
            "float32": cls.FLOAT32,
            "float": cls.FLOAT32,
            "float4": cls.FLOAT32,
            "real": cls.FLOAT32,
            "float64": cls.FLOAT64,
            "double": cls.FLOAT64,
            "float8": cls.FLOAT64,
            "numeric": cls.DECIMAL,
            "decimal": cls.DECIMAL,
            "boolean": cls.BOOLEAN,
            "bool": cls.BOOLEAN,
            "string": cls.STRING,
            "str": cls.STRING,
            "varchar": cls.STRING,
            "text": cls.STRING,
            "char": cls.STRING,
            "date": cls.DATE,
            "timestamp": cls.TIMESTAMP,
            "datetime": cls.TIMESTAMP,
            "timestamp_ntz": cls.TIMESTAMP,
            "timestamp_tz": cls.TIMESTAMP_TZ,
            "timestamptz": cls.TIMESTAMP_TZ,
            "timestamp with time zone": cls.TIMESTAMP_TZ,
            "categorical": cls.CATEGORICAL,
            "category": cls.CATEGORICAL,
            "json": cls.JSON,
            "jsonb": cls.JSON,
            "struct": cls.JSON,
            "map": cls.JSON,
            "list": cls.JSON,
            "array": cls.JSON,
        }

        # Handle parameterized types like timestamp[us], decimal(18,4)
        if norm.startswith("timestamp") and ("tz" in norm or "zone" in norm):
            return cls.TIMESTAMP_TZ
        if norm.startswith("timestamp"):
            return cls.TIMESTAMP
        if norm.startswith(("decimal", "numeric")):
            return cls.DECIMAL
        if norm.startswith(("varchar", "char")):
            return cls.STRING

        return mapping.get(norm, cls.UNKNOWN)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, DataType):
            return self.value == other.value
        if isinstance(other, str):
            try:
                resolved = DataType.from_str(other)
                return self.value == resolved.value
            except (ValueError, TypeError, AttributeError):
                return self.value == other.strip().lower()
        return False

    def __hash__(self) -> int:
        return hash(self.value)


# Safe type widening graph: source -> set of allowed wider targets
SAFE_WIDENING_MAP: dict[DataType, set[DataType]] = {
    DataType.INT8: {
        DataType.INT16,
        DataType.INT32,
        DataType.INT64,
        DataType.FLOAT32,
        DataType.FLOAT64,
        DataType.DECIMAL,
    },
    DataType.INT16: {
        DataType.INT32,
        DataType.INT64,
        DataType.FLOAT32,
        DataType.FLOAT64,
        DataType.DECIMAL,
    },
    DataType.INT32: {DataType.INT64, DataType.FLOAT64, DataType.DECIMAL},
    DataType.INT64: {DataType.FLOAT64, DataType.DECIMAL},
    DataType.FLOAT32: {DataType.FLOAT64},
    DataType.DATE: {DataType.TIMESTAMP, DataType.TIMESTAMP_TZ},
    DataType.TIMESTAMP: {DataType.TIMESTAMP_TZ},
    DataType.CATEGORICAL: {DataType.STRING},
}


class ColumnDefinition(BaseModel):
    """Specification of a single schema column."""

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    name: str = ""
    data_type: DataType = DataType.UNKNOWN
    physical_type: str | None = None
    nullable: bool = True
    null_count: int | None = None
    null_percentage: float | None = None
    distinct_count: int | None = None
    is_primary_key_candidate: bool = False
    is_categorical: bool = False
    categories: list[str] | None = None
    format: str | None = None
    timezone: str | None = None
    description: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _parse_input(cls, data: Any) -> Any:
        if isinstance(data, dict) and "data_type" not in data and "type" in data:
            data["data_type"] = data["type"]
        return data

    @field_validator("data_type", mode="before")
    @classmethod
    def _parse_data_type(cls, val: Any) -> DataType:
        return DataType.from_str(val)

    @property
    def type(self) -> DataType:
        """Alias for data_type matching JSON representation."""
        return self.data_type

    def to_dict(self) -> dict[str, Any]:
        """Serialize column definition to dictionary."""
        d: dict[str, Any] = {
            "name": self.name,
            "type": self.data_type.value,
            "nullable": self.nullable,
        }
        if self.physical_type is not None:
            d["physical_type"] = self.physical_type
        if self.null_count is not None:
            d["null_count"] = self.null_count
        if self.null_percentage is not None:
            d["null_percentage"] = self.null_percentage
        if self.distinct_count is not None:
            d["distinct_count"] = self.distinct_count
        if self.is_primary_key_candidate:
            d["is_primary_key_candidate"] = self.is_primary_key_candidate
        if self.is_categorical:
            d["is_categorical"] = self.is_categorical
        if self.categories is not None:
            d["categories"] = self.categories
        if self.format is not None:
            d["format"] = self.format
        if self.timezone is not None:
            d["timezone"] = self.timezone
        if self.description is not None:
            d["description"] = self.description
        return d


class SchemaDefinition(BaseModel):
    """Versioned schema contract defining dataset columns, types, and primary keys."""

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    schema_version: str = "1.0.0"
    dataset_id: str
    vendor_id: str
    created_at: datetime | None = None
    total_rows_sampled: int | None = None
    file_format: str | None = None
    columns: dict[str, ColumnDefinition] = Field(default_factory=dict)
    primary_key: list[str] = Field(default_factory=list)
    primary_key_candidates: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _normalize_columns(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        raw_cols = data.get("columns", {})
        norm_cols: dict[str, Any] = {}

        if isinstance(raw_cols, list):
            for col in raw_cols:
                if isinstance(col, ColumnDefinition):
                    norm_cols[col.name] = col
                elif isinstance(col, dict):
                    col_name = col.get("name", "")
                    norm_cols[col_name] = col
        elif isinstance(raw_cols, dict):
            for col_name, col_data in raw_cols.items():
                if isinstance(col_data, ColumnDefinition):
                    if not col_data.name:
                        col_data.name = col_name
                    norm_cols[col_name] = col_data
                elif isinstance(col_data, dict):
                    col_copy = dict(col_data)
                    if "name" not in col_copy or not col_copy["name"]:
                        col_copy["name"] = col_name
                    norm_cols[col_name] = col_copy
                else:
                    norm_cols[col_name] = col_data

        data["columns"] = norm_cols
        return data

    @property
    def column_names(self) -> list[str]:
        """Ordered list of column names."""
        return list(self.columns.keys())

    def get_column(self, name: str) -> ColumnDefinition | None:
        """Retrieve column definition by name (case-insensitive fallback)."""
        if name in self.columns:
            return self.columns[name]
        lowered = name.lower()
        for k, col in self.columns.items():
            if k.lower() == lowered:
                return col
        return None

    def to_dict(self) -> dict[str, Any]:
        """Serialize schema to dictionary adhering to standard schema contract."""
        cols_dict = {}
        for name, col in self.columns.items():
            c_dict: dict[str, Any] = {
                "type": col.data_type.value,
                "nullable": col.nullable,
            }
            if col.physical_type:
                c_dict["physical_type"] = col.physical_type
            if col.null_count is not None:
                c_dict["null_count"] = col.null_count
            if col.null_percentage is not None:
                c_dict["null_percentage"] = col.null_percentage
            if col.distinct_count is not None:
                c_dict["distinct_count"] = col.distinct_count
            if col.is_primary_key_candidate:
                c_dict["is_primary_key_candidate"] = col.is_primary_key_candidate
            if col.is_categorical:
                c_dict["is_categorical"] = col.is_categorical
            if col.categories is not None:
                c_dict["categories"] = col.categories
            if col.format:
                c_dict["format"] = col.format
            if col.timezone:
                c_dict["timezone"] = col.timezone
            if col.description:
                c_dict["description"] = col.description
            cols_dict[name] = c_dict

        res: dict[str, Any] = {
            "schema_version": self.schema_version,
            "dataset_id": self.dataset_id,
            "vendor_id": self.vendor_id,
            "primary_key": self.primary_key,
            "columns": cols_dict,
        }
        if self.created_at is not None:
            res["created_at"] = self.created_at.isoformat()
        if self.total_rows_sampled is not None:
            res["total_rows_sampled"] = self.total_rows_sampled
        if self.file_format is not None:
            res["file_format"] = self.file_format
        if self.primary_key_candidates:
            res["primary_key_candidates"] = self.primary_key_candidates
        if self.metadata:
            res["metadata"] = self.metadata
        return res

    def to_json(self, indent: int = 2) -> str:
        """Serialize schema to formatted JSON string."""
        return json.dumps(self.to_dict(), indent=indent, default=str)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SchemaDefinition:
        """Deserialize SchemaDefinition from dictionary."""
        return cls.model_validate(data)

    @classmethod
    def from_json(cls, json_str: str) -> SchemaDefinition:
        """Deserialize SchemaDefinition from JSON string."""
        data = json.loads(json_str)
        return cls.from_dict(data)


class DriftSeverity(str, Enum):
    """Severity classification of schema drift."""

    BREAKING = "BREAKING"
    WARNING = "WARNING"
    INFO = "INFO"


class DriftType(str, Enum):
    """Categorical type of schema drift."""

    MISSING_COLUMN = "MISSING_COLUMN"
    TYPE_INCOMPATIBLE = "TYPE_INCOMPATIBLE"
    NULLABILITY_VIOLATION = "NULLABILITY_VIOLATION"
    NULLABILITY_CHANGED = "NULLABILITY_CHANGED"
    PRIMARY_KEY_CHANGED = "PRIMARY_KEY_CHANGED"
    PRIMARY_KEY_VIOLATION = "PRIMARY_KEY_VIOLATION"
    CASE_MISMATCH = "CASE_MISMATCH"
    NEW_COLUMN = "NEW_COLUMN"
    TYPE_WIDENING = "TYPE_WIDENING"
    NEW_CATEGORICAL_VALUES = "NEW_CATEGORICAL_VALUES"
    COLUMN_REORDERED = "COLUMN_REORDERED"
    METADATA_CHANGED = "METADATA_CHANGED"


class DriftIssue(BaseModel):
    """An individual schema drift issue."""

    column_name: str | None = None
    drift_type: DriftType
    severity: DriftSeverity
    message: str
    expected: Any = None
    actual: Any = None

    @property
    def column(self) -> str | None:
        """Alias for column_name."""
        return self.column_name

    @column.setter
    def column(self, value: str | None) -> None:
        self.column_name = value


class DriftReport(BaseModel):
    """Comprehensive schema drift evaluation report."""

    issues: list[DriftIssue] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    pinned_schema_version: str | None = None

    @property
    def is_breaking(self) -> bool:
        """True if any issue has BREAKING severity."""
        return any(i.severity == DriftSeverity.BREAKING for i in self.issues)

    @property
    def breaking_count(self) -> int:
        """Count of BREAKING drift issues."""
        return sum(1 for i in self.issues if i.severity == DriftSeverity.BREAKING)

    @property
    def warning_count(self) -> int:
        """Count of WARNING drift issues."""
        return sum(1 for i in self.issues if i.severity == DriftSeverity.WARNING)

    @property
    def info_count(self) -> int:
        """Count of INFO drift issues."""
        return sum(1 for i in self.issues if i.severity == DriftSeverity.INFO)

    def summary(self) -> dict[str, Any]:
        """Structured dictionary summary of drift report."""
        return {
            "is_breaking": self.is_breaking,
            "breaking_count": self.breaking_count,
            "warning_count": self.warning_count,
            "info_count": self.info_count,
            "total_issues": len(self.issues),
            "pinned_schema_version": self.pinned_schema_version,
            "timestamp": self.timestamp.isoformat(),
        }

    def to_markdown_table(self) -> str:
        """Format drift issues into a GitHub-flavored Markdown table."""
        if not self.issues:
            return "No schema drift detected. Schema matches pinned contract perfectly."

        lines = [
            "| Column | Severity | Drift Type | Message | Expected | Actual |",
            "|:---|:---|:---|:---|:---|:---|",
        ]
        for issue in self.issues:
            col = issue.column_name or "-"
            exp = str(issue.expected) if issue.expected is not None else "-"
            act = str(issue.actual) if issue.actual is not None else "-"
            # Escape pipe characters in message
            msg = issue.message.replace("|", "\\|")
            lines.append(
                f"| `{col}` | **{issue.severity.value}** | `{issue.drift_type.value}` | "
                f"{msg} | {exp} | {act} |"
            )
        return "\n".join(lines)


def pin_schema(schema: SchemaDefinition, output_path: str | Path) -> None:
    """Save schema contract to specified JSON file path."""
    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(schema.to_json(), encoding="utf-8")


def load_schema(schema_path: str | Path) -> SchemaDefinition:
    """Load and parse schema contract from JSON file path."""
    path = Path(schema_path)
    if not path.is_file():
        raise FileNotFoundError(f"Pinned schema file not found at: {path}")
    content = path.read_text(encoding="utf-8")
    return SchemaDefinition.from_json(content)


def detect_drift(
    current_schema: SchemaDefinition,
    pinned_schema_path: str | Path | SchemaDefinition,
) -> DriftReport:
    """Compare current schema against pinned schema contract and generate drift report.

    Classification rules:
    - BREAKING: missing column, incompatible type change (narrowing, type mismatch),
      nullability violation on non-null column with actual nulls, primary key uniqueness
      or composition violation, column casing mismatch.
    - WARNING: new column added, safe type widening (e.g. int32 -> int64, float32 -> float64,
      categorical -> string, date -> timestamp), nullability relaxed (non-null to nullable),
      new categorical values.
    - INFO: column reordering, metadata changes.
    """
    if isinstance(pinned_schema_path, SchemaDefinition):
        pinned = pinned_schema_path
    else:
        pinned = load_schema(pinned_schema_path)

    issues: list[DriftIssue] = []

    # Map column names (exact and lowercased)
    pinned_cols = pinned.columns
    curr_cols = current_schema.columns

    pinned_lower_map = {k.lower(): k for k in pinned_cols}
    curr_lower_map = {k.lower(): k for k in curr_cols}

    # 1. Check for Missing Columns (in pinned, missing in current)
    for p_name, p_col in pinned_cols.items():
        if p_name not in curr_cols:
            if p_name.lower() in curr_lower_map:
                # Column name casing mismatch
                actual_name = curr_lower_map[p_name.lower()]
                issues.append(
                    DriftIssue(
                        column_name=p_name,
                        drift_type=DriftType.CASE_MISMATCH,
                        severity=DriftSeverity.BREAKING,
                        message=(
                            f"Column name casing mismatch: expected '{p_name}', got '{actual_name}'"
                        ),
                        expected=p_name,
                        actual=actual_name,
                    )
                )
            else:
                # Missing column
                issues.append(
                    DriftIssue(
                        column_name=p_name,
                        drift_type=DriftType.MISSING_COLUMN,
                        severity=DriftSeverity.BREAKING,
                        message=f"Pinned column '{p_name}' is missing in incoming schema",
                        expected=p_col.data_type.value,
                        actual=None,
                    )
                )

    # 2. Check for New Columns (in current, not in pinned)
    for c_name, c_col in curr_cols.items():
        if c_name not in pinned_cols and c_name.lower() not in pinned_lower_map:
            issues.append(
                DriftIssue(
                    column_name=c_name,
                    drift_type=DriftType.NEW_COLUMN,
                    severity=DriftSeverity.WARNING,
                    message=f"New column '{c_name}' detected not present in pinned schema",
                    expected=None,
                    actual=c_col.data_type.value,
                )
            )

    # 3. Check Common Columns for Type, Nullability, and Categorical drift
    for p_name, p_col in pinned_cols.items():
        if p_name in curr_cols:
            c_col = curr_cols[p_name]

            # 3a. Type checks
            if p_col.data_type != c_col.data_type:
                allowed_widening = SAFE_WIDENING_MAP.get(p_col.data_type, set())
                if c_col.data_type in allowed_widening:
                    issues.append(
                        DriftIssue(
                            column_name=p_name,
                            drift_type=DriftType.TYPE_WIDENING,
                            severity=DriftSeverity.WARNING,
                            message=(
                                f"Safe type widening for '{p_name}': "
                                f"{p_col.data_type.value} -> {c_col.data_type.value}"
                            ),
                            expected=p_col.data_type.value,
                            actual=c_col.data_type.value,
                        )
                    )
                else:
                    issues.append(
                        DriftIssue(
                            column_name=p_name,
                            drift_type=DriftType.TYPE_INCOMPATIBLE,
                            severity=DriftSeverity.BREAKING,
                            message=(
                                f"Incompatible type change for '{p_name}': "
                                f"{p_col.data_type.value} -> {c_col.data_type.value}"
                            ),
                            expected=p_col.data_type.value,
                            actual=c_col.data_type.value,
                        )
                    )

            # 3b. Nullability checks
            # Case 1: Pinned is non-null (nullable=False), but incoming data contains nulls
            if not p_col.nullable and c_col.null_count is not None and c_col.null_count > 0:
                issues.append(
                    DriftIssue(
                        column_name=p_name,
                        drift_type=DriftType.NULLABILITY_VIOLATION,
                        severity=DriftSeverity.BREAKING,
                        message=(
                            f"Non-nullable column '{p_name}' contains "
                            f"{c_col.null_count} null values in incoming data"
                        ),
                        expected="nullable=False (0 nulls)",
                        actual=f"{c_col.null_count} nulls",
                    )
                )
            # Case 2: Pinned is non-null (nullable=False), but current contract declares nullable=True
            elif not p_col.nullable and c_col.nullable:
                # If it's part of the pinned primary key, relaxing nullability is BREAKING
                if p_name in pinned.primary_key:
                    issues.append(
                        DriftIssue(
                            column_name=p_name,
                            drift_type=DriftType.NULLABILITY_VIOLATION,
                            severity=DriftSeverity.BREAKING,
                            message=(
                                f"Primary key column '{p_name}' was changed to nullable"
                            ),
                            expected="nullable=False",
                            actual="nullable=True",
                        )
                    )
                else:
                    issues.append(
                        DriftIssue(
                            column_name=p_name,
                            drift_type=DriftType.NULLABILITY_CHANGED,
                            severity=DriftSeverity.WARNING,
                            message=f"Column '{p_name}' nullability relaxed (non-null -> nullable)",
                            expected="nullable=False",
                            actual="nullable=True",
                        )
                    )
            # Case 3: Pinned was nullable, current is non-nullable (contract strengthened)
            elif p_col.nullable and not c_col.nullable:
                issues.append(
                    DriftIssue(
                        column_name=p_name,
                        drift_type=DriftType.NULLABILITY_CHANGED,
                        severity=DriftSeverity.INFO,
                        message=f"Column '{p_name}' nullability tightened (nullable -> non-null)",
                        expected="nullable=True",
                        actual="nullable=False",
                    )
                )

            # 3c. Categorical values check
            if p_col.categories and c_col.categories:
                pinned_cats = set(p_col.categories)
                curr_cats = set(c_col.categories)
                new_cats = curr_cats - pinned_cats
                if new_cats:
                    issues.append(
                        DriftIssue(
                            column_name=p_name,
                            drift_type=DriftType.NEW_CATEGORICAL_VALUES,
                            severity=DriftSeverity.WARNING,
                            message=(
                                f"New categorical values detected in '{p_name}': "
                                f"{sorted(new_cats)}"
                            ),
                            expected=sorted(pinned_cats),
                            actual=sorted(curr_cats),
                        )
                    )

    # 4. Primary Key checks
    if pinned.primary_key:
        if current_schema.primary_key and pinned.primary_key != current_schema.primary_key:
            issues.append(
                DriftIssue(
                    column_name=None,
                    drift_type=DriftType.PRIMARY_KEY_CHANGED,
                    severity=DriftSeverity.BREAKING,
                    message=(
                        f"Primary key definition changed from "
                        f"{pinned.primary_key} to {current_schema.primary_key}"
                    ),
                    expected=pinned.primary_key,
                    actual=current_schema.primary_key,
                )
            )

        # Check if any pinned primary key column has duplicates or nulls
        for pk_col_name in pinned.primary_key:
            pk_col = curr_cols.get(pk_col_name)
            if pk_col and pk_col.null_count is not None and pk_col.null_count > 0:
                issues.append(
                    DriftIssue(
                        column_name=pk_col_name,
                        drift_type=DriftType.PRIMARY_KEY_VIOLATION,
                        severity=DriftSeverity.BREAKING,
                        message=(
                            f"Primary key column '{pk_col_name}' contains "
                            f"{pk_col.null_count} nulls"
                        ),
                        expected=0,
                        actual=pk_col.null_count,
                    )
                )
            # For single-column primary keys, check for duplicate values
            if (
                len(pinned.primary_key) == 1
                and pk_col
                and pk_col.distinct_count is not None
                and current_schema.total_rows_sampled is not None
                and current_schema.total_rows_sampled > 0
                and pk_col.distinct_count < current_schema.total_rows_sampled
            ):
                dups = current_schema.total_rows_sampled - pk_col.distinct_count
                issues.append(
                    DriftIssue(
                        column_name=pk_col_name,
                        drift_type=DriftType.PRIMARY_KEY_VIOLATION,
                        severity=DriftSeverity.BREAKING,
                        message=(
                            f"Primary key column '{pk_col_name}' contains duplicate values "
                            f"({pk_col.distinct_count} distinct in {current_schema.total_rows_sampled} rows, {dups} duplicates)"
                        ),
                        expected=current_schema.total_rows_sampled,
                        actual=pk_col.distinct_count,
                    )
                )

    # 5. Check column ordering
    common_pinned = [k for k in pinned_cols if k in curr_cols]
    common_curr = [k for k in curr_cols if k in pinned_cols]
    if common_pinned != common_curr:
        issues.append(
            DriftIssue(
                column_name=None,
                drift_type=DriftType.COLUMN_REORDERED,
                severity=DriftSeverity.INFO,
                message="Column order has changed relative to pinned schema",
                expected=common_pinned,
                actual=common_curr,
            )
        )

    # 6. Metadata changes
    if pinned.metadata != current_schema.metadata and current_schema.metadata:
        issues.append(
            DriftIssue(
                column_name=None,
                drift_type=DriftType.METADATA_CHANGED,
                severity=DriftSeverity.INFO,
                message="Dataset metadata has changed",
                expected=pinned.metadata,
                actual=current_schema.metadata,
            )
        )

    return DriftReport(
        issues=issues,
        timestamp=datetime.now(UTC),
        pinned_schema_version=pinned.schema_version,
    )
