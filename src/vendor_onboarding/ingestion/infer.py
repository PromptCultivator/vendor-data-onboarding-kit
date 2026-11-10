"""Statistical schema inference engine with single-pass columnar profiling."""

from __future__ import annotations

from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from vendor_onboarding.ingestion.reader import IngestReader
from vendor_onboarding.models.schema import (
    ColumnDefinition,
    DataType,
    SchemaDefinition,
)


def _map_duckdb_type(duckdb_type_str: str) -> tuple[DataType, str | None, str | None]:
    """Map DuckDB type string to DataType enum, format, and timezone."""
    raw = duckdb_type_str.upper().strip()

    if "TIME ZONE" in raw or "TIMESTAMPTZ" in raw:
        return DataType.TIMESTAMP, "%Y-%m-%dT%H:%M:%SZ", "UTC"
    if "TIMESTAMP" in raw:
        return DataType.TIMESTAMP, "%Y-%m-%dT%H:%M:%SZ", None
    if raw == "DATE":
        return DataType.DATE, "%Y-%m-%d", None
    if raw in ("BOOLEAN", "BOOL"):
        return DataType.BOOLEAN, None, None
    if raw in ("TINYINT", "INT1"):
        return DataType.INT8, None, None
    if raw in ("SMALLINT", "INT2"):
        return DataType.INT16, None, None
    if raw in ("INTEGER", "INT4", "INT"):
        return DataType.INT32, None, None
    if raw in ("BIGINT", "INT8", "HUGEINT"):
        return DataType.INT64, None, None
    if raw in ("FLOAT", "FLOAT4", "REAL"):
        return DataType.FLOAT32, None, None
    if raw in ("DOUBLE", "FLOAT8"):
        return DataType.FLOAT64, None, None
    if "DECIMAL" in raw or "NUMERIC" in raw:
        return DataType.DECIMAL, None, None
    if any(k in raw for k in ("STRUCT", "MAP", "LIST", "[]", "JSON")):
        return DataType.JSON, None, None
    if any(k in raw for k in ("VARCHAR", "TEXT", "STRING", "CHAR", "BLOB")):
        return DataType.STRING, None, None

    return DataType.UNKNOWN, None, None


def infer_schema(
    file_path: str | Path,
    sample_rows: int = 10000,
    dataset_id: str | None = None,
    vendor_id: str | None = None,
    primary_key: list[str] | None = None,
) -> SchemaDefinition:
    """Infer versioned schema contract from a vendor data file using columnar profiling.

    Profiles data types, nullability, cardinality, categorical domains, and primary key candidates
    in a single vectorized aggregation pass.
    """
    path = Path(file_path).resolve()
    if not path.is_file():
        raise FileNotFoundError(f"File not found for schema inference: {path}")

    # Use IngestReader without source line to profile raw vendor data
    reader = IngestReader(path, add_source_line=False)
    file_fmt = reader.file_format.upper()

    # Handle 0-byte files or empty JSON structures without generating phantom columns
    if path.stat().st_size == 0:
        return SchemaDefinition(
            dataset_id=dataset_id or path.stem.split(".")[0].upper(),
            vendor_id=vendor_id or "VENDOR",
            file_format=file_fmt,
            total_rows_sampled=0,
            columns={},
        )

    if reader.file_format in ("json", "jsonl"):
        with suppress(OSError, UnicodeDecodeError):
            raw_text = path.read_text(encoding="utf-8").strip()
            if raw_text in ("[]", "{}"):
                return SchemaDefinition(
                    dataset_id=dataset_id or path.stem.split(".")[0].upper(),
                    vendor_id=vendor_id or "VENDOR",
                    file_format=file_fmt,
                    total_rows_sampled=0,
                    columns={},
                )

    con = reader.con

    # Fast Parquet metadata check if applicable
    total_file_rows: int | None = None
    if reader.file_format == "parquet":
        try:
            meta = pq.read_metadata(path)
            total_file_rows = meta.num_rows
        except (OSError, pa.ArrowException):
            pass

    # Read sample
    base_rel = reader.to_relation()
    sample_rel = base_rel.limit(sample_rows)
    con.register("_sample_data", sample_rel)

    cols = sample_rel.columns
    if not cols:
        return SchemaDefinition(
            dataset_id=dataset_id or "UNKNOWN_DATASET",
            vendor_id=vendor_id or "UNKNOWN_VENDOR",
            file_format=file_fmt,
            total_rows_sampled=0,
            columns={},
        )

    # Single-pass SQL aggregation across all columns
    select_parts = ["COUNT(*) AS _total"]
    for i, col in enumerate(cols):
        escaped_col = col.replace('"', '""')
        select_parts.append(f'COUNT("{escaped_col}") AS _non_null_{i}')
        select_parts.append(f'COUNT(DISTINCT "{escaped_col}") AS _distinct_{i}')

    agg_sql = f"SELECT {', '.join(select_parts)} FROM _sample_data"
    agg_res = con.sql(agg_sql).fetchall()[0]
    total_sampled = int(agg_res[0])

    # Get physical types via DESCRIBE
    describe_rows = con.sql("DESCRIBE _sample_data").fetchall()
    physical_type_map = {r[0]: str(r[1]) for r in describe_rows}

    # Extract dataset_id and vendor_id from data if present and not provided
    inferred_dataset_id = dataset_id
    inferred_vendor_id = vendor_id

    if inferred_dataset_id is None and "dataset_id" in cols and total_sampled > 0:
        val_res = con.sql("SELECT dataset_id FROM _sample_data WHERE dataset_id IS NOT NULL LIMIT 1").fetchall()
        if val_res and val_res[0][0]:
            inferred_dataset_id = str(val_res[0][0])

    if inferred_vendor_id is None and "vendor_id" in cols and total_sampled > 0:
        val_res = con.sql("SELECT vendor_id FROM _sample_data WHERE vendor_id IS NOT NULL LIMIT 1").fetchall()
        if val_res and val_res[0][0]:
            inferred_vendor_id = str(val_res[0][0])

    if not inferred_dataset_id:
        # Fall back to file stem uppercase
        inferred_dataset_id = path.stem.split(".")[0].upper()
    if not inferred_vendor_id:
        inferred_vendor_id = "VENDOR"

    columns_def: dict[str, ColumnDefinition] = {}
    pk_candidates: list[str] = []

    for i, col_name in enumerate(cols):
        non_null_cnt = int(agg_res[1 + i * 2])
        distinct_cnt = int(agg_res[2 + i * 2])
        null_cnt = total_sampled - non_null_cnt
        null_pct = float(null_cnt / total_sampled) if total_sampled > 0 else 0.0
        phys_type = physical_type_map.get(col_name, "VARCHAR")
        data_type, date_format, tz = _map_duckdb_type(phys_type)

        nullable = null_cnt > 0
        is_numeric_float = data_type in (DataType.FLOAT32, DataType.FLOAT64, DataType.DECIMAL)
        is_pk_candidate = (
            not is_numeric_float
            and null_cnt == 0
            and distinct_cnt == total_sampled
            and total_sampled > 0
        )
        if is_pk_candidate:
            pk_candidates.append(col_name)

        # Categorical classification: distinct_count <= 25 or (non_null > 0 and ratio < 0.05)
        is_categorical = False
        categories: list[str] | None = None
        if data_type in (DataType.STRING, DataType.CATEGORICAL):
            ratio = (distinct_cnt / non_null_cnt) if non_null_cnt > 0 else 0.0
            if distinct_cnt <= 25 or (ratio < 0.05 and distinct_cnt <= 100):
                is_categorical = True
                escaped_col = col_name.replace('"', '""')
                cat_rows = con.sql(
                    f'SELECT DISTINCT "{escaped_col}" FROM _sample_data '
                    f'WHERE "{escaped_col}" IS NOT NULL ORDER BY 1 LIMIT 50'
                ).fetchall()
                categories = [str(r[0]) for r in cat_rows if r[0] is not None]

        col_def = ColumnDefinition(
            name=col_name,
            data_type=data_type,
            physical_type=phys_type,
            nullable=nullable,
            null_count=null_cnt,
            null_percentage=round(null_pct, 4),
            distinct_count=distinct_cnt,
            is_primary_key_candidate=is_pk_candidate,
            is_categorical=is_categorical,
            categories=categories,
            format=date_format,
            timezone=tz,
        )
        columns_def[col_name] = col_def

    # Determine primary key
    resolved_pk: list[str] = []
    if primary_key is not None:
        resolved_pk = list(primary_key)
    elif "record_id" in pk_candidates:
        resolved_pk = ["record_id"]
    elif pk_candidates:
        resolved_pk = [pk_candidates[0]]

    return SchemaDefinition(
        schema_version="1.0.0",
        dataset_id=inferred_dataset_id,
        vendor_id=inferred_vendor_id,
        created_at=datetime.now(UTC),
        total_rows_sampled=total_sampled,
        file_format=file_fmt,
        columns=columns_def,
        primary_key=resolved_pk,
        primary_key_candidates=pk_candidates,
        metadata={
            "file_name": path.name,
            "total_file_rows": total_file_rows or total_sampled,
        },
    )
