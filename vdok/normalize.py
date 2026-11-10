"""Canonical row model and deterministic normalizers.

Defines the Apache Arrow canonical storage schema, the Pydantic CanonicalRow
record model, exact fixed-point decimal parsing that strictly rejects binary
floating-point representations, UTC timezone standardization, standard unit
mapping with percentage-to-ratio scaling, string identifier whitespace and
case normalization, cryptographic SHA-256 row fingerprints, and drop batch
normalization.
"""

from __future__ import annotations

import datetime
import gzip
import hashlib
import json
import zoneinfo
from datetime import timezone
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from vdok.discover import Compression, FileFormat, scan
from vdok.manifest import EntityIdType, KnowledgeTimeSource, VendorManifest
from vdok.symbols import SymbolMaster

# 1. Canonical Arrow Schema (12 canonical fields plus unmapped)
CANONICAL_ARROW_SCHEMA = pa.schema(
    [
        pa.field("entity_id", pa.string(), nullable=False),
        pa.field("entity_id_type", pa.string(), nullable=False),
        pa.field("field", pa.string(), nullable=False),
        pa.field("event_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("knowledge_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("value_num", pa.decimal128(38, 9), nullable=True),
        pa.field("value_str", pa.string(), nullable=True),
        pa.field("unit", pa.string(), nullable=True),
        pa.field("drop_id", pa.string(), nullable=False),
        pa.field("source_file", pa.string(), nullable=False),
        pa.field("source_row", pa.int64(), nullable=False),
        pa.field("row_hash", pa.string(), nullable=False),
        pa.field("unmapped", pa.bool_(), nullable=False),
    ]
)

# 2. Standard Unit Mapping Tables
UNIT_MAPPINGS: dict[str, str] = {
    # Currency
    "usd": "USD",
    "us$": "USD",
    "dollar": "USD",
    "dollars": "USD",
    "$": "USD",
    # Shares
    "shares": "shares",
    "share": "shares",
    "shs": "shares",
    # Dimensionless
    "pure": "pure",
    "scalar": "pure",
    "dimensionless": "pure",
    "none": "pure",
    "count": "pure",
    # Ratios
    "%": "ratio",
    "percent": "ratio",
    "pct": "ratio",
    "percentage": "ratio",
    "ratio": "ratio",
}

PERCENT_UNITS = frozenset({"%", "percent", "pct", "percentage"})


# 3. Pure Normalization Functions
def parse_decimal_exact(raw_val: Any) -> Decimal | None:
    """Parse exact fixed-point Decimal, strictly rejecting binary float types.

    Raises TypeError on binary float instances to prevent precision loss on monetary data.
    Returns None if raw_val is None or empty string.
    Raises ValueError on non-finite (NaN, infinite) or malformed representations.
    """
    if raw_val is None:
        return None
    if isinstance(raw_val, float):
        raise TypeError(
            f"Binary floating-point value {raw_val!r} rejected for monetary field; "
            "exact string, int, or Decimal input required."
        )
    if isinstance(raw_val, Decimal):
        if raw_val.is_nan() or raw_val.is_infinite():
            raise ValueError(f"Non-finite decimal value rejected: {raw_val}")
        return raw_val
    if isinstance(raw_val, int):
        return Decimal(raw_val)

    if not isinstance(raw_val, (str, bytes)):
        raise TypeError(f"Unsupported type for exact decimal parsing: {type(raw_val)}")

    val_str = raw_val.decode("utf-8") if isinstance(raw_val, bytes) else raw_val
    val_str = val_str.strip()
    if val_str == "":
        return None
    # Strip standard thousands grouping commas if followed by decimal point
    if "," in val_str and "." in val_str:
        val_str = val_str.replace(",", "")
    try:
        dec = Decimal(val_str)
        if dec.is_nan() or dec.is_infinite():
            raise ValueError(f"Non-finite decimal value rejected: {raw_val}")
        return dec
    except InvalidOperation as err:
        raise ValueError(f"Malformed decimal literal: {raw_val!r}") from err


def parse_timestamp_utc(
    raw_val: Any,
    fmt: str | None = None,
    source_tz: str = "UTC",
) -> datetime.datetime:
    """Parse date or timestamp, attach source timezone if naive, and convert to UTC.

    Rejects floating-point timestamps. Supports datetime, date, integer representations,
    and formatted string inputs.
    """
    if raw_val is None:
        raise ValueError("Cannot parse None as timestamp")
    if isinstance(raw_val, float):
        raise TypeError(f"Binary floating-point timestamp rejected: {raw_val}")
    if isinstance(raw_val, datetime.datetime):
        if raw_val.tzinfo is None:
            localized = raw_val.replace(tzinfo=zoneinfo.ZoneInfo(source_tz))
        else:
            localized = raw_val
        return localized.astimezone(timezone.utc)
    if isinstance(raw_val, datetime.date):
        dt = datetime.datetime.combine(raw_val, datetime.time.min)
        localized = dt.replace(tzinfo=zoneinfo.ZoneInfo(source_tz))
        return localized.astimezone(timezone.utc)
    if isinstance(raw_val, int):
        raw_val = str(raw_val)

    s = str(raw_val).strip()
    if not s:
        raise ValueError("Timestamp string cannot be empty")

    if fmt:
        try:
            parsed = datetime.datetime.strptime(s, fmt)
        except ValueError:
            try:
                parsed = datetime.datetime.fromisoformat(s)
            except ValueError:
                from dateutil import parser as dt_parser

                parsed = dt_parser.parse(s)
    else:
        try:
            parsed = datetime.datetime.fromisoformat(s)
        except ValueError:
            from dateutil import parser as dt_parser

            parsed = dt_parser.parse(s)

    if parsed.tzinfo is None:
        localized = parsed.replace(tzinfo=zoneinfo.ZoneInfo(source_tz))
    else:
        localized = parsed
    return localized.astimezone(timezone.utc)


def normalize_unit(raw_unit: str | None, strict: bool = False) -> str | None:
    """Normalize unit string against the standard unit classification table."""
    if raw_unit is None:
        return None
    stripped = raw_unit.strip()
    if not stripped:
        return None
    lowered = stripped.lower()
    if lowered in UNIT_MAPPINGS:
        return UNIT_MAPPINGS[lowered]
    if strict:
        raise ValueError(f"Unrecognized unit of measure: {raw_unit!r}")
    return stripped


def normalize_unit_and_value(
    value_num: Decimal | None,
    raw_unit: str | None,
) -> tuple[Decimal | None, str | None]:
    """Normalize unit code and apply percentage scaling (/ 100) to value_num."""
    if raw_unit is None:
        return value_num, None
    stripped = raw_unit.strip()
    if not stripped:
        return value_num, None
    lowered = stripped.lower()
    if lowered in PERCENT_UNITS:
        canonical_unit = "ratio"
        scaled_val = (value_num / Decimal(100)) if value_num is not None else None
        return scaled_val, canonical_unit
    canonical_unit = UNIT_MAPPINGS.get(lowered, stripped)
    return value_num, canonical_unit


def normalize_identifier(
    raw_id: str,
    id_type: str | EntityIdType | None = None,
) -> str:
    """Normalize entity identifier with whitespace stripping and case folding."""
    cleaned = raw_id.strip()
    if not cleaned:
        raise ValueError("Entity identifier cannot be empty")
    type_str = id_type.value if isinstance(id_type, EntityIdType) else (id_type or "").lower()
    if type_str == "cik":
        digits = "".join(ch for ch in cleaned if ch.isdigit())
        if not digits:
            raise ValueError(f"Invalid CIK identifier (no digits found): {raw_id!r}")
        return digits.zfill(10)
    elif type_str in ("ticker", "isin", "cusip"):
        return cleaned.upper()
    return cleaned


def compute_row_hash(
    entity_id: str,
    field: str,
    event_time: datetime.datetime,
    value_num: Decimal | None,
    value_str: str | None,
    unit: str | None,
) -> str:
    """Compute deterministic SHA-256 fingerprint for canonical row.

    Uses null byte (\0) delimiter across canonical fields. Unmapped status is excluded
    from the row hash to maintain fingerprint stability.
    """
    if event_time.tzinfo is None:
        raise ValueError("event_time must be timezone-aware UTC datetime")
    event_time_utc = event_time.astimezone(timezone.utc)
    ts_str = event_time_utc.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    num_str = f"{value_num:.9f}" if value_num is not None else ""
    str_val = value_str if value_str is not None else ""
    unit_val = unit if unit is not None else ""
    payload = f"{entity_id}\0{field}\0{ts_str}\0{num_str}\0{str_val}\0{unit_val}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# 4. Pydantic Canonical Record Model
class CanonicalRow(BaseModel):
    """Canonical bitemporal record conforming to the platform storage specification."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    entity_id: str = Field(..., min_length=1)
    entity_id_type: str = Field(..., min_length=1)
    field: str = Field(..., min_length=1)
    event_time: datetime.datetime
    knowledge_time: datetime.datetime
    value_num: Decimal | None = None
    value_str: str | None = None
    unit: str | None = None
    drop_id: str = Field(..., pattern=r"^[0-9a-f]{16}$")
    source_file: str = Field(..., min_length=1)
    source_row: int = Field(..., ge=1)
    row_hash: str = Field(default="", pattern=r"^([0-9a-f]{64})?$")
    unmapped: bool = False

    @field_validator("value_num", mode="before")
    @classmethod
    def validate_value_num(cls, v: Any) -> Decimal | None:
        return parse_decimal_exact(v)

    @field_validator("event_time", "knowledge_time", mode="after")
    @classmethod
    def validate_utc_datetime(cls, v: datetime.datetime) -> datetime.datetime:
        if v.tzinfo is None:
            raise ValueError("Timestamp must be timezone-aware UTC datetime")
        return v.astimezone(timezone.utc)

    @model_validator(mode="after")
    def verify_or_compute_row_hash(self) -> CanonicalRow:
        expected = compute_row_hash(
            entity_id=self.entity_id,
            field=self.field,
            event_time=self.event_time,
            value_num=self.value_num,
            value_str=self.value_str,
            unit=self.unit,
        )
        if self.row_hash and self.row_hash != expected:
            raise ValueError(f"row_hash mismatch: provided {self.row_hash}, expected {expected}")
        self.row_hash = expected
        return self


def _quote_ident(name: str) -> str:
    """Quote SQL identifier safely for DuckDB."""
    return '"' + name.replace('"', '""') + '"'


def _build_ts_sql(col_ident: str, fmt: str | None) -> str:
    """Build UTC timestamp SQL expression matching parse_timestamp_utc."""
    candidates: list[str] = []
    if fmt:
        fmt_esc = fmt.replace("'", "''")
        candidates.append(f"try_strptime(trim({col_ident}::VARCHAR), '{fmt_esc}')::TIMESTAMPTZ")
    candidates.append(f"TRY_CAST(trim({col_ident}::VARCHAR) AS TIMESTAMPTZ)")
    return f"COALESCE({', '.join(candidates)})"


def _normalize_drop_vectorized(
    drop_meta: Any,
    manifest: VendorManifest,
    drop_id: str,
    symbol_master: SymbolMaster | None,
    default_drop_kt: datetime.datetime,
) -> pa.Table | None:
    """Vectorized DuckDB normalization fast-path for UTC CSV and Parquet drops.

    Returns None when custom non-UTC timezone or non-ISO dateutil fallback is needed.
    """
    if manifest.timezone.upper() not in ("UTC", "ETC/UTC", "Z", "GMT"):
        return None
    if drop_meta.file_format not in (FileFormat.CSV, FileFormat.PARQUET):
        return None

    if drop_meta.file_format == FileFormat.PARQUET:
        for df in drop_meta.files:
            pq_schema = pq.read_schema(df.path)
            for vc in manifest.value_columns:
                if vc in pq_schema.names and pa.types.is_floating(pq_schema.field(vc).type):
                    return None

    tables: list[pa.Table] = []
    default_kt_iso = default_drop_kt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f+00")
    drop_id_esc = drop_id.replace("'", "''")
    id_type_esc = manifest.entity_id_type.value.replace("'", "''")

    for disc_file in drop_meta.files:
        f_path = disc_file.path
        rel_esc = disc_file.relative_path.replace("'", "''")
        path_esc = str(f_path).replace("'", "''")

        con = duckdb.connect()
        try:
            con.execute("SET TimeZone = 'UTC';")
            if disc_file.format == FileFormat.CSV:
                src_sql = f"read_csv('{path_esc}', all_varchar=True)"
            else:
                src_sql = f"read_parquet('{path_esc}')"

            avail_cols = set(con.sql(f"SELECT * FROM {src_sql} LIMIT 0").columns)
            ent_cols = [c for c in manifest.entity_columns if c in avail_cols]
            if not ent_cols or manifest.event_time_column not in avail_cols:
                continue

            if manifest.knowledge_time.source == KnowledgeTimeSource.COLUMN:
                kt_col = manifest.knowledge_time.column
                if not kt_col or kt_col not in avail_cols:
                    continue
            else:
                kt_col = None

            ent_parts = [f"NULLIF(trim({_quote_ident(c)}::VARCHAR), '')" for c in ent_cols]
            ent_expr = f"COALESCE({', '.join(ent_parts)})" if len(ent_parts) > 1 else ent_parts[0]

            ev_ident = _quote_ident(manifest.event_time_column)
            ev_ts_expr = _build_ts_sql(ev_ident, manifest.event_time_format)

            if kt_col is not None:
                kt_ident = _quote_ident(kt_col)
                kt_ts_expr = _build_ts_sql(kt_ident, manifest.knowledge_time.format)
                kt_null_filter = f"{kt_ident} IS NOT NULL"
                kt_fail_check = (
                    f"({kt_ident} IS NOT NULL AND trim({kt_ident}::VARCHAR) != '' "
                    f"AND {kt_ts_expr} IS NULL)"
                )
            else:
                kt_ts_expr = f"'{default_kt_iso}'::TIMESTAMPTZ"
                kt_null_filter = "TRUE"
                kt_fail_check = "FALSE"

            con.execute(
                f"""
                CREATE TEMP TABLE raw_numbered AS
                SELECT *, row_number() OVER () AS _vdok_src_row, {ent_expr} AS _vdok_raw_ent
                FROM {src_sql}
                """
            )

            # Check if any non-empty timestamp requires Python dateutil fallback
            ts_fail_rows = con.sql(
                f"""
                SELECT count(*) FROM raw_numbered
                WHERE _vdok_raw_ent IS NOT NULL
                  AND (
                      ({ev_ident} IS NOT NULL AND trim({ev_ident}::VARCHAR) != ''
                       AND {ev_ts_expr} IS NULL)
                      OR {kt_fail_check}
                  )
                """
            ).fetchall()
            if ts_fail_rows and int(ts_fail_rows[0][0]) > 0:
                return None

            # Resolve distinct entity IDs via exact Python SymbolMaster / normalize_identifier
            distinct_ents = [
                str(r[0])
                for r in con.sql(
                    "SELECT DISTINCT _vdok_raw_ent FROM raw_numbered "
                    "WHERE _vdok_raw_ent IS NOT NULL"
                ).fetchall()
            ]
            if not distinct_ents:
                continue

            raw_e_list: list[str] = []
            res_e_list: list[str] = []
            unmap_list: list[bool] = []
            for e in distinct_ents:
                if symbol_master is not None:
                    res_id, is_unmap = symbol_master.resolve(
                        raw_id=e, id_type=manifest.entity_id_type
                    )
                else:
                    try:
                        res_id = normalize_identifier(e, id_type=manifest.entity_id_type)
                    except ValueError:
                        return None
                    is_unmap = False
                raw_e_list.append(e)
                res_e_list.append(res_id)
                unmap_list.append(is_unmap)

            con.register(
                "ent_map_tbl",
                pa.table(
                    {
                        "_raw_ent": pa.array(raw_e_list, type=pa.string()),
                        "_res_ent": pa.array(res_e_list, type=pa.string()),
                        "_is_unmap": pa.array(unmap_list, type=pa.bool_()),
                    }
                ),
            )

            # Build distinct unit mapping table if unit_column exists
            has_unit_col = bool(manifest.unit_column and manifest.unit_column in avail_cols)
            if has_unit_col and manifest.unit_column is not None:
                u_ident = _quote_ident(manifest.unit_column)
                distinct_u = [
                    str(r[0])
                    for r in con.sql(
                        f"SELECT DISTINCT {u_ident}::VARCHAR FROM raw_numbered "
                        f"WHERE {u_ident} IS NOT NULL"
                    ).fetchall()
                ]
                u_raw: list[str] = []
                u_mapped: list[str | None] = []
                u_pct: list[bool] = []
                for u_str in distinct_u:
                    stripped = u_str.strip()
                    lowered = stripped.lower()
                    u_raw.append(u_str)
                    u_mapped.append(normalize_unit(u_str))
                    u_pct.append(bool(stripped and lowered in PERCENT_UNITS))
                con.register(
                    "unit_map_tbl",
                    pa.table(
                        {
                            "_raw_u": pa.array(u_raw, type=pa.string()),
                            "_mapped_u": pa.array(u_mapped, type=pa.string()),
                            "_is_pct": pa.array(u_pct, type=pa.bool_()),
                        }
                    ),
                )
                unit_join = f"LEFT JOIN unit_map_tbl um ON r.{u_ident}::VARCHAR = um._raw_u"
                mapped_u_expr = "um._mapped_u"
                is_pct_expr = "COALESCE(um._is_pct, FALSE)"
            else:
                unit_join = ""
                mapped_u_expr = "NULL::VARCHAR"
                is_pct_expr = "FALSE"

            union_branches: list[str] = []
            for val_ord, val_col in enumerate(manifest.value_columns):
                if manifest.field_columns:
                    f_parts = [
                        f"COALESCE(r.{_quote_ident(fc)}::VARCHAR, '')" if fc in avail_cols else "''"
                        for fc in manifest.field_columns
                    ]
                    if len(manifest.value_columns) > 1:
                        vc_esc = val_col.replace("'", "''")
                        f_parts.append(f"'{vc_esc}'")
                    field_expr = " || '|' || ".join(f_parts)
                else:
                    vc_esc = val_col.replace("'", "''")
                    field_expr = f"'{vc_esc}'"

                if val_col in avail_cols:
                    vc_ident = f"r.{_quote_ident(val_col)}"
                    raw_str_expr = f"{vc_ident}::VARCHAR"
                else:
                    raw_str_expr = "NULL::VARCHAR"

                union_branches.append(
                    f"""
                    SELECT
                        em._res_ent AS entity_id,
                        '{id_type_esc}' AS entity_id_type,
                        {field_expr} AS field,
                        {ev_ts_expr} AS event_time,
                        {kt_ts_expr} AS knowledge_time,
                        {raw_str_expr} AS _raw_v,
                        {mapped_u_expr} AS _mapped_u,
                        {is_pct_expr} AS _is_pct,
                        r._vdok_src_row AS _src_row,
                        {val_ord} AS _val_ord,
                        em._is_unmap AS unmapped
                    FROM raw_numbered r
                    JOIN ent_map_tbl em ON r._vdok_raw_ent = em._raw_ent
                    {unit_join}
                    WHERE r.{ev_ident} IS NOT NULL
                      AND {ev_ts_expr} IS NOT NULL
                      AND {kt_null_filter}
                    """
                )

            if not union_branches:
                continue

            unioned_sql = " UNION ALL ".join(union_branches)
            final_sql = f"""
            WITH unpivoted AS (
                {unioned_sql}
            ),
            cleaned AS (
                SELECT
                    *,
                    CASE
                        WHEN _raw_v IS NULL THEN NULL
                        WHEN contains(trim(_raw_v), ',') AND contains(trim(_raw_v), '.')
                        THEN replace(trim(_raw_v), ',', '')
                        ELSE trim(_raw_v)
                    END AS _clean_v
                FROM unpivoted
            ),
            typed AS (
                SELECT
                    entity_id,
                    entity_id_type,
                    field,
                    event_time,
                    knowledge_time,
                    CASE
                        WHEN _clean_v IS NULL OR _clean_v = '' THEN NULL::DECIMAL(38, 9)
                        WHEN TRY_CAST(_clean_v AS DECIMAL(38, 12)) IS NOT NULL THEN
                            ROUND(
                                CASE
                                    WHEN _is_pct
                                    THEN TRY_CAST(_clean_v AS DECIMAL(38, 12)) / 100
                                    ELSE TRY_CAST(_clean_v AS DECIMAL(38, 12))
                                END,
                                9
                            )::DECIMAL(38, 9)
                        ELSE NULL::DECIMAL(38, 9)
                    END AS value_num,
                    CASE
                        WHEN _clean_v IS NOT NULL AND _clean_v != ''
                             AND TRY_CAST(_clean_v AS DECIMAL(38, 12)) IS NULL
                        THEN _raw_v
                        ELSE NULL::VARCHAR
                    END AS value_str,
                    _mapped_u AS unit,
                    '{drop_id_esc}' AS drop_id,
                    '{rel_esc}' AS source_file,
                    _src_row::BIGINT AS source_row,
                    _val_ord,
                    unmapped
                FROM cleaned
            )
            SELECT
                entity_id,
                entity_id_type,
                field,
                event_time,
                knowledge_time,
                value_num,
                value_str,
                unit,
                drop_id,
                source_file,
                source_row,
                sha256(
                    (
                        entity_id || chr(0) ||
                        field || chr(0) ||
                        strftime(
                            event_time AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%S.%fZ'
                        ) || chr(0) ||
                        COALESCE(CAST(value_num AS VARCHAR), '') || chr(0) ||
                        COALESCE(value_str, '') || chr(0) ||
                        COALESCE(unit, '')
                    )::BLOB
                ) AS row_hash,
                unmapped
            FROM typed
            ORDER BY source_row, _val_ord
            """
            file_tbl = con.sql(final_sql).to_arrow_table()
            if len(file_tbl) > 0:
                tables.append(file_tbl.cast(CANONICAL_ARROW_SCHEMA))
        finally:
            con.close()

    if not tables:
        return CANONICAL_ARROW_SCHEMA.empty_table()
    if len(tables) == 1:
        return tables[0]
    return pa.concat_tables(tables)


# 5. Drop Normalization Function
def normalize_drop(
    drop_dir: Path | str,
    manifest: VendorManifest,
    drop_id: str,
    symbol_master: SymbolMaster | None = None,
    as_of: datetime.datetime | None = None,
) -> pa.Table:
    """Normalize an incoming drop batch into a canonical Apache Arrow Table.

    Reads CSV, JSON Lines, or Parquet files from the drop path, applies field
    extraction, exact decimal conversion, UTC temporal alignment, unit translation,
    and symbol master resolution.
    """
    if manifest.vendor == "sec_fsds":
        from vdok.adapters.sec_fsds import is_sec_fsds_drop, normalize_sec_fsds_drop

        if is_sec_fsds_drop(drop_dir):
            return normalize_sec_fsds_drop(
                drop_dir=drop_dir,
                manifest=manifest,
                drop_id=drop_id,
                symbol_master=symbol_master,
                as_of=as_of,
            )

    drop_path = Path(drop_dir)
    drop_meta = scan(drop_path)

    default_drop_kt = as_of if as_of is not None else datetime.datetime.now(timezone.utc)
    vec_tbl = _normalize_drop_vectorized(
        drop_meta=drop_meta,
        manifest=manifest,
        drop_id=drop_id,
        symbol_master=symbol_master,
        default_drop_kt=default_drop_kt,
    )
    if vec_tbl is not None:
        return vec_tbl

    records: list[dict[str, Any]] = []

    for disc_file in drop_meta.files:
        f_path = disc_file.path
        rel_path = disc_file.relative_path
        f_fmt = disc_file.format
        f_comp = disc_file.compression

        if f_fmt == FileFormat.CSV:
            con = duckdb.connect()
            # Read all columns as varchar to prevent floating point coercion
            rel = con.read_csv(str(f_path), all_varchar=True)
            # Use to_arrow_table to avoid pytz dependency
            arrow_tbl = rel.to_arrow_table()
            rows = arrow_tbl.to_pylist()
            con.close()
        elif f_fmt == FileFormat.JSONL:
            rows = []
            if f_comp == Compression.GZIP:
                with gzip.open(f_path, "rt", encoding="utf-8") as gf:
                    for line in gf:
                        line_str = line.strip()
                        if line_str:
                            rows.append(json.loads(line_str, parse_float=Decimal))
            else:
                with open(f_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line_str = line.strip()
                        if line_str:
                            rows.append(json.loads(line_str, parse_float=Decimal))
        elif f_fmt == FileFormat.PARQUET:
            pq_table = pq.read_table(f_path)
            rows = pq_table.to_pylist()
        else:
            raise ValueError(f"Unsupported file format in drop: {f_fmt}")

        for row_idx, row in enumerate(rows, start=1):
            # Extract raw entity identifier from priority columns
            raw_entity: str | None = None
            for col in manifest.entity_columns:
                val = row.get(col)
                if val is not None and str(val).strip():
                    raw_entity = str(val).strip()
                    break

            if raw_entity is None:
                continue

            # Extract event time
            raw_event = row.get(manifest.event_time_column)
            if raw_event is None:
                continue
            event_dt = parse_timestamp_utc(
                raw_event,
                fmt=manifest.event_time_format,
                source_tz=manifest.timezone,
            )

            # Resolve entity identifier
            if symbol_master is not None:
                resolved_id, is_unmapped = symbol_master.resolve(
                    raw_id=raw_entity,
                    id_type=manifest.entity_id_type,
                )
            else:
                resolved_id = normalize_identifier(raw_entity, id_type=manifest.entity_id_type)
                is_unmapped = False

            # Extract knowledge time
            if manifest.knowledge_time.source == KnowledgeTimeSource.COLUMN:
                kt_col = manifest.knowledge_time.column
                if not kt_col or row.get(kt_col) is None:
                    continue
                kt_dt = parse_timestamp_utc(
                    row[kt_col],
                    fmt=manifest.knowledge_time.format,
                    source_tz=manifest.timezone,
                )
            else:
                kt_dt = default_drop_kt

            # Extract unit of measure
            raw_unit_val: str | None = None
            if manifest.unit_column and row.get(manifest.unit_column) is not None:
                raw_unit_val = str(row[manifest.unit_column])

            # Process value columns
            for val_col in manifest.value_columns:
                raw_val = row.get(val_col)

                if manifest.field_columns:
                    if len(manifest.value_columns) == 1:
                        field_name = "|".join(str(row.get(c, "")) for c in manifest.field_columns)
                    else:
                        field_name = "|".join(
                            [str(row.get(c, "")) for c in manifest.field_columns] + [val_col]
                        )
                else:
                    field_name = val_col

                val_num: Decimal | None
                val_str: str | None
                mapped_unit: str | None

                try:
                    dec_val = parse_decimal_exact(raw_val)
                    scaled_dec, mapped_unit = normalize_unit_and_value(dec_val, raw_unit_val)
                    # Quantize to 9 decimal places if exponent exceeds 9 places
                    if scaled_dec is not None:
                        exp = scaled_dec.as_tuple().exponent
                        if isinstance(exp, int) and exp < -9:
                            val_num = scaled_dec.quantize(
                                Decimal("0.000000001"), rounding=ROUND_HALF_UP
                            )
                        else:
                            val_num = scaled_dec
                    else:
                        val_num = None
                    val_str = None
                except (TypeError, ValueError):
                    val_num = None
                    val_str = str(raw_val) if raw_val is not None else None
                    mapped_unit = normalize_unit(raw_unit_val)

                r_hash = compute_row_hash(
                    entity_id=resolved_id,
                    field=field_name,
                    event_time=event_dt,
                    value_num=val_num,
                    value_str=val_str,
                    unit=mapped_unit,
                )

                records.append(
                    {
                        "entity_id": resolved_id,
                        "entity_id_type": manifest.entity_id_type.value,
                        "field": field_name,
                        "event_time": event_dt,
                        "knowledge_time": kt_dt,
                        "value_num": val_num,
                        "value_str": val_str,
                        "unit": mapped_unit,
                        "drop_id": drop_id,
                        "source_file": rel_path,
                        "source_row": row_idx,
                        "row_hash": r_hash,
                        "unmapped": is_unmapped,
                    }
                )

    if not records:
        return CANONICAL_ARROW_SCHEMA.empty_table()

    return pa.Table.from_pylist(records, schema=CANONICAL_ARROW_SCHEMA)
