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
        parsed = datetime.datetime.strptime(s, fmt)
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


# 5. Drop Normalization Function
def normalize_drop(
    drop_dir: Path | str,
    manifest: VendorManifest,
    drop_id: str,
    symbol_master: SymbolMaster | None = None,
) -> pa.Table:
    """Normalize an incoming drop batch into a canonical Apache Arrow Table.

    Reads CSV, JSON Lines, or Parquet files from the drop path, applies field
    extraction, exact decimal conversion, UTC temporal alignment, unit translation,
    and symbol master resolution.
    """
    drop_path = Path(drop_dir)
    drop_meta = scan(drop_path)

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
                kt_dt = datetime.datetime.now(timezone.utc)

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
