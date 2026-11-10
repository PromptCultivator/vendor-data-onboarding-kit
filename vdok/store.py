"""Bitemporal partition storage engine and DuckDB catalog interface."""

from __future__ import annotations

import os
from collections import defaultdict
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from vdok.normalize import CANONICAL_ARROW_SCHEMA
from vdok.validate import QUARANTINE_ARROW_SCHEMA


class AppendOnlyViolationError(FileExistsError):
    """Raised when an attempt is made to overwrite an existing drop in the append-only store."""


def resolve_field_bucket(field: str, manifest_buckets: dict[str, str] | None = None) -> str:
    """Determine partition bucket for a field name using deterministic taxonomy rules."""
    if manifest_buckets and field in manifest_buckets:
        return manifest_buckets[field]

    primary_tag = field.split("|")[0].lower()

    # Tier 2: Financial statement taxonomy
    income_keywords = {
        "revenue",
        "sales",
        "cogs",
        "costof",
        "grossprofit",
        "operatingincome",
        "operatingexpense",
        "netincome",
        "eps",
        "earningspershare",
        "interestexpense",
        "profitloss",
        "ebit",
        "ebitda",
    }
    if any(k in primary_tag for k in income_keywords):
        return "income_statement"

    cash_flow_keywords = {
        "cashflow",
        "operatingactivit",
        "investingactivit",
        "financingactivit",
        "depreciation",
        "amortization",
        "capitalexpenditure",
        "dividendspaid",
    }
    if any(k in primary_tag for k in cash_flow_keywords):
        return "cash_flow"

    balance_keywords = {
        "asset",
        "liabilit",
        "equity",
        "stockholder",
        "retainedearning",
        "cash",
        "debt",
        "inventory",
        "receivable",
        "payable",
        "goodwill",
        "borrowing",
    }
    if any(k in primary_tag for k in balance_keywords):
        return "balance_sheet"

    # Tier 3: Market data taxonomy
    market_keywords = {
        "price",
        "trade_price",
        "bid",
        "ask",
        "open",
        "high",
        "low",
        "close",
        "vwap",
        "quote",
        "volume",
        "spread",
        "return",
    }
    if any(k in primary_tag for k in market_keywords):
        return "market_prices"

    # Tier 4: Macroeconomic taxonomy
    macro_keywords = {
        "gdp",
        "cpi",
        "inflation",
        "unrate",
        "unemployment",
        "interest_rate",
        "fedfunds",
    }
    if any(k in primary_tag for k in macro_keywords):
        return "macro"

    # Tier 5: General fallback
    return "general"


def refresh_catalog_views(con: duckdb.DuckDBPyConnection, vendor_dir: Path) -> None:
    """Update relational views for vendor partitions and quarantine files."""
    resolved_path = vendor_dir.resolve()
    part_files = list(resolved_path.glob("*/*/*.parquet"))

    if part_files:
        glob_pattern = f"{resolved_path}/*/*/*.parquet"
        con.execute(
            "CREATE OR REPLACE VIEW vendor_events AS "
            f"SELECT * FROM read_parquet('{glob_pattern}', hive_partitioning=false)"
        )
    else:
        empty_canonical = CANONICAL_ARROW_SCHEMA.empty_table()
        con.register("_empty_canonical_table", empty_canonical)
        con.execute("CREATE OR REPLACE VIEW vendor_events AS SELECT * FROM _empty_canonical_table")

    quar_files = list(resolved_path.glob("quarantine/*.parquet"))
    if quar_files:
        q_pattern = f"{resolved_path}/quarantine/*.parquet"
        con.execute(
            "CREATE OR REPLACE VIEW quarantine_events AS "
            f"SELECT * FROM read_parquet('{q_pattern}', hive_partitioning=false)"
        )
    else:
        empty_quar = QUARANTINE_ARROW_SCHEMA.empty_table()
        con.register("_empty_quarantine_table", empty_quar)
        con.execute(
            "CREATE OR REPLACE VIEW quarantine_events AS SELECT * FROM _empty_quarantine_table"
        )


def sync_metadata_from_disk(con: duckdb.DuckDBPyConnection, meta_dir: Path) -> None:
    """Populate catalog tables from metadata Parquet files if tables are empty."""
    drops_parquet = meta_dir / "drops.parquet"
    if drops_parquet.exists():
        count = con.execute("SELECT COUNT(*) FROM drops").fetchone()[0]  # type: ignore
        if count == 0:
            con.execute(
                f"INSERT INTO drops SELECT * FROM read_parquet('{drops_parquet.resolve()}')"
            )

    held_parquet = meta_dir / "held_drops.parquet"
    if held_parquet.exists():
        count = con.execute("SELECT COUNT(*) FROM held_drops").fetchone()[0]  # type: ignore
        if count == 0:
            con.execute(
                f"INSERT INTO held_drops SELECT * FROM read_parquet('{held_parquet.resolve()}')"
            )

    revisions_parquet = meta_dir / "revisions.parquet"
    if revisions_parquet.exists():
        count = con.execute("SELECT COUNT(*) FROM revisions").fetchone()[0]  # type: ignore
        if count == 0:
            con.execute(
                f"INSERT INTO revisions SELECT * FROM read_parquet('{revisions_parquet.resolve()}')"
            )


def init_store_catalog(base_path: Path | str, vendor: str) -> duckdb.DuckDBPyConnection:
    """Initialize DuckDB catalog and register relational views."""
    base = Path(base_path)
    vendor_dir = base if base.name == vendor else base / vendor
    vendor_dir.mkdir(parents=True, exist_ok=True)

    meta_dir = vendor_dir / "metadata"
    meta_dir.mkdir(parents=True, exist_ok=True)

    db_path = vendor_dir / "store.duckdb"
    con = duckdb.connect(str(db_path))

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS drops (
            drop_id VARCHAR PRIMARY KEY,
            vendor VARCHAR NOT NULL,
            status VARCHAR NOT NULL,
            committed_at TIMESTAMPTZ NOT NULL,
            total_rows BIGINT NOT NULL,
            valid_rows BIGINT NOT NULL,
            quarantine_rows BIGINT NOT NULL,
            total_bytes BIGINT NOT NULL,
            file_count INTEGER NOT NULL,
            file_format VARCHAR NOT NULL,
            compression VARCHAR NOT NULL,
            event_time_min TIMESTAMPTZ,
            event_time_max TIMESTAMPTZ,
            knowledge_time_min TIMESTAMPTZ,
            knowledge_time_max TIMESTAMPTZ
        );
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS held_drops (
            drop_id VARCHAR NOT NULL,
            vendor VARCHAR NOT NULL,
            held_at TIMESTAMPTZ NOT NULL,
            reason VARCHAR NOT NULL,
            rule_name VARCHAR NOT NULL,
            details_json VARCHAR NOT NULL
        );
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_versions (
            vendor VARCHAR NOT NULL,
            version INTEGER NOT NULL,
            pinned_at TIMESTAMPTZ NOT NULL,
            columns_json VARCHAR NOT NULL,
            PRIMARY KEY (vendor, version)
        );
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS symbol_master_versions (
            version_id VARCHAR PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL,
            record_count BIGINT NOT NULL,
            source_file VARCHAR NOT NULL,
            source_hash VARCHAR NOT NULL
        );
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS revisions (
            entity_id VARCHAR NOT NULL,
            field VARCHAR NOT NULL,
            event_time TIMESTAMPTZ NOT NULL,
            old_value_num DECIMAL(38, 9),
            new_value_num DECIMAL(38, 9),
            old_value_str VARCHAR,
            new_value_str VARCHAR,
            old_unit VARCHAR,
            new_unit VARCHAR,
            old_knowledge_time TIMESTAMPTZ NOT NULL,
            new_knowledge_time TIMESTAMPTZ NOT NULL,
            old_drop_id VARCHAR NOT NULL,
            new_drop_id VARCHAR NOT NULL,
            relative_change DOUBLE
        );
        """
    )

    sync_metadata_from_disk(con, meta_dir)
    refresh_catalog_views(con, vendor_dir)

    return con


def write_partitioned_store(
    table: pa.Table,
    vendor: str,
    drop_id: str,
    base_path: Path | str = Path("data/store"),
    manifest_buckets: dict[str, str] | None = None,
) -> list[Path]:
    """Write canonical records to bitemporal Parquet partitions with 0o444 POSIX mode."""
    base = Path(base_path)
    vendor_dir = base if base.name == vendor else base / vendor

    # Guarantee 1 & 3: Check for existing partition files for this drop_id
    if vendor_dir.exists():
        existing = list(vendor_dir.glob(f"*/*/part-{drop_id}.parquet"))
        if existing:
            raise AppendOnlyViolationError(
                f"Append-only violation: drop '{drop_id}' already has partition files "
                f"at {existing[0]}. In-place updates and replacements are prohibited."
            )

    if len(table) == 0:
        return []

    # Ensure table conforms to canonical schema
    casted_table = table.cast(CANONICAL_ARROW_SCHEMA)

    field_col = casted_table["field"]
    unique_fields = field_col.unique().to_pylist()
    bucket_map = {f: resolve_field_bucket(f, manifest_buckets) for f in unique_fields}

    bucket_to_fields: dict[str, list[str]] = defaultdict(list)
    for f, b in bucket_map.items():
        bucket_to_fields[b].append(f)

    written_paths: list[Path] = []

    _pc: Any = pc
    for bucket, fields in bucket_to_fields.items():
        field_mask = _pc.is_in(field_col, value_set=pa.array(fields, type=pa.string()))
        bucket_slice = casted_table.filter(field_mask)

        years_array = _pc.year(bucket_slice["event_time"])
        unique_years = _pc.unique(years_array).to_pylist()

        for yr in unique_years:
            year_mask = _pc.equal(years_array, yr)
            part_slice = bucket_slice.filter(year_mask)

            part_dir = vendor_dir / bucket / str(yr)
            part_dir.mkdir(parents=True, exist_ok=True)
            part_file = part_dir / f"part-{drop_id}.parquet"

            pq.write_table(part_slice, part_file)
            os.chmod(part_file, 0o444)
            written_paths.append(part_file)

    # Update catalog views if catalog file exists
    db_file = vendor_dir / "store.duckdb"
    if db_file.exists():
        try:
            con = duckdb.connect(str(db_file))
            try:
                refresh_catalog_views(con, vendor_dir)
            finally:
                con.close()
        except Exception:
            pass

    return written_paths


def record_drop_metadata(base_path: Path | str, vendor: str, drop_record: dict[str, Any]) -> None:
    """Record committed drop entry into DuckDB catalog and sync to metadata/drops.parquet."""
    base = Path(base_path)
    vendor_dir = base if base.name == vendor else base / vendor
    meta_dir = vendor_dir / "metadata"
    meta_dir.mkdir(parents=True, exist_ok=True)

    db_path = vendor_dir / "store.duckdb"
    if not db_path.exists():
        c = init_store_catalog(base, vendor)
        c.close()

    con = duckdb.connect(str(db_path))
    try:
        drop_id = drop_record["drop_id"]
        res = con.execute("SELECT 1 FROM drops WHERE drop_id = ?", [drop_id]).fetchone()
        if res is not None:
            raise AppendOnlyViolationError(
                f"Append-only violation: drop '{drop_id}' is already recorded in drops catalog."
            )

        cols = [
            "drop_id",
            "vendor",
            "status",
            "committed_at",
            "total_rows",
            "valid_rows",
            "quarantine_rows",
            "total_bytes",
            "file_count",
            "file_format",
            "compression",
            "event_time_min",
            "event_time_max",
            "knowledge_time_min",
            "knowledge_time_max",
        ]
        values = [drop_record.get(c) for c in cols]
        placeholders = ", ".join(["?"] * len(cols))
        con.execute(
            f"INSERT INTO drops ({', '.join(cols)}) VALUES ({placeholders})",
            values,
        )

        drops_parquet = meta_dir / "drops.parquet"
        con.execute(f"COPY drops TO '{drops_parquet.resolve()}' (FORMAT PARQUET);")

        refresh_catalog_views(con, vendor_dir)
    finally:
        con.close()
