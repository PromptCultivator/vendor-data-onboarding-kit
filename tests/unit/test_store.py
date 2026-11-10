"""Unit tests for bitemporal store and DuckDB catalog management."""

from __future__ import annotations

import os
import stat
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest

from vdok.normalize import CANONICAL_ARROW_SCHEMA
from vdok.store import (
    AppendOnlyViolationError,
    init_store_catalog,
    record_drop_metadata,
    resolve_field_bucket,
    write_partitioned_store,
)


def _make_sample_table(
    drop_id: str = "drop_001",
    field: str = "Revenues",
    event_year: int = 2023,
    knowledge_time: datetime | None = None,
) -> pa.Table:
    kt = knowledge_time or datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    ev = datetime(event_year, 6, 30, 0, 0, 0, tzinfo=timezone.utc)
    data = {
        "entity_id": ["0000320193"],
        "entity_id_type": ["cik"],
        "field": [field],
        "event_time": [ev],
        "knowledge_time": [kt],
        "value_num": [Decimal("12345.670000000")],
        "value_str": [None],
        "unit": ["USD"],
        "drop_id": [drop_id],
        "source_file": ["sample.csv"],
        "source_row": [1],
        "row_hash": ["hash123"],
        "unmapped": [False],
    }
    return pa.Table.from_pydict(data, schema=CANONICAL_ARROW_SCHEMA)


def test_resolve_field_bucket() -> None:
    """Verify deterministic 4-tier taxonomy bucketing."""
    # Tier 1: Manifest override
    manifest_map = {"custom_metric": "custom_bucket"}
    assert resolve_field_bucket("custom_metric", manifest_map) == "custom_bucket"

    # Tier 2: Financial statements
    assert resolve_field_bucket("Revenues|USD|4Q") == "income_statement"
    assert resolve_field_bucket("GrossProfit") == "income_statement"
    assert resolve_field_bucket("OperatingIncomeLoss") == "income_statement"
    assert resolve_field_bucket("AssetsCurrent") == "balance_sheet"
    assert resolve_field_bucket("StockholdersEquity") == "balance_sheet"
    assert resolve_field_bucket("CashAndCashEquivalents") == "balance_sheet"
    assert resolve_field_bucket("CashFlowFromOperatingActivities") == "cash_flow"
    assert resolve_field_bucket("CapitalExpenditures") == "cash_flow"

    # Tier 3: Market data
    assert resolve_field_bucket("price") == "market_prices"
    assert resolve_field_bucket("trade_price") == "market_prices"
    assert resolve_field_bucket("close") == "market_prices"
    assert resolve_field_bucket("volume") == "market_prices"

    # Tier 4: Macro
    assert resolve_field_bucket("gdp") == "macro"
    assert resolve_field_bucket("cpi") == "macro"
    assert resolve_field_bucket("unemployment_rate") == "macro"

    # Tier 5: Fallback
    assert resolve_field_bucket("unknown_arbitrary_tag") == "general"


def test_write_partitioned_store_basic(tmp_path: Path) -> None:
    """Verify writing partitions, path hierarchy, and 0o444 permissions."""
    tbl = _make_sample_table(drop_id="drop_abc", field="Revenues", event_year=2023)
    written = write_partitioned_store(
        tbl, vendor="sec_fsds", drop_id="drop_abc", base_path=tmp_path
    )

    assert len(written) == 1
    expected_path = tmp_path / "sec_fsds" / "income_statement" / "2023" / "part-drop_abc.parquet"
    assert written[0] == expected_path
    assert expected_path.exists()

    # Verify POSIX permissions 0o444 (read-only for all)
    file_mode = stat.S_IMODE(os.stat(expected_path).st_mode)
    assert file_mode == 0o444


def test_write_partitioned_store_multi_partition(tmp_path: Path) -> None:
    """Verify table with multiple fields and years splits into multiple partition files."""
    kt = datetime(2024, 2, 1, tzinfo=timezone.utc)
    t1 = datetime(2022, 12, 31, tzinfo=timezone.utc)
    t2 = datetime(2023, 12, 31, tzinfo=timezone.utc)

    data = {
        "entity_id": ["0000320193", "0000320193", "AAPL"],
        "entity_id_type": ["cik", "cik", "ticker"],
        "field": ["Revenues", "AssetsCurrent", "price"],
        "event_time": [t1, t2, t2],
        "knowledge_time": [kt, kt, kt],
        "value_num": [Decimal("100.0"), Decimal("200.0"), Decimal("150.0")],
        "value_str": [None, None, None],
        "unit": ["USD", "USD", "USD"],
        "drop_id": ["drop_multi", "drop_multi", "drop_multi"],
        "source_file": ["f.csv", "f.csv", "f.csv"],
        "source_row": [1, 2, 3],
        "row_hash": ["h1", "h2", "h3"],
        "unmapped": [False, False, False],
    }
    multi_table = pa.Table.from_pydict(data, schema=CANONICAL_ARROW_SCHEMA)

    written = write_partitioned_store(
        multi_table, vendor="test_vendor", drop_id="drop_multi", base_path=tmp_path
    )
    assert len(written) == 3

    p_income = tmp_path / "test_vendor" / "income_statement" / "2022" / "part-drop_multi.parquet"
    p_balance = tmp_path / "test_vendor" / "balance_sheet" / "2023" / "part-drop_multi.parquet"
    p_market = tmp_path / "test_vendor" / "market_prices" / "2023" / "part-drop_multi.parquet"

    assert p_income in written
    assert p_balance in written
    assert p_market in written


def test_write_partitioned_store_empty_table(tmp_path: Path) -> None:
    """Verify empty table writes zero partition files."""
    empty = CANONICAL_ARROW_SCHEMA.empty_table()
    written = write_partitioned_store(
        empty, vendor="test_vendor", drop_id="drop_empty", base_path=tmp_path
    )
    assert written == []


def test_write_partitioned_store_append_only_violation(tmp_path: Path) -> None:
    """Verify attempting to overwrite existing drop_id raises AppendOnlyViolationError."""
    tbl = _make_sample_table(drop_id="drop_dup")
    write_partitioned_store(tbl, vendor="sec_fsds", drop_id="drop_dup", base_path=tmp_path)

    with pytest.raises(AppendOnlyViolationError) as exc_info:
        write_partitioned_store(tbl, vendor="sec_fsds", drop_id="drop_dup", base_path=tmp_path)

    assert "already has partition files" in str(exc_info.value)
    assert issubclass(AppendOnlyViolationError, FileExistsError)


def test_init_store_catalog_and_views(tmp_path: Path) -> None:
    """Verify catalog initialization, metadata tables, and depth isolation views."""
    con = init_store_catalog(base_path=tmp_path, vendor="sec_fsds")
    try:
        tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
        assert "drops" in tables
        assert "held_drops" in tables
        assert "schema_versions" in tables
        assert "symbol_master_versions" in tables
        assert "revisions" in tables
        assert "vendor_events" in tables
        assert "quarantine_events" in tables

        # Empty view should return 0 rows without failing
        row = con.execute("SELECT COUNT(*) FROM vendor_events").fetchone()
        assert row is not None
        assert row[0] == 0
    finally:
        con.close()


def test_record_drop_metadata_and_sync(tmp_path: Path) -> None:
    """Verify recording drop metadata persists to DuckDB and drops.parquet."""
    now = datetime(2024, 3, 1, 12, 0, 0, tzinfo=timezone.utc)
    rec = {
        "drop_id": "drop_meta_1",
        "vendor": "sec_fsds",
        "status": "COMMITTED",
        "committed_at": now,
        "total_rows": 1000,
        "valid_rows": 990,
        "quarantine_rows": 10,
        "total_bytes": 50000,
        "file_count": 1,
        "file_format": "csv",
        "compression": "none",
        "event_time_min": now,
        "event_time_max": now,
        "knowledge_time_min": now,
        "knowledge_time_max": now,
    }

    record_drop_metadata(base_path=tmp_path, vendor="sec_fsds", drop_record=rec)

    # Check duckdb table
    con = duckdb.connect(str(tmp_path / "sec_fsds" / "store.duckdb"))
    try:
        row = con.execute(
            "SELECT drop_id, valid_rows FROM drops WHERE drop_id = 'drop_meta_1'"
        ).fetchone()
        assert row is not None
        assert row[0] == "drop_meta_1"
        assert row[1] == 990
    finally:
        con.close()

    # Check metadata/drops.parquet
    drops_pq = tmp_path / "sec_fsds" / "metadata" / "drops.parquet"
    assert drops_pq.exists()

    # Attempting duplicate record raises AppendOnlyViolationError
    with pytest.raises(AppendOnlyViolationError):
        record_drop_metadata(base_path=tmp_path, vendor="sec_fsds", drop_record=rec)
