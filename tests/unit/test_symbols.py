"""Unit tests for symbol master construction and deterministic identifier resolution."""

from __future__ import annotations

import datetime
import json
from datetime import timezone
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from vdok.manifest import EntityIdType
from vdok.symbols import (
    SYMBOL_MASTER_ARROW_SCHEMA,
    ResolutionResult,
    SymbolMaster,
    build_symbol_master,
)

FIXTURE_PATH = Path("tests/fixtures/symbols/company_tickers.json")
FIXTURE_V2_PATH = Path("tests/fixtures/symbols/company_tickers_v2.json")


# 1. Tests for build_symbol_master
def test_build_symbol_master_from_fixture(tmp_path: Path) -> None:
    """Build immutable Parquet symbol master from SEC reference fixture."""
    output_path = build_symbol_master(FIXTURE_PATH, output_dir=tmp_path)
    assert output_path.exists()
    assert output_path.name.startswith("master_v")
    assert output_path.suffix == ".parquet"

    # Verify Parquet schema and record count
    table = pq.read_table(output_path)
    assert table.schema == SYMBOL_MASTER_ARROW_SCHEMA
    assert len(table) == 200

    # Verify file permissions are read-only
    stat_mode = output_path.stat().st_mode & 0o777
    assert stat_mode == 0o444


def test_build_symbol_master_idempotency(tmp_path: Path) -> None:
    """Re-building from identical source returns existing path without error."""
    path1 = build_symbol_master(FIXTURE_PATH, output_dir=tmp_path)
    path2 = build_symbol_master(FIXTURE_PATH, output_dir=tmp_path)
    assert path1 == path2


def test_build_symbol_master_version_migration(tmp_path: Path) -> None:
    """Modified fixture produces a distinct cryptographic version hash."""
    path1 = build_symbol_master(FIXTURE_PATH, output_dir=tmp_path)
    path2 = build_symbol_master(FIXTURE_V2_PATH, output_dir=tmp_path)
    assert path1.name != path2.name
    assert path1.stem != path2.stem


def test_build_symbol_master_list_of_dicts(tmp_path: Path) -> None:
    """Build master from list-of-dicts JSON representation."""
    list_data = [
        {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
        {"cik": "0000789019", "ticker": "MSFT", "title": "MICROSOFT CORP"},
    ]
    json_path = tmp_path / "list_source.json"
    json_path.write_text(json.dumps(list_data))

    out_path = build_symbol_master(json_path, output_dir=tmp_path)
    table = pq.read_table(out_path)
    assert len(table) == 2


def test_build_symbol_master_errors(tmp_path: Path) -> None:
    """Nonexistent source or empty source raises appropriate error."""
    with pytest.raises(FileNotFoundError, match="not found"):
        build_symbol_master(tmp_path / "nonexistent.json", output_dir=tmp_path)

    empty_json = tmp_path / "empty.json"
    empty_json.write_text(json.dumps({}))
    with pytest.raises(ValueError, match="Zero valid corporate entries"):
        build_symbol_master(empty_json, output_dir=tmp_path)

    invalid_json = tmp_path / "invalid.json"
    invalid_json.write_text(json.dumps("string_is_invalid"))
    with pytest.raises(ValueError, match="Unrecognized symbol reference"):
        build_symbol_master(invalid_json, output_dir=tmp_path)


# 2. Tests for SymbolMaster.load
def test_symbol_master_load(tmp_path: Path) -> None:
    """Load symbol master and inspect catalog properties."""
    parquet_path = build_symbol_master(FIXTURE_PATH, output_dir=tmp_path)
    master = SymbolMaster.load(parquet_path)
    assert master.size == 200
    assert len(master.version_id) == 16


def test_symbol_master_load_missing_file_raises(tmp_path: Path) -> None:
    """Loading nonexistent Parquet file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError, match="not found"):
        SymbolMaster.load(tmp_path / "nonexistent.parquet")


# 3. Tests for SymbolMaster.resolve
@pytest.fixture
def master(tmp_path: Path) -> SymbolMaster:
    parquet_path = build_symbol_master(FIXTURE_PATH, output_dir=tmp_path)
    return SymbolMaster.load(parquet_path)


def test_resolve_cik(master: SymbolMaster) -> None:
    """Exact CIK matching with zero-padding."""
    res = master.resolve("320193", "cik")
    assert res == ResolutionResult("0000320193", False)

    res2 = master.resolve("0000320193", EntityIdType.CIK)
    assert res2 == ResolutionResult("0000320193", False)

    # Unknown CIK
    res_unk = master.resolve("9999999999", "cik")
    assert res_unk == ResolutionResult("9999999999", True)

    # CIK with no digits
    res_no_digits = master.resolve("INVALID", "cik")
    assert res_no_digits == ResolutionResult("INVALID", True)


def test_resolve_ticker_direct(master: SymbolMaster) -> None:
    """Exact uppercase ticker resolution."""
    res = master.resolve("AAPL", "ticker")
    assert res == ResolutionResult("0000320193", False)

    # Whitespace and lowercase folding
    res2 = master.resolve("  msft  ", EntityIdType.TICKER)
    assert res2 == ResolutionResult("0000789019", False)

    # Unknown ticker
    res_unk = master.resolve("NONEXISTENT", "ticker")
    assert res_unk == ResolutionResult("NONEXISTENT", True)


def test_resolve_ticker_exchange_suffixes(master: SymbolMaster) -> None:
    """Strip standard exchange suffixes from ticker symbols."""
    # NASDAQ suffixes (.OQ, .O)
    assert master.resolve("AAPL.OQ", "ticker") == ResolutionResult("0000320193", False)
    assert master.resolve("AAPL.O", "ticker") == ResolutionResult("0000320193", False)

    # NYSE suffix (.N)
    assert master.resolve("IBM.N", "ticker") == ResolutionResult("0000051143", False)

    # NYSE Arca suffix (.P)
    assert master.resolve("V.P", "ticker") == ResolutionResult("0001403161", False)


def test_resolve_ticker_dual_class_shares(master: SymbolMaster) -> None:
    """Translate dot and slash delimiters in dual class shares to hyphens."""
    # Berkshire Hathaway Class A (BRK-A)
    assert master.resolve("BRK.A", "ticker") == ResolutionResult("0001067983", False)
    assert master.resolve("BRK/A", "ticker") == ResolutionResult("0001067983", False)

    # Berkshire Hathaway Class B (BRK-B)
    assert master.resolve("BRK.B", "ticker") == ResolutionResult("0001067983", False)
    assert master.resolve("BRK/B", "ticker") == ResolutionResult("0001067983", False)

    # Suffix combined with dual class
    assert master.resolve("BRK.B.N", "ticker") == ResolutionResult("0001067983", False)


def test_resolve_corporate_title(master: SymbolMaster) -> None:
    """Exact title resolution with case and whitespace folding."""
    res = master.resolve("Apple Inc.", "title")
    assert res == ResolutionResult("0000320193", False)

    res_lower = master.resolve("apple inc.", "name")
    assert res_lower == ResolutionResult("0000320193", False)

    res_spaces = master.resolve("  Apple   Inc.  ", "title")
    assert res_spaces == ResolutionResult("0000320193", False)


def test_resolve_prohibits_fuzzy_matching(master: SymbolMaster) -> None:
    """Heuristic and fuzzy matching are strictly prohibited."""
    # Slight typographical errors must never match
    assert master.resolve("Appel Inc.", "title") == ResolutionResult("Appel Inc.", True)
    assert master.resolve("Apple Incorporated", "title") == ResolutionResult(
        "Apple Incorporated", True
    )
    assert master.resolve("AAPX", "ticker") == ResolutionResult("AAPX", True)
    assert master.resolve("Micro-soft Corp", "title") == ResolutionResult("Micro-soft Corp", True)


def test_resolve_unsupported_identifier_schemes(master: SymbolMaster) -> None:
    """Third-party schemes (ISIN, CUSIP, vendor_id) return unmapped."""
    assert master.resolve("US0378331005", "isin") == ResolutionResult("US0378331005", True)
    assert master.resolve("037833100", "cusip") == ResolutionResult("037833100", True)
    assert master.resolve("VEND_999", "vendor_id") == ResolutionResult("VEND_999", True)


def test_resolve_empty_string(master: SymbolMaster) -> None:
    """Empty strings return unmapped without crashing."""
    assert master.resolve("", "ticker") == ResolutionResult("", True)
    assert master.resolve("   ", "cik") == ResolutionResult("   ", True)


def test_resolve_with_as_of_interval(tmp_path: Path) -> None:
    """Temporal as_of parameter filters against valid_from interval."""
    # Build with an effective date in the future
    future_date = datetime.datetime(2030, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    p = build_symbol_master(FIXTURE_PATH, output_dir=tmp_path, effective_date=future_date)
    m = SymbolMaster.load(p)

    # Querying with as_of earlier than valid_from returns unmapped
    query_past = datetime.datetime(2024, 1, 1, tzinfo=timezone.utc)
    res_past = m.resolve("AAPL", "ticker", as_of=query_past)
    assert res_past.unmapped

    # Querying with as_of after valid_from resolves
    query_future = datetime.datetime(2030, 6, 1, tzinfo=timezone.utc)
    res_future = m.resolve("AAPL", "ticker", as_of=query_future)
    assert not res_future.unmapped
    assert res_future.entity_id == "0000320193"
