"""Unit test suite for SEC Financial Statement Data Sets adapter and downloader.

Verifies:
1. Normalization of SEC FSDS tabular datasets to canonical Arrow schemas.
2. Handling of coreg subsidiaries with '|coreg=<name>' suffixing.
3. Support for direct ZIP archive normalization.
4. Revision detection across consecutive quarters (>= 20 revisions).
5. Zero point-in-time violations during bitemporal queries (Guarantee G2).
6. Symbol master mapping rate exceeding 95% threshold.
7. Rate limiter enforcement (at most 5 requests per second).
8. User-Agent formatting and environment variable configuration.
9. File download caching and SHA-256 digest validation.
10. Row counts extraction for SEC tabular files.
11. Deterministic fixture selection reproducibility.
"""

from __future__ import annotations

import io
import time
import zipfile
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from vdok.adapters.sec_fsds import (
    SecRateLimiter,
    download_company_tickers,
    download_file,
    download_sec_fsds_quarter,
    get_sec_row_counts,
    get_sec_user_agent,
    is_sec_fsds_drop,
    normalize_sec_fsds_drop,
    select_sec_fixtures,
)
from vdok.cli import app
from vdok.manifest import VendorManifest
from vdok.onboard import onboard_drop
from vdok.query import Store
from vdok.symbols import SymbolMaster

runner = CliRunner()
MANIFEST_PATH = Path("manifests/sec_fsds.yaml")
RULES_PATH = Path("rules/sec_fsds.yaml")
FIXTURE_Q3 = Path("tests/fixtures/sec_fsds/2023q3")
FIXTURE_Q4 = Path("tests/fixtures/sec_fsds/2023q4")
SYMBOLS_FILE = Path("tests/fixtures/symbols/company_tickers.json")


def test_sec_fsds_fixture_normalization() -> None:
    """Verify SEC FSDS normalization produces conforming canonical table."""
    manifest = VendorManifest.from_yaml(MANIFEST_PATH.read_text(encoding="utf-8"))
    drop_id = "0123456789abcdef"

    table = normalize_sec_fsds_drop(
        drop_dir=FIXTURE_Q3,
        manifest=manifest,
        drop_id=drop_id,
    )

    assert len(table) == 250
    assert "entity_id" in table.column_names
    assert "field" in table.column_names
    assert "event_time" in table.column_names
    assert "knowledge_time" in table.column_names
    assert "value_num" in table.column_names

    # Check first row formatting
    first = table.slice(0, 1).to_pylist()[0]
    assert len(first["entity_id"]) == 10
    assert first["entity_id"].isdigit()
    assert first["field"].startswith("Revenues|USD|")
    assert first["event_time"].year == 2023
    assert first["knowledge_time"].year == 2023
    assert isinstance(first["value_num"], Decimal)
    assert first["drop_id"] == drop_id


def test_sec_fsds_coreg_suffixing(tmp_path: Path) -> None:
    """Verify rows with coreg set receive '|coreg=<name>' suffix."""
    manifest = VendorManifest.from_yaml(MANIFEST_PATH.read_text(encoding="utf-8"))
    drop_dir = tmp_path / "sec_coreg"
    drop_dir.mkdir()

    sub_content = (
        "adsh\tcik\tname\tsic\tcountryba\tstprba\tcityba\tzipba\tbas1\tbas2\tbaph\t"
        "countryma\tstprma\tcityma\tzipma\tmas1\tmas2\tcountryinc\tstprinc\tein\t"
        "former\tchanged\tafs\twksi\tfye\tform\tperiod\tfy\tfp\tfiled\taccepted\t"
        "prevrpt\tdetail\tinstance\tnciks\taciks\n"
        "0000320193-23-000001\t320193\tApple Inc.\t7372\tUS\tCA\tCupertino\t95014\t"
        "One Apple Park Way\t\t555-0100\tUS\tCA\tCupertino\t95014\t\t\tUS\tDE\t123456789\t"
        "\t\t1-LAF\t1\t1231\t10-Q\t20230630\t2023\tQ2\t20230815\t2023-08-15 16:30:00\t"
        "0\t1\tfiling.htm\t1\t\n"
    )
    num_content = (
        "adsh\ttag\tversion\tcoreg\tddate\tqtrs\tuom\tvalue\tfootnote\n"
        "0000320193-23-000001\tRevenues\tus-gaap/2023\tSubsidiaryHoldings\t20230630\t2\tUSD\t1000000.00\t\n"
    )

    (drop_dir / "sub.txt").write_text(sub_content, encoding="utf-8")
    (drop_dir / "num.txt").write_text(num_content, encoding="utf-8")

    table = normalize_sec_fsds_drop(
        drop_dir=drop_dir,
        manifest=manifest,
        drop_id="1111222233334444",
    )
    assert len(table) == 1
    row = table.slice(0, 1).to_pylist()[0]
    assert row["entity_id"] == "0000320193|coreg=SubsidiaryHoldings"


def test_sec_fsds_zip_archive_normalization(tmp_path: Path) -> None:
    """Verify normalize_sec_fsds_drop accepts ZIP archive inputs."""
    manifest = VendorManifest.from_yaml(MANIFEST_PATH.read_text(encoding="utf-8"))
    zip_path = tmp_path / "2023q3.zip"

    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.write(FIXTURE_Q3 / "sub.txt", arcname="sub.txt")
        zf.write(FIXTURE_Q3 / "num.txt", arcname="num.txt")

    assert is_sec_fsds_drop(zip_path)

    table = normalize_sec_fsds_drop(
        drop_dir=zip_path,
        manifest=manifest,
        drop_id="aaaaaaaaaaaaaaaa",
    )
    assert len(table) == 250


def test_sec_fsds_revisions_across_quarters(tmp_path: Path) -> None:
    """Verify onboarding consecutive quarters detects >= 20 revisions."""
    store_dir = tmp_path / "store"
    results_dir = tmp_path / "results"

    rep1, code1 = onboard_drop(
        vendor="sec_fsds",
        drop_path=FIXTURE_Q3,
        manifest_path=str(MANIFEST_PATH),
        rules_path=str(RULES_PATH),
        symbol_master_path=str(SYMBOLS_FILE),
        store_dir=str(store_dir),
        results_dir=str(results_dir),
    )
    assert code1 == 0

    rep2, code2 = onboard_drop(
        vendor="sec_fsds",
        drop_path=FIXTURE_Q4,
        manifest_path=str(MANIFEST_PATH),
        rules_path=str(RULES_PATH),
        symbol_master_path=str(SYMBOLS_FILE),
        store_dir=str(store_dir),
        results_dir=str(results_dir),
    )
    assert code2 == 0

    # Inspect revision ledger
    ledger_path = store_dir / "sec_fsds" / "metadata" / "revisions.parquet"
    assert ledger_path.exists()

    import pyarrow.parquet as pq

    rev_table = pq.read_table(ledger_path)
    assert len(rev_table) >= 20, f"Expected >= 20 revisions, found {len(rev_table)}"
    assert rep2.revisions_report.total_revisions >= 20


def test_sec_fsds_zero_point_in_time_violations(tmp_path: Path) -> None:
    """Verify point-in-time correctness returns 0 violations on restated facts (Guarantee G2)."""
    store_dir = tmp_path / "store"
    results_dir = tmp_path / "results"

    onboard_drop(
        vendor="sec_fsds",
        drop_path=FIXTURE_Q3,
        manifest_path=str(MANIFEST_PATH),
        rules_path=str(RULES_PATH),
        symbol_master_path=str(SYMBOLS_FILE),
        store_dir=str(store_dir),
        results_dir=str(results_dir),
    )
    onboard_drop(
        vendor="sec_fsds",
        drop_path=FIXTURE_Q4,
        manifest_path=str(MANIFEST_PATH),
        rules_path=str(RULES_PATH),
        symbol_master_path=str(SYMBOLS_FILE),
        store_dir=str(store_dir),
        results_dir=str(results_dir),
    )

    store = Store(store_path=str(store_dir / "sec_fsds"))

    # Entity 1 CIK is padded to 10 digits
    sample_cik = "0000320193"
    field = "Revenues|USD|2"
    event_time = "2023-06-30"

    # Query before Q4 filing was published: must see original Q3 value
    v_early = store.value(
        entity_id=sample_cik,
        field=field,
        event_time=event_time,
        as_of="2023-09-01",
    )
    assert v_early is not None

    # Query after Q4 filing was published: must see restated value
    v_late = store.value(
        entity_id=sample_cik,
        field=field,
        event_time=event_time,
        as_of="2023-12-01",
    )
    assert v_late is not None

    # Restated value in Q4 is strictly greater than original value in Q3
    assert v_late.value_num > v_early.value_num

    # Query before Q3 filing: must return None (no look-ahead leak)
    v_before = store.value(
        entity_id=sample_cik,
        field=field,
        event_time=event_time,
        as_of="2023-01-01",
    )
    assert v_before is None


def test_sec_fsds_symbol_mapping_share(tmp_path: Path) -> None:
    """Verify mapped entity share exceeds 95% threshold."""
    manifest = VendorManifest.from_yaml(MANIFEST_PATH.read_text(encoding="utf-8"))
    from vdok.symbols import build_symbol_master

    master_path = build_symbol_master(SYMBOLS_FILE, output_dir=tmp_path / "symbols")
    symbol_master = SymbolMaster.load(master_path)

    table = normalize_sec_fsds_drop(
        drop_dir=FIXTURE_Q3,
        manifest=manifest,
        drop_id="1234567812345678",
        symbol_master=symbol_master,
    )

    unmapped_flags = table["unmapped"].to_pylist()
    total_count = len(unmapped_flags)
    mapped_count = sum(1 for u in unmapped_flags if not u)
    share = mapped_count / total_count

    assert share >= 0.95, f"Mapped share {share:.2%} fell below required 95%"
    assert share == 1.0


def test_sec_rate_limiter() -> None:
    """Verify rate limiter enforces maximum frequency constraint."""
    limiter = SecRateLimiter(min_interval_seconds=0.05)
    t0 = time.monotonic()
    for _ in range(3):
        limiter.acquire()
    elapsed = time.monotonic() - t0
    # 3 calls with 0.05s intervals require at least 0.10s
    assert elapsed >= 0.09


def test_sec_user_agent_configuration() -> None:
    """Verify User-Agent header resolves according to SEC compliance specification."""
    # 1. Custom explicit header
    assert get_sec_user_agent("custom-bot contact@fund.com") == "custom-bot contact@fund.com"

    # 2. Environment variable configuration
    with patch.dict("os.environ", {"VDOK_CONTACT": "quant@institution.org"}):
        assert get_sec_user_agent() == "vendor-data-onboarding-kit quant@institution.org"

    # 3. Default fallback
    with patch.dict("os.environ", {}, clear=True):
        assert get_sec_user_agent() == "vendor-data-onboarding-kit research@example.com"


def test_sec_download_file_caching(tmp_path: Path) -> None:
    """Verify file downloader caches downloads by SHA-256 hash."""
    target_file = tmp_path / "test_download.json"
    content = b'{"status": "ok"}'

    mock_resp = MagicMock()
    mock_resp.read.side_effect = [content, b""]
    mock_resp.__enter__.return_value = mock_resp

    with patch("urllib.request.urlopen", return_value=mock_resp) as mock_urlopen:
        # First download
        p1, h1 = download_file(
            url="https://www.sec.gov/files/test.json",
            dest_path=target_file,
            force=False,
        )
        assert p1.exists()
        assert mock_urlopen.call_count == 1
        assert (tmp_path / "test_download.json.sha256").exists()

        # Second call with force=False must use cache without network request
        p2, h2 = download_file(
            url="https://www.sec.gov/files/test.json",
            dest_path=target_file,
            force=False,
        )
        assert p2 == p1
        assert h2 == h1
        assert mock_urlopen.call_count == 1

        # Third call with force=True redownloads
        mock_resp.read.side_effect = [content, b""]
        p3, h3 = download_file(
            url="https://www.sec.gov/files/test.json",
            dest_path=target_file,
            force=True,
        )
        assert mock_urlopen.call_count == 2


def test_sec_download_company_tickers_and_quarter(tmp_path: Path) -> None:
    """Verify company_tickers and quarter download helpers."""
    tickers_content = b'{"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}}'

    mock_resp = MagicMock()
    mock_resp.read.side_effect = [tickers_content, b""]
    mock_resp.__enter__.return_value = mock_resp

    with patch("urllib.request.urlopen", return_value=mock_resp):
        json_p, master_p, digest = download_company_tickers(
            dest_dir=tmp_path / "symbols",
            force=True,
            build_master=True,
        )
        assert json_p.exists()
        assert master_p is not None and master_p.exists()
        assert len(digest) == 64

    # Test quarter zip download and extraction
    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w") as zf:
        zf.writestr("sub.txt", "adsh\tcik\n1\t320193\n")
        zf.writestr("num.txt", "adsh\ttag\n1\tRevenues\n")
    zip_data = zip_bytes.getvalue()

    mock_zip_resp = MagicMock()
    mock_zip_resp.read.side_effect = [zip_data, b""]
    mock_zip_resp.__enter__.return_value = mock_zip_resp

    with patch("urllib.request.urlopen", return_value=mock_zip_resp):
        archive_p, extract_p, q_digest = download_sec_fsds_quarter(
            quarter="2023Q3",
            dest_dir=tmp_path / "raw",
            extract=True,
            force=True,
        )
        assert archive_p.exists()
        assert extract_p is not None and extract_p.exists()
        assert (extract_p / "sub.txt").exists()
        assert (extract_p / "num.txt").exists()


def test_sec_row_counts_extraction() -> None:
    """Verify row counts function counts records in directory and ZIP."""
    counts = get_sec_row_counts(FIXTURE_Q3)
    assert counts["sub"] == 50
    assert counts["num"] == 250
    assert counts["tag"] == 0
    assert counts["pre"] == 0


def test_select_sec_fixtures_reproducibility(tmp_path: Path) -> None:
    """Verify deterministic fixture selector creates target fixtures."""
    out_dir = tmp_path / "fixtures"
    stats = select_sec_fixtures(
        q3_path=FIXTURE_Q3,
        q4_path=FIXTURE_Q4,
        output_dir=out_dir,
        target_ciks=50,
        min_revision_pairs=20,
    )
    assert (out_dir / "2023q3" / "sub.txt").exists()
    assert (out_dir / "2023q3" / "num.txt").exists()
    assert (out_dir / "2023q4" / "sub.txt").exists()
    assert (out_dir / "2023q4" / "num.txt").exists()
    assert stats["ciks"] >= 50
    assert stats["revision_pairs"] >= 20


def test_cli_data_command(tmp_path: Path) -> None:
    """Verify CLI data command executes with mock downloader."""
    result = runner.invoke(app, ["data", "--help"])
    assert result.exit_code == 0
    assert "Download SEC FSDS" in result.output

    # Run with json output and mock downloader
    mock_result = {
        "symbols": {
            "json_path": "data/symbols/company_tickers.json",
            "master_parquet": "data/symbols/master.parquet",
            "sha256": "abcdef0123456789",
        },
        "quarters": {
            "2023q3": {
                "archive_path": "data/raw/sec_fsds/2023q3.zip",
                "extracted_path": "data/raw/sec_fsds/2023q3",
                "sha256": "9876543210fedcba",
                "row_counts": {"sub": 50, "num": 250, "tag": 0, "pre": 0},
            }
        },
    }
    with patch("vdok.adapters.sec_fsds.download_sec_fsds", return_value=mock_result):
        res = runner.invoke(
            app,
            ["data", "--quarters", "2023q3", "--json-output"],
        )
        assert res.exit_code == 0
        assert "abcdef0123456789" in res.output


def test_download_sec_fsds_orchestrator(tmp_path: Path) -> None:
    """Verify download_sec_fsds coordinates symbols and quarter downloads."""
    from vdok.adapters.sec_fsds import download_sec_fsds

    tickers_content = b'{"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}}'
    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w") as zf:
        zf.writestr("sub.txt", "adsh\tcik\n1\t320193\n")
        zf.writestr("num.txt", "adsh\ttag\n1\tRevenues\n")
    zip_data = zip_bytes.getvalue()

    def mock_urlopen(req, **kwargs):  # type: ignore
        url = req.full_url if hasattr(req, "full_url") else str(req)
        mock_resp = MagicMock()
        if "company_tickers" in url:
            mock_resp.read.side_effect = [tickers_content, b""]
        else:
            mock_resp.read.side_effect = [zip_data, b""]
        mock_resp.__enter__.return_value = mock_resp
        return mock_resp

    with patch("urllib.request.urlopen", side_effect=mock_urlopen):
        summary = download_sec_fsds(
            quarters=["2023q3"],
            dest_dir=tmp_path / "raw",
            symbols_dir=tmp_path / "symbols",
            extract=True,
            force=True,
        )
        assert "symbols" in summary
        assert "2023q3" in summary["quarters"]
        counts = summary["quarters"]["2023q3"]["row_counts"]
        assert counts["sub"] == 1
        assert counts["num"] == 1


def test_download_file_failure_cleanup(tmp_path: Path) -> None:
    """Verify temporary target file is removed upon network error."""
    import pytest

    target_file = tmp_path / "failing.json"
    with patch("urllib.request.urlopen", side_effect=RuntimeError("Network broken")):
        with pytest.raises(RuntimeError, match="Network broken"):
            download_file(
                url="https://www.sec.gov/files/fail.json",
                dest_path=target_file,
                force=True,
            )
    assert not target_file.exists()
    assert not target_file.with_suffix(".json.tmp").exists()
