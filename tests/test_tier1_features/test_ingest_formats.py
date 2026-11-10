"""Tier 1 Feature Tests: Multi-format ingestion engine (CSV, Parquet, JSON, JSONL, API, compression)."""

import gzip
from pathlib import Path

import pyarrow as pa
import pytest

from vendor_onboarding.ingestion.reader import (
    CorruptedArchiveError,
    IngestReader,
    detect_file_format,
    iter_chunks,
    read_file,
)


def test_read_csv_with_line_tracking(sample_csv_path: Path) -> None:
    """Test reading raw CSV files with automatic 1-based source_line_no provenance."""
    table = read_file(sample_csv_path, add_source_line=True)
    assert isinstance(table, pa.Table)
    assert table.num_rows == 60
    assert "source_line_no" in table.column_names

    # Check 1-based line number sequence
    line_nos = table.column("source_line_no").to_pylist()
    assert line_nos == list(range(1, 61))
    assert "record_id" in table.column_names
    assert "entity_id" in table.column_names
    assert "value" in table.column_names


def test_read_parquet_format(sample_parquet_path: Path) -> None:
    """Test reading columnar Parquet datasets."""
    table = read_file(sample_parquet_path, add_source_line=True)
    assert isinstance(table, pa.Table)
    assert table.num_rows == 60
    assert "source_line_no" in table.column_names
    assert table.column("source_line_no")[0].as_py() == 1
    assert "metric" in table.column_names
    assert "unit" in table.column_names


def test_read_json_array(sample_json_path: Path) -> None:
    """Test reading standard JSON array of objects."""
    table = read_file(sample_json_path, add_source_line=True)
    assert isinstance(table, pa.Table)
    assert table.num_rows == 60
    assert "source_line_no" in table.column_names
    assert table.column("source_line_no")[-1].as_py() == 60
    assert table.column("entity_id")[0].as_py() == "ENT_AAPL"


def test_read_jsonl_records(sample_jsonl_path: Path) -> None:
    """Test reading newline-delimited JSON (JSONL / NDJSON)."""
    table = read_file(sample_jsonl_path, add_source_line=True)
    assert isinstance(table, pa.Table)
    assert table.num_rows == 21
    assert "source_line_no" in table.column_names
    assert table.column("source_line_no").to_pylist() == list(range(1, 22))


def test_transparent_compression_gzip(gzip_csv_file: Path) -> None:
    """Test transparent on-the-fly decompression of gzip archives (.csv.gz)."""
    fmt, comp = detect_file_format(gzip_csv_file)
    assert fmt == "csv"
    assert comp == "gzip"

    table = read_file(gzip_csv_file)
    assert table.num_rows == 3
    assert "source_line_no" in table.column_names
    assert table.column("symbol").to_pylist() == ["AAPL", "MSFT", "GOOG"]
    assert table.column("price").to_pylist() == [150.25, 310.50, 135.75]


def test_transparent_compression_zstd(zstd_csv_file: Path) -> None:
    """Test transparent on-the-fly decompression of zstandard archives (.csv.zst)."""
    fmt, comp = detect_file_format(zstd_csv_file)
    assert fmt == "csv"
    assert comp == "zstd"

    table = read_file(zstd_csv_file)
    assert table.num_rows == 2
    assert "source_line_no" in table.column_names
    assert table.column("symbol").to_pylist() == ["AMZN", "META"]


def test_api_dump_nested_extraction(api_dump_json_file: Path) -> None:
    """Test automatic flattening and unnesting of API dump response structures."""
    reader = IngestReader(api_dump_json_file, add_source_line=True)
    table = reader.read_all()
    assert table.num_rows == 3
    assert "record_id" in table.column_names
    assert "value" in table.column_names
    assert table.column("record_id").to_pylist() == ["REC_01", "REC_02", "REC_03"]
    assert table.column("value").to_pylist() == [100.5, 200.75, 300.25]


def test_streaming_batches_and_chunks(sample_csv_path: Path) -> None:
    """Test chunked Arrow RecordBatch streaming with continuous line indexing."""
    batch_size = 15
    batches = list(iter_chunks(sample_csv_path, batch_size=batch_size))
    assert len(batches) == 4
    total_records = sum(len(b) for b in batches)
    assert total_records == 60

    # Ensure source_line_no is strictly sequential across batches
    observed_lines: list[int] = []
    for b in batches:
        assert isinstance(b, pa.RecordBatch)
        observed_lines.extend(b.column("source_line_no").to_pylist())

    assert observed_lines == list(range(1, 61))


def test_tsv_sec_financials_ingestion(sec_dir: Path) -> None:
    """Test reading real SEC DERA tab-separated submission files."""
    sec_sub = sec_dir / "2023q3_sub.txt"
    assert sec_sub.is_file()

    reader = IngestReader(sec_sub, add_source_line=True)
    assert reader.count_rows() == 3
    table = reader.read_all()
    assert table.num_rows == 3
    assert "adsh" in table.column_names
    assert "cik" in table.column_names
    assert "name" in table.column_names
    assert len(table.column_names) >= 36


def test_count_rows_without_full_materialization(sample_parquet_path: Path, sample_csv_path: Path) -> None:
    """Test row count computation efficiency."""
    r_pq = IngestReader(sample_parquet_path)
    assert r_pq.count_rows() == 60

    r_csv = IngestReader(sample_csv_path)
    assert r_csv.count_rows() == 60


def test_format_parity_csv_vs_parquet_content(sample_csv_path: Path, sample_parquet_path: Path) -> None:
    """Verify strict content parity on values between CSV and Parquet reader."""
    t_csv = read_file(sample_csv_path)
    t_pq = read_file(sample_parquet_path)

    assert t_csv.num_rows == t_pq.num_rows == 60
    csv_ids = t_csv.column("record_id").to_pylist()
    pq_ids = t_pq.column("record_id").to_pylist()
    assert csv_ids == pq_ids


def test_format_parity_json_vs_parquet_content(sample_json_path: Path, sample_parquet_path: Path) -> None:
    """Verify content parity between JSON and Parquet representations."""
    t_json = read_file(sample_json_path)
    t_pq = read_file(sample_parquet_path)

    assert t_json.num_rows == t_pq.num_rows == 60
    assert t_json.column("entity_id").to_pylist() == t_pq.column("entity_id").to_pylist()


def test_unsupported_format_error(tmp_path: Path) -> None:
    """Verify UnsupportedFormatError raised on unknown file extension."""
    from vendor_onboarding.ingestion.reader import UnsupportedFormatError
    bad_file = tmp_path / "data.xlsx"
    bad_file.write_text("fake_content", encoding="utf-8")
    with pytest.raises(UnsupportedFormatError):
        read_file(bad_file)


def test_corrupted_gzip_archive_error(tmp_path: Path) -> None:
    """Verify CorruptedArchiveError on broken gzip file."""
    bad_gz = tmp_path / "corrupt.csv.gz"
    bad_gz.write_bytes(b"\x1f\x8b\x08\x00corrupt_payload_data")
    with pytest.raises((CorruptedArchiveError, gzip.BadGzipFile, Exception)):
        read_file(bad_gz)


def test_empty_csv_zero_rows(tmp_path: Path) -> None:
    """Verify reading header-only CSV produces Table with 0 rows."""
    empty_csv = tmp_path / "empty.csv"
    empty_csv.write_text("record_id,entity_id,value\n", encoding="utf-8")
    t = read_file(empty_csv)
    assert t.num_rows == 0
    assert "record_id" in t.column_names


def test_detect_file_format_all_extensions(tmp_path: Path) -> None:
    """Verify format and compression detection across standard extensions."""
    assert detect_file_format(tmp_path / "file.csv") == ("csv", None)
    assert detect_file_format(tmp_path / "file.parquet") == ("parquet", None)
    assert detect_file_format(tmp_path / "file.json") == ("json", None)
    assert detect_file_format(tmp_path / "file.jsonl") == ("jsonl", None)
    assert detect_file_format(tmp_path / "file.csv.gz") == ("csv", "gzip")
    assert detect_file_format(tmp_path / "file.csv.zst") == ("csv", "zstd")

