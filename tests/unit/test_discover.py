"""Unit tests for drop discovery, format inspection, and drop identity."""

from __future__ import annotations

import gzip
from pathlib import Path

import duckdb
import pytest

from vdok.discover import (
    Compression,
    CorruptedArchiveError,
    DiscoveryError,
    EmptyDropError,
    FileFormat,
    HeterogeneousFormatError,
    check_idempotency,
    compute_drop_id,
    detect_file_format,
    scan,
)

FIXTURES_DIR = Path("tests/fixtures")
CSV_HASH = "54390f9a26ad847a15758eee3903f93f004443be90372e8a17dbfdd02db611c9"
JSONL_HASH = "3a5f309004d6aa9f9df6c8815827de1a164482ede54ea5454b178bb8e11d520c"
PARQUET_HASH = "99d309112c4a8d6536a570854b766f1aec91a4cdd123ef633a5545534f0e79d0"


def test_discover_baseline_csv() -> None:
    drop_path = FIXTURES_DIR / "drops" / "baseline_csv"
    meta = scan(drop_path)
    assert meta.drop_id == "6ad1187f7e1ff96c"
    assert meta.file_format == FileFormat.CSV
    assert meta.compression == Compression.NONE
    assert len(meta.files) == 1
    assert meta.files[0].content_hash == CSV_HASH
    assert meta.total_bytes == 909


def test_discover_baseline_csv_gz() -> None:
    drop_path = FIXTURES_DIR / "drops" / "baseline_csv_gz"
    meta = scan(drop_path)
    # Crucial guarantee: uncompressed payload hashing gives identical drop_id as uncompressed CSV
    assert meta.drop_id == "6ad1187f7e1ff96c"
    assert meta.file_format == FileFormat.CSV
    assert meta.compression == Compression.GZIP
    assert len(meta.files) == 1
    assert meta.files[0].content_hash == CSV_HASH


def test_discover_baseline_jsonl() -> None:
    drop_path = FIXTURES_DIR / "drops" / "baseline_jsonl"
    meta = scan(drop_path)
    assert meta.drop_id == "8f548c6a42b31fa6"
    assert meta.file_format == FileFormat.JSONL
    assert meta.compression == Compression.NONE
    assert len(meta.files) == 1
    assert meta.files[0].content_hash == JSONL_HASH


def test_discover_baseline_jsonl_gz() -> None:
    drop_path = FIXTURES_DIR / "drops" / "baseline_jsonl_gz"
    meta = scan(drop_path)
    # Crucial guarantee: uncompressed payload hashing gives identical drop_id as uncompressed JSONL
    assert meta.drop_id == "8f548c6a42b31fa6"
    assert meta.file_format == FileFormat.JSONL
    assert meta.compression == Compression.GZIP
    assert len(meta.files) == 1
    assert meta.files[0].content_hash == JSONL_HASH


def test_discover_baseline_parquet() -> None:
    drop_path = FIXTURES_DIR / "drops" / "baseline_parquet"
    meta = scan(drop_path)
    assert meta.drop_id == "88b5201eb8c05f51"
    assert meta.file_format == FileFormat.PARQUET
    assert meta.compression == Compression.NONE
    assert len(meta.files) == 1
    assert meta.files[0].content_hash == PARQUET_HASH


def test_discover_multi_file_csv() -> None:
    drop_path = FIXTURES_DIR / "drops" / "multi_file_csv"
    meta = scan(drop_path)
    assert meta.drop_id == "4de7e8bd5d87d6c4"
    assert meta.file_format == FileFormat.CSV
    assert meta.compression == Compression.NONE
    assert len(meta.files) == 2
    assert [f.relative_path for f in meta.files] == ["part1.csv", "part2.csv"]


def test_discover_multi_file_parquet() -> None:
    drop_path = FIXTURES_DIR / "drops" / "multi_file_parquet"
    meta = scan(drop_path)
    assert meta.drop_id == "8dda38b3b038793b"
    assert meta.file_format == FileFormat.PARQUET
    assert meta.compression == Compression.NONE
    assert len(meta.files) == 2
    assert [f.relative_path for f in meta.files] == ["part1.parquet", "part2.parquet"]


def test_discover_single_file() -> None:
    single_file = FIXTURES_DIR / "drops" / "baseline_csv" / "data.csv"
    meta = scan(single_file)
    assert meta.drop_id == "6ad1187f7e1ff96c"
    assert len(meta.files) == 1


def test_discover_nonexistent_path_raises() -> None:
    with pytest.raises(FileNotFoundError):
        scan(FIXTURES_DIR / "drops" / "does_not_exist_anywhere")


def test_discover_empty_directory_raises(tmp_path: Path) -> None:
    empty_dir = tmp_path / "empty_drop"
    empty_dir.mkdir()
    with pytest.raises(EmptyDropError):
        scan(empty_dir)


def test_discover_mixed_formats_rejected() -> None:
    mixed_dir = FIXTURES_DIR / "drops" / "invalid_mixed_formats"
    with pytest.raises(HeterogeneousFormatError, match="mixed data formats"):
        scan(mixed_dir)


def test_discover_corrupted_gzip_rejected() -> None:
    corrupt_dir = FIXTURES_DIR / "drops" / "invalid_corrupted_gzip"
    with pytest.raises(CorruptedArchiveError):
        scan(corrupt_dir)


def test_discover_empty_file_rejected(tmp_path: Path) -> None:
    empty_file = tmp_path / "empty.csv"
    empty_file.write_bytes(b"")
    with pytest.raises(DiscoveryError, match="empty"):
        scan(empty_file)


def test_compute_drop_id_order_invariance() -> None:
    hashes_1 = [
        "f560666911612b9df26f8eca62d69ae17340cacb908fd05fc11b54eb9e4a023c",
        "b2d412f61ccb31ad92ea210d16c29f40339fea44902ddf9d6188f7d9fd0b52a4",
    ]
    hashes_2 = list(reversed(hashes_1))
    assert compute_drop_id(hashes_1) == compute_drop_id(hashes_2)
    assert compute_drop_id(hashes_1) == "4de7e8bd5d87d6c4"


def test_detect_file_format_direct() -> None:
    fmt, comp = detect_file_format(FIXTURES_DIR / "drops" / "baseline_csv" / "data.csv")
    assert fmt == FileFormat.CSV
    assert comp == Compression.NONE

    fmt, comp = detect_file_format(FIXTURES_DIR / "drops" / "baseline_csv_gz" / "data.csv.gz")
    assert fmt == FileFormat.CSV
    assert comp == Compression.GZIP

    fmt, comp = detect_file_format(FIXTURES_DIR / "drops" / "baseline_jsonl" / "data.jsonl")
    assert fmt == FileFormat.JSONL
    assert comp == Compression.NONE

    fmt, comp = detect_file_format(FIXTURES_DIR / "drops" / "baseline_parquet" / "data.parquet")
    assert fmt == FileFormat.PARQUET
    assert comp == Compression.NONE


def test_check_idempotency_duckdb(tmp_path: Path) -> None:
    db_file = tmp_path / "store.duckdb"
    con = duckdb.connect(str(db_file))
    con.execute("CREATE TABLE drops (drop_id VARCHAR, status VARCHAR)")
    con.execute("INSERT INTO drops VALUES ('6ad1187f7e1ff96c', 'COMMITTED')")
    con.close()

    assert check_idempotency("6ad1187f7e1ff96c", store_path=tmp_path) is True
    assert check_idempotency("uncommitted_drop", store_path=tmp_path) is False
    assert check_idempotency("6ad1187f7e1ff96c", store_path=None) is False


def test_check_idempotency_parquet_partition(tmp_path: Path) -> None:
    partition_file = tmp_path / "part-6ad1187f7e1ff96c.parquet"
    partition_file.touch()

    assert check_idempotency("6ad1187f7e1ff96c", store_path=tmp_path) is True
    assert check_idempotency("other_id", store_path=tmp_path) is False


def test_detect_file_format_errors(tmp_path: Path) -> None:
    from vdok.discover import FileAccessError, FormatMismatchError, UnrecognizedFormatError

    # Non-file path raises FileAccessError
    with pytest.raises(FileAccessError):
        detect_file_format(tmp_path)

    # Fake parquet file without PAR1 header/footer raises FormatMismatchError
    bad_pq = tmp_path / "corrupt.parquet"
    bad_pq.write_bytes(b"NOT_A_PARQUET_HEADER_OR_FOOTER")
    with pytest.raises(FormatMismatchError):
        detect_file_format(bad_pq)

    # Whitespace only file raises DiscoveryError
    whitespace_file = tmp_path / "whitespace.txt"
    whitespace_file.write_text("   \n\n\t  \n", encoding="utf-8")
    with pytest.raises(DiscoveryError, match="no non-empty"):
        detect_file_format(whitespace_file)

    # Unrecognized binary format raises UnrecognizedFormatError
    unrec_file = tmp_path / "unknown.bin"
    unrec_file.write_bytes(b"\x00\x01\x02\x03\x04\x05")
    with pytest.raises(UnrecognizedFormatError):
        detect_file_format(unrec_file)


def test_discover_mixed_compression(tmp_path: Path) -> None:
    drop_dir = tmp_path / "mixed_comp_drop"
    drop_dir.mkdir()
    (drop_dir / "a.csv").write_text("id,val\n1,10\n", encoding="utf-8")
    import gzip

    (drop_dir / "b.csv.gz").write_bytes(gzip.compress(b"id,val\n2,20\n", mtime=0))

    with pytest.raises(HeterogeneousFormatError, match="mixed compression"):
        scan(drop_dir)


def test_check_idempotency_nonexistent_store(tmp_path: Path) -> None:
    assert check_idempotency("any_id", store_path=tmp_path / "nonexistent") is False


def test_discover_single_column_csv_unquoted(tmp_path: Path) -> None:
    """Verify single-column CSV without commas is recognized as CSV."""
    f = tmp_path / "tickers.csv"
    f.write_text("ticker\nAAPL\nMSFT\nGOOGL\n", encoding="utf-8")
    fmt, comp = detect_file_format(f)
    assert fmt == FileFormat.CSV
    assert comp == Compression.NONE

    meta = scan(tmp_path)
    assert meta.file_format == FileFormat.CSV
    assert len(meta.files) == 1


def test_discover_single_column_csv_quoted(tmp_path: Path) -> None:
    """Verify single-column CSV with quotes is recognized as CSV."""
    f = tmp_path / "ciks.csv"
    f.write_text('"cik"\n"0000320193"\n"0000320194"\n', encoding="utf-8")
    fmt, comp = detect_file_format(f)
    assert fmt == FileFormat.CSV
    assert comp == Compression.NONE


def test_discover_single_column_csv_gzipped(tmp_path: Path) -> None:
    """Verify single-column gzipped CSV is recognized as CSV with GZIP compression."""
    f = tmp_path / "tickers.csv.gz"
    f.write_bytes(gzip.compress(b"ticker\nAAPL\nMSFT\n", mtime=0))
    fmt, comp = detect_file_format(f)
    assert fmt == FileFormat.CSV
    assert comp == Compression.GZIP


def test_discover_leading_empty_lines_tolerance_csv(tmp_path: Path) -> None:
    """Verify files with 6 or more leading blank lines are recognized without DiscoveryError."""
    f = tmp_path / "leading_blanks.csv"
    f.write_text("\n\n\n\n\n\ncik,val\n0000320193,100\n", encoding="utf-8")
    fmt, comp = detect_file_format(f)
    assert fmt == FileFormat.CSV
    assert comp == Compression.NONE

    meta = scan(tmp_path)
    assert meta.file_format == FileFormat.CSV


def test_discover_leading_empty_lines_tolerance_csv_gz(tmp_path: Path) -> None:
    """Verify gzipped files with leading blank lines are recognized."""
    f = tmp_path / "leading_blanks.csv.gz"
    f.write_bytes(gzip.compress(b"\n\n\n\n\n\n\n\ncik,val\n0000320193,100\n", mtime=0))
    fmt, comp = detect_file_format(f)
    assert fmt == FileFormat.CSV
    assert comp == Compression.GZIP


def test_discover_corrupted_gzip_deflate_stream_raises_corrupted_archive(tmp_path: Path) -> None:
    """Verify corrupted DEFLATE streams trigger CorruptedArchiveError without leaking."""
    valid_data = b"cik,event_time,value\n" + b"".join(
        f"0000320193,2023-12-31,{i}\n".encode("utf-8") for i in range(50)
    )
    corrupted_gz = bytearray(gzip.compress(valid_data, mtime=0))
    corrupted_gz[35] ^= 0xFF

    drop_dir = tmp_path / "corrupted_drop"
    drop_dir.mkdir()
    (drop_dir / "data.csv.gz").write_bytes(bytes(corrupted_gz))

    with pytest.raises(CorruptedArchiveError):
        scan(drop_dir)

    with pytest.raises(CorruptedArchiveError):
        detect_file_format(drop_dir / "data.csv.gz")


def test_check_idempotency_wildcard_rejection(tmp_path: Path) -> None:
    """Verify wildcard characters in drop_id return False and do not match parquet partitions."""
    part_file = tmp_path / "part-6ad1187f7e1ff96c.parquet"
    part_file.touch()

    assert check_idempotency("6ad1187f7e1ff96c", store_path=tmp_path) is True
    assert check_idempotency("*", store_path=tmp_path) is False
    assert check_idempotency("part-*", store_path=tmp_path) is False
    assert check_idempotency("6ad1187f7e1ff96?", store_path=tmp_path) is False
    assert check_idempotency("[0-9]*", store_path=tmp_path) is False


def test_check_idempotency_wildcard_in_duckdb(tmp_path: Path) -> None:
    """Verify wildcard queries return False when querying DuckDB store."""
    db_file = tmp_path / "store.duckdb"
    con = duckdb.connect(str(db_file))
    con.execute("CREATE TABLE drops (drop_id VARCHAR, status VARCHAR)")
    con.execute("INSERT INTO drops VALUES ('6ad1187f7e1ff96c', 'COMMITTED')")
    con.close()

    assert check_idempotency("6ad1187f7e1ff96c", store_path=tmp_path) is True
    assert check_idempotency("*", store_path=tmp_path) is False
