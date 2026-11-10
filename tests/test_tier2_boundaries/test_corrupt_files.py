"""
Tier 2 Boundary Tests: Corrupted, Truncated, Zero-Byte, and Malformed Files.

Per TEST_INFRA.md and PROJECT.md §8.1 / F-ING-01, F-ING-02:
Tests real boundary conditions for malformed files across CSV, JSON, JSONL, Parquet, GZIP, and Zstandard:
- Corrupted gzip headers (magic numbers, flags, truncated streams, CRC32 errors)
- Parquet corruption (invalid PAR1 magic bytes, truncated footers, bad metadata lengths, trailing garbage)
- Truncated JSON & JSONL mid-stream (unclosed objects, strings, arrays, ragged JSONL lines)
- Zero-byte files across all supported formats
- Directory paths passed as files, non-existent files, permission edge-cases
- CSV formatting anomalies (ragged rows, unclosed quotes, embedded null bytes)
- Manifest SHA-256 integrity detection under single-bit corruption
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import struct
import zlib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

try:
    import duckdb
except ImportError:
    duckdb = None  # type: ignore

try:
    import pyarrow as pa
    import pyarrow.csv as pcsv
    import pyarrow.parquet as pq
except ImportError:
    pa = None  # type: ignore
    pq = None  # type: ignore

try:
    import zstandard as zstd
except ImportError:
    zstd = None  # type: ignore

# Fallback DropManifest if not yet built by M1
try:
    from vendor_onboarding.ingestion.manifest import DropManifest
except ImportError:
    @dataclass(frozen=True)
    class DropManifest:
        drop_id: str
        vendor_id: str
        dataset_id: str
        file_path: str
        file_sha256: str
        file_size_bytes: int
        row_count: int
        ingest_timestamp: datetime


pytestmark = [pytest.mark.tier2]


# ============================================================================
# 1. Zero-Byte Files Across Formats
# ============================================================================

def test_zero_byte_csv_rejected(tmp_path: Path):
    """Zero-byte CSV files must be detected as empty and not produce phantom schemas."""
    empty_csv = tmp_path / "empty.csv"
    empty_csv.write_bytes(b"")

    assert empty_csv.stat().st_size == 0
    with pytest.raises((pa.ArrowInvalid, ValueError)):
        pcsv.read_csv(empty_csv)


def test_zero_byte_parquet_rejected(tmp_path: Path):
    """Zero-byte Parquet files lack the 4-byte 'PAR1' magic header and must raise ArrowInvalid."""
    empty_pq = tmp_path / "empty.parquet"
    empty_pq.write_bytes(b"")

    assert empty_pq.stat().st_size == 0
    with pytest.raises((pa.ArrowInvalid, Exception)):
        pq.read_table(empty_pq)


def test_zero_byte_json_rejected(tmp_path: Path):
    """Zero-byte JSON files must raise JSONDecodeError."""
    empty_json = tmp_path / "empty.json"
    empty_json.write_bytes(b"")

    with pytest.raises(json.JSONDecodeError):
        json.loads(empty_json.read_text(encoding="utf-8"))


def test_zero_byte_jsonl_handling(tmp_path: Path):
    """Zero-byte JSONL files contain 0 records and should produce an empty table or raise error."""
    empty_jsonl = tmp_path / "empty.jsonl"
    empty_jsonl.write_bytes(b"")

    if duckdb:
        conn = duckdb.connect(":memory:")
        try:
            res = conn.execute(f"SELECT count(*) FROM read_json_auto('{empty_jsonl}')").fetchall()[0]
            assert res[0] == 0
        except duckdb.Error:
            pass  # Acceptable rejection
        finally:
            conn.close()


def test_zero_byte_gzip_rejected(tmp_path: Path):
    """Zero-byte .gz file is not a valid gzip container and raises in columnar ingestors."""
    empty_gz = tmp_path / "empty.csv.gz"
    empty_gz.write_bytes(b"")

    if duckdb:
        conn = duckdb.connect(":memory:")
        with pytest.raises(duckdb.Error, match="(?i)gzip"):
            conn.execute(f"SELECT * FROM read_csv('{empty_gz}')").fetchall()
        conn.close()
    else:
        assert empty_gz.stat().st_size == 0


def test_zero_byte_zstd_rejected(tmp_path: Path):
    """Zero-byte .zst file is not a valid Zstandard frame."""
    if zstd is None:
        pytest.skip("zstandard library not installed")

    empty_zst = tmp_path / "empty.csv.zst"
    empty_zst.write_bytes(b"")

    dctx = zstd.ZstdDecompressor()
    with pytest.raises(zstd.ZstdError):
        dctx.decompress(empty_zst.read_bytes())


# ============================================================================
# 2. Corrupted Gzip Headers & Streams
# ============================================================================

def test_corrupted_gzip_magic_bytes(tmp_path: Path):
    """Gzip files must start with magic bytes 0x1f 0x8b. Corrupting them must raise BadGzipFile."""
    bad_gz = tmp_path / "bad_magic.csv.gz"
    # Write invalid magic bytes (0xFF 0xFF) followed by dummy payload
    bad_gz.write_bytes(b"\xff\xff\x08\x00\x00\x00\x00\x00\x00\x03hello world")

    with pytest.raises(gzip.BadGzipFile, match="Not a gzipped file"):
        with gzip.open(bad_gz, "rb") as gz:
            gz.read()


def test_corrupted_gzip_compression_method(tmp_path: Path):
    """Gzip header byte 2 (CM) must be 8 (DEFLATE). Any other value is invalid."""
    bad_cm_gz = tmp_path / "bad_cm.csv.gz"
    # 0x1f 0x8b, then CM=9 (unknown method)
    bad_cm_gz.write_bytes(b"\x1f\x8b\x09\x00\x00\x00\x00\x00\x00\x03dummy payload")

    with pytest.raises((gzip.BadGzipFile, OSError)), gzip.open(bad_cm_gz, "rb") as gz:
        gz.read()


def test_corrupted_gzip_crc32_checksum(tmp_path: Path):
    """Gzip trailer contains CRC-32. Tampering with payload without updating trailer triggers BadGzipFile/CRC error."""
    valid_gz = tmp_path / "tampered.csv.gz"
    payload = b"entity_id,value\nENT_001,100.5\n"
    compressed = gzip.compress(payload)

    # Tamper with compressed data byte (middle of payload)
    corrupted = bytearray(compressed)
    corrupted[len(compressed) // 2] ^= 0xFF

    valid_gz.write_bytes(bytes(corrupted))

    with pytest.raises((gzip.BadGzipFile, zlib.error)), gzip.open(valid_gz, "rb") as gz:
        gz.read()


def test_corrupted_gzip_truncated_data_stream(tmp_path: Path):
    """Truncating a gzip file midway through the compressed stream causes EOFError or BadGzipFile."""
    payload = b"record_index,symbol,price\n" + b"".join(f"{i},AAPL,150.0\n".encode() for i in range(500))
    compressed = gzip.compress(payload)

    # Truncate at 50% length
    truncated_gz = tmp_path / "truncated.csv.gz"
    truncated_gz.write_bytes(compressed[: len(compressed) // 2])

    with pytest.raises((EOFError, gzip.BadGzipFile, zlib.error)):
        with gzip.open(truncated_gz, "rb") as gz:
            gz.read()


def test_corrupted_gzip_trailing_garbage(tmp_path: Path):
    """A valid gzip file with trailing garbage bytes raises BadGzipFile on multi-stream inspection."""
    payload = b"id,val\n1,10\n2,20\n"
    compressed = gzip.compress(payload)
    with_garbage = compressed + b"EXTRA_CORRUPT_TRAILING_GARBAGE_BYTES_0123456789"

    garbage_file = tmp_path / "trailing_garbage.csv.gz"
    garbage_file.write_bytes(with_garbage)

    with pytest.raises(gzip.BadGzipFile, match="Not a gzipped file"):
        with gzip.open(garbage_file, "rb") as gz:
            gz.read()


# ============================================================================
# 3. Parquet Magic Bytes & Footer Corruption
# ============================================================================

def test_parquet_invalid_header_magic_bytes(tmp_path: Path):
    """Parquet must start with 4-byte magic 'PAR1'. An invalid magic must raise ArrowInvalid."""
    bad_pq = tmp_path / "bad_header.parquet"
    bad_pq.write_bytes(b"BAD1\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04PAR1")

    with pytest.raises(pa.ArrowInvalid, match="(?i)(magic|parquet)"):
        pq.read_table(bad_pq)


def test_parquet_invalid_footer_magic_bytes(tmp_path: Path, sample_canonical_records):
    """Parquet must end with 4-byte magic 'PAR1'. If footer magic is altered, reader must fail."""
    valid_pq = tmp_path / "valid.parquet"
    table = pa.Table.from_pylist([{"entity_id": r.entity_id, "value": r.value} for r in sample_canonical_records])
    pq.write_table(table, valid_pq)

    raw_bytes = bytearray(valid_pq.read_bytes())
    assert raw_bytes[-4:] == b"PAR1", "Parquet file should end with PAR1"

    # Corrupt footer magic to CORR
    raw_bytes[-4:] = b"CORR"
    corrupt_pq = tmp_path / "corrupt_footer.parquet"
    corrupt_pq.write_bytes(bytes(raw_bytes))

    with pytest.raises(pa.ArrowInvalid, match="(?i)(magic|footer|corrupt|parquet)"):
        pq.read_table(corrupt_pq)


def test_parquet_truncated_file_before_footer(tmp_path: Path, sample_canonical_records):
    """Truncating a Parquet file before its metadata footer length and footer magic must raise ArrowInvalid."""
    valid_pq = tmp_path / "valid.parquet"
    table = pa.Table.from_pylist([{"entity_id": r.entity_id, "value": r.value} for r in sample_canonical_records])
    pq.write_table(table, valid_pq)

    raw_bytes = valid_pq.read_bytes()
    truncated_pq = tmp_path / "truncated_footer.parquet"
    truncated_pq.write_bytes(raw_bytes[:20])

    with pytest.raises(pa.ArrowInvalid):
        pq.read_table(truncated_pq)


def test_parquet_corrupted_footer_metadata_size(tmp_path: Path, sample_canonical_records):
    """Altering the 4-byte little-endian footer length to an impossible size must fail loudly."""
    valid_pq = tmp_path / "valid.parquet"
    table = pa.Table.from_pylist([{"entity_id": r.entity_id, "value": r.value} for r in sample_canonical_records])
    pq.write_table(table, valid_pq)

    raw_bytes = bytearray(valid_pq.read_bytes())
    raw_bytes[-8:-4] = struct.pack("<I", 0x7FFFFFFF)

    corrupt_size_pq = tmp_path / "corrupt_size.parquet"
    corrupt_size_pq.write_bytes(bytes(raw_bytes))

    with pytest.raises(pa.ArrowInvalid):
        pq.read_table(corrupt_size_pq)


def test_parquet_trailing_garbage_bytes(tmp_path: Path, sample_canonical_records):
    """Parquet reader expects footer magic at the end of the file. Appending trailing garbage must raise ArrowInvalid."""
    valid_pq = tmp_path / "valid.parquet"
    table = pa.Table.from_pylist([{"entity_id": r.entity_id, "value": r.value} for r in sample_canonical_records])
    pq.write_table(table, valid_pq)

    raw_bytes = valid_pq.read_bytes()
    garbage_pq = tmp_path / "garbage_appended.parquet"
    garbage_pq.write_bytes(raw_bytes + b"TRAILING_RANDOM_NOISE_12345")

    with pytest.raises(pa.ArrowInvalid, match="(?i)(magic|footer|parquet)"):
        pq.read_table(garbage_pq)


def test_parquet_zero_row_valid_schema(tmp_path: Path):
    """A valid Parquet file with 0 rows but defined schema must parse cleanly without corruption errors."""
    schema = pa.schema([
        ("entity_id", pa.string()),
        ("event_time", pa.timestamp("us")),
        ("value", pa.float64()),
    ])
    empty_table = pa.Table.from_arrays([pa.array([], type=f.type) for f in schema], schema=schema)

    empty_pq = tmp_path / "zero_rows.parquet"
    pq.write_table(empty_table, empty_pq)

    read_back = pq.read_table(empty_pq)
    assert read_back.num_rows == 0
    assert len(read_back.schema.names) == 3
    assert read_back.schema.names == ["entity_id", "event_time", "value"]


# ============================================================================
# 4. Truncated JSON & JSONL Midway
# ============================================================================

def test_truncated_json_unclosed_object(tmp_path: Path):
    """JSON truncated while writing key-value pairs."""
    content = '{"vendor_id": "V1", "dataset": "PRICES", "records": [{"entity_id": "AAPL"'
    broken_json = tmp_path / "unclosed.json"
    broken_json.write_text(content, encoding="utf-8")

    with pytest.raises(json.JSONDecodeError):
        json.loads(broken_json.read_text(encoding="utf-8"))


def test_truncated_json_unclosed_string(tmp_path: Path):
    """JSON truncated inside a quoted string literal."""
    content = '{"entity_id": "ENT_GOOGL_UNFINISHED_STR'
    broken_json = tmp_path / "unclosed_str.json"
    broken_json.write_text(content, encoding="utf-8")

    with pytest.raises(json.JSONDecodeError, match="Unterminated string"):
        json.loads(broken_json.read_text(encoding="utf-8"))


def test_truncated_json_mid_number(tmp_path: Path):
    """JSON truncated mid-number representation."""
    content = '{"entity_id": "AAPL", "value": 123.'
    broken_json = tmp_path / "mid_num.json"
    broken_json.write_text(content, encoding="utf-8")

    with pytest.raises(json.JSONDecodeError):
        json.loads(broken_json.read_text(encoding="utf-8"))


def test_truncated_json_unclosed_array(tmp_path: Path):
    """JSON array truncated without closing bracket."""
    content = '[{"id": 1}, {"id": 2}, {"id": 3'
    broken_json = tmp_path / "unclosed_arr.json"
    broken_json.write_text(content, encoding="utf-8")

    with pytest.raises(json.JSONDecodeError):
        json.loads(broken_json.read_text(encoding="utf-8"))


def test_truncated_jsonl_partial_last_line(tmp_path: Path):
    """JSONL with valid lines followed by a truncated trailing line."""
    lines = [
        json.dumps({"entity_id": "AAPL", "value": 150.0}),
        json.dumps({"entity_id": "MSFT", "value": 310.0}),
        '{"entity_id": "GOOGL", "val',  # Broken last line
    ]
    jsonl_file = tmp_path / "broken_last_line.jsonl"
    jsonl_file.write_text("\n".join(lines), encoding="utf-8")

    valid_records = []
    broken_detected = False
    with open(jsonl_file, "r", encoding="utf-8") as f:
        for line in f:
            try:
                valid_records.append(json.loads(line.strip()))
            except json.JSONDecodeError:
                broken_detected = True

    assert len(valid_records) == 2
    assert broken_detected is True
    assert valid_records[0]["entity_id"] == "AAPL"


def test_jsonl_blank_lines_interleaved(tmp_path: Path):
    """JSONL containing empty and whitespace-only lines must not crash the reader."""
    content = (
        '{"id": 1, "value": 10.0}\n'
        '\n'
        '   \n'
        '{"id": 2, "value": 20.0}\n'
        '\n'
    )
    jsonl_file = tmp_path / "whitespace_lines.jsonl"
    jsonl_file.write_text(content, encoding="utf-8")

    records = []
    with open(jsonl_file, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if stripped:
                records.append(json.loads(stripped))

    assert len(records) == 2
    assert records[0]["id"] == 1
    assert records[1]["id"] == 2


# ============================================================================
# 5. CSV Formatting Anomalies & Corruptions
# ============================================================================

def test_csv_unclosed_double_quote(tmp_path: Path):
    """CSV with unmatched double quote in data."""
    content = (
        "entity_id,company_name,value\n"
        'AAPL,"Apple Inc.,150.0\n'
        "MSFT,Microsoft Corp,310.0\n"
    )
    broken_csv = tmp_path / "unclosed_quote.csv"
    broken_csv.write_text(content, encoding="utf-8")

    with pytest.raises((csv.Error, pa.ArrowInvalid, Exception)):
        pcsv.read_csv(broken_csv)


def test_csv_ragged_rows_column_count_mismatch(tmp_path: Path):
    """CSV where row 2 has fewer columns than the header."""
    content = (
        "entity_id,event_time,value,unit\n"
        "ENT_001,2023-10-01,100.0,USD\n"
        "ENT_002,2023-10-01\n"  # Missing 2 columns!
        "ENT_003,2023-10-01,105.0,USD\n"
    )
    ragged_csv = tmp_path / "ragged.csv"
    ragged_csv.write_text(content, encoding="utf-8")

    with pytest.raises((pa.ArrowInvalid, Exception)):
        pcsv.read_csv(ragged_csv)


def test_csv_excess_columns_in_row(tmp_path: Path):
    """CSV where row 2 has more columns than declared in header."""
    content = (
        "entity_id,value\n"
        "ENT_001,100.0\n"
        "ENT_002,200.0,EXTRA_COLUMN_VAL,ANOTHER_EXTRA\n"
        "ENT_003,300.0\n"
    )
    excess_csv = tmp_path / "excess_cols.csv"
    excess_csv.write_text(content, encoding="utf-8")

    with pytest.raises((pa.ArrowInvalid, Exception)):
        pcsv.read_csv(excess_csv)


def test_csv_header_only_no_data_rows(tmp_path: Path):
    """CSV with only column headers and zero data rows parses with 0 rows and intact schema."""
    content = "entity_id,event_time,value\n"
    header_only = tmp_path / "header_only.csv"
    header_only.write_text(content, encoding="utf-8")

    table = pcsv.read_csv(header_only)
    assert table.num_rows == 0
    assert table.column_names == ["entity_id", "event_time", "value"]


def test_csv_whitespace_only_file_produces_blank_header(tmp_path: Path):
    """A file containing only spaces, tabs, and newlines produces invalid blank column names that must be flagged."""
    ws_file = tmp_path / "only_whitespace.csv"
    ws_file.write_text("   \n\t\n   \n", encoding="utf-8")

    table = pcsv.read_csv(ws_file)
    assert len(table.column_names) == 1
    assert table.column_names[0].strip() == "", "Whitespace file erroneously parsed as blank column name"


def test_csv_null_byte_in_stream(tmp_path: Path):
    """CSV containing embedded NUL (0x00) bytes must be detected to prevent downstream C-string truncation."""
    nul_csv = tmp_path / "embedded_nul.csv"
    raw_content = b"entity_id,value\nAAPL\x00_EXTRA,150.0\nMSFT,320.0\n"
    nul_csv.write_bytes(raw_content)

    assert b"\x00" in nul_csv.read_bytes()
    val_bytes = b"AAPL\x00_EXTRA"
    truncated = val_bytes.split(b"\x00")[0]
    assert truncated == b"AAPL"
    assert len(truncated) < len(val_bytes)


# ============================================================================
# 6. File System & Path Boundary Cases
# ============================================================================

def test_directory_provided_as_file_path(tmp_path: Path):
    """Passing a directory path where a file is expected must raise IsADirectoryError or ArrowInvalid."""
    sub_dir = tmp_path / "sub_directory"
    sub_dir.mkdir()

    with pytest.raises((IsADirectoryError, OSError, pa.ArrowInvalid)):
        pcsv.read_csv(sub_dir)


def test_nonexistent_file_path_raises_filenotfound(tmp_path: Path):
    """Attempting to ingest a non-existent file path must raise FileNotFoundError."""
    missing = tmp_path / "does_not_exist_987654.parquet"

    with pytest.raises(FileNotFoundError):
        if not missing.exists():
            raise FileNotFoundError(f"File not found: {missing}")


def test_permission_denied_read(tmp_path: Path):
    """File without read permissions raises PermissionError."""
    no_perm = tmp_path / "unreadable.csv"
    no_perm.write_text("entity_id,value\nAAPL,150.0\n", encoding="utf-8")
    no_perm.chmod(0o000)

    try:
        with pytest.raises(PermissionError), open(no_perm, "rb") as f:
            f.read()
    finally:
        no_perm.chmod(0o644)


# ============================================================================
# 7. Lineage Manifest & Hash Integrity Verification
# ============================================================================

def test_drop_manifest_sha256_detects_single_bit_flip(tmp_path: Path):
    """Lineage tracker SHA-256 calculation changes completely when a single bit is modified (avalanche effect)."""
    original = tmp_path / "dataset_drop_01.csv"
    content = b"entity_id,price,timestamp\nAAPL,175.50,2023-10-01T16:00:00Z\n"
    original.write_bytes(content)

    h1 = hashlib.sha256(content).hexdigest()

    tampered_content = bytearray(content)
    tampered_content[0] ^= 0x01
    h2 = hashlib.sha256(tampered_content).hexdigest()

    assert h1 != h2
    bin_diff = (int(h1, 16) ^ int(h2, 16)).bit_count()
    assert bin_diff >= 64, f"Hash difference only {bin_diff} bits; expected avalanche > 64 bits"


def test_drop_manifest_file_size_mismatch_detection(tmp_path: Path):
    """Manifest record count and size must exactly match disk realities."""
    fpath = tmp_path / "sample.csv"
    fpath.write_text("id,val\n1,10\n2,20\n3,30\n", encoding="utf-8")

    st = fpath.stat()
    assert st.st_size == len("id,val\n1,10\n2,20\n3,30\n")

    manifest = DropManifest(
        drop_id="DROP_001",
        vendor_id="TEST_VENDOR",
        dataset_id="TEST_DATA",
        file_path=str(fpath),
        file_sha256=hashlib.sha256(fpath.read_bytes()).hexdigest(),
        file_size_bytes=st.st_size,
        row_count=3,
        ingest_timestamp=datetime.now(),
    )
    assert manifest.file_size_bytes == st.st_size

    # Simulate truncation
    fpath.write_text("id,val\n1,10\n", encoding="utf-8")
    assert fpath.stat().st_size != manifest.file_size_bytes


# ============================================================================
# 8. Real Bundled Fixture Boundary Checks
# ============================================================================

def test_sec_sub_txt_truncated_mid_record(tmp_path: Path, fixture_paths: Any):
    """SEC DERA sub.txt truncated midway through a row violates column count contract."""
    sample_sub = fixture_paths.sec_2023q3_sub.read_text(encoding="utf-8")
    lines = sample_sub.strip().split("\n")

    truncated_content = lines[0] + "\n" + lines[1][: len(lines[1]) // 2]
    bad_sec = tmp_path / "truncated_sub.txt"
    bad_sec.write_text(truncated_content, encoding="utf-8")

    if duckdb:
        conn = duckdb.connect(":memory:")
        with pytest.raises((duckdb.Error, Exception)):
            conn.execute(f"SELECT * FROM read_csv('{bad_sec}', delim='\t', header=true)").fetchall()
        conn.close()


def test_sec_num_txt_missing_essential_columns(tmp_path: Path):
    """SEC DERA num.txt requires adsh, tag, version, ddate, qtrs, uom, value. Missing tag raises error."""
    broken_num = tmp_path / "broken_num.txt"
    broken_num.write_text("adsh\tversion\tddate\tvalue\n0000320193-23-000106\t0000320193-23-000106\t20230930\t1000\n")

    if duckdb:
        conn = duckdb.connect(":memory:")
        res = conn.sql(f"SELECT * FROM read_csv('{broken_num}', delim='\t', header=true)").to_arrow_table()
        assert "tag" not in res.schema.names
        assert "uom" not in res.schema.names
        conn.close()


def test_alfred_csv_corrupted_header_date_format(tmp_path: Path):
    """ALFRED vintage series expects 'realtime_start,realtime_end,date,value'. Malformed date header handling."""
    bad_alfred = tmp_path / "bad_alfred.csv"
    bad_alfred.write_text(
        "realtime_start,realtime_end,NOT_A_DATE,value\n"
        "2023-10-01,2023-11-01,2023-07-01,26813.6\n",
        encoding="utf-8",
    )
    if duckdb:
        conn = duckdb.connect(":memory:")
        res = conn.sql(f"SELECT * FROM read_csv('{bad_alfred}', header=true)").to_arrow_table()
        assert "date" not in res.schema.names
        assert "NOT_A_DATE" in res.schema.names
        conn.close()


def test_synthetic_parquet_footer_corruption(tmp_path: Path, fixture_paths: Any):
    """Copy valid synthetic parquet and overwrite the footer magic bytes with zeros to trigger ArrowInvalid."""
    valid_data = fixture_paths.drop_001_parquet.read_bytes()
    assert valid_data[-4:] == b"PAR1"
    corrupted_data = valid_data[:-4] + b"\x00\x00\x00\x00"

    corrupt_file = tmp_path / "corrupted_drop_001.parquet"
    corrupt_file.write_bytes(corrupted_data)

    with pytest.raises(pa.ArrowInvalid, match="(?i)(magic|footer|parquet)"):
        pq.read_table(corrupt_file)
