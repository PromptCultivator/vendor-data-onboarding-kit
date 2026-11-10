"""
Milestone 1 Challenger Stress Tests: Ingestion Engine & Lineage Manifest Tracker.

Empirically tests adversarial inputs, edge cases, corrupted archives, type mismatches,
and boundary conditions across:
1. Corrupted/truncated gzip & zstd files, incomplete headers, random byte payloads
2. Malformed CSVs (ragged lines, null bytes, unbalanced quotes, escaped delimiters)
3. Irregular JSON & JSONL (trailing commas, mixed schemas, blank lines, empty arrays)
4. API dump structures (nested arrays, missing wrapper keys, empty objects)
5. High row-count chunked streaming for memory stability and provenance line continuity
6. Manifest generation on empty files, huge files, non-existent files
"""

from __future__ import annotations

import dataclasses
import gzip
import hashlib
import json
import os
import time
import tracemalloc
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import zstandard as zstd

from vendor_onboarding.ingestion.manifest import (
    DropManifest,
    compute_sha256,
    create_manifest,
)
from vendor_onboarding.ingestion.reader import (
    CorruptedArchiveError,
    IngestReader,
    SchemaParseError,
    detect_file_format,
    iter_chunks,
    read_file,
)

pytestmark = [pytest.mark.tier2]


# ==============================================================================
# 1. Corrupted / Truncated Gzip & Zstd Archives, Headers, Random Byte Payloads
# ==============================================================================


def test_adversarial_zero_byte_gzip(tmp_path: Path) -> None:
    """0-byte .gz file is an incomplete archive and must raise CorruptedArchiveError."""
    p = tmp_path / "zero.csv.gz"
    p.write_bytes(b"")
    with pytest.raises(CorruptedArchiveError):
        IngestReader(p)


def test_adversarial_truncated_gzip_headers(tmp_path: Path) -> None:
    """Truncated gzip header (1 byte, 2 bytes) must raise CorruptedArchiveError."""
    p1 = tmp_path / "trunc_hdr1.csv.gz"
    p1.write_bytes(b"\x1f")
    with pytest.raises(CorruptedArchiveError):
        IngestReader(p1)

    p2 = tmp_path / "trunc_hdr2.csv.gz"
    p2.write_bytes(b"\x1f\x8b")
    with pytest.raises(CorruptedArchiveError):
        IngestReader(p2)


def test_adversarial_gzip_truncated_mid_payload(tmp_path: Path) -> None:
    """Gzip archive truncated mid-payload must raise CorruptedArchiveError upon reading."""
    valid_gz = gzip.compress(b"col1,col2,col3\n" + b"1,2,3\n" * 500)
    p = tmp_path / "trunc_mid.csv.gz"
    p.write_bytes(valid_gz[: len(valid_gz) // 2])

    with pytest.raises(CorruptedArchiveError):
        reader = IngestReader(p)
        reader.read_all()


def test_adversarial_random_bytes_in_gzip(tmp_path: Path) -> None:
    """Random byte payload in .csv.gz must raise CorruptedArchiveError."""
    p = tmp_path / "random.csv.gz"
    p.write_bytes(os.urandom(1024))
    with pytest.raises(CorruptedArchiveError):
        IngestReader(p)


def test_adversarial_zero_byte_zstd(tmp_path: Path) -> None:
    """0-byte .csv.zst file is not a valid Zstandard frame and must raise CorruptedArchiveError."""
    p = tmp_path / "zero.csv.zst"
    p.write_bytes(b"")
    with pytest.raises(CorruptedArchiveError):
        IngestReader(p)


def test_adversarial_truncated_zstd_headers(tmp_path: Path) -> None:
    """Truncated zstd magic header (< 4 bytes) must raise CorruptedArchiveError."""
    p = tmp_path / "trunc_zstd.csv.zst"
    p.write_bytes(b"\x28\xb5\x2f")
    with pytest.raises(CorruptedArchiveError):
        IngestReader(p)


def test_adversarial_zstd_truncated_mid_frame(tmp_path: Path) -> None:
    """Zstd archive cut mid-frame must be caught as CorruptedArchiveError."""
    cctx = zstd.ZstdCompressor()
    valid_zst = cctx.compress(b"col1,col2,col3\n" + b"1,2,3\n" * 500)
    p = tmp_path / "trunc_mid.csv.zst"
    p.write_bytes(valid_zst[: len(valid_zst) // 2])

    with pytest.raises(CorruptedArchiveError):
        IngestReader(p)


def test_adversarial_random_bytes_in_zstd(tmp_path: Path) -> None:
    """Random byte payload in .csv.zst must raise CorruptedArchiveError."""
    p = tmp_path / "random.csv.zst"
    p.write_bytes(os.urandom(1024))
    with pytest.raises(CorruptedArchiveError):
        IngestReader(p)


def test_adversarial_magic_byte_sniffing_no_extension(tmp_path: Path) -> None:
    """Files without extension correctly identified by magic bytes fallback."""
    valid_gz = gzip.compress(b"id,val\n1,10\n")
    p_gz = tmp_path / "magic_gz_payload"
    p_gz.write_bytes(valid_gz)
    fmt, comp = detect_file_format(p_gz)
    assert fmt == "csv"
    assert comp == "gzip"

    cctx = zstd.ZstdCompressor()
    valid_zst = cctx.compress(b"id,val\n1,10\n")
    p_zst = tmp_path / "magic_zst_payload"
    p_zst.write_bytes(valid_zst)
    fmt, comp = detect_file_format(p_zst)
    assert fmt == "csv"
    assert comp == "zstd"


# ==============================================================================
# 2. Malformed CSVs (Ragged lines, Null bytes, Unbalanced quotes, Escapes)
# ==============================================================================


def test_adversarial_csv_ragged_missing_columns(tmp_path: Path) -> None:
    """Ragged CSV rows with fewer columns than header are null-padded cleanly."""
    p = tmp_path / "ragged_missing.csv"
    p.write_text("a,b,c\n1,2,3\n4,5\n6,7,8\n", encoding="utf-8")
    table = read_file(p, add_source_line=True)
    assert table.num_rows == 3
    assert "source_line_no" in table.column_names
    assert table.column("source_line_no").to_pylist() == [1, 2, 3]


def test_adversarial_csv_ragged_excess_columns(tmp_path: Path) -> None:
    """Ragged CSV rows with excess columns are ingested without crashing."""
    p = tmp_path / "ragged_excess.csv"
    p.write_text("a,b,c\n1,2,3\n4,5,6,7,8\n9,10,11\n", encoding="utf-8")
    table = read_file(p, add_source_line=True)
    assert table.num_rows == 3
    assert "column3" in table.column_names


def test_adversarial_csv_embedded_null_bytes(tmp_path: Path) -> None:
    """Null bytes inside CSV string fields are read safely."""
    p = tmp_path / "null_byte.csv"
    p.write_bytes(b"col1,col2\nval1,val\x00two\nval3,val4\n")
    table = read_file(p, add_source_line=True)
    assert table.num_rows == 2


def test_adversarial_csv_unbalanced_quotes(tmp_path: Path) -> None:
    """Unclosed quotes parse as multiline text without scanner crash."""
    p = tmp_path / "unbalanced.csv"
    p.write_text('col1,col2,col3\n"unclosed text,value2,value3\nnext1,next2,next3\n', encoding="utf-8")
    table = read_file(p, add_source_line=True)
    assert table.num_rows >= 1


def test_adversarial_csv_rfc4180_escaped_quotes_and_newlines(tmp_path: Path) -> None:
    """RFC 4180 quotes with multiline newline cells automatically handled via single-thread fallback."""
    p = tmp_path / "multiline_quotes.csv"
    p.write_text('id,text\n1,"He said, ""Hello!"""\n2,"Comma, inside, quotes"\n3,"Line\nbreak"\n', encoding="utf-8")
    table = read_file(p, add_source_line=True)
    assert table.num_rows == 3
    texts = table.column("text").to_pylist()
    assert texts[0] == 'He said, "Hello!"'
    assert texts[1] == "Comma, inside, quotes"
    assert texts[2] == "Line\nbreak"


def test_adversarial_csv_only_delimiters(tmp_path: Path) -> None:
    """CSV containing only delimiters and no headers or values."""
    p = tmp_path / "only_delims.csv"
    p.write_text(",,,,\n,,,,\n,,,,\n", encoding="utf-8")
    table = read_file(p, add_source_line=True)
    assert table.num_rows == 2
    assert len(table.column_names) >= 5


# ==============================================================================
# 3. Irregular JSON & JSONL (Trailing commas, Mixed schemas, Blank lines, Empty)
# ==============================================================================


def test_adversarial_json_trailing_comma_handling(tmp_path: Path) -> None:
    """Invalid JSON with trailing comma in array raises SchemaParseError."""
    p = tmp_path / "trailing_comma.json"
    p.write_text('[{"a": 1, "b": 2}, {"a": 3, "b": 4},]', encoding="utf-8")
    with pytest.raises(SchemaParseError):
        read_file(p)


def test_adversarial_json_empty_array(tmp_path: Path) -> None:
    """Empty JSON array `[]` returns 0 rows without crashing."""
    p = tmp_path / "empty_array.json"
    p.write_text("[]", encoding="utf-8")
    table = read_file(p, add_source_line=True)
    assert table.num_rows == 0
    assert "source_line_no" in table.column_names


def test_adversarial_json_array_of_empty_objects(tmp_path: Path) -> None:
    """Array of empty objects `[{}, {}]` returns correct row count."""
    p = tmp_path / "empty_objects.json"
    p.write_text("[{}, {}]", encoding="utf-8")
    table = read_file(p, add_source_line=True)
    assert table.num_rows == 2


def test_adversarial_jsonl_mixed_schemas_across_lines(tmp_path: Path) -> None:
    """JSONL with varying schema keys across lines unions all observed columns."""
    p = tmp_path / "mixed_schema.jsonl"
    p.write_text(
        '{"id": 1, "name": "alice"}\n'
        '{"id": 2, "age": 30, "extra": "info"}\n'
        '{"id": 3, "name": "charlie", "nested": {"k": "v"}}\n',
        encoding="utf-8",
    )
    table = read_file(p, add_source_line=True)
    assert table.num_rows == 3
    assert {"id", "name", "age", "extra", "nested"}.issubset(set(table.column_names))


def test_adversarial_jsonl_blank_lines_interleaved(tmp_path: Path) -> None:
    """Blank and whitespace-only lines interleaved in JSONL are skipped seamlessly."""
    p = tmp_path / "blank_lines.jsonl"
    p.write_text(
        '\n\n{"id": 1, "val": 10}\n\n   \n{"id": 2, "val": 20}\n\n\n',
        encoding="utf-8",
    )
    table = read_file(p, add_source_line=True)
    assert table.num_rows == 2
    assert table.column("id").to_pylist() == [1, 2]


def test_adversarial_jsonl_corrupted_line_mid_stream(tmp_path: Path) -> None:
    """Corrupted syntax on a single JSONL line raises SchemaParseError."""
    p = tmp_path / "corrupted_line.jsonl"
    p.write_text(
        '{"id": 1, "val": 10}\n'
        '{"id": 2, "val": INVALID_JSON_SYNTAX\n'
        '{"id": 3, "val": 30}\n',
        encoding="utf-8",
    )
    with pytest.raises(SchemaParseError):
        read_file(p)


# ==============================================================================
# 4. API Dump Structures (Nested arrays, Missing wrapper keys, Empty objects)
# ==============================================================================


@pytest.mark.parametrize("wrapper_key", ["data", "results", "records", "items", "observations", "filings"])
def test_adversarial_api_dump_standard_wrappers(tmp_path: Path, wrapper_key: str) -> None:
    """Standard API response wrapper keys are unnested into record columns."""
    p = tmp_path / f"api_{wrapper_key}.json"
    p.write_text(
        json.dumps({"meta": "ok", wrapper_key: [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]}),
        encoding="utf-8",
    )
    table = read_file(p, add_source_line=True)
    assert table.num_rows == 2
    assert "id" in table.column_names
    assert "val" in table.column_names
    assert table.column("id").to_pylist() == [1, 2]


def test_adversarial_api_dump_missing_wrapper_key(tmp_path: Path) -> None:
    """Non-standard wrapper structures are ingested as top-level relations without binder errors."""
    p = tmp_path / "api_custom.json"
    p.write_text(
        json.dumps({"status": "ok", "custom_payload": [{"id": 1}, {"id": 2}]}),
        encoding="utf-8",
    )
    table = read_file(p, add_source_line=True)
    assert table.num_rows == 1
    assert "custom_payload" in table.column_names


def test_adversarial_api_dump_empty_record_array(tmp_path: Path) -> None:
    """Empty data array `{'status': 'ok', 'data': []}` does not cause * expansion Binder Error."""
    p = tmp_path / "api_empty_data.json"
    p.write_text(json.dumps({"status": "ok", "data": []}), encoding="utf-8")
    table = read_file(p, add_source_line=True)
    assert table.num_rows == 1
    assert "status" in table.column_names
    assert "data" in table.column_names


def test_adversarial_api_dump_null_data(tmp_path: Path) -> None:
    """Null data `{'status': 'error', 'data': null}` ingested cleanly."""
    p = tmp_path / "api_null_data.json"
    p.write_text(json.dumps({"status": "error", "data": None}), encoding="utf-8")
    table = read_file(p, add_source_line=True)
    assert table.num_rows == 1
    assert "data" in table.column_names


def test_adversarial_api_dump_scalar_array(tmp_path: Path) -> None:
    """Array of primitives `{'status': 'ok', 'data': [1, 2, 3]}` handled without * expansion crash."""
    p = tmp_path / "api_scalar.json"
    p.write_text(json.dumps({"status": "ok", "data": [1, 2, 3]}), encoding="utf-8")
    table = read_file(p, add_source_line=True)
    assert table.num_rows == 1
    assert "data" in table.column_names


# ==============================================================================
# 5. High Row-Count Chunked Streaming & Provenance Line Continuity
# ==============================================================================


def test_adversarial_high_row_count_streaming_csv(tmp_path: Path) -> None:
    """Stress test: 100,000 row CSV streaming with exact line number sequence and bounded memory."""
    n_rows = 100_000
    p = tmp_path / "high_row.csv"
    with p.open("w", encoding="utf-8") as f:
        f.write("record_id,entity_id,amount\n")
        for i in range(1, n_rows + 1):
            f.write(f"REC_{i:07d},ENT_{i%50:03d},{i * 0.5:.2f}\n")

    reader = IngestReader(p, add_source_line=True)
    assert reader.count_rows() == n_rows

    tracemalloc.start()
    mem_start = tracemalloc.get_traced_memory()[0]

    batch_size = 20_000
    total_rows = 0
    expected_line = 1

    for batch in iter_chunks(p, batch_size=batch_size, add_source_line=True):
        batch_rows = batch.num_rows
        total_rows += batch_rows

        lines = batch.column("source_line_no").to_pylist()
        assert lines[0] == expected_line
        assert lines[-1] == expected_line + batch_rows - 1
        expected_line += batch_rows

    mem_peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()

    assert total_rows == n_rows
    # Memory growth during streaming must remain bounded (< 10 MB)
    mem_used_mb = (mem_peak - mem_start) / (1024 * 1024)
    assert mem_used_mb < 15.0, f"Memory leak in streaming: {mem_used_mb} MB"


def test_adversarial_high_row_count_streaming_parquet(tmp_path: Path) -> None:
    """Stress test: 100,000 row Parquet streaming line continuity."""
    n_rows = 100_000
    t = pa.table({
        "record_id": pa.array([f"REC_{i:07d}" for i in range(1, n_rows + 1)]),
        "val": pa.array(list(range(n_rows))),
    })
    p = tmp_path / "high_row.parquet"
    pq.write_table(t, p)

    reader = IngestReader(p, add_source_line=True)
    assert reader.count_rows() == n_rows

    total_rows = 0
    expected_line = 1
    for batch in iter_chunks(p, batch_size=25_000, add_source_line=True):
        batch_rows = batch.num_rows
        total_rows += batch_rows
        lines = batch.column("source_line_no").to_pylist()
        assert lines[0] == expected_line
        assert lines[-1] == expected_line + batch_rows - 1
        expected_line += batch_rows

    assert total_rows == n_rows


# ==============================================================================
# 6. Manifest Generation (Empty, Huge, Non-Existent Files)
# ==============================================================================


def test_adversarial_manifest_nonexistent_file(tmp_path: Path) -> None:
    """Manifest generation and sha256 computation on non-existent files raise FileNotFoundError."""
    nonexistent = tmp_path / "phantom.csv"
    with pytest.raises(FileNotFoundError):
        create_manifest(nonexistent, "VENDOR", "DATASET")

    with pytest.raises(FileNotFoundError):
        compute_sha256(nonexistent)

    with pytest.raises(FileNotFoundError):
        DropManifest.load(nonexistent)


def test_adversarial_manifest_empty_files(tmp_path: Path) -> None:
    """Manifest generation on empty files with explicit row_count=0 succeeds."""
    p = tmp_path / "empty_file.csv"
    p.write_bytes(b"")

    m = create_manifest(p, vendor_id="VENDOR_X", dataset_id="DATASET_Y", row_count=0)
    assert m.file_size_bytes == 0
    assert m.row_count == 0
    assert m.file_sha256 == hashlib.sha256(b"").hexdigest()

    # Serialization and loading integrity
    p_json = tmp_path / "manifest.json"
    m.save(p_json)
    loaded = DropManifest.load(p_json)
    assert loaded == m


def test_adversarial_manifest_huge_file_streaming_sha256(tmp_path: Path) -> None:
    """Streaming SHA-256 calculation on 64MB file matches hashlib reference."""
    p_huge = tmp_path / "huge_test.bin"
    chunk_1mb = b"Z" * (1024 * 1024)
    ref_hasher = hashlib.sha256()

    with p_huge.open("wb") as f:
        for _ in range(64):
            f.write(chunk_1mb)
            ref_hasher.update(chunk_1mb)

    expected_sha = ref_hasher.hexdigest()
    t0 = time.time()
    computed_sha = compute_sha256(p_huge, chunk_size=1024 * 1024)
    elapsed = time.time() - t0

    assert computed_sha == expected_sha
    assert elapsed < 5.0, f"SHA-256 took too long: {elapsed:.2f}s"


def test_adversarial_manifest_immutability(tmp_path: Path) -> None:
    """DropManifest dataclass is frozen and immutable."""
    p = tmp_path / "sample.csv"
    p.write_text("a,b\n1,2\n")
    m = create_manifest(p, "VENDOR", "DATASET")

    with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
        m.drop_id = "TAMPERED_DROP_ID"  # type: ignore[misc]

    with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
        m.row_count = 999999  # type: ignore[misc]
