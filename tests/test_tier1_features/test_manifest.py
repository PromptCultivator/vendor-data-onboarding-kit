"""Tier 1 Feature Tests: Lineage & Manifest Tracker (SHA-256, DropManifest)."""

import hashlib
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from pathlib import Path

import pytest

from vendor_onboarding.ingestion.manifest import (
    DropManifest,
    compute_sha256,
    create_manifest,
    extract_row_count,
)


def test_drop_manifest_immutability(sample_csv_path: Path) -> None:
    """Ensure DropManifest is strictly frozen and immutable."""
    manifest = DropManifest(
        drop_id="DROP_001",
        vendor_id="SYNTH_VENDOR_A",
        dataset_id="EQUITY_DAILY_FUNDAMENTALS",
        file_path=str(sample_csv_path),
        file_sha256="abc123hash",
        file_size_bytes=1024,
        row_count=60,
        ingest_timestamp=datetime.now(UTC),
    )

    with pytest.raises(FrozenInstanceError):
        manifest.row_count = 100  # type: ignore[misc]

    with pytest.raises(FrozenInstanceError):
        manifest.file_sha256 = "new_hash"  # type: ignore[misc]


def test_compute_sha256_streaming(sample_csv_path: Path) -> None:
    """Test streaming SHA-256 calculation matches complete file hash."""
    stream_hash = compute_sha256(sample_csv_path, chunk_size=512)
    full_content = sample_csv_path.read_bytes()
    expected_hash = hashlib.sha256(full_content).hexdigest()

    assert stream_hash == expected_hash
    assert len(stream_hash) == 64


def test_create_manifest_auto_generation(sample_csv_path: Path) -> None:
    """Test create_manifest populates provenance metadata accurately."""
    manifest = create_manifest(
        file_path=sample_csv_path,
        vendor_id="SYNTH_VENDOR_A",
        dataset_id="EQUITY_DAILY_FUNDAMENTALS",
    )

    assert manifest.vendor_id == "SYNTH_VENDOR_A"
    assert manifest.dataset_id == "EQUITY_DAILY_FUNDAMENTALS"
    assert manifest.row_count == 60
    assert manifest.file_size_bytes == sample_csv_path.stat().st_size
    assert manifest.file_sha256 == compute_sha256(sample_csv_path)
    assert manifest.drop_id.startswith("DROP_SYNTH_VENDOR_A_EQUITY_DAILY_FUNDAMENTALS_")
    assert manifest.ingest_timestamp.tzinfo is not None


def test_create_manifest_custom_fields(sample_parquet_path: Path) -> None:
    """Test create_manifest with custom drop_id and provided row_count."""
    custom_drop_id = "CUSTOM_DROP_2023_Q4_001"
    manifest = create_manifest(
        file_path=sample_parquet_path,
        vendor_id="SYNTH_VENDOR_B",
        dataset_id="EQUITY_DAILY_PRICES",
        drop_id=custom_drop_id,
        row_count=60,
    )

    assert manifest.drop_id == custom_drop_id
    assert manifest.row_count == 60
    assert manifest.vendor_id == "SYNTH_VENDOR_B"


def test_manifest_dict_and_json_roundtrip(sample_csv_path: Path) -> None:
    """Test serialization and deserialization across dict and JSON representations."""
    original = create_manifest(
        file_path=sample_csv_path,
        vendor_id="VENDOR_X",
        dataset_id="DATASET_Y",
        drop_id="DROP_X_Y_1",
    )

    # Dict roundtrip
    d = original.to_dict()
    assert isinstance(d["ingest_timestamp"], str)
    from_d = DropManifest.from_dict(d)
    assert from_d == original

    # JSON roundtrip
    j = original.to_json()
    from_j = DropManifest.from_json(j)
    assert from_j == original


def test_manifest_save_and_load(sample_csv_path: Path, tmp_path: Path) -> None:
    """Test file persistence and retrieval of drop manifests."""
    manifest = create_manifest(
        file_path=sample_csv_path,
        vendor_id="VENDOR_TEST",
        dataset_id="DATASET_TEST",
    )
    dest_path = tmp_path / "drop_manifest.json"
    manifest.save(dest_path)

    assert dest_path.is_file()
    loaded = DropManifest.load(dest_path)
    assert loaded == manifest
    assert loaded.file_sha256 == manifest.file_sha256


def test_extract_row_count(sample_csv_path: Path, sample_parquet_path: Path) -> None:
    """Test row count extractor utility across formats."""
    assert extract_row_count(sample_csv_path) == 60
    assert extract_row_count(sample_parquet_path) == 60


def test_manifest_nonexistent_file_raises() -> None:
    """Ensure FileNotFoundError is raised when attempting to manifest non-existent file."""
    with pytest.raises(FileNotFoundError):
        compute_sha256("/nonexistent/file.csv")

    with pytest.raises(FileNotFoundError):
        create_manifest(
            file_path="/nonexistent/file.csv",
            vendor_id="V",
            dataset_id="D",
        )


def test_manifest_tamper_detection_byte_flip(sample_csv_path: Path, tmp_path: Path) -> None:
    """Verify modifying a single byte invalidates the SHA-256 digest."""
    tampered = tmp_path / "tampered.csv"
    data = bytearray(sample_csv_path.read_bytes())
    data[15] = data[15] ^ 0xFF
    tampered.write_bytes(data)

    original_sha = compute_sha256(sample_csv_path)
    tampered_sha = compute_sha256(tampered)
    assert original_sha != tampered_sha


def test_manifest_tamper_detection_row_insertion(sample_csv_path: Path, tmp_path: Path) -> None:
    """Verify appending a row changes both record count and SHA-256."""
    tampered = tmp_path / "appended.csv"
    content = sample_csv_path.read_text(encoding="utf-8")
    tampered.write_text(content + "EXTRA_ROW,ENT_EXTRA,EXT,TICKER,2023-10-06T16:00:00Z,2023-10-06T20:00:00Z,CLOSE,100,USD,V,D,False,1\n", encoding="utf-8")

    m_orig = create_manifest(sample_csv_path, "V", "D")
    m_tamp = create_manifest(tampered, "V", "D")

    assert m_tamp.row_count == m_orig.row_count + 1
    assert m_tamp.file_sha256 != m_orig.file_sha256


def test_manifest_tamper_detection_truncation(sample_csv_path: Path, tmp_path: Path) -> None:
    """Verify truncating file bytes changes byte size and digest."""
    truncated = tmp_path / "truncated.csv"
    truncated.write_bytes(sample_csv_path.read_bytes()[:-20])

    m_orig = create_manifest(sample_csv_path, "V", "D")
    m_trunc = create_manifest(truncated, "V", "D")

    assert m_trunc.file_size_bytes < m_orig.file_size_bytes
    assert m_trunc.file_sha256 != m_orig.file_sha256


def test_manifest_deterministic_sorting_keys(sample_csv_path: Path) -> None:
    """Verify JSON output produces sorted keys deterministically."""
    import json
    m = create_manifest(sample_csv_path, "V", "D")
    j1 = m.to_json()
    j2 = m.to_json()
    assert j1 == j2
    parsed = json.loads(j1)
    expected_keys = {"drop_id", "vendor_id", "dataset_id", "file_path", "file_sha256", "file_size_bytes", "row_count", "ingest_timestamp"}
    assert set(parsed.keys()) == expected_keys

