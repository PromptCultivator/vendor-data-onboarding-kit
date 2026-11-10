"""Lineage and manifest tracking for vendor drops."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class DropManifest:
    """Immutable provenance manifest for an ingested vendor drop."""

    drop_id: str
    vendor_id: str
    dataset_id: str
    file_path: str
    file_sha256: str
    file_size_bytes: int
    row_count: int
    ingest_timestamp: datetime

    def to_dict(self) -> dict[str, Any]:
        """Convert manifest to JSON-serializable dictionary."""
        d = asdict(self)
        d["ingest_timestamp"] = self.ingest_timestamp.isoformat()
        return d

    def to_json(self, indent: int = 2, sort_keys: bool = True) -> str:
        """Serialize manifest to JSON string."""
        return json.dumps(self.to_dict(), indent=indent, sort_keys=sort_keys)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DropManifest:
        """Reconstruct DropManifest from dictionary."""
        d = dict(data)
        ts_val = d["ingest_timestamp"]
        if isinstance(ts_val, str):
            # Parse ISO 8601 string
            if ts_val.endswith("Z"):
                ts_val = ts_val[:-1] + "+00:00"
            d["ingest_timestamp"] = datetime.fromisoformat(ts_val)
        return cls(**d)

    @classmethod
    def from_json(cls, json_str: str) -> DropManifest:
        """Reconstruct DropManifest from JSON string."""
        data = json.loads(json_str)
        return cls.from_dict(data)

    def save(self, output_path: str | Path) -> None:
        """Persist manifest to specified file path."""
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.to_json(), encoding="utf-8")

    @classmethod
    def load(cls, manifest_path: str | Path) -> DropManifest:
        """Load manifest from JSON file."""
        path = Path(manifest_path)
        if not path.is_file():
            raise FileNotFoundError(f"Manifest file not found: {path}")
        content = path.read_text(encoding="utf-8")
        return cls.from_json(content)


def compute_sha256(file_path: str | Path, chunk_size: int = 1024 * 1024) -> str:
    """Compute SHA-256 hash of a file streaming in 1MB chunks."""
    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"File not found: {path}")

    hasher = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(chunk_size):
            hasher.update(chunk)
    return hasher.hexdigest()


def extract_row_count(file_path: str | Path) -> int:
    """Extract exact row count of any supported vendor file using DuckDB reader."""
    from vendor_onboarding.ingestion.reader import IngestReader

    reader = IngestReader(file_path, add_source_line=False)
    return reader.count_rows()


def create_manifest(
    file_path: str | Path,
    vendor_id: str,
    dataset_id: str,
    drop_id: str | None = None,
    row_count: int | None = None,
) -> DropManifest:
    """Create a DropManifest tracking SHA-256, byte count, row count, and provenance timestamp."""
    path = Path(file_path).resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Source file not found: {path}")

    sha256_hash = compute_sha256(path)
    file_size = path.stat().st_size
    now = datetime.now(UTC)

    if row_count is None:
        count = extract_row_count(path)
    else:
        count = row_count

    if drop_id is None:
        ts_slug = now.strftime("%Y%m%d_%H%M%S")
        drop_id = f"DROP_{vendor_id}_{dataset_id}_{ts_slug}_{sha256_hash[:8]}"

    return DropManifest(
        drop_id=drop_id,
        vendor_id=vendor_id,
        dataset_id=dataset_id,
        file_path=str(path),
        file_sha256=sha256_hash,
        file_size_bytes=file_size,
        row_count=count,
        ingest_timestamp=now,
    )
