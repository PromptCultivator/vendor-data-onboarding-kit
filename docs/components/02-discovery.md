# Component: Drop Discovery & Fingerprinting (`vdok.discover`)

## 1. Overview & Responsibility

The `vdok.discover` module inspects incoming drop paths (directories or individual files), identifies serialized file formats, validates compression schemes, computes cryptographic hash fingerprints, and derives unique drop identifiers (`drop_id`).

This component provides the foundation for **Guarantee 3 (G3)**: drop ingestion idempotency. By calculating the `drop_id` strictly from file payload contents, re-submitting an identical batch of files results in an immediate no-op detection.

---

## 2. Drop Scanning & Format Inspection

### 2.1 Supported Formats and Compression
- **File Formats**: CSV (`.csv`), JSON Lines (`.jsonl`), Apache Parquet (`.parquet`).
- **Compression**: Uncompressed or Gzip (`.gz`).
- **Single-Format Invariant**: A drop must contain homogeneous file formats. If a directory contains both `.csv` and `.parquet` files, `discover.scan()` halts execution with a configuration error.

### 2.2 Discovery Data Contract

```python
from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class FileFormat(str, Enum):
    CSV = "csv"
    JSONL = "jsonl"
    PARQUET = "parquet"


class Compression(str, Enum):
    NONE = "none"
    GZIP = "gzip"


@dataclass(frozen=True)
class DiscoveredFile:
    path: Path
    relative_path: str
    format: FileFormat
    compression: Compression
    size_bytes: int
    content_hash: str  # SHA-256 hex digest


@dataclass(frozen=True)
class DropMetadata:
    drop_id: str  # 16-character hexadecimal identifier
    file_format: FileFormat
    compression: Compression
    files: list[DiscoveredFile]
    total_bytes: int
    discovered_at: str  # UTC ISO-8601 timestamp
```

---

## 3. Cryptographic Drop Identity Algorithm

The `drop_id` represents a deterministic digest of the data payload. It is computed as follows:

1. For each file in the drop, calculate the SHA-256 digest of the uncompressed data bytes:
   `H_i = SHA256(payload_i)`
2. Sort all file hashes lexicographically:
   `H_sorted = sort([H_1, H_2, ..., H_n])`
3. Concatenate the sorted hashes and compute the parent digest:
   `H_drop = SHA256(H_sorted_1 + H_sorted_2 + ... + H_sorted_n)`
4. Truncate the resulting hexadecimal string to 16 characters:
   `drop_id = H_drop[0:16]`

```python
import hashlib
from pathlib import Path


def compute_drop_id(file_hashes: list[str]) -> str:
    sorted_hashes = sorted(file_hashes)
    concatenated = "".join(sorted_hashes).encode("utf-8")
    full_digest = hashlib.sha256(concatenated).hexdigest()
    return full_digest[:16]
```

---

## 4. Idempotency Check (Guarantee 3)

Prior to executing ingestion, the engine checks the store metadata table `drops`:

```sql
SELECT drop_id, ingested_at, row_count, status
FROM drops
WHERE drop_id = ?
```

- If a record with the matching `drop_id` exists with status `COMMITTED`, the ingestion pipeline terminates execution immediately.
- The existing execution receipt is returned to the caller.
- No files are written to the partitioned store, and no revision rows are generated.

---

## 5. Error Conditions

| Condition | Cause | Handler Response |
|---|---|---|
| Empty Drop Path | Directory contains zero data files | Raises `EmptyDropError` |
| Format Collision | Directory mixes `.csv` and `.jsonl` | Raises `HeterogeneousFormatError` |
| Truncated Gzip File | EOF encountered prematurely | Raises `CorruptedArchiveError` |
| Unreadable File | Operating system permissions error | Raises `FileAccessError` |
