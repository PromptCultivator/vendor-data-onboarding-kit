"""File discovery, format detection, and drop identification.

Inspects incoming drop paths, identifies file formats and compression, computes
cryptographic SHA-256 payload digests, and generates deterministic drop identifiers.
"""

from __future__ import annotations

import datetime
import gzip
import hashlib
import json
import re
import zlib
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

import duckdb


class FileFormat(str, Enum):
    """Recognized tabular data file formats."""

    CSV = "csv"
    JSONL = "jsonl"
    PARQUET = "parquet"


class Compression(str, Enum):
    """Recognized stream compression algorithms."""

    NONE = "none"
    GZIP = "gzip"


@dataclass(frozen=True)
class DiscoveredFile:
    """Metadata for an individual data file discovered within a drop."""

    path: Path
    relative_path: str
    format: FileFormat
    compression: Compression
    size_bytes: int
    content_hash: str


@dataclass(frozen=True)
class DropMetadata:
    """Aggregated manifest describing an entire ingested drop batch."""

    drop_id: str
    file_format: FileFormat
    compression: Compression
    files: list[DiscoveredFile]
    total_bytes: int
    discovered_at: str


class DiscoveryError(Exception):
    """Base exception for file discovery and inspection errors."""


class EmptyDropError(DiscoveryError):
    """Raised when drop path contains zero recognized data files."""


class HeterogeneousFormatError(DiscoveryError):
    """Raised when drop contains mixed file formats or compression modes."""


class CorruptedArchiveError(DiscoveryError):
    """Raised when an archive stream fails decompression or is truncated."""


class FormatMismatchError(DiscoveryError):
    """Raised when file content conflicts with declared format or extension."""


class FileAccessError(DiscoveryError):
    """Raised when file system permissions prevent reading a file."""


class UnrecognizedFormatError(DiscoveryError):
    """Raised when file format cannot be recognized."""


def compute_drop_id(file_hashes: list[str]) -> str:
    """Compute 16-character hexadecimal drop identifier from sorted file hashes."""
    sorted_hashes = sorted(file_hashes)
    concatenated = "".join(sorted_hashes).encode("utf-8")
    return hashlib.sha256(concatenated).hexdigest()[:16]


def detect_file_format(path: Path) -> tuple[FileFormat, Compression]:
    """Detect the data format and compression mode for a given file path.

    Uses magic byte inspection, text stream validation, and delimiter sniffing.
    """
    if not path.is_file():
        raise FileAccessError(f"Path is not a regular file: {path}")

    file_size = path.stat().st_size
    if file_size == 0:
        raise DiscoveryError(f"File {path} is empty (0 bytes).")

    # 1. Inspect first 2 bytes for gzip header
    try:
        with open(path, "rb") as f:
            magic2 = f.read(2)
    except OSError as exc:
        raise FileAccessError(f"Cannot read file header for {path}: {exc}") from exc

    is_gz_ext = path.name.endswith(".gz")
    is_gzip = magic2 == b"\x1f\x8b"

    if is_gz_ext and not is_gzip:
        raise CorruptedArchiveError(
            f"File {path} has .gz extension but lacks valid gzip magic header."
        )

    compression = Compression.GZIP if is_gzip else Compression.NONE

    # 2. Check Parquet header/footer when uncompressed or check binary null bytes
    if not is_gzip:
        if file_size >= 8:
            try:
                with open(path, "rb") as f:
                    head4 = f.read(4)
                    f.seek(-4, 2)
                    tail4 = f.read(4)
                if head4 == b"PAR1" and tail4 == b"PAR1":
                    return FileFormat.PARQUET, compression
            except OSError as exc:
                raise FileAccessError(f"Cannot read Parquet bounds in {path}: {exc}") from exc
        if path.suffix.lower() == ".parquet":
            raise FormatMismatchError(
                f"File {path} has .parquet extension but lacks PAR1 header/footer."
            )
        try:
            with open(path, "rb") as f:
                raw_sample = f.read(4096)
            if b"\x00" in raw_sample:
                raise UnrecognizedFormatError(f"File {path} contains binary null bytes.")
        except OSError as exc:
            raise FileAccessError(f"Cannot inspect bytes in {path}: {exc}") from exc
    else:
        try:
            with gzip.open(path, "rb") as gz_raw:
                decomp_sample = gz_raw.read(4096)
            if b"\x00" in decomp_sample:
                raise UnrecognizedFormatError(f"File {path} contains binary null bytes.")
        except (gzip.BadGzipFile, OSError, EOFError, zlib.error) as exc:
            raise CorruptedArchiveError(f"Cannot decompress gzip file {path}: {exc}") from exc

    # 3. Read leading lines from stream for text-based inspection
    MAX_LOOKAHEAD_LINES = 50
    DESIRED_NON_EMPTY_RECORDS = 5
    non_empty: list[str] = []

    if is_gzip:
        try:
            with gzip.open(path, "rt", encoding="utf-8", errors="replace") as gz:
                for _ in range(MAX_LOOKAHEAD_LINES):
                    line = gz.readline()
                    if not line:
                        break
                    stripped = line.strip()
                    if stripped:
                        non_empty.append(stripped)
                        if len(non_empty) >= DESIRED_NON_EMPTY_RECORDS:
                            break
        except (gzip.BadGzipFile, OSError, EOFError, zlib.error) as exc:
            raise CorruptedArchiveError(f"Cannot decompress gzip file {path}: {exc}") from exc
    else:
        try:
            with open(path, "rt", encoding="utf-8", errors="replace") as f:
                for _ in range(MAX_LOOKAHEAD_LINES):
                    line = f.readline()
                    if not line:
                        break
                    stripped = line.strip()
                    if stripped:
                        non_empty.append(stripped)
                        if len(non_empty) >= DESIRED_NON_EMPTY_RECORDS:
                            break
        except OSError as exc:
            raise FileAccessError(f"Cannot read text records from {path}: {exc}") from exc

    if not non_empty:
        raise DiscoveryError(f"File {path} contains no non-empty text records.")

    # 4. Check JSONL: must start with '{' and end with '}'
    first_record = non_empty[0]
    if first_record.startswith("{") and first_record.endswith("}"):
        try:
            parsed: Any = json.loads(first_record)
            if isinstance(parsed, dict):
                return FileFormat.JSONL, compression
        except json.JSONDecodeError:
            pass

    # 5. Check CSV via DuckDB sniffer
    con = duckdb.connect()
    try:
        res = con.execute("SELECT Delimiter, Columns FROM sniff_csv(?)", [str(path)]).fetchone()
        if res:
            delim = res[0]
            cols = res[1]
            if delim and delim in first_record:
                return FileFormat.CSV, compression
            if (cols and len(cols) == 1) or path.name.lower().endswith((".csv", ".csv.gz")):
                return FileFormat.CSV, compression
    except Exception:
        pass
    finally:
        con.close()

    # 6. Fallback delimiter inspection for comma, tab, or pipe
    for candidate_delim in (",", "\t", "|", ";"):
        if candidate_delim in first_record:
            return FileFormat.CSV, compression

    if path.name.lower().endswith((".csv", ".csv.gz")):
        return FileFormat.CSV, compression

    raise UnrecognizedFormatError(f"File {path} does not match supported data formats.")


def hash_file_payload(path: Path, compression: Compression) -> tuple[str, int]:
    """Compute SHA-256 digest of uncompressed data payload and return with on-disk byte size."""
    hasher = hashlib.sha256()
    size_bytes = path.stat().st_size

    if compression == Compression.GZIP:
        try:
            with gzip.open(path, "rb") as gz:
                while True:
                    chunk = gz.read(65536)
                    if not chunk:
                        break
                    hasher.update(chunk)
        except (gzip.BadGzipFile, OSError, EOFError, zlib.error) as exc:
            raise CorruptedArchiveError(
                f"Decompression failure during hashing of {path}: {exc}"
            ) from exc
    else:
        try:
            with open(path, "rb") as f:
                while True:
                    chunk = f.read(65536)
                    if not chunk:
                        break
                    hasher.update(chunk)
        except OSError as exc:
            raise FileAccessError(f"Cannot read bytes from {path}: {exc}") from exc

    return hasher.hexdigest(), size_bytes


def scan(path: Path | str) -> DropMetadata:
    """Scan input path (directory or file) and construct complete DropMetadata."""
    target = Path(path).resolve()
    if not target.exists():
        raise FileNotFoundError(f"Drop path does not exist: {target}")

    candidate_files: list[Path] = []
    if target.is_file():
        candidate_files.append(target)
        root_dir = target.parent
    else:
        root_dir = target
        for p in target.rglob("*"):
            if not p.is_file():
                continue
            # Skip hidden files and temp files
            if p.name.startswith(".") or p.name.endswith("~") or p.name.endswith(".tmp"):
                continue
            candidate_files.append(p)

    if not candidate_files:
        raise EmptyDropError(f"No data files found in drop path: {target}")

    # Sort files deterministically by relative path
    candidate_files.sort(key=lambda p: p.relative_to(root_dir).as_posix())

    discovered: list[DiscoveredFile] = []
    for f in candidate_files:
        fmt, comp = detect_file_format(f)
        digest, size_bytes = hash_file_payload(f, comp)
        discovered.append(
            DiscoveredFile(
                path=f,
                relative_path=f.relative_to(root_dir).as_posix(),
                format=fmt,
                compression=comp,
                size_bytes=size_bytes,
                content_hash=digest,
            )
        )

    # Validate homogeneous format and compression
    formats = {df.format for df in discovered}
    if len(formats) > 1:
        fmt_names = ", ".join(sorted(f.value for f in formats))
        raise HeterogeneousFormatError(f"Drop contains mixed data formats: {fmt_names} in {target}")

    compressions = {df.compression for df in discovered}
    if len(compressions) > 1:
        comp_names = ", ".join(sorted(c.value for c in compressions))
        raise HeterogeneousFormatError(
            f"Drop contains mixed compression modes: {comp_names} in {target}"
        )

    common_format = discovered[0].format
    common_compression = discovered[0].compression
    file_hashes = [df.content_hash for df in discovered]
    drop_id = compute_drop_id(file_hashes)
    total_bytes = sum(df.size_bytes for df in discovered)
    discovered_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

    return DropMetadata(
        drop_id=drop_id,
        file_format=common_format,
        compression=common_compression,
        files=discovered,
        total_bytes=total_bytes,
        discovered_at=discovered_at,
    )


def check_idempotency(drop_id: str, store_path: Path | str | None = None) -> bool:
    """Verify whether drop_id has already been committed to the store."""
    if store_path is None:
        return False

    base_path = Path(store_path)
    if not base_path.exists():
        return False

    db_path = base_path / "store.duckdb" if base_path.is_dir() else base_path
    if db_path.is_file() and db_path.suffix == ".duckdb":
        con = None
        try:
            con = duckdb.connect(str(db_path), read_only=True)
            tables = con.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            if "drops" in table_names:
                row = con.execute(
                    "SELECT 1 FROM drops WHERE drop_id = ? AND status = 'COMMITTED'",
                    [drop_id],
                ).fetchone()
                return row is not None
        except Exception:
            pass
        finally:
            if con is not None:
                try:
                    con.close()
                except Exception:
                    pass

    if base_path.is_dir():
        if re.fullmatch(r"^[0-9a-f]{16}$", drop_id):
            matches = list(base_path.glob(f"**/part-{drop_id}.parquet"))
            if matches:
                return True

    return False
