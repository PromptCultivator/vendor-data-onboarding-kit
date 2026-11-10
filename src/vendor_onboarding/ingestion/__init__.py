"""Ingestion, multi-format reading, lineage manifests, and schema inference."""

from vendor_onboarding.ingestion.infer import infer_schema
from vendor_onboarding.ingestion.manifest import (
    DropManifest,
    compute_sha256,
    create_manifest,
    extract_row_count,
)
from vendor_onboarding.ingestion.reader import (
    CorruptedArchiveError,
    IngestionError,
    IngestReader,
    SchemaParseError,
    UnsupportedFormatError,
    detect_file_format,
    iter_chunks,
    read_file,
)

__all__ = [
    "CorruptedArchiveError",
    "DropManifest",
    "IngestReader",
    "IngestionError",
    "SchemaParseError",
    "UnsupportedFormatError",
    "compute_sha256",
    "create_manifest",
    "detect_file_format",
    "extract_row_count",
    "infer_schema",
    "iter_chunks",
    "read_file",
]
