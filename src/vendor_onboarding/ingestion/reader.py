"""Multi-format columnar ingestion engine with transparent decompression and provenance tracking."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa


class IngestionError(Exception):
    """Base exception for ingestion pipeline errors."""


class CorruptedArchiveError(IngestionError):
    """Raised when a compressed archive or file container is corrupted or truncated."""


class UnsupportedFormatError(IngestionError):
    """Raised when encountering an unsupported file format or extension."""


class SchemaParseError(IngestionError):
    """Raised when file contents cannot be parsed into a coherent schema or tabular structure."""


KNOWN_NULL_STRINGS = ["", "NA", "N/A", "null", "NULL", "None", "-", "#N/A", "NaN", "nan"]


def detect_file_format(file_path: str | Path) -> tuple[str, str | None]:
    """Detect file format and compression encoding from file name and magic bytes.

    Returns:
        tuple of (format, compression), e.g. ('csv', 'gzip'), ('parquet', None)
    """
    path = Path(file_path)
    name_lower = path.name.lower()

    # Determine compression
    compression: str | None = None
    stripped_name = name_lower
    if name_lower.endswith(".gz"):
        compression = "gzip"
        stripped_name = name_lower[:-3]
    elif name_lower.endswith((".zst", ".zstd")):
        compression = "zstd"
        stripped_name = (
            name_lower[:-4] if name_lower.endswith(".zst") else name_lower[:-5]
        )

    # Determine format
    if stripped_name.endswith((".parquet", ".pq")):
        return "parquet", compression
    if stripped_name.endswith(".csv"):
        return "csv", compression
    if stripped_name.endswith(".tsv"):
        return "tsv", compression
    if stripped_name.endswith(".txt"):
        return "csv", compression
    if stripped_name.endswith((".jsonl", ".ndjson")):
        return "jsonl", compression
    if stripped_name.endswith(".json"):
        return "json", compression

    # Fallback to inspecting magic bytes if file exists
    if path.is_file() and path.stat().st_size >= 4:
        with path.open("rb") as f:
            header = f.read(4)
        if header.startswith(b"PAR1"):
            return "parquet", None
        if header.startswith(b"\x1f\x8b"):
            return "csv", "gzip"
        if header.startswith(b"\x28\xb5\x2f\xfd"):
            return "csv", "zstd"
        if header.startswith((b"{", b"[")):
            return "json", None

    raise UnsupportedFormatError(
        f"Unable to determine format for file: '{path.name}'. "
        "Supported formats: CSV, TSV, JSON, JSONL/NDJSON, Parquet (with optional .gz, .zst)."
    )


class IngestReader:
    """High-throughput multi-format reader backed by DuckDB 2.0 and PyArrow.

    Supports CSV, TSV, JSON, JSONL/NDJSON, Parquet, and nested API dumps with transparent
    decompression (.gz, .zst) and 1-based provenance line tracking (`source_line_no`).
    """

    def __init__(
        self,
        file_path: str | Path,
        options: dict[str, Any] | None = None,
        add_source_line: bool = True,
    ) -> None:
        self.file_path = Path(file_path).resolve()
        if not self.file_path.is_file():
            raise IngestionError(f"Target file does not exist: {self.file_path}")

        self.options = options or {}
        self.add_source_line = add_source_line
        self.file_format, self.compression = detect_file_format(self.file_path)

        # Pre-validate compression headers if file is compressed
        if self.compression == "gzip":
            if self.file_path.stat().st_size < 2:
                raise CorruptedArchiveError(
                    f"Truncated or empty GZIP archive ({self.file_path.stat().st_size} bytes): {self.file_path}"
                )
            with self.file_path.open("rb") as f:
                hdr = f.read(2)
            if not hdr.startswith(b"\x1f\x8b"):
                raise CorruptedArchiveError(f"Invalid GZIP magic bytes in archive: {self.file_path}")
        elif self.compression == "zstd":
            if self.file_path.stat().st_size < 4:
                raise CorruptedArchiveError(
                    f"Truncated or empty Zstandard archive ({self.file_path.stat().st_size} bytes): {self.file_path}"
                )
            with self.file_path.open("rb") as f:
                hdr = f.read(4)
            if not (hdr == b"\x28\xb5\x2f\xfd" or (hdr[:3] == b"\x18\x4d\x2a" and 0x50 <= hdr[3] <= 0x5F)):
                raise CorruptedArchiveError(f"Invalid Zstandard magic bytes in archive: {self.file_path}")
            # Verify stream integrity and completion to guard against truncated frames
            try:
                import zstandard as zstd

                dctx = zstd.ZstdDecompressor()
                dobj = dctx.decompressobj()
                with self.file_path.open("rb") as f:
                    while chunk := f.read(65536):
                        dobj.decompress(chunk)
                if not dobj.eof:
                    raise CorruptedArchiveError(
                        f"Truncated Zstandard stream (incomplete frame) in archive: {self.file_path}"
                    )
            except CorruptedArchiveError:
                raise
            except Exception as e:
                raise CorruptedArchiveError(
                    f"Corrupted Zstandard stream in {self.file_path}: {e}"
                ) from e

        # Isolated DuckDB in-memory connection
        self.con = duckdb.connect(":memory:")
        self._base_relation: duckdb.DuckDBPyRelation | None = None
        self._init_relation()

    def _wrap_duckdb_exception(self, e: Exception) -> IngestionError:
        """Map DuckDB / Arrow exceptions to specialized IngestionError subtypes."""
        if isinstance(e, IngestionError):
            return e
        err_msg = str(e)
        err_lower = err_msg.lower()
        is_archive_error = (
            self.compression in ("gzip", "zstd")
            or "gzip" in err_lower
            or "zstd" in err_lower
            or "magic bytes" in err_lower
            or "frame descriptor" in err_lower
            or "unexpected end" in err_lower
            or "stream" in err_lower
        ) and any(
            k in err_lower
            for k in (
                "gzip",
                "zstd",
                "magic bytes",
                "frame descriptor",
                "stream",
                "corrupt",
                "header",
                "crc",
                "eof",
                "unexpected end",
            )
        )
        if is_archive_error:
            return CorruptedArchiveError(f"Corrupted or invalid archive file: {err_msg}")
        if "malformed json" in err_lower or "malformed" in err_lower:
            return SchemaParseError(f"Failed to parse JSON file: {err_msg}")
        return IngestionError(f"Error processing {self.file_path}: {err_msg}")

    def _init_relation(self) -> None:
        """Initialize the base DuckDB relation according to file format and options."""
        escaped_path = str(self.file_path).replace("'", "''")

        try:
            if self.file_format in ("csv", "tsv"):
                delim = self.options.get("delimiter", "\t" if self.file_format == "tsv" else None)
                delim_sql = f", delim='{delim}'" if delim else ""
                null_padding = self.options.get("null_padding", True)
                null_padding_sql = ", null_padding=true" if null_padding else ""
                nullstr_list = self.options.get("nullstr", KNOWN_NULL_STRINGS)
                nullstr_sql = f", nullstr={nullstr_list}"
                parallel = self.options.get("parallel", None)
                parallel_sql = f", parallel={str(parallel).lower()}" if parallel is not None else ""

                query = (
                    f"SELECT * FROM read_csv('{escaped_path}'"
                    f"{delim_sql}{null_padding_sql}{nullstr_sql}{parallel_sql})"
                )
                try:
                    self._base_relation = self.con.sql(query)
                    # Trigger a preview fetch to verify parallel scanner compatibility
                    self._base_relation.limit(1).to_arrow_table()
                except Exception as e:
                    err_s = str(e)
                    if "parallel scanner does not support null_padding" in err_s or "quoted new lines" in err_s:
                        query = (
                            f"SELECT * FROM read_csv('{escaped_path}'"
                            f"{delim_sql}{null_padding_sql}{nullstr_sql}, parallel=false)"
                        )
                        self._base_relation = self.con.sql(query)
                    else:
                        raise

            elif self.file_format == "parquet":
                query = f"SELECT * FROM read_parquet('{escaped_path}')"
                self._base_relation = self.con.sql(query)

            elif self.file_format in ("json", "jsonl"):
                # DuckDB read_json natively handles array of objects, jsonl, and raw objects
                query = f"SELECT * FROM read_json('{escaped_path}')"
                raw_rel = self.con.sql(query)

                # Check if this is an API dump containing nested record array (e.g. data, results)
                api_record_keys = (
                    "data",
                    "results",
                    "records",
                    "items",
                    "observations",
                    "filings",
                )
                col_types = dict(zip(raw_rel.columns, [str(t) for t in raw_rel.types]))
                unnest_target: str | None = None
                for key in api_record_keys:
                    if key in col_types:
                        t = col_types[key]
                        if "STRUCT" in t:
                            unnest_target = key
                            break

                if unnest_target:
                    unnest_query = (
                        f"SELECT _rec.* FROM ("
                        f"SELECT unnest({unnest_target}) AS _rec FROM read_json('{escaped_path}')"
                        f")"
                    )
                    self._base_relation = self.con.sql(unnest_query)
                else:
                    self._base_relation = raw_rel

            else:
                raise UnsupportedFormatError(f"Unsupported format: {self.file_format}")

            # Verify relation is readable
            _ = self._base_relation.columns

        except duckdb.InvalidInputException as e:
            raise self._wrap_duckdb_exception(e) from e

        except duckdb.IOException as e:
            raise self._wrap_duckdb_exception(e) from e

        except duckdb.ParserException as e:
            raise SchemaParseError(f"SQL parse error while reading {self.file_path}: {e}") from e

        except Exception as e:
            if isinstance(e, IngestionError):
                raise
            raise self._wrap_duckdb_exception(e) from e

    def count_rows(self) -> int:
        """Count total rows in file without loading entire dataset into memory."""
        if self._base_relation is None:
            return 0
        try:
            res = self._base_relation.count("*").fetchall()
            return int(res[0][0]) if res else 0
        except Exception as e:
            raise self._wrap_duckdb_exception(e) from e

    def to_relation(self) -> duckdb.DuckDBPyRelation:
        """Return DuckDBPyRelation with optional 1-based source_line_no tracking."""
        if self._base_relation is None:
            raise IngestionError("Reader relation is uninitialized.")

        if self.add_source_line and "source_line_no" not in self._base_relation.columns:
            # Register base relation as a temporary view to project 1-based line number
            self.con.register("_source_view", self._base_relation)
            return self.con.sql(
                "SELECT CAST(row_number() OVER () AS BIGINT) AS source_line_no, * FROM _source_view"
            )
        return self._base_relation

    def read_all(self) -> pa.Table:
        """Read all rows into an in-memory PyArrow Table."""
        rel = self.to_relation()
        try:
            return rel.to_arrow_table()
        except Exception as e:
            raise self._wrap_duckdb_exception(e) from e

    def iter_batches(self, batch_size: int = 65536) -> Iterator[pa.RecordBatch]:
        """Stream data in Arrow RecordBatches for zero-copy high throughput processing."""
        rel = self.to_relation()
        try:
            reader = rel.to_arrow_reader(batch_size=batch_size)
            yield from reader
        except Exception as e:
            raise self._wrap_duckdb_exception(e) from e


def read_file(
    file_path: str | Path,
    options: dict[str, Any] | None = None,
    add_source_line: bool = True,
) -> pa.Table:
    """Convenience helper to read any supported file format into a PyArrow Table."""
    reader = IngestReader(file_path, options=options, add_source_line=add_source_line)
    return reader.read_all()


def iter_chunks(
    file_path: str | Path,
    batch_size: int = 65536,
    options: dict[str, Any] | None = None,
    add_source_line: bool = True,
) -> Iterator[pa.RecordBatch]:
    """Convenience helper to iterate over a file in columnar Arrow batches."""
    reader = IngestReader(file_path, options=options, add_source_line=add_source_line)
    yield from reader.iter_batches(batch_size=batch_size)
