"""Append-only partitioned Parquet bitemporal store and storage factor reporter."""

from __future__ import annotations

import os
import stat
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from vendor_onboarding.models.record import CanonicalRecord, records_to_arrow
from vendor_onboarding.storage.query import BitemporalQueryEngine


class ImmutabilityError(IOError, ValueError):
    """Raised when an attempt is made to overwrite an immutable partition file."""


@dataclass(frozen=True)
class StorageFactorReport:
    """Storage expansion factor report comparing physical bitemporal bytes to deduplicated state."""

    dataset_id: str
    total_bitemporal_bytes: int
    deduplicated_bytes: int
    expansion_factor: float

    @property
    def meets_sla(self) -> bool:
        """Checks if storage expansion factor satisfies SLA (<= 1.30x)."""
        return self.expansion_factor <= 1.30


class BitemporalStore(BitemporalQueryEngine):
    """Institutional partitioned append-only Parquet bitemporal store.

    Enforces physical file immutability, Snappy/ZSTD compression, dictionary encoding,
    and read-only filesystem permissions (0o444).
    """

    def __init__(
        self,
        root_dir: Path | str,
        conn: duckdb.DuckDBPyConnection | None = None,
        compression: str = "SNAPPY",
    ) -> None:
        super().__init__(root_dir=root_dir, conn=conn)
        self.compression = compression

    def append(
        self,
        records: pa.Table | Sequence[CanonicalRecord],
        dataset_id: str,
        drop_id: str | None = None,
        compression: str | None = None,
    ) -> int:
        """Appends a new partition drop to the store with strict physical immutability.

        Args:
            records: PyArrow Table or sequence of CanonicalRecord instances.
            dataset_id: Target dataset family identifier.
            drop_id: Ingestion drop identifier. If None, inferred from records.
            compression: Optional compression codec ("SNAPPY", "ZSTD"). Defaults to store configuration.

        Returns:
            Number of rows appended.

        Raises:
            ValueError: If drop_id cannot be determined.
            ImmutabilityError: If partition destination file already exists.
        """
        # Normalize input records to PyArrow Table
        if isinstance(records, pa.Table):
            table = records
        elif isinstance(records, Sequence):
            table = records_to_arrow(records)
        else:
            raise TypeError(f"Unsupported records type: {type(records)}")

        # Extract drop_id if not explicitly provided
        if drop_id is None or not str(drop_id).strip():
            if isinstance(records, pa.Table):
                if "drop_id" in records.column_names and records.num_rows > 0:
                    val = records["drop_id"][0].as_py()
                    if val is not None and str(val).strip():
                        drop_id = str(val).strip()
            elif isinstance(records, Sequence) and len(records) > 0:
                first = records[0]
                if isinstance(first, CanonicalRecord) or hasattr(first, "drop_id"):
                    val = first.drop_id
                    if val is not None and str(val).strip():
                        drop_id = str(val).strip()

        if drop_id is None or not str(drop_id).strip():
            raise ValueError(
                "Cannot determine drop_id; drop_id must be provided or present in records."
            )

        dest_dir = self.root_dir / dataset_id / f"drop_id={drop_id}"
        dest_file = dest_dir / "data.parquet"

        # Strict physical immutability: never overwrite existing partition file
        if dest_file.exists():
            raise ImmutabilityError(
                f"Partition already exists and is immutable: {dest_file}"
            )

        # Create parent directories
        dest_dir.mkdir(parents=True, exist_ok=True)

        # Write Parquet atomically to temporary file, set permissions, then rename
        comp = compression or self.compression
        temp_file = dest_dir / f".data_{os.getpid()}_{id(table)}.tmp"
        try:
            pq.write_table(table, temp_file, compression=comp, use_dictionary=True)
            # Set read-only file permissions (0o444) on temp file
            os.chmod(temp_file, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)

            # Final check before atomic rename to preserve strict immutability
            if dest_file.exists():
                raise ImmutabilityError(
                    f"Partition already exists and is immutable: {dest_file}"
                )
            os.replace(temp_file, dest_file)
        finally:
            if temp_file.exists():
                try:
                    temp_file.unlink()
                except OSError:
                    pass

        return int(table.num_rows)

    def storage_factor(self, dataset_id: str) -> StorageFactorReport:
        """Calculates storage expansion factor comparing physical bitemporal bytes to deduplicated state."""
        dataset_dir = self.root_dir / dataset_id
        if not dataset_dir.is_dir():
            return StorageFactorReport(
                dataset_id=dataset_id,
                total_bitemporal_bytes=0,
                deduplicated_bytes=0,
                expansion_factor=1.0,
            )

        parquet_files = list(dataset_dir.glob("**/*.parquet"))
        if not parquet_files:
            return StorageFactorReport(
                dataset_id=dataset_id,
                total_bitemporal_bytes=0,
                deduplicated_bytes=0,
                expansion_factor=1.0,
            )

        total_bytes = sum(f.stat().st_size for f in parquet_files)

        view_sql = self.get_view_query(dataset_id)
        cur = self.conn.cursor()
        sample = cur.sql(f"SELECT * FROM ({view_sql}) LIMIT 0").arrow().read_all()
        available_cols = set(sample.column_names)

        partition_cols = (
            "entity_id, event_time, metric"
            if "metric" in available_cols
            else "entity_id, event_time"
        )
        window_order = (
            "knowledge_time DESC, revision_seq DESC"
            if "revision_seq" in available_cols
            else "knowledge_time DESC"
        )

        dedup_query = f"""
            WITH ranked AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY {partition_cols}
                        ORDER BY {window_order}
                    ) as rn
                FROM ({view_sql})
            )
            SELECT * EXCLUDE (rn) FROM ranked WHERE rn = 1
        """
        dedup_table = cur.sql(dedup_query).arrow().read_all()
        sink = pa.BufferOutputStream()
        pq.write_table(dedup_table, sink, compression="SNAPPY")
        dedup_bytes = sink.getvalue().size

        factor = (total_bytes / dedup_bytes) if dedup_bytes > 0 else 1.0
        return StorageFactorReport(
            dataset_id=dataset_id,
            total_bitemporal_bytes=total_bytes,
            deduplicated_bytes=dedup_bytes,
            expansion_factor=factor,
        )


# Alias per requirement F-STR-01
BitemporalParquetStore = BitemporalStore
