"""Point-in-time ("as-of") query engine and vintage audit trail with zero look-ahead bias."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import Self

import duckdb
import pyarrow as pa

# Canonical schema used for empty query results when dataset is missing or empty
CANONICAL_AS_OF_SCHEMA = pa.schema([
    ("entity_id", pa.string()),
    ("event_time", pa.timestamp("us", tz="UTC")),
    ("knowledge_time", pa.timestamp("us", tz="UTC")),
    ("metric", pa.string()),
    ("value", pa.float64()),
    ("unit", pa.string()),
    ("source_symbol", pa.string()),
    ("source_symbol_type", pa.string()),
    ("vendor_id", pa.string()),
    ("dataset_id", pa.string()),
    ("is_restatement", pa.bool_()),
    ("revision_seq", pa.int64()),
])

CANONICAL_AUDIT_SCHEMA = pa.schema([
    ("entity_id", pa.string()),
    ("event_time", pa.timestamp("us", tz="UTC")),
    ("knowledge_time", pa.timestamp("us", tz="UTC")),
    ("metric", pa.string()),
    ("value", pa.float64()),
    ("is_restatement", pa.bool_()),
    ("revision_seq", pa.int64()),
    ("drop_id", pa.string()),
])

PREFERRED_AS_OF_COLUMNS = [
    "entity_id",
    "event_time",
    "knowledge_time",
    "metric",
    "value",
    "unit",
    "source_symbol",
    "source_symbol_type",
    "vendor_id",
    "dataset_id",
    "is_restatement",
    "revision_seq",
]

PREFERRED_AUDIT_COLUMNS = [
    "entity_id",
    "event_time",
    "knowledge_time",
    "metric",
    "value",
    "is_restatement",
    "revision_seq",
    "drop_id",
]


def _normalize_to_utc(dt: datetime) -> datetime:
    """Normalizes naive or timezone-aware datetime to UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


class BitemporalQueryEngine:
    """DuckDB-powered zero look-ahead point-in-time and vintage audit query engine."""

    def __init__(
        self,
        root_dir: Path | str,
        conn: duckdb.DuckDBPyConnection | None = None,
    ) -> None:
        self.root_dir = Path(root_dir)
        self._owns_conn = conn is None
        self.conn = conn or duckdb.connect()
        try:
            self.conn.execute("SET timezone = 'UTC';")
        except duckdb.Error:
            pass

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()

    def close(self, force: bool = False) -> None:
        """Closes internally owned DuckDB connection or when force=True."""
        if (self._owns_conn or force) and hasattr(self, "conn") and self.conn is not None:
            self.conn.close()

    def _has_parquet_files(self, dataset_id: str) -> bool:
        """Checks if dataset directory exists and contains at least one parquet file."""
        dataset_dir = self.root_dir / dataset_id
        if not dataset_dir.is_dir():
            return False
        return any(dataset_dir.glob("**/*.parquet"))

    def get_view_query(self, dataset_id: str) -> str:
        """Returns SQL string to query all partitions with automatic hive partitioning."""
        pattern = str(self.root_dir / dataset_id / "**" / "*.parquet")
        clean_pattern = pattern.replace("'", "''")
        return f"SELECT * FROM read_parquet('{clean_pattern}', hive_partitioning=true)"

    def as_of_query(
        self,
        dataset_id: str,
        as_of_time: datetime,
        entity_ids: list[str] | None = None,
        metric: str | None = None,
    ) -> pa.Table:
        """Executes a point-in-time query returning the latest state known as of `as_of_time`.

        Zero look-ahead bias is strictly guaranteed by filtering `knowledge_time <= as_of_time`
        and window-deduplicating using DuckDB QUALIFY ROW_NUMBER().
        """
        as_of_utc = _normalize_to_utc(as_of_time)
        as_of_iso = as_of_utc.isoformat()

        if not self._has_parquet_files(dataset_id):
            return pa.Table.from_pydict(
                {f.name: [] for f in CANONICAL_AS_OF_SCHEMA},
                schema=CANONICAL_AS_OF_SCHEMA,
            )

        view_sql = self.get_view_query(dataset_id)
        cur = self.conn.cursor()
        sample = cur.sql(f"SELECT * FROM ({view_sql}) LIMIT 0").arrow().read_all()
        available_cols = set(sample.column_names)

        if "metric" in available_cols:
            partition_cols = "entity_id, event_time, metric"
            order_cols = "entity_id, event_time, metric"
        else:
            partition_cols = "entity_id, event_time"
            order_cols = "entity_id, event_time"

        window_order = (
            "knowledge_time DESC, revision_seq DESC"
            if "revision_seq" in available_cols
            else "knowledge_time DESC"
        )

        entity_filter = ""
        if entity_ids is not None:
            if len(entity_ids) == 0:
                entity_filter = "AND 1=0"
            else:
                escaped_ids = [e.replace("'", "''") for e in entity_ids]
                ent_list = ", ".join(f"'{e}'" for e in escaped_ids)
                entity_filter = f"AND entity_id IN ({ent_list})"

        metric_filter = ""
        if metric is not None and "metric" in available_cols:
            clean_metric = metric.replace("'", "''")
            metric_filter = f"AND metric = '{clean_metric}'"

        select_cols = [c for c in PREFERRED_AS_OF_COLUMNS if c in available_cols]
        if not select_cols:
            select_cols = [c for c in available_cols if c != "rn"]
        select_clause = ", ".join(select_cols)

        query = f"""
            SELECT {select_clause}
            FROM ({view_sql})
            WHERE knowledge_time <= TIMESTAMP '{as_of_iso}' {entity_filter} {metric_filter}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY {partition_cols}
                ORDER BY {window_order}
            ) = 1
            ORDER BY {order_cols}
        """
        return cur.sql(query).arrow().read_all()

    def audit_trail(
        self,
        dataset_id: str,
        entity_id: str,
        event_time: datetime,
        metric: str = "CLOSE_PRICE",
    ) -> pa.Table:
        """Retrieves complete revision trajectory for an entity and event_time ordered monotonically."""
        event_time_utc = _normalize_to_utc(event_time)
        ev_iso = event_time_utc.isoformat()

        if not self._has_parquet_files(dataset_id):
            return pa.Table.from_pydict(
                {f.name: [] for f in CANONICAL_AUDIT_SCHEMA},
                schema=CANONICAL_AUDIT_SCHEMA,
            )

        view_sql = self.get_view_query(dataset_id)
        cur = self.conn.cursor()
        sample = cur.sql(f"SELECT * FROM ({view_sql}) LIMIT 0").arrow().read_all()
        available_cols = set(sample.column_names)

        clean_entity_id = entity_id.replace("'", "''")
        clean_metric = metric.replace("'", "''")

        metric_cond = ""
        if "metric" in available_cols:
            metric_cond = f"AND metric = '{clean_metric}'"

        select_cols = [c for c in PREFERRED_AUDIT_COLUMNS if c in available_cols]
        if not select_cols:
            select_cols = list(available_cols)
        select_clause = ", ".join(select_cols)

        order_by = (
            "knowledge_time ASC, revision_seq ASC"
            if "revision_seq" in available_cols
            else "knowledge_time ASC"
        )

        query = f"""
            SELECT {select_clause}
            FROM ({view_sql})
            WHERE entity_id = '{clean_entity_id}'
              AND event_time = TIMESTAMP '{ev_iso}'
              {metric_cond}
            ORDER BY {order_by}
        """
        return cur.sql(query).arrow().read_all()
