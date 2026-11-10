"""FRED/ALFRED Macroeconomic Vintage Series Ingestion Adapter.

Ingestion adapter for Federal Reserve Economic Data (FRED/ALFRED) vintage time series
with publication vintages (realtime_start mapped to knowledge_time) and observation
dates (date mapped to event_time), tracking revision sequences and zero look-ahead as-of filtering.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa


class AlfredVintageLoader:
    """Ingestion loader for FRED/ALFRED macroeconomic vintage time series."""

    def __init__(self, conn: duckdb.DuckDBPyConnection | None = None) -> None:
        self.conn = conn or duckdb.connect()
        try:
            self.conn.execute("SET timezone = 'UTC';")
        except duckdb.Error:
            pass

    def load_raw(self, csv_path: Path | str) -> pa.Table:
        """Loads raw vintage series with vintage_date, observation_date, and metric_value."""
        return load_alfred_raw(csv_path, conn=self.conn)

    def load_series(
        self,
        csv_path: Path | str,
        series_id: str = "GDPC1",
        vendor_id: str = "FRED_ALFRED",
        unit: str | None = None,
    ) -> pa.Table:
        """Loads canonical bitemporal records tracking revision sequence per observation date."""
        return load_alfred_series(
            csv_path,
            conn=self.conn,
            series_id=series_id,
            vendor_id=vendor_id,
            unit=unit,
        )

    def as_of(
        self,
        table: pa.Table,
        as_of_time: datetime | str | date,
        latest_only: bool = False,
    ) -> pa.Table:
        """Filters table as-of timestamp with strictly zero look-ahead bias."""
        return as_of(table, as_of_time, conn=self.conn, latest_only=latest_only)


def load_alfred_raw(
    csv_path: Path | str,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> pa.Table:
    """Loads raw ALFRED vintage series CSV into a PyArrow Table.

    Returns columns: vintage_date (DATE), observation_date (DATE), metric_value (DOUBLE).
    """
    con = conn or duckdb.connect()
    clean_path = str(Path(csv_path).resolve()).replace("'", "''")

    query = f"""
        SELECT
            CAST(realtime_start AS DATE) as vintage_date,
            CAST(date AS DATE) as observation_date,
            TRY_CAST(value AS DOUBLE) as metric_value
        FROM read_csv('{clean_path}', header=true, nullstr=['.', 'NA', 'null', ''])
        ORDER BY observation_date, vintage_date
    """
    return con.sql(query).arrow().read_all()


def load_alfred_series(
    csv_path: Path | str,
    conn: duckdb.DuckDBPyConnection | None = None,
    series_id: str = "GDPC1",
    vendor_id: str = "FRED_ALFRED",
    unit: str | None = None,
) -> pa.Table:
    """Loads ALFRED vintage series CSV into canonical bitemporal records.

    Maps realtime_start -> knowledge_time, date -> event_time, value -> metric_value.
    Tracks sequential revisions per observation date:
    - revision_seq: 1, 2, 3...
    - is_restatement: True if revision_seq > 1
    """
    con = conn or duckdb.connect()
    clean_path = str(Path(csv_path).resolve()).replace("'", "''")

    query = f"""
        SELECT
            strptime(CAST(realtime_start AS VARCHAR), '%Y-%m-%d')::TIMESTAMP as knowledge_time,
            strptime(CAST(date AS VARCHAR), '%Y-%m-%d')::TIMESTAMP as event_time,
            TRY_CAST(value AS DOUBLE) as value
        FROM read_csv('{clean_path}', header=true, nullstr=['.', 'NA', 'null', ''])
        ORDER BY event_time, knowledge_time
    """
    raw_tbl = con.sql(query).arrow().read_all()

    # Determine default unit
    if unit is not None:
        unit_str = unit
    elif series_id == "GDPC1":
        unit_str = "USD_BILLIONS"
    elif series_id == "CPIAUCSL":
        unit_str = "INDEX_1982_1984"
    else:
        unit_str = "THOUSANDS_PERSONS"

    records: list[dict[str, Any]] = []
    obs_counts: dict[datetime, int] = {}

    for r in raw_tbl.to_pylist():
        ev_dt = (
            r["event_time"].replace(tzinfo=UTC)
            if r["event_time"].tzinfo is None
            else r["event_time"]
        )
        kn_dt = (
            r["knowledge_time"].replace(tzinfo=UTC)
            if r["knowledge_time"].tzinfo is None
            else r["knowledge_time"]
        )

        obs_counts[ev_dt] = obs_counts.get(ev_dt, 0) + 1
        seq = obs_counts[ev_dt]

        records.append(
            {
                "record_id": f"{series_id}_{ev_dt.strftime('%Y%m%d')}_V{seq}",
                "entity_id": series_id,
                "source_symbol": series_id,
                "source_symbol_type": "SERIES_ID",
                "event_time": ev_dt,
                "knowledge_time": kn_dt,
                "metric": series_id,
                "value": r["value"],
                "unit": unit_str,
                "vendor_id": vendor_id,
                "dataset_id": "MACRO_ALFRED_VINTAGES",
                "drop_id": f"VINTAGE_{kn_dt.strftime('%Y%m%d')}",
                "is_restatement": seq > 1,
                "revision_seq": seq,
            }
        )

    if not records:
        schema = pa.schema(
            [
                ("record_id", pa.string()),
                ("entity_id", pa.string()),
                ("source_symbol", pa.string()),
                ("source_symbol_type", pa.string()),
                ("event_time", pa.timestamp("us", tz="UTC")),
                ("knowledge_time", pa.timestamp("us", tz="UTC")),
                ("metric", pa.string()),
                ("value", pa.float64()),
                ("unit", pa.string()),
                ("vendor_id", pa.string()),
                ("dataset_id", pa.string()),
                ("drop_id", pa.string()),
                ("is_restatement", pa.bool_()),
                ("revision_seq", pa.int64()),
            ]
        )
        return pa.Table.from_pydict({f.name: [] for f in schema}, schema=schema)

    return pa.Table.from_pylist(records)


def as_of(
    table: pa.Table,
    as_of_time: datetime | str | date,
    conn: duckdb.DuckDBPyConnection | None = None,
    latest_only: bool = False,
) -> pa.Table:
    """Filters records with strictly zero look-ahead bias (knowledge_time <= as_of_time).

    Works on both canonical bitemporal tables (using knowledge_time)
    and raw vintage tables (using vintage_date).
    If latest_only=True, applies ROW_NUMBER() window to return only the latest
    vintage per event_time / observation_date.
    """
    con = conn or duckdb.connect()
    con.register("tbl", table)

    # Normalize as_of timestamp / date
    if isinstance(as_of_time, datetime):
        as_of_utc = (
            as_of_time.replace(tzinfo=UTC)
            if as_of_time.tzinfo is None
            else as_of_time.astimezone(UTC)
        )
        as_of_iso = as_of_utc.isoformat()
        as_of_date_str = as_of_utc.strftime("%Y-%m-%d")
    elif isinstance(as_of_time, date):
        as_of_date_str = as_of_time.strftime("%Y-%m-%d")
        as_of_iso = f"{as_of_date_str}T23:59:59.999999Z"
    else:
        # String representation
        as_of_str = str(as_of_time).strip()
        as_of_date_str = as_of_str[:10]
        if len(as_of_str) == 10:
            as_of_iso = f"{as_of_date_str}T23:59:59.999999Z"
        else:
            as_of_iso = as_of_str

    if "knowledge_time" in table.column_names:
        clean_iso = as_of_iso.replace("'", "''")
        tie_break = ", revision_seq DESC" if "revision_seq" in table.column_names else ""
        if latest_only:
            query = f"""
                WITH filtered AS (
                    SELECT * FROM tbl
                    WHERE knowledge_time <= TIMESTAMPTZ '{clean_iso}'
                ),
                ranked AS (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY event_time
                            ORDER BY knowledge_time DESC{tie_break}
                        ) as rn
                    FROM filtered
                )
                SELECT * EXCLUDE (rn) FROM ranked WHERE rn = 1
                ORDER BY event_time
            """
        else:
            query = f"""
                SELECT * FROM tbl
                WHERE knowledge_time <= TIMESTAMPTZ '{clean_iso}'
                ORDER BY event_time, knowledge_time
            """
    elif "vintage_date" in table.column_names:
        clean_d = as_of_date_str.replace("'", "''")
        if latest_only:
            query = f"""
                WITH filtered AS (
                    SELECT * FROM tbl
                    WHERE vintage_date <= DATE '{clean_d}'
                ),
                ranked AS (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY observation_date
                            ORDER BY vintage_date DESC
                        ) as rn
                    FROM filtered
                )
                SELECT * EXCLUDE (rn) FROM ranked WHERE rn = 1
                ORDER BY observation_date
            """
        else:
            query = f"""
                SELECT * FROM tbl
                WHERE vintage_date <= DATE '{clean_d}'
                ORDER BY observation_date, vintage_date
            """
    else:
        raise ValueError(
            f"Table does not contain 'knowledge_time' or 'vintage_date' column. Found: {table.column_names}"
        )

    return con.sql(query).arrow().read_all()
