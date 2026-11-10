"""SEC DERA Financial Statement Data Sets Ingestion Adapter.

Relational ingestion loader joining sub.txt (submissions) and num.txt (numeric facts)
on accession number (adsh), zero-padding CIK identifiers, mapping filing dates to
knowledge time and balance sheet dates to event time, and detecting Form 10-K/A amendments.
"""

from __future__ import annotations

from datetime import UTC
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa


class SECDeraLoader:
    """Relational loader for SEC DERA financial statements data sets."""

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection | None = None,
        symbology_master: Any = None,
    ) -> None:
        self.conn = conn or duckdb.connect()
        try:
            self.conn.execute("SET timezone = 'UTC';")
        except duckdb.Error:
            pass
        self.symbology_master = symbology_master

    def load_raw(self, sub_path: Path | str, num_path: Path | str) -> pa.Table:
        """Executes relational join between sub.txt and num.txt returning raw tabular facts."""
        return load_sec_dera(sub_path=sub_path, num_path=num_path, conn=self.conn)

    def load_facts(
        self,
        sub_path: Path | str,
        num_path: Path | str,
        symbology_master: Any = None,
        drop_id: str = "SEC_DROP",
        vendor_id: str = "SEC_EDGAR",
    ) -> pa.Table:
        """Loads canonical bitemporal fact records with CIK resolution and restatement flagging."""
        master = symbology_master if symbology_master is not None else self.symbology_master
        return load_sec_dera_facts(
            sub_path=sub_path,
            num_path=num_path,
            conn=self.conn,
            symbology_master=master,
            drop_id=drop_id,
            vendor_id=vendor_id,
        )


def load_sec_dera(
    sub_path: Path | str,
    num_path: Path | str,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> pa.Table:
    """Performs relational join on sub.txt and num.txt on accession number (adsh).

    Returns PyArrow Table with zero-padded CIK, company_name, filing_form,
    knowledge_date, metric_tag, taxonomy_version, event_date, unit, fact_value, adsh.
    """
    con = conn or duckdb.connect()
    sub_str = str(Path(sub_path).resolve()).replace("'", "''")
    num_str = str(Path(num_path).resolve()).replace("'", "''")

    query = f"""
        SELECT
            printf('%010d', CAST(s.cik AS BIGINT)) as cik,
            s.name as company_name,
            s.form as filing_form,
            strptime(CAST(s.filed AS VARCHAR), '%Y%m%d')::DATE as knowledge_date,
            n.tag as metric_tag,
            n.version as taxonomy_version,
            strptime(CAST(n.ddate AS VARCHAR), '%Y%m%d')::DATE as event_date,
            n.uom as unit,
            TRY_CAST(n.value AS DOUBLE) as fact_value,
            trim(CAST(s.adsh AS VARCHAR)) as adsh
        FROM read_csv('{sub_str}', delim='\t', header=true) s
        JOIN read_csv('{num_str}', delim='\t', header=true) n
          ON trim(CAST(s.adsh AS VARCHAR)) = trim(CAST(n.adsh AS VARCHAR))
        ORDER BY cik, event_date, metric_tag
    """
    return con.sql(query).arrow().read_all()


def load_sec_dera_facts(
    sub_path: Path | str,
    num_path: Path | str,
    conn: duckdb.DuckDBPyConnection | None = None,
    symbology_master: Any = None,
    drop_id: str = "SEC_DROP",
    vendor_id: str = "SEC_EDGAR",
) -> pa.Table:
    """Loads SEC DERA financial statement facts as canonical bitemporal records.

    Maps CIK to canonical entity_id (via symbology_master or ENT_CIK_{cik}),
    normalizes event_time (ddate) and knowledge_time (filed) to UTC,
    and flags Form 10-K/A and 10-Q/A amendments as restatements with revision_seq=2.
    """
    con = conn or duckdb.connect()
    sub_str = str(Path(sub_path).resolve()).replace("'", "''")
    num_str = str(Path(num_path).resolve()).replace("'", "''")

    query = f"""
        SELECT
            printf('%010d', CAST(s.cik AS BIGINT)) as cik,
            s.name as company_name,
            s.form as filing_form,
            trim(CAST(s.adsh AS VARCHAR)) as adsh,
            strptime(CAST(s.filed AS VARCHAR), '%Y%m%d')::TIMESTAMP as knowledge_time,
            strptime(CAST(n.ddate AS VARCHAR), '%Y%m%d')::TIMESTAMP as event_time,
            n.tag as metric,
            TRY_CAST(n.value AS DOUBLE) as value,
            n.uom as unit
        FROM read_csv('{sub_str}', delim='\t', header=true) s
        JOIN read_csv('{num_str}', delim='\t', header=true) n
          ON trim(CAST(s.adsh AS VARCHAR)) = trim(CAST(n.adsh AS VARCHAR))
        ORDER BY cik, event_time, metric
    """
    raw_tbl = con.sql(query).arrow().read_all()

    records: list[dict[str, Any]] = []
    for r in raw_tbl.to_pylist():
        as_of_dt = (
            r["knowledge_time"].replace(tzinfo=UTC)
            if r["knowledge_time"].tzinfo is None
            else r["knowledge_time"]
        )
        ev_dt = (
            r["event_time"].replace(tzinfo=UTC)
            if r["event_time"].tzinfo is None
            else r["event_time"]
        )

        cik_str = str(r["cik"])
        if symbology_master is not None and hasattr(symbology_master, "resolve"):
            entity_id = symbology_master.resolve(
                cik_str, "CIK", as_of=as_of_dt, vendor_id=vendor_id
            )
        else:
            entity_id = f"ENT_CIK_{cik_str}"

        is_restatement = "10-K/A" in str(r["filing_form"]) or "10-Q/A" in str(r["filing_form"])

        records.append(
            {
                "record_id": f"{r['adsh']}_{r['metric']}",
                "entity_id": entity_id,
                "source_symbol": cik_str,
                "source_symbol_type": "CIK",
                "company_name": r["company_name"],
                "filing_form": r["filing_form"],
                "adsh": r["adsh"],
                "event_time": ev_dt,
                "knowledge_time": as_of_dt,
                "metric": r["metric"],
                "value": r["value"],
                "unit": r["unit"],
                "vendor_id": vendor_id,
                "dataset_id": "SEC_DERA_FINANCIALS",
                "drop_id": drop_id,
                "is_restatement": is_restatement,
                "revision_seq": 2 if is_restatement else 1,
            }
        )

    if not records:
        schema = pa.schema(
            [
                ("record_id", pa.string()),
                ("entity_id", pa.string()),
                ("source_symbol", pa.string()),
                ("source_symbol_type", pa.string()),
                ("company_name", pa.string()),
                ("filing_form", pa.string()),
                ("adsh", pa.string()),
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
