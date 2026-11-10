"""SEC Financial Statement Data Sets (FSDS) adapter.

Ingests quarterly SEC FSDS releases containing tab-delimited sub.txt and num.txt
data feeds, joins filing metadata to numerical disclosures on accession number (adsh),
pads CIK identifiers to 10 digits, normalizes bitemporal dates, and builds
canonical Arrow tables.
"""

from __future__ import annotations

import datetime
import zipfile
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa

from vdok.manifest import VendorManifest
from vdok.normalize import CANONICAL_ARROW_SCHEMA, compute_row_hash, normalize_unit
from vdok.symbols import SymbolMaster


def is_sec_fsds_drop(drop_path: Path | str) -> bool:
    """Determine whether drop directory or archive matches SEC FSDS layout."""
    p = Path(drop_path)
    if p.is_file() and p.suffix.lower() == ".zip":
        with zipfile.ZipFile(p, "r") as zf:
            names = [Path(n).name.lower() for n in zf.namelist()]
            return any("sub" in n for n in names) and any("num" in n for n in names)
    if p.is_dir():
        names = [f.name.lower() for f in p.glob("*")]
        return any("sub" in n for n in names) and any("num" in n for n in names)
    return False


def normalize_sec_fsds_drop(
    drop_dir: Path | str,
    manifest: VendorManifest,
    drop_id: str,
    symbol_master: SymbolMaster | None = None,
    as_of: datetime.datetime | None = None,
) -> pa.Table:
    """Normalize an SEC FSDS quarterly drop into a canonical Arrow table.

    Joins sub.txt (metadata and filing dates) with num.txt (disclosed facts)
    on accession number (adsh).
    """
    p = Path(drop_dir)
    con = duckdb.connect()

    sub_file: str | None = None
    num_file: str | None = None

    if p.is_dir():
        for f in p.glob("*"):
            name = f.name.lower()
            if "sub" in name and (name.endswith(".txt") or name.endswith(".tsv")):
                sub_file = str(f)
            elif "num" in name and (name.endswith(".txt") or name.endswith(".tsv")):
                num_file = str(f)

    if not sub_file or not num_file:
        con.close()
        raise FileNotFoundError(
            f"SEC FSDS drop directory {drop_dir} must contain sub.txt and num.txt files."
        )

    # Load and join using DuckDB SQL
    query = f"""
    SELECT
        trim(sub.cik::VARCHAR) AS raw_cik,
        trim(sub.adsh::VARCHAR) AS adsh,
        trim(sub.filed::VARCHAR) AS filed,
        trim(num.tag::VARCHAR) AS tag,
        trim(coalesce(num.version::VARCHAR, '')) AS version,
        trim(coalesce(num.coreg::VARCHAR, '')) AS coreg,
        trim(num.ddate::VARCHAR) AS ddate,
        trim(coalesce(num.qtrs::VARCHAR, '0')) AS qtrs,
        trim(coalesce(num.uom::VARCHAR, '')) AS uom,
        trim(num.value::VARCHAR) AS raw_value,
        row_number() OVER () AS src_row
    FROM read_csv('{sub_file}', delim='\t', header=True, all_varchar=True) sub
    JOIN read_csv('{num_file}', delim='\t', header=True, all_varchar=True) num
      ON trim(sub.adsh::VARCHAR) = trim(num.adsh::VARCHAR)
    WHERE num.value IS NOT NULL AND trim(num.value::VARCHAR) != ''
    """

    res = con.sql(query).to_arrow_table()
    con.close()

    rows = res.to_pylist()
    if not rows:
        return CANONICAL_ARROW_SCHEMA.empty_table()

    records: list[dict[str, Any]] = []

    for r in rows:
        raw_cik = str(r["raw_cik"]).strip()
        padded_cik = raw_cik.zfill(10)
        coreg = str(r["coreg"]).strip() if r.get("coreg") else ""

        if coreg:
            entity_id = f"{padded_cik}|coreg={coreg}"
        else:
            entity_id = padded_cik

        # Identifier resolution via symbol master if available
        is_unmapped = False
        if symbol_master is not None:
            resolved_id, is_unmapped = symbol_master.resolve(
                raw_id=padded_cik, id_type=manifest.entity_id_type
            )
            if coreg:
                entity_id = f"{resolved_id}|coreg={coreg}"
            else:
                entity_id = resolved_id

        tag = str(r["tag"]).strip()
        uom = str(r["uom"]).strip() if r.get("uom") else ""
        qtrs = str(r["qtrs"]).strip() if r.get("qtrs") else "0"
        field_name = f"{tag}|{uom}|{qtrs}"

        # Parse event time (ddate: YYYYMMDD)
        ddate_str = str(r["ddate"]).strip()
        try:
            event_dt = datetime.datetime.strptime(ddate_str, "%Y%m%d").replace(
                tzinfo=datetime.timezone.utc
            )
        except ValueError:
            continue

        # Parse knowledge time (filed: YYYYMMDD)
        filed_str = str(r["filed"]).strip()
        try:
            kt_dt = datetime.datetime.strptime(filed_str, "%Y%m%d").replace(
                tzinfo=datetime.timezone.utc
            )
        except ValueError:
            kt_dt = as_of if as_of is not None else datetime.datetime.now(datetime.timezone.utc)

        raw_val = str(r["raw_value"]).strip()
        try:
            val_num = Decimal(raw_val)
            val_str = None
        except Exception:
            val_num = None
            val_str = raw_val

        mapped_unit = normalize_unit(uom) if uom else None

        r_hash = compute_row_hash(
            entity_id=entity_id,
            field=field_name,
            event_time=event_dt,
            value_num=val_num,
            value_str=val_str,
            unit=mapped_unit,
        )

        records.append(
            {
                "entity_id": entity_id,
                "entity_id_type": manifest.entity_id_type.value,
                "field": field_name,
                "event_time": event_dt,
                "knowledge_time": kt_dt,
                "value_num": val_num,
                "value_str": val_str,
                "unit": mapped_unit,
                "drop_id": drop_id,
                "source_file": Path(num_file).name,
                "source_row": int(r["src_row"]),
                "row_hash": r_hash,
                "unmapped": is_unmapped,
            }
        )

    if not records:
        return CANONICAL_ARROW_SCHEMA.empty_table()

    return pa.Table.from_pylist(records, schema=CANONICAL_ARROW_SCHEMA)
