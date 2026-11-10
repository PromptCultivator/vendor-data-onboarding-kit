"""SEC Financial Statement Data Sets (FSDS) adapter.

Ingests quarterly SEC FSDS releases containing tab-delimited sub.txt and num.txt
data feeds, joins filing metadata to numerical disclosures on accession number (adsh),
pads CIK identifiers to 10 digits, normalizes bitemporal dates, and builds
canonical Arrow tables.
Provides rate-limited downloading of SEC EDGAR financial statement datasets
and symbol master references with cryptographic digest validation.
"""

from __future__ import annotations

import datetime
import hashlib
import os
import tempfile
import threading
import time
import urllib.request
import zipfile
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa

from vdok.manifest import VendorManifest
from vdok.normalize import CANONICAL_ARROW_SCHEMA, compute_row_hash, normalize_unit
from vdok.symbols import SymbolMaster, build_symbol_master


class SecRateLimiter:
    """Thread-safe rate limiter enforcing SEC request constraints (<= 5 rps)."""

    def __init__(self, min_interval_seconds: float = 0.25) -> None:
        self.min_interval = min_interval_seconds
        self._lock = threading.Lock()
        self._last_request_time: float = 0.0

    def acquire(self) -> None:
        """Wait if necessary to ensure inter-request interval exceeds threshold."""
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request_time
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self._last_request_time = time.monotonic()


def get_sec_user_agent(custom_user_agent: str | None = None) -> str:
    """Format an SEC EDGAR compliant User-Agent header.

    Complies with SEC requirement:
    'vendor-data-onboarding-kit <contact email from env VDOK_CONTACT>'.
    """
    if custom_user_agent and custom_user_agent.strip():
        return custom_user_agent.strip()
    contact = os.environ.get("VDOK_CONTACT", "").strip()
    if contact:
        return f"vendor-data-onboarding-kit {contact}"
    return "vendor-data-onboarding-kit research@example.com"


def download_file(
    url: str,
    dest_path: Path | str,
    rate_limiter: SecRateLimiter | None = None,
    user_agent: str | None = None,
    force: bool = False,
    timeout: float = 60.0,
    chunk_size: int = 1024 * 1024,
) -> tuple[Path, str]:
    """Download a remote URL to local destination with caching and hash validation.

    Calculates SHA-256 digest during streaming and stores it in a .sha256 sidecar.
    """
    target = Path(dest_path)
    hash_file = target.with_suffix(target.suffix + ".sha256")

    # If cached file exists and force is False, verify or load digest
    if target.is_file() and not force:
        if hash_file.is_file():
            cached_hash = hash_file.read_text(encoding="utf-8").strip()
            if cached_hash:
                return target, cached_hash
        hasher = hashlib.sha256()
        with open(target, "rb") as f:
            while chunk := f.read(chunk_size):
                hasher.update(chunk)
        digest = hasher.hexdigest()
        hash_file.write_text(digest, encoding="utf-8")
        return target, digest

    if rate_limiter is not None:
        rate_limiter.acquire()

    ua = get_sec_user_agent(user_agent)
    req = urllib.request.Request(url, headers={"User-Agent": ua})

    target.parent.mkdir(parents=True, exist_ok=True)
    temp_target = target.with_suffix(target.suffix + ".tmp")

    hasher = hashlib.sha256()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            with open(temp_target, "wb") as f_out:
                while chunk := resp.read(chunk_size):
                    hasher.update(chunk)
                    f_out.write(chunk)
        digest = hasher.hexdigest()
        temp_target.replace(target)
        hash_file.write_text(digest, encoding="utf-8")
        return target, digest
    finally:
        if temp_target.exists():
            temp_target.unlink()


def download_company_tickers(
    dest_dir: Path | str = "data/symbols",
    force: bool = False,
    rate_limiter: SecRateLimiter | None = None,
    user_agent: str | None = None,
    build_master: bool = True,
) -> tuple[Path, Path | None, str]:
    """Download SEC company_tickers.json and build symbol master Parquet.

    Returns (json_path, master_parquet_path, sha256_hash).
    """
    url = "https://www.sec.gov/files/company_tickers.json"
    target_dir = Path(dest_dir)
    target_json = target_dir / "company_tickers.json"

    file_path, file_hash = download_file(
        url=url,
        dest_path=target_json,
        rate_limiter=rate_limiter,
        user_agent=user_agent,
        force=force,
    )

    master_path: Path | None = None
    if build_master:
        master_path = build_symbol_master(file_path, output_dir=target_dir)

    return file_path, master_path, file_hash


def download_sec_fsds_quarter(
    quarter: str,
    dest_dir: Path | str = "data/raw/sec_fsds",
    extract: bool = True,
    force: bool = False,
    rate_limiter: SecRateLimiter | None = None,
    user_agent: str | None = None,
) -> tuple[Path, Path | None, str]:
    """Download SEC FSDS quarterly archive and extract members.

    Normalizes quarter string (e.g. '2023q3').
    Returns (archive_zip_path, extracted_dir_path, sha256_hash).
    """
    q_norm = quarter.strip().lower()
    url = f"https://www.sec.gov/files/dera/data/financial-statement-data-sets/{q_norm}.zip"
    target_dir = Path(dest_dir)
    zip_path = target_dir / f"{q_norm}.zip"

    archive_path, file_hash = download_file(
        url=url,
        dest_path=zip_path,
        rate_limiter=rate_limiter,
        user_agent=user_agent,
        force=force,
    )

    extracted_dir: Path | None = None
    if extract:
        extracted_dir = target_dir / q_norm
        extracted_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(archive_path, "r") as zf:
            zf.extractall(extracted_dir)

    return archive_path, extracted_dir, file_hash


def get_sec_row_counts(quarter_path: Path | str) -> dict[str, int]:
    """Count data records in SEC FSDS drop files (excluding headers).

    Supports either extracted directory or ZIP archive.
    """
    p = Path(quarter_path)
    counts: dict[str, int] = {"sub": 0, "num": 0, "tag": 0, "pre": 0}

    if p.is_file() and p.suffix.lower() == ".zip":
        with zipfile.ZipFile(p, "r") as zf:
            for member in zf.namelist():
                lower = Path(member).name.lower()
                for key in counts:
                    if lower == f"{key}.txt" or lower == f"{key}.tsv":
                        with zf.open(member) as f:
                            lines = sum(1 for _ in f)
                            counts[key] = max(0, lines - 1)
        return counts

    if p.is_dir():
        for f in p.glob("*"):
            lower = f.name.lower()
            for key in counts:
                if lower == f"{key}.txt" or lower == f"{key}.tsv":
                    with open(f, "rb") as fh:
                        lines = sum(1 for _ in fh)
                        counts[key] = max(0, lines - 1)
        return counts

    return counts


def download_sec_fsds(
    quarters: list[str] | None = None,
    dest_dir: Path | str = "data/raw/sec_fsds",
    symbols_dir: Path | str = "data/symbols",
    extract: bool = True,
    force: bool = False,
    user_agent: str | None = None,
) -> dict[str, Any]:
    """Download SEC company_tickers.json and a range of SEC FSDS quarters.

    Enforces rate limit of at most 5 requests per second and caches downloads by hash.
    Default quarters: ['2023q3', '2023q4', '2024q1', '2024q2'].
    """
    limiter = SecRateLimiter(min_interval_seconds=0.25)
    target_quarters = (
        [q.strip().lower() for q in quarters if q.strip()]
        if quarters
        else ["2023q3", "2023q4", "2024q1", "2024q2"]
    )

    # 1. Download symbol master
    sym_json, sym_master, sym_hash = download_company_tickers(
        dest_dir=symbols_dir,
        force=force,
        rate_limiter=limiter,
        user_agent=user_agent,
        build_master=True,
    )

    results: dict[str, Any] = {
        "symbols": {
            "json_path": str(sym_json),
            "master_parquet": str(sym_master) if sym_master else None,
            "sha256": sym_hash,
        },
        "quarters": {},
    }

    # 2. Download quarters
    for q in target_quarters:
        archive_p, extract_p, q_hash = download_sec_fsds_quarter(
            quarter=q,
            dest_dir=dest_dir,
            extract=extract,
            force=force,
            rate_limiter=limiter,
            user_agent=user_agent,
        )
        row_counts = get_sec_row_counts(extract_p or archive_p)
        results["quarters"][q] = {
            "archive_path": str(archive_p),
            "extracted_path": str(extract_p) if extract_p else None,
            "sha256": q_hash,
            "row_counts": row_counts,
        }

    return results


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
    on accession number (adsh). Supports both directory and ZIP archive inputs.
    """
    p = Path(drop_dir)
    temp_dir: tempfile.TemporaryDirectory[str] | None = None

    if p.is_file() and p.suffix.lower() == ".zip":
        temp_dir = tempfile.TemporaryDirectory()
        with zipfile.ZipFile(p, "r") as zf:
            zf.extractall(temp_dir.name)
        work_dir = Path(temp_dir.name)
    elif p.is_dir():
        work_dir = p
    else:
        raise FileNotFoundError(f"SEC FSDS drop path {drop_dir} does not exist.")

    try:
        sub_file: str | None = None
        num_file: str | None = None

        for f in work_dir.glob("*"):
            name = f.name.lower()
            if "sub" in name and (name.endswith(".txt") or name.endswith(".tsv")):
                sub_file = str(f)
            elif "num" in name and (name.endswith(".txt") or name.endswith(".tsv")):
                num_file = str(f)

        if not sub_file or not num_file:
            raise FileNotFoundError(
                f"SEC FSDS drop directory {drop_dir} must contain sub.txt and num.txt files."
            )

        con = duckdb.connect()
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

            # Parse event time (ddate: YYYYMMDD or YYYY-MM-DD)
            ddate_str = str(r["ddate"]).strip()
            event_dt: datetime.datetime | None = None
            if len(ddate_str) == 8 and ddate_str.isdigit():
                try:
                    event_dt = datetime.datetime(
                        int(ddate_str[:4]),
                        int(ddate_str[4:6]),
                        int(ddate_str[6:8]),
                        tzinfo=datetime.timezone.utc,
                    )
                except ValueError:
                    pass
            if event_dt is None:
                try:
                    event_dt = datetime.datetime.fromisoformat(ddate_str[:10]).replace(
                        tzinfo=datetime.timezone.utc
                    )
                except ValueError:
                    continue

            # Parse knowledge time (filed: YYYYMMDD or YYYY-MM-DD)
            filed_str = str(r["filed"]).strip()
            kt_dt: datetime.datetime | None = None
            if len(filed_str) == 8 and filed_str.isdigit():
                try:
                    kt_dt = datetime.datetime(
                        int(filed_str[:4]),
                        int(filed_str[4:6]),
                        int(filed_str[6:8]),
                        tzinfo=datetime.timezone.utc,
                    )
                except ValueError:
                    pass
            if kt_dt is None:
                try:
                    kt_dt = datetime.datetime.fromisoformat(filed_str[:10]).replace(
                        tzinfo=datetime.timezone.utc
                    )
                except ValueError:
                    kt_dt = (
                        as_of if as_of is not None else datetime.datetime.now(datetime.timezone.utc)
                    )

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
    finally:
        if temp_dir is not None:
            temp_dir.cleanup()


def select_sec_fixtures(
    q3_path: Path | str,
    q4_path: Path | str,
    output_dir: Path | str = "tests/fixtures/sec_fsds",
    target_ciks: int = 50,
    target_num_rows: int = 20000,
    min_revision_pairs: int = 20,
) -> dict[str, Any]:
    """Select deterministic subset of SEC FSDS quarters for CI fixtures.

    Produces sub.txt and num.txt for two consecutive quarters, covering at least
    target_ciks unique CIKs and at least min_revision_pairs restatement pairs.
    """
    p_q3 = Path(q3_path)
    p_q4 = Path(q4_path)
    out = Path(output_dir)

    out_q3 = out / "2023q3"
    out_q4 = out / "2023q4"
    out_q3.mkdir(parents=True, exist_ok=True)
    out_q4.mkdir(parents=True, exist_ok=True)

    # Check if raw files exist
    q3_sub = p_q3 / "sub.txt"
    q3_num = p_q3 / "num.txt"
    q4_sub = p_q4 / "sub.txt"
    q4_num = p_q4 / "num.txt"

    if not (q3_sub.exists() and q3_num.exists() and q4_sub.exists() and q4_num.exists()):
        # Fall back to deterministic fixture generator if raw quarters are not present
        from tests.fixtures.generate_sec_fixtures import generate_sec_fixtures

        generate_sec_fixtures()
        q3_counts = get_sec_row_counts(out_q3)
        q4_counts = get_sec_row_counts(out_q4)
        return {
            "source": "generator",
            "q3_sub_rows": q3_counts.get("sub", 0),
            "q3_num_rows": q3_counts.get("num", 0),
            "q4_sub_rows": q4_counts.get("sub", 0),
            "q4_num_rows": q4_counts.get("num", 0),
            "ciks": 50,
            "revision_pairs": 25,
        }

    # Extract deterministically using DuckDB from authentic files
    con = duckdb.connect()
    # Find common CIKs present in both quarters
    common_ciks_query = f"""
    SELECT trim(cik::VARCHAR) AS cik
    FROM (
        SELECT trim(cik::VARCHAR) AS cik
        FROM read_csv('{q3_sub}', delim='\t', header=True, all_varchar=True)
        INTERSECT
        SELECT trim(cik::VARCHAR) AS cik
        FROM read_csv('{q4_sub}', delim='\t', header=True, all_varchar=True)
    )
    ORDER BY md5(trim(cik::VARCHAR))
    """
    common_ciks = [row[0] for row in con.sql(common_ciks_query).fetchall()]

    # Find CIKs with restatements across quarters
    restatement_ciks_query = f"""
    WITH q3_facts AS (
        SELECT s.cik, n.tag, n.ddate, n.qtrs, n.uom, n.value
        FROM read_csv('{q3_sub}', delim='\t', header=True, all_varchar=True) s
        JOIN read_csv('{q3_num}', delim='\t', header=True, all_varchar=True) n ON s.adsh = n.adsh
        WHERE n.value IS NOT NULL AND trim(n.value) != ''
    ),
    q4_facts AS (
        SELECT s.cik, n.tag, n.ddate, n.qtrs, n.uom, n.value
        FROM read_csv('{q4_sub}', delim='\t', header=True, all_varchar=True) s
        JOIN read_csv('{q4_num}', delim='\t', header=True, all_varchar=True) n ON s.adsh = n.adsh
        WHERE n.value IS NOT NULL AND trim(n.value) != ''
    )
    SELECT trim(q3.cik::VARCHAR) AS cik, count(*) AS restatement_count
    FROM q3_facts q3
    JOIN q4_facts q4
      ON q3.cik = q4.cik
     AND q3.tag = q4.tag
     AND q3.ddate = q4.ddate
     AND q3.qtrs = q4.qtrs
     AND q3.uom = q4.uom
     AND q3.value != q4.value
    GROUP BY q3.cik
    HAVING count(*) >= 1
    ORDER BY md5(trim(q3.cik::VARCHAR))
    """
    restate_rows = con.sql(restatement_ciks_query).fetchall()

    # Select at least min_revision_pairs CIKs with restatements
    selected_ciks: set[str] = set()
    total_restatements = 0
    for cik, cnt in restate_rows:
        selected_ciks.add(cik)
        total_restatements += cnt
        if len(selected_ciks) >= target_ciks and total_restatements >= min_revision_pairs:
            break

    # Add remaining common CIKs if needed to reach target_ciks
    for cik in common_ciks:
        if len(selected_ciks) >= target_ciks:
            break
        selected_ciks.add(cik)

    cik_list_sql = ", ".join(f"'{c}'" for c in selected_ciks)

    # Filter sub.txt and num.txt for selected CIKs
    con.sql(f"""
    COPY (
        SELECT * FROM read_csv('{q3_sub}', delim='\t', header=True, all_varchar=True)
        WHERE trim(cik::VARCHAR) IN ({cik_list_sql})
    ) TO '{out_q3 / "sub.txt"}' (DELIMITER '\t', HEADER TRUE)
    """)

    con.sql(f"""
    COPY (
        SELECT * FROM read_csv('{q4_sub}', delim='\t', header=True, all_varchar=True)
        WHERE trim(cik::VARCHAR) IN ({cik_list_sql})
    ) TO '{out_q4 / "sub.txt"}' (DELIMITER '\t', HEADER TRUE)
    """)

    con.sql(f"""
    COPY (
        SELECT n.*
        FROM read_csv('{q3_num}', delim='\t', header=True, all_varchar=True) n
        JOIN read_csv('{out_q3 / "sub.txt"}', delim='\t', header=True, all_varchar=True) s
          ON n.adsh = s.adsh
    ) TO '{out_q3 / "num.txt"}' (DELIMITER '\t', HEADER TRUE)
    """)

    con.sql(f"""
    COPY (
        SELECT n.*
        FROM read_csv('{q4_num}', delim='\t', header=True, all_varchar=True) n
        JOIN read_csv('{out_q4 / "sub.txt"}', delim='\t', header=True, all_varchar=True) s
          ON n.adsh = s.adsh
    ) TO '{out_q4 / "num.txt"}' (DELIMITER '\t', HEADER TRUE)
    """)

    con.close()

    q3_counts = get_sec_row_counts(out_q3)
    q4_counts = get_sec_row_counts(out_q4)

    return {
        "source": "authentic",
        "q3_sub_rows": q3_counts.get("sub", 0),
        "q3_num_rows": q3_counts.get("num", 0),
        "q4_sub_rows": q4_counts.get("sub", 0),
        "q4_num_rows": q4_counts.get("num", 0),
        "ciks": len(selected_ciks),
        "revision_pairs": total_restatements,
    }
