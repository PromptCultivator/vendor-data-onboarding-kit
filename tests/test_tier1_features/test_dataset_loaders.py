"""
Tier 1 Feature Tests: F-DAT-01..03 Dataset Loaders & Synthetic Generator.

Tests SEC DERA financial statement loader (sub.txt and num.txt relational join, 10-K/A restatements),
FRED/ALFRED macroeconomic vintage series loader (realtime_start as knowledge time, as-of filtering),
and scalable multi-profile synthetic generator (reproducibility, anomaly injection, truth manifest).
"""

from __future__ import annotations

import csv
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest


# Reference SEC Loader helper
def _load_sec_dera(sub_path: Path, num_path: Path, conn: duckdb.DuckDBPyConnection) -> pa.Table:
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
            CAST(n.value AS DOUBLE) as fact_value,
            s.adsh
        FROM read_csv('{sub_path}', delim='\t', header=true) s
        JOIN read_csv('{num_path}', delim='\t', header=true) n
          ON s.adsh = n.adsh
        ORDER BY cik, event_date, metric_tag
    """
    return conn.sql(query).arrow().read_all()


# Reference ALFRED Loader helper
def _load_alfred_series(csv_path: Path, conn: duckdb.DuckDBPyConnection) -> pa.Table:
    query = f"""
        SELECT
            CAST(realtime_start AS DATE) as vintage_date,
            CAST(date AS DATE) as observation_date,
            CAST(value AS DOUBLE) as metric_value
        FROM read_csv('{csv_path}', header=true)
        ORDER BY observation_date, vintage_date
    """
    return conn.sql(query).arrow().read_all()


# Reference Synthetic Generator helper
def _generate_synthetic_drop(
    seed: int = 42,
    num_entities: int = 10,
    days: int = 5,
    inject_negative_rate: float = 0.0,
) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    tickers = [f"SYM_{i:02d}" for i in range(num_entities)]
    records = []
    base_date = datetime(2023, 10, 2, 16, 0, 0, tzinfo=timezone.utc)

    rec_id = 1
    for d in range(days):
        ev_dt = base_date.replace(day=base_date.day + d)
        for sym in tickers:
            val = rng.uniform(100.0, 500.0)
            if inject_negative_rate > 0 and rng.random() < inject_negative_rate:
                val = -val

            records.append({
                "record_id": f"SYNTH_{rec_id:05d}",
                "entity_id": f"ENT_{sym}",
                "source_symbol": sym,
                "event_time": ev_dt.isoformat(),
                "knowledge_time": "2023-10-06T20:00:00Z",
                "metric": "CLOSE_PRICE",
                "value": round(val, 2),
                "unit": "USD",
                "vendor_id": "SYNTH_GEN",
                "dataset_id": "EQUITY_DAILY",
                "is_restatement": False,
                "revision_seq": 1,
            })
            rec_id += 1
    return records


# ============================================================================
# F-DAT-01: SEC DERA Financials Loader Tests
# ============================================================================

@pytest.mark.tier1
def test_sec_loader_sub_num_relational_join(fixture_paths, temp_duckdb_conn):
    """Verify joining sub.txt and num.txt extracts relational facts for filers."""
    table = _load_sec_dera(fixture_paths.sec_2023q3_sub, fixture_paths.sec_2023q3_num, temp_duckdb_conn)
    assert table.num_rows >= 16
    assert "cik" in table.column_names
    assert "metric_tag" in table.column_names
    assert "fact_value" in table.column_names


@pytest.mark.tier1
def test_sec_loader_cik_zero_padding(fixture_paths, temp_duckdb_conn):
    """Verify CIK values are formatted with standard 10-digit zero padding."""
    table = _load_sec_dera(fixture_paths.sec_2023q3_sub, fixture_paths.sec_2023q3_num, temp_duckdb_conn)
    ciks = set(table["cik"].to_pylist())
    # Apple (0000320193), Microsoft (0000789019), Tesla (0001318605)
    for cik in ciks:
        assert len(cik) == 10
        assert cik.isdigit()
    assert "0000320193" in ciks


@pytest.mark.tier1
def test_sec_loader_10k_and_10ka_filing_types(fixture_paths, temp_duckdb_conn):
    """Verify identification of original Form 10-K and amended Form 10-K/A filings."""
    t_orig = _load_sec_dera(fixture_paths.sec_2023q4_sub, fixture_paths.sec_2023q4_num, temp_duckdb_conn)
    forms_orig = set(t_orig["filing_form"].to_pylist())
    assert "10-K" in forms_orig

    t_amended = _load_sec_dera(fixture_paths.sec_2024q1_sub, fixture_paths.sec_2024q1_num, temp_duckdb_conn)
    forms_amended = set(t_amended["filing_form"].to_pylist())
    assert "10-K/A" in forms_amended


@pytest.mark.tier1
def test_sec_loader_restatement_detection(fixture_paths, temp_duckdb_conn):
    """Verify restatements between Form 10-K and Form 10-K/A for identical period end date."""
    t_orig = _load_sec_dera(fixture_paths.sec_2023q4_sub, fixture_paths.sec_2023q4_num, temp_duckdb_conn)
    t_amended = _load_sec_dera(fixture_paths.sec_2024q1_sub, fixture_paths.sec_2024q1_num, temp_duckdb_conn)

    temp_duckdb_conn.register("orig", t_orig)
    temp_duckdb_conn.register("amended", t_amended)

    # Join on CIK, metric_tag, and event_date to find differences
    diffs = temp_duckdb_conn.sql("""
        SELECT
            orig.cik, orig.metric_tag, orig.event_date,
            orig.fact_value as orig_val,
            amended.fact_value as amended_val,
            (amended.fact_value - orig.fact_value) as delta
        FROM orig
        JOIN amended
          ON orig.cik = amended.cik
         AND orig.metric_tag = amended.metric_tag
         AND orig.event_date = amended.event_date
        WHERE abs(orig.fact_value - amended.fact_value) > 1.0
    """).fetchall()

    # The restatement fixture plants revisions
    assert len(diffs) >= 1
    assert diffs[0][3] != diffs[0][4]  # orig_val != amended_val


@pytest.mark.tier1
def test_sec_loader_bitemporal_time_mapping(fixture_paths, temp_duckdb_conn):
    """Verify balance sheet period ddate maps to event_time and filed maps to knowledge_time."""
    table = _load_sec_dera(fixture_paths.sec_2023q3_sub, fixture_paths.sec_2023q3_num, temp_duckdb_conn)
    row = table.to_pylist()[0]
    assert row["event_date"] is not None
    assert row["knowledge_date"] is not None
    # Event date (period end) must precede knowledge date (filing date)
    assert row["event_date"] <= row["knowledge_date"]


@pytest.mark.tier1
def test_sec_loader_numeric_unit_parsing(fixture_paths, temp_duckdb_conn):
    """Verify unit of measure (USD, shares) is extracted properly."""
    table = _load_sec_dera(fixture_paths.sec_2023q3_sub, fixture_paths.sec_2023q3_num, temp_duckdb_conn)
    units = set(table["unit"].to_pylist())
    assert "USD" in units


# ============================================================================
# F-DAT-02: FRED/ALFRED Vintage Series Loader Tests
# ============================================================================

@pytest.mark.tier1
def test_alfred_loader_gdp_series(fixture_paths, temp_duckdb_conn):
    """Verify ALFRED Real GDP series parses observation dates and vintages."""
    table = _load_alfred_series(fixture_paths.alfred_gdpc1, temp_duckdb_conn)
    assert table.num_rows == 30
    dates = set(table["observation_date"].to_pylist())
    assert len(dates) == 8  # 8 distinct quarters


@pytest.mark.tier1
def test_alfred_loader_payems_series(fixture_paths, temp_duckdb_conn):
    """Verify ALFRED Nonfarm Payrolls series parses 12 monthly observations across 4 vintages."""
    table = _load_alfred_series(fixture_paths.alfred_payems, temp_duckdb_conn)
    assert table.num_rows == 48
    vintages = set(table["vintage_date"].to_pylist())
    assert len(vintages) >= 4


@pytest.mark.tier1
def test_alfred_loader_cpi_series(fixture_paths, temp_duckdb_conn):
    """Verify ALFRED CPI series loader parses 24 records."""
    table = _load_alfred_series(fixture_paths.alfred_cpiaucsl, temp_duckdb_conn)
    assert table.num_rows == 24


@pytest.mark.tier1
def test_alfred_loader_as_of_vintage_filtering(fixture_paths, temp_duckdb_conn):
    """Verify as-of vintage query strictly excludes subsequent releases (zero look-ahead)."""
    table = _load_alfred_series(fixture_paths.alfred_gdpc1, temp_duckdb_conn)
    temp_duckdb_conn.register("gdp", table)

    # Filter as of 2023-11-01
    as_of_table = temp_duckdb_conn.sql("""
        SELECT * FROM gdp WHERE vintage_date <= DATE '2023-11-01'
    """).arrow().read_all()

    for v_date in as_of_table["vintage_date"].to_pylist():
        # Compare as string or date object
        assert str(v_date) <= "2023-11-01"


@pytest.mark.tier1
def test_alfred_loader_revision_identification(fixture_paths, temp_duckdb_conn):
    """Verify detection of revised macroeconomic observations across successive vintages."""
    table = _load_alfred_series(fixture_paths.alfred_gdpc1, temp_duckdb_conn)
    temp_duckdb_conn.register("gdp", table)

    # Find observation dates with more than 1 distinct value across vintages
    revised = temp_duckdb_conn.sql("""
        SELECT observation_date, count(DISTINCT metric_value) as val_count
        FROM gdp
        GROUP BY observation_date
        HAVING val_count > 1
    """).fetchall()

    assert len(revised) >= 1  # Macro indicators undergo regular revisions


@pytest.mark.tier1
def test_alfred_loader_monotone_observation_dates(fixture_paths, temp_duckdb_conn):
    """Verify observation dates are valid sequential time-series dates."""
    table = _load_alfred_series(fixture_paths.alfred_payems, temp_duckdb_conn)
    dates = sorted(set(str(d) for d in table["observation_date"].to_pylist()))
    assert len(dates) == 12
    # First date precedes last date
    assert dates[0] < dates[-1]


# ============================================================================
# F-DAT-03: Synthetic Generator Tests
# ============================================================================

@pytest.mark.tier1
def test_synthetic_generator_deterministic_seed():
    """Verify generator with fixed seed produces identical records deterministically."""
    drop1 = _generate_synthetic_drop(seed=42, num_entities=5, days=3)
    drop2 = _generate_synthetic_drop(seed=42, num_entities=5, days=3)

    assert len(drop1) == len(drop2) == 15
    for r1, r2 in zip(drop1, drop2):
        assert r1["record_id"] == r2["record_id"]
        assert r1["value"] == r2["value"]


@pytest.mark.tier1
def test_synthetic_generator_equity_profile_schema():
    """Verify generated records conform to canonical equity fields."""
    drop = _generate_synthetic_drop(seed=123, num_entities=2, days=2)
    expected_keys = {"record_id", "entity_id", "source_symbol", "event_time", "knowledge_time", "metric", "value", "unit"}
    for r in drop:
        assert expected_keys.issubset(r.keys())
        assert r["unit"] == "USD"
        assert r["metric"] == "CLOSE_PRICE"


@pytest.mark.tier1
def test_synthetic_generator_anomaly_injection_profile():
    """Verify injected anomaly rate produces expected proportion of negative values."""
    drop = _generate_synthetic_drop(seed=999, num_entities=20, days=10, inject_negative_rate=0.25)
    assert len(drop) == 200

    negatives = [r for r in drop if r["value"] < 0]
    # Expected ~25% (around 50 out of 200)
    assert 30 <= len(negatives) <= 70


@pytest.mark.tier1
def test_synthetic_generator_planted_restatements_count(fixture_paths, temp_duckdb_conn):
    """Verify planted restatements count in bundled fixtures equals 10."""
    res = temp_duckdb_conn.sql(f"""
        SELECT count(*)
        FROM read_parquet('{fixture_paths.truth_manifest_parquet}')
        WHERE drop_id = 'DROP_002' AND is_planted_restatement = true
    """).fetchall()[0][0]
    assert res == 10


@pytest.mark.tier1
def test_synthetic_generator_truth_manifest_reconciliation(fixture_paths, temp_duckdb_conn):
    """Verify truth manifest registers all mutation categories for testing."""
    categories = temp_duckdb_conn.sql(f"""
        SELECT DISTINCT mutation_type
        FROM read_parquet('{fixture_paths.truth_manifest_parquet}')
    """).fetchall()
    cats = {c[0] for c in categories}
    expected_cats = {"BASELINE", "UNCHANGED", "RESTATED", "NEW", "DELETED", "ANOMALOUS"}
    assert expected_cats.issubset(cats)
