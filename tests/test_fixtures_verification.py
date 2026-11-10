"""
Sanity and verification tests for conftest.py, test fixtures, schemas, and PIT invariants.
"""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
import pytest

try:
    import duckdb
except ImportError:
    duckdb = None

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    pa = None
    pq = None


def test_fixture_paths_exist(fixture_paths):
    """Verify that every registered fixture file exists on disk."""
    assert fixture_paths.sec_dir.is_dir()
    assert fixture_paths.sec_2023q3_sub.is_file()
    assert fixture_paths.sec_2023q3_num.is_file()
    assert fixture_paths.sec_2023q4_sub.is_file()
    assert fixture_paths.sec_2023q4_num.is_file()
    assert fixture_paths.sec_2024q1_sub.is_file()
    assert fixture_paths.sec_2024q1_num.is_file()
    assert fixture_paths.sec_readme.is_file()

    assert fixture_paths.alfred_dir.is_dir()
    assert fixture_paths.alfred_gdpc1.is_file()
    assert fixture_paths.alfred_payems.is_file()
    assert fixture_paths.alfred_cpiaucsl.is_file()
    assert fixture_paths.alfred_readme.is_file()

    assert fixture_paths.synth_dir.is_dir()
    assert fixture_paths.drop_001_csv.is_file()
    assert fixture_paths.drop_001_parquet.is_file()
    assert fixture_paths.drop_001_json.is_file()
    assert fixture_paths.drop_002_csv.is_file()
    assert fixture_paths.drop_002_parquet.is_file()
    assert fixture_paths.drop_003_csv.is_file()
    assert fixture_paths.drop_003_jsonl.is_file()
    assert fixture_paths.schema_baseline.is_file()
    assert fixture_paths.schema_drift_breaking.is_file()
    assert fixture_paths.schema_drift_warning.is_file()
    assert fixture_paths.truth_manifest_parquet.is_file()
    assert fixture_paths.synth_readme.is_file()


def test_sec_fixtures_structure(fixture_paths, temp_duckdb_conn):
    """Verify SEC DERA fixtures parse as tab-delimited tables with expected joins."""
    for q in ["2023q3", "2023q4", "2024q1_restatement"]:
        sub_path = getattr(fixture_paths, f"sec_{q[:6] if 'restatement' not in q else '2024q1'}_sub")
        num_path = getattr(fixture_paths, f"sec_{q[:6] if 'restatement' not in q else '2024q1'}_num")

        res = temp_duckdb_conn.execute(
            f"SELECT count(distinct s.adsh), count(n.tag) "
            f"FROM read_csv('{sub_path}', delim='\t', header=true) s "
            f"JOIN read_csv('{num_path}', delim='\t', header=true) n "
            f"ON s.adsh = n.adsh"
        ).fetchone()

        assert res[0] == 3, f"Expected 3 filers in {q}, got {res[0]}"
        assert res[1] >= 16, f"Expected >= 16 facts in {q}, got {res[1]}"


def test_alfred_fixtures_queryable(fixture_paths, temp_duckdb_conn):
    """Verify ALFRED vintage series parse and have bitemporal structure."""
    res_gdp = temp_duckdb_conn.execute(
        f"SELECT count(*), min(realtime_start), max(realtime_start), count(distinct date) "
        f"FROM read_csv('{fixture_paths.alfred_gdpc1}', header=true)"
    ).fetchone()
    assert res_gdp[0] == 30
    assert res_gdp[3] == 8  # 8 quarters

    res_payems = temp_duckdb_conn.execute(
        f"SELECT count(*), count(distinct date) "
        f"FROM read_csv('{fixture_paths.alfred_payems}', header=true)"
    ).fetchone()
    assert res_payems[0] == 48
    assert res_payems[1] == 12  # 12 months

    res_cpi = temp_duckdb_conn.execute(
        f"SELECT count(*), count(distinct date) "
        f"FROM read_csv('{fixture_paths.alfred_cpiaucsl}', header=true)"
    ).fetchone()
    assert res_cpi[0] == 24
    assert res_cpi[1] == 12  # 12 months


def test_synthetic_drops_and_truth_manifest(fixture_paths, temp_duckdb_conn):
    """Verify synthetic drops row counts and truth manifest reconciliation."""
    d1_count = temp_duckdb_conn.execute(
        f"SELECT count(*) FROM read_parquet('{fixture_paths.drop_001_parquet}')"
    ).fetchone()[0]
    assert d1_count == 60

    d2_count = temp_duckdb_conn.execute(
        f"SELECT count(*) FROM read_parquet('{fixture_paths.drop_002_parquet}')"
    ).fetchone()[0]
    assert d2_count == 65

    # Check truth manifest mutations
    mutations = dict(
        temp_duckdb_conn.execute(
            f"SELECT mutation_type, count(*) "
            f"FROM read_parquet('{fixture_paths.truth_manifest_parquet}') "
            f"WHERE drop_id = 'DROP_002' "
            f"GROUP BY mutation_type"
        ).fetchall()
    )

    assert mutations["RESTATED"] == 10
    assert mutations["NEW"] == 10
    assert mutations["DELETED"] == 5
    assert mutations["UNCHANGED"] == 45


def test_pit_invariance_helper(pit_invariance_assert, sample_canonical_records):
    """Verify that PIT invariance assertion passes when H_1 == H_2, and fails on discrepancy."""
    h1 = sample_canonical_records
    h2 = list(sample_canonical_records)

    # Invariant holds
    assert pit_invariance_assert(h1, h2) is True

    # Mutate h2 to simulate look-ahead leak or unexpected restatement
    leak_record = type(h1[0])(
        entity_id="ENT_AAPL",
        event_time=h1[0].event_time,
        knowledge_time=h1[0].knowledge_time,
        value=9999.99,  # Mutated value
        unit=h1[0].unit,
        source_symbol=h1[0].source_symbol,
        source_symbol_type=h1[0].source_symbol_type,
        vendor_id=h1[0].vendor_id,
        dataset_id=h1[0].dataset_id,
        drop_id="DROP_002",
        is_restatement=True,
        revision_seq=2,
    )
    h2_leaked = [leak_record, h1[1], h1[2]]

    with pytest.raises(AssertionError, match="PIT Invariance Violation"):
        pit_invariance_assert(h1, h2_leaked)
