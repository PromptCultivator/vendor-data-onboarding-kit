"""
Tier 2 Boundary Tests: Challenger 1 Suite for Milestone 5.

Adversarial stress-testing, boundary condition analysis, and mutation testing
for SEC DERA loader, FRED/ALFRED vintage series loader, and Synthetic Market Data Generator:
1. SEC DERA Loader Stress:
   - Empty files (0-byte), headers-only files, mismatched delimiters, missing columns,
     accession numbers with whitespace padding, unmatched sub/num records, invalid dates.
   - Restatement detection for Form 10-K vs 10-K/A and 10-Q/A amendments.
2. FRED/ALFRED Vintage Series Stress:
   - Missing values ('.' / 'NA' / empty strings), extreme dates (1900, 2099), microsecond timestamps.
   - Zero look-ahead bias validation under rapid successive vintages.
   - Point-in-time filtering via as_of with latest_only=False and latest_only=True across date/datetime/string formats.
3. Synthetic Generator Stress & Reproducibility:
   - Boundary parameters (inject_negative_rate=1.0, num_entities=1, days=0, days=1).
   - Deterministic seed exact binary reproducibility across repeated runs.
   - Multi-drop truth manifest reconciliation.
"""

from __future__ import annotations

import tempfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pytest

from vendor_onboarding.datasets.fred import (
    AlfredVintageLoader,
    as_of,
    load_alfred_raw,
    load_alfred_series,
)
from vendor_onboarding.datasets.sec import (
    SECDeraLoader,
    load_sec_dera,
    load_sec_dera_facts,
)
from vendor_onboarding.datasets.synthetic import (
    SyntheticGenerator,
    generate_multi_drop_suite,
    generate_synthetic_drop,
)

pytestmark = [pytest.mark.tier2]


# ============================================================================
# 1. SEC DERA Loader Adversarial Stress Tests
# ============================================================================


def test_sec_loader_empty_files_fail_gracefully(tmp_path: Path) -> None:
    """Empty (0-byte) sub.txt and num.txt files should raise clear DuckDB binder errors."""
    sub_empty = tmp_path / "sub_empty.txt"
    num_empty = tmp_path / "num_empty.txt"
    sub_empty.touch()
    num_empty.touch()

    with pytest.raises(duckdb.Error) as exc_info:
        load_sec_dera(sub_empty, num_empty)
    assert "adsh" in str(exc_info.value) or "Binder Error" in str(exc_info.value)


def test_sec_loader_headers_only_returns_empty_table_with_canonical_schema(tmp_path: Path) -> None:
    """Headers-only TSV files must parse cleanly and return an empty PyArrow Table with full schema."""
    sub_hdr = tmp_path / "sub_hdr.txt"
    num_hdr = tmp_path / "num_hdr.txt"
    sub_hdr.write_text(
        "adsh\tcik\tname\tsic\tcountryba\tstprba\tcityba\tzipba\tbas1\tbas2\tbaph\tcountryma\tstprma\tcityma\tzipma\tmas1\tmas2\tcountryinc\tstprinc\tein\tformer\tchanged\tafs\twksi\tfye\tform\tperiod\tfy\tfp\tfiled\taccepted\tprevrpt\tdetail\tinstance\tnciks\taccts\n"
    )
    num_hdr.write_text(
        "adsh\ttag\tversion\tddate\tqtrs\tuom\tdimhash\tcoreg\tdurp\tdats\tdcube\tdval\tfootnote\tfootlen\tcontext\tvalue\n"
    )

    tbl_raw = load_sec_dera(sub_hdr, num_hdr)
    assert tbl_raw.num_rows == 0
    assert "cik" in tbl_raw.column_names
    assert "adsh" in tbl_raw.column_names

    tbl_facts = load_sec_dera_facts(sub_hdr, num_hdr)
    assert tbl_facts.num_rows == 0
    expected_cols = [
        "record_id",
        "entity_id",
        "source_symbol",
        "source_symbol_type",
        "company_name",
        "filing_form",
        "adsh",
        "event_time",
        "knowledge_time",
        "metric",
        "value",
        "unit",
        "vendor_id",
        "dataset_id",
        "drop_id",
        "is_restatement",
        "revision_seq",
    ]
    for col in expected_cols:
        assert col in tbl_facts.column_names


def test_sec_loader_mismatched_delimiters_raise_error(tmp_path: Path) -> None:
    """Comma-separated CSV files instead of tab-separated TSV must raise a column missing error."""
    sub_csv = tmp_path / "sub.csv"
    num_csv = tmp_path / "num.csv"
    sub_csv.write_text("adsh,cik,name,form,filed\n0000320193-23-000106,320193,Apple Inc.,10-K,20231103\n")
    num_csv.write_text("adsh,tag,version,ddate,uom,value\n0000320193-23-000106,Assets,us-gaap/2023,20230930,USD,352583000000\n")

    with pytest.raises(duckdb.Error):
        load_sec_dera(sub_csv, num_csv)


def test_sec_loader_missing_columns_raise_error(tmp_path: Path) -> None:
    """TSV files missing mandatory relational columns must fail with a clear schema error."""
    sub_bad = tmp_path / "sub_bad.txt"
    num_bad = tmp_path / "num_bad.txt"
    # sub missing 'filed'
    sub_bad.write_text("adsh\tcik\tname\tform\n0000320193-23-000106\t320193\tApple Inc.\t10-K\n")
    num_bad.write_text("adsh\ttag\tversion\tddate\tuom\tvalue\n0000320193-23-000106\tAssets\tus-gaap/2023\t20230930\tUSD\t352583000000\n")

    with pytest.raises(duckdb.Error) as exc_info:
        load_sec_dera(sub_bad, num_bad)
    assert "filed" in str(exc_info.value) or "Binder Error" in str(exc_info.value)


def test_sec_loader_accession_whitespace_trimming(tmp_path: Path) -> None:
    """Accession numbers (adsh) with leading or trailing whitespace must be properly joined and trimmed."""
    sub = tmp_path / "sub_ws.txt"
    num = tmp_path / "num_ws.txt"
    sub.write_text("adsh\tcik\tname\tform\tfiled\n  0000320193-23-000106 \t320193\tApple Inc.\t10-K\t20231103\n")
    num.write_text("adsh\ttag\tversion\tddate\tuom\tvalue\n0000320193-23-000106  \tAssets\tus-gaap/2023\t20230930\tUSD\t352583000000\n")

    tbl = load_sec_dera(sub, num)
    assert tbl.num_rows == 1
    assert tbl["adsh"].to_pylist()[0] == "0000320193-23-000106"
    assert tbl["cik"].to_pylist()[0] == "0000320193"

    facts = load_sec_dera_facts(sub, num)
    assert facts.num_rows == 1
    assert facts["adsh"].to_pylist()[0] == "0000320193-23-000106"
    assert facts["source_symbol"].to_pylist()[0] == "0000320193"


def test_sec_loader_unmatched_sub_and_num_records(tmp_path: Path) -> None:
    """Unmatched records in sub or num must not crash the loader and must only join matching adsh."""
    sub = tmp_path / "sub_unmatched.txt"
    num = tmp_path / "num_unmatched.txt"
    sub.write_text(
        "adsh\tcik\tname\tform\tfiled\n"
        "ADSH_MATCH\t320193\tApple Inc.\t10-K\t20231103\n"
        "ADSH_ORPHAN_SUB\t789019\tMicrosoft\t10-K\t20231024\n"
    )
    num.write_text(
        "adsh\ttag\tversion\tddate\tuom\tvalue\n"
        "ADSH_MATCH\tAssets\tus-gaap/2023\t20230930\tUSD\t352583000000\n"
        "ADSH_ORPHAN_NUM\tRevenues\tus-gaap/2023\t20230930\tUSD\t1000000000\n"
    )

    tbl = load_sec_dera(sub, num)
    assert tbl.num_rows == 1
    assert tbl["adsh"].to_pylist()[0] == "ADSH_MATCH"


def test_sec_loader_invalid_dates_raise_error(tmp_path: Path) -> None:
    """Non-existent or corrupt date strings must raise a conversion exception."""
    sub = tmp_path / "sub_baddate.txt"
    num = tmp_path / "num_baddate.txt"
    sub.write_text("adsh\tcik\tname\tform\tfiled\n0001\t320193\tApple Inc.\t10-K\t20239999\n")
    num.write_text("adsh\ttag\tversion\tddate\tuom\tvalue\n0001\tAssets\tus-gaap/2023\t20230930\tUSD\t100\n")

    with pytest.raises(duckdb.Error):
        load_sec_dera(sub, num)


def test_sec_loader_restatements_comparative_differentiation(tmp_path: Path) -> None:
    """Form 10-K vs 10-K/A comparative restatements must be flagged and revision_seq incremented."""
    sub = tmp_path / "sub_rest.txt"
    num = tmp_path / "num_rest.txt"
    sub.write_text(
        "adsh\tcik\tname\tform\tfiled\n"
        "0000320193-23-000106\t320193\tApple Inc.\t10-K\t20231103\n"
        "0000320193-24-000002\t320193\tApple Inc.\t10-K/A\t20240115\n"
    )
    num.write_text(
        "adsh\ttag\tversion\tddate\tuom\tvalue\n"
        "0000320193-23-000106\tRevenues\tus-gaap/2023\t20230930\tUSD\t383285000000\n"
        "0000320193-24-000002\tRevenues\tus-gaap/2023\t20230930\tUSD\t381500000000\n"
    )

    facts = load_sec_dera_facts(sub, num)
    assert facts.num_rows == 2

    records = facts.to_pylist()
    orig = next(r for r in records if r["filing_form"] == "10-K")
    amended = next(r for r in records if r["filing_form"] == "10-K/A")

    assert orig["is_restatement"] is False
    assert orig["revision_seq"] == 1
    assert orig["value"] == 383285000000.0

    assert amended["is_restatement"] is True
    assert amended["revision_seq"] == 2
    assert amended["value"] == 381500000000.0
    assert orig["event_time"] == amended["event_time"]
    assert amended["knowledge_time"] > orig["knowledge_time"]


def test_sec_loader_class_interface_and_symbology(tmp_path: Path) -> None:
    """SECDeraLoader class instance must support custom connection, drop_id, and symbology resolution."""
    sub = tmp_path / "sub_cls.txt"
    num = tmp_path / "num_cls.txt"
    sub.write_text("adsh\tcik\tname\tform\tfiled\n0001\t320193\tApple Inc.\t10-K\t20231103\n")
    num.write_text("adsh\ttag\tversion\tddate\tuom\tvalue\n0001\tAssets\tus-gaap/2023\t20230930\tUSD\t1000\n")

    class MockSymbologyMaster:
        def resolve(self, symbol: str, symbol_type: str, as_of: Any = None, vendor_id: str = "") -> str:
            return f"CANONICAL_{symbol}"

    conn = duckdb.connect()
    loader = SECDeraLoader(conn=conn, symbology_master=MockSymbologyMaster())

    raw = loader.load_raw(sub, num)
    assert raw.num_rows == 1

    facts = loader.load_facts(sub, num, drop_id="DROP_SEC_CUSTOM", vendor_id="SEC_TEST")
    assert facts.num_rows == 1
    row = facts.to_pylist()[0]
    assert row["entity_id"] == "CANONICAL_0000320193"
    assert row["drop_id"] == "DROP_SEC_CUSTOM"
    assert row["vendor_id"] == "SEC_TEST"


# ============================================================================
# 2. FRED/ALFRED Vintage Series Stress Tests
# ============================================================================


def test_alfred_loader_missing_values_dot_and_na(tmp_path: Path) -> None:
    """FRED standard missing value representations ('.' and 'NA') must parse gracefully to NULL."""
    csv_file = tmp_path / "fred_missing.csv"
    csv_file.write_text(
        "realtime_start,date,value\n"
        "2022-04-28,2022-01-01,19727.918\n"
        "2022-04-28,2022-02-01,.\n"
        "2022-04-28,2022-03-01,NA\n"
        "2022-05-26,2022-02-01,19702.404\n"
    )

    tbl_raw = load_alfred_raw(csv_file)
    assert tbl_raw.num_rows == 4
    # Note: ORDER BY observation_date, vintage_date puts 2022-02-01 before 2022-03-01
    records_raw = tbl_raw.to_pylist()
    assert records_raw[0]["metric_value"] == 19727.918
    assert records_raw[1]["metric_value"] is None  # 2022-02-01, 2022-04-28 (.)
    assert records_raw[2]["metric_value"] == 19702.404  # 2022-02-01, 2022-05-26
    assert records_raw[3]["metric_value"] is None  # 2022-03-01, 2022-04-28 (NA)

    tbl_series = load_alfred_series(csv_file, series_id="GDPC1")
    assert tbl_series.num_rows == 4
    records_series = tbl_series.to_pylist()
    assert records_series[1]["value"] is None
    assert records_series[3]["value"] is None
    # Initial vintages must not be restatements
    assert records_series[0]["is_restatement"] is False
    assert records_series[0]["revision_seq"] == 1
    assert records_series[1]["is_restatement"] is False
    assert records_series[1]["revision_seq"] == 1
    # Subsequent revision on 2022-02-01 must be flagged as restatement
    assert records_series[2]["is_restatement"] is True
    assert records_series[2]["revision_seq"] == 2


def test_alfred_loader_extreme_dates(tmp_path: Path) -> None:
    """ALFRED series spanning century extremes (1900 to 2099) must be parsed accurately."""
    csv_file = tmp_path / "fred_extreme_dates.csv"
    csv_file.write_text(
        "realtime_start,date,value\n"
        "1900-01-01,1900-01-01,50.0\n"
        "2099-12-31,2099-10-01,99999.9\n"
    )

    tbl = load_alfred_raw(csv_file)
    assert tbl.num_rows == 2
    dates = [str(d) for d in tbl["observation_date"].to_pylist()]
    assert dates == ["1900-01-01", "2099-10-01"]


def test_alfred_as_of_microsecond_timestamps() -> None:
    """as_of query with microsecond resolution must strictly separate vintages published sub-second apart."""
    schema = pa.schema([
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
    ])

    ev = datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC)
    records = [
        {
            "record_id": "REC_V1",
            "entity_id": "GDPC1",
            "source_symbol": "GDPC1",
            "source_symbol_type": "SERIES_ID",
            "event_time": ev,
            "knowledge_time": datetime(2023, 4, 28, 12, 0, 0, 100000, tzinfo=UTC),
            "metric": "GDPC1",
            "value": 100.0,
            "unit": "USD_BILLIONS",
            "vendor_id": "FRED",
            "dataset_id": "MACRO",
            "drop_id": "DROP_1",
            "is_restatement": False,
            "revision_seq": 1,
        },
        {
            "record_id": "REC_V2",
            "entity_id": "GDPC1",
            "source_symbol": "GDPC1",
            "source_symbol_type": "SERIES_ID",
            "event_time": ev,
            "knowledge_time": datetime(2023, 4, 28, 12, 0, 0, 500000, tzinfo=UTC),
            "metric": "GDPC1",
            "value": 105.0,
            "unit": "USD_BILLIONS",
            "vendor_id": "FRED",
            "dataset_id": "MACRO",
            "drop_id": "DROP_2",
            "is_restatement": True,
            "revision_seq": 2,
        },
    ]
    tbl = pa.Table.from_pylist(records, schema=schema)

    # Query at 300,000 us (between V1 and V2)
    as_of_mid = datetime(2023, 4, 28, 12, 0, 0, 300000, tzinfo=UTC)
    res_mid = as_of(tbl, as_of_mid, latest_only=True)
    assert res_mid.num_rows == 1
    assert res_mid["value"].to_pylist()[0] == 100.0
    assert res_mid["revision_seq"].to_pylist()[0] == 1

    # Query at 600,000 us (after V2)
    as_of_after = datetime(2023, 4, 28, 12, 0, 0, 600000, tzinfo=UTC)
    res_after = as_of(tbl, as_of_after, latest_only=True)
    assert res_after.num_rows == 1
    assert res_after["value"].to_pylist()[0] == 105.0
    assert res_after["revision_seq"].to_pylist()[0] == 2


def test_alfred_as_of_zero_lookahead_rapid_successive_vintages() -> None:
    """Rapid successive vintages must exhibit zero look-ahead bias across all query horizons."""
    ev = datetime(2022, 1, 1, 0, 0, 0, tzinfo=UTC)
    vintages = [
        (datetime(2022, 4, 28, tzinfo=UTC), 19727.918, 1),
        (datetime(2022, 5, 26, tzinfo=UTC), 19702.404, 2),
        (datetime(2022, 6, 29, tzinfo=UTC), 19699.465, 3),
        (datetime(2022, 9, 29, tzinfo=UTC), 19895.271, 4),
        (datetime(2023, 9, 28, tzinfo=UTC), 21738.871, 5),
    ]

    records = [
        {
            "record_id": f"GDPC1_20220101_V{seq}",
            "entity_id": "GDPC1",
            "source_symbol": "GDPC1",
            "source_symbol_type": "SERIES_ID",
            "event_time": ev,
            "knowledge_time": kt,
            "metric": "GDPC1",
            "value": val,
            "unit": "USD_BILLIONS",
            "vendor_id": "FRED_ALFRED",
            "dataset_id": "MACRO_ALFRED_VINTAGES",
            "drop_id": f"VINTAGE_{kt.strftime('%Y%m%d')}",
            "is_restatement": seq > 1,
            "revision_seq": seq,
        }
        for kt, val, seq in vintages
    ]
    tbl = pa.Table.from_pylist(records)

    # 1. Before any vintage is released
    res_pre = as_of(tbl, datetime(2022, 4, 20, tzinfo=UTC), latest_only=True)
    assert res_pre.num_rows == 0

    # 2. As of Advance estimate
    res_v1 = as_of(tbl, datetime(2022, 5, 4, tzinfo=UTC), latest_only=True)
    assert res_v1.num_rows == 1
    assert res_v1["value"].to_pylist()[0] == 19727.918

    # 3. As of Second estimate
    res_v2 = as_of(tbl, datetime(2022, 6, 15, tzinfo=UTC), latest_only=True)
    assert res_v2.num_rows == 1
    assert res_v2["value"].to_pylist()[0] == 19702.404

    # 4. As of Third estimate
    res_v3 = as_of(tbl, datetime(2022, 7, 27, tzinfo=UTC), latest_only=True)
    assert res_v3.num_rows == 1
    assert res_v3["value"].to_pylist()[0] == 19699.465

    # 5. Full history when latest_only=False
    res_history = as_of(tbl, datetime(2022, 7, 27, tzinfo=UTC), latest_only=False)
    assert res_history.num_rows == 3
    assert res_history["value"].to_pylist() == [19727.918, 19702.404, 19699.465]


def test_alfred_as_of_latest_only_and_input_types() -> None:
    """as_of function must support date objects, datetime objects, and strings with latest_only toggles."""
    csv_data = [
        {"vintage_date": date(2022, 4, 28), "observation_date": date(2022, 1, 1), "metric_value": 100.0},
        {"vintage_date": date(2022, 5, 26), "observation_date": date(2022, 1, 1), "metric_value": 105.0},
        {"vintage_date": date(2022, 4, 28), "observation_date": date(2022, 4, 1), "metric_value": 200.0},
    ]
    raw_tbl = pa.Table.from_pylist(csv_data)

    # Raw vintage table as-of: date string, latest_only=True
    filtered_latest = as_of(raw_tbl, "2022-05-30", latest_only=True)
    assert filtered_latest.num_rows == 2
    # 2022-01-01 should have latest value 105.0
    r_jan = [r for r in filtered_latest.to_pylist() if str(r["observation_date"]) == "2022-01-01"][0]
    assert r_jan["metric_value"] == 105.0

    # Raw vintage table as-of: date object, latest_only=False
    filtered_all = as_of(raw_tbl, date(2022, 5, 30), latest_only=False)
    assert filtered_all.num_rows == 3

    # Invalid table without temporal columns raises ValueError
    bad_tbl = pa.Table.from_pydict({"foo": [1, 2, 3]})
    with pytest.raises(ValueError) as exc:
        as_of(bad_tbl, "2022-05-30")
    assert "Found: ['foo']" in str(exc.value)


# ============================================================================
# 3. Synthetic Generator Stress & Mutation Testing
# ============================================================================


def test_synthetic_generator_edge_parameters() -> None:
    """Stress test boundary parameters: inject_negative_rate=1.0, num_entities=1, days=0, days=1."""
    # 1. 100% negative injection
    drop_neg = generate_synthetic_drop(seed=42, num_entities=10, days=5, inject_negative_rate=1.0)
    assert len(drop_neg) == 50
    for r in drop_neg:
        assert r["value"] < 0, f"Expected negative value, got {r['value']}"

    # 2. 0% negative injection
    drop_pos = generate_synthetic_drop(seed=42, num_entities=10, days=5, inject_negative_rate=0.0)
    assert len(drop_pos) == 50
    for r in drop_pos:
        assert r["value"] > 0, f"Expected positive value, got {r['value']}"

    # 3. Single entity
    drop_single = generate_synthetic_drop(seed=42, num_entities=1, days=3)
    assert len(drop_single) == 3
    entities = {r["entity_id"] for r in drop_single}
    assert entities == {"ENT_SYM_00"}

    # 4. Zero days (empty drop)
    drop_zero = generate_synthetic_drop(seed=42, num_entities=10, days=0)
    assert len(drop_zero) == 0

    # 5. Single day
    drop_oneday = generate_synthetic_drop(seed=42, num_entities=5, days=1)
    assert len(drop_oneday) == 5


def test_synthetic_generator_seed_determinism_and_binary_reproducibility() -> None:
    """Verify exact binary reproducibility across repeated runs with identical seed."""
    gen = SyntheticGenerator(seed=777)

    # 1. Repeated dict generation
    run1 = gen.generate_drop(seed=777, num_entities=20, days=10)
    run2 = gen.generate_drop(seed=777, num_entities=20, days=10)
    assert run1 == run2

    # 2. Repeated PyArrow Table generation (binary equality)
    tbl1 = gen.generate_drop_table(seed=777, num_entities=20, days=10)
    tbl2 = gen.generate_drop_table(seed=777, num_entities=20, days=10)
    assert tbl1.equals(tbl2)
    assert tbl1.schema == tbl2.schema
    assert tbl1.num_rows == 200

    # 3. Multi-drop suite determinism
    suite1 = gen.generate_multi_drop_suite()
    suite2 = gen.generate_multi_drop_suite()
    assert suite1["drop_001"].equals(suite2["drop_001"])
    assert suite1["drop_002"].equals(suite2["drop_002"])
    assert suite1["drop_003"].equals(suite2["drop_003"])
    assert suite1["truth_manifest"].equals(suite2["truth_manifest"])


def test_synthetic_generator_high_volume_stress() -> None:
    """Generate 10,000 synthetic records and verify execution completes promptly with exact schema."""
    gen = SyntheticGenerator(seed=12345)
    tbl = gen.generate_drop_table(num_entities=100, days=100)
    assert tbl.num_rows == 10000
    assert len(tbl.column_names) == 14
    assert set(tbl["metric"].to_pylist()) == {"CLOSE_PRICE"}
    assert set(tbl["unit"].to_pylist()) == {"USD"}


def test_synthetic_generator_multi_drop_suite_manifest_reconciliation(tmp_path: Path) -> None:
    """Verify complete truth manifest reconciliation across Drop 1, Drop 2, and Drop 3."""
    suite = generate_multi_drop_suite(output_dir=tmp_path, base_seed=42)

    # Verify parquet files written to output directory
    assert (tmp_path / "drop_001_baseline.parquet").exists()
    assert (tmp_path / "drop_002_restatements.parquet").exists()
    assert (tmp_path / "drop_003_anomalies.parquet").exists()
    assert (tmp_path / "truth_manifest.parquet").exists()

    # Drop 1: 60 records (50 price + 10 volume)
    d1 = suite["drop_001"]
    assert d1.num_rows == 60
    metrics_d1 = set(d1["metric"].to_pylist())
    assert metrics_d1 == {"CLOSE_PRICE", "VOLUME"}

    # Drop 2: 65 records (45 unchanged + 10 restatements + 10 new)
    d2 = suite["drop_002"]
    assert d2.num_rows == 65
    restated_d2 = [r for r in d2.to_pylist() if r["is_restatement"]]
    assert len(restated_d2) == 10

    # Drop 3: 21 records (11 controls + 10 anomalies)
    d3 = suite["drop_003"]
    assert d3.num_rows == 21

    # Truth Manifest: 151 total records
    manifest = suite["truth_manifest"]
    assert manifest.num_rows == 151
    manifest_rows = manifest.to_pylist()

    mutation_types = {r["mutation_type"] for r in manifest_rows}
    assert mutation_types == {"BASELINE", "UNCHANGED", "DELETED", "RESTATED", "NEW", "ANOMALOUS"}

    # Planted anomalies count = 10, each with recognized rule violation
    anomalies = [r for r in manifest_rows if r["is_planted_anomaly"]]
    assert len(anomalies) == 10
    violations = {r["expected_rule_violation"] for r in anomalies}
    assert violations == {"F-VAL-01", "F-VAL-02", "F-VAL-04", "F-VAL-05", "F-SYM-03"}
