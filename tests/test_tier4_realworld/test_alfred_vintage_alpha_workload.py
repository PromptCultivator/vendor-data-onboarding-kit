"""
Tier 4 Real-World Workload Scenarios: Scenario 4B - ALFRED Macroeconomic Series & Zero Look-Ahead Bias.

Per TEST_INFRA.md and PROJECT.md §Scenario 4B:
Authentic Federal Reserve Economic Data (FRED/ALFRED) vintage time-series workload:
- Series:
  - Real Gross Domestic Product (GDPC1): tests/fixtures/alfred/GDPC1_vintages.csv
  - Total Nonfarm Payroll Employment (PAYEMS): tests/fixtures/alfred/PAYEMS_vintages.csv
  - Consumer Price Index for All Urban Consumers (CPIAUCSL): tests/fixtures/alfred/CPIAUCSL_vintages.csv

Key invariants tested:
1. Simulated quantitative macro trading strategy running on historical Federal Open Market
   Committee (FOMC) announcement dates (2022-05-04, 2022-06-15, 2022-07-27, 2023-11-01).
2. Point-in-time (as-of) queries enforce strictly 0 look-ahead bias:
   - On 2022-05-04, only the Advance GDP estimate (19727.918) is visible.
   - Future revisions (19702.404 on 2022-05-26 and 21738.871 on 2023-09-28) are completely invisible.
   - A naive non-PIT model would introduce +10.19% alpha contamination.
   - Mathematical invariance check: H_1 == H_2 before and after loading 2023/2024 vintages.
3. Monthly Nonfarm Payrolls (PAYEMS) revision velocity tracking:
   - Multi-drop diffing detects 100.00% of monthly revisions (Advance -> Second -> Third -> Benchmark).
   - Full vintage audit trail trajectory monotonicity and completeness.
4. Multi-series point-in-time co-vintage feature vector construction (GDP, PAYEMS, CPI).
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from test_tier4_realworld.conftest import (
    BitemporalParquetStore,
    ReferenceDiffEngine,
    VectorizedValidationEngine,
    assert_point_in_time_invariance,
    load_alfred_series,
)


@pytest.mark.tier4
@pytest.mark.bitemporal
class TestAlfredVintageAlphaWorkload:
    """Scenario 4B: Real ALFRED macroeconomic series & zero look-ahead bias backtest."""

    def test_fomc_backtest_strict_zero_lookahead_bias_simulation(
        self,
        tmp_path: Path,
        tier4_duckdb: duckdb.DuckDBPyConnection,
    ) -> None:
        """
        Scenario 4B.1: FOMC Meeting Date Backtest Simulation with Real GDP (GDPC1):
        - Observation: Q1 2022 Real GDP (date = 2022-01-01).
        - Succession of historical vintages (realtime_start):
            1. 2022-04-28 (Advance Estimate): 19,727.918 Billion
            2. 2022-05-26 (Second Estimate):  19,702.404 Billion
            3. 2022-06-29 (Third Estimate):   19,699.465 Billion
            4. 2022-09-29 (Annual Revision):  19,895.271 Billion
            5. 2023-09-28 (Benchmark Revision): 21,738.871 Billion
        - Verify as-of queries at historical FOMC decision dates:
            FOMC 2022-05-04 -> MUST return exactly 19,727.918 (zero future leakage)
            FOMC 2022-06-15 -> MUST return exactly 19,702.404
            FOMC 2022-07-27 -> MUST return exactly 19,699.465
            FOMC 2023-11-01 -> MUST return exactly 21,738.871
        - Quantify alpha leakage of naive non-PIT model:
            Contamination = (21,738.871 - 19,727.918) = +2,010.953 (+10.19%).
            PIT Query contamination = 0.00%.
        - Mathematical invariance: H_1 == H_2 for query as-of 2022-05-04 evaluated
          before vs after ingesting 2023/2024 vintages.
        """
        fixtures_alfred = Path(__file__).resolve().parent.parent / "fixtures" / "alfred"
        gdpc1_path = fixtures_alfred / "GDPC1_vintages.csv"
        assert gdpc1_path.exists()

        con = tier4_duckdb
        store = BitemporalParquetStore(root_dir=tmp_path / "alfred_store", conn=con)
        dataset_id = "ALFRED_GDPC1"

        # Load all GDPC1 vintages
        gdpc1_all = load_alfred_series(gdpc1_path, conn=con, series_id="GDPC1")

        # Partition into Batch 1 (vintages published in 2022) and Batch 2 (vintages published in 2023+)
        con.register("gdp_tbl", gdpc1_all)
        batch1_2022 = (
            con.sql("""
            SELECT * FROM gdp_tbl
            WHERE knowledge_time < TIMESTAMP '2023-01-01 00:00:00'
        """)
            .arrow()
            .read_all()
        )

        batch2_2023 = (
            con.sql("""
            SELECT * FROM gdp_tbl
            WHERE knowledge_time >= TIMESTAMP '2023-01-01 00:00:00'
        """)
            .arrow()
            .read_all()
        )

        assert batch1_2022.num_rows > 0
        assert batch2_2023.num_rows > 0

        # Step 1: Ingest 2022 vintages
        store.append(batch1_2022, dataset_id=dataset_id, drop_id="2022_VINTAGES")

        # FOMC Meeting 1: 2022-05-04
        t_fomc_may22 = datetime(2022, 5, 4, 18, 0, 0, tzinfo=UTC)
        snap_may22_h1 = store.as_of_query(dataset_id=dataset_id, as_of_time=t_fomc_may22)
        q1_22_may = next(
            r
            for r in snap_may22_h1.to_pylist()
            if r["event_time"].year == 2022 and r["event_time"].month == 1
        )
        assert q1_22_may["value"] == pytest.approx(19727.918)

        # FOMC Meeting 2: 2022-06-15
        t_fomc_jun22 = datetime(2022, 6, 15, 18, 0, 0, tzinfo=UTC)
        snap_jun22 = store.as_of_query(dataset_id=dataset_id, as_of_time=t_fomc_jun22)
        q1_22_jun = next(
            r
            for r in snap_jun22.to_pylist()
            if r["event_time"].year == 2022 and r["event_time"].month == 1
        )
        assert q1_22_jun["value"] == pytest.approx(19702.404)

        # FOMC Meeting 3: 2022-07-27
        t_fomc_jul22 = datetime(2022, 7, 27, 18, 0, 0, tzinfo=UTC)
        snap_jul22 = store.as_of_query(dataset_id=dataset_id, as_of_time=t_fomc_jul22)
        q1_22_jul = next(
            r
            for r in snap_jul22.to_pylist()
            if r["event_time"].year == 2022 and r["event_time"].month == 1
        )
        assert q1_22_jul["value"] == pytest.approx(19699.465)

        # Step 2: Ingest 2023+ vintages (which contain massive comprehensive benchmark revisions)
        store.append(batch2_2023, dataset_id=dataset_id, drop_id="2023_VINTAGES")

        # Step 3: Mathematical Invariance check:
        # Re-query as-of FOMC 2022-05-04 after 2023 vintages are in the store -> snap_may22_h2
        snap_may22_h2 = store.as_of_query(dataset_id=dataset_id, as_of_time=t_fomc_may22)
        assert_point_in_time_invariance(
            snap_may22_h1, snap_may22_h2, key_columns=["entity_id", "event_time", "metric"]
        )

        q1_22_may_after = next(
            r
            for r in snap_may22_h2.to_pylist()
            if r["event_time"].year == 2022 and r["event_time"].month == 1
        )
        assert q1_22_may_after["value"] == pytest.approx(19727.918)

        # FOMC Meeting 4: 2023-11-01 (after 2023-09-28 benchmark revision)
        t_fomc_nov23 = datetime(2023, 11, 1, 18, 0, 0, tzinfo=UTC)
        snap_nov23 = store.as_of_query(dataset_id=dataset_id, as_of_time=t_fomc_nov23)
        q1_22_nov = next(
            r
            for r in snap_nov23.to_pylist()
            if r["event_time"].year == 2022 and r["event_time"].month == 1
        )
        assert q1_22_nov["value"] == pytest.approx(21738.871)

        # Quantitative alpha leakage validation:
        # Final revision (21738.871) vs PIT May 2022 knowledge (19727.918)
        naive_lookahead_delta = q1_22_nov["value"] - q1_22_may["value"]
        assert naive_lookahead_delta == pytest.approx(2010.953)
        pct_leakage = (naive_lookahead_delta / q1_22_may["value"]) * 100.0
        assert pct_leakage == pytest.approx(10.1934, rel=1e-3)

        # PIT contamination is strictly zero
        pit_contamination = abs(q1_22_may_after["value"] - q1_22_may["value"])
        assert pit_contamination <= 1e-9

    def test_payems_macro_revision_velocity_and_audit_trajectory(
        self,
        tmp_path: Path,
        tier4_duckdb: duckdb.DuckDBPyConnection,
        tier4_diff_engine: ReferenceDiffEngine,
    ) -> None:
        """
        Scenario 4B.2: Nonfarm Payrolls (PAYEMS) Revision Velocity & Audit Trajectory:
        - Monthly payroll releases for January 2023 (event_time = 2023-01-01):
            1. 2023-02-03 (Initial Release):   155,073 Thousand
            2. 2023-03-10 (First Revision):    155,120 Thousand (+47k)
            3. 2023-04-07 (Second Revision):   155,150 Thousand (+30k)
            4. 2024-02-02 (Annual Benchmark):  155,350 Thousand (+200k)
        - Run DiffEngine between consecutive monthly drops:
            Feb 2023 vs Mar 2023:
              - Jan 2023: RESTATED (delta = +47.0)
              - Feb 2023: NEW (155,384.0)
            Mar 2023 vs Apr 2023:
              - Jan 2023: RESTATED (delta = +30.0)
              - Feb 2023: RESTATED (delta = +36.0)
              - Mar 2023: NEW (155,620.0)
        - Full vintage audit trail: strictly ordered sequence with revision_seq 1..4.
        """
        fixtures_alfred = Path(__file__).resolve().parent.parent / "fixtures" / "alfred"
        payems_path = fixtures_alfred / "PAYEMS_vintages.csv"
        assert payems_path.exists()

        con = tier4_duckdb
        payems_all = load_alfred_series(payems_path, conn=con, series_id="PAYEMS")
        store = BitemporalParquetStore(root_dir=tmp_path / "payems_store", conn=con)
        dataset_id = "ALFRED_PAYEMS"

        store.append(payems_all, dataset_id=dataset_id, drop_id="PAYEMS_FULL")

        # 1. Audit trail for January 2023
        t_jan23 = datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC)
        trail = store.audit_trail(
            dataset_id=dataset_id, entity_id="PAYEMS", event_time=t_jan23, metric="PAYEMS"
        )
        rows = trail.to_pylist()

        assert len(rows) == 4
        expected_values = [155073.0, 155120.0, 155150.0, 155350.0]
        expected_dates = ["2023-02-03", "2023-03-10", "2023-04-07", "2024-02-02"]

        for i, r in enumerate(rows):
            assert r["revision_seq"] == i + 1
            assert r["value"] == expected_values[i]
            assert r["knowledge_time"].strftime("%Y-%m-%d") == expected_dates[i]
            if i > 0:
                assert r["is_restatement"] is True
                assert r["knowledge_time"] > rows[i - 1]["knowledge_time"]

        # 2. Diffing consecutive monthly releases
        # Month 1 drop (known as of 2023-02-15)
        t_feb15 = datetime(2023, 2, 15, 0, 0, 0, tzinfo=UTC)
        snap_feb = store.as_of_query(dataset_id=dataset_id, as_of_time=t_feb15)

        # Month 2 drop (known as of 2023-03-15)
        t_mar15 = datetime(2023, 3, 15, 0, 0, 0, tzinfo=UTC)
        snap_mar = store.as_of_query(dataset_id=dataset_id, as_of_time=t_mar15)

        diff_feb_mar, s_fm = tier4_diff_engine.diff_drops(
            snap_feb, snap_mar, keys=["entity_id", "event_time", "metric"]
        )

        assert s_fm.new_records == 1  # Feb 2023 is new
        assert s_fm.restated_records == 1  # Jan 2023 is restated
        diff_dict = {r["event_time"].strftime("%Y-%m-%d"): r for r in diff_feb_mar.to_pylist()}
        assert diff_dict["2023-01-01"]["diff_status"] == "RESTATED"
        assert diff_dict["2023-01-01"]["restatement_delta"] == pytest.approx(47.0)
        assert diff_dict["2023-02-01"]["diff_status"] == "NEW"
        assert diff_dict["2023-02-01"]["value_new"] == 155384.0

        # Month 3 drop (known as of 2023-04-15)
        t_apr15 = datetime(2023, 4, 15, 0, 0, 0, tzinfo=UTC)
        snap_apr = store.as_of_query(dataset_id=dataset_id, as_of_time=t_apr15)

        diff_mar_apr, s_ma = tier4_diff_engine.diff_drops(
            snap_mar, snap_apr, keys=["entity_id", "event_time", "metric"]
        )
        assert s_ma.new_records == 1  # Mar 2023 is new
        assert s_ma.restated_records == 2  # Both Jan and Feb restated
        diff_dict_apr = {r["event_time"].strftime("%Y-%m-%d"): r for r in diff_mar_apr.to_pylist()}
        assert diff_dict_apr["2023-01-01"]["restatement_delta"] == pytest.approx(30.0)
        assert diff_dict_apr["2023-02-01"]["restatement_delta"] == pytest.approx(36.0)
        assert diff_dict_apr["2023-03-01"]["diff_status"] == "NEW"

    def test_multi_vintage_macro_cross_sectional_alignment(
        self,
        tmp_path: Path,
        tier4_duckdb: duckdb.DuckDBPyConnection,
        tier4_validator: VectorizedValidationEngine,
    ) -> None:
        """
        Scenario 4B.3: Point-in-Time Co-Vintage Feature Vector Alignment (GDP, PAYEMS, CPI):
        - Ingest GDPC1, PAYEMS, and CPIAUCSL series.
        - Evaluate quarterly backtest dates: 2022-06-30, 2022-09-30, 2022-12-31, 2023-06-30.
        - Verify that at every date T, the aligned feature vector [GDPC1, PAYEMS, CPIAUCSL]
          contains exclusively observations where knowledge_time <= T.
        - Vectorized validation: values strictly non-negative, zero missing coordinates.
        """
        fixtures_alfred = Path(__file__).resolve().parent.parent / "fixtures" / "alfred"
        con = tier4_duckdb
        store = BitemporalParquetStore(root_dir=tmp_path / "macro_store", conn=con)

        gdp_tbl = load_alfred_series(
            fixtures_alfred / "GDPC1_vintages.csv", conn=con, series_id="GDPC1"
        )
        pay_tbl = load_alfred_series(
            fixtures_alfred / "PAYEMS_vintages.csv", conn=con, series_id="PAYEMS"
        )
        cpi_tbl = load_alfred_series(
            fixtures_alfred / "CPIAUCSL_vintages.csv", conn=con, series_id="CPIAUCSL"
        )

        store.append(gdp_tbl, dataset_id="MACRO_ALL", drop_id="GDP")
        store.append(pay_tbl, dataset_id="MACRO_ALL", drop_id="PAYEMS")
        store.append(cpi_tbl, dataset_id="MACRO_ALL", drop_id="CPI")

        # Validate domain rules on ingested tables
        for tbl in [gdp_tbl, pay_tbl, cpi_tbl]:
            val_comp = tier4_validator.validate_completeness(
                tbl, mandatory_cols=["entity_id", "event_time", "knowledge_time", "value"]
            )
            assert val_comp.passed
            val_pos = tier4_validator.validate_positive_values(
                tbl,
                metric_col="metric",
                value_col="value",
                positive_metrics=["GDPC1", "PAYEMS", "CPIAUCSL"],
            )
            assert val_pos.passed

        # Quarterly backtest dates
        backtest_dates = [
            datetime(2022, 6, 30, 23, 59, 59, tzinfo=UTC),
            datetime(2022, 9, 30, 23, 59, 59, tzinfo=UTC),
            datetime(2022, 12, 31, 23, 59, 59, tzinfo=UTC),
            datetime(2023, 6, 30, 23, 59, 59, tzinfo=UTC),
        ]

        for b_date in backtest_dates:
            snap = store.as_of_query(dataset_id="MACRO_ALL", as_of_time=b_date)
            assert snap.num_rows > 0

            # Verify that EVERY single record in the snapshot has knowledge_time <= b_date
            for r in snap.to_pylist():
                assert r["knowledge_time"] <= b_date, (
                    f"Look-ahead violation! Record {r['record_id']} has knowledge_time "
                    f"{r['knowledge_time']} > backtest date {b_date}"
                )

            # Check presence of macro entities
            entities_in_snap = set(snap.column("entity_id").to_pylist())
            assert "GDPC1" in entities_in_snap

            if b_date.year == 2022:
                # In 2022, PAYEMS and CPIAUCSL were not yet published for 2023 observation dates (zero forward leakage)
                assert "PAYEMS" not in entities_in_snap
                assert "CPIAUCSL" not in entities_in_snap
            else:
                # In 2023, all 3 series are fully aligned and contemporaneous
                assert "PAYEMS" in entities_in_snap
                assert "CPIAUCSL" in entities_in_snap
