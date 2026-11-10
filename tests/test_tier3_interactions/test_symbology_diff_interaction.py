"""
Tier 3 Interaction Tests: Symbology Master & Multi-Drop Diffing Engine Interactions.

Tests the stateful interaction between Point-in-Time (PIT) symbology resolution
(F-SYM-01, F-SYM-02, F-SYM-03) and the Multi-Drop Diffing Engine (F-DIF-01, F-DIF-02, F-DIF-03):
- Corporate action ticker rename FB -> META concurrent with financial restatement in Drop 2.
- Corporate action rename without value change yielding 100% UNCHANGED.
- Multi-vendor symbol collision (two vendors using same ticker for distinct entities).
- Cross-identifier master with diffing (CUSIP, ISIN, SEDOL, FIGI switches).
- Temporal symbology boundary effective dates during diff calculations.
- Diffing resolution before vs after pipeline stage invariant.
- Unmapped entity transition to mapped entity across subsequent drops.
- Multi-drop ticker churn and restatements ledger across 3 lifecycle stages.
- Source lineage preservation through symbology resolution and diff classification.
"""

from __future__ import annotations

from datetime import UTC, datetime

import duckdb
import pyarrow as pa
import pytest

from test_tier3_interactions.conftest import (
    ReferenceDiffEngine,
    SymbologyMapping,
    SymbologyMaster,
)


@pytest.mark.tier3
class TestSymbologyDiffInteraction:
    """Test suite for symbology and diffing engine interactions."""

    def test_corporate_action_fb_to_meta_concurrent_with_restatement(
        self, standard_symbology_master: SymbologyMaster, tier3_diff_engine: ReferenceDiffEngine
    ) -> None:
        """
        Corporate action rename FB -> META on 2022-06-09 concurrent with financial restatement.

        In Drop 1 (ingested 2022-05-20), ticker is 'FB', price is 200.0 for event_time 2022-05-15.
        In Drop 2 (ingested 2022-06-15), ticker is 'META', price is restated to 195.0 for 2022-05-15.
        Symbology resolves both FB (as-of 2022-05-20) and META (as-of 2022-06-15) to ENT_META.
        DiffEngine MUST identify 1 RESTATED record (delta = -5.0), NOT 1 DELETED and 1 NEW record.
        """
        master = standard_symbology_master
        t_event = datetime(2022, 5, 15, 16, 0, 0, tzinfo=UTC)
        t_known_1 = datetime(2022, 5, 20, 20, 0, 0, tzinfo=UTC)
        t_known_2 = datetime(2022, 6, 15, 20, 0, 0, tzinfo=UTC)

        # Baseline Drop 1 (FB)
        resolved_ent_1 = master.resolve("FB", "TICKER", as_of=t_known_1)
        assert resolved_ent_1 == "ENT_META"

        base_data = {
            "record_id": ["REC_001"],
            "entity_id": [resolved_ent_1],
            "source_symbol": ["FB"],
            "event_time": [t_event],
            "metric": ["CLOSE_PRICE"],
            "value": [200.0],
            "unit": ["USD"],
        }
        base_tbl = pa.Table.from_pydict(base_data)

        # Current Drop 2 (META, restated price 195.0)
        resolved_ent_2 = master.resolve("META", "TICKER", as_of=t_known_2)
        assert resolved_ent_2 == "ENT_META"

        curr_data = {
            "record_id": ["REC_002"],
            "entity_id": [resolved_ent_2],
            "source_symbol": ["META"],
            "event_time": [t_event],
            "metric": ["CLOSE_PRICE"],
            "value": [195.0],
            "unit": ["USD"],
        }
        curr_tbl = pa.Table.from_pydict(curr_data)

        # Diffing with symbology resolution
        diff_tbl, summary = tier3_diff_engine.diff_drops(
            base_tbl, curr_tbl, drop_id_baseline="DROP_001", drop_id_current="DROP_002"
        )

        assert summary.restated_records == 1
        assert summary.new_records == 0
        assert summary.dropped_records == 0
        assert summary.unchanged_records == 0

        row = diff_tbl.to_pylist()[0]
        assert row["entity_id"] == "ENT_META"
        assert row["diff_status"] == "RESTATED"
        assert row["value_old"] == 200.0
        assert row["value_new"] == 195.0
        assert row["restatement_delta"] == pytest.approx(-5.0)
        assert row["restatement_pct_delta"] == pytest.approx(-2.5)

        # Contrast: if naively diffed on raw ticker without symbology resolution
        naive_base = base_tbl.set_column(1, "entity_id", pa.array(["FB"]))
        naive_curr = curr_tbl.set_column(1, "entity_id", pa.array(["META"]))
        _, naive_summary = tier3_diff_engine.diff_drops(naive_base, naive_curr)
        assert naive_summary.restated_records == 0
        assert naive_summary.dropped_records == 1  # False deletion
        assert naive_summary.new_records == 1  # False addition

    def test_corporate_action_rename_without_value_change_yields_unchanged(
        self, standard_symbology_master: SymbologyMaster, tier3_diff_engine: ReferenceDiffEngine
    ) -> None:
        """
        Corporate action ticker rename FB -> META where observation value is UNCHANGED.
        DiffEngine must classify as UNCHANGED, preventing ticker churn from causing false revisions.
        """
        master = standard_symbology_master
        t_event = datetime(2022, 5, 15, 16, 0, 0, tzinfo=UTC)
        t_known_1 = datetime(2022, 5, 20, 20, 0, 0, tzinfo=UTC)
        t_known_2 = datetime(2022, 6, 15, 20, 0, 0, tzinfo=UTC)

        ent_1 = master.resolve("FB", "TICKER", as_of=t_known_1)
        ent_2 = master.resolve("META", "TICKER", as_of=t_known_2)
        assert ent_1 == ent_2 == "ENT_META"

        base_tbl = pa.Table.from_pydict(
            {
                "record_id": ["REC_01"],
                "entity_id": [ent_1],
                "source_symbol": ["FB"],
                "event_time": [t_event],
                "metric": ["CLOSE_PRICE"],
                "value": [198.50],
                "unit": ["USD"],
            }
        )
        curr_tbl = pa.Table.from_pydict(
            {
                "record_id": ["REC_02"],
                "entity_id": [ent_2],
                "source_symbol": ["META"],
                "event_time": [t_event],
                "metric": ["CLOSE_PRICE"],
                "value": [198.50],  # Exactly identical value
                "unit": ["USD"],
            }
        )

        diff_tbl, summary = tier3_diff_engine.diff_drops(base_tbl, curr_tbl)
        assert summary.unchanged_records == 1
        assert summary.restated_records == 0
        assert summary.new_records == 0
        assert summary.dropped_records == 0

        row = diff_tbl.to_pylist()[0]
        assert row["diff_status"] == "UNCHANGED"
        assert row["restatement_delta"] == pytest.approx(0.0)

    def test_multi_vendor_symbol_collision_resolved_to_distinct_entities(
        self, tier3_diff_engine: ReferenceDiffEngine
    ) -> None:
        """
        Multi-vendor symbol collision: Vendor A uses 'BAT' for British American Tobacco,
        while Vendor B uses 'BAT' for Basic Attention Token (crypto).
        Symbology resolution scopes by vendor_id, ensuring distinct entity_ids and isolated diffing.
        """
        master = SymbologyMaster()
        t_epoch = datetime(2000, 1, 1, tzinfo=UTC)
        t_inf = datetime(2099, 12, 31, tzinfo=UTC)

        # Mapping for Vendor A (LSE Equity)
        master.add_mapping(
            SymbologyMapping(
                entity_id="ENT_BATS_LSE",
                ticker="BAT",
                cik="0001303523",
                cusip="110448107",
                isin="GB0002875804",
                sedol="0287580",
                figi="BBG000BH3S62",
                effective_start=t_epoch,
                effective_end=t_inf,
            )
        )
        # Mapping for Vendor B (Crypto Token)
        master.add_mapping(
            SymbologyMapping(
                entity_id="ENT_BAT_CRYPTO",
                ticker="BAT_CRYPTO",
                cik="0000000000",
                cusip="000000000",
                isin="000000000000",
                sedol="0000000",
                figi="BBG00F123456",
                effective_start=t_epoch,
                effective_end=t_inf,
            )
        )

        t_asof = datetime(2023, 1, 15, tzinfo=UTC)
        # Custom vendor resolver mapping
        ent_vendor_a = master.resolve("BAT", "TICKER", as_of=t_asof)
        assert ent_vendor_a == "ENT_BATS_LSE"

        # Vendor B uses symbol BAT but vendor-qualified
        ent_vendor_b = master.resolve("BAT_CRYPTO", "TICKER", as_of=t_asof)
        assert ent_vendor_b == "ENT_BAT_CRYPTO"

        t_event = datetime(2023, 1, 10, 16, 0, 0, tzinfo=UTC)
        # Drop 1 has both entities
        base_tbl = pa.Table.from_pydict(
            {
                "record_id": ["REC_A1", "REC_B1"],
                "entity_id": [ent_vendor_a, ent_vendor_b],
                "source_symbol": ["BAT", "BAT"],
                "event_time": [t_event, t_event],
                "metric": ["CLOSE_PRICE", "CLOSE_PRICE"],
                "value": [30.50, 0.25],
                "unit": ["GBP", "USD"],
            }
        )
        # Drop 2 restates only Vendor A's British American Tobacco (30.50 -> 31.00)
        curr_tbl = pa.Table.from_pydict(
            {
                "record_id": ["REC_A2", "REC_B2"],
                "entity_id": [ent_vendor_a, ent_vendor_b],
                "source_symbol": ["BAT", "BAT"],
                "event_time": [t_event, t_event],
                "metric": ["CLOSE_PRICE", "CLOSE_PRICE"],
                "value": [31.00, 0.25],  # Crypto unchanged, Tobacco restated
                "unit": ["GBP", "USD"],
            }
        )

        diff_tbl, summary = tier3_diff_engine.diff_drops(base_tbl, curr_tbl)
        assert summary.restated_records == 1
        assert summary.unchanged_records == 1
        assert summary.new_records == 0

        rows = {r["entity_id"]: r for r in diff_tbl.to_pylist()}
        assert rows["ENT_BATS_LSE"]["diff_status"] == "RESTATED"
        assert rows["ENT_BATS_LSE"]["restatement_delta"] == pytest.approx(0.50)
        assert rows["ENT_BAT_CRYPTO"]["diff_status"] == "UNCHANGED"
        assert rows["ENT_BAT_CRYPTO"]["restatement_delta"] == pytest.approx(0.0)

    def test_cross_identifier_cusip_to_isin_switch_with_restatement(
        self, standard_symbology_master: SymbologyMaster, tier3_diff_engine: ReferenceDiffEngine
    ) -> None:
        """
        Drop 1 delivers CUSIP ('037833100'); Drop 2 delivers ISIN ('US0378331005').
        Both cross-reference to ENT_AAPL. Drop 2 carries a financial restatement.
        DiffEngine matches records by resolved entity_id and detects restatement.
        """
        master = standard_symbology_master
        t_asof = datetime(2023, 10, 1, tzinfo=UTC)
        t_event = datetime(2023, 9, 30, 0, 0, 0, tzinfo=UTC)

        ent_cusip = master.resolve("037833100", "CUSIP", as_of=t_asof)
        ent_isin = master.resolve("US0378331005", "ISIN", as_of=t_asof)
        assert ent_cusip == ent_isin == "ENT_AAPL"

        base_tbl = pa.Table.from_pydict(
            {
                "record_id": ["BASE_01"],
                "entity_id": [ent_cusip],
                "source_symbol": ["037833100"],
                "event_time": [t_event],
                "metric": ["TOTAL_REVENUE"],
                "value": [383285000000.0],
                "unit": ["USD"],
            }
        )
        curr_tbl = pa.Table.from_pydict(
            {
                "record_id": ["CURR_01"],
                "entity_id": [ent_isin],
                "source_symbol": ["US0378331005"],
                "event_time": [t_event],
                "metric": ["TOTAL_REVENUE"],
                "value": [382500000000.0],  # Restated downward by 785M
                "unit": ["USD"],
            }
        )

        diff_tbl, summary = tier3_diff_engine.diff_drops(base_tbl, curr_tbl)
        assert summary.restated_records == 1
        assert summary.unchanged_records == 0

        row = diff_tbl.to_pylist()[0]
        assert row["entity_id"] == "ENT_AAPL"
        assert row["diff_status"] == "RESTATED"
        assert row["restatement_delta"] == pytest.approx(-785000000.0)
        assert row["restatement_pct_delta"] == pytest.approx(-0.204808, rel=1e-3)

    def test_cross_identifier_figi_to_sedol_resolution_in_diff(
        self, standard_symbology_master: SymbologyMaster, tier3_diff_engine: ReferenceDiffEngine
    ) -> None:
        """
        Drop 1 identifies Microsoft by FIGI ('BBG000BPH459'); Drop 2 uses SEDOL ('2588173').
        Both resolve to ENT_MSFT. Identical values yield 100% UNCHANGED diffing.
        """
        master = standard_symbology_master
        t_asof = datetime(2023, 6, 1, tzinfo=UTC)
        t_event = datetime(2023, 5, 31, 16, 0, 0, tzinfo=UTC)

        ent_figi = master.resolve("BBG000BPH459", "FIGI", as_of=t_asof)
        ent_sedol = master.resolve("2588173", "SEDOL", as_of=t_asof)
        assert ent_figi == ent_sedol == "ENT_MSFT"

        base_tbl = pa.Table.from_pydict(
            {
                "record_id": ["REC_FIGI"],
                "entity_id": [ent_figi],
                "source_symbol": ["BBG000BPH459"],
                "event_time": [t_event],
                "metric": ["DIVIDEND_PER_SHARE"],
                "value": [0.68],
                "unit": ["USD"],
            }
        )
        curr_tbl = pa.Table.from_pydict(
            {
                "record_id": ["REC_SEDOL"],
                "entity_id": [ent_sedol],
                "source_symbol": ["2588173"],
                "event_time": [t_event],
                "metric": ["DIVIDEND_PER_SHARE"],
                "value": [0.68],
                "unit": ["USD"],
            }
        )

        _, summary = tier3_diff_engine.diff_drops(base_tbl, curr_tbl)
        assert summary.unchanged_records == 1
        assert summary.restated_records == 0
        assert summary.new_records == 0
        assert summary.dropped_records == 0

    def test_temporal_symbology_boundary_effective_dates_in_diffing(
        self, standard_symbology_master: SymbologyMaster, tier3_diff_engine: ReferenceDiffEngine
    ) -> None:
        """
        Test exact boundary timestamps:
        Before 2022-06-09T00:00:00Z, 'FB' is valid.
        On or after 2022-06-09T00:00:00Z, 'FB' is expired and 'META' is active.
        Diffing drops respecting the boundary resolves consistent canonical entity_ids.
        """
        master = standard_symbology_master
        t_boundary = datetime(2022, 6, 9, 0, 0, 0, tzinfo=UTC)
        t_before = datetime(2022, 6, 8, 23, 59, 59, tzinfo=UTC)

        # 1 second before boundary: 'FB' resolves to ENT_META, 'META' is unmapped (quarantined)
        ent_fb_before = master.resolve("FB", "TICKER", as_of=t_before)
        assert ent_fb_before == "ENT_META"

        # At boundary: 'META' resolves to ENT_META, 'FB' is quarantined
        ent_meta_at = master.resolve("META", "TICKER", as_of=t_boundary)
        assert ent_meta_at == "ENT_META"

        # Baseline ingested before boundary with FB
        base_tbl = pa.Table.from_pydict(
            {
                "record_id": ["B1"],
                "entity_id": [ent_fb_before],
                "source_symbol": ["FB"],
                "event_time": [t_before],
                "metric": ["CLOSE"],
                "value": [196.0],
                "unit": ["USD"],
            }
        )
        # Current ingested at boundary with META
        curr_tbl = pa.Table.from_pydict(
            {
                "record_id": ["C1"],
                "entity_id": [ent_meta_at],
                "source_symbol": ["META"],
                "event_time": [t_before],
                "metric": ["CLOSE"],
                "value": [196.0],
                "unit": ["USD"],
            }
        )

        diff_tbl, summary = tier3_diff_engine.diff_drops(base_tbl, curr_tbl)
        assert summary.unchanged_records == 1
        assert diff_tbl.to_pylist()[0]["diff_status"] == "UNCHANGED"

    def test_unmapped_entity_transition_to_mapped_across_drops(
        self, tier3_diff_engine: ReferenceDiffEngine
    ) -> None:
        """
        Drop 1 arrives with unknown ticker 'NEW_IPO_CORP'.
        Symbology Master quarantines it as synthetic ID 'UNRESOLVED_VENDOR_NEW_IPO_CORP'.
        Prior to Drop 2, security master is updated to map 'NEW_IPO_CORP' to 'ENT_NEW_IPO'.
        Pipeline reconciles the synthetic ID to canonical entity and diffs correctly.
        """
        master = SymbologyMaster()
        t_drop1 = datetime(2023, 8, 1, tzinfo=UTC)
        t_drop2 = datetime(2023, 8, 15, tzinfo=UTC)
        t_event = datetime(2023, 7, 31, 16, 0, 0, tzinfo=UTC)

        # Drop 1: unmapped -> quarantine
        synth_id = master.resolve("NEW_IPO_CORP", "TICKER", as_of=t_drop1, vendor_id="VENDOR_X")
        assert synth_id == "UNRESOLVED_VENDOR_X_NEW_IPO_CORP"
        assert len(master.quarantine_ledger) == 1

        base_tbl = pa.Table.from_pydict(
            {
                "record_id": ["D1_01"],
                "entity_id": [synth_id],
                "source_symbol": ["NEW_IPO_CORP"],
                "event_time": [t_event],
                "metric": ["CLOSE"],
                "value": [45.0],
                "unit": ["USD"],
            }
        )

        # Security Master updated prior to Drop 2
        master.add_mapping(
            SymbologyMapping(
                entity_id="ENT_NEW_IPO",
                ticker="NEW_IPO_CORP",
                cik="0001999999",
                cusip="999999100",
                isin="US9999991000",
                sedol="9999999",
                figi="BBG000999999",
                effective_start=datetime(2020, 1, 1, tzinfo=UTC),
                effective_end=datetime(2099, 12, 31, tzinfo=UTC),
            )
        )

        # Drop 2 arrives with resolved canonical entity and restated price 48.0
        resolved_ent = master.resolve("NEW_IPO_CORP", "TICKER", as_of=t_drop2, vendor_id="VENDOR_X")
        assert resolved_ent == "ENT_NEW_IPO"

        # Re-link baseline synthetic ID to canonical entity before diffing
        con = duckdb.connect()
        con.register("base", base_tbl)
        reconciled_base = (
            con.sql(f"""
            SELECT
                record_id,
                CASE WHEN entity_id = '{synth_id}' THEN '{resolved_ent}' ELSE entity_id END as entity_id,
                source_symbol,
                event_time,
                metric,
                value,
                unit
            FROM base
        """)
            .arrow()
            .read_all()
        )

        curr_tbl = pa.Table.from_pydict(
            {
                "record_id": ["D2_01"],
                "entity_id": [resolved_ent],
                "source_symbol": ["NEW_IPO_CORP"],
                "event_time": [t_event],
                "metric": ["CLOSE"],
                "value": [48.0],
                "unit": ["USD"],
            }
        )

        diff_tbl, summary = tier3_diff_engine.diff_drops(reconciled_base, curr_tbl)
        assert summary.restated_records == 1
        row = diff_tbl.to_pylist()[0]
        assert row["entity_id"] == "ENT_NEW_IPO"
        assert row["diff_status"] == "RESTATED"
        assert row["restatement_delta"] == pytest.approx(3.0)

    def test_multi_drop_ticker_churn_three_stage_lifecycle(
        self, standard_symbology_master: SymbologyMaster, tier3_diff_engine: ReferenceDiffEngine
    ) -> None:
        """
        Three-stage lifecycle:
        Drop 1: Ticker 'FB', Price 200.0 (Baseline)
        Drop 2: Ticker 'META', Price 200.0 (Corporate action ticker rename, identical value)
        Drop 3: Ticker 'META', Price 210.0 (Subsequent restatement)

        Stage 1->2 diff MUST be 100% UNCHANGED.
        Stage 2->3 diff MUST be 100% RESTATED (delta = +10.0).
        """
        master = standard_symbology_master
        t_event = datetime(2022, 5, 1, 16, 0, 0, tzinfo=UTC)
        t1 = datetime(2022, 5, 5, tzinfo=UTC)
        t2 = datetime(2022, 6, 12, tzinfo=UTC)
        t3 = datetime(2022, 7, 1, tzinfo=UTC)

        ent_drop1 = master.resolve("FB", "TICKER", as_of=t1)
        ent_drop2 = master.resolve("META", "TICKER", as_of=t2)
        ent_drop3 = master.resolve("META", "TICKER", as_of=t3)
        assert ent_drop1 == ent_drop2 == ent_drop3 == "ENT_META"

        d1 = pa.Table.from_pydict(
            {
                "record_id": ["R1"],
                "entity_id": [ent_drop1],
                "source_symbol": ["FB"],
                "event_time": [t_event],
                "metric": ["CLOSE"],
                "value": [200.0],
                "unit": ["USD"],
            }
        )
        d2 = pa.Table.from_pydict(
            {
                "record_id": ["R2"],
                "entity_id": [ent_drop2],
                "source_symbol": ["META"],
                "event_time": [t_event],
                "metric": ["CLOSE"],
                "value": [200.0],
                "unit": ["USD"],
            }
        )
        d3 = pa.Table.from_pydict(
            {
                "record_id": ["R3"],
                "entity_id": [ent_drop3],
                "source_symbol": ["META"],
                "event_time": [t_event],
                "metric": ["CLOSE"],
                "value": [210.0],
                "unit": ["USD"],
            }
        )

        # Diff Stage 1 -> 2
        _, s12 = tier3_diff_engine.diff_drops(d1, d2)
        assert s12.unchanged_records == 1
        assert s12.restated_records == 0

        # Diff Stage 2 -> 3
        tbl23, s23 = tier3_diff_engine.diff_drops(d2, d3)
        assert s23.unchanged_records == 0
        assert s23.restated_records == 1
        assert tbl23.to_pylist()[0]["restatement_delta"] == pytest.approx(10.0)

    def test_symbology_diff_preserves_source_lineage_metadata(
        self, standard_symbology_master: SymbologyMaster, tier3_diff_engine: ReferenceDiffEngine
    ) -> None:
        """
        Verify that diff output preserves original source_symbol and record_id lineage,
        ensuring auditability back to raw vendor drop records while diffing on canonical entity_id.
        """
        master = standard_symbology_master
        t_asof = datetime(2023, 10, 1, tzinfo=UTC)
        t_event = datetime(2023, 9, 30, 0, 0, 0, tzinfo=UTC)

        ent_aapl = master.resolve("AAPL", "TICKER", as_of=t_asof)
        ent_msft = master.resolve("MSFT", "TICKER", as_of=t_asof)

        base_tbl = pa.Table.from_pydict(
            {
                "record_id": ["RAW_AAPL_D1", "RAW_MSFT_D1"],
                "entity_id": [ent_aapl, ent_msft],
                "source_symbol": ["AAPL", "MSFT"],
                "event_time": [t_event, t_event],
                "metric": ["NET_INCOME", "NET_INCOME"],
                "value": [96995000000.0, 72361000000.0],
                "unit": ["USD", "USD"],
            }
        )
        curr_tbl = pa.Table.from_pydict(
            {
                "record_id": ["RAW_AAPL_D2", "RAW_MSFT_D2"],
                "entity_id": [ent_aapl, ent_msft],
                "source_symbol": ["AAPL", "MSFT"],
                "event_time": [t_event, t_event],
                "metric": ["NET_INCOME", "NET_INCOME"],
                "value": [96450000000.0, 72361000000.0],  # AAPL restated, MSFT unchanged
                "unit": ["USD", "USD"],
            }
        )

        diff_tbl, summary = tier3_diff_engine.diff_drops(base_tbl, curr_tbl)
        assert summary.restated_records == 1
        assert summary.unchanged_records == 1

        rows = {r["entity_id"]: r for r in diff_tbl.to_pylist()}
        assert "source_symbol" in rows["ENT_AAPL"]
        assert rows["ENT_AAPL"]["source_symbol"] == "AAPL"
        assert rows["ENT_AAPL"]["diff_status"] == "RESTATED"
        assert rows["ENT_AAPL"]["restatement_delta"] == pytest.approx(-545000000.0)
        assert rows["ENT_MSFT"]["diff_status"] == "UNCHANGED"
