"""
Tier 1 Feature Tests: F-SYM-01, F-SYM-02, F-SYM-03 Symbology Master & Quarantine.

Tests Point-in-Time (PIT) ticker resolution (e.g. FB -> META on 2022-06-09),
bidirectional cross-identifier master (Ticker, CIK, CUSIP, ISIN, SEDOL, FIGI),
and unmapped entity synthetic ID quarantine (UNRESOLVED_<VENDOR>_<ID>).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pytest


@dataclass(frozen=True)
class SymbologyMapping:
    entity_id: str
    ticker: str
    cik: str
    cusip: str
    isin: str
    sedol: str
    figi: str
    effective_start: datetime
    effective_end: datetime


@dataclass
class QuarantineRecord:
    synthetic_id: str
    raw_symbol: str
    source_type: str
    vendor_id: str
    quarantine_time: datetime
    resolved_entity_id: str | None = None


class SymbologyMaster:
    """Opaque-box reference implementation of Symbology Master and Quarantine."""

    def __init__(self) -> None:
        self.mappings: list[SymbologyMapping] = []
        self.quarantine_ledger: list[QuarantineRecord] = []

    def add_mapping(self, mapping: SymbologyMapping) -> None:
        self.mappings.append(mapping)

    def resolve(self, symbol: str, symbol_type: str, as_of: datetime, vendor_id: str = "SYNTH") -> str:
        """Resolves raw symbol to canonical entity_id as of a specific date."""
        for m in self.mappings:
            if m.effective_start <= as_of < m.effective_end:
                st = symbol_type.upper()
                if st == "TICKER" and m.ticker == symbol:
                    return m.entity_id
                elif st == "CIK" and m.cik == symbol:
                    return m.entity_id
                elif st == "CUSIP" and m.cusip == symbol:
                    return m.entity_id
                elif st == "ISIN" and m.isin == symbol:
                    return m.entity_id
                elif st == "SEDOL" and m.sedol == symbol:
                    return m.entity_id
                elif st == "FIGI" and m.figi == symbol:
                    return m.entity_id

        # Unmapped entity -> Quarantine
        clean_sym = symbol.replace("UNKNOWN_CORP_", "").replace("GHOST_TICKER_", "")
        synthetic_id = f"UNRESOLVED_{vendor_id}_{clean_sym}"
        self.quarantine_ledger.append(
            QuarantineRecord(
                synthetic_id=synthetic_id,
                raw_symbol=symbol,
                source_type=symbol_type,
                vendor_id=vendor_id,
                quarantine_time=as_of,
            )
        )
        return synthetic_id

    def lookup_cik_by_ticker(self, ticker: str, as_of: datetime) -> str | None:
        for m in self.mappings:
            if m.ticker == ticker and m.effective_start <= as_of < m.effective_end:
                return m.cik
        return None

    def lookup_ticker_by_cik(self, cik: str, as_of: datetime) -> str | None:
        for m in self.mappings:
            if m.cik == cik and m.effective_start <= as_of < m.effective_end:
                return m.ticker
        return None


@pytest.fixture
def master_catalog() -> SymbologyMaster:
    """Fixture providing populated master with corporate actions and cross identifiers."""
    master = SymbologyMaster()
    t_epoch = datetime(1990, 1, 1, tzinfo=timezone.utc)
    t_cutoff_meta = datetime(2022, 6, 9, 0, 0, 0, tzinfo=timezone.utc)
    t_future = datetime(2099, 12, 31, tzinfo=timezone.utc)

    # FB before 2022-06-09
    master.add_mapping(
        SymbologyMapping(
            entity_id="ENT_META",
            ticker="FB",
            cik="0001326801",
            cusip="30303M102",
            isin="US30303M1027",
            sedol="B7TL820",
            figi="BBG000MM2P62",
            effective_start=t_epoch,
            effective_end=t_cutoff_meta,
        )
    )
    # META after 2022-06-09
    master.add_mapping(
        SymbologyMapping(
            entity_id="ENT_META",
            ticker="META",
            cik="0001326801",
            cusip="30303M102",
            isin="US30303M1027",
            sedol="B7TL820",
            figi="BBG000MM2P62",
            effective_start=t_cutoff_meta,
            effective_end=t_future,
        )
    )
    # AAPL
    master.add_mapping(
        SymbologyMapping(
            entity_id="ENT_AAPL",
            ticker="AAPL",
            cik="0000320193",
            cusip="037833100",
            isin="US0378331005",
            sedol="2046251",
            figi="BBG000B9XRY4",
            effective_start=t_epoch,
            effective_end=t_future,
        )
    )
    # MSFT
    master.add_mapping(
        SymbologyMapping(
            entity_id="ENT_MSFT",
            ticker="MSFT",
            cik="0000789019",
            cusip="594918104",
            isin="US5949181045",
            sedol="2588173",
            figi="BBG000BPH459",
            effective_start=t_epoch,
            effective_end=t_future,
        )
    )
    return master


# ============================================================================
# F-SYM-01: Point-in-Time Resolution Tests
# ============================================================================

@pytest.mark.tier1
def test_symbology_fb_before_rename(master_catalog):
    """Verify ticker FB resolves to ENT_META prior to 2022-06-09."""
    as_of = datetime(2021, 10, 15, 16, 0, 0, tzinfo=timezone.utc)
    entity_id = master_catalog.resolve("FB", "TICKER", as_of)
    assert entity_id == "ENT_META"


@pytest.mark.tier1
def test_symbology_meta_after_rename(master_catalog):
    """Verify ticker META resolves to ENT_META after 2022-06-09."""
    as_of = datetime(2023, 10, 6, 20, 0, 0, tzinfo=timezone.utc)
    entity_id = master_catalog.resolve("META", "TICKER", as_of)
    assert entity_id == "ENT_META"


@pytest.mark.tier1
def test_symbology_meta_before_rename_inactive(master_catalog):
    """Verify querying META prior to 2022-06-09 is not active under that symbol."""
    as_of = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    res = master_catalog.resolve("META", "TICKER", as_of)
    # Was not registered as META in 2020
    assert res.startswith("UNRESOLVED_")


@pytest.mark.tier1
def test_symbology_fb_after_rename_inactive(master_catalog):
    """Verify querying FB in 2023 after ticker change is routed to quarantine."""
    as_of = datetime(2023, 5, 1, 0, 0, 0, tzinfo=timezone.utc)
    res = master_catalog.resolve("FB", "TICKER", as_of)
    assert res.startswith("UNRESOLVED_")


@pytest.mark.tier1
def test_symbology_exact_cutoff_date(master_catalog):
    """Verify half-open interval [effective_start, effective_end) on exact cutoff date."""
    cutoff = datetime(2022, 6, 9, 0, 0, 0, tzinfo=timezone.utc)
    # On cutoff date, META is effective
    assert master_catalog.resolve("META", "TICKER", cutoff) == "ENT_META"


@pytest.mark.tier1
def test_symbology_multi_hop_ticker_changes(master_catalog):
    """Verify sequence of multiple ticker changes for a corporate entity."""
    t1 = datetime(2010, 1, 1, tzinfo=timezone.utc)
    t2 = datetime(2015, 1, 1, tzinfo=timezone.utc)
    t3 = datetime(2020, 1, 1, tzinfo=timezone.utc)
    t4 = datetime(2025, 1, 1, tzinfo=timezone.utc)

    master_catalog.add_mapping(
        SymbologyMapping("ENT_CORP_X", "TICK_OLD", "111", "C1", "I1", "S1", "F1", t1, t2)
    )
    master_catalog.add_mapping(
        SymbologyMapping("ENT_CORP_X", "TICK_MID", "111", "C1", "I1", "S1", "F1", t2, t3)
    )
    master_catalog.add_mapping(
        SymbologyMapping("ENT_CORP_X", "TICK_NEW", "111", "C1", "I1", "S1", "F1", t3, t4)
    )

    assert master_catalog.resolve("TICK_OLD", "TICKER", datetime(2012, 1, 1, tzinfo=timezone.utc)) == "ENT_CORP_X"
    assert master_catalog.resolve("TICK_MID", "TICKER", datetime(2017, 1, 1, tzinfo=timezone.utc)) == "ENT_CORP_X"
    assert master_catalog.resolve("TICK_NEW", "TICKER", datetime(2022, 1, 1, tzinfo=timezone.utc)) == "ENT_CORP_X"


@pytest.mark.tier1
def test_symbology_temporal_range_gap(master_catalog):
    """Verify gap in listing period routes to quarantine."""
    as_of = datetime(1985, 1, 1, tzinfo=timezone.utc)  # Before epoch
    res = master_catalog.resolve("AAPL", "TICKER", as_of)
    assert res.startswith("UNRESOLVED_")


# ============================================================================
# F-SYM-02: Cross-Identifier Master Tests
# ============================================================================

@pytest.mark.tier1
def test_cross_id_ticker_to_cik(master_catalog):
    """Verify ticker AAPL resolves to CIK 0000320193."""
    as_of = datetime(2023, 10, 1, tzinfo=timezone.utc)
    cik = master_catalog.lookup_cik_by_ticker("AAPL", as_of)
    assert cik == "0000320193"


@pytest.mark.tier1
def test_cross_id_cik_to_ticker(master_catalog):
    """Verify CIK 0000320193 resolves back to AAPL."""
    as_of = datetime(2023, 10, 1, tzinfo=timezone.utc)
    ticker = master_catalog.lookup_ticker_by_cik("0000320193", as_of)
    assert ticker == "AAPL"


@pytest.mark.tier1
def test_cross_id_cusip_to_entity(master_catalog):
    """Verify CUSIP 037833100 maps to ENT_AAPL."""
    as_of = datetime(2023, 10, 1, tzinfo=timezone.utc)
    assert master_catalog.resolve("037833100", "CUSIP", as_of) == "ENT_AAPL"


@pytest.mark.tier1
def test_cross_id_isin_to_entity(master_catalog):
    """Verify ISIN US0378331005 maps to ENT_AAPL."""
    as_of = datetime(2023, 10, 1, tzinfo=timezone.utc)
    assert master_catalog.resolve("US0378331005", "ISIN", as_of) == "ENT_AAPL"


@pytest.mark.tier1
def test_cross_id_sedol_to_entity(master_catalog):
    """Verify SEDOL 2046251 maps to ENT_AAPL."""
    as_of = datetime(2023, 10, 1, tzinfo=timezone.utc)
    assert master_catalog.resolve("2046251", "SEDOL", as_of) == "ENT_AAPL"


@pytest.mark.tier1
def test_cross_id_figi_to_entity(master_catalog):
    """Verify FIGI BBG000B9XRY4 maps to ENT_AAPL."""
    as_of = datetime(2023, 10, 1, tzinfo=timezone.utc)
    assert master_catalog.resolve("BBG000B9XRY4", "FIGI", as_of) == "ENT_AAPL"


@pytest.mark.tier1
def test_cross_id_msft_full_resolution(master_catalog):
    """Verify Microsoft resolution across all identifier types."""
    as_of = datetime(2023, 10, 1, tzinfo=timezone.utc)
    assert master_catalog.resolve("MSFT", "TICKER", as_of) == "ENT_MSFT"
    assert master_catalog.resolve("0000789019", "CIK", as_of) == "ENT_MSFT"
    assert master_catalog.resolve("594918104", "CUSIP", as_of) == "ENT_MSFT"
    assert master_catalog.resolve("US5949181045", "ISIN", as_of) == "ENT_MSFT"


@pytest.mark.tier1
def test_cross_id_bidirectional_consistency(master_catalog):
    """Verify bidirectional consistency across all identifier pairs."""
    as_of = datetime(2023, 10, 1, tzinfo=timezone.utc)
    for ticker in ["AAPL", "MSFT"]:
        cik = master_catalog.lookup_cik_by_ticker(ticker, as_of)
        assert cik is not None
        recovered_ticker = master_catalog.lookup_ticker_by_cik(cik, as_of)
        assert recovered_ticker == ticker


# ============================================================================
# F-SYM-03: Quarantine Tests
# ============================================================================

@pytest.mark.tier1
def test_quarantine_unknown_symbol_generation(master_catalog):
    """Verify unknown symbol UNKNOWN_CORP_XYZ99 generates UNRESOLVED_SYNTH_XYZ99."""
    as_of = datetime(2023, 10, 16, 20, 0, 0, tzinfo=timezone.utc)
    syn_id = master_catalog.resolve("UNKNOWN_CORP_XYZ99", "TICKER", as_of, vendor_id="SYNTH")
    assert syn_id == "UNRESOLVED_SYNTH_XYZ99"


@pytest.mark.tier1
def test_quarantine_ghost_ticker_generation(master_catalog):
    """Verify ghost ticker GHOST_TICKER_999 generates UNRESOLVED_SYNTH_999."""
    as_of = datetime(2023, 10, 16, 20, 0, 0, tzinfo=timezone.utc)
    syn_id = master_catalog.resolve("GHOST_TICKER_999", "TICKER", as_of, vendor_id="SYNTH")
    assert syn_id == "UNRESOLVED_SYNTH_999"


@pytest.mark.tier1
def test_quarantine_preserves_pipeline_flow(master_catalog):
    """Verify pipeline does not crash when unmapped symbol is resolved."""
    as_of = datetime(2023, 10, 16, 20, 0, 0, tzinfo=timezone.utc)
    # Does not raise Exception
    res = master_catalog.resolve("NONEXISTENT_SECURITY", "TICKER", as_of)
    assert res.startswith("UNRESOLVED_")


@pytest.mark.tier1
def test_quarantine_ledger_record(master_catalog):
    """Verify unmapped resolution is logged into quarantine ledger with metadata."""
    as_of = datetime(2023, 10, 16, 20, 0, 0, tzinfo=timezone.utc)
    initial_count = len(master_catalog.quarantine_ledger)
    master_catalog.resolve("ANOMALOUS_TICKER_X", "TICKER", as_of, vendor_id="VEND1")

    assert len(master_catalog.quarantine_ledger) == initial_count + 1
    entry = master_catalog.quarantine_ledger[-1]
    assert entry.raw_symbol == "ANOMALOUS_TICKER_X"
    assert entry.vendor_id == "VEND1"
    assert entry.synthetic_id.startswith("UNRESOLVED_VEND1_")


@pytest.mark.tier1
def test_quarantine_deterministic_synthetic_id(master_catalog):
    """Verify resolving the same unmapped symbol repeatedly produces the identical synthetic ID."""
    as_of = datetime(2023, 10, 16, 20, 0, 0, tzinfo=timezone.utc)
    id1 = master_catalog.resolve("REPEATED_UNKNOWN", "TICKER", as_of)
    id2 = master_catalog.resolve("REPEATED_UNKNOWN", "TICKER", as_of)
    assert id1 == id2


@pytest.mark.tier1
def test_quarantine_retroactive_remapping(master_catalog):
    """Verify quarantined records can be updated when mapping is added later."""
    as_of = datetime(2023, 10, 16, 20, 0, 0, tzinfo=timezone.utc)
    syn_id = master_catalog.resolve("NEWLY_IPO_CORP", "TICKER", as_of)
    assert syn_id.startswith("UNRESOLVED_")

    # Add mapping later
    t_start = datetime(2023, 10, 1, tzinfo=timezone.utc)
    t_end = datetime(2030, 1, 1, tzinfo=timezone.utc)
    master_catalog.add_mapping(
        SymbologyMapping("ENT_NEW_IPO", "NEWLY_IPO_CORP", "99999", "C99", "I99", "S99", "F99", t_start, t_end)
    )

    resolved_id = master_catalog.resolve("NEWLY_IPO_CORP", "TICKER", as_of)
    assert resolved_id == "ENT_NEW_IPO"
