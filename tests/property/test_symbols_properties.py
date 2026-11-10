"""Hypothesis property-based tests for symbol master and deterministic resolution engine."""

from __future__ import annotations

import random
import tempfile

from hypothesis import given
from hypothesis import strategies as st

from vdok.symbols import ResolutionResult, SymbolMaster, build_symbol_master

FIXTURE_PATH = "tests/fixtures/symbols/company_tickers.json"


# Helper fixture loaded once
def get_test_master() -> SymbolMaster:
    temp_dir = tempfile.mkdtemp(prefix="vdok_test_symbols_")
    out = build_symbol_master(FIXTURE_PATH, output_dir=temp_dir)
    return SymbolMaster.load(out)


_SHARED_MASTER = get_test_master()
_KNOWN_TICKERS = list(_SHARED_MASTER._tickers_to_cik.keys())


@given(st.text(min_size=0, max_size=50))
def test_arbitrary_string_resolution_safety(raw_input: str) -> None:
    """Arbitrary text input always returns a valid ResolutionResult without crashing."""
    for id_type in ["ticker", "cik", "title", "isin", "cusip"]:
        res = _SHARED_MASTER.resolve(raw_input, id_type)
        assert isinstance(res, ResolutionResult)
        assert isinstance(res.entity_id, str)
        assert isinstance(res.unmapped, bool)


@given(st.sampled_from(_KNOWN_TICKERS))
def test_resolved_cik_resolution_idempotency(ticker: str) -> None:
    """Resolving a known ticker yields a CIK that resolves to itself identically."""
    res_ticker = _SHARED_MASTER.resolve(ticker, "ticker")
    assert not res_ticker.unmapped
    cik = res_ticker.entity_id

    res_cik = _SHARED_MASTER.resolve(cik, "cik")
    assert not res_cik.unmapped
    assert res_cik.entity_id == cik


@given(
    st.sampled_from(_KNOWN_TICKERS),
    st.text(alphabet=" \t", min_size=0, max_size=3),
    st.text(alphabet=" \t", min_size=0, max_size=3),
)
def test_ticker_case_whitespace_invariance(ticker: str, pad_l: str, pad_r: str) -> None:
    """Casing variations and whitespace padding resolve to identical canonical CIK."""
    # Permute case
    mixed_ticker = "".join(ch.lower() if random.random() > 0.5 else ch.upper() for ch in ticker)
    raw = f"{pad_l}{mixed_ticker}{pad_r}"

    res = _SHARED_MASTER.resolve(raw, "ticker")
    expected = _SHARED_MASTER.resolve(ticker, "ticker")
    assert res == expected


@given(st.sampled_from(["BRK.A", "BRK/A", "BRK-A", "BRK.B", "BRK/B", "BRK-B"]))
def test_dual_class_symbol_punctuation_equivalence(symbol: str) -> None:
    """Dual-class share punctuation forms resolve to the Berkshire Hathaway CIK."""
    res = _SHARED_MASTER.resolve(symbol, "ticker")
    assert not res.unmapped
    assert res.entity_id == "0001067983"


@given(
    st.sampled_from(_KNOWN_TICKERS),
    st.sampled_from([".OQ", ".O", ".N", ".P", ".ARC", ".ASE", ".MW"]),
)
def test_exchange_suffix_invariance(ticker: str, suffix: str) -> None:
    """Appending exchange suffix to a known ticker resolves to identical canonical CIK."""
    suffixed = f"{ticker}{suffix}"
    res = _SHARED_MASTER.resolve(suffixed, "ticker")
    expected = _SHARED_MASTER.resolve(ticker, "ticker")
    assert res == expected


@given(st.text(alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ", min_size=6, max_size=10))
def test_unknown_tickers_strictly_unmapped_no_fuzzy_guess(raw_symbol: str) -> None:
    """Unknown symbols are strictly flagged unmapped; fuzzy guessing is prohibited."""
    if raw_symbol not in _SHARED_MASTER._tickers_to_cik:
        res = _SHARED_MASTER.resolve(raw_symbol, "ticker")
        assert res.unmapped
        assert res.entity_id == raw_symbol


@given(st.text(min_size=1, max_size=40))
def test_unmapped_preserves_raw_identifier_exactly(raw_id: str) -> None:
    """Unmapped resolutions preserve raw identifier string verbatim."""
    res = _SHARED_MASTER.resolve(raw_id, "vendor_id")
    assert res.unmapped
    assert res.entity_id == raw_id
