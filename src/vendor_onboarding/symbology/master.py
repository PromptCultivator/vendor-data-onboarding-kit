"""Symbology master catalog, point-in-time cross-identifier resolution, and quarantine ledger."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass(frozen=True)
class SymbologyMapping:
    """Point-in-Time mapping between canonical entity ID and market identifiers.

    Attributes:
        entity_id: Canonical internal entity identifier.
        ticker: Exchange ticker symbol.
        cik: SEC Central Index Key (unpadded or zero-padded).
        cusip: Committee on Uniform Securities Identification Procedures identifier.
        isin: International Securities Identification Number.
        sedol: Stock Exchange Daily Official List identifier.
        figi: Financial Instrument Global Identifier.
        effective_start: Timestamp when this mapping became effective (inclusive, UTC).
        effective_end: Timestamp when this mapping ceased to be effective (exclusive, UTC).
    """

    entity_id: str
    ticker: str
    cik: str
    cusip: str
    isin: str
    sedol: str
    figi: str
    effective_start: datetime
    effective_end: datetime = datetime(2099, 12, 31, tzinfo=UTC)

    def __post_init__(self) -> None:
        object.__setattr__(self, "cik", str(self.cik))
        if self.effective_start.tzinfo is None:
            object.__setattr__(
                self, "effective_start", self.effective_start.replace(tzinfo=UTC)
            )
        else:
            object.__setattr__(
                self, "effective_start", self.effective_start.astimezone(UTC)
            )

        if self.effective_end.tzinfo is None:
            object.__setattr__(
                self, "effective_end", self.effective_end.replace(tzinfo=UTC)
            )
        else:
            object.__setattr__(
                self, "effective_end", self.effective_end.astimezone(UTC)
            )


@dataclass
class QuarantineRecord:
    """Audit ledger entry for unmapped symbols routed to quarantine.

    Attributes:
        synthetic_id: Generated synthetic identifier (e.g. UNRESOLVED_<VENDOR>_<SYMBOL>).
        raw_symbol: Original unmapped symbol from feed.
        source_type: Identifier classification (TICKER, CIK, etc.).
        vendor_id: Vendor source identifier.
        quarantine_time: Timestamp when quarantine occurred (UTC).
        resolved_entity_id: Populated if retroactively resolved to a canonical ID.
        reason: Quarantine reason explanation code.
    """

    synthetic_id: str
    raw_symbol: str
    source_type: str
    vendor_id: str
    quarantine_time: datetime
    resolved_entity_id: str | None = None
    reason: str = "UNMAPPED_SYMBOL"

    def __post_init__(self) -> None:
        if self.quarantine_time.tzinfo is None:
            self.quarantine_time = self.quarantine_time.replace(tzinfo=UTC)
        else:
            self.quarantine_time = self.quarantine_time.astimezone(UTC)


def _cik_matches(m_cik: str | int, s_cik: str | int) -> bool:
    """Matches SEC CIK strings or integers, tolerating leading zeros."""
    m = str(m_cik).strip()
    s = str(s_cik).strip()
    if m == s:
        return True
    if m.isdigit() and s.isdigit():
        return int(m) == int(s)
    return False


ALLOWED_IDENTIFIER_FIELDS = {
    "entity_id",
    "ticker",
    "cik",
    "cusip",
    "isin",
    "sedol",
    "figi",
}


class SymbologyMaster:
    """Authoritative Point-in-Time symbology resolution and quarantine manager."""

    def __init__(self) -> None:
        self.mappings: list[SymbologyMapping] = []
        self.quarantine_ledger: list[QuarantineRecord] = []

    def add_mapping(self, mapping: SymbologyMapping) -> None:
        """Appends a new symbology mapping to the master catalog."""
        self.mappings.append(mapping)

    def register_mapping(self, mapping: SymbologyMapping) -> None:
        """Alias for add_mapping."""
        self.add_mapping(mapping)

    def _normalize_as_of(self, as_of: datetime) -> datetime:
        if as_of.tzinfo is None:
            return as_of.replace(tzinfo=UTC)
        return as_of.astimezone(UTC)

    def resolve(
        self,
        symbol: str,
        symbol_type: str,
        as_of: datetime,
        vendor_id: str = "SYNTH",
    ) -> str:
        """Resolves raw symbol to canonical entity_id as of a specific point-in-time.

        If unmapped, generates a deterministic synthetic quarantine ID and logs
        an entry to self.quarantine_ledger.
        """
        as_of_utc = self._normalize_as_of(as_of)
        st = symbol_type.upper().strip()
        clean_sym = str(symbol).strip()
        lookup_sym = (
            clean_sym.upper()
            if st in ("TICKER", "CUSIP", "ISIN", "SEDOL", "FIGI")
            else clean_sym
        )

        for m in self.mappings:
            if m.effective_start <= as_of_utc < m.effective_end and (
                (st == "TICKER" and m.ticker == lookup_sym)
                or (st == "CIK" and _cik_matches(m.cik, clean_sym))
                or (st == "CUSIP" and m.cusip == lookup_sym)
                or (st == "ISIN" and m.isin == lookup_sym)
                or (st == "SEDOL" and m.sedol == lookup_sym)
                or (st == "FIGI" and m.figi == lookup_sym)
                or (st in ("ENTITY_ID", "ENTITY") and m.entity_id == clean_sym)
            ):
                return m.entity_id

        # Unmapped entity -> Quarantine
        quarantine_sym = clean_sym.replace("UNKNOWN_CORP_", "").replace("GHOST_TICKER_", "")
        synthetic_id = f"UNRESOLVED_{vendor_id}_{quarantine_sym}"
        self.quarantine_ledger.append(
            QuarantineRecord(
                synthetic_id=synthetic_id,
                raw_symbol=str(symbol),
                source_type=symbol_type,
                vendor_id=vendor_id,
                quarantine_time=as_of_utc,
                reason="UNMAPPED_SYMBOL",
            )
        )
        return synthetic_id

    def lookup_cik_by_ticker(self, ticker: str, as_of: datetime) -> str | None:
        """Looks up active CIK corresponding to ticker as of date."""
        as_of_utc = self._normalize_as_of(as_of)
        clean_ticker = str(ticker).strip().upper()
        for m in self.mappings:
            if m.ticker == clean_ticker and m.effective_start <= as_of_utc < m.effective_end:
                return m.cik
        return None

    def lookup_ticker_by_cik(self, cik: str, as_of: datetime) -> str | None:
        """Looks up active ticker corresponding to CIK as of date."""
        as_of_utc = self._normalize_as_of(as_of)
        for m in self.mappings:
            if _cik_matches(m.cik, cik) and m.effective_start <= as_of_utc < m.effective_end:
                return m.ticker
        return None

    def cross_reference(
        self,
        from_symbol: str,
        from_type: str,
        to_type: str,
        as_of: datetime,
    ) -> str | None:
        """Cross-references between any supported identifier types as of date."""
        target_attr = to_type.lower().strip()
        if target_attr in ("entity", "entity_id"):
            target_attr = "entity_id"

        if target_attr not in ALLOWED_IDENTIFIER_FIELDS:
            return None

        as_of_utc = self._normalize_as_of(as_of)
        st = from_type.upper().strip()
        clean_sym = str(from_symbol).strip()
        lookup_sym = (
            clean_sym.upper()
            if st in ("TICKER", "CUSIP", "ISIN", "SEDOL", "FIGI")
            else clean_sym
        )

        for m in self.mappings:
            if m.effective_start <= as_of_utc < m.effective_end and (
                (st == "TICKER" and m.ticker == lookup_sym)
                or (st == "CIK" and _cik_matches(m.cik, clean_sym))
                or (st == "CUSIP" and m.cusip == lookup_sym)
                or (st == "ISIN" and m.isin == lookup_sym)
                or (st == "SEDOL" and m.sedol == lookup_sym)
                or (st == "FIGI" and m.figi == lookup_sym)
                or (st in ("ENTITY_ID", "ENTITY") and m.entity_id == clean_sym)
            ):
                return getattr(m, target_attr, None)
        return None

    def bulk_resolve(
        self,
        symbols: list[tuple[str, str]],
        as_of: datetime,
        vendor_id: str = "SYNTH",
    ) -> list[str]:
        """Resolves a list of (symbol, symbol_type) pairs."""
        return [self.resolve(sym, stype, as_of, vendor_id=vendor_id) for sym, stype in symbols]

    def get_quarantine_records(
        self, vendor_id: str | None = None
    ) -> list[QuarantineRecord]:
        """Returns quarantine ledger entries, optionally filtered by vendor_id."""
        if vendor_id is None:
            return list(self.quarantine_ledger)
        return [q for q in self.quarantine_ledger if q.vendor_id == vendor_id]
