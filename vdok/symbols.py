"""Symbol master and deterministic identifier resolution.

Constructs and versions reference symbol masters from SEC company ticker datasets,
persisting immutable Apache Parquet snapshots, and provides deterministic rule-based
identifier resolution across Central Index Keys (CIK), ticker symbols with exchange
suffix stripping and dual-class share translations, and corporate titles.
Probabilistic and heuristic entity matching algorithms are prohibited.
"""

from __future__ import annotations

import datetime
import hashlib
import json
from datetime import timezone
from pathlib import Path
from typing import Any, NamedTuple

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, ConfigDict, Field

from vdok.manifest import EntityIdType

SYMBOL_MASTER_ARROW_SCHEMA = pa.schema(
    [
        pa.field("cik", pa.string(), nullable=False),
        pa.field("ticker", pa.string(), nullable=False),
        pa.field("title", pa.string(), nullable=False),
        pa.field("valid_from", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("valid_to", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("version_id", pa.string(), nullable=False),
    ]
)

EXCHANGE_SUFFIXES: tuple[str, ...] = (
    ".OQ",
    ".ARC",
    ".ASE",
    ".PH",
    ".MW",
    ".AM",
    ".WS",
    ".AR",
    "/WS",
    ".O",
    ".N",
    ".P",
    ".A",
    ".B",
    ".C",
    ".M",
    ".X",
)


class SymbolRecord(BaseModel):
    """Single corporate entity record in the symbol master catalog."""

    model_config = ConfigDict(frozen=True)

    cik: str = Field(..., pattern=r"^\d{10}$")
    ticker: str = Field(..., min_length=1)
    title: str = Field(..., min_length=1)
    valid_from: datetime.datetime
    valid_to: datetime.datetime | None = None
    version_id: str = Field(..., min_length=1)


class SymbolMasterMetadata(BaseModel):
    """Metadata describing a versioned symbol master snapshot."""

    model_config = ConfigDict(frozen=True)

    version_id: str
    source_hash: str
    entry_count: int
    parquet_path: Path
    created_at: datetime.datetime
    effective_date: datetime.datetime


class ResolutionResult(NamedTuple):
    """Result of resolving an entity identifier against the symbol master."""

    entity_id: str
    unmapped: bool


def _normalize_title_key(title: str) -> str:
    """Normalize corporate title string for exact comparison."""
    return " ".join(title.strip().upper().split())


def build_symbol_master(
    source_path: Path | str,
    output_dir: Path | str = "data/symbols",
    effective_date: datetime.datetime | None = None,
) -> Path:
    """Build and persist an immutable Parquet symbol master from a JSON reference file.

    Computes a cryptographic version identifier from the raw source payload.
    If the target Parquet snapshot already exists, returns the existing path
    without redundant re-generation.
    """
    src = Path(source_path)
    if not src.is_file():
        raise FileNotFoundError(f"Symbol source file not found: {src}")

    raw_bytes = src.read_bytes()
    source_hash = hashlib.sha256(raw_bytes).hexdigest()
    version_id = source_hash[:16]

    out_dir = Path(output_dir)
    target_path = out_dir / f"master_v{version_id}.parquet"
    if target_path.exists():
        return target_path

    payload = json.loads(raw_bytes.decode("utf-8"))
    raw_entries: list[dict[str, Any]]
    if isinstance(payload, dict):
        raw_entries = list(payload.values())
    elif isinstance(payload, list):
        raw_entries = payload
    else:
        raise ValueError(f"Unrecognized symbol reference JSON structure: {type(payload)}")

    if effective_date is None:
        valid_from_dt = datetime.datetime.now(timezone.utc)
    elif effective_date.tzinfo is None:
        valid_from_dt = effective_date.replace(tzinfo=timezone.utc)
    else:
        valid_from_dt = effective_date.astimezone(timezone.utc)

    ciks: list[str] = []
    tickers: list[str] = []
    titles: list[str] = []
    valid_froms: list[datetime.datetime] = []
    valid_tos: list[datetime.datetime | None] = []
    version_ids: list[str] = []

    for item in raw_entries:
        raw_cik = item.get("cik_str", item.get("cik"))
        if raw_cik is None:
            continue
        cik_digits = "".join(ch for ch in str(raw_cik) if ch.isdigit())
        if not cik_digits:
            continue
        cik_str = cik_digits.zfill(10)

        raw_ticker = str(item.get("ticker", "")).strip().upper()
        raw_title = str(item.get("title", "")).strip()
        if not raw_ticker or not raw_title:
            continue

        ciks.append(cik_str)
        tickers.append(raw_ticker)
        titles.append(raw_title)
        valid_froms.append(valid_from_dt)
        valid_tos.append(None)
        version_ids.append(version_id)

    if not ciks:
        raise ValueError(f"Zero valid corporate entries found in reference source: {src}")

    table = pa.Table.from_arrays(
        [
            pa.array(ciks, type=pa.string()),
            pa.array(tickers, type=pa.string()),
            pa.array(titles, type=pa.string()),
            pa.array(valid_froms, type=pa.timestamp("us", tz="UTC")),
            pa.array(valid_tos, type=pa.timestamp("us", tz="UTC")),
            pa.array(version_ids, type=pa.string()),
        ],
        schema=SYMBOL_MASTER_ARROW_SCHEMA,
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, target_path)

    # Set file read-only to preserve immutability
    target_path.chmod(0o444)
    return target_path


class SymbolMaster:
    """In-memory symbol master resolution engine."""

    def __init__(
        self,
        table: pa.Table,
        version_id: str,
        path: Path | None = None,
    ) -> None:
        self._table = table
        self._version_id = version_id
        self._path = path

        self._ciks: set[str] = set()
        self._tickers_to_cik: dict[str, str] = {}
        self._titles_to_cik: dict[str, str] = {}
        self._records: list[SymbolRecord] = []

        cik_col = table["cik"].to_pylist()
        ticker_col = table["ticker"].to_pylist()
        title_col = table["title"].to_pylist()
        vf_col = table["valid_from"].to_pylist()
        vt_col = table["valid_to"].to_pylist()
        ver_col = table["version_id"].to_pylist()

        for cik, ticker, title, vf, vt, ver in zip(
            cik_col, ticker_col, title_col, vf_col, vt_col, ver_col, strict=True
        ):
            self._ciks.add(cik)
            self._tickers_to_cik[ticker] = cik
            norm_title = _normalize_title_key(title)
            self._titles_to_cik[norm_title] = cik
            self._records.append(
                SymbolRecord(
                    cik=cik,
                    ticker=ticker,
                    title=title,
                    valid_from=vf,
                    valid_to=vt,
                    version_id=ver,
                )
            )

    @property
    def version_id(self) -> str:
        """Return the active symbol master version hash."""
        return self._version_id

    @property
    def size(self) -> int:
        """Return the total number of registered corporate records."""
        return len(self._records)

    @classmethod
    def load(cls, parquet_path: Path | str) -> SymbolMaster:
        """Load symbol master snapshot from an Apache Parquet file."""
        p = Path(parquet_path)
        if not p.is_file():
            raise FileNotFoundError(f"Symbol master Parquet file not found: {p}")
        table = pq.read_table(p)
        version_col = table["version_id"].to_pylist()
        version_id = version_col[0] if version_col else "unknown"
        return cls(table=table, version_id=version_id, path=p)

    def resolve(
        self,
        raw_id: str,
        id_type: str | EntityIdType,
        as_of: datetime.datetime | None = None,
    ) -> ResolutionResult:
        """Resolve a raw entity identifier to a canonical Central Index Key (CIK).

        Performs deterministic, rule-based matching without probabilistic or fuzzy
        heuristics. Unresolved identifiers return the raw identifier with unmapped=True.
        """
        cleaned = raw_id.strip()
        if not cleaned:
            return ResolutionResult(raw_id, True)

        type_str = (id_type.value if isinstance(id_type, EntityIdType) else str(id_type)).lower()

        # If as_of is provided, verify point-in-time interval validity
        if as_of is not None and as_of.tzinfo is None:
            as_of_utc = as_of.replace(tzinfo=timezone.utc)
        elif as_of is not None:
            as_of_utc = as_of.astimezone(timezone.utc)
        else:
            as_of_utc = None

        if type_str == "cik":
            digits = "".join(ch for ch in cleaned if ch.isdigit())
            if not digits:
                return ResolutionResult(raw_id, True)
            candidate = digits.zfill(10)
            if candidate in self._ciks:
                if as_of_utc is not None and not self._is_active(candidate, as_of_utc):
                    return ResolutionResult(raw_id, True)
                return ResolutionResult(candidate, False)
            return ResolutionResult(raw_id, True)

        if type_str == "ticker":
            ticker_upper = cleaned.upper()

            # 1. Direct match on uppercase ticker
            if ticker_upper in self._tickers_to_cik:
                cik = self._tickers_to_cik[ticker_upper]
                if as_of_utc is None or self._is_active(cik, as_of_utc):
                    return ResolutionResult(cik, False)

            # 2. Dual class punctuation translation (. or / -> -)
            if "." in ticker_upper or "/" in ticker_upper:
                translated = ticker_upper.replace(".", "-").replace("/", "-")
                if translated in self._tickers_to_cik:
                    cik = self._tickers_to_cik[translated]
                    if as_of_utc is None or self._is_active(cik, as_of_utc):
                        return ResolutionResult(cik, False)

            # 3. Exchange suffix stripping
            for suffix in EXCHANGE_SUFFIXES:
                if ticker_upper.endswith(suffix):
                    base = ticker_upper[: -len(suffix)]
                    if base in self._tickers_to_cik:
                        cik = self._tickers_to_cik[base]
                        if as_of_utc is None or self._is_active(cik, as_of_utc):
                            return ResolutionResult(cik, False)
                    # Suffix stripped with dual class translation
                    base_translated = base.replace(".", "-").replace("/", "-")
                    if base_translated in self._tickers_to_cik:
                        cik = self._tickers_to_cik[base_translated]
                        if as_of_utc is None or self._is_active(cik, as_of_utc):
                            return ResolutionResult(cik, False)

            return ResolutionResult(raw_id, True)

        if type_str in ("name", "title"):
            norm_key = _normalize_title_key(cleaned)
            if norm_key in self._titles_to_cik:
                cik = self._titles_to_cik[norm_key]
                if as_of_utc is None or self._is_active(cik, as_of_utc):
                    return ResolutionResult(cik, False)
            return ResolutionResult(raw_id, True)

        # ISIN, CUSIP, VENDOR_ID, or other schemes cannot be resolved against SEC CIK tables
        return ResolutionResult(raw_id, True)

    def _is_active(self, cik: str, as_of_utc: datetime.datetime) -> bool:
        """Check whether a CIK record was valid as of a specific UTC timestamp."""
        for rec in self._records:
            if rec.cik == cik:
                if rec.valid_from <= as_of_utc:
                    if rec.valid_to is None or as_of_utc < rec.valid_to:
                        return True
        return False
