# Component: Symbol Master & Identifier Resolution (`vdok.symbols`)

## 1. Overview & Responsibility

`vdok.symbols` builds immutable, content-hash-versioned entity master snapshots from public SEC `company_tickers.json` data and resolves vendor identifiers using strictly deterministic rules. Probabilistic, fuzzy, and model-based entity matching are prohibited to prevent silent false-positive joins.

---

## 2. Symbol Master Storage & Versioning (`build_symbol_master`)

- **Source**: SEC `company_tickers.json` (`cik_str`, `ticker`, `title`).
- **Versioning**: Computes `version_id = sha256(raw_json_bytes)[:16]`.
- **Storage**: Writes `data/symbols/master_v<version_id>.parquet` with schema `SymbolRecord` (`cik`, `ticker`, `title`, `valid_from`, `valid_to`, `version_id`) and locks the file read-only (`0o444`).
- **Metadata**: Records `SymbolMasterMetadata` (`version_id`, `source_hash`, `entry_count`, `parquet_path`, `created_at`, `effective_date`).

---

## 3. Deterministic Resolution Rules (`SymbolMaster.resolve`)

`SymbolMaster.resolve(raw_id, id_type, as_of=None)` returns `ResolutionResult(entity_id, unmapped)`:

1. **CIK (`cik`)**: Strips whitespace/non-digits, zero-pads to 10 characters, and checks exact membership in the master CIK set.
2. **Ticker (`ticker`)**: Folds to uppercase, checks exact ticker match, then strips documented exchange suffixes (`.OQ`, `.O`, `.N`, `.P`, `.ARC`, `.A`, `.B`, `.C`, `.M`, `.X`, etc.) and translates dual-class separators (`.` or `/` → `-`), re-checking exact membership.
3. **Company Name (`name` / `title` / `vendor_id`)**: Normalizes whitespace and case (`_normalize_title_key`) and performs exact title lookup only.
4. **ISIN / CUSIP**: Normalizes whitespace and uppercase; resolves when an exact mapping exists, otherwise flags `unmapped`.

---

## 4. Unmapped Handling

When no exact rule matches:
- The row is retained in the pipeline (never dropped or guessed).
- `entity_id` keeps the normalized raw identifier and `unmapped` is set to `True`.
- `vdok.report` tracks overall mapped share and lists the top unmapped identifiers in the readiness report for operator investigation.
