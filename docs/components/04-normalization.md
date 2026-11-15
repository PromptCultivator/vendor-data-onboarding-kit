# Component: Canonical Normalization (`vdok.normalize`)

## 1. Overview & Responsibility

`vdok.normalize` converts heterogeneous vendor CSV, JSON Lines, and Parquet drops into a single 13-column Apache Arrow schema (`CANONICAL_ARROW_SCHEMA`) with exact fixed-point decimals, UTC microsecond timestamps, canonical units, and cryptographic row fingerprints.

---

## 2. Canonical Schema (`CANONICAL_ARROW_SCHEMA` & `CanonicalRow`)

```
entity_id:       string                  # Resolved canonical entity ID or raw ID if unmapped
entity_id_type:  string                  # cik | ticker | isin | cusip | vendor_id
field:           string                  # Field columns joined by "|", or value column name
event_time:      timestamp[us, UTC]      # Period end or observation timestamp
knowledge_time:  timestamp[us, UTC]      # Filing or batch drop visibility timestamp
value_num:       decimal128(38, 9)       # Exact fixed-point numeric value (null if text)
value_str:       string                  # Textual value (null if numeric)
unit:            string                  # Canonical unit (USD, shares, pure, ratio)
drop_id:         string                  # 16-character drop identifier
source_file:     string                  # Relative path within drop
source_row:      int64                   # 1-based source row index
row_hash:        string                  # SHA-256 fingerprint over \0-delimited canonical fields
unmapped:        bool                    # True if identifier resolution found no master entry
```

---

## 3. Pure Normalizers & Invariants

All scalar normalizers are pure functions verified by Hypothesis property tests (`tests/property/test_normalize_properties.py`):

- **`parse_decimal_exact`**: Unconditionally rejects Python `float` inputs with `TypeError` to prevent IEEE 754 representation drift. Accepts `str`, `int`, or `Decimal`, stripping whitespace and quantizing cleanly to 9 fractional digits.
- **`parse_timestamp_utc`**: Attempts the manifest `strptime` format first, then falls back to `datetime.fromisoformat()` and `dateutil.parser.parse()`. Naive timestamps are localized via standard-library `zoneinfo.ZoneInfo` using the manifest timezone and converted to UTC.
- **`normalize_unit` / `normalize_unit_and_value`**: Maps unit variants (`usd`, `US$`, `dollar`, `dollars`, `$` → `USD`; `shares`, `share`, `shs` → `shares`; `pure`, `scalar`, `dimensionless` → `pure`; `%`, `percent`, `pct` → `ratio` dividing `value_num` by `100` in exact `Decimal` arithmetic).
- **`normalize_identifier`**: Trims whitespace, zero-pads numeric CIKs to 10 digits, and uppercases alphanumeric codes such as tickers.
- **`compute_row_hash`**: Computes `sha256` over `(entity_id, field, event_time_iso, value_num, value_str, unit)` separated by ASCII null bytes (`\0`) so delimiter characters inside field values cannot cause hash collisions.

---

## 4. Batch Normalization & Performance (`normalize_drop`)

- **Drop-Level Knowledge Time Pinning**: When `knowledge_time.source` is `drop_time`, a single UTC timestamp (`default_drop_kt`) is pinned once per drop batch so every row in the transmission shares an identical knowledge coordinate.
- **Vectorized DuckDB Path (`_normalize_drop_vectorized`)**: Large CSV and Parquet feeds execute in-process inside DuckDB with `all_varchar=True` for CSVs, exact `DECIMAL(38, 12)` → `DECIMAL(38, 9)` casting (routing non-numeric strings in `value_columns` to `value_str` with `value_num = NULL` for downstream `type_flip` quarantine), distinct-entity `SymbolMaster` join resolution, and SQL `sha256` computation matching `compute_row_hash` bit-for-bit.
