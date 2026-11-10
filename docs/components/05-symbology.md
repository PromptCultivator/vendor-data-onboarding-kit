# Component: Symbol Master & Identifier Resolution (`vdok.symbols`)

## 1. Overview & Responsibility

The `vdok.symbols` module provides point-in-time entity resolution across financial identifiers. Market data vendors utilize conflicting identifier schemes: SEC filings use Central Index Keys (CIK), market feeds use ticker symbols, pricing vendors use CUSIPs or SEDOLs, and global custodians use ISINs. Furthermore, ticker symbols are reused over time as companies rebrand, merge, or dissolve.

`vdok.symbols` executes deterministic, rule-based identifier resolution against a versioned symbol master. Model-based probabilistic entity matching is explicitly excluded to preserve auditability and prevent false join propagation.

---

## 2. Symbol Master Architecture

### 2.1 Data Sources & Versioning
- **Primary Source**: SEC Edgar corporate database (`company_tickers.json`), containing `cik`, `ticker`, and `title`.
- **Physical Format**: Stored as Apache Parquet in `data/symbols/master_v<hash>.parquet`.
- **Version Control**: When an updated reference file arrives via `make data`, its SHA-256 digest is compared against existing versions. If the digest differs, a new master version is registered in the DuckDB table `symbol_master_versions` with an effective activation timestamp.

### 2.2 Schema of the Symbol Master
```
cik:             string                  # 10-digit zero-padded CIK (e.g., "0000320193")
ticker:          string                  # Primary US trading symbol (e.g., "AAPL")
title:           string                  # Official SEC registrant entity name
valid_from:      timestamp[us, UTC]      # Inclusive beginning of applicability interval
valid_to:        timestamp[us, UTC]      # Exclusive end of applicability interval
version_id:      string                  # 16-character SHA-256 master version hash
```

---

## 3. Rule-Based Resolution Logic

Entity resolution follows a strict sequence:

```
Raw Identifier Input (value, id_type, observation_time)
                         │
                         ▼
        [Exact Match on id_type & Interval]
                         │
        ┌────────────────┴────────────────┐
        ▼ Match                           ▼ No Match
 Return Canonical CIK            [Suffix Stripping (Tickers)]
                                 - Strip ".O", ".N", "/WS", etc.
                                 - Re-evaluate Exact Match
                                          │
                         ┌────────────────┴────────────────┐
                         ▼ Match                           ▼ No Match
                  Return Canonical CIK            [Unmapped Isolation]
                                                  - Flag unmapped = true
                                                  - Preserve raw identifier
                                                  - Increment unmapped metric
```

### 3.1 Exchange Suffix Stripping
Market data feeds frequently append vendor-specific exchange codes to equity tickers. The resolution engine normalizes tickers using a documented suffix stripping table:

| Raw Suffix | Exchange Identified | Normalized Symbol |
|---|---|---|
| `AAPL.OQ`, `AAPL.O` | NASDAQ | `AAPL` |
| `IBM.N` | New York Stock Exchange | `IBM` |
| `SPY.P`, `SPY.ARC` | NYSE Arca | `SPY` |
| `BRK/A`, `BRK.A` | Dual Class Share Class A | `BRK.A` |

---

## 4. Unmapped Identifier Handling & Quarantine

When an identifier cannot be resolved against the symbol master:
1. The row is **not dropped** and the ingestion pipeline **does not abort**.
2. The record retains its raw identifier in `entity_id`.
3. An internal metadata flag `unmapped=true` is associated with the row.
4. The readiness report records the occurrence and lists the most frequent unmapped identifiers, giving data operations teams a concrete list for vendor investigation.
5. In downstream queries, unmapped records remain addressable via their raw identifier string.
