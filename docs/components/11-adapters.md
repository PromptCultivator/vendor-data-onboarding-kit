# Component: Ingestion Adapters (`vdok.adapters`)

## 1. Overview & Responsibility

`vdok.adapters` provides domain-specific ingestion and data generation modules alongside the generic manifest-driven pipeline:
1. **`vdok/adapters/sec_fsds.py`**: Public-domain SEC Financial Statement Data Sets (FSDS) downloader, fixture selector, and relational normalizer.
2. **`vdok/adapters/synthetic.py`**: Reproducible multi-drop synthetic dataset generator with configurable size targets and planted fault injection.

---

## 2. SEC Financial Statement Data Sets Adapter (`sec_fsds.py`)

### 2.1 Rate-Limited Acquisition & Atomic Caching
- **`SecRateLimiter`**: Thread-safe (`threading.Lock`) rate limiter enforcing a minimum interval of `0.25` seconds between HTTP requests (≤ 4 req/s, comfortably inside SEC EDGAR's 5 req/s guideline).
- **`get_sec_user_agent`**: Formats `User-Agent: vendor-data-onboarding-kit <contact>` from `--user-agent` or `VDOK_CONTACT`.
- **`download_file`**: Streams downloads to `.tmp` files, computes SHA-256 digests in-flight, verifies against `.sha256` sidecars on cache hits, and atomically replaces targets on completion.
- **`download_sec_fsds` / `download_company_tickers`**: Downloads quarterly archives (`sub.txt`, `num.txt`, `tag.txt`, `pre.txt`) and `company_tickers.json` into `data/raw/sec_fsds/` and `data/symbols/`.

### 2.2 Vectorized Relational Normalization (`normalize_sec_fsds_drop`)
Accepts either extracted directories or `.zip` archives and runs in-process DuckDB SQL joining `num.txt` to `sub.txt` on accession number (`adsh`):
- `entity_id`: 10-digit zero-padded `cik`, suffixed with `|coreg=<name>` when `coreg` is populated.
- `field`: `tag || '|' || uom || '|' || qtrs`.
- `event_time`: `ddate` (`%Y%m%d`) as UTC `timestamp[us, UTC]`.
- `knowledge_time`: `filed` (`%Y%m%d`) as UTC `timestamp[us, UTC]`.
- `value_num`: `value` cast via exact `DECIMAL(38, 9)` without floating-point intermediaries.
- `row_hash`: Null-byte (`\0`) delimited SHA-256 matching `compute_row_hash`.

### 2.3 Fixture Selection (`select_sec_fixtures`)
Samples deterministic subsets across consecutive quarters (`2023q3`, `2023q4`) guaranteeing ≥ 50 CIKs and ≥ 20 ground-truth restatements for offline CI tests.

---

## 3. Synthetic Generator (`synthetic.py`)

`SyntheticGenerator` and `generate_synthetic_sequence` emit reproducible drop sequences (`csv`, `parquet`, or `jsonl`) alongside a ground-truth manifest for benchmarking and planted-fault verification.

Supported planted faults (`--faults default` or comma-separated / dict overrides):
- **`duplicate_row`**: Sub-threshold (`0.5%`, quarantined) or super-threshold (`2.0%`, batch hold) primary-key duplicates in drop 2.
- **`type_flip`**: Non-numeric strings injected into monetary `price` cells in drop 2 (quarantined by `type_flip`).
- **`out_of_range`**: Negative `price` values in drop 2 (quarantined by `out_of_range`).
- **`restatement`**: Altered `price` values on 25 historical keys with advancing `knowledge_time` in drop 2 (ground truth for revision recall and precision).
- **`repeat_drop`**: Byte-identical retransmission of drop 1.
- **`schema_drift`**: Removed required column in drop 3 (held at schema guard).
