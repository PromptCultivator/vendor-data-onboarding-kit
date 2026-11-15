![Vendor Data Onboarding Kit (vdok)](.github/social-preview.jpg)

# Vendor Data Onboarding Kit (`vdok`)

An append-only bitemporal onboarding kit that validates, normalizes, and stores raw vendor feeds in partitioned Parquet storage with zero look-ahead bias, revision tracking, and deterministic readiness reporting.

## Status

`benchmarked on Intel(R) Xeon(R) Platinum 8481C (2 cores, 7.8 GiB RAM)`

## Benchmark Results

Values report median and min–max range across 5 isolated subprocess runs per workload.

| Metric | Synthetic (50 MB) | Synthetic (1 GB) | Synthetic (10 GB) | Synthetic (50 GB) | SEC FSDS (2023q3–2023q4) |
|---|---|---|---|---|---|
| Time from raw drop to report | 8.27 s (7.96–8.45 s) | 56.69 s (55.54–56.81 s) | TBD | TBD | 68.81 s (68.13–88.27 s) |
| Validation throughput | 0.029 GB/s/core (0.028–0.030; 675,641 rows/s, cores=2) | 0.037 GB/s/core (0.036–0.039; 849,928 rows/s, cores=2) | TBD | TBD | 0.053 GB/s/core (0.048–0.053; 634,408 rows/s, cores=2) |
| Store write throughput | 937,370 rows/s (67.4 MB/s, 64.3–68.9) | 1,644,264 rows/s (123.6 MB/s, 77.3–133.5) | TBD | TBD | 421,645 rows/s (41.4 MB/s, 32.6–42.1) |
| Peak resident memory | 630.1 MiB (606.0–653.8 MiB) | 4967.0 MiB (4892.8–5053.1 MiB) | TBD | TBD | 4080.0 MiB (4070.8–4142.9 MiB) |
| Revision detection recall and precision | recall 100.0%, precision 100.0% (25 revisions) | recall 100.0%, precision 100.0% (25 revisions) | TBD | TBD | 15,518 revisions detected (unlabeled) |
| Point-in-time violations | 0 / 100,000 | 0 / 100,000 | TBD | TBD | 0 / 100,000 |
| Leak detector recall | 100.0% (1,000/1,000, 0 false positives) | 100.0% (1,000/1,000, 0 false positives) | TBD | TBD | 100.0% (1,000/1,000, 0 false positives) |
| Storage factor | 0.585× (0.585–0.586×) | 0.580× | TBD | TBD | 0.581× (0.580–0.581×) |

### Per-Stage Wall-Clock Breakdown (Median)

| Stage | Synthetic (50 MB) | Synthetic (1 GB) | SEC FSDS (2023q3–2023q4) |
|---|---|---|---|
| Discovery | 0.252 s (0.249–0.258 s) | 1.322 s (1.258–1.384 s) | 1.765 s (1.758–5.766 s) |
| Schema | 3.910 s (3.755–4.059 s) | 5.038 s (4.876–5.171 s) | 6.714 s (6.459–7.407 s) |
| Normalization | 2.003 s (1.945–2.064 s) | 28.193 s (27.135–28.681 s) | 18.078 s (17.907–26.208 s) |
| Validation | 0.611 s (0.584–0.629 s) | 9.934 s (9.331–10.087 s) | 10.943 s (10.852–12.117 s) |
| Store write | 0.441 s (0.431–0.462 s) | 5.136 s (4.758–8.210 s) | 16.465 s (16.202–20.942 s) |
| Revisions | 0.419 s (0.403–0.422 s) | 4.296 s (3.908–4.363 s) | 9.022 s (8.899–9.535 s) |
| Reporting | 0.311 s (0.304–0.326 s) | 1.692 s (1.623–1.758 s) | 2.104 s (2.058–2.123 s) |
| **Total** | 8.27 s (7.96–8.45 s) | 56.69 s (55.54–56.81 s) | 68.81 s (68.13–88.27 s) |

Full CSV and Markdown outputs are stored in [bench/results/bench.csv](bench/results/bench.csv) and [bench/results/bench.md](bench/results/bench.md).

### Hardware and Environment

```
Hardware & Environment:
  CPU Model: Intel(R) Xeon(R) Platinum 8481C
  CPU Cores: 2
  Memory: 7.8 GiB
  Disk Type: Virtual block store (virtio_blk)
  Operating System: Linux-6.12.105+-x86_64-with-glibc2.36
  Python Version: 3.12.16
  DuckDB Version: 2.0.0
  PyArrow Version: 26.0.1
  Git Commit: 84a7f040b8912605b127413e1a0ae6b60615df79
```

## Quick Start

Requires Python 3.11 or 3.12 and `uv` (or `pip`). Optional `VDOK_CONTACT` sets the `User-Agent` contact email for public SEC EDGAR downloads.

```bash
git clone https://github.com/PromptCultivator/vendor-data-onboarding-kit.git
cd vendor-data-onboarding-kit
make setup
source .venv/bin/activate
```

| Target | Command | Description |
|---|---|---|
| Setup environment | `make setup` | Create `.venv` and install exact pinned dependencies |
| Lint codebase | `make lint` | Run `ruff check` and `ruff format --check` |
| Type check | `make typecheck` | Run `pyright` static analysis |
| Run test suite | `make test` | Run `pytest` with 85% line coverage gate |
| Download raw data | `make data` | Download SEC FSDS archives and `company_tickers.json` |
| Generate synthetic data | `make synth SIZE=1GB SEED=1` | Generate synthetic drops with planted faults |
| Onboard drop | `make onboard VENDOR=sec_fsds DROP=<path>` | Discover, validate, normalize, and store a drop |
| Emit readiness report | `make report VENDOR=sec_fsds DROP=<drop_id>` | Display Markdown or JSON readiness report |
| Run full benchmark | `make bench SIZE=10GB RUNS=5 SEED=1` | Run benchmark suite and update `bench/results/` |
| Run benchmark smoke | `make bench-smoke` | Run 50 MB smoke benchmark asserting zero PIT violations |
| Update golden reports | `make goldens` | Regenerate golden report fixtures |
| Run inside container | `make docker` | Build Docker image and execute `make test` |

## Problem

Vendor data feeds fail in four recurring ways during quantitative onboarding. Schemas drift without notice when vendors add, drop, or widen columns. Entity identifiers shift across tickers, CIKs, and proprietary codes, introducing survivorship bias when mapped inconsistently. Historical observations undergo silent restatements that corrupt backtests if overwritten in place. Row-level defects, duplicate keys, and truncated deliveries break downstream pipelines when they are neither isolated nor flagged before ingestion.

## How It Works

### Pipeline Architecture

```
Raw Vendor Drop (CSV / CSV.GZ / JSONL / JSONL.GZ / Parquet / SEC FSDS)
                 │
                 ▼
  [1. Discovery & Cryptographic Drop ID]
                 │
                 ▼
  [2. Schema Inference & Drift Guard]
                 │
                 ▼
  [3. Canonical Normalization & Symbology]
                 │
                 ▼
  [4. Vectorized Validation Engine (DuckDB)]
   ├── Pass ───────► [5. Append-Only Parquet Store (0o444)]
   ├── Warn ───────► [Warning Metrics in Report]
   ├── Quarantine ─► [quarantine/part-<drop_id>.parquet]
   └── Hold ───────► [held_drops.parquet / Ingestion Aborted]
                 │
                 ▼
  [6. Revision Ledger & Restatement Diffing]
                 │
                 ▼
  [7. Readiness Report (report.md + report.json)]
                 │
                 ▼
  [8. Point-in-Time Query API & Leak Detector]
```

### Canonical Row

Every record is normalized to a 13-column Apache Arrow schema (`CANONICAL_ARROW_SCHEMA`) with fixed-point decimal money values (`decimal128(38, 9)`), UTC microsecond timestamps, and an explicit `unmapped` flag:

```
entity_id:       string              # Canonical identifier or raw ID when unmapped
entity_id_type:  string              # Identifier scheme (cik, ticker, isin, cusip, vendor_id)
field:           string              # Metric identifier joined by "|" (e.g., "Revenues|USD|4Q")
event_time:      timestamp[us, UTC]  # Observation period or event timestamp
knowledge_time:  timestamp[us, UTC]  # Filing or drop receipt visibility timestamp
value_num:       decimal128(38, 9)   # Exact fixed-point numeric value (null if textual)
value_str:       string              # Categorical or text value (null if numeric)
unit:            string              # Canonical unit code (USD, shares, ratio, pure)
drop_id:         string              # 16-char SHA-256 prefix of sorted uncompressed file hashes
source_file:     string              # Relative path within the drop
source_row:      int64               # 1-based row index in the source file
row_hash:        string              # SHA-256 digest of null-byte delimited canonical fields
unmapped:        bool                # True when identifier resolution found no master match
```

### Manifest Example

Vendor feeds are configured declaratively under `manifests/<vendor>.yaml`:

```yaml
vendor: sec_fsds
entity_columns: [cik]
entity_id_type: cik
event_time_column: ddate
event_time_format: "%Y%m%d"
knowledge_time:
  source: column
  column: filed
  format: "%Y%m%d"
timezone: UTC
value_columns: [value]
unit_column: uom
field_columns: [tag]
expected_calendar: quarterly
expected_universe: null
rules: rules/sec_fsds.yaml
```

### Severity Tiers

Validation rules in `rules/<vendor>.yaml` compile into vectorized DuckDB SQL and route records across three tiers:

- **`warn`**: Records anomalies in validation metrics while letting rows enter the store.
- **`quarantine`**: Isolates defective rows into `quarantine/part-<drop_id>.parquet` with an array of all failed rule names (`quarantine_reasons`), allowing conforming rows to proceed.
- **`hold`**: Aborts the drop before any data partition is written and logs a diagnostic record to `metadata/held_drops.parquet`.

### Bitemporal Store

Accepted records are written once to partitioned Parquet files:

```
data/store/<vendor>/
├── store.duckdb
├── <field_bucket>/<event_year>/part-<drop_id>.parquet
├── quarantine/part-<drop_id>.parquet
└── metadata/
    ├── drops.parquet
    ├── held_drops.parquet
    ├── schema_versions.parquet
    └── revisions.parquet
```

Field buckets (`income_statement`, `cash_flow`, `balance_sheet`, `market_prices`, `macro`, `general`) bound directory fanout without per-tag inode explosion. Written partition files are locked read-only (`0o444`).

### Revision Ledger

After each committed drop, a windowed SQL pass compares incoming `(entity_id, field, event_time)` keys against the prior latest knowledge state using `IS DISTINCT FROM`. Changed values or units with a newer `knowledge_time` are appended to `metadata/revisions.parquet`.

### Point-in-Time Query API

`Store` resolves point-in-time queries using per-partition Arrow tables indexed by a 64-bit MD5 key prefix (`_k64`) and `bisect_left` over zero-copy scalar buffers:

```python
from vdok.query import Store

with Store("data/store/sec_fsds", audit=True) as store:
    record = store.value(
        entity_id="0000320193",
        field="Revenues|USD|4Q",
        event_time="2023-12-31",
        as_of="2024-02-15",
    )
    if record:
        print(record["value_num"], record["unit"], record["knowledge_time"])
```

### Leak Detector

When `audit=True`, `Store` logs `(decision_time, as_of, key, knowledge_time_returned)` to `<store_path>/audit/query_audit.jsonl`. `vdok leaks` audits the log and flags reads where `as_of > decision_time` or `knowledge_time_returned > decision_time`:

```bash
vdok leaks --log data/store/sec_fsds/audit/query_audit.jsonl --decision-time-column decision_time
```

## Guarantees

Three structural invariants are enforced in code and verified by named automated tests in [tests/unit/test_guarantees.py](tests/unit/test_guarantees.py):

- **(G1) Store is append-only**: Partition writes verify no target file for `drop_id` exists (`AppendOnlyViolationError`), lock written files to `0o444`, and expose no update or delete methods. Checked by `test_g1_store_is_append_only`.
- **(G2) Point-in-time queries never read future knowledge**: `Store.value()` and `Store.series()` return only rows with `knowledge_time ≤ as_of`, verified against an unpruned streaming reference implementation (`BruteForceStore`). Checked by `test_g2_pit_never_reads_future_knowledge`.
- **(G3) Re-running the same drop adds no new rows**: Re-ingesting a drop with an already committed `drop_id` (computed from uncompressed file payload hashes) exits as a no-op without duplicating rows or writing files. Checked by `test_g3_rerun_same_drop_adds_no_rows`.

## Benchmark Method

- **Workloads**: 50 MB synthetic (CI smoke), 1 GB synthetic, 10 GB and 50 GB synthetic scalability targets, and real SEC FSDS (`2023q3`–`2023q4`).
- **Isolation and Repetitions**: Each benchmark runs 5 iterations inside fresh `--internal-worker` child subprocesses so `resource.getrusage` measures an independent OS process-tree peak RSS and native C++ allocators release memory between runs. Results report median and min–max range.
- **Correctness Checks**: Every iteration executes 100,000 random `Store.value()` queries checked against `BruteForceStore.batch_values_unpruned` (asserting 0 violations), evaluates 1,000 planted leaking reads through `detect_leaks`, and scores revision recall/precision against synthetic ground truth.
- **Unmeasured Factors**: Network download latency and initial OS page-cache state outside the benchmark subprocess are excluded.

## Design Decisions

Architectural decisions are documented in [docs/adr/](docs/adr) and [docs/DECISIONS.md](docs/DECISIONS.md):

| ADR | Title | Optimizes | Costs |
|---|---|---|---|
| [ADR 001](docs/adr/001-bitemporal-append-only.md) | Bitemporal Append-Only Store | Point-in-time reproducibility and zero look-ahead bias | Linear storage growth across vintages (`0.580×`–`0.585×` Parquet vs raw drop text) and bitemporal query indexing |
| [ADR 002](docs/adr/002-severity-tiers.md) | Severity Tiers: Warn, Quarantine, Hold | Availability of conforming rows (`96.75%` admitted on SEC `2023q3`) | Secondary quarantine write path and operational reconciliation |
| [ADR 003](docs/adr/003-schema-pinning.md) | Schema Inference, Pinning, and Drift Detection | Early detection of unannounced vendor schema changes | Explicit operator approval (`vdok schema accept`) on breaking changes |
| [ADR 004](docs/adr/004-local-first-duckdb-parquet.md) | Local-First DuckDB and Parquet Store | Single-command local reproducibility with no external services | Single-node memory/disk bounds; no distributed object-store coordination |
| [ADR 005](docs/adr/005-model-free-identifier-mapping.md) | Model-Free Rule-Based Identifier Mapping | Deterministic auditability and zero false-positive entity joins | Explicit unmapped queue (`28.10%` on SEC `2023q3` non-exchange filers) |

## Limits and Follow-Ups

Explicit non-goals outside the scope of this repository:
- Proprietary or paid vendor connectors requiring commercial credentials.
- User entitlements, authentication, or row-level access control.
- Web UI or interactive dashboards.
- Distributed multi-node execution or cloud object-store consistency protocols.
- Probabilistic, fuzzy, or machine-learning entity resolution and report generation.
- Arrow Flight RPC server endpoints.

## Related Work

- **Great Expectations and Soda**: Provide declarative data quality assertions and validation reports, without bitemporal append-only storage or historical revision ledgers.
- **ArcticDB**: Provides versioned bitemporal time-series storage (BSL 1.1), without vendor schema pinning, multi-tier quarantine routing, or vendor readiness reports.
- **Feast and Feature Stores**: Support point-in-time feature joins for model training and serving, without raw drop discovery, schema drift gating, or vendor inquiry generation.
- **Quanted**: Commercial financial data validation platform, without an open-source local-first CLI and Parquet pipeline.

`vdok` combines schema drift gating, multi-tier validation, an append-only bitemporal Parquet store, a revision ledger, a readiness report, and a point-in-time leak detector in a single local-first pipeline.

## Data

- **SEC Financial Statement Data Sets (FSDS)**: Public domain quarterly XBRL extracts from the U.S. SEC (`sub.txt`, `num.txt`, `tag.txt`, `pre.txt`), downloaded via `make data` with rate limiting (≤ 4 req/s under the 5 req/s ceiling) and SHA-256 sidecar caching in `data/raw/`.
- **SEC Symbol Master**: Public domain `company_tickers.json` from `sec.gov`, stored as immutable `0o444` Parquet snapshots in `data/symbols/`.
- **Committed Test Fixtures**: Only deterministic subsets are committed in [tests/fixtures/](tests/fixtures/README.md) (~20,000 `num` fixture scale across `2023q3`/`2023q4` covering ≥ 50 CIKs and ≥ 20 restatements, a 200-entry symbol master fixture, and seeded synthetic drops).

## Documentation

- [System Specification](docs/SPEC.md)
- [High-Level Design](docs/HIGH_LEVEL_DESIGN.md)
- [Design Decisions Log](docs/DECISIONS.md) (`D-001`–`D-030`)
- [Architecture Decision Records](docs/adr/) (`ADR 001`–`005`)
- [Subsystem Component Guides](docs/components/) (`01`–`12`)

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
