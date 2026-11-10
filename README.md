# Vendor Data Onboarding Kit

A toolkit that onboards raw vendor datasets into an append-only bitemporal store and produces vendor readiness reports.

## Status

`Status: design complete`

## Benchmark Results

| Metric | Synthetic (50 MB) | Synthetic (1 GB) | Synthetic (10 GB) | SEC FSDS (4 Quarters) |
|---|---|---|---|---|
| Time from raw drop to report | TBD | TBD | TBD | TBD |
| Validation throughput | TBD | TBD | TBD | TBD |
| Store write throughput | TBD | TBD | TBD | TBD |
| Peak resident memory | TBD | TBD | TBD | TBD |
| Revision detection recall and precision | TBD | TBD | TBD | N/A (unlabeled) |
| Point-in-time violations | TBD | TBD | TBD | TBD |
| Leak detector recall | TBD | TBD | TBD | TBD |
| Storage factor | TBD | TBD | TBD | TBD |

```
Hardware & Environment:
  CPU Model: Intel(R) Xeon(R) Platinum 8481C
  CPU Cores: 2
  Memory: 7.8 GiB
  Disk Type: NVMe / SSD (Virtual block store)
  Operating System: Linux-6.12.105+-x86_64-with-glibc2.36
  Python Version: 3.11.2
  DuckDB Version: 2.0.0
  PyArrow Version: 26.0.1
  Git Commit: pending
```

## Quick Start

The kit provides dedicated `make` targets for standard operations:

| Target | Command | Description |
|---|---|---|
| Setup environment | `make setup` | Create virtual environment and install pinned dependencies |
| Lint codebase | `make lint` | Run ruff check and format check |
| Type check | `make typecheck` | Run pyright static type analysis |
| Run test suite | `make test` | Run pytest with line coverage gate (85%) |
| Download raw data | `make data` | Download SEC FSDS quarterly archives and ticker master |
| Generate synthetic data | `make synth` | Generate synthetic drops with planted faults |
| Onboard drop | `make onboard VENDOR=sec_fsds DROP=path/to/drop` | Validate, normalize, and write to store |
| Emit readiness report | `make report VENDOR=sec_fsds DROP=drop_id` | Generate Markdown and JSON readiness reports |
| Run full benchmark | `make bench SIZE=10GB RUNS=5 SEED=1` | Execute benchmark suite and write results |
| Run benchmark smoke | `make bench-smoke` | 50 MB smoke run checking zero violations |
| Update golden reports | `make goldens` | Regenerate report golden test fixtures |
| Run inside container | `make docker` | Build container image and execute test target |

## Problem

Vendor datasets arrive with unpinned schemas, inconsistent identifier types, and undocumented changes across delivery batches. Historical values undergo silent revisions without explicit notice from data vendors. Downstream quantitative research pipelines fail or produce biased signals when data feeds leak future knowledge or breach delivery service level agreements. Resolving these defects requires point-in-time verification, revision auditing, and automated data quality checks before feeds reach production stores.

## How It Works

### Pipeline Architecture

```
Raw Vendor Drop (CSV / JSONL / Parquet)
                 │
                 ▼
      [Format Discovery & Hash]
                 │
                 ▼
     [Schema Check vs Pinned YAML]
                 │
                 ▼
   [Normalization & Identifier Mapping]
                 │
                 ▼
  [Vectorized Rule Engine (DuckDB SQL)]
   ├── Pass ───────► [Append-Only Parquet Store]
   ├── Warn ───────► [Warning Event Ledger]
   ├── Quarantine ─► [quarantine.parquet]
   └── Hold ───────► [Drop Held / Execution Aborted]
                 │
                 ▼
       [Revision Ledger Engine]
                 │
                 ▼
    [Readiness Report (MD + JSON)]
```

### Canonical Row

Every incoming record normalizes to a canonical Arrow tuple:

```
entity_id: string                  # Mapped identifier or raw value if unmapped
entity_id_type: string             # Identifier type (cik, ticker, isin, cusip)
field: string                      # Combined field tags or metric name
event_time: timestamp[us, UTC]     # Observation timestamp
knowledge_time: timestamp[us, UTC] # Receipt or filing timestamp
value_num: decimal128(38, 9)       # Numeric value (no float conversion for money)
value_str: string                  # Categorical or textual payload
unit: string                       # Normalized unit (USD, shares, ratio)
drop_id: string                    # Hex digest of input file set
source_file: string                # Original filename
source_row: int64                  # Line or row position
row_hash: string                   # SHA-256 digest of key values
```

### Manifest Example

Vendor feeds configure via declarative YAML manifests (`manifests/<vendor>.yaml`):

```yaml
vendor: sec_fsds
entity_columns: [cik]
entity_id_type: cik
event_time_column: ddate
event_time_format: "%Y%m%d"
knowledge_time: {source: column, column: filed, format: "%Y%m%d"}
timezone: UTC
value_columns: [value]
unit_column: uom
field_columns: [tag]
expected_calendar: quarterly
expected_universe: null
rules: rules/sec_fsds.yaml
```

### Severity Tiers

The rule compiler evaluates incoming drops across three explicit severity tiers:
- **Warn**: Records data quality deviations without excluding rows or delaying ingestion.
- **Quarantine**: Diverts non-compliant rows into an isolated `quarantine.parquet` file with error explanations, allowing valid rows to proceed.
- **Hold**: Aborts drop ingestion before writing to the store when batch-level invariants fail (such as schema column removal or anomalous volume shifts).

### Bitemporal Store

Data writes to an append-only Parquet tree partitioned by vendor, field bucket, and event year:
`data/store/<vendor>/<field_bucket>/<event_year>/part-<drop_id>.parquet`
DuckDB provides SQL views over the partition tree alongside relational metadata tables (`drops`, `held_drops`, `schema_versions`, `symbol_master_versions`, and `revisions`).

### Revision Ledger

Following drop insertion, the engine queries for existing `(entity_id, field, event_time)` keys where values changed at a later `knowledge_time`. Detected revisions record prior value, updated value, timestamps, and relative delta into the revision ledger.

### Point-in-Time API

The query engine answers queries without leaking future knowledge:

```python
from vdok.query import Store

store = Store("data/store/sec_fsds")
value = store.value(
    entity_id="0000320193",
    field="Revenues|USD|4Q",
    event_time="2023-12-31",
    as_of="2024-02-15",
)
```

### Leak Detector

When initialized with audit logging (`Store(..., audit=True)`), queries record decision times, requested as-of times, and returned knowledge times. The `vdok leaks` command scans audit logs and flags records where returned data post-dates decision time.

## Guarantees

The kit provides three explicit behavioral guarantees, each checked by dedicated automated tests:

- **(G1) Store is append-only**: File writers reject updates or deletions of existing drop partitions. Checked by `test_g1_store_is_append_only`.
- **(G2) Point-in-time queries never read future knowledge**: Queries filtered by `as_of` time never return records with `knowledge_time > as_of`. Checked by `test_g2_pit_never_reads_future_knowledge`.
- **(G3) Re-running the same drop adds no new rows**: Ingestion of an identical file set is idempotent and skips duplicate writes. Checked by `test_g3_rerun_same_drop_adds_no_rows`.

## Benchmark Method

- **Workload Sizes**: 50 MB (continuous integration smoke check), 1 GB (local evaluation), 10 GB and 50 GB (scalability runs).
- **Repetitions**: 5 runs per configuration, reporting median and min-max range.
- **Hardware Recording**: Captured automatically via `bench/hardware.py` before test execution.
- **Measured Metrics**: Wall-clock duration per stage, validation throughput (GB/s per core and rows/s), write throughput (MB/s and rows/s), peak resident set size (RSS via `getrusage`), revision detection recall and precision, point-in-time violation count (asserted 0 across 100,000 queries), leak detector recall across 1,000 planted leaks, and storage expansion factor.
- **Unmeasured Dimensions**: Network transfer latency and operating system filesystem cache warm-up cycles are excluded from timing measurements.

## Design Decisions

| ADR | Title | Optimizes | Costs |
|---|---|---|---|
| ADR 001 | Bitemporal Append-Only Store | Point-in-time reproducibility | Storage growth and bitemporal query path |
| ADR 002 | Severity Tiers: Warn, Quarantine, Hold | Availability of valid rows | Secondary write path and quarantine reconciliation |
| ADR 003 | Schema Inference, Pinning, and Drift Detection | Early detection of silent vendor structural drift | Manual approval step on schema migrations |
| ADR 004 | Local-First DuckDB and Parquet Store | Single-command local reproducibility | Absence of distributed clustering and cloud object store semantics |
| ADR 005 | Model-Free Rule-Based Identifier Mapping | Auditability and deterministic tracing | Unmapped identifier review queue |

## Limits and Follow-Ups

The following capabilities represent explicit non-goals for this kit:
- Commercial vendor connectors requiring proprietary credentials.
- Entitlement management, user authentication, or data access authorization.
- Graphical user interfaces or web dashboards.
- Distributed cluster computing engines.
- Generative or probabilistic machine-learning models for transformation or mapping.
- Arrow Flight remote RPC services.

## Related Work

- **Great Expectations and Soda**: Focus on validation rules and data quality assertions, without providing bitemporal storage structures or historical revision ledgers.
- **ArcticDB**: Provides versioned time-series storage under a BSL 1.1 license, but does not provide end-to-end vendor drop onboarding, schema drift classification, or vendor readiness reporting.
- **Feast and Feature Stores**: Support point-in-time joins for machine learning feature retrieval, without raw file ingestion validation, quarantine pipelines, or vendor SLA audits.
- **Quanted**: Offers commercial financial data validation software without public open-source local-first pipeline execution.

A revision ledger, vendor readiness report, and point-in-time leak detector combined into a single local-first pipeline.

## Data

- **Primary Source**: U.S. Securities and Exchange Commission Financial Statement Data Sets (SEC FSDS), published quarterly in the public domain.
- **Symbol Master**: SEC company tickers registry (`company_tickers.json`), public domain.
- **Committed Fixtures**: A deterministic subset of approximately 20,000 `num` records and matching `sub` rows from SEC filings, plus a 200-entity ticker master fixture.
- **Downloaded Data**: Raw quarterly archives downloaded via `make data` with declared User-Agent contact information and rate limiting.
- **Optional Sources**: Federal Reserve Economic Data (FRED/ALFRED) vintage series; requires API registration. FRED data is not committed to the repository.

## AI Assistance

Generated files, functions, and manual verification statuses are tracked in [docs/AI-ASSISTANCE.md](docs/AI-ASSISTANCE.md).

## License

Licensed under the Apache License, Version 2.0 (the "License"). See [LICENSE](LICENSE) for the full license text.
