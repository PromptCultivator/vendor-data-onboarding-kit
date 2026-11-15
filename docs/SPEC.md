# System Specification: `vendor-data-onboarding-kit`

This document specifies the functional requirements, behavioral guarantees, data contracts, CLI surface, and verification standards for `vendor-data-onboarding-kit` (`vdok`).

---

## 1. Purpose and Scope

`vdok` is a self-contained, local-first Python repository that onboards raw vendor datasets into an append-only, point-in-time-correct Parquet store, tracks historical restatements in a revision ledger, produces deterministic vendor readiness reports, and audits downstream reads for look-ahead leaks.

### Explicit Non-Goals

- Proprietary or paid vendor connectors.
- User authentication, entitlements, or access control.
- Web user interfaces or dashboards.
- Distributed multi-node execution or cloud object-store semantics.
- Probabilistic, fuzzy, or machine-learning stages for schema inference, identifier mapping, or report text.
- Arrow Flight or Java RPC servers.

---

## 2. Core Guarantees

The system enforces three testable structural invariants:

1. **Guarantee 1 (G1) — Append-Only Physical Storage**: Partition files are written once and locked read-only (`0o444`). The store exposes no update or delete paths and raises `AppendOnlyViolationError` if a write targets an existing `drop_id`. Verified by `test_g1_store_is_append_only`.
2. **Guarantee 2 (G2) — Strict Point-in-Time Isolation**: `Store.value()` and `Store.series()` queried at `as_of` never return records with `knowledge_time > as_of`. Verified against an unpruned streaming reference oracle (`BruteForceStore`) by `test_g2_pit_never_reads_future_knowledge`.
3. **Guarantee 3 (G3) — Drop Ingestion Idempotency**: Re-onboarding a drop whose `drop_id` is already committed is a no-op that writes no partition files and creates no duplicate rows. Verified by `test_g3_rerun_same_drop_adds_no_rows`.

---

## 3. Repository Layout

```
vendor-data-onboarding-kit/
├── README.md
├── LICENSE                       # Apache-2.0
├── pyproject.toml                # PEP 621 hatchling metadata, exact == pins, `vdok` script
├── Makefile                      # Standard task entry points
├── Dockerfile                    # python:3.12-slim container running `make test`
├── .github/workflows/ci.yml      # Lint, typecheck, test + coverage gate, bench-smoke
├── vdok/
│   ├── __init__.py
│   ├── cli.py                    # Typer CLI entry point
│   ├── manifest.py               # VendorManifest Pydantic v2 configuration model
│   ├── discover.py               # File discovery, format sniffing, payload drop_id hashing
│   ├── schema.py                 # Statistical inference, YAML pinning, drift classification
│   ├── normalize.py              # 13-field canonical Arrow schema, pure & vectorized normalizers
│   ├── symbols.py                # Content-hash-versioned symbol master and rule-based resolver
│   ├── validate.py               # Declarative DuckDB SQL rule compiler and severity router
│   ├── store.py                  # Partitioned Parquet store and DuckDB catalog views
│   ├── revisions.py              # Windowed restatement detection and revision ledger
│   ├── query.py                  # Partition-indexed PIT query API, BruteForceStore, leak detector
│   ├── report.py                 # Readiness report (Markdown + JSON) and drop diff engine
│   ├── onboard.py                # End-to-end onboarding orchestrator
│   └── adapters/
│       ├── sec_fsds.py           # SEC Financial Statement Data Sets downloader & adapter
│       └── synthetic.py          # Seeded multi-drop generator with planted faults
├── manifests/                    # Declarative vendor configurations (YAML)
├── rules/                        # Severity-tiered validation rules (YAML)
├── schemas/                      # Pinned vendor schema contracts (YAML)
├── scripts/                      # Fixture extraction scripts
├── tests/
│   ├── unit/
│   ├── property/
│   ├── golden/
│   └── fixtures/
├── bench/
│   ├── run.py
│   ├── hardware.py
│   └── results/
└── docs/
    ├── SPEC.md
    ├── HIGH_LEVEL_DESIGN.md
    ├── DECISIONS.md
    ├── adr/001-...md ... 005-...md
    └── components/01-...md ... 12-...md
```

---

## 4. Functional Specification

### 4.1 Inputs and Vendor Manifest

Supported raw drop inputs: single files or directories containing `.csv`, `.jsonl`, or `.parquet`, uncompressed or `.gz` compressed (plus SEC FSDS directory/ZIP archives). Mixed formats within a single drop are rejected with `HeterogeneousFormatError`.

Each vendor is configured in `manifests/<vendor>.yaml`:

```yaml
vendor: sec_fsds
entity_columns: [cik]
entity_id_type: cik               # cik | ticker | isin | cusip | vendor_id
event_time_column: ddate
event_time_format: "%Y%m%d"
knowledge_time: {source: column, column: filed, format: "%Y%m%d"}   # or {source: drop_time}
timezone: UTC
value_columns: [value]
unit_column: uom
field_columns: [tag]
expected_calendar: quarterly      # daily | weekly | monthly | quarterly | none
expected_universe: null
rules: rules/sec_fsds.yaml
```

`vdok onboard` supports `--as-of` to supply or override the drop knowledge timestamp when `knowledge_time.source` is `drop_time` (defaulting to UTC ingestion receipt time).

### 4.2 Discovery and Drop Identity

- `discover.scan(path)` inspects magic bytes, validates JSON Lines structure before CSV delimiter sniffing, extracts uncompressed payload streams for `.gz` archives, and raises typed errors (`EmptyDropError`, `HeterogeneousFormatError`, `CorruptedArchiveError`, `FileAccessError`, `UnrecognizedFormatError`).
- `drop_id = sha256(sorted(uncompressed_file_sha256s))[:16]`.
- Re-submitting a committed `drop_id` is detected via `drops.parquet` / `store.duckdb` before validation or writes (G3).

### 4.3 Schema Inference and Pinning

- Samples up to 200,000 rows per file using DuckDB's programmatic relation API (`read_csv`, `read_json`) or PyArrow Parquet readers.
- Infers types (`integer`, `decimal`, `float`, `string`, `date`, `datetime`, `boolean`), nullability, distinct count, and min/max bounds. Monetary columns (`value_columns` or matching monetary names) enforce exact fixed-point `decimal` inference rather than IEEE 754 floats.
- Samples with `≥ 100` non-null values apply a `≤ 1.0%` cell-anomaly tolerance so isolated corrupted cells proceed to row-level `type_flip` quarantine rather than triggering a false batch-level `type_widened` hold.
- First onboarding pins `schemas/<vendor>.yaml`. Subsequent drops classify drift:
  - `removed_column` → `hold`
  - `type_widened` → `hold`
  - `added_column` → `warn`
  - `type_narrowed` → `warn`
  - `nullability_change` → `warn`
- Operators approve legitimate schema evolution with `vdok schema accept <vendor>`.

### 4.4 Canonical Normalization

Every normalized record conforms to `CANONICAL_ARROW_SCHEMA` (and `CanonicalRow` Pydantic model):

```
entity_id:       string
entity_id_type:  string
field:           string
event_time:      timestamp[us, UTC]
knowledge_time:  timestamp[us, UTC]
value_num:       decimal128(38, 9) | null
value_str:       string | null
unit:            string | null
drop_id:         string
source_file:     string
source_row:      int64
row_hash:        string
unmapped:        bool
```

Invariants:
- `parse_decimal_exact` raises `TypeError` on Python `float` inputs; DuckDB text ingestion uses `all_varchar=True` and exact `DECIMAL(38, 9)` casting.
- `parse_timestamp_utc` tries the manifest `strptime` format first and falls back to ISO-8601 and `dateutil.parser.parse`, localizing naive values to the manifest IANA timezone and converting to UTC.
- Drop-level `knowledge_time` defaults are evaluated once per drop batch for temporal coherence.
- Units normalize deterministically (`usd`/`US$`/`dollars` → `USD`, `shares`/`shs` → `shares`, `pure`/`scalar` → `pure`, `%`/`percent`/`pct` → `ratio` with value ÷ 100).
- `row_hash` hashes `(entity_id, field, event_time, value_num, value_str, unit)` separated by ASCII `\0` delimiters.

### 4.5 Validation and Severity Routing

Rules in `rules/<vendor>.yaml` compile into vectorized DuckDB SQL executed once per drop:

- **Row-level (`warn` | `quarantine`)**: `not_null`, `range`, `allowed_values`, `regex_pattern`, `allowed_units`, `decimal_scale` (`ROUND(col, scale) != col` without float coercion), `temporal_bounds` (`[1990-01-01, drop_time + 1 day]`), `temporal_order` (`knowledge_time ≥ event_time`), `sql_expression`. Quarantined rows are routed to `quarantine/part-<drop_id>.parquet` with a `quarantine_reasons: list[string]` array containing all triggered rule names via DuckDB `list_filter`.
- **Batch-level (`hold`)**: `schema_drift` (`removed_column`, `type_widened`), `batch_volume` (row count outside `[0.5, 2.0] ×` trailing median of last 7 drops; first drop exempt), `batch_duplicate_ratio` (key `(entity_id, field, event_time, knowledge_time)` duplicate share `> 1.0%` holds batch; `≤ 1.0%` quarantines duplicate rows via a single materialized `numbered` window view), `byte_identical`, `coverage_gap` (evaluated on conforming rows after row-level quarantine). Held drops write one record to `metadata/held_drops.parquet` and zero partition files.

### 4.6 Identifier Mapping

- `SymbolMaster` loads immutable `0o444` Parquet snapshots (`data/symbols/master_v<version_id>.parquet`) derived from `company_tickers.json` and versioned by 16-hex SHA-256 content hash.
- Resolution is strictly rule-based and deterministic: 10-digit zero-padded CIK matching, uppercase ticker matching with documented exchange suffix stripping (`.OQ`, `.O`, `.N`, `.P`, `.ARC`, etc.) and dual-class separator normalization (`.`/`/` → `-`), and exact whitespace/case-normalized title matching.
- No fuzzy or model-based matching is permitted. Unresolved entities keep their raw normalized identifier with `unmapped=True`.

### 4.7 Bitemporal Store and Revision Ledger

- Conforming rows write to `data/store/<vendor>/<field_bucket>/<event_year>/part-<drop_id>.parquet` (with 4-tier `resolve_field_bucket` taxonomy to bound directory count) and are locked `0o444`.
- `store.duckdb` maintains unified views (`vendor_events`, `quarantine_events`) and metadata tables (`drops`, `held_drops`, `schema_versions`, `symbol_master_versions`, `revisions`).
- Revision detection joins the current drop against `ROW_NUMBER() OVER (PARTITION BY entity_id, field, event_time ORDER BY knowledge_time DESC, drop_id DESC) = 1` over prior drops using `IS DISTINCT FROM` on `(value_num, value_str, unit)` with `curr.knowledge_time > prev.old_knowledge_time`, appending detected changes to `metadata/revisions.parquet`.

### 4.8 Point-in-Time Query API and Leak Detector

- `Store(store_path, audit=False, audit_log_path=None, default_decision_time=None)` exposes `value(entity_id, field, event_time, as_of, decision_time=None)` and `series(entity_id, field, as_of, start=None, end=None)`.
- Partitioned Arrow tables are cached up to 256 partitions / 2 GiB, sorted by `(_k64 ASC, knowledge_time DESC, drop_id DESC)` where `_k64` is a 64-bit MD5 prefix of `(entity_id, field, event_time)`, and searched via `bisect_left` over zero-copy `memoryview` buffers.
- `BruteForceStore.batch_values_unpruned` streams every partition file via `ParquetFile.iter_batches` with no pruning or indexing as the reference oracle.
- `Store(..., audit=True)` logs reads to `<store_path>/audit/query_audit.jsonl`. `detect_leaks` and `vdok leaks` flag reads where `as_of > decision_time` or `knowledge_time_returned > decision_time`.

### 4.9 Readiness Report and Diff

- `vdok onboard` and `vdok report` emit `data/results/<vendor>/<drop_id>/report.md` and `report.json` with deterministic multi-key sorting (`(-rate, field_name)`), verdict (`READY`, `READY WITH WARNINGS`, `HELD`), drop summary, schema status, calendar coverage, validation counts, identifier mapping share, revision analytics (including stale-vendor zero-revision alerts across > 3 drops), template-driven vendor inquiries, and per-stage timings/RSS.
- `vdok diff <vendor> <drop_a> <drop_b>` compares two drops for added keys, removed keys, restated values, and schema differences.

---

## 5. Datasets and Adapters

- **SEC FSDS (`vdok.adapters.sec_fsds`)**: Downloads quarterly archives and `company_tickers.json` via `SecRateLimiter` (≤ 4 req/s under the 5 req/s SEC ceiling) with `User-Agent: vendor-data-onboarding-kit <contact>` and atomic `.tmp`/`.sha256` caching. Joins `num` to `sub` on `adsh` in vectorized DuckDB SQL, zero-pads `cik` (suffixing `|coreg=<name>` when `coreg` is set), maps `tag|uom|qtrs` to `field`, `ddate` to `event_time`, and `filed` to `knowledge_time`.
- **Synthetic Generator (`vdok.adapters.synthetic`)**: Generates reproducible seeded drop sequences across configurable sizes (50 MB, 1 GB, 10 GB, 50 GB) and formats (`csv`, `parquet`, `jsonl`) with ground-truth manifests for planted `restatement`, `duplicate_row`, `type_flip`, `out_of_range`, `repeat_drop`, and `schema_drift` faults.

---

## 6. Dependencies and Environment

Runtime dependencies (exact `==` pinned in `pyproject.toml`):
- `duckdb==2.0.0`, `pyarrow==26.0.1`, `polars==2.1.0`, `pydantic==2.14.3`, `typer==0.29.0`, `pyyaml==6.0.3`, `python-dateutil==4.0.0`.

Development dependencies:
- `pytest==9.2.0`, `pytest-cov==7.1.0`, `hypothesis==6.170.15`, `ruff==0.17.4`, `pyright==1.1.414`.

Standard library `zoneinfo` handles all timezone conversions without `pytz`.

---

## 7. Verification and Benchmark Standards

- **Tests**: Unit tests per module, Hypothesis property tests (`test_normalize_properties.py`, `test_schema_properties.py`, `test_store_properties.py`, `test_symbols_properties.py`), golden Markdown report tests (`tests/golden/`), planted-fault 50 MB tests (`test_planted_faults_50mb.py`), planted-leak tests (`test_planted_leak.py`), planted-restatement tests (`test_planted_restatements.py`), SEC fixture tests (`test_sec_fsds.py`: ≥ 50 CIKs, ≥ 20 revisions, 0 PIT violations, > 95% mapped share), and named guarantee tests (`test_g1_store_is_append_only`, `test_g2_pit_never_reads_future_knowledge`, `test_g3_rerun_same_drop_adds_no_rows`). All tests run completely offline on committed fixtures with an 85% minimum line coverage gate.
- **Benchmarks**: `bench/run.py` runs each iteration in an isolated `--internal-worker` subprocess to record independent `(RUSAGE_SELF + RUSAGE_CHILDREN)` peak RSS and reports median and min–max ranges for stage timings, validation throughput, store write throughput, revision recall/precision, 100,000 PIT query verification against `BruteForceStore` (must be 0 violations), 1,000 planted leak recall, and storage factor in `bench/results/bench.csv` and `bench/results/bench.md`.
