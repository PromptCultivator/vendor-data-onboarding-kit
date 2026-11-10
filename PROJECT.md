# Project: vendor-data-onboarding-kit

## Architecture
The `vendor-data-onboarding-kit` is an institutional-grade research infrastructure and market data platform designed for quant hedge funds and data engineering teams. It standardizes heterogeneous vendor drops into an append-only bitemporal ledger while verifying data quality, tracking silent restatements, and generating automated vendor readiness reports.

```
Raw Vendor Drop (CSV, JSON, Parquet, API dump)
               │
               ▼
   [1. Ingestion & Lineage] ──► drop_manifest.json (SHA-256, source line tracking)
               │
               ▼
     [2. Schema Engine]     ──► Schema Inference ──► Pinned schema.json ──► Drift Detection
               │
               ▼
    [3. Symbology Master]   ──► Point-in-Time Resolution (FB -> META) & Quarantine
               │
               ▼
   [4. Canonical Normalizer]──► (entity_id, event_time, knowledge_time, value, unit)
               │
               ▼
   [5. Validation Engine]   ──► Vectorized Rules (DuckDB/Arrow): Completeness,
               │                Ranges, Duplicates, Monotonicity, Universe Coverage
               ├────────────────────────────┬────────────────────────────┐
               ▼                            ▼                            ▼
   [6. Bitemporal Store]        [7. Revision Ledger]         [8. Readiness Report]
   - Append-only Parquet        - Drop N vs N-1 diffing      - Daily coverage heatmap
   - DuckDB SQL catalog         - Restatement detection      - Validation scorecard
   - As-of (PIT) queries        - Never-overwrite audit      - Vendor email inquiries
   - Zero look-ahead bias       - 100% planted recall        - Markdown + JSON
```

## Feature Inventory
Every feature identified during the survey is assigned to a specific milestone. No feature is unassigned.

| # | Feature ID | Feature Name | Description | Milestone | Source |
|---|---|---|---|---|---|
| 1 | F-ENV-01 | Environment & Packaging | PEP 621 `pyproject.toml` with `uv` virtualenv support, Hatchling build backend, and DuckDB 2.0/PyArrow 26 stack | M1 | survey_1 |
| 2 | F-ING-01 | Multi-Format Ingest Engine | Chunked parsing of CSV, JSON, JSONL, Parquet, and API dumps with transparent compression (gz, zstd) | M1 | survey_2 |
| 3 | F-ING-02 | Lineage & Manifest Tracker | Computes SHA-256 provenance, record counts, and drop metadata (`drop_manifest.json`) | M1 | survey_2 |
| 4 | F-SCH-01 | Schema Inference | Statistical schema inference for types, nullable constraints, and temporal formats | M1 | survey_2 |
| 5 | F-SCH-02 | Schema Pinning Contract | Serializes and enforces versioned `schema.json` contract defining columns, types, and primary keys | M1 | survey_2 |
| 6 | F-SCH-03 | Schema Drift Detection | Classifies schema changes into `BREAKING`, `WARNING`, and `INFO` with structured drift reports | M1 | survey_2 |
| 7 | F-MOD-01 | Canonical Bitemporal Model | Standard tuple `(entity_id, event_time, knowledge_time, value, unit)` with lineage metadata | M2 | survey_2 |
| 8 | F-SYM-01 | Point-in-Time Symbology | Resolves identifiers across corporate action dates (e.g. `FB` -> `META` on 2022-06-09) | M2 | survey_2 |
| 9 | F-SYM-02 | Cross-Identifier Master | Bidirectional cross-reference across Ticker, CUSIP, ISIN, SEDOL, FIGI, and CIK | M2 | survey_2 |
| 10 | F-SYM-03 | Unmapped Entity Quarantine | Synthetic ID generation (`UNRESOLVED_<VENDOR>_<ID>`) preventing pipeline crashes on unmapped symbols | M2 | survey_2 |
| 11 | F-VAL-01 | Completeness Validator | Evaluates null counts and percentages against configurable SLA thresholds | M2 | survey_2 |
| 12 | F-VAL-02 | Range & Domain Check | Identifies negative prices/volumes, invalid enums, and statistical outlier spikes ($> 5\sigma$) | M2 | survey_2 |
| 13 | F-VAL-03 | Intra-Drop Duplicate Check | Detects exact duplicates and flags conflicting values for same `(entity, event_time)` within a drop | M2 | survey_2 |
| 14 | F-VAL-04 | Monotone Timestamp Check | Enforces non-decreasing `event_time` sequences per entity, flagging time inversions | M2 | survey_2 |
| 15 | F-VAL-05 | Future Timestamp Check | Flags records where `event_time > knowledge_time + tolerance` | M2 | survey_2 |
| 16 | F-VAL-06 | Universe Coverage Check | Validates presence against expected benchmark universe (detecting dropouts and unlisted additions) | M2 | survey_2 |
| 17 | F-VAL-07 | Vectorized Rule Execution | Executes validation rules using DuckDB SQL and Arrow compute kernels ($\ge 1.0$ GB/s/core) | M2 | survey_1,2 |
| 18 | F-STR-01 | Append-Only Parquet Store | Partitioned Parquet writes (`dataset/year/month`) with strict physical immutability of historical files | M3 | survey_2 |
| 19 | F-STR-02 | DuckDB Bitemporal Views | Exposes SQL views and macros over partitioned Parquet datasets | M3 | survey_1,2 |
| 20 | F-STR-03 | Point-In-Time ("As-Of") Query | Retrieves latest state known as of `knowledge_time <= T_as_of` with strictly 0 look-ahead bias | M3 | survey_2,3 |
| 21 | F-STR-04 | Full Vintage Audit Query | Returns complete historical revision trajectory `[(v_1, k_1), (v_2, k_2), ...]` for any entity and time | M3 | survey_2,3 |
| 22 | F-STR-05 | Storage Factor Reporter | Calculates and reports storage growth ratio (`bitemporal_size / deduplicated_latest_size`) | M3 | survey_2 |
| 23 | F-DIF-01 | Multi-Drop Diffing Engine | Classifies records between Drop $N$ and Drop $N-1$ into New, Unchanged, Restated, and Deleted | M4 | survey_2,3 |
| 24 | F-DIF-02 | Restatement Detection & Flagging | Flags restatements without in-place mutation; updates revision sequence in append-only ledger | M4 | survey_2,3 |
| 25 | F-DIF-03 | Revision Delta Statistics | Computes magnitude distribution of revisions (absolute $\Delta$ and percentage $\Delta$) | M4 | survey_2,3 |
| 26 | F-REP-01 | Daily Coverage Matrix | Builds entity $\times$ day matrix with anomaly alerts for missing dates or $>20\%$ coverage dips | M4 | survey_2 |
| 27 | F-REP-02 | Validation Scorecard | Summarizes rule outcomes into a pass/warn/fail scorecard with violation sample extracts | M4 | survey_2 |
| 28 | F-REP-03 | Automated Vendor Questions | Converts observed anomalies, schema drifts, and coverage drops into formatted vendor inquiry drafts | M4 | survey_2 |
| 29 | F-REP-04 | Multi-Format Report Emitter | Emits both human-readable Markdown (`readiness_report.md`) and machine-readable JSON (`readiness_report.json`) | M4 | survey_2 |
| 30 | F-DAT-01 | SEC Financials Loader | Ingestion adapter for SEC DERA relational dataset (`sub.txt`, `num.txt`) capturing real 10-K/A restatements | M5 | survey_3 |
| 31 | F-DAT-02 | FRED/ALFRED Vintage Loader | Ingestion adapter for macroeconomic time series with historical vintages (`realtime_start`) | M5 | survey_3 |
| 32 | F-DAT-03 | Scalable Synthetic Generator | Multi-profile generator with configurable anomaly injection, multi-drop restatement simulation, and `truth_manifest.parquet` | M5 | survey_3 |
| 33 | F-CLI-01 | CLI Command Suite | `vendor-kit` CLI with subcommands: `infer`, `pin`, `validate`, `ingest`, `diff`, `query`, `report`, `benchmark` | M5 | survey_2 |
| 34 | F-API-01 | Python Programmatic API | Typed Python API exposing clean entry points for all kit subsystems | M5 | survey_2 |
| 35 | F-FLT-01 | Arrow Flight Read Service | Low-latency zero-copy Arrow Flight bitemporal stream endpoint (Citadel-aligned) | M5 | survey_1,2 |
| 36 | F-E2E-01 | 100% E2E Test Suite Pass | Complete verification across Tiers 1–4 test suite with zero failures | M6 | survey_3 |
| 37 | F-E2E-02 | Tier 5 Adversarial Hardening | Adversarial stress testing, fuzzing, and extreme edge-case validation | M6 | survey_3 |

## Milestones

| # | Name | Scope | Dependencies | Status |
|---|---|---|---|---|
| M1 | Ingestion, Schema & Foundation | F-ENV-01, F-ING-01, F-ING-02, F-SCH-01, F-SCH-02, F-SCH-03 | none | DONE (18612dbeb2cbda500ddee52d1e0e87e4) |
| M2 | Canonical Model, Symbology & Validation | F-MOD-01, F-SYM-01, F-SYM-02, F-SYM-03, F-VAL-01..07 | M1 | DONE (d9f05c0d17485f796db255d55be3e0d8) |
| M3 | Append-Only Bitemporal Store & Query | F-STR-01, F-STR-02, F-STR-03, F-STR-04, F-STR-05 | M1, M2 | DONE (2dd73320e385fddf7b8e00c4c373ebf3) |
| M4 | Diff Engine, Revision Ledger & Readiness Report | F-DIF-01, F-DIF-02, F-DIF-03, F-REP-01, F-REP-02, F-REP-03, F-REP-04 | M2, M3 | DONE (31e7f2f1eaf413eb5b55eaf5478eb137) |
| M5 | Datasets, Benchmarks, CLI & APIs | F-DAT-01, F-DAT-02, F-DAT-03, F-CLI-01, F-API-01, F-FLT-01 | M1, M2, M3, M4 | DONE (fc202d6aacdf137e50ba728b0b1e359b) |
| M6 | E2E Integration Pass & Adversarial Hardening | F-E2E-01 (100% pass on Tiers 1–4), F-E2E-02 (Tier 5 Challenger loop) | M1..M5, TEST_READY.md | DONE (ebbc7bbf44fdd4a9b62ab0c6f01612a6) |

## Interface Contracts

### 1. Ingestion & Schema Engine
```python
@dataclass(frozen=True)
class DropManifest:
    drop_id: str
    vendor_id: str
    dataset_id: str
    file_path: str
    file_sha256: str
    file_size_bytes: int
    row_count: int
    ingest_timestamp: datetime

def infer_schema(file_path: str, sample_rows: int = 10000) -> SchemaDefinition: ...
def pin_schema(schema: SchemaDefinition, output_path: str) -> None: ...
def detect_drift(current_schema: SchemaDefinition, pinned_schema_path: str) -> DriftReport: ...
```

### 2. Canonical Bitemporal Model & Symbology
```python
@dataclass(frozen=True)
class CanonicalRecord:
    entity_id: str              # Internal resolved identifier (e.g. ENT_AAPL)
    event_time: datetime         # Observation / effective timestamp (UTC)
    knowledge_time: datetime     # Timestamp when record was published/ingested (UTC)
    value: float | str | int     # Numeric or categorical fact value
    unit: str                   # Unit of measure (USD, SHARES, RATIO, etc.)
    source_symbol: str          # Original raw identifier (e.g. AAPL, 037833100)
    source_symbol_type: str     # TICKER, CIK, CUSIP, ISIN, VENDOR_ID
    vendor_id: str              # Identifier of the vendor
    dataset_id: str             # Identifier of the dataset
    drop_id: str                # Unique identifier of the drop
    is_restatement: bool = False
    revision_seq: int = 1
```

### 3. Validation Engine
```python
@dataclass
class ValidationResult:
    check_name: str
    passed: bool
    total_records: int
    violation_count: int
    violation_rate: float
    violations_sample: list[dict]
    threshold: float
    error_message: str | None = None

class ValidationEngine:
    def validate(self, table: pa.Table | duckdb.DuckDBPyRelation) -> list[ValidationResult]: ...
```

### 4. Bitemporal Storage & Query Engine
```python
class BitemporalStore:
    def append(self, records: pa.Table, dataset_id: str) -> int: ...
    def as_of_query(self, dataset_id: str, as_of_time: datetime, entity_ids: list[str] | None = None) -> pa.Table: ...
    def audit_trail(self, dataset_id: str, entity_id: str, event_time: datetime) -> pa.Table: ...
    def storage_factor(self, dataset_id: str) -> StorageFactorReport: ...
```

### 5. Diff Engine & Revision Ledger
```python
@dataclass
class DiffSummary:
    drop_id_baseline: str
    drop_id_current: str
    new_records: int
    unchanged_records: int
    restated_records: int
    dropped_records: int
    recall_rate: float

class DiffEngine:
    def diff_drops(self, baseline_table: pa.Table, current_table: pa.Table) -> tuple[pa.Table, DiffSummary]: ...
```

### 6. Readiness Report
```python
class ReadinessReporter:
    def generate_report(self, run_context: PipelineRunContext) -> tuple[str, dict]:
        """Returns (markdown_report_str, json_report_dict)"""
        ...
```

## Code Layout & Write Boundaries
To enforce modularity and ensure no concurrent write conflicts across milestones:

```
vendor-data-onboarding-kit/
├── pyproject.toml              [M1 Worker exclusive]
├── README.md                   [M5 Worker exclusive]
├── .gitignore                  [M1 Worker exclusive]
├── src/
│   └── vendor_onboarding/
│       ├── __init__.py         [M1 Worker exclusive]
│       ├── cli.py              [M5 Worker exclusive]
│       ├── models/             [M1/M2 Worker exclusive]
│       │   ├── __init__.py
│       │   ├── schema.py       [M1 Worker exclusive]
│       │   └── record.py       [M2 Worker exclusive]
│       ├── ingestion/          [M1 Worker exclusive]
│       │   ├── __init__.py
│       │   ├── reader.py
│       │   ├── manifest.py
│       │   └── infer.py
│       ├── symbology/          [M2 Worker exclusive]
│       │   ├── __init__.py
│       │   └── master.py
│       ├── validation/         [M2 Worker exclusive]
│       │   ├── __init__.py
│       │   ├── rules.py
│       │   └── engine.py
│       ├── storage/            [M3 Worker exclusive]
│       │   ├── __init__.py
│       │   ├── store.py
│       │   └── query.py
│       ├── diff/               [M4 Worker exclusive]
│       │   ├── __init__.py
│       │   └── ledger.py
│       ├── report/             [M4 Worker exclusive]
│       │   ├── __init__.py
│       │   ├── coverage.py
│       │   ├── questions.py
│       │   └── generator.py
│       ├── flight/             [M5 Worker exclusive]
│       │   ├── __init__.py
│       │   └── server.py
│       └── datasets/           [M5 Worker exclusive]
│           ├── __init__.py
│           ├── sec.py
│           ├── fred.py
│           └── synthetic.py
├── tests/
│   ├── conftest.py             [E2E Testing Track]
│   ├── test_tier1_features/    [E2E Testing Track]
│   ├── test_tier2_boundaries/  [E2E Testing Track]
│   ├── test_tier3_interactions/[E2E Testing Track]
│   ├── test_tier4_realworld/   [E2E Testing Track]
│   └── test_tier5_adversarial/ [Tier 5 Challenger]
└── benchmarks/
    ├── __init__.py
    └── run_benchmarks.py       [M5 Worker exclusive]
```

## Quantitative Acceptance Metrics
- **Drop-to-Report Latency**: $\le 180\text{ s}$ per 1 GB drop on standard 2-core runner.
- **Validation Throughput**: $\ge 0.5$ to $1.0\text{ GB/s/core}$ (columnar DuckDB / PyArrow execution).
- **Revision Detection Recall**: **100.00%** on planted restatements verified against `truth_manifest.parquet`.
- **Revision Detection Precision**: **100.00%** (zero false positive revisions on unchanged records).
- **Point-in-Time Contamination**: **Strictly 0 violations** ($H_1 == H_2$ in all as-of temporal invariant tests).
- **Storage Expansion Factor**: $\le 1.30\times$ relative to compressed Parquet.
- **Vendor Questions Coverage**: **100.00%** of flagged anomaly types produce actionable inquiry text.
