# System Architecture & High-Level Design (`vdok`)

## 1. Executive Summary

`vendor-data-onboarding-kit` (`vdok`) is a research infrastructure and market data platform designed for quantitative investment management. The system automates the ingestion, validation, normalization, and bitemporal storage of heterogeneous vendor datasets (such as SEC financial statement filings, economic data releases, and alternative data feeds).

Quantitative research pipelines often suffer from data integrity failures:
1. **Unannounced Schema Modifications**: Vendors alter column names, add fields, or alter data representations without prior notification.
2. **Identifier Instability**: Ticker symbols change during corporate reorganizations, acquisitions, and ticker reassignments, causing survivor bias.
3. **Silent Historical Restatements**: Financial and macroeconomic metrics undergo retroactive revision without notification, leading to look-ahead bias if historical records are modified in place.
4. **Data Quality Degradation**: Missing records, corrupted values, duplicate entries, or non-monotonic timestamps degrade model training and execution.

`vdok` addresses these operational risks through an append-only architecture that enforces immutable records, verifies strict point-in-time temporal consistency, isolates anomalies using multi-tier severity routing, and emits deterministic readiness reports.

---

## 2. Core Guarantees

The architecture enforces three structural guarantees verified by automated test suites:

- **Guarantee 1 (G1) — Append-Only Physical Storage**: The underlying data store operates without update or delete operations. Incoming data files are written once. Re-writing to an existing partition is rejected.
- **Guarantee 2 (G2) — Strict Point-in-Time Isolation**: Queries evaluated as of historical knowledge timestamp K never expose values whose visibility timestamp occurred after K. This eliminates look-ahead bias in backtesting.
- **Guarantee 3 (G3) — Drop Ingestion Idempotency**: Processing an identical drop repeatedly results in a no-operation state without duplicating rows or creating redundant storage files.

---

## 3. High-Level Architecture & End-to-End Pipeline

```
Raw Vendor Drop (CSV, JSON Lines, Parquet)
                    │
                    ▼
       [1. Discovery & Fingerprint]
      - SHA-256 Digest & Drop ID
      - Format & Compression Inspection
                    │
                    ▼
       [2. Schema & Contract Guard]
      - Statistical Type Sampling
      - Pinned YAML Contract
      - Breaking Drift Classification
                    │
                    ▼
      [3. Symbology & Normalization]
      - Interval-Aware Identifier Resolution
      - Unmapped Identifier Quarantine
      - Canonical Decimal & UTC Normalization
                    │
                    ▼
         [4. Validation Engine]
      - DuckDB Vectorized Rule Evaluation
      - Row-Level Quarantine Routing
      - Batch-Level Hold Enforcement
                    │
        ┌───────────┴───────────┐
        ▼                       ▼
   Batch HELD           Batch ACCEPTED
  (Store Halts)                 │
                    ┌───────────┴───────────┐
                    ▼                       ▼
        [5. Append-Only Store]     [6. Revision Ledger]
       - Partitioned Parquet      - Drop N vs N-1 Diffing
       - Write-Once Immutability  - Restatement Extraction
                    │                       │
                    └───────────┬───────────┘
                                ▼
                   [7. Readiness Reporting]
                  - Coverage Matrix & Gaps
                  - Validation Anomaly Audit
                  - Template-Driven Inquiries
                                │
                                ▼
                 [8. PIT Query & Leak Auditing]
                - As-Of Historical Queries
                - Decision-Time Leak Detection
```

---

## 4. Canonical Bitemporal Data Model

Every ingested record is transformed into a standardized bitemporal representation. This model explicitly separates the event timestamp (when an economic phenomenon occurred) from the knowledge timestamp (when the observation was published and accessible).

### Schema Specification

| Column | Type | Description |
|---|---|---|
| `entity_id` | `string` | Canonical identifier from the symbol master, or raw identifier if unresolved |
| `entity_id_type` | `string` | Identifier standard (`cik`, `ticker`, `isin`, `cusip`, `vendor_id`) |
| `field` | `string` | Metric name joined from field columns (e.g., `Revenues\|USD\|4Q`) |
| `event_time` | `timestamp[us, UTC]` | Period end date or observation time |
| `knowledge_time` | `timestamp[us, UTC]` | Timestamp when the data became visible to market participants |
| `value_num` | `decimal128(38, 9)` | Exact decimal representation for monetary and numeric quantities |
| `value_str` | `string` | Textual state or categorical value (null when numeric) |
| `unit` | `string` | Standardized unit code (`USD`, `shares`, `ratio`, `pure`) |
| `drop_id` | `string` | Hexadecimal SHA-256 prefix identifying the source drop batch |
| `source_file` | `string` | Relative path of the raw file within the drop |
| `source_row` | `int64` | One-based row index in the raw file |
| `row_hash` | `string` | Cryptographic SHA-256 fingerprint of the normalized values |

---

## 5. Subsystem Breakdown

The platform consists of twelve modular subsystems located in the `vdok/` package:

```
vdok/
├── __init__.py           # Package version and exports
├── cli.py                # Command-line interface orchestration
├── manifest.py           # Declarative vendor configuration parser
├── discover.py           # Drop scanner and cryptographic fingerprinting
├── schema.py             # Schema inference, contract pinning, and drift detection
├── normalize.py          # Row canonicalization and deterministic parsing
├── symbols.py            # Point-in-time symbology master and identifier resolution
├── validate.py           # Vectorized rule engine and multi-tier severity execution
├── store.py              # Partitioned Parquet bitemporal store and DuckDB catalog
├── revisions.py          # Drop-over-drop diffing and restatement ledger
├── query.py              # Zero-bias point-in-time query engine and leak auditor
├── report.py             # Readiness scorecard and vendor inquiry generator
└── adapters/
    ├── sec_fsds.py       # SEC Financial Statement Data Sets relational loader
    ├── synthetic.py      # Fault-injected synthetic benchmark dataset generator
    └── alfred.py         # St. Louis Fed FRED/ALFRED vintage time-series adapter
```

### Component Documentation Directory
Detailed engineering documentation for each subsystem is provided in `docs/components/`:
- [01 - Vendor Manifest Model](components/01-manifest.md)
- [02 - Drop Discovery & Fingerprint](components/02-discovery.md)
- [03 - Schema Engine & Drift Guard](components/03-schema-engine.md)
- [04 - Canonical Normalization](components/04-normalization.md)
- [05 - Symbology Master](components/05-symbology.md)
- [06 - Validation & Severity Routing](components/06-validation.md)
- [07 - Append-Only Bitemporal Store](components/07-bitemporal-store.md)
- [08 - Revision Ledger & Diffing](components/08-revision-ledger.md)
- [09 - PIT Query & Leak Detector](components/09-query-engine-leak-detector.md)
- [10 - Readiness Reporting & Vendor Inquiries](components/10-readiness-report.md)
- [11 - Ingestion Adapters](components/11-adapters.md)
- [12 - CLI Workflows & Tooling](components/12-cli.md)

---

## 6. Multi-Tier Severity Routing

Data validation applies severity tiers to prevent corrupted data from entering research datasets while maintaining throughput for valid observations:

| Severity Tier | Scope | Operational Response | Target Artifact |
|---|---|---|---|
| `warn` | Row | Logged in validation metrics; record continues through pipeline | Readiness Report |
| `quarantine` | Row | Record extracted from analytical ingestion flow | `quarantine.parquet` |
| `hold` | Batch | Ingestion halted immediately; zero records written to storage | `held_drops` ledger |

### Severity Routing Logic
- **Row-Level Rules**: Missing mandatory fields, range violations, invalid enum codes, non-monotonic timestamps per entity, or future-dated events send individual records to `quarantine.parquet` annotated with rejection reasons.
- **Batch-Level Rules**: Structural schema discrepancies (column removal, type widening), abnormal drop size contractions (< 0.5× or > 2.0× trailing median), excessive duplicate ratios (> 1%), or unexpected multi-period coverage dropouts place the entire drop in `hold` status.

---

## 7. Storage Layout & Partitioning

The storage hierarchy organizes data files by vendor, field grouping, and event year:

```
data/store/<vendor>/
├── store.duckdb                 # Relational catalog views and drop tracking
├── <field_bucket>/
│   └── <event_year>/
│       ├── part-<drop_id_1>.parquet
│       └── part-<drop_id_2>.parquet
├── quarantine/
│   └── part-<drop_id>.parquet   # Quarantined rows with error reason arrays
└── metadata/
    ├── drops.parquet            # Ingestion audit log
    ├── held_drops.parquet       # Held drop records and failure reasons
    ├── schema_versions.parquet  # Pinned contract lineage
    └── revisions.parquet        # Historical value modification ledger
```

File permissions are restricted to read-only (`0o444`) immediately following completion of disk writes to prevent accidental modification or truncation.

---

## 8. Point-in-Time Query Mechanics

Point-in-time evaluation resolves the most recent observation known at or prior to historical timestamp T_as_of:

```
value(E, F, T_event, T_as_of) = latest record where knowledge_time ≤ T_as_of
```

This query is compiled into vectorized DuckDB SQL execution:

```sql
SELECT entity_id, field, event_time, knowledge_time, value_num, value_str, unit
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY entity_id, field, event_time
               ORDER BY knowledge_time DESC, drop_id DESC
           ) AS rank_idx
    FROM read_parquet('data/store/sec_fsds/**/*.parquet')
    WHERE entity_id = '0000320193'
      AND field = 'Revenues|USD|4Q'
      AND event_time = '2023-12-31'
      AND knowledge_time <= '2024-02-15 00:00:00'
)
WHERE rank_idx = 1;
```

---

## 9. Design Decisions & Trade-Offs

Key architecture decisions recorded in `docs/adr/`:

1. **ADR 001: Bitemporal Append-Only Architecture**: Values are never modified in place; each vintage is stored with its knowledge timestamp. Optimizes point-in-time reproducibility; costs additional storage capacity and bitemporal query indexing.
2. **ADR 002: Multi-Tier Severity Routing**: Differentiates row-level warnings and quarantine from batch-level holds. Optimizes operational availability of valid records; costs separate quarantine storage maintenance.
3. **ADR 003: Schema Pinning & Approval Workflow**: Schemas are inferred from samples and pinned as explicit contracts. Drift requires human review. Optimizes early detection of vendor modifications; costs manual approval intervention.
4. **ADR 004: Local-First Embedded DuckDB & Parquet**: Single-node file-based storage using columnar Parquet files and DuckDB views. Optimizes execution reproducibility without cloud service dependencies; defers distributed multi-node coordination.
5. **ADR 005: Deterministic Model-Free Identifier Mapping**: Rule-based resolution using versioned SEC ticker cross-references. Unresolved symbols are quarantined. Optimizes auditability and prevents false match propagation; costs unmapped entity queue management.
