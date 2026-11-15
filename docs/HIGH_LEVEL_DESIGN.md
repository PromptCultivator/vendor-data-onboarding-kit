# System Architecture & High-Level Design (`vdok`)

## 1. Executive Summary

`vendor-data-onboarding-kit` (`vdok`) is a local-first market and fundamental data onboarding platform for quantitative research. It ingests raw vendor feeds (CSV, JSON Lines, Parquet, and SEC Financial Statement Data Sets archives), enforces pinned schema contracts, normalizes records into an immutable bitemporal Parquet store, captures historical restatements in a revision ledger, emits deterministic vendor readiness reports, and audits point-in-time reads for look-ahead bias.

Quantitative research pipelines fail on raw vendor data for four main reasons:
1. **Unannounced Schema Modifications**: Vendors add, drop, or widen columns without notice.
2. **Identifier Instability**: Corporate actions, exchange suffixes, and ticker reuse cause mismatched joins or survivorship bias.
3. **Silent Historical Restatements**: Retroactive updates overwrite prior values unless every vintage is preserved with its publication timestamp.
4. **Data Quality Degradation**: Corrupted cells, duplicate keys, unit shifts, or truncated drops propagate silently when not isolated before storage.

`vdok` addresses these risks with physical append-only storage, strict point-in-time query isolation, drop idempotency, multi-tier severity routing, and deterministic readiness reporting.

---

## 2. Core Guarantees

Three structural invariants are enforced by the runtime and verified by named automated tests in [tests/unit/test_guarantees.py](../../tests/unit/test_guarantees.py):

- **Guarantee 1 (G1) — Append-Only Physical Storage** (`test_g1_store_is_append_only`): Partition files are written once and locked read-only (`0o444`). There is no update or delete path; attempting to write an existing `drop_id` raises `AppendOnlyViolationError`.
- **Guarantee 2 (G2) — Strict Point-in-Time Isolation** (`test_g2_pit_never_reads_future_knowledge`): Point-in-time queries evaluated at `as_of` never read or return records whose `knowledge_time > as_of`, verified against an unpruned streaming reference oracle (`BruteForceStore`).
- **Guarantee 3 (G3) — Drop Ingestion Idempotency** (`test_g3_rerun_same_drop_adds_no_rows`): Re-onboarding an identical payload (`drop_id` derived from sorted uncompressed file SHA-256 hashes) is detected before store writes and returns the existing drop metadata without adding rows.

---

## 3. End-to-End Pipeline

```
Raw Vendor Drop (CSV, CSV.GZ, JSONL, JSONL.GZ, Parquet, SEC FSDS)
                    │
                    ▼
       [1. Discovery & Fingerprint]
      - Magic-byte & JSONL-first format inspection
      - Uncompressed payload SHA-256 & 16-hex drop_id
                    │
                    ▼
       [2. Schema & Contract Guard]
      - 200,000-row statistical sampling (DuckDB relation API)
      - Pinned YAML contract comparison & drift severity
                    │
                    ▼
      [3. Symbology & Normalization]
      - Hash-versioned rule-based identifier mapping
      - Strict fixed-point Decimal & UTC microsecond parsing
                    │
                    ▼
         [4. Validation Engine]
      - Vectorized DuckDB SQL rule evaluation
      - Multi-reason row quarantine & batch hold checks
                    │
        ┌───────────┴───────────┐
        ▼                       ▼
   Batch HELD           Batch ACCEPTED
 (held_drops.parquet)           │
                    ┌───────────┴───────────┐
                    ▼                       ▼
        [5. Append-Only Store]     [6. Revision Ledger]
       - Bucketed Parquet (0o444) - Windowed prior-latest diff
       - DuckDB catalog views     - Metadata revisions.parquet
                    │                       │
                    └───────────┬───────────┘
                                ▼
                   [7. Readiness Reporting]
                  - Verdict (READY / WARNINGS / HELD)
                  - Coverage, anomalies, deterministic inquiries
                                │
                                ▼
                 [8. PIT Query & Leak Auditing]
                - Sorted _k64 MD5 partition index
                - JSONL query audit & leak detection
```

---

## 4. Canonical Bitemporal Data Model

Every normalized record conforms to `CANONICAL_ARROW_SCHEMA` (13 fields: 12 canonical columns plus `unmapped`), separating `event_time` (the period described) from `knowledge_time` (when the value became known).

| Column | Arrow Type | Description |
|---|---|---|
| `entity_id` | `string` | Canonical identifier from the symbol master, or normalized raw ID if unresolved |
| `entity_id_type` | `string` | Identifier standard (`cik`, `ticker`, `isin`, `cusip`, `vendor_id`) |
| `field` | `string` | Metric identifier joined with `\|` (e.g., `Revenues\|USD\|4Q`) |
| `event_time` | `timestamp[us, UTC]` | Period end date or observation timestamp |
| `knowledge_time` | `timestamp[us, UTC]` | Filing acceptance timestamp or batch drop receipt timestamp |
| `value_num` | `decimal128(38, 9)` | Exact fixed-point numeric value (null for non-numeric rows) |
| `value_str` | `string` | Textual or categorical value (null for numeric rows) |
| `unit` | `string` | Standardized unit (`USD`, `shares`, `ratio`, `pure`) |
| `drop_id` | `string` | 16-character hexadecimal prefix of sorted uncompressed file digests |
| `source_file` | `string` | Relative source file path within the drop |
| `source_row` | `int64` | One-based source row index |
| `row_hash` | `string` | SHA-256 digest over null-byte (`\0`) delimited canonical fields |
| `unmapped` | `bool` | `true` when identifier resolution found no symbol master match |

---

## 5. Subsystem Breakdown

All core modules reside in `vdok/`:

```
vdok/
├── __init__.py           # Package version and exports
├── cli.py                # Typer CLI application (`vdok`)
├── manifest.py           # VendorManifest Pydantic v2 model and IANA timezone validator
├── discover.py           # Format detection, decompression safety, drop_id hashing
├── schema.py             # Sampling inference, YAML pinning, tolerant drift classification
├── normalize.py          # Canonical Arrow/Pydantic models, pure & vectorized normalizers
├── symbols.py            # Content-hash-versioned Parquet symbol master & deterministic mapping
├── validate.py           # DuckDB SQL rule compiler and multi-tier severity engine
├── store.py              # Bucketed Parquet writer, 0o444 locking, DuckDB catalog sync
├── revisions.py          # Windowed restatement detection and revision ledger
├── query.py              # Partition-indexed PIT Store, BruteForceStore oracle, leak detector
├── report.py             # Readiness report compiler (MD + JSON), inquiry templates, drop diff
├── onboard.py            # Pipeline orchestrator coordinating all stages
└── adapters/
    ├── sec_fsds.py       # SEC FSDS rate-limited downloader, caching, SQL normalizer
    └── synthetic.py      # Seeded multi-drop generator with planted fault suites
```

### Component Documentation

- [01 - Vendor Manifest Model](components/01-manifest.md)
- [02 - Drop Discovery & Fingerprinting](components/02-discovery.md)
- [03 - Schema Engine & Drift Guard](components/03-schema-engine.md)
- [04 - Canonical Normalization](components/04-normalization.md)
- [05 - Symbol Master & Resolution](components/05-symbology.md)
- [06 - Validation & Severity Routing](components/06-validation.md)
- [07 - Append-Only Bitemporal Store](components/07-bitemporal-store.md)
- [08 - Revision Ledger & Diffing](components/08-revision-ledger.md)
- [09 - PIT Query Engine & Leak Detector](components/09-query-engine-leak-detector.md)
- [10 - Readiness Reporting & Vendor Inquiries](components/10-readiness-report.md)
- [11 - Ingestion Adapters](components/11-adapters.md)
- [12 - CLI Workflows & Tooling](components/12-cli.md)

---

## 6. Multi-Tier Severity Routing

Validation compiles declarative YAML rules into single-pass DuckDB SQL and separates row-level defects from batch-level structural failures:

| Severity Tier | Scope | Operational Response | Target Artifact |
|---|---|---|---|
| `warn` | Row / Schema | Logged in metrics; rows continue to store | Readiness Report |
| `quarantine` | Row | Rows diverted with `quarantine_reasons` list; conforming rows proceed | `quarantine/part-<drop_id>.parquet` |
| `hold` | Batch | Ingestion aborts before store write; zero partition files written | `metadata/held_drops.parquet` |

---

## 7. Storage Layout & Partitioning

```
data/store/<vendor>/
├── store.duckdb                           # DuckDB catalog views and metadata tables
├── <field_bucket>/
│   └── <event_year>/
│       ├── part-<drop_id_1>.parquet       # Immutable 0o444 data partition
│       └── part-<drop_id_2>.parquet
├── quarantine/
│   └── part-<drop_id>.parquet             # Quarantined rows with quarantine_reasons
├── audit/
│   └── query_audit.jsonl                  # Optional PIT read audit log
└── metadata/
    ├── drops.parquet                      # Committed drop log
    ├── held_drops.parquet                 # Held drop records and hold reasons
    ├── schema_versions.parquet            # Schema version lineage
    └── revisions.parquet                  # Cross-drop restatement ledger
```

---

## 8. Point-in-Time Query Mechanics

For a logical key `(entity_id, field, event_time)` and cutoff `as_of`, the query engine resolves:

```
value(E, F, T_event, T_as_of) = record with greatest (knowledge_time, drop_id) where knowledge_time ≤ T_as_of
```

Rather than executing a full-table DuckDB scan per lookup, `Store` caches per-partition `(field_bucket, event_year)` Arrow tables sorted by `(_k64 ASC, knowledge_time DESC, drop_id DESC)`, where `_k64` is the leading 64 bits of `md5(entity_id || '\0' || field || '\0' || epoch_us(event_time))`. Lookups execute `bisect_left` over zero-copy `memoryview` scalar buffers within a 2 GiB / 256-partition cache budget. Correctness is verified in tests and benchmarks against `BruteForceStore.batch_values_unpruned`, which streams all Parquet files with PyArrow without pruning or indexing.

---

## 9. Architectural Decisions

Key architectural trade-offs are captured in [docs/adr/](adr) and detailed implementation choices (`D-001`–`D-030`) are cataloged in [docs/DECISIONS.md](DECISIONS.md):

1. **ADR 001 — Bitemporal Append-Only Store**: Preserves all vintages with `knowledge_time`; eliminates look-ahead bias at a measured `0.580×`–`0.585×` Parquet storage factor relative to raw drops.
2. **ADR 002 — Severity Tiers (`warn`, `quarantine`, `hold`)**: Keeps conforming data flowing (`96.75%` admitted on SEC `2023q3`) while isolating bad rows and halting structural failures.
3. **ADR 003 — Schema Pinning & Drift Guard**: Infers and pins YAML contracts with `≤ 1.0%` cell-anomaly tolerance on `≥ 100`-row samples and requires explicit `vdok schema accept` approval for structural migrations.
4. **ADR 004 — Local-First DuckDB & Parquet**: Single-process local execution with embedded DuckDB and Parquet partitions; defers distributed multi-node storage and cloud object-store consistency.
5. **ADR 005 — Model-Free Identifier Mapping**: Deterministic CIK/ticker/title matching against a hash-versioned SEC master with explicit `unmapped` flags; eliminates silent fuzzy-join corruption.
