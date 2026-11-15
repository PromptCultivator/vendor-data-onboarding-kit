# ADR 001: Bitemporal Append-Only Store

## Status
Accepted

## Context
Market data, corporate financial statements, and macroeconomic series undergo frequent retroactive restatements. When an ingestion pipeline overwrites historical values in place, backtests evaluated at past decision timestamps observe figures that were not publicly available until later, introducing look-ahead bias.

## Options Considered

1. **In-Place Updates (Latest State Only)**:
   - Overwrite historical records when new data drops arrive.
   - *Pros*: Smallest storage footprint; single-key lookups without temporal filtering.
   - *Cons*: Destroys historical auditability and introduces look-ahead bias into backtests.

2. **Full Snapshot Copies per Drop**:
   - Duplicate the entire dataset directory on every drop.
   - *Pros*: Preserves complete drop states.
   - *Cons*: Storage grows as O(N × M) in drop count N and universe size M; cross-vintage queries require external reconciliation.

3. **Bitemporal Append-Only Event Ledger**:
   - Store every observation as an immutable record tagged with both `event_time` (observation period) and `knowledge_time` (visibility timestamp), locking partition files read-only and never updating or deleting existing files.
   - Resolve point-in-time states dynamically by selecting the greatest `(knowledge_time, drop_id)` with `knowledge_time ≤ as_of`.

## Decision
Adopt **Option 3 (Bitemporal Append-Only Event Ledger)**.

Every record carries:
- `event_time`: The historical period or timestamp the observation describes.
- `knowledge_time`: The timestamp when the observation became known and accessible.

Partitioned Apache Parquet files are written once and locked to POSIX read-only permissions (`0o444`). Incoming drops never modify or replace existing partition files.

## Consequences

- **Optimizes**: Point-in-time reproducibility and elimination of look-ahead bias. Historical research queries observe exact data states visible at any decision timestamp.
- **Costs**:
  - Storage growth: Preserving historical vintages scales storage linearly in drop count N relative to in-place overwrites. Columnar Parquet dictionary encoding and compression yield measured storage factors (vendor store bytes divided by raw drop sequence bytes) below 1.0×:
    - Synthetic (50 MB): `0.585×` (range `0.585–0.586×`; `0.855×` across the two non-held ingested drops).
    - Synthetic (1 GB): `0.580×` (`0.847×` across the two non-held ingested drops).
    - SEC FSDS (2023q3–2023q4): `0.581×` (range `0.580–0.581×` across both ingested quarters).
  - Query path complexity: Point-in-time reads require temporal filtering (`knowledge_time ≤ as_of`) and ordering by `(knowledge_time DESC, drop_id DESC)` rather than direct point overwrites.
