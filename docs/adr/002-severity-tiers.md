# ADR 002: Severity Tiers: Warn, Quarantine, Hold

## Status
Accepted

## Context
Raw vendor drops inevitably contain anomalies: corrupted numeric formats, missing mandatory attributes, out-of-range values, unexpected duplicate entries, or systematic data transmission truncations. 

Ingestion pipelines face a dilemma:
- If a pipeline fails immediately on any error (all-or-nothing), a single malformed row among millions will block all valid data from downstream consumption.
- If a pipeline silently ignores or coerces bad rows, corrupted records leak into production models.

## Options Considered

1. **Strict Binary Pass/Fail (Fail-Fast)**:
   - Any single invalid row causes the entire batch ingestion to abort.
   - *Pros*: Zero corrupted records can enter storage.
   - *Cons*: High operational fragility; minor vendor typos block high-priority market feeds.

2. **Permissive Logging (Log and Continue)**:
   - Log errors and skip bad rows or fill missing fields with defaults.
   - *Pros*: High availability for downstream consumers.
   - *Cons*: Silent loss of critical records; data reconciliation is difficult without an isolated record log.

3. **Multi-Tier Severity Routing (`warn`, `quarantine`, `hold`)**:
   - Classify errors into operational tiers:
     - `warn`: Record continues; notification logged in readiness report.
     - `quarantine`: Row extracted and written to a separate quarantine Parquet file with reasons; valid rows proceed.
     - `hold`: Batch-level integrity failure; ingestion halted completely before storage writes.

## Decision
Adopt **Option 3 (Multi-Tier Severity Routing)**.

- **Row-level anomalies** (such as out-of-bounds metrics, malformed dates, unmapped identifiers, or duplicate keys below 1% threshold) route affected records into `quarantine.parquet` while allowing valid records to proceed to partitioned storage.
- **Batch-level anomalies** (such as unexpected column removals, type widening, drop volume falling below 50% or exceeding 200% of trailing median, duplicate ratios exceeding 1%, or multi-period coverage dropouts) place the entire drop in `hold` status, writing zero rows to the main store.

## Consequences

- **Optimizes**: Availability of valid data rows while isolating anomalies and preventing feed-level data corruption from entering production stores.
- **Costs**:
  - Secondary write path: Requires maintaining a dedicated quarantine storage hierarchy (`store/<vendor>/quarantine/part-<drop_id>.parquet`).
  - Operational reconciliation: Quarantined records require inspection workflows or vendor clarification to reprocess.
