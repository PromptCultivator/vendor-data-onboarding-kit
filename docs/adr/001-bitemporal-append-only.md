# ADR 001: Bitemporal Append-Only Store

## Status
Accepted

## Context
Market data, alternative data, and macroeconomic time series undergo frequent retroactive revisions. For example, corporate filers restate previous quarters during annual 10-K filings, and government agencies publish updated vintages of GDP and employment statistics. 

If data ingestion pipelines update historical records in place, historical backtests suffer from look-ahead bias: trading models evaluated on past dates inadvertently observe figures that were not published until months or years later. This inflates simulated strategy performance and conceals risk.

## Options Considered

1. **In-Place Updates (Latest State Only)**:
   - Overwrite historical records when new data drops arrive.
   - *Pros*: Smallest storage footprint; direct single-key queries without temporal filtering.
   - *Cons*: Destroys point-in-time historical auditability; causes severe look-ahead bias in backtests.

2. **Snapshot Copies per Drop**:
   - Duplicate the entire dataset directory on every drop.
   - *Pros*: Preserves exact historical drop states.
   - *Cons*: Prohibitive storage duplication (O(N * M) growth where N is drop count and M is universe size); difficult to query across drops without custom reconciliation.

3. **Bitemporal Append-Only Event Ledger**:
   - Store every observation as an immutable record tagged with both event time (observation period) and knowledge time (market publication timestamp). Never update or delete existing files.
   - Resolve point-in-time views dynamically using window functions.

## Decision
Adopt **Option 3 (Bitemporal Append-Only Event Ledger)**. 

Every record is stored with:
- `event_time`: The historical timestamp the metric describes.
- `knowledge_time`: The timestamp when the observation became known and accessible.

Partitioned Apache Parquet files are written once with read-only permissions (`0o444`). An incoming drop never modifies or replaces existing partition files.

## Consequences

- **Optimizes**: Point-in-time reproducibility and elimination of look-ahead bias. Historical research models observe exact data states visible at the decision timestamp.
- **Costs**: 
  - Storage growth: Preserving all historical vintages increases disk utilization. The system tracks this via the Storage Expansion Factor (S_f = total bytes / latest deduplicated bytes).
  - Query overhead: Point-in-time queries require a temporal filter (`knowledge_time <= as_of`) and a window ranking function (`ROW_NUMBER() OVER (...) = 1`) over partition files.
