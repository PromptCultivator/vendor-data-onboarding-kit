# Component: PIT Query Engine & Leak Detector (`vdok.query`)

## 1. Overview & Responsibility

The `vdok.query` module provides point-in-time (PIT) retrieval interfaces and an automated leak detector. In quantitative investment research, look-ahead bias occurs when a strategy decision at historical time T_decision accesses information that was not publicly known until T_knowledge > T_decision.

`vdok.query` enforces **Guarantee 2 (G2)**: queries evaluated as of timestamp T_as_of strictly ignore records published after that timestamp. An audit logger and leak detector verify that quantitative backtests maintain strict temporal causality.

---

## 2. Point-in-Time Query API

The Python API exposes a `Store` query abstraction:

```python
from datetime import datetime, timezone
from decimal import Decimal
from vdok.query import Store

store = Store("data/store/sec_fsds")

# Query single point-in-time value
record = store.value(
    entity_id="0000320193",
    field="Revenues|USD|4Q",
    event_time="2023-12-31",
    as_of="2024-02-15T00:00:00Z",
)

# Query historical time series as of a specific vintage
series = store.series(
    entity_id="0000320193",
    field="Revenues|USD|4Q",
    as_of="2024-02-15T00:00:00Z",
    start="2020-01-01",
    end="2023-12-31",
)
```

---

## 3. Query Compilation & Vectorized Execution

Queries filter partition files directly via DuckDB without pre-loading entire datasets into Python memory.

### 3.1 Point Query Window Execution
To resolve the authoritative observation for key `(entity_id, field, event_time)` known as of `as_of`:

```sql
SELECT entity_id, field, event_time, knowledge_time, value_num, value_str, unit
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY entity_id, field, event_time
               ORDER BY knowledge_time DESC, drop_id DESC
           ) AS rank_idx
    FROM read_parquet('data/store/sec_fsds/**/*.parquet')
    WHERE entity_id = ?
      AND field = ?
      AND event_time = ?
      AND knowledge_time <= ?
)
WHERE rank_idx = 1;
```

### 3.2 Guarantee 2 (G2) Verification
The automated test suite verifies G2 by:
1. Ingesting drop 1 where entity E has value V_1 known at T_1.
2. Ingesting drop 2 where entity E is restated to value V_2 known at T_2 (where T_2 > T_1).
3. Executing `store.value(..., as_of=T_1)` and asserting that the returned value is exactly V_1.
4. Executing `store.value(..., as_of=T_2)` and asserting that the returned value is V_2.

---

## 4. Audit Logging & Leak Detection Engine

When researchers execute strategy backtests, subtle bugs can inadvertently set `as_of` forward in time or evaluate trading signals using future knowledge dates.

### 4.1 Audit Logging
Initializing `Store(audit=True)` records every read operation to an audit log file:
```
data/audit/query_audit_<session>.jsonl
```

Each log line contains:
```json
{
  "session_id": "8f9a2e3b",
  "decision_time": "2024-01-01T09:30:00Z",
  "as_of": "2024-01-01T09:30:00Z",
  "entity_id": "0000320193",
  "field": "Revenues|USD|4Q",
  "event_time": "2023-09-30T00:00:00Z",
  "knowledge_time_returned": "2023-11-03T16:05:00Z",
  "row_hash": "4c8f..."
}
```

### 4.2 Leak Detection Analysis (`vdok leaks`)
The `vdok leaks` command audits query logs against model decision timestamps:

```bash
vdok leaks --log data/audit/query_audit.jsonl --decision-time-column decision_time
```

The leak detector identifies two categories of temporal violations:
1. **As-Of Leakage**: A query where the caller requested data with an `as_of` timestamp situated in the future relative to the trading decision:
   `T_as_of > T_decision`
2. **Knowledge Leakage**: A condition where the record returned by the store carries a publication timestamp exceeding the decision timestamp:
   `T_knowledge_returned > T_decision`

Any detected violation produces a detailed incident report pinpointing the source line, query parameters, and timestamp discrepancy.
