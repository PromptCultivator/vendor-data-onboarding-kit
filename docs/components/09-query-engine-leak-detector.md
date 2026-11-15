# Component: PIT Query Engine & Leak Detector (`vdok.query`)

## 1. Overview & Responsibility

`vdok.query` implements the point-in-time query interface (`Store`), an unpruned streaming reference oracle (`BruteForceStore`), JSON Lines read audit logging (`AuditLogger`), and the look-ahead leak detector (`detect_leaks`). It enforces **Guarantee 2 (G2)**: queries evaluated at `as_of` never read or return observations with `knowledge_time > as_of`.

---

## 2. Point-in-Time Query API (`Store`)

```python
from vdok.query import Store

with Store("data/store/sec_fsds", audit=True) as store:
    record = store.value(
        entity_id="0000320193",
        field="Revenues|USD|4Q",
        event_time="2023-12-31",
        as_of="2024-02-15T00:00:00Z",
    )
    series = store.series(
        entity_id="0000320193",
        field="Revenues|USD|4Q",
        as_of="2024-02-15T00:00:00Z",
        start="2020-01-01",
        end="2023-12-31",
    )
```

- `Store.value(...)` returns a `StoreRecord` (or `None`) for the greatest `(knowledge_time, drop_id)` with `knowledge_time ≤ as_of`, normalizing entity identifiers (including 10-digit CIK zero-padding lookup fallback).
- `Store.series(...)` returns a chronologically ordered list of point-in-time `StoreRecord` entries across `[start, end]` as of `as_of`.

---

## 3. Sorted Partition MD5 Prefix Index & Reference Oracle

### 3.1 Indexed Partition Lookup (`_PartitionIndex`)
To support sub-millisecond `Store.value()` lookups at 100,000-query benchmark scale while remaining within memory budgets:
- `Store` loads each `(field_bucket, event_year)` partition on demand into an Arrow table sorted by `(_k64 ASC, knowledge_time DESC, drop_id DESC)`, where `_k64` is the leading 64-bit unsigned integer of `md5(entity_id || '\0' || field || '\0' || epoch_us(event_time))`.
- Partition tables are cached in memory up to 256 partitions / 2 GiB (`_MAX_CACHED_BYTES`), invalidated automatically if partition file sizes change.
- Lookups run `bisect_left` over zero-copy Arrow scalar `memoryview` buffers (`k64_mv`, `ev_mv`, `kt_mv`), returning the first matching row with `knowledge_time ≤ as_of`.
- All DuckDB-to-Python materialization uses `.to_arrow_table()` so UTC `zoneinfo` timestamps work without `pytz`.

### 3.2 Unpruned Reference Oracle (`BruteForceStore`)
`BruteForceStore.batch_values_unpruned` streams every `part-*.parquet` file across all buckets and years via PyArrow `ParquetFile.iter_batches` with zero directory pruning, zero SQL, and zero hash indexing. Automated tests and benchmarks assert 0 mismatches between `Store` and `BruteForceStore`.

---

## 4. Audit Logging & Leak Detection (`AuditLogger` & `detect_leaks`)

When `Store(..., audit=True)` is active, reads append JSON Lines records to `<store_path>/audit/query_audit.jsonl` (or a custom `audit_log_path`):

```json
{
  "decision_time": "2024-01-01T09:30:00+00:00",
  "as_of": "2024-01-01T09:30:00+00:00",
  "key": "0000320193|Revenues|USD|4Q|2023-09-30T00:00:00+00:00",
  "knowledge_time_returned": "2023-11-03T16:05:00+00:00",
  "query_type": "value"
}
```

`detect_leaks(log_path, decision_time_column="decision_time")` (exposed via `vdok leaks`) scans the log and reports `LeakReport` counts for two violation types:
1. **As-Of Leaks**: `as_of > decision_time`.
2. **Knowledge Leaks**: `knowledge_time_returned > decision_time`.
