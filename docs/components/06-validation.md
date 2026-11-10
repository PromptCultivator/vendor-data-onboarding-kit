# Component: Rule Engine & Multi-Tier Severity System (`vdok.validate`)

## 1. Overview & Responsibility

The `vdok.validate` module verifies data correctness, completeness, and consistency before records reach permanent storage. Raw vendor data contains various defects, including transmission truncation, duplicated records, impossible values (such as negative trading volumes), and invalid date sequences.

To manage these defects effectively, `vdok.validate` applies a **multi-tier severity architecture**:
- `warn`: Logs validation anomalies for operational tracking; valid records continue through ingestion.
- `quarantine`: Isolates defective records into a separate Parquet file without stopping the rest of the batch.
- `hold`: Immediately halts drop ingestion prior to storage writes when systematic corruption is detected.

All validation rules are defined declaratively in `rules/<vendor>.yaml` and compiled into vectorized DuckDB SQL queries executed over the dataset in a single evaluation pass.

---

## 2. Rule Configuration Specification

Rules are declared in YAML:

```yaml
rules:
  # Row-level rules
  - name: non_null_entity
    check: not_null
    column: entity_id
    severity: quarantine

  - name: non_negative_revenue
    check: range
    column: value_num
    filter: "field LIKE 'Revenues%'"
    min: 0
    severity: warn

  - name: plausible_event_time
    check: temporal_bounds
    column: event_time
    min: "1990-01-01"
    max: "now + 1 day"
    severity: quarantine

  - name: valid_unit_of_measure
    check: allowed_values
    column: unit
    values: ["USD", "shares", "ratio", "pure"]
    severity: quarantine

  # Batch-level rules
  - name: volume_stability
    check: batch_volume
    ratio_min: 0.5
    ratio_max: 2.0
    trailing_drops: 7
    severity: hold

  - name: duplicate_threshold
    check: batch_duplicate_ratio
    max_ratio: 0.01
    severity: hold
```

---

## 3. Row-Level Rules & Quarantine Routing

Row-level rules identify invalid records without interrupting the processing of valid data:

1. **Not Null Checks**: Verifies mandatory fields (`entity_id`, `field`, `event_time`, `knowledge_time`).
2. **Value Domain & Range Bounds**: Asserts numeric values fall within expected economic intervals (e.g., share counts $> 0$).
3. **Allowed Unit of Measure**: Confirms units belong to the recognized financial unit registry.
4. **Monotone Timestamps**: Checks that observations for a given entity follow non-decreasing event time order.
5. **Future Observation Guard**: Enforces that event time does not extend beyond knowledge time when the vendor reports historical events after publication.

### Quarantine Record Format
Quarantined records are written to `store/<vendor>/quarantine/part-<drop_id>.parquet`:
```
canonical_row_data...                   # All canonical columns
quarantine_reasons: list[string]        # Identifiers of failed rules
quarantined_at:     timestamp[us, UTC]  # Execution timestamp
```

---

## 4. Batch-Level Rules & Hold Execution

Batch-level rules evaluate drop-wide aggregate metrics. When a batch rule fails, the entire drop is classified as `HELD`:
- **No data files are written to the partitioned store.**
- A single record is inserted into the `held_drops` metadata table detailing the failure reason.
- The readiness report assigns a `HELD` verdict, preventing downstream systems from consuming incomplete or corrupted data.

### 4.1 Batch Volume Variance Check
To detect partial file transmissions or truncated extracts, the row count $N_{\text{current}}$ is compared against the trailing median of the prior 7 completed drops:

$$0.5 \times \operatorname{median}(N_{t-7}, \dots, N_{t-1}) \le N_{\text{current}} \le 2.0 \times \operatorname{median}(N_{t-7}, \dots, N_{t-1})$$

Drops falling outside this boundary are placed on hold (initial drops are exempt).

### 4.2 Duplicate Ratio Escalation
Duplicate observations sharing the same primary key `(entity_id, field, event_time, knowledge_time)` are evaluated:
- If duplicate records comprise $\le 1.0\%$ of the drop, the duplicate occurrences are routed to `quarantine.parquet`, and valid records proceed.
- If duplicate records exceed $1.0\%$, systematic feed corruption is assumed, and the batch rule escalates to a full `hold`.

### 4.3 Redundant Byte-Identical Drop Guard
If a drop has the identical file hash sequence as the immediate predecessor drop, processing halts to prevent duplicate ingestion passes.

---

## 5. Vectorized Compilation & Execution

Rules are translated into DuckDB SQL CASE expressions and executed within a single query pass:

```sql
SELECT *,
       ARRAY_FILTER([
           CASE WHEN entity_id IS NULL THEN 'non_null_entity' ELSE NULL END,
           CASE WHEN unit NOT IN ('USD', 'shares', 'ratio', 'pure') THEN 'valid_unit_of_measure' ELSE NULL END,
           CASE WHEN event_time < '1990-01-01'::TIMESTAMPTZ THEN 'plausible_event_time' ELSE NULL END
       ], x -> x IS NOT NULL) AS quarantine_reasons
FROM normalized_drop_view;
```

This columnar evaluation pattern avoids per-row Python overhead, enabling multi-gigabyte per second throughput on modern multicore hardware.
