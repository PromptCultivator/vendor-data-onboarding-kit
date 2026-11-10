# Component: Revision Ledger & Restatement Diffing (`vdok.revisions`)

## 1. Overview & Responsibility

The `vdok.revisions` module identifies and catalogs historical data restatements. When a vendor publishes a subsequent data drop, they frequently alter historical observations (e.g., a corporate filer amending a previous quarter's net income, or an economic agency issuing revised GDP benchmarks).

Traditional data ingestion pipelines overwrite previous records with new values, destroying the historical audit trail. `vdok.revisions` computes drop-over-drop differences, extracts restatements into a permanent revision ledger, and calculates revision rates across financial metrics.

---

## 2. Restatement Detection Algorithm

Following the validation and ingestion of drop N, the revision engine compares the new drop against all prior committed vintages in the store.

A record is classified as a restatement if:
1. It shares an identical logical key `(entity_id, field, event_time)` with an existing record.
2. The incoming record has a later knowledge timestamp:
   `t_knowledge_new > t_knowledge_old`
3. The observation payload differs beyond floating-point rounding tolerance:
   `|v_num_new - v_num_old| > ε` or `v_str_new ≠ v_str_old` or `u_new ≠ u_old`

```sql
SELECT
    curr.entity_id,
    curr.field,
    curr.event_time,
    prev.value_num AS old_value_num,
    curr.value_num AS new_value_num,
    prev.value_str AS old_value_str,
    curr.value_str AS new_value_str,
    prev.unit AS old_unit,
    curr.unit AS new_unit,
    prev.knowledge_time AS old_knowledge_time,
    curr.knowledge_time AS new_knowledge_time,
    prev.drop_id AS old_drop_id,
    curr.drop_id AS new_drop_id,
    CASE
        WHEN prev.value_num IS NOT NULL AND prev.value_num != 0
        THEN (curr.value_num - prev.value_num) / ABS(prev.value_num)
        ELSE NULL
    END AS relative_change
FROM normalized_current_drop curr
JOIN prior_latest_state prev
  ON curr.entity_id = prev.entity_id
 AND curr.field = prev.field
 AND curr.event_time = prev.event_time
WHERE (
    curr.value_num IS DISTINCT FROM prev.value_num
    OR curr.value_str IS DISTINCT FROM prev.value_str
    OR curr.unit IS DISTINCT FROM prev.unit
)
AND curr.knowledge_time > prev.knowledge_time;
```

---

## 3. Revision Ledger Data Model

Restatements are recorded permanently in `data/store/<vendor>/metadata/revisions.parquet`:

| Column | Type | Description |
|---|---|---|
| `entity_id` | `string` | Canonical entity identifier |
| `field` | `string` | Normalized metric identifier |
| `event_time` | `timestamp[us, UTC]` | Period end date of the restated metric |
| `old_value_num` | `decimal128(38, 9)` | Numerical value prior to restatement |
| `new_value_num` | `decimal128(38, 9)` | Numerical value following restatement |
| `old_value_str` | `string` | Categorical string prior to restatement |
| `new_value_str` | `string` | Categorical string following restatement |
| `old_unit` | `string` | Unit of measure prior to restatement |
| `new_unit` | `string` | Unit of measure following restatement |
| `old_knowledge_time`| `timestamp[us, UTC]` | Publication timestamp of the original value |
| `new_knowledge_time`| `timestamp[us, UTC]` | Publication timestamp of the restated value |
| `old_drop_id` | `string` | Originating drop ID of the prior vintage |
| `new_drop_id` | `string` | Originating drop ID of the restating drop |
| `relative_change` | `float64` | Fractional delta: `(V_new - V_old) / |V_old|` |
| `detected_at` | `timestamp[us, UTC]` | Ingestion execution timestamp |

---

## 4. Multi-Drop Diffing Interface (`vdok diff`)

The CLI provides an explicit comparison tool for inspecting variances between arbitrary drops:

```bash
vdok diff sec_fsds drop_2023q3 drop_2023q4
```

The diff engine classifies records into four disjoint subsets:
1. **Added Keys**: Entities or metrics present in `drop_b` that did not exist in `drop_a`.
2. **Removed Keys**: Observations present in `drop_a` that are missing from `drop_b`.
3. **Restated Values**: Observations with matching keys but divergent values.
4. **Unchanged Records**: Exact matches across both drops.

---

## 5. Vendor Behavior Diagnostics

The revision ledger provides operational signals regarding vendor data management practices:
- **High Revision Rates**: If > 5% of a vendor's historical records undergo revision per drop, data quality warnings are surfaced.
- **Zero Revisions Alert**: If a vendor reports zero revisions across four or more consecutive drops for macroeconomic or financial datasets known to undergo regular adjustment, the engine alerts operators to possible data freezing or lack of revision tracking on the vendor's side.
