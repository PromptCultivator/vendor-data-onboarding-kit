# Component: Revision Ledger & Restatement Diffing (`vdok.revisions`)

## 1. Overview & Responsibility

`vdok.revisions` detects historical restatements whenever a new drop updates a previously recorded `(entity_id, field, event_time)` key at a newer `knowledge_time`, records those changes in `metadata/revisions.parquet`, and computes per-drop and per-field revision statistics.

---

## 2. Windowed Restatement Detection (`detect_revisions`)

Comparing an incoming drop against every historical vintage would multiply cascading restatements across drops. Instead, `detect_revisions` executes a windowed DuckDB SQL query comparing incoming records exclusively against the single latest prior baseline:

1. **`current_drop`**: Selects the latest `(knowledge_time DESC, source_row DESC)` entry per `(entity_id, field, event_time)` in the incoming drop.
2. **`prior_latest`**: Ranks prior store records (`drop_id != incoming_drop_id`) with `ROW_NUMBER() OVER (PARTITION BY entity_id, field, event_time ORDER BY knowledge_time DESC, drop_id DESC) = 1`.
3. **Relational Distinct Comparison**: Joins `current_drop` to `prior_latest` where `curr.knowledge_time > prev.old_knowledge_time` and payload fields differ under null-safe `IS DISTINCT FROM`:
   `curr.value_num IS DISTINCT FROM prev.old_value_num OR curr.value_str IS DISTINCT FROM prev.old_value_str OR curr.unit IS DISTINCT FROM prev.old_unit`.

---

## 3. Revision Ledger Schema (`REVISIONS_ARROW_SCHEMA`)

Restatements are persisted in `data/store/<vendor>/metadata/revisions.parquet` and synchronized into the `revisions` table of `store.duckdb`:

| Column | Arrow Type | Description |
|---|---|---|
| `entity_id` | `string` | Canonical entity identifier |
| `field` | `string` | Normalized metric identifier |
| `event_time` | `timestamp[us, UTC]` | Observation period timestamp |
| `old_value_num` | `decimal128(38, 9)` | Prior fixed-point numeric value |
| `new_value_num` | `decimal128(38, 9)` | Updated fixed-point numeric value |
| `old_value_str` | `string` | Prior textual value |
| `new_value_str` | `string` | Updated textual value |
| `old_unit` | `string` | Prior unit of measure |
| `new_unit` | `string` | Updated unit of measure |
| `old_knowledge_time` | `timestamp[us, UTC]` | Prior visibility timestamp |
| `new_knowledge_time` | `timestamp[us, UTC]` | Restated visibility timestamp |
| `old_drop_id` | `string` | Source drop ID of prior baseline |
| `new_drop_id` | `string` | Source drop ID of restating drop |
| `relative_change` | `float64` | `(new_value_num - old_value_num) / abs(old_value_num)` when non-zero |

---

## 4. Revision Analytics & Silent Overwrite Signals (`compute_revision_rates`)

`RevisionStats` reports total revisions, drop-level revision rate, per-field revision rates, largest relative changes, and silent-overwrite alerts (`silent_overwrite_fields`) when a field exhibits zero revisions across more than 3 consecutive drops.
