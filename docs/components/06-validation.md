# Component: Rule Engine & Multi-Tier Severity System (`vdok.validate`)

## 1. Overview & Responsibility

`vdok.validate` compiles declarative vendor rules (`rules/<vendor>.yaml`) into vectorized DuckDB SQL and routes data quality defects across three severity tiers (`warn`, `quarantine`, `hold`).

---

## 2. Rule Definitions (`RuleDefinition` & `RuleCheckType`)

- **Row-Level Checks**:
  - `not_null`: Mandatory column must be non-null (used e.g. for `type_flip` when non-numeric input leaves `value_num` null).
  - `range`: Numeric bounds (`min`, `max`, `min_inclusive`, `max_inclusive`) with optional SQL `filter`.
  - `allowed_values` / `allowed_units`: Set membership checks.
  - `regex_pattern`: Regular expression matching via DuckDB `regexp_matches`.
  - `decimal_scale`: Exact fixed-point check `({col} IS NOT NULL AND ROUND({col}, {scale}) != {col})` without floating-point coercion.
  - `temporal_bounds`: Verifies `event_time` lies within `[1990-01-01, drop_time + 1 day]`.
  - `temporal_order`: Enforces `knowledge_time ≥ event_time` for post-event reporting feeds.
  - `sql_expression`: Custom boolean SQL predicate.
- **Batch-Level Checks**:
  - `schema_drift`: Holds batch when `removed_column` or `type_widened` drift occurs.
  - `batch_volume`: Holds batch when row count falls outside `[0.5, 2.0] ×` trailing 7-drop median (first drop exempt).
  - `batch_duplicate_ratio`: Evaluates key `(entity_id, field, event_time, knowledge_time)` duplicate share (`max_ratio = 0.01`); quarantines duplicates when `≤ 1.0%` and holds the batch when `> 1.0%`.
  - `byte_identical`: Holds drop if identical file hash sequence matches the immediate predecessor drop.
  - `coverage_gap`: Evaluates missing expected calendar periods on conforming rows after row-level quarantine filtering.

---

## 3. Single-Pass Vectorized Execution (`RuleCompiler` & `ValidationEngine`)

1. **Duplicate Window Materialization**: A single temporary DuckDB view `numbered` computes `ROW_NUMBER() OVER (PARTITION BY entity_id, field, event_time, knowledge_time ORDER BY source_file, source_row) AS _vdok_dup_rank`, powering both the batch duplicate ratio check and row-level `'duplicate_key'` tagging in one pass.
2. **Multi-Reason Quarantine Array**: Every active quarantine rule compiles into `CASE WHEN (<predicate>) THEN '<rule_name>' ELSE NULL END` inside a DuckDB array literal, pruned natively with `list_filter([...], lambda x: x IS NOT NULL)` into `quarantine_reasons: list[string]`.
3. **Quarantine Output**: Rows with non-empty `quarantine_reasons` are written to `data/store/<vendor>/quarantine/part-<drop_id>.parquet` using `QUARANTINE_ARROW_SCHEMA` (`CANONICAL_ARROW_SCHEMA` + `quarantine_reasons` + `quarantined_at`).
4. **Batch Hold Output**: If any batch-level hold rule triggers, zero data partitions are written and a `HeldDropRecord` is persisted to `data/store/<vendor>/metadata/held_drops.parquet`.
