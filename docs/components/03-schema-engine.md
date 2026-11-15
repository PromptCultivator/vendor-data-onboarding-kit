# Component: Schema Engine & Drift Guard (`vdok.schema`)

## 1. Overview & Responsibility

`vdok.schema` infers structural contracts from raw vendor drops, pins approved contracts to `schemas/<vendor>.yaml`, detects column and type drift on subsequent deliveries, and manages operator-approved contract upgrades.

---

## 2. Statistical Schema Inference (`infer_schema`)

- **Sampling**: Inspects up to `max_sample_rows` (default `200,000`) rows per file.
- **Path-Safe Reading**: Uses DuckDB's programmatic relation API (`con.read_csv`, `con.read_json`) for text formats and PyArrow for Parquet so paths with quotes or spaces require no SQL string escaping.
- **Inferred Types (`InferredType`)**: `integer`, `decimal`, `float`, `string`, `date`, `datetime`, `boolean`.
- **Exact Monetary Typing**: Columns listed in manifest `value_columns` or matching monetary names (`_is_money_column`) enforce exact `decimal` inference with derived `(precision, scale)` instead of IEEE 754 `float`.
- **Tolerant Cell-Anomaly Sampling**: On samples with `≥ 100` non-null values, if `≤ 1.0%` of sampled cells fail narrow numeric/date parsing, the narrow type is preserved so row-level validation routes the isolated corrupted cells to `quarantine.parquet` rather than triggering a false batch-level `type_widened` hold.
- **Column Metadata (`ColumnSchema`)**: Stores `type`, `nullable`, `distinct_count`, `min`, `max`, `precision`, and `scale`.

---

## 3. Pinned Contract Format (`VendorSchema`)

Pinned contracts reside at `schemas/<vendor>.yaml`:

```yaml
vendor: synthetic
version: 1
pinned_at: "2026-11-10T13:25:37.107659+00:00"
columns:
  record_id:
    type: integer
    nullable: false
    distinct_count: 6316
    min: 1000
    max: 7315
  entity_id:
    type: string
    nullable: false
  price:
    type: decimal
    nullable: false
    precision: 18
    scale: 4
```

---

## 4. Drift Classification (`compare_schemas` / `check_schema`)

Every incoming drop is compared column-by-column against the pinned contract:

| `DriftCategory` | `DriftSeverity` | Condition | Operational Action |
|---|---|---|---|
| `removed_column` | `hold` | Column in pinned schema is missing from drop | Holds drop before store write |
| `type_widened` | `hold` | Column type widened incompatibly (e.g., `integer` → `string`) | Holds drop before store write |
| `added_column` | `warn` | New column present in drop not in pinned schema | Logs warning and proposed addition |
| `type_narrowed` | `warn` | Column type narrowed compatibly (e.g., `float` → `integer`) | Logs warning and proposed narrowing |
| `nullability_change` | `warn` | Non-nullable column contains null values | Logs warning; row rules handle nulls |

---

## 5. Contract Lifecycle (`pin_schema`, `accept_schema_changes`)

- **Initial Pinning**: First onboarding (or `vdok schema pin`) writes `schemas/<vendor>.yaml` at `version: 1`.
- **Accepting Changes**: `vdok schema accept <vendor>` applies proposed differences from the latest drop (or `--drop-path`), increments `version`, updates `pinned_at`, and writes the updated YAML contract so legitimate vendor migrations can proceed.
