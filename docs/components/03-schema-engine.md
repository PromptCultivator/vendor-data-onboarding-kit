# Component: Schema Engine & Drift Guard (`vdok.schema`)

## 1. Overview & Responsibility

The `vdok.schema` module infers, pins, and verifies structural data contracts for vendor drops. Data providers frequently change payloads by renaming fields, modifying data types, or adding nullable fields without advance notification.

`vdok.schema` prevents unexpected data corruption through a two-stage contract lifecycle:
1. **Statistical Schema Inference**: Generates a strongly typed schema definition by sampling raw file contents.
2. **Contract Pinning & Drift Guard**: Serializes the verified schema to `schemas/<vendor>.yaml` and compares every future drop against this contract, halting execution on breaking alterations.

---

## 2. Statistical Schema Inference

### 2.1 Sampling Strategy
To balance verification depth with execution speed on large datasets, the engine samples up to 200,000 rows per file.

### 2.2 Inferred Data Types
- `integer`: Signed 64-bit integer values (`int64`).
- `decimal`: Exact fixed-point numeric representation (`decimal(38, 9)`). Floats are prohibited for monetary fields to prevent IEEE 754 precision loss.
- `float`: General floating-point values for scientific measurements.
- `string`: Textual strings and categorical codes.
- `date`: Calendar dates formatted as `YYYY-MM-DD`.
- `datetime`: Point-in-time timestamps with microsecond precision.
- `boolean`: Binary truth values (`true`, `false`).

### 2.3 Statistical Attributes Computed
For each column, the inference engine computes:
- Data type classification.
- Nullability ratio ($N_{\text{null}} / N_{\text{sample}}$).
- Approximate distinct values (HyperLogLog or exact count).
- Numeric or temporal bounds: $\min$ and $\max$ values.

---

## 3. Pinned Contract Specification

Pinned schemas are written to `schemas/<vendor>.yaml` in human-readable YAML format:

```yaml
vendor: sec_fsds
version: 1
pinned_at: "2024-01-15T12:00:00Z"
columns:
  cik:
    type: string
    nullable: false
    distinct_count: 520
  tag:
    type: string
    nullable: false
    distinct_count: 2450
  version:
    type: string
    nullable: false
  ddate:
    type: date
    nullable: false
    min: "2020-01-01"
    max: "2024-03-31"
  qtrs:
    type: integer
    nullable: false
  uom:
    type: string
    nullable: false
  value:
    type: decimal
    scale: 4
    precision: 28
    nullable: true
```

---

## 4. Schema Drift Classification

When a drop is onboarded against an existing pinned schema, the engine computes column-by-column diffs and classifies changes into three severity categories:

| Drift Category | Severity | Description | Operational Response |
|---|---|---|---|
| `removed_column` | `hold` | A column present in pinned schema is absent from drop | Halts drop immediately; writes to `held_drops` |
| `type_widened` | `hold` | Data type expanded incompatibly (e.g., `integer` $\to$ `string`) | Halts drop immediately; writes to `held_drops` |
| `added_column` | `warn` | A new column appears in drop that is not in pinned schema | Logs warning; ignores extra column or passes with notice |
| `type_narrowed` | `warn` | Data type tightened compatibly (e.g., `float` $\to$ `integer`) | Logs warning; accepts drop |
| `nullability_change` | `warn` | Column declared non-nullable now contains null values | Logs warning; routes affected rows to quarantine |

---

## 5. Human-in-the-Loop Contract Updates

When legitimate vendor updates occur (such as a planned schema version release), operators review proposed changes in the readiness report and accept them via the CLI:

```bash
vdok schema accept sec_fsds
```

This command:
1. Archives the previous contract version in storage table `schema_versions`.
2. Increments the schema version number.
3. Updates `schemas/<vendor>.yaml` with the updated column definitions.
4. Allows previously held drops to proceed with re-validation.
