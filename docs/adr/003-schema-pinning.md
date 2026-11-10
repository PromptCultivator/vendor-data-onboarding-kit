# ADR 003: Schema Inference, Pinning, and Drift Detection

## Status
Accepted

## Context
Commercial data vendors frequently alter feed schemas without warning: renaming columns, modifying data types (e.g., changing an integer status flag into a free-form string), or adding/removing fields. 

If an ingestion pipeline relies on dynamic runtime schema inference on every run, breaking vendor changes pass into storage unnoticed until downstream SQL queries or machine learning pipelines fail with runtime errors.

## Options Considered

1. **Fully Dynamic Runtime Schema Inference**:
   - Re-infer types and column structures dynamically on every drop.
   - *Pros*: Zero upfront configuration; automatically adapts to new columns.
   - *Cons*: Silent type coercions and column drops pass undetected; downstream consumer schemas break unexpectedly.

2. **Strict Hand-Crafted Code Contracts**:
   - Manually code and hardcode every field and data type in Python classes before onboarding.
   - *Pros*: Explicit compile-time type safety.
   - *Cons*: Tedious to initialize for datasets with hundreds of fields; slow onboarding turnaround.

3. **Inferred, Versioned, and Pinned Contract with Drift Guard**:
   - Sample raw data (up to 200,000 rows) during initial onboarding to generate a candidate contract.
   - Pin the contract to version-controlled YAML (`schemas/<vendor>.yaml`).
   - Validate future drops against the pinned contract, categorizing differences into breaking (`hold`) and non-breaking (`warn`).
   - Require explicit human operator command (`vdok schema accept <vendor>`) to update pinned contracts.

## Decision
Adopt **Option 3 (Inferred, Versioned, and Pinned Contract with Drift Guard)**.

- Initial onboarding creates `schemas/<vendor>.yaml`.
- Subsequent drops check for:
  - `removed_column`: Batch `hold`.
  - `type_widened`: Batch `hold` when more than `1.0%` of non-null sampled values (or any value on samples below `100` rows) widen the column's scalar type; isolated cell corruptions (`≤ 1.0%` on samples of `≥ 100` rows) retain the narrow contract type so row-level validation routes the defective cells to `quarantine`.
  - `added_column`: Logged as `warn`.
  - `type_narrowed`: Logged as `warn`.
  - `nullability_change`: Logged as `warn` with affected rows sent to quarantine.
- The readiness report presents proposed contract adjustments, which operators review and accept using `vdok schema accept <vendor>`.

## Consequences

- **Optimizes**: Early detection of silent vendor schema modifications before defective structures contaminate persistent storage.
- **Costs**: 
  - Operator intervention: Legitimate vendor changes require an explicit review and approval command before the batch proceeds.
  - Contract versioning overhead: Schema history must be tracked across versions in metadata storage.
