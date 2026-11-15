# Component: Readiness Reporting & Vendor Inquiries (`vdok.report`)

## 1. Overview & Responsibility

`vdok.report` compiles deterministic readiness scorecards (`ReadinessReport`) in Markdown (`report.md`) and JSON (`report.json`) under `data/results/<vendor>/<drop_id>/`, and implements cross-drop structural and value diffing (`diff_drops`).

---

## 2. Report Structure

Each readiness report contains nine sections:
1. **Verdict**: `READY`, `READY WITH WARNINGS`, or `HELD`, with an explicit reason string.
2. **Drop Summary**: Discovered file list, total raw bytes, row count, distinct entities, distinct fields, `[event_time_min, event_time_max]`, and `[knowledge_time_min, knowledge_time_max]`.
3. **Schema**: Active pinned contract version, detected drift items, and proposed schema updates.
4. **Coverage by Period**: Row counts per `expected_calendar` bucket (`daily`, `weekly`, `monthly`, `quarterly`, or `none`) with gap markers.
5. **Validation**: Violation counts per rule and severity, quarantine share, and top quarantine reasons.
6. **Identifiers**: Mapped share (`mapped_count / total_rows`) and top 10 unresolved identifiers.
7. **Revisions**: Total revisions, per-field revision rates sorted deterministically by `(-rate, field_name)`, largest relative changes, and stale-vendor zero-revision alerts across > 3 consecutive drops.
8. **Questions for the Vendor**: Deterministic inquiries generated from failed rules and anomalies.
9. **Timings & Memory**: Per-stage wall-clock durations and peak resident set size.

---

## 3. Deterministic Verdict & Inquiry Rules

### Verdict State Machine (`compute_verdict`)
- `HELD` when `held == True` (batch-level rule failure or disallowed schema drift).
- `READY WITH WARNINGS` when `quarantine_count > 0`, `warn_count > 0`, `unmapped_count > 0`, or non-breaking schema warnings exist.
- `READY` otherwise.

### Deterministic Inquiries (`generate_vendor_inquiries`)
Inquiries are constructed purely from deterministic templates keyed by schema drift category, rule name (`unit`, `range`, `type_flip`, `duplicate`, `volume`, `byte_identical`), coverage gaps, top unmapped identifiers, silent-overwrite fields, and high revision rates (`> 5%`).

### Reproducibility Safeguards
All aggregation tables sort with explicit tie-breakers (`(-rate, field_name)`), analytical queries use DuckDB's relation API (`.sql().to_arrow_table()`), and scalar counts use `.fetchall()[0][0]`, guaranteeing bit-identical Markdown and golden test outputs across runs.

---

## 4. Drop Diffing (`diff_drops`)

`vdok diff <vendor> <drop_a> <drop_b>` compares two committed drops and returns `DropDiffResult` (rendered to Markdown or JSON) covering added keys, removed keys, restated values from the revision ledger, and schema changes.
