# Component: Readiness Reporting & Vendor Inquiries (`vdok.report`)

## 1. Overview & Responsibility

The `vdok.report` module evaluates whether an onboarded vendor drop is fit for production quantitative research. Rather than leaving data operations teams to manually inspect raw logs and metrics, `vdok.report` compiles a comprehensive readiness scorecard and renders it in both human-readable Markdown (`report.md`) and machine-readable JSON (`report.json`).

The scorecard issues an unequivocal verdict (`READY`, `READY WITH WARNINGS`, or `HELD`), quantifies coverage and revision rates, and generates deterministic vendor inquiry templates tailored to observed data anomalies.

---

## 2. Report Sections & Schema

The generated report contains nine structured sections:

```
data/results/<vendor>/<drop_id>/
├── report.md     # Formatted markdown document
└── report.json   # Machine-readable evaluation payload
```

### 2.1 Specification of Sections

1. **Verdict Header**:
   - `READY`: All validation checks passed, zero quarantine records, no schema drift.
   - `READY WITH WARNINGS`: Minor anomalies detected (e.g., warnings logged, quarantine ratio < 0.1%).
   - `HELD`: Batch rule failure, schema incompatibility, or duplicate ratio breach. Clear justification stated.
2. **Drop Metadata Summary**:
   - File counts, raw bytes, total records, distinct entities, and distinct fields.
   - Temporal spans: [T_event_min, T_event_max] and [T_knowledge_min, T_knowledge_max].
3. **Schema Status & Drift**:
   - Active pinned schema contract version.
   - Detected schema drift items with proposed updates for human review.
4. **Calendar Coverage Matrix**:
   - Tabular record counts per expected calendar period (daily, weekly, monthly, quarterly).
   - Missing periods flagged with explicit gap indicators.
5. **Validation Rule Execution**:
   - Aggregate violation counts categorized by severity (`warn`, `quarantine`, `hold`).
   - Quarantine percentage of total payload.
   - Frequency breakdown of top quarantine causes.
6. **Symbology Resolution Metrics**:
   - Successfully resolved identifier percentage (N_mapped / N_total).
   - Frequency listing of top unresolved entity codes.
7. **Revision Ledger Analytics**:
   - Restatement percentage across active fields.
   - Ranking of top restatements by absolute relative change.
   - Stale vendor alert if zero revisions are observed across > 3 consecutive periods.
8. **Deterministic Vendor Inquiries**:
   - Email and ticketing drafts compiled from pre-defined templates keyed directly to failed rules.
9. **Resource Execution Profile**:
   - Wall-clock run time per stage (discovery, normalization, validation, store write, diffing).
   - Peak resident memory (RSS).

---

## 3. Deterministic Inquiry Generation

To prevent hallucinations, vendor inquiries are generated strictly through deterministic string templates:

```python
INQUIRY_TEMPLATES = {
    "unknown_unit": (
        "Column '{column}' contains {count} observations with undocumented unit of measure: "
        "'{sample_values}'. Which unit is authoritative for downstream calculations?"
    ),
    "coverage_gap": (
        "Expected {calendar} coverage for entity universe shows zero observations during "
        "period {period_identifier}. Did the data feed drop these records in transmission?"
    ),
    "abrupt_drop_contraction": (
        "Current drop row volume of {row_count:,} records represents a {ratio:.1%} reduction "
        "relative to the trailing 7-drop median ({trailing_median:,} rows). "
        "Was this contraction intentional?"
    ),
    "excessive_restatements": (
        "Field '{field}' observed {restatement_count:,} revised historical values "
        "({restatement_rate:.2%} of total entries). Please confirm if an official "
        "restatement was filed by the underlying entities."
    ),
}
```

---

## 4. Verdict Decision State Machine

The overall verdict is derived deterministically:

```
                           [Evaluate Batch Checks]
                                     │
                    ┌────────────────┴────────────────┐
                    ▼ Fail                            ▼ Pass
              Verdict: HELD              [Evaluate Quarantine & Warn]
                                                      │
                                     ┌────────────────┴────────────────┐
                                     ▼ High Severity                   ▼ Zero Violations
                              Verdict: WARNINGS                 Verdict: READY
```

- If `held == true`, verdict is `HELD`.
- Else if `quarantine_count > 0` or `warn_count > 0` or `unmapped_count > 0`, verdict is `READY WITH WARNINGS`.
- Else, verdict is `READY`.
