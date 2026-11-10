# Vendor Readiness Report: synthetic

- **Drop Identifier:** `[STABILIZED_DROP_ID]`
- **Generated At:** 2024-01-15 00:00:00 UTC

## 1. Verdict

**Verdict:** `READY`

> **Reason:** All validation checks passed, zero quarantine records, no schema drift.

## 2. Drop Summary

| Metric | Value |
|---|---|
| Files | 1 (`data.csv`) |
| Raw Payload Size | 571,679 bytes |
| Total Records | 6,316 |
| Distinct Entities | 10 |
| Distinct Fields | 1 |
| Event Time Range | 2024-01-01 00:00:00 UTC to 2024-01-01 00:10:31 UTC |
| Knowledge Time Range | 2024-01-15 00:00:00 UTC to 2024-01-15 00:00:00 UTC |

## 3. Schema

- **Pinned Schema Version:** 1
- **Status:** COMPLIANT (0 schema differences observed against pinned contract).

## 4. Coverage by Period

- **Expected Calendar Cadence:** `daily`
- **Total Reporting Periods:** 1
- **Coverage Gaps:** 0

| Period | Observations | Status |
|---|---|---|
| 2024-01-01 | 6,316 | OK |

## 5. Validation

| Metric | Count | Ratio |
|---|---|---|
| Total Evaluated Rows | 6,316 | 100.0% |
| Conforming Valid Rows | 6,316 | 100.00% |
| Quarantined Rows | 0 | 0.00% |
| Warning Tier Flags | 0 | - |
| Batch Hold Status | `PASSED` | - |

## 6. Identifiers

| Metric | Count | Ratio |
|---|---|---|
| Total Evaluated Rows | 6,316 | 100.0% |
| Resolved Identifiers | 6,316 | 100.00% |
| Unmapped Identifiers | 0 | 0.00% |

## 7. Revisions

- **Total Historical Revisions:** 0
- **Entities with Restated Values:** 0

### Restatement Rate per Field

| Field | Restatement Rate |
|---|---|
| `price` | 0.00% |

## 8. Questions for the Vendor

Zero vendor inquiries required. Drop meets all data quality criteria.

## 9. Performance & Resource Profile

| Stage | Duration |
|---|---|
| Discovery  | 0.100 s |
| Schema  | 0.100 s |
| Normalization  | 0.100 s |
| Validation  | 0.100 s |
| Store write  | 0.100 s |
| Revisions  | 0.100 s |
| Reporting  | 0.100 s |
| **Total Wall Clock** | **0.500 s** |

- **Peak Resident Memory (RSS):** 50.00 MB
