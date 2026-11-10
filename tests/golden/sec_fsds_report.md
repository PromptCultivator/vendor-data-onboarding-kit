# Vendor Readiness Report: sec_fsds

- **Drop Identifier:** `[STABILIZED_DROP_ID]`
- **Generated At:** 2024-01-15 00:00:00 UTC

## 1. Verdict

**Verdict:** `READY`

> **Reason:** All validation checks passed, zero quarantine records, no schema drift.

## 2. Drop Summary

| Metric | Value |
|---|---|
| Files | 2 (`num.txt, sub.txt`) |
| Raw Payload Size | 30,992 bytes |
| Total Records | 250 |
| Distinct Entities | 50 |
| Distinct Fields | 5 |
| Event Time Range | 2023-06-30 00:00:00 UTC to 2023-06-30 00:00:00 UTC |
| Knowledge Time Range | 2024-01-15 00:00:00 UTC to 2024-01-15 00:00:00 UTC |

## 3. Schema

- **Pinned Schema Version:** 1
- **Status:** COMPLIANT (0 schema differences observed against pinned contract).

## 4. Coverage by Period

- **Expected Calendar Cadence:** `quarterly`
- **Total Reporting Periods:** 1
- **Coverage Gaps:** 0

| Period | Observations | Status |
|---|---|---|
| 2023-Q2 | 250 | OK |

## 5. Validation

| Metric | Count | Ratio |
|---|---|---|
| Total Evaluated Rows | 250 | 100.0% |
| Conforming Valid Rows | 250 | 100.00% |
| Quarantined Rows | 0 | 0.00% |
| Warning Tier Flags | 0 | - |
| Batch Hold Status | `PASSED` | - |

## 6. Identifiers

| Metric | Count | Ratio |
|---|---|---|
| Total Evaluated Rows | 250 | 100.0% |
| Resolved Identifiers | 250 | 100.00% |
| Unmapped Identifiers | 0 | 0.00% |

## 7. Revisions

- **Total Historical Revisions:** 0
- **Entities with Restated Values:** 0

### Restatement Rate per Field

| Field | Restatement Rate |
|---|---|
| `Assets|USD|2` | 0.00% |
| `Liabilities|USD|2` | 0.00% |
| `NetIncomeLoss|USD|2` | 0.00% |
| `OperatingIncomeLoss|USD|2` | 0.00% |
| `Revenues|USD|2` | 0.00% |

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
