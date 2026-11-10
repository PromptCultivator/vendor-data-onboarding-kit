# Vendor Readiness Report: synthetic

- **Drop Identifier:** `[STABILIZED_DROP_ID]`
- **Generated At:** 2024-01-15 00:00:00 UTC

## 1. Verdict

**Verdict:** `HELD`

> **Reason:** Batch placed on HOLD: Rule 'duplicate_threshold': Duplicate primary key share 0.0199 exceeds threshold 0.0100 (64 duplicate rows out of 3222)

## 2. Drop Summary

| Metric | Value |
|---|---|
| Files | 1 (`data.csv`) |
| Raw Payload Size | 291,675 bytes |
| Total Records | 3,222 |
| Distinct Entities | 10 |
| Distinct Fields | 1 |
| Event Time Range | 2024-01-01 00:00:00 UTC to 2024-01-01 00:05:15 UTC |
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
| 2024-01-01 | 3,222 | OK |

## 5. Validation

| Metric | Count | Ratio |
|---|---|---|
| Total Evaluated Rows | 3,222 | 100.0% |
| Conforming Valid Rows | 0 | 0.00% |
| Quarantined Rows | 64 | 1.99% |
| Warning Tier Flags | 0 | - |
| Batch Hold Status | `HELD` | - |

### Top Validation Quarantine Causes

| Rule / Reason | Severity | Occurrences |
|---|---|---|
| `duplicate_key` | QUARANTINE | 64 |
| `duplicate_threshold` | QUARANTINE | 1 |

## 6. Identifiers

| Metric | Count | Ratio |
|---|---|---|
| Total Evaluated Rows | 3,222 | 100.0% |
| Resolved Identifiers | 3,222 | 100.00% |
| Unmapped Identifiers | 0 | 0.00% |

## 7. Revisions

- **Total Historical Revisions:** 0
- **Entities with Restated Values:** 0

## 8. Questions for the Vendor

1. Batch contains 64 duplicate observation keys within the same transmission drop. Which record is authoritative?
2. Batch-level duplicate key threshold breached. Please investigate upstream ingestion deduplication failures.

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
