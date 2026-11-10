# Synthetic Test Fixtures & Ground Truth Ledger

## Overview
These synthetic fixtures test multi-drop ingestion, schema inference and pinning, drift classification, vectorized rule validation, diffing engine recall, and append-only bitemporal queries.

## File Manifest

| File | Format | Record Count | Description |
|---|---|---|---|
| `drop_001_baseline.csv` | CSV | 60 | Baseline Drop 1: 50 daily close prices (10 equities x 5 days) + 10 volume records |
| `drop_001_baseline.parquet` | Parquet | 60 | Identical PyArrow / Parquet table for high-throughput vectorized validation |
| `drop_001_baseline.json` | JSON Array | 60 | Identical JSON records testing multi-format parser parity |
| `drop_002_restatements.csv` | CSV | 65 | Drop 2: 10 restated records, 10 new records, 45 unchanged records (5 baseline dropped) |
| `drop_002_restatements.parquet`| Parquet | 65 | Drop 2 Parquet table |
| `drop_003_anomalies.csv` | CSV | 21 | Drop 3 containing 10 planted anomalies + 11 valid control records |
| `drop_003_anomalies.jsonl` | JSONL | 21 | Drop 3 in line-delimited JSON format |
| `schema_baseline.json` | JSON | - | Pinned schema contract v1.0.0 |
| `schema_drift_breaking.json` | JSON | - | Schema v2.0.0 with BREAKING changes (dropped column `unit`, incompatible type `value`: double -> string) |
| `schema_drift_warning.json` | JSON | - | Schema v1.1.0 with WARNING changes (relaxed nullability on `source_symbol`, added optional `currency_code`) |
| `truth_manifest.parquet` | Parquet | 156 | Ground truth verification ledger tracking all mutations and planted anomalies across Drops 1-3 |

## Drop 1 vs Drop 2 Diff Specification

| Metric | Ground Truth Count | Notes |
|---|---|---|
| Drop 1 Baseline Records | 60 | Initial drop ingested as-of `2023-10-06T20:00:00Z` |
| Drop 2 Records | 65 | Ingested as-of `2023-10-13T20:00:00Z` |
| **New Records** | **10** | Records for trading date `2023-10-09` across all 10 equities |
| **Unchanged Records** | **45** | Identical event time and values |
| **Restated Records** | **10** | Line items restated with explicit delta (`REC_002_REST_0001` .. `0010`) |
| **Dropped Records** | **5** | Baseline records `REC_001_0011` .. `REC_001_0015` omitted in Drop 2 |
| **Expected Restatement Recall** | **100.00%** | Zero false negatives |
| **Expected Restatement Precision** | **100.00%** | Zero false positives on unchanged records |

## Planted Anomaly Catalog (Drop 3)

| Record ID | Symbol | Planted Anomaly | Target Rule | Expected Outcome |
|---|---|---|---|---|
| `REC_003_ANOM_0001` | `AAPL` | `value = -178.50` | `F-VAL-02` (Range & Domain) | Fail (Negative price) |
| `REC_003_ANOM_0002` | `TSLA` | `value = -242.00` | `F-VAL-02` (Range & Domain) | Fail (Negative price) |
| `REC_003_ANOM_0003` | `MSFT` | `value = None` | `F-VAL-01` (Completeness / SLA Null) | Fail (Null value in mandatory field) |
| `REC_003_ANOM_0004` | `GOOGL`| `entity_id = None` | `F-VAL-01` (Completeness / SLA Null) | Fail (Null entity identifier) |
| `REC_003_ANOM_0005` | `AMZN` | `event_time = 2025-10-16` | `F-VAL-05` (Future Timestamp Guard) | Fail (Event time > knowledge time) |
| `REC_003_ANOM_0006` | `NVDA` | `event_time = 2030-01-01` | `F-VAL-05` (Future Timestamp Guard) | Fail (Event time far in future) |
| `REC_003_ANOM_0007` | `META` | Inverted `event_time` (2023-10-15 after 17th) | `F-VAL-04` (Monotone Timestamp) | Fail (Non-monotone clock jump) |
| `REC_003_ANOM_0008` | `META` | Inverted `event_time` (2023-10-14 after 15th) | `F-VAL-04` (Monotone Timestamp) | Fail (Clock jump backwards) |
| `REC_003_ANOM_0009` | `UNKNOWN_CORP_XYZ99` | Unknown symbol | `F-SYM-03` (Unmapped Entity Quarantine) | Quarantine (`UNRESOLVED_SYNTH_XYZ99`) |
| `REC_003_ANOM_0010` | `GHOST_TICKER_999` | Unknown symbol | `F-SYM-03` (Unmapped Entity Quarantine) | Quarantine (`UNRESOLVED_SYNTH_999`) |

## Ground Truth Manifest Schema
The `truth_manifest.parquet` file contains the canonical ledger for automated testing assertions:
- `drop_id` (string)
- `record_id` (string)
- `entity_id` (string)
- `source_symbol` (string)
- `event_time` (timestamp[us, UTC])
- `knowledge_time` (timestamp[us, UTC])
- `value` (double, nullable)
- `mutation_type` (string: BASELINE, UNCHANGED, RESTATED, NEW, DELETED, ANOMALOUS)
- `restatement_delta` (double)
- `restatement_pct_delta` (double)
- `is_planted_restatement` (bool)
- `is_planted_anomaly` (bool)
- `expected_rule_violation` (string)
