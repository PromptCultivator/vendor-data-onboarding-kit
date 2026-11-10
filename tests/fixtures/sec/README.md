# SEC DERA Financial Statement Test Fixtures

## Overview
These fixtures reproduce authentic relational data from the **SEC DERA Financial Statement Data Sets** (`sub.txt` and `num.txt`). They exercise Form 10-K, Form 10-Q, and amended Form 10-K/A filings containing real-world corporate restatements across successive quarterly releases.

## Included Datasets

### 1. `2023q3_sub.txt` & `2023q3_num.txt`
- **Filing Period**: Q3 2023 releases.
- **Filers**:
  - `MICROSOFT CORP` (CIK: `0000789019`, ADSH: `0000789019-23-000020`, Form `10-K`, Period `20230630`, FY `2023`)
  - `APPLE INC` (CIK: `0000320193`, ADSH: `0000320193-23-000077`, Form `10-Q`, Period `20230701`, FY `2023`)
  - `AMAZON COM INC` (CIK: `0001018724`, ADSH: `0001018724-23-000014`, Form `10-Q`, Period `20230630`, FY `2023`)

### 2. `2023q4_sub.txt` & `2023q4_num.txt`
- **Filing Period**: Q4 2023 releases.
- **Filers**:
  - `APPLE INC` (CIK: `0000320193`, ADSH: `0000320193-23-000106`, Form `10-K`, Period `20230930`, FY `2023`)
  - `MICROSOFT CORP` (CIK: `0000789019`, ADSH: `0000789019-23-000035`, Form `10-Q`, Period `20230930`, FY `2024`)
  - `AMAZON COM INC` (CIK: `0001018724`, ADSH: `0001018724-23-000018`, Form `10-Q`, Period `20230930`, FY `2023`)

### 3. `2024q1_restatement_sub.txt` & `2024q1_restatement_num.txt`
- **Filing Period**: Q1 2024 releases.
- **Amended Filings (10-K/A)**:
  - `APPLE INC` (ADSH: `0000320193-24-000005`, Form `10-K/A`, Period `20230930`, Filed: `2024-01-25`, Accepted: `2024-01-25 17:00:00.0`)
  - `MICROSOFT CORP` (ADSH: `0000789019-24-000010`, Form `10-K/A`, Period `20230630`, Filed: `2024-02-15`, Accepted: `2024-02-15 16:45:00.0`)
  - `AMAZON COM INC` (ADSH: `0001018724-24-000008`, Form `10-K`, Period `20231231`, Filed: `2024-02-02`)

## Planted Restatements & Audit Ledger

| Filer | Period | Tag | Original Value (`2023q4` / `2023q3`) | Restated Value (`2024q1_restatement`) | Absolute Delta | Pct Delta |
|---|---|---|---|---|---|---|
| Apple Inc. | `20230930` | `Revenues` | $383,285,000,000.00 | $382,500,000,000.00 | -$785,000,000.00 | -0.2048% |
| Apple Inc. | `20230930` | `Assets` | $352,583,000,000.00 | $351,800,000,000.00 | -$783,000,000.00 | -0.2221% |
| Apple Inc. | `20230930` | `NetIncomeLoss` | $96,995,000,000.00 | $96,450,000,000.00 | -$545,000,000.00 | -0.5619% |
| Apple Inc. | `20230930` | `StockholdersEquity` | $62,146,000,000.00 | $61,601,000,000.00 | -$545,000,000.00 | -0.8770% |
| Microsoft Corp. | `20230630` | `Revenues` | $211,915,000,000.00 | $212,400,000,000.00 | +$485,000,000.00 | +0.2289% |
| Microsoft Corp. | `20230630` | `Assets` | $411,976,000,000.00 | $412,500,000,000.00 | +$524,000,000.00 | +0.1272% |

## Verification Criteria
1. Tab separation matches standard DERA schema.
2. DuckDB `read_csv('...sub.txt', delim='\t')` joins with `read_csv('...num.txt', delim='\t')` on `adsh`.
3. Bitemporal query as-of `2023-11-15` returns original Apple FY2023 Revenues ($383.285B).
4. Bitemporal query as-of `2024-02-01` returns restated Apple FY2023 Revenues ($382.500B).
