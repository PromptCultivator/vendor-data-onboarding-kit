# FRED / ALFRED Historical Vintage Series Test Fixtures

## Overview
These fixtures reproduce macroeconomic vintage data from the Federal Reserve Bank of St. Louis **ALFRED** (ArchivaL Federal Reserve Economic Data) database. Each record captures the exact bitemporal horizon when an economic fact was released and superseded.

## Bitemporal Schema
Standard ALFRED columns:
- `realtime_start`: Date (YYYY-MM-DD) when this vintage was published ($t_k$).
- `realtime_end`: Date (YYYY-MM-DD) when this vintage was superseded by a revision ($t_k'$), or `9999-12-31` if active.
- `date`: Macroeconomic observation period date ($t_e$).
- `value`: Numerical observation fact.

## Included Series

### 1. `GDPC1_vintages.csv` (Real Gross Domestic Product)
- **Unit**: Billions of Chained 2012 / 2017 Dollars, Seasonally Adjusted Annual Rate.
- **Frequency**: Quarterly (`2022-01-01` to `2023-10-01`).
- **Release Schedule**:
  1. *Advance Estimate*: ~28 days after quarter close.
  2. *Second Estimate*: ~56 days after quarter close.
  3. *Third Estimate*: ~85 days after quarter close.
  4. *Annual Comprehensive Update*: Released September 28, 2023 (re-benchmarked to 2017 Chained Dollars).

### 2. `PAYEMS_vintages.csv` (All Employees, Total Nonfarm Payrolls)
- **Unit**: Thousands of Persons, Seasonally Adjusted.
- **Frequency**: Monthly (`2023-01-01` to `2023-12-01`).
- **Release Schedule**: First Friday of each subsequent month, followed by 1st revision, 2nd revision, and annual BLS benchmark revision (Feb 2, 2024).

### 3. `CPIAUCSL_vintages.csv` (Consumer Price Index for All Urban Consumers: All Items)
- **Unit**: Index 1982-1984=100, Seasonally Adjusted.
- **Frequency**: Monthly (`2023-01-01` to `2023-12-01`).
- **Release Schedule**: Mid-month (~10th-14th), followed by annual seasonal adjustment revision on February 13, 2024.

## Expected As-Of Test Values (Zero Look-Ahead Invariants)

| Series | Observation Date (`date`) | As-Of Date (`as_of`) | Expected Vintage | Expected Value | Rationale |
|---|---|---|---|---|---|
| `GDPC1` | `2023-01-01` (Q1 2023) | `2023-05-01` | Advance | `20005.109` | As of May 1, 2023, only Advance release (Apr 27) was known. Second release (May 25) must NOT leak. |
| `GDPC1` | `2023-01-01` (Q1 2023) | `2023-06-01` | Second | `20023.231` | As of June 1, 2023, Second release (May 25) was active. |
| `GDPC1` | `2023-01-01` (Q1 2023) | `2023-10-01` | Comprehensive | `22112.329` | Rebenchmarked to 2017 chained dollars on Sep 28, 2023. |
| `PAYEMS` | `2023-01-01` (Jan 2023) | `2023-02-15` | Initial Release | `155073` | Released Feb 3, 2023. First revision (Mar 10) not yet known. |
| `PAYEMS` | `2023-01-01` (Jan 2023) | `2023-03-15` | 1st Revision | `155120` | Released Mar 10, 2023. |
| `PAYEMS` | `2023-01-01` (Jan 2023) | `2024-03-01` | Benchmark | `155350` | BLS Annual Benchmark released Feb 2, 2024. |
| `CPIAUCSL`| `2023-06-01` (Jun 2023) | `2023-08-01` | Initial Release | `304.127` | Released Jul 12, 2023. |
| `CPIAUCSL`| `2023-06-01` (Jun 2023) | `2024-03-01` | Seasonal Rev | `304.131` | Annual seasonal factor update on Feb 13, 2024. |
