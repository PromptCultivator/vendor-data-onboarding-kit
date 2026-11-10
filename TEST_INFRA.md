# E2E Test Infra: vendor-data-onboarding-kit

## Test Philosophy
- **Opaque-Box & Requirement-Driven**: Tests are designed directly against `ORIGINAL_REQUEST.md` and user specifications, completely independent of internal implementation artifacts.
- **Methodology**: Systematic 4-tier methodology:
  1. *Category-Partition Testing* (representative happy paths per feature)
  2. *Boundary Value Analysis (BVA)* (domain extremes, clock jumps, IEEE 754 specials, corrupted formatting)
  3. *Pairwise Combinatorial Testing* (cross-subsystem interactions, symbology changes + restatements + bitemporal queries)
  4. *Real-World Workload Testing* (authentic SEC 10-K/A corporate restatements and ALFRED macroeconomic vintages)
- **Progressive Testability**: Verification mechanisms must not depend on features more complex than what is being tested.
- **Zero-Tolerance Invariants**: Strict mathematical assertions on point-in-time correctness (0 violations) and revision recall (100.00%).

## Feature Inventory & Tier Mapping

| # | Feature | Category | Tier 1 (Coverage) | Tier 2 (Boundary) | Tier 3 (Cross-Feature) | Tier 4 (Workload) |
|---|---|---|:---:|:---:|:---:|:---:|
| 1 | Multi-Format Ingestion (CSV, JSON, Parquet, API) | Ingestion | ≥ 5 | ≥ 5 | ✓ | ✓ |
| 2 | Lineage & Manifest Tracker (SHA-256) | Ingestion | ≥ 5 | ≥ 5 | ✓ | |
| 3 | Schema Inference & Pinning Contract | Schema | ≥ 5 | ≥ 5 | ✓ | ✓ |
| 4 | Schema Drift Detection (Breaking/Warn/Info) | Schema | ≥ 5 | ≥ 5 | ✓ | |
| 5 | Canonical Bitemporal Data Model | Model | ≥ 5 | ≥ 5 | ✓ | ✓ |
| 6 | Point-in-Time Symbology & Resolution | Symbology | ≥ 5 | ≥ 5 | ✓ | ✓ |
| 7 | Cross-Identifier Master & Quarantine | Symbology | ≥ 5 | ≥ 5 | ✓ | |
| 8 | Completeness & SLA Null Validation | Validation | ≥ 5 | ≥ 5 | ✓ | ✓ |
| 9 | Range & Statistical Domain Validation | Validation | ≥ 5 | ≥ 5 | ✓ | ✓ |
| 10 | Intra-Drop Duplicate Key & Conflict Validation | Validation | ≥ 5 | ≥ 5 | ✓ | |
| 11 | Monotone Timestamp & Clock Jump Check | Validation | ≥ 5 | ≥ 5 | ✓ | |
| 12 | Future Timestamp Guard | Validation | ≥ 5 | ≥ 5 | ✓ | |
| 13 | Universe Coverage & Dropout Validation | Validation | ≥ 5 | ≥ 5 | ✓ | |
| 14 | Vectorized High-Throughput Validation | Validation | ≥ 5 | ≥ 5 | ✓ | ✓ |
| 15 | Append-Only Partitioned Parquet Store | Storage | ≥ 5 | ≥ 5 | ✓ | ✓ |
| 16 | DuckDB SQL Catalog & Macro Views | Storage | ≥ 5 | ≥ 5 | ✓ | ✓ |
| 17 | Point-in-Time ("As-Of") Query Engine | Storage | ≥ 5 | ≥ 5 | ✓ | ✓ |
| 18 | Full Vintage Audit Query Trajectory | Storage | ≥ 5 | ≥ 5 | ✓ | ✓ |
| 19 | Storage Expansion Factor Reporter | Storage | ≥ 5 | ≥ 5 | ✓ | |
| 20 | Multi-Drop Diffing Engine | Revision | ≥ 5 | ≥ 5 | ✓ | ✓ |
| 21 | Restatement Detection & Flagging | Revision | ≥ 5 | ≥ 5 | ✓ | ✓ |
| 22 | Revision Magnitude Delta Statistics | Revision | ≥ 5 | ≥ 5 | ✓ | |
| 23 | Daily Coverage Matrix & Dip Detector | Report | ≥ 5 | ≥ 5 | ✓ | ✓ |
| 24 | Validation Scorecard Generator | Report | ≥ 5 | ≥ 5 | ✓ | ✓ |
| 25 | Automated Vendor Questions Generator | Report | ≥ 5 | ≥ 5 | ✓ | ✓ |
| 26 | Multi-Format Markdown/JSON Report Emitter | Report | ≥ 5 | ≥ 5 | ✓ | |
| 27 | SEC Financials Loader & Restatements | Datasets | ≥ 5 | ≥ 5 | ✓ | ✓ |
| 28 | FRED/ALFRED Vintage Series Loader | Datasets | ≥ 5 | ≥ 5 | ✓ | ✓ |
| 29 | Scalable Multi-Profile Synthetic Generator | Datasets | ≥ 5 | ≥ 5 | ✓ | ✓ |
| 30 | CLI Suite & API Runner | CLI / API | ≥ 5 | ≥ 5 | ✓ | ✓ |

## Test Architecture

### Directory Layout
```
tests/
├── conftest.py                   # Global fixtures, temporary databases, test environments
├── fixtures/                     # Bundled representative sample data
│   ├── sec/                      # SEC DERA quarterly slices (sub.txt, num.txt, 10-K/A restatements)
│   ├── alfred/                   # FRED/ALFRED historical vintage series CSVs
│   └── synthetic/                # Ground truth test datasets & manifests
├── test_tier1_features/          # Tier 1: Feature Isolation Unit Tests (>= 5 tests per feature)
│   ├── test_ingest_formats.py
│   ├── test_manifest.py
│   ├── test_schema_engine.py
│   ├── test_canonical_model.py
│   ├── test_symbology.py
│   ├── test_validation_rules.py
│   ├── test_bitemporal_store.py
│   ├── test_diff_engine.py
│   ├── test_readiness_report.py
│   ├── test_dataset_loaders.py
│   └── test_cli_api.py
├── test_tier2_boundaries/        # Tier 2: Boundary Value Analysis & Malformed Inputs (>= 5 tests per feature)
│   ├── test_corrupt_files.py
│   ├── test_numeric_extremes.py
│   ├── test_temporal_DST_leap.py
│   ├── test_schema_drift_limits.py
│   └── test_unicode_encodings.py
├── test_tier3_interactions/      # Tier 3: Cross-Feature Interactions & Bitemporal Invariants
│   ├── test_symbology_diff_interaction.py
│   ├── test_duplicate_isolation_diff.py
│   ├── test_point_in_time_invariance.py
│   └── test_quarantine_pipeline_flow.py
└── test_tier4_realworld/         # Tier 4: Real-World Workload Scenarios
    ├── test_sec_restatements_workload.py
    └── test_alfred_vintage_alpha_workload.py
```

### Test Invocation Semantics
- Full test suite runner: `uv run pytest -v tests/`
- Tier-specific execution: `uv run pytest -v tests/test_tier1_features/`
- Strict pass criteria: Exit code 0, zero warnings treated as errors, 100% assertions verified.

## Real-World Application Scenarios (Tier 4)

### Scenario 4A: SEC Financial Statement Ingestion & 10-K/A Restatement Verification
- **Features Exercised**: Multi-format parsing, SEC data model, CIK symbology resolution, balance sheet validation rules, append-only bitemporal store, 10-K/A restatement ledger, readiness reporting.
- **Complexity**: High.
- **Workflow**:
  1. Ingest baseline quarterly SEC facts (Form 10-K) for a portfolio of filers.
  2. Ingest subsequent amended filing (Form 10-K/A) reclassifying revenue/assets.
  3. Verify original facts remain accessible at historical as-of timestamp.
  4. Verify revision ledger detects 100% of restated line items with delta statistics.
  5. Verify readiness report flags restatements and generates vendor inquiries.

### Scenario 4B: ALFRED Macroeconomic Series & Zero Look-Ahead Bias Backtest
- **Features Exercised**: Time-series ingestion, macroeconomic observation date ($t_e$) vs vintage date ($t_k$), as-of temporal queries, point-in-time invariant assertion.
- **Complexity**: High.
- **Workflow**:
  1. Ingest successive vintages of US Real GDP (`GDPC1`) and Nonfarm Payrolls (`PAYEMS`).
  2. Execute simulated quantitative backtest requiring data as of FOMC announcement dates.
  3. Verify query outputs exactly match data available on the historical date with 0 point-in-time leakage.

## Coverage Thresholds
- **Tier 1 (Feature Coverage)**: $\ge 5$ test cases per feature across all 30 features ($\ge 150$ test cases).
- **Tier 2 (Boundary & Corner)**: $\ge 5$ boundary/extreme test cases per feature area ($\ge 150$ test cases).
- **Tier 3 (Cross-Feature Combinations)**: $\ge 30$ pairwise interaction test cases testing stateful pipelines.
- **Tier 4 (Real-World Application Scenarios)**: $\ge 5$ end-to-end multi-step application scenarios.
- **Total Minimum Target**: **$\ge 335$ automated test cases**.
