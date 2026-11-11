# Vendor Data Onboarding Kit (vdok)

[![CI](https://github.com/PromptCultivator/vendor-data-onboarding-kit/actions/workflows/ci.yml/badge.svg)](https://github.com/PromptCultivator/vendor-data-onboarding-kit/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python Version](https://img.shields.io/badge/python-3.11%20%7C%203.12-blue.svg)](https://www.python.org/downloads/)
[![Code style: ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

> An append-only bitemporal data onboarding kit that ingests raw vendor feeds into structured Parquet storage with zero look-ahead bias. It validates incoming datasets against pinned schema contracts, tracks historical restatements in a revision ledger, and isolates corrupt records using multi-tier severity routing. Quantitative teams use this toolkit to prevent future information leakage and ensure reproducible backtests.

## ✨ Features

- **Append-Only Bitemporal Store**: Partitions records by both event time and knowledge time into immutable Apache Parquet files, preventing retroactive in-place data mutations.
- **Vectorized DuckDB Validation Engine**: Executes high-throughput SQL validation checks directly in-process without external database services.
- **Multi-Tier Severity Routing**: Automatically routes valid rows to the store, logs warnings to an event ledger, quarantines bad rows into `quarantine.parquet`, and halts breaking batch violations.
- **Schema Pinning and Drift Detection**: Infers and locks vendor schema contracts in declarative YAML, detecting unexpected column additions, drops, or type alterations.
- **Historical Revision Ledger**: Automatically compares incoming drop vintages against previous deliveries to capture restatements and record exact numerical deltas.
- **Point-in-Time Query Engine**: Delivers sub-millisecond point-in-time reads and chronologically sorted series with O(log N) binary search partition indexes.
- **Look-Ahead Leak Detector**: Audits query access logs to verify that point-in-time reads never access knowledge published after historical decision points.
- **Model-Free Point-in-Time Symbology**: Resolves entity identifiers (CIKs, tickers, CUSIPs) through temporal symbol master tables without survivorship bias or ambiguous fuzzy guessing.
- **Core Behavioral Guarantees**: Enforces physical append-only immutability (G1), zero look-ahead future reads (G2), and drop ingestion idempotency (G3).

## 🚀 Getting Started

### Prerequisites

- **Python**: Version `3.11` or `3.12`.
- **Package Manager**: [`uv`](https://docs.astral.sh/uv/) (recommended) or standard `pip`.
- **Core Dependencies**:
  - DuckDB `== 2.0.0`
  - PyArrow `== 26.0.1`
  - Polars `== 2.1.0`
  - Pydantic `== 2.14.3`
  - Typer `== 0.29.0`
- **Optional Runtimes**: Docker `20.10+` for containerized execution.
- **Optional Configurations**:
  - `FRED_API_KEY`: Required only when fetching optional vintage series from Federal Reserve Economic Data (not required for synthetic or SEC FSDS data).
  - `VDOK_CONTACT`: User-Agent contact declaration string when downloading public SEC archives (e.g., `User Name user@example.com`).

### Installation

Clone the repository and install dependencies:

```bash
# Clone the repository
git clone https://github.com/PromptCultivator/vendor-data-onboarding-kit.git
cd vendor-data-onboarding-kit

# Create the virtual environment and install pinned dependencies
make setup

# Activate the virtual environment
source .venv/bin/activate

# (Optional) Set environment variables for data downloads
export VDOK_CONTACT="YourName yourname@example.com"
export FRED_API_KEY="your_api_key_here"
```

### Running the Project

Run the end-to-end synthetic pipeline to generate data, onboard a drop, and review readiness:

```bash
# 1. Generate a synthetic dataset sequence with planted anomalies
make synth SIZE=50MB SEED=1

# 2. Onboard the first generated drop into the bitemporal store
make onboard VENDOR=synthetic DROP=data/raw/synthetic/drop_001.csv

# 3. Generate and view the vendor readiness report
make report VENDOR=synthetic

# Alternatively, run the end-to-end benchmark smoke verification
make bench-smoke
```

## 💻 Usage & Min&#105;mal Examples

![vdok Onboarding and Audit Demo](docs/assets/demo.gif)
> *Walk-through illustrating raw drop ingestion, validation checks, and readiness report generation.*

### Python Point-in-Time Query

Query financial records as of a historical date without look-ahead bias:

```python
from vdok.query import Store

# Open the bitemporal store with query audit logging enabled
store = Store("data/store/sec_fsds", audit=True)

# Query a point-in-time value as known on a historical cutoff date
record = store.value(
    entity_id="0000320193",  # Apple Inc. CIK
    field="Revenues|USD|4Q",  # Normalized metric name
    event_time="2023-12-31",  # Period end date
    as_of="2024-02-15",  # Historical knowledge cutoff
)

if record:
    print(f"Entity:         {record.entity_id}")
    print(f"Observed Value: {record.value_num} {record.unit}")
    print(f"Knowledge Time: {record.knowledge_time}")

store.close()
```

### CLI Leak Audit

Analyze query audit logs to verify that backtest signals did not access future data:

```bash
# Scan query audit logs for look-ahead read violations
vdok leaks --log data/store/sec_fsds/audit/query_audit.jsonl
```

## 📁 Project Structure

```
vendor-data-onboarding-kit/
├── .github/          # CI/CD workflows and automated validation jobs
├── bench/            # Performance benchmarking suites and hardware profiling
├── data/             # Ingestion targets, raw archives, and partitioned Parquet stores
├── docs/             # Technical specifications, architectural decision records, and component guides
├── manifests/        # Declarative vendor configuration manifests
├── rules/            # Vectorized validation rules and severity classifications
├── schemas/          # Pinned vendor schema contracts and drift baselines
├── scripts/          # Automation scripts for dataset extraction and fixture creation
├── tests/            # Test suites covering unit, property, and golden report cases
├── vdok/             # Core Python package and CLI implementation
├── Dockerfile        # Container build definition for reproducible testing
├── LICENSE           # Apache License 2.0 terms
├── Makefile          # Project automation shortcuts
└── pyproject.toml    # Python package dependencies and tooling configuration
```

- `manifests/`: Declarative YAML configurations specifying raw drop file layouts, column mappings, and timestamp parsing rules.
- `rules/`: Vectorized DuckDB SQL validation checks categorized by severity tiers (`warn`, `quarantine`, and `hold`).
- `schemas/`: Pinned vendor schema contracts used to detect unannounced upstream column additions, removals, or type mutations.
- `bench/`: Automated benchmarking tools that measure throughput, peak memory usage, and temporal correctness across workload sizes.
- `docs/`: Technical specifications, architectural decision records (ADRs 001–005), and subsystem guides.
- `vdok/`: Core implementation package housing the bitemporal store, revision engine, symbology resolver, and leak auditor.

## 🤝 Contributing

We welcome contributions to the toolkit. Follow these three steps to submit bug fixes or improvements:

1. **Fork and Branch**: Fork the repository on GitHub and create a topic branch from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```
2. **Implement and Test**: Apply your changes and confirm all quality gates pass:
   ```bash
   make lint       # Run ruff lint and format checks
   make typecheck  # Run pyright static type analysis
   make test       # Run pytest test suite with coverage gate (>= 85%)
   ```
3. **Open a Pull Request**: Push your branch to GitHub and submit a Pull Request describing your changes and testing approach.

## 📄 License

This project is licensed under the Apache License, Version 2.0. See the [LICENSE](LICENSE) file for full details.
