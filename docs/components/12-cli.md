# Component: CLI Workflows & Tooling (`vdok.cli`)

## 1. Overview & Responsibility

The `vdok.cli` module exposes the platform's capabilities via a unified command-line application built with Typer. Designed for quantitative analysts, data engineers, and automated scheduled batch runners (e.g., cron, Airflow, Prefect), the CLI provides composable commands for onboarding, schema management, diffing, reporting, and data leak auditing.

The console script entry point is registered as `vdok` in `pyproject.toml`.

---

## 2. Command Hierarchy

```
vdok
├── schema
│   ├── infer     # Sample raw data files and generate contract draft
│   ├── pin       # Pin inferred schema as an authoritative contract
│   ├── check     # Compare a drop against the pinned contract
│   └── accept    # Accept proposed schema modifications
├── onboard       # Execute full ingestion, validation, and storage
├── report        # Compile readiness scorecard and vendor inquiries
├── diff          # Compare two drops and identify restatements
├── synth         # Generate synthetic drops with planted faults
└── leaks         # Audit query logs for temporal causality violations
```

---

## 3. Subcommand Specifications & Invocations

### 3.1 `vdok schema` Group

```bash
# Infer schema from raw vendor files
vdok schema infer data/raw/sec_fsds/2023q4/ --sample-rows 200000 -o schemas/sec_fsds.draft.yaml

# Pin schema as the authoritative contract
vdok schema pin schemas/sec_fsds.draft.yaml -o schemas/sec_fsds.yaml

# Check a new drop for schema drift
vdok schema check sec_fsds data/raw/sec_fsds/2024q1/

# Accept schema changes proposed in a readiness report
vdok schema accept sec_fsds
```

### 3.2 `vdok onboard`
Executes discovery, schema checking, normalization, validation, partitioned store write, and revision detection in a single execution pass:

```bash
vdok onboard sec_fsds data/raw/sec_fsds/2023q4/ --as-of "2024-01-15T00:00:00Z"
```

Exit Codes:
- `0`: Ingestion completed successfully (`READY` or `READY WITH WARNINGS`).
- `1`: Batch placed on `HOLD` due to structural violations or volume collapse.
- `2`: System execution error (I/O, invalid arguments, or missing manifest).

### 3.3 `vdok report`
Generates human-readable Markdown and machine-readable JSON readiness reports:

```bash
vdok report sec_fsds --drop 8f9a2e3b1c4d5e6f --output-dir data/results/sec_fsds/
```

### 3.4 `vdok diff`
Performs drop-over-drop analytical diffing between two historical drops:

```bash
vdok diff sec_fsds drop_001 drop_002
```

Outputs tabular counts of added keys, dropped keys, restated values, and unchanged entries.

### 3.5 `vdok leaks`
Inspects historical query audit logs to verify that strategy backtests operated without look-ahead bias:

```bash
vdok leaks --log data/audit/query_log.jsonl --decision-time-column decision_time
```

Reports temporal violations where query requests exceeded model decision timestamps.

### 3.6 `vdok synth`
Generates reproducible synthetic datasets for pipeline benchmarking and stress testing:

```bash
vdok synth --size-gb 1 --seed 42 --faults default --output-dir data/synth/
```

---

## 4. Integration with Automation Tooling

The CLI is designed for direct integration with standard CI/CD runners and workflow orchestrators:
- Emits structured exit codes.
- Writes diagnostic error messages to `stderr` and structured results to `stdout`.
- Avoids interactive prompts during standard operation to ensure headless automation reliability.
