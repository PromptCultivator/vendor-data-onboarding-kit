# Component: CLI Workflows & Tooling (`vdok.cli`)

## 1. Overview & Responsibility

`vdok.cli` exposes the toolkit through a Typer command-line application registered as the console entry point `vdok` in `pyproject.toml`. All commands validate input paths and arguments strictly and emit deterministic exit codes (`0` success, `1` batch hold or look-ahead leaks detected, `2` missing file/argument or configuration error) for headless batch and CI automation.

---

## 2. Command Tree

```
vdok
├── version        # Print installed package version
├── data           # Download SEC FSDS quarterly archives and symbol master
├── synth          # Generate seeded synthetic drops with planted faults
├── onboard        # Run discovery, schema check, normalization, validation, store, revisions, report
├── report         # Render or inspect a readiness report (Markdown or JSON)
├── diff           # Compare two committed drops for key, value, and schema differences
├── leaks          # Audit query_audit.jsonl logs for look-ahead violations
└── schema
    ├── infer      # Sample raw files and emit inferred YAML schema
    ├── pin        # Pin inferred schema to schemas/<vendor>.yaml
    ├── check      # Compare drop against pinned schema
    └── accept     # Apply and bump pinned schema version
```

---

## 3. Command Invocations

### 3.1 `vdok data`
```bash
vdok data --quarters 2023q3,2023q4 --raw-dir data/raw/sec_fsds --symbols-dir data/symbols
```
Supports `--user-agent`, `--extract/--no-extract`, `--force`, and `--json-output`.

### 3.2 `vdok synth`
```bash
vdok synth --size-gb 1 --seed 1 --faults default --format csv --num-drops 3 --output-dir data/raw/synthetic
```

### 3.3 `vdok schema` Group
```bash
vdok schema infer sec_fsds data/raw/sec_fsds/2023q3 --sample-rows 200000 -o schemas/sec_fsds.draft.yaml
vdok schema pin sec_fsds data/raw/sec_fsds/2023q3 --schema-dir schemas
vdok schema check sec_fsds data/raw/sec_fsds/2023q4 --schema-dir schemas
vdok schema accept sec_fsds --drop-path data/raw/sec_fsds/2023q4 --schema-dir schemas
```

### 3.4 `vdok onboard`
```bash
vdok onboard sec_fsds data/raw/sec_fsds/2023q3 --as-of "2024-01-15T00:00:00Z"
```
Supports `--manifest`, `--rules`, `--symbols`, `--store-dir`, `--results-dir`, and `--force`. Exits `0` on `READY` / `READY WITH WARNINGS`, `1` on `HELD`, and `2` on invalid paths or missing configuration.

### 3.5 `vdok report` and `vdok diff`
```bash
vdok report sec_fsds --drop <drop_id> --results-dir data/results
vdok diff sec_fsds <drop_a> <drop_b> --store-dir data/store
```
Both commands support `--json-output`. When `--drop` is omitted on `vdok report`, the most recent report under `data/results/<vendor>/` is displayed.

### 3.6 `vdok leaks`
```bash
vdok leaks --log data/store/sec_fsds/audit/query_audit.jsonl --decision-time-column decision_time
```
Exits `0` when zero look-ahead violations are found and `1` when any as-of or knowledge leak is detected.
