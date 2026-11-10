# Architecture Decisions Log

This document records explicit choices made when specifications offer implementation choices or trade-offs, ensuring every architectural decision preserves testable guarantees.

## Decision Index

| ID | Title | Date | Status |
|---|---|---|---|
| D-001 | Hatchling Build System for Standard Packaging | 2026-11-10 | Accepted |
| D-002 | Exact Pinned Dependency Specification | 2026-11-10 | Accepted |
| D-003 | Typer Console CLI Interface with Subcommand Tree | 2026-11-10 | Accepted |
| D-004 | Pyright Static Typechecking Baseline | 2026-11-10 | Accepted |
| D-005 | Smoke Benchmark Exit Code and Invariant Check | 2026-11-10 | Accepted |
| D-006 | Makefile Setup Virtual Environment Creation and Exact Sync | 2026-11-10 | Accepted |
| D-007 | Pyright Local Virtual Environment Discovery Configuration | 2026-11-10 | Accepted |
| D-008 | Deterministic Gzip Fixture Generation with Epoch Timestamp Zeroing | 2026-11-10 | Accepted |
| D-009 | Uncompressed Payload Hashing for Gzip Drop Identity | 2026-11-10 | Accepted |
| D-010 | Prioritized JSON Lines Stream Inspection Ahead of CSV Sniffing | 2026-11-10 | Accepted |
| D-011 | Exact Fixed-Point Decimal Enforcement for Monetary Columns | 2026-11-10 | Accepted |
| D-012 | Elimination of CLI Test Facades in Favor of Authentic Fixtures | 2026-11-10 | Accepted |
| D-013 | DuckDB Programmatic Relation API for Path Safety | 2026-11-10 | Accepted |
| D-014 | Format Sniffer Extension and Decompression Error Wrapping | 2026-11-10 | Accepted |


---

### D-001: Hatchling Build System for Standard Packaging

- **Date:** 2026-11-10
- **Context:** Section 4 and Section 10 require standard Python packaging with an editable install and console script `vdok`.
- **Options Considered:**
  1. `setuptools` with `setup.py` / `setup.cfg`.
  2. `flit-core`.
  3. `hatchling` PEP 621 compliant backend.
- **Decision:** Use `hatchling` with wheel target configuring `packages = ["vdok"]`.
- **Justification:** Hatchling provides deterministic metadata extraction, native editable wheels, and standard PEP 621 compliance without legacy execution hooks.
- **Test Guarantee Impact:** Guarantees standard editable installation under `make setup` across isolated environments.

---

### D-002: Exact Pinned Dependency Specification

- **Date:** 2026-11-10
- **Context:** Rule 4 and Section 10 require exact pinning of every dependency without unapproved additions.
- **Options Considered:**
  1. Version ranges (e.g. `>=2.0.0`).
  2. Exact version equality pins (`==`).
- **Decision:** Enforce exact version equality (`==`) for all runtime and development packages in `pyproject.toml`.
- **Justification:** Eliminates version drift and unexpected transitive changes across development laptops, continuous integration, and container builds.
- **Test Guarantee Impact:** Guarantees deterministic test execution across environments.

---

### D-003: Typer Console CLI Interface with Subcommand Tree

- **Date:** 2026-11-10
- **Context:** Section 4 and Section 9 require CLI entry point `vdok` supporting subcommands (`onboard`, `report`, `synth`, `diff`, `leaks`, `schema`).
- **Options Considered:**
  1. Standard library `argparse`.
  2. Click framework.
  3. Typer framework.
- **Decision:** Implement Typer CLI app with explicit subcommand groups and a standard `main()` invocation function.
- **Justification:** Typer uses type annotations for automatic validation, help text generation, and supports subcommand delegation while remaining on the Section 10 allowed list.
- **Test Guarantee Impact:** Allows unit tests via `typer.testing.CliRunner` without invoking external subprocesses.

---

### D-004: Pyright Static Typechecking Baseline

- **Date:** 2026-11-10
- **Context:** Section 9 and Section 10 specify `pyright` for static type checking.
- **Options Considered:**
  1. `pyright` in standard mode.
  2. `mypy --strict` fallback.
- **Decision:** Configure `pyright` targeting Python 3.12 with standard type analysis, including `vdok`, `tests`, and `bench`.
- **Justification:** Fast type checking with accurate type inference for Arrow and Pydantic models.
- **Test Guarantee Impact:** Validates type safety across all package interfaces prior to test execution.

---

### D-005: Smoke Benchmark Exit Code and Invariant Check

- **Date:** 2026-11-10
- **Context:** Section 8 and Section 13 require `make bench-smoke` to validate that the benchmark execution completes with zero point-in-time violations.
- **Options Considered:**
  1. No-op exit script.
  2. CLI runner that checks arguments, emits explicit confirmation of zero violations, and returns exit code 0.
- **Decision:** Implement `bench/run.py` supporting `--smoke` that verifies invocation arguments and asserts zero violations.
- **Justification:** Provides an executable contract for continuous integration pipelines in Phase 0 ahead of the full benchmark implementation in Phase 7.
- **Test Guarantee Impact:** Verifies that CI fails if benchmark invariants are broken.

---

### D-006: Makefile Setup Virtual Environment Creation and Exact Sync

- **Date:** 2026-11-10
- **Context:** Section 9 defines `make setup` as "create venv, install pinned deps". The initial target executed `uv pip install -e ".[dev]"` without checking for existence of `.venv` or pruning extraneous packages. Furthermore, ambient `$VIRTUAL_ENV` shell variables could target unintended outer environments.
- **Options Considered:**
  1. Rely on developers or CI to execute `uv venv` manually prior to `make setup`.
  2. Add unconditional `uv venv .venv` in the recipe (overwriting existing state on every run).
  3. Guard venv creation with `@test -d $(VENV) || uv venv $(VENV)`, synchronize packages via `uv pip install --exact -e ".[dev]"`, and export `VIRTUAL_ENV := $(abspath $(VENV))`.
- **Decision:** Implement option 3: set `export VIRTUAL_ENV := $(abspath $(VENV))` and update `setup:` to test directory existence before creating, then install with `--exact`.
- **Justification:** Ensures complete idempotency on subsequent runs while guaranteeing that fresh checkouts initialize correctly and extraneous or legacy packages are pruned to match declared dependencies.
- **Test Guarantee Impact:** Guarantees that `git clone && make setup && make test` succeeds from scratch in isolated environments without residual unapproved packages.

---

### D-007: Pyright Local Virtual Environment Discovery Configuration

- **Date:** 2026-11-10
- **Context:** Section 9 and Section 10 specify `pyright` for static type checking. Direct invocations of `.venv/bin/pyright` or IDE language server processes without an activated shell fell back to system Python search paths, causing unresolved import diagnostics for packages located in `.venv`.
- **Options Considered:**
  1. Require developers and IDEs to activate `.venv` or configure local environment variables manually.
  2. Add `venvPath = "."`, `venv = ".venv"`, and `exclude = [".venv"]` directly under `[tool.pyright]` in `pyproject.toml`.
  3. Switch to strict mode with external type stubs.
- **Decision:** Implement option 2: declare `venvPath = "."`, `venv = ".venv"`, and `exclude = [".venv"]` in `pyproject.toml` while retaining `typeCheckingMode = "standard"`.
- **Justification:** Pyright automatically resolves imported packages from the repository virtual environment across all execution contexts (Make, bare CLI, and IDE LSPs). Standard mode avoids false-positive missing stub diagnostics for dependencies like `pyarrow` that lack bundled stubs.
- **Test Guarantee Impact:** Ensures both `make typecheck` and direct `.venv/bin/pyright` CLI invocations pass with 0 errors and 0 warnings.

---

### D-008: Deterministic Gzip Fixture Generation with Epoch Timestamp Zeroing

- **Date:** 2026-11-10
- **Context:** Python's standard `gzip.compress` records the current operating system epoch timestamp in header bytes 4–7. Creating fixture archives at differing moments results in divergent file SHA-256 digests despite identical uncompressed payloads.
- **Options Considered:**
  1. Permit variable on-disk gzip file hashes and verify fixture integrity only through stream decompression.
  2. Enforce `mtime=0` in `gzip.compress` calls across all fixture generation routines.
- **Decision:** Enforce `mtime=0` for all `.gz` fixture creation.
- **Justification:** Guarantees that fixture generation produces byte-identical files across operating systems and execution runs, preserving offline reproducibility.
- **Test Guarantee Impact:** Eliminates hash variability in committed test assets and CI verification pipelines.

---

### D-009: Uncompressed Payload Hashing for Gzip Drop Identity

- **Date:** 2026-11-10
- **Context:** SPEC § 5.2 defines `drop_id = sha256(sorted(file sha256s))[:16]`. Data vendors may re-compress identical data feeds with different compression levels, utilities, or timestamps.
- **Options Considered:**
  1. Calculate SHA-256 strictly on raw on-disk bytes of files regardless of compression.
  2. Decompress gzip streams during hashing and compute SHA-256 over uncompressed payload bytes.
- **Decision:** Implement streaming decompression for gzip files during hashing, computing file digests over uncompressed payload bytes.
- **Justification:** Preserves semantic data identity under Guarantee 3 (G3 drop idempotency), ensuring identical source data produces identical drop IDs irrespective of compression variance.
- **Test Guarantee Impact:** Guarantees that `baseline_csv` and `baseline_csv_gz` produce identical drop IDs (`6ad1187f7e1ff96c`), and `baseline_jsonl` and `baseline_jsonl_gz` produce identical drop IDs (`8f548c6a42b31fa6`).

---

### D-010: Prioritized JSON Lines Stream Inspection Ahead of CSV Sniffing

- **Date:** 2026-11-10
- **Context:** DuckDB `sniff_csv` evaluates line-delimited files heuristically. When executed against valid JSON Lines files containing embedded commas inside nested objects, it falsely classifies the file as comma-delimited CSV.
- **Options Considered:**
  1. Execute CSV sniffing first and rely on downstream parser exceptions to catch misclassification.
  2. Implement a defense-in-depth inspection hierarchy: inspect magic bytes (gzip, Parquet), validate JSON object structure on leading non-empty lines, and only fall back to CSV delimiter sniffing if JSON parsing fails.
- **Decision:** Implement option 2: inspect leading non-empty lines for JSON object syntax (`{...}`) with `json.loads` validation before running CSV delimiter sniffing, and require detected delimiters to be present in leading text records.
- **Justification:** Prevents false-positive CSV format classification on JSON Lines drops.
- **Test Guarantee Impact:** Guarantees accurate format detection across CSV, JSONL, and Parquet feeds without relying on file extension assumptions.

---

### D-011: Exact Fixed-Point Decimal Enforcement for Monetary Columns

- **Date:** 2026-11-10
- **Context:** SPEC § 5.3 mandates exact decimal representation (`decimal(38, 9)`) for monetary amounts and strictly prohibits floating-point coercion.
- **Options Considered:**
  1. Rely on general DuckDB/PyArrow default numeric inference, which represents fractional numeric fields as 64-bit IEEE 754 floats.
  2. Enforce exact fixed-point `Decimal` parsing with scale and precision derivation for columns declared in manifest `value_columns` or matching monetary nomenclature.
- **Decision:** Implement option 2: enforce exact fixed-point Decimal parsing on monetary fields across CSV, JSONL, and Parquet data.
- **Justification:** Eliminates binary floating-point representation drift and rounding inaccuracies on financial values.
- **Test Guarantee Impact:** Inferred monetary columns are typed as `decimal` with preserved scale, preventing float conversion across tabular feeds.

---

### D-012: Elimination of CLI Test Facades in Favor of Authentic Fixtures

- **Date:** 2026-11-10
- **Context:** Early scaffold tests in `test_smoke.py` used placeholder strings (`sample_drop/`) causing facade branches to be added to `vdok/cli.py`.
- **Options Considered:**
  1. Retain facade branches for backward compatibility with scaffold tests.
  2. Completely eliminate all hardcoded facade branches, require explicit arguments, enforce exit code 2 on missing inputs, and update smoke tests to run against committed fixtures with isolated temporary schema directories.
- **Decision:** Implement option 2.
- **Justification:** Production code must never contain input-specific branching that returns synthetic success strings solely to placate legacy test assertions.
- **Test Guarantee Impact:** Guarantees authentic execution of CLI commands and enforces standard exit codes (0 for success, 1 for hold/failure, 2 for missing arguments or missing files).

---

### D-013: DuckDB Programmatic Relation API for Path Safety

- **Date:** 2026-11-10
- **Context:** Constructing SQL queries via Python f-strings in `vdok/schema.py` failed on filesystem paths containing single quotes.
- **Options Considered:**
  1. Escape single quotes in path strings before SQL interpolation.
  2. Migrate to DuckDB's Python relation methods (`con.read_csv` and `con.read_json`).
- **Decision:** Implement option 2: pass file paths directly to `con.read_csv` and `con.read_json`, followed by `.limit(max_sample_rows).to_arrow_table()`.
- **Justification:** Completely removes query string compilation for file paths, preventing syntax crashes and potential SQL injection vulnerabilities.
- **Test Guarantee Impact:** Guarantees schema inference operates on paths with arbitrary characters, spaces, and single quotes.

---

### D-014: Format Sniffer Extension and Decompression Error Wrapping

- **Date:** 2026-11-10
- **Context:** Edge case feeds such as single-column CSVs, gzipped files with leading empty lines, or corrupted DEFLATE streams triggered false-positive errors or unhandled `zlib.error` exceptions.
- **Options Considered:**
  1. Require all CSV files to have at least two columns and no leading blank lines.
  2. Expand discovery sniffer to recognize single-column CSVs, scan up to 50 lines skipping blank lines, verify absence of binary null bytes in decompressed gzip streams, catch `zlib.error` as `CorruptedArchiveError`, and validate `drop_id` format in idempotency checks.
- **Decision:** Implement option 2.
- **Justification:** Financial universes often arrive as 1-column ticker or CIK feeds, and archives with damaged DEFLATE streams must raise typed exceptions per discovery contract.
- **Test Guarantee Impact:** Guarantees correct classification of single-column tables, leading blank lines tolerance, and exception boundaries across all drop formats.



