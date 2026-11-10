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

