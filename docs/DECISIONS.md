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
