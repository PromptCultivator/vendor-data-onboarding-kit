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
| D-015 | Canonical Arrow and Pydantic Bitemporal Schema with Strict Decimal Float Rejection | 2026-11-10 | Accepted |
| D-016 | Immutable Hash-Versioned Symbol Master and Deterministic Identifier Mapping | 2026-11-10 | Accepted |
| D-017 | Multi-Reason Quarantine Aggregation via Vectorized DuckDB List Lambda | 2026-11-10 | Accepted |
| D-018 | Window Ranking Materialization for Primary Key Duplicate Ratio and Escalation | 2026-11-10 | Accepted |
| D-019 | Exact Decimal Scale Validation in DuckDB Without Floating Point Conversion | 2026-11-10 | Accepted |
| D-020 | Deterministic 4-Tier Field Bucketing Without Inode Explosion | 2026-11-10 | Accepted |
| D-021 | Read-Only POSIX 0o444 Partition Locking and Append-Only Collision Refusal | 2026-11-10 | Accepted |
| D-022 | Windowed Revision Detection via Relational Distinct Comparisons | 2026-11-10 | Accepted |
| D-023 | Timezone-Aware Zero-Pytz Query Path via PyArrow Table Materialization | 2026-11-10 | Accepted |
| D-024 | Drop-Level Knowledge Time Evaluation for Batch Temporal Coherence | 2026-11-10 | Accepted |
| D-025 | Dual-Path Fallback for ISO-8601 and Delimited Timestamp Ingestion | 2026-11-10 | Accepted |
| D-026 | Deterministic Multi-Tie Breaking and Relation Separation for Diff and Readiness Reports | 2026-11-10 | Accepted |
| D-027 | Thread-Safe SEC EDGAR Rate Limiting and Atomic Ingestion Caching | 2026-11-10 | Accepted |


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

---

### D-015: Canonical Arrow and Pydantic Bitemporal Schema with Strict Decimal Float Rejection

- **Date:** 2026-11-10
- **Context:** SPEC § 5.4 mandates a canonical row model (as both an Apache Arrow schema and a Pydantic model for unit tests) with exact bit widths, UTC microseconds timestamps, and fixed-point `decimal128(38, 9)` for numeric amounts. SPEC § 5.4 and § 13 explicitly require that decimal monetary fields must never pass through IEEE 754 floating-point representations. Furthermore, drop normalization requires deterministic hashing across fields and unit standardisation.
- **Options Considered:**
  1. Permit automatic type coercion from standard Python floats to Decimal or PyArrow float64 columns during row parsing.
  2. Enforce strict type validation: reject Python `float` instances at normalizer entry points (`parse_decimal_exact`) with `TypeError`, parse string/integer inputs into `Decimal` instances with explicit quantization, enforce `all_varchar=True` during DuckDB tabular ingestion of raw CSV drops, and generate deterministic SHA-256 row fingerprints using ASCII null delimiters (`\0`) to eliminate delimiter collisions.
- **Decision:** Implement option 2: define `CANONICAL_ARROW_SCHEMA` with 12 canonical fields plus `unmapped: bool`, implement Pydantic v2 `CanonicalRow` with strict float rejection on `value_num`, provide pure normalizers (`parse_decimal_exact`, `parse_timestamp_utc`, `normalize_unit`, `normalize_unit_and_value`, `normalize_identifier`, `compute_row_hash`), and transform drop batches into canonical Arrow tables without third-party timezone libraries.
- **Justification:** Floating-point representations introduce precision errors and rounding artifacts in quantitative and financial computations (such as fractional pennies or non-exact decimal fractions like 0.1). Rejecting floats unconditionally at the boundary ensures financial calculations remain exact. Separating fields with null-byte delimiters in `compute_row_hash` eliminates collision vulnerabilities present in arbitrary punctuation joins.
- **Consequences:** Data loaders and adapters must supply numeric data as strings, integers, or `Decimal` instances. Floating-point inputs raise explicit `TypeError` exceptions that must be handled or converted upstream. Memory footprint is slightly larger for exact Decimal objects compared to primitive IEEE floats, but precision and auditability are preserved.
- **Test Guarantee Impact:** Verified by Hypothesis property tests (`test_normalize_properties.py`) over all floating-point numbers asserting strict rejection, and unit tests (`test_normalize.py`) verifying exact scale preservation, null-delimiter collision resistance, and schema conformance across CSV, JSON Lines, and Parquet.

---

### D-016: Immutable Hash-Versioned Symbol Master and Deterministic Identifier Mapping

- **Date:** 2026-11-10
- **Context:** SPEC § 5.6 and ADR 005 require symbol master construction from public SEC `company_tickers.json` files, stored in Parquet format, refreshed by content hash so that any upstream modification produces a new version with an effective date. Mapping must be rule-based and deterministic: exact matching on identifier type, case folding and exchange suffix stripping for tickers, dual-class share translation, exact title matching for company names, and zero heuristic or fuzzy matching. Unresolved records must retain their raw identifier and be flagged with `unmapped=True`.
- **Options Considered:**
  1. Use heuristic string distance (Levenshtein, Jaro-Winkler) or machine learning embeddings to infer matching tickers or CIKs for ambiguous or unmapped vendor entities.
  2. Implement an immutable, content-hash-versioned symbol master stored as read-only Parquet (`data/symbols/master_v<version_id>.parquet` with mode `0o444`), executing deterministic exact lookups: zero-padded 10-digit CIK lookup, uppercase ticker lookup with exchange suffix stripping (`.OQ`, `.N`, etc.) and dual-class translation (`.` or `/` to `-`), and exact normalized company title lookup. Flag all unresolved rows as `unmapped=True` with original raw identifiers preserved.
- **Decision:** Implement option 2: build `SymbolMaster` with content-derived 16-hex version IDs (`sha256(raw_bytes)[:16]`), write immutable Parquet files with POSIX read-only permissions (`0o444`), execute rule-based deterministic resolution returning `ResolutionResult(entity_id, unmapped)`, and prohibit heuristic fuzzy resolution.
- **Justification:** Probabilistic or fuzzy matching introduces silent identification errors in financial data onboarding, potentially attributing financial statements or market prices to incorrect corporate entities. Flagging unresolved rows explicitly preserves complete auditability and exposes unmapped rates in downstream readiness reports without corrupting data truth. Hash-versioned storage ensures immutable lineage tracking across drops.
- **Consequences:** Vendor entities with unrecognized abbreviations, typographical errors, or tickers not present in the SEC master cannot be resolved automatically; they remain flagged as unmapped and require human intervention or master updates.
- **Test Guarantee Impact:** Verified by Hypothesis property tests (`test_symbols_properties.py`) and unit tests (`test_symbols.py`) confirming CIK padding invariants, ticker folding invariance, suffix stripping invariance, dual-class equivalence, version migration immutability, and 100% rejection of fuzzy matches.

---

### D-017: Multi-Reason Quarantine Aggregation via Vectorized DuckDB List Lambda

- **Date:** 2026-11-10
- **Context:** SPEC § 5.5 requires that rows failing validation rules are routed to quarantine with an array column `quarantine_reasons: list[str]` recording every violation triggered by that row (rather than halting on the first failure). Iterating row-by-row in Python is computationally slow and exceeds runtime budgets for multi-gigabyte drops.
- **Options Considered:**
  1. Row-by-row Python evaluation iterating through records and appending failure reason strings to a list.
  2. Multiple SQL passes filtering rows for each rule and joining intermediate failure tables.
  3. Single-pass vectorized SQL compiling each row predicate into a conditional expression (`CASE WHEN (<predicate>) THEN '<rule_name>' ELSE NULL END`), packing the results into a DuckDB array literal, and pruning null elements using DuckDB's native list lambda function: `list_filter([ ... ], lambda x: x IS NOT NULL)`.
- **Decision:** Implement Option 3 in `RuleCompiler` and `ValidationEngine`. Every active row rule compiles into a CASE expression yielding either its rule identifier or NULL. All expressions are bundled into an array and filtered natively in DuckDB.
- **Justification:** Vectorized list filtering executes entirely within DuckDB's C++ vectorized engine without transferring intermediate rows to Python. It produces complete multi-violation reason arrays in a single table scan, satisfying runtime budgets across large datasets.
- **Consequences:** Rules must compile into valid DuckDB SQL boolean predicates. Quarantine schemas declare `quarantine_reasons` as `list_(pa.string())`.
- **Test Guarantee Impact:** Verified by `tests/unit/test_validate.py::test_combined_multiple_reasons` and `tests/unit/test_empirical_challenger2.py::test_simultaneous_multiple_row_failures`, proving rows failing multiple checks capture all triggered rule names in their quarantine reason list.

---

### D-018: Window Ranking Materialization for Primary Key Duplicate Ratio and Escalation

- **Date:** 2026-11-10
- **Context:** SPEC § 5.5 and Guarantee 4 require duplicate detection on the canonical primary key (`entity_id, field, event_time, knowledge_time`). If the duplicate share is less than or equal to 1.0% (0.01), duplicate rows are routed to quarantine while unique first-occurrence rows proceed to the valid dataset. If duplicate share exceeds 1.0%, the entire drop batch must be escalated to a HOLD status, halting main ingestion. Calculating duplicate counts and routing individual duplicates separately would require multiple window function evaluations over massive tables.
- **Options Considered:**
  1. Two separate window evaluations: one aggregation query (`COUNT(*) > 1`) to calculate batch duplicate ratio, and a subsequent window query during row routing to isolate duplicate records.
  2. Sorting the table in Python to identify duplicate keys and computing fractions in memory.
  3. Materializing a temporary DuckDB view `numbered` computing `ROW_NUMBER() OVER (PARTITION BY entity_id, field, event_time, knowledge_time ORDER BY source_file, source_row) AS _vdok_dup_rank` once. The duplicate count is queried directly from `numbered WHERE _vdok_dup_rank > 1`, and `_vdok_dup_rank > 1` is incorporated into the vectorized quarantine reason array as `'duplicate_key'`.
- **Decision:** Implement Option 3. Materialize the window calculation once in temporary view `numbered`.
- **Justification:** Computing the window function once avoids redundant partitioning and sorting passes over large datasets. The resulting integer rank `_vdok_dup_rank` directly drives both the batch-level escalation check (`dup_share > max_ratio`) and row-level quarantine routing (`_vdok_dup_rank > 1`). First occurrences (`_vdok_dup_rank = 1`) pass through to the valid relation if no other rule triggers quarantine.
- **Consequences:** Drop sorting is deterministic using `source_file, source_row`. Memory overhead is restricted to an integer rank column during the temporary view lifetime.
- **Test Guarantee Impact:** Verified by `tests/unit/test_validate.py::test_batch_duplicate_ratio_escalation` and `tests/unit/test_planted_faults_50mb.py::test_50mb_planted_fault_duplicate_row`, confirming sub-threshold duplicates are quarantined while super-threshold duplicates trigger batch holds.

---

### D-019: Exact Decimal Scale Validation in DuckDB Without Floating Point Conversion

- **Date:** 2026-11-10
- **Context:** SPEC § 5.4, § 5.5, and Guarantee 5 mandate that monetary values maintain exact decimal representation and scale without ever passing through IEEE 754 floating point arithmetic. Validation rule `decimal_scale` must verify that numeric values in decimal columns do not exceed a configured fractional digit limit (such as 4 decimal places for securities pricing).
- **Options Considered:**
  1. Converting decimal values to Python `float` or SQL `DOUBLE` and inspecting modulo or string conversions.
  2. Converting values to strings via `CAST(col AS VARCHAR)` and measuring substring length after the decimal separator.
  3. Vectorized numeric comparison using DuckDB's exact decimal rounding: `ROUND(col, scale) != col`.
- **Decision:** Implement Option 3. The rule compiler emits `({col} IS NOT NULL AND ROUND({col}, {scale}) != {col})`.
- **Justification:** DuckDB executes `ROUND` directly on fixed-point `DECIMAL` types without intermediate floating point conversion. If a number contains fractional components beyond `scale` digits, `ROUND(col, scale)` differs from `col`, flagging the record. This maintains 100% adherence to the zero-float mandate of Section 13 while delivering high execution performance in SQL.
- **Consequences:** Columns evaluated by `decimal_scale` must be numeric or Decimal representations in Arrow. Inputs outside the configured scale are quarantined with rule violation tags.
- **Test Guarantee Impact:** Verified by `tests/unit/test_validate.py::test_rule_decimal_scale_without_float` and `tests/unit/test_empirical_challenger2.py::test_decimal_scale_boundaries_and_precision`, confirming boundary scale values without float conversion.

---

### D-020: Deterministic 4-Tier Field Bucketing Without Inode Explosion

- **Date:** 2026-11-10
- **Context:** SPEC § 5.7 requires partitioning data as `store/<vendor>/<field_bucket>/<event_year>/part-<drop_id>.parquet`. If raw tags or fields each formed a directory, datasets with thousands of accounting line items (like SEC FSDS) would cause filesystem inode exhaustion and partition explosion. Conversely, placing all fields into a single catch-all folder eliminates thematic file pruning during queries.
- **Options Considered:**
  1. Creating a directory for every unique field name or tag.
  2. Single flat directory structure without field categorization.
  3. Deterministic 4-tier taxonomy mapping fields into high-level thematic groups (`income_statement`, `balance_sheet`, `cash_flow`, `market_prices`, `macro`, and a `'general'` fallback).
- **Decision:** Implement Option 3. Check manifest overrides first, followed by financial statement keyword taxonomies (cash flow before balance sheet to avoid collision on "cash"), market quotes, macroeconomic indicators, and a general fallback.
- **Justification:** Bounding directory fanout to fewer than 10 directories per vendor prevents inode exhaustion while providing directory pruning during point-in-time analytical queries.
- **Consequences:** Directory layout remains deterministic and bounded. Queries on specific statements or market prices only inspect relevant directory trees.
- **Test Guarantee Impact:** Verified by `tests/unit/test_store.py::test_resolve_field_bucket` and `test_write_partitioned_store_multi_partition`.

---

### D-021: Read-Only POSIX 0o444 Partition Locking and Append-Only Collision Refusal

- **Date:** 2026-11-10
- **Context:** SPEC § 5.7, Guarantee 1 (G1), and Guarantee 3 (G3) require that the partition store is strictly append-only. Partition files must be immutable once written, and attempting to overwrite an existing drop must be refused.
- **Options Considered:**
  1. Standard file overwrite (`mode='w'`) with silent replacement.
  2. Pre-write filesystem scanning for existing `part-<drop_id>.parquet` files, raising a dedicated `AppendOnlyViolationError(FileExistsError)` upon collision, and immediately setting written file modes to POSIX `0o444`.
- **Decision:** Implement Option 2. Before any partition file is written, scan for existing matching partitions for `drop_id`. If detected, abort and raise `AppendOnlyViolationError`. Immediately after writing, lock file permissions with `os.chmod(path, 0o444)`.
- **Justification:** Multiple layers of protection guarantee that historical records can never be mutated or replaced in place, preventing backtest look-ahead bias and preserving data integrity.
- **Consequences:** Re-ingesting an identical drop requires explicit detection at the idempotency layer. Files cannot be modified in place.
- **Test Guarantee Impact:** Verified by `tests/unit/test_guarantees.py::test_g1_store_is_append_only` and `test_g3_rerun_same_drop_adds_no_rows`.

---

### D-022: Windowed Revision Detection via Relational Distinct Comparisons

- **Date:** 2026-11-10
- **Context:** SPEC § 5.8 requires that after each drop, a query finds keys that already exist with different values at earlier knowledge times and catalogs them into a revisions ledger. Comparing against all historical vintages would cause Cartesian growth across cascading restatements.
- **Options Considered:**
  1. Full Cartesian cross-join across all historical drops.
  2. Windowed SQL ranking prior historical store records with `ROW_NUMBER() OVER (PARTITION BY entity_id, field, event_time ORDER BY knowledge_time DESC) = 1` and comparing incoming rows against this latest active baseline using `IS DISTINCT FROM`.
- **Decision:** Implement Option 2. Query pairs incoming drop keys exclusively against the immediately prior latest state, using `IS DISTINCT FROM` across numeric, string, and unit fields.
- **Justification:** Prevents redundant duplicate revision pairs when a single value undergoes successive revisions across drops 1, 2, and 3. `IS DISTINCT FROM` preserves exact decimal comparisons without float coercion.
- **Consequences:** Memory and compute complexity scale linearly with incoming drop volume rather than quadratically with historical drop count.
- **Test Guarantee Impact:** Verified by `tests/unit/test_planted_restatements.py` achieving 100% recall and 100% precision on synthetic planted restatement sequences and at 50 MB scale.

---

### D-023: Timezone-Aware Zero-Pytz Query Path via PyArrow Table Materialization

- **Date:** 2026-11-10
- **Context:** SPEC § 10 restricts runtime dependencies, prohibiting `pytz`. When DuckDB executes DB-API `fetchall()` on tables containing `TIMESTAMPTZ` columns, DuckDB raises an internal error attempting to import `pytz`.
- **Options Considered:**
  1. Add `pytz` to dependencies (violates allowed dependency constraint Rule 4).
  2. Strip timezone information from timestamps (violates UTC microsecond precision mandate).
  3. Materialize DuckDB query results directly into PyArrow tables via `con.execute(...).to_arrow_table().to_pylist()`.
- **Decision:** Implement Option 3 in `vdok/query.py` and format temporal strings with `strftime` in analytical reporting.
- **Justification:** PyArrow uses Python 3.9+'s native standard library `zoneinfo.ZoneInfo(key='Etc/UTC')` for UTC timestamps, resolving timezone conversions without external packages.
- **Consequences:** Eliminates third-party timezone dependencies while preserving microsecond precision across all query interfaces.
- **Test Guarantee Impact:** Verified by `tests/unit/test_query.py` and `tests/unit/test_revisions.py`.

---

### D-024: Drop-Level Knowledge Time Evaluation for Batch Temporal Coherence

- **Date:** 2026-11-10
- **Context:** SPEC § 5.3 allows `knowledge_time` to be derived either from a dedicated metadata column or from `drop_time`. When evaluating `knowledge_time` inside per-row parsing loops, repeated calls to `datetime.now(timezone.utc)` caused different rows within the same transmission drop to have microsecond-divergent knowledge timestamps.
- **Options Considered:**
  1. Evaluate `datetime.now(timezone.utc)` inside the per-row parsing iteration.
  2. Pin a single batch-level UTC knowledge timestamp at drop initialization (`default_drop_kt`), applying this identical timestamp across all rows in the drop batch.
- **Decision:** Implement Option 2 in `vdok/normalize.py`.
- **Justification:** Records delivered in a single transmission drop share an identical knowledge time unless row-level filing timestamps state otherwise. Evaluating once at the drop boundary ensures deterministic bitemporal coordinates across the entire drop payload.
- **Test Guarantee Impact:** Verified by `tests/unit/test_normalize.py` and `tests/golden/test_report_goldens.py`.

---

### D-025: Dual-Path Fallback for ISO-8601 and Delimited Timestamp Ingestion

- **Date:** 2026-11-10
- **Context:** Vendor manifests declare explicit timestamp formats (such as `%Y-%m-%d %H:%M:%S`), but transmission feeds frequently deliver ISO-8601 formatted timestamps with `T` separators and `Z` timezone designators. Strict format parsing raises ValueError when format specifiers deviate from payload variations.
- **Options Considered:**
  1. Enforce strict rejection of all timestamps that do not conform exactly to the declared format string.
  2. Attempt manifest format parsing first; upon format failure, fall back to Python's standard `datetime.fromisoformat()`, followed by `dateutil.parser.parse()`, localizing naive timestamps to the declared manifest timezone and converting to UTC.
- **Decision:** Implement Option 2 in `parse_timestamp_utc`.
- **Justification:** Financial drops frequently mix strict date-time representations and ISO-8601 variations. Providing a resilient fallback prevents spurious ingestion aborts while maintaining strict conversion to UTC microsecond timestamps.
- **Test Guarantee Impact:** Verified by `tests/unit/test_normalize.py::test_parse_timestamp_utc_dateutil_fallback` and `tests/unit/test_onboard.py`.

---

### D-026: Deterministic Multi-Tie Breaking and Relation Separation for Diff and Readiness Reports

- **Date:** 2026-11-10
- **Context:** SPEC § 5.9 and § 5.10 require deterministic readiness reporting and drop diffing. In DuckDB, executing aggregation queries without explicit tie-breakers resulted in indeterminate dictionary key ordering for zero-rate restatements. Additionally, invoking `.to_arrow_table()` on active DB-API execute cursors raised `UserWarning` or `ResourceInUseException`.
- **Options Considered:**
  1. Rely on DuckDB internal group iteration order and DB-API cursor state.
  2. Enforce deterministic multi-key sorting `(-rate, field_name)` across all report tables, use DuckDB's relation-level `.sql().to_arrow_table()` API, and utilize `.fetchall()[0][0]` for scalar count queries.
- **Decision:** Implement Option 2 in `vdok/report.py`.
- **Justification:** Guarantees bit-for-bit identical markdown reports across runs regardless of query execution plans or filesystem iteration order. Eliminates connection cursor conflicts when running multiple analytical passes over Parquet views.
- **Test Guarantee Impact:** Verified by `tests/golden/test_report_goldens.py` and `tests/unit/test_report.py`.

---

### D-027: Thread-Safe SEC EDGAR Rate Limiting and Atomic Ingestion Caching

- **Date:** 2026-11-10
- **Context:** SPEC § 6.1 and Section 7 require rate-limiting requests to the SEC EDGAR system at at most 5 requests per second with a compliant User-Agent (`vendor-data-onboarding-kit <contact email>`), as well as caching downloads by SHA-256 hash in `data/raw/`.
- **Options Considered:**
  1. Plain `time.sleep(0.2)` in iterative loops without concurrency safety or hash verification.
  2. Implement a dedicated `SecRateLimiter` class using a thread-safe `threading.Lock` enforcing inter-request intervals of at least 0.25 seconds (at most 4 requests per second, strictly complying with the 5 rps ceiling). Stream downloads to `.tmp` files, compute SHA-256 digests in-flight, atomically replace targets upon verification, and store digests in `.sha256` sidecars for instant cache validation.
- **Decision:** Implement Option 2 in `vdok/adapters/sec_fsds.py`.
- **Justification:** SEC EDGAR strictly blocks IP addresses that exceed 10 requests per second or fail User-Agent identification standards. Enforcing atomic streaming and sidecar hashing prevents corrupted partial archives from entering the store while eliminating duplicate downloads.
- **Test Guarantee Impact:** Verified by `tests/unit/test_sec_fsds.py` (rate limiting, User-Agent formatting, hash caching, and real download of 2023q3).

