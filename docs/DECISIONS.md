# Architecture & Implementation Decisions

This document catalogs the engineering decisions governing `vendor-data-onboarding-kit` (`vdok`). Every decision preserves testable behavioral guarantees across packaging, ingestion, normalization, validation, storage, querying, and benchmarking.

## Decision Index

| ID | Title | Status |
|---|---|---|
| D-001 | Hatchling Build System for PEP 621 Packaging | Accepted |
| D-002 | Exact Pinned Dependency Specification | Accepted |
| D-003 | Typer Console CLI Interface with Subcommand Tree | Accepted |
| D-004 | Pyright Standard Static Type-Checking Baseline | Accepted |
| D-005 | Smoke Benchmark Invariant Contract | Accepted |
| D-006 | Idempotent Virtual Environment Setup with Exact Sync | Accepted |
| D-007 | Local Virtual Environment Discovery for Pyright | Accepted |
| D-008 | Deterministic Gzip Fixtures with Zeroed Epoch Timestamps | Accepted |
| D-009 | Uncompressed Payload Hashing for Gzip Drop Identity | Accepted |
| D-010 | JSON Lines Inspection Prioritized Ahead of CSV Sniffing | Accepted |
| D-011 | Exact Fixed-Point Decimal Enforcement for Monetary Columns | Accepted |
| D-012 | Strict CLI Argument Validation and Exit Code Contracts | Accepted |
| D-013 | DuckDB Programmatic Relation API for Path Safety | Accepted |
| D-014 | Single-Column Sniffer Support and Decompression Error Wrapping | Accepted |
| D-015 | Canonical Arrow and Pydantic Schema with Unconditional Float Rejection | Accepted |
| D-016 | Immutable Hash-Versioned Symbol Master and Deterministic Mapping | Accepted |
| D-017 | Multi-Reason Quarantine Aggregation via Vectorized DuckDB List Lambda | Accepted |
| D-018 | Single Window Materialization for Duplicate Ratio and Escalation | Accepted |
| D-019 | Exact Decimal Scale Validation in DuckDB Without Float Conversion | Accepted |
| D-020 | Deterministic 4-Tier Field Bucketing Without Inode Explosion | Accepted |
| D-021 | Read-Only POSIX `0o444` Partition Locking and Collision Refusal | Accepted |
| D-022 | Windowed Revision Detection via Relational `IS DISTINCT FROM` | Accepted |
| D-023 | Timezone-Aware Zero-`pytz` Query Path via PyArrow Materialization | Accepted |
| D-024 | Drop-Level Knowledge Time Pinning for Batch Temporal Coherence | Accepted |
| D-025 | Dual-Path Fallback for ISO-8601 and Delimited Timestamp Ingestion | Accepted |
| D-026 | Deterministic Multi-Tie Sorting and Relation Separation in Reporting | Accepted |
| D-027 | Thread-Safe SEC EDGAR Rate Limiting and Atomic SHA-256 Caching | Accepted |
| D-028 | Vectorized DuckDB Normalization and Tolerant Schema Sampling | Accepted |
| D-029 | Sorted Partition MD5 Prefix Index and Unpruned Streaming Oracle | Accepted |
| D-030 | Per-Iteration Subprocess Isolation for Process-Tree Peak RSS | Accepted |

---

### D-001: Hatchling Build System for PEP 621 Packaging

- **Context:** The repository requires standard Python packaging supporting editable installs and the `vdok` console entry point.
- **Options Considered:**
  1. `setuptools` with `setup.py` / `setup.cfg`.
  2. `flit-core`.
  3. `hatchling` PEP 621 build backend.
- **Decision:** Use `hatchling` with wheel target `packages = ["vdok"]` and console script `vdok = "vdok.cli:main"`.
- **Justification:** Provides deterministic metadata extraction, native editable wheels, and standard PEP 621 configuration without imperative setup hooks.
- **Verification Impact:** Guarantees repeatable editable installation under `make setup` across isolated environments and container builds.

---

### D-002: Exact Pinned Dependency Specification

- **Context:** Quantitative reproducibility requires identical runtime and tooling behavior across developer workstations, CI runners, and Docker containers.
- **Options Considered:**
  1. Minimum or bounded version ranges (e.g., `>=2.0.0`).
  2. Exact version equality pins (`==`).
- **Decision:** Pin every runtime and development dependency in `pyproject.toml` with exact `==` version equality.
- **Justification:** Eliminates transitive version drift across environments and ensures benchmark and test runs exercise identical library builds.
- **Verification Impact:** Guarantees deterministic test execution and static analysis behavior across environments.

---

### D-003: Typer Console CLI Interface with Subcommand Tree

- **Context:** The `vdok` binary exposes top-level commands (`version`, `data`, `synth`, `onboard`, `report`, `diff`, `leaks`) alongside a nested `schema` subcommand group (`infer`, `pin`, `check`, `accept`).
- **Options Considered:**
  1. Standard library `argparse`.
  2. Click framework.
  3. Typer framework.
- **Decision:** Implement the CLI in `vdok/cli.py` as a Typer application with an explicit `schema_app` sub-typer and `main()` entry point.
- **Justification:** Uses Python type annotations for option validation and help generation while supporting native subcommand composition.
- **Verification Impact:** Enables direct in-process CLI testing via `typer.testing.CliRunner` without spawning external shells.

---

### D-004: Pyright Standard Static Type-Checking Baseline

- **Context:** Automated static analysis must validate interfaces across core modules, tests, and benchmarks.
- **Options Considered:**
  1. `pyright` in `standard` mode targeting Python 3.12.
  2. `mypy --strict`.
- **Decision:** Configure `pyright` in `pyproject.toml` with `pythonVersion = "3.12"`, `typeCheckingMode = "standard"`, and `include = ["vdok", "tests", "bench"]`.
- **Justification:** Provides fast static type analysis with accurate inference over Pydantic models and PyArrow/DuckDB APIs while avoiding false positives on third-party packages without bundled stubs.
- **Verification Impact:** Validates interface contracts across `vdok`, `tests`, and `bench` via `make typecheck`.

---

### D-005: Smoke Benchmark Invariant Contract

- **Context:** CI requires a fast end-to-end benchmark target (`make bench-smoke`) that validates the full pipeline and point-in-time invariants on every commit.
- **Options Considered:**
  1. Unit-test-only CI checks without running the benchmark harness.
  2. Dedicated `--smoke` mode in `bench/run.py` executing a 50 MB synthetic workload for 1 isolated subprocess run and asserting zero point-in-time violations.
- **Decision:** Implement `bench/run.py --smoke` running a 50 MB single-run benchmark and exiting non-zero if any point-in-time violation occurs.
- **Justification:** Exercises the full generation, onboarding, query verification, and leak-detection pipeline in CI within seconds.
- **Verification Impact:** Fails CI immediately if end-to-end pipeline invariants regress.

---

### D-006: Idempotent Virtual Environment Setup with Exact Sync

- **Context:** `make setup` must initialize `.venv` on fresh checkouts, remain safe to re-run, and ignore any ambient `$VIRTUAL_ENV` from an outer shell.
- **Options Considered:**
  1. Require manual `uv venv` creation prior to `make setup`.
  2. Recreate `.venv` unconditionally on every run.
  3. Export `VIRTUAL_ENV := $(abspath $(VENV))`, create `.venv` only when absent (`@test -d $(VENV) || uv venv $(VENV)`), and sync with `uv pip install --exact -e ".[dev]"`.
- **Decision:** Implement Option 3 in `Makefile`.
- **Justification:** Guarantees idempotent environment synchronization while pruning extraneous or mismatched packages to match `pyproject.toml` pins.
- **Verification Impact:** Ensures `git clone && make setup && make test` succeeds reproducibly in isolated environments.

---

### D-007: Local Virtual Environment Discovery for Pyright

- **Context:** Invoking `.venv/bin/pyright` or an editor language server outside an activated shell must still resolve project dependencies installed in `.venv`.
- **Options Considered:**
  1. Require manual shell activation before running `pyright`.
  2. Declare `venvPath = "."`, `venv = ".venv"`, and `exclude = [".venv"]` in `[tool.pyright]` inside `pyproject.toml`.
- **Decision:** Configure `.venv` discovery directly in `pyproject.toml`.
- **Justification:** Both `make typecheck` and bare CLI/LSP invocations resolve dependencies identically without environment activation.
- **Verification Impact:** Guarantees consistent zero-error `pyright` runs across Make, CI, and local tooling.

---

### D-008: Deterministic Gzip Fixtures with Zeroed Epoch Timestamps

- **Context:** Standard `gzip.compress` embeds the current OS timestamp in header bytes 4–7, producing differing file SHA-256 digests when fixtures are regenerated.
- **Options Considered:**
  1. Allow variable on-disk `.gz` digests across runs.
  2. Pin `mtime=0` on all compressed fixture generation calls.
- **Decision:** Enforce `mtime=0` whenever creating `.gz` test fixtures.
- **Justification:** Guarantees byte-identical compressed fixtures across operating systems and execution timestamps.
- **Verification Impact:** Preserves deterministic fixture checksums in source control and CI.

---

### D-009: Uncompressed Payload Hashing for Gzip Drop Identity

- **Context:** Vendors may deliver identical datasets re-compressed with different gzip timestamps, utilities, or compression levels. Under Guarantee 3 (G3), identical data payloads must yield identical `drop_id` values.
- **Options Considered:**
  1. Compute SHA-256 on raw compressed bytes on disk.
  2. Decompress gzip streams during hashing and compute SHA-256 over uncompressed payload bytes.
- **Decision:** Stream-decompress gzip files in `vdok/discover.py` (`hash_file_payload`) and hash the uncompressed payload bytes when deriving `drop_id`.
- **Justification:** Preserves semantic drop identity across compression variations.
- **Verification Impact:** Verified by `tests/unit/test_discover.py` confirming `baseline_csv` and `baseline_csv_gz` yield identical `drop_id` (`6ad1187f7e1ff96c`), and `baseline_jsonl` and `baseline_jsonl_gz` yield identical `drop_id` (`8f548c6a42b31fa6`).

---

### D-010: JSON Lines Inspection Prioritized Ahead of CSV Sniffing

- **Context:** Heuristic CSV sniffers misclassify valid JSON Lines files containing commas inside JSON objects as comma-delimited CSV.
- **Options Considered:**
  1. Run CSV sniffing first and recover on downstream parser failures.
  2. Inspect magic bytes (Parquet, gzip), validate leading non-empty lines with `json.loads` for JSON objects (`{...}`), and fall back to CSV delimiter sniffing only when JSON validation fails.
- **Decision:** Implement the JSONL-first inspection hierarchy in `vdok/discover.py` (`detect_file_format`).
- **Justification:** Eliminates false-positive CSV classification on JSON Lines payloads without relying on filename extensions.
- **Verification Impact:** Guarantees accurate format detection across CSV, JSONL, and Parquet drops in `tests/unit/test_discover.py`.

---

### D-011: Exact Fixed-Point Decimal Enforcement for Monetary Columns

- **Context:** Default numeric type inference converts decimal fractions into IEEE 754 binary floats, introducing rounding drift in financial amounts.
- **Options Considered:**
  1. Use default DuckDB/PyArrow `DOUBLE` numeric inference.
  2. Enforce exact fixed-point `decimal` inference with derived precision and scale for manifest `value_columns` and monetary column names (`_is_money_column`).
- **Decision:** Enforce exact fixed-point `decimal` typing on monetary fields in `vdok/schema.py` across CSV, JSONL, and Parquet feeds.
- **Justification:** Eliminates binary floating-point rounding artifacts in monetary amounts.
- **Verification Impact:** Verified by schema inference tests confirming monetary columns infer as `decimal` with preserved scale.

---

### D-012: Strict CLI Argument Validation and Exit Code Contracts

- **Context:** Automated schedulers and pipelines rely on deterministic CLI exit codes and explicit argument validation without input-specific shortcuts.
- **Options Considered:**
  1. Permissive defaults or fallback branches for missing paths.
  2. Strict path/argument existence checks returning exit code `2` on missing files/manifests/arguments, exit code `1` on batch `HELD` verdicts or leaks detected, and exit code `0` on `READY` / `READY WITH WARNINGS` / zero leaks.
- **Decision:** Enforce strict validation and standard exit codes (`0`, `1`, `2`) across all `vdok` CLI subcommands in `vdok/cli.py`.
- **Justification:** Ensures headless batch runners distinguish configuration/I/O errors (`2`) from data-quality holds (`1`) and successful ingestion (`0`).
- **Verification Impact:** Verified by `tests/unit/test_smoke.py` and `tests/unit/test_cli_schema.py` against real fixtures.

---

### D-013: DuckDB Programmatic Relation API for Path Safety

- **Context:** Interpolating filesystem paths into DuckDB SQL strings fails when paths contain single quotes or special characters and introduces SQL injection risk.
- **Options Considered:**
  1. Manually escape quotes in SQL path strings.
  2. Use DuckDB's programmatic relation API (`con.read_csv`, `con.read_json`).
- **Decision:** Pass path strings directly to `con.read_csv` and `con.read_json` in `vdok/schema.py` followed by `.limit(max_sample_rows).to_arrow_table()`.
- **Justification:** Removes string interpolation for raw input paths in schema sampling and prevents syntax/injection hazards.
- **Verification Impact:** Verified by regression tests in `tests/unit/test_schema.py` using paths containing spaces and single quotes.

---

### D-014: Single-Column Sniffer Support and Decompression Error Wrapping

- **Context:** Financial entity universes frequently arrive as single-column CSV files, gzipped feeds may contain leading blank lines, and truncated archives raise low-level `zlib.error` exceptions.
- **Options Considered:**
  1. Require multi-column CSVs without leading blank lines and expose raw `zlib` errors.
  2. Support single-column CSVs in `detect_file_format`, scan up to 50 non-empty lines, reject binary null bytes in text streams, wrap `zlib.error` / `EOFError` as `CorruptedArchiveError`, and validate `drop_id` hex format in `check_idempotency`.
- **Decision:** Implement Option 2 in `vdok/discover.py`.
- **Justification:** Normalizes edge-case text layouts while enforcing typed exception boundaries (`CorruptedArchiveError`, `UnrecognizedFormatError`, `EmptyDropError`).
- **Verification Impact:** Verified across edge-case discovery tests in `tests/unit/test_discover.py`.

---

### D-015: Canonical Arrow and Pydantic Schema with Unconditional Float Rejection

- **Context:** Financial quantities in `value_num` (`decimal128(38, 9)`) must never pass through IEEE 754 floating-point intermediaries, and canonical row hashes must avoid field-concatenation collisions.
- **Options Considered:**
  1. Coerce Python `float` inputs into `Decimal` at normalization boundaries and join hash fields with printable punctuation.
  2. Define the 13-field `CANONICAL_ARROW_SCHEMA` and Pydantic v2 `CanonicalRow`, unconditionally reject Python `float` values in `parse_decimal_exact` with `TypeError`, read raw CSVs with `all_varchar=True` in DuckDB, and compute `row_hash` over ASCII null-byte (`\0`) delimited fields.
- **Decision:** Implement Option 2 in `vdok/normalize.py`.
- **Justification:** Enforces exact decimal arithmetic from raw text to Arrow storage and prevents delimiter-injection hash collisions in `compute_row_hash`.
- **Consequences:** Callers supplying numeric values programmatically must pass strings, integers, or `Decimal` objects; `float` inputs fail immediately with `TypeError`.
- **Verification Impact:** Verified by Hypothesis property tests in `tests/property/test_normalize_properties.py` and unit tests in `tests/unit/test_normalize.py`.

---

### D-016: Immutable Hash-Versioned Symbol Master and Deterministic Mapping

- **Context:** Entity identifiers arrive as CIKs, tickers with exchange suffixes, dual-class shares, or company titles. Probabilistic or fuzzy matching risks silently attributing financial observations to the wrong corporate entity.
- **Options Considered:**
  1. Fuzzy string matching (Levenshtein / Jaro-Winkler) or model-based entity matching.
  2. Content-hash-versioned (`sha256(raw_bytes)[:16]`), read-only (`0o444`) Parquet symbol master (`data/symbols/master_v<version_id>.parquet`) executing deterministic rule-based resolution and flagging unresolved identifiers with `unmapped=True`.
- **Decision:** Implement Option 2 in `vdok/symbols.py` (`build_symbol_master`, `SymbolMaster.resolve`).
- **Justification:** Guarantees 100% auditable, deterministic entity mapping: 10-digit zero-padded CIK lookup, uppercase ticker lookup with documented exchange suffix stripping (`.OQ`, `.N`, etc.) and dual-class translation (`.`/`/` → `-`), and exact normalized company title matching. Unresolved rows preserve raw identifiers and surface in the readiness report.
- **Consequences:** Abbreviations, typos, or non-master entities remain flagged `unmapped=True` for manual review rather than being guessed.
- **Verification Impact:** Verified by `tests/property/test_symbols_properties.py` and `tests/unit/test_symbols.py` confirming zero fuzzy matching and version immutability.

---

### D-017: Multi-Reason Quarantine Aggregation via Vectorized DuckDB List Lambda

- **Context:** Rows failing multiple validation checks must record every violated rule name in `quarantine_reasons: list[str]` while sustaining high throughput over millions of rows.
- **Options Considered:**
  1. Per-row Python validation loops.
  2. Multiple SQL passes per rule joined back by row ID.
  3. Single-pass vectorized DuckDB SQL compiling each rule into `CASE WHEN (<predicate>) THEN '<rule_name>' ELSE NULL END` inside an array literal filtered via `list_filter([ ... ], lambda x: x IS NOT NULL)`.
- **Decision:** Implement Option 3 in `RuleCompiler` and `ValidationEngine` inside `vdok/validate.py`.
- **Justification:** Evaluates all active row rules in one C++ vectorized scan without transferring intermediate rows to Python.
- **Consequences:** All row rules must compile to valid DuckDB SQL predicates; quarantine tables carry `quarantine_reasons` as `pa.list_(pa.string())`.
- **Verification Impact:** Verified by `tests/unit/test_validate.py::test_combined_multiple_reasons` and `tests/unit/test_empirical_challenger2.py::test_simultaneous_multiple_row_failures`.

---

### D-018: Single Window Materialization for Duplicate Ratio and Escalation

- **Context:** Primary key `(entity_id, field, event_time, knowledge_time)` duplicates must route to quarantine when their share is `≤ 1.0%` (`0.01`) and escalate to a full batch `hold` when their share exceeds `1.0%`.
- **Options Considered:**
  1. Execute separate aggregation and routing window queries.
  2. Sort records in Python memory.
  3. Materialize a single temporary DuckDB view `numbered` computing `ROW_NUMBER() OVER (PARTITION BY entity_id, field, event_time, knowledge_time ORDER BY source_file, source_row) AS _vdok_dup_rank`, deriving both the batch duplicate count (`WHERE _vdok_dup_rank > 1`) and the row-level `'duplicate_key'` quarantine tag from that view.
- **Decision:** Implement Option 3 in `ValidationEngine` (`vdok/validate.py`).
- **Justification:** Evaluates the expensive partition-and-sort window once per drop while keeping deterministic ordering (`source_file, source_row`).
- **Verification Impact:** Verified by `tests/unit/test_validate.py::test_batch_duplicate_ratio_escalation` and `tests/unit/test_planted_faults_50mb.py::test_50mb_planted_fault_duplicate_row`.

---

### D-019: Exact Decimal Scale Validation in DuckDB Without Float Conversion

- **Context:** The `decimal_scale` validation rule must detect values exceeding a configured fractional digit limit without converting `decimal128` columns to floating-point types.
- **Options Considered:**
  1. Convert to `DOUBLE` and inspect modulo remainders.
  2. Cast to `VARCHAR` and parse substring lengths after `.`.
  3. Compare exact fixed-point rounding in DuckDB: `({col} IS NOT NULL AND ROUND({col}, {scale}) != {col})`.
- **Decision:** Implement Option 3 in `RuleCompiler` (`vdok/validate.py`).
- **Justification:** DuckDB executes `ROUND` directly on fixed-point `DECIMAL` values in vectorized C++ with zero floating-point conversion.
- **Verification Impact:** Verified by `tests/unit/test_validate.py::test_rule_decimal_scale_without_float` and `tests/unit/test_empirical_challenger2.py::test_decimal_scale_boundaries_and_precision`.

---

### D-020: Deterministic 4-Tier Field Bucketing Without Inode Explosion

- **Context:** Partitioning Parquet files by raw `field` name across thousands of XBRL tags causes filesystem inode explosion, whereas a single flat folder eliminates partition pruning.
- **Options Considered:**
  1. One directory per distinct field name.
  2. Flat unbucketed directory per event year.
  3. Deterministic 4-tier taxonomy (`resolve_field_bucket`) mapping fields to `income_statement`, `cash_flow`, `balance_sheet`, `market_prices`, `macro`, or `general` (with manifest override support and `cash_flow` evaluated prior to `balance_sheet` to disambiguate `cash`).
- **Decision:** Implement Option 3 in `vdok/store.py`.
- **Justification:** Caps directory fanout to fewer than 10 buckets per vendor while enabling directory-level pruning by financial domain and `event_year`.
- **Verification Impact:** Verified by `tests/unit/test_store.py::test_resolve_field_bucket` and `test_write_partitioned_store_multi_partition`.

---

### D-021: Read-Only POSIX `0o444` Partition Locking and Collision Refusal

- **Context:** Guarantee 1 (G1) and Guarantee 3 (G3) forbid in-place overwrites or deletions of committed store partitions.
- **Options Considered:**
  1. Standard writable file writes with overwrite allowed.
  2. Pre-write scan for existing `part-<drop_id>.parquet` files raising `AppendOnlyViolationError(FileExistsError)` on collision, followed by immediate `os.chmod(path, 0o444)` after writing.
- **Decision:** Implement Option 2 in `write_partitioned_store` (`vdok/store.py`).
- **Justification:** Combines pre-write collision detection with OS-level read-only file permissions and zero update/delete APIs.
- **Verification Impact:** Verified by `tests/unit/test_guarantees.py::test_g1_store_is_append_only` and `test_g3_rerun_same_drop_adds_no_rows`.

---

### D-022: Windowed Revision Detection via Relational `IS DISTINCT FROM`

- **Context:** Comparing an incoming drop against every prior historical vintage causes quadratic growth across cascading restatements.
- **Options Considered:**
  1. Full cross-join against all historical drop records.
  2. Rank prior store rows via `ROW_NUMBER() OVER (PARTITION BY entity_id, field, event_time ORDER BY knowledge_time DESC, drop_id DESC) = 1` and compare incoming keys against only the prior latest baseline using `IS DISTINCT FROM` across `(value_num, value_str, unit)` where `curr.knowledge_time > prev.old_knowledge_time`.
- **Decision:** Implement Option 2 in `detect_revisions` (`vdok/revisions.py`).
- **Justification:** Prevents duplicate revision emissions across multi-drop restatement chains and preserves exact decimal and null-safe comparisons.
- **Verification Impact:** Verified by `tests/unit/test_planted_restatements.py` (100% recall and 100% precision).

---

### D-023: Timezone-Aware Zero-`pytz` Query Path via PyArrow Materialization

- **Context:** DuckDB's DB-API `fetchall()` attempts to import `pytz` when reading `TIMESTAMPTZ` columns, whereas the runtime relies solely on standard-library `zoneinfo`.
- **Options Considered:**
  1. Add `pytz` as a runtime dependency.
  2. Drop timezone awareness from timestamps.
  3. Materialize query results directly into Arrow tables via `.to_arrow_table().to_pylist()` and format timestamps in SQL via `strftime`.
- **Decision:** Implement Option 3 across `vdok/query.py` and `vdok/report.py`.
- **Justification:** PyArrow converts `timestamp[us, UTC]` columns natively using Python's standard library `zoneinfo`, preserving microsecond UTC precision with zero third-party timezone dependencies.
- **Verification Impact:** Verified by `tests/unit/test_query.py` and `tests/unit/test_revisions.py`.

---

### D-024: Drop-Level Knowledge Time Pinning for Batch Temporal Coherence

- **Context:** When `knowledge_time.source` is `drop_time`, calling `datetime.now(timezone.utc)` per row inside normalization loops yields microsecond-divergent knowledge timestamps for rows from the same drop.
- **Options Considered:**
  1. Evaluate `datetime.now(timezone.utc)` per row.
  2. Pin a single `default_drop_kt` UTC timestamp once at drop initialization and apply it uniformly to all rows in the drop.
- **Decision:** Implement Option 2 in `vdok/normalize.py`.
- **Justification:** Ensures deterministic bitemporal coordinates and reproducible `row_hash`/report spans across all rows delivered in one batch.
- **Verification Impact:** Verified by `tests/unit/test_normalize.py` and `tests/golden/test_report_goldens.py`.

---

### D-025: Dual-Path Fallback for ISO-8601 and Delimited Timestamp Ingestion

- **Context:** Vendor feeds occasionally mix manifest `strptime` formats (e.g., `%Y-%m-%d %H:%M:%S`) with ISO-8601 variants (`T` separators, `Z` suffixes).
- **Options Considered:**
  1. Reject any timestamp not matching the exact `strptime` format string.
  2. Attempt manifest `strptime` parsing first, falling back to `datetime.fromisoformat()` and `dateutil.parser.parse()`, then localizing naive timestamps to the manifest IANA timezone and converting to UTC.
- **Decision:** Implement Option 2 in `parse_timestamp_utc` (`vdok/normalize.py`).
- **Justification:** Prevents spurious rejections on ISO-8601 variations while maintaining strict UTC microsecond normalization.
- **Verification Impact:** Verified by `tests/unit/test_normalize.py::test_parse_timestamp_utc_dateutil_fallback` and `tests/unit/test_onboard.py`.

---

### D-026: Deterministic Multi-Tie Sorting and Relation Separation in Reporting

- **Context:** Readiness reports and golden outputs require bit-for-bit determinism regardless of DuckDB group iteration order, and calling `.to_arrow_table()` on active DB-API cursors triggers resource/cursor warnings.
- **Options Considered:**
  1. Rely on default SQL grouping order and cursor `execute()`.
  2. Enforce explicit multi-key sorting (`(-rate, field_name)`), use DuckDB's relation API (`.sql().to_arrow_table()`), and use `.fetchall()[0][0]` for scalar counts.
- **Decision:** Implement Option 2 in `vdok/report.py`.
- **Justification:** Produces reproducible Markdown and JSON reports across runs without cursor-state warnings.
- **Verification Impact:** Verified by `tests/golden/test_report_goldens.py` and `tests/unit/test_report.py`.

---

### D-027: Thread-Safe SEC EDGAR Rate Limiting and Atomic SHA-256 Caching

- **Context:** SEC EDGAR enforces strict request-rate and `User-Agent` rules (`vendor-data-onboarding-kit <contact>`), and multi-hundred-megabyte quarterly ZIP downloads must not leave partial files on interruption.
- **Options Considered:**
  1. Unsynchronized `time.sleep(0.2)` loops writing directly to target paths.
  2. Implement thread-safe `SecRateLimiter` (`threading.Lock` enforcing ≥ 0.25 s intervals, i.e., ≤ 4 req/s under the 5 req/s ceiling), stream downloads to `.tmp` files, compute SHA-256 digests in-flight, atomically rename upon completion, and store `.sha256` sidecars for cache hits.
- **Decision:** Implement Option 2 in `vdok/adapters/sec_fsds.py`.
- **Justification:** Complies with SEC access limits and guarantees atomic, hash-verified local caching in `data/raw/`.
- **Verification Impact:** Verified by `tests/unit/test_sec_fsds.py`.

---

### D-028: Vectorized DuckDB Normalization and Tolerant Schema Sampling

- **Context:** Real quarterly SEC FSDS archives exceed 3.5 million rows, include companion documentation files (`readme.htm`), alphanumeric postal codes, and multi-decade comparative footnotes. Additionally, small numbers of corrupted numeric cells (`10` cells in `200,000+` rows) should route to row-level `type_flip` quarantine rather than triggering a batch-level `type_widened` schema hold.
- **Options Considered:**
  1. Row-by-row Python normalization and zero-tolerance scalar type inference.
  2. Vectorized DuckDB SQL normalization (`all_varchar=True`, exact `DECIMAL(38, 9)` casting, distinct entity lookup joins, bit-identical `sha256` row hashing), non-data sidecar filtering in `discover.scan`, `≤ 1.0%` cell-anomaly tolerance on samples with `≥ 100` non-null values in `vdok/schema.py`, and evaluating `COVERAGE_GAP` after row-level quarantine in `vdok/validate.py`.
- **Decision:** Implement Option 2 across `vdok/adapters/sec_fsds.py`, `vdok/normalize.py`, `vdok/discover.py`, `vdok/schema.py`, and `vdok/validate.py`.
- **Justification:** Sustains multi-million-row normalization without floating-point intermediaries while cleanly separating cell-level defects (`quarantine`) from column-wide structural widening (`hold`).
- **Verification Impact:** Verified by `tests/golden/test_report_goldens.py`, `tests/unit/test_planted_faults_50mb.py`, and real SEC `2023q3`/`2023q4` onboarding benchmarks.

---

### D-029: Sorted Partition MD5 Prefix Index and Unpruned Streaming Oracle

- **Context:** Benchmarks verify 100,000 random `Store.value()` point-in-time queries per iteration with zero violations against an independent brute-force reference scan within single-node memory limits.
- **Options Considered:**
  1. Run 100,000 separate DuckDB `read_parquet` queries and materialize the full store in Python dictionaries for `BruteForceStore`.
  2. Cache per-partition `(bucket, year)` Arrow tables sorted by `(_k64 ASC, knowledge_time DESC, drop_id DESC)` queried via `bisect_left` over zero-copy `memoryview` buffers (2 GiB / 256-partition cache budget), and verify batches against `BruteForceStore.batch_values_unpruned` streaming every Parquet partition via `ParquetFile.iter_batches` with zero pruning or indexing.
- **Decision:** Implement Option 2 in `vdok/query.py`.
- **Justification:** Answers 100,000 `Store.value()` lookups in sub-second wall time while enforcing `knowledge_time ≤ as_of` (G2) and bounding memory in both `Store` and `BruteForceStore`.
- **Verification Impact:** Verified by `tests/unit/test_guarantees.py`, `tests/property/test_store_properties.py`, and `tests/unit/test_bench.py`.

---

### D-030: Per-Iteration Subprocess Isolation for Process-Tree Peak RSS

- **Context:** POSIX `resource.getrusage` tracks a monotonically non-decreasing lifetime high-water mark per process, and native DuckDB/PyArrow C++ allocators retain memory arenas across in-process iterations.
- **Options Considered:**
  1. Execute all benchmark runs inside a single Python process.
  2. Spawn an isolated `--internal-worker` child subprocess per benchmark iteration, recording `(RUSAGE_SELF + RUSAGE_CHILDREN)` peak RSS inside each worker and removing temporary store artifacts between runs.
- **Decision:** Implement Option 2 in `bench/run.py`.
- **Justification:** Measures an independent OS kernel resident memory high-water mark per run and reclaims all native C++ pages between iterations.
- **Verification Impact:** Verified by `tests/unit/test_bench.py::test_bench_cli_subprocess_entrypoint` and multi-run `make bench` outputs.
