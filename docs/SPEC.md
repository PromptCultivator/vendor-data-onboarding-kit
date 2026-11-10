# Build prompt: `vendor-data-onboarding-kit`

How to use this file: save it as `docs/SPEC.md` in an empty repository, then start the coding agent with:

> Read `docs/SPEC.md` in full. Execute Phase 0. Stop at the end of each phase and report using the format in section 14. Do not start the next phase until I say so.

Everything below is addressed to the coding agent.

---

## 1. Role and working rules

You are building a small, self-contained, open-source Python repository that onboards a raw vendor dataset into a point-in-time-correct store and produces a readiness report. The repository will be read by engineers at quantitative trading firms. It is a showcase of engineering judgment, so correctness guarantees, measured numbers, and documented trade-offs matter more than feature count.

Rules that apply to every phase:

1. Work in the phases of section 13, in order. After each phase, run the full test suite, then stop and report. Do not begin the next phase without confirmation.
2. Never fabricate a number. Benchmark cells in the README stay `TBD` until `make bench` has produced them on the machine the README names. Dataset statistics come from code you ran. If a command fails, report the failure and the output; do not paper over it.
3. Never reference any company's internal systems, metrics, code, or data. Public datasets and generated data only.
4. Do not add a dependency outside the allowed list in section 10 without stopping to ask. Pin every dependency to an exact version.
5. No network access in tests. CI runs on committed fixtures only. Downloads happen only in `make data`, with a declared `User-Agent` and rate limit.
6. Keep each commit small with an imperative message that names what changed and why. Do not squash. Do not rewrite history.
7. Every design decision listed in section 11 gets an ADR in `docs/adr/` with context, options considered, the decision, and the measured or expected cost. If you make a decision not listed there, add an ADR and note it in your phase report.
8. Record what you generated. Maintain `docs/AI-ASSISTANCE.md`: a list of files and functions you wrote, with a column the human fills in after review ("hand-verified: yes/no, how"). The README's "AI assistance" section links to it.
9. Prefer plain code over frameworks. One repository, one `make` entry point per task, runs on a laptop.
10. When this specification is ambiguous, choose the option that keeps a guarantee checkable by a test, write down the choice in `docs/DECISIONS.md`, and continue.

## 2. Project summary

A vendor dataset arrives as files (CSV, JSON Lines, or Parquet) in an unknown schema with inconsistent identifiers. The kit infers and pins the schema, normalizes rows to a canonical bitemporal model, maps identifiers through a symbol master, validates rows and batches with severity tiers, writes an append-only store keyed by `(entity, event_time, knowledge_time)`, detects revisions when a later drop changes an earlier value, and emits a readiness report that tells a researcher whether the dataset is fit for use and what to ask the vendor. A point-in-time query API answers "what did this dataset say about entity E at event time T, as known at time K" and a leak detector reports any read that used information from after its decision time.

The primary real dataset is the SEC Financial Statement Data Sets, which contain real restatements and give revision-detection ground truth for free. Everything else is generated.

## 3. Goals and non-goals

Goals:

- One command onboards a drop and produces a report.
- Three guarantees, each checked by a test and reported in the README: (G1) the store is append-only, (G2) a point-in-time query never returns a value whose knowledge time is after the requested as-of time, (G3) re-running the same drop produces no new rows.
- Measured numbers: time from raw drop to report, validation throughput, store write throughput, peak memory, revision-detection recall and precision on planted restatements, point-in-time violations (must be 0), leak-detector recall on planted leaks.

Non-goals (do not build): paid vendor connectors, entitlements or access control, a web UI, distributed execution, a model-based (LLM) stage, a Java or Arrow Flight server. These may be listed as follow-ups in the README.

## 4. Repository layout

```
vendor-data-onboarding-kit/
  README.md
  LICENSE                      Apache-2.0
  NOTICE                       only if third-party notices are redistributed
  pyproject.toml               license = "Apache-2.0"; pinned deps; console script `vdok`
  Makefile                     targets in section 9
  Dockerfile                   python:3.12-slim; runs `make test` by default
  .github/workflows/ci.yml     lint, typecheck, tests, bench smoke
  vdok/
    __init__.py
    cli.py                     typer app
    manifest.py                vendor manifest model (Pydantic)
    discover.py                file discovery, format detection, drop id
    schema.py                  inference, pinning, drift detection
    normalize.py               canonical row model and normalizers
    symbols.py                 symbol master and identifier mapping
    validate.py                rule engine with severities
    store.py                   bitemporal Parquet store, DuckDB views
    revisions.py               revision ledger
    query.py                   point-in-time API and leak detector
    report.py                  readiness report (Markdown + JSON)
    adapters/
      sec_fsds.py              SEC Financial Statement Data Sets adapter
      alfred.py                optional FRED/ALFRED vintages adapter (off by default)
      synthetic.py             generator with planted faults
  schemas/                     pinned schemas, one YAML per vendor
  manifests/                   vendor manifests, one YAML per vendor
  data/                        gitignored; raw/ , store/ , results/
  tests/
    unit/  property/  golden/  fixtures/
  bench/
    run.py  hardware.py  results/
  docs/
    SPEC.md  DECISIONS.md  AI-ASSISTANCE.md
    adr/001-...md ... 005-...md
```

## 5. Functional specification

### 5.1 Inputs

- A drop directory or file: `*.csv`, `*.jsonl`, `*.parquet`, optionally gzip-compressed. Mixed formats in one drop are rejected with a clear error.
- A vendor manifest (`manifests/<vendor>.yaml`):

```yaml
vendor: sec_fsds
entity_columns: [cik]              # columns that identify the entity, in priority order
entity_id_type: cik                # cik | ticker | isin | cusip | vendor_id
event_time_column: ddate           # the time the value is about
event_time_format: "%Y%m%d"
knowledge_time: {source: column, column: filed, format: "%Y%m%d"}   # or {source: drop_time}
timezone: UTC
value_columns: [value]
unit_column: uom                   # optional
field_columns: [tag]               # columns whose combination names the field; optional
expected_calendar: quarterly       # daily | weekly | monthly | quarterly | none
expected_universe: null            # optional path to a list of entity ids
rules: rules/sec_fsds.yaml         # validation rules (section 5.5)
```

- `--as-of` override for knowledge time when the drop carries none (defaults to drop receipt time, recorded in UTC).

### 5.2 Discovery and drop identity

- `discover.scan(path)` returns the file list, detected format, compression, sizes, and a `drop_id = sha256(sorted(file sha256s))[:16]`.
- Re-onboarding a drop with the same `drop_id` is a no-op that returns the existing drop record (supports G3).

### 5.3 Schema inference and pinning

- Infer column types from a sample of up to 200,000 rows per file: integer, decimal (exact, not float, for money), float, string, date, datetime, boolean; nullability; distinct-count estimate; min and max for numerics and dates.
- First onboarding writes `schemas/<vendor>.yaml`. Later drops are checked against the pinned schema. Differences are classified: `added_column` (warn), `removed_column` (hold batch), `type_narrowed` (warn), `type_widened` (hold batch), `nullability_change` (warn). Each difference appears in the report with a proposed schema change the human can accept with `vdok schema accept <vendor>`.
- Pinned schemas are human-editable and version-controlled.

### 5.4 Normalization

Canonical row (Arrow schema, also a Pydantic model for tests):

```
entity_id: string          # mapped identifier (section 5.6) or the raw id if unmapped
entity_id_type: string
field: string              # from field_columns, joined by "|", or the value column name
event_time: timestamp[us, UTC]
knowledge_time: timestamp[us, UTC]
value_num: decimal128(38, 9) | null
value_str: string | null
unit: string | null
drop_id: string
source_file: string
source_row: int64
row_hash: string           # sha256 of the normalized (entity_id, field, event_time, value_num, value_str, unit)
```

Normalizers: timezone attachment, date and datetime parsing with the manifest's format, decimal parsing that rejects floats for money fields, unit normalization through a small table (`USD`, `shares`, `pure`, `%` to `ratio`), whitespace and case folding for string identifiers. Every normalizer is a pure function with property tests.

### 5.5 Validation with severity tiers

Rules live in `rules/<vendor>.yaml` and are compiled into DuckDB SQL executed once per drop. Each rule has `severity: warn | quarantine | hold`.

- Row-level rules (`quarantine` sends the row to `quarantine.parquet` with a `reasons` array; `warn` records only): not null, range, allowed values, regex pattern, unit in allowed set, decimal scale within bounds, event_time within `[1990-01-01, drop_time + 1 day]`, knowledge_time ≥ event_time when the manifest says the vendor reports after the fact.
- Batch-level rules (`hold` stops the drop before the store write and writes one `held_drops` record): schema `removed_column` or `type_widened`; row count outside `[0.5, 2.0] ×` trailing median of the last 7 drops (first drop exempt); duplicate `(entity_id, field, event_time, knowledge_time)` share above a threshold (default 1%; below it, duplicates are quarantined as rows); byte-identical to the previous drop; coverage gap: zero rows for more than N expected calendar periods (manifest-defined).
- Output of validation: counts per rule, per severity; the quarantine file; a boolean `held`.

### 5.6 Identifier mapping

- Symbol master built from the SEC `company_tickers.json` (CIK, ticker, title) by `make data`; stored as Parquet; refreshed by hash so a changed file produces a new master version with an effective date.
- Mapping is rule-based: exact match on the manifest's `entity_id_type`; for `ticker`, case-fold and strip exchange suffixes from a documented list; for `name`, exact match on normalized title only (no fuzzy matching in v1). Unresolved rows keep the raw id, are flagged `unmapped=true`, and are counted in the report. No model-based mapping; the master is never written by inferred matches.

### 5.7 Bitemporal store

- Parquet files partitioned as `store/<vendor>/<field_bucket>/<event_year>/part-<drop_id>.parquet`. Append-only: the writer has no update or delete path; `store.write()` refuses if a file for `drop_id` already exists (G1, G3).
- A DuckDB database file holds views over the Parquet tree plus small tables: `drops`, `held_drops`, `schema_versions`, `symbol_master_versions`, `revisions`.
- `knowledge_time` is the manifest's knowledge column (for SEC FSDS, the filing's `filed` date) or the drop receipt time.

### 5.8 Revision ledger

- After each drop, a query finds `(entity_id, field, event_time)` keys that already exist with a different `(value_num, value_str, unit)` at an earlier `knowledge_time`. Each such pair becomes a `revisions` row: key, old value, new value, old knowledge_time, new knowledge_time, drop ids, relative change.
- Revision rate per field and per drop goes into the report.

### 5.9 Point-in-time query API and leak detector

```python
from vdok.query import Store
s = Store("data/store/sec_fsds")
s.value(entity_id="0000320193", field="Revenues|USD|4Q", event_time="2023-12-31", as_of="2024-02-15")
s.series(entity_id=..., field=..., as_of=..., start=..., end=...)
```

- `value` returns the row with the greatest `knowledge_time ≤ as_of` for the key, or `None`. It never reads rows with `knowledge_time > as_of` (G2); the test for G2 plants a later revision and asserts the earlier value is returned.
- `Store(..., audit=True)` logs every read as `(decision_time, as_of, key, knowledge_time_returned)`. `vdok leaks --log <file> --decision-time-column <col>` reports reads where `as_of > decision_time` or where the returned `knowledge_time > decision_time`. A planted-leak test drives a toy model twice and shows the leak's effect on its score.

### 5.10 Readiness report

`vdok report <vendor> --drop <drop_id>` writes `data/results/<vendor>/<drop_id>/report.md` and `report.json` with:

1. Verdict line: `READY`, `READY WITH WARNINGS`, or `HELD`, with the reason.
2. Drop summary: files, bytes, rows, distinct entities, fields, event-time span, knowledge-time span.
3. Schema: pinned version, differences found, proposed changes.
4. Coverage by period: a compact table of row counts per expected calendar period; gaps marked.
5. Validation: counts per rule and severity; quarantine share; top reasons.
6. Identifiers: mapped share; top unmapped ids.
7. Revisions: rate per field; largest relative changes; a note when a field has zero revisions across more than three drops (a signal the vendor overwrites history).
8. Questions for the vendor: generated deterministically from templates keyed by failed rule (for example: "Column `uom` contains 3 units outside the documented set: ... Which is authoritative?"). No model generation.
9. Timings per stage and peak memory for this run.

### 5.11 Diff

`vdok diff <vendor> <drop_a> <drop_b>` reports added and removed keys, changed values (from the revision ledger), and schema differences between two drops.

## 6. Datasets and adapters

### 6.1 SEC Financial Statement Data Sets (primary; public domain)

- Quarterly ZIP archives, each with tab-delimited `sub.txt`, `num.txt`, `tag.txt`, `pre.txt`. `make data` downloads a configurable range (default: the four most recent quarters) with `User-Agent: vendor-data-onboarding-kit <contact email from env VDOK_CONTACT>` and at most 5 requests per second. Downloads are cached by hash in `data/raw/`.
- Adapter: joins `num` to `sub` on `adsh`; entity = `cik` (zero-padded to 10); field = `tag|uom|qtrs`; event_time = `ddate`; knowledge_time = `filed` (date the filing was accepted by EDGAR); value = `value` as decimal. Rows with `coreg` set are kept with `entity_id` suffixed by `|coreg=<name>` so parent-only queries remain simple.
- Why this dataset: the same `(cik, tag, ddate, qtrs)` appears in several filings with different values when companies restate, so the revision ledger has real ground truth. The adapter test asserts that known restatement patterns produce revision rows.
- Fixture: a committed subset of about 20,000 `num` rows and the matching `sub` rows from one quarter, selected deterministically by hash so CI covers at least 50 CIKs and at least 20 revision pairs across two quarters. Document the selection command in `tests/fixtures/README.md`.

### 6.2 Symbol master (public domain)

- `https://www.sec.gov/files/company_tickers.json`, downloaded by `make data`, versioned by hash. Fixture: 200 entries.

### 6.3 FRED / ALFRED vintages (optional, off by default)

- The FRED API returns vintages through `realtime_start` and `realtime_end`; a free API key is required (`FRED_API_KEY`). Adapter maps series observations to canonical rows with `knowledge_time = realtime_start`. Do not commit FRED data as fixtures; the adapter's tests use a synthetic response fixture. The README states that the FRED terms of use must be read before any redistribution.

### 6.4 Synthetic generator (for benchmarks and planted faults)

`vdok synth --size-gb 1 --seed 1 --faults default` writes a drop sequence (default 3 drops) with a known ground truth file. Planted faults, each individually switchable, each with a known count:

- `restatement`: later drop changes a value for an earlier key (ground truth for revision recall and precision)
- `duplicate_row`, `type_flip`, `out_of_range`, `missing_period`, `identifier_change` (entity id rewritten to an alias present in the symbol master), `unit_change` (USD to thousands of USD), `late_row` (event_time long before knowledge_time), `repeat_drop` (byte-identical), `schema_drift` (removed column in drop 3)

Default sizes: 50 MB for CI smoke, 1 GB for local runs, 10 GB and 50 GB for the README benchmark.

## 7. Tests

- Unit tests per module. Property tests (Hypothesis) for normalizers (round trips, timezone, decimal exactness) and for the store (append-only, idempotent re-run, point-in-time selection against a brute-force reference implementation that scans all rows).
- Golden tests for the report Markdown on the fixture drop; update goldens only through `make goldens` with a diff shown in the report.
- Guarantee tests, named so a reader can find them: `test_g1_store_is_append_only`, `test_g2_pit_never_reads_future_knowledge`, `test_g3_rerun_same_drop_adds_no_rows`.
- Planted-fault tests on the synthetic generator at 50 MB: every fault type is detected with the expected severity and count.
- SEC adapter test on the fixture: at least 20 revision rows found; zero point-in-time violations; mapped share above 95% for the fixture's CIKs.
- Coverage target 85% lines; the CI job fails below it.

## 8. Benchmark

`make bench SIZE=10GB RUNS=5 SEED=1` runs onboarding on a synthetic drop sequence and writes `bench/results/bench.csv` and `bench/results/bench.md`:

| Metric | How measured |
| --- | --- |
| Time from raw drop to report | wall clock, per stage and total |
| Validation throughput | GB/s per core and rows/s, single process; report core count used |
| Store write throughput | rows/s and MB/s to Parquet |
| Peak resident memory | `resource.getrusage` max RSS of the process tree |
| Revision detection recall and precision | against the generator's ground truth |
| Point-in-time violations | count over 100,000 random `value()` queries checked against the brute-force reference; must be 0 |
| Leak detector recall | on 1,000 planted leaking reads |
| Storage factor | bytes in store divided by bytes of the raw drops |

- 5 runs; report median and min–max. `bench/hardware.py` records CPU model, cores, RAM, disk type, OS, Python, DuckDB and PyArrow versions, and the git commit; the README copies this block verbatim.
- The CI smoke run uses `SIZE=50MB RUNS=1` and asserts only that the benchmark completes and that violations are 0.
- Also run the benchmark on the real SEC range configured by `make data` and report the same metrics where applicable (recall and precision are not available without ground truth; report revision counts instead).

## 9. Makefile targets

```
make setup      create venv, install pinned deps
make lint       ruff check + ruff format --check
make typecheck  pyright (or mypy --strict if pyright is unavailable)
make test       pytest with coverage gate
make data       download SEC FSDS range + company_tickers.json (rate-limited, cached)
make synth      generate synthetic drops (SIZE, SEED, FAULTS)
make onboard    vdok onboard $(VENDOR) $(DROP)
make report     vdok report $(VENDOR) --drop $(DROP)
make bench      full benchmark (SIZE, RUNS, SEED)
make bench-smoke 50 MB, one run, used by CI
make goldens    regenerate golden report files
make docker     build the image and run `make test` inside it
```

## 10. Allowed dependencies (pin exact versions)

Runtime: `duckdb`, `pyarrow`, `polars` (optional; use for CSV and JSONL scanning if it is faster than PyArrow in your measurement, and record the measurement), `pydantic>=2`, `typer`, `pyyaml`, `python-dateutil`. Development: `pytest`, `pytest-cov`, `hypothesis`, `ruff`, `pyright`. Nothing else without asking. No LLM clients. No telemetry.

## 11. ADRs to write

1. `001-bitemporal-append-only.md`: values are never updated in place; each is stored with its knowledge time. Optimizes point-in-time reproducibility; costs storage growth (report the measured storage factor) and a bitemporal query path.
2. `002-severity-tiers.md`: warn / quarantine row / hold batch. Optimizes availability of good rows; costs a second write path and the reconciliation implied by quarantine.
3. `003-schema-pinning.md`: inferred then pinned, drift classified, changes accepted by a human command. Optimizes detection of silent vendor changes; costs an approval step on each change.
4. `004-local-first-duckdb-parquet.md`: single process, Parquet tree, DuckDB views. Optimizes one-command reproducibility; gives up distributed scale and cloud object-store semantics (listing cost, eventual consistency), listed as follow-ups.
5. `005-model-free-identifier-mapping.md`: rule-based mapping against a versioned master; unresolved rows flagged, never guessed. Optimizes auditability; costs a manual queue whose size is reported.

## 12. README requirements

Write the README in plain, short sentences. Sections, in this order:

1. Title and one sentence stating what the kit does.
2. Status line: `design complete` → `benchmarked on <hardware>` once numbers exist.
3. Results table with the metrics of section 8, `TBD` until measured, plus the hardware block.
4. Quick start: the Makefile targets.
5. Problem: three or four sentences on why vendor onboarding fails (unknown schema, inconsistent identifiers, silent revisions, downstream SLA).
6. How it works: the pipeline diagram (ASCII), the canonical row, the manifest example, the severity tiers, the bitemporal store, the revision ledger, the point-in-time API, the leak detector.
7. Guarantees: G1, G2, G3 with the names of the tests that check them.
8. Benchmark method: sizes, runs, hardware recording, what is and is not measured.
9. Design decisions: a table of the five ADRs with "optimizes" and "costs" columns.
10. Limits and follow-ups: the non-goals of section 3.
11. Related work: Great Expectations and Soda (validation rules only), ArcticDB (versioned time-series storage, BSL 1.1), Feast and other feature stores (point-in-time joins), Quanted (commercial validation). State only what their public documentation says; if unsure, omit the claim. One sentence on the differentiator: a revision ledger plus a readiness report plus a leak detector in one pipeline.
12. Data: sources, licenses, what is committed (fixtures only) and what is downloaded.
13. AI assistance: link to `docs/AI-ASSISTANCE.md`.
14. License: Apache-2.0.

Do not use the words "simply", "robust", "seamless", "best", "optimal", "clean", "minimal", "powerful", or "cutting-edge" anywhere in the repository.

## 13. Phased delivery

Each phase ends with: all tests green, lint and typecheck clean, a commit, and a report (section 14).

- **Phase 0 — Scaffold.** Repository layout, `pyproject.toml` with pinned deps and `license = "Apache-2.0"`, `LICENSE` text with the current year and the owner's name as given by the human (ask if not given), Makefile, Dockerfile, CI workflow (lint, typecheck, test, bench-smoke on a stub), README with all sections present and `TBD` placeholders, `docs/DECISIONS.md`, `docs/AI-ASSISTANCE.md`, empty ADR files with titles. Acceptance: `make test` passes on a trivial test; CI config validates.
- **Phase 1 — Discovery and schema.** `discover.py`, `schema.py`, manifest model, `vdok schema infer|pin|check|accept`. Acceptance: inference and drift classification tests pass on fixtures in all three formats; a drop id is stable across runs.
- **Phase 2 — Normalization and identifier mapping.** `normalize.py`, `symbols.py`, symbol-master build from a fixture. Acceptance: property tests pass; decimal money fields never pass through float; unmapped rows are flagged, never guessed.
- **Phase 3 — Validation.** `validate.py` rule compiler and severities; quarantine and held_drops outputs. Acceptance: each rule type has a test; the planted-fault tests for `duplicate_row`, `type_flip`, `out_of_range`, `repeat_drop`, `schema_drift` pass at 50 MB.
- **Phase 4 — Store, revisions, point-in-time API.** `store.py`, `revisions.py`, `query.py` with the brute-force reference and the audit log. Acceptance: G1, G2, G3 tests pass; `restatement` planted-fault test reports exact recall and precision; leak detector finds all planted leaks.
- **Phase 5 — Report and diff.** `report.py`, `vdok diff`, golden tests. Acceptance: report renders for the synthetic drop and the SEC fixture; verdict logic tested for all three outcomes.
- **Phase 6 — SEC adapter and data download.** `adapters/sec_fsds.py`, `make data` with rate limit and caching, fixture selection script. Acceptance: fixture tests of section 7 pass; a real download of one quarter completes locally and is reported with row counts.
- **Phase 7 — Benchmark and ADRs.** `bench/run.py`, `bench/hardware.py`, five ADRs written, README results table filled from `bench/results/bench.md` only after the human confirms the run happened on the named machine. Acceptance: `make bench-smoke` passes in CI; the full benchmark outputs exist for 1 GB locally; the 10 GB and 50 GB cells may remain `TBD` until the human runs them.
- **Phase 8 (optional, only if asked).** ALFRED adapter; Arrow Flight read endpoint in Python.

## 14. Phase report format

At the end of each phase, report in this order and nothing else:

1. Phase number and one line on what now works.
2. Commands run and their exit status; test count, coverage percentage, lint and typecheck status.
3. Measured numbers produced in this phase, with the command that produced each.
4. Decisions made that were not in the spec, each with its `DECISIONS.md` entry.
5. Open questions for the human, each with the default you will take if unanswered.
6. The next phase's first three steps.

## 15. Definition of done

- `git clone`, `make setup`, `make test` pass on a clean machine in under 10 minutes without network access beyond package installation.
- `make data && make onboard VENDOR=sec_fsds DROP=data/raw/sec_fsds/<quarter> && make report ...` produces a report on real data.
- The README results table contains numbers only from `bench/results/` runs on the hardware named in the README.
- G1, G2, G3 are each enforced by a named test and described in the README.
- Five ADRs exist with measured or stated costs.
- `docs/AI-ASSISTANCE.md` lists every generated file.
- No fabricated figure, no employer reference, no banned word.
