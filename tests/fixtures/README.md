# Test Fixtures Provenance and Selection

This directory contains deterministic offline fixtures used by the test suite and CI pipeline.

## 1. SEC Financial Statement Data Sets (FSDS)

### Provenance and License
The SEC Financial Statement Data Sets are published quarterly in the public domain by the U.S. Securities and Exchange Commission (SEC) Office of Structured Disclosure:
- `https://www.sec.gov/files/dera/data/financial-statement-data-sets/2023q3.zip`
- `https://www.sec.gov/files/dera/data/financial-statement-data-sets/2023q4.zip`

### Structure
The fixture directory (`tests/fixtures/sec_fsds/`) provides two consecutive quarters:
- `2023q3/`: `sub.txt` (submissions metadata) and `num.txt` (numeric disclosures).
- `2023q4/`: `sub.txt` and `num.txt` for the subsequent quarter, including restatements of `2023q3` observations.

### Selection and Generation Commands
Fixtures are sampled deterministically by hash from downloaded quarterly archives:

```bash
python scripts/select_sec_fixtures.py \
  --q3 data/raw/sec_fsds/2023q3 \
  --q4 data/raw/sec_fsds/2023q4 \
  --output-dir tests/fixtures/sec_fsds \
  --target-ciks 50 \
  --target-num-rows 20000 \
  --min-revisions 20
```

When raw quarterly downloads are unavailable locally, the synthetic SEC schema fixture generator produces deterministic schema-matching fixtures:

```bash
python tests/fixtures/generate_sec_fixtures.py
```

### Verification Criteria
The SEC FSDS fixture suite (`tests/unit/test_sec_fsds.py`) enforces:
1. **Entity Coverage**: At least 50 distinct corporate CIKs.
2. **Restatement Ground Truth**: At least 20 revision pairs detected across `2023q3` and `2023q4`.
3. **Point-in-Time Integrity**: 0 point-in-time violations during historical `as_of` queries.
4. **Symbol Resolution**: Mapped identifier share exceeding 95% against the fixture symbol master.

## 2. Symbol Master Reference

- Source: `https://www.sec.gov/files/company_tickers.json` (SEC public domain).
- Committed fixtures (`tests/fixtures/symbols/company_tickers.json` and `company_tickers_v2.json`) provide 200 corporate entries with `cik_str`, `ticker`, and `title`.

## 3. Synthetic Benchmark and Format Drops

- `tests/fixtures/drops/`: Deterministic CSV, CSV.GZ, JSONL, JSONL.GZ, Parquet, multi-file, and corrupted-archive drops (`mtime=0` for gzip reproducibility).
- `tests/fixtures/drift/`: Format-specific drops exercising schema drift classification (`added_column`, `removed_column`, `type_widened`, `type_narrowed`, `nullability_change`).
