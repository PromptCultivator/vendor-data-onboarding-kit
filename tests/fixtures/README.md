# Test Fixtures Provenance and Selection

This directory contains deterministic test fixtures used by the test suite and continuous integration pipeline.

## 1. SEC Financial Statement Data Sets (FSDS)

### Provenance and License
The SEC Financial Statement Data Sets are compiled and published quarterly by the US Securities and Exchange Commission (SEC) Office of Structured Disclosure (OSD) in the public domain.

Source archive locations:
- `https://www.sec.gov/files/dera/data/financial-statement-data-sets/2023q3.zip`
- `https://www.sec.gov/files/dera/data/financial-statement-data-sets/2023q4.zip`

### Structure
The SEC FSDS fixture directory (`tests/fixtures/sec_fsds`) provides two sequential quarters:
- `2023q3/`:
  - `sub.txt`: Submissions metadata (accession number `adsh`, `cik`, company name, filing date `filed`, etc.).
  - `num.txt`: Numerical disclosures (accession number `adsh`, financial `tag`, `ddate`, `qtrs`, `uom`, `value`).
- `2023q4/`:
  - `sub.txt`: Submissions metadata for subsequent quarter filings.
  - `num.txt`: Numerical disclosures for subsequent quarter filings, including restatements of earlier periods.

### Selection Command
Per Section 6.1 of the specification, fixtures are sampled deterministically by hash from source data:

```bash
python scripts/select_sec_fixtures.py \
  --q3 data/raw/sec_fsds/2023q3 \
  --q4 data/raw/sec_fsds/2023q4 \
  --output-dir tests/fixtures/sec_fsds \
  --target-ciks 50 \
  --target-num-rows 20000 \
  --min-revisions 20
```

When raw multi-gigabyte quarterly downloads are not locally mounted, the generator script provides an identical deterministic schema fixture:

```bash
python tests/fixtures/generate_sec_fixtures.py
```

### Coverage and Verification Criteria
The test suite enforces the following acceptance thresholds on the SEC FSDS fixtures:
1. **Entity Coverage**: At least 50 distinct corporate Central Index Keys (CIKs).
2. **Restatement Ground Truth**: At least 20 revision pairs detected across consecutive quarters (`2023q3` and `2023q4`).
3. **Point-In-Time Integrity**: 0 point-in-time violations during retrospective bitemporal queries.
4. **Symbol Resolution**: Mapped identifier share exceeds 95% against the symbol master reference.

## 2. Symbol Master Reference

### Provenance
- `https://www.sec.gov/files/company_tickers.json`
- Public domain corporate reference table published by the SEC.
- The committed fixture (`tests/fixtures/symbols/company_tickers.json`) contains 200 verified corporate entities with `cik_str`, `ticker`, and `title`.

## 3. Synthetic Benchmark Drops

- Committed in `tests/fixtures/drops/` across CSV, JSON Lines, and Parquet formats.
- Generated with deterministic seeds to verify baseline schema inference and planted faults.
