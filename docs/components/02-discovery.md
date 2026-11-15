# Component: Drop Discovery & Fingerprinting (`vdok.discover`)

## 1. Overview & Responsibility

`vdok.discover` scans incoming file or directory drop paths, detects file formats and compression, verifies archive integrity, filters non-data documentation sidecars, computes uncompressed payload SHA-256 digests, and derives the deterministic 16-character `drop_id`. This underpins **Guarantee 3 (G3)** (drop ingestion idempotency).

---

## 2. Format Inspection & Data Contracts

### 2.1 Supported Formats
- **Tabular Formats (`FileFormat`)**: `csv`, `jsonl`, `parquet`.
- **Compression (`Compression`)**: `none`, `gzip` (`.gz`).
- **Homogeneous Drop Invariant**: All data files within a drop must share the same `FileFormat`. Mixing formats raises `HeterogeneousFormatError`.

### 2.2 Inspection Hierarchy (`detect_file_format`)
1. Inspect leading magic bytes for Parquet (`PAR1`) and Gzip (`\x1f\x8b`).
2. For gzipped streams, verify decompression without `zlib.error` / `EOFError` (wrapping failures as `CorruptedArchiveError`) and reject binary null bytes in text payloads.
3. Scan up to 50 non-empty leading lines and test with `json.loads` for JSON object syntax (`{...}`) **ahead** of CSV sniffing so JSON Lines containing embedded commas are never misclassified as CSV.
4. Fall back to CSV delimiter sniffing (supporting single-column CSV tables as well as comma, tab, pipe, and semicolon delimiters).

### 2.3 Data Classes
- **`DiscoveredFile`**: `path`, `relative_path`, `format`, `compression`, `size_bytes`, `content_hash`.
- **`DropMetadata`**: `drop_id`, `file_format`, `compression`, `files`, `total_bytes`, `discovered_at`.

---

## 3. Cryptographic Drop Identity (`compute_drop_id`)

1. For each discovered file, `hash_file_payload` streams uncompressed bytes (decompressing `.gz` files in-flight) and computes its SHA-256 hex digest. Hashing uncompressed payloads guarantees identical `drop_id` values regardless of gzip timestamps or compression levels.
2. All per-file digests are sorted lexicographically and concatenated.
3. `drop_id` is the first 16 hexadecimal characters of `sha256(concatenated_hashes)`.

---

## 4. Idempotency Check (`check_idempotency`)

Before running schema checks, normalization, or validation, `check_idempotency(drop_id, store_path)` validates that `drop_id` matches `^[0-9a-f]{16}$` and queries `metadata/drops.parquet` / `store.duckdb` for a committed record with that `drop_id`. When present (and `--force` is not set), `onboard_drop` returns the existing report without writing partition or revision rows.

---

## 5. Exception Taxonomy

| Exception (`DiscoveryError`) | Trigger |
|---|---|
| `EmptyDropError` | Drop path contains no supported data files |
| `HeterogeneousFormatError` | Drop mixes distinct `FileFormat` values (e.g., CSV and Parquet) |
| `CorruptedArchiveError` | Truncated or invalid gzip/DEFLATE stream (`zlib.error` / `EOFError`) |
| `FormatMismatchError` | Declared or inspected stream conflicts with file structure |
| `FileAccessError` | Missing path or OS permission failure |
| `UnrecognizedFormatError` | File matches neither Parquet, JSON Lines, nor valid delimited text |
