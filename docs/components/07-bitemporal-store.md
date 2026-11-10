# Component: Append-Only Bitemporal Store (`vdok.store`)

## 1. Overview & Responsibility

The `vdok.store` module manages the persistent on-disk data storage of the platform. In quantitative research, overwriting historical data destroys reproducibility: a financial metric updated retroactively introduces look-ahead bias into historical simulations, generating unrealistic backtest performance.

`vdok.store` provides **Guarantee 1 (G1)**: strict append-only persistence. Records are written once into partitioned Apache Parquet files and never modified or deleted. Point-in-time states are reconstructed dynamically using DuckDB analytical views.

---

## 2. Partitioning Hierarchy & File Layout

Storage partitions records across three logical dimensions: vendor, field grouping, and observation year:

```
data/store/<vendor>/
├── _duckdb/
│   └── catalog.duckdb                      # DuckDB catalog database
├── <field_bucket>/
│   └── <event_year>/
│       ├── part-<drop_id_001>.parquet
│       └── part-<drop_id_002>.parquet
├── quarantine/
│   └── part-<drop_id_001>.parquet          # Quarantined rows
└── metadata/
    ├── drops.parquet                       # Committed drop log
    ├── held_drops.parquet                  # Held drop log
    ├── schema_versions.parquet             # Contract version log
    └── revisions.parquet                   # Historical revision ledger
```

### Partitioning Parameters
- `field_bucket`: High-level thematic grouping derived from the field name (e.g., `income_statement`, `balance_sheet`, `market_prices`). This prevents partition explosion while facilitating column pruning during targeted queries.
- `event_year`: Year of the `event_time` timestamp (`YYYY`), enabling temporal partition elimination when filtering historical ranges.
- `part-<drop_id>.parquet`: File name tagged with the 16-character drop fingerprint.

---

## 3. Physical Immutability & Append-Only Guarantees (G1)

The storage engine implements two safeguards against accidental data modification:

1. **Existence Verification**: Before writing partition files for an incoming drop, `store.write()` verifies whether any destination file with the same `drop_id` already exists on disk. If a collision is found, the write operation is aborted immediately:
   ```python
   destination_path = partition_dir / f"part-{drop_id}.parquet"
   if destination_path.exists():
       raise FileExistsError(
           f"Append-only invariant violated: partition file {destination_path} already exists."
       )
   ```
2. **File System Permissions**: Immediately upon completing file serialization, write permissions are removed from the file, setting POSIX mode to read-only (`0o444`):
   ```python
   import os

   os.chmod(destination_path, 0o444)
   ```
3. **No Update/Delete API**: The Python storage module exposes only read and append operations. No interface methods exist for row deletion, file replacement, or in-place value updates.

---

## 4. DuckDB Catalog Integration

To deliver interactive query performance over deep Parquet file trees, an embedded DuckDB database file (`catalog.duckdb`) maintains unified relational views:

```sql
CREATE OR REPLACE VIEW vendor_store AS
SELECT *
FROM read_parquet(
    'data/store/sec_fsds/**/*.parquet',
    hive_partitioning = false
);
```

### Metadata Tables Managed
- `drops`: Records committed drop executions, row counts, ingestion timestamps, and file statistics.
- `held_drops`: Records blocked drop executions and failure diagnostics.
- `schema_versions`: Tracks pinned schema contract definitions and activation timestamps.
- `symbol_master_versions`: Tracks reference symbology versions and effective intervals.
- `revisions`: Stores restatements identified across consecutive drops.

---

## 5. Storage Expansion Accounting

Because the store never overwrites older vintages, total disk footprint grows with each revision. The storage module measures and reports the **Storage Expansion Factor** ($S_f$):

$$S_f = \frac{\text{Bytes on Disk (All Historical Vintages)}}{\text{Bytes Required for Latest Deduplicated Snapshot}}$$

This metric enables engineering teams to monitor capacity expansion trends over multi-year ingestion schedules.
