# Component: Append-Only Bitemporal Store (`vdok.store`)

## 1. Overview & Responsibility

`vdok.store` manages persistent Parquet partitions and the embedded DuckDB catalog (`store.duckdb`). It enforces **Guarantee 1 (G1)**: physical append-only storage with zero update or delete paths.

---

## 2. Partitioning Hierarchy & Field Bucketing

Records are partitioned by vendor, thematic field bucket, and observation year:

```
data/store/<vendor>/
├── store.duckdb                           # Embedded DuckDB catalog and views
├── <field_bucket>/
│   └── <event_year>/
│       ├── part-<drop_id_1>.parquet       # Immutable 0o444 data partition
│       └── part-<drop_id_2>.parquet
├── quarantine/
│   └── part-<drop_id>.parquet             # Quarantined records
└── metadata/
    ├── drops.parquet                      # Committed drop log
    ├── held_drops.parquet                 # Held drop log
    ├── schema_versions.parquet            # Schema version history
    └── revisions.parquet                  # Historical restatement ledger
```

### Deterministic 4-Tier Field Bucketing (`resolve_field_bucket`)
To prevent filesystem inode explosion across thousands of distinct accounting tags while preserving partition pruning, `resolve_field_bucket` maps `field` to a bounded set of buckets:
1. **Manifest Override**: Explicit `manifest_buckets` mapping if supplied.
2. **Financial Statement Taxonomy**: `cash_flow` (evaluated before `balance_sheet` to avoid `cash` keyword collisions), `income_statement`, and `balance_sheet`.
3. **Market Data Taxonomy**: `market_prices` (`price`, `bid`, `ask`, `open`, `high`, `low`, `close`, `vwap`, `volume`, etc.).
4. **Macroeconomic Taxonomy & Fallback**: `macro` (`gdp`, `cpi`, `unrate`, etc.) or `general`.

---

## 3. Physical Immutability & Collision Refusal (G1)

`write_partitioned_store` and `BitemporalStore.write_drop` enforce physical immutability through three controls:
1. **Pre-Write Collision Check**: Scans the vendor store for any existing `part-<drop_id>.parquet`. If found, it aborts before writing and raises `AppendOnlyViolationError(FileExistsError)`.
2. **POSIX Read-Only Permissions**: Immediately upon writing each partition file, permissions are locked to `0o444` via `os.chmod(path, 0o444)`.
3. **Append-Only Interface**: The store module exposes read and append operations only; no row update or deletion methods exist.

---

## 4. DuckDB Catalog Integration (`init_store_catalog`)

`store.duckdb` maintains relational views over the Parquet tree (`vendor_events` over `*/*/*.parquet`, `quarantine_events` over `quarantine/*.parquet`) and synchronizes metadata tables (`drops`, `held_drops`, `schema_versions`, `symbol_master_versions`, `revisions`) from the Parquet files under `metadata/`.
