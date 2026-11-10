# ADR 004: Local-First DuckDB and Parquet Store

## Status
Accepted

## Context
Data infrastructure in quantitative firms often requires multi-node clusters, external databases, cloud object stores (e.g., AWS S3, Google Cloud Storage), and dedicated orchestration services. 

However, external service dependencies make local testing, rapid iteration, and self-contained reproduction cumbersome. Developers cannot easily replicate pipeline behavior on a workstation, and CI/CD pipelines require elaborate service mocks or live credentials.

## Options Considered

1. **Cloud Data Warehouse / Object Store Architecture (Snowflake, BigQuery, Delta Lake on S3)**:
   - Stream raw data directly to cloud object storage and query via cloud data warehouses.
   - *Pros*: Scalable to petabyte volumes; built-in cloud identity management.
   - *Cons*: Network latency; listing latencies and eventual consistency costs; cannot execute locally or offline; credential setup blocks local evaluation.

2. **Traditional Relational Database (PostgreSQL / TimescaleDB)**:
   - Ingest records into an external database server running via Docker or a remote host.
   - *Pros*: ACID transactions; mature ecosystem.
   - *Cons*: Requires background daemon management; row-oriented storage slows analytical aggregations over millions of historical observations.

3. **Local-First Columnar Parquet Tree with Embedded DuckDB**:
   - Store records in partitioned columnar Apache Parquet files directly on the local file system.
   - Execute vectorized SQL queries, window evaluations, and catalog views using embedded DuckDB.

## Decision
Adopt **Option 3 (Local-First Columnar Parquet Tree with Embedded DuckDB)**.

- Ingested records are written to local partitioned Parquet files (`store/<vendor>/<field_bucket>/<event_year>/part-<drop_id>.parquet`).
- An embedded DuckDB database (`catalog.duckdb`) provides zero-copy analytical queries, unified views, and catalog tracking without requiring external server processes.
- Single-command execution: runs on any developer laptop via `make onboard` without network dependencies.

## Consequences

- **Optimizes**: Single-command reproducibility, zero-setup onboarding, local development execution, and columnar query throughput (multi-GB/s per core).
- **Costs**:
  - Scale limit: Constrained to single-node disk and RAM capacity (sufficient for gigabyte-to-terabyte scale on modern workstations, but not petabyte scale without follow-up distributed compute).
  - Cloud semantics omitted: Does not handle distributed object store listing latency or multi-region eventual consistency. These are cataloged as future follow-ups.
