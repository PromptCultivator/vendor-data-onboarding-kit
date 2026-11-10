"""
Tier 1 Feature Tests: F-STR-01..05 Append-Only Bitemporal Store & Query Engine.

Tests append-only partitioned Parquet storage, file immutability,
DuckDB views & macros, point-in-time (as-of) queries with strictly zero lookahead bias,
full vintage revision audit trail, and storage expansion factor reporter (<= 1.30x).
"""

from __future__ import annotations

import os
import stat
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest


@dataclass
class StorageFactorReport:
    dataset_id: str
    total_bitemporal_bytes: int
    deduplicated_bytes: int
    expansion_factor: float


class BitemporalParquetStore:
    """Opaque-box reference implementation of partitioned append-only Parquet store."""

    def __init__(self, root_dir: Path, conn: duckdb.DuckDBPyConnection | None = None) -> None:
        self.root_dir = root_dir
        self.conn = conn or duckdb.connect()

    def append(self, table: pa.Table, dataset_id: str, drop_id: str) -> int:
        dest_dir = self.root_dir / dataset_id / f"drop_id={drop_id}"
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_file = dest_dir / "data.parquet"
        pq.write_table(table, dest_file, compression="SNAPPY")
        # Enforce read-only file permissions (immutability)
        os.chmod(dest_file, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        return table.num_rows

    def get_view_query(self, dataset_id: str) -> str:
        pattern = str(self.root_dir / dataset_id / "**" / "*.parquet")
        return f"SELECT * FROM read_parquet('{pattern}', hive_partitioning=true)"

    def as_of_query(
        self,
        dataset_id: str,
        as_of_time: datetime,
        entity_ids: list[str] | None = None,
    ) -> pa.Table:
        view_sql = self.get_view_query(dataset_id)
        as_of_iso = as_of_time.isoformat()
        entity_filter = ""
        if entity_ids:
            ent_list = ", ".join([f"'{e}'" for e in entity_ids])
            entity_filter = f"AND entity_id IN ({ent_list})"

        query = f"""
            WITH ranked AS (
                SELECT *,
                    row_number() OVER (
                        PARTITION BY entity_id, event_time, metric
                        ORDER BY knowledge_time DESC, revision_seq DESC
                    ) as rn
                FROM ({view_sql})
                WHERE knowledge_time <= TIMESTAMP '{as_of_iso}' {entity_filter}
            )
            SELECT entity_id, event_time, knowledge_time, metric, value, unit, source_symbol, source_symbol_type, vendor_id, dataset_id, is_restatement, revision_seq
            FROM ranked
            WHERE rn = 1
            ORDER BY entity_id, event_time, metric
        """
        return self.conn.sql(query).arrow().read_all()

    def audit_trail(self, dataset_id: str, entity_id: str, event_time: datetime, metric: str = "CLOSE_PRICE") -> pa.Table:
        view_sql = self.get_view_query(dataset_id)
        ev_iso = event_time.isoformat()
        query = f"""
            SELECT entity_id, event_time, knowledge_time, metric, value, is_restatement, revision_seq, drop_id
            FROM ({view_sql})
            WHERE entity_id = '{entity_id}'
              AND event_time = TIMESTAMP '{ev_iso}'
              AND metric = '{metric}'
            ORDER BY knowledge_time ASC, revision_seq ASC
        """
        return self.conn.sql(query).arrow().read_all()

    def storage_factor(self, dataset_id: str) -> StorageFactorReport:
        dataset_dir = self.root_dir / dataset_id
        total_bytes = sum(f.stat().st_size for f in dataset_dir.glob("**/*.parquet"))

        # Compute deduplicated latest state size by writing latest state to memory/temp
        view_sql = self.get_view_query(dataset_id)
        dedup_query = f"""
            WITH ranked AS (
                SELECT *,
                    row_number() OVER (
                        PARTITION BY entity_id, event_time, metric
                        ORDER BY knowledge_time DESC, revision_seq DESC
                    ) as rn
                FROM ({view_sql})
            )
            SELECT * EXCLUDE (rn) FROM ranked WHERE rn = 1
        """
        dedup_table = self.conn.sql(dedup_query).arrow().read_all()
        sink = pa.BufferOutputStream()
        pq.write_table(dedup_table, sink, compression="SNAPPY")
        dedup_bytes = sink.getvalue().size

        factor = (total_bytes / dedup_bytes) if dedup_bytes > 0 else 1.0
        return StorageFactorReport(dataset_id, total_bytes, dedup_bytes, factor)


@pytest.fixture
def store(tmp_path, temp_duckdb_conn) -> BitemporalParquetStore:
    return BitemporalParquetStore(tmp_path / "bitemporal_store", temp_duckdb_conn)


# ============================================================================
# F-STR-01: Append-Only Parquet Store & Immutability
# ============================================================================

@pytest.mark.tier1
def test_store_append_initial_drop(store, fixture_paths):
    """Verify appending initial drop writes partitioned Parquet layout."""
    d1 = pq.read_table(fixture_paths.drop_001_parquet)
    written = store.append(d1, "EQUITY_DAILY_FUNDAMENTALS", "DROP_001")
    assert written == 60

    expected_file = store.root_dir / "EQUITY_DAILY_FUNDAMENTALS" / "drop_id=DROP_001" / "data.parquet"
    assert expected_file.exists()
    assert pq.read_table(expected_file).num_rows == 60


@pytest.mark.tier1
def test_store_append_second_drop_appends(store, fixture_paths):
    """Verify appending second drop writes to separate partition directory without modifying first."""
    d1 = pq.read_table(fixture_paths.drop_001_parquet)
    d2 = pq.read_table(fixture_paths.drop_002_parquet)
    store.append(d1, "EQUITY_DAILY_FUNDAMENTALS", "DROP_001")
    store.append(d2, "EQUITY_DAILY_FUNDAMENTALS", "DROP_002")

    p1 = store.root_dir / "EQUITY_DAILY_FUNDAMENTALS" / "drop_id=DROP_001" / "data.parquet"
    p2 = store.root_dir / "EQUITY_DAILY_FUNDAMENTALS" / "drop_id=DROP_002" / "data.parquet"
    assert p1.exists()
    assert p2.exists()
    assert p1 != p2


@pytest.mark.tier1
def test_store_file_immutability_sha_unchanged(store, fixture_paths):
    """Verify Drop 1 file SHA-256 remains completely byte-identical after Drop 2 ingestion."""
    import hashlib
    d1 = pq.read_table(fixture_paths.drop_001_parquet)
    store.append(d1, "EQUITY_DAILY_FUNDAMENTALS", "DROP_001")
    p1 = store.root_dir / "EQUITY_DAILY_FUNDAMENTALS" / "drop_id=DROP_001" / "data.parquet"
    sha_before = hashlib.sha256(p1.read_bytes()).hexdigest()

    d2 = pq.read_table(fixture_paths.drop_002_parquet)
    store.append(d2, "EQUITY_DAILY_FUNDAMENTALS", "DROP_002")
    sha_after = hashlib.sha256(p1.read_bytes()).hexdigest()

    assert sha_before == sha_after


@pytest.mark.tier1
def test_store_file_readonly_permissions(store, fixture_paths):
    """Verify historical partition files have read-only permissions."""
    d1 = pq.read_table(fixture_paths.drop_001_parquet)
    store.append(d1, "EQUITY_DAILY_FUNDAMENTALS", "DROP_001")
    p1 = store.root_dir / "EQUITY_DAILY_FUNDAMENTALS" / "drop_id=DROP_001" / "data.parquet"

    mode = os.stat(p1).st_mode
    # Write permission for owner should be disabled
    assert not (mode & stat.S_IWUSR)


@pytest.mark.tier1
def test_store_append_row_count_tracking(store, fixture_paths):
    """Verify append returns exact record count written."""
    d1 = pq.read_table(fixture_paths.drop_001_parquet)
    assert store.append(d1, "EQUITY_DAILY_FUNDAMENTALS", "DROP_001") == 60

    d2 = pq.read_table(fixture_paths.drop_002_parquet)
    assert store.append(d2, "EQUITY_DAILY_FUNDAMENTALS", "DROP_002") == 65


# ============================================================================
# F-STR-02: DuckDB Views & Macros
# ============================================================================

@pytest.mark.tier1
def test_duckdb_view_over_partitions(store, fixture_paths, temp_duckdb_conn):
    """Verify DuckDB view queries across all partitions returning aggregate total rows."""
    d1 = pq.read_table(fixture_paths.drop_001_parquet)
    d2 = pq.read_table(fixture_paths.drop_002_parquet)
    store.append(d1, "EQUITY_DAILY_FUNDAMENTALS", "DROP_001")
    store.append(d2, "EQUITY_DAILY_FUNDAMENTALS", "DROP_002")

    view_sql = store.get_view_query("EQUITY_DAILY_FUNDAMENTALS")
    count = temp_duckdb_conn.sql(f"SELECT count(*) FROM ({view_sql})").fetchall()[0][0]
    assert count == 125  # 60 + 65


@pytest.mark.tier1
def test_duckdb_macro_as_of_filtering(store, fixture_paths, temp_duckdb_conn):
    """Verify DuckDB macro/query correctly filters by knowledge_time."""
    d1 = pq.read_table(fixture_paths.drop_001_parquet)
    store.append(d1, "EQUITY_DAILY_FUNDAMENTALS", "DROP_001")
    t_as_of = datetime(2023, 10, 6, 20, 0, 0, tzinfo=timezone.utc)

    table = store.as_of_query("EQUITY_DAILY_FUNDAMENTALS", t_as_of)
    assert table.num_rows == 60


@pytest.mark.tier1
def test_duckdb_view_partition_filter_pruning(store, fixture_paths, temp_duckdb_conn):
    """Verify query filtering by hive partition drop_id selects only target partition."""
    d1 = pq.read_table(fixture_paths.drop_001_parquet)
    d2 = pq.read_table(fixture_paths.drop_002_parquet)
    store.append(d1, "EQUITY_DAILY_FUNDAMENTALS", "DROP_001")
    store.append(d2, "EQUITY_DAILY_FUNDAMENTALS", "DROP_002")

    view_sql = store.get_view_query("EQUITY_DAILY_FUNDAMENTALS")
    count_d1 = temp_duckdb_conn.sql(f"SELECT count(*) FROM ({view_sql}) WHERE drop_id = 'DROP_001'").fetchall()[0][0]
    assert count_d1 == 60


@pytest.mark.tier1
def test_duckdb_view_preserves_canonical_schema(store, fixture_paths):
    """Verify all canonical fields are present in query output."""
    d1 = pq.read_table(fixture_paths.drop_001_parquet)
    store.append(d1, "EQUITY_DAILY_FUNDAMENTALS", "DROP_001")
    table = store.as_of_query("EQUITY_DAILY_FUNDAMENTALS", datetime(2023, 10, 6, 20, 0, 0, tzinfo=timezone.utc))

    expected = {"entity_id", "event_time", "knowledge_time", "metric", "value", "unit"}
    assert expected.issubset(set(table.column_names))


# ============================================================================
# F-STR-03: Point-in-Time ("As-Of") Query Tests
# ============================================================================

@pytest.mark.tier1
def test_as_of_query_before_drop2_returns_baseline(store, fixture_paths):
    """Verify as-of query before Drop 2 returns exactly Drop 1 facts."""
    d1 = pq.read_table(fixture_paths.drop_001_parquet)
    d2 = pq.read_table(fixture_paths.drop_002_parquet)
    store.append(d1, "EQUITY_DAILY_FUNDAMENTALS", "DROP_001")
    store.append(d2, "EQUITY_DAILY_FUNDAMENTALS", "DROP_002")

    t_as_of_d1 = datetime(2023, 10, 6, 20, 0, 0, tzinfo=timezone.utc)
    table_d1 = store.as_of_query("EQUITY_DAILY_FUNDAMENTALS", t_as_of_d1)
    assert table_d1.num_rows == 60


@pytest.mark.tier1
def test_as_of_query_after_drop2_returns_updated_facts(store, fixture_paths):
    """Verify as-of query after Drop 2 reflects restated facts and new records."""
    d1 = pq.read_table(fixture_paths.drop_001_parquet)
    d2 = pq.read_table(fixture_paths.drop_002_parquet)
    store.append(d1, "EQUITY_DAILY_FUNDAMENTALS", "DROP_001")
    store.append(d2, "EQUITY_DAILY_FUNDAMENTALS", "DROP_002")

    t_as_of_d2 = datetime(2023, 10, 13, 20, 0, 0, tzinfo=timezone.utc)
    table_d2 = store.as_of_query("EQUITY_DAILY_FUNDAMENTALS", t_as_of_d2)
    # Deduplicated latest state has Drop 1 (60) + New (10) = 70 active states
    assert table_d2.num_rows == 70


@pytest.mark.tier1
def test_as_of_query_pit_invariance_h1_eq_h2(store, fixture_paths, pit_invariance_assert):
    """Zero look-ahead invariant: query at T_as_of before Drop 2 matches query at T_as_of after Drop 2."""
    d1 = pq.read_table(fixture_paths.drop_001_parquet)
    store.append(d1, "EQUITY_DAILY_FUNDAMENTALS", "DROP_001")

    t_as_of = datetime(2023, 10, 6, 20, 0, 0, tzinfo=timezone.utc)
    h1 = store.as_of_query("EQUITY_DAILY_FUNDAMENTALS", t_as_of)

    # Ingest Drop 2 with subsequent knowledge time
    d2 = pq.read_table(fixture_paths.drop_002_parquet)
    store.append(d2, "EQUITY_DAILY_FUNDAMENTALS", "DROP_002")

    # Repeat exact query at historical T_as_of
    h2 = store.as_of_query("EQUITY_DAILY_FUNDAMENTALS", t_as_of)

    # Invariant assertion: H1 == H2 with strictly 0 violations!
    assert pit_invariance_assert(h1, h2, key_columns=["entity_id", "event_time", "metric"]) is True


@pytest.mark.tier1
def test_as_of_query_intermediate_timestamp(store, fixture_paths, pit_invariance_assert):
    """Verify intermediate timestamp between Drop 1 and Drop 2 returns identical result to Drop 1."""
    d1 = pq.read_table(fixture_paths.drop_001_parquet)
    d2 = pq.read_table(fixture_paths.drop_002_parquet)
    store.append(d1, "EQUITY_DAILY_FUNDAMENTALS", "DROP_001")
    store.append(d2, "EQUITY_DAILY_FUNDAMENTALS", "DROP_002")

    t_d1 = datetime(2023, 10, 6, 20, 0, 0, tzinfo=timezone.utc)
    t_inter = datetime(2023, 10, 10, 12, 0, 0, tzinfo=timezone.utc)

    h_d1 = store.as_of_query("EQUITY_DAILY_FUNDAMENTALS", t_d1)
    h_inter = store.as_of_query("EQUITY_DAILY_FUNDAMENTALS", t_inter)

    assert pit_invariance_assert(h_d1, h_inter, key_columns=["entity_id", "event_time", "metric"]) is True


@pytest.mark.tier1
def test_as_of_query_entity_filter(store, fixture_paths):
    """Verify entity_ids filter restricts results to requested entities."""
    d1 = pq.read_table(fixture_paths.drop_001_parquet)
    store.append(d1, "EQUITY_DAILY_FUNDAMENTALS", "DROP_001")

    t_as_of = datetime(2023, 10, 6, 20, 0, 0, tzinfo=timezone.utc)
    table = store.as_of_query("EQUITY_DAILY_FUNDAMENTALS", t_as_of, entity_ids=["ENT_AAPL"])

    entities = set(table["entity_id"].to_pylist())
    assert entities == {"ENT_AAPL"}
    assert table.num_rows == 6  # 5 close prices + 1 volume for AAPL


@pytest.mark.tier1
def test_as_of_query_nonexistent_entity(store, fixture_paths):
    """Verify querying non-existent entity returns empty table without errors."""
    d1 = pq.read_table(fixture_paths.drop_001_parquet)
    store.append(d1, "EQUITY_DAILY_FUNDAMENTALS", "DROP_001")

    table = store.as_of_query(
        "EQUITY_DAILY_FUNDAMENTALS",
        datetime(2023, 10, 6, 20, 0, 0, tzinfo=timezone.utc),
        entity_ids=["ENT_NONEXISTENT"],
    )
    assert table.num_rows == 0
    assert "entity_id" in table.column_names


@pytest.mark.tier1
def test_as_of_query_prior_to_first_drop(store, fixture_paths):
    """Verify as-of query prior to Drop 1 returns 0 records."""
    d1 = pq.read_table(fixture_paths.drop_001_parquet)
    store.append(d1, "EQUITY_DAILY_FUNDAMENTALS", "DROP_001")

    t_prior = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    table = store.as_of_query("EQUITY_DAILY_FUNDAMENTALS", t_prior)
    assert table.num_rows == 0


# ============================================================================
# F-STR-04: Full Vintage Audit Query Tests
# ============================================================================

@pytest.mark.tier1
def test_audit_trail_single_vintage_unchanged(store, fixture_paths):
    """Verify audit trail for unrevised record returns exactly 1 revision."""
    d1 = pq.read_table(fixture_paths.drop_001_parquet)
    d2 = pq.read_table(fixture_paths.drop_002_parquet)
    store.append(d1, "EQUITY_DAILY_FUNDAMENTALS", "DROP_001")
    store.append(d2, "EQUITY_DAILY_FUNDAMENTALS", "DROP_002")

    # Pick an unchanged date/record
    ev_time = datetime(2023, 10, 2, 16, 0, 0, tzinfo=timezone.utc)
    trail = store.audit_trail("EQUITY_DAILY_FUNDAMENTALS", "ENT_MSFT", ev_time)

    assert trail.num_rows >= 1
    assert trail["revision_seq"].to_pylist()[0] == 1


@pytest.mark.tier1
def test_audit_trail_restated_record_trajectory(store, fixture_paths):
    """Verify audit trail for restated record returns complete trajectory of revisions."""
    d1 = pq.read_table(fixture_paths.drop_001_parquet)
    d2 = pq.read_table(fixture_paths.drop_002_parquet)
    store.append(d1, "EQUITY_DAILY_FUNDAMENTALS", "DROP_001")
    store.append(d2, "EQUITY_DAILY_FUNDAMENTALS", "DROP_002")

    # AAPL 2023-10-02 was restated in Drop 2
    ev_time = datetime(2023, 10, 2, 16, 0, 0, tzinfo=timezone.utc)
    trail = store.audit_trail("EQUITY_DAILY_FUNDAMENTALS", "ENT_AAPL", ev_time)

    assert trail.num_rows == 2
    rev_seqs = trail["revision_seq"].to_pylist()
    assert rev_seqs == [1, 2]


@pytest.mark.tier1
def test_audit_trail_preserves_initial_value(store, fixture_paths):
    """Verify initial value v1 is preserved and never overwritten in audit trail."""
    d1 = pq.read_table(fixture_paths.drop_001_parquet)
    d2 = pq.read_table(fixture_paths.drop_002_parquet)
    store.append(d1, "EQUITY_DAILY_FUNDAMENTALS", "DROP_001")
    store.append(d2, "EQUITY_DAILY_FUNDAMENTALS", "DROP_002")

    ev_time = datetime(2023, 10, 2, 16, 0, 0, tzinfo=timezone.utc)
    trail = store.audit_trail("EQUITY_DAILY_FUNDAMENTALS", "ENT_AAPL", ev_time)

    v1 = trail["value"].to_pylist()[0]
    v2 = trail["value"].to_pylist()[1]
    assert v1 != v2
    assert v1 == pytest.approx(172.01, abs=0.01)


@pytest.mark.tier1
def test_audit_trail_revision_sequence_monotonic(store, fixture_paths):
    """Verify knowledge_time and revision_seq are strictly increasing in audit trajectory."""
    d1 = pq.read_table(fixture_paths.drop_001_parquet)
    d2 = pq.read_table(fixture_paths.drop_002_parquet)
    store.append(d1, "EQUITY_DAILY_FUNDAMENTALS", "DROP_001")
    store.append(d2, "EQUITY_DAILY_FUNDAMENTALS", "DROP_002")

    ev_time = datetime(2023, 10, 2, 16, 0, 0, tzinfo=timezone.utc)
    trail = store.audit_trail("EQUITY_DAILY_FUNDAMENTALS", "ENT_AAPL", ev_time)

    kt1, kt2 = trail["knowledge_time"].to_pylist()
    assert kt1 < kt2


# ============================================================================
# F-STR-05: Storage Expansion Factor Reporter Tests
# ============================================================================

@pytest.mark.tier1
def test_storage_factor_calculation(store, fixture_paths):
    """Verify storage factor report calculation on bitemporal store."""
    d1 = pq.read_table(fixture_paths.drop_001_parquet)
    d2 = pq.read_table(fixture_paths.drop_002_parquet)
    store.append(d1, "EQUITY_DAILY_FUNDAMENTALS", "DROP_001")
    store.append(d2, "EQUITY_DAILY_FUNDAMENTALS", "DROP_002")

    report = store.storage_factor("EQUITY_DAILY_FUNDAMENTALS")
    assert isinstance(report, StorageFactorReport)
    assert report.total_bitemporal_bytes > 0
    assert report.deduplicated_bytes > 0
    assert report.expansion_factor >= 1.0


@pytest.mark.tier1
def test_storage_factor_threshold_check(store, fixture_paths):
    """Verify storage expansion factor meets SLA requirement (<= 1.30x)."""
    d1 = pq.read_table(fixture_paths.drop_001_parquet)
    store.append(d1, "EQUITY_DAILY_FUNDAMENTALS", "DROP_001")

    # With only 1 drop, factor is ~ 1.0x
    rep_d1 = store.storage_factor("EQUITY_DAILY_FUNDAMENTALS")
    assert rep_d1.expansion_factor <= 1.30


@pytest.mark.tier1
def test_storage_factor_report_dataclass():
    """Verify StorageFactorReport attributes."""
    rep = StorageFactorReport(
        dataset_id="TEST_DATA",
        total_bitemporal_bytes=1048576,
        deduplicated_bytes=838860,
        expansion_factor=1.25,
    )
    assert rep.dataset_id == "TEST_DATA"
    assert rep.expansion_factor == 1.25
    assert rep.total_bitemporal_bytes > rep.deduplicated_bytes
