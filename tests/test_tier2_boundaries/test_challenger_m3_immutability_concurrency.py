"""
Tier 2 Boundary Tests: Immutability, Concurrency, and Storage Expansion Stress.

Challenger 1 suite for Milestone 3 (Append-Only Bitemporal Store & Query):
1. Immutability attack:
   - Attempt to overwrite an existing partition drop via append().
   - Assert ImmutabilityError is raised.
   - Assert existing Parquet file content and SHA-256 hash are bit-for-bit unchanged.
   - Assert file modification time (mtime) and size are unchanged.
2. Permission enforcement:
   - Verify write operations directly to partition file raise PermissionError (0o444).
   - Verify direct tampering (open wb, open a, truncate, pq.write_table) fails.
3. Multi-threaded / Concurrent reads:
   - Execute concurrent as_of_query calls across multiple threads on separate DuckDB connections.
   - Verify no file locking issues or deadlocks.
   - Verify concurrent reads while writer is appending disjoint drops.
4. Storage expansion factor:
   - Test datasets with heavy restatements vs zero/low restatements.
   - Verify meets_sla behaves correctly (<= 1.30 threshold).
   - Boundary tests for empty datasets and single-drop datasets.
5. Empty dataset / 0-row table append boundary tests:
   - Non-existent dataset queries return canonical empty schema.
   - 0-row table append with drop_id succeeds and queries cleanly.
   - 0-row table append without drop_id raises ValueError.
   - Empty sequence [] append boundaries.
6. Microsecond timestamp precision & DST transitions:
   - Microsecond delta between revisions.
   - Daylight saving time transitions with fold=0 and fold=1.
7. High-revision audit trail & Large scale append:
   - 50 sequential revisions monotonic ordering.
   - Large table append (25,000+ rows) performance and correctness.
8. Corrupt partition handling & Connection lifecycle:
   - Corrupted parquet file handling.
   - DuckDB connection lifecycle and resource cleanup.
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import stat
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from vendor_onboarding.models.record import CanonicalRecord
from vendor_onboarding.storage.query import CANONICAL_AS_OF_SCHEMA, CANONICAL_AUDIT_SCHEMA
from vendor_onboarding.storage.store import BitemporalStore, ImmutabilityError, StorageFactorReport

pytestmark = [pytest.mark.tier2]


def _sha256(path: Path) -> str:
    """Computes SHA-256 checksum of a file."""
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def _make_sample_table(
    entity_ids: list[str] | None = None,
    event_time: datetime | None = None,
    knowledge_time: datetime | None = None,
    drop_id: str = "DROP_001",
    dataset_id: str = "TEST_DATASET",
    revision_seq: int = 1,
    is_restatement: bool = False,
    value: float = 100.0,
) -> pa.Table:
    """Helper to construct a valid CanonicalRecord-compatible PyArrow table."""
    entities = entity_ids or ["ENT_AAPL"]
    n = len(entities)
    ev = event_time or datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
    kt = knowledge_time or datetime(2024, 1, 1, 14, 0, 0, tzinfo=UTC)

    return pa.Table.from_pydict({
        "entity_id": entities,
        "event_time": [ev] * n,
        "knowledge_time": [kt] * n,
        "metric": ["CLOSE_PRICE"] * n,
        "value": [value] * n,
        "unit": ["USD"] * n,
        "source_symbol": entities,
        "source_symbol_type": ["TICKER"] * n,
        "vendor_id": ["VENDOR_TEST"] * n,
        "dataset_id": [dataset_id] * n,
        "is_restatement": [is_restatement] * n,
        "revision_seq": [revision_seq] * n,
        "drop_id": [drop_id] * n,
    })


# ============================================================================
# Scenario 1: Immutability Attack Prevention
# ============================================================================


def test_immutability_attack_overwrite_prevention(tmp_path: Path) -> None:
    """Attempting to overwrite an existing partition drop via append() must raise ImmutabilityError.

    Verifies that the target file content and SHA-256 hash remain completely unchanged.
    """
    store = BitemporalStore(root_dir=tmp_path)
    tbl_initial = _make_sample_table(entity_ids=["ENT_AAPL"], drop_id="DROP_001", value=150.0)

    # Initial valid append
    rows_written = store.append(tbl_initial, dataset_id="EQUITY_PRICES", drop_id="DROP_001")
    assert rows_written == 1

    parquet_file = tmp_path / "EQUITY_PRICES" / "drop_id=DROP_001" / "data.parquet"
    assert parquet_file.exists()

    initial_sha = _sha256(parquet_file)
    initial_stat = parquet_file.stat()

    # Attack 1: Re-append identical table
    with pytest.raises(ImmutabilityError, match="Partition already exists and is immutable"):
        store.append(tbl_initial, dataset_id="EQUITY_PRICES", drop_id="DROP_001")

    # Attack 2: Append malicious table with mutated values
    tbl_malicious = _make_sample_table(
        entity_ids=["ENT_AAPL"], drop_id="DROP_001", value=999999.99
    )
    with pytest.raises(ImmutabilityError):
        store.append(tbl_malicious, dataset_id="EQUITY_PRICES", drop_id="DROP_001")

    # Attack 3: Append using CanonicalRecord list with inferred drop_id
    rec = CanonicalRecord(
        entity_id="ENT_AAPL",
        event_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
        knowledge_time=datetime(2024, 1, 1, 14, 0, 0, tzinfo=UTC),
        value=0.0,
        unit="USD",
        source_symbol="ENT_AAPL",
        source_symbol_type="TICKER",
        vendor_id="VENDOR_TEST",
        dataset_id="EQUITY_PRICES",
        is_restatement=True,
        revision_seq=99,
        drop_id="DROP_001",
    )
    with pytest.raises(ImmutabilityError):
        store.append([rec], dataset_id="EQUITY_PRICES")

    # Verify byte-for-byte integrity and metadata preservation
    assert _sha256(parquet_file) == initial_sha
    final_stat = parquet_file.stat()
    assert final_stat.st_size == initial_stat.st_size
    assert final_stat.st_mtime == initial_stat.st_mtime

    # Verify query still returns authentic uncorrupted value
    pit = store.as_of_query("EQUITY_PRICES", as_of_time=datetime(2024, 1, 2, tzinfo=UTC))
    assert pit["value"][0].as_py() == pytest.approx(150.0)


# ============================================================================
# Scenario 2: Physical File Permission Enforcement & Tampering Defense
# ============================================================================


def test_permission_enforcement_and_tampering_defense(tmp_path: Path) -> None:
    """Verifies partition files are marked read-only (0o444) and direct write attempts fail."""
    store = BitemporalStore(root_dir=tmp_path)
    tbl = _make_sample_table(drop_id="DROP_001")
    store.append(tbl, dataset_id="EQUITY_PRICES", drop_id="DROP_001")

    parquet_file = tmp_path / "EQUITY_PRICES" / "drop_id=DROP_001" / "data.parquet"
    mode = stat.S_IMODE(parquet_file.stat().st_mode)

    # Must be 0o444 (read-only for user, group, other)
    assert mode == 0o444
    assert not (mode & stat.S_IWUSR)
    assert not (mode & stat.S_IWGRP)
    assert not (mode & stat.S_IWOTH)

    # Tampering attempt 1: open with 'wb'
    with pytest.raises(PermissionError):
        with open(parquet_file, "wb") as f:
            f.write(b"tampered content")

    # Tampering attempt 2: open with append mode 'a'
    with pytest.raises(PermissionError):
        with open(parquet_file, "a") as f:
            f.write("tampered")

    # Tampering attempt 3: open with update mode 'r+b'
    with pytest.raises(PermissionError):
        with open(parquet_file, "r+b") as f:
            f.write(b"0")

    # Tampering attempt 4: PyArrow write_table overwrite
    with pytest.raises((PermissionError, OSError)):
        pq.write_table(tbl, parquet_file)


# ============================================================================
# Scenario 3: Multi-threaded / Concurrent Reads
# ============================================================================


def test_multithreaded_concurrent_reads_shared_connection(tmp_path: Path) -> None:
    """Execute concurrent as_of_query calls across multiple threads sharing store connection."""
    store = BitemporalStore(root_dir=tmp_path)

    # Seed with 3 historical drops
    for d in range(1, 4):
        tbl = _make_sample_table(
            entity_ids=["AAPL", "MSFT", "GOOG", "AMZN", "META"],
            event_time=datetime(2024, 1, d, 12, tzinfo=UTC),
            knowledge_time=datetime(2024, 1, d, 14, tzinfo=UTC),
            drop_id=f"DROP_{d:03d}",
            value=100.0 * d,
        )
        store.append(tbl, dataset_id="MARKET_DATA", drop_id=f"DROP_{d:03d}")

    # Concurrently query from 10 threads
    def worker(query_idx: int) -> int:
        as_of = datetime(2024, 1, 2, 15, tzinfo=UTC)
        # As of 2024-01-02 15:00, drops 1 and 2 are visible; 2 entities across 2 event dates = 4 rows
        res = store.as_of_query("MARKET_DATA", as_of_time=as_of, entity_ids=["AAPL", "MSFT"])
        assert res.num_rows == 4
        return res.num_rows

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(worker, i) for i in range(50)]
        results = [f.result() for f in futures]

    assert len(results) == 50
    assert all(r == 4 for r in results)


def test_multithreaded_concurrent_reads_isolated_connections(tmp_path: Path) -> None:
    """Execute concurrent as_of_query calls with independent DuckDB connections per thread."""
    store = BitemporalStore(root_dir=tmp_path)

    for d in range(1, 4):
        tbl = _make_sample_table(
            entity_ids=[f"SYM_{i:02d}" for i in range(20)],
            event_time=datetime(2024, 1, d, 12, tzinfo=UTC),
            knowledge_time=datetime(2024, 1, d, 14, tzinfo=UTC),
            drop_id=f"DROP_{d:03d}",
            value=50.0 + d,
        )
        store.append(tbl, dataset_id="ISOLATED_CONNS", drop_id=f"DROP_{d:03d}")

    def worker(worker_id: int) -> int:
        # Separate engine and connection per thread
        with BitemporalStore(root_dir=tmp_path) as engine:
            res = engine.as_of_query(
                "ISOLATED_CONNS",
                as_of_time=datetime(2024, 1, 2, 23, tzinfo=UTC),
                metric="CLOSE_PRICE",
            )
            return res.num_rows

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(worker, i) for i in range(40)]
        results = [f.result() for f in futures]

    assert len(results) == 40
    # At 2024-01-02 23:00, drops 1 and 2 are visible; 20 entities * 2 event dates => 40 rows
    assert all(r == 40 for r in results)


def test_concurrent_readers_during_disjoint_writer(tmp_path: Path) -> None:
    """Readers querying historical snapshots must not deadlock or observe corrupted data while a writer appends."""
    store = BitemporalStore(root_dir=tmp_path)

    # Initial drop 1
    tbl1 = _make_sample_table(
        entity_ids=["AAPL", "NVDA"],
        event_time=datetime(2024, 1, 1, 12, tzinfo=UTC),
        knowledge_time=datetime(2024, 1, 1, 14, tzinfo=UTC),
        drop_id="DROP_001",
        value=100.0,
    )
    store.append(tbl1, dataset_id="CONCURRENT_RW", drop_id="DROP_001")

    errors: list[Exception] = []
    read_results: list[int] = []

    def reader_task() -> None:
        for _ in range(25):
            try:
                with BitemporalStore(root_dir=tmp_path) as thread_store:
                    # Query strictly as of drop 1's knowledge horizon
                    res = thread_store.as_of_query(
                        "CONCURRENT_RW", as_of_time=datetime(2024, 1, 1, 15, tzinfo=UTC)
                    )
                    read_results.append(res.num_rows)
            except Exception as e:  # noqa: BLE001
                errors.append(e)
            time.sleep(0.005)

    def writer_task() -> None:
        for d in range(2, 6):
            time.sleep(0.015)
            tbl = _make_sample_table(
                entity_ids=["AAPL", "NVDA"],
                event_time=datetime(2024, 1, d, 12, tzinfo=UTC),
                knowledge_time=datetime(2024, 1, d, 14, tzinfo=UTC),
                drop_id=f"DROP_{d:03d}",
                value=100.0 * d,
            )
            try:
                store.append(tbl, dataset_id="CONCURRENT_RW", drop_id=f"DROP_{d:03d}")
            except Exception as e:  # noqa: BLE001
                errors.append(e)

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        reader_futures = [executor.submit(reader_task) for _ in range(4)]
        writer_future = executor.submit(writer_task)
        writer_future.result()
        for rf in reader_futures:
            rf.result()

    assert not errors, f"Concurrent read-write produced errors: {errors}"
    # Every historical query should have seen exactly 2 rows
    assert all(r == 2 for r in read_results)

    # After writer completes, querying at drop 5 horizon sees all 5 event dates (10 total facts)
    final_query = store.as_of_query(
        "CONCURRENT_RW", as_of_time=datetime(2024, 1, 10, tzinfo=UTC)
    )
    assert final_query.num_rows == 10


# ============================================================================
# Scenario 4: Storage Expansion Factor SLA Tests
# ============================================================================


def test_storage_expansion_factor_meets_sla_low_restatements(tmp_path: Path) -> None:
    """Datasets with zero or minimal restatements satisfy the storage factor SLA (<= 1.30x)."""
    store = BitemporalStore(root_dir=tmp_path)

    # Single drop dataset: factor is ~1.0x
    n = 1000
    dates = [datetime(2024, 1, 1, tzinfo=UTC) + timedelta(days=j) for j in range(n)]
    tbl1 = pa.Table.from_pydict({
        "entity_id": ["AAPL"] * n,
        "event_time": dates,
        "knowledge_time": [datetime(2024, 1, 1, tzinfo=UTC)] * n,
        "metric": ["CLOSE_PRICE"] * n,
        "value": [150.0 + j for j in range(n)],
        "unit": ["USD"] * n,
        "source_symbol": ["AAPL"] * n,
        "source_symbol_type": ["TICKER"] * n,
        "vendor_id": ["BLOOMBERG"] * n,
        "dataset_id": ["SLA_PASS"] * n,
        "is_restatement": [False] * n,
        "revision_seq": [1] * n,
        "drop_id": ["drop_001"] * n,
    })
    store.append(tbl1, dataset_id="SLA_PASS", drop_id="drop_001")

    rep1 = store.storage_factor("SLA_PASS")
    assert isinstance(rep1, StorageFactorReport)
    assert rep1.meets_sla
    assert 0.95 <= rep1.expansion_factor <= 1.05

    # Append second drop with minimal restatements (5% overlap, 95% new events)
    dates2 = dates[:50] + [
        datetime(2024, 6, 1, tzinfo=UTC) + timedelta(days=j) for j in range(50)
    ]
    tbl2 = pa.Table.from_pydict({
        "entity_id": ["AAPL"] * 100,
        "event_time": dates2,
        "knowledge_time": [datetime(2024, 1, 2, tzinfo=UTC)] * 100,
        "metric": ["CLOSE_PRICE"] * 100,
        "value": [155.0 + j for j in range(100)],
        "unit": ["USD"] * 100,
        "source_symbol": ["AAPL"] * 100,
        "source_symbol_type": ["TICKER"] * 100,
        "vendor_id": ["BLOOMBERG"] * 100,
        "dataset_id": ["SLA_PASS"] * 100,
        "is_restatement": [True] * 50 + [False] * 50,
        "revision_seq": [2] * 50 + [1] * 50,
        "drop_id": ["drop_002"] * 100,
    })
    store.append(tbl2, dataset_id="SLA_PASS", drop_id="drop_002")

    rep2 = store.storage_factor("SLA_PASS")
    assert rep2.meets_sla
    assert rep2.expansion_factor <= 1.30


def test_storage_expansion_factor_fails_sla_heavy_restatements(tmp_path: Path) -> None:
    """Datasets with repeated heavy restatements exceed SLA threshold (> 1.30x) and meets_sla is False."""
    store = BitemporalStore(root_dir=tmp_path)

    # 4 drops repeatedly restating the exact same 1,000 entities and event times
    n = 1000
    dates = [datetime(2024, 1, 1, tzinfo=UTC) + timedelta(days=j) for j in range(n)]

    for drop_i in range(4):
        tbl = pa.Table.from_pydict({
            "entity_id": ["AAPL"] * n,
            "event_time": dates,
            "knowledge_time": [datetime(2024, 1, 1 + drop_i, tzinfo=UTC)] * n,
            "metric": ["CLOSE_PRICE"] * n,
            "value": [150.0 + drop_i * 10.0 for _ in range(n)],
            "unit": ["USD"] * n,
            "source_symbol": ["AAPL"] * n,
            "source_symbol_type": ["TICKER"] * n,
            "vendor_id": ["BLOOMBERG"] * n,
            "dataset_id": ["SLA_FAIL"] * n,
            "is_restatement": [drop_i > 0] * n,
            "revision_seq": [drop_i + 1] * n,
            "drop_id": [f"drop_{drop_i}"] * n,
        })
        store.append(tbl, dataset_id="SLA_FAIL", drop_id=f"drop_{drop_i}")

    rep = store.storage_factor("SLA_FAIL")
    assert isinstance(rep, StorageFactorReport)
    assert not rep.meets_sla
    assert rep.expansion_factor > 2.0
    assert rep.total_bitemporal_bytes > rep.deduplicated_bytes


def test_storage_factor_boundary_empty_and_missing_datasets(tmp_path: Path) -> None:
    """Non-existent or empty datasets report factor=1.0 and meets_sla=True without error."""
    store = BitemporalStore(root_dir=tmp_path)

    # 1. Non-existent dataset directory
    rep_missing = store.storage_factor("MISSING_DATASET")
    assert rep_missing.dataset_id == "MISSING_DATASET"
    assert rep_missing.total_bitemporal_bytes == 0
    assert rep_missing.deduplicated_bytes == 0
    assert rep_missing.expansion_factor == 1.0
    assert rep_missing.meets_sla

    # 2. Existing directory with no parquet files
    (tmp_path / "EMPTY_DIR").mkdir(parents=True)
    rep_empty = store.storage_factor("EMPTY_DIR")
    assert rep_empty.total_bitemporal_bytes == 0
    assert rep_empty.deduplicated_bytes == 0
    assert rep_empty.expansion_factor == 1.0
    assert rep_empty.meets_sla


# ============================================================================
# Scenario 5: Empty Dataset & 0-row Table Append Boundaries
# ============================================================================


def test_empty_dataset_query_boundaries(tmp_path: Path) -> None:
    """Querying non-existent or empty datasets returns empty PyArrow tables with canonical schema."""
    store = BitemporalStore(root_dir=tmp_path)
    now = datetime.now(UTC)

    # 1. Non-existent dataset as_of_query
    q_as_of = store.as_of_query("NON_EXISTENT", as_of_time=now)
    assert q_as_of.num_rows == 0
    assert q_as_of.schema == CANONICAL_AS_OF_SCHEMA

    # 2. Non-existent dataset audit_trail
    q_audit = store.audit_trail("NON_EXISTENT", entity_id="AAPL", event_time=now)
    assert q_audit.num_rows == 0
    assert q_audit.schema == CANONICAL_AUDIT_SCHEMA

    # 3. Existing empty directory
    (tmp_path / "EMPTY_DS").mkdir(parents=True)
    q_empty = store.as_of_query("EMPTY_DS", as_of_time=now)
    assert q_empty.num_rows == 0
    assert q_empty.schema == CANONICAL_AS_OF_SCHEMA


def test_zero_row_table_append_and_query(tmp_path: Path) -> None:
    """Appending a 0-row PyArrow table or empty sequence with explicit drop_id succeeds and queries safely."""
    store = BitemporalStore(root_dir=tmp_path)

    empty_table = pa.Table.from_pydict({
        "entity_id": pa.array([], type=pa.string()),
        "event_time": pa.array([], type=pa.timestamp("us", tz="UTC")),
        "knowledge_time": pa.array([], type=pa.timestamp("us", tz="UTC")),
        "metric": pa.array([], type=pa.string()),
        "value": pa.array([], type=pa.float64()),
        "unit": pa.array([], type=pa.string()),
        "source_symbol": pa.array([], type=pa.string()),
        "source_symbol_type": pa.array([], type=pa.string()),
        "vendor_id": pa.array([], type=pa.string()),
        "dataset_id": pa.array([], type=pa.string()),
        "is_restatement": pa.array([], type=pa.bool_()),
        "revision_seq": pa.array([], type=pa.int64()),
        "drop_id": pa.array([], type=pa.string()),
    })

    # Append 0-row table with drop_id
    rows = store.append(empty_table, dataset_id="ZERO_ROWS_DS", drop_id="DROP_ZERO")
    assert rows == 0

    # Querying the 0-row dataset must return 0 rows without DuckDB error
    q_res = store.as_of_query("ZERO_ROWS_DS", as_of_time=datetime.now(UTC))
    assert q_res.num_rows == 0

    # Audit trail on 0-row dataset
    a_res = store.audit_trail("ZERO_ROWS_DS", entity_id="AAPL", event_time=datetime.now(UTC))
    assert a_res.num_rows == 0

    # Storage factor on 0-row dataset handles cleanly
    sf = store.storage_factor("ZERO_ROWS_DS")
    assert sf.total_bitemporal_bytes > 0
    assert sf.deduplicated_bytes > 0
    assert sf.meets_sla

    # Empty sequence append with drop_id succeeds
    rows_empty_seq = store.append([], dataset_id="EMPTY_SEQ_DS", drop_id="DROP_EMPTY")
    assert rows_empty_seq == 0

    # Appending 0 rows WITHOUT drop_id must raise ValueError
    with pytest.raises(ValueError, match="Cannot determine drop_id"):
        store.append(empty_table, dataset_id="ZERO_ROWS_DS")

    with pytest.raises(ValueError, match="Cannot determine drop_id"):
        store.append([], dataset_id="ZERO_ROWS_DS")


# ============================================================================
# Scenario 6: Microsecond Timestamp Precision & Zero Look-ahead Bias
# ============================================================================


def test_microsecond_timestamp_resolution(tmp_path: Path) -> None:
    """Verifies that point-in-time filtering operates strictly at microsecond precision."""
    store = BitemporalStore(root_dir=tmp_path)

    t_event = datetime(2024, 1, 1, 9, 30, 0, 0, tzinfo=UTC)
    t_k1 = datetime(2024, 1, 1, 10, 0, 0, 100, tzinfo=UTC)  # 100 us
    t_k2 = datetime(2024, 1, 1, 10, 0, 0, 101, tzinfo=UTC)  # 101 us (+1 us)

    tbl1 = _make_sample_table(
        entity_ids=["AAPL"],
        event_time=t_event,
        knowledge_time=t_k1,
        drop_id="DROP_001",
        revision_seq=1,
        value=150.0,
    )
    store.append(tbl1, "MICROS", "DROP_001")

    tbl2 = _make_sample_table(
        entity_ids=["AAPL"],
        event_time=t_event,
        knowledge_time=t_k2,
        drop_id="DROP_002",
        revision_seq=2,
        is_restatement=True,
        value=155.0,
    )
    store.append(tbl2, "MICROS", "DROP_002")

    # 1. Query at t_k1 - 1 us -> strictly 0 rows (zero look-ahead bias)
    q_prior = store.as_of_query(
        "MICROS", as_of_time=datetime(2024, 1, 1, 10, 0, 0, 99, tzinfo=UTC)
    )
    assert q_prior.num_rows == 0

    # 2. Query exactly at t_k1 (100 us) -> returns revision 1 (150.0)
    q_k1 = store.as_of_query("MICROS", as_of_time=t_k1)
    assert q_k1.num_rows == 1
    assert q_k1["value"][0].as_py() == pytest.approx(150.0)
    assert q_k1["revision_seq"][0].as_py() == 1

    # 3. Query exactly at t_k2 (101 us) -> returns revision 2 (155.0)
    q_k2 = store.as_of_query("MICROS", as_of_time=t_k2)
    assert q_k2.num_rows == 1
    assert q_k2["value"][0].as_py() == pytest.approx(155.0)
    assert q_k2["revision_seq"][0].as_py() == 2


# ============================================================================
# Scenario 7: Timezone and DST Transitions
# ============================================================================


def test_dst_transition_point_in_time_query(tmp_path: Path) -> None:
    """Verifies that datetimes across Daylight Saving Time boundaries normalize to UTC without ambiguity."""
    store = BitemporalStore(root_dir=tmp_path)
    ny_tz = ZoneInfo("America/New_York")

    # 2024-11-03 US Eastern clock fallback: 01:30 EDT (fold=0, UTC 05:30) and 01:30 EST (fold=1, UTC 06:30)
    t_edt = datetime(2024, 11, 3, 1, 30, 0, fold=0, tzinfo=ny_tz)
    t_est = datetime(2024, 11, 3, 1, 30, 0, fold=1, tzinfo=ny_tz)

    tbl1 = _make_sample_table(
        entity_ids=["AAPL"],
        event_time=datetime(2024, 11, 2, 16, tzinfo=UTC),
        knowledge_time=t_edt.astimezone(UTC),
        drop_id="DROP_EDT",
        revision_seq=1,
        value=200.0,
    )
    store.append(tbl1, "DST_DS", "DROP_EDT")

    tbl2 = _make_sample_table(
        entity_ids=["AAPL"],
        event_time=datetime(2024, 11, 2, 16, tzinfo=UTC),
        knowledge_time=t_est.astimezone(UTC),
        drop_id="DROP_EST",
        revision_seq=2,
        is_restatement=True,
        value=205.0,
    )
    store.append(tbl2, "DST_DS", "DROP_EST")

    # Query with non-UTC datetime at fold=0 (EDT)
    res_edt = store.as_of_query("DST_DS", as_of_time=t_edt)
    assert res_edt.num_rows == 1
    assert res_edt["value"][0].as_py() == pytest.approx(200.0)

    # Query midway between EDT and EST (06:00 UTC)
    res_mid = store.as_of_query("DST_DS", as_of_time=datetime(2024, 11, 3, 6, 0, tzinfo=UTC))
    assert res_mid.num_rows == 1
    assert res_mid["value"][0].as_py() == pytest.approx(200.0)

    # Query with non-UTC datetime at fold=1 (EST)
    res_est = store.as_of_query("DST_DS", as_of_time=t_est)
    assert res_est.num_rows == 1
    assert res_est["value"][0].as_py() == pytest.approx(205.0)


# ============================================================================
# Scenario 8: Deep Restatement Trajectory & Monotonicity
# ============================================================================


def test_deep_restatement_trajectory_monotonicity(tmp_path: Path) -> None:
    """Verifies audit trail for an entity with 50 consecutive revisions is strictly monotonic."""
    store = BitemporalStore(root_dir=tmp_path)
    ev_time = datetime(2024, 1, 1, 12, tzinfo=UTC)
    base_kt = datetime(2024, 1, 1, 13, tzinfo=UTC)

    # Append 50 drops, each restating the fact
    for i in range(1, 51):
        tbl = _make_sample_table(
            entity_ids=["AAPL"],
            event_time=ev_time,
            knowledge_time=base_kt + timedelta(hours=i),
            drop_id=f"DROP_{i:03d}",
            revision_seq=i,
            is_restatement=(i > 1),
            value=100.0 + i,
        )
        store.append(tbl, "DEEP_RESTATE", f"DROP_{i:03d}")

    # Audit trail must retrieve all 50 in strict order
    trail = store.audit_trail("DEEP_RESTATE", entity_id="AAPL", event_time=ev_time)
    assert trail.num_rows == 50

    seqs = trail["revision_seq"].to_pylist()
    assert seqs == list(range(1, 51))

    kts = trail["knowledge_time"].to_pylist()
    assert all(kts[j] < kts[j + 1] for j in range(len(kts) - 1))

    vals = trail["value"].to_pylist()
    assert vals[0] == pytest.approx(101.0)
    assert vals[-1] == pytest.approx(150.0)

    # Intermediate PIT query at revision 30
    q30 = store.as_of_query("DEEP_RESTATE", as_of_time=base_kt + timedelta(hours=30))
    assert q30.num_rows == 1
    assert q30["revision_seq"][0].as_py() == 30
    assert q30["value"][0].as_py() == pytest.approx(130.0)


# ============================================================================
# Scenario 9: Large Scale Append & Filter Performance
# ============================================================================


def test_large_scale_table_append_and_filter(tmp_path: Path) -> None:
    """Verifies appending and querying 25,000 records in a single partition drop."""
    store = BitemporalStore(root_dir=tmp_path)
    n = 25000
    dates = [datetime(2024, 1, 1, tzinfo=UTC) + timedelta(minutes=j % 1440) for j in range(n)]
    entities = [f"ENT_{j % 250:04d}" for j in range(n)]

    tbl = pa.Table.from_pydict({
        "entity_id": entities,
        "event_time": dates,
        "knowledge_time": [datetime(2024, 1, 2, tzinfo=UTC)] * n,
        "metric": ["PRICE"] * n,
        "value": [float(j) for j in range(n)],
        "unit": ["USD"] * n,
        "source_symbol": entities,
        "source_symbol_type": ["TICKER"] * n,
        "vendor_id": ["VENDOR"] * n,
        "dataset_id": ["SCALE_DS"] * n,
        "is_restatement": [False] * n,
        "revision_seq": [1] * n,
        "drop_id": ["drop_large"] * n,
    })

    t0 = time.perf_counter()
    rows = store.append(tbl, dataset_id="SCALE_DS", drop_id="drop_large")
    t_append = time.perf_counter() - t0

    assert rows == n
    assert t_append < 2.0  # Parquet write SLA

    # Filtered query
    t_q0 = time.perf_counter()
    res = store.as_of_query(
        "SCALE_DS",
        as_of_time=datetime(2024, 1, 3, tzinfo=UTC),
        entity_ids=["ENT_0001", "ENT_0042"],
    )
    t_query = time.perf_counter() - t_q0

    assert res.num_rows == 200  # 2 entities * 100 occurrences each
    assert t_query < 1.0


# ============================================================================
# Scenario 10: Corrupt Partition Handling & Connection Lifecycle
# ============================================================================


def test_corrupt_partition_handling(tmp_path: Path) -> None:
    """Verifies corrupt partition files in the store raise proper exceptions without silent crashes."""
    store = BitemporalStore(root_dir=tmp_path)

    bad_partition = tmp_path / "CORRUPT_DATASET" / "drop_id=DROP_BAD"
    bad_partition.mkdir(parents=True)
    (bad_partition / "data.parquet").write_bytes(b"INVALID_TRUNCATED_PARQUET_HEADER")

    with pytest.raises(duckdb.Error):
        store.as_of_query("CORRUPT_DATASET", as_of_time=datetime.now(UTC))


def test_duckdb_connection_lifecycle_and_leak_prevention(tmp_path: Path) -> None:
    """Verifies DuckDB connection ownership, context manager cleanup, and lack of leaks."""
    # Internal connection ownership
    with BitemporalStore(root_dir=tmp_path) as s:
        assert s._owns_conn
        conn_ref = s.conn

    # After exit, internal connection should be closed
    with pytest.raises(duckdb.Error):
        conn_ref.execute("SELECT 1")

    # External connection ownership
    external_conn = duckdb.connect()
    with BitemporalStore(root_dir=tmp_path, conn=external_conn) as s_ext:
        assert not s_ext._owns_conn

    # External connection should remain open after store exit
    res = external_conn.execute("SELECT 1").fetchall()
    assert res == [(1,)]
    external_conn.close()
