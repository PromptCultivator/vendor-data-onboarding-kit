"""Tier 5 Adversarial Coverage Hardening Suite: Storage, Revision, Reporting, API & Flight.

Comprehensive empirical stress testing, boundary fuzzing, race-hazard probing, and mathematical invariant verification for:
1. Bitemporal Store & DuckDB Query Engine:
   - Concurrent read/write stress: multiple threads appending partitions while simultaneous as-of queries execute.
   - Strict point-in-time (H_1 == H_2) invariance across 6 sequential drops with interleaved historical restatements.
   - DuckDB connection lifecycle: stress-test 120+ connection initializations and teardowns checking for file descriptor leaks and lock conflicts.
   - Non-standard event times (1970 epoch boundary, pre-epoch, leap days, far future 2050/2099) and as-of queries at exact event microsecond boundaries.
2. Diff Engine & Revision Ledger:
   - Cyclic revisions (v_1 -> v_2 -> v_1) verifying revision sequence monotonic increment (1 -> 2 -> 3).
   - 100% restatement drop vs 0% restatement drop vs completely disjoint drop.
   - Division-by-zero guards on percentage delta when baseline fact value is 0.0 or near machine epsilon (1e-15).
   - Restatement recall and precision invariant: verify strictly 100.00% under high revision volume.
3. Readiness Reporting & Anomaly Synthesis:
   - Degenerate daily coverage matrix: zero entities, 1 entity, missing dates, discontinuous drops.
   - Adversarial text in vendor questions (quotes, script tags, special characters, markdown formatting characters) ensuring safe report generation in both JSON and Markdown formats.
4. Flight Read API & Python API:
   - Arrow Flight RPC server: concurrent client connection burst, ticket parsing robustness, invalid tickets, zero-row result batches.
   - High-throughput streaming of bitemporal queries over Arrow Flight.
   - Domain exception hierarchy verification in Python API.
5. Synthetic & Real Dataset Loaders:
   - Relational integrity edge cases in SEC DERA parsing (orphaned numbers without matching sub headers, non-numeric values).
   - ALFRED vintage monotonicity violations and missing vintages.
"""

from __future__ import annotations

import concurrent.futures
import json
import socket
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.flight as fl
import pyarrow.parquet as pq
import pytest

from vendor_onboarding.api import (
    IngestionError,
    SchemaValidationError,
    VendorKitError,
    diff_drops,
    infer_and_pin,
    query_as_of,
    run_onboarding_pipeline,
    validate_drop,
)
from vendor_onboarding.datasets.fred import AlfredVintageLoader
from vendor_onboarding.datasets.sec import SECDeraLoader
from vendor_onboarding.diff.ledger import DiffEngine
from vendor_onboarding.flight.server import (
    BitemporalFlightClient,
    BitemporalFlightServer,
    FlightInvalidArgumentError,
    create_flight_ticket,
    parse_flight_ticket,
)
from vendor_onboarding.report.coverage import (
    build_coverage_matrix,
    detect_coverage_dips,
    detect_coverage_gaps,
)
from vendor_onboarding.report.generator import ReadinessReporter, export_reports
from vendor_onboarding.storage.query import (
    CANONICAL_AS_OF_SCHEMA,
    BitemporalQueryEngine,
)
from vendor_onboarding.storage.store import BitemporalStore
from vendor_onboarding.validation.rules import ValidationResult

pytestmark = [pytest.mark.tier5, pytest.mark.bitemporal]


def _get_free_port() -> int:
    """Finds an available local port for Arrow Flight gRPC server."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()
    return int(port)


def assert_exact_pit_invariance(h1: pa.Table, h2: pa.Table, tol: float = 1e-9) -> None:
    """Asserts mathematical equivalence between two PIT snapshots H_1 and H_2."""
    assert h1.num_rows == h2.num_rows, f"Row count mismatch: {h1.num_rows} != {h2.num_rows}"
    if h1.num_rows == 0:
        return

    r1 = h1.to_pylist()
    r2 = h2.to_pylist()

    key_fn = lambda r: (str(r.get("entity_id")), str(r.get("event_time")), str(r.get("metric", "")))
    map1 = {key_fn(r): r for r in r1}
    map2 = {key_fn(r): r for r in r2}

    assert set(map1.keys()) == set(map2.keys()), (
        f"Key set mismatch: {set(map1.keys()) ^ set(map2.keys())}"
    )

    for k, row1 in map1.items():
        row2 = map2[k]
        for field in row1:
            v1 = row1[field]
            v2 = row2.get(field)
            if isinstance(v1, float) and isinstance(v2, float):
                diff = abs(v1 - v2)
                assert diff <= tol, f"Float diff {diff} > {tol} for key {k}, field {field}: {v1} vs {v2}"
            else:
                assert v1 == v2, f"Value mismatch for key {k}, field {field}: {v1!r} vs {v2!r}"


def _make_records(
    entity_ids: list[str],
    event_times: list[datetime],
    knowledge_time: datetime,
    drop_id: str,
    base_val: float = 100.0,
    dataset_id: str = "TEST_DATASET",
    revision_seq: int = 1,
    is_restatement: bool = False,
) -> pa.Table:
    """Helper to generate canonical arrow tables for testing."""
    records: list[dict[str, Any]] = []
    for ent in entity_ids:
        for i, ev_dt in enumerate(event_times):
            val = round(base_val + (i * 1.5), 4)
            records.append({
                "record_id": f"{drop_id}_{ent}_{i}",
                "entity_id": ent,
                "source_symbol": ent.replace("ENT_", ""),
                "source_symbol_type": "TICKER",
                "event_time": ev_dt,
                "knowledge_time": knowledge_time,
                "metric": "CLOSE_PRICE",
                "value": val,
                "unit": "USD",
                "vendor_id": "SYNTH_VENDOR",
                "dataset_id": dataset_id,
                "drop_id": drop_id,
                "is_restatement": is_restatement,
                "revision_seq": revision_seq,
            })
    return pa.Table.from_pylist(records)


# ==============================================================================
# Dimension 1: Bitemporal Store & DuckDB Query Engine Adversarial Stress
# ==============================================================================


class TestBitemporalStoreAdversarialStress:
    """Stress tests on append-only partitioned store and DuckDB bitemporal queries."""

    def test_concurrent_append_and_as_of_queries(self, tmp_path: Path) -> None:
        """Multiple worker threads appending partitions while simultaneous as-of queries execute."""
        store_dir = tmp_path / "concurrent_store"
        store = BitemporalStore(root_dir=store_dir)
        ds = "CONCURRENT_EQUITY"

        # Pre-seed initial Drop 0
        t_base_ev = datetime(2023, 10, 1, 16, 0, 0, tzinfo=UTC)
        t_base_kt = datetime(2023, 10, 1, 20, 0, 0, tzinfo=UTC)
        entities = [f"ENT_{i:02d}" for i in range(10)]
        seed_tbl = _make_records(
            entity_ids=entities,
            event_times=[t_base_ev],
            knowledge_time=t_base_kt,
            drop_id="DROP_000",
            base_val=100.0,
            dataset_id=ds,
        )
        store.append(seed_tbl, dataset_id=ds, drop_id="DROP_000")

        # Prepare 6 distinct partition batches for concurrent writers
        num_writers = 6
        writer_drops: list[tuple[str, pa.Table]] = []
        for w in range(1, num_writers + 1):
            d_id = f"DROP_{w:03d}"
            ev_dt = t_base_ev + timedelta(days=w)
            kt_dt = t_base_kt + timedelta(days=w)
            tbl = _make_records(
                entity_ids=entities,
                event_times=[ev_dt],
                knowledge_time=kt_dt,
                drop_id=d_id,
                base_val=100.0 + (w * 10.0),
                dataset_id=ds,
            )
            writer_drops.append((d_id, tbl))

        def writer_task(d_id: str, table: pa.Table) -> int:
            local_store = BitemporalStore(root_dir=store_dir)
            try:
                rows = local_store.append(table, dataset_id=ds, drop_id=d_id)
                return rows
            finally:
                local_store.close()

        def reader_task(reader_id: int) -> list[int]:
            local_store = BitemporalStore(root_dir=store_dir)
            row_counts: list[int] = []
            try:
                for step in range(12):
                    as_of_t = t_base_kt + timedelta(days=step % (num_writers + 2))
                    res = local_store.as_of_query(ds, as_of_time=as_of_t)
                    row_counts.append(res.num_rows)
                return row_counts
            finally:
                local_store.close()

        # Execute concurrent writers and readers
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            writer_futures = [executor.submit(writer_task, d_id, tbl) for d_id, tbl in writer_drops]
            reader_futures = [executor.submit(reader_task, r_id) for r_id in range(4)]

            writer_results = [f.result(timeout=15.0) for f in writer_futures]
            reader_results = [f.result(timeout=15.0) for f in reader_futures]

        # Verify all writers succeeded
        assert len(writer_results) == num_writers
        assert all(count == 10 for count in writer_results)

        # Verify all readers completed without unhandled crash or partial read errors
        assert len(reader_results) == 4
        for r_counts in reader_results:
            assert len(r_counts) == 12
            assert all(c >= 0 for c in r_counts)

        # Verify all partitions exist, are read-only (0o444), and no temp files left
        dataset_dir = store_dir / ds
        partition_dirs = list(dataset_dir.glob("drop_id=*"))
        assert len(partition_dirs) == num_writers + 1
        for pdir in partition_dirs:
            pfile = pdir / "data.parquet"
            assert pfile.exists()
            mode = stat.S_IMODE(pfile.stat().st_mode)
            assert mode == 0o444, f"File {pfile} permissions {oct(mode)} != 0o444"
            tmp_files = list(pdir.glob("*.tmp"))
            assert len(tmp_files) == 0

        # Verify final state: 10 entities x 7 event dates = 70 deduplicated records
        final_query = store.as_of_query(ds, as_of_time=t_base_kt + timedelta(days=100))
        assert final_query.num_rows == 70
        rep = store.storage_factor(ds)
        assert rep.total_bitemporal_bytes > 0
        assert rep.deduplicated_bytes > 0
        assert rep.expansion_factor >= 1.0
        store.close()

    def test_strict_pit_invariance_across_cascading_historical_restatements(self, tmp_path: Path) -> None:
        """Strict point-in-time (H_1 == H_2) invariance across 6 sequential drops with interleaved restatements."""
        store_dir = tmp_path / "pit_cascading_store"
        store = BitemporalStore(root_dir=store_dir)
        ds = "PIT_CASCADING_DS"

        e1 = datetime(2023, 10, 2, 16, 0, 0, tzinfo=UTC)
        e2 = datetime(2023, 10, 3, 16, 0, 0, tzinfo=UTC)
        e3 = datetime(2023, 10, 4, 16, 0, 0, tzinfo=UTC)
        e4 = datetime(2023, 10, 5, 16, 0, 0, tzinfo=UTC)
        e5 = datetime(2023, 10, 6, 16, 0, 0, tzinfo=UTC)
        e6 = datetime(2023, 10, 7, 16, 0, 0, tzinfo=UTC)

        kt1 = datetime(2023, 10, 6, 20, 0, 0, tzinfo=UTC)
        kt2 = datetime(2023, 10, 13, 20, 0, 0, tzinfo=UTC)
        kt3 = datetime(2023, 10, 20, 20, 0, 0, tzinfo=UTC)
        kt4 = datetime(2023, 10, 27, 20, 0, 0, tzinfo=UTC)
        kt5 = datetime(2023, 11, 3, 20, 0, 0, tzinfo=UTC)
        kt6 = datetime(2023, 11, 10, 20, 0, 0, tzinfo=UTC)

        entities = ["ENT_01", "ENT_02", "ENT_03", "ENT_04", "ENT_05"]

        # Drop 1: Initial baseline across e1, e2, e3 for all 5 entities (15 records)
        d1 = _make_records(entities, [e1, e2, e3], kt1, "DROP_001", base_val=100.0, dataset_id=ds)
        store.append(d1, dataset_id=ds, drop_id="DROP_001")
        h1_initial = store.as_of_query(ds, as_of_time=kt1)
        assert h1_initial.num_rows == 15

        # Drop 2: New date e4 for all entities (5 recs) + restatement for ENT_01 on e2 (value 199.0, rev 2)
        d2_new = _make_records(entities, [e4], kt2, "DROP_002", base_val=105.0, dataset_id=ds)
        d2_restat = pa.Table.from_pylist([{
            "record_id": "D2_REST_01",
            "entity_id": "ENT_01",
            "source_symbol": "01",
            "source_symbol_type": "TICKER",
            "event_time": e2,
            "knowledge_time": kt2,
            "metric": "CLOSE_PRICE",
            "value": 199.0,
            "unit": "USD",
            "vendor_id": "SYNTH_VENDOR",
            "dataset_id": ds,
            "drop_id": "DROP_002",
            "is_restatement": True,
            "revision_seq": 2,
        }])
        d2 = pa.concat_tables([d2_new, d2_restat])
        store.append(d2, dataset_id=ds, drop_id="DROP_002")
        h2_initial = store.as_of_query(ds, as_of_time=kt2)
        assert h2_initial.num_rows == 20  # 5 entities x 4 dates

        # Drop 3: New date e5 (5 recs) + restatement for ENT_02 on e1 (value 250.0, rev 2)
        d3_new = _make_records(entities, [e5], kt3, "DROP_003", base_val=110.0, dataset_id=ds)
        d3_restat = pa.Table.from_pylist([{
            "record_id": "D3_REST_02",
            "entity_id": "ENT_02",
            "source_symbol": "02",
            "source_symbol_type": "TICKER",
            "event_time": e1,
            "knowledge_time": kt3,
            "metric": "CLOSE_PRICE",
            "value": 250.0,
            "unit": "USD",
            "vendor_id": "SYNTH_VENDOR",
            "dataset_id": ds,
            "drop_id": "DROP_003",
            "is_restatement": True,
            "revision_seq": 2,
        }])
        d3 = pa.concat_tables([d3_new, d3_restat])
        store.append(d3, dataset_id=ds, drop_id="DROP_003")
        h3_initial = store.as_of_query(ds, as_of_time=kt3)
        assert h3_initial.num_rows == 25

        # Drop 4: Cascading restatement on ENT_01 on e2 AGAIN (revision 3, value 299.0) + restatement ENT_03 on e3
        d4 = pa.Table.from_pylist([
            {
                "record_id": "D4_REST_01_REV3",
                "entity_id": "ENT_01",
                "source_symbol": "01",
                "source_symbol_type": "TICKER",
                "event_time": e2,
                "knowledge_time": kt4,
                "metric": "CLOSE_PRICE",
                "value": 299.0,
                "unit": "USD",
                "vendor_id": "SYNTH_VENDOR",
                "dataset_id": ds,
                "drop_id": "DROP_004",
                "is_restatement": True,
                "revision_seq": 3,
            },
            {
                "record_id": "D4_REST_03",
                "entity_id": "ENT_03",
                "source_symbol": "03",
                "source_symbol_type": "TICKER",
                "event_time": e3,
                "knowledge_time": kt4,
                "metric": "CLOSE_PRICE",
                "value": 155.0,
                "unit": "USD",
                "vendor_id": "SYNTH_VENDOR",
                "dataset_id": ds,
                "drop_id": "DROP_004",
                "is_restatement": True,
                "revision_seq": 2,
            },
        ])
        store.append(d4, dataset_id=ds, drop_id="DROP_004")
        h4_initial = store.as_of_query(ds, as_of_time=kt4)

        # Drop 5: New date e6 (5 recs) + restatement ENT_04 on e1
        d5_new = _make_records(entities, [e6], kt5, "DROP_005", base_val=115.0, dataset_id=ds)
        d5_restat = pa.Table.from_pylist([{
            "record_id": "D5_REST_04",
            "entity_id": "ENT_04",
            "source_symbol": "04",
            "source_symbol_type": "TICKER",
            "event_time": e1,
            "knowledge_time": kt5,
            "metric": "CLOSE_PRICE",
            "value": 180.0,
            "unit": "USD",
            "vendor_id": "SYNTH_VENDOR",
            "dataset_id": ds,
            "drop_id": "DROP_005",
            "is_restatement": True,
            "revision_seq": 2,
        }])
        d5 = pa.concat_tables([d5_new, d5_restat])
        store.append(d5, dataset_id=ds, drop_id="DROP_005")

        # Drop 6: Restatement ENT_05 on e5 + restatement ENT_01 on e1
        d6 = pa.Table.from_pylist([
            {
                "record_id": "D6_REST_05",
                "entity_id": "ENT_05",
                "source_symbol": "05",
                "source_symbol_type": "TICKER",
                "event_time": e5,
                "knowledge_time": kt6,
                "metric": "CLOSE_PRICE",
                "value": 210.0,
                "unit": "USD",
                "vendor_id": "SYNTH_VENDOR",
                "dataset_id": ds,
                "drop_id": "DROP_006",
                "is_restatement": True,
                "revision_seq": 2,
            },
            {
                "record_id": "D6_REST_01_E1",
                "entity_id": "ENT_01",
                "source_symbol": "01",
                "source_symbol_type": "TICKER",
                "event_time": e1,
                "knowledge_time": kt6,
                "metric": "CLOSE_PRICE",
                "value": 175.0,
                "unit": "USD",
                "vendor_id": "SYNTH_VENDOR",
                "dataset_id": ds,
                "drop_id": "DROP_006",
                "is_restatement": True,
                "revision_seq": 2,
            },
        ])
        store.append(d6, dataset_id=ds, drop_id="DROP_006")

        # Adversarial Verification: Post Drop-6 Point-In-Time Invariance Assertions
        h1_post = store.as_of_query(ds, as_of_time=kt1)
        assert_exact_pit_invariance(h1_initial, h1_post)

        h2_post = store.as_of_query(ds, as_of_time=kt2)
        assert_exact_pit_invariance(h2_initial, h2_post)

        h3_post = store.as_of_query(ds, as_of_time=kt3)
        assert_exact_pit_invariance(h3_initial, h3_post)

        h4_post = store.as_of_query(ds, as_of_time=kt4)
        assert_exact_pit_invariance(h4_initial, h4_post)

        # Audit trail checks for cascading revision sequence
        audit_ent1_e2 = store.audit_trail(ds, "ENT_01", e2)
        assert audit_ent1_e2.num_rows == 3
        seqs = audit_ent1_e2["revision_seq"].to_pylist()
        vals = audit_ent1_e2["value"].to_pylist()
        assert seqs == [1, 2, 3]
        assert vals == [101.5, 199.0, 299.0]
        store.close()

    def test_duckdb_connection_lifecycle_fd_leaks_and_concurrency(self, tmp_path: Path) -> None:
        """Stress-test 120+ connection initializations and teardowns checking for file descriptor leaks."""
        store_dir = tmp_path / "conn_lifecycle_store"
        store = BitemporalStore(root_dir=store_dir)
        ds = "FD_STRESS_DS"
        t_ev = datetime(2023, 10, 1, 16, 0, 0, tzinfo=UTC)
        t_kt = datetime(2023, 10, 1, 20, 0, 0, tzinfo=UTC)
        seed_tbl = _make_records(["ENT_A"], [t_ev], t_kt, "DROP_001", dataset_id=ds)
        store.append(seed_tbl, dataset_id=ds, drop_id="DROP_001")
        store.close()

        # Check baseline open file descriptors in Linux environment
        fd_path = Path("/proc/self/fd")
        initial_fds = len(list(fd_path.iterdir())) if fd_path.exists() else 0

        # Execute 120 connection cycles
        cycles = 120
        for _ in range(cycles):
            with BitemporalQueryEngine(root_dir=store_dir) as qe:
                res = qe.as_of_query(ds, as_of_time=t_kt)
                assert res.num_rows == 1

        if fd_path.exists():
            final_fds = len(list(fd_path.iterdir()))
            fd_delta = abs(final_fds - initial_fds)
            assert fd_delta < 15, f"FD leak detected: initial={initial_fds}, final={final_fds}, delta={fd_delta}"

        # Test concurrent independent query connections on the same Parquet store
        def run_independent_conn(idx: int) -> int:
            with BitemporalStore(root_dir=store_dir) as local_store:
                return int(local_store.as_of_query(ds, as_of_time=t_kt).num_rows)

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(run_independent_conn, i) for i in range(16)]
            results = [f.result(timeout=10.0) for f in futures]
        assert all(r == 1 for r in results)

    def test_non_standard_event_times_and_microsecond_boundary_precision(self, tmp_path: Path) -> None:
        """Non-standard event times (1970, leap days, 2050) and as-of queries at microsecond boundaries."""
        store_dir = tmp_path / "microsecond_store"
        store = BitemporalStore(root_dir=store_dir)
        ds = "TEMPORAL_EXTREMES"

        # 1. Non-standard event times
        t_epoch_zero = datetime(1970, 1, 1, 0, 0, 0, tzinfo=UTC)
        t_pre_epoch = datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)
        t_leap_day = datetime(2024, 2, 29, 12, 34, 56, 789012, tzinfo=UTC)
        t_century_leap = datetime(2000, 2, 29, 0, 0, 0, tzinfo=UTC)
        t_far_future = datetime(2050, 1, 1, 0, 0, 0, tzinfo=UTC)

        kt_exact = datetime(2024, 2, 29, 15, 0, 0, 500000, tzinfo=UTC)

        records = [
            {
                "record_id": "REC_EPOCH",
                "entity_id": "ENT_EPOCH",
                "source_symbol": "EPOCH",
                "source_symbol_type": "TICKER",
                "event_time": t_epoch_zero,
                "knowledge_time": kt_exact,
                "metric": "CLOSE_PRICE",
                "value": 10.0,
                "unit": "USD",
                "vendor_id": "V1",
                "dataset_id": ds,
                "drop_id": "DROP_001",
                "is_restatement": False,
                "revision_seq": 1,
            },
            {
                "record_id": "REC_PRE_EPOCH",
                "entity_id": "ENT_PRE_EPOCH",
                "source_symbol": "PRE_EPOCH",
                "source_symbol_type": "TICKER",
                "event_time": t_pre_epoch,
                "knowledge_time": kt_exact,
                "metric": "CLOSE_PRICE",
                "value": 20.0,
                "unit": "USD",
                "vendor_id": "V1",
                "dataset_id": ds,
                "drop_id": "DROP_001",
                "is_restatement": False,
                "revision_seq": 1,
            },
            {
                "record_id": "REC_LEAP",
                "entity_id": "ENT_LEAP",
                "source_symbol": "LEAP",
                "source_symbol_type": "TICKER",
                "event_time": t_leap_day,
                "knowledge_time": kt_exact,
                "metric": "CLOSE_PRICE",
                "value": 30.0,
                "unit": "USD",
                "vendor_id": "V1",
                "dataset_id": ds,
                "drop_id": "DROP_001",
                "is_restatement": False,
                "revision_seq": 1,
            },
            {
                "record_id": "REC_CENTURY",
                "entity_id": "ENT_CENTURY",
                "source_symbol": "CENTURY",
                "source_symbol_type": "TICKER",
                "event_time": t_century_leap,
                "knowledge_time": kt_exact,
                "metric": "CLOSE_PRICE",
                "value": 40.0,
                "unit": "USD",
                "vendor_id": "V1",
                "dataset_id": ds,
                "drop_id": "DROP_001",
                "is_restatement": False,
                "revision_seq": 1,
            },
            {
                "record_id": "REC_FUTURE",
                "entity_id": "ENT_FUTURE",
                "source_symbol": "FUTURE",
                "source_symbol_type": "TICKER",
                "event_time": t_far_future,
                "knowledge_time": kt_exact,
                "metric": "CLOSE_PRICE",
                "value": 50.0,
                "unit": "USD",
                "vendor_id": "V1",
                "dataset_id": ds,
                "drop_id": "DROP_001",
                "is_restatement": False,
                "revision_seq": 1,
            },
        ]
        store.append(pa.Table.from_pylist(records), dataset_id=ds, drop_id="DROP_001")

        # 2. Query exactly 1 microsecond before knowledge time: strictly 0 rows visible
        t_minus_1us = kt_exact - timedelta(microseconds=1)
        res_before = store.as_of_query(ds, as_of_time=t_minus_1us)
        assert res_before.num_rows == 0

        # 3. Query at exact knowledge time microsecond: all 5 rows visible
        res_exact = store.as_of_query(ds, as_of_time=kt_exact)
        assert res_exact.num_rows == 5

        # 4. Query exactly 1 microsecond after knowledge time: all 5 rows visible
        t_plus_1us = kt_exact + timedelta(microseconds=1)
        res_after = store.as_of_query(ds, as_of_time=t_plus_1us)
        assert res_after.num_rows == 5

        # 5. Append Drop 2 with microsecond restatement on leap day record
        kt_rev2 = kt_exact + timedelta(hours=2)
        rec_rev2 = [{
            "record_id": "REC_LEAP_REV2",
            "entity_id": "ENT_LEAP",
            "source_symbol": "LEAP",
            "source_symbol_type": "TICKER",
            "event_time": t_leap_day,
            "knowledge_time": kt_rev2,
            "metric": "CLOSE_PRICE",
            "value": 333.33,
            "unit": "USD",
            "vendor_id": "V1",
            "dataset_id": ds,
            "drop_id": "DROP_002",
            "is_restatement": True,
            "revision_seq": 2,
        }]
        store.append(pa.Table.from_pylist(rec_rev2), dataset_id=ds, drop_id="DROP_002")

        # As of 1us before kt_rev2: should return original value (30.0)
        q_before_rev = store.as_of_query(
            ds, as_of_time=kt_rev2 - timedelta(microseconds=1), entity_ids=["ENT_LEAP"]
        )
        assert q_before_rev.num_rows == 1
        assert q_before_rev["value"][0].as_py() == 30.0

        # As of exact kt_rev2: should return restated value (333.33)
        q_exact_rev = store.as_of_query(ds, as_of_time=kt_rev2, entity_ids=["ENT_LEAP"])
        assert q_exact_rev.num_rows == 1
        assert q_exact_rev["value"][0].as_py() == 333.33
        store.close()


# ==============================================================================
# Dimension 2: Diff Engine & Revision Ledger Adversarial Stress
# ==============================================================================


class TestDiffEngineRevisionLedgerAdversarial:
    """Adversarial stress testing of revision classification, precision/recall, and delta math."""

    def test_cyclic_revisions_monotonic_sequence_increment(self) -> None:
        """Cyclic revisions (Value v_1 -> v_2 -> v_1 -> v_3) verify revision sequence monotonic increment."""
        t_ev = datetime(2023, 10, 2, 16, 0, 0, tzinfo=UTC)
        engine = DiffEngine()

        def make_row(val: float, rev_seq: int, drop_id: str) -> pa.Table:
            return pa.Table.from_pylist([{
                "record_id": f"REC_{drop_id}",
                "entity_id": "ENT_CYCLIC",
                "source_symbol": "CYC",
                "source_symbol_type": "TICKER",
                "event_time": t_ev,
                "metric": "CLOSE_PRICE",
                "value": val,
                "unit": "USD",
                "revision_seq": rev_seq,
            }])

        # Drop 1: v1 = 100.0, rev = 1
        d1 = make_row(100.0, 1, "DROP_001")

        # Drop 2: v2 = 120.0 (restated)
        d2 = make_row(120.0, 1, "DROP_002")
        diff_1_2, sum_1_2 = engine.diff_drops(d1, d2)
        assert sum_1_2.restated_records == 1
        assert diff_1_2["revision_seq"][0].as_py() == 2
        assert diff_1_2["is_restatement"][0].as_py() is True
        assert diff_1_2["restatement_delta"][0].as_py() == 20.0

        # Construct Drop 2 state with revision_seq=2
        d2_state = make_row(120.0, 2, "DROP_002")

        # Drop 3: v3 = 100.0 (reverted to original baseline value!)
        d3 = make_row(100.0, 1, "DROP_003")
        diff_2_3, sum_2_3 = engine.diff_drops(d2_state, d3)
        assert sum_2_3.restated_records == 1
        # Invariant: reversion still increments revision sequence to 3!
        assert diff_2_3["revision_seq"][0].as_py() == 3
        assert diff_2_3["is_restatement"][0].as_py() is True
        assert diff_2_3["restatement_delta"][0].as_py() == -20.0

        d3_state = make_row(100.0, 3, "DROP_003")

        # Drop 4: v4 = 150.0
        d4 = make_row(150.0, 1, "DROP_004")
        diff_3_4, sum_3_4 = engine.diff_drops(d3_state, d4)
        assert sum_3_4.restated_records == 1
        assert diff_3_4["revision_seq"][0].as_py() == 4
        assert diff_3_4["is_restatement"][0].as_py() is True

        d4_state = make_row(150.0, 4, "DROP_004")

        # Drop 5: v5 = 150.0 (unchanged from Drop 4, retaining revision 4)
        d5 = make_row(150.0, 4, "DROP_005")
        diff_4_5, sum_4_5 = engine.diff_drops(d4_state, d5)
        assert sum_4_5.unchanged_records == 1
        assert sum_4_5.restated_records == 0
        assert diff_4_5["revision_seq"][0].as_py() == 4
        assert diff_4_5["is_restatement"][0].as_py() is False

        # Direct test of apply_drop_revisions
        applied = engine.apply_drop_revisions(d1, d2)
        assert applied.num_rows == 1
        assert applied["diff_status"][0].as_py() == "RESTATED"
        engine.close()

    def test_extreme_drop_distributions_all_restated_all_unchanged_disjoint(self) -> None:
        """100% restatement drop vs 0% restatement drop vs completely disjoint drop."""
        engine = DiffEngine()
        t_ev = datetime(2023, 10, 2, 16, 0, 0, tzinfo=UTC)

        # 1. 100% restatement drop
        n_rows = 50
        base_records = [
            {"entity_id": f"E_{i}", "event_time": t_ev, "metric": "CLOSE", "value": float(i * 10)}
            for i in range(n_rows)
        ]
        curr_restated = [
            {"entity_id": f"E_{i}", "event_time": t_ev, "metric": "CLOSE", "value": float(i * 10 + 5)}
            for i in range(n_rows)
        ]
        tbl_base = pa.Table.from_pylist(base_records)
        tbl_curr = pa.Table.from_pylist(curr_restated)

        _, sum_all_restated = engine.diff_drops(tbl_base, tbl_curr)
        assert sum_all_restated.restated_records == n_rows
        assert sum_all_restated.unchanged_records == 0
        assert sum_all_restated.new_records == 0
        assert sum_all_restated.dropped_records == 0
        assert sum_all_restated.recall_rate == 1.0
        assert sum_all_restated.precision_rate == 1.0

        # 2. 0% restatement drop (100% unchanged)
        _, sum_unchanged = engine.diff_drops(tbl_base, tbl_base)
        assert sum_unchanged.unchanged_records == n_rows
        assert sum_unchanged.restated_records == 0
        assert sum_unchanged.new_records == 0
        assert sum_unchanged.dropped_records == 0

        # 3. Completely disjoint drop
        curr_disjoint = [
            {"entity_id": f"NEW_E_{i}", "event_time": t_ev, "metric": "CLOSE", "value": float(i * 10)}
            for i in range(30)
        ]
        tbl_disjoint = pa.Table.from_pylist(curr_disjoint)
        _, sum_disjoint = engine.diff_drops(tbl_base, tbl_disjoint)
        assert sum_disjoint.dropped_records == n_rows
        assert sum_disjoint.new_records == 30
        assert sum_disjoint.unchanged_records == 0
        assert sum_disjoint.restated_records == 0
        engine.close()

    def test_division_by_zero_guards_and_extreme_epsilons(self) -> None:
        """Division-by-zero guards on percentage delta when baseline fact value is 0.0 or near machine epsilon."""
        engine = DiffEngine()
        t_ev = datetime(2023, 10, 2, 16, 0, 0, tzinfo=UTC)

        base_rows = [
            {"entity_id": "ZERO_BASE", "event_time": t_ev, "metric": "PX", "value": 0.0},
            {"entity_id": "NEG_ZERO", "event_time": t_ev, "metric": "PX", "value": -0.0},
            {"entity_id": "EPSILON_BASE", "event_time": t_ev, "metric": "PX", "value": 1e-15},
            {"entity_id": "NULL_BASE", "event_time": t_ev, "metric": "PX", "value": None},
            {"entity_id": "VALID_BASE", "event_time": t_ev, "metric": "PX", "value": 100.0},
        ]
        curr_rows = [
            {"entity_id": "ZERO_BASE", "event_time": t_ev, "metric": "PX", "value": 50.0},  # 0 -> 50: pct is NULL
            {"entity_id": "NEG_ZERO", "event_time": t_ev, "metric": "PX", "value": -25.0},  # -0 -> -25: pct is NULL
            {"entity_id": "EPSILON_BASE", "event_time": t_ev, "metric": "PX", "value": 10.0},  # 1e-15 -> 10.0: finite pct
            {"entity_id": "NULL_BASE", "event_time": t_ev, "metric": "PX", "value": 10.0},  # None -> 10: pct NULL
            {"entity_id": "VALID_BASE", "event_time": t_ev, "metric": "PX", "value": 120.0},  # 100 -> 120: pct 20.0
        ]

        tbl_base = pa.Table.from_pylist(base_rows)
        tbl_curr = pa.Table.from_pylist(curr_rows)

        diff_tbl, summary = engine.diff_drops(tbl_base, tbl_curr)
        assert summary.restated_records == 5

        res_dict = {r["entity_id"]: r for r in diff_tbl.to_pylist()}

        # 0.0 baseline: restatement_delta = 50.0, restatement_pct_delta MUST be None (guarded from div-by-zero)
        assert res_dict["ZERO_BASE"]["restatement_delta"] == 50.0
        assert res_dict["ZERO_BASE"]["restatement_pct_delta"] is None

        # -0.0 baseline: restatement_delta = -25.0, restatement_pct_delta MUST be None
        assert res_dict["NEG_ZERO"]["restatement_delta"] == -25.0
        assert res_dict["NEG_ZERO"]["restatement_pct_delta"] is None

        # 1e-15 baseline: restatement_delta finite, pct delta finite without zero division crash
        assert abs(res_dict["EPSILON_BASE"]["restatement_delta"] - 10.0) < 1e-6
        assert res_dict["EPSILON_BASE"]["restatement_pct_delta"] > 1e15

        # NULL baseline: delta is None, pct is None
        assert res_dict["NULL_BASE"]["restatement_delta"] is None
        assert res_dict["NULL_BASE"]["restatement_pct_delta"] is None

        # Valid baseline: delta = 20.0, pct = 20.0
        assert res_dict["VALID_BASE"]["restatement_delta"] == 20.0
        assert res_dict["VALID_BASE"]["restatement_pct_delta"] == 20.0

        # Check compute_delta_statistics robustness on mixture of None pct deltas
        stats = engine.compute_delta_statistics(diff_tbl)
        assert stats.count == 5
        assert stats.mean_delta is not None
        assert stats.mean_pct_delta is not None
        assert stats.to_dict()["count"] == 5
        engine.close()

    def test_high_volume_restatement_precision_and_recall_invariant(self) -> None:
        """Restatement recall and precision invariant: verify 100.00% under high revision volume (1,200 rows)."""
        engine = DiffEngine()
        t_ev = datetime(2023, 10, 2, 16, 0, 0, tzinfo=UTC)

        total_base = 1200
        num_restated = 400
        num_deleted = 200
        num_new = 300
        # num_unchanged = 1200 - 400 - 200 = 600

        base_rows: list[dict[str, Any]] = []
        for i in range(total_base):
            base_rows.append({
                "record_id": f"REC_B_{i:04d}",
                "entity_id": f"ENT_{i:04d}",
                "event_time": t_ev,
                "metric": "CLOSE",
                "value": float(100 + (i % 50)),
            })

        curr_rows: list[dict[str, Any]] = []
        planted_restated_keys: set[str] = set()

        # 1. 600 unchanged records (indices 0..599)
        for r in base_rows[:600]:
            curr_rows.append(dict(r))

        # 2. 400 restated records (indices 600..999) with non-zero delta
        for r in base_rows[600:1000]:
            r_mod = dict(r)
            r_mod["value"] = float(r["value"] + 7.25)
            curr_rows.append(r_mod)
            planted_restated_keys.add(r["entity_id"])

        # 3. 200 dropped records (indices 1000..1199) omitted from curr_rows

        # 4. 300 new records
        for i in range(num_new):
            curr_rows.append({
                "record_id": f"REC_NEW_{i:04d}",
                "entity_id": f"ENT_NEW_{i:04d}",
                "event_time": t_ev,
                "metric": "CLOSE",
                "value": float(200 + i),
            })

        tbl_base = pa.Table.from_pylist(base_rows)
        tbl_curr = pa.Table.from_pylist(curr_rows)

        diff_tbl, summary = engine.diff_drops(tbl_base, tbl_curr)

        assert summary.restated_records == num_restated
        assert summary.unchanged_records == 600
        assert summary.dropped_records == num_deleted
        assert summary.new_records == num_new
        assert summary.total_records == total_base + num_new

        # Strict mathematical invariants: 100.00% recall and 100.00% precision
        assert summary.recall_rate == 1.0
        assert summary.precision_rate == 1.0

        # Exact match of restated entity keys with zero false positives or false negatives
        detected_restated = {
            r["entity_id"] for r in diff_tbl.to_pylist() if r["diff_status"] == "RESTATED"
        }
        assert detected_restated == planted_restated_keys
        engine.close()


# ==============================================================================
# Dimension 3: Readiness Reporting & Anomaly Synthesis Adversarial Stress
# ==============================================================================


class TestReadinessReportingAdversarialStress:
    """Degenerate coverage matrices, adversarial text injection, and dual-format report parity."""

    def test_degenerate_daily_coverage_matrix(self) -> None:
        """Degenerate daily coverage matrix: zero entities, 1 entity, missing dates, discontinuous drops."""
        # Case 1: Empty table (zero entities)
        empty_tbl = pa.table({})
        matrix_empty = build_coverage_matrix(empty_tbl)
        assert len(matrix_empty) == 0
        assert matrix_empty.to_dict() == {}
        assert detect_coverage_dips(empty_tbl) == []
        assert detect_coverage_gaps(empty_tbl) == []

        # Case 2: 1 entity on 1 date
        t_ev = datetime(2023, 10, 2, 16, 0, 0, tzinfo=UTC)
        single_tbl = pa.Table.from_pylist([{"entity_id": "SOLO", "event_time": t_ev}])
        matrix_single = build_coverage_matrix(single_tbl)
        assert len(matrix_single) == 1
        assert "SOLO" in matrix_single
        assert matrix_single["SOLO"]["2023-10-02"] == 1
        assert detect_coverage_dips(single_tbl) == []

        # Case 3: Discontinuous calendar & coverage gaps
        gap_tbl = pa.Table.from_pylist([
            {"entity_id": "E1", "event_time": datetime(2023, 10, 1, 16, 0, 0, tzinfo=UTC)},
            {"entity_id": "E1", "event_time": datetime(2023, 10, 5, 16, 0, 0, tzinfo=UTC)},
            {"entity_id": "E2", "event_time": datetime(2023, 10, 1, 16, 0, 0, tzinfo=UTC)},
            {"entity_id": "E2", "event_time": datetime(2023, 10, 3, 16, 0, 0, tzinfo=UTC)},
            {"entity_id": "E2", "event_time": datetime(2023, 10, 5, 16, 0, 0, tzinfo=UTC)},
        ])
        expected_dts = ["2023-10-01", "2023-10-03", "2023-10-05"]
        gaps = detect_coverage_gaps(gap_tbl, expected_dates=expected_dts)
        assert len(gaps) == 1
        assert gaps[0].entity_id == "E1"
        assert gaps[0].missing_dates == ["2023-10-03"]
        assert gaps[0].expected_count == 3
        assert gaps[0].actual_count == 2

        # Case 4: Day-over-day entity coverage dip exceeding 20%
        dip_tbl = pa.Table.from_pylist([
            # Day 1: 10 entities
            *[{"entity_id": f"ENT_{i}", "event_time": datetime(2023, 10, 1, 16, 0, 0, tzinfo=UTC)} for i in range(10)],
            # Day 2: 2 entities (80% dip!)
            *[{"entity_id": f"ENT_{i}", "event_time": datetime(2023, 10, 2, 16, 0, 0, tzinfo=UTC)} for i in range(2)],
        ])
        dips = detect_coverage_dips(dip_tbl, dip_threshold_pct=20.0)
        assert len(dips) == 1
        assert dips[0].date == "2023-10-02"
        assert dips[0].baseline_count == 10
        assert dips[0].current_count == 2
        assert dips[0].dip_percentage == 80.0

    def test_adversarial_text_in_vendor_inquiries_dual_format_safety(self, tmp_path: Path) -> None:
        """Adversarial text in vendor questions (quotes, script tags, special characters) ensuring safe generation."""
        # Prepare adversarial symbols, quotes, scripts, and markdown format characters
        adversarial_symbols = [
            "<script>alert('XSS')</script>",
            "O'Reilly & Co. \"DoubleQuote\"",
            "| Pipe | Injection | Table |",
            "### Heading Injection",
            "\u202eRTL_OVERRIDE\u202c",
            "Emoji_💥_⚠️",
        ]

        # Validation results containing adversarial text
        val_results = [
            ValidationResult(
                check_name="range_domain",
                passed=False,
                total_records=100,
                violation_count=6,
                violation_rate=0.06,
                threshold=0.0,
                violations_sample=[{"source_symbol": sym} for sym in adversarial_symbols],
            ),
            ValidationResult(
                check_name="completeness",
                passed=False,
                total_records=100,
                violation_count=3,
                violation_rate=0.03,
                violations_sample=[],
                threshold=0.01,
            ),

        ]

        reporter = ReadinessReporter()
        dummy_tbl = pa.Table.from_pylist([
            {"entity_id": "TEST_ENT", "event_time": datetime(2023, 10, 1, 16, 0, 0, tzinfo=UTC)}
        ])

        md_report, json_report = reporter.generate_report(
            dataset_id_or_context="ADVERSARIAL_TEXT_DS",
            vendor_id="HACKER_VENDOR_<script>",
            table=dummy_tbl,
            validation_results=val_results,
        )

        # 1. Verify JSON validity and round-trip fidelity
        assert json_report["overall_status"] == "FAIL"
        assert json_report["total_records"] == 1
        json_serialized = json.dumps(json_report, indent=2)
        parsed_json = json.loads(json_serialized)
        assert parsed_json["vendor_id"] == "HACKER_VENDOR_<script>"
        assert len(parsed_json["vendor_inquiries"]) >= 2

        # 2. Verify Markdown formatting does not break or crash
        assert "# Vendor Data Readiness Report: ADVERSARIAL_TEXT_DS" in md_report
        assert "## 1. Validation Scorecard" in md_report
        assert "## 3. Automated Vendor Inquiries" in md_report
        assert "```text\n" in md_report

        # 3. Export reports and verify physical file persistence
        md_file, json_file = export_reports(tmp_path, md_report, json_report, base_name="test_adv_report")
        assert md_file.exists()
        assert json_file.exists()
        assert json_file.read_text(encoding="utf-8") == json_serialized


# ==============================================================================
# Dimension 4: Flight Read API & Python API Adversarial Stress
# ==============================================================================


class TestFlightReadAndPythonAPIAdversarial:
    """Arrow Flight RPC server stress, ticket fuzzing, zero-row batches, and domain API workflows."""

    def test_flight_server_burst_concurrent_clients(self, tmp_path: Path) -> None:
        """Arrow Flight RPC server: concurrent client connection burst."""
        store_dir = tmp_path / "flight_burst_store"
        store = BitemporalStore(root_dir=store_dir)
        ds = "BURST_FLIGHT_DS"

        t_ev = datetime(2023, 10, 2, 16, 0, 0, tzinfo=UTC)
        t_kt = datetime(2023, 10, 6, 20, 0, 0, tzinfo=UTC)
        entities = [f"ENT_{i:02d}" for i in range(20)]
        seed_tbl = _make_records(entities, [t_ev], t_kt, "DROP_001", dataset_id=ds)
        store.append(seed_tbl, dataset_id=ds, drop_id="DROP_001")
        store.close()

        port = _get_free_port()
        server = BitemporalFlightServer(location=f"grpc://127.0.0.1:{port}", store=store_dir)

        def query_flight(client_idx: int) -> int:
            with BitemporalFlightClient(f"grpc://127.0.0.1:{port}") as cl:
                ent_filter = [f"ENT_{client_idx:02d}"] if client_idx < 10 else None
                tbl = cl.as_of_query(
                    dataset_id=ds,
                    as_of=t_kt,
                    entity_ids=ent_filter,
                )
                return int(tbl.num_rows)

        try:
            num_clients = 16
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                futures = [pool.submit(query_flight, i) for i in range(num_clients)]
                results = [f.result(timeout=10.0) for f in futures]

            assert len(results) == num_clients
            for idx, r_count in enumerate(results):
                if idx < 10:
                    assert r_count == 1  # filtered to single entity
                else:
                    assert r_count == 20  # unfiltered
        finally:
            server.shutdown()

    def test_flight_ticket_adversarial_payload_fuzzing(self) -> None:
        """Ticket parsing robustness, invalid tickets, and FlightInvalidArgumentError."""
        server = BitemporalFlightServer(location=f"grpc://127.0.0.1:{_get_free_port()}", store="data/store")
        try:
            # 1. Non-JSON bytes
            with pytest.raises(FlightInvalidArgumentError):
                server.do_get(None, fl.Ticket(b"\x00\xff\xfe\x01\x02"))

            # 2. Empty JSON dict (missing mandatory dataset_id and as_of)
            with pytest.raises(FlightInvalidArgumentError):
                server.do_get(None, fl.Ticket(b"{}"))

            # 3. Missing as_of timestamp
            with pytest.raises(FlightInvalidArgumentError):
                server.do_get(None, fl.Ticket(b'{"dataset_id": "DS"}'))

            # 4. Missing dataset_id
            with pytest.raises(FlightInvalidArgumentError):
                server.do_get(None, fl.Ticket(b'{"as_of": "2023-10-06T20:00:00Z"}'))

            # 5. Invalid date string
            with pytest.raises(FlightInvalidArgumentError):
                server.do_get(None, fl.Ticket(b'{"dataset_id": "DS", "as_of": "not-a-datetime"}'))

            # 6. JSON array payload
            with pytest.raises(FlightInvalidArgumentError):
                server.do_get(None, fl.Ticket(b'["dataset_id", "as_of"]'))

            # 7. Valid ticket with harmless unexpected keys (should parse and execute without crash)
            valid_with_extra = create_flight_ticket(
                dataset_id="TEST_DS",
                as_of=datetime.now(UTC),
            )
            data = json.loads(valid_with_extra.decode("utf-8"))
            data["unexpected_injected_param"] = "attack_payload"
            custom_ticket = json.dumps(data).encode("utf-8")
            parsed = parse_flight_ticket(custom_ticket)
            assert parsed["dataset_id"] == "TEST_DS"
            assert "unexpected_injected_param" in parsed
        finally:
            server.shutdown()

    def test_flight_zero_row_and_high_throughput_streaming(self, tmp_path: Path) -> None:
        """Zero-row result batches and high-throughput multi-batch streaming over Arrow Flight."""
        store_dir = tmp_path / "flight_stream_store"
        store = BitemporalStore(root_dir=store_dir)
        ds = "STREAMING_DS"

        t_ev = datetime(2023, 10, 2, 16, 0, 0, tzinfo=UTC)
        t_kt = datetime(2023, 10, 6, 20, 0, 0, tzinfo=UTC)

        # Ingest 1,000 records across 50 entities and 20 event dates
        entities = [f"ENT_{i:03d}" for i in range(50)]
        event_dates = [t_ev + timedelta(days=d) for d in range(20)]
        bulk_tbl = _make_records(entities, event_dates, t_kt, "DROP_001", dataset_id=ds)
        assert bulk_tbl.num_rows == 1000
        store.append(bulk_tbl, dataset_id=ds, drop_id="DROP_001")
        store.close()

        port = _get_free_port()
        server = BitemporalFlightServer(location=f"grpc://127.0.0.1:{port}", store=store_dir)
        try:
            with BitemporalFlightClient(f"grpc://127.0.0.1:{port}") as client:
                # 1. Zero-row query (querying non-existent entity)
                zero_res = client.as_of_query(
                    dataset_id=ds,
                    as_of=t_kt,
                    entity_ids=["NON_EXISTENT_ENTITY_XYZ"],
                )
                assert zero_res.num_rows == 0
                assert "entity_id" in zero_res.column_names
                assert "value" in zero_res.column_names

                # 2. High-throughput query (streaming all records)
                stream_res = client.as_of_query(
                    dataset_id=ds,
                    as_of=t_kt,
                )
                # Since as_of_query deduplicates latest state per (entity_id, event_time, metric),
                # all 1,000 rows should be present
                assert stream_res.num_rows == 1000
                expected_cols = [
                    "entity_id",
                    "event_time",
                    "knowledge_time",
                    "metric",
                    "value",
                    "unit",
                    "source_symbol",
                    "source_symbol_type",
                    "vendor_id",
                    "dataset_id",
                    "is_restatement",
                    "revision_seq",
                ]
                assert stream_res.column_names == expected_cols
        finally:
            server.shutdown()

    def test_python_api_domain_exceptions_and_edge_cases(self, tmp_path: Path) -> None:
        """Python programmatic API exception hierarchy and error handling."""
        # 1. run_onboarding_pipeline on missing file raises IngestionError
        missing_file = tmp_path / "ghost_file.csv"
        with pytest.raises(IngestionError) as exc_info:
            run_onboarding_pipeline(missing_file)
        assert issubclass(IngestionError, VendorKitError)
        assert "not found" in str(exc_info.value)

        # 2. diff_drops on missing file raises IngestionError
        valid_file = tmp_path / "valid.parquet"
        pq.write_table(pa.table({"entity_id": ["E1"]}), valid_file)
        with pytest.raises(IngestionError):
            diff_drops(missing_file, valid_file)
        with pytest.raises(IngestionError):
            diff_drops(valid_file, missing_file)

        # 3. infer_and_pin on missing file raises IngestionError
        with pytest.raises(IngestionError):
            infer_and_pin(missing_file, tmp_path / "schema.json")

        # 4. validate_drop on missing file raises IngestionError
        with pytest.raises(IngestionError):
            validate_drop(missing_file)

        # 5. validate_drop on missing schema path raises SchemaValidationError
        with pytest.raises(SchemaValidationError):
            validate_drop(valid_file, schema_path=missing_file)

        # 6. query_as_of on non-existent dataset returns empty canonical schema
        empty_store_dir = tmp_path / "empty_store"
        empty_res = query_as_of("GHOST_DATASET", "2023-10-06T20:00:00Z", empty_store_dir)
        assert empty_res.num_rows == 0
        assert empty_res.schema.equals(CANONICAL_AS_OF_SCHEMA)


# ==============================================================================
# Dimension 5: Synthetic & Real Dataset Loaders Adversarial Stress
# ==============================================================================


class TestDatasetLoadersAdversarialStress:
    """Relational join anomalies in SEC DERA and monotonicity violations in ALFRED vintages."""

    def test_sec_dera_relational_orphans_and_malformed_values(self, tmp_path: Path) -> None:
        """Relational integrity edge cases in SEC DERA parsing (orphaned numbers, non-numeric values)."""
        sub_file = tmp_path / "test_sub.txt"
        num_file = tmp_path / "test_num.txt"

        # sub.txt with 2 valid filings: 10-K and 10-K/A amendment
        sub_content = (
            "adsh\tcik\tname\tsic\tcountryba\tstprba\tcityba\tzipba\tbas1\tbas2\tbphone\tcountryma\tstprma\tcityma\tzipma\tmas1\tmas2\tcountryinc\tstprinc\tein\tformer\tdformer\tchanged\tafs\twksi\tfye\tform\tperiod\tfy\tfp\tfiled\taccepted\tprevrpt\tnrpts\tacount\tinqda\tfsdone\tfside\tactive\n"
            "0000320193-23-000100\t320193\tAPPLE INC\t3571\tUS\tCA\tCUPERTINO\t95014\t1 APPLE PARK WAY\t\t4089961010\tUS\tCA\tCUPERTINO\t95014\t1 APPLE PARK WAY\t\tUS\tCA\t942404110\t\t\t\t1-LA\t1\t0930\t10-K\t20230930\t2023\tFY\t20231103\t2023-11-03 18:00:00\t0\t1\t1\t0\t1\t\t1\n"
            "0000320193-23-000101\t320193\tAPPLE INC\t3571\tUS\tCA\tCUPERTINO\t95014\t1 APPLE PARK WAY\t\t4089961010\tUS\tCA\tCUPERTINO\t95014\t1 APPLE PARK WAY\t\tUS\tCA\t942404110\t\t\t\t1-LA\t1\t0930\t10-K/A\t20230930\t2023\tFY\t20231110\t2023-11-10 18:00:00\t0\t1\t1\t0\t1\t\t1\n"
        )
        sub_file.write_text(sub_content, encoding="utf-8")

        # num.txt with:
        # - Matching fact for 10-K (valid numeric value)
        # - Matching fact for 10-K/A (amended value)
        # - Fact with non-numeric value 'CORRUPTED_VALUE'
        # - Orphaned fact for adsh '9999999999-99-999999' not present in sub.txt
        num_content = (
            "adsh\ttag\tversion\tcoreg\tddate\tqtrs\tuom\tvalue\tfootnote\n"
            "0000320193-23-000100\tRevenues\tus-gaap/2023\t\t20230930\t4\tUSD\t383285000000.0\t\n"
            "0000320193-23-000101\tRevenues\tus-gaap/2023\t\t20230930\t4\tUSD\t383290000000.0\t\n"
            "0000320193-23-000100\tNonNumericMetric\tus-gaap/2023\t\t20230930\t4\tUSD\tCORRUPTED_VALUE\t\n"
            "9999999999-99-999999\tOrphanMetric\tus-gaap/2023\t\t20230930\t4\tUSD\t1000000.0\t\n"
        )
        num_file.write_text(num_content, encoding="utf-8")

        loader = SECDeraLoader()
        facts_tbl = loader.load_facts(sub_path=sub_file, num_path=num_file)

        facts = facts_tbl.to_pylist()
        # 1. Orphaned adsh '9999999999-99-999999' MUST be dropped by relational inner join
        adsh_set = {r["adsh"] for r in facts}
        assert "9999999999-99-999999" not in adsh_set
        assert len(facts) == 3

        # 2. CIK must be zero-padded to 10 digits
        for r in facts:
            assert r["source_symbol"] == "0000320193"
            assert r["entity_id"] == "ENT_CIK_0000320193"

        # 3. Non-numeric value is safely converted to NULL
        non_num_fact = next(r for r in facts if r["metric"] == "NonNumericMetric")
        assert non_num_fact["value"] is None

        # 4. Form 10-K fact has is_restatement=False, revision_seq=1
        orig_fact = next(r for r in facts if r["metric"] == "Revenues" and r["adsh"] == "0000320193-23-000100")
        assert orig_fact["is_restatement"] is False
        assert orig_fact["revision_seq"] == 1
        assert orig_fact["value"] == 383285000000.0

        # 5. Form 10-K/A amendment fact has is_restatement=True, revision_seq=2
        amend_fact = next(r for r in facts if r["metric"] == "Revenues" and r["adsh"] == "0000320193-23-000101")
        assert amend_fact["is_restatement"] is True
        assert amend_fact["revision_seq"] == 2
        assert amend_fact["value"] == 383290000000.0

    def test_alfred_vintage_monotonicity_and_as_of_filtering(self, tmp_path: Path) -> None:
        """ALFRED vintage monotonicity violations, out-of-order vintages, and zero look-ahead as_of."""
        alfred_csv = tmp_path / "test_alfred.csv"

        # Out-of-order CSV records: observation 2023-01-01 has 3 vintages written out of order
        csv_content = (
            "realtime_start,date,value\n"
            "2023-03-30,2023-01-01,22114.9\n"  # v3
            "2023-01-26,2023-01-01,22026.0\n"  # v1
            "2023-02-23,2023-01-01,22080.5\n"  # v2
            "2023-04-27,2023-04-01,22225.4\n"  # observation 2023-04-01 v1
        )
        alfred_csv.write_text(csv_content, encoding="utf-8")

        loader = AlfredVintageLoader()
        tbl = loader.load_series(alfred_csv, series_id="GDPC1")

        # Verify SQL ORDER BY event_time, knowledge_time restored correct chronological revision sequence
        records = tbl.to_pylist()
        obs_1_records = [r for r in records if r["event_time"].strftime("%Y-%m-%d") == "2023-01-01"]
        assert len(obs_1_records) == 3

        # Chronological order check
        assert obs_1_records[0]["knowledge_time"].strftime("%Y-%m-%d") == "2023-01-26"
        assert obs_1_records[0]["revision_seq"] == 1
        assert obs_1_records[0]["is_restatement"] is False
        assert obs_1_records[0]["value"] == 22026.0

        assert obs_1_records[1]["knowledge_time"].strftime("%Y-%m-%d") == "2023-02-23"
        assert obs_1_records[1]["revision_seq"] == 2
        assert obs_1_records[1]["is_restatement"] is True
        assert obs_1_records[1]["value"] == 22080.5

        assert obs_1_records[2]["knowledge_time"].strftime("%Y-%m-%d") == "2023-03-30"
        assert obs_1_records[2]["revision_seq"] == 3
        assert obs_1_records[2]["is_restatement"] is True
        assert obs_1_records[2]["value"] == 22114.9

        # As-of filtering tests verifying zero look-ahead bias
        # 1. As-of before any vintage published: 0 rows
        as_of_t0 = loader.as_of(tbl, "2023-01-15", latest_only=True)
        assert as_of_t0.num_rows == 0

        # 2. As-of after v1: exactly v1 value
        as_of_t1 = loader.as_of(tbl, "2023-02-01", latest_only=True)
        assert as_of_t1.num_rows == 1
        assert as_of_t1["value"][0].as_py() == 22026.0

        # 3. As-of after v2: exactly v2 value
        as_of_t2 = loader.as_of(tbl, "2023-03-01", latest_only=True)
        assert as_of_t2.num_rows == 1
        assert as_of_t2["value"][0].as_py() == 22080.5

        # 4. As-of after v3 and observation 2023-04-01: exactly 2 rows
        as_of_t3 = loader.as_of(tbl, "2023-05-01", latest_only=True)
        assert as_of_t3.num_rows == 2
        rows_t3 = as_of_t3.to_pylist()
        assert rows_t3[0]["value"] == 22114.9  # v3 for 2023-01-01
        assert rows_t3[1]["value"] == 22225.4  # v1 for 2023-04-01
