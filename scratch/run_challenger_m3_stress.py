#!/usr/bin/env python3
"""
Milestone 3 Challenger 1: Empirical Adversarial Stress & Storage Expansion Benchmark.

Executes exhaustive empirical boundary attacks against:
- src/vendor_onboarding/storage/store.py
- src/vendor_onboarding/storage/query.py
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import os
import stat
import sys
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from vendor_onboarding.models.record import CanonicalRecord
from vendor_onboarding.storage.query import CANONICAL_AS_OF_SCHEMA, CANONICAL_AUDIT_SCHEMA
from vendor_onboarding.storage.store import (
    BitemporalStore,
    ImmutabilityError,
    StorageFactorReport,
)


def compute_sha256(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def run_scenario_1_immutability(tmpdir: Path) -> bool:
    print("\n" + "=" * 70)
    print("SCENARIO 1: Immutability Attack Prevention & Bit-for-Bit Hash Integrity")
    print("=" * 70)

    store = BitemporalStore(root_dir=tmpdir)
    tbl = pa.Table.from_pydict({
        "entity_id": ["AAPL", "MSFT"],
        "event_time": [datetime(2024, 1, 1, 12, tzinfo=UTC)] * 2,
        "knowledge_time": [datetime(2024, 1, 1, 14, tzinfo=UTC)] * 2,
        "metric": ["CLOSE_PRICE"] * 2,
        "value": [150.0, 380.0],
        "unit": ["USD"] * 2,
        "source_symbol": ["AAPL", "MSFT"],
        "source_symbol_type": ["TICKER"] * 2,
        "vendor_id": ["BLOOMBERG"] * 2,
        "dataset_id": ["EQ_PRICES"] * 2,
        "is_restatement": [False] * 2,
        "revision_seq": [1] * 2,
        "drop_id": ["DROP_001"] * 2,
    })

    t0 = time.perf_counter()
    rows = store.append(tbl, dataset_id="EQ_PRICES", drop_id="DROP_001")
    t_append = time.perf_counter() - t0
    parquet_path = tmpdir / "EQ_PRICES" / "drop_id=DROP_001" / "data.parquet"

    initial_sha = compute_sha256(parquet_path)
    initial_stat = parquet_path.stat()
    print(f"  [+] Initial partition drop created: {rows} rows written in {t_append*1000:.2f}ms")
    print(f"  [+] Parquet path: {parquet_path}")
    print(f"  [+] Initial SHA-256: {initial_sha}")
    print(f"  [+] File size: {initial_stat.st_size} bytes, mtime: {initial_stat.st_mtime}")

    # Attack 1: Overwrite identical records
    try:
        store.append(tbl, dataset_id="EQ_PRICES", drop_id="DROP_001")
        print("  [-] FAIL: Overwrite with identical records did not raise ImmutabilityError!")
        return False
    except ImmutabilityError as e:
        print(f"  [+] PASS: Attack 1 blocked with ImmutabilityError: {e}")

    # Attack 2: Overwrite with malicious/corrupted records
    tbl_malicious = pa.Table.from_pydict({
        "entity_id": ["AAPL", "MSFT"],
        "event_time": [datetime(2024, 1, 1, 12, tzinfo=UTC)] * 2,
        "knowledge_time": [datetime(2024, 1, 1, 14, tzinfo=UTC)] * 2,
        "metric": ["CLOSE_PRICE"] * 2,
        "value": [999999.0, -1.0],
        "unit": ["USD"] * 2,
        "source_symbol": ["AAPL", "MSFT"],
        "source_symbol_type": ["TICKER"] * 2,
        "vendor_id": ["EVIL_VENDOR"] * 2,
        "dataset_id": ["EQ_PRICES"] * 2,
        "is_restatement": [True] * 2,
        "revision_seq": [99] * 2,
        "drop_id": ["DROP_001"] * 2,
    })
    try:
        store.append(tbl_malicious, dataset_id="EQ_PRICES", drop_id="DROP_001")
        print("  [-] FAIL: Overwrite with malicious records did not raise ImmutabilityError!")
        return False
    except ImmutabilityError as e:
        print(f"  [+] PASS: Attack 2 blocked with ImmutabilityError: {e}")

    # Attack 3: CanonicalRecord sequence overwrite
    rec = CanonicalRecord(
        entity_id="AAPL",
        event_time=datetime(2024, 1, 1, 12, tzinfo=UTC),
        knowledge_time=datetime(2024, 1, 1, 14, tzinfo=UTC),
        value=0.0,
        unit="USD",
        source_symbol="AAPL",
        source_symbol_type="TICKER",
        vendor_id="VENDOR",
        dataset_id="EQ_PRICES",
        is_restatement=True,
        revision_seq=999,
        drop_id="DROP_001",
    )
    try:
        store.append([rec], dataset_id="EQ_PRICES")
        print("  [-] FAIL: Overwrite with CanonicalRecord sequence did not raise ImmutabilityError!")
        return False
    except ImmutabilityError as e:
        print(f"  [+] PASS: Attack 3 blocked with ImmutabilityError: {e}")

    # Checksum & metadata verification
    post_sha = compute_sha256(parquet_path)
    post_stat = parquet_path.stat()
    assert post_sha == initial_sha, "SHA-256 altered after attacks!"
    assert post_stat.st_size == initial_stat.st_size, "File size altered!"
    assert post_stat.st_mtime == initial_stat.st_mtime, "File mtime altered!"
    print(f"  [+] SHA-256 byte-for-byte post-attack verification: {post_sha} (MATCH)")
    return True


def run_scenario_2_permissions(tmpdir: Path) -> bool:
    print("\n" + "=" * 70)
    print("SCENARIO 2: Physical Permission Enforcement (0o444) & Direct Tampering")
    print("=" * 70)

    store = BitemporalStore(root_dir=tmpdir)
    tbl = pa.Table.from_pydict({
        "entity_id": ["AAPL"],
        "event_time": [datetime(2024, 1, 1, 12, tzinfo=UTC)],
        "knowledge_time": [datetime(2024, 1, 1, 14, tzinfo=UTC)],
        "metric": ["CLOSE_PRICE"],
        "value": [150.0],
        "unit": ["USD"],
        "source_symbol": ["AAPL"],
        "source_symbol_type": ["TICKER"],
        "vendor_id": ["BLOOMBERG"],
        "dataset_id": ["PERM_TEST"],
        "is_restatement": [False],
        "revision_seq": [1],
        "drop_id": ["DROP_001"],
    })
    store.append(tbl, dataset_id="PERM_TEST", drop_id="DROP_001")
    pfile = tmpdir / "PERM_TEST" / "drop_id=DROP_001" / "data.parquet"

    mode = stat.S_IMODE(pfile.stat().st_mode)
    print(f"  [+] File permissions mode: {oct(mode)} (expected: 0o444)")
    assert mode == 0o444, f"Mode {oct(mode)} != 0o444"
    assert not (mode & stat.S_IWUSR), "Owner write permission must be disabled!"
    assert not (mode & stat.S_IWGRP), "Group write permission must be disabled!"
    assert not (mode & stat.S_IWOTH), "Other write permission must be disabled!"

    # Tampering test 1: open with 'wb'
    try:
        with open(pfile, "wb") as f:
            f.write(b"tamper_wb")
        print("  [-] FAIL: Direct open('wb') succeeded on 0o444 file!")
        return False
    except PermissionError as e:
        print(f"  [+] PASS: open('wb') blocked by OS: {e}")

    # Tampering test 2: open with 'a'
    try:
        with open(pfile, "a") as f:
            f.write("tamper_a")
        print("  [-] FAIL: Direct open('a') succeeded!")
        return False
    except PermissionError as e:
        print(f"  [+] PASS: open('a') blocked by OS: {e}")

    # Tampering test 3: PyArrow direct write
    try:
        pq.write_table(tbl, pfile)
        print("  [-] FAIL: pq.write_table direct overwrite succeeded!")
        return False
    except (PermissionError, OSError) as e:
        print(f"  [+] PASS: pq.write_table direct write blocked: {e}")

    return True


def run_scenario_3_concurrency(tmpdir: Path) -> bool:
    print("\n" + "=" * 70)
    print("SCENARIO 3: Multi-Threaded Concurrent Reads & Non-Blocking Appends")
    print("=" * 70)

    store = BitemporalStore(root_dir=tmpdir)

    # Prepopulate with 5 drops
    for d in range(1, 6):
        n = 100
        tbl = pa.Table.from_pydict({
            "entity_id": [f"SYM_{i:03d}" for i in range(n)],
            "event_time": [datetime(2024, 1, d, 12, tzinfo=UTC)] * n,
            "knowledge_time": [datetime(2024, 1, d, 14, tzinfo=UTC)] * n,
            "metric": ["CLOSE_PRICE"] * n,
            "value": [100.0 + d + i * 0.1 for i in range(n)],
            "unit": ["USD"] * n,
            "source_symbol": [f"SYM_{i:03d}" for i in range(n)],
            "source_symbol_type": ["TICKER"] * n,
            "vendor_id": ["BLOOMBERG"] * n,
            "dataset_id": ["CONCURRENCY_DS"] * n,
            "is_restatement": [False] * n,
            "revision_seq": [1] * n,
            "drop_id": [f"DROP_{d:03d}"] * n,
        })
        store.append(tbl, dataset_id="CONCURRENCY_DS", drop_id=f"DROP_{d:03d}")

    # Test 3A: 16 concurrent reader threads across independent DuckDB connections
    NUM_WORKERS = 16
    QUERIES_PER_WORKER = 15
    TOTAL_QUERIES = NUM_WORKERS * QUERIES_PER_WORKER

    def read_worker(worker_id: int) -> list[int]:
        counts = []
        with BitemporalStore(root_dir=tmpdir) as thread_store:
            for q in range(QUERIES_PER_WORKER):
                # As of day 3, 3 event dates * 100 symbols = 300 rows
                res = thread_store.as_of_query(
                    "CONCURRENCY_DS", as_of_time=datetime(2024, 1, 3, 23, tzinfo=UTC)
                )
                counts.append(res.num_rows)
        return counts

    t0 = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKERS) as pool:
        futures = [pool.submit(read_worker, i) for i in range(NUM_WORKERS)]
        results = [f.result() for f in futures]
    t_concur = time.perf_counter() - t0

    all_counts = [c for r in results for c in r]
    print(f"  [+] Executed {TOTAL_QUERIES} concurrent queries across {NUM_WORKERS} threads in {t_concur:.3f}s")
    print(f"  [+] Throughput: {TOTAL_QUERIES / t_concur:.1f} queries/sec")
    assert len(all_counts) == TOTAL_QUERIES
    assert all(c == 300 for c in all_counts), f"Count discrepancy: {set(all_counts)}"
    print("  [+] PASS: 100% deterministic results across all threads with 0 lock contention")

    # Test 3B: Concurrent Read-While-Writing (Atomic Write Isolation)
    print("  [+] Testing concurrent reads while writer is appending new drops...")
    errors: list[Exception] = []
    read_counts = []

    def reader_loop():
        for _ in range(30):
            try:
                with BitemporalStore(root_dir=tmpdir) as ts:
                    res = ts.as_of_query(
                        "CONCURRENCY_DS", as_of_time=datetime(2024, 1, 1, 15, tzinfo=UTC)
                    )
                    read_counts.append(res.num_rows)
            except Exception as e:  # noqa: BLE001
                errors.append(e)
            time.sleep(0.005)

    def writer_loop():
        for d in range(6, 10):
            time.sleep(0.01)
            tbl_new = pa.Table.from_pydict({
                "entity_id": ["SYM_001"],
                "event_time": [datetime(2024, 1, d, 12, tzinfo=UTC)],
                "knowledge_time": [datetime(2024, 1, d, 14, tzinfo=UTC)],
                "metric": ["CLOSE_PRICE"],
                "value": [200.0],
                "unit": ["USD"],
                "source_symbol": ["SYM_001"],
                "source_symbol_type": ["TICKER"],
                "vendor_id": ["BLOOMBERG"],
                "dataset_id": ["CONCURRENCY_DS"],
                "is_restatement": [False],
                "revision_seq": [1],
                "drop_id": [f"DROP_{d:03d}"],
            })
            try:
                store.append(tbl_new, dataset_id="CONCURRENCY_DS", drop_id=f"DROP_{d:03d}")
            except Exception as e:  # noqa: BLE001
                errors.append(e)

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        rf = [pool.submit(reader_loop) for _ in range(5)]
        wf = pool.submit(writer_loop)
        wf.result()
        for f in rf:
            f.result()

    assert not errors, f"Errors during concurrent read-write: {errors}"
    assert all(c == 100 for c in read_counts), f"Inconsistent read counts: {set(read_counts)}"
    print(f"  [+] PASS: Read-while-write atomic isolation verified ({len(read_counts)} queries, 0 errors)")
    return True


def run_scenario_4_storage_expansion(tmpdir: Path) -> bool:
    print("\n" + "=" * 70)
    print("SCENARIO 4: Storage Expansion Factor SLA Compliance (<= 1.30x)")
    print("=" * 70)

    store = BitemporalStore(root_dir=tmpdir)

    # 4A: Baseline dataset (1 drop) -> expansion factor ~ 1.0x
    n = 2000
    dates = [datetime(2024, 1, 1, tzinfo=UTC) + timedelta(days=j) for j in range(n)]
    tbl_base = pa.Table.from_pydict({
        "entity_id": ["AAPL"] * n,
        "event_time": dates,
        "knowledge_time": [datetime(2024, 1, 1, tzinfo=UTC)] * n,
        "metric": ["CLOSE_PRICE"] * n,
        "value": [150.0 + j for j in range(n)],
        "unit": ["USD"] * n,
        "source_symbol": ["AAPL"] * n,
        "source_symbol_type": ["TICKER"] * n,
        "vendor_id": ["BLOOMBERG"] * n,
        "dataset_id": ["SLA_BASE"] * n,
        "is_restatement": [False] * n,
        "revision_seq": [1] * n,
        "drop_id": ["drop_001"] * n,
    })
    store.append(tbl_base, dataset_id="SLA_BASE", drop_id="drop_001")
    rep_base = store.storage_factor("SLA_BASE")
    print(f"  [+] Single drop: physical={rep_base.total_bitemporal_bytes}B, dedup={rep_base.deduplicated_bytes}B, factor={rep_base.expansion_factor:.4f}x, meets_sla={rep_base.meets_sla}")
    assert rep_base.meets_sla, "Single drop must satisfy SLA"

    # 4B: Low restatement dataset (5% restatements across drops) -> meets_sla == True
    dates_part2 = dates[:100] + [datetime(2024, 6, 1, tzinfo=UTC) + timedelta(days=j) for j in range(100)]
    tbl_part2 = pa.Table.from_pydict({
        "entity_id": ["AAPL"] * 200,
        "event_time": dates_part2,
        "knowledge_time": [datetime(2024, 1, 2, tzinfo=UTC)] * 200,
        "metric": ["CLOSE_PRICE"] * 200,
        "value": [155.0 + j for j in range(200)],
        "unit": ["USD"] * 200,
        "source_symbol": ["AAPL"] * 200,
        "source_symbol_type": ["TICKER"] * 200,
        "vendor_id": ["BLOOMBERG"] * 200,
        "dataset_id": ["SLA_BASE"] * 200,
        "is_restatement": [True] * 100 + [False] * 100,
        "revision_seq": [2] * 100 + [1] * 100,
        "drop_id": ["drop_002"] * 200,
    })
    store.append(tbl_part2, dataset_id="SLA_BASE", drop_id="drop_002")
    rep_low = store.storage_factor("SLA_BASE")
    print(f"  [+] Low restatement (5%): physical={rep_low.total_bitemporal_bytes}B, dedup={rep_low.deduplicated_bytes}B, factor={rep_low.expansion_factor:.4f}x, meets_sla={rep_low.meets_sla}")
    assert rep_low.meets_sla, "Low restatements must satisfy SLA (<= 1.30x)"

    # 4C: Heavy restatement dataset (4 drops restating 100% of rows) -> factor > 2.0x, meets_sla == False
    for drop_i in range(4):
        tbl_h = pa.Table.from_pydict({
            "entity_id": ["AAPL"] * 1000,
            "event_time": dates[:1000],
            "knowledge_time": [datetime(2024, 1, 1 + drop_i, tzinfo=UTC)] * 1000,
            "metric": ["CLOSE_PRICE"] * 1000,
            "value": [150.0 + drop_i * 10.0 for _ in range(1000)],
            "unit": ["USD"] * 1000,
            "source_symbol": ["AAPL"] * 1000,
            "source_symbol_type": ["TICKER"] * 1000,
            "vendor_id": ["BLOOMBERG"] * 1000,
            "dataset_id": ["SLA_HEAVY"] * 1000,
            "is_restatement": [drop_i > 0] * 1000,
            "revision_seq": [drop_i + 1] * 1000,
            "drop_id": [f"drop_{drop_i}"] * 1000,
        })
        store.append(tbl_h, dataset_id="SLA_HEAVY", drop_id=f"drop_{drop_i}")

    rep_heavy = store.storage_factor("SLA_HEAVY")
    print(f"  [+] Heavy restatement (4x): physical={rep_heavy.total_bitemporal_bytes}B, dedup={rep_heavy.deduplicated_bytes}B, factor={rep_heavy.expansion_factor:.4f}x, meets_sla={rep_heavy.meets_sla}")
    assert not rep_heavy.meets_sla, "Heavy restatements must fail SLA (> 1.30x)"
    assert rep_heavy.expansion_factor > 2.0

    # 4D: Non-existent dataset boundary
    rep_missing = store.storage_factor("DOES_NOT_EXIST")
    print(f"  [+] Missing dataset: physical={rep_missing.total_bitemporal_bytes}B, dedup={rep_missing.deduplicated_bytes}B, factor={rep_missing.expansion_factor:.2f}x, meets_sla={rep_missing.meets_sla}")
    assert rep_missing.expansion_factor == 1.0
    assert rep_missing.meets_sla

    return True


def run_scenario_5_boundaries_and_extremes(tmpdir: Path) -> bool:
    print("\n" + "=" * 70)
    print("SCENARIO 5: Boundary Values (0-row, Microsecond Resolution, DST, 50 Revisions)")
    print("=" * 70)

    store = BitemporalStore(root_dir=tmpdir)

    # 5A: 0-row table append and query
    empty_tbl = pa.Table.from_pydict({
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
    rows = store.append(empty_tbl, dataset_id="EMPTY_DS", drop_id="DROP_ZERO")
    assert rows == 0
    q_empty = store.as_of_query("EMPTY_DS", datetime.now(UTC))
    assert q_empty.num_rows == 0
    print("  [+] PASS: 0-row table append (0 rows written) & query (0 rows returned)")

    # 5B: Microsecond precision
    t_ev = datetime(2024, 1, 1, 9, 30, 0, tzinfo=UTC)
    t_k1 = datetime(2024, 1, 1, 10, 0, 0, 100, tzinfo=UTC)
    t_k2 = datetime(2024, 1, 1, 10, 0, 0, 101, tzinfo=UTC)  # +1 microsecond

    store.append(
        pa.Table.from_pydict({
            "entity_id": ["AAPL"], "event_time": [t_ev], "knowledge_time": [t_k1],
            "metric": ["PRICE"], "value": [100.0], "unit": ["USD"], "source_symbol": ["AAPL"],
            "source_symbol_type": ["TICKER"], "vendor_id": ["V"], "dataset_id": ["US_RES"],
            "is_restatement": [False], "revision_seq": [1], "drop_id": ["d1"]
        }), "US_RES", "d1"
    )
    store.append(
        pa.Table.from_pydict({
            "entity_id": ["AAPL"], "event_time": [t_ev], "knowledge_time": [t_k2],
            "metric": ["PRICE"], "value": [105.0], "unit": ["USD"], "source_symbol": ["AAPL"],
            "source_symbol_type": ["TICKER"], "vendor_id": ["V"], "dataset_id": ["US_RES"],
            "is_restatement": [True], "revision_seq": [2], "drop_id": ["d2"]
        }), "US_RES", "d2"
    )

    q0 = store.as_of_query("US_RES", datetime(2024, 1, 1, 10, 0, 0, 99, tzinfo=UTC))
    q1 = store.as_of_query("US_RES", t_k1)
    q2 = store.as_of_query("US_RES", t_k2)
    assert q0.num_rows == 0
    assert q1["value"][0].as_py() == 100.0
    assert q2["value"][0].as_py() == 105.0
    print("  [+] PASS: Microsecond timestamp resolution (1us delta strictly distinguished)")

    # 5C: DST transition with fold=0 and fold=1
    ny_tz = ZoneInfo("America/New_York")
    t_edt = datetime(2024, 11, 3, 1, 30, 0, fold=0, tzinfo=ny_tz)  # UTC 05:30
    t_est = datetime(2024, 11, 3, 1, 30, 0, fold=1, tzinfo=ny_tz)  # UTC 06:30
    store.append(
        pa.Table.from_pydict({
            "entity_id": ["AAPL"], "event_time": [t_ev], "knowledge_time": [t_edt.astimezone(UTC)],
            "metric": ["PRICE"], "value": [200.0], "unit": ["USD"], "source_symbol": ["AAPL"],
            "source_symbol_type": ["TICKER"], "vendor_id": ["V"], "dataset_id": ["DST_DS"],
            "is_restatement": [False], "revision_seq": [1], "drop_id": ["d_edt"]
        }), "DST_DS", "d_edt"
    )
    store.append(
        pa.Table.from_pydict({
            "entity_id": ["AAPL"], "event_time": [t_ev], "knowledge_time": [t_est.astimezone(UTC)],
            "metric": ["PRICE"], "value": [210.0], "unit": ["USD"], "source_symbol": ["AAPL"],
            "source_symbol_type": ["TICKER"], "vendor_id": ["V"], "dataset_id": ["DST_DS"],
            "is_restatement": [True], "revision_seq": [2], "drop_id": ["d_est"]
        }), "DST_DS", "d_est"
    )
    r_edt = store.as_of_query("DST_DS", t_edt)
    r_est = store.as_of_query("DST_DS", t_est)
    assert r_edt["value"][0].as_py() == 200.0
    assert r_est["value"][0].as_py() == 210.0
    print("  [+] PASS: DST transitions correctly normalized to UTC without fold ambiguity")

    # 5D: Deep 50-revision trajectory monotonicity
    for rev in range(1, 51):
        store.append(
            pa.Table.from_pydict({
                "entity_id": ["GOOG"], "event_time": [t_ev],
                "knowledge_time": [datetime(2024, 1, 2, tzinfo=UTC) + timedelta(minutes=rev)],
                "metric": ["PRICE"], "value": [1000.0 + rev], "unit": ["USD"],
                "source_symbol": ["GOOG"], "source_symbol_type": ["TICKER"],
                "vendor_id": ["V"], "dataset_id": ["REV_DS"], "is_restatement": [rev > 1],
                "revision_seq": [rev], "drop_id": [f"drop_{rev:03d}"]
            }), "REV_DS", f"drop_{rev:03d}"
        )
    trail = store.audit_trail("REV_DS", "GOOG", t_ev, metric="PRICE")
    assert trail.num_rows == 50
    assert trail["revision_seq"].to_pylist() == list(range(1, 51))
    print(f"  [+] PASS: Deep 50-revision audit trail: strictly monotonic from seq 1 to 50")

    return True


def run_scenario_6_scale_benchmark(tmpdir: Path) -> bool:
    print("\n" + "=" * 70)
    print("SCENARIO 6: Large-Scale Append & PIT Query Performance (50,000 Records)")
    print("=" * 70)

    store = BitemporalStore(root_dir=tmpdir)
    n = 50_000
    dates = [datetime(2024, 1, 1, tzinfo=UTC) + timedelta(minutes=j % 1440) for j in range(n)]
    entities = [f"SYM_{j % 500:04d}" for j in range(n)]

    t0 = time.perf_counter()
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
        "dataset_id": ["SCALE_BENCH"] * n,
        "is_restatement": [False] * n,
        "revision_seq": [1] * n,
        "drop_id": ["drop_bench"] * n,
    })
    t_gen = time.perf_counter() - t0

    t_write0 = time.perf_counter()
    rows = store.append(tbl, "SCALE_BENCH", "drop_bench")
    t_write = time.perf_counter() - t_write0

    print(f"  [+] Appended {rows} records in {t_write:.3f}s ({rows / t_write:,.0f} rows/sec)")
    assert rows == n
    assert t_write < 1.5, f"Write too slow: {t_write}s"

    t_query0 = time.perf_counter()
    res = store.as_of_query(
        "SCALE_BENCH",
        as_of_time=datetime(2024, 1, 3, tzinfo=UTC),
        entity_ids=["SYM_0042", "SYM_0123"],
    )
    t_query = time.perf_counter() - t_query0
    print(f"  [+] Filtered point-in-time query returned {res.num_rows} rows in {t_query*1000:.2f}ms")
    # 2 entities * 72 distinct minute event times (modulo 500 and 1440 alignment) = 144 deduplicated rows
    assert res.num_rows == 144
    assert t_query < 0.5, f"Query too slow: {t_query}s"

    return True


def main() -> None:
    print("=" * 70)
    print("MILESTONE 3 CHALLENGER 1: ADVERSARIAL STRESS SUITE")
    print("=" * 70)

    start_time = time.perf_counter()
    with tempfile.TemporaryDirectory() as raw_tmp:
        tmpdir = Path(raw_tmp)

        scenarios = [
            ("Scenario 1: Immutability Attack", run_scenario_1_immutability),
            ("Scenario 2: Permission Enforcement", run_scenario_2_permissions),
            ("Scenario 3: Concurrency Stress", run_scenario_3_concurrency),
            ("Scenario 4: Storage Expansion Factor", run_scenario_4_storage_expansion),
            ("Scenario 5: Boundaries & Extremes", run_scenario_5_boundaries_and_extremes),
            ("Scenario 6: Scale Benchmark", run_scenario_6_scale_benchmark),
        ]

        passed = 0
        failed = 0
        for name, fn in scenarios:
            try:
                ok = fn(tmpdir)
                if ok:
                    passed += 1
                else:
                    failed += 1
            except Exception as e:
                print(f"  [-] EXCEPTION in {name}: {type(e).__name__}: {e}")
                failed += 1

    total_time = time.perf_counter() - start_time
    print("\n" + "=" * 70)
    print(f"FINAL ADVERSARIAL CHALLENGER VERDICT: {'ALL PASS' if failed == 0 else 'FAIL'}")
    print(f"Passed: {passed}/{len(scenarios)} in {total_time:.2f}s")
    print("=" * 70)

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
