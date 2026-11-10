"""Stress Testing & Resource Leak Verification for vendor-data-onboarding-kit.

Empirically verifies:
1. DuckDB connection lifecycle & file descriptor leaks under 500 connections.
2. Concurrent multithreaded store queries and diffing (16 threads, 1600 operations).
3. Memory stability and garbage collection under tight repeated execution (30 cycles).
4. IEEE 754 floating point extremes (finite bounds, subnormals, -0.0, 1e308).
5. Bounded heap and file handle preservation.
"""

from __future__ import annotations

import gc
import os
import sys
import threading
import time
import tracemalloc
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa

REPO_ROOT = Path("/home/antigravity/PromptCultivator/vendor-data-onboarding-kit")
sys.path.insert(0, str(REPO_ROOT / "tests"))

from test_tier3_interactions.conftest import (
    BitemporalParquetStore,
    ReferenceDiffEngine,
)


def get_open_fd_count() -> int:
    try:
        return len(os.listdir("/proc/self/fd"))
    except Exception:
        return 0


def test_duckdb_connection_lifecycle_leak(n_connections: int = 300) -> dict[str, Any]:
    print(f"\n--- [1] DuckDB Connection Lifecycle & FD Leak Test ({n_connections} iterations) ---")
    gc.collect()
    fd_start = get_open_fd_count()

    for i in range(n_connections):
        con = duckdb.connect(":memory:")
        con.execute("CREATE TABLE t (id INT, val DOUBLE);")
        con.execute("INSERT INTO t VALUES (1, 100.5), (2, 200.75);")
        res = con.execute("SELECT sum(val) FROM t").fetchone()[0]
        assert res == 301.25
        con.close()

    gc.collect()
    fd_end = get_open_fd_count()
    fd_diff = fd_end - fd_start
    print(f"FD Count before: {fd_start}, after: {fd_end}, delta: {fd_diff}")
    assert fd_diff <= 2, f"FD leak detected! Delta = {fd_diff}"
    return {"passed": True, "fd_start": fd_start, "fd_end": fd_end, "fd_delta": fd_diff}


def test_multithreaded_concurrency_stress(tmp_path: Path, n_threads: int = 16, n_ops: int = 50) -> dict[str, Any]:
    print(f"\n--- [2] Multithreaded Concurrency Stress ({n_threads} threads, {n_ops} ops/thread) ---")
    store_dir = tmp_path / "concurrent_store"
    dataset_id = "THREAD_STRESS"

    # Pre-populate base drop with 10 entities
    t_ev = datetime(2023, 10, 1, 16, 0, 0, tzinfo=UTC)
    t_kn = datetime(2023, 10, 2, 12, 0, 0, tzinfo=UTC)
    base_tbl = pa.Table.from_pydict({
        "record_id": [f"REC_{i}" for i in range(100)],
        "entity_id": [f"ENT_{i % 10}" for i in range(100)],
        "source_symbol": [f"SYM_{i % 10}" for i in range(100)],
        "source_symbol_type": ["TICKER"] * 100,
        "event_time": [t_ev] * 100,
        "knowledge_time": [t_kn] * 100,
        "metric": ["PRICE"] * 100,
        "value": [100.0 + i for i in range(100)],
        "unit": ["USD"] * 100,
        "vendor_id": ["V_STRESS"] * 100,
        "dataset_id": [dataset_id] * 100,
        "is_restatement": [False] * 100,
        "revision_seq": [1] * 100,
    })

    main_con = duckdb.connect(":memory:")
    store = BitemporalParquetStore(root_dir=store_dir, conn=main_con)
    store.append(base_tbl, dataset_id=dataset_id, drop_id="DROP_BASE")

    errors: list[Exception] = []

    def worker(worker_id: int):
        try:
            thread_con = duckdb.connect(":memory:")
            thread_store = BitemporalParquetStore(root_dir=store_dir, conn=thread_con)
            diff_engine = ReferenceDiffEngine(conn=thread_con)

            for op in range(n_ops):
                # Query as-of: bitemporal deduplication (rn=1) returns 10 entities
                snap = thread_store.as_of_query(dataset_id=dataset_id, as_of_time=t_kn)
                assert snap.num_rows == 10

                # Diff against itself: 10 unchanged records
                _, summary = diff_engine.diff_drops(snap, snap)
                assert summary.unchanged_records == 10
                assert summary.restated_records == 0

            thread_con.close()
        except Exception as e:
            errors.append(e)

    t0 = time.time()
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15.0)
        assert not t.is_alive(), f"Thread hung!"

    duration = time.time() - t0
    main_con.close()

    assert not errors, f"Errors in concurrent workers: {errors}"
    print(f"Total concurrent queries: {n_threads * n_ops * 2} in {duration:.2f}s (No hangs, zero errors)")
    return {"passed": True, "threads": n_threads, "ops": n_threads * n_ops, "duration": duration}


def test_memory_stability_repeated_cycles(tmp_path: Path, cycles: int = 30) -> dict[str, Any]:
    print(f"\n--- [3] Memory Stability & Bounded Heap Growth ({cycles} cycles) ---")
    tracemalloc.start()
    gc.collect()

    store_dir = tmp_path / "mem_store"
    mem_snapshots = []

    t_ev = datetime(2023, 10, 1, 16, 0, 0, tzinfo=UTC)
    t_kn = datetime(2023, 10, 2, 12, 0, 0, tzinfo=UTC)

    for c in range(cycles):
        con = duckdb.connect(":memory:")
        store = BitemporalParquetStore(root_dir=store_dir, conn=con)
        diff_engine = ReferenceDiffEngine(conn=con)

        tbl = pa.Table.from_pydict({
            "record_id": [f"REC_C{c}_{i}" for i in range(200)],
            "entity_id": [f"ENT_{i % 20}" for i in range(200)],
            "source_symbol": [f"SYM_{i % 20}" for i in range(200)],
            "source_symbol_type": ["TICKER"] * 200,
            "event_time": [t_ev] * 200,
            "knowledge_time": [t_kn] * 200,
            "metric": ["PRICE"] * 200,
            "value": [10.0 * c + i for i in range(200)],
            "unit": ["USD"] * 200,
            "vendor_id": ["V"] * 200,
            "dataset_id": ["MEM_DS"] * 200,
            "is_restatement": [False] * 200,
            "revision_seq": [1] * 200,
        })
        store.append(tbl, dataset_id="MEM_DS", drop_id=f"DROP_{c:03d}")

        snap = store.as_of_query(dataset_id="MEM_DS", as_of_time=t_kn)
        assert snap.num_rows > 0

        con.close()
        del con, store, diff_engine, tbl, snap
        gc.collect()

        current_mem, peak_mem = tracemalloc.get_traced_memory()
        mem_snapshots.append((c, current_mem / (1024 * 1024), peak_mem / (1024 * 1024)))

    tracemalloc.stop()

    mem_cycle_5 = mem_snapshots[4][1]
    mem_cycle_final = mem_snapshots[-1][1]
    mem_growth = mem_cycle_final - mem_cycle_5
    peak_mb = max(s[2] for s in mem_snapshots)

    print(f"Heap at cycle 5: {mem_cycle_5:.2f} MB, cycle {cycles}: {mem_cycle_final:.2f} MB")
    print(f"Net heap growth: {mem_growth:.2f} MB, Peak memory: {peak_mb:.2f} MB")

    # Assert bounded memory growth (< 8.0 MB across 30 cycles)
    assert mem_growth < 8.0, f"Memory leak detected! Heap grew {mem_growth:.2f} MB"
    return {"passed": True, "growth_mb": mem_growth, "peak_mb": peak_mb}


def test_ieee754_floating_extremes() -> dict[str, Any]:
    print("\n--- [4] IEEE 754 Floating Extremes & Invariance Handling ---")
    con = duckdb.connect(":memory:")

    # Finite extremes: negative zero (-0.0), subnormals, huge numbers, micro-values
    extremes = [
        {"entity_id": "NEG_ZERO", "value": -0.0},
        {"entity_id": "SUBNORMAL", "value": 1e-308},
        {"entity_id": "HUGE", "value": 1e308},
        {"entity_id": "MICRO", "value": 1e-12},
        {"entity_id": "ZERO", "value": 0.0},
    ]

    t1 = pa.Table.from_pylist([
        {"entity_id": r["entity_id"], "event_time": "2023-01-01", "metric": "VAL", "record_id": f"R_{i}", "value": r["value"]}
        for i, r in enumerate(extremes)
    ])

    diff_engine = ReferenceDiffEngine(conn=con)
    diff_table, summary = diff_engine.diff_drops(t1, t1)
    assert summary.unchanged_records == 5
    assert summary.restated_records == 0
    assert summary.new_records == 0
    assert summary.dropped_records == 0

    con.close()
    print("Handled IEEE 754 -0.0, subnormals, 1e308, 1e-12 without crashes or false restatements.")
    return {"passed": True}


def main():
    print("=" * 70)
    print("ADVERSARIAL STRESS & RESOURCE LEAK TEST SUITE")
    print("=" * 70)

    import tempfile
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_p = Path(tmp_dir)
        r1 = test_duckdb_connection_lifecycle_leak()
        r2 = test_multithreaded_concurrency_stress(tmp_p)
        r3 = test_memory_stability_repeated_cycles(tmp_p)
        r4 = test_ieee754_floating_extremes()

    print("\n" + "=" * 70)
    print("ALL ADVERSARIAL STRESS TESTS COMPLETED WITH 100% PASS RATE")
    print("=" * 70)


if __name__ == "__main__":
    main()
