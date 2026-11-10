"""Unit tests for point-in-time Store, BruteForceStore, AuditLogger, and LeakDetector."""

from __future__ import annotations

import concurrent.futures
import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest

from vdok.normalize import CANONICAL_ARROW_SCHEMA
from vdok.query import (
    AuditLogger,
    BruteForceStore,
    LeakReport,
    Store,
    StoreRecord,
    detect_leaks,
)
from vdok.store import write_partitioned_store


def _seed_store_data(store_path: Path) -> None:
    # Key 1: Apple 2023-12-31, planted revision from 100 to 110
    ev1 = datetime(2023, 12, 31, tzinfo=timezone.utc)
    ev2 = datetime(2023, 9, 30, tzinfo=timezone.utc)
    t1 = datetime(2024, 1, 15, tzinfo=timezone.utc)
    t2 = datetime(2024, 2, 15, tzinfo=timezone.utc)

    # Drop 1: initial values
    d1 = {
        "entity_id": ["0000320193", "0000320193"],
        "entity_id_type": ["cik", "cik"],
        "field": ["Revenues", "Revenues"],
        "event_time": [ev1, ev2],
        "knowledge_time": [t1, t1],
        "value_num": [Decimal("100.0"), Decimal("90.0")],
        "value_str": [None, None],
        "unit": ["USD", "USD"],
        "drop_id": ["drop_1", "drop_1"],
        "source_file": ["f.csv", "f.csv"],
        "source_row": [1, 2],
        "row_hash": ["h1", "h2"],
        "unmapped": [False, False],
    }
    write_partitioned_store(
        pa.Table.from_pydict(d1, schema=CANONICAL_ARROW_SCHEMA),
        "sec_fsds",
        "drop_1",
        base_path=store_path,
    )

    # Drop 2: revision for ev1 (100 -> 110)
    d2 = {
        "entity_id": ["0000320193"],
        "entity_id_type": ["cik"],
        "field": ["Revenues"],
        "event_time": [ev1],
        "knowledge_time": [t2],
        "value_num": [Decimal("110.0")],
        "value_str": [None],
        "unit": ["USD"],
        "drop_id": ["drop_2"],
        "source_file": ["f.csv"],
        "source_row": [1],
        "row_hash": ["h3"],
        "unmapped": [False],
    }
    write_partitioned_store(
        pa.Table.from_pydict(d2, schema=CANONICAL_ARROW_SCHEMA),
        "sec_fsds",
        "drop_2",
        base_path=store_path,
    )


def test_store_record_subscript_and_attributes() -> None:
    """Verify StoreRecord supports both dictionary keys and attribute access."""
    rec = StoreRecord({"entity_id": "AAPL", "value_num": Decimal("150.0")})
    assert rec["entity_id"] == "AAPL"
    assert rec.entity_id == "AAPL"
    assert rec["value_num"] == Decimal("150.0")
    assert rec.value_num == Decimal("150.0")

    with pytest.raises(KeyError):
        _ = rec["nonexistent"]

    with pytest.raises(AttributeError):
        _ = rec.nonexistent


def test_store_point_query_and_g2(tmp_path: Path) -> None:
    """Verify Store.value point lookup and strict Guarantee 2 enforcement."""
    vendor_store = tmp_path / "sec_fsds"
    _seed_store_data(tmp_path)

    ev1 = "2023-12-31"

    with Store(vendor_store) as s:
        # Before drop 1 knowledge time: must return None
        res_before = s.value("0000320193", "Revenues", ev1, as_of="2024-01-01")
        assert res_before is None

        # As of drop 1 (between 2024-01-15 and 2024-02-15): must return 100.0
        res_d1 = s.value("0000320193", "Revenues", ev1, as_of="2024-01-20")
        assert res_d1 is not None
        assert res_d1["value_num"] == Decimal("100.000000000")
        assert res_d1["drop_id"] == "drop_1"

        # As of drop 2 (on or after 2024-02-15): must return 110.0
        res_d2 = s.value("0000320193", "Revenues", ev1, as_of="2024-02-20")
        assert res_d2 is not None
        assert res_d2["value_num"] == Decimal("110.000000000")
        assert res_d2["drop_id"] == "drop_2"

        # Missing entity returns None
        res_missing = s.value("9999999999", "Revenues", ev1, as_of="2024-02-20")
        assert res_missing is None


def test_store_series_query(tmp_path: Path) -> None:
    """Verify Store.series returns sorted observations with date filtering."""
    vendor_store = tmp_path / "sec_fsds"
    _seed_store_data(tmp_path)

    with Store(vendor_store) as s:
        # Query series as of 2024-01-20 (prior to drop 2 revision)
        series_d1 = s.series("0000320193", "Revenues", as_of="2024-01-20")
        assert len(series_d1) == 2
        # Chronological order: 2023-09-30 then 2023-12-31
        assert series_d1[0]["value_num"] == Decimal("90.000000000")
        assert series_d1[1]["value_num"] == Decimal("100.000000000")

        # Query series as of 2024-02-20 (after drop 2 revision)
        series_d2 = s.series("0000320193", "Revenues", as_of="2024-02-20")
        assert len(series_d2) == 2
        assert series_d2[1]["value_num"] == Decimal("110.000000000")

        # Range filter [2023-11-01, 2024-01-01]
        filtered = s.series(
            "0000320193",
            "Revenues",
            as_of="2024-02-20",
            start="2023-11-01",
            end="2024-01-01",
        )
        assert len(filtered) == 1
        assert filtered[0]["value_num"] == Decimal("110.000000000")


def test_bruteforce_store_equivalence(tmp_path: Path) -> None:
    """Verify BruteForceStore oracle produces identical results to Store."""
    vendor_store = tmp_path / "sec_fsds"
    _seed_store_data(tmp_path)

    s = Store(vendor_store)
    bf = BruteForceStore(vendor_store)
    try:
        ev1 = "2023-12-31"

        # Compare value queries across three different as_of dates
        dates = ["2024-01-01", "2024-01-20", "2024-02-20"]
        for dt in dates:
            v_store = s.value("0000320193", "Revenues", ev1, as_of=dt)
            v_bf = bf.value("0000320193", "Revenues", ev1, as_of=dt)

            if v_store is None:
                assert v_bf is None
            else:
                assert v_bf is not None
                assert v_store["value_num"] == v_bf["value_num"]
                assert v_store["knowledge_time"] == v_bf["knowledge_time"]
                assert v_store["drop_id"] == v_bf["drop_id"]

        # Compare series queries
        ser_store = s.series("0000320193", "Revenues", as_of="2024-02-20")
        ser_bf = bf.series("0000320193", "Revenues", as_of="2024-02-20")
        assert len(ser_store) == len(ser_bf)
        for r_s, r_bf in zip(ser_store, ser_bf):
            assert r_s["value_num"] == r_bf["value_num"]
            assert r_s["event_time"] == r_bf["event_time"]
    finally:
        s.close()


def test_audit_logger_concurrent_writes(tmp_path: Path) -> None:
    """Verify thread-safe concurrent audit log emission."""
    log_file = tmp_path / "audit.jsonl"
    logger = AuditLogger(log_file, session_id="test_session")

    def _worker(worker_id: int) -> None:
        for i in range(50):
            logger.log_read(
                decision_time="2024-01-01T09:30:00Z",
                as_of="2024-01-01T09:30:00Z",
                key=f"worker_{worker_id}_key_{i}",
                knowledge_time_returned="2023-12-31T20:00:00Z",
            )

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(_worker, wid) for wid in range(8)]
        for f in futures:
            f.result()

    lines = log_file.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 8 * 50
    # Every line must parse as valid JSON
    for line in lines:
        entry = json.loads(line)
        assert entry["session_id"] == "test_session"


def test_detect_leaks_categorization(tmp_path: Path) -> None:
    """Verify leak detector accurately identifies as-of and knowledge look-ahead leaks."""
    log_file = tmp_path / "audit_test.jsonl"
    entries = [
        # 1. Valid causal read: as_of <= decision_time, knowledge_time <= decision_time
        {
            "decision_time": "2024-03-01T09:30:00Z",
            "as_of": "2024-03-01T09:30:00Z",
            "key": "K1",
            "knowledge_time_returned": "2024-02-15T18:00:00Z",
        },
        # 2. As-Of leak: as_of > decision_time
        {
            "decision_time": "2024-03-01T09:30:00Z",
            "as_of": "2024-03-15T09:30:00Z",
            "key": "K2",
            "knowledge_time_returned": "2024-02-15T18:00:00Z",
        },
        # 3. Knowledge leak: knowledge_time_returned > decision_time
        {
            "decision_time": "2024-03-01T09:30:00Z",
            "as_of": "2024-03-01T09:30:00Z",
            "key": "K3",
            "knowledge_time_returned": "2024-03-05T12:00:00Z",
        },
        # 4. Both as_of and knowledge leak
        {
            "decision_time": "2024-03-01T09:30:00Z",
            "as_of": "2024-03-10T09:30:00Z",
            "key": "K4",
            "knowledge_time_returned": "2024-03-08T12:00:00Z",
        },
    ]

    with open(log_file, "w", encoding="utf-8") as f:
        for e in entries:
            f.write(json.dumps(e) + "\n")

    report: LeakReport = detect_leaks(log_file)
    assert report.total_reads == 4
    assert report.leaks_detected == 3
    assert report.as_of_leaks == 2  # entries 2 and 4
    assert report.knowledge_leaks == 2  # entries 3 and 4
    assert report.has_leaks is True
    assert len(report.leak_records) == 3
