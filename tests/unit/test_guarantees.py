"""Guarantee tests for G1 (append-only), G2 (no future knowledge), and G3 (idempotence)."""

from __future__ import annotations

import os
import stat
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from vdok.discover import check_idempotency
from vdok.normalize import CANONICAL_ARROW_SCHEMA
from vdok.query import Store
from vdok.revisions import REVISIONS_ARROW_SCHEMA, record_revisions
from vdok.store import (
    AppendOnlyViolationError,
    init_store_catalog,
    record_drop_metadata,
    write_partitioned_store,
)


def _create_sample_canonical_table(
    drop_id: str,
    entity_id: str = "0000320193",
    field: str = "Revenues",
    event_time: datetime | None = None,
    knowledge_time: datetime | None = None,
    value_num: Decimal = Decimal("100.0"),
) -> pa.Table:
    ev = event_time or datetime(2023, 12, 31, 0, 0, 0, tzinfo=timezone.utc)
    kt = knowledge_time or datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

    data = {
        "entity_id": [entity_id],
        "entity_id_type": ["cik"],
        "field": [field],
        "event_time": [ev],
        "knowledge_time": [kt],
        "value_num": [value_num],
        "value_str": [None],
        "unit": ["USD"],
        "drop_id": [drop_id],
        "source_file": ["data.csv"],
        "source_row": [1],
        "row_hash": [f"hash_{drop_id}_1"],
        "unmapped": [False],
    }
    return pa.Table.from_pydict(data, schema=CANONICAL_ARROW_SCHEMA)


def test_g1_store_is_append_only(tmp_path: Path) -> None:
    """Guarantee 1 (G1): The store is append-only. Partitions are immutable (0o444)."""
    drop_id = "drop_g1_001"
    table = _create_sample_canonical_table(drop_id=drop_id)

    written_paths = write_partitioned_store(
        table, vendor="sec_fsds", drop_id=drop_id, base_path=tmp_path
    )
    assert len(written_paths) == 1
    part_file = written_paths[0]
    assert part_file.exists()

    # POSIX permissions are read-only (0o444)
    file_mode = stat.S_IMODE(os.stat(part_file).st_mode)
    assert file_mode == 0o444

    # Direct overwrite attempt must raise AppendOnlyViolationError
    with pytest.raises(AppendOnlyViolationError) as exc_info:
        write_partitioned_store(table, vendor="sec_fsds", drop_id=drop_id, base_path=tmp_path)
    assert "Append-only violation" in str(exc_info.value)
    assert issubclass(AppendOnlyViolationError, FileExistsError)

    # Catalog record duplicate must also raise AppendOnlyViolationError
    now = datetime(2024, 1, 15, tzinfo=timezone.utc)
    record = {
        "drop_id": drop_id,
        "vendor": "sec_fsds",
        "status": "COMMITTED",
        "committed_at": now,
        "total_rows": 1,
        "valid_rows": 1,
        "quarantine_rows": 0,
        "total_bytes": 1024,
        "file_count": 1,
        "file_format": "csv",
        "compression": "none",
        "event_time_min": now,
        "event_time_max": now,
        "knowledge_time_min": now,
        "knowledge_time_max": now,
    }
    record_drop_metadata(tmp_path, "sec_fsds", record)

    with pytest.raises(AppendOnlyViolationError):
        record_drop_metadata(tmp_path, "sec_fsds", record)


def test_g2_pit_never_reads_future_knowledge(tmp_path: Path) -> None:
    """Guarantee 2 (G2): Point-in-time queries never observe future knowledge.

    Plants an initial observation at T1 and a later restatement at T2 > T1.
    Confirms as_of < T1 returns None.
    Confirms as_of in [T1, T2) returns initial value (never the future revision).
    Confirms as_of >= T2 returns revised value.
    """
    ev = datetime(2023, 12, 31, 0, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    t2 = datetime(2024, 2, 15, 12, 0, 0, tzinfo=timezone.utc)

    # Initial Drop 1: Revenue = 100.0 at T1
    tbl_d1 = _create_sample_canonical_table(
        drop_id="drop_1",
        event_time=ev,
        knowledge_time=t1,
        value_num=Decimal("100.0"),
    )
    write_partitioned_store(tbl_d1, "sec_fsds", "drop_1", base_path=tmp_path)

    # Revised Drop 2: Revenue = 110.0 at T2
    tbl_d2 = _create_sample_canonical_table(
        drop_id="drop_2",
        event_time=ev,
        knowledge_time=t2,
        value_num=Decimal("110.0"),
    )
    write_partitioned_store(tbl_d2, "sec_fsds", "drop_2", base_path=tmp_path)

    vendor_store = tmp_path / "sec_fsds"
    with Store(vendor_store) as s:
        # Case A: Before initial knowledge time T1 -> None
        res_before_t1 = s.value(
            entity_id="0000320193",
            field="Revenues",
            event_time=ev,
            as_of=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        assert res_before_t1 is None

        # Case B: Between T1 and T2 -> returns Drop 1 value (100.0), NOT Drop 2 (110.0)
        res_between = s.value(
            entity_id="0000320193",
            field="Revenues",
            event_time=ev,
            as_of=datetime(2024, 2, 1, tzinfo=timezone.utc),
        )
        assert res_between is not None
        assert res_between["value_num"] == Decimal("100.000000000")
        assert res_between["drop_id"] == "drop_1"
        assert res_between["knowledge_time"] == t1

        # Case C: At or after T2 -> returns Drop 2 revised value (110.0)
        res_after_t2 = s.value(
            entity_id="0000320193",
            field="Revenues",
            event_time=ev,
            as_of=datetime(2024, 3, 1, tzinfo=timezone.utc),
        )
        assert res_after_t2 is not None
        assert res_after_t2["value_num"] == Decimal("110.000000000")
        assert res_after_t2["drop_id"] == "drop_2"
        assert res_after_t2["knowledge_time"] == t2


def test_g3_rerun_same_drop_adds_no_rows(tmp_path: Path) -> None:
    """Guarantee 3 (G3): Re-running the same drop produces zero new rows (idempotence)."""
    drop_id = "drop_g3_001"
    table = _create_sample_canonical_table(drop_id=drop_id)

    # Run 1: initial ingestion
    written_paths = write_partitioned_store(
        table, vendor="sec_fsds", drop_id=drop_id, base_path=tmp_path
    )
    assert len(written_paths) == 1

    now = datetime(2024, 1, 15, tzinfo=timezone.utc)
    record = {
        "drop_id": drop_id,
        "vendor": "sec_fsds",
        "status": "COMMITTED",
        "committed_at": now,
        "total_rows": 1,
        "valid_rows": 1,
        "quarantine_rows": 0,
        "total_bytes": 1024,
        "file_count": 1,
        "file_format": "csv",
        "compression": "none",
        "event_time_min": now,
        "event_time_max": now,
        "knowledge_time_min": now,
        "knowledge_time_max": now,
    }
    record_drop_metadata(tmp_path, "sec_fsds", record)

    # Verification 1: discover.check_idempotency detects prior commitment
    is_committed = check_idempotency(drop_id, store_path=tmp_path / "sec_fsds")
    assert is_committed is True

    # Verification 2: store refuses write with AppendOnlyViolationError
    with pytest.raises(AppendOnlyViolationError):
        write_partitioned_store(table, vendor="sec_fsds", drop_id=drop_id, base_path=tmp_path)

    # Verification 3: record_revisions appends zero duplicate revision rows
    ev = datetime(2023, 12, 31, tzinfo=timezone.utc)
    rev_dict = {
        "entity_id": ["0000320193"],
        "field": ["Revenues"],
        "event_time": [ev],
        "old_value_num": [Decimal("90.0")],
        "new_value_num": [Decimal("100.0")],
        "old_value_str": [None],
        "new_value_str": [None],
        "old_unit": ["USD"],
        "new_unit": ["USD"],
        "old_knowledge_time": [datetime(2024, 1, 1, tzinfo=timezone.utc)],
        "new_knowledge_time": [now],
        "old_drop_id": ["drop_0"],
        "new_drop_id": [drop_id],
        "relative_change": [0.111111],
    }
    rev_tbl = pa.Table.from_pydict(rev_dict, schema=REVISIONS_ARROW_SCHEMA)

    init_store_catalog(tmp_path, "sec_fsds").close()
    record_revisions(tmp_path, "sec_fsds", rev_tbl)

    rev_file = tmp_path / "sec_fsds" / "metadata" / "revisions.parquet"
    count_first = len(pq.read_table(rev_file))
    assert count_first == 1

    # Second invocation with same drop_id produces delta rows == 0
    record_revisions(tmp_path, "sec_fsds", rev_tbl)
    count_second = len(pq.read_table(rev_file))
    assert count_second == count_first
