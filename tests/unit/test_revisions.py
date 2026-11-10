"""Unit tests for bitemporal revision ledger and rate metrics."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from vdok.normalize import CANONICAL_ARROW_SCHEMA
from vdok.revisions import (
    REVISIONS_ARROW_SCHEMA,
    compute_revision_rates,
    detect_revisions,
    record_revisions,
)
from vdok.store import init_store_catalog, write_partitioned_store


def _build_row(
    entity_id: str,
    field: str,
    event_time: datetime,
    knowledge_time: datetime,
    value_num: Decimal | None,
    value_str: str | None = None,
    unit: str | None = "USD",
    drop_id: str = "drop_1",
    source_row: int = 1,
) -> dict[str, list[object]]:
    return {
        "entity_id": [entity_id],
        "entity_id_type": ["cik"],
        "field": [field],
        "event_time": [event_time],
        "knowledge_time": [knowledge_time],
        "value_num": [value_num],
        "value_str": [value_str],
        "unit": [unit],
        "drop_id": [drop_id],
        "source_file": ["data.csv"],
        "source_row": [source_row],
        "row_hash": [f"hash_{drop_id}_{source_row}"],
        "unmapped": [False],
    }


def test_revisions_schema_fields() -> None:
    """Verify REVISIONS_ARROW_SCHEMA contains expected 14 columns."""
    assert len(REVISIONS_ARROW_SCHEMA) == 14
    names = REVISIONS_ARROW_SCHEMA.names
    expected = [
        "entity_id",
        "field",
        "event_time",
        "old_value_num",
        "new_value_num",
        "old_value_str",
        "new_value_str",
        "old_unit",
        "new_unit",
        "old_knowledge_time",
        "new_knowledge_time",
        "old_drop_id",
        "new_drop_id",
        "relative_change",
    ]
    assert names == expected


def test_detect_revisions_numeric_and_relative_change(tmp_path: Path) -> None:
    """Verify revision detection and arithmetic relative_change calculation."""
    ev = datetime(2023, 12, 31, tzinfo=timezone.utc)
    t1 = datetime(2024, 1, 15, tzinfo=timezone.utc)
    t2 = datetime(2024, 2, 15, tzinfo=timezone.utc)

    # Prior drop 1
    d1 = _build_row("0000320193", "Revenues", ev, t1, Decimal("100.00"), drop_id="drop_1")
    t1_arrow = pa.Table.from_pydict(d1, schema=CANONICAL_ARROW_SCHEMA)
    write_partitioned_store(t1_arrow, "sec_fsds", "drop_1", base_path=tmp_path)

    # Incoming drop 2 (revised upward from 100 to 110)
    d2 = _build_row("0000320193", "Revenues", ev, t2, Decimal("110.00"), drop_id="drop_2")
    t2_arrow = pa.Table.from_pydict(d2, schema=CANONICAL_ARROW_SCHEMA)
    write_partitioned_store(t2_arrow, "sec_fsds", "drop_2", base_path=tmp_path)

    con = init_store_catalog(tmp_path, "sec_fsds")
    try:
        revs = detect_revisions(con, "sec_fsds", "drop_2", base_path=tmp_path)
        assert len(revs) == 1
        row = revs.to_pylist()[0]
        assert row["entity_id"] == "0000320193"
        assert row["field"] == "Revenues"
        assert row["old_value_num"] == Decimal("100.000000000")
        assert row["new_value_num"] == Decimal("110.000000000")
        assert row["old_drop_id"] == "drop_1"
        assert row["new_drop_id"] == "drop_2"
        # Relative change: (110 - 100) / 100 = 0.10
        assert row["relative_change"] is not None
        assert abs(row["relative_change"] - 0.10) < 1e-6
    finally:
        con.close()


def test_detect_revisions_negative_and_zero_base(tmp_path: Path) -> None:
    """Verify relative_change with negative old value and zero old value."""
    ev = datetime(2023, 12, 31, tzinfo=timezone.utc)
    t1 = datetime(2024, 1, 15, tzinfo=timezone.utc)
    t2 = datetime(2024, 2, 15, tzinfo=timezone.utc)

    # Entity A: -10 to -5 (+50% increase)
    # Entity B: 0 to 10 (division by zero avoided -> NULL)
    rows_d1 = {
        "entity_id": ["ENT_NEG", "ENT_ZERO"],
        "entity_id_type": ["cik", "cik"],
        "field": ["net_income", "operating_income"],
        "event_time": [ev, ev],
        "knowledge_time": [t1, t1],
        "value_num": [Decimal("-10.0"), Decimal("0.0")],
        "value_str": [None, None],
        "unit": ["USD", "USD"],
        "drop_id": ["drop_1", "drop_1"],
        "source_file": ["f.csv", "f.csv"],
        "source_row": [1, 2],
        "row_hash": ["h1", "h2"],
        "unmapped": [False, False],
    }
    t1_arrow = pa.Table.from_pydict(rows_d1, schema=CANONICAL_ARROW_SCHEMA)
    write_partitioned_store(t1_arrow, "sec_fsds", "drop_1", base_path=tmp_path)

    rows_d2 = {
        "entity_id": ["ENT_NEG", "ENT_ZERO"],
        "entity_id_type": ["cik", "cik"],
        "field": ["net_income", "operating_income"],
        "event_time": [ev, ev],
        "knowledge_time": [t2, t2],
        "value_num": [Decimal("-5.0"), Decimal("10.0")],
        "value_str": [None, None],
        "unit": ["USD", "USD"],
        "drop_id": ["drop_2", "drop_2"],
        "source_file": ["f.csv", "f.csv"],
        "source_row": [1, 2],
        "row_hash": ["h3", "h4"],
        "unmapped": [False, False],
    }
    t2_arrow = pa.Table.from_pydict(rows_d2, schema=CANONICAL_ARROW_SCHEMA)
    write_partitioned_store(t2_arrow, "sec_fsds", "drop_2", base_path=tmp_path)

    con = init_store_catalog(tmp_path, "sec_fsds")
    try:
        revs = detect_revisions(con, "sec_fsds", "drop_2", base_path=tmp_path)
        assert len(revs) == 2
        rev_map = {r["entity_id"]: r for r in revs.to_pylist()}

        # Negative base: (-5 - (-10)) / |-10| = +5 / 10 = +0.50
        assert rev_map["ENT_NEG"]["relative_change"] is not None
        assert abs(rev_map["ENT_NEG"]["relative_change"] - 0.50) < 1e-6

        # Zero base: division by zero avoided
        assert rev_map["ENT_ZERO"]["relative_change"] is None
    finally:
        con.close()


def test_detect_revisions_string_and_unit_changes(tmp_path: Path) -> None:
    """Verify categorical string and unit modifications trigger revisions."""
    ev = datetime(2023, 12, 31, tzinfo=timezone.utc)
    t1 = datetime(2024, 1, 15, tzinfo=timezone.utc)
    t2 = datetime(2024, 2, 15, tzinfo=timezone.utc)

    # String change: status 'ACTIVE' -> 'SUSPENDED'
    # Unit change: 50 'USD' -> 50 'EUR'
    rows_d1 = {
        "entity_id": ["ENT_STR", "ENT_UNIT"],
        "entity_id_type": ["cik", "cik"],
        "field": ["audit_status", "capital"],
        "event_time": [ev, ev],
        "knowledge_time": [t1, t1],
        "value_num": [None, Decimal("50.0")],
        "value_str": ["ACTIVE", None],
        "unit": [None, "USD"],
        "drop_id": ["drop_1", "drop_1"],
        "source_file": ["f.csv", "f.csv"],
        "source_row": [1, 2],
        "row_hash": ["h1", "h2"],
        "unmapped": [False, False],
    }
    t1_arrow = pa.Table.from_pydict(rows_d1, schema=CANONICAL_ARROW_SCHEMA)
    write_partitioned_store(t1_arrow, "v", "drop_1", base_path=tmp_path)

    rows_d2 = {
        "entity_id": ["ENT_STR", "ENT_UNIT"],
        "entity_id_type": ["cik", "cik"],
        "field": ["audit_status", "capital"],
        "event_time": [ev, ev],
        "knowledge_time": [t2, t2],
        "value_num": [None, Decimal("50.0")],
        "value_str": ["SUSPENDED", None],
        "unit": [None, "EUR"],
        "drop_id": ["drop_2", "drop_2"],
        "source_file": ["f.csv", "f.csv"],
        "source_row": [1, 2],
        "row_hash": ["h3", "h4"],
        "unmapped": [False, False],
    }
    t2_arrow = pa.Table.from_pydict(rows_d2, schema=CANONICAL_ARROW_SCHEMA)
    write_partitioned_store(t2_arrow, "v", "drop_2", base_path=tmp_path)

    con = init_store_catalog(tmp_path, "v")
    try:
        revs = detect_revisions(con, "v", "drop_2", base_path=tmp_path)
        assert len(revs) == 2
        rev_map = {r["entity_id"]: r for r in revs.to_pylist()}

        assert rev_map["ENT_STR"]["old_value_str"] == "ACTIVE"
        assert rev_map["ENT_STR"]["new_value_str"] == "SUSPENDED"
        assert rev_map["ENT_STR"]["relative_change"] is None

        assert rev_map["ENT_UNIT"]["old_unit"] == "USD"
        assert rev_map["ENT_UNIT"]["new_unit"] == "EUR"
        assert rev_map["ENT_UNIT"]["relative_change"] == 0.0
    finally:
        con.close()


def test_detect_revisions_cascading_restatements(tmp_path: Path) -> None:
    """Verify 3 sequential restatements compare against immediately prior vintage."""
    ev = datetime(2023, 12, 31, tzinfo=timezone.utc)
    t1 = datetime(2024, 1, 15, tzinfo=timezone.utc)
    t2 = datetime(2024, 2, 15, tzinfo=timezone.utc)
    t3 = datetime(2024, 3, 15, tzinfo=timezone.utc)

    # Drop 1: 100
    # Drop 2: 105
    # Drop 3: 110
    d1 = _build_row("E1", "price", ev, t1, Decimal("100.00"), drop_id="drop_1")
    write_partitioned_store(
        pa.Table.from_pydict(d1, schema=CANONICAL_ARROW_SCHEMA), "v", "drop_1", tmp_path
    )

    d2 = _build_row("E1", "price", ev, t2, Decimal("105.00"), drop_id="drop_2")
    write_partitioned_store(
        pa.Table.from_pydict(d2, schema=CANONICAL_ARROW_SCHEMA), "v", "drop_2", tmp_path
    )

    d3 = _build_row("E1", "price", ev, t3, Decimal("110.00"), drop_id="drop_3")
    write_partitioned_store(
        pa.Table.from_pydict(d3, schema=CANONICAL_ARROW_SCHEMA), "v", "drop_3", tmp_path
    )

    con = init_store_catalog(tmp_path, "v")
    try:
        revs = detect_revisions(con, "v", "drop_3", base_path=tmp_path)
        # Drop 3 must pair with Drop 2 (105), not Drop 1 (100)
        assert len(revs) == 1
        r = revs.to_pylist()[0]
        assert r["old_drop_id"] == "drop_2"
        assert r["old_value_num"] == Decimal("105.000000000")
        assert r["new_value_num"] == Decimal("110.000000000")
    finally:
        con.close()


def test_record_revisions_and_idempotency(tmp_path: Path) -> None:
    """Verify persisting revisions to metadata/revisions.parquet is idempotent."""
    ev = datetime(2023, 12, 31, tzinfo=timezone.utc)
    t1 = datetime(2024, 1, 15, tzinfo=timezone.utc)
    t2 = datetime(2024, 2, 15, tzinfo=timezone.utc)

    rev_dict = {
        "entity_id": ["E1"],
        "field": ["price"],
        "event_time": [ev],
        "old_value_num": [Decimal("100.0")],
        "new_value_num": [Decimal("105.0")],
        "old_value_str": [None],
        "new_value_str": [None],
        "old_unit": ["USD"],
        "new_unit": ["USD"],
        "old_knowledge_time": [t1],
        "new_knowledge_time": [t2],
        "old_drop_id": ["drop_1"],
        "new_drop_id": ["drop_2"],
        "relative_change": [0.05],
    }
    rev_table = pa.Table.from_pydict(rev_dict, schema=REVISIONS_ARROW_SCHEMA)

    init_store_catalog(tmp_path, "v").close()

    record_revisions(tmp_path, "v", rev_table)

    rev_file = tmp_path / "v" / "metadata" / "revisions.parquet"
    assert rev_file.exists()
    assert len(pq.read_table(rev_file)) == 1

    # Re-recording identical revisions appends zero new rows
    record_revisions(tmp_path, "v", rev_table)
    assert len(pq.read_table(rev_file)) == 1


def test_compute_revision_rates_and_silent_overwrite(tmp_path: Path) -> None:
    """Verify revision rate calculation and >3 drops silent overwrite flag."""
    ev = datetime(2023, 12, 31, tzinfo=timezone.utc)

    # Create 4 drops for field 'constant_field' with identical values across all 4 drops
    # Create 4 drops for field 'revised_field' with a revision in drop 2
    for idx in range(1, 5):
        drop_id = f"drop_{idx}"
        kt = datetime(2024, idx, 15, tzinfo=timezone.utc)
        val = Decimal("105.0") if (idx >= 2) else Decimal("100.0")

        rows = {
            "entity_id": ["E1", "E1"],
            "entity_id_type": ["cik", "cik"],
            "field": ["constant_field", "revised_field"],
            "event_time": [ev, ev],
            "knowledge_time": [kt, kt],
            "value_num": [Decimal("50.0"), val],
            "value_str": [None, None],
            "unit": ["USD", "USD"],
            "drop_id": [drop_id, drop_id],
            "source_file": ["f.csv", "f.csv"],
            "source_row": [1, 2],
            "row_hash": [f"h_{idx}_1", f"h_{idx}_2"],
            "unmapped": [False, False],
        }
        write_partitioned_store(
            pa.Table.from_pydict(rows, schema=CANONICAL_ARROW_SCHEMA),
            "v",
            drop_id,
            tmp_path,
        )

    # Record 1 revision for 'revised_field'
    rev_dict = {
        "entity_id": ["E1"],
        "field": ["revised_field"],
        "event_time": [ev],
        "old_value_num": [Decimal("100.0")],
        "new_value_num": [Decimal("105.0")],
        "old_value_str": [None],
        "new_value_str": [None],
        "old_unit": ["USD"],
        "new_unit": ["USD"],
        "old_knowledge_time": [datetime(2024, 1, 15, tzinfo=timezone.utc)],
        "new_knowledge_time": [datetime(2024, 2, 15, tzinfo=timezone.utc)],
        "old_drop_id": ["drop_1"],
        "new_drop_id": ["drop_2"],
        "relative_change": [0.05],
    }
    record_revisions(tmp_path, "v", pa.Table.from_pydict(rev_dict, schema=REVISIONS_ARROW_SCHEMA))

    stats = compute_revision_rates(tmp_path, "v")
    assert stats.total_valid_rows == 8  # 4 drops * 2 rows
    assert stats.total_revisions == 1
    assert abs(stats.drop_rate - (1 / 8)) < 1e-6

    # 'constant_field' was present in 4 drops and has 0 revisions: silent overwrite warning flagged
    assert "constant_field" in stats.silent_overwrite_fields
    assert "revised_field" not in stats.silent_overwrite_fields

    # Verify per-drop rate calculation
    stats_d2 = compute_revision_rates(tmp_path, "v", drop_id="drop_2")
    assert stats_d2.drop_id == "drop_2"
    assert stats_d2.total_valid_rows == 2
    assert stats_d2.total_revisions == 1
    assert stats_d2.drop_rate == 0.5
    assert len(stats_d2.largest_changes) == 1

    # Verify empty vendor store handling
    stats_empty = compute_revision_rates(tmp_path, "empty_vendor")
    assert stats_empty.total_valid_rows == 0
    assert stats_empty.total_revisions == 0
