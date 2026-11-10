"""Hypothesis property tests for point-in-time Store equivalence against BruteForceStore."""

from __future__ import annotations

import tempfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from vdok.normalize import CANONICAL_ARROW_SCHEMA
from vdok.query import BruteForceStore, Store
from vdok.store import AppendOnlyViolationError, write_partitioned_store

ENTITIES = ["0000320193", "0000789019", "AAPL", "MSFT"]
FIELDS = ["Revenues", "OperatingIncome", "price", "volume"]


@st.composite
def canonical_drop_strategy(draw: st.DrawFn) -> tuple[pa.Table, str, datetime]:
    """Generate random canonical drop table with advancing knowledge time."""
    drop_id = f"drop_{draw(st.integers(min_value=1000, max_value=9999))}"
    num_rows = draw(st.integers(min_value=5, max_value=25))

    base_ev = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    base_kt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    kt_offset = draw(st.integers(min_value=1, max_value=60))
    drop_kt = base_kt + timedelta(days=kt_offset)

    entity_ids = [draw(st.sampled_from(ENTITIES)) for _ in range(num_rows)]
    fields = [draw(st.sampled_from(FIELDS)) for _ in range(num_rows)]
    # Fixed small set of event dates to produce realistic point-in-time revision candidates
    ev_offsets = [draw(st.integers(min_value=0, max_value=5)) * 30 for _ in range(num_rows)]
    event_times = [base_ev + timedelta(days=off) for off in ev_offsets]
    knowledge_times = [drop_kt] * num_rows

    values = [
        Decimal(str(draw(st.integers(min_value=10, max_value=5000)))) for _ in range(num_rows)
    ]

    data = {
        "entity_id": entity_ids,
        "entity_id_type": ["cik"] * num_rows,
        "field": fields,
        "event_time": event_times,
        "knowledge_time": knowledge_times,
        "value_num": values,
        "value_str": [None] * num_rows,
        "unit": ["USD"] * num_rows,
        "drop_id": [drop_id] * num_rows,
        "source_file": ["data.csv"] * num_rows,
        "source_row": list(range(1, num_rows + 1)),
        "row_hash": [f"h_{drop_id}_{i}" for i in range(num_rows)],
        "unmapped": [False] * num_rows,
    }
    table = pa.Table.from_pydict(data, schema=CANONICAL_ARROW_SCHEMA)
    return table, drop_id, drop_kt


@settings(
    max_examples=15,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
@given(
    drop1=canonical_drop_strategy(),
    drop2=canonical_drop_strategy(),
    query_entity=st.sampled_from(ENTITIES),
    query_field=st.sampled_from(FIELDS),
    query_ev_offset=st.integers(min_value=0, max_value=5),
    query_kt_offset=st.integers(min_value=-10, max_value=80),
)
def test_pit_selection_matches_bruteforce_reference(
    drop1: tuple[pa.Table, str, datetime],
    drop2: tuple[pa.Table, str, datetime],
    query_entity: str,
    query_field: str,
    query_ev_offset: int,
    query_kt_offset: int,
) -> None:
    """Verify Store.value equivalence against BruteForceStore across random queries."""
    tbl1, id1, kt1 = drop1
    tbl2, id2, kt2 = drop2

    # Ensure distinct drop IDs and temporal advancement for drop 2
    if id1 == id2:
        id2 = f"{id1}_b"
    if kt2 <= kt1:
        kt2 = kt1 + timedelta(days=1)
        tbl2 = tbl2.set_column(
            4, "knowledge_time", pa.array([kt2] * len(tbl2), type=pa.timestamp("us", tz="UTC"))
        )

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        write_partitioned_store(tbl1, "synth", id1, base_path=tmp_path)
        write_partitioned_store(tbl2, "synth", id2, base_path=tmp_path)

        store_dir = tmp_path / "synth"
        with Store(store_dir) as s:
            bf = BruteForceStore(store_dir)

            ev_time = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc) + timedelta(
                days=query_ev_offset * 30
            )
            as_of_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc) + timedelta(
                days=query_kt_offset
            )

            res_store = s.value(
                entity_id=query_entity,
                field=query_field,
                event_time=ev_time,
                as_of=as_of_time,
            )
            res_bf = bf.value(
                entity_id=query_entity,
                field=query_field,
                event_time=ev_time,
                as_of=as_of_time,
            )

            if res_store is None:
                assert res_bf is None, f"Store returned None but BruteForce returned {res_bf}"
            else:
                assert res_bf is not None, (
                    f"Store returned {res_store} but BruteForce returned None"
                )
                assert res_store["value_num"] == res_bf["value_num"]
                assert res_store["knowledge_time"] == res_bf["knowledge_time"]
                assert res_store["drop_id"] == res_bf["drop_id"]


@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
@given(drop=canonical_drop_strategy())
def test_append_only_violation_property(
    drop: tuple[pa.Table, str, datetime],
) -> None:
    """Verify partition overwrite attempts always raise AppendOnlyViolationError."""
    tbl, drop_id, _ = drop
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        write_partitioned_store(tbl, "synth", drop_id, base_path=tmp_path)

        with pytest.raises(AppendOnlyViolationError):
            write_partitioned_store(tbl, "synth", drop_id, base_path=tmp_path)
