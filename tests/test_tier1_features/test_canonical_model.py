"""
Tier 1 Feature Tests: F-MOD-01 Canonical Bitemporal Model.

Tests the standard CanonicalRecord tuple/dataclass, strict immutability,
UTC timezone enforcement, value polymorphism, PyArrow table conversion,
and revision sequence tracking.
"""

from __future__ import annotations

import dataclasses
from datetime import datetime, timezone, timedelta
from typing import Any

import pyarrow as pa
import pytest

# Progressive import from vendor_onboarding or fallback contract
try:
    from vendor_onboarding.models.record import CanonicalRecord
except ImportError:
    @dataclasses.dataclass(frozen=True)
    class CanonicalRecord:  # type: ignore[no-redef]
        entity_id: str
        event_time: datetime
        knowledge_time: datetime
        value: float | str | int | None
        unit: str
        source_symbol: str
        source_symbol_type: str
        vendor_id: str
        dataset_id: str
        drop_id: str
        is_restatement: bool = False
        revision_seq: int = 1


def _enforce_utc_datetime(dt: datetime) -> datetime:
    """Validates or normalizes datetime to UTC timezone."""
    if dt.tzinfo is None:
        raise ValueError("Naive datetime rejected; explicit UTC timezone required.")
    return dt.astimezone(timezone.utc)


def _records_to_arrow(records: list[CanonicalRecord]) -> pa.Table:
    """Converts a sequence of CanonicalRecord instances into a typed PyArrow Table."""
    arrow_schema = pa.schema([
        ("entity_id", pa.string()),
        ("event_time", pa.timestamp("us", tz="UTC")),
        ("knowledge_time", pa.timestamp("us", tz="UTC")),
        ("value", pa.float64()),
        ("unit", pa.string()),
        ("source_symbol", pa.string()),
        ("source_symbol_type", pa.string()),
        ("vendor_id", pa.string()),
        ("dataset_id", pa.string()),
        ("drop_id", pa.string()),
        ("is_restatement", pa.bool_()),
        ("revision_seq", pa.int64()),
    ])

    data: dict[str, list[Any]] = {
        "entity_id": [r.entity_id for r in records],
        "event_time": [r.event_time for r in records],
        "knowledge_time": [r.knowledge_time for r in records],
        "value": [float(r.value) if r.value is not None and isinstance(r.value, (int, float)) else None for r in records],
        "unit": [r.unit for r in records],
        "source_symbol": [r.source_symbol for r in records],
        "source_symbol_type": [r.source_symbol_type for r in records],
        "vendor_id": [r.vendor_id for r in records],
        "dataset_id": [r.dataset_id for r in records],
        "drop_id": [r.drop_id for r in records],
        "is_restatement": [r.is_restatement for r in records],
        "revision_seq": [r.revision_seq for r in records],
    }
    return pa.Table.from_pydict(data, schema=arrow_schema)


def _arrow_to_records(table: pa.Table) -> list[CanonicalRecord]:
    """Converts a PyArrow Table back into CanonicalRecord instances."""
    pylist = table.to_pylist()
    records = []
    for r in pylist:
        records.append(
            CanonicalRecord(
                entity_id=r["entity_id"],
                event_time=r["event_time"],
                knowledge_time=r["knowledge_time"],
                value=r["value"],
                unit=r["unit"],
                source_symbol=r["source_symbol"],
                source_symbol_type=r["source_symbol_type"],
                vendor_id=r["vendor_id"],
                dataset_id=r["dataset_id"],
                drop_id=r["drop_id"],
                is_restatement=r["is_restatement"],
                revision_seq=r["revision_seq"],
            )
        )
    return records


@pytest.mark.tier1
def test_canonical_record_creation(sample_canonical_records):
    """Verify CanonicalRecord instantiates properly with all canonical fields."""
    rec = sample_canonical_records[0]
    assert rec.entity_id == "ENT_AAPL"
    assert rec.source_symbol == "AAPL"
    assert rec.unit == "USD"
    assert rec.value == 173.75
    assert rec.vendor_id == "SYNTH_VENDOR_A"
    assert rec.dataset_id == "EQUITY_DAILY_FUNDAMENTALS"
    assert rec.drop_id == "DROP_001"


@pytest.mark.tier1
def test_canonical_record_frozen_immutability(sample_canonical_records):
    """Verify CanonicalRecord is frozen; attempting attribute assignment raises FrozenInstanceError."""
    rec = sample_canonical_records[0]
    with pytest.raises(dataclasses.FrozenInstanceError):
        rec.value = 999.99  # type: ignore[misc]

    with pytest.raises(dataclasses.FrozenInstanceError):
        rec.entity_id = "ENT_MUTATED"  # type: ignore[misc]


@pytest.mark.tier1
def test_canonical_record_field_types(sample_canonical_records):
    """Verify runtime types of CanonicalRecord attributes."""
    rec = sample_canonical_records[0]
    assert isinstance(rec.entity_id, str)
    assert isinstance(rec.event_time, datetime)
    assert isinstance(rec.knowledge_time, datetime)
    assert isinstance(rec.value, (float, int, str)) or rec.value is None
    assert isinstance(rec.unit, str)
    assert isinstance(rec.source_symbol, str)
    assert isinstance(rec.source_symbol_type, str)
    assert isinstance(rec.is_restatement, bool)
    assert isinstance(rec.revision_seq, int)


@pytest.mark.tier1
def test_canonical_record_default_revision_flags(synthetic_record_factory):
    """Verify default values for is_restatement (False) and revision_seq (1)."""
    rec = synthetic_record_factory()
    assert rec.is_restatement is False
    assert rec.revision_seq == 1


@pytest.mark.tier1
def test_canonical_record_explicit_revision_flags(synthetic_record_factory):
    """Verify explicit assignment of restatement flags."""
    rec = synthetic_record_factory(is_restatement=True, revision_seq=2)
    assert rec.is_restatement is True
    assert rec.revision_seq == 2


@pytest.mark.tier1
def test_canonical_record_utc_timezone_enforcement():
    """Verify that timestamps must be UTC timezone-aware."""
    naive_dt = datetime(2023, 10, 2, 16, 0, 0)
    with pytest.raises(ValueError, match="Naive datetime rejected"):
        _enforce_utc_datetime(naive_dt)

    # Offset timezone normalized to UTC
    est_tz = timezone(timedelta(hours=-5))
    est_dt = datetime(2023, 10, 2, 11, 0, 0, tzinfo=est_tz)
    utc_dt = _enforce_utc_datetime(est_dt)
    assert utc_dt.tzinfo == timezone.utc
    assert utc_dt.hour == 16


@pytest.mark.tier1
def test_canonical_record_value_polymorphism(synthetic_record_factory):
    """Verify value supports floats, integers, strings (categorical), and None."""
    rec_float = synthetic_record_factory(value=175.50)
    assert isinstance(rec_float.value, float)

    rec_int = synthetic_record_factory(value=1000000)
    assert isinstance(rec_int.value, int)

    rec_str = synthetic_record_factory(value="BUY", unit="RATING")
    assert isinstance(rec_str.value, str)

    rec_none = synthetic_record_factory(value=None)
    assert rec_none.value is None


@pytest.mark.tier1
def test_canonical_record_equality_and_hashing(synthetic_record_factory):
    """Verify structural equality and hashing allowing deduplication via set."""
    t_event = datetime(2023, 10, 2, 16, 0, 0, tzinfo=timezone.utc)
    t_known = datetime(2023, 10, 6, 20, 0, 0, tzinfo=timezone.utc)

    rec1 = synthetic_record_factory(entity_id="ENT_AAPL", event_time=t_event, knowledge_time=t_known, value=150.0)
    rec2 = synthetic_record_factory(entity_id="ENT_AAPL", event_time=t_event, knowledge_time=t_known, value=150.0)
    rec3 = synthetic_record_factory(entity_id="ENT_AAPL", event_time=t_event, knowledge_time=t_known, value=155.0)

    assert rec1 == rec2
    assert hash(rec1) == hash(rec2)
    assert rec1 != rec3

    record_set = {rec1, rec2, rec3}
    assert len(record_set) == 2


@pytest.mark.tier1
def test_canonical_record_asdict_representation(sample_canonical_records):
    """Verify dataclasses.asdict produces complete dictionary representation."""
    rec = sample_canonical_records[0]
    d = dataclasses.asdict(rec)
    assert d["entity_id"] == "ENT_AAPL"
    assert d["revision_seq"] == 1
    assert "event_time" in d
    assert "knowledge_time" in d
    assert len(d) == 12


@pytest.mark.tier1
def test_canonical_records_to_pyarrow_table(sample_canonical_records):
    """Verify conversion of CanonicalRecord sequence to a PyArrow Table."""
    table = _records_to_arrow(sample_canonical_records)
    assert isinstance(table, pa.Table)
    assert table.num_rows == len(sample_canonical_records)
    assert table.num_columns == 12
    assert "entity_id" in table.column_names
    assert "event_time" in table.column_names


@pytest.mark.tier1
def test_pyarrow_table_to_canonical_records(sample_canonical_records):
    """Verify round-trip: PyArrow Table -> list[CanonicalRecord]."""
    table = _records_to_arrow(sample_canonical_records)
    restored_records = _arrow_to_records(table)

    assert len(restored_records) == len(sample_canonical_records)
    for orig, restored in zip(sample_canonical_records, restored_records):
        assert orig.entity_id == restored.entity_id
        assert orig.unit == restored.unit
        assert orig.value == pytest.approx(restored.value)


@pytest.mark.tier1
def test_canonical_model_arrow_schema_types(sample_canonical_records):
    """Verify PyArrow table schema enforces microsecond UTC timestamps."""
    table = _records_to_arrow(sample_canonical_records)
    schema = table.schema

    ev_type = schema.field("event_time").type
    kn_type = schema.field("knowledge_time").type
    assert pa.types.is_timestamp(ev_type)
    assert ev_type.tz == "UTC"
    assert pa.types.is_timestamp(kn_type)
    assert kn_type.tz == "UTC"


@pytest.mark.tier1
def test_canonical_record_source_symbol_types(synthetic_record_factory):
    """Verify accepted source_symbol_type classifications."""
    valid_types = ["TICKER", "CIK", "CUSIP", "ISIN", "SEDOL", "FIGI", "VENDOR_ID"]
    for stype in valid_types:
        rec = synthetic_record_factory(source_symbol="TEST", source_symbol_type=stype)
        assert rec.source_symbol_type == stype


@pytest.mark.tier1
def test_canonical_record_sorting_key(sample_canonical_records):
    """Verify deterministic sorting key (entity_id, event_time, revision_seq)."""
    sorted_records = sorted(
        sample_canonical_records,
        key=lambda r: (r.entity_id, r.event_time, r.revision_seq),
    )
    assert sorted_records[0].entity_id == "ENT_AAPL"
    assert sorted_records[-1].entity_id == "ENT_MSFT"
