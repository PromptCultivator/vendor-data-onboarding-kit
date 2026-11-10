"""Canonical bitemporal data model and Arrow conversion utilities."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import total_ordering
from typing import Any

import pyarrow as pa


def enforce_utc_datetime(dt: datetime) -> datetime:
    """Validates or normalizes datetime to UTC timezone.

    Raises:
        ValueError: If dt is timezone-naive.
    """
    if dt.tzinfo is None:
        raise ValueError("Naive datetime rejected; explicit UTC timezone required.")
    return dt.astimezone(UTC)


@total_ordering
@dataclass(frozen=True)
class CanonicalRecord:
    """Canonical bitemporal record representation.

    Attributes:
        entity_id: Authoritative entity identifier.
        event_time: Point-in-time timestamp when the event occurred in the real world (UTC).
        knowledge_time: Timestamp when this record became known to the system (UTC).
        value: Fact or metric value (numeric, string categorical, or None).
        unit: Measurement unit or metric label.
        source_symbol: Original raw symbol from vendor feed.
        source_symbol_type: Classification of source symbol (TICKER, CIK, CUSIP, etc.).
        vendor_id: Vendor identifier.
        dataset_id: Dataset family identifier.
        drop_id: Ingestion drop identifier.
        is_restatement: Flag indicating whether this record restates a prior historical value.
        revision_seq: Monotonically increasing revision sequence number per entity/time key.
    """

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

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_time", enforce_utc_datetime(self.event_time))
        object.__setattr__(
            self, "knowledge_time", enforce_utc_datetime(self.knowledge_time)
        )

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, CanonicalRecord):
            return NotImplemented
        return (self.entity_id, self.event_time, self.revision_seq) < (
            other.entity_id,
            other.event_time,
            other.revision_seq,
        )


CANONICAL_ARROW_SCHEMA = pa.schema([
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


def records_to_arrow(
    records: Sequence[CanonicalRecord], schema: pa.Schema | None = None
) -> pa.Table:
    """Converts a sequence of CanonicalRecord instances into a typed PyArrow Table."""
    arrow_schema = schema if schema is not None else CANONICAL_ARROW_SCHEMA
    if not records:
        return pa.Table.from_pydict(
            {f.name: [] for f in arrow_schema}, schema=arrow_schema
        )

    val_field = arrow_schema.field("value")
    values: list[Any]
    if pa.types.is_integer(val_field.type):
        values = [
            int(r.value)
            if r.value is not None and isinstance(r.value, (int, float))
            else None
            for r in records
        ]
    elif pa.types.is_floating(val_field.type):
        values = [
            float(r.value)
            if r.value is not None and isinstance(r.value, (int, float))
            else None
            for r in records
        ]
    else:
        values = [r.value for r in records]

    data: dict[str, list[Any]] = {
        "entity_id": [r.entity_id for r in records],
        "event_time": [r.event_time for r in records],
        "knowledge_time": [r.knowledge_time for r in records],
        "value": values,
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


def arrow_to_records(table: pa.Table) -> list[CanonicalRecord]:
    """Converts a PyArrow Table back into a list of CanonicalRecord instances."""
    pylist = table.to_pylist()
    records: list[CanonicalRecord] = []
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
                is_restatement=r.get("is_restatement", False),
                revision_seq=r.get("revision_seq", 1),
            )
        )
    return records
