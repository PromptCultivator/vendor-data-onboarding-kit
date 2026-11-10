"""Arrow Flight Bitemporal Read Service and High-Performance Client.

Provides zero-copy, low-latency Arrow Flight endpoints for point-in-time
("as-of") bitemporal queries over partitioned datasets, adhering to Citadel-grade
market data read patterns.
"""

from __future__ import annotations

import json
from collections.abc import Generator
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import Any, Self

import pyarrow as pa
import pyarrow.flight as fl

if not hasattr(fl, "FlightInvalidArgumentError"):

    class FlightInvalidArgumentError(fl.FlightServerError):
        """Raised when a Flight ticket payload is malformed or invalid."""

    fl.FlightInvalidArgumentError = FlightInvalidArgumentError  # type: ignore[attr-defined]

from vendor_onboarding.storage.query import CANONICAL_AS_OF_SCHEMA
from vendor_onboarding.storage.store import BitemporalParquetStore, BitemporalStore


def create_flight_ticket(
    dataset_id: str,
    as_of: str | datetime,
    entity_ids: list[str] | None = None,
    metric: str | None = None,
) -> bytes:
    """Serializes bitemporal as-of query parameters into JSON bytes ticket payload."""
    as_of_str = as_of.isoformat() if isinstance(as_of, datetime) else str(as_of)
    command_dict: dict[str, Any] = {
        "dataset_id": dataset_id,
        "as_of": as_of_str,
    }
    if entity_ids is not None:
        command_dict["entity_ids"] = list(entity_ids)
    if metric is not None:
        command_dict["metric"] = metric

    return json.dumps(command_dict).encode("utf-8")


def parse_flight_ticket(ticket_bytes: bytes) -> dict[str, Any]:
    """Deserializes JSON bytes ticket payload into query parameter dictionary."""
    return json.loads(ticket_bytes.decode("utf-8"))  # type: ignore[no-any-return]


class BitemporalFlightServer(fl.FlightServerBase):
    """Citadel-aligned Arrow Flight read service for zero-copy bitemporal queries."""

    def __init__(
        self,
        location: str | fl.Location | None = None,
        store: BitemporalStore | BitemporalParquetStore | Path | str | None = None,
        host: str = "localhost",
        port: int = 8815,
        **kwargs: Any,
    ) -> None:
        self.host = host
        target_location = location or f"grpc://{host}:{port}"
        super().__init__(target_location, **kwargs)

        if isinstance(store, (str, Path)):
            self.store: Any = BitemporalParquetStore(store)
        elif store is None:
            self.store = BitemporalParquetStore(Path("data/store"))
        else:
            self.store = store

    def do_get(
        self,
        context: fl.ServerCallContext,
        ticket: fl.Ticket,
    ) -> fl.RecordBatchStream:
        """Executes point-in-time query specified in ticket and streams Arrow RecordBatches."""
        try:
            payload = parse_flight_ticket(ticket.ticket)
            dataset_id: str = payload["dataset_id"]
            as_of_raw = payload["as_of"]

            if isinstance(as_of_raw, datetime):
                as_of_dt = as_of_raw
            else:
                clean_str = str(as_of_raw).replace("Z", "+00:00")
                as_of_dt = datetime.fromisoformat(clean_str)

            if as_of_dt.tzinfo is None:
                as_of_dt = as_of_dt.replace(tzinfo=UTC)
            else:
                as_of_dt = as_of_dt.astimezone(UTC)
        except (json.JSONDecodeError, KeyError, ValueError, TypeError) as err:
            raise fl.FlightInvalidArgumentError(f"Invalid flight ticket: {err}") from err



        entity_ids = payload.get("entity_ids")
        metric = payload.get("metric")

        table: pa.Table = self.store.as_of_query(
            dataset_id=dataset_id,
            as_of_time=as_of_dt,
            entity_ids=entity_ids,
            metric=metric,
        )

        reader = table.to_reader(max_chunksize=5000)
        return fl.RecordBatchStream(reader)

    def get_flight_info(
        self,
        context: fl.ServerCallContext,
        descriptor: fl.FlightDescriptor,
    ) -> fl.FlightInfo:
        """Returns FlightInfo metadata for the requested dataset descriptor."""
        if descriptor.descriptor_type == fl.DescriptorType.PATH and descriptor.path:
            path_segments = [
                p.decode("utf-8") if isinstance(p, bytes) else str(p) for p in descriptor.path
            ]
            dataset_id = path_segments[-1]
        else:
            dataset_id = "UNKNOWN"

        loc = fl.Location.for_grpc_tcp(self.host, self.port)
        ticket_bytes = create_flight_ticket(
            dataset_id=dataset_id,
            as_of=datetime.now(UTC).isoformat(),
        )
        endpoint = fl.FlightEndpoint(ticket=fl.Ticket(ticket_bytes), locations=[loc])

        return fl.FlightInfo(
            schema=CANONICAL_AS_OF_SCHEMA,
            descriptor=descriptor,
            endpoints=[endpoint],
            total_records=-1,
            total_bytes=-1,
        )

    def list_flights(
        self,
        context: fl.ServerCallContext,
        criteria: bytes,
    ) -> Generator[fl.FlightInfo, None, None]:
        """Lists available bitemporal dataset flights registered in storage."""
        root = getattr(self.store, "root_dir", None)
        if root is not None and Path(root).is_dir():
            for d in sorted(Path(root).iterdir()):
                if d.is_dir() and not d.name.startswith("."):
                    desc = fl.FlightDescriptor.for_path("v1", "datasets", d.name)
                    yield self.get_flight_info(context, desc)


class BitemporalFlightClient:
    """Client for querying bitemporal datasets over Arrow Flight RPC."""

    def __init__(
        self,
        location: str | fl.Location = "grpc://localhost:8815",
        **kwargs: Any,
    ) -> None:
        if isinstance(location, str) and not location.startswith(
            ("grpc://", "grpc+tcp://", "grpc+unix://")
        ):
            location = f"grpc://{location}"
        self.client = fl.FlightClient(location, **kwargs)

    def as_of_query(
        self,
        dataset_id: str,
        as_of: str | datetime,
        entity_ids: list[str] | None = None,
        metric: str | None = None,
    ) -> pa.Table:
        """Performs zero-copy point-in-time query and returns a PyArrow Table."""
        ticket_bytes = create_flight_ticket(
            dataset_id=dataset_id,
            as_of=as_of,
            entity_ids=entity_ids,
            metric=metric,
        )
        reader = self.client.do_get(fl.Ticket(ticket_bytes))
        return reader.read_all()

    def get_flight_info(self, dataset_id: str) -> fl.FlightInfo:
        """Retrieves FlightInfo metadata for a given dataset."""
        descriptor = fl.FlightDescriptor.for_path("v1", "datasets", dataset_id)
        return self.client.get_flight_info(descriptor)

    def list_flights(self) -> list[fl.FlightInfo]:
        """Lists all available flights on the server."""
        return list(self.client.list_flights())

    def close(self) -> None:
        """Closes the underlying FlightClient connection."""
        if hasattr(self.client, "close"):
            self.client.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()
