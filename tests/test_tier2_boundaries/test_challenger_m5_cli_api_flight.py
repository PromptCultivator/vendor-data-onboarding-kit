"""Challenger Adversarial Stress & Fuzzing Suite: Milestone 5.

Comprehensive empirical stress testing, boundary fuzzing, exit code validation,
concurrency testing, and client lifecycle verification for:
1. CLI Suite (cli.py): exit codes (2 on invalid CLI syntax, 1 on validation/ingest/benchmark failure, 0 on success),
   subcommands (infer, pin, validate, diff, query, report, ingest, benchmark).
2. Programmatic API (api.py): validate_drop schema drift vs contract, exception hierarchy (SchemaValidationError,
   IngestionError, StorageError, DiffError).
3. Flight Service (flight/server.py): malformed tickets, fl.FlightInvalidArgumentError, concurrent client requests,
   client lifecycle (close, __enter__, __exit__).
4. Benchmarks (benchmarks/run_benchmarks.py): execution, json export, failure handling.
"""

from __future__ import annotations

import json
import socket
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

from click.testing import CliRunner
import pyarrow as pa
import pyarrow.flight as fl
import pyarrow.parquet as pq
import pytest

from vendor_onboarding.api import (
    DiffError,
    IngestionError,
    SchemaValidationError,
    StorageError,
    VendorKitError,
    diff_drops,
    infer_and_pin,
    query_as_of,
    run_onboarding_pipeline,
    validate_drop,
)
from vendor_onboarding.cli import cli_app
from vendor_onboarding.datasets.fred import AlfredVintageLoader, load_alfred_raw
from vendor_onboarding.datasets.sec import SECDeraLoader
from vendor_onboarding.datasets.synthetic import SyntheticGenerator
from vendor_onboarding.flight.server import (
    BitemporalFlightClient,
    BitemporalFlightServer,
    FlightInvalidArgumentError,
    create_flight_ticket,
    parse_flight_ticket,
)
from vendor_onboarding.models.schema import SchemaDefinition, load_schema
from vendor_onboarding.storage.store import BitemporalParquetStore


def _get_free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()
    return port


# ============================================================================
# 1. CLI Suite Stress & Exit Codes
# ============================================================================


@pytest.mark.tier2
class TestCLIAdversarialStress:
    """Stress tests and boundary checks on the Click CLI application."""

    def test_cli_invalid_command_exit_code_2(self) -> None:
        """Invoking nonexistent commands returns Click syntax error code 2."""
        runner = CliRunner()
        result = runner.invoke(cli_app, ["nonexistent_subcommand"])
        assert result.exit_code == 2
        assert "No such command" in result.output

        result2 = runner.invoke(cli_app, ["random_string_12345"])
        assert result2.exit_code == 2

    def test_cli_missing_mandatory_arguments_exit_code_2(self) -> None:
        """Missing required arguments or options returns exit code 2."""
        runner = CliRunner()

        # infer requires file_path
        res = runner.invoke(cli_app, ["infer"])
        assert res.exit_code == 2
        assert "Missing argument" in res.output

        # pin requires schema_path and --output
        res = runner.invoke(cli_app, ["pin"])
        assert res.exit_code == 2

        res = runner.invoke(cli_app, ["pin", "some_file.json"])
        assert res.exit_code == 2

        # validate requires file_path
        res = runner.invoke(cli_app, ["validate"])
        assert res.exit_code == 2

        # diff requires baseline_path and current_path
        res = runner.invoke(cli_app, ["diff"])
        assert res.exit_code == 2

        res = runner.invoke(cli_app, ["diff", "only_one_path.csv"])
        assert res.exit_code == 2

        # query requires dataset_id and --as-of
        res = runner.invoke(cli_app, ["query"])
        assert res.exit_code == 2

        res = runner.invoke(cli_app, ["query", "EQUITY_DAILY"])
        assert res.exit_code == 2
        assert "Missing option '--as-of'" in res.output

        # report requires drop_id
        res = runner.invoke(cli_app, ["report"])
        assert res.exit_code == 2

        # report with invalid format choice
        res = runner.invoke(cli_app, ["report", "DROP_001", "--format", "xml"])
        assert res.exit_code == 2
        assert "Invalid value for '--format'" in res.output

        # ingest requires file_path
        res = runner.invoke(cli_app, ["ingest"])
        assert res.exit_code == 2

    def test_cli_nonexistent_files_exit_code_2(self) -> None:
        """Providing nonexistent file paths to path-checked arguments returns exit code 2."""
        runner = CliRunner()
        ghost = "/tmp/strictly_nonexistent_ghost_file_xyz_987654.parquet"

        res = runner.invoke(cli_app, ["infer", ghost])
        assert res.exit_code == 2
        assert "does not exist" in res.output

        res = runner.invoke(cli_app, ["pin", ghost, "-o", "/tmp/out.json"])
        assert res.exit_code == 2

        res = runner.invoke(cli_app, ["validate", ghost])
        assert res.exit_code == 2

        res = runner.invoke(cli_app, ["diff", ghost, ghost])
        assert res.exit_code == 2

        res = runner.invoke(cli_app, ["ingest", ghost])
        assert res.exit_code == 2

    def test_cli_infer_subcommand_full(self, fixture_paths: Any) -> None:
        """Infer subcommand runs, samples rows, and writes schema when --output is provided."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as td:
            out_schema = Path(td) / "inferred_schema.json"
            res = runner.invoke(
                cli_app,
                [
                    "infer",
                    str(fixture_paths.drop_001_parquet),
                    "--sample-rows",
                    "50",
                    "--output",
                    str(out_schema),
                ],
            )
            assert res.exit_code == 0
            assert "Inferred schema from" in res.output
            assert out_schema.exists()

            loaded = load_schema(out_schema)
            assert isinstance(loaded, SchemaDefinition)
            assert len(loaded.columns) > 0

    def test_cli_pin_subcommand_full(self, fixture_paths: Any) -> None:
        """Pin subcommand loads existing schema and saves immutable contract."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as td:
            out_pinned = Path(td) / "pinned_contract.json"
            res = runner.invoke(
                cli_app,
                ["pin", str(fixture_paths.schema_baseline), "--output", str(out_pinned)],
            )
            assert res.exit_code == 0
            assert "Pinned schema to" in res.output
            assert out_pinned.exists()

            loaded = load_schema(out_pinned)
            assert loaded.dataset_id == "EQUITY_DAILY_FUNDAMENTALS"

    def test_cli_validate_success_exit_code_0(self, fixture_paths: Any) -> None:
        """Valid drop validation succeeds and exits with code 0."""
        runner = CliRunner()
        res = runner.invoke(cli_app, ["validate", str(fixture_paths.drop_001_parquet)])
        assert res.exit_code == 0
        assert "Validation PASSED" in res.output
        assert "0 violations" in res.output

    def test_cli_validate_data_quality_failure_exit_code_1(self, fixture_paths: Any) -> None:
        """Drop with data quality anomalies fails validation and exits with code 1."""
        runner = CliRunner()
        res = runner.invoke(cli_app, ["validate", str(fixture_paths.synth_dir / "drop_003_anomalies.csv")])
        assert res.exit_code == 1
        assert "Validation FAILED" in res.output

    def test_cli_validate_breaking_schema_drift_exit_code_1(self, fixture_paths: Any) -> None:
        """Validating drop against incompatible schema contract detects drift and exits with code 1."""
        runner = CliRunner()
        res = runner.invoke(
            cli_app,
            [
                "validate",
                str(fixture_paths.drop_001_parquet),
                "--schema",
                str(fixture_paths.schema_drift_breaking),
            ],
        )
        assert res.exit_code == 1
        assert "Validation FAILED" in res.output

    def test_cli_diff_subcommand_full(self, fixture_paths: Any) -> None:
        """Diff subcommand compares baseline and restatement drops, detects restatements, and exports ledger."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as td:
            out_ledger = Path(td) / "diff_ledger.parquet"
            res = runner.invoke(
                cli_app,
                [
                    "diff",
                    str(fixture_paths.drop_001_parquet),
                    str(fixture_paths.drop_002_parquet),
                    "--output",
                    str(out_ledger),
                ],
            )
            assert res.exit_code == 0
            assert "Diff Summary" in res.output
            assert "10 restatements detected" in res.output
            assert out_ledger.exists()
            tbl = pq.read_table(out_ledger)
            assert tbl.num_rows > 0

    def test_cli_query_subcommand_full(self, fixture_paths: Any) -> None:
        """Query subcommand executes point-in-time lookup with entity filter."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as td:
            store_dir = Path(td) / "store"
            store = BitemporalParquetStore(store_dir)
            tbl = pq.read_table(fixture_paths.drop_001_parquet)
            store.append(tbl, dataset_id="EQUITY_DAILY", drop_id="DROP_001")

            res = runner.invoke(
                cli_app,
                [
                    "query",
                    "EQUITY_DAILY",
                    "--as-of",
                    "2023-10-06T20:00:00Z",
                    "--entity",
                    "ENT_AAPL",
                    "--store-dir",
                    str(store_dir),
                ],
            )
            assert res.exit_code == 0
            assert "Executed PIT query on EQUITY_DAILY as of 2023-10-06T20:00:00Z" in res.output

    def test_cli_report_subcommand_formats(self) -> None:
        """Report subcommand generates markdown, json, and all formats in destination directory."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as td:
            out_dir = Path(td) / "reports"

            # Markdown
            res_md = runner.invoke(cli_app, ["report", "DROP_TEST_MD", "--format", "markdown", "--output-dir", str(out_dir)])
            assert res_md.exit_code == 0
            assert (out_dir / "readiness_report_DROP_TEST_MD.md").exists()

            # JSON
            res_json = runner.invoke(cli_app, ["report", "DROP_TEST_JSON", "--format", "json", "--output-dir", str(out_dir)])
            assert res_json.exit_code == 0
            assert (out_dir / "readiness_report_DROP_TEST_JSON.json").exists()

            # All
            res_all = runner.invoke(cli_app, ["report", "DROP_TEST_ALL", "--format", "all", "--output-dir", str(out_dir)])
            assert res_all.exit_code == 0
            assert (out_dir / "readiness_report_DROP_TEST_ALL.md").exists()
            assert (out_dir / "readiness_report_DROP_TEST_ALL.json").exists()

    def test_cli_ingest_success_exit_code_0(self, fixture_paths: Any) -> None:
        """Ingest subcommand appends valid drop to store and exits with code 0."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as td:
            store_dir = Path(td) / "store"
            res = runner.invoke(
                cli_app,
                [
                    "ingest",
                    str(fixture_paths.drop_001_parquet),
                    "--dataset-id",
                    "EQUITY_DAILY",
                    "--drop-id",
                    "DROP_001",
                    "--store-dir",
                    str(store_dir),
                ],
            )
            assert res.exit_code == 0
            assert "Ingestion SUCCESS" in res.output
            assert store_dir.exists()

    def test_cli_ingest_failure_exit_code_1(self) -> None:
        """Ingest subcommand fails on corrupted/invalid file and exits with code 1."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as td:
            corrupted_file = Path(td) / "corrupt_data.parquet"
            corrupted_file.write_bytes(b"INVALID_HEADER_AND_GARBAGE_BYTES")
            res = runner.invoke(cli_app, ["ingest", str(corrupted_file)])
            assert res.exit_code == 1
            assert "Ingestion FAILED" in res.output

    def test_cli_benchmark_subcommand_quick(self) -> None:
        """Benchmark subcommand executes institutional benchmarks pass and exits with code 0."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as td:
            metrics_file = Path(td) / "metrics.json"
            res = runner.invoke(cli_app, ["benchmark", "--quick", "--output-json", str(metrics_file)])
            assert res.exit_code == 0
            assert metrics_file.exists()
            data = json.loads(metrics_file.read_text())
            assert data["all_passed"] is True

    def test_cli_benchmark_failure_exit_code_1(self) -> None:
        """Benchmark subcommand exits with code 1 when benchmarks fail."""
        runner = CliRunner()
        with patch("benchmarks.run_benchmarks.run_benchmark_suite", return_value={"all_passed": False}):
            res = runner.invoke(cli_app, ["benchmark"])
            assert res.exit_code == 1


# ============================================================================
# 2. Programmatic API Stress & Boundary Tests
# ============================================================================


@pytest.mark.tier2
class TestProgrammaticAPIStress:
    """Stress tests for high-level Python API functions and domain exceptions."""

    def test_api_validate_drop_matching_pinned_schema(self, fixture_paths: Any) -> None:
        """validate_drop passes when validated against a schema inferred and pinned from same drop."""
        with tempfile.TemporaryDirectory() as td:
            pinned_path = Path(td) / "clean_schema.json"
            infer_and_pin(fixture_paths.drop_001_parquet, pinned_path)

            results = validate_drop(fixture_paths.drop_001_parquet, schema_path=pinned_path)
            assert len(results) > 0
            assert all(r.passed for r in results)

    def test_api_validate_drop_breaking_drift_raises_schema_validation_error(self, fixture_paths: Any) -> None:
        """validate_drop raises SchemaValidationError on breaking schema drift."""
        with pytest.raises(SchemaValidationError) as excinfo:
            validate_drop(fixture_paths.drop_001_parquet, schema_path=fixture_paths.schema_drift_breaking)
        assert "breaking schema drift" in str(excinfo.value)

    def test_api_validate_drop_missing_required_column_raises(self, fixture_paths: Any) -> None:
        """validate_drop raises SchemaValidationError when a required column is removed."""
        with tempfile.TemporaryDirectory() as td:
            pinned_path = Path(td) / "clean_schema.json"
            infer_and_pin(fixture_paths.drop_001_parquet, pinned_path)

            # Drop 'metric' column from table and save to new parquet
            orig_table = pq.read_table(fixture_paths.drop_001_parquet)
            mutated_table = orig_table.drop(["metric"])
            mutated_path = Path(td) / "mutated_missing_metric.parquet"
            pq.write_table(mutated_table, mutated_path)

            with pytest.raises(SchemaValidationError) as excinfo:
                validate_drop(mutated_path, schema_path=pinned_path)
            assert "metric" in str(excinfo.value)

    def test_api_validate_drop_incompatible_type_change_raises(self, fixture_paths: Any) -> None:
        """validate_drop raises SchemaValidationError when column type changes incompatibly."""
        with tempfile.TemporaryDirectory() as td:
            pinned_path = Path(td) / "clean_schema.json"
            infer_and_pin(fixture_paths.drop_001_parquet, pinned_path)

            # Alter 'value' column from double to string
            orig_table = pq.read_table(fixture_paths.drop_001_parquet)
            str_values = [str(x) for x in orig_table["value"].to_pylist()]
            cols = []
            for name in orig_table.column_names:
                if name == "value":
                    cols.append(pa.array(str_values, type=pa.string()))
                else:
                    cols.append(orig_table[name])
            mutated_table = pa.Table.from_arrays(cols, names=orig_table.column_names)
            mutated_path = Path(td) / "mutated_type.parquet"
            pq.write_table(mutated_table, mutated_path)

            with pytest.raises(SchemaValidationError) as excinfo:
                validate_drop(mutated_path, schema_path=pinned_path)
            assert "Incompatible type" in str(excinfo.value)

    def test_api_validate_drop_nonexistent_files_raise(self) -> None:
        """validate_drop raises IngestionError or SchemaValidationError for missing paths."""
        with pytest.raises(IngestionError):
            validate_drop("/tmp/missing_vendor_drop_file.parquet")

        with tempfile.TemporaryDirectory() as td:
            dummy_file = Path(td) / "dummy.parquet"
            pq.write_table(pa.Table.from_arrays([pa.array([1])], names=["col"]), dummy_file)
            with pytest.raises(SchemaValidationError):
                validate_drop(dummy_file, schema_path="/tmp/nonexistent_schema.json")

    def test_api_run_onboarding_pipeline_e2e(self, fixture_paths: Any) -> None:
        """run_onboarding_pipeline executes full cycle returning structured metadata."""
        with tempfile.TemporaryDirectory() as td:
            store_dir = Path(td) / "bitemporal_store"
            result = run_onboarding_pipeline(
                file_path=fixture_paths.drop_001_parquet,
                dataset_id="EQUITY_DAILY",
                vendor_id="SYNTH_VENDOR_A",
                drop_id="DROP_001",
                store_dir=store_dir,
            )
            assert result["status"] == "SUCCESS"
            assert result["validation_passed"] is True
            assert result["record_count"] == 60
            assert result["dataset_id"] == "EQUITY_DAILY"
            assert len(result["validation_results"]) > 0

    def test_api_query_as_of_timezones(self, fixture_paths: Any) -> None:
        """query_as_of handles aware datetime, naive datetime, ISO string with Z and offset."""
        with tempfile.TemporaryDirectory() as td:
            store_dir = Path(td) / "store"
            store = BitemporalParquetStore(store_dir)
            tbl = pq.read_table(fixture_paths.drop_001_parquet)
            store.append(tbl, dataset_id="EQUITY_DAILY", drop_id="DROP_001")

            # 1. ISO string with Z
            t1 = query_as_of("EQUITY_DAILY", "2023-10-06T20:00:00Z", store_dir)
            assert t1.num_rows > 0

            # 2. ISO string with offset
            t2 = query_as_of("EQUITY_DAILY", "2023-10-06T20:00:00+00:00", store_dir)
            assert t2.num_rows == t1.num_rows

            # 3. Naive datetime
            dt_naive = datetime(2023, 10, 6, 20, 0, 0)
            t3 = query_as_of("EQUITY_DAILY", dt_naive, store_dir)
            assert t3.num_rows == t1.num_rows

            # 4. Aware datetime
            dt_aware = datetime(2023, 10, 6, 20, 0, 0, tzinfo=UTC)
            t4 = query_as_of("EQUITY_DAILY", dt_aware, store_dir)
            assert t4.num_rows == t1.num_rows

    def test_api_diff_drops_missing_files_raise(self, fixture_paths: Any) -> None:
        """diff_drops raises IngestionError if either file does not exist."""
        ghost = "/tmp/strictly_missing.parquet"
        with pytest.raises(IngestionError):
            diff_drops(ghost, fixture_paths.drop_002_parquet)

        with pytest.raises(IngestionError):
            diff_drops(fixture_paths.drop_001_parquet, ghost)

    def test_api_diff_drops_success(self, fixture_paths: Any) -> None:
        """diff_drops returns diff table and summary detecting restatements."""
        diff_tbl, summary = diff_drops(fixture_paths.drop_001_parquet, fixture_paths.drop_002_parquet)
        assert summary.restated_records == 10
        assert diff_tbl.num_rows > 0


# ============================================================================
# 3. Flight Server & Client Stress & Lifecycle
# ============================================================================


@pytest.mark.tier2
class TestFlightServerClientStress:
    """Stress tests, malformed ticket fuzzing, concurrency, and client lifecycle for Arrow Flight."""

    def test_flight_ticket_serialization_and_deserialization(self) -> None:
        """Round-trip encoding and decoding of query ticket parameters."""
        as_of_dt = datetime(2023, 10, 6, 20, 0, 0, tzinfo=UTC)
        ticket_bytes = create_flight_ticket(
            dataset_id="TEST_DS",
            as_of=as_of_dt,
            entity_ids=["ENT_1", "ENT_2"],
            metric="CLOSE_PRICE",
        )
        parsed = parse_flight_ticket(ticket_bytes)
        assert parsed["dataset_id"] == "TEST_DS"
        assert parsed["as_of"] == as_of_dt.isoformat()
        assert parsed["entity_ids"] == ["ENT_1", "ENT_2"]
        assert parsed["metric"] == "CLOSE_PRICE"

    def test_flight_server_malformed_tickets_raise_invalid_argument(self) -> None:
        """Flight server do_get raises fl.FlightInvalidArgumentError on malformed tickets."""
        server = BitemporalFlightServer(location=f"grpc://127.0.0.1:{_get_free_port()}", store="data/store")
        try:
            # 1. Empty bytes
            with pytest.raises(FlightInvalidArgumentError):
                server.do_get(None, fl.Ticket(b""))

            # 2. Empty JSON dict (missing dataset_id and as_of)
            with pytest.raises(FlightInvalidArgumentError):
                server.do_get(None, fl.Ticket(b"{}"))

            # 3. Missing as_of
            with pytest.raises(FlightInvalidArgumentError):
                server.do_get(None, fl.Ticket(b'{"dataset_id": "DS"}'))

            # 4. Missing dataset_id
            with pytest.raises(FlightInvalidArgumentError):
                server.do_get(None, fl.Ticket(b'{"as_of": "2023-10-06T20:00:00Z"}'))

            # 5. Invalid JSON syntax
            with pytest.raises(FlightInvalidArgumentError):
                server.do_get(None, fl.Ticket(b'{"dataset_id": "DS", invalid_json}'))

            # 6. Invalid date format
            with pytest.raises(FlightInvalidArgumentError):
                server.do_get(None, fl.Ticket(b'{"dataset_id": "DS", "as_of": "not-a-valid-timestamp"}'))

            # 7. Non-dict JSON payload (list)
            with pytest.raises(FlightInvalidArgumentError):
                server.do_get(None, fl.Ticket(b"[1, 2, 3]"))

            # 8. Non-dict JSON payload (string)
            with pytest.raises(FlightInvalidArgumentError):
                server.do_get(None, fl.Ticket(b'"hello world"'))
        finally:
            server.shutdown()

    def test_flight_client_rpc_wire_error_propagation(self) -> None:
        """Client receives FlightServerError detailing invalid ticket over gRPC wire protocol."""
        port = _get_free_port()
        server = BitemporalFlightServer(location=f"grpc://127.0.0.1:{port}", store="data/store")
        client = BitemporalFlightClient(f"grpc://127.0.0.1:{port}")
        try:
            with pytest.raises(fl.FlightServerError) as excinfo:
                reader = client.client.do_get(fl.Ticket(b"{}"))
                reader.read_all()
            assert "Invalid flight ticket" in str(excinfo.value)
        finally:
            client.close()
            server.shutdown()

    def test_flight_server_concurrent_client_requests(self, fixture_paths: Any) -> None:
        """Server handles high-concurrency client requests across threads without failure."""
        port = _get_free_port()
        with tempfile.TemporaryDirectory() as td:
            store_dir = Path(td) / "store"
            store = BitemporalParquetStore(store_dir)
            tbl = pq.read_table(fixture_paths.drop_001_parquet)
            store.append(tbl, dataset_id="EQUITY_DAILY", drop_id="DROP_001")

            server = BitemporalFlightServer(location=f"grpc://127.0.0.1:{port}", store=store_dir)

            def client_query_task(idx: int) -> int:
                with BitemporalFlightClient(f"grpc://127.0.0.1:{port}") as cl:
                    ent = ["ENT_AAPL"] if idx % 2 == 0 else None
                    table = cl.as_of_query(
                        dataset_id="EQUITY_DAILY",
                        as_of="2023-10-06T20:00:00Z",
                        entity_ids=ent,
                    )
                    return table.num_rows

            try:
                num_requests = 20
                with ThreadPoolExecutor(max_workers=8) as pool:
                    futures = [pool.submit(client_query_task, i) for i in range(num_requests)]
                    row_counts = [f.result(timeout=10.0) for f in futures]

                assert len(row_counts) == num_requests
                # Half filtered to ENT_AAPL (6 rows), half unfiltered (60 rows)
                assert all(c in (6, 60) for c in row_counts)
            finally:
                server.shutdown()

    def test_flight_client_lifecycle_and_context_manager(self) -> None:
        """Client supports close(), multiple close() calls, and context manager protocol."""
        port = _get_free_port()
        server = BitemporalFlightServer(location=f"grpc://127.0.0.1:{port}", store="data/store")
        try:
            # Context manager usage
            with BitemporalFlightClient(f"grpc://127.0.0.1:{port}") as client:
                info = client.get_flight_info("EQUITY_DAILY")
                assert info is not None
                assert len(info.endpoints) > 0

            # Multiple explicit close calls are idempotent and non-raising
            client2 = BitemporalFlightClient(f"grpc://127.0.0.1:{port}")
            client2.close()
            client2.close()
        finally:
            server.shutdown()

    def test_flight_server_metadata_endpoints(self, fixture_paths: Any) -> None:
        """Server responds to list_flights and get_flight_info descriptors."""
        port = _get_free_port()
        with tempfile.TemporaryDirectory() as td:
            store_dir = Path(td) / "store"
            store = BitemporalParquetStore(store_dir)
            tbl = pq.read_table(fixture_paths.drop_001_parquet)
            store.append(tbl, dataset_id="EQUITY_DAILY", drop_id="DROP_001")

            server = BitemporalFlightServer(location=f"grpc://127.0.0.1:{port}", store=store_dir)
            with BitemporalFlightClient(f"grpc://127.0.0.1:{port}") as client:
                try:
                    flights = client.list_flights()
                    assert len(flights) >= 1

                    info = client.get_flight_info("EQUITY_DAILY")
                    assert info.schema is not None
                    assert "entity_id" in [f.name for f in info.schema]
                finally:
                    server.shutdown()


# ============================================================================
# 4. Dataset Loaders & Synthetic Generator Boundaries
# ============================================================================


@pytest.mark.tier2
class TestDatasetLoadersAndGeneratorBoundaries:
    """Boundary conditions and corrupt file handling for SEC, ALFRED, and Synthetic loaders."""

    def test_sec_loader_corrupt_files_raise(self) -> None:
        """SECDeraLoader raises or handles unparseable TSV archives gracefully."""
        loader = SECDeraLoader()
        with tempfile.TemporaryDirectory() as td:
            bad_sub = Path(td) / "sub.txt"
            bad_num = Path(td) / "num.txt"
            bad_sub.write_text("random\tmalformed\tdata\n")
            bad_num.write_text("corrupted\n")

            with pytest.raises(Exception):
                loader.load_raw(bad_sub, bad_num)

    def test_alfred_loader_corrupt_csv_raise(self) -> None:
        """AlfredVintageLoader raises or handles unparseable CSV series gracefully."""
        loader = AlfredVintageLoader()
        with tempfile.TemporaryDirectory() as td:
            bad_csv = Path(td) / "bad.csv"
            bad_csv.write_text("not_a_valid_alfred_csv\nno_headers_here\n")

            with pytest.raises(Exception):
                loader.load_series(bad_csv)

    def test_synthetic_generator_negative_anomalies_and_reproducibility(self) -> None:
        """SyntheticGenerator reproduces exact records across identical seeds and injects negative values."""
        gen1 = SyntheticGenerator(seed=123)
        records1 = gen1.generate_drop(num_entities=5, days=3, inject_negative_rate=0.5)

        gen2 = SyntheticGenerator(seed=123)
        records2 = gen2.generate_drop(num_entities=5, days=3, inject_negative_rate=0.5)

        assert records1 == records2
        # Verify negative values were actually injected
        has_negative = any(r["value"] < 0 for r in records1)
        assert has_negative is True
