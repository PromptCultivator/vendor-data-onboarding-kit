"""Smoke tests verifying package import, version, and CLI commands."""

from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

import vdok
from vdok.cli import app, main

runner = CliRunner()


def test_package_version() -> None:
    """Verify package exposes expected version string."""
    assert vdok.__version__ == "0.1.0"


def test_cli_help() -> None:
    """Verify top-level help text renders successfully."""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "Vendor Data Onboarding Kit command-line tool" in result.output


def test_cli_version() -> None:
    """Verify version command prints version number."""
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert "0.1.0" in result.output


def test_cli_synth_command() -> None:
    """Verify synth command stub executes."""
    result = runner.invoke(app, ["synth", "--size-gb", "1", "--seed", "42"])
    assert result.exit_code == 0
    assert "Synthetic generator" in result.output


def test_cli_onboard_command() -> None:
    """Verify onboard command stub executes."""
    result = runner.invoke(app, ["onboard", "sec_fsds", "data/raw/sample"])
    assert result.exit_code == 0
    assert "Onboard vendor drop" in result.output


def test_cli_report_command() -> None:
    """Verify report command stub executes."""
    result = runner.invoke(app, ["report", "sec_fsds", "--drop", "drop_001"])
    assert result.exit_code == 0
    assert "Readiness report" in result.output


def test_cli_diff_command() -> None:
    """Verify diff command stub executes."""
    result = runner.invoke(app, ["diff", "sec_fsds", "drop_001", "drop_002"])
    assert result.exit_code == 0
    assert "Diff vendor drops" in result.output


def test_cli_leaks_command() -> None:
    """Verify leaks command stub executes."""
    result = runner.invoke(app, ["leaks", "--log", "audit.log"])
    assert result.exit_code == 0
    assert "Leak detection" in result.output


def test_cli_schema_subcommands(tmp_path: Path) -> None:
    """Verify schema subcommand group executes authentically against fixtures."""
    fixture_drop = "tests/fixtures/drops/baseline_csv"
    schema_dir = str(tmp_path / "schemas")

    # 1. Infer schema from authentic fixture
    infer_res = runner.invoke(app, ["schema", "infer", fixture_drop])
    assert infer_res.exit_code == 0
    assert "Schema infer" in infer_res.output
    assert "record_id" in infer_res.output

    # 2. Pin schema for vendor into isolated schema directory
    pin_res = runner.invoke(
        app,
        ["schema", "pin", "sec_fsds", fixture_drop, "--schema-dir", schema_dir],
    )
    assert pin_res.exit_code == 0
    assert "Schema pin" in pin_res.output
    assert "vendor=sec_fsds" in pin_res.output
    assert (Path(schema_dir) / "sec_fsds.yaml").exists()

    # 3. Check schema against the pinned contract
    check_res = runner.invoke(
        app,
        ["schema", "check", "sec_fsds", fixture_drop, "--schema-dir", schema_dir],
    )
    assert check_res.exit_code == 0
    assert "Schema check" in check_res.output
    assert "COMPLIANT" in check_res.output

    # 4. Accept schema changes (version increment)
    accept_res = runner.invoke(
        app,
        ["schema", "accept", "sec_fsds", "--schema-dir", schema_dir],
    )
    assert accept_res.exit_code == 0
    assert "Schema accept" in accept_res.output
    assert "updated to version 2" in accept_res.output


def test_cli_schema_missing_arguments_error(tmp_path: Path) -> None:
    """Verify schema subcommands exit with code 2 on missing drops or schemas."""
    # Infer with non-existent drop
    infer_res = runner.invoke(app, ["schema", "infer", "nonexistent/drop/path"])
    assert infer_res.exit_code == 2
    assert "Drop path does not exist" in infer_res.output

    # Pin with non-existent drop
    pin_res = runner.invoke(
        app,
        ["schema", "pin", "sec_fsds", "nonexistent/drop/path", "--schema-dir", str(tmp_path)],
    )
    assert pin_res.exit_code == 2
    assert "Drop path does not exist" in pin_res.output

    # Check with non-existent schema
    check_res = runner.invoke(
        app,
        ["schema", "check", "nonexistent_vendor", "tests/fixtures/drops/baseline_csv", "--schema-dir", str(tmp_path)],
    )
    assert check_res.exit_code == 2
    assert "Pinned schema not found" in check_res.output

    # Accept with non-existent schema
    accept_res = runner.invoke(
        app,
        ["schema", "accept", "nonexistent_vendor", "--schema-dir", str(tmp_path)],
    )
    assert accept_res.exit_code == 2
    assert "Pinned schema not found" in accept_res.output


def test_cli_main_entrypoint() -> None:
    """Verify main() function executes without error."""
    with patch("sys.argv", ["vdok", "version"]):
        try:
            main()
        except SystemExit as exc:
            assert exc.code == 0
