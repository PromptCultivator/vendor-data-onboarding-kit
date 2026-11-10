"""Smoke tests verifying package import, version, and CLI commands."""

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


def test_cli_schema_subcommands() -> None:
    """Verify schema subcommand group stubs execute."""
    infer_res = runner.invoke(app, ["schema", "infer", "sample_drop/"])
    assert infer_res.exit_code == 0
    assert "Schema infer" in infer_res.output

    pin_res = runner.invoke(app, ["schema", "pin", "sec_fsds"])
    assert pin_res.exit_code == 0
    assert "Schema pin" in pin_res.output

    check_res = runner.invoke(app, ["schema", "check", "sec_fsds", "sample_drop/"])
    assert check_res.exit_code == 0
    assert "Schema check" in check_res.output

    accept_res = runner.invoke(app, ["schema", "accept", "sec_fsds"])
    assert accept_res.exit_code == 0
    assert "Schema accept" in accept_res.output


def test_cli_main_entrypoint() -> None:
    """Verify main() function executes without error."""
    with patch("sys.argv", ["vdok", "version"]):
        try:
            main()
        except SystemExit as exc:
            assert exc.code == 0
