"""Unit tests for vdok CLI schema subcommands (infer, pin, check, accept)."""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from vdok.cli import app

runner = CliRunner()
FIXTURES_DIR = Path("tests/fixtures")


def test_cli_infer_stdout() -> None:
    res = runner.invoke(app, ["schema", "infer", str(FIXTURES_DIR / "drops" / "baseline_csv")])
    assert res.exit_code == 0
    assert "Schema infer" in res.output
    assert "record_id" in res.output
    assert "price" in res.output


def test_cli_infer_with_vendor() -> None:
    res = runner.invoke(
        app,
        ["schema", "infer", "sec_fsds", str(FIXTURES_DIR / "drops" / "baseline_csv")],
    )
    assert res.exit_code == 0
    assert "vendor: sec_fsds" in res.output


def test_cli_infer_output_file(tmp_path: Path) -> None:
    out_file = tmp_path / "inferred.yaml"
    res = runner.invoke(
        app,
        [
            "schema",
            "infer",
            str(FIXTURES_DIR / "drops" / "baseline_csv"),
            "-o",
            str(out_file),
        ],
    )
    assert res.exit_code == 0
    assert out_file.exists()
    assert "record_id" in out_file.read_text(encoding="utf-8")


def test_cli_infer_missing_drop() -> None:
    res = runner.invoke(app, ["schema", "infer", "nonexistent/path/drop"])
    assert res.exit_code == 2
    assert "Drop path does not exist" in res.output


def test_cli_pin_success(tmp_path: Path) -> None:
    res = runner.invoke(
        app,
        [
            "schema",
            "pin",
            "vendor_test",
            str(FIXTURES_DIR / "drops" / "baseline_csv"),
            "--schema-dir",
            str(tmp_path),
        ],
    )
    assert res.exit_code == 0
    assert "Schema pin" in res.output
    assert (tmp_path / "vendor_test.yaml").exists()


def test_cli_pin_missing_drop(tmp_path: Path) -> None:
    res = runner.invoke(
        app,
        [
            "schema",
            "pin",
            "vendor_test",
            "nonexistent/drop/path",
            "--schema-dir",
            str(tmp_path),
        ],
    )
    assert res.exit_code == 2
    assert "Drop path does not exist" in res.output


def test_cli_check_compliant(tmp_path: Path) -> None:
    # First pin schema
    runner.invoke(
        app,
        [
            "schema",
            "pin",
            "vendor_check",
            str(FIXTURES_DIR / "drops" / "baseline_csv"),
            "--schema-dir",
            str(tmp_path),
        ],
    )

    # Check identical drop
    res = runner.invoke(
        app,
        [
            "schema",
            "check",
            "vendor_check",
            str(FIXTURES_DIR / "drops" / "baseline_csv"),
            "--schema-dir",
            str(tmp_path),
        ],
    )
    assert res.exit_code == 0
    assert "VERDICT: COMPLIANT" in res.output


def test_cli_check_warning(tmp_path: Path) -> None:
    # Pin baseline
    runner.invoke(
        app,
        [
            "schema",
            "pin",
            "vendor_warn",
            str(FIXTURES_DIR / "drops" / "baseline_csv"),
            "--schema-dir",
            str(tmp_path),
        ],
    )

    # Check drop with added_column (warning only)
    res = runner.invoke(
        app,
        [
            "schema",
            "check",
            "vendor_warn",
            str(FIXTURES_DIR / "drift" / "csv" / "added_column"),
            "--schema-dir",
            str(tmp_path),
        ],
    )
    assert res.exit_code == 0
    assert "READY WITH WARNINGS" in res.output
    assert "added_column" in res.output


def test_cli_check_hold_removed_column(tmp_path: Path) -> None:
    # Pin baseline
    runner.invoke(
        app,
        [
            "schema",
            "pin",
            "vendor_hold",
            str(FIXTURES_DIR / "drops" / "baseline_csv"),
            "--schema-dir",
            str(tmp_path),
        ],
    )

    # Check drop with removed_column (breaking hold)
    res = runner.invoke(
        app,
        [
            "schema",
            "check",
            "vendor_hold",
            str(FIXTURES_DIR / "drift" / "csv" / "removed_column"),
            "--schema-dir",
            str(tmp_path),
        ],
    )
    assert res.exit_code == 1
    assert "VERDICT: HELD" in res.output
    assert "removed_column" in res.output


def test_cli_check_hold_type_widened(tmp_path: Path) -> None:
    # Pin baseline
    runner.invoke(
        app,
        [
            "schema",
            "pin",
            "vendor_widened",
            str(FIXTURES_DIR / "drops" / "baseline_csv"),
            "--schema-dir",
            str(tmp_path),
        ],
    )

    # Check drop with type_widened (breaking hold)
    res = runner.invoke(
        app,
        [
            "schema",
            "check",
            "vendor_widened",
            str(FIXTURES_DIR / "drift" / "csv" / "type_widened"),
            "--schema-dir",
            str(tmp_path),
        ],
    )
    assert res.exit_code == 1
    assert "VERDICT: HELD" in res.output
    assert "type_widened" in res.output


def test_cli_check_missing_schema(tmp_path: Path) -> None:
    res = runner.invoke(
        app,
        [
            "schema",
            "check",
            "missing_vendor",
            str(FIXTURES_DIR / "drops" / "baseline_csv"),
            "--schema-dir",
            str(tmp_path),
        ],
    )
    assert res.exit_code == 2
    assert "Pinned schema not found" in res.output


def test_cli_check_missing_drop(tmp_path: Path) -> None:
    runner.invoke(
        app,
        [
            "schema",
            "pin",
            "v_drop_test",
            str(FIXTURES_DIR / "drops" / "baseline_csv"),
            "--schema-dir",
            str(tmp_path),
        ],
    )
    res = runner.invoke(
        app,
        [
            "schema",
            "check",
            "v_drop_test",
            "nonexistent/drop/path",
            "--schema-dir",
            str(tmp_path),
        ],
    )
    assert res.exit_code == 2
    assert "Drop path does not exist" in res.output


def test_cli_accept_workflow(tmp_path: Path) -> None:
    # Pin baseline
    runner.invoke(
        app,
        [
            "schema",
            "pin",
            "v_accept",
            str(FIXTURES_DIR / "drops" / "baseline_csv"),
            "--schema-dir",
            str(tmp_path),
        ],
    )

    # Accept changes from drift drop
    res = runner.invoke(
        app,
        [
            "schema",
            "accept",
            "v_accept",
            "--drop-path",
            str(FIXTURES_DIR / "drift" / "csv" / "added_column"),
            "--schema-dir",
            str(tmp_path),
        ],
    )
    assert res.exit_code == 0
    assert "updated to version 2" in res.output

    # Accept without drop path increments version
    res2 = runner.invoke(
        app,
        [
            "schema",
            "accept",
            "v_accept",
            "--schema-dir",
            str(tmp_path),
        ],
    )
    assert res2.exit_code == 0
    assert "updated to version 3" in res2.output


def test_cli_accept_missing_schema(tmp_path: Path) -> None:
    res = runner.invoke(
        app,
        ["schema", "accept", "missing_vendor", "--schema-dir", str(tmp_path)],
    )
    assert res.exit_code == 2
    assert "Pinned schema not found" in res.output


def test_cli_accept_missing_drop(tmp_path: Path) -> None:
    runner.invoke(
        app,
        [
            "schema",
            "pin",
            "v_accept_missing",
            str(FIXTURES_DIR / "drops" / "baseline_csv"),
            "--schema-dir",
            str(tmp_path),
        ],
    )
    res = runner.invoke(
        app,
        [
            "schema",
            "accept",
            "v_accept_missing",
            "--drop-path",
            "nonexistent/drop",
            "--schema-dir",
            str(tmp_path),
        ],
    )
    assert res.exit_code == 2
    assert "Drop path does not exist" in res.output


def test_cli_pin_custom_output(tmp_path: Path) -> None:
    custom_out = tmp_path / "custom_schema.yaml"
    res = runner.invoke(
        app,
        [
            "schema",
            "pin",
            "v_custom",
            str(FIXTURES_DIR / "drops" / "baseline_csv"),
            "-o",
            str(custom_out),
        ],
    )
    assert res.exit_code == 0
    assert custom_out.exists()
