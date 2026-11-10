"""Command-line interface for the Vendor Data Onboarding Kit."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from vdok import __version__
from vdok.adapters.synthetic import generate_synthetic_sequence
from vdok.schema import (
    accept_schema_changes,
    check_schema,
    infer_schema,
    pin_schema,
)

app = typer.Typer(
    name="vdok",
    help="Vendor Data Onboarding Kit command-line tool.",
    add_completion=False,
)

schema_app = typer.Typer(
    name="schema",
    help="Vendor schema inference, pinning, drift checking, and acceptance.",
)
app.add_typer(schema_app, name="schema")


@app.command()
def version() -> None:
    """Print package version."""
    typer.echo(f"vdok version {__version__}")


@app.command()
def synth(
    size_gb: str = typer.Option("1", "--size-gb", help="Target size in gigabytes"),
    seed: int = typer.Option(1, "--seed", help="Random seed for reproducibility"),
    faults: str = typer.Option("default", "--faults", help="Planted fault configuration"),
    output_dir: str = typer.Option(
        "data/raw/synthetic", "--output-dir", help="Output directory path"
    ),
    format: str = typer.Option("csv", "--format", help="File format (csv, parquet, jsonl)"),
    num_drops: int = typer.Option(3, "--num-drops", help="Number of sequential drops to produce"),
) -> None:
    """Generate synthetic vendor drop sequence with planted faults."""
    out_path = Path(output_dir)
    res_path = generate_synthetic_sequence(
        output_dir=out_path,
        size_gb=size_gb,
        seed=seed,
        faults=faults,
        num_drops=num_drops,
        format=format,
    )
    typer.echo(
        f"Synthetic generator: size_gb={size_gb}, seed={seed}, faults={faults} -> {res_path}"
    )


@app.command()
def onboard(
    vendor: str = typer.Argument(..., help="Vendor identifier"),
    drop: str = typer.Argument(..., help="Path to raw drop directory or archive"),
    as_of: Optional[str] = typer.Option(
        None, "--as-of", help="Override knowledge timestamp (ISO 8601 string)"
    ),
    manifest: Optional[str] = typer.Option(
        None, "--manifest", "-m", help="Path to vendor manifest YAML"
    ),
    rules: Optional[str] = typer.Option(
        None, "--rules", "-r", help="Path to validation rules YAML"
    ),
    symbols: Optional[str] = typer.Option(
        None, "--symbols", "-s", help="Path to symbol master Parquet"
    ),
    store_dir: str = typer.Option("data/store", "--store-dir", help="Base store directory"),
    results_dir: str = typer.Option(
        "data/results", "--results-dir", help="Base results directory for reports"
    ),
    force: bool = typer.Option(False, "--force", "-f", help="Force re-ingestion if drop exists"),
) -> None:
    """Onboard a vendor drop into the bitemporal store."""
    import datetime

    import dateutil.parser

    from vdok.onboard import onboard_drop

    p_drop = Path(drop)
    if not p_drop.exists():
        typer.echo(f"Error: Drop path does not exist: {drop}", err=True)
        raise typer.Exit(code=2)

    as_of_dt: datetime.datetime | None = None
    if as_of:
        try:
            as_of_dt = dateutil.parser.isoparse(as_of)
            if as_of_dt.tzinfo is None:
                as_of_dt = as_of_dt.replace(tzinfo=datetime.timezone.utc)
        except Exception as exc:
            typer.echo(f"Error: Invalid --as-of timestamp '{as_of}': {exc}", err=True)
            raise typer.Exit(code=2)

    typer.echo(f"Onboarding vendor drop: vendor={vendor}, drop={drop}")
    try:
        rep, exit_code = onboard_drop(
            vendor=vendor,
            drop_path=p_drop,
            manifest_path=manifest,
            rules_path=rules,
            symbol_master_path=symbols,
            store_dir=store_dir,
            results_dir=results_dir,
            as_of=as_of_dt,
            force=force,
        )
    except FileNotFoundError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=2) from exc
    except Exception as exc:
        typer.echo(f"Onboarding failure: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    typer.echo(f"Verdict: {rep.verdict.value}")
    typer.echo(f"Reason: {rep.verdict_reason}")
    typer.echo(f"Report written to: {results_dir}/{vendor}/{rep.drop_id}/report.md")

    if exit_code != 0:
        raise typer.Exit(code=exit_code)


@app.command()
def report(
    vendor: str = typer.Argument(..., help="Vendor identifier"),
    drop: Optional[str] = typer.Option(None, "--drop", help="Drop identifier to report on"),
    results_dir: str = typer.Option(
        "data/results", "--results-dir", help="Base directory where reports reside"
    ),
    json_output: bool = typer.Option(False, "--json-output", help="Output report in JSON format"),
) -> None:
    """Generate or display a vendor readiness report."""
    from vdok.report import load_report

    target_drop = drop
    if target_drop is None:
        v_results = Path(results_dir) / vendor
        if v_results.exists():
            subdirs = [
                d for d in v_results.iterdir() if d.is_dir() and (d / "report.json").exists()
            ]
            if subdirs:
                subdirs.sort(key=lambda d: (d / "report.json").stat().st_mtime, reverse=True)
                target_drop = subdirs[0].name

    if target_drop is None:
        typer.echo(
            f"Error: No report found for vendor '{vendor}'. Provide --drop <drop_id>.",
            err=True,
        )
        raise typer.Exit(code=2)

    rep = load_report(vendor=vendor, drop_id=target_drop, base_dir=results_dir)
    if rep is None:
        typer.echo(
            f"Error: Report not found for vendor '{vendor}' and drop '{target_drop}'.",
            err=True,
        )
        raise typer.Exit(code=2)

    if json_output:
        typer.echo(rep.to_json())
    else:
        typer.echo(rep.to_markdown())


@app.command()
def diff(
    vendor: str = typer.Argument(..., help="Vendor identifier"),
    drop_a: str = typer.Argument(..., help="First drop identifier"),
    drop_b: str = typer.Argument(..., help="Second drop identifier"),
    store_dir: str = typer.Option("data/store", "--store-dir", help="Base store directory"),
    json_output: bool = typer.Option(False, "--json-output", help="Output diff in JSON format"),
) -> None:
    """Compare two vendor drops and emit differences."""
    from vdok.report import diff_drops

    try:
        diff_res = diff_drops(
            store_dir=store_dir,
            vendor=vendor,
            drop_a=drop_a,
            drop_b=drop_b,
        )
    except FileNotFoundError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=2) from exc
    except Exception as exc:
        typer.echo(f"Diff execution failure: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    if json_output:
        typer.echo(diff_res.to_json())
    else:
        typer.echo(diff_res.to_markdown())


@app.command()
def leaks(
    log: str = typer.Option(..., "--log", help="Path to audit log file"),
    decision_time_column: str = typer.Option(
        "decision_time",
        "--decision-time-column",
        help="Decision time column name",
    ),
    json_output: bool = typer.Option(
        False,
        "--json-output",
        help="Output report in JSON format",
    ),
) -> None:
    """Detect point-in-time read leaks from query audit logs."""
    from vdok.query import detect_leaks

    typer.echo(f"Leak detection: log={log}, col={decision_time_column}")

    log_path = Path(log)
    if not log_path.exists():
        typer.echo(f"Audit log file not found: {log_path}")
        return

    report = detect_leaks(log_path, decision_time_column=decision_time_column)

    if json_output:
        typer.echo(report.model_dump_json(indent=2))
    else:
        typer.echo(
            f"  Total queries: {report.total_reads}\n"
            f"  Total leaks: {report.leaks_detected}\n"
            f"  As-Of leaks: {report.as_of_leaks}\n"
            f"  Knowledge leaks: {report.knowledge_leaks}"
        )

    if report.has_leaks:
        typer.echo("FAILURE: Look-ahead bias detected in query audit log.")
        raise typer.Exit(code=1)
    else:
        typer.echo("SUCCESS: Zero look-ahead leaks detected.")
        raise typer.Exit(code=0)


@schema_app.command("infer")
def schema_infer(
    arg1: str = typer.Argument(..., help="Drop path or vendor identifier"),
    arg2: Optional[str] = typer.Argument(
        None, help="Drop path if vendor identifier is provided first"
    ),
    sample_rows: int = typer.Option(200000, "--sample-rows", help="Maximum sample rows per file"),
    output: Optional[str] = typer.Option(
        None, "--output", "-o", help="Optional destination path for schema YAML"
    ),
) -> None:
    """Infer column schema from sample files."""
    if arg2 is not None:
        vendor = arg1
        drop_path = arg2
    else:
        vendor = "default"
        drop_path = arg1

    p = Path(drop_path)
    if not p.exists():
        typer.echo(f"Error: Drop path does not exist: {drop_path}", err=True)
        raise typer.Exit(code=2)

    typer.echo(f"Schema infer: path={drop_path}")
    try:
        inferred = infer_schema(
            drop_path=drop_path,
            vendor=vendor,
            max_sample_rows=sample_rows,
        )
        yaml_out = inferred.to_yaml()
        if output:
            out_p = Path(output)
            out_p.parent.mkdir(parents=True, exist_ok=True)
            out_p.write_text(yaml_out, encoding="utf-8")
            typer.echo(f"Schema written to: {output}")
        else:
            typer.echo(yaml_out)
    except Exception as exc:
        typer.echo(f"Error inferring schema: {exc}", err=True)
        raise typer.Exit(code=1) from exc


@schema_app.command("pin")
def schema_pin(
    vendor: str = typer.Argument(..., help="Vendor identifier"),
    drop_path: str = typer.Argument(..., help="Path to drop directory or file to pin"),
    schema_dir: str = typer.Option(
        "schemas", "--schema-dir", help="Directory where schemas are stored"
    ),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Explicit target file path"),
) -> None:
    """Pin current inferred schema for vendor to schemas/<vendor>.yaml."""
    p = Path(drop_path)
    if not p.exists():
        typer.echo(f"Error: Drop path does not exist: {drop_path}", err=True)
        raise typer.Exit(code=2)

    try:
        target_dir = Path(output).parent if output else Path(schema_dir)
        pinned_path = pin_schema(
            vendor=vendor,
            drop_path=drop_path,
            schema_dir=target_dir,
        )
        if output and str(pinned_path) != output:
            pinned_path.replace(output)
            pinned_path = Path(output)
        typer.echo(f"Schema pin: vendor={vendor}, pinned to {pinned_path}")
    except Exception as exc:
        typer.echo(f"Error pinning schema: {exc}", err=True)
        raise typer.Exit(code=1) from exc


@schema_app.command("check")
def schema_check(
    vendor: str = typer.Argument(..., help="Vendor identifier"),
    drop_path: str = typer.Argument(..., help="Path to drop to verify"),
    schema_dir: str = typer.Option(
        "schemas", "--schema-dir", help="Directory where pinned schemas reside"
    ),
) -> None:
    """Check drop schema against pinned vendor schema."""
    schema_file = Path(schema_dir) / f"{vendor}.yaml"
    if not schema_file.exists():
        typer.echo(
            f"Error: Pinned schema not found for vendor '{vendor}' at {schema_file}. "
            "Run 'vdok schema pin' first.",
            err=True,
        )
        raise typer.Exit(code=2)

    p = Path(drop_path)
    if not p.exists():
        typer.echo(f"Error: Drop path does not exist: {drop_path}", err=True)
        raise typer.Exit(code=2)

    typer.echo(f"Schema check: vendor={vendor}, path={drop_path}")
    try:
        diff = check_schema(
            vendor=vendor,
            drop_path=drop_path,
            schema_dir=schema_dir,
        )
    except Exception as exc:
        typer.echo(f"Error checking schema: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    if not diff.differences:
        typer.echo("VERDICT: COMPLIANT - Drop matches pinned schema.")
        return

    if diff.has_hold:
        typer.echo("VERDICT: HELD - Breaking schema drift detected:")
        for d in diff.differences:
            typer.echo(
                f"  [{d.severity.value.upper()}] {d.category.value} on '{d.column}': "
                f"{d.proposed_change}"
            )
        raise typer.Exit(code=1)

    typer.echo("VERDICT: READY WITH WARNINGS - Non-breaking schema differences detected:")
    for d in diff.differences:
        typer.echo(
            f"  [{d.severity.value.upper()}] {d.category.value} on '{d.column}': "
            f"{d.proposed_change}"
        )


@schema_app.command("accept")
def schema_accept(
    vendor: str = typer.Argument(..., help="Vendor identifier"),
    drop_path: Optional[str] = typer.Option(
        None, "--drop-path", help="Optional drop path to apply drift changes from"
    ),
    schema_dir: str = typer.Option(
        "schemas", "--schema-dir", help="Directory where schemas reside"
    ),
) -> None:
    """Accept proposed schema changes for vendor."""
    schema_file = Path(schema_dir) / f"{vendor}.yaml"
    if not schema_file.exists():
        typer.echo(
            f"Error: Pinned schema not found for vendor '{vendor}' at {schema_file}.",
            err=True,
        )
        raise typer.Exit(code=2)

    if drop_path is not None and not Path(drop_path).exists():
        typer.echo(f"Error: Drop path does not exist: {drop_path}", err=True)
        raise typer.Exit(code=2)

    try:
        updated = accept_schema_changes(
            vendor=vendor,
            drop_path=drop_path,
            schema_dir=schema_dir,
        )
        typer.echo(f"Schema accept: vendor={vendor}, updated to version {updated.version}")
    except Exception as exc:
        typer.echo(f"Error accepting schema changes: {exc}", err=True)
        raise typer.Exit(code=1) from exc


def main() -> None:
    """Entry point for console script."""
    app()


if __name__ == "__main__":
    main()
