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
) -> None:
    """Onboard a vendor drop into the bitemporal store."""
    typer.echo(f"Onboard vendor drop: vendor={vendor}, drop={drop}")


@app.command()
def report(
    vendor: str = typer.Argument(..., help="Vendor identifier"),
    drop: Optional[str] = typer.Option(None, "--drop", help="Drop identifier to report on"),
) -> None:
    """Generate a vendor readiness report."""
    typer.echo(f"Readiness report: vendor={vendor}, drop={drop}")


@app.command()
def diff(
    vendor: str = typer.Argument(..., help="Vendor identifier"),
    drop_a: str = typer.Argument(..., help="First drop identifier"),
    drop_b: str = typer.Argument(..., help="Second drop identifier"),
) -> None:
    """Compare two vendor drops and emit differences."""
    typer.echo(f"Diff vendor drops: vendor={vendor}, drop_a={drop_a}, drop_b={drop_b}")


@app.command()
def leaks(
    log: str = typer.Option(..., "--log", help="Path to audit log file"),
    decision_time_column: str = typer.Option(
        "decision_time",
        "--decision-time-column",
        help="Decision time column name",
    ),
) -> None:
    """Detect point-in-time read leaks from query audit logs."""
    typer.echo(f"Leak detection: log={log}, col={decision_time_column}")


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
