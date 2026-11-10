"""Command-line interface for the Vendor Data Onboarding Kit."""

from typing import Optional

import typer

from vdok import __version__

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
) -> None:
    """Generate synthetic vendor drop sequence with planted faults."""
    typer.echo(f"Synthetic generator: size_gb={size_gb}, seed={seed}, faults={faults}")


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
    drop_path: str = typer.Argument(..., help="Path to sample drop"),
) -> None:
    """Infer column schema from sample files."""
    typer.echo(f"Schema infer: path={drop_path}")


@schema_app.command("pin")
def schema_pin(
    vendor: str = typer.Argument(..., help="Vendor identifier"),
) -> None:
    """Pin current inferred schema for vendor."""
    typer.echo(f"Schema pin: vendor={vendor}")


@schema_app.command("check")
def schema_check(
    vendor: str = typer.Argument(..., help="Vendor identifier"),
    drop_path: str = typer.Argument(..., help="Path to drop to verify"),
) -> None:
    """Check drop schema against pinned vendor schema."""
    typer.echo(f"Schema check: vendor={vendor}, path={drop_path}")


@schema_app.command("accept")
def schema_accept(
    vendor: str = typer.Argument(..., help="Vendor identifier"),
) -> None:
    """Accept proposed schema changes for vendor."""
    typer.echo(f"Schema accept: vendor={vendor}")


def main() -> None:
    """Entry point for console script."""
    app()


if __name__ == "__main__":
    main()
