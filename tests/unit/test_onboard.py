"""Unit and integration tests for the end-to-end onboard pipeline and CLI commands."""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from vdok.adapters.synthetic import generate_synthetic_sequence
from vdok.cli import app
from vdok.onboard import onboard_drop
from vdok.report import ReportVerdict, load_report
from vdok.store import BitemporalStore

runner = CliRunner()


def test_onboard_synthetic_conforming_drop(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    store_dir = tmp_path / "store"
    results_dir = tmp_path / "results"
    schemas_dir = tmp_path / "schemas"

    # Generate single conforming drop
    generate_synthetic_sequence(
        output_dir=raw_dir,
        size_gb="0.001",
        seed=42,
        faults="none",
        num_drops=1,
        format="csv",
    )

    drop_path = raw_dir / "drop_1"

    report, exit_code = onboard_drop(
        vendor="synthetic",
        drop_path=drop_path,
        manifest_path="manifests/synthetic.yaml",
        schema_dir=schemas_dir,
        store_dir=store_dir,
        results_dir=results_dir,
    )

    assert exit_code == 0
    assert report.verdict in (ReportVerdict.READY, ReportVerdict.READY_WITH_WARNINGS)
    assert report.drop_summary.total_rows > 0

    # Verify report files saved on disk
    saved = load_report(vendor="synthetic", drop_id=report.drop_id, base_dir=results_dir)
    assert saved is not None
    assert saved.drop_id == report.drop_id

    # Verify partition written to store
    store = BitemporalStore(base_dir=store_dir, vendor="synthetic")
    history = store.get_manifest_history()
    assert len(history) == 1
    assert history[0].drop_id == report.drop_id

    # Verify G3 Idempotency: re-running same drop returns existing report without duplicates
    report2, exit_code2 = onboard_drop(
        vendor="synthetic",
        drop_path=drop_path,
        manifest_path="manifests/synthetic.yaml",
        schema_dir=schemas_dir,
        store_dir=store_dir,
        results_dir=results_dir,
    )
    assert exit_code2 == 0
    assert report2.drop_id == report.drop_id
    history2 = store.get_manifest_history()
    assert len(history2) == 1


def test_onboard_held_breaking_fault(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    store_dir = tmp_path / "store"
    results_dir = tmp_path / "results"
    schemas_dir = tmp_path / "schemas"

    # Generate drop with super-threshold duplicates (causes HOLD on drop 2)
    generate_synthetic_sequence(
        output_dir=raw_dir,
        size_gb="0.001",
        seed=101,
        faults="duplicate_row_super",
        num_drops=2,
        format="csv",
    )

    drop_path = raw_dir / "drop_2"

    report, exit_code = onboard_drop(
        vendor="synthetic",
        drop_path=drop_path,
        manifest_path="manifests/synthetic.yaml",
        schema_dir=schemas_dir,
        store_dir=store_dir,
        results_dir=results_dir,
    )

    assert exit_code == 1
    assert report.verdict == ReportVerdict.HELD
    assert "Batch placed on HOLD" in report.verdict_reason

    # Store must NOT have this drop written
    store = BitemporalStore(base_dir=store_dir, vendor="synthetic")
    history = store.get_manifest_history()
    assert len(history) == 0

    # Held drops record must exist
    held_path = store_dir / "synthetic" / "metadata" / "held_drops.parquet"
    assert held_path.exists()


def test_cli_onboard_report_diff(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    store_dir = tmp_path / "store"
    results_dir = tmp_path / "results"

    # Generate 2 drops with restatement
    generate_synthetic_sequence(
        output_dir=raw_dir,
        size_gb="0.001",
        seed=77,
        faults="restatement",
        num_drops=2,
        format="csv",
    )

    drop_1 = str(raw_dir / "drop_1")
    drop_2 = str(raw_dir / "drop_2")

    # 1. CLI onboard drop 1
    res1 = runner.invoke(
        app,
        [
            "onboard",
            "synthetic",
            drop_1,
            "--manifest",
            "manifests/synthetic.yaml",
            "--store-dir",
            str(store_dir),
            "--results-dir",
            str(results_dir),
        ],
    )
    assert res1.exit_code == 0
    assert "Verdict:" in res1.stdout

    # 2. CLI onboard drop 2
    res2 = runner.invoke(
        app,
        [
            "onboard",
            "synthetic",
            drop_2,
            "--manifest",
            "manifests/synthetic.yaml",
            "--store-dir",
            str(store_dir),
            "--results-dir",
            str(results_dir),
        ],
    )
    assert res2.exit_code == 0
    assert "Verdict:" in res2.stdout

    # Get drop IDs
    store = BitemporalStore(base_dir=store_dir, vendor="synthetic")
    history = store.get_manifest_history()
    assert len(history) == 2
    id1 = history[0].drop_id
    id2 = history[1].drop_id

    # 3. CLI report
    res_rep = runner.invoke(
        app,
        [
            "report",
            "synthetic",
            "--drop",
            id1,
            "--results-dir",
            str(results_dir),
        ],
    )
    assert res_rep.exit_code == 0
    assert "# Vendor Readiness Report: synthetic" in res_rep.stdout

    # 4. CLI report in JSON
    res_rep_json = runner.invoke(
        app,
        [
            "report",
            "synthetic",
            "--drop",
            id1,
            "--results-dir",
            str(results_dir),
            "--json-output",
        ],
    )
    assert res_rep_json.exit_code == 0
    assert '"vendor": "synthetic"' in res_rep_json.stdout

    # 5. CLI diff
    res_diff = runner.invoke(
        app,
        [
            "diff",
            "synthetic",
            id1,
            id2,
            "--store-dir",
            str(store_dir),
        ],
    )
    assert res_diff.exit_code == 0
    assert "# Drop Diff: synthetic" in res_diff.stdout
    assert "Common Observation Keys" in res_diff.stdout
