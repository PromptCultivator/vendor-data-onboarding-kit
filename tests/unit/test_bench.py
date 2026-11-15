"""Unit tests for benchmark hardware profiling and execution harness."""

from __future__ import annotations

import csv
import os
from pathlib import Path

from bench.hardware import format_hardware_block, get_hardware_info
from bench.run import main, parse_workload_label, purge_tree, run_benchmark


def test_hardware_info_and_block() -> None:
    """Verify hardware profiler captures all required environment attributes."""
    info = get_hardware_info()
    expected_keys = {
        "CPU Model",
        "CPU Cores",
        "Memory",
        "Disk Type",
        "Operating System",
        "Python Version",
        "DuckDB Version",
        "PyArrow Version",
        "Git Commit",
    }
    assert set(info.keys()) == expected_keys
    for key, val in info.items():
        assert isinstance(val, str) and len(val.strip()) > 0, f"Empty hardware value for {key}"

    block = format_hardware_block()
    assert block.startswith("```\nHardware & Environment:\n")
    assert block.endswith("\n```")
    for key in expected_keys:
        assert f"  {key}: " in block


def test_parse_workload_label_and_purge_tree(tmp_path: Path) -> None:
    """Verify workload label normalization and removal of read-only 0o444 trees."""
    assert parse_workload_label("50MB") == ("synthetic_50mb", "Synthetic (50 MB)")
    assert parse_workload_label("1GB") == ("synthetic_1gb", "Synthetic (1 GB)")
    assert parse_workload_label("10gb") == ("synthetic_10gb", "Synthetic (10 GB)")
    assert parse_workload_label("200KB") == ("synthetic_200kb", "Synthetic (200 KB)")
    assert parse_workload_label("5") == ("synthetic_5gb", "Synthetic (5 GB)")

    locked_dir = tmp_path / "locked_store" / "sub"
    locked_dir.mkdir(parents=True)
    locked_file = locked_dir / "part-test.parquet"
    locked_file.write_bytes(b"immutable")
    os.chmod(locked_file, 0o444)
    os.chmod(locked_dir, 0o555)

    purge_tree(tmp_path / "locked_store")
    assert not (tmp_path / "locked_store").exists()


def test_run_benchmark_synthetic_and_sec_merge(tmp_path: Path) -> None:
    """Verify synthetic ground-truth recall, PIT oracle check, leak recall, and SEC merge."""
    syn_row = run_benchmark(
        mode="synthetic",
        size="500KB",
        runs=2,
        seed=7,
        output_dir=tmp_path,
        pit_queries=250,
        leak_queries=50,
        merge_existing=False,
        in_process=True,
    )

    assert syn_row["workload"] == "synthetic_500kb"
    assert syn_row["runs"] == "2"
    assert syn_row["revision_mode"] == "ground_truth"
    assert syn_row["revision_recall_median"] == "1.0000"
    assert syn_row["revision_precision_median"] == "1.0000"
    assert syn_row["revisions_detected_median"] == "25"
    assert syn_row["pit_violations_max"] == "0"
    assert syn_row["leak_recall_median"] == "1.0000"
    assert syn_row["leak_false_positives_max"] == "0"
    assert float(syn_row["storage_factor_median"]) > 0.0

    sec_fixture_dir = Path("tests/fixtures/sec_fsds")
    sec_row = run_benchmark(
        mode="sec_fsds",
        runs=1,
        seed=7,
        sec_dir=sec_fixture_dir,
        quarters="2023q3,2023q4",
        output_dir=tmp_path,
        pit_queries=200,
        leak_queries=40,
        merge_existing=True,
        in_process=True,
    )

    assert sec_row["workload"] == "sec_fsds"
    assert sec_row["revision_mode"] == "unlabeled"
    assert sec_row["revision_recall_median"] == "N/A"
    assert int(sec_row["revisions_detected_median"]) >= 20
    assert sec_row["pit_violations_max"] == "0"
    assert sec_row["leak_recall_median"] == "1.0000"
    assert sec_row["leak_false_positives_max"] == "0"

    csv_path = tmp_path / "bench.csv"
    md_path = tmp_path / "bench.md"
    assert csv_path.exists()
    assert md_path.exists()

    with open(csv_path, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    workloads = {r["workload"] for r in rows}
    assert workloads == {"synthetic_500kb", "sec_fsds"}

    md_text = md_path.read_text(encoding="utf-8")
    assert "Synthetic (500 KB)" in md_text
    assert "SEC FSDS (2023q3–2023q4)" in md_text
    assert "Hardware & Environment:" in md_text
    assert "$" not in md_text


def test_bench_cli_subprocess_entrypoint(tmp_path: Path) -> None:
    """Verify CLI main() spawns isolated subprocess worker and exits 0 with 0 violations."""
    rc = main(
        [
            "--size",
            "500KB",
            "--runs",
            "1",
            "--seed",
            "3",
            "--pit-queries",
            "120",
            "--leak-queries",
            "25",
            "--no-merge",
            "--output-dir",
            str(tmp_path),
        ]
    )
    assert rc == 0
    assert (tmp_path / "bench.csv").exists()
    assert (tmp_path / "bench.md").exists()
