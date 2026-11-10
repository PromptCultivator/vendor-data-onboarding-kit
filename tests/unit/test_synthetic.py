"""Unit tests for synthetic drop generator and planted fault injector."""

from __future__ import annotations

import csv
import json
from pathlib import Path

import pyarrow.parquet as pq
from typer.testing import CliRunner

from vdok.adapters.synthetic import (
    SyntheticGenerator,
    generate_synthetic_sequence,
    parse_size_bytes,
)
from vdok.cli import app
from vdok.normalize import CANONICAL_ARROW_SCHEMA

runner = CliRunner()


def test_parse_size_bytes() -> None:
    assert parse_size_bytes("50MB") == 52_428_800
    assert parse_size_bytes("1GB") == 1_073_741_824
    assert parse_size_bytes("500KB") == 512_000
    assert parse_size_bytes("1024B") == 1024
    assert parse_size_bytes(0.05) == int(0.05 * 1024 * 1024 * 1024)
    assert parse_size_bytes(100_000_000) == 100_000_000


def test_seed_reproducibility(tmp_path: Path) -> None:
    dir1 = tmp_path / "run1"
    dir2 = tmp_path / "run2"

    generate_synthetic_sequence(output_dir=dir1, size_gb="1MB", seed=42, num_drops=2)
    generate_synthetic_sequence(output_dir=dir2, size_gb="1MB", seed=42, num_drops=2)

    f1 = dir1 / "drop_1" / "data.csv"
    f2 = dir2 / "drop_1" / "data.csv"
    assert f1.read_bytes() == f2.read_bytes()


def test_different_seeds(tmp_path: Path) -> None:
    dir1 = tmp_path / "run1"
    dir2 = tmp_path / "run2"

    generate_synthetic_sequence(output_dir=dir1, size_gb="1MB", seed=10, num_drops=1)
    generate_synthetic_sequence(output_dir=dir2, size_gb="1MB", seed=20, num_drops=1)

    f1 = (dir1 / "drop_1" / "data.csv").read_text()
    f2 = (dir2 / "drop_1" / "data.csv").read_text()
    # Different seeds generate distinct random prices / sequences
    assert f1 != f2


def test_generate_sequence_structure_and_ground_truth(tmp_path: Path) -> None:
    out_dir = tmp_path / "synth_feed"
    generate_synthetic_sequence(
        output_dir=out_dir, size_gb="2MB", seed=1, faults="default", num_drops=3
    )

    assert (out_dir / "drop_1" / "data.csv").exists()
    assert (out_dir / "drop_2" / "data.csv").exists()
    assert (out_dir / "drop_3" / "data.csv").exists()
    assert (out_dir / "ground_truth.json").exists()
    assert (out_dir / "manifest.yaml").exists()

    gt = json.loads((out_dir / "ground_truth.json").read_text(encoding="utf-8"))
    assert gt["vendor"] == "synthetic"
    assert gt["num_drops"] == 3
    assert "drop_1" in gt["drops"]
    assert "drop_2" in gt["drops"]
    assert "drop_3" in gt["drops"]

    # Verify planted faults in drop 2
    d2_faults = gt["drops"]["drop_2"]["faults"]
    f_types = {f["type"] for f in d2_faults}
    assert "duplicate_row" in f_types
    assert "type_flip" in f_types
    assert "out_of_range" in f_types

    # Verify schema drift in drop 3
    d3_faults = gt["drops"]["drop_3"]["faults"]
    assert any(f["type"] == "schema_drift" for f in d3_faults)


def test_planted_fault_type_flip_content(tmp_path: Path) -> None:
    out_dir = tmp_path / "tf_feed"
    generate_synthetic_sequence(
        output_dir=out_dir, size_gb="1MB", seed=1, faults=["type_flip"], num_drops=2
    )

    drop_2_csv = out_dir / "drop_2" / "data.csv"
    with open(drop_2_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        corrupt_rows = [r for r in reader if r["price"] == "CORRUPT_STR"]
        assert len(corrupt_rows) == 10


def test_planted_fault_out_of_range_content(tmp_path: Path) -> None:
    out_dir = tmp_path / "oor_feed"
    generate_synthetic_sequence(
        output_dir=out_dir, size_gb="1MB", seed=1, faults=["out_of_range"], num_drops=2
    )

    drop_2_csv = out_dir / "drop_2" / "data.csv"
    with open(drop_2_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        oor_rows = [r for r in reader if float(r["price"]) < 0.0]
        assert len(oor_rows) == 10


def test_planted_fault_repeat_drop_content(tmp_path: Path) -> None:
    out_dir = tmp_path / "repeat_feed"
    generate_synthetic_sequence(
        output_dir=out_dir, size_gb="1MB", seed=1, faults=["repeat_drop"], num_drops=2
    )

    d1_bytes = (out_dir / "drop_1" / "data.csv").read_bytes()
    d2_bytes = (out_dir / "drop_2" / "data.csv").read_bytes()
    assert d1_bytes == d2_bytes


def test_planted_fault_schema_drift_content(tmp_path: Path) -> None:
    out_dir = tmp_path / "drift_feed"
    generate_synthetic_sequence(
        output_dir=out_dir, size_gb="1MB", seed=1, faults=["schema_drift"], num_drops=3
    )

    with open(out_dir / "drop_1" / "data.csv", "r", encoding="utf-8") as f1:
        d1_header = f1.readline()
    with open(out_dir / "drop_3" / "data.csv", "r", encoding="utf-8") as f3:
        d3_header = f3.readline()

    assert "weight" in d1_header
    assert "weight" not in d3_header  # Column removed in drop 3


def test_generate_canonical_table_in_memory() -> None:
    gen = SyntheticGenerator(seed=1, size_bytes=100_000)
    table, info = gen.generate_canonical_table(
        rows=500, faults=["duplicate_row", "type_flip", "out_of_range"]
    )

    assert table.schema == CANONICAL_ARROW_SCHEMA
    assert info["has_duplicate"]
    assert info["has_type_flip"]
    assert info["has_out_of_range"]
    assert len(table) > 500  # Due to duplicate rows appended


def test_formats_parquet_and_jsonl(tmp_path: Path) -> None:
    p_dir = tmp_path / "parquet_feed"
    j_dir = tmp_path / "jsonl_feed"

    generate_synthetic_sequence(
        output_dir=p_dir, size_gb="1MB", seed=1, format="parquet", num_drops=1
    )
    assert (p_dir / "drop_1" / "data.parquet").exists()
    tbl = pq.read_table(p_dir / "drop_1" / "data.parquet")
    assert len(tbl) > 0

    generate_synthetic_sequence(
        output_dir=j_dir, size_gb="1MB", seed=1, format="jsonl", num_drops=1
    )
    assert (j_dir / "drop_1" / "data.jsonl").exists()
    with open(j_dir / "drop_1" / "data.jsonl") as jf:
        first_line = json.loads(jf.readline())
        assert "entity_id" in first_line


def test_cli_synth(tmp_path: Path) -> None:
    out_dir = tmp_path / "cli_synth"
    res = runner.invoke(
        app,
        [
            "synth",
            "--size-gb",
            "1MB",
            "--seed",
            "42",
            "--faults",
            "default",
            "--output-dir",
            str(out_dir),
        ],
    )
    assert res.exit_code == 0
    assert "Synthetic generator" in res.output
    assert (out_dir / "ground_truth.json").exists()


def test_custom_dict_fault_configurations(tmp_path: Path) -> None:
    out_dir = tmp_path / "dict_faults"
    faults_cfg = {
        "duplicate_row": {"share": 0.02},
        "type_flip": {"count": 5},
        "out_of_range": {"count": 5},
        "repeat_drop": True,
        "schema_drift": True,
    }
    generate_synthetic_sequence(
        output_dir=out_dir, size_gb="1MB", seed=42, faults=faults_cfg, num_drops=3
    )

    gt = json.loads((out_dir / "ground_truth.json").read_text(encoding="utf-8"))
    d2_faults = gt["drops"]["drop_2"]["faults"]
    assert any(f["type"] == "repeat_drop" for f in d2_faults)

    # Test in-memory table with dict configuration
    gen = SyntheticGenerator(seed=1)
    table, info = gen.generate_canonical_table(
        rows=200,
        faults={
            "duplicate_row": {"share": 0.03},
            "type_flip": 4,
            "out_of_range": 3,
        },
    )
    assert info["has_duplicate"]
    assert info["type_flip_count"] == 4
    assert info["out_of_range_count"] == 3


def test_super_threshold_flag() -> None:
    gen = SyntheticGenerator(seed=1, faults=[])
    flags = gen._resolve_fault_flags(drop_idx=2)
    assert not flags["duplicate_row"]

    gen_super = SyntheticGenerator(seed=1, faults=["duplicate_row_super"])
    flags_super = gen_super._resolve_fault_flags(drop_idx=2)
    assert flags_super["duplicate_row"]
    assert flags_super["duplicate_share"] == 0.02
