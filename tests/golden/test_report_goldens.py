"""Golden tests for report Markdown rendering against synthetic drops and SEC fixture drops."""

from __future__ import annotations

import difflib
import re
from pathlib import Path

import pytest

from vdok.adapters.synthetic import generate_synthetic_sequence
from vdok.onboard import onboard_drop
from vdok.report import ReportVerdict

GOLDEN_DIR = Path(__file__).parent


def normalize_dynamic_report_lines(md_text: str) -> str:
    """Normalize volatile metadata (timestamps, durations, memory) for deterministic comparison."""
    # Normalize Drop Identifier
    text = re.sub(
        r"- \*\*Drop Identifier:\*\* `[^`]+`",
        "- **Drop Identifier:** `[STABILIZED_DROP_ID]`",
        md_text,
    )
    # Normalize Generated At
    text = re.sub(
        r"- \*\*Generated At:\*\* [^\n]+", "- **Generated At:** 2024-01-15 00:00:00 UTC", text
    )
    # Normalize Timing table durations
    text = re.sub(r"\|\s*([A-Za-z ]+)\s*\|\s*[0-9.]+ s\s*\|", r"| \1 | 0.100 s |", text)
    text = re.sub(
        r"\|\s*\*\*Total Wall Clock\*\*\s*\|\s*\*\*[0-9.]+ s\*\*\s*\|",
        r"| **Total Wall Clock** | **0.500 s** |",
        text,
    )
    # Normalize Peak Memory
    text = re.sub(
        r"- \*\*Peak Resident Memory \(RSS\):\*\* [0-9.]+ MB",
        "- **Peak Resident Memory (RSS):** 50.00 MB",
        text,
    )
    # Normalize volatile Knowledge Time Range (for drop_time synthetic manifests)
    text = re.sub(
        r"\| Knowledge Time Range \| [^|]+ \|",
        "| Knowledge Time Range | 2024-01-15 00:00:00 UTC to 2024-01-15 00:00:00 UTC |",
        text,
    )
    return text


def assert_or_update_golden(golden_path: Path, actual_text: str, update: bool) -> None:
    """Compare normalized report against golden file, or update golden file if requested."""
    norm_actual = normalize_dynamic_report_lines(actual_text)

    if update or not golden_path.exists():
        old_text = golden_path.read_text(encoding="utf-8") if golden_path.exists() else ""
        golden_path.write_text(norm_actual, encoding="utf-8")
        if old_text and old_text != norm_actual:
            diff = "".join(
                difflib.unified_diff(
                    old_text.splitlines(keepends=True),
                    norm_actual.splitlines(keepends=True),
                    fromfile=str(golden_path),
                    tofile="actual_report",
                )
            )
            print(f"\n[UPDATED GOLDEN: {golden_path.name}]\n{diff}")
        return

    expected_text = golden_path.read_text(encoding="utf-8")
    if expected_text != norm_actual:
        diff = "".join(
            difflib.unified_diff(
                expected_text.splitlines(keepends=True),
                norm_actual.splitlines(keepends=True),
                fromfile=str(golden_path),
                tofile="actual_report",
            )
        )
        pytest.fail(f"Golden report mismatch in {golden_path.name}:\n{diff}")


def test_golden_synthetic_clean_report(tmp_path: Path, update_goldens: bool) -> None:
    raw_dir = tmp_path / "raw"
    generate_synthetic_sequence(
        output_dir=raw_dir,
        size_gb="0.0005",
        seed=1,
        faults="none",
        num_drops=1,
        format="csv",
    )

    report, exit_code = onboard_drop(
        vendor="synthetic",
        drop_path=raw_dir / "drop_1",
        manifest_path="manifests/synthetic.yaml",
        schema_dir=tmp_path / "schemas",
        store_dir=tmp_path / "store",
        results_dir=tmp_path / "results",
    )

    assert exit_code == 0
    assert report.verdict in (ReportVerdict.READY, ReportVerdict.READY_WITH_WARNINGS)

    golden_file = GOLDEN_DIR / "synthetic_clean_report.md"
    assert_or_update_golden(golden_file, report.to_markdown(), update_goldens)


def test_golden_synthetic_warnings_report(tmp_path: Path, update_goldens: bool) -> None:
    raw_dir = tmp_path / "raw"
    # Sub-threshold duplicates trigger quarantine warning
    generate_synthetic_sequence(
        output_dir=raw_dir,
        size_gb="0.0005",
        seed=1,
        faults="duplicate_row",
        num_drops=2,
        format="csv",
    )

    report, exit_code = onboard_drop(
        vendor="synthetic",
        drop_path=raw_dir / "drop_2",
        manifest_path="manifests/synthetic.yaml",
        schema_dir=tmp_path / "schemas",
        store_dir=tmp_path / "store",
        results_dir=tmp_path / "results",
    )

    assert exit_code == 0
    assert report.verdict == ReportVerdict.READY_WITH_WARNINGS

    golden_file = GOLDEN_DIR / "synthetic_warnings_report.md"
    assert_or_update_golden(golden_file, report.to_markdown(), update_goldens)


def test_golden_synthetic_held_report(tmp_path: Path, update_goldens: bool) -> None:
    raw_dir = tmp_path / "raw"
    # Super-threshold duplicates trigger batch hold
    generate_synthetic_sequence(
        output_dir=raw_dir,
        size_gb="0.0005",
        seed=1,
        faults="duplicate_row_super",
        num_drops=2,
        format="csv",
    )

    report, exit_code = onboard_drop(
        vendor="synthetic",
        drop_path=raw_dir / "drop_2",
        manifest_path="manifests/synthetic.yaml",
        schema_dir=tmp_path / "schemas",
        store_dir=tmp_path / "store",
        results_dir=tmp_path / "results",
    )

    assert exit_code == 1
    assert report.verdict == ReportVerdict.HELD

    golden_file = GOLDEN_DIR / "synthetic_held_report.md"
    assert_or_update_golden(golden_file, report.to_markdown(), update_goldens)


def test_golden_sec_fsds_report(tmp_path: Path, update_goldens: bool) -> None:
    sec_fixture_path = Path("tests/fixtures/sec_fsds/2023q3")
    assert sec_fixture_path.exists()

    report, exit_code = onboard_drop(
        vendor="sec_fsds",
        drop_path=sec_fixture_path,
        manifest_path="manifests/sec_fsds.yaml",
        schema_dir=tmp_path / "schemas",
        store_dir=tmp_path / "store",
        results_dir=tmp_path / "results",
    )

    assert exit_code == 0
    assert report.drop_summary.total_rows == 250
    assert report.drop_summary.distinct_entities == 50

    golden_file = GOLDEN_DIR / "sec_fsds_report.md"
    assert_or_update_golden(golden_file, report.to_markdown(), update_goldens)
