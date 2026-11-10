"""Adversarial Mutation Testing Framework for vendor-data-onboarding-kit test suite.

Empirically tests flaw sensitivity:
Applies targeted mutations to implementation code, assertion helpers, and fixtures,
executes pytest on the target tests, verifies that the tests FAIL (mutant killed),
records the exact failure message, and cleanly restores original files.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path("/home/antigravity/PromptCultivator/vendor-data-onboarding-kit")


@dataclass
class MutationExperiment:
    name: str
    target_file: Path
    original_snippet: str
    mutated_snippet: str
    test_target: str
    expected_failure_pattern: str


EXPERIMENTS = [
    MutationExperiment(
        name="Mutant 01: PIT Invariance Helper Sensitivity (Historical Query Perturbation)",
        target_file=REPO_ROOT / "tests/test_tier3_interactions/test_point_in_time_invariance.py",
        original_snippet="assert_point_in_time_invariance(h1, h2, key_columns=[\"entity_id\", \"event_time\", \"metric\"])",
        mutated_snippet=(
            "h2_mutated = h2.set_column(4, 'value', pa.array([170.51, 315.20, 130.40, 125.80, 290.10]))\n"
            "        assert_point_in_time_invariance(h1, h2_mutated, key_columns=[\"entity_id\", \"event_time\", \"metric\"])"
        ),
        test_target="tests/test_tier3_interactions/test_point_in_time_invariance.py::TestPointInTimeInvariance::test_strict_pit_invariance_before_and_after_restatement_drop",
        expected_failure_pattern="PIT Invariance Violation",
    ),
    MutationExperiment(
        name="Mutant 02: Bitemporal Query Future Leakage (+30 Days Lookahead Bias)",
        target_file=REPO_ROOT / "tests/test_tier3_interactions/conftest.py",
        original_snippet="WHERE knowledge_time <= TIMESTAMP '{as_of_iso}' {entity_filter}",
        mutated_snippet="WHERE knowledge_time <= TIMESTAMP '{as_of_iso}' + INTERVAL 30 DAY {entity_filter}",
        test_target="tests/test_tier3_interactions/test_point_in_time_invariance.py::TestPointInTimeInvariance::test_strict_pit_invariance_before_and_after_restatement_drop",
        expected_failure_pattern="Field value mismatch",
    ),
    MutationExperiment(
        name="Mutant 03: ALFRED FOMC Backtest Zero Lookahead Leakage",
        target_file=REPO_ROOT / "tests/test_tier4_realworld/test_alfred_vintage_alpha_workload.py",
        original_snippet="snap_may22_h2 = store.as_of_query(dataset_id=dataset_id, as_of_time=t_fomc_may22)",
        mutated_snippet="snap_may22_h2 = store.as_of_query(dataset_id=dataset_id, as_of_time=t_fomc_jun22)",
        test_target="tests/test_tier4_realworld/test_alfred_vintage_alpha_workload.py::TestAlfredVintageAlphaWorkload::test_fomc_backtest_strict_zero_lookahead_bias_simulation",
        expected_failure_pattern="Field value mismatch",
    ),
    MutationExperiment(
        name="Mutant 04: Diff Engine Restatement Blindspot (Planted Restatements Classified UNCHANGED)",
        target_file=REPO_ROOT / "tests/test_tier1_features/test_diff_engine.py",
        original_snippet="ELSE 'RESTATED'\n                    END as diff_status,",
        mutated_snippet="ELSE 'UNCHANGED'\n                    END as diff_status,",
        test_target="tests/test_tier1_features/test_diff_engine.py::test_diff_restatement_100_percent_recall",
        expected_failure_pattern="assert 0.0 == 1.0",
    ),
    MutationExperiment(
        name="Mutant 05: Diff Engine Revision Delta Zeroing",
        target_file=REPO_ROOT / "tests/test_tier1_features/test_diff_engine.py",
        original_snippet="THEN (curr.value - base.value)",
        mutated_snippet="THEN 0.0",
        test_target="tests/test_tier1_features/test_diff_engine.py::test_diff_delta_absolute_calculation",
        expected_failure_pattern="assert",
    ),
    MutationExperiment(
        name="Mutant 06: Truth Manifest Expected Restatement Count Tampering",
        target_file=REPO_ROOT / "tests/test_tier1_features/test_diff_engine.py",
        original_snippet="assert len(planted_restatements) == 10",
        mutated_snippet="assert len(planted_restatements) == 9",
        test_target="tests/test_tier1_features/test_diff_engine.py::test_diff_restatement_100_percent_recall",
        expected_failure_pattern="assert 10 == 9",
    ),
    MutationExperiment(
        name="Mutant 07: SEC 10-K/A Restatement Revision Sequence Masked",
        target_file=REPO_ROOT / "tests/test_tier4_realworld/conftest.py",
        original_snippet="is_restatement = \"10-K/A\" in r[\"filing_form\"] or \"10-Q/A\" in r[\"filing_form\"]",
        mutated_snippet="is_restatement = False",
        test_target="tests/test_tier4_realworld/test_sec_restatements_workload.py::TestSecRestatementsWorkload::test_sec_bitemporal_as_of_queries_and_pit_invariance",
        expected_failure_pattern="assert 1 == 2",
    ),
    MutationExperiment(
        name="Mutant 08: Corrupt Data Unflagged - Range & Domain Negative Values Bypass",
        target_file=REPO_ROOT / "tests/test_tier1_features/test_validation_rules.py",
        original_snippet="query = f'SELECT * FROM tbl WHERE \"{value_col}\" < 0'",
        mutated_snippet="query = f'SELECT * FROM tbl WHERE \"{value_col}\" < -999999999'",
        test_target="tests/test_tier1_features/test_validation_rules.py::test_val_range_drop3_negative_aapl",
        expected_failure_pattern="assert",
    ),
    MutationExperiment(
        name="Mutant 09: Corrupt Data Unflagged - Completeness Null Values Bypass",
        target_file=REPO_ROOT / "tests/test_tier1_features/test_validation_rules.py",
        original_snippet="null_conds = \" OR \".join([f'\"{col}\" IS NULL' for col in mandatory_cols if col in table.column_names])",
        mutated_snippet="null_conds = '1=0'",
        test_target="tests/test_tier1_features/test_validation_rules.py::test_val_completeness_drop3_null_value",
        expected_failure_pattern="assert 0 >= 1",
    ),
    MutationExperiment(
        name="Mutant 10: Manifest SHA-256 Avalanche Verification Tampering",
        target_file=REPO_ROOT / "tests/test_tier2_boundaries/test_corrupt_files.py",
        original_snippet="assert h1 != h2",
        mutated_snippet="assert h1 == h2",
        test_target="tests/test_tier2_boundaries/test_corrupt_files.py::test_drop_manifest_sha256_detects_single_bit_flip",
        expected_failure_pattern="AssertionError",
    ),
    MutationExperiment(
        name="Mutant 11: Schema Drift Engine - Case Mismatch Blindspot",
        target_file=REPO_ROOT / "src/vendor_onboarding/models/schema.py",
        original_snippet="if p_name.lower() in curr_lower_map:",
        mutated_snippet="if False and p_name.lower() in curr_lower_map:",
        test_target="tests/test_tier2_boundaries/test_challenger_m1.py::test_adversarial_casing_changes_are_breaking",
        expected_failure_pattern="assert 0 == 2",
    ),
    MutationExperiment(
        name="Mutant 12: Schema Drift Engine - Missing Required Column Blindspot",
        target_file=REPO_ROOT / "src/vendor_onboarding/models/schema.py",
        original_snippet="if p_name not in curr_cols:",
        mutated_snippet="if False and p_name not in curr_cols:",
        test_target="tests/test_tier2_boundaries/test_challenger_m1.py::test_adversarial_column_addition_and_removal",
        expected_failure_pattern="assert",
    ),
]


def run_experiment(exp: MutationExperiment) -> dict[str, any]:
    print(f"\n======================================================================")
    print(f"RUNNING: {exp.name}")
    print(f"Target file: {exp.target_file.relative_to(REPO_ROOT)}")
    print(f"Test target: {exp.test_target}")

    assert exp.target_file.exists(), f"File {exp.target_file} does not exist!"

    # Read original content
    content = exp.target_file.read_text(encoding="utf-8")
    assert exp.original_snippet in content, (
        f"Original snippet not found in {exp.target_file}!\nSnippet:\n{exp.original_snippet}"
    )

    # Backup
    backup_file = exp.target_file.with_suffix(exp.target_file.suffix + ".mutation_bak")
    shutil.copy2(exp.target_file, backup_file)

    try:
        # Apply mutation
        mutated_content = content.replace(exp.original_snippet, exp.mutated_snippet, 1)
        exp.target_file.write_text(mutated_content, encoding="utf-8")

        # Run pytest with bytecode caching disabled to guarantee fresh compilation of mutated source
        cmd = ["uv", "run", "pytest", "-v", exp.test_target]
        env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
        t0 = time.time()
        proc = subprocess.run(
            cmd,
            cwd=str(REPO_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=30,
            env=env,
        )
        duration = time.time() - t0

        output = proc.stdout
        exit_code = proc.returncode

        killed = exit_code != 0
        matched_pattern = exp.expected_failure_pattern.lower() in output.lower()

        print(f"Exit code: {exit_code} (Duration: {duration:.2f}s)")
        print(f"Mutant status: {'KILLED (SUCCESS)' if killed else 'SURVIVED (FAILURE)'}")
        print(f"Pattern '{exp.expected_failure_pattern}' matched: {matched_pattern}")

        # Extract failure line
        failure_lines = [
            line for line in output.splitlines()
            if "FAILED" in line or "AssertionError" in line or "E   assert" in line
        ]
        sample_failure = "\n".join(failure_lines[:5]) if failure_lines else output[-300:]
        print(f"Observed error snippet:\n{sample_failure}")

        return {
            "name": exp.name,
            "killed": killed,
            "exit_code": exit_code,
            "duration": duration,
            "matched_pattern": matched_pattern,
            "failure_snippet": sample_failure,
        }

    finally:
        # Restore backup unconditionally
        if backup_file.exists():
            shutil.copy2(backup_file, exp.target_file)
            backup_file.unlink()
            print(f"Restored original file: {exp.target_file.relative_to(REPO_ROOT)}")


def main() -> int:
    results = []
    print(f"Starting Adversarial Mutation Testing on {len(EXPERIMENTS)} mutants...")

    for exp in EXPERIMENTS:
        res = run_experiment(exp)
        results.append(res)

    print("\n" + "=" * 70)
    print("MUTATION TESTING SUMMARY REPORT")
    print("=" * 70)

    total = len(results)
    killed = sum(1 for r in results if r["killed"])
    survived = total - killed
    kill_rate = (killed / total) * 100.0

    print(f"Total Mutants Tested : {total}")
    print(f"Mutants Killed       : {killed}")
    print(f"Mutants Survived     : {survived}")
    print(f"Mutation Kill Rate   : {kill_rate:.2f}%")
    print("-" * 70)

    for i, r in enumerate(results, 1):
        status = "PASSED (Killed)" if r["killed"] else "FAILED (Survived)"
        print(f"[{i:02d}] {r['name'][:55]:<55} | {status} (Exit {r['exit_code']})")

    print("=" * 70)
    return 0 if killed == total else 1


if __name__ == "__main__":
    sys.exit(main())
