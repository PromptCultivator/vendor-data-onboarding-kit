"""Acceptance tests for planted restatements and bitemporal revision ledger precision and recall."""

from __future__ import annotations

import json
import time
from pathlib import Path

from vdok.adapters.synthetic import SyntheticGenerator, generate_synthetic_sequence
from vdok.manifest import VendorManifest
from vdok.normalize import normalize_drop
from vdok.revisions import detect_revisions
from vdok.store import init_store_catalog, write_partitioned_store
from vdok.validate import validate_drop

ROWS_50MB = 440_000


def test_planted_restatement_synthetic_sequence_exact_recall_precision(tmp_path: Path) -> None:
    """Verify revision detection achieves 100% recall and 100% precision on planted sequence."""
    out_dir = tmp_path / "synthetic_feed"
    generate_synthetic_sequence(
        output_dir=out_dir,
        size_gb="2MB",
        seed=42,
        faults="default",
        num_drops=2,
    )

    gt_file = out_dir / "ground_truth.json"
    assert gt_file.exists()
    gt = json.loads(gt_file.read_text(encoding="utf-8"))

    # Extract planted restatements metadata from ground truth
    d2_faults = gt["drops"]["drop_2"]["faults"]
    restate_fault = next(f for f in d2_faults if f["type"] == "restatement")
    expected_count = restate_fault["count"]
    expected_indices = set(restate_fault["indices"])

    # Load vendor manifest
    manifest = VendorManifest.from_file(out_dir / "manifest.yaml")

    # Onboard Drop 1
    d1_dir = out_dir / "drop_1"
    t1_norm = normalize_drop(d1_dir, manifest=manifest, drop_id="drop_1")
    t1_val = validate_drop(
        t1_norm, manifest=manifest, drop_id="drop_1", rules_path="rules/synthetic.yaml"
    )
    assert not t1_val.held
    write_partitioned_store(t1_val.valid_table, "synthetic", "drop_1", base_path=tmp_path)

    # Onboard Drop 2
    d2_dir = out_dir / "drop_2"
    t2_norm = normalize_drop(d2_dir, manifest=manifest, drop_id="drop_2")
    t2_val = validate_drop(
        t2_norm, manifest=manifest, drop_id="drop_2", rules_path="rules/synthetic.yaml"
    )
    assert not t2_val.held
    write_partitioned_store(t2_val.valid_table, "synthetic", "drop_2", base_path=tmp_path)

    # Execute revision detection
    con = init_store_catalog(tmp_path, "synthetic")
    try:
        revs = detect_revisions(con, "synthetic", "drop_2", base_path=tmp_path)
        assert len(revs) == expected_count

        rev_rows = revs.to_pylist()

        # Build ground-truth keys from Drop 1 source table using planted indices
        d1_pylist = t1_norm.to_pylist()
        planted_keys = {
            (
                d1_pylist[idx]["entity_id"],
                d1_pylist[idx]["field"],
                str(d1_pylist[idx]["event_time"]),
            )
            for idx in expected_indices
        }

        detected_keys = {(r["entity_id"], r["field"], str(r["event_time"])) for r in rev_rows}

        # Calculate exact Recall and Precision
        true_positives = len(detected_keys.intersection(planted_keys))
        false_positives = len(detected_keys - planted_keys)
        false_negatives = len(planted_keys - detected_keys)

        recall = true_positives / len(planted_keys)
        precision = true_positives / (true_positives + false_positives)

        assert recall == 1.0, f"Expected 1.0 recall, got {recall} (FN: {false_negatives})"
        assert precision == 1.0, f"Expected 1.0 precision, got {precision} (FP: {false_positives})"

        # Verify relative change is 10% on all detected revisions
        for r in rev_rows:
            assert r["relative_change"] is not None
            assert abs(r["relative_change"] - 0.10) < 1e-4
    finally:
        con.close()


def test_50mb_planted_restatements_scale(tmp_path: Path) -> None:
    """Verify revision detection at 50 MB scale (440,000 rows) with 100% recall/precision."""
    gen = SyntheticGenerator(seed=303)

    # Generate 50 MB Drop 1 (440,000 canonical rows)
    table_d1, _ = gen.generate_canonical_table(rows=ROWS_50MB, faults=[], drop_id="drop_1")
    assert table_d1.nbytes >= 50 * 1024 * 1024
    write_partitioned_store(table_d1, "synthetic", "drop_1", base_path=tmp_path)

    # Generate 50 MB Drop 2 with 25 planted restatements (indices 20..44)
    table_d2, info_d2 = gen.generate_canonical_table(
        rows=ROWS_50MB, faults=["restatement"], drop_id="drop_2"
    )
    assert table_d2.nbytes >= 50 * 1024 * 1024
    write_partitioned_store(table_d2, "synthetic", "drop_2", base_path=tmp_path)

    # Execute revision detection with timing measurement
    con = init_store_catalog(tmp_path, "synthetic")
    try:
        t_start = time.perf_counter()
        revs = detect_revisions(con, "synthetic", "drop_2", base_path=tmp_path)
        duration = time.perf_counter() - t_start

        # Verification: exact recall and precision at 50 MB scale
        assert len(revs) == 25
        assert duration < 5.0  # Vectorized execution completes in under 5 seconds

        rev_rows = revs.to_pylist()
        d1_rows = table_d1.to_pylist()

        expected_keys = {
            (d1_rows[i]["entity_id"], d1_rows[i]["field"], str(d1_rows[i]["event_time"]))
            for i in range(20, 45)
        }
        detected_keys = {(r["entity_id"], r["field"], str(r["event_time"])) for r in rev_rows}

        tp = len(detected_keys.intersection(expected_keys))
        assert tp == 25
        assert len(detected_keys - expected_keys) == 0  # 0 false positives

        recall = tp / len(expected_keys)
        precision = tp / len(detected_keys)
        assert recall == 1.0
        assert precision == 1.0
    finally:
        con.close()
