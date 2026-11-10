"""Acceptance tests for planted faults at 50 MB dataset scale.

Verifies detection of the five required planted faults:
1. duplicate_row (sub-threshold quarantine and super-threshold hold)
2. type_flip (non-numeric strings in numeric column)
3. out_of_range (out-of-bounds economic values)
4. repeat_drop (byte-identical duplicate drop)
5. schema_drift (removed column)
"""

from __future__ import annotations

import time

from vdok.adapters.synthetic import SyntheticGenerator
from vdok.manifest import VendorManifest
from vdok.schema import DriftCategory, DriftDifference, DriftSeverity, SchemaDiff
from vdok.validate import validate_drop

SYNTHETIC_MANIFEST_YAML = """
vendor: synthetic
entity_columns: [entity_id]
entity_id_type: cik
event_time_column: event_time
knowledge_time:
  source: drop_time
value_columns: [price]
"""

# 440,000 rows in canonical table is 50.14 MB in raw Arrow memory (> 50 MB)
ROWS_50MB = 440_000


def test_50mb_planted_fault_duplicate_row() -> None:
    manifest = VendorManifest.from_yaml(SYNTHETIC_MANIFEST_YAML)
    gen = SyntheticGenerator(seed=101)

    # 1. Sub-threshold duplicate share (0.5% <= 1.0% threshold)
    t_start = time.perf_counter()
    table_sub, info_sub = gen.generate_canonical_table(
        rows=ROWS_50MB, faults=["duplicate_row"], drop_id="drop_sub"
    )
    assert table_sub.nbytes >= 50 * 1024 * 1024

    res_sub = validate_drop(
        table_sub, manifest=manifest, drop_id="drop_sub", rules_path="rules/synthetic.yaml"
    )
    duration_sub = time.perf_counter() - t_start

    assert not res_sub.held
    assert res_sub.valid_count == ROWS_50MB
    assert res_sub.quarantine_count == info_sub["duplicate_count"]
    assert res_sub.rule_counts.get("duplicate_key") == info_sub["duplicate_count"]
    assert duration_sub < 3.0

    # 2. Super-threshold duplicate share (2.0% > 1.0% threshold)
    t_start_super = time.perf_counter()
    table_super, info_super = gen.generate_canonical_table(
        rows=ROWS_50MB, faults=["duplicate_row_super"], drop_id="drop_super"
    )
    res_super = validate_drop(
        table_super, manifest=manifest, drop_id="drop_super", rules_path="rules/synthetic.yaml"
    )
    duration_super = time.perf_counter() - t_start_super

    assert res_super.held
    assert res_super.valid_count == 0  # Main ingestion halted on batch hold
    assert res_super.held_record is not None
    assert res_super.held_record.rule_name == "duplicate_threshold"
    assert duration_super < 3.0


def test_50mb_planted_fault_type_flip() -> None:
    manifest = VendorManifest.from_yaml(SYNTHETIC_MANIFEST_YAML)
    gen = SyntheticGenerator(seed=202)

    t_start = time.perf_counter()
    table, info = gen.generate_canonical_table(
        rows=ROWS_50MB, faults=["type_flip"], drop_id="drop_tf"
    )
    assert table.nbytes >= 50 * 1024 * 1024

    res = validate_drop(
        table, manifest=manifest, drop_id="drop_tf", rules_path="rules/synthetic.yaml"
    )
    duration = time.perf_counter() - t_start

    assert not res.held
    assert res.quarantine_count == info["type_flip_count"]
    assert res.valid_count == ROWS_50MB - info["type_flip_count"]
    assert res.rule_counts.get("type_flip") == info["type_flip_count"]

    # Verify quarantine reasons on defective records
    reasons_list = res.quarantine_table.to_pydict()["quarantine_reasons"]
    for reasons in reasons_list:
        assert "type_flip" in reasons
    assert duration < 3.0


def test_50mb_planted_fault_out_of_range() -> None:
    manifest = VendorManifest.from_yaml(SYNTHETIC_MANIFEST_YAML)
    gen = SyntheticGenerator(seed=303)

    t_start = time.perf_counter()
    table, info = gen.generate_canonical_table(
        rows=ROWS_50MB, faults=["out_of_range"], drop_id="drop_oor"
    )
    assert table.nbytes >= 50 * 1024 * 1024

    res = validate_drop(
        table, manifest=manifest, drop_id="drop_oor", rules_path="rules/synthetic.yaml"
    )
    duration = time.perf_counter() - t_start

    assert not res.held
    assert res.quarantine_count == info["out_of_range_count"]
    assert res.valid_count == ROWS_50MB - info["out_of_range_count"]
    assert res.rule_counts.get("out_of_range") == info["out_of_range_count"]

    reasons_list = res.quarantine_table.to_pydict()["quarantine_reasons"]
    for reasons in reasons_list:
        assert "out_of_range" in reasons
    assert duration < 3.0


def test_50mb_planted_fault_repeat_drop() -> None:
    manifest = VendorManifest.from_yaml(SYNTHETIC_MANIFEST_YAML)
    gen = SyntheticGenerator(seed=404)

    t_start = time.perf_counter()
    table, _ = gen.generate_canonical_table(rows=ROWS_50MB, drop_id="drop_50mb_repeat")
    assert table.nbytes >= 50 * 1024 * 1024

    # Drop 2 has identical drop identifier to predecessor drop
    res = validate_drop(
        table,
        manifest=manifest,
        drop_id="drop_50mb_repeat",
        prior_drop_id="drop_50mb_repeat",
        rules_path="rules/synthetic.yaml",
    )
    duration = time.perf_counter() - t_start

    assert res.held
    assert res.valid_count == 0
    assert res.held_record is not None
    assert res.held_record.rule_name == "redundant_drop_guard"
    assert duration < 3.0


def test_50mb_planted_fault_schema_drift() -> None:
    manifest = VendorManifest.from_yaml(SYNTHETIC_MANIFEST_YAML)
    gen = SyntheticGenerator(seed=505)

    t_start = time.perf_counter()
    table, _ = gen.generate_canonical_table(rows=ROWS_50MB, drop_id="drop_50mb_drift")
    assert table.nbytes >= 50 * 1024 * 1024

    # Drop 3 has removed column drift detected against pinned schema
    drift_diff = SchemaDiff(
        vendor="synthetic",
        pinned_version=1,
        has_hold=True,
        differences=[
            DriftDifference(
                column="weight",
                category=DriftCategory.REMOVED_COLUMN,
                severity=DriftSeverity.HOLD,
                pinned_value="float",
                drop_value=None,
                proposed_change="Remove column",
            )
        ],
    )

    res = validate_drop(
        table,
        manifest=manifest,
        drop_id="drop_50mb_drift",
        schema_diff=drift_diff,
        rules_path="rules/synthetic.yaml",
    )
    duration = time.perf_counter() - t_start

    assert res.held
    assert res.valid_count == 0
    assert res.held_record is not None
    assert res.held_record.rule_name == "schema_drift_guard"
    assert duration < 3.0
