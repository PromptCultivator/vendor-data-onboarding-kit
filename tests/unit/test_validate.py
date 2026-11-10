"""Unit tests for validation engine, rule compiler, and severity tier routing."""

from __future__ import annotations

import datetime
import json
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from vdok.manifest import VendorManifest
from vdok.normalize import CANONICAL_ARROW_SCHEMA
from vdok.schema import DriftCategory, DriftDifference, DriftSeverity, SchemaDiff
from vdok.validate import (
    HELD_DROPS_ARROW_SCHEMA,
    QUARANTINE_ARROW_SCHEMA,
    HeldDropRecord,
    RuleCheckType,
    RuleDefinition,
    RuleScope,
    RuleSeverity,
    VendorRules,
    get_default_rules,
    load_rules,
    validate_drop,
    write_held_record,
    write_quarantine_table,
)

SAMPLE_MANIFEST_YAML = """
vendor: test_vendor
entity_columns: [entity_id]
entity_id_type: cik
event_time_column: event_time
knowledge_time:
  source: drop_time
value_columns: [price]
"""


def _make_canonical_row(
    entity_id: str = "0000320193",
    field: str = "price",
    event_time: datetime.datetime | None = None,
    knowledge_time: datetime.datetime | None = None,
    value_num: Decimal | None = Decimal("100.500000000"),
    value_str: str | None = None,
    unit: str | None = "USD",
    drop_id: str = "drop_1",
    source_file: str = "data.csv",
    source_row: int = 1,
    row_hash: str = "hash_1",
    unmapped: bool = False,
) -> dict[str, object]:
    et = event_time or (
        datetime.datetime(2024, 1, 15, 14, 30, tzinfo=datetime.timezone.utc)
        + datetime.timedelta(minutes=source_row)
    )
    kt = knowledge_time or (
        datetime.datetime(2024, 1, 15, 16, 0, tzinfo=datetime.timezone.utc)
        + datetime.timedelta(minutes=source_row)
    )
    return {
        "entity_id": entity_id,
        "entity_id_type": "cik",
        "field": field,
        "event_time": et,
        "knowledge_time": kt,
        "value_num": value_num,
        "value_str": value_str,
        "unit": unit,
        "drop_id": drop_id,
        "source_file": source_file,
        "source_row": source_row,
        "row_hash": row_hash,
        "unmapped": unmapped,
    }


def _make_table(rows: list[dict[str, object]]) -> pa.Table:
    if not rows:
        return pa.Table.from_batches([], schema=CANONICAL_ARROW_SCHEMA)
    return pa.Table.from_pylist(rows, schema=CANONICAL_ARROW_SCHEMA)


# ---------------------------------------------------------------------------
# 1. Row-Level Rule Tests
# ---------------------------------------------------------------------------


def test_rule_not_null() -> None:
    manifest = VendorManifest.from_yaml(SAMPLE_MANIFEST_YAML)
    rules = VendorRules(
        vendor="test_vendor",
        rules=[
            RuleDefinition(
                name="not_null_val",
                check=RuleCheckType.NOT_NULL,
                column="value_num",
                severity=RuleSeverity.QUARANTINE,
            )
        ],
    )
    rows = [
        _make_canonical_row(source_row=1, value_num=Decimal("150.000000000")),
        _make_canonical_row(source_row=2, value_num=None, row_hash="h2"),
    ]
    table = _make_table(rows)
    res = validate_drop(table, manifest=manifest, rules=rules)

    assert not res.held
    assert res.valid_count == 1
    assert res.quarantine_count == 1
    assert res.rule_counts.get("not_null_val") == 1
    q_rows = res.quarantine_table.to_pylist()
    assert q_rows[0]["source_row"] == 2
    assert "not_null_val" in q_rows[0]["quarantine_reasons"]


def test_rule_range_bounds() -> None:
    manifest = VendorManifest.from_yaml(SAMPLE_MANIFEST_YAML)
    rules = VendorRules(
        vendor="test_vendor",
        rules=[
            RuleDefinition(
                name="price_range",
                check=RuleCheckType.RANGE,
                column="value_num",
                min=10.0,
                max=500.0,
                min_inclusive=True,
                max_inclusive=True,
                severity=RuleSeverity.QUARANTINE,
            )
        ],
    )
    rows = [
        _make_canonical_row(source_row=1, value_num=Decimal("10.000000000")),
        _make_canonical_row(source_row=2, value_num=Decimal("500.000000000"), row_hash="h2"),
        _make_canonical_row(source_row=3, value_num=Decimal("5.000000000"), row_hash="h3"),
        _make_canonical_row(source_row=4, value_num=Decimal("550.000000000"), row_hash="h4"),
    ]
    table = _make_table(rows)
    res = validate_drop(table, manifest=manifest, rules=rules)

    assert not res.held
    assert res.valid_count == 2
    assert res.quarantine_count == 2
    assert res.rule_counts.get("price_range") == 2


def test_rule_range_exclusive() -> None:
    manifest = VendorManifest.from_yaml(SAMPLE_MANIFEST_YAML)
    rules = VendorRules(
        vendor="test_vendor",
        rules=[
            RuleDefinition(
                name="strict_range",
                check=RuleCheckType.RANGE,
                column="value_num",
                min=10.0,
                max=100.0,
                min_inclusive=False,
                max_inclusive=False,
                severity=RuleSeverity.QUARANTINE,
            )
        ],
    )
    rows = [
        _make_canonical_row(source_row=1, value_num=Decimal("10.000000000")),
        _make_canonical_row(source_row=2, value_num=Decimal("50.000000000"), row_hash="h2"),
        _make_canonical_row(source_row=3, value_num=Decimal("100.000000000"), row_hash="h3"),
    ]
    table = _make_table(rows)
    res = validate_drop(table, manifest=manifest, rules=rules)

    assert res.valid_count == 1
    assert res.quarantine_count == 2


def test_rule_allowed_values() -> None:
    manifest = VendorManifest.from_yaml(SAMPLE_MANIFEST_YAML)
    rules = VendorRules(
        vendor="test_vendor",
        rules=[
            RuleDefinition(
                name="status_check",
                check=RuleCheckType.ALLOWED_VALUES,
                column="value_str",
                values=["ACTIVE", "PENDING"],
                severity=RuleSeverity.QUARANTINE,
            )
        ],
    )
    rows = [
        _make_canonical_row(source_row=1, value_str="ACTIVE"),
        _make_canonical_row(source_row=2, value_str="PENDING", row_hash="h2"),
        _make_canonical_row(source_row=3, value_str="DISCONTINUED", row_hash="h3"),
    ]
    table = _make_table(rows)
    res = validate_drop(table, manifest=manifest, rules=rules)

    assert res.valid_count == 2
    assert res.quarantine_count == 1
    q_rows = res.quarantine_table.to_pylist()
    assert q_rows[0]["value_str"] == "DISCONTINUED"


def test_rule_regex_pattern() -> None:
    manifest = VendorManifest.from_yaml(SAMPLE_MANIFEST_YAML)
    rules = VendorRules(
        vendor="test_vendor",
        rules=[
            RuleDefinition(
                name="cik_regex",
                check=RuleCheckType.REGEX_PATTERN,
                column="entity_id",
                pattern=r"^[0-9]{10}$",
                severity=RuleSeverity.QUARANTINE,
            )
        ],
    )
    rows = [
        _make_canonical_row(source_row=1, entity_id="0000320193"),
        _make_canonical_row(source_row=2, entity_id="INVALID_CIK", row_hash="h2"),
    ]
    table = _make_table(rows)
    res = validate_drop(table, manifest=manifest, rules=rules)

    assert res.valid_count == 1
    assert res.quarantine_count == 1
    assert res.rule_counts.get("cik_regex") == 1


def test_rule_allowed_units() -> None:
    manifest = VendorManifest.from_yaml(SAMPLE_MANIFEST_YAML)
    rules = VendorRules(
        vendor="test_vendor",
        rules=[
            RuleDefinition(
                name="allowed_units",
                check=RuleCheckType.ALLOWED_UNITS,
                column="unit",
                units=["USD", "shares"],
                severity=RuleSeverity.QUARANTINE,
            )
        ],
    )
    rows = [
        _make_canonical_row(source_row=1, unit="USD"),
        _make_canonical_row(source_row=2, unit="shares", row_hash="h2"),
        _make_canonical_row(source_row=3, unit="EUR", row_hash="h3"),
    ]
    table = _make_table(rows)
    res = validate_drop(table, manifest=manifest, rules=rules)

    assert res.valid_count == 2
    assert res.quarantine_count == 1
    assert res.rule_counts.get("allowed_units") == 1


def test_rule_decimal_scale_without_float() -> None:
    manifest = VendorManifest.from_yaml(SAMPLE_MANIFEST_YAML)
    rules = VendorRules(
        vendor="test_vendor",
        rules=[
            RuleDefinition(
                name="max_scale_2",
                check=RuleCheckType.DECIMAL_SCALE,
                column="value_num",
                max_scale=2,
                severity=RuleSeverity.QUARANTINE,
            )
        ],
    )
    rows = [
        _make_canonical_row(source_row=1, value_num=Decimal("123.450000000")),
        _make_canonical_row(source_row=2, value_num=Decimal("123.456000000"), row_hash="h2"),
    ]
    table = _make_table(rows)
    res = validate_drop(table, manifest=manifest, rules=rules)

    assert res.valid_count == 1
    assert res.quarantine_count == 1
    assert res.rule_counts.get("max_scale_2") == 1


def test_rule_temporal_bounds() -> None:
    manifest = VendorManifest.from_yaml(SAMPLE_MANIFEST_YAML)
    rules = VendorRules(
        vendor="test_vendor",
        rules=[
            RuleDefinition(
                name="plausible_event_time",
                check=RuleCheckType.TEMPORAL_BOUNDS,
                column="event_time",
                min="1990-01-01",
                max="drop_time + 1 day",
                severity=RuleSeverity.QUARANTINE,
            )
        ],
    )
    drop_time = datetime.datetime(2024, 6, 1, 12, 0, tzinfo=datetime.timezone.utc)
    rows = [
        _make_canonical_row(
            source_row=1,
            event_time=datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc),
        ),
        _make_canonical_row(
            source_row=2,
            event_time=datetime.datetime(1985, 1, 1, tzinfo=datetime.timezone.utc),
            row_hash="h2",
        ),
        _make_canonical_row(
            source_row=3,
            event_time=datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc),
            row_hash="h3",
        ),
    ]
    table = _make_table(rows)
    res = validate_drop(table, manifest=manifest, drop_time=drop_time, rules=rules)

    assert res.valid_count == 1
    assert res.quarantine_count == 2
    assert res.rule_counts.get("plausible_event_time") == 2


def test_rule_temporal_order() -> None:
    manifest = VendorManifest.from_yaml(SAMPLE_MANIFEST_YAML)
    rules = VendorRules(
        vendor="test_vendor",
        rules=[
            RuleDefinition(
                name="knowledge_after_event",
                check=RuleCheckType.TEMPORAL_ORDER,
                event_time_column="event_time",
                knowledge_time_column="knowledge_time",
                severity=RuleSeverity.QUARANTINE,
            )
        ],
    )
    t1 = datetime.datetime(2024, 1, 1, 12, 0, tzinfo=datetime.timezone.utc)
    t2 = datetime.datetime(2024, 1, 2, 12, 0, tzinfo=datetime.timezone.utc)

    rows = [
        _make_canonical_row(source_row=1, event_time=t1, knowledge_time=t2),
        _make_canonical_row(source_row=2, event_time=t2, knowledge_time=t1, row_hash="h2"),
    ]
    table = _make_table(rows)
    res = validate_drop(table, manifest=manifest, rules=rules)

    assert res.valid_count == 1
    assert res.quarantine_count == 1
    assert res.rule_counts.get("knowledge_after_event") == 1


def test_rule_sql_expression() -> None:
    manifest = VendorManifest.from_yaml(SAMPLE_MANIFEST_YAML)
    rules = VendorRules(
        vendor="test_vendor",
        rules=[
            RuleDefinition(
                name="custom_sql",
                check=RuleCheckType.SQL_EXPRESSION,
                expression="value_num > 50.0",
                severity=RuleSeverity.QUARANTINE,
            )
        ],
    )
    rows = [
        _make_canonical_row(source_row=1, value_num=Decimal("100.000000000")),
        _make_canonical_row(source_row=2, value_num=Decimal("25.000000000"), row_hash="h2"),
    ]
    table = _make_table(rows)
    res = validate_drop(table, manifest=manifest, rules=rules)

    assert res.valid_count == 1
    assert res.quarantine_count == 1
    assert res.rule_counts.get("custom_sql") == 1


# ---------------------------------------------------------------------------
# 2. Multi-Tier Severity Execution Tests (warn, quarantine, hold)
# ---------------------------------------------------------------------------


def test_severity_warn_tier() -> None:
    manifest = VendorManifest.from_yaml(SAMPLE_MANIFEST_YAML)
    rules = VendorRules(
        vendor="test_vendor",
        rules=[
            RuleDefinition(
                name="price_warning",
                check=RuleCheckType.RANGE,
                column="value_num",
                min=50.0,
                severity=RuleSeverity.WARN,
            )
        ],
    )
    rows = [
        _make_canonical_row(source_row=1, value_num=Decimal("100.000000000")),
        _make_canonical_row(source_row=2, value_num=Decimal("20.000000000"), row_hash="h2"),
    ]
    table = _make_table(rows)
    res = validate_drop(table, manifest=manifest, rules=rules)

    assert not res.held
    assert res.valid_count == 2  # Warned record is NOT quarantined
    assert res.quarantine_count == 0
    assert res.warn_count == 1
    assert res.rule_counts.get("price_warning") == 1
    assert res.severity_counts.get("warn") == 1


def test_combined_multiple_reasons() -> None:
    manifest = VendorManifest.from_yaml(SAMPLE_MANIFEST_YAML)
    rules = VendorRules(
        vendor="test_vendor",
        rules=[
            RuleDefinition(
                name="range_fail",
                check=RuleCheckType.RANGE,
                column="value_num",
                min=0.0,
                severity=RuleSeverity.QUARANTINE,
            ),
            RuleDefinition(
                name="unit_fail",
                check=RuleCheckType.ALLOWED_UNITS,
                column="unit",
                units=["USD"],
                severity=RuleSeverity.QUARANTINE,
            ),
        ],
    )
    rows = [
        _make_canonical_row(
            source_row=1,
            value_num=Decimal("-10.000000000"),
            unit="INVALID",
        )
    ]
    table = _make_table(rows)
    res = validate_drop(table, manifest=manifest, rules=rules)

    assert res.quarantine_count == 1
    reasons = res.quarantine_table.to_pylist()[0]["quarantine_reasons"]
    assert "range_fail" in reasons
    assert "unit_fail" in reasons


# ---------------------------------------------------------------------------
# 3. Batch-Level Rule Tests
# ---------------------------------------------------------------------------


def test_batch_schema_drift_hold() -> None:
    manifest = VendorManifest.from_yaml(SAMPLE_MANIFEST_YAML)
    rules = VendorRules(
        vendor="test_vendor",
        rules=[
            RuleDefinition(
                name="schema_drift_guard",
                check=RuleCheckType.SCHEMA_DRIFT,
                severity=RuleSeverity.HOLD,
            )
        ],
    )
    diff = SchemaDiff(
        vendor="test_vendor",
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
    table = _make_table([_make_canonical_row()])
    res = validate_drop(table, manifest=manifest, schema_diff=diff, rules=rules)

    assert res.held
    assert res.valid_count == 0  # Main ingestion halted
    assert res.held_record is not None
    assert res.held_record.rule_name == "schema_drift_guard"
    assert "schema_drift_guard" in res.hold_reasons[0]


def test_batch_volume_stability() -> None:
    manifest = VendorManifest.from_yaml(SAMPLE_MANIFEST_YAML)
    rules = VendorRules(
        vendor="test_vendor",
        rules=[
            RuleDefinition(
                name="volume_stability",
                check=RuleCheckType.BATCH_VOLUME,
                ratio_min=0.5,
                ratio_max=2.0,
                trailing_drops=7,
                first_drop_exempt=True,
                severity=RuleSeverity.HOLD,
            )
        ],
    )

    # First drop exemption
    table = _make_table([_make_canonical_row()])
    res_first = validate_drop(table, manifest=manifest, trailing_row_counts=[], rules=rules)
    assert not res_first.held

    # Volume anomaly (10 rows when trailing median is 100 -> lower bound 50)
    history = [100, 105, 95, 100, 100, 98, 102]
    res_anomaly = validate_drop(table, manifest=manifest, trailing_row_counts=history, rules=rules)
    assert res_anomaly.held
    assert res_anomaly.valid_count == 0
    assert res_anomaly.held_record is not None
    assert res_anomaly.held_record.rule_name == "volume_stability"


def test_batch_duplicate_ratio_escalation() -> None:
    manifest = VendorManifest.from_yaml(SAMPLE_MANIFEST_YAML)
    rules = VendorRules(
        vendor="test_vendor",
        rules=[
            RuleDefinition(
                name="duplicate_threshold",
                check=RuleCheckType.BATCH_DUPLICATE_RATIO,
                max_ratio=0.01,
                severity=RuleSeverity.HOLD,
            )
        ],
    )

    # Sub-threshold duplicates (1 duplicate out of 200 rows = 0.5% <= 1.0%)
    rows_sub: list[dict[str, object]] = []
    base_t = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    for i in range(200):
        t = base_t + datetime.timedelta(days=i)
        rows_sub.append(_make_canonical_row(source_row=i + 1, event_time=t, row_hash=f"h_{i}"))
    # Add 1 duplicate of row 0
    dup_row_0 = dict(rows_sub[0])
    dup_row_0["source_row"] = 201
    dup_row_0["row_hash"] = "dup_h_0"
    rows_sub.append(dup_row_0)
    table_sub = _make_table(rows_sub)
    res_sub = validate_drop(table_sub, manifest=manifest, rules=rules)

    assert not res_sub.held
    assert res_sub.valid_count == 200
    assert res_sub.quarantine_count == 1
    q_reasons = res_sub.quarantine_table.to_pylist()[0]["quarantine_reasons"]
    assert "duplicate_key" in q_reasons

    # Super-threshold duplicates (10 duplicates out of 200 rows = 5.0% > 1.0%)
    rows_super = list(rows_sub[:200])
    for i in range(10):
        dup_row_i = dict(rows_sub[i])
        dup_row_i["source_row"] = 201 + i
        dup_row_i["row_hash"] = f"dup_h_{i}"
        rows_super.append(dup_row_i)
    table_super = _make_table(rows_super)
    res_super = validate_drop(table_super, manifest=manifest, rules=rules)

    assert res_super.held
    assert res_super.valid_count == 0
    assert res_super.held_record is not None
    assert res_super.held_record.rule_name == "duplicate_threshold"


def test_batch_byte_identical() -> None:
    manifest = VendorManifest.from_yaml(SAMPLE_MANIFEST_YAML)
    rules = VendorRules(
        vendor="test_vendor",
        rules=[
            RuleDefinition(
                name="redundant_drop_guard",
                check=RuleCheckType.BYTE_IDENTICAL,
                severity=RuleSeverity.HOLD,
            )
        ],
    )
    table = _make_table([_make_canonical_row()])
    res = validate_drop(
        table,
        manifest=manifest,
        drop_id="drop_abc",
        prior_drop_id="drop_abc",
        rules=rules,
    )

    assert res.held
    assert res.held_record is not None
    assert res.held_record.rule_name == "redundant_drop_guard"


def test_batch_coverage_gap() -> None:
    manifest_calendar = """
vendor: test_vendor
entity_columns: [entity_id]
entity_id_type: cik
event_time_column: event_time
knowledge_time:
  source: drop_time
value_columns: [price]
expected_calendar: quarterly
"""
    manifest = VendorManifest.from_yaml(manifest_calendar)
    rules = VendorRules(
        vendor="test_vendor",
        rules=[
            RuleDefinition(
                name="coverage_gap_guard",
                check=RuleCheckType.COVERAGE_GAP,
                max_missing_periods=1,
                severity=RuleSeverity.HOLD,
            )
        ],
    )

    # 2023-Q1 and 2024-Q1 -> 3 missing quarters (Q2, Q3, Q4) > max_missing_periods (1)
    rows = [
        _make_canonical_row(
            source_row=1,
            event_time=datetime.datetime(2023, 1, 15, tzinfo=datetime.timezone.utc),
        ),
        _make_canonical_row(
            source_row=2,
            event_time=datetime.datetime(2024, 1, 15, tzinfo=datetime.timezone.utc),
            row_hash="h2",
        ),
    ]
    table = _make_table(rows)
    res = validate_drop(table, manifest=manifest, rules=rules)

    assert res.held
    assert res.held_record is not None
    assert res.held_record.rule_name == "coverage_gap_guard"


# ---------------------------------------------------------------------------
# 4. Output Schemas and Persistence Tests
# ---------------------------------------------------------------------------


def test_quarantine_parquet_schema(tmp_path: Path) -> None:
    manifest = VendorManifest.from_yaml(SAMPLE_MANIFEST_YAML)
    rules = VendorRules(
        vendor="test_vendor",
        rules=[
            RuleDefinition(
                name="non_null",
                check=RuleCheckType.NOT_NULL,
                column="value_num",
                severity=RuleSeverity.QUARANTINE,
            )
        ],
    )
    table = _make_table([_make_canonical_row(value_num=None)])
    res = validate_drop(table, manifest=manifest, drop_id="drop_test", rules=rules)

    q_file = write_quarantine_table(
        res.quarantine_table, store_dir=tmp_path, drop_id="drop_test", vendor="test_vendor"
    )
    assert q_file.exists()

    read_back = pq.read_table(q_file)
    assert read_back.schema == QUARANTINE_ARROW_SCHEMA
    assert len(read_back) == 1


def test_held_drops_parquet_schema(tmp_path: Path) -> None:
    rec = HeldDropRecord(
        drop_id="drop_test_hold",
        vendor="test_vendor",
        held_at=datetime.datetime.now(datetime.timezone.utc),
        reason="Planted test failure",
        rule_name="test_rule",
        details_json=json.dumps({"detail": 1}),
    )
    held_file = write_held_record(rec, store_dir=tmp_path)
    assert held_file.exists()

    read_back = pq.read_table(held_file)
    assert read_back.schema == HELD_DROPS_ARROW_SCHEMA
    assert len(read_back) == 1

    # Appending a second held drop record
    rec2 = HeldDropRecord(
        drop_id="drop_test_hold_2",
        vendor="test_vendor",
        held_at=datetime.datetime.now(datetime.timezone.utc),
        reason="Second hold",
        rule_name="test_rule_2",
        details_json=json.dumps({"detail": 2}),
    )
    write_held_record(rec2, store_dir=tmp_path)
    read_back2 = pq.read_table(held_file)
    assert len(read_back2) == 2


# ---------------------------------------------------------------------------
# 5. Configuration & Rule Parsing Tests
# ---------------------------------------------------------------------------


def test_load_rules_and_scope(tmp_path: Path) -> None:
    yaml_content = """
vendor: sample
version: 1
rules:
  - name: r1
    check: not_null
    column: entity_id
    severity: quarantine
  - name: r2
    check: schema_drift
    severity: hold
"""
    f = tmp_path / "rules.yaml"
    f.write_text(yaml_content, encoding="utf-8")

    loaded = load_rules(f)
    assert loaded.vendor == "sample"
    assert len(loaded.rules) == 2
    assert loaded.rules[0].scope == RuleScope.ROW
    assert loaded.rules[1].scope == RuleScope.BATCH


def test_get_default_rules() -> None:
    defaults = get_default_rules("sec_fsds")
    assert defaults.vendor == "sec_fsds"
    assert len(defaults.rules) >= 8


def test_empty_table_validation() -> None:
    manifest = VendorManifest.from_yaml(SAMPLE_MANIFEST_YAML)
    empty_table = _make_table([])
    res = validate_drop(empty_table, manifest=manifest)

    assert not res.held
    assert res.total_rows == 0
    assert res.valid_count == 0
    assert res.quarantine_count == 0
