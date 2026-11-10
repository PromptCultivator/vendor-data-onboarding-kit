"""Empirical stress tests for DuckDB rule compiler, severity routing, and invariants.

Created by Challenger 2 to stress-test:
1. Simultaneous multiple row failures in a single quarantine_reasons array.
2. Decimal scale boundaries (2, 4, 9, 10 decimals) without floating-point conversion.
3. Temporal bounds (1989-12-31, 1990-01-01, drop_time + 1d, drop_time + 2d).
4. Batch volume boundaries (0.49x, 0.50x, 2.00x, 2.01x of trailing 7-drop median).
5. Coverage gaps across daily, monthly, and quarterly calendars.
6. Section 12 prohibited vocabulary verification.
"""

from __future__ import annotations

import datetime
import re
from decimal import Decimal
from pathlib import Path

import duckdb
import pyarrow as pa

from vdok.manifest import VendorManifest
from vdok.normalize import CANONICAL_ARROW_SCHEMA
from vdok.validate import (
    RuleCheckType,
    RuleDefinition,
    RuleSeverity,
    VendorRules,
    validate_drop,
)

SAMPLE_MANIFEST = """
vendor: challenger_vendor
entity_columns: [entity_id]
entity_id_type: cik
event_time_column: event_time
knowledge_time:
  source: drop_time
value_columns: [value_num]
"""


def _build_test_table(rows: list[dict[str, object]]) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=CANONICAL_ARROW_SCHEMA)


def test_simultaneous_multiple_row_failures() -> None:
    """Plant a row failing not_null, range, and decimal_scale simultaneously."""
    now = datetime.datetime.now(datetime.timezone.utc)
    manifest = VendorManifest.from_yaml(SAMPLE_MANIFEST)

    rules = VendorRules(
        vendor="challenger_vendor",
        rules=[
            RuleDefinition(
                name="unit_not_null",
                check=RuleCheckType.NOT_NULL,
                column="unit",
                severity=RuleSeverity.QUARANTINE,
            ),
            RuleDefinition(
                name="value_range",
                check=RuleCheckType.RANGE,
                column="value_num",
                min=0,
                max=100,
                severity=RuleSeverity.QUARANTINE,
            ),
            RuleDefinition(
                name="value_scale",
                check=RuleCheckType.DECIMAL_SCALE,
                column="value_num",
                max_scale=2,
                severity=RuleSeverity.QUARANTINE,
            ),
        ],
    )

    row = {
        "entity_id": "0000320193",
        "entity_id_type": "cik",
        "field": "price",
        "event_time": now,
        "knowledge_time": now,
        "value_num": Decimal("150.123456000"),  # > 100 (range) and 6 decimal places (scale)
        "value_str": None,
        "unit": None,  # fails not_null
        "drop_id": "drop_multi_fail",
        "source_file": "test.csv",
        "source_row": 1,
        "row_hash": "hash_1",
        "unmapped": False,
    }

    table = _build_test_table([row])
    result = validate_drop(table, manifest=manifest, rules=rules)

    assert not result.held
    assert result.valid_count == 0
    assert result.quarantine_count == 1
    assert result.severity_counts.get("quarantine") == 1

    quarantined = result.quarantine_table.to_pylist()[0]
    reasons = quarantined["quarantine_reasons"]
    assert isinstance(reasons, list)
    assert len(reasons) == 3
    assert set(reasons) == {"unit_not_null", "value_range", "value_scale"}


def test_decimal_scale_boundaries_and_precision() -> None:
    """Test values with 2, 4, 9 decimal places with exact precision preservation."""
    base_time = datetime.datetime(2024, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)
    manifest = VendorManifest.from_yaml(SAMPLE_MANIFEST)

    rows = [
        # Exactly 2 decimals (100.50)
        {
            "entity_id": "E1",
            "entity_id_type": "cik",
            "field": "price",
            "event_time": base_time,
            "knowledge_time": base_time,
            "value_num": Decimal("100.500000000"),
            "value_str": None,
            "unit": "USD",
            "drop_id": "d1",
            "source_file": "f1",
            "source_row": 1,
            "row_hash": "h1",
            "unmapped": False,
        },
        # Exactly 4 decimals (100.5555)
        {
            "entity_id": "E2",
            "entity_id_type": "cik",
            "field": "price",
            "event_time": base_time + datetime.timedelta(days=1),
            "knowledge_time": base_time + datetime.timedelta(days=1),
            "value_num": Decimal("100.555500000"),
            "value_str": None,
            "unit": "USD",
            "drop_id": "d1",
            "source_file": "f1",
            "source_row": 2,
            "row_hash": "h2",
            "unmapped": False,
        },
        # Exactly 9 decimals (100.123456789)
        {
            "entity_id": "E3",
            "entity_id_type": "cik",
            "field": "price",
            "event_time": base_time + datetime.timedelta(days=2),
            "knowledge_time": base_time + datetime.timedelta(days=2),
            "value_num": Decimal("100.123456789"),
            "value_str": None,
            "unit": "USD",
            "drop_id": "d1",
            "source_file": "f1",
            "source_row": 3,
            "row_hash": "h3",
            "unmapped": False,
        },
    ]
    table = _build_test_table(rows)

    # Test scale threshold 2 -> only row 1 valid (rows 2 and 3 quarantined)
    rules_2 = VendorRules(
        vendor="challenger_vendor",
        rules=[
            RuleDefinition(
                name="scale_2",
                check=RuleCheckType.DECIMAL_SCALE,
                column="value_num",
                max_scale=2,
                severity=RuleSeverity.QUARANTINE,
            )
        ],
    )
    res_2 = validate_drop(table, manifest=manifest, rules=rules_2)
    assert res_2.valid_count == 1
    assert res_2.quarantine_count == 2
    assert res_2.valid_table.schema.field("value_num").type == pa.decimal128(38, 9)
    assert res_2.quarantine_table.schema.field("value_num").type == pa.decimal128(38, 9)

    # Test scale threshold 4 -> rows 1 and 2 valid (row 3 quarantined)
    rules_4 = VendorRules(
        vendor="challenger_vendor",
        rules=[
            RuleDefinition(
                name="scale_4",
                check=RuleCheckType.DECIMAL_SCALE,
                column="value_num",
                max_scale=4,
                severity=RuleSeverity.QUARANTINE,
            )
        ],
    )
    res_4 = validate_drop(table, manifest=manifest, rules=rules_4)
    assert res_4.valid_count == 2
    assert res_4.quarantine_count == 1

    # Test scale threshold 9 -> all 3 rows valid
    rules_9 = VendorRules(
        vendor="challenger_vendor",
        rules=[
            RuleDefinition(
                name="scale_9",
                check=RuleCheckType.DECIMAL_SCALE,
                column="value_num",
                max_scale=9,
                severity=RuleSeverity.QUARANTINE,
            )
        ],
    )
    res_9 = validate_drop(table, manifest=manifest, rules=rules_9)
    assert res_9.valid_count == 3
    assert res_9.quarantine_count == 0

    # Test 10 decimal places in DuckDB engine directly without float conversion
    con = duckdb.connect()
    try:
        t10 = pa.Table.from_pydict(
            {"val": [Decimal("123.4567890123")]},
            schema=pa.schema([pa.field("val", pa.decimal128(38, 10))]),
        )
        con.register("t10", t10)
        query = (
            "SELECT typeof(val), ROUND(val, 9) != val AS fail_9, "
            "ROUND(val, 10) != val AS fail_10 FROM t10"
        )
        out = con.sql(query).fetchall()
        assert out[0][0] == "DECIMAL(38,10)"
        assert out[0][1] is True  # fails scale 9
        assert out[0][2] is False  # passes scale 10
    finally:
        con.close()


def test_temporal_bounds_and_order() -> None:
    """Test temporal bounds (1989-12-31, 1990-01-01, drop_time + 1d, drop_time + 2d) and order."""
    drop_time = datetime.datetime(2024, 6, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    manifest = VendorManifest.from_yaml(SAMPLE_MANIFEST)

    rows = [
        # Row 1: 1989-12-31 23:59:59 (out of range)
        {
            "entity_id": "E1",
            "entity_id_type": "cik",
            "field": "price",
            "event_time": datetime.datetime(1989, 12, 31, 23, 59, 59, tzinfo=datetime.timezone.utc),
            "knowledge_time": drop_time,
            "value_num": Decimal("100.0"),
            "value_str": None,
            "unit": "USD",
            "drop_id": "d1",
            "source_file": "f1",
            "source_row": 1,
            "row_hash": "h1",
            "unmapped": False,
        },
        # Row 2: 1990-01-01 00:00:00 (valid lower bound)
        {
            "entity_id": "E2",
            "entity_id_type": "cik",
            "field": "price",
            "event_time": datetime.datetime(1990, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
            "knowledge_time": drop_time,
            "value_num": Decimal("100.0"),
            "value_str": None,
            "unit": "USD",
            "drop_id": "d1",
            "source_file": "f1",
            "source_row": 2,
            "row_hash": "h2",
            "unmapped": False,
        },
        # Row 3: drop_time + 1 day (valid upper bound)
        {
            "entity_id": "E3",
            "entity_id_type": "cik",
            "field": "price",
            "event_time": drop_time + datetime.timedelta(days=1),
            "knowledge_time": drop_time + datetime.timedelta(days=1),
            "value_num": Decimal("100.0"),
            "value_str": None,
            "unit": "USD",
            "drop_id": "d1",
            "source_file": "f1",
            "source_row": 3,
            "row_hash": "h3",
            "unmapped": False,
        },
        # Row 4: drop_time + 2 days (out of range upper bound)
        {
            "entity_id": "E4",
            "entity_id_type": "cik",
            "field": "price",
            "event_time": drop_time + datetime.timedelta(days=2),
            "knowledge_time": drop_time + datetime.timedelta(days=2),
            "value_num": Decimal("100.0"),
            "value_str": None,
            "unit": "USD",
            "drop_id": "d1",
            "source_file": "f1",
            "source_row": 4,
            "row_hash": "h4",
            "unmapped": False,
        },
        # Row 5: knowledge_time < event_time (out of temporal order)
        {
            "entity_id": "E5",
            "entity_id_type": "cik",
            "field": "price",
            "event_time": datetime.datetime(2024, 1, 2, 0, 0, tzinfo=datetime.timezone.utc),
            "knowledge_time": datetime.datetime(2024, 1, 1, 0, 0, tzinfo=datetime.timezone.utc),
            "value_num": Decimal("100.0"),
            "value_str": None,
            "unit": "USD",
            "drop_id": "d1",
            "source_file": "f1",
            "source_row": 5,
            "row_hash": "h5",
            "unmapped": False,
        },
    ]

    table = _build_test_table(rows)
    rules = VendorRules(
        vendor="challenger_vendor",
        rules=[
            RuleDefinition(
                name="temporal_bounds",
                check=RuleCheckType.TEMPORAL_BOUNDS,
                column="event_time",
                min="1990-01-01",
                max="drop_time + 1 day",
                severity=RuleSeverity.QUARANTINE,
            ),
            RuleDefinition(
                name="temporal_order",
                check=RuleCheckType.TEMPORAL_ORDER,
                event_time_column="event_time",
                knowledge_time_column="knowledge_time",
                severity=RuleSeverity.QUARANTINE,
            ),
        ],
    )

    result = validate_drop(table, manifest=manifest, drop_time=drop_time, rules=rules)

    assert result.valid_count == 2
    assert result.quarantine_count == 3

    valid_ids = [r["entity_id"] for r in result.valid_table.to_pylist()]
    assert set(valid_ids) == {"E2", "E3"}

    q_map = {r["entity_id"]: r["quarantine_reasons"] for r in result.quarantine_table.to_pylist()}
    assert "temporal_bounds" in q_map["E1"]
    assert "temporal_bounds" in q_map["E4"]
    assert "temporal_order" in q_map["E5"]


def test_batch_volume_boundaries() -> None:
    """Test batch volume boundaries at exactly 0.49x, 0.50x, 2.00x, 2.01x of trailing median."""
    manifest = VendorManifest.from_yaml(SAMPLE_MANIFEST)
    history = [10000] * 7  # trailing 7-drop median is 10,000

    rules = VendorRules(
        vendor="challenger_vendor",
        rules=[
            RuleDefinition(
                name="volume_stability",
                check=RuleCheckType.BATCH_VOLUME,
                ratio_min=0.5,
                ratio_max=2.0,
                trailing_drops=7,
                severity=RuleSeverity.HOLD,
            )
        ],
    )

    def _generate_batch(n: int) -> pa.Table:
        base_time = datetime.datetime(2024, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)
        return pa.Table.from_arrays(
            [
                pa.array([f"E_{i}" for i in range(n)], type=pa.string()),
                pa.array(["cik"] * n, type=pa.string()),
                pa.array(["price"] * n, type=pa.string()),
                pa.array([base_time] * n, type=pa.timestamp("us", tz="UTC")),
                pa.array([base_time] * n, type=pa.timestamp("us", tz="UTC")),
                pa.array([Decimal("100.0")] * n, type=pa.decimal128(38, 9)),
                pa.array([None] * n, type=pa.string()),
                pa.array(["USD"] * n, type=pa.string()),
                pa.array(["d1"] * n, type=pa.string()),
                pa.array(["f1"] * n, type=pa.string()),
                pa.array(list(range(1, n + 1)), type=pa.int64()),
                pa.array([f"h_{i}" for i in range(n)], type=pa.string()),
                pa.array([False] * n, type=pa.bool_()),
            ],
            schema=CANONICAL_ARROW_SCHEMA,
        )

    # 0.49x -> 4,900 rows -> held
    res_049 = validate_drop(
        _generate_batch(4900),
        manifest=manifest,
        trailing_row_counts=history,
        rules=rules,
    )
    assert res_049.held is True
    assert "volume_stability" in res_049.hold_reasons[0]

    # 0.50x -> 5,000 rows -> not held (boundary accepted)
    res_050 = validate_drop(
        _generate_batch(5000),
        manifest=manifest,
        trailing_row_counts=history,
        rules=rules,
    )
    assert res_050.held is False

    # 2.00x -> 20,000 rows -> not held (boundary accepted)
    res_200 = validate_drop(
        _generate_batch(20000),
        manifest=manifest,
        trailing_row_counts=history,
        rules=rules,
    )
    assert res_200.held is False

    # 2.01x -> 20,100 rows -> held
    res_201 = validate_drop(
        _generate_batch(20100),
        manifest=manifest,
        trailing_row_counts=history,
        rules=rules,
    )
    assert res_201.held is True
    assert "volume_stability" in res_201.hold_reasons[0]


def test_coverage_gaps_daily_monthly_quarterly() -> None:
    """Test coverage gaps across daily, monthly, and quarterly calendars."""
    rules = VendorRules(
        vendor="challenger_vendor",
        rules=[
            RuleDefinition(
                name="coverage_gap_guard",
                check=RuleCheckType.COVERAGE_GAP,
                max_missing_periods=1,
                severity=RuleSeverity.HOLD,
            )
        ],
    )

    def _make_pair(t1: datetime.datetime, t2: datetime.datetime) -> pa.Table:
        rows = [
            {
                "entity_id": "E1",
                "entity_id_type": "cik",
                "field": "price",
                "event_time": t1,
                "knowledge_time": t1,
                "value_num": Decimal("100.0"),
                "value_str": None,
                "unit": "USD",
                "drop_id": "d1",
                "source_file": "f1",
                "source_row": 1,
                "row_hash": "h1",
                "unmapped": False,
            },
            {
                "entity_id": "E2",
                "entity_id_type": "cik",
                "field": "price",
                "event_time": t2,
                "knowledge_time": t2,
                "value_num": Decimal("100.0"),
                "value_str": None,
                "unit": "USD",
                "drop_id": "d1",
                "source_file": "f1",
                "source_row": 2,
                "row_hash": "h2",
                "unmapped": False,
            },
        ]
        return _build_test_table(rows)

    # 1. Daily calendar
    man_daily = VendorManifest.from_yaml(SAMPLE_MANIFEST + "\nexpected_calendar: daily\n")
    # Consecutive days (0 missing) -> pass
    res_d0 = validate_drop(
        _make_pair(
            datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
            datetime.datetime(2024, 1, 2, tzinfo=datetime.timezone.utc),
        ),
        manifest=man_daily,
        rules=rules,
    )
    assert not res_d0.held

    # 1 missing day -> pass (<= 1)
    res_d1 = validate_drop(
        _make_pair(
            datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
            datetime.datetime(2024, 1, 3, tzinfo=datetime.timezone.utc),
        ),
        manifest=man_daily,
        rules=rules,
    )
    assert not res_d1.held

    # 2 missing days -> hold (> 1)
    res_d2 = validate_drop(
        _make_pair(
            datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
            datetime.datetime(2024, 1, 4, tzinfo=datetime.timezone.utc),
        ),
        manifest=man_daily,
        rules=rules,
    )
    assert res_d2.held
    assert "coverage_gap_guard" in res_d2.hold_reasons[0]

    # 2. Monthly calendar
    man_monthly = VendorManifest.from_yaml(SAMPLE_MANIFEST + "\nexpected_calendar: monthly\n")
    # 1 missing month -> pass
    res_m1 = validate_drop(
        _make_pair(
            datetime.datetime(2024, 1, 15, tzinfo=datetime.timezone.utc),
            datetime.datetime(2024, 3, 15, tzinfo=datetime.timezone.utc),
        ),
        manifest=man_monthly,
        rules=rules,
    )
    assert not res_m1.held

    # 2 missing months -> hold
    res_m2 = validate_drop(
        _make_pair(
            datetime.datetime(2024, 1, 15, tzinfo=datetime.timezone.utc),
            datetime.datetime(2024, 4, 15, tzinfo=datetime.timezone.utc),
        ),
        manifest=man_monthly,
        rules=rules,
    )
    assert res_m2.held

    # 3. Quarterly calendar (intra-year and cross-year)
    man_quarterly = VendorManifest.from_yaml(SAMPLE_MANIFEST + "\nexpected_calendar: quarterly\n")
    # 1 missing quarter within year -> pass
    res_q1 = validate_drop(
        _make_pair(
            datetime.datetime(2023, 1, 15, tzinfo=datetime.timezone.utc),
            datetime.datetime(2023, 7, 15, tzinfo=datetime.timezone.utc),
        ),
        manifest=man_quarterly,
        rules=rules,
    )
    assert not res_q1.held

    # 2 missing quarters within year -> hold
    res_q2 = validate_drop(
        _make_pair(
            datetime.datetime(2023, 1, 15, tzinfo=datetime.timezone.utc),
            datetime.datetime(2023, 10, 15, tzinfo=datetime.timezone.utc),
        ),
        manifest=man_quarterly,
        rules=rules,
    )
    assert res_q2.held

    # Cross-year boundary: Q4 2023 to Q1 2024 (0 missing) -> pass
    res_q_cross0 = validate_drop(
        _make_pair(
            datetime.datetime(2023, 11, 15, tzinfo=datetime.timezone.utc),
            datetime.datetime(2024, 2, 15, tzinfo=datetime.timezone.utc),
        ),
        manifest=man_quarterly,
        rules=rules,
    )
    assert not res_q_cross0.held

    # Cross-year boundary: Q3 2023 to Q2 2024 (2 missing quarters: Q4 2023, Q1 2024) -> hold
    res_q_cross2 = validate_drop(
        _make_pair(
            datetime.datetime(2023, 8, 15, tzinfo=datetime.timezone.utc),
            datetime.datetime(2024, 5, 15, tzinfo=datetime.timezone.utc),
        ),
        manifest=man_quarterly,
        rules=rules,
    )
    assert res_q_cross2.held


def test_section_12_prohibited_vocabulary_absence() -> None:
    """Verify Section 12 prohibited vocabulary is absent from all project code and docs."""
    banned = re.compile(
        r"\b(simply|robust|seamless|best|optimal|clean|minimal|powerful|cutting-edge)\b",
        re.IGNORECASE,
    )
    violations: list[str] = []

    this_file = Path(__file__).resolve()
    for root_dir in ["vdok", "rules", "tests", "manifests", "schemas", "bench"]:
        for p in Path(root_dir).rglob("*"):
            if (
                p.is_file()
                and p.resolve() != this_file
                and p.suffix in (".py", ".yaml", ".yml", ".md")
            ):
                text = p.read_text(encoding="utf-8", errors="ignore")
                for line_no, line in enumerate(text.splitlines(), 1):
                    m = banned.search(line)
                    if m:
                        violations.append(
                            f"{p}:{line_no}: banned '{m.group(0)}' in: {line.strip()}"
                        )

    for f in Path("docs").glob("*.md"):
        if f.name == "SPEC.md":
            continue
        text = f.read_text(encoding="utf-8", errors="ignore")
        for line_no, line in enumerate(text.splitlines(), 1):
            m = banned.search(line)
            if m:
                violations.append(f"{f}:{line_no}: banned '{m.group(0)}' in: {line.strip()}")

    assert not violations, "Prohibited vocabulary violations detected:\n" + "\n".join(violations)
