"""Unit tests for readiness report generation, verdict logic, inquiries, and drop diffing."""

from __future__ import annotations

import datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa

from vdok.manifest import ExpectedCalendar, VendorManifest
from vdok.report import (
    CoveragePeriod,
    CoverageReport,
    IdentifiersReport,
    ReportVerdict,
    RevisionsReport,
    SchemaDifferenceItem,
    SchemaReport,
    ValidationReport,
    compile_readiness_report,
    compute_coverage,
    compute_verdict,
    diff_drops,
    generate_vendor_inquiries,
    load_report,
)
from vdok.store import BitemporalStore
from vdok.validate import ValidationResult, ValidationSummary


def test_compute_verdict_all_outcomes() -> None:
    # 1. READY outcome
    v1, r1 = compute_verdict(
        held=False,
        held_reasons=[],
        quarantine_count=0,
        warn_count=0,
        unmapped_count=0,
        schema_differences=[],
    )
    assert v1 == ReportVerdict.READY
    assert "All validation checks passed" in r1

    # 2. READY WITH WARNINGS outcome (quarantine records)
    v2, r2 = compute_verdict(
        held=False,
        held_reasons=[],
        quarantine_count=5,
        warn_count=0,
        unmapped_count=0,
        schema_differences=[],
    )
    assert v2 == ReportVerdict.READY_WITH_WARNINGS
    assert "5 records quarantined" in r2

    # 3. READY WITH WARNINGS outcome (unmapped identifiers)
    v3, r3 = compute_verdict(
        held=False,
        held_reasons=[],
        quarantine_count=0,
        warn_count=2,
        unmapped_count=10,
        schema_differences=[],
    )
    assert v3 == ReportVerdict.READY_WITH_WARNINGS
    assert "10 unmapped identifiers" in r3

    # 4. READY WITH WARNINGS outcome (non-breaking schema drift)
    v4, r4 = compute_verdict(
        held=False,
        held_reasons=[],
        quarantine_count=0,
        warn_count=0,
        unmapped_count=0,
        schema_differences=[
            SchemaDifferenceItem(
                column="extra",
                category="added_column",
                severity="warn",
                proposed_change="Add column",
            )
        ],
    )
    assert v4 == ReportVerdict.READY_WITH_WARNINGS
    assert "non-breaking schema differences" in r4

    # 5. HELD outcome
    v5, r5 = compute_verdict(
        held=True,
        held_reasons=["Batch volume collapsed by 85%", "Duplicate ratio 4.2%"],
        quarantine_count=10,
        warn_count=5,
        unmapped_count=0,
        schema_differences=[],
    )
    assert v5 == ReportVerdict.HELD
    assert "Batch placed on HOLD" in r5
    assert "Batch volume collapsed" in r5


def test_compute_coverage_cadences() -> None:
    schema = pa.schema(
        [
            pa.field("event_time", pa.timestamp("us", tz="UTC")),
            pa.field("entity_id", pa.string()),
        ]
    )

    t0 = datetime.datetime(2024, 1, 15, 10, 0, 0, tzinfo=datetime.timezone.utc)
    t1 = datetime.datetime(2024, 2, 20, 10, 0, 0, tzinfo=datetime.timezone.utc)
    t2 = datetime.datetime(2024, 5, 10, 10, 0, 0, tzinfo=datetime.timezone.utc)

    tbl = pa.Table.from_pydict(
        {
            "event_time": [t0, t1, t2],
            "entity_id": ["E1", "E2", "E3"],
        },
        schema=schema,
    )

    # Monthly coverage
    cov_monthly = compute_coverage(tbl, ExpectedCalendar.MONTHLY)
    assert cov_monthly.expected_calendar == "monthly"
    assert cov_monthly.total_periods == 3
    assert [p.period for p in cov_monthly.periods] == ["2024-01", "2024-02", "2024-05"]

    # Quarterly coverage
    cov_quarterly = compute_coverage(tbl, ExpectedCalendar.QUARTERLY)
    assert cov_quarterly.expected_calendar == "quarterly"
    assert cov_quarterly.total_periods == 2
    assert [p.period for p in cov_quarterly.periods] == ["2024-Q1", "2024-Q2"]

    # None calendar
    cov_none = compute_coverage(tbl, ExpectedCalendar.NONE)
    assert cov_none.total_periods == 1
    assert cov_none.periods[0].period == "all"
    assert cov_none.periods[0].row_count == 3


def test_generate_vendor_inquiries_deterministic() -> None:
    val_rep = ValidationReport(
        total_evaluated=1000,
        valid_count=980,
        quarantine_count=20,
        quarantine_share=0.02,
        warn_count=5,
        held=False,
        rule_counts={
            "valid_unit_of_measure": 12,
            "out_of_range": 8,
            "duplicate_key": 4,
        },
    )
    cov_rep = CoverageReport(
        expected_calendar="quarterly",
        total_periods=4,
        gaps_count=1,
        periods=[
            CoveragePeriod(period="2023-Q1", row_count=500),
            CoveragePeriod(period="2023-Q2", row_count=0, is_gap=True),
        ],
    )
    id_rep = IdentifiersReport(
        total_rows=1000,
        mapped_count=990,
        unmapped_count=10,
        mapped_share=0.99,
        top_unmapped=[{"id": "UNMAPPED_CORP", "count": 10}],
    )
    rev_rep = RevisionsReport(
        total_revisions=5,
        rate_per_field={"price": 0.08},
        zero_revision_fields=["volume"],
        consecutive_drops_without_revisions=4,
    )
    sch_rep = SchemaReport(
        differences=[
            SchemaDifferenceItem(
                column="legacy_tag",
                category="removed_column",
                severity="hold",
                proposed_change="Remove column",
            )
        ]
    )

    inquiries = generate_vendor_inquiries(
        validation_report=val_rep,
        coverage_report=cov_rep,
        identifiers_report=id_rep,
        revisions_report=rev_rep,
        schema_report=sch_rep,
    )

    assert len(inquiries) >= 6
    # Verify templates
    assert any("Schema contract breach: Column 'legacy_tag'" in q for q in inquiries)
    assert any("Column 'unit' contains 12 observations" in q for q in inquiries)
    assert any("Rule 'out_of_range' flagged 8 observations" in q for q in inquiries)
    assert any("Batch contains 4 duplicate observation keys" in q for q in inquiries)
    assert any("Expected quarterly coverage shows zero observations" in q for q in inquiries)
    assert any("Feed contains 10 unmapped entity identifiers" in q for q in inquiries)
    assert any(
        "Zero historical revisions observed across 4 consecutive drops" in q for q in inquiries
    )
    assert any("Field 'price' observed a restatement rate of 8.0%" in q for q in inquiries)


def test_render_markdown_and_json_roundtrip(tmp_path: Path) -> None:
    manifest_yaml = """
vendor: synthetic
entity_columns: [entity_id]
entity_id_type: ticker
event_time_column: event_time
knowledge_time:
  source: drop_time
value_columns: [price]
expected_calendar: daily
"""
    manifest = VendorManifest.from_yaml(manifest_yaml)

    t = datetime.datetime(2024, 1, 15, 12, 0, 0, tzinfo=datetime.timezone.utc)
    tbl = pa.Table.from_pydict(
        {
            "entity_id": ["AAPL", "MSFT"],
            "entity_id_type": ["ticker", "ticker"],
            "field": ["price", "price"],
            "event_time": [t, t],
            "knowledge_time": [t, t],
            "value_num": [Decimal("185.50"), Decimal("420.00")],
            "value_str": [None, None],
            "unit": ["USD", "USD"],
            "drop_id": ["drop_01", "drop_01"],
            "source_file": ["data.csv", "data.csv"],
            "source_row": [1, 2],
            "row_hash": ["h1", "h2"],
            "unmapped": [False, False],
        }
    )

    val_res = ValidationResult(
        drop_id="drop_01",
        vendor="synthetic",
        held=False,
        valid_table=tbl,
        quarantine_table=tbl.slice(0, 0),
        summary=ValidationSummary(
            total_rows=2,
            valid_count=2,
            quarantine_count=0,
            warn_count=0,
            quarantine_share=0.0,
            held=False,
            rule_counts={},
            severity_counts={},
            hold_reasons=[],
        ),
    )

    report = compile_readiness_report(
        vendor="synthetic",
        drop_id="drop_01",
        discovery_result=None,
        schema_diff=None,
        manifest=manifest,
        canonical_table=tbl,
        validation_result=val_res,
        revision_summary=None,
        timings={"normalization": 0.01, "validation": 0.02, "total": 0.05},
        peak_memory_mb=32.5,
    )

    assert report.verdict == ReportVerdict.READY
    md_content = report.to_markdown()
    assert "# Vendor Readiness Report: synthetic" in md_content
    assert "## 1. Verdict" in md_content
    assert "## 2. Drop Summary" in md_content
    assert "## 3. Schema" in md_content
    assert "## 4. Coverage by Period" in md_content
    assert "## 5. Validation" in md_content
    assert "## 6. Identifiers" in md_content
    assert "## 7. Revisions" in md_content
    assert "## 8. Questions for the Vendor" in md_content
    assert "## 9. Performance & Resource Profile" in md_content

    # Save and reload
    md_file, json_file = report.save(base_dir=tmp_path / "results")
    assert md_file.exists()
    assert json_file.exists()

    loaded = load_report(vendor="synthetic", drop_id="drop_01", base_dir=tmp_path / "results")
    assert loaded is not None
    assert loaded.vendor == "synthetic"
    assert loaded.drop_id == "drop_01"
    assert loaded.verdict == ReportVerdict.READY
    assert loaded.drop_summary.total_rows == 2


def test_diff_drops_comparison(tmp_path: Path) -> None:
    store_dir = tmp_path / "store"
    store = BitemporalStore(base_dir=store_dir, vendor="synthetic")

    manifest = VendorManifest.from_yaml("""
vendor: synthetic
entity_columns: [entity_id]
entity_id_type: ticker
event_time_column: event_time
knowledge_time:
  source: drop_time
value_columns: [price]
""")

    t1 = datetime.datetime(2024, 1, 15, 12, 0, 0, tzinfo=datetime.timezone.utc)
    t2 = datetime.datetime(2024, 1, 16, 12, 0, 0, tzinfo=datetime.timezone.utc)

    # Drop A: AAPL=180, MSFT=400, GOOGL=150
    table_a = pa.Table.from_pydict(
        {
            "entity_id": ["AAPL", "MSFT", "GOOGL"],
            "entity_id_type": ["ticker", "ticker", "ticker"],
            "field": ["price", "price", "price"],
            "event_time": [t1, t1, t1],
            "knowledge_time": [t1, t1, t1],
            "value_num": [Decimal("180.00"), Decimal("400.00"), Decimal("150.00")],
            "value_str": [None, None, None],
            "unit": ["USD", "USD", "USD"],
            "drop_id": ["drop_a", "drop_a", "drop_a"],
            "source_file": ["a.csv", "a.csv", "a.csv"],
            "source_row": [1, 2, 3],
            "row_hash": ["ha1", "ha2", "ha3"],
            "unmapped": [False, False, False],
        }
    )
    store.write_drop(table_a, manifest=manifest)

    # Drop B: AAPL=190 (restated), MSFT=400 (unchanged), NVDA=500 (added), GOOGL removed
    table_b = pa.Table.from_pydict(
        {
            "entity_id": ["AAPL", "MSFT", "NVDA"],
            "entity_id_type": ["ticker", "ticker", "ticker"],
            "field": ["price", "price", "price"],
            "event_time": [t1, t1, t2],
            "knowledge_time": [t2, t2, t2],
            "value_num": [Decimal("190.00"), Decimal("400.00"), Decimal("500.00")],
            "value_str": [None, None, None],
            "unit": ["USD", "USD", "USD"],
            "drop_id": ["drop_b", "drop_b", "drop_b"],
            "source_file": ["b.csv", "b.csv", "b.csv"],
            "source_row": [1, 2, 3],
            "row_hash": ["hb1", "hb2", "hb3"],
            "unmapped": [False, False, False],
        }
    )
    store.write_drop(table_b, manifest=manifest)

    diff_res = diff_drops(
        store_dir=store_dir,
        vendor="synthetic",
        drop_a="drop_a",
        drop_b="drop_b",
    )

    assert diff_res.total_keys_a == 3
    assert diff_res.total_keys_b == 3
    assert diff_res.common_keys == 2  # AAPL and MSFT
    assert diff_res.added_keys == 1  # NVDA
    assert diff_res.removed_keys == 1  # GOOGL
    assert diff_res.changed_values == 1  # AAPL
    assert diff_res.unchanged_values == 1  # MSFT
    assert diff_res.restatement_rate == 0.5

    md_diff = diff_res.to_markdown()
    assert "# Drop Diff: synthetic" in md_diff
    assert "Common Observation Keys | 2" in md_diff
    assert "Changed Values (Restatements) | 1" in md_diff
    assert "AAPL" in md_diff
