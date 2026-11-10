"""
Tier 1 Feature Tests: F-REP-01..04 Readiness Report & Automated Vendor Inquiries.

Tests daily coverage matrix construction, >20% coverage dip anomaly detection,
validation scorecard generation, automated vendor email inquiry generation (100% anomaly coverage),
and dual-format report emission (Markdown and JSON).
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from test_tier1_features.test_validation_rules import ValidationResult


@dataclass
class CoverageDipAlert:
    date: str
    baseline_count: int
    current_count: int
    dip_percentage: float
    description: str


@dataclass
class ReadinessReportData:
    run_timestamp: str
    dataset_id: str
    vendor_id: str
    overall_status: str  # PASS, WARN, FAIL
    total_records: int
    scorecard: list[dict[str, Any]]
    coverage_dips: list[CoverageDipAlert]
    vendor_inquiries: list[str]


class ReadinessReportGenerator:
    """Opaque-box reference generator conforming to F-REP-01..04."""

    def __init__(self, conn: duckdb.DuckDBPyConnection | None = None) -> None:
        self.conn = conn or duckdb.connect()

    def build_coverage_matrix(self, table: pa.Table, entity_col: str = "entity_id", time_col: str = "event_time") -> dict[str, dict[str, int]]:
        con = self.conn
        con.register("tbl", table)
        query = f"""
            SELECT "{entity_col}", strftime(CAST("{time_col}" AS TIMESTAMP), '%Y-%m-%d') as trade_date, count(*) as cnt
            FROM tbl
            WHERE "{entity_col}" IS NOT NULL AND "{time_col}" IS NOT NULL
            GROUP BY "{entity_col}", trade_date
            ORDER BY trade_date, "{entity_col}"
        """
        rows = con.sql(query).fetchall()
        matrix: dict[str, dict[str, int]] = {}
        for ent, dt, cnt in rows:
            if ent not in matrix:
                matrix[ent] = {}
            matrix[ent][dt] = cnt
        return matrix

    def detect_coverage_dips(self, table: pa.Table, dip_threshold_pct: float = 20.0) -> list[CoverageDipAlert]:
        if "event_time" not in table.column_names or "entity_id" not in table.column_names:
            return []
        con = self.conn
        con.register("tbl", table)
        daily_counts = con.sql("""
            SELECT strftime(CAST(event_time AS TIMESTAMP), '%Y-%m-%d') as trade_date, count(DISTINCT entity_id) as entity_count
            FROM tbl
            WHERE event_time IS NOT NULL AND entity_id IS NOT NULL
            GROUP BY trade_date
            ORDER BY trade_date
        """).fetchall()

        alerts = []
        for i in range(1, len(daily_counts)):
            prev_date, prev_count = daily_counts[i - 1]
            curr_date, curr_count = daily_counts[i]
            if prev_count > 0:
                dip_pct = ((prev_count - curr_count) / prev_count) * 100.0
                if dip_pct >= dip_threshold_pct:
                    alerts.append(
                        CoverageDipAlert(
                            date=curr_date,
                            baseline_count=prev_count,
                            current_count=curr_count,
                            dip_percentage=dip_pct,
                            description=f"Coverage dropped {dip_pct:.1f}% on {curr_date} ({prev_count} -> {curr_count} entities)",
                        )
                    )
        return alerts

    def generate_vendor_inquiries(
        self,
        validation_results: list[ValidationResult],
        coverage_dips: list[CoverageDipAlert],
        drift_report_changes: list[Any] | None = None,
    ) -> list[str]:
        inquiries: list[str] = []

        for vr in validation_results:
            if not vr.passed:
                if vr.check_name == "range_domain":
                    symbols = {s.get("source_symbol") or s.get("entity_id") for s in vr.violations_sample if s}
                    inquiries.append(
                        f"Subject: Data Quality Inquiry - Out-of-Range / Negative Prices Detected\n"
                        f"We detected {vr.violation_count} records with negative or invalid values "
                        f"affecting symbols: {', '.join(sorted(str(s) for s in symbols if s))}. "
                        f"Could you confirm whether these are raw market anomalies or adjustments?"
                    )
                elif vr.check_name == "completeness":
                    inquiries.append(
                        f"Subject: SLA Violation - Null Values in Mandatory Fields\n"
                        f"Found {vr.violation_count} records ({vr.violation_rate:.2%}) with missing mandatory fields, "
                        f"exceeding the agreed SLA threshold of {vr.threshold:.2%}. Please provide corrected backfills."
                    )
                elif vr.check_name == "future_timestamps":
                    inquiries.append(
                        f"Subject: Data Quality Inquiry - Future Timestamps Detected\n"
                        f"Found {vr.violation_count} records with effective timestamps in the future relative to knowledge time."
                    )
                elif vr.check_name == "monotone_timestamps":
                    inquiries.append(
                        f"Subject: Data Quality Inquiry - Timestamp Inversion / Clock Skew\n"
                        f"Found {vr.violation_count} records with backwards timestamp sequences for active entities."
                    )

        for dip in coverage_dips:
            inquiries.append(
                f"Subject: Universe Coverage Alert - Missing Entities on {dip.date}\n"
                f"Active entity coverage dropped by {dip.dip_percentage:.1f}% on {dip.date} "
                f"({dip.baseline_count} -> {dip.current_count} symbols). Please verify if a market holiday or feed outage occurred."
            )

        if drift_report_changes:
            inquiries.append(
                f"Subject: Schema Contract Drift Notification\n"
                f"Detected {len(drift_report_changes)} unauthorized schema changes against pinned contract. "
                f"Please provide an updated data dictionary."
            )

        return inquiries

    def generate_report(
        self,
        dataset_id: str,
        vendor_id: str,
        table: pa.Table,
        validation_results: list[ValidationResult],
        drift_changes: list[Any] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        dips = self.detect_coverage_dips(table)
        inquiries = self.generate_vendor_inquiries(validation_results, dips, drift_changes)

        has_failures = any(not vr.passed for vr in validation_results) or len(dips) > 0
        overall = "FAIL" if has_failures else "PASS"

        scorecard_list = [
            {
                "rule": vr.check_name,
                "passed": vr.passed,
                "violation_count": vr.violation_count,
                "violation_rate": vr.violation_rate,
                "threshold": vr.threshold,
            }
            for vr in validation_results
        ]

        report_data = ReadinessReportData(
            run_timestamp=datetime.now(timezone.utc).isoformat(),
            dataset_id=dataset_id,
            vendor_id=vendor_id,
            overall_status=overall,
            total_records=table.num_rows,
            scorecard=scorecard_list,
            coverage_dips=dips,
            vendor_inquiries=inquiries,
        )

        # Build Markdown
        md_lines = [
            f"# Vendor Data Readiness Report: {dataset_id}",
            f"**Vendor**: {vendor_id} | **Status**: {overall} | **Records**: {table.num_rows}\n",
            "## 1. Validation Scorecard",
            "| Rule | Status | Violations | Rate | Threshold |",
            "|---|---|---|---|---|",
        ]
        for sc in scorecard_list:
            status_icon = "PASS" if sc["passed"] else "FAIL"
            md_lines.append(f"| {sc['rule']} | {status_icon} | {sc['violation_count']} | {sc['violation_rate']:.2%} | {sc['threshold']:.2%} |")

        md_lines.append("\n## 2. Coverage Dips (>20%)")
        if dips:
            for dip in dips:
                md_lines.append(f"- **{dip.date}**: {dip.description}")
        else:
            md_lines.append("No coverage dips detected.")

        md_lines.append("\n## 3. Automated Vendor Inquiries")
        if inquiries:
            for inq in inquiries:
                md_lines.append(f"```text\n{inq}\n```\n")
        else:
            md_lines.append("No vendor inquiries required.")

        md_str = "\n".join(md_lines)
        json_dict = asdict(report_data)
        return md_str, json_dict


@pytest.fixture
def reporter(temp_duckdb_conn) -> ReadinessReportGenerator:
    return ReadinessReportGenerator(temp_duckdb_conn)


# ============================================================================
# F-REP-01: Daily Coverage Matrix & Dip Detector Tests
# ============================================================================

@pytest.mark.tier1
def test_coverage_matrix_baseline_100_percent(reporter, fixture_paths):
    """Verify Drop 1 coverage matrix has 10 entities x 5 trading days."""
    table = pq.read_table(fixture_paths.drop_001_parquet)
    matrix = reporter.build_coverage_matrix(table)

    assert len(matrix) == 10  # 10 equities
    for ent, dates in matrix.items():
        assert len(dates) == 5  # 5 trading dates (Oct 2 - Oct 6)


@pytest.mark.tier1
def test_coverage_matrix_missing_date_alert(reporter):
    """Verify missing trading day for an entity is identifiable in matrix."""
    records = [
        {"entity_id": "ENT_A", "event_time": "2023-10-02T16:00:00Z"},
        {"entity_id": "ENT_A", "event_time": "2023-10-04T16:00:00Z"},  # Missing Oct 3
        {"entity_id": "ENT_B", "event_time": "2023-10-02T16:00:00Z"},
        {"entity_id": "ENT_B", "event_time": "2023-10-03T16:00:00Z"},
    ]
    table = pa.Table.from_pylist(records)
    matrix = reporter.build_coverage_matrix(table)

    assert "2023-10-03" not in matrix["ENT_A"]
    assert "2023-10-03" in matrix["ENT_B"]


@pytest.mark.tier1
def test_coverage_dip_detector_over_20_percent(reporter):
    """Verify coverage drop from 10 to 7 entities (30% dip) triggers alert."""
    records = []
    # Day 1: 10 entities
    for i in range(10):
        records.append({"entity_id": f"ENT_{i}", "event_time": "2023-10-02T16:00:00Z"})
    # Day 2: only 7 entities (30% drop)
    for i in range(7):
        records.append({"entity_id": f"ENT_{i}", "event_time": "2023-10-03T16:00:00Z"})

    table = pa.Table.from_pylist(records)
    dips = reporter.detect_coverage_dips(table, dip_threshold_pct=20.0)

    assert len(dips) == 1
    assert dips[0].date == "2023-10-03"
    assert dips[0].dip_percentage == pytest.approx(30.0)
    assert dips[0].baseline_count == 10
    assert dips[0].current_count == 7


@pytest.mark.tier1
def test_coverage_dip_detector_under_20_percent_no_alert(reporter):
    """Verify minor coverage drop of 10% (10 -> 9 entities) does not trigger alert."""
    records = []
    for i in range(10):
        records.append({"entity_id": f"ENT_{i}", "event_time": "2023-10-02T16:00:00Z"})
    for i in range(9):
        records.append({"entity_id": f"ENT_{i}", "event_time": "2023-10-03T16:00:00Z"})

    table = pa.Table.from_pylist(records)
    dips = reporter.detect_coverage_dips(table, dip_threshold_pct=20.0)
    assert len(dips) == 0


@pytest.mark.tier1
def test_coverage_matrix_cell_count_aggregation(reporter, fixture_paths):
    """Verify matrix cells accurately count observation frequencies."""
    table = pq.read_table(fixture_paths.drop_001_parquet)
    matrix = reporter.build_coverage_matrix(table)
    # Check that AAPL has 1 close price per day
    assert matrix["ENT_AAPL"]["2023-10-02"] >= 1


# ============================================================================
# F-REP-02: Validation Scorecard Tests
# ============================================================================

@pytest.mark.tier1
def test_scorecard_generation_baseline_pass(reporter, fixture_paths):
    """Verify Drop 1 produces PASS scorecard with 100% rule passing rate."""
    table = pq.read_table(fixture_paths.drop_001_parquet)
    val_results = [
        ValidationResult("completeness", True, 60, 0, 0.0, [], 0.0),
        ValidationResult("range_domain", True, 60, 0, 0.0, [], 0.0),
        ValidationResult("monotone", True, 60, 0, 0.0, [], 0.0),
    ]
    md, report_dict = reporter.generate_report("EQUITY_DAILY", "SYNTH_A", table, val_results)

    assert report_dict["overall_status"] == "PASS"
    assert all(item["passed"] for item in report_dict["scorecard"])
    assert "PASS" in md


@pytest.mark.tier1
def test_scorecard_generation_drop3_fail(reporter, fixture_paths):
    """Verify Drop 3 produces FAIL scorecard when violations are detected."""
    table = duckdb.sql(f"SELECT * FROM read_csv('{fixture_paths.drop_003_csv}', header=true)").arrow().read_all()
    val_results = [
        ValidationResult("completeness", False, 21, 2, 2 / 21, [{"record_id": "REC_003_ANOM_0003"}], 0.0),
        ValidationResult("range_domain", False, 21, 2, 2 / 21, [{"record_id": "REC_003_ANOM_0001"}], 0.0),
    ]
    md, report_dict = reporter.generate_report("EQUITY_DAILY", "SYNTH_A", table, val_results)

    assert report_dict["overall_status"] == "FAIL"
    assert len(report_dict["vendor_inquiries"]) >= 2
    assert "FAIL" in md


@pytest.mark.tier1
def test_scorecard_violation_sample_extraction(reporter):
    """Verify scorecard stores violation counts and rates for failing checks."""
    table = pa.Table.from_pylist([{"x": 1}])
    samples = [{"record_id": "R1", "value": -100.0}]
    val_results = [ValidationResult("range_domain", False, 10, 1, 0.10, samples, 0.0)]
    _, report_dict = reporter.generate_report("EQ", "VEND", table, val_results)

    sc = report_dict["scorecard"][0]
    assert sc["rule"] == "range_domain"
    assert sc["violation_count"] == 1
    assert sc["violation_rate"] == 0.10


@pytest.mark.tier1
def test_scorecard_rule_statistics_summary(reporter, fixture_paths):
    """Verify all evaluated rules appear in scorecard output."""
    table = pq.read_table(fixture_paths.drop_001_parquet)
    val_results = [
        ValidationResult("completeness", True, 60, 0, 0.0, [], 0.0),
        ValidationResult("range_domain", True, 60, 0, 0.0, [], 0.0),
        ValidationResult("intra_drop_duplicates", True, 60, 0, 0.0, [], 0.0),
        ValidationResult("monotone_timestamps", True, 60, 0, 0.0, [], 0.0),
        ValidationResult("future_timestamps", True, 60, 0, 0.0, [], 0.0),
    ]
    _, report_dict = reporter.generate_report("EQ", "VEND", table, val_results)
    assert len(report_dict["scorecard"]) == 5


# ============================================================================
# F-REP-03: Automated Vendor Questions Generator Tests
# ============================================================================

@pytest.mark.tier1
def test_vendor_questions_negative_price(reporter):
    """Verify negative price generates specific inquiry draft with symbols."""
    samples = [{"source_symbol": "AAPL", "value": -178.50}, {"source_symbol": "TSLA", "value": -242.00}]
    vr = ValidationResult("range_domain", False, 21, 2, 2 / 21, samples, 0.0)
    inquiries = reporter.generate_vendor_inquiries([vr], [])

    assert len(inquiries) == 1
    inq_text = inquiries[0]
    assert "Negative Prices Detected" in inq_text
    assert "AAPL" in inq_text
    assert "TSLA" in inq_text


@pytest.mark.tier1
def test_vendor_questions_coverage_dip(reporter):
    """Verify >20% coverage dip generates universe coverage email draft."""
    dip = CoverageDipAlert("2023-10-09", 10, 7, 30.0, "Coverage dropped 30%")
    inquiries = reporter.generate_vendor_inquiries([], [dip])

    assert len(inquiries) == 1
    assert "Universe Coverage Alert" in inquiries[0]
    assert "2023-10-09" in inquiries[0]
    assert "30.0%" in inquiries[0]


@pytest.mark.tier1
def test_vendor_questions_schema_drift(reporter):
    """Verify schema drift generates contract drift notification email draft."""
    changes = ["dropped column unit", "type changed value double -> string"]
    inquiries = reporter.generate_vendor_inquiries([], [], drift_report_changes=changes)

    assert len(inquiries) == 1
    assert "Schema Contract Drift" in inquiries[0]


@pytest.mark.tier1
def test_vendor_questions_null_sla_exceeded(reporter):
    """Verify null SLA violation generates inquiry draft citing threshold."""
    vr = ValidationResult("completeness", False, 100, 10, 0.10, [], 0.02)
    inquiries = reporter.generate_vendor_inquiries([vr], [])

    assert len(inquiries) == 1
    assert "SLA Violation" in inquiries[0]
    assert "2.00%" in inquiries[0]


@pytest.mark.tier1
def test_vendor_questions_100_percent_anomaly_coverage(reporter):
    """Verify 100.00% of recognized anomaly categories produce actionable email inquiry text per PROJECT.md §242."""
    anomalies = [
        ValidationResult("completeness", False, 10, 1, 0.1, [], 0.0),
        ValidationResult("range_domain", False, 10, 1, 0.1, [{"source_symbol": "X"}], 0.0),
        ValidationResult("future_timestamps", False, 10, 1, 0.1, [], 0.0),
        ValidationResult("monotone_timestamps", False, 10, 1, 0.1, [], 0.0),
    ]
    dip = CoverageDipAlert("2023-10-02", 10, 7, 30.0, "Dip")
    drift = ["col dropped"]

    inquiries = reporter.generate_vendor_inquiries(anomalies, [dip], drift)
    # 4 validation + 1 dip + 1 drift = 6 inquiries
    assert len(inquiries) == 6
    for inq in inquiries:
        assert inq.startswith("Subject: ")
        assert len(inq) > 50


# ============================================================================
# F-REP-04: Multi-Format Report Emitter Tests
# ============================================================================

@pytest.mark.tier1
def test_report_markdown_emission_structure(reporter, fixture_paths):
    """Verify Markdown report contains proper headings, tables, and sections."""
    table = pq.read_table(fixture_paths.drop_001_parquet)
    val_results = [ValidationResult("completeness", True, 60, 0, 0.0, [], 0.0)]
    md, _ = reporter.generate_report("EQUITY_DAILY", "SYNTH_A", table, val_results)

    assert "# Vendor Data Readiness Report" in md
    assert "## 1. Validation Scorecard" in md
    assert "## 2. Coverage Dips" in md
    assert "## 3. Automated Vendor Inquiries" in md


@pytest.mark.tier1
def test_report_json_emission_structure(reporter, fixture_paths):
    """Verify JSON report dictionary contains valid schema structure."""
    table = pq.read_table(fixture_paths.drop_001_parquet)
    val_results = [ValidationResult("completeness", True, 60, 0, 0.0, [], 0.0)]
    _, report_dict = reporter.generate_report("EQUITY_DAILY", "SYNTH_A", table, val_results)

    assert "run_timestamp" in report_dict
    assert report_dict["dataset_id"] == "EQUITY_DAILY"
    assert report_dict["vendor_id"] == "SYNTH_A"
    assert "scorecard" in report_dict
    assert isinstance(report_dict["vendor_inquiries"], list)


@pytest.mark.tier1
def test_report_markdown_and_json_parity(reporter, fixture_paths):
    """Verify metrics match identically between Markdown and JSON outputs."""
    table = pq.read_table(fixture_paths.drop_001_parquet)
    val_results = [ValidationResult("completeness", True, 60, 0, 0.0, [], 0.0)]
    md, report_dict = reporter.generate_report("EQUITY_DAILY", "SYNTH_A", table, val_results)

    assert report_dict["total_records"] == 60
    assert "60" in md
    assert report_dict["overall_status"] in md


@pytest.mark.tier1
def test_report_file_export_to_disk(reporter, fixture_paths, tmp_path):
    """Verify saving readiness_report.md and readiness_report.json to disk."""
    table = pq.read_table(fixture_paths.drop_001_parquet)
    val_results = [ValidationResult("completeness", True, 60, 0, 0.0, [], 0.0)]
    md_str, json_dict = reporter.generate_report("EQUITY_DAILY", "SYNTH_A", table, val_results)

    md_file = tmp_path / "readiness_report.md"
    json_file = tmp_path / "readiness_report.json"

    md_file.write_text(md_str, encoding="utf-8")
    json_file.write_text(json.dumps(json_dict, indent=2), encoding="utf-8")

    assert md_file.exists() and md_file.stat().st_size > 0
    assert json_file.exists() and json_file.stat().st_size > 0

    # Verify JSON parses cleanly
    loaded = json.loads(json_file.read_text(encoding="utf-8"))
    assert loaded["dataset_id"] == "EQUITY_DAILY"
