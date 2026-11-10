"""Scorecard and Multi-Format Readiness Report Emitter (F-REP-02, F-REP-04)."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa

from vendor_onboarding.report.coverage import (
    CoverageAnalyzer,
    CoverageDipAlert,
    DailyCoverageMatrix,
)
from vendor_onboarding.report.questions import VendorQuestionsGenerator


@dataclass
class PipelineRunContext:
    """Context object encapsulating ingestion, validation, and diff artifacts for a run."""

    dataset_id: str
    vendor_id: str
    table: pa.Table
    validation_results: list[Any] = field(default_factory=list)
    drift_changes: list[Any] | None = None
    diff_summary: Any | None = None
    quarantine_records: list[Any] | None = None
    drop_id: str = "DROP_001"


@dataclass
class ReadinessReportData:
    """Strongly-typed structure for readiness scorecard and inquiry drafts."""

    run_timestamp: str
    dataset_id: str
    vendor_id: str
    overall_status: str  # PASS, WARN, FAIL
    total_records: int
    scorecard: list[dict[str, Any]]
    coverage_dips: list[CoverageDipAlert]
    vendor_inquiries: list[str]


class ReadinessReporter:
    """Generates dual-format (Markdown and JSON) vendor onboarding readiness reports."""

    def __init__(self, conn: duckdb.DuckDBPyConnection | None = None) -> None:
        self.conn = conn or duckdb.connect()
        self.conn.execute("SET timezone = 'UTC';")
        self.analyzer = CoverageAnalyzer(conn=self.conn)
        self.questions_gen = VendorQuestionsGenerator()

    def build_coverage_matrix(
        self,
        table: pa.Table,
        entity_col: str = "entity_id",
        time_col: str = "event_time",
    ) -> DailyCoverageMatrix:
        """Convenience delegation to CoverageAnalyzer."""
        return self.analyzer.build_matrix(table, entity_col=entity_col, time_col=time_col)

    def detect_coverage_dips(
        self,
        table: pa.Table,
        dip_threshold_pct: float = 20.0,
        entity_col: str = "entity_id",
        time_col: str = "event_time",
    ) -> list[CoverageDipAlert]:
        """Convenience delegation to CoverageAnalyzer."""
        return self.analyzer.detect_dips(
            table,
            dip_threshold_pct=dip_threshold_pct,
            entity_col=entity_col,
            time_col=time_col,
        )

    def generate_vendor_inquiries(
        self,
        validation_results: list[Any] | None = None,
        coverage_dips: list[CoverageDipAlert] | None = None,
        drift_report_changes: list[Any] | None = None,
        diff_summary: Any | None = None,
        quarantine_records: list[Any] | None = None,
        drop_id: str = "DROP_001",
    ) -> list[str]:
        """Convenience delegation to VendorQuestionsGenerator."""
        return self.questions_gen.generate_inquiries(
            validation_results=validation_results,
            coverage_dips=coverage_dips,
            drift_report_changes=drift_report_changes,
            diff_summary=diff_summary,
            quarantine_records=quarantine_records,
            drop_id=drop_id,
        )

    def generate_report(
        self,
        dataset_id_or_context: str | PipelineRunContext,
        vendor_id: str | None = None,
        table: pa.Table | None = None,
        validation_results: list[Any] | None = None,
        drift_changes: list[Any] | None = None,
        diff_summary: Any | None = None,
        quarantine_records: list[Any] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        """Emits dual-format report as (markdown_str, json_dict)."""
        if isinstance(dataset_id_or_context, PipelineRunContext):
            ctx = dataset_id_or_context
            dataset_id = ctx.dataset_id
            vend_id = ctx.vendor_id
            tbl = ctx.table
            val_results = ctx.validation_results or []
            drift = ctx.drift_changes
            diff_sum = ctx.diff_summary
            quarantine = ctx.quarantine_records
            drop_id = ctx.drop_id
        else:
            dataset_id = dataset_id_or_context
            vend_id = vendor_id or "UNKNOWN_VENDOR"
            tbl = table if table is not None else pa.table({})
            val_results = validation_results or []
            drift = drift_changes
            diff_sum = diff_summary
            quarantine = quarantine_records
            drop_id = getattr(diff_summary, "drop_id_current", "DROP_001") if diff_summary else "DROP_001"

        dips = self.detect_coverage_dips(tbl)
        inquiries = self.generate_vendor_inquiries(
            validation_results=val_results,
            coverage_dips=dips,
            drift_report_changes=drift,
            diff_summary=diff_sum,
            quarantine_records=quarantine,
            drop_id=drop_id,
        )

        has_failures = any(not getattr(vr, "passed", True) for vr in val_results) or len(dips) > 0
        overall = "FAIL" if has_failures else "PASS"

        scorecard_list = [
            {
                "rule": getattr(vr, "check_name", "unknown"),
                "passed": getattr(vr, "passed", True),
                "violation_count": getattr(vr, "violation_count", 0),
                "violation_rate": getattr(vr, "violation_rate", 0.0),
                "threshold": getattr(vr, "threshold", 0.0),
            }
            for vr in val_results
        ]

        report_data = ReadinessReportData(
            run_timestamp=datetime.now(UTC).isoformat(),
            dataset_id=dataset_id,
            vendor_id=vend_id,
            overall_status=overall,
            total_records=tbl.num_rows,
            scorecard=scorecard_list,
            coverage_dips=dips,
            vendor_inquiries=inquiries,
        )

        # Build Markdown
        md_lines = [
            f"# Vendor Data Readiness Report: {dataset_id}",
            f"**Vendor**: {vend_id} | **Status**: {overall} | **Records**: {tbl.num_rows}\n",
            "## 1. Validation Scorecard",
            "| Rule | Status | Violations | Rate | Threshold |",
            "|---|---|---|---|---|",
        ]
        for sc in scorecard_list:
            status_icon = "PASS" if sc["passed"] else "FAIL"
            md_lines.append(
                f"| {sc['rule']} | {status_icon} | {sc['violation_count']} | "
                f"{sc['violation_rate']:.2%} | {sc['threshold']:.2%} |"
            )

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


# Public alias per specification
ReadinessReportGenerator = ReadinessReporter


def export_reports(
    output_dir: str | Path,
    markdown_str: str,
    json_dict: dict[str, Any],
    base_name: str = "readiness_report",
) -> tuple[Path, Path]:
    """Persists readiness report Markdown and JSON files to disk."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    md_path = out / f"{base_name}.md"
    json_path = out / f"{base_name}.json"
    md_path.write_text(markdown_str, encoding="utf-8")
    json_path.write_text(json.dumps(json_dict, indent=2, default=str), encoding="utf-8")
    return md_path, json_path
