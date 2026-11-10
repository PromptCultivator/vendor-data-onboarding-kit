"""Readiness reporting and drop diffing engine.

Generates comprehensive evaluation scorecards in Markdown (report.md) and JSON
(report.json), issues categorical readiness verdicts (READY, READY WITH WARNINGS,
HELD), computes calendar coverage matrices, revision analytics, and deterministic
vendor inquiries without model generation.
"""

from __future__ import annotations

import datetime
import resource
from enum import Enum
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
from pydantic import BaseModel, Field

from vdok.discover import DropMetadata
from vdok.manifest import ExpectedCalendar, VendorManifest
from vdok.revisions import RevisionSummary
from vdok.schema import SchemaDiff
from vdok.validate import ValidationResult


class ReportVerdict(str, Enum):
    """Categorical evaluation verdict for vendor readiness."""

    READY = "READY"
    READY_WITH_WARNINGS = "READY WITH WARNINGS"
    HELD = "HELD"


class DropSummary(BaseModel):
    """Comprehensive summary metrics for a raw drop batch."""

    files: list[str] = Field(default_factory=list)
    total_bytes: int = 0
    total_rows: int = 0
    distinct_entities: int = 0
    distinct_fields: int = 0
    event_time_min: str | None = None
    event_time_max: str | None = None
    knowledge_time_min: str | None = None
    knowledge_time_max: str | None = None


class SchemaDifferenceItem(BaseModel):
    """Individual schema contract drift classification."""

    column: str
    category: str
    severity: str
    proposed_change: str


class SchemaReport(BaseModel):
    """Evaluation of drop schema against pinned contract."""

    pinned_version: int | None = None
    has_differences: bool = False
    differences: list[SchemaDifferenceItem] = Field(default_factory=list)
    proposed_changes: list[str] = Field(default_factory=list)


class CoveragePeriod(BaseModel):
    """Observation count and gap status for a reporting period."""

    period: str
    row_count: int
    is_gap: bool = False


class CoverageReport(BaseModel):
    """Calendar coverage distribution across expected reporting cadences."""

    expected_calendar: str = "none"
    total_periods: int = 0
    gaps_count: int = 0
    periods: list[CoveragePeriod] = Field(default_factory=list)


class ValidationReport(BaseModel):
    """Aggregated validation rule execution metrics and quarantine statistics."""

    total_evaluated: int = 0
    valid_count: int = 0
    quarantine_count: int = 0
    quarantine_share: float = 0.0
    warn_count: int = 0
    held: bool = False
    held_reasons: list[str] = Field(default_factory=list)
    rule_counts: dict[str, int] = Field(default_factory=dict)
    severity_counts: dict[str, int] = Field(default_factory=dict)
    top_reasons: list[dict[str, Any]] = Field(default_factory=list)


class IdentifiersReport(BaseModel):
    """Identifier resolution metrics and unmapped entity counts."""

    total_rows: int = 0
    mapped_count: int = 0
    unmapped_count: int = 0
    mapped_share: float = 1.0
    top_unmapped: list[dict[str, Any]] = Field(default_factory=list)


class TopRevision(BaseModel):
    """Individual historical restatement disclosure."""

    entity_id: str
    field: str
    event_time: str
    old_value: str
    new_value: str
    relative_change: float | None = None
    old_knowledge_time: str
    new_knowledge_time: str


class RevisionsReport(BaseModel):
    """Analytics from drop-over-drop revision ledger."""

    total_revisions: int = 0
    revised_entities_count: int = 0
    rate_per_field: dict[str, float] = Field(default_factory=dict)
    largest_changes: list[TopRevision] = Field(default_factory=list)
    zero_revision_fields: list[str] = Field(default_factory=list)
    consecutive_drops_without_revisions: int = 0
    silent_overwrite_warning: str | None = None


class TimingsReport(BaseModel):
    """Resource consumption and phase duration profile."""

    timings_per_stage_seconds: dict[str, float] = Field(default_factory=dict)
    total_seconds: float = 0.0
    peak_memory_mb: float = 0.0


class ReadinessReport(BaseModel):
    """Root model for vendor dataset readiness evaluation."""

    vendor: str
    drop_id: str
    verdict: ReportVerdict
    verdict_reason: str
    generated_at: str
    drop_summary: DropSummary
    schema_report: SchemaReport
    coverage_report: CoverageReport
    validation_report: ValidationReport
    identifiers_report: IdentifiersReport
    revisions_report: RevisionsReport
    vendor_inquiries: list[str] = Field(default_factory=list)
    timings: TimingsReport

    def to_markdown(self) -> str:
        """Render readiness evaluation report into GitHub Flavored Markdown."""
        return render_markdown_report(self)

    def to_json(self) -> str:
        """Serialize readiness report to formatted JSON string."""
        return self.model_dump_json(indent=2)

    def save(self, base_dir: Path | str = "data/results") -> tuple[Path, Path]:
        """Save report.md and report.json to disk under data/results/<vendor>/<drop_id>/."""
        target_dir = Path(base_dir) / self.vendor / self.drop_id
        target_dir.mkdir(parents=True, exist_ok=True)
        md_path = target_dir / "report.md"
        json_path = target_dir / "report.json"

        md_path.write_text(self.to_markdown(), encoding="utf-8")
        json_path.write_text(self.to_json(), encoding="utf-8")
        return md_path, json_path


class DropDiffResult(BaseModel):
    """Analytical difference comparison between two historical drops."""

    vendor: str
    drop_a: str
    drop_b: str
    total_keys_a: int = 0
    total_keys_b: int = 0
    common_keys: int = 0
    added_keys: int = 0
    removed_keys: int = 0
    changed_values: int = 0
    unchanged_values: int = 0
    restatement_rate: float = 0.0
    schema_differences: list[str] = Field(default_factory=list)
    top_changed_fields: list[dict[str, Any]] = Field(default_factory=list)
    largest_changes: list[dict[str, Any]] = Field(default_factory=list)

    def to_markdown(self) -> str:
        """Render drop diff into GitHub Flavored Markdown."""
        return render_diff_markdown(self)

    def to_json(self) -> str:
        """Serialize drop diff to formatted JSON string."""
        return self.model_dump_json(indent=2)


def compute_verdict(
    held: bool,
    held_reasons: list[str],
    quarantine_count: int,
    warn_count: int,
    unmapped_count: int,
    schema_differences: list[SchemaDifferenceItem],
) -> tuple[ReportVerdict, str]:
    """Compute overall readiness verdict using deterministic state rules."""
    if held:
        reason_str = (
            "; ".join(held_reasons)
            if held_reasons
            else "Batch held by validation rule or schema drift."
        )
        return ReportVerdict.HELD, f"Batch placed on HOLD: {reason_str}"

    warnings: list[str] = []
    if quarantine_count > 0:
        warnings.append(f"{quarantine_count:,} records quarantined")
    if warn_count > 0:
        warnings.append(f"{warn_count:,} validation warnings")
    if unmapped_count > 0:
        warnings.append(f"{unmapped_count:,} unmapped identifiers")

    has_schema_warn = any(d.severity == "warn" for d in schema_differences)
    if has_schema_warn:
        warnings.append(f"{len(schema_differences)} non-breaking schema differences")

    if warnings:
        return (
            ReportVerdict.READY_WITH_WARNINGS,
            f"Anomalies detected: {', '.join(warnings)}.",
        )

    return (
        ReportVerdict.READY,
        "All validation checks passed, zero quarantine records, no schema drift.",
    )


def compute_coverage(
    table: pa.Table,
    expected_calendar: ExpectedCalendar | str,
) -> CoverageReport:
    """Calculate temporal coverage by period and identify coverage gaps."""
    cadence = (
        expected_calendar.value
        if isinstance(expected_calendar, ExpectedCalendar)
        else str(expected_calendar).lower()
    )

    if len(table) == 0:
        return CoverageReport(
            expected_calendar=cadence,
            total_periods=0,
            gaps_count=0,
            periods=[],
        )

    if cadence == "none":
        return CoverageReport(
            expected_calendar=cadence,
            total_periods=1,
            gaps_count=0,
            periods=[CoveragePeriod(period="all", row_count=len(table), is_gap=False)],
        )

    con = duckdb.connect()
    con.register("tbl", table)

    # SQL period extraction expression
    if cadence == "daily":
        expr = "strftime(event_time, '%Y-%m-%d')"
    elif cadence == "weekly":
        expr = "strftime(event_time, '%Y-W%W')"
    elif cadence == "monthly":
        expr = "strftime(event_time, '%Y-%m')"
    elif cadence == "quarterly":
        expr = "strftime(event_time, '%Y') || '-Q' || ceil(month(event_time) / 3.0)::INT"
    else:
        expr = "'all'"

    query = f"""
    SELECT
        {expr} AS period,
        count(*) AS cnt,
        min(event_time) AS min_t,
        max(event_time) AS max_t
    FROM tbl
    GROUP BY period
    ORDER BY min_t
    """
    res = con.sql(query).to_arrow_table().to_pylist()
    con.close()

    periods: list[CoveragePeriod] = []
    gaps_count = 0

    for r in res:
        cnt = int(r["cnt"])
        is_gap = cnt == 0
        if is_gap:
            gaps_count += 1
        periods.append(CoveragePeriod(period=str(r["period"]), row_count=cnt, is_gap=is_gap))

    return CoverageReport(
        expected_calendar=cadence,
        total_periods=len(periods),
        gaps_count=gaps_count,
        periods=periods,
    )


def compute_drop_summary(
    table: pa.Table,
    discovery_result: DropMetadata | None = None,
) -> DropSummary:
    """Compute summary metrics over canonical table and file manifest."""
    files: list[str] = []
    total_bytes = 0

    if discovery_result is not None:
        files = [f.relative_path for f in discovery_result.files]
        total_bytes = discovery_result.total_bytes
    else:
        total_bytes = table.nbytes

    if len(table) == 0:
        return DropSummary(
            files=files,
            total_bytes=total_bytes,
            total_rows=0,
            distinct_entities=0,
            distinct_fields=0,
        )

    con = duckdb.connect()
    con.register("tbl", table)
    query = """
    SELECT
        count(*) AS total_rows,
        count(DISTINCT entity_id) AS distinct_entities,
        count(DISTINCT field) AS distinct_fields,
        strftime(min(event_time), '%Y-%m-%d %H:%M:%S UTC') AS event_min,
        strftime(max(event_time), '%Y-%m-%d %H:%M:%S UTC') AS event_max,
        strftime(min(knowledge_time), '%Y-%m-%d %H:%M:%S UTC') AS knowledge_min,
        strftime(max(knowledge_time), '%Y-%m-%d %H:%M:%S UTC') AS knowledge_max
    FROM tbl
    """
    row = con.sql(query).to_arrow_table().to_pylist()[0]
    con.close()

    return DropSummary(
        files=files,
        total_bytes=total_bytes,
        total_rows=int(row["total_rows"]),
        distinct_entities=int(row["distinct_entities"]),
        distinct_fields=int(row["distinct_fields"]),
        event_time_min=row["event_min"],
        event_time_max=row["event_max"],
        knowledge_time_min=row["knowledge_min"],
        knowledge_time_max=row["knowledge_max"],
    )


def compute_identifiers_report(table: pa.Table) -> IdentifiersReport:
    """Evaluate identifier resolution metrics and count unmapped entries."""
    if len(table) == 0:
        return IdentifiersReport()

    con = duckdb.connect()
    con.register("tbl", table)

    # Check unmapped column existence
    if "unmapped" in table.column_names:
        stats = (
            con.sql("""
        SELECT
            count(*) AS total,
            sum(CASE WHEN unmapped THEN 1 ELSE 0 END) AS unmapped_count,
            sum(CASE WHEN NOT unmapped THEN 1 ELSE 0 END) AS mapped_count
        FROM tbl
        """)
            .to_arrow_table()
            .to_pylist()[0]
        )

        total = int(stats["total"])
        unmapped = int(stats["unmapped_count"] or 0)
        mapped = int(stats["mapped_count"] or 0)
        mapped_share = mapped / total if total > 0 else 1.0

        top_unmapped_rows = (
            con.sql("""
        SELECT entity_id AS id, count(*) AS count
        FROM tbl
        WHERE unmapped
        GROUP BY entity_id
        ORDER BY count DESC
        LIMIT 10
        """)
            .to_arrow_table()
            .to_pylist()
        )
    else:
        total = len(table)
        mapped = total
        unmapped = 0
        mapped_share = 1.0
        top_unmapped_rows = []

    con.close()

    return IdentifiersReport(
        total_rows=total,
        mapped_count=mapped,
        unmapped_count=unmapped,
        mapped_share=mapped_share,
        top_unmapped=[{"id": str(r["id"]), "count": int(r["count"])} for r in top_unmapped_rows],
    )


def generate_vendor_inquiries(
    validation_report: ValidationReport,
    coverage_report: CoverageReport,
    identifiers_report: IdentifiersReport,
    revisions_report: RevisionsReport,
    schema_report: SchemaReport,
) -> list[str]:
    """Generate deterministic vendor inquiries from failed rules and anomalies."""
    inquiries: list[str] = []

    # 1. Schema drift inquiries
    for diff in schema_report.differences:
        cat = diff.category
        col = diff.column
        sev = diff.severity
        if sev == "hold" or cat in ("removed_column", "type_widened"):
            inquiries.append(
                f"Schema contract breach: Column '{col}' was {cat.replace('_', ' ')}. "
                "Please provide advance notice and migration documentation before altering "
                "structural schema contracts."
            )
        elif cat == "added_column":
            inquiries.append(
                f"Schema extension detected: Undocumented column '{col}' was added. "
                "Please confirm authoritative specification and semantic definition for this field."
            )

    # 2. Validation rule failures
    for rule_name, count in validation_report.rule_counts.items():
        if count <= 0:
            continue
        if "unit" in rule_name:
            inquiries.append(
                f"Column 'unit' contains {count:,} observations with undocumented or non-standard "
                "units of measure. "
                "Which unit representation is authoritative for downstream calculations?"
            )
        elif "range" in rule_name:
            inquiries.append(
                f"Rule '{rule_name}' flagged {count:,} observations outside valid economic "
                "boundaries. "
                "Please verify whether these values reflect unusual market conditions or "
                "feed corruption."
            )
        elif "type_flip" in rule_name:
            inquiries.append(
                f"Rule '{rule_name}' flagged {count:,} non-numeric string values in numeric "
                "metric columns. "
                "Please confirm field data type specification."
            )
        elif "duplicate" in rule_name:
            if "threshold" in rule_name or "share" in rule_name:
                inquiries.append(
                    "Batch-level duplicate key threshold breached. "
                    "Please investigate upstream ingestion deduplication failures."
                )
            else:
                inquiries.append(
                    f"Batch contains {count:,} duplicate observation keys within the same "
                    "transmission drop. "
                    "Which record is authoritative?"
                )
        elif "volume" in rule_name or "row_count" in rule_name:
            inquiries.append(
                "Batch volume stability alert: Row count deviates from trailing median. "
                "Was this transmission volume shift intentional?"
            )
        elif "byte_identical" in rule_name:
            inquiries.append(
                "Batch payload is byte-identical to previous drop transmission. "
                "Please verify if redundant drop transmission occurred."
            )

    # 3. Coverage gaps
    if coverage_report.gaps_count > 0:
        gap_periods = [p.period for p in coverage_report.periods if p.is_gap]
        periods_str = ", ".join(gap_periods[:5])
        if len(gap_periods) > 5:
            periods_str += f" (and {len(gap_periods) - 5} more)"
        inquiries.append(
            f"Expected {coverage_report.expected_calendar} coverage shows zero observations "
            f"during period(s): {periods_str}. "
            "Did the data feed drop these records in transmission?"
        )

    # 4. Unmapped identifiers
    if identifiers_report.unmapped_count > 0:
        top_samples = ", ".join([str(item["id"]) for item in identifiers_report.top_unmapped[:5]])
        inquiries.append(
            f"Feed contains {identifiers_report.unmapped_count:,} unmapped entity identifiers "
            f"(top unmapped: {top_samples}). "
            "Please provide corresponding CIK or ticker cross-reference mappings."
        )

    # 5. Silent overwrites (zero revisions across > 3 drops)
    if revisions_report.zero_revision_fields:
        fields_str = ", ".join(revisions_report.zero_revision_fields[:5])
        drops_cnt = revisions_report.consecutive_drops_without_revisions
        inquiries.append(
            f"Zero historical revisions observed across {drops_cnt} consecutive drops "
            f"for field(s): {fields_str}. Please confirm whether historical restatements are "
            "silently overwritten rather than versioned."
        )

    # 6. Excessive restatements
    for field, rate in revisions_report.rate_per_field.items():
        if rate > 0.05:
            inquiries.append(
                f"Field '{field}' observed a restatement rate of {rate:.1%}. "
                "Please confirm if an official corporate restatement was filed by "
                "the underlying entities."
            )

    return inquiries


def compile_readiness_report(
    vendor: str,
    drop_id: str,
    discovery_result: DropMetadata | None,
    schema_diff: SchemaDiff | None,
    manifest: VendorManifest,
    canonical_table: pa.Table,
    validation_result: ValidationResult,
    revision_summary: RevisionSummary | None = None,
    timings: dict[str, float] | None = None,
    peak_memory_mb: float = 0.0,
    store_drops_count: int = 1,
) -> ReadinessReport:
    """Compile all stage metrics into a unified ReadinessReport."""
    # 1. Drop summary
    drop_summary = compute_drop_summary(canonical_table, discovery_result)

    # 2. Schema report
    diff_items: list[SchemaDifferenceItem] = []
    proposed_changes: list[str] = []
    if schema_diff is not None:
        for d in schema_diff.differences:
            diff_items.append(
                SchemaDifferenceItem(
                    column=d.column,
                    category=d.category.value,
                    severity=d.severity.value,
                    proposed_change=d.proposed_change,
                )
            )
            proposed_changes.append(d.proposed_change)
    schema_report = SchemaReport(
        pinned_version=1,
        has_differences=len(diff_items) > 0,
        differences=diff_items,
        proposed_changes=proposed_changes,
    )

    # 3. Coverage report
    coverage_report = compute_coverage(canonical_table, manifest.expected_calendar)

    # 4. Validation report
    top_reasons: list[dict[str, Any]] = []
    for r in validation_result.top_reasons:
        top_reasons.append(
            {
                "reason": r.get("reason", "unknown"),
                "count": int(r.get("count", 0)),
                "severity": r.get("severity", "quarantine"),
            }
        )
    val_report = ValidationReport(
        total_evaluated=validation_result.total_evaluated,
        valid_count=validation_result.valid_count,
        quarantine_count=validation_result.quarantine_count,
        quarantine_share=validation_result.quarantine_share,
        warn_count=validation_result.warn_count,
        held=validation_result.held,
        held_reasons=validation_result.held_reasons,
        rule_counts=validation_result.rule_counts,
        severity_counts=validation_result.severity_counts,
        top_reasons=top_reasons,
    )

    # 5. Identifiers report
    id_report = compute_identifiers_report(canonical_table)

    # 6. Revisions report
    largest_changes: list[TopRevision] = []
    rate_per_field: dict[str, float] = {}
    zero_revision_fields: list[str] = []
    total_revs = 0

    if revision_summary is not None:
        total_revs = revision_summary.total_revisions
        rate_per_field = revision_summary.rate_per_field
        for item in revision_summary.largest_changes[:10]:
            largest_changes.append(
                TopRevision(
                    entity_id=item.get("entity_id", ""),
                    field=item.get("field", ""),
                    event_time=str(item.get("event_time", "")),
                    old_value=str(item.get("old_value", "")),
                    new_value=str(item.get("new_value", "")),
                    relative_change=item.get("relative_change"),
                    old_knowledge_time=str(item.get("old_knowledge_time", "")),
                    new_knowledge_time=str(item.get("new_knowledge_time", "")),
                )
            )

    # Check zero revisions across > 3 drops
    if store_drops_count > 3:
        for f, rate in rate_per_field.items():
            if rate == 0.0:
                zero_revision_fields.append(f)

    rev_report = RevisionsReport(
        total_revisions=total_revs,
        revised_entities_count=len(set(r.entity_id for r in largest_changes)),
        rate_per_field=rate_per_field,
        largest_changes=largest_changes,
        zero_revision_fields=zero_revision_fields,
        consecutive_drops_without_revisions=store_drops_count if zero_revision_fields else 0,
        silent_overwrite_warning=(
            f"Zero historical revisions observed across {store_drops_count} drops."
            if zero_revision_fields
            else None
        ),
    )

    # 7. Verdict
    verdict, verdict_reason = compute_verdict(
        held=val_report.held,
        held_reasons=val_report.held_reasons,
        quarantine_count=val_report.quarantine_count,
        warn_count=val_report.warn_count,
        unmapped_count=id_report.unmapped_count,
        schema_differences=schema_report.differences,
    )

    # 8. Vendor Inquiries
    inquiries = generate_vendor_inquiries(
        validation_report=val_report,
        coverage_report=coverage_report,
        identifiers_report=id_report,
        revisions_report=rev_report,
        schema_report=schema_report,
    )

    # 9. Timings
    stage_timings = timings or {}
    total_sec = stage_timings.get("total", sum(stage_timings.values()))
    if peak_memory_mb <= 0:
        peak_rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        peak_memory_mb = peak_rss_kb / 1024.0

    timings_report = TimingsReport(
        timings_per_stage_seconds=stage_timings,
        total_seconds=total_sec,
        peak_memory_mb=peak_memory_mb,
    )

    now_utc = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    return ReadinessReport(
        vendor=vendor,
        drop_id=drop_id,
        verdict=verdict,
        verdict_reason=verdict_reason,
        generated_at=now_utc,
        drop_summary=drop_summary,
        schema_report=schema_report,
        coverage_report=coverage_report,
        validation_report=val_report,
        identifiers_report=id_report,
        revisions_report=rev_report,
        vendor_inquiries=inquiries,
        timings=timings_report,
    )


def render_markdown_report(r: ReadinessReport) -> str:
    """Format a ReadinessReport into standard GitHub Flavored Markdown."""
    lines: list[str] = []

    lines.append(f"# Vendor Readiness Report: {r.vendor}")
    lines.append("")
    lines.append(f"- **Drop Identifier:** `{r.drop_id}`")
    lines.append(f"- **Generated At:** {r.generated_at}")
    lines.append("")

    # Section 1: Verdict
    lines.append("## 1. Verdict")
    lines.append("")
    lines.append(f"**Verdict:** `{r.verdict.value}`")
    lines.append("")
    lines.append(f"> **Reason:** {r.verdict_reason}")
    lines.append("")

    # Section 2: Drop Summary
    lines.append("## 2. Drop Summary")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|---|---|")
    files_str = ", ".join(r.drop_summary.files) if r.drop_summary.files else "In-memory table"
    lines.append(f"| Files | {len(r.drop_summary.files)} (`{files_str}`) |")
    lines.append(f"| Raw Payload Size | {r.drop_summary.total_bytes:,} bytes |")
    lines.append(f"| Total Records | {r.drop_summary.total_rows:,} |")
    lines.append(f"| Distinct Entities | {r.drop_summary.distinct_entities:,} |")
    lines.append(f"| Distinct Fields | {r.drop_summary.distinct_fields:,} |")
    ev_min = r.drop_summary.event_time_min or "N/A"
    ev_max = r.drop_summary.event_time_max or "N/A"
    lines.append(f"| Event Time Range | {ev_min} to {ev_max} |")
    kt_min = r.drop_summary.knowledge_time_min or "N/A"
    kt_max = r.drop_summary.knowledge_time_max or "N/A"
    lines.append(f"| Knowledge Time Range | {kt_min} to {kt_max} |")
    lines.append("")

    # Section 3: Schema
    lines.append("## 3. Schema")
    lines.append("")
    lines.append(f"- **Pinned Schema Version:** {r.schema_report.pinned_version or 'N/A'}")
    if r.schema_report.has_differences:
        lines.append("- **Status:** Schema contract differences detected.")
        lines.append("")
        lines.append("| Column | Category | Severity | Proposed Action |")
        lines.append("|---|---|---|---|")
        for diff in r.schema_report.differences:
            sev_str = diff.severity.upper()
            lines.append(
                f"| `{diff.column}` | {diff.category} | `{sev_str}` | {diff.proposed_change} |"
            )
        lines.append("")
        lines.append(
            "Run `vdok schema accept <vendor>` to acknowledge and version non-breaking "
            "contract changes."
        )
    else:
        lines.append(
            "- **Status:** COMPLIANT (0 schema differences observed against pinned contract)."
        )
    lines.append("")

    # Section 4: Coverage by Period
    lines.append("## 4. Coverage by Period")
    lines.append("")
    lines.append(f"- **Expected Calendar Cadence:** `{r.coverage_report.expected_calendar}`")
    lines.append(f"- **Total Reporting Periods:** {r.coverage_report.total_periods}")
    lines.append(f"- **Coverage Gaps:** {r.coverage_report.gaps_count}")
    lines.append("")
    if r.coverage_report.periods:
        lines.append("| Period | Observations | Status |")
        lines.append("|---|---|---|")
        for p in r.coverage_report.periods[:15]:
            status = "GAP (0 rows)" if p.is_gap else "OK"
            lines.append(f"| {p.period} | {p.row_count:,} | {status} |")
        if len(r.coverage_report.periods) > 15:
            lines.append(f"| ... | ({len(r.coverage_report.periods) - 15} more periods) | ... |")
    else:
        lines.append("No temporal coverage periods recorded.")
    lines.append("")

    # Section 5: Validation
    lines.append("## 5. Validation")
    lines.append("")
    lines.append("| Metric | Count | Ratio |")
    lines.append("|---|---|---|")
    lines.append(f"| Total Evaluated Rows | {r.validation_report.total_evaluated:,} | 100.0% |")
    val_cnt = r.validation_report.valid_count
    tot_eval = max(1, r.validation_report.total_evaluated)
    valid_ratio = val_cnt / tot_eval
    lines.append(f"| Conforming Valid Rows | {val_cnt:,} | {valid_ratio:.2%} |")
    q_cnt = r.validation_report.quarantine_count
    q_share = r.validation_report.quarantine_share
    lines.append(f"| Quarantined Rows | {q_cnt:,} | {q_share:.2%} |")
    lines.append(f"| Warning Tier Flags | {r.validation_report.warn_count:,} | - |")
    hold_str = "HELD" if r.validation_report.held else "PASSED"
    lines.append(f"| Batch Hold Status | `{hold_str}` | - |")
    lines.append("")

    if r.validation_report.top_reasons:
        lines.append("### Top Validation Quarantine Causes")
        lines.append("")
        lines.append("| Rule / Reason | Severity | Occurrences |")
        lines.append("|---|---|---|")
        for item in r.validation_report.top_reasons[:10]:
            r_reas = item.get("reason")
            r_sev = str(item.get("severity")).upper()
            r_cnt = int(item.get("count", 0))
            lines.append(f"| `{r_reas}` | {r_sev} | {r_cnt:,} |")
        lines.append("")

    # Section 6: Identifiers
    lines.append("## 6. Identifiers")
    lines.append("")
    lines.append("| Metric | Count | Ratio |")
    lines.append("|---|---|---|")
    lines.append(f"| Total Evaluated Rows | {r.identifiers_report.total_rows:,} | 100.0% |")
    m_cnt = r.identifiers_report.mapped_count
    m_share = r.identifiers_report.mapped_share
    lines.append(f"| Resolved Identifiers | {m_cnt:,} | {m_share:.2%} |")
    u_cnt = r.identifiers_report.unmapped_count
    u_share = 1.0 - m_share
    lines.append(f"| Unmapped Identifiers | {u_cnt:,} | {u_share:.2%} |")
    lines.append("")

    if r.identifiers_report.top_unmapped:
        lines.append("### Top Unresolved Identifiers")
        lines.append("")
        lines.append("| Raw Identifier | Count |")
        lines.append("|---|---|")
        for item in r.identifiers_report.top_unmapped[:10]:
            lines.append(f"| `{item.get('id')}` | {int(item.get('count', 0)):,} |")
        lines.append("")

    # Section 7: Revisions
    lines.append("## 7. Revisions")
    lines.append("")
    lines.append(f"- **Total Historical Revisions:** {r.revisions_report.total_revisions:,}")
    lines.append(
        f"- **Entities with Restated Values:** {r.revisions_report.revised_entities_count:,}"
    )
    lines.append("")

    if r.revisions_report.rate_per_field:
        lines.append("### Restatement Rate per Field")
        lines.append("")
        lines.append("| Field | Restatement Rate |")
        lines.append("|---|---|")
        for field, rate in sorted(
            r.revisions_report.rate_per_field.items(), key=lambda x: (-x[1], x[0])
        )[:10]:
            lines.append(f"| `{field}` | {rate:.2%} |")
        lines.append("")

    if r.revisions_report.largest_changes:
        lines.append("### Largest Observed Revisions")
        lines.append("")
        lines.append(
            "| Entity ID | Field | Event Time | Prior Value | Revised Value | Relative Change |"
        )
        lines.append("|---|---|---|---|---|---|")
        for rev in r.revisions_report.largest_changes[:10]:
            rel_str = f"{rev.relative_change:.2%}" if rev.relative_change is not None else "N/A"
            cols = [
                f"`{rev.entity_id}`",
                f"`{rev.field}`",
                str(rev.event_time),
                str(rev.old_value),
                str(rev.new_value),
                rel_str,
            ]
            lines.append(f"| {' | '.join(cols)} |")
        lines.append("")

    if r.revisions_report.silent_overwrite_warning:
        lines.append(f"> **Warning:** {r.revisions_report.silent_overwrite_warning}")
        lines.append("")

    # Section 8: Questions for the Vendor
    lines.append("## 8. Questions for the Vendor")
    lines.append("")
    if r.vendor_inquiries:
        for idx, q in enumerate(r.vendor_inquiries, start=1):
            lines.append(f"{idx}. {q}")
    else:
        lines.append("Zero vendor inquiries required. Drop meets all data quality criteria.")
    lines.append("")

    # Section 9: Timings and Resource Profile
    lines.append("## 9. Performance & Resource Profile")
    lines.append("")
    lines.append("| Stage | Duration |")
    lines.append("|---|---|")
    for stage, sec in r.timings.timings_per_stage_seconds.items():
        if stage != "total":
            lines.append(f"| {stage.replace('_', ' ').capitalize()} | {sec:.3f} s |")
    lines.append(f"| **Total Wall Clock** | **{r.timings.total_seconds:.3f} s** |")
    lines.append("")
    lines.append(f"- **Peak Resident Memory (RSS):** {r.timings.peak_memory_mb:.2f} MB")
    lines.append("")

    return "\n".join(lines)


def render_diff_markdown(d: DropDiffResult) -> str:
    """Render analytical drop diff results into GitHub Flavored Markdown."""
    lines: list[str] = []

    lines.append(f"# Drop Diff: {d.vendor}")
    lines.append("")
    lines.append(f"- **Baseline Drop (A):** `{d.drop_a}`")
    lines.append(f"- **Target Drop (B):** `{d.drop_b}`")
    lines.append("")

    lines.append("## 1. Key Evolution Summary")
    lines.append("")
    lines.append("| Metric | Count | Share |")
    lines.append("|---|---|---|")
    lines.append(f"| Keys in Drop A | {d.total_keys_a:,} | - |")
    lines.append(f"| Keys in Drop B | {d.total_keys_b:,} | - |")
    lines.append(f"| Common Observation Keys | {d.common_keys:,} | 100.0% of overlap |")
    lines.append(f"| Added Keys (in B, not in A) | {d.added_keys:,} | - |")
    lines.append(f"| Removed Keys (in A, not in B) | {d.removed_keys:,} | - |")
    lines.append(
        f"| Changed Values (Restatements) | {d.changed_values:,} | {d.restatement_rate:.2%} |"
    )
    lines.append(
        f"| Unchanged Values | {d.unchanged_values:,} | {(1.0 - d.restatement_rate):.2%} |"
    )
    lines.append("")

    if d.schema_differences:
        lines.append("## 2. Schema Evolution")
        lines.append("")
        for s_diff in d.schema_differences:
            lines.append(f"- {s_diff}")
        lines.append("")
    else:
        lines.append("## 2. Schema Evolution")
        lines.append("")
        lines.append("No structural schema differences between drops.")
        lines.append("")

    if d.top_changed_fields:
        lines.append("## 3. Top Restated Fields")
        lines.append("")
        lines.append("| Field | Restatement Count |")
        lines.append("|---|---|")
        for f_item in d.top_changed_fields:
            lines.append(f"| `{f_item['field']}` | {f_item['count']:,} |")
        lines.append("")

    if d.largest_changes:
        lines.append("## 4. Largest Relative Disclosures")
        lines.append("")
        lines.append(
            "| Entity ID | Field | Event Time | Drop A Value | Drop B Value | Relative Change |"
        )
        lines.append("|---|---|---|---|---|---|")
        for item in d.largest_changes:
            rel_str = (
                f"{item['relative_change']:.2%}"
                if item.get("relative_change") is not None
                else "N/A"
            )
            cols = [
                f"`{item['entity_id']}`",
                f"`{item['field']}`",
                str(item["event_time"]),
                str(item["old_val"]),
                str(item["new_val"]),
                rel_str,
            ]
            lines.append(f"| {' | '.join(cols)} |")
        lines.append("")

    return "\n".join(lines)


def diff_drops(
    store_dir: Path | str,
    vendor: str,
    drop_a: str,
    drop_b: str,
) -> DropDiffResult:
    """Compare two vendor drops using DuckDB views over Parquet partition trees."""
    store_path = Path(store_dir)
    vendor_dir = store_path / vendor

    # Locate parquet files for drop_a and drop_b
    files_a = list(vendor_dir.glob(f"*/*/part-{drop_a}.parquet"))
    files_b = list(vendor_dir.glob(f"*/*/part-{drop_b}.parquet"))

    if not files_a:
        raise FileNotFoundError(
            f"No store partition files found for drop '{drop_a}' under {vendor_dir}"
        )
    if not files_b:
        raise FileNotFoundError(
            f"No store partition files found for drop '{drop_b}' under {vendor_dir}"
        )

    con = duckdb.connect()

    # Create temporary views
    globs_a = [str(f) for f in files_a]
    globs_b = [str(f) for f in files_b]

    con.sql(f"CREATE TEMP VIEW drop_a AS SELECT * FROM read_parquet({globs_a})")
    con.sql(f"CREATE TEMP VIEW drop_b AS SELECT * FROM read_parquet({globs_b})")

    # Counts
    cnt_a = con.sql("SELECT count(*) FROM drop_a").fetchall()[0][0]
    cnt_b = con.sql("SELECT count(*) FROM drop_b").fetchall()[0][0]

    # Added, removed, common keys
    added = con.sql("""
    SELECT count(*) FROM (
        SELECT entity_id, field, event_time FROM drop_b
        EXCEPT
        SELECT entity_id, field, event_time FROM drop_a
    )
    """).fetchall()[0][0]

    removed = con.sql("""
    SELECT count(*) FROM (
        SELECT entity_id, field, event_time FROM drop_a
        EXCEPT
        SELECT entity_id, field, event_time FROM drop_b
    )
    """).fetchall()[0][0]

    common = con.sql("""
    SELECT count(*) FROM (
        SELECT entity_id, field, event_time FROM drop_a
        INTERSECT
        SELECT entity_id, field, event_time FROM drop_b
    )
    """).fetchall()[0][0]

    # Changed vs unchanged on common keys
    diff_query = """
    SELECT
        a.entity_id,
        a.field,
        strftime(a.event_time, '%Y-%m-%d %H:%M:%S UTC') AS event_time,
        coalesce(a.value_num::VARCHAR, a.value_str) AS old_val,
        coalesce(b.value_num::VARCHAR, b.value_str) AS new_val,
        CASE
            WHEN a.value_num IS NOT NULL AND b.value_num IS NOT NULL AND a.value_num != 0
            THEN abs(b.value_num - a.value_num) / abs(a.value_num)
            ELSE NULL
        END AS relative_change
    FROM drop_a a
    JOIN drop_b b
      ON a.entity_id = b.entity_id
     AND a.field = b.field
     AND a.event_time = b.event_time
    WHERE a.value_num IS DISTINCT FROM b.value_num
       OR a.value_str IS DISTINCT FROM b.value_str
       OR a.unit IS DISTINCT FROM b.unit
    ORDER BY relative_change DESC NULLS LAST
    """
    diff_rows = con.sql(diff_query).to_arrow_table().to_pylist()

    changed_cnt = len(diff_rows)
    unchanged_cnt = max(0, common - changed_cnt)
    restate_rate = changed_cnt / common if common > 0 else 0.0

    # Top changed fields
    top_fields_query = """
    SELECT
        a.field,
        count(*) AS cnt
    FROM drop_a a
    JOIN drop_b b
      ON a.entity_id = b.entity_id
     AND a.field = b.field
     AND a.event_time = b.event_time
    WHERE a.value_num IS DISTINCT FROM b.value_num
       OR a.value_str IS DISTINCT FROM b.value_str
       OR a.unit IS DISTINCT FROM b.unit
    GROUP BY a.field
    ORDER BY cnt DESC
    LIMIT 10
    """
    top_fields = con.sql(top_fields_query).to_arrow_table().to_pylist()

    con.close()

    top_changed_fields = [{"field": str(r["field"]), "count": int(r["cnt"])} for r in top_fields]
    largest_changes = [
        {
            "entity_id": str(r["entity_id"]),
            "field": str(r["field"]),
            "event_time": str(r["event_time"]),
            "old_val": str(r["old_val"]),
            "new_val": str(r["new_val"]),
            "relative_change": float(r["relative_change"])
            if r["relative_change"] is not None
            else None,
        }
        for r in diff_rows[:10]
    ]

    return DropDiffResult(
        vendor=vendor,
        drop_a=drop_a,
        drop_b=drop_b,
        total_keys_a=int(cnt_a),
        total_keys_b=int(cnt_b),
        common_keys=int(common),
        added_keys=int(added),
        removed_keys=int(removed),
        changed_values=int(changed_cnt),
        unchanged_values=int(unchanged_cnt),
        restatement_rate=float(restate_rate),
        schema_differences=[],
        top_changed_fields=top_changed_fields,
        largest_changes=largest_changes,
    )


def load_report(
    vendor: str,
    drop_id: str,
    base_dir: Path | str = "data/results",
) -> ReadinessReport | None:
    """Load previously saved report from data/results/<vendor>/<drop_id>/report.json."""
    json_path = Path(base_dir) / vendor / drop_id / "report.json"
    if not json_path.exists():
        return None
    content = json_path.read_text(encoding="utf-8")
    return ReadinessReport.model_validate_json(content)
