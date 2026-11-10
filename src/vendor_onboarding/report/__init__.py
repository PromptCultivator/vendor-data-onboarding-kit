"""Readiness Report and Coverage Matrix subsystem."""

from __future__ import annotations

from vendor_onboarding.report.coverage import (
    CoverageAnalyzer,
    CoverageDipAlert,
    CoverageGapAlert,
    DailyCoverageMatrix,
    build_coverage_matrix,
    detect_coverage_dips,
    detect_coverage_gaps,
)
from vendor_onboarding.report.generator import (
    PipelineRunContext,
    ReadinessReportData,
    ReadinessReporter,
    ReadinessReportGenerator,
    export_reports,
)
from vendor_onboarding.report.questions import (
    VendorQuestionsGenerator,
    generate_vendor_inquiries,
)

__all__ = [
    "CoverageAnalyzer",
    "CoverageDipAlert",
    "CoverageGapAlert",
    "DailyCoverageMatrix",
    "PipelineRunContext",
    "ReadinessReportData",
    "ReadinessReportGenerator",
    "ReadinessReporter",
    "VendorQuestionsGenerator",
    "build_coverage_matrix",
    "detect_coverage_dips",
    "detect_coverage_gaps",
    "export_reports",
    "generate_vendor_inquiries",
]
