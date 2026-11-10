"""Automated Vendor Questions Generator (F-REP-03)."""

from __future__ import annotations

from typing import Any

from vendor_onboarding.report.coverage import CoverageDipAlert


class VendorQuestionsGenerator:
    """Generates structured email inquiry drafts targeting observed anomalies."""

    def generate_inquiries(
        self,
        validation_results: list[Any] | None = None,
        coverage_dips: list[CoverageDipAlert] | None = None,
        drift_report_changes: list[Any] | None = None,
        diff_summary: Any | None = None,
        quarantine_records: list[Any] | None = None,
        drop_id: str = "DROP_001",
    ) -> list[str]:
        """Translates validation failures, coverage dips, and drift into vendor inquiries."""
        inquiries: list[str] = []

        # 1. Validation Rule Failures
        if validation_results:
            for vr in validation_results:
                if getattr(vr, "passed", True):
                    continue

                check = getattr(vr, "check_name", "unknown")
                v_count = getattr(vr, "violation_count", 0)
                v_rate = getattr(vr, "violation_rate", 0.0)
                v_thresh = getattr(vr, "threshold", 0.0)
                samples = getattr(vr, "violations_sample", []) or []

                # Extract affected symbols/entities if present in sample
                affected_symbols = set()
                for s in samples:
                    if isinstance(s, dict):
                        sym = s.get("source_symbol") or s.get("entity_id")
                        if sym:
                            affected_symbols.add(str(sym))
                sym_str = (
                    f" affecting symbols: {', '.join(sorted(affected_symbols))}"
                    if affected_symbols
                    else ""
                )

                if check in ("range_domain", "range_and_domain"):
                    inquiries.append(
                        f"Subject: Data Quality Inquiry - Out-of-Range / Negative Prices Detected\n"
                        f"We detected {v_count} records with negative or invalid values{sym_str}. "
                        f"Could you confirm whether these are raw market anomalies or adjustments?"
                    )
                elif check == "completeness":
                    inquiries.append(
                        f"Subject: SLA Violation - Null Values in Mandatory Fields\n"
                        f"Found {v_count} records ({v_rate:.2%}) with missing mandatory fields, "
                        f"exceeding the agreed SLA threshold of {v_thresh:.2%}. "
                        f"Please provide corrected backfills."
                    )
                elif check == "future_timestamps":
                    inquiries.append(
                        f"Subject: Data Quality Inquiry - Future Timestamps Detected\n"
                        f"Found {v_count} records with effective timestamps in the future relative to "
                        f"knowledge time. Please verify feed clock synchronization."
                    )
                elif check in ("monotone_timestamps", "monotone"):
                    inquiries.append(
                        f"Subject: Data Quality Inquiry - Timestamp Inversion / Clock Skew\n"
                        f"Found {v_count} records with backwards timestamp sequences for active entities. "
                        f"Please investigate potential feed clock skew or out-of-order delivery."
                    )
                elif check == "intra_drop_duplicates":
                    inquiries.append(
                        f"Subject: Data Quality Inquiry - Conflicting Intra-Drop Duplicates Detected\n"
                        f"Found {v_count} conflicting duplicate records in the incoming drop{sym_str}. "
                        f"Please verify feed deduplication before dispatch."
                    )
                elif check == "universe_coverage":
                    inquiries.append(
                        f"Subject: Universe Coverage SLA Violation - Entity Breadth Drop\n"
                        f"Found universe coverage violation with {v_count} missing expected entities "
                        f"(observed coverage {1.0 - v_rate:.2%}, SLA threshold {v_thresh:.2%}). "
                        f"Please clarify if there was an unscheduled market closure or ticker delisting."
                    )
                elif check == "positive_values":
                    inquiries.append(
                        f"Subject: Data Quality Inquiry - Non-Positive Values in Required Positive Fields\n"
                        f"Found {v_count} non-positive values in fields requiring strictly positive observations{sym_str}. "
                        f"Please confirm if these reflect exceptional corporate events or data errors."
                    )
                else:
                    inquiries.append(
                        f"Subject: Data Quality Inquiry - Rule Violation: {check}\n"
                        f"Validation rule '{check}' failed with {v_count} violations ({v_rate:.2%}), "
                        f"exceeding threshold of {v_thresh:.2%}. Please review and provide remediation."
                    )

        # 2. Coverage Dips (>20%)
        if coverage_dips:
            for dip in coverage_dips:
                inquiries.append(
                    f"Subject: Universe Coverage Alert - Missing Entities on {dip.date}\n"
                    f"Active entity coverage dropped by {dip.dip_percentage:.1f}% on {dip.date} "
                    f"({dip.baseline_count} -> {dip.current_count} symbols). "
                    f"Please verify if a market holiday or feed outage occurred."
                )

        # 3. Schema Drift Changes
        if drift_report_changes:
            changes_list: list[Any] = []
            if hasattr(drift_report_changes, "changes"):
                changes_list = drift_report_changes.changes
            elif isinstance(drift_report_changes, list):
                changes_list = drift_report_changes
            elif drift_report_changes is not None:
                changes_list = [drift_report_changes]

            if changes_list:
                inquiries.append(
                    f"Subject: Schema Contract Drift Notification\n"
                    f"Detected {len(changes_list)} unauthorized schema changes against pinned contract. "
                    f"Please provide an updated data dictionary."
                )

        # 4. High Revision Velocity (DiffSummary)
        if diff_summary is not None:
            r_cnt = getattr(diff_summary, "restated_records", 0)
            if r_cnt > 0:
                b_id = getattr(diff_summary, "drop_id_baseline", "baseline")
                c_id = getattr(diff_summary, "drop_id_current", "current")
                inquiries.append(
                    f"Subject: Revision Velocity Inquiry - High Restatement Volume Detected\n"
                    f"Diff comparison between {b_id} and {c_id} detected {r_cnt} restated records. "
                    f"Please provide background on the cause of these revisions and whether further restatements are anticipated."
                )

        # 5. Quarantined Unmapped Symbols
        if quarantine_records:
            by_sym: dict[str, list[Any]] = {}
            for q in quarantine_records:
                sym = getattr(q, "raw_symbol", None) or (
                    q.get("raw_symbol") if isinstance(q, dict) else str(q)
                )
                by_sym.setdefault(str(sym), []).append(q)

            for sym, q_list in sorted(by_sym.items()):
                first_q = q_list[0]
                vendor = (
                    getattr(first_q, "vendor_id", "Vendor")
                    if hasattr(first_q, "vendor_id")
                    else "Vendor"
                )
                syn_id = (
                    getattr(first_q, "synthetic_id", f"UNRESOLVED_{vendor}_{sym}")
                    if hasattr(first_q, "synthetic_id")
                    else f"UNRESOLVED_{vendor}_{sym}"
                )
                cnt = len(q_list)
                inquiries.append(
                    f"Subject: [Action Required] Unmapped Identifier '{sym}' in {drop_id}\n\n"
                    f"Dear {vendor} Support Team,\n\n"
                    f"During automated ingestion of drop '{drop_id}', the system encountered {cnt} records "
                    f"referencing unmapped symbol '{sym}'.\n"
                    f"Our security master has quarantined these records under synthetic identifier: {syn_id}.\n\n"
                    f"Please provide the corresponding authoritative cross-identifiers (CIK, CUSIP, ISIN, or FIGI) "
                    f"so we may link this entity to our canonical master.\n\n"
                    f"Sincerely,\nMarket Data Operations Team"
                )

        return inquiries


def generate_vendor_inquiries(
    validation_results: list[Any] | None = None,
    coverage_dips: list[CoverageDipAlert] | None = None,
    drift_report_changes: list[Any] | None = None,
    diff_summary: Any | None = None,
    quarantine_records: list[Any] | None = None,
    drop_id: str = "DROP_001",
) -> list[str]:
    """Generates automated vendor inquiry drafts conforming to 100% anomaly coverage."""
    generator = VendorQuestionsGenerator()
    return generator.generate_inquiries(
        validation_results=validation_results,
        coverage_dips=coverage_dips,
        drift_report_changes=drift_report_changes,
        diff_summary=diff_summary,
        quarantine_records=quarantine_records,
        drop_id=drop_id,
    )
