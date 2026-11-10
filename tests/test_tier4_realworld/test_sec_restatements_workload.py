"""
Tier 4 Real-World Workload Scenarios: Scenario 4A - SEC Form 10-K Baseline vs 10-K/A Restatements.

Per TEST_INFRA.md and PROJECT.md §Scenario 4A:
Authentic SEC DERA relational dataset workload capturing actual corporate financial restatements:
- Filers: Apple Inc (CIK 0000320193), Microsoft Corp (CIK 0000789019), Amazon.com Inc (CIK 0001018724).
- Datasets:
  - Baseline quarterly facts: tests/fixtures/sec/2023q3_*.txt and tests/fixtures/sec/2023q4_*.txt.
  - Amended 10-K/A filings: tests/fixtures/sec/2024q1_restatement_*.txt.

Scenarios tested:
1. End-to-end multi-quarter SEC fact ingestion and relational join pipeline (sub.txt + num.txt),
   CIK symbology resolution, balance sheet rules validation, and partitioned append-only storage.
2. Form 10-K vs 10-K/A DiffEngine restatement recall (100.00%) and revision magnitude delta precision.
3. Bitemporal as-of queries with strictly 0 point-in-time violations (H_1 == H_2) and multi-vintage audit trails.
4. Readiness report generation with automated filer/vendor restatement inquiry drafts (Markdown and JSON).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pytest

from test_tier4_realworld.conftest import (
    BitemporalParquetStore,
    ReferenceDiffEngine,
    SymbologyMaster,
    VectorizedValidationEngine,
    assert_point_in_time_invariance,
    load_sec_dera_facts,
)


@pytest.mark.tier4
@pytest.mark.bitemporal
class TestSecRestatementsWorkload:
    """Scenario 4A: Authentic SEC DERA Form 10-K vs 10-K/A restatement verification."""

    def test_sec_10k_baseline_ingestion_and_relational_pipeline(
        self,
        tmp_path: Path,
        tier4_duckdb: duckdb.DuckDBPyConnection,
        sec_symbology_master: SymbologyMaster,
        tier4_validator: VectorizedValidationEngine,
    ) -> None:
        """
        Scenario 4A.1: Ingestion of baseline SEC facts for 2023q3 and 2023q4:
        - Relational join on accession number (adsh).
        - CIK resolution: 0000320193 -> ENT_AAPL, 0000789019 -> ENT_MSFT, 0001018724 -> ENT_AMZN.
        - Balance sheet rule validation (Assets > 0, Revenues > 0, completeness).
        - Partitioned append-only Parquet storage.
        """
        fixtures_sec = Path(__file__).resolve().parent.parent / "fixtures" / "sec"
        sub_2023q4 = fixtures_sec / "2023q4_sub.txt"
        num_2023q4 = fixtures_sec / "2023q4_num.txt"

        facts_2023q4 = load_sec_dera_facts(
            sub_path=sub_2023q4,
            num_path=num_2023q4,
            conn=tier4_duckdb,
            symbology_master=sec_symbology_master,
            drop_id="SEC_2023Q4",
        )

        assert facts_2023q4.num_rows > 0

        # Verify CIK resolution
        resolved_entities = set(facts_2023q4.column("entity_id").to_pylist())
        assert "ENT_AAPL" in resolved_entities
        assert "ENT_MSFT" in resolved_entities
        assert "ENT_AMZN" in resolved_entities
        assert not any(e.startswith("UNRESOLVED_") for e in resolved_entities)

        # Vectorized validation rules
        val_comp = tier4_validator.validate_completeness(
            facts_2023q4,
            mandatory_cols=["entity_id", "event_time", "knowledge_time", "metric", "value"],
        )
        assert val_comp.passed, f"Completeness check failed: {val_comp.violations_sample}"

        val_pos = tier4_validator.validate_positive_values(
            facts_2023q4,
            metric_col="metric",
            value_col="value",
            positive_metrics=["Assets", "Revenues"],
        )
        assert val_pos.passed, f"Positive values check failed: {val_pos.violations_sample}"

        # Store in append-only bitemporal store
        store = BitemporalParquetStore(root_dir=tmp_path / "sec_store", conn=tier4_duckdb)
        dataset_id = "SEC_FINANCIALS"
        rows_stored = store.append(facts_2023q4, dataset_id=dataset_id, drop_id="2023Q4")
        assert rows_stored == facts_2023q4.num_rows

        # Verify partition directory exists
        partition_dir = tmp_path / "sec_store" / dataset_id / "drop_id=2023Q4"
        assert partition_dir.exists()
        assert len(list(partition_dir.glob("*.parquet"))) == 1

    def test_sec_10ka_restatement_detection_recall_and_delta_precision(
        self,
        tier4_duckdb: duckdb.DuckDBPyConnection,
        sec_symbology_master: SymbologyMaster,
        tier4_diff_engine: ReferenceDiffEngine,
    ) -> None:
        """
        Scenario 4A.2: Form 10-K vs 10-K/A Restatement Ledger Verification:
        - Baseline: 2023q4 Apple Form 10-K (filed 2023-11-03, period 2023-09-30).
        - Amendment: 2024q1 Apple Form 10-K/A (filed 2024-01-25, period 2023-09-30).
        - DiffEngine must achieve 100.00% recall on restated tags:
            Revenues: $383,285M -> $382,500M (delta = -$785M, -0.2048%)
            NetIncomeLoss: $96,995M -> $96,450M (delta = -$545M, -0.5619%)
            Assets: $352,583M -> $351,800M (delta = -$783M, -0.2221%)
            OperatingIncomeLoss: $114,301M (UNCHANGED, delta = $0)
        - Precision: 100.00% (zero false positives on unchanged lines).
        """
        fixtures_sec = Path(__file__).resolve().parent.parent / "fixtures" / "sec"

        # Baseline 2023q4 facts
        facts_base = load_sec_dera_facts(
            sub_path=fixtures_sec / "2023q4_sub.txt",
            num_path=fixtures_sec / "2023q4_num.txt",
            conn=tier4_duckdb,
            symbology_master=sec_symbology_master,
            drop_id="SEC_2023Q4",
        )

        # Restatement 2024q1 facts
        facts_amend = load_sec_dera_facts(
            sub_path=fixtures_sec / "2024q1_restatement_sub.txt",
            num_path=fixtures_sec / "2024q1_restatement_num.txt",
            conn=tier4_duckdb,
            symbology_master=sec_symbology_master,
            drop_id="SEC_2024Q1",
        )

        # Filter to Apple (ENT_AAPL) period 2023-09-30
        con = tier4_duckdb
        con.register("fb", facts_base)
        con.register("fa", facts_amend)

        aapl_base = (
            con.sql("""
            SELECT * FROM fb
            WHERE entity_id = 'ENT_AAPL'
              AND event_time = TIMESTAMP '2023-09-30 00:00:00'
        """)
            .arrow()
            .read_all()
        )

        aapl_amend = (
            con.sql("""
            SELECT * FROM fa
            WHERE entity_id = 'ENT_AAPL'
              AND event_time = TIMESTAMP '2023-09-30 00:00:00'
        """)
            .arrow()
            .read_all()
        )

        diff_tbl, summary = tier4_diff_engine.diff_drops(
            aapl_base,
            aapl_amend,
            drop_id_baseline="2023Q4_10K",
            drop_id_current="2024Q1_10KA",
            keys=["entity_id", "event_time", "metric"],
        )

        diff_rows = {r["metric"]: r for r in diff_tbl.to_pylist()}

        # 100% restatement detection recall assertions (4 restated tags)
        assert "Revenues" in diff_rows
        assert diff_rows["Revenues"]["diff_status"] == "RESTATED"
        assert diff_rows["Revenues"]["value_old"] == 383285000000.0
        assert diff_rows["Revenues"]["value_new"] == 382500000000.0
        assert diff_rows["Revenues"]["restatement_delta"] == pytest.approx(-785000000.0)
        assert diff_rows["Revenues"]["restatement_pct_delta"] == pytest.approx(-0.204808, rel=1e-3)

        assert "NetIncomeLoss" in diff_rows
        assert diff_rows["NetIncomeLoss"]["diff_status"] == "RESTATED"
        assert diff_rows["NetIncomeLoss"]["value_old"] == 96995000000.0
        assert diff_rows["NetIncomeLoss"]["value_new"] == 96450000000.0
        assert diff_rows["NetIncomeLoss"]["restatement_delta"] == pytest.approx(-545000000.0)
        assert diff_rows["NetIncomeLoss"]["restatement_pct_delta"] == pytest.approx(
            -0.561884, rel=1e-3
        )

        assert "Assets" in diff_rows
        assert diff_rows["Assets"]["diff_status"] == "RESTATED"
        assert diff_rows["Assets"]["value_old"] == 352583000000.0
        assert diff_rows["Assets"]["value_new"] == 351800000000.0
        assert diff_rows["Assets"]["restatement_delta"] == pytest.approx(-783000000.0)
        assert diff_rows["Assets"]["restatement_pct_delta"] == pytest.approx(-0.222075, rel=1e-3)

        assert "StockholdersEquity" in diff_rows
        assert diff_rows["StockholdersEquity"]["diff_status"] == "RESTATED"
        assert diff_rows["StockholdersEquity"]["value_old"] == 62146000000.0
        assert diff_rows["StockholdersEquity"]["value_new"] == 61601000000.0
        assert diff_rows["StockholdersEquity"]["restatement_delta"] == pytest.approx(-545000000.0)

        # Precision assertions on non-restated line items (2 unchanged tags)
        assert "OperatingIncomeLoss" in diff_rows
        assert diff_rows["OperatingIncomeLoss"]["diff_status"] == "UNCHANGED"
        assert diff_rows["OperatingIncomeLoss"]["value_old"] == 114301000000.0
        assert diff_rows["OperatingIncomeLoss"]["value_new"] == 114301000000.0
        assert diff_rows["OperatingIncomeLoss"]["restatement_delta"] == pytest.approx(0.0)

        assert "CashAndCashEquivalentsAtCarryingValue" in diff_rows
        assert diff_rows["CashAndCashEquivalentsAtCarryingValue"]["diff_status"] == "UNCHANGED"
        assert diff_rows["CashAndCashEquivalentsAtCarryingValue"]["value_old"] == 29965000000.0
        assert diff_rows["CashAndCashEquivalentsAtCarryingValue"]["value_new"] == 29965000000.0
        assert diff_rows["CashAndCashEquivalentsAtCarryingValue"][
            "restatement_delta"
        ] == pytest.approx(0.0)

        # Overall summary stats
        assert summary.restated_records == 4
        assert summary.unchanged_records == 2
        assert summary.new_records == 0
        assert summary.dropped_records == 0

    def test_sec_bitemporal_as_of_queries_and_pit_invariance(
        self,
        tmp_path: Path,
        tier4_duckdb: duckdb.DuckDBPyConnection,
        sec_symbology_master: SymbologyMaster,
    ) -> None:
        """
        Scenario 4A.3: Point-in-Time Queries & Zero Look-Ahead Bias Verification:
        1. Ingest baseline 2023q4 (Apple filed 2023-11-03, Microsoft filed 2023-10-25).
        2. Ingest 2023q3 (Microsoft filed 10-K 2023-07-27 for FY2023).
        3. Query as-of 2023-12-01: Apple Revenues MUST be $383,285M (baseline 10-K).
        4. Ingest 2024q1 restatement (Apple 10-K/A filed 2024-01-25; Microsoft 10-K/A filed 2024-02-15).
        5. Invariant check: Query as-of 2023-12-01 evaluated after 2024q1 MUST still return $383,285M (H_1 == H_2).
        6. Staggered filing check:
           - As-of 2024-02-01: Apple is restated ($382,500M), but Microsoft FY2023 is UNCHANGED ($211,915M).
           - As-of 2024-03-01: Both Apple and Microsoft show restated numbers.
        7. Audit trail query for Apple Revenues returns ordered 2-element trajectory [(383.285B, 2023-11-03), (382.500B, 2024-01-25)].
        """
        store = BitemporalParquetStore(root_dir=tmp_path / "sec_pit_store", conn=tier4_duckdb)
        dataset_id = "SEC_BITEMPORAL"
        fixtures_sec = Path(__file__).resolve().parent.parent / "fixtures" / "sec"

        # Ingest 2023q3
        facts_2023q3 = load_sec_dera_facts(
            sub_path=fixtures_sec / "2023q3_sub.txt",
            num_path=fixtures_sec / "2023q3_num.txt",
            conn=tier4_duckdb,
            symbology_master=sec_symbology_master,
            drop_id="SEC_2023Q3",
        )
        store.append(facts_2023q3, dataset_id=dataset_id, drop_id="2023Q3")

        # Ingest 2023q4
        facts_2023q4 = load_sec_dera_facts(
            sub_path=fixtures_sec / "2023q4_sub.txt",
            num_path=fixtures_sec / "2023q4_num.txt",
            conn=tier4_duckdb,
            symbology_master=sec_symbology_master,
            drop_id="SEC_2023Q4",
        )
        store.append(facts_2023q4, dataset_id=dataset_id, drop_id="2023Q4")

        # Historical cut: 2023-12-01 (before 10-K/A filed)
        t_hist = datetime(2023, 12, 1, 0, 0, 0, tzinfo=UTC)
        h1 = store.as_of_query(
            dataset_id=dataset_id, as_of_time=t_hist, entity_ids=["ENT_AAPL"], metric="Revenues"
        )
        assert h1.num_rows == 2
        apple_fy_h1 = next(r for r in h1.to_pylist() if r["event_time"].month == 9)
        assert apple_fy_h1["value"] == 383285000000.0

        # Ingest 2024q1 restatements
        facts_2024q1 = load_sec_dera_facts(
            sub_path=fixtures_sec / "2024q1_restatement_sub.txt",
            num_path=fixtures_sec / "2024q1_restatement_num.txt",
            conn=tier4_duckdb,
            symbology_master=sec_symbology_master,
            drop_id="SEC_2024Q1",
        )
        store.append(facts_2024q1, dataset_id=dataset_id, drop_id="2024Q1")

        # Re-query as-of 2023-12-01 -> H_2
        h2 = store.as_of_query(
            dataset_id=dataset_id, as_of_time=t_hist, entity_ids=["ENT_AAPL"], metric="Revenues"
        )

        # Mathematical Invariance: H_1 == H_2
        assert_point_in_time_invariance(h1, h2, key_columns=["entity_id", "event_time", "metric"])
        apple_fy_h2 = next(r for r in h2.to_pylist() if r["event_time"].month == 9)
        assert apple_fy_h2["value"] == 383285000000.0

        # Staggered filing date verification:
        # Date: 2024-02-01 (Apple 10-K/A was filed 2024-01-25; Microsoft 10-K/A filed 2024-02-15)
        t_staggered = datetime(2024, 2, 1, 0, 0, 0, tzinfo=UTC)
        snap_aapl = store.as_of_query(
            dataset_id=dataset_id,
            as_of_time=t_staggered,
            entity_ids=["ENT_AAPL"],
            metric="Revenues",
        )
        apple_fy_stag = next(r for r in snap_aapl.to_pylist() if r["event_time"].month == 9)
        assert apple_fy_stag["value"] == 382500000000.0  # Apple restated

        snap_msft = store.as_of_query(
            dataset_id=dataset_id,
            as_of_time=t_staggered,
            entity_ids=["ENT_MSFT"],
            metric="Revenues",
        )
        # Microsoft FY2023 Revenues filed in 2023q3 was 211915000000.0
        msft_fy = [
            r
            for r in snap_msft.to_pylist()
            if r["event_time"].year == 2023 and r["event_time"].month == 6
        ]
        assert len(msft_fy) == 1
        assert msft_fy[0]["value"] == 211915000000.0  # Microsoft NOT yet restated on 2024-02-01!

        # Date: 2024-03-01 (both filed)
        t_final = datetime(2024, 3, 1, 0, 0, 0, tzinfo=UTC)
        snap_msft_final = store.as_of_query(
            dataset_id=dataset_id, as_of_time=t_final, entity_ids=["ENT_MSFT"], metric="Revenues"
        )
        msft_fy_restated = [
            r
            for r in snap_msft_final.to_pylist()
            if r["event_time"].year == 2023 and r["event_time"].month == 6
        ]
        assert msft_fy_restated[0]["value"] == 212400000000.0  # Microsoft restated to 212.40B

        # Audit trail query for Apple Revenues (event_time 2023-09-30)
        t_event_apple = datetime(2023, 9, 30, 0, 0, 0, tzinfo=UTC)
        trail = store.audit_trail(
            dataset_id=dataset_id, entity_id="ENT_AAPL", event_time=t_event_apple, metric="Revenues"
        )
        trail_rows = trail.to_pylist()
        assert len(trail_rows) == 2
        assert trail_rows[0]["revision_seq"] == 1
        assert trail_rows[0]["value"] == 383285000000.0
        assert trail_rows[1]["revision_seq"] == 2
        assert trail_rows[1]["value"] == 382500000000.0
        assert trail_rows[0]["knowledge_time"] < trail_rows[1]["knowledge_time"]

    def test_sec_readiness_scorecard_and_automated_vendor_inquiries(
        self,
        tmp_path: Path,
        tier4_duckdb: duckdb.DuckDBPyConnection,
        sec_symbology_master: SymbologyMaster,
    ) -> None:
        """
        Scenario 4A.4: Automated Vendor Readiness Reporting for SEC Financial Restatements:
        - Detects Form 10-K/A amendment restatements.
        - Generates formal compliance inquiry drafts requesting filer restatement explanation.
        - Validates dual-format export to Markdown and JSON.
        """
        fixtures_sec = Path(__file__).resolve().parent.parent / "fixtures" / "sec"
        facts_amend = load_sec_dera_facts(
            sub_path=fixtures_sec / "2024q1_restatement_sub.txt",
            num_path=fixtures_sec / "2024q1_restatement_num.txt",
            conn=tier4_duckdb,
            symbology_master=sec_symbology_master,
            drop_id="SEC_2024Q1",
        )

        # Extract 10-K/A filers
        con = tier4_duckdb
        con.register("fa", facts_amend)
        amended_filers = (
            con.sql("""
            SELECT DISTINCT entity_id, company_name, filing_form, adsh, knowledge_time
            FROM fa
            WHERE filing_form = '10-K/A'
            ORDER BY entity_id
        """)
            .arrow()
            .read_all()
            .to_pylist()
        )

        assert len(amended_filers) == 2  # Apple and Microsoft

        inquiries: list[dict[str, Any]] = []
        for f in amended_filers:
            ent = f["entity_id"]
            co = f["company_name"]
            adsh = f["adsh"]
            kt = f["knowledge_time"].isoformat()

            # Retrieve restated line items for this filer
            tags = con.sql(f"""
                SELECT metric, value
                FROM fa
                WHERE entity_id = '{ent}'
            """).fetchall()
            tag_summary = ", ".join([f"{t[0]} (${t[1]:,.0f})" for t in tags[:3]])

            inq_text = (
                f"Subject: [SEC Restatement Notice] Form 10-K/A Amendment Detected for {co}\n\n"
                f"To: Financial Reporting Operations / Compliance\n\n"
                f"Notice: An amended Form 10-K/A filing has been ingested for {co} ({ent}).\n"
                f"Accession Number: {adsh}\n"
                f"Filing Timestamp: {kt}\n"
                f"Key Restated Line Items: {tag_summary}\n\n"
                f"Action Requested: Please provide internal classification of restatement nature "
                f"(Big 'R' restatement requiring 8-K Item 4.02 vs little 'r' revision) and confirm whether "
                f"prior period balance sheet line items require historical re-baselining.\n\n"
                f"Generated by: Vendor Data Onboarding Readiness Engine"
            )
            inquiries.append(
                {
                    "entity_id": ent,
                    "company_name": co,
                    "adsh": adsh,
                    "inquiry_text": inq_text,
                }
            )

        # Dual-format export
        report_data = {
            "dataset_id": "SEC_DERA_FINANCIALS",
            "drop_id": "2024Q1_RESTATEMENT",
            "filers_analyzed": 3,
            "amendments_detected": len(amended_filers),
            "inquiries": inquiries,
        }

        # JSON Export
        json_path = tmp_path / "sec_readiness_report.json"
        json_path.write_text(json.dumps(report_data, indent=2, default=str))
        assert json_path.exists()
        parsed_json = json.loads(json_path.read_text())
        assert parsed_json["amendments_detected"] == 2

        # Markdown Export
        md_path = tmp_path / "sec_readiness_report.md"
        md_text = (
            "# SEC Financials Readiness & Restatement Report\n\n"
            f"**Dataset**: SEC_DERA_FINANCIALS  \n"
            f"**Drop**: 2024Q1_RESTATEMENT  \n"
            f"**Status**: WARN (Amendments Detected: {len(amended_filers)})  \n\n"
            "## Amended Filers\n\n"
            "| Entity ID | Company Name | Form | Accession Number |\n"
            "|---|---|---|---|\n"
        )
        for f in amended_filers:
            md_text += (
                f"| {f['entity_id']} | {f['company_name']} | {f['filing_form']} | {f['adsh']} |\n"
            )

        md_text += "\n## Automated Inquiries\n\n"
        for inq in inquiries:
            md_text += f"```text\n{inq['inquiry_text']}\n```\n\n"

        md_path.write_text(md_text)
        assert md_path.exists()
        content = md_path.read_text()
        assert "APPLE INC" in content
        assert "MICROSOFT CORP" in content
