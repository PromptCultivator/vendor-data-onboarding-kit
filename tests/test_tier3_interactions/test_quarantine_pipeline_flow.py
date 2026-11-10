"""
Tier 3 Interaction Tests: Unmapped Entity Quarantine & End-to-End Pipeline Flow.

Tests the stateful interaction between the Symbology Quarantine subsystem (F-SYM-03),
Vectorized Validation (F-VAL-01..07), Append-Only Storage (F-STR-01..03),
Revision Ledger (F-DIF-01), and Automated Vendor Reporting (F-REP-03, F-REP-04):
- Quarantine synthetic ID UNRESOLVED_<VENDOR>_<ID> generation for unmapped symbols.
- Synthetic IDs flowing safely through VectorizedValidationEngine without crashing.
- Storage of quarantined records in BitemporalParquetStore preserving all raw data.
- DiffEngine tracking revisions on synthetic IDs between consecutive drops.
- Automated generation of vendor inquiry text targeted specifically at quarantined symbols.
- Retroactive re-linking of quarantined records to canonical entity IDs upon resolution.
- Multi-vendor unmapped symbol isolation (preventing collisions across vendors).
- Mixed drop accounting: clean symbols vs quarantined symbols correctly partitioned.
- Dual-format readiness report export (Markdown and JSON) with quarantine details.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pytest

from test_tier3_interactions.conftest import (
    BitemporalParquetStore,
    QuarantineRecord,
    ReferenceDiffEngine,
    SymbologyMaster,
    VectorizedValidationEngine,
)


@dataclass
class QuarantineVendorInquiry:
    vendor_id: str
    raw_symbol: str
    synthetic_id: str
    record_count: int
    first_seen: str
    last_seen: str
    inquiry_message: str


class QuarantineReportingPipeline:
    """Opaque-box reporting engine for unmapped entity quarantine and vendor inquiries."""

    def __init__(self, conn: duckdb.DuckDBPyConnection | None = None) -> None:
        self.conn = conn or duckdb.connect()

    def generate_vendor_inquiries(
        self, quarantine_records: list[QuarantineRecord], drop_id: str = "DROP_001"
    ) -> list[QuarantineVendorInquiry]:
        inquiries = []
        by_vendor: dict[str, list[QuarantineRecord]] = {}
        for q in quarantine_records:
            by_vendor.setdefault(q.vendor_id, []).append(q)

        for vendor, q_list in by_vendor.items():
            symbols = sorted({q.raw_symbol for q in q_list})
            for sym in symbols:
                sym_records = [q for q in q_list if q.raw_symbol == sym]
                syn_id = sym_records[0].synthetic_id
                t_first = min(q.quarantine_time for q in sym_records).isoformat()
                t_last = max(q.quarantine_time for q in sym_records).isoformat()
                cnt = len(sym_records)

                msg = (
                    f"Subject: [Action Required] Unmapped Identifier '{sym}' in {drop_id}\n\n"
                    f"Dear {vendor} Support Team,\n\n"
                    f"During automated ingestion of drop '{drop_id}', the system encountered {cnt} records "
                    f"referencing unmapped symbol '{sym}'.\n"
                    f"Our security master has quarantined these records under synthetic identifier: {syn_id}.\n"
                    f"Date range: {t_first} to {t_last}.\n\n"
                    f"Please provide the corresponding authoritative cross-identifiers (CIK, CUSIP, ISIN, or FIGI) "
                    f"so we may link this entity to our canonical master.\n\n"
                    f"Sincerely,\nMarket Data Operations Team"
                )
                inquiries.append(
                    QuarantineVendorInquiry(
                        vendor_id=vendor,
                        raw_symbol=sym,
                        synthetic_id=syn_id,
                        record_count=cnt,
                        first_seen=t_first,
                        last_seen=t_last,
                        inquiry_message=msg,
                    )
                )
        return inquiries

    def build_quarantine_scorecard(
        self, total_records: int, mapped_records: int, quarantined_records: int
    ) -> dict[str, Any]:
        q_rate = (quarantined_records / total_records) if total_records > 0 else 0.0
        status = "PASS" if quarantined_records == 0 else ("WARN" if q_rate <= 0.20 else "FAIL")
        return {
            "total_records": total_records,
            "mapped_records": mapped_records,
            "quarantined_records": quarantined_records,
            "quarantine_rate_pct": round(q_rate * 100.0, 2),
            "status": status,
        }


@pytest.mark.tier3
class TestQuarantinePipelineFlow:
    """Test suite for quarantine flow across all pipeline stages."""

    def test_unmapped_symbol_synthetic_id_quarantine_generation(self) -> None:
        """
        Ingestion of an unmapped symbol generates synthetic ID UNRESOLVED_<VENDOR>_<SYMBOL>.
        The quarantine ledger must record raw symbol, source type, vendor, and timestamp.
        """
        master = SymbologyMaster()
        t_asof = datetime(2023, 10, 2, 16, 0, 0, tzinfo=UTC)

        synth_id = master.resolve(
            symbol="GHOST_CORP_42",
            symbol_type="TICKER",
            as_of=t_asof,
            vendor_id="REUTERS",
        )
        assert synth_id == "UNRESOLVED_REUTERS_GHOST_CORP_42"
        assert len(master.quarantine_ledger) == 1

        entry = master.quarantine_ledger[0]
        assert entry.synthetic_id == synth_id
        assert entry.raw_symbol == "GHOST_CORP_42"
        assert entry.source_type == "TICKER"
        assert entry.vendor_id == "REUTERS"
        assert entry.quarantine_time == t_asof
        assert entry.reason == "UNMAPPED_SYMBOL"

    def test_quarantine_synthetic_id_validation_non_crashing(
        self, tier3_validator: VectorizedValidationEngine
    ) -> None:
        """
        A table containing synthetic quarantine IDs must pass through VectorizedValidationEngine
        without raising exceptions, while successfully verifying completeness, range, and duplicates.
        """
        t_event = datetime(2023, 10, 2, 16, 0, 0, tzinfo=UTC)
        quarantined_tbl = pa.Table.from_pydict(
            {
                "record_id": ["REC_Q1", "REC_Q2"],
                "entity_id": ["UNRESOLVED_V_UNKNOWN1", "UNRESOLVED_V_UNKNOWN2"],
                "source_symbol": ["UNKNOWN1", "UNKNOWN2"],
                "event_time": [t_event, t_event],
                "metric": ["CLOSE", "CLOSE"],
                "value": [100.0, 250.0],
                "unit": ["USD", "USD"],
            }
        )

        # Completeness validation
        res_comp = tier3_validator.validate_completeness(
            quarantined_tbl, mandatory_cols=["entity_id", "event_time", "metric", "value"]
        )
        assert res_comp.passed
        assert res_comp.violation_count == 0

        # Range & domain validation
        res_range = tier3_validator.validate_range_and_domain(quarantined_tbl, value_col="value")
        assert res_range.passed
        assert res_range.violation_count == 0

        # Intra-drop duplicates check
        res_dup = tier3_validator.validate_intra_drop_duplicates(
            quarantined_tbl, key_cols=["entity_id", "event_time", "metric"]
        )
        assert res_dup.passed
        assert res_dup.violation_count == 0

    def test_quarantine_records_stored_in_bitemporal_parquet(
        self, tmp_path: Path, tier3_duckdb: duckdb.DuckDBPyConnection
    ) -> None:
        """
        Quarantined records must be stored into append-only partitioned Parquet storage.
        Data is NEVER dropped silently. As-of query with synthetic ID retrieves the facts.
        """
        store = BitemporalParquetStore(root_dir=tmp_path / "q_store", conn=tier3_duckdb)
        dataset_id = "QUARANTINE_DS"
        t_event = datetime(2023, 10, 2, 16, 0, 0, tzinfo=UTC)
        t_known = datetime(2023, 10, 6, 20, 0, 0, tzinfo=UTC)

        synth_id = "UNRESOLVED_VENDOR_A_MYSTERY_CORP"
        tbl = pa.Table.from_pydict(
            {
                "record_id": ["REC_Q_001"],
                "entity_id": [synth_id],
                "source_symbol": ["MYSTERY_CORP"],
                "source_symbol_type": ["TICKER"],
                "event_time": [t_event],
                "knowledge_time": [t_known],
                "metric": ["CLOSE"],
                "value": [88.50],
                "unit": ["USD"],
                "vendor_id": ["VENDOR_A"],
                "dataset_id": [dataset_id],
                "is_restatement": [False],
                "revision_seq": [1],
            }
        )

        rows_written = store.append(tbl, dataset_id=dataset_id, drop_id="DROP_001")
        assert rows_written == 1

        # Query using synthetic ID
        as_of_res = store.as_of_query(
            dataset_id=dataset_id, as_of_time=t_known, entity_ids=[synth_id]
        )
        assert as_of_res.num_rows == 1
        row = as_of_res.to_pylist()[0]
        assert row["entity_id"] == synth_id
        assert row["value"] == 88.50
        assert row["source_symbol"] == "MYSTERY_CORP"

    def test_quarantine_records_diffed_in_revision_ledger(
        self, tier3_diff_engine: ReferenceDiffEngine
    ) -> None:
        """
        When vendor drops restatements for an unmapped entity before it is resolved,
        DiffEngine diffs using the synthetic ID: detects RESTATED with exact delta.
        """
        t_event = datetime(2023, 10, 2, 16, 0, 0, tzinfo=UTC)
        synth_id = "UNRESOLVED_VENDOR_X_UNLISTED_TECH"

        base_tbl = pa.Table.from_pydict(
            {
                "record_id": ["B_01"],
                "entity_id": [synth_id],
                "source_symbol": ["UNLISTED_TECH"],
                "event_time": [t_event],
                "metric": ["VALUATION"],
                "value": [1000000.0],
                "unit": ["USD"],
            }
        )
        curr_tbl = pa.Table.from_pydict(
            {
                "record_id": ["C_01"],
                "entity_id": [synth_id],
                "source_symbol": ["UNLISTED_TECH"],
                "event_time": [t_event],
                "metric": ["VALUATION"],
                "value": [1200000.0],  # Restated upward
                "unit": ["USD"],
            }
        )

        diff_tbl, summary = tier3_diff_engine.diff_drops(base_tbl, curr_tbl)
        assert summary.restated_records == 1
        assert summary.unchanged_records == 0

        row = diff_tbl.to_pylist()[0]
        assert row["entity_id"] == synth_id
        assert row["diff_status"] == "RESTATED"
        assert row["restatement_delta"] == pytest.approx(200000.0)
        assert row["restatement_pct_delta"] == pytest.approx(20.0)

    def test_quarantine_triggers_actionable_vendor_inquiry_draft(self) -> None:
        """
        Readiness reporter converts quarantined records into actionable vendor email drafts
        containing vendor ID, raw symbol, synthetic ID, and formal request for security master mapping.
        """
        t_asof = datetime(2023, 10, 2, 16, 0, 0, tzinfo=UTC)
        records = [
            QuarantineRecord(
                synthetic_id="UNRESOLVED_BLOOMBERG_PRIVATE_CORP_A",
                raw_symbol="PRIVATE_CORP_A",
                source_type="TICKER",
                vendor_id="BLOOMBERG",
                quarantine_time=t_asof,
            ),
            QuarantineRecord(
                synthetic_id="UNRESOLVED_BLOOMBERG_PRIVATE_CORP_A",
                raw_symbol="PRIVATE_CORP_A",
                source_type="TICKER",
                vendor_id="BLOOMBERG",
                quarantine_time=datetime(2023, 10, 3, 16, 0, 0, tzinfo=UTC),
            ),
        ]

        reporter = QuarantineReportingPipeline()
        inquiries = reporter.generate_vendor_inquiries(records, drop_id="DROP_20231003")

        assert len(inquiries) == 1
        inq = inquiries[0]
        assert inq.vendor_id == "BLOOMBERG"
        assert inq.raw_symbol == "PRIVATE_CORP_A"
        assert inq.record_count == 2
        assert "UNRESOLVED_BLOOMBERG_PRIVATE_CORP_A" in inq.inquiry_message
        assert "DROP_20231003" in inq.inquiry_message
        assert "CIK, CUSIP, ISIN, or FIGI" in inq.inquiry_message

    def test_retroactive_relinking_preserves_historical_bitemporal_store(
        self, tmp_path: Path, tier3_duckdb: duckdb.DuckDBPyConnection
    ) -> None:
        """
        Retroactive re-linking scenario:
        1. T_1: Ingest unmapped symbol as UNRESOLVED_V_ACME.
        2. T_2: Vendor clarifies that ACME maps to ENT_ACME_CORP.
        3. Operations appends re-linked revision at T_2 with entity_id = ENT_ACME_CORP, revision_seq = 2.
        4. Invariant: Query as-of T_1 STILL returns UNRESOLVED_V_ACME (no retroactive data corruption).
        5. Query as-of T_2 returns ENT_ACME_CORP.
        """
        store = BitemporalParquetStore(root_dir=tmp_path / "relink_store", conn=tier3_duckdb)
        dataset_id = "RELINK_DS"
        t_event = datetime(2023, 9, 1, 16, 0, 0, tzinfo=UTC)
        t_1 = datetime(2023, 9, 5, 20, 0, 0, tzinfo=UTC)
        t_2 = datetime(2023, 9, 20, 20, 0, 0, tzinfo=UTC)

        synth_id = "UNRESOLVED_VENDOR_A_ACME"
        canonical_id = "ENT_ACME_CORP"

        # T_1: initial drop with synthetic ID
        d1 = pa.Table.from_pydict(
            {
                "record_id": ["D1_01"],
                "entity_id": [synth_id],
                "source_symbol": ["ACME"],
                "source_symbol_type": ["TICKER"],
                "event_time": [t_event],
                "knowledge_time": [t_1],
                "metric": ["PRICE"],
                "value": [150.0],
                "unit": ["USD"],
                "vendor_id": ["VENDOR_A"],
                "dataset_id": [dataset_id],
                "is_restatement": [False],
                "revision_seq": [1],
            }
        )
        store.append(d1, dataset_id=dataset_id, drop_id="DROP_001")

        # Snapshot at T_1
        h1 = store.as_of_query(dataset_id=dataset_id, as_of_time=t_1)
        assert h1.num_rows == 1
        assert h1.to_pylist()[0]["entity_id"] == synth_id

        # T_2: Re-linked drop with canonical ID
        d2 = pa.Table.from_pydict(
            {
                "record_id": ["D2_01"],
                "entity_id": [canonical_id],
                "source_symbol": ["ACME"],
                "source_symbol_type": ["TICKER"],
                "event_time": [t_event],
                "knowledge_time": [t_2],
                "metric": ["PRICE"],
                "value": [150.0],
                "unit": ["USD"],
                "vendor_id": ["VENDOR_A"],
                "dataset_id": [dataset_id],
                "is_restatement": [True],
                "revision_seq": [2],
            }
        )
        store.append(d2, dataset_id=dataset_id, drop_id="DROP_002")

        # Invariant: Query as-of T_1 evaluated after T_2 MUST be identical
        h1_after = store.as_of_query(dataset_id=dataset_id, as_of_time=t_1)
        assert h1_after.to_pylist()[0]["entity_id"] == synth_id

        # Query as-of T_2 sees canonical entity
        h2 = store.as_of_query(dataset_id=dataset_id, as_of_time=t_2, entity_ids=[canonical_id])
        assert h2.num_rows == 1
        assert h2.to_pylist()[0]["entity_id"] == canonical_id

    def test_multi_vendor_unresolved_symbol_isolation(self) -> None:
        """
        Two vendors drop an unmapped symbol with the same string name ('TITAN').
        Quarantine must generate distinct synthetic IDs:
        UNRESOLVED_VENDOR1_TITAN vs UNRESOLVED_VENDOR2_TITAN.
        Assert zero cross-vendor entity collision.
        """
        master = SymbologyMaster()
        t_asof = datetime(2023, 10, 1, tzinfo=UTC)

        id1 = master.resolve("TITAN", "TICKER", as_of=t_asof, vendor_id="VENDOR_1")
        id2 = master.resolve("TITAN", "TICKER", as_of=t_asof, vendor_id="VENDOR_2")

        assert id1 == "UNRESOLVED_VENDOR_1_TITAN"
        assert id2 == "UNRESOLVED_VENDOR_2_TITAN"
        assert id1 != id2
        assert len(master.quarantine_ledger) == 2

    def test_mixed_drop_clean_and_quarantine_scorecard_accounting(self) -> None:
        """
        Drop with 100 records: 85 mapped to canonical entities, 15 unmapped (quarantined).
        Pipeline produces structured scorecard:
        total=100, mapped=85, quarantined=15, rate=15.0%, status=WARN.
        """
        reporter = QuarantineReportingPipeline()
        scorecard = reporter.build_quarantine_scorecard(
            total_records=100, mapped_records=85, quarantined_records=15
        )

        assert scorecard["total_records"] == 100
        assert scorecard["mapped_records"] == 85
        assert scorecard["quarantined_records"] == 15
        assert scorecard["quarantine_rate_pct"] == 15.0
        assert scorecard["status"] == "WARN"

        # Severe drop (>20% unmapped) -> status=FAIL
        fail_scorecard = reporter.build_quarantine_scorecard(
            total_records=100, mapped_records=70, quarantined_records=30
        )
        assert fail_scorecard["quarantine_rate_pct"] == 30.0
        assert fail_scorecard["status"] == "FAIL"

    def test_quarantine_dual_format_report_export(self, tmp_path: Path) -> None:
        """
        Verify export of quarantine reporting metrics to both Markdown and JSON formats.
        Both formats must include the exact list of quarantined entities and counts.
        """
        reporter = QuarantineReportingPipeline()
        records = [
            QuarantineRecord(
                synthetic_id="UNRESOLVED_BLOOMBERG_CORP_X",
                raw_symbol="CORP_X",
                source_type="TICKER",
                vendor_id="BLOOMBERG",
                quarantine_time=datetime(2023, 10, 2, tzinfo=UTC),
            )
        ]
        inquiries = reporter.generate_vendor_inquiries(records)
        scorecard = reporter.build_quarantine_scorecard(100, 99, 1)

        # JSON Export
        json_payload = {
            "scorecard": scorecard,
            "quarantined_entities": [asdict(r) for r in records],
            "vendor_inquiries": [asdict(inq) for inq in inquiries],
        }
        # Serialize datetime safely
        json_path = tmp_path / "quarantine_report.json"
        json_path.write_text(json.dumps(json_payload, indent=2, default=str))
        assert json_path.exists()

        parsed = json.loads(json_path.read_text())
        assert parsed["scorecard"]["quarantined_records"] == 1
        assert parsed["quarantined_entities"][0]["synthetic_id"] == "UNRESOLVED_BLOOMBERG_CORP_X"

        # Markdown Export
        md_content = (
            "# Data Quarantine & Unmapped Entity Report\n\n"
            f"**Status**: {scorecard['status']}  \n"
            f"**Total Records**: {scorecard['total_records']}  \n"
            f"**Quarantined**: {scorecard['quarantined_records']} ({scorecard['quarantine_rate_pct']}%)  \n\n"
            "## Quarantined Entities\n\n"
            "| Synthetic ID | Raw Symbol | Vendor | Date |\n"
            "|---|---|---|---|\n"
            f"| {records[0].synthetic_id} | {records[0].raw_symbol} | {records[0].vendor_id} | {records[0].quarantine_time.date()} |\n"
        )
        md_path = tmp_path / "quarantine_report.md"
        md_path.write_text(md_content)
        assert md_path.exists()
        assert "UNRESOLVED_BLOOMBERG_CORP_X" in md_path.read_text()
