"""Scalable Multi-Profile Synthetic Market Data Generator.

Generates reproducible synthetic equity drops with deterministic random number generation,
configurable anomaly injection (negatives, SLA nulls, clock inversions, future timestamps),
multi-drop restatement simulation, and truth manifest generation.
"""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


class SyntheticGenerator:
    """Scalable generator of canonical market data drops and multi-drop suites."""

    def __init__(self, seed: int = 42) -> None:
        self.seed = seed

    def generate_drop(
        self,
        seed: int | None = None,
        num_entities: int = 10,
        days: int = 5,
        inject_negative_rate: float = 0.0,
        drop_id: str = "DROP_001",
        vendor_id: str = "SYNTH_GEN",
        dataset_id: str = "EQUITY_DAILY",
    ) -> list[dict[str, Any]]:
        """Generates synthetic records conforming to the canonical equity schema."""
        s = seed if seed is not None else self.seed
        return generate_synthetic_drop(
            seed=s,
            num_entities=num_entities,
            days=days,
            inject_negative_rate=inject_negative_rate,
            drop_id=drop_id,
            vendor_id=vendor_id,
            dataset_id=dataset_id,
        )

    def generate_drop_table(
        self,
        seed: int | None = None,
        num_entities: int = 10,
        days: int = 5,
        inject_negative_rate: float = 0.0,
        drop_id: str = "DROP_001",
        vendor_id: str = "SYNTH_GEN",
        dataset_id: str = "EQUITY_DAILY",
    ) -> pa.Table:
        """Generates synthetic drop as a PyArrow Table."""
        records = self.generate_drop(
            seed=seed,
            num_entities=num_entities,
            days=days,
            inject_negative_rate=inject_negative_rate,
            drop_id=drop_id,
            vendor_id=vendor_id,
            dataset_id=dataset_id,
        )
        return pa.Table.from_pylist(records)

    def generate_multi_drop_suite(
        self,
        output_dir: Path | str | None = None,
    ) -> dict[str, Any]:
        """Generates a complete multi-drop verification suite with ground truth manifest.

        Includes:
        - Drop 1 (baseline): 60 records across 10 major equities
        - Drop 2 (mutations): 65 records with 10 restatements, 10 new, 45 unchanged, 5 dropped
        - Drop 3 (anomalies): 21 records with 10 planted anomalies + 11 valid control records
        - Truth manifest: 151 records mapping every record's ground truth mutation status.
        """
        return generate_multi_drop_suite(output_dir=output_dir, base_seed=self.seed)


def generate_synthetic_drop(
    seed: int = 42,
    num_entities: int = 10,
    days: int = 5,
    inject_negative_rate: float = 0.0,
    drop_id: str = "DROP_001",
    vendor_id: str = "SYNTH_GEN",
    dataset_id: str = "EQUITY_DAILY",
) -> list[dict[str, Any]]:
    """Generates synthetic daily equity market data records deterministically.

    Conforms to canonical record schema:
    record_id, entity_id, source_symbol, source_symbol_type, event_time, knowledge_time, metric, value, unit.
    """
    rng = random.Random(seed)
    tickers = [f"SYM_{i:02d}" for i in range(num_entities)]
    records: list[dict[str, Any]] = []
    base_date = datetime(2023, 10, 2, 16, 0, 0, tzinfo=UTC)

    rec_id = 1
    for d in range(days):
        ev_dt = base_date + timedelta(days=d)
        for sym in tickers:
            val = rng.uniform(100.0, 500.0)
            if inject_negative_rate > 0.0 and rng.random() < inject_negative_rate:
                val = -val

            records.append(
                {
                    "record_id": f"SYNTH_{rec_id:05d}",
                    "entity_id": f"ENT_{sym}",
                    "source_symbol": sym,
                    "source_symbol_type": "TICKER",
                    "event_time": ev_dt.isoformat(),
                    "knowledge_time": "2023-10-06T20:00:00Z",
                    "metric": "CLOSE_PRICE",
                    "value": round(val, 2),
                    "unit": "USD",
                    "vendor_id": vendor_id,
                    "dataset_id": dataset_id,
                    "drop_id": drop_id,
                    "is_restatement": False,
                    "revision_seq": 1,
                }
            )
            rec_id += 1

    return records


def generate_multi_drop_suite(
    output_dir: Path | str | None = None,
    base_seed: int = 42,
) -> dict[str, Any]:
    """Generates Drop 1 (baseline), Drop 2 (restatements), Drop 3 (anomalies), and truth manifest.

    If output_dir is provided, writes parquet/csv files to that directory.
    Returns dictionary with generated tables and manifest.
    """
    equities = [
        ("AAPL", "ENT_AAPL", 172.0),
        ("MSFT", "ENT_MSFT", 318.0),
        ("GOOGL", "ENT_GOOGL", 135.0),
        ("AMZN", "ENT_AMZN", 127.0),
        ("NVDA", "ENT_NVDA", 440.0),
        ("META", "ENT_META", 300.0),
        ("TSLA", "ENT_TSLA", 250.0),
        ("JPM", "ENT_JPM", 145.0),
        ("UNH", "ENT_UNH", 505.0),
        ("XOM", "ENT_XOM", 112.0),
    ]

    base_dates = [datetime(2023, 10, d, 16, 0, 0, tzinfo=UTC) for d in range(2, 7)]
    kt_drop1 = datetime(2023, 10, 6, 20, 0, 0, tzinfo=UTC)
    kt_drop2 = datetime(2023, 10, 13, 20, 0, 0, tzinfo=UTC)
    kt_drop3 = datetime(2023, 10, 20, 20, 0, 0, tzinfo=UTC)

    # 1. Drop 1: 50 price records (10 equities x 5 days) + 10 volume records = 60 records
    drop1_records: list[dict[str, Any]] = []
    manifest_records: list[dict[str, Any]] = []

    rec_counter = 1
    for sym, ent, base_px in equities:
        for i, dt in enumerate(base_dates):
            rec_id = f"REC_001_{rec_counter:04d}"
            px = round(base_px + (i * 0.87), 2)
            drop1_records.append(
                {
                    "record_id": rec_id,
                    "entity_id": ent,
                    "source_symbol": sym,
                    "source_symbol_type": "TICKER",
                    "event_time": dt,
                    "knowledge_time": kt_drop1,
                    "metric": "CLOSE_PRICE",
                    "value": px,
                    "unit": "USD",
                    "vendor_id": "SYNTH_EQUITY_CORP",
                    "dataset_id": "EQUITY_DAILY",
                    "drop_id": "DROP_001",
                    "is_restatement": False,
                    "revision_seq": 1,
                }
            )
            manifest_records.append(
                {
                    "drop_id": "DROP_001",
                    "record_id": rec_id,
                    "entity_id": ent,
                    "source_symbol": sym,
                    "event_time": dt,
                    "knowledge_time": kt_drop1,
                    "value": px,
                    "mutation_type": "BASELINE",
                    "restatement_delta": 0.0,
                    "restatement_pct_delta": 0.0,
                    "is_planted_restatement": False,
                    "is_planted_anomaly": False,
                    "expected_rule_violation": "NONE",
                }
            )
            rec_counter += 1

    # Add 10 volume records
    for sym, ent, _ in equities:
        rec_id = f"REC_001_{rec_counter:04d}"
        vol = 5000000.0 + (rec_counter * 10000.0)
        dt = base_dates[4]
        drop1_records.append(
            {
                "record_id": rec_id,
                "entity_id": ent,
                "source_symbol": sym,
                "source_symbol_type": "TICKER",
                "event_time": dt,
                "knowledge_time": kt_drop1,
                "metric": "VOLUME",
                "value": vol,
                "unit": "SHARES",
                "vendor_id": "SYNTH_EQUITY_CORP",
                "dataset_id": "EQUITY_DAILY",
                "drop_id": "DROP_001",
                "is_restatement": False,
                "revision_seq": 1,
            }
        )
        manifest_records.append(
            {
                "drop_id": "DROP_001",
                "record_id": rec_id,
                "entity_id": ent,
                "source_symbol": sym,
                "event_time": dt,
                "knowledge_time": kt_drop1,
                "value": vol,
                "mutation_type": "BASELINE",
                "restatement_delta": 0.0,
                "restatement_pct_delta": 0.0,
                "is_planted_restatement": False,
                "is_planted_anomaly": False,
                "expected_rule_violation": "NONE",
            }
        )
        rec_counter += 1

    # 2. Drop 2: 65 records
    # - 45 unchanged (baseline records 0..44)
    # - 5 dropped (baseline records 45..49 omitted)
    # - 10 restated records (records 50..59 with altered values)
    # - 10 new records (trading date 2023-10-09)
    drop2_records: list[dict[str, Any]] = []

    # 45 Unchanged records
    for r in drop1_records[:45]:
        drop2_records.append(
            {
                **r,
                "source_symbol_type": "TICKER",
                "knowledge_time": kt_drop2,
                "drop_id": "DROP_002",
            }
        )
        manifest_records.append(
            {
                "drop_id": "DROP_002",
                "record_id": r["record_id"],
                "entity_id": r["entity_id"],
                "source_symbol": r["source_symbol"],
                "event_time": r["event_time"],
                "knowledge_time": kt_drop2,
                "value": r["value"],
                "mutation_type": "UNCHANGED",
                "restatement_delta": 0.0,
                "restatement_pct_delta": 0.0,
                "is_planted_restatement": False,
                "is_planted_anomaly": False,
                "expected_rule_violation": "NONE",
            }
        )

    # 5 Dropped records (manifest only)
    for r in drop1_records[45:50]:
        manifest_records.append(
            {
                "drop_id": "DROP_002",
                "record_id": r["record_id"],
                "entity_id": r["entity_id"],
                "source_symbol": r["source_symbol"],
                "event_time": r["event_time"],
                "knowledge_time": kt_drop2,
                "value": r["value"],
                "mutation_type": "DELETED",
                "restatement_delta": 0.0,
                "restatement_pct_delta": 0.0,
                "is_planted_restatement": False,
                "is_planted_anomaly": False,
                "expected_rule_violation": "NONE",
            }
        )

    # 10 Restated records
    for i, r in enumerate(drop1_records[50:60]):
        rest_id = f"REC_002_REST_{i + 1:04d}"
        old_val = float(r["value"])
        delta = round(old_val * 0.05, 2)
        new_val = round(old_val + delta, 2)
        pct_delta = round((delta / old_val) * 100.0, 4)

        drop2_records.append(
            {
                **r,
                "record_id": rest_id,
                "source_symbol_type": "TICKER",
                "value": new_val,
                "knowledge_time": kt_drop2,
                "drop_id": "DROP_002",
                "is_restatement": True,
                "revision_seq": 2,
            }
        )
        manifest_records.append(
            {
                "drop_id": "DROP_002",
                "record_id": rest_id,
                "entity_id": r["entity_id"],
                "source_symbol": r["source_symbol"],
                "event_time": r["event_time"],
                "knowledge_time": kt_drop2,
                "value": new_val,
                "mutation_type": "RESTATED",
                "restatement_delta": delta,
                "restatement_pct_delta": pct_delta,
                "is_planted_restatement": True,
                "is_planted_anomaly": False,
                "expected_rule_violation": "NONE",
            }
        )

    # 10 New records (Monday 2023-10-09)
    new_date = datetime(2023, 10, 9, 16, 0, 0, tzinfo=UTC)
    for i, (sym, ent, base_px) in enumerate(equities):
        new_id = f"REC_002_NEW_{i + 1:04d}"
        new_px = round(base_px + 5.5, 2)
        drop2_records.append(
            {
                "record_id": new_id,
                "entity_id": ent,
                "source_symbol": sym,
                "source_symbol_type": "TICKER",
                "event_time": new_date,
                "knowledge_time": kt_drop2,
                "metric": "CLOSE_PRICE",
                "value": new_px,
                "unit": "USD",
                "vendor_id": "SYNTH_EQUITY_CORP",
                "dataset_id": "EQUITY_DAILY",
                "drop_id": "DROP_002",
                "is_restatement": False,
                "revision_seq": 1,
            }
        )
        manifest_records.append(
            {
                "drop_id": "DROP_002",
                "record_id": new_id,
                "entity_id": ent,
                "source_symbol": sym,
                "event_time": new_date,
                "knowledge_time": kt_drop2,
                "value": new_px,
                "mutation_type": "NEW",
                "restatement_delta": 0.0,
                "restatement_pct_delta": 0.0,
                "is_planted_restatement": False,
                "is_planted_anomaly": False,
                "expected_rule_violation": "NONE",
            }
        )

    # 3. Drop 3: 21 records with 11 control records + 10 planted anomalies
    drop3_records: list[dict[str, Any]] = []

    # 11 Valid control records:
    # 9 baseline controls for JPM, UNH, XOM (3 days each = 9 records)
    for i, (sym, ent, base_px) in enumerate(equities[7:]):
        for d_idx in range(3):
            ctrl_id = f"REC_003_CTRL_{i * 3 + d_idx + 1:04d}"
            px = round(base_px + d_idx * 1.1, 2)
            dt = base_dates[d_idx]
            drop3_records.append(
                {
                    "record_id": ctrl_id,
                    "entity_id": ent,
                    "source_symbol": sym,
                    "source_symbol_type": "TICKER",
                    "event_time": dt,
                    "knowledge_time": kt_drop3,
                    "metric": "CLOSE_PRICE",
                    "value": px,
                    "unit": "USD",
                    "vendor_id": "SYNTH_EQUITY_CORP",
                    "dataset_id": "EQUITY_DAILY",
                    "drop_id": "DROP_003",
                    "is_restatement": False,
                    "revision_seq": 1,
                }
            )
            manifest_records.append(
                {
                    "drop_id": "DROP_003",
                    "record_id": ctrl_id,
                    "entity_id": ent,
                    "source_symbol": sym,
                    "event_time": dt,
                    "knowledge_time": kt_drop3,
                    "value": px,
                    "mutation_type": "BASELINE",
                    "restatement_delta": 0.0,
                    "restatement_pct_delta": 0.0,
                    "is_planted_restatement": False,
                    "is_planted_anomaly": False,
                    "expected_rule_violation": "NONE",
                }
            )

    # 2 META baseline control records (2023-10-16 and 2023-10-17) before inverted anomalies
    meta_controls = [
        {
            "record_id": "REC_003_META_0001",
            "entity_id": "ENT_META",
            "source_symbol": "META",
            "event_time": datetime(2023, 10, 16, 16, 0, 0, tzinfo=UTC),
            "value": 300.00,
        },
        {
            "record_id": "REC_003_META_0002",
            "entity_id": "ENT_META",
            "source_symbol": "META",
            "event_time": datetime(2023, 10, 17, 16, 0, 0, tzinfo=UTC),
            "value": 301.50,
        },
    ]
    for mc in meta_controls:
        drop3_records.append(
            {
                "record_id": mc["record_id"],
                "entity_id": mc["entity_id"],
                "source_symbol": mc["source_symbol"],
                "source_symbol_type": "TICKER",
                "event_time": mc["event_time"],
                "knowledge_time": kt_drop3,
                "metric": "CLOSE_PRICE",
                "value": mc["value"],
                "unit": "USD",
                "vendor_id": "SYNTH_EQUITY_CORP",
                "dataset_id": "EQUITY_DAILY",
                "drop_id": "DROP_003",
                "is_restatement": False,
                "revision_seq": 1,
            }
        )
        manifest_records.append(
            {
                "drop_id": "DROP_003",
                "record_id": mc["record_id"],
                "entity_id": mc["entity_id"],
                "source_symbol": mc["source_symbol"],
                "event_time": mc["event_time"],
                "knowledge_time": kt_drop3,
                "value": mc["value"],
                "mutation_type": "BASELINE",
                "restatement_delta": 0.0,
                "restatement_pct_delta": 0.0,
                "is_planted_restatement": False,
                "is_planted_anomaly": False,
                "expected_rule_violation": "NONE",
            }
        )

    # 10 Planted anomalies
    anomalies: list[dict[str, Any]] = [
        {
            "record_id": "REC_003_ANOM_0001",
            "entity_id": "ENT_AAPL",
            "source_symbol": "AAPL",
            "event_time": base_dates[0],
            "value": -178.50,
            "metric": "CLOSE_PRICE",
            "unit": "USD",
            "violation": "F-VAL-02",
        },
        {
            "record_id": "REC_003_ANOM_0002",
            "entity_id": "ENT_TSLA",
            "source_symbol": "TSLA",
            "event_time": base_dates[0],
            "value": -242.00,
            "metric": "CLOSE_PRICE",
            "unit": "USD",
            "violation": "F-VAL-02",
        },
        {
            "record_id": "REC_003_ANOM_0003",
            "entity_id": "ENT_MSFT",
            "source_symbol": "MSFT",
            "event_time": base_dates[1],
            "value": None,
            "metric": "CLOSE_PRICE",
            "unit": "USD",
            "violation": "F-VAL-01",
        },
        {
            "record_id": "REC_003_ANOM_0004",
            "entity_id": None,
            "source_symbol": "GOOGL",
            "event_time": base_dates[1],
            "value": 135.50,
            "metric": "CLOSE_PRICE",
            "unit": "USD",
            "violation": "F-VAL-01",
        },
        {
            "record_id": "REC_003_ANOM_0005",
            "entity_id": "ENT_AMZN",
            "source_symbol": "AMZN",
            "event_time": datetime(2025, 10, 16, 16, 0, 0, tzinfo=UTC),
            "value": 130.00,
            "metric": "CLOSE_PRICE",
            "unit": "USD",
            "violation": "F-VAL-05",
        },
        {
            "record_id": "REC_003_ANOM_0006",
            "entity_id": "ENT_NVDA",
            "source_symbol": "NVDA",
            "event_time": datetime(2030, 1, 1, 16, 0, 0, tzinfo=UTC),
            "value": 450.00,
            "metric": "CLOSE_PRICE",
            "unit": "USD",
            "violation": "F-VAL-05",
        },
        {
            "record_id": "REC_003_ANOM_0007",
            "entity_id": "ENT_META",
            "source_symbol": "META",
            "event_time": datetime(2023, 10, 15, 16, 0, 0, tzinfo=UTC),
            "value": 305.00,
            "metric": "CLOSE_PRICE",
            "unit": "USD",
            "violation": "F-VAL-04",
        },
        {
            "record_id": "REC_003_ANOM_0008",
            "entity_id": "ENT_META",
            "source_symbol": "META",
            "event_time": datetime(2023, 10, 14, 16, 0, 0, tzinfo=UTC),
            "value": 304.00,
            "metric": "CLOSE_PRICE",
            "unit": "USD",
            "violation": "F-VAL-04",
        },
        {
            "record_id": "REC_003_ANOM_0009",
            "entity_id": "UNRESOLVED_SYNTH_XYZ99",
            "source_symbol": "UNKNOWN_CORP_XYZ99",
            "event_time": base_dates[2],
            "value": 100.00,
            "metric": "CLOSE_PRICE",
            "unit": "USD",
            "violation": "F-SYM-03",
        },
        {
            "record_id": "REC_003_ANOM_0010",
            "entity_id": "UNRESOLVED_SYNTH_999",
            "source_symbol": "GHOST_TICKER_999",
            "event_time": base_dates[2],
            "value": 50.00,
            "metric": "CLOSE_PRICE",
            "unit": "USD",
            "violation": "F-SYM-03",
        },
    ]

    for an in anomalies:
        drop3_records.append(
            {
                "record_id": an["record_id"],
                "entity_id": an["entity_id"],
                "source_symbol": an["source_symbol"],
                "source_symbol_type": "TICKER",
                "event_time": an["event_time"],
                "knowledge_time": kt_drop3,
                "metric": an["metric"],
                "value": an["value"],
                "unit": an["unit"],
                "vendor_id": "SYNTH_EQUITY_CORP",
                "dataset_id": "EQUITY_DAILY",
                "drop_id": "DROP_003",
                "is_restatement": False,
                "revision_seq": 1,
            }
        )
        manifest_records.append(
            {
                "drop_id": "DROP_003",
                "record_id": an["record_id"],
                "entity_id": an["entity_id"] or "NONE",
                "source_symbol": an["source_symbol"],
                "event_time": an["event_time"],
                "knowledge_time": kt_drop3,
                "value": an["value"],
                "mutation_type": "ANOMALOUS",
                "restatement_delta": 0.0,
                "restatement_pct_delta": 0.0,
                "is_planted_restatement": False,
                "is_planted_anomaly": True,
                "expected_rule_violation": an["violation"],
            }
        )

    tbl_drop1 = pa.Table.from_pylist(drop1_records)
    tbl_drop2 = pa.Table.from_pylist(drop2_records)
    tbl_drop3 = pa.Table.from_pylist(drop3_records)
    tbl_manifest = pa.Table.from_pylist(manifest_records)

    result = {
        "drop_001": tbl_drop1,
        "drop_002": tbl_drop2,
        "drop_003": tbl_drop3,
        "truth_manifest": tbl_manifest,
    }

    if output_dir is not None:
        out_path = Path(output_dir)
        out_path.mkdir(parents=True, exist_ok=True)
        pq.write_table(tbl_drop1, out_path / "drop_001_baseline.parquet")
        pq.write_table(tbl_drop2, out_path / "drop_002_restatements.parquet")
        pq.write_table(tbl_drop3, out_path / "drop_003_anomalies.parquet")
        pq.write_table(tbl_manifest, out_path / "truth_manifest.parquet")

    return result
