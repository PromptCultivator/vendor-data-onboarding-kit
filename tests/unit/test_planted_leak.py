"""Acceptance tests for planted look-ahead leak simulation and LeakDetector recall."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
from typer.testing import CliRunner

from vdok.cli import app
from vdok.normalize import CANONICAL_ARROW_SCHEMA
from vdok.query import Store, detect_leaks
from vdok.store import write_partitioned_store

runner = CliRunner()


def _populate_toy_simulation_store(store_path: Path, days: int = 1005) -> list[Decimal]:
    """Populate store with historical daily prices for simulation and return prices."""
    base_time = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    ev_times: list[datetime] = []
    kt_times: list[datetime] = []
    prices: list[Decimal] = []

    current_price = Decimal("100.0")
    for d in range(days):
        ev = base_time + timedelta(days=d)
        kt = ev + timedelta(hours=16)  # Market close on same day
        ev_times.append(ev)
        kt_times.append(kt)

        # Deterministic pseudo-random price oscillation
        delta = Decimal(str(((d * 17) % 7) - 3)) * Decimal("0.50")
        current_price = max(Decimal("10.0"), current_price + delta)
        prices.append(current_price)

    rows = {
        "entity_id": ["ASSET_1"] * days,
        "entity_id_type": ["ticker"] * days,
        "field": ["price"] * days,
        "event_time": ev_times,
        "knowledge_time": kt_times,
        "value_num": prices,
        "value_str": [None] * days,
        "unit": ["USD"] * days,
        "drop_id": ["drop_sim"] * days,
        "source_file": ["sim.csv"] * days,
        "source_row": list(range(1, days + 1)),
        "row_hash": [f"h_{i}" for i in range(days)],
        "unmapped": [False] * days,
    }
    table = pa.Table.from_pydict(rows, schema=CANONICAL_ARROW_SCHEMA)
    write_partitioned_store(table, "synthetic_sim", "drop_sim", base_path=store_path)
    return prices


def test_planted_lookahead_leak_simulation_and_recall(tmp_path: Path) -> None:
    """Drive toy trading model twice (causal vs leaking), verify divergence and 100% leak recall."""
    store_base = tmp_path / "store"
    prices = _populate_toy_simulation_store(store_base, days=1005)
    vendor_store = store_base / "synthetic_sim"

    causal_log = tmp_path / "audit_causal.jsonl"
    leaking_log = tmp_path / "audit_leaking.jsonl"

    base_time = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    sim_days = 1000

    # -------------------------------------------------------------
    # Run A: Strictly causal model execution (decision_time == as_of)
    # -------------------------------------------------------------
    score_causal = Decimal("0.0")
    with Store(vendor_store, audit=True, audit_log_path=causal_log) as store_a:
        for t in range(sim_days):
            dec_time = base_time + timedelta(days=t)
            as_of_time = dec_time + timedelta(hours=18)  # Evening of decision day

            rec = store_a.value(
                entity_id="ASSET_1",
                field="price",
                event_time=dec_time,
                as_of=as_of_time,
                decision_time=as_of_time,
            )
            assert rec is not None
            # Causal decision: arbitrary periodic signal
            signal = Decimal("1.0") if (t % 2 == 0) else Decimal("-1.0")
            p_today = Decimal(str(rec["value_num"]))
            p_next = prices[t + 1]
            score_causal += signal * (p_next - p_today)

    # -------------------------------------------------------------
    # Run B: Look-ahead leaking execution (as_of > decision_time)
    # -------------------------------------------------------------
    score_leaking = Decimal("0.0")
    with Store(vendor_store, audit=True, audit_log_path=leaking_log) as store_b:
        for t in range(sim_days):
            dec_time = base_time + timedelta(days=t)
            # Future lookahead: queries tomorrow's price with as_of > decision_time
            future_event = dec_time + timedelta(days=1)
            future_as_of = future_event + timedelta(hours=18)

            rec = store_b.value(
                entity_id="ASSET_1",
                field="price",
                event_time=future_event,
                as_of=future_as_of,
                decision_time=dec_time,
            )
            assert rec is not None
            p_today = prices[t]
            p_next = Decimal(str(rec["value_num"]))
            # Perfect lookahead decision: always takes the winning side
            signal = Decimal("1.0") if (p_next >= p_today) else Decimal("-1.0")
            score_leaking += signal * (p_next - p_today)

    # 1. Model score divergence: Run A and Run B produce divergent simulation scores
    assert score_causal != score_leaking
    assert score_leaking > score_causal

    # 2. Leak detection on Run A (causal): zero leaks, 1000 total reads
    report_a = detect_leaks(causal_log)
    assert report_a.total_reads == 1000
    assert report_a.leaks_detected == 0
    assert report_a.as_of_leaks == 0
    assert report_a.has_leaks is False

    # 3. Leak detection on Run B (lookahead): exactly 1000 leaks, 100% recall, 0 false negatives
    report_b = detect_leaks(leaking_log)
    assert report_b.total_reads == 1000
    assert report_b.leaks_detected == 1000
    assert report_b.as_of_leaks == 1000
    assert report_b.has_leaks is True

    recall_b = report_b.leaks_detected / 1000
    assert recall_b == 1.0

    # 4. CLI tool exit code verification
    # Run A: exit code 0 (no leaks)
    res_a = runner.invoke(app, ["leaks", "--log", str(causal_log)])
    assert res_a.exit_code == 0
    assert "SUCCESS" in res_a.output

    # Run B: exit code 1 (leaks detected)
    res_b = runner.invoke(app, ["leaks", "--log", str(leaking_log)])
    assert res_b.exit_code == 1
    assert "FAILURE" in res_b.output
