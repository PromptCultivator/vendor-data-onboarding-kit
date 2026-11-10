"""
Tier 2 Boundary Tests: Challenger 2 Suite for Milestone 3.
Point-In-Time Invariance & Adversarial Temporal Queries.

Empirical Adversarial Stress Tests against `as_of_query` and `audit_trail`:
1. Extreme temporal boundaries: query at exact microsecond timestamp of drop, 1us before, 1us after.
2. Leap days (2024-02-29), Year 2038 rollover, far future (9999-12-31), far past (1970-01-01), DST transitions.
3. Cascading 10-revision ladder: ingest 10 consecutive drops restating the same entity/event;
   verify `audit_trail` returns all 10 revisions in strict chronological order and `as_of_query`
   returns the exact valid revision as of each point in time.
4. SQL injection attack strings in `entity_ids`, `dataset_id`, and `metric`.
5. Mathematical invariance: assert H_1 == H_2 across all test permutations with floating-point tolerance 10^-9.
6. Additional stress: concurrency, immutability, empty partitions, and connection cleanup.
"""

from __future__ import annotations

import concurrent.futures
import os
import stat
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Any

import duckdb
import pyarrow as pa
import pytest

from vendor_onboarding.storage.store import BitemporalStore, ImmutabilityError
from vendor_onboarding.storage.query import CANONICAL_AS_OF_SCHEMA, CANONICAL_AUDIT_SCHEMA

pytestmark = [pytest.mark.tier2]


def assert_pit_invariance(h1: pa.Table, h2: pa.Table, tol: float = 1e-9) -> None:
    """Assert mathematical equivalence between H1 and H2 with float tolerance 1e-9."""
    assert h1.num_rows == h2.num_rows, f"Row count mismatch: {h1.num_rows} != {h2.num_rows}"
    if h1.num_rows == 0:
        return

    r1 = h1.to_pylist()
    r2 = h2.to_pylist()

    key_fn = lambda r: (r["entity_id"], str(r["event_time"]), r.get("metric", ""))
    map1 = {key_fn(r): r for r in r1}
    map2 = {key_fn(r): r for r in r2}

    assert set(map1.keys()) == set(map2.keys()), (
        f"Key set mismatch: {set(map1.keys()) ^ set(map2.keys())}"
    )

    for k, row1 in map1.items():
        row2 = map2[k]
        for field in row1:
            v1 = row1[field]
            v2 = row2.get(field)
            if isinstance(v1, float) and isinstance(v2, float):
                diff = abs(v1 - v2)
                assert diff <= tol, (
                    f"Float diff {diff} > {tol} for key {k}, field {field}: {v1} vs {v2}"
                )
            else:
                assert v1 == v2, f"Value mismatch for key {k}, field {field}: {v1!r} vs {v2!r}"


# ==============================================================================
# 1. Extreme Temporal Boundaries (Microsecond Precision)
# ==============================================================================


class TestExtremeMicrosecondBoundaries:
    """Microsecond resolution boundary attacks on as_of_query."""

    def test_single_microsecond_boundary_transitions(self, tmp_path: Path) -> None:
        """Verify query 1us before, exact us, and 1us after drop knowledge_time."""
        store = BitemporalStore(root_dir=tmp_path / "store_us")
        ds = "EQ_MICROSECOND"
        t_event = datetime(2023, 10, 1, 16, 0, 0, tzinfo=UTC)
        t_known = datetime(2023, 10, 1, 20, 0, 0, 500000, tzinfo=UTC)

        tbl = pa.Table.from_pydict({
            "entity_id": ["AAPL"],
            "event_time": [t_event],
            "knowledge_time": [t_known],
            "metric": ["CLOSE_PRICE"],
            "value": [150.0],
            "unit": ["USD"],
            "source_symbol": ["AAPL"],
            "source_symbol_type": ["TICKER"],
            "vendor_id": ["V1"],
            "dataset_id": [ds],
            "is_restatement": [False],
            "revision_seq": [1],
            "drop_id": ["DROP_001"],
        })
        store.append(tbl, dataset_id=ds, drop_id="DROP_001")

        # 1 microsecond before: drop should NOT be visible
        q_before = store.as_of_query(ds, as_of_time=t_known - timedelta(microseconds=1))
        assert q_before.num_rows == 0

        # Exact microsecond: drop MUST be visible
        q_exact = store.as_of_query(ds, as_of_time=t_known)
        assert q_exact.num_rows == 1
        assert q_exact["value"][0].as_py() == 150.0

        # 1 microsecond after: drop MUST be visible
        q_after = store.as_of_query(ds, as_of_time=t_known + timedelta(microseconds=1))
        assert q_after.num_rows == 1
        assert q_after["value"][0].as_py() == 150.0

        assert_pit_invariance(q_exact, q_after)

    def test_consecutive_drops_separated_by_one_microsecond(self, tmp_path: Path) -> None:
        """Verify 3 consecutive restatements separated by exactly 1 microsecond each."""
        store = BitemporalStore(root_dir=tmp_path / "store_consec_us")
        ds = "EQ_CONSEC_US"
        t_event = datetime(2023, 10, 1, 16, 0, 0, tzinfo=UTC)
        t_base = datetime(2023, 10, 1, 20, 0, 0, 100000, tzinfo=UTC)

        for i in range(3):
            t_drop = t_base + timedelta(microseconds=i)
            tbl = pa.Table.from_pydict({
                "entity_id": ["AAPL"],
                "event_time": [t_event],
                "knowledge_time": [t_drop],
                "metric": ["CLOSE_PRICE"],
                "value": [100.0 + i],
                "unit": ["USD"],
                "source_symbol": ["AAPL"],
                "source_symbol_type": ["TICKER"],
                "vendor_id": ["V1"],
                "dataset_id": [ds],
                "is_restatement": [i > 0],
                "revision_seq": [i + 1],
                "drop_id": [f"DROP_{i+1:03d}"],
            })
            store.append(tbl, dataset_id=ds, drop_id=f"DROP_{i+1:03d}")

        # Query before drop 1
        assert store.as_of_query(ds, as_of_time=t_base - timedelta(microseconds=1)).num_rows == 0

        # Query at exact drop 1 (t_base)
        q1 = store.as_of_query(ds, as_of_time=t_base)
        assert q1["value"][0].as_py() == 100.0
        assert q1["revision_seq"][0].as_py() == 1

        # Query at exact drop 2 (t_base + 1us)
        q2 = store.as_of_query(ds, as_of_time=t_base + timedelta(microseconds=1))
        assert q2["value"][0].as_py() == 101.0
        assert q2["revision_seq"][0].as_py() == 2

        # Query at exact drop 3 (t_base + 2us)
        q3 = store.as_of_query(ds, as_of_time=t_base + timedelta(microseconds=2))
        assert q3["value"][0].as_py() == 102.0
        assert q3["revision_seq"][0].as_py() == 3


# ==============================================================================
# 2. Leap Days, Year 2038 Rollover, Far Future, Far Past, and DST Transitions
# ==============================================================================


class TestExtremeCalendarAndClockBoundaries:
    """Extreme calendar boundaries, 32-bit epoch rollover, and timezone normalization."""

    def test_leap_day_and_century_leap_days(self, tmp_path: Path) -> None:
        """Verify leap day (2024-02-29) and century leap year (2000-02-29)."""
        store = BitemporalStore(root_dir=tmp_path / "store_leap")
        ds = "CALENDAR_EXTREMES"

        t_leap_2024_ev = datetime(2024, 2, 29, 16, 0, 0, tzinfo=UTC)
        t_leap_2024_kt = datetime(2024, 2, 29, 23, 59, 59, 999999, tzinfo=UTC)

        t_leap_2000_ev = datetime(2000, 2, 29, 16, 0, 0, tzinfo=UTC)
        t_leap_2000_kt = datetime(2000, 2, 29, 20, 0, 0, tzinfo=UTC)

        tbl = pa.Table.from_pydict({
            "entity_id": ["LEAP_2024", "LEAP_2000"],
            "event_time": [t_leap_2024_ev, t_leap_2000_ev],
            "knowledge_time": [t_leap_2024_kt, t_leap_2000_kt],
            "metric": ["VAL", "VAL"],
            "value": [2024.29, 2000.29],
            "unit": ["USD", "USD"],
            "source_symbol": ["L24", "L00"],
            "source_symbol_type": ["TICKER", "TICKER"],
            "vendor_id": ["V_CAL", "V_CAL"],
            "dataset_id": [ds, ds],
            "is_restatement": [False, False],
            "revision_seq": [1, 1],
            "drop_id": ["DROP_001", "DROP_001"],
        })
        store.append(tbl, dataset_id=ds, drop_id="DROP_001")

        # Leap 2024 exact us
        res = store.as_of_query(ds, as_of_time=t_leap_2024_kt, entity_ids=["LEAP_2024"])
        assert res.num_rows == 1
        assert res["value"][0].as_py() == 2024.29

        # Leap 2024 1us before
        res_bef = store.as_of_query(
            ds, as_of_time=t_leap_2024_kt - timedelta(microseconds=1), entity_ids=["LEAP_2024"]
        )
        assert res_bef.num_rows == 0

        # Leap 2000 query
        res_2000 = store.as_of_query(ds, as_of_time=t_leap_2000_kt, entity_ids=["LEAP_2000"])
        assert res_2000.num_rows == 1
        assert res_2000["value"][0].as_py() == 2000.29

    def test_year_2038_rollover_boundary(self, tmp_path: Path) -> None:
        """Verify 32-bit signed UNIX epoch rollover at 2038-01-19 03:14:07 UTC."""
        store = BitemporalStore(root_dir=tmp_path / "store_2038")
        ds = "EPOCH_2038"

        # 2^31 - 1 is 2038-01-19 03:14:07 UTC; 2^31 is 03:14:08 UTC
        t_2038_rollover = datetime(2038, 1, 19, 3, 14, 8, tzinfo=UTC)
        tbl = pa.Table.from_pydict({
            "entity_id": ["ENT_2038"],
            "event_time": [datetime(2038, 1, 19, 0, 0, 0, tzinfo=UTC)],
            "knowledge_time": [t_2038_rollover],
            "metric": ["CLOSE_PRICE"],
            "value": [2038.5],
            "unit": ["USD"],
            "source_symbol": ["E38"],
            "source_symbol_type": ["TICKER"],
            "vendor_id": ["V38"],
            "dataset_id": [ds],
            "is_restatement": [False],
            "revision_seq": [1],
            "drop_id": ["DROP_001"],
        })
        store.append(tbl, dataset_id=ds, drop_id="DROP_001")

        # Before rollover
        res_bef = store.as_of_query(ds, as_of_time=t_2038_rollover - timedelta(microseconds=1))
        assert res_bef.num_rows == 0

        # At rollover
        res_at = store.as_of_query(ds, as_of_time=t_2038_rollover)
        assert res_at.num_rows == 1
        assert res_at["value"][0].as_py() == 2038.5

        # Years after rollover: 2050
        res_post = store.as_of_query(ds, as_of_time=datetime(2050, 1, 1, 0, 0, 0, tzinfo=UTC))
        assert res_post.num_rows == 1
        assert_pit_invariance(res_at, res_post)

    def test_far_future_9999_and_far_past_1970_pre_epoch(self, tmp_path: Path) -> None:
        """Verify far future 9999-12-31, Unix epoch 1970-01-01, and pre-epoch dates."""
        store = BitemporalStore(root_dir=tmp_path / "store_far")
        ds = "FAR_DATES"

        t_9999_kt = datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)
        t_1970_kt = datetime(1970, 1, 1, 0, 0, 0, 0, tzinfo=UTC)
        t_1969_kt = datetime(1969, 7, 20, 20, 17, 0, tzinfo=UTC)  # Pre-epoch

        tbl = pa.Table.from_pydict({
            "entity_id": ["FAR_FUTURE", "EPOCH_1970", "PRE_EPOCH"],
            "event_time": [
                datetime(9999, 12, 31, 12, 0, 0, tzinfo=UTC),
                datetime(1970, 1, 1, 0, 0, 0, tzinfo=UTC),
                datetime(1969, 7, 20, 12, 0, 0, tzinfo=UTC),
            ],
            "knowledge_time": [t_9999_kt, t_1970_kt, t_1969_kt],
            "metric": ["VAL"] * 3,
            "value": [9999.0, 1970.0, 1969.0],
            "unit": ["USD"] * 3,
            "source_symbol": ["FF", "E70", "PE"],
            "source_symbol_type": ["TICKER"] * 3,
            "vendor_id": ["V_FAR"] * 3,
            "dataset_id": [ds] * 3,
            "is_restatement": [False] * 3,
            "revision_seq": [1] * 3,
            "drop_id": ["DROP_001"] * 3,
        })
        store.append(tbl, dataset_id=ds, drop_id="DROP_001")

        # Query 9999
        res_9999 = store.as_of_query(ds, as_of_time=t_9999_kt, entity_ids=["FAR_FUTURE"])
        assert res_9999.num_rows == 1
        assert res_9999["value"][0].as_py() == 9999.0

        # Query 1970
        res_1970 = store.as_of_query(ds, as_of_time=t_1970_kt, entity_ids=["EPOCH_1970"])
        assert res_1970.num_rows == 1
        assert res_1970["value"][0].as_py() == 1970.0

        # Query pre-epoch 1969
        res_1969 = store.as_of_query(ds, as_of_time=t_1969_kt, entity_ids=["PRE_EPOCH"])
        assert res_1969.num_rows == 1
        assert res_1969["value"][0].as_py() == 1969.0

        # Query before 1969 returns 0
        res_deep = store.as_of_query(
            ds, as_of_time=datetime(1900, 1, 1, 0, 0, 0, tzinfo=UTC), entity_ids=["PRE_EPOCH"]
        )
        assert res_deep.num_rows == 0

    def test_dst_transitions_and_timezone_invariance(self, tmp_path: Path) -> None:
        """Verify DST transitions and multi-timezone query invariance."""
        store = BitemporalStore(root_dir=tmp_path / "store_dst")
        ds = "DST_TEST"

        t_ev = datetime(2023, 11, 5, 16, 0, 0, tzinfo=UTC)
        t_kt_utc = datetime(2023, 11, 5, 20, 0, 0, tzinfo=UTC)

        tbl = pa.Table.from_pydict({
            "entity_id": ["DST_ENT"],
            "event_time": [t_ev],
            "knowledge_time": [t_kt_utc],
            "metric": ["CLOSE_PRICE"],
            "value": [100.0],
            "unit": ["USD"],
            "source_symbol": ["DST"],
            "source_symbol_type": ["TICKER"],
            "vendor_id": ["V_DST"],
            "dataset_id": [ds],
            "is_restatement": [False],
            "revision_seq": [1],
            "drop_id": ["DROP_001"],
        })
        store.append(tbl, dataset_id=ds, drop_id="DROP_001")

        # Query in UTC
        h_utc = store.as_of_query(ds, as_of_time=t_kt_utc)
        assert h_utc.num_rows == 1

        # Query equivalent time in America/New_York (EST: UTC-5)
        ny_tz = ZoneInfo("America/New_York")
        t_ny = t_kt_utc.astimezone(ny_tz)
        h_ny = store.as_of_query(ds, as_of_time=t_ny)
        assert_pit_invariance(h_utc, h_ny)

        # Query equivalent time in Asia/Tokyo (JST: UTC+9)
        tokyo_tz = ZoneInfo("Asia/Tokyo")
        t_tokyo = t_kt_utc.astimezone(tokyo_tz)
        h_tokyo = store.as_of_query(ds, as_of_time=t_tokyo)
        assert_pit_invariance(h_utc, h_tokyo)

        # Query with naive datetime assumes UTC
        t_naive = t_kt_utc.replace(tzinfo=None)
        h_naive = store.as_of_query(ds, as_of_time=t_naive)
        assert_pit_invariance(h_utc, h_naive)


# ==============================================================================
# 3. Cascading 10-Revision Ladder
# ==============================================================================


class TestCascading10RevisionLadder:
    """10-step sequential restatement ladder verifying audit_trail and as_of_query."""

    def test_cascading_10_revision_ladder_audit_and_pit(self, tmp_path: Path) -> None:
        """Ingest 10 drops; verify audit_trail ordering and point-in-time state at each step."""
        store = BitemporalStore(root_dir=tmp_path / "store_ladder")
        ds = "EQUITY_LADDER_10"
        t_ev = datetime(2023, 10, 1, 16, 0, 0, tzinfo=UTC)

        n_revs = 10
        kts = [
            datetime(2023, 10, 1 + i, 20, 0, 0, 100000 * i, tzinfo=UTC)
            for i in range(n_revs)
        ]
        vals = [150.0 + i * 1.25 for i in range(n_revs)]
        h1_snapshots: list[tuple[datetime, float, int, pa.Table]] = []

        for r in range(1, n_revs + 1):
            drop_id = f"DROP_{r:03d}"
            kt = kts[r - 1]
            val = vals[r - 1]
            tbl = pa.Table.from_pydict({
                "entity_id": ["ENT_LADDER"],
                "event_time": [t_ev],
                "knowledge_time": [kt],
                "metric": ["CLOSE_PRICE"],
                "value": [val],
                "unit": ["USD"],
                "source_symbol": ["LAD"],
                "source_symbol_type": ["TICKER"],
                "vendor_id": ["V_LAD"],
                "dataset_id": [ds],
                "is_restatement": [r > 1],
                "revision_seq": [r],
                "drop_id": [drop_id],
            })
            store.append(tbl, dataset_id=ds, drop_id=drop_id)

            # Query at current kt -> H1 snapshot
            h1 = store.as_of_query(ds, as_of_time=kt, entity_ids=["ENT_LADDER"])
            assert h1.num_rows == 1
            assert h1["value"][0].as_py() == val
            assert h1["revision_seq"][0].as_py() == r
            h1_snapshots.append((kt, val, r, h1))

        # ---------------------------------------------------------
        # Audit Trail Verification
        # ---------------------------------------------------------
        trail = store.audit_trail(ds, entity_id="ENT_LADDER", event_time=t_ev, metric="CLOSE_PRICE")
        assert trail.num_rows == 10

        # Monotonicity checks
        assert trail["revision_seq"].to_pylist() == list(range(1, 11))
        assert trail["value"].to_pylist() == vals
        assert trail["drop_id"].to_pylist() == [f"DROP_{i:03d}" for i in range(1, 11)]
        assert trail["is_restatement"].to_pylist() == [False] + [True] * 9

        trail_kts = trail["knowledge_time"].to_pylist()
        assert trail_kts == sorted(trail_kts), "Knowledge times must strictly increase"

        # ---------------------------------------------------------
        # Point-in-Time Zero Look-Ahead Bias Verification
        # ---------------------------------------------------------
        # Before drop 1
        assert store.as_of_query(ds, as_of_time=kts[0] - timedelta(microseconds=1)).num_rows == 0

        # At each historical drop point, assert H1 == H2 invariant
        for kt, expected_val, expected_rev, h1 in h1_snapshots:
            h2 = store.as_of_query(ds, as_of_time=kt, entity_ids=["ENT_LADDER"])
            assert_pit_invariance(h1, h2, tol=1e-9)

            # 1 us before kt
            h_bef = store.as_of_query(
                ds, as_of_time=kt - timedelta(microseconds=1), entity_ids=["ENT_LADDER"]
            )
            if expected_rev == 1:
                assert h_bef.num_rows == 0
            else:
                assert h_bef.num_rows == 1
                assert h_bef["revision_seq"][0].as_py() == expected_rev - 1
                assert h_bef["value"][0].as_py() == vals[expected_rev - 2]

            # 1 us after kt
            h_aft = store.as_of_query(
                ds, as_of_time=kt + timedelta(microseconds=1), entity_ids=["ENT_LADDER"]
            )
            assert h_aft.num_rows == 1
            assert h_aft["revision_seq"][0].as_py() == expected_rev
            assert h_aft["value"][0].as_py() == expected_val

        # Query far future: should return revision 10
        h_future = store.as_of_query(
            ds, as_of_time=datetime(2030, 1, 1, 0, 0, 0, tzinfo=UTC), entity_ids=["ENT_LADDER"]
        )
        assert h_future.num_rows == 1
        assert h_future["revision_seq"][0].as_py() == 10
        assert h_future["value"][0].as_py() == vals[-1]


# ==============================================================================
# 4. SQL Injection Resistance
# ==============================================================================


class TestSqlInjectionResistance:
    """Rigorous SQL injection adversarial attacks on all query parameters."""

    @pytest.fixture
    def populated_store(self, tmp_path: Path) -> tuple[BitemporalStore, str]:
        store = BitemporalStore(root_dir=tmp_path / "store_sqli")
        ds = "SECURITY_TEST"
        t_ev = datetime(2023, 10, 1, 16, 0, 0, tzinfo=UTC)
        t_kt = datetime(2023, 10, 1, 20, 0, 0, tzinfo=UTC)

        tbl = pa.Table.from_pydict({
            "entity_id": ["LEGIT_1", "LEGIT_2", "O'REILLY", "SYM\"QUOTE", "SEMI;COLON"],
            "event_time": [t_ev] * 5,
            "knowledge_time": [t_kt] * 5,
            "metric": ["CLOSE_PRICE", "VOLUME", "CLOSE'PRICE", "EV;METRIC", "RAW"],
            "value": [10.0, 20.0, 30.0, 40.0, 50.0],
            "unit": ["USD"] * 5,
            "source_symbol": ["S1", "S2", "S3", "S4", "S5"],
            "source_symbol_type": ["TICKER"] * 5,
            "vendor_id": ["V_SEC"] * 5,
            "dataset_id": [ds] * 5,
            "is_restatement": [False] * 5,
            "revision_seq": [1] * 5,
            "drop_id": ["DROP_001"] * 5,
        })
        store.append(tbl, dataset_id=ds, drop_id="DROP_001")
        return store, ds

    @pytest.mark.parametrize(
        "payload",
        [
            "' OR '1'='1",
            "'; DROP TABLE dummy; --",
            "LEGIT_1' UNION SELECT * FROM (SELECT 1) --",
            "'; ATTACH 'malicious.db'; --",
            "' OR 1=1 --",
            "''''''''",
            "\\\\'; DROP TABLE students; --",
            "\" OR \"\"=\"",
            "LEGIT_1' AND 1=0 UNION ALL SELECT * FROM read_parquet('/*') --",
            "-- comment line\nLEGIT_1",
            "/* block comment */ LEGIT_1",
        ],
    )
    def test_sql_injection_in_entity_ids(
        self, populated_store: tuple[BitemporalStore, str], payload: str
    ) -> None:
        """Ensure malicious entity_ids payloads do not execute arbitrary SQL or return unrequested rows."""
        store, ds = populated_store
        t_kt = datetime(2023, 10, 1, 20, 0, 0, tzinfo=UTC)
        res = store.as_of_query(ds, as_of_time=t_kt, entity_ids=[payload])
        assert res.num_rows == 0

    @pytest.mark.parametrize(
        "payload",
        [
            "CLOSE_PRICE' OR '1'='1",
            "'; DROP TABLE dummy; --",
            "CLOSE_PRICE' UNION SELECT ... --",
            "'''",
            "/* comment */ CLOSE_PRICE",
        ],
    )
    def test_sql_injection_in_metric(
        self, populated_store: tuple[BitemporalStore, str], payload: str
    ) -> None:
        """Ensure malicious metric payloads do not bypass metric filtering or execute arbitrary SQL."""
        store, ds = populated_store
        t_kt = datetime(2023, 10, 1, 20, 0, 0, tzinfo=UTC)
        res = store.as_of_query(ds, as_of_time=t_kt, metric=payload)
        assert res.num_rows == 0

    @pytest.mark.parametrize(
        "payload",
        [
            "SECURITY_TEST'; DROP TABLE data; --",
            "../../etc/passwd",
            "NONEXISTENT' OR '1'='1",
            "'; ATTACH ':memory:'; --",
        ],
    )
    def test_sql_injection_in_dataset_id(
        self, populated_store: tuple[BitemporalStore, str], payload: str
    ) -> None:
        """Ensure path traversal or SQL injection in dataset_id returns empty canonical schema gracefully."""
        store, _ = populated_store
        t_kt = datetime(2023, 10, 1, 20, 0, 0, tzinfo=UTC)
        res = store.as_of_query(payload, as_of_time=t_kt)
        assert res.num_rows == 0
        assert "entity_id" in res.column_names

    def test_legitimate_special_characters_handling(
        self, populated_store: tuple[BitemporalStore, str]
    ) -> None:
        """Verify entities and metrics containing quotes and semicolons are queried accurately."""
        store, ds = populated_store
        t_kt = datetime(2023, 10, 1, 20, 0, 0, tzinfo=UTC)

        res_oreilly = store.as_of_query(ds, as_of_time=t_kt, entity_ids=["O'REILLY"])
        assert res_oreilly.num_rows == 1
        assert res_oreilly["value"][0].as_py() == 30.0

        res_quote = store.as_of_query(ds, as_of_time=t_kt, entity_ids=["SYM\"QUOTE"])
        assert res_quote.num_rows == 1

        res_semi = store.as_of_query(ds, as_of_time=t_kt, entity_ids=["SEMI;COLON"])
        assert res_semi.num_rows == 1

        res_metric = store.as_of_query(ds, as_of_time=t_kt, metric="CLOSE'PRICE")
        assert res_metric.num_rows == 1
        assert res_metric["entity_id"][0].as_py() == "O'REILLY"

    def test_audit_trail_sql_injection_resistance(
        self, populated_store: tuple[BitemporalStore, str]
    ) -> None:
        """Verify audit_trail parameters neutralize SQL injection payloads."""
        store, ds = populated_store
        t_ev = datetime(2023, 10, 1, 16, 0, 0, tzinfo=UTC)

        # Entity injection in audit_trail
        res_ent = store.audit_trail(ds, entity_id="LEGIT_1' OR 1=1 --", event_time=t_ev)
        assert res_ent.num_rows == 0

        # Metric injection in audit_trail
        res_m = store.audit_trail(
            ds, entity_id="LEGIT_1", event_time=t_ev, metric="CLOSE_PRICE'; DROP TABLE foo;--"
        )
        assert res_m.num_rows == 0

        # Legitimate single quote in audit_trail
        res_legit = store.audit_trail(
            ds, entity_id="O'REILLY", event_time=t_ev, metric="CLOSE'PRICE"
        )
        assert res_legit.num_rows == 1
        assert res_legit["value"][0].as_py() == 30.0


# ==============================================================================
# 5. Mathematical Invariance: H_1 == H_2 with tolerance 10^-9
# ==============================================================================


class TestMathematicalPointInTimeInvariance:
    """Strict mathematical verification that H_1 == H_2 across multi-drop restatement permutations."""

    def test_multi_drop_restatement_permutations_h1_eq_h2(self, tmp_path: Path) -> None:
        """5 successive drops restating alternating entity subsets; verify H_1 == H_2."""
        store = BitemporalStore(root_dir=tmp_path / "store_perm")
        ds = "PERM_INVARIANCE"
        entities = [f"ENT_{i:04d}" for i in range(25)]
        t_ev = datetime(2023, 10, 1, 16, 0, 0, tzinfo=UTC)
        t_d1 = datetime(2023, 10, 1, 20, 0, 0, tzinfo=UTC)

        # Drop 1
        tbl_d1 = pa.Table.from_pydict({
            "entity_id": entities,
            "event_time": [t_ev] * len(entities),
            "knowledge_time": [t_d1] * len(entities),
            "metric": ["CLOSE_PRICE"] * len(entities),
            "value": [100.0 + i * 0.1234567890123 for i in range(len(entities))],
            "unit": ["USD"] * len(entities),
            "source_symbol": [f"SYM_{i}" for i in range(len(entities))],
            "source_symbol_type": ["TICKER"] * len(entities),
            "vendor_id": ["V_PERM"] * len(entities),
            "dataset_id": [ds] * len(entities),
            "is_restatement": [False] * len(entities),
            "revision_seq": [1] * len(entities),
            "drop_id": ["DROP_001"] * len(entities),
        })
        store.append(tbl_d1, dataset_id=ds, drop_id="DROP_001")

        # Snapshot H_1 at t_d1
        h1 = store.as_of_query(ds, as_of_time=t_d1)
        assert h1.num_rows == 25

        # Ingest drops 2 through 6 with cascading restatements
        for d in range(2, 7):
            t_dk = datetime(2023, 10, d, 20, 0, 0, tzinfo=UTC)
            subset = [e for idx, e in enumerate(entities) if (idx + d) % 2 == 0]
            tbl_dk = pa.Table.from_pydict({
                "entity_id": subset,
                "event_time": [t_ev] * len(subset),
                "knowledge_time": [t_dk] * len(subset),
                "metric": ["CLOSE_PRICE"] * len(subset),
                "value": [200.0 + d * 15.0 + i for i in range(len(subset))],
                "unit": ["USD"] * len(subset),
                "source_symbol": subset,
                "source_symbol_type": ["TICKER"] * len(subset),
                "vendor_id": ["V_PERM"] * len(subset),
                "dataset_id": [ds] * len(subset),
                "is_restatement": [True] * len(subset),
                "revision_seq": [d] * len(subset),
                "drop_id": [f"DROP_{d:03d}"] * len(subset),
            })
            store.append(tbl_dk, dataset_id=ds, drop_id=f"DROP_{d:03d}")

            # Verify Point-In-Time Invariance: H1 == H2 at historical t_d1
            h2 = store.as_of_query(ds, as_of_time=t_d1)
            assert_pit_invariance(h1, h2, tol=1e-9)

    def test_sparse_reporting_asynchronous_dates_invariance(self, tmp_path: Path) -> None:
        """Asynchronously reported financial metrics preserve invariant historical snapshots."""
        store = BitemporalStore(root_dir=tmp_path / "store_async")
        ds = "ASYNC_FUNDAMENTALS"

        # Three entities filing on different dates
        # AAPL filed Oct 2; MSFT filed Oct 5; GOOG filed Oct 8
        t_ev = datetime(2023, 9, 30, 16, 0, 0, tzinfo=UTC)
        t_aapl = datetime(2023, 10, 2, 20, 0, 0, tzinfo=UTC)
        t_msft = datetime(2023, 10, 5, 20, 0, 0, tzinfo=UTC)
        t_goog = datetime(2023, 10, 8, 20, 0, 0, tzinfo=UTC)

        def make_row(ent, kt, val, drop):
            return pa.Table.from_pydict({
                "entity_id": [ent],
                "event_time": [t_ev],
                "knowledge_time": [kt],
                "metric": ["REVENUE"],
                "value": [val],
                "unit": ["USD"],
                "source_symbol": [ent.replace("ENT_", "")],
                "source_symbol_type": ["TICKER"],
                "vendor_id": ["V_SEC"],
                "dataset_id": [ds],
                "is_restatement": [False],
                "revision_seq": [1],
                "drop_id": [drop],
            })

        store.append(make_row("ENT_AAPL", t_aapl, 89500.0, "DROP_AAPL"), dataset_id=ds, drop_id="DROP_AAPL")
        # Snapshot at Oct 3 (only AAPL known)
        t_snap_oct3 = datetime(2023, 10, 3, 12, 0, 0, tzinfo=UTC)
        h1_oct3 = store.as_of_query(ds, as_of_time=t_snap_oct3)
        assert h1_oct3.num_rows == 1
        assert h1_oct3["entity_id"][0].as_py() == "ENT_AAPL"

        # Ingest MSFT on Oct 5
        store.append(make_row("ENT_MSFT", t_msft, 56500.0, "DROP_MSFT"), dataset_id=ds, drop_id="DROP_MSFT")
        # Ingest GOOG on Oct 8
        store.append(make_row("ENT_GOOG", t_goog, 76700.0, "DROP_GOOG"), dataset_id=ds, drop_id="DROP_GOOG")

        # Re-query at Oct 3: H1 == H2 invariant must hold (MSFT and GOOG must NOT appear!)
        h2_oct3 = store.as_of_query(ds, as_of_time=t_snap_oct3)
        assert_pit_invariance(h1_oct3, h2_oct3, tol=1e-9)

        # Snapshot at Oct 6 (AAPL and MSFT known, GOOG unknown)
        t_snap_oct6 = datetime(2023, 10, 6, 12, 0, 0, tzinfo=UTC)
        h_oct6 = store.as_of_query(ds, as_of_time=t_snap_oct6)
        assert h_oct6.num_rows == 2
        assert set(h_oct6["entity_id"].to_pylist()) == {"ENT_AAPL", "ENT_MSFT"}

        # Snapshot at Oct 9 (all three known)
        t_snap_oct9 = datetime(2023, 10, 9, 12, 0, 0, tzinfo=UTC)
        h_oct9 = store.as_of_query(ds, as_of_time=t_snap_oct9)
        assert h_oct9.num_rows == 3


# ==============================================================================
# 6. Concurrency, Immutability, Empty Partitions, and Connection Cleanup
# ==============================================================================


class TestStoreConcurrencyAndImmutability:
    """Extreme concurrency, physical file immutability, and resource management."""

    def test_physical_immutability_and_readonly_chmod(self, tmp_path: Path) -> None:
        """Attempting to overwrite existing partition raises ImmutabilityError and permissions are 0o444."""
        store = BitemporalStore(root_dir=tmp_path / "store_immut")
        ds = "IMMUT_TEST"
        t_ev = datetime(2023, 10, 1, 16, 0, 0, tzinfo=UTC)
        t_kt = datetime(2023, 10, 1, 20, 0, 0, tzinfo=UTC)

        tbl = pa.Table.from_pydict({
            "entity_id": ["ENT_1"],
            "event_time": [t_ev],
            "knowledge_time": [t_kt],
            "metric": ["PRICE"],
            "value": [10.0],
            "unit": ["USD"],
            "source_symbol": ["E1"],
            "source_symbol_type": ["TICKER"],
            "vendor_id": ["V1"],
            "dataset_id": [ds],
            "is_restatement": [False],
            "revision_seq": [1],
            "drop_id": ["DROP_001"],
        })
        store.append(tbl, dataset_id=ds, drop_id="DROP_001")

        parquet_file = tmp_path / "store_immut" / ds / "drop_id=DROP_001" / "data.parquet"
        assert parquet_file.exists()

        # Check read-only permissions (0o444, no write bits)
        file_mode = stat.S_IMODE(os.stat(parquet_file).st_mode)
        assert not (file_mode & 0o222), f"Write permissions detected: {oct(file_mode)}"

        # Attempt to append to same drop_id must raise ImmutabilityError
        with pytest.raises(ImmutabilityError, match="Partition already exists and is immutable"):
            store.append(tbl, dataset_id=ds, drop_id="DROP_001")

    def test_concurrent_queries_under_multithreaded_load(self, tmp_path: Path) -> None:
        """Verify thread-safety of BitemporalStore queries under concurrent threads."""
        store = BitemporalStore(root_dir=tmp_path / "store_concurrent")
        ds = "CONCURRENT_TEST"
        t_ev = datetime(2023, 10, 1, 16, 0, 0, tzinfo=UTC)
        t_kt = datetime(2023, 10, 1, 20, 0, 0, tzinfo=UTC)

        entities = [f"ENT_{i}" for i in range(50)]
        tbl = pa.Table.from_pydict({
            "entity_id": entities,
            "event_time": [t_ev] * len(entities),
            "knowledge_time": [t_kt] * len(entities),
            "metric": ["CLOSE_PRICE"] * len(entities),
            "value": [100.0 + i for i in range(len(entities))],
            "unit": ["USD"] * len(entities),
            "source_symbol": entities,
            "source_symbol_type": ["TICKER"] * len(entities),
            "vendor_id": ["V_CONC"] * len(entities),
            "dataset_id": [ds] * len(entities),
            "is_restatement": [False] * len(entities),
            "revision_seq": [1] * len(entities),
            "drop_id": ["DROP_001"] * len(entities),
        })
        store.append(tbl, dataset_id=ds, drop_id="DROP_001")

        def query_worker(thread_id: int) -> int:
            for _ in range(15):
                res = store.as_of_query(ds, as_of_time=t_kt)
                assert res.num_rows == 50
                trail = store.audit_trail(ds, entity_id="ENT_10", event_time=t_ev)
                assert trail.num_rows == 1
            return thread_id

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(query_worker, i) for i in range(8)]
            results = [f.result() for f in futures]

        assert len(results) == 8
        store.close()

    def test_empty_dataset_and_missing_directory_queries(self, tmp_path: Path) -> None:
        """Verify query against non-existent dataset returns CANONICAL schema without error."""
        store = BitemporalStore(root_dir=tmp_path / "store_empty")
        t_query = datetime(2023, 10, 1, 20, 0, 0, tzinfo=UTC)

        empty_as_of = store.as_of_query("NON_EXISTENT_DATASET", as_of_time=t_query)
        assert empty_as_of.num_rows == 0
        assert empty_as_of.schema == CANONICAL_AS_OF_SCHEMA

        empty_audit = store.audit_trail(
            "NON_EXISTENT_DATASET", entity_id="ENT_1", event_time=t_query
        )
        assert empty_audit.num_rows == 0
        assert empty_audit.schema == CANONICAL_AUDIT_SCHEMA
