"""
Tier 2 Boundary Tests: Challenger 1 Suite for Milestone 2.

Empirical Adversarial Stress Tests covering:
1. CanonicalRecord and Arrow conversion:
   - Naive datetimes (must raise ValueError).
   - Offset datetimes (e.g. UTC+5, UTC-8, DST boundaries) properly normalized to UTC.
   - Unicode symbols (emojis, CJK, special characters) round-tripping through Arrow.
   - Extreme float values (NaN, Inf, 1e18, 1e-18, subnormals, signed zero).
2. Symbology & Quarantine:
   - Point-in-time exact boundary conditions: query at effective_start (must match),
     query at effective_end (must NOT match).
   - CIK integer vs string zero padding (320193 vs 0000320193).
   - Unmapped symbols quarantine generation and ledger verification.
   - Bulk resolution with 10,000+ entries under concurrent load.
"""

from __future__ import annotations

import concurrent.futures
import math
import time
from datetime import UTC, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any

import pyarrow as pa
import pytest

from vendor_onboarding.models.record import (
    CanonicalRecord,
    enforce_utc_datetime,
    records_to_arrow,
    arrow_to_records,
    CANONICAL_ARROW_SCHEMA,
)
from vendor_onboarding.symbology.master import (
    SymbologyMaster,
    SymbologyMapping,
    QuarantineRecord,
    _cik_matches,
)

pytestmark = [pytest.mark.tier2]


# ============================================================================
# 1. CanonicalRecord Temporal Validation & Normalization Adversarial Tests
# ============================================================================


class TestCanonicalRecordTemporalAdversarial:
    """Adversarial stress tests for datetime timezone enforcement and UTC normalization."""

    def test_naive_datetime_rejection_in_enforce_utc_datetime(self) -> None:
        """enforce_utc_datetime must strictly raise ValueError on timezone-naive datetime."""
        naive_dts = [
            datetime(2023, 1, 1),
            datetime(2024, 2, 29, 23, 59, 59),  # Leap day
            datetime(1970, 1, 1, 0, 0, 0),       # Unix epoch naive
            datetime.min,                        # Year 0001
            datetime.max,                        # Year 9999
        ]
        for dt in naive_dts:
            with pytest.raises(ValueError, match="Naive datetime rejected; explicit UTC timezone required."):
                enforce_utc_datetime(dt)

    def test_canonical_record_rejects_naive_event_time(self) -> None:
        """CanonicalRecord must reject naive event_time with ValueError."""
        with pytest.raises(ValueError, match="Naive datetime rejected; explicit UTC timezone required."):
            CanonicalRecord(
                entity_id="ENT_TEST",
                event_time=datetime(2023, 6, 15, 12, 0, 0),  # Naive
                knowledge_time=datetime(2023, 6, 15, 12, 0, 0, tzinfo=UTC),
                value=100.0,
                unit="USD",
                source_symbol="TEST",
                source_symbol_type="TICKER",
                vendor_id="V_TEST",
                dataset_id="D_TEST",
                drop_id="DROP_001",
            )

    def test_canonical_record_rejects_naive_knowledge_time(self) -> None:
        """CanonicalRecord must reject naive knowledge_time with ValueError."""
        with pytest.raises(ValueError, match="Naive datetime rejected; explicit UTC timezone required."):
            CanonicalRecord(
                entity_id="ENT_TEST",
                event_time=datetime(2023, 6, 15, 12, 0, 0, tzinfo=UTC),
                knowledge_time=datetime(2023, 6, 15, 12, 0, 0),  # Naive
                value=100.0,
                unit="USD",
                source_symbol="TEST",
                source_symbol_type="TICKER",
                vendor_id="V_TEST",
                dataset_id="D_TEST",
                drop_id="DROP_001",
            )

    def test_arrow_to_records_rejects_naive_timestamp_columns(self) -> None:
        """arrow_to_records must raise ValueError if Arrow table has naive timestamps."""
        naive_schema = pa.schema([
            ("entity_id", pa.string()),
            ("event_time", pa.timestamp("us")),  # No tz!
            ("knowledge_time", pa.timestamp("us")),  # No tz!
            ("value", pa.float64()),
            ("unit", pa.string()),
            ("source_symbol", pa.string()),
            ("source_symbol_type", pa.string()),
            ("vendor_id", pa.string()),
            ("dataset_id", pa.string()),
            ("drop_id", pa.string()),
            ("is_restatement", pa.bool_()),
            ("revision_seq", pa.int64()),
        ])
        tbl = pa.Table.from_pydict({
            "entity_id": ["E1"],
            "event_time": [datetime(2023, 1, 1, 12, 0)],
            "knowledge_time": [datetime(2023, 1, 1, 12, 0)],
            "value": [42.0],
            "unit": ["USD"],
            "source_symbol": ["SYM"],
            "source_symbol_type": ["TICKER"],
            "vendor_id": ["V"],
            "dataset_id": ["D"],
            "drop_id": ["DR"],
            "is_restatement": [False],
            "revision_seq": [1],
        }, schema=naive_schema)

        with pytest.raises(ValueError, match="Naive datetime rejected; explicit UTC timezone required."):
            arrow_to_records(tbl)

    def test_positive_offset_normalization(self) -> None:
        """Offset datetimes with positive shifts (UTC+5, UTC+5:30, UTC+9, UTC+14) normalize to UTC."""
        # UTC+5:00 (PKT)
        tz_plus5 = timezone(timedelta(hours=5))
        r1 = CanonicalRecord(
            entity_id="ENT_PKT",
            event_time=datetime(2023, 6, 1, 17, 0, tzinfo=tz_plus5),
            knowledge_time=datetime(2023, 6, 1, 17, 0, tzinfo=tz_plus5),
            value=10.0, unit="USD", source_symbol="S", source_symbol_type="T",
            vendor_id="V", dataset_id="D", drop_id="DR",
        )
        assert r1.event_time.tzinfo == UTC
        assert r1.event_time == datetime(2023, 6, 1, 12, 0, tzinfo=UTC)

        # UTC+5:30 (IST)
        ist = timezone(timedelta(hours=5, minutes=30))
        r2 = CanonicalRecord(
            entity_id="ENT_IST",
            event_time=datetime(2023, 6, 1, 15, 30, tzinfo=ist),
            knowledge_time=datetime(2023, 6, 1, 15, 30, tzinfo=ist),
            value=20.0, unit="INR", source_symbol="S", source_symbol_type="T",
            vendor_id="V", dataset_id="D", drop_id="DR",
        )
        assert r2.event_time == datetime(2023, 6, 1, 10, 0, tzinfo=UTC)

        # UTC+14:00 (Line Islands / Kiribati) - rolls back across year boundary
        tz_plus14 = timezone(timedelta(hours=14))
        r3 = CanonicalRecord(
            entity_id="ENT_KIR",
            event_time=datetime(2024, 1, 1, 8, 0, tzinfo=tz_plus14),
            knowledge_time=datetime(2024, 1, 1, 8, 0, tzinfo=tz_plus14),
            value=30.0, unit="AUD", source_symbol="S", source_symbol_type="T",
            vendor_id="V", dataset_id="D", drop_id="DR",
        )
        assert r3.event_time == datetime(2023, 12, 31, 18, 0, tzinfo=UTC)

    def test_negative_offset_normalization(self) -> None:
        """Offset datetimes with negative shifts (UTC-8, UTC-5, UTC-12) normalize to UTC."""
        # UTC-8:00 (PST)
        pst = timezone(timedelta(hours=-8))
        r1 = CanonicalRecord(
            entity_id="ENT_PST",
            event_time=datetime(2023, 1, 15, 4, 0, tzinfo=pst),
            knowledge_time=datetime(2023, 1, 15, 4, 0, tzinfo=pst),
            value=10.0, unit="USD", source_symbol="S", source_symbol_type="T",
            vendor_id="V", dataset_id="D", drop_id="DR",
        )
        assert r1.event_time.tzinfo == UTC
        assert r1.event_time == datetime(2023, 1, 15, 12, 0, tzinfo=UTC)

        # UTC-12:00 (Baker Island) - rolls forward across date boundary
        tz_minus12 = timezone(timedelta(hours=-12))
        r2 = CanonicalRecord(
            entity_id="ENT_BAKER",
            event_time=datetime(2023, 12, 31, 20, 0, tzinfo=tz_minus12),
            knowledge_time=datetime(2023, 12, 31, 20, 0, tzinfo=tz_minus12),
            value=15.0, unit="USD", source_symbol="S", source_symbol_type="T",
            vendor_id="V", dataset_id="D", drop_id="DR",
        )
        assert r2.event_time == datetime(2024, 1, 1, 8, 0, tzinfo=UTC)

    def test_dst_boundary_transitions_and_ambiguous_hours(self) -> None:
        """DST transitions (spring-forward 23h and fall-back ambiguous fold 0/1) normalize accurately."""
        ny_tz = ZoneInfo("America/New_York")

        # 1. Spring-forward (2024-03-10): 01:59:59 EST -> 03:00:00 EDT
        t_pre_spring = datetime(2024, 3, 10, 1, 59, 59, tzinfo=ny_tz)  # UTC-5
        t_post_spring = datetime(2024, 3, 10, 3, 0, 0, tzinfo=ny_tz)    # UTC-4

        r_pre = CanonicalRecord(
            entity_id="ENT_PRE",
            event_time=t_pre_spring, knowledge_time=t_pre_spring,
            value=1.0, unit="USD", source_symbol="S", source_symbol_type="T",
            vendor_id="V", dataset_id="D", drop_id="DR",
        )
        r_post = CanonicalRecord(
            entity_id="ENT_POST",
            event_time=t_post_spring, knowledge_time=t_post_spring,
            value=1.0, unit="USD", source_symbol="S", source_symbol_type="T",
            vendor_id="V", dataset_id="D", drop_id="DR",
        )
        # Pre-spring: 01:59:59 EST = 06:59:59 UTC
        # Post-spring: 03:00:00 EDT = 07:00:00 UTC (1 second apart in real world time)
        assert r_pre.event_time == datetime(2024, 3, 10, 6, 59, 59, tzinfo=UTC)
        assert r_post.event_time == datetime(2024, 3, 10, 7, 0, 0, tzinfo=UTC)
        assert (r_post.event_time - r_pre.event_time) == timedelta(seconds=1)

        # 2. Fall-back ambiguous hour (2024-11-03): 01:30 occurs twice (fold=0 EDT, fold=1 EST)
        t_fb_fold0 = datetime(2024, 11, 3, 1, 30, fold=0, tzinfo=ny_tz)  # UTC-4
        t_fb_fold1 = datetime(2024, 11, 3, 1, 30, fold=1, tzinfo=ny_tz)  # UTC-5

        r_fb0 = CanonicalRecord(
            entity_id="ENT_FB0",
            event_time=t_fb_fold0, knowledge_time=t_fb_fold0,
            value=1.0, unit="USD", source_symbol="S", source_symbol_type="T",
            vendor_id="V", dataset_id="D", drop_id="DR",
        )
        r_fb1 = CanonicalRecord(
            entity_id="ENT_FB1",
            event_time=t_fb_fold1, knowledge_time=t_fb_fold1,
            value=1.0, unit="USD", source_symbol="S", source_symbol_type="T",
            vendor_id="V", dataset_id="D", drop_id="DR",
        )
        # fold 0 is 05:30 UTC, fold 1 is 06:30 UTC
        assert r_fb0.event_time == datetime(2024, 11, 3, 5, 30, tzinfo=UTC)
        assert r_fb1.event_time == datetime(2024, 11, 3, 6, 30, tzinfo=UTC)
        assert (r_fb1.event_time - r_fb0.event_time) == timedelta(hours=1)

    def test_posix_timestamp_invariant_under_normalization(self) -> None:
        """The absolute epoch timestamp (.timestamp()) must remain identical before and after normalization."""
        for offset_hours in [-12, -8, -5, 0, 3, 5.5, 9, 12, 14]:
            hrs = int(offset_hours)
            mins = int((offset_hours - hrs) * 60)
            tz = timezone(timedelta(hours=hrs, minutes=mins))
            orig_dt = datetime(2024, 6, 15, 14, 25, 30, 123456, tzinfo=tz)

            rec = CanonicalRecord(
                entity_id="E",
                event_time=orig_dt,
                knowledge_time=orig_dt,
                value=10.0, unit="USD", source_symbol="S", source_symbol_type="T",
                vendor_id="V", dataset_id="D", drop_id="DR",
            )
            assert rec.event_time.timestamp() == orig_dt.timestamp()
            assert rec.knowledge_time.timestamp() == orig_dt.timestamp()


# ============================================================================
# 2. CanonicalRecord Arrow Conversion: Unicode & Float Extremes
# ============================================================================


class TestCanonicalRecordArrowUnicodeAndNumericExtremes:
    """Adversarial stress tests for Arrow serialization with Unicode characters and float extremes."""

    def test_unicode_symbols_roundtrip_through_arrow(self) -> None:
        """Emojis, CJK, RTL scripts, special characters, and whitespace round-trip losslessly."""
        unicode_corpus = [
            "AAPL🚀💎",
            "🔥📈💸📊",
            "₿_BITCOIN",
            "💩_POOP",
            "👩‍💻_DEV_ZWJ",                # Zero-width joiner sequence
            "🇺🇸_USA_FLAGS_🇯🇵",            # Regional indicators
            "腾讯控股有限公司_700.HK",      # Traditional Chinese
            "任天堂株式会社_7974.T",         # Japanese Kanji/Katakana
            "삼성전자_005930.KS",          # Korean Hangul
            "أرامكو السعودية_2222.SR",      # Arabic Right-to-Left
            "בנק הפועלים_POLI.TA",          # Hebrew Right-to-Left
            "Сбербанк_России_SBER.ME",      # Cyrillic
            "Münchener Rück AG",           # German umlaut
            "Ørsted A/S",                  # Scandinavian slashed O
            "São Paulo B3",                # Portuguese tilde
            "∑_SUM_∏_PROD_∆_DELTA_≈≠≤≥",   # Mathematical symbols
            "TAB\tSEPARATED\nNEWLINE\rCR",  # Control characters
            "A" * 8192,                    # Long string boundary (8KB)
        ]

        records: list[CanonicalRecord] = []
        base_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)

        for i, symbol in enumerate(unicode_corpus):
            records.append(
                CanonicalRecord(
                    entity_id=f"ENTITY_{symbol[:25]}_{i}",
                    event_time=base_time + timedelta(minutes=i),
                    knowledge_time=base_time + timedelta(minutes=i),
                    value=float(i * 100),
                    unit=f"UNIT_{symbol[:10]}",
                    source_symbol=symbol,
                    source_symbol_type="UNICODE_FEED",
                    vendor_id=f"VENDOR_{symbol[:5]}",
                    dataset_id="CORPUS_DATASET",
                    drop_id=f"DROP_{i:04d}",
                    is_restatement=(i % 2 == 1),
                    revision_seq=i + 1,
                )
            )

        table = records_to_arrow(records)
        assert table.num_rows == len(unicode_corpus)

        roundtripped = arrow_to_records(table)
        assert len(roundtripped) == len(records)

        for orig, rt in zip(records, roundtripped):
            assert orig.entity_id == rt.entity_id
            assert orig.source_symbol == rt.source_symbol
            assert orig.source_symbol_type == rt.source_symbol_type
            assert orig.unit == rt.unit
            assert orig.vendor_id == rt.vendor_id
            assert orig.dataset_id == rt.dataset_id
            assert orig.drop_id == rt.drop_id
            assert orig.event_time == rt.event_time
            assert orig.knowledge_time == rt.knowledge_time
            assert orig.is_restatement == rt.is_restatement
            assert orig.revision_seq == rt.revision_seq
            assert orig.value == rt.value

    def test_extreme_float_values_roundtrip_through_arrow(self) -> None:
        """NaN, Inf, -Inf, 1e18, 1e-18, max/min doubles, and subnormals round-trip accurately."""
        extreme_values: list[float | None] = [
            float("nan"),
            float("inf"),
            float("-inf"),
            1e18,
            -1e18,
            1e-18,
            -1e-18,
            1.7976931348623157e+308,   # Max IEEE 754 float64
            -1.7976931348623157e+308,  # Min IEEE 754 float64
            2.2250738585072014e-308,   # Smallest positive normal float64
            -2.2250738585072014e-308,
            5e-324,                    # Smallest positive subnormal float64
            0.0,
            -0.0,
            None,                      # Null value
        ]

        records: list[CanonicalRecord] = []
        base_time = datetime(2024, 1, 1, 12, 0, tzinfo=UTC)

        for i, val in enumerate(extreme_values):
            records.append(
                CanonicalRecord(
                    entity_id=f"EXTREME_VAL_{i}",
                    event_time=base_time + timedelta(seconds=i),
                    knowledge_time=base_time + timedelta(seconds=i),
                    value=val,
                    unit="RATIO",
                    source_symbol="EXTREME_METRIC",
                    source_symbol_type="METRIC",
                    vendor_id="V_NUMERIC",
                    dataset_id="D_NUMERIC",
                    drop_id="DROP_EXTREMES",
                )
            )

        table = records_to_arrow(records)
        roundtripped = arrow_to_records(table)
        assert len(roundtripped) == len(records)

        for orig, rt in zip(records, roundtripped):
            assert orig.entity_id == rt.entity_id
            if orig.value is None:
                assert rt.value is None
            elif math.isnan(orig.value):
                assert math.isnan(rt.value)
            elif math.isinf(orig.value):
                assert math.isinf(rt.value)
                assert (orig.value > 0) == (rt.value > 0)
            else:
                assert orig.value == rt.value

    def test_integer_and_empty_records_arrow_conversion(self) -> None:
        """records_to_arrow correctly converts integers to float64 and handles empty sequences."""
        # Empty sequence
        empty_table = records_to_arrow([])
        assert empty_table.num_rows == 0
        assert empty_table.schema == CANONICAL_ARROW_SCHEMA
        assert arrow_to_records(empty_table) == []

        # Integer inputs converted to float64
        int_records = [
            CanonicalRecord(
                entity_id="ENT_INT",
                event_time=datetime(2024, 1, 1, tzinfo=UTC),
                knowledge_time=datetime(2024, 1, 1, tzinfo=UTC),
                value=42,  # int
                unit="COUNT",
                source_symbol="COUNT_SYM",
                source_symbol_type="TICKER",
                vendor_id="V", dataset_id="D", drop_id="DR",
            ),
            CanonicalRecord(
                entity_id="ENT_INT2",
                event_time=datetime(2024, 1, 1, tzinfo=UTC),
                knowledge_time=datetime(2024, 1, 1, tzinfo=UTC),
                value=0,   # int zero
                unit="COUNT",
                source_symbol="COUNT_SYM2",
                source_symbol_type="TICKER",
                vendor_id="V", dataset_id="D", drop_id="DR",
            ),
        ]
        table = records_to_arrow(int_records)
        val_col = table.column("value").to_pylist()
        assert val_col == [42.0, 0.0]

        rt = arrow_to_records(table)
        assert rt[0].value == 42.0
        assert rt[1].value == 0.0


# ============================================================================
# 3. Symbology Point-in-Time Exact Boundary Conditions
# ============================================================================


class TestSymbologyPointInTimeExactBoundaries:
    """Adversarial stress tests for exact boundary conditions [effective_start, effective_end)."""

    @pytest.fixture
    def corporate_action_catalog(self) -> SymbologyMaster:
        """Catalog modeling FB -> META corporate action on 2022-06-09 00:00:00 UTC."""
        master = SymbologyMaster()
        t_fb_start = datetime(2012, 5, 18, 0, 0, tzinfo=UTC)
        t_fb_end = datetime(2022, 6, 9, 0, 0, tzinfo=UTC)
        t_meta_end = datetime(2099, 12, 31, tzinfo=UTC)

        m_fb = SymbologyMapping(
            entity_id="ENT_META",
            ticker="FB",
            cik="0001326801",
            cusip="30303M102",
            isin="US30303M1027",
            sedol="B7TL820",
            figi="BBG000MM2P62",
            effective_start=t_fb_start,
            effective_end=t_fb_end,
        )
        m_meta = SymbologyMapping(
            entity_id="ENT_META",
            ticker="META",
            cik="0001326801",
            cusip="30303M102",
            isin="US30303M1027",
            sedol="B7TL820",
            figi="BBG000MM2P62",
            effective_start=t_fb_end,
            effective_end=t_meta_end,
        )
        master.add_mapping(m_fb)
        master.add_mapping(m_meta)
        return master

    def test_effective_start_exact_microsecond_boundary(
        self, corporate_action_catalog: SymbologyMaster
    ) -> None:
        """Query at effective_start must MATCH; query at effective_start - 1us must NOT match."""
        t_start = datetime(2012, 5, 18, 0, 0, tzinfo=UTC)
        one_us = timedelta(microseconds=1)

        # 1 microsecond before start -> Must NOT match FB
        res_before = corporate_action_catalog.resolve("FB", "TICKER", t_start - one_us)
        assert res_before.startswith("UNRESOLVED_")
        assert corporate_action_catalog.lookup_cik_by_ticker("FB", t_start - one_us) is None

        # At exact start -> MUST match FB
        res_at = corporate_action_catalog.resolve("FB", "TICKER", t_start)
        assert res_at == "ENT_META"
        assert corporate_action_catalog.lookup_cik_by_ticker("FB", t_start) == "0001326801"

        # 1 microsecond after start -> MUST match FB
        res_after = corporate_action_catalog.resolve("FB", "TICKER", t_start + one_us)
        assert res_after == "ENT_META"

    def test_effective_end_exact_microsecond_boundary_and_handover(
        self, corporate_action_catalog: SymbologyMaster
    ) -> None:
        """Query at effective_end must NOT match prior mapping; hand-off to new mapping must be exact."""
        t_transition = datetime(2022, 6, 9, 0, 0, tzinfo=UTC)
        one_us = timedelta(microseconds=1)

        # 1 microsecond before transition: FB is active, META is NOT yet active
        assert corporate_action_catalog.resolve("FB", "TICKER", t_transition - one_us) == "ENT_META"
        res_meta_early = corporate_action_catalog.resolve("META", "TICKER", t_transition - one_us)
        assert res_meta_early.startswith("UNRESOLVED_")
        assert corporate_action_catalog.lookup_ticker_by_cik("0001326801", t_transition - one_us) == "FB"

        # At exact transition: FB ceases to match; META becomes active!
        res_fb_end = corporate_action_catalog.resolve("FB", "TICKER", t_transition)
        assert res_fb_end.startswith("UNRESOLVED_")  # FB expired
        assert corporate_action_catalog.resolve("META", "TICKER", t_transition) == "ENT_META"  # META started
        assert corporate_action_catalog.lookup_ticker_by_cik("0001326801", t_transition) == "META"

        # 1 microsecond after transition: FB unmapped; META active
        assert corporate_action_catalog.resolve("FB", "TICKER", t_transition + one_us).startswith("UNRESOLVED_")
        assert corporate_action_catalog.resolve("META", "TICKER", t_transition + one_us) == "ENT_META"

    def test_cross_reference_exact_boundary_conditions(
        self, corporate_action_catalog: SymbologyMaster
    ) -> None:
        """cross_reference obeys the exact half-open [effective_start, effective_end) boundary."""
        t_transition = datetime(2022, 6, 9, 0, 0, tzinfo=UTC)
        one_us = timedelta(microseconds=1)

        # 1us before: CUSIP resolves to FB
        assert corporate_action_catalog.cross_reference(
            "30303M102", "CUSIP", "TICKER", t_transition - one_us
        ) == "FB"

        # At transition: CUSIP resolves to META
        assert corporate_action_catalog.cross_reference(
            "30303M102", "CUSIP", "TICKER", t_transition
        ) == "META"

        # At transition: TICKER FB does not cross-reference to CIK
        assert corporate_action_catalog.cross_reference(
            "FB", "TICKER", "CIK", t_transition
        ) is None

        # At transition: TICKER META cross-references to CIK
        assert corporate_action_catalog.cross_reference(
            "META", "TICKER", "CIK", t_transition
        ) == "0001326801"

    def test_boundary_query_with_different_timezones(
        self, corporate_action_catalog: SymbologyMaster
    ) -> None:
        """Querying at effective_end in non-UTC timezone converts accurately to UTC for boundary check."""
        # 2022-06-09 00:00:00 UTC in Tokyo (UTC+9) is 2022-06-09 09:00:00+09:00
        tokyo_tz = timezone(timedelta(hours=9))
        t_transition_tokyo = datetime(2022, 6, 9, 9, 0, 0, tzinfo=tokyo_tz)

        # Must evaluate to exact transition boundary: FB expired, META active
        assert corporate_action_catalog.resolve("FB", "TICKER", t_transition_tokyo).startswith("UNRESOLVED_")
        assert corporate_action_catalog.resolve("META", "TICKER", t_transition_tokyo) == "ENT_META"

        # In US Eastern (EDT = UTC-4): 2022-06-08 20:00:00 EDT
        ny_tz = ZoneInfo("America/New_York")
        t_transition_ny = datetime(2022, 6, 8, 20, 0, 0, tzinfo=ny_tz)
        assert corporate_action_catalog.resolve("FB", "TICKER", t_transition_ny).startswith("UNRESOLVED_")
        assert corporate_action_catalog.resolve("META", "TICKER", t_transition_ny) == "ENT_META"


# ============================================================================
# 4. Symbology CIK Padding & Quarantine Ledger Stress Tests
# ============================================================================


class TestSymbologyCikPaddingAndQuarantineAdversarial:
    """Adversarial stress tests for SEC CIK zero padding and unmapped quarantine generation."""

    @pytest.fixture
    def symbology_catalog(self) -> SymbologyMaster:
        master = SymbologyMaster()
        t_start = datetime(2020, 1, 1, tzinfo=UTC)

        # Apple: 10-digit zero-padded CIK stored in catalog
        master.add_mapping(
            SymbologyMapping(
                entity_id="ENT_AAPL",
                ticker="AAPL",
                cik="0000320193",
                cusip="037833100",
                isin="US0378331005",
                sedol="2046251",
                figi="BBG000B9XRY4",
                effective_start=t_start,
            )
        )

        # Microsoft: unpadded integer CIK stored in catalog
        master.add_mapping(
            SymbologyMapping(
                entity_id="ENT_MSFT",
                ticker="MSFT",
                cik="789019",
                cusip="594918104",
                isin="US5949181045",
                sedol="2588173",
                figi="BBG000BPH459",
                effective_start=t_start,
            )
        )
        return master

    def test_cik_matches_helper_direct_matrix(self) -> None:
        """_cik_matches correctly matches integers and strings with varied zero padding."""
        # Unpadded vs padded
        assert _cik_matches("320193", "0000320193") is True
        assert _cik_matches("0000320193", "320193") is True
        assert _cik_matches("00320193", "0000320193") is True
        assert _cik_matches("320193", "320193") is True

        # Python integer vs string
        assert _cik_matches(320193, "0000320193") is True
        assert _cik_matches("0000320193", 320193) is True
        assert _cik_matches(320193, 320193) is True

        # Zeros
        assert _cik_matches("0", "0000000000") is True
        assert _cik_matches(0, "0000000000") is True

        # Mismatch
        assert _cik_matches("320193", "0000320194") is False
        assert _cik_matches("320193", "INVALID") is False
        assert _cik_matches("XYZ", "XYZ") is True
        assert _cik_matches("XYZ", "ABC") is False

    def test_cik_zero_padding_resolution_apple(
        self, symbology_catalog: SymbologyMaster
    ) -> None:
        """Catalog has '0000320193'; resolves all permutations (unpadded, padded, integer)."""
        as_of = datetime(2023, 1, 1, tzinfo=UTC)
        ciks_to_test = [
            "320193",
            "0320193",
            "00320193",
            "000320193",
            "0000320193",
            "000000000000320193",
            320193,  # Raw integer
        ]

        for cik in ciks_to_test:
            # resolve
            assert symbology_catalog.resolve(cik, "CIK", as_of) == "ENT_AAPL", f"Failed resolve for {cik}"
            # lookup_ticker_by_cik
            assert symbology_catalog.lookup_ticker_by_cik(cik, as_of) == "AAPL", f"Failed lookup for {cik}"
            # cross_reference CIK -> ISIN
            assert symbology_catalog.cross_reference(cik, "CIK", "ISIN", as_of) == "US0378331005", f"Failed cross_ref for {cik}"

    def test_cik_zero_padding_resolution_microsoft(
        self, symbology_catalog: SymbologyMaster
    ) -> None:
        """Catalog has unpadded '789019'; resolves all permutations (padded, unpadded, integer)."""
        as_of = datetime(2023, 1, 1, tzinfo=UTC)
        ciks_to_test = [
            "789019",
            "0000789019",
            "00000000789019",
            789019,  # Raw integer
        ]

        for cik in ciks_to_test:
            assert symbology_catalog.resolve(cik, "CIK", as_of) == "ENT_MSFT", f"Failed resolve for {cik}"
            assert symbology_catalog.lookup_ticker_by_cik(cik, as_of) == "MSFT", f"Failed lookup for {cik}"
            assert symbology_catalog.cross_reference(cik, "CIK", "TICKER", as_of) == "MSFT", f"Failed cross_ref for {cik}"

    def test_unmapped_symbols_quarantine_generation_and_prefix_stripping(self) -> None:
        """Unmapped symbols generate synthetic IDs and strip vendor prefixes (UNKNOWN_CORP_, GHOST_TICKER_)."""
        master = SymbologyMaster()
        as_of = datetime(2024, 2, 1, 12, 0, tzinfo=UTC)

        # Standard unmapped
        syn1 = master.resolve("GHOST_INC", "TICKER", as_of, vendor_id="V_SYNTH")
        assert syn1 == "UNRESOLVED_V_SYNTH_GHOST_INC"

        # UNKNOWN_CORP_ prefix stripped
        syn2 = master.resolve("UNKNOWN_CORP_ACME_CORP", "TICKER", as_of, vendor_id="SEC_EDGAR")
        assert syn2 == "UNRESOLVED_SEC_EDGAR_ACME_CORP"

        # GHOST_TICKER_ prefix stripped
        syn3 = master.resolve("GHOST_TICKER_PHANTOM", "TICKER", as_of, vendor_id="BLOOMBERG")
        assert syn3 == "UNRESOLVED_BLOOMBERG_PHANTOM"

        # Integer unmapped symbol
        syn4 = master.resolve(999999999, "CIK", as_of, vendor_id="SEC_DERA")
        assert syn4 == "UNRESOLVED_SEC_DERA_999999999"

    def test_quarantine_ledger_verification_and_vendor_isolation(self) -> None:
        """Quarantine ledger records entries accurately and supports vendor isolation."""
        master = SymbologyMaster()
        as_of_v1 = datetime(2024, 1, 15, 10, 0, tzinfo=UTC)
        as_of_v2 = datetime(2024, 1, 15, 11, 0, tzinfo=UTC)

        master.resolve("UNMAPPED_A", "TICKER", as_of_v1, vendor_id="VENDOR_1")
        master.resolve("UNMAPPED_B", "TICKER", as_of_v1, vendor_id="VENDOR_1")
        master.resolve("UNMAPPED_C", "CIK", as_of_v2, vendor_id="VENDOR_2")

        ledger = master.get_quarantine_records()
        assert len(ledger) == 3

        v1_records = master.get_quarantine_records("VENDOR_1")
        v2_records = master.get_quarantine_records("VENDOR_2")
        assert len(v1_records) == 2
        assert len(v2_records) == 1

        rec = v2_records[0]
        assert rec.synthetic_id == "UNRESOLVED_VENDOR_2_UNMAPPED_C"
        assert rec.raw_symbol == "UNMAPPED_C"
        assert rec.source_type == "CIK"
        assert rec.vendor_id == "VENDOR_2"
        assert rec.quarantine_time == as_of_v2
        assert rec.reason == "UNMAPPED_SYMBOL"
        assert rec.resolved_entity_id is None


# ============================================================================
# 5. Symbology Bulk Resolution & Concurrency Stress Tests
# ============================================================================


class TestSymbologyBulkResolutionAndConcurrencyStress:
    """Stress testing SymbologyMaster under 10,000+ entries and multithreaded concurrency."""

    @pytest.fixture
    def high_cardinality_catalog(self) -> SymbologyMaster:
        """Generates a catalog with 500 distinct multi-identifier securities."""
        master = SymbologyMaster()
        t_start = datetime(2020, 1, 1, tzinfo=UTC)
        t_end = datetime(2030, 1, 1, tzinfo=UTC)

        for i in range(500):
            master.add_mapping(
                SymbologyMapping(
                    entity_id=f"ENT_{i:04d}",
                    ticker=f"TICK_{i:04d}",
                    cik=f"{100000 + i:010d}",  # Zero-padded 10-digit
                    cusip=f"CUSIP{i:04d}",
                    isin=f"US{i:010d}",
                    sedol=f"SED{i:04d}",
                    figi=f"BBG{i:08d}",
                    effective_start=t_start,
                    effective_end=t_end,
                )
            )
        return master

    def test_bulk_resolve_15000_entries_scale_and_precision(
        self, high_cardinality_catalog: SymbologyMaster
    ) -> None:
        """Resolves 15,000 mixed queries (tickers, padded CIK, unpadded CIK, unmapped) with 100% precision."""
        queries: list[tuple[str | int, str]] = []
        expected_results: list[str] = []

        for k in range(15000):
            idx = (k // 4) % 500
            if k % 4 == 0:
                # Valid ticker
                queries.append((f"TICK_{idx:04d}", "TICKER"))
                expected_results.append(f"ENT_{idx:04d}")
            elif k % 4 == 1:
                # Valid CIK (zero-padded string)
                queries.append((f"{100000 + idx:010d}", "CIK"))
                expected_results.append(f"ENT_{idx:04d}")
            elif k % 4 == 2:
                # Valid CIK (unpadded integer string)
                queries.append((f"{100000 + idx}", "CIK"))
                expected_results.append(f"ENT_{idx:04d}")
            else:
                # Unmapped symbol -> quarantine
                queries.append((f"GHOST_TICKER_PHANTOM_{k}", "TICKER"))
                expected_results.append(f"UNRESOLVED_BULK_PHANTOM_{k}")

        as_of = datetime(2023, 6, 15, tzinfo=UTC)

        start_time = time.perf_counter()
        results = high_cardinality_catalog.bulk_resolve(
            queries, as_of=as_of, vendor_id="BULK"  # type: ignore[arg-type]
        )
        elapsed = time.perf_counter() - start_time

        assert len(results) == 15000
        assert results == expected_results

        # Throughput assertion: 15k resolutions must execute within 3 seconds
        assert elapsed < 3.0, f"Bulk resolution took {elapsed:.2f}s, exceeding 3.0s SLA."

        # Quarantine verification: exactly 3,750 unmapped symbols
        unmapped_recs = high_cardinality_catalog.get_quarantine_records("BULK")
        assert len(unmapped_recs) == 3750

    def test_multithreaded_concurrent_bulk_resolution(
        self, high_cardinality_catalog: SymbologyMaster
    ) -> None:
        """10 threads concurrently resolving 1,200 queries each (12,000 total) without corruption."""
        as_of = datetime(2023, 6, 15, tzinfo=UTC)

        def worker_fn(worker_id: int) -> list[str]:
            local_queries = []
            for m in range(1200):
                target_idx = (worker_id * 1200 + m) % 500
                if m % 3 == 0:
                    local_queries.append((f"TICK_{target_idx:04d}", "TICKER"))
                elif m % 3 == 1:
                    local_queries.append((f"{100000 + target_idx}", "CIK"))
                else:
                    local_queries.append((f"UNMAPPED_W{worker_id}_{m}", "TICKER"))

            return high_cardinality_catalog.bulk_resolve(
                local_queries, as_of=as_of, vendor_id=f"THREAD_{worker_id}"  # type: ignore[arg-type]
            )

        start_time = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(worker_fn, w) for w in range(10)]
            thread_results = [f.result() for f in futures]
        elapsed = time.perf_counter() - start_time

        total_queries = sum(len(res) for res in thread_results)
        assert total_queries == 12000
        assert elapsed < 5.0, f"Concurrent resolution took {elapsed:.2f}s, exceeding 5.0s SLA."

        # Verify total quarantine ledger entries: 10 threads * 400 unmapped = 4000
        thread_quarantine = [
            q for q in high_cardinality_catalog.quarantine_ledger if q.vendor_id.startswith("THREAD_")
        ]
        assert len(thread_quarantine) == 4000
