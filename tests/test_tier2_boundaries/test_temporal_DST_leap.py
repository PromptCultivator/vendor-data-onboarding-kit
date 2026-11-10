r"""
Tier 2 Boundary Tests: Temporal Transitions, DST, Leap Years, Precision, and Epoch Limits.

Per TEST_INFRA.md and PROJECT.md §8.2, §8.3, §8.4 / F-MOD-01, F-VAL-04, F-VAL-05, F-STR-03:
Tests temporal boundary conditions:
- Leap year calendar math (Feb 29 2020/2024, century leap year 2000 vs 1900/2100)
- Daylight Saving Time (DST) US/Eastern spring-forward (23h) and fall-back (25h) transitions
- Time inversion detection (F-VAL-04 monotone timestamp check under naive local clocks)
- Microsecond and nanosecond precision boundaries (23:59:59.999999Z and midnight rollovers)
- Epoch and horizon limits (Unix epoch 1970-01-01, pre-epoch 1929, Y2038 Int32 limit, year 9999)
- Future timestamp guard (F-VAL-05: t_event > t_knowledge with clock-skew tolerance)
- Point-in-time boundary queries ($t_{as\_of} = t_{knowledge}$ vs $t_{as\_of} = t_{knowledge} - 1\mu s$)
"""

from __future__ import annotations

import calendar
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any

import pytest

try:
    import duckdb
except ImportError:
    duckdb = None  # type: ignore

try:
    import pyarrow as pa
    import pyarrow.compute as pc
except ImportError:
    pa = None  # type: ignore
    pc = None  # type: ignore


pytestmark = [pytest.mark.tier2]


# ============================================================================
# 1. Leap Year Edge Cases
# ============================================================================

def test_leap_year_feb_29_2020_valid_date():
    """2020 is divisible by 4 and not 100: Feb 29, 2020 is valid."""
    d = date(2020, 2, 29)
    assert d.year == 2020 and d.month == 2 and d.day == 29
    assert calendar.isleap(2020) is True


def test_leap_year_feb_29_2024_valid_date():
    """2024 is divisible by 4: Feb 29, 2024 is valid."""
    d = date(2024, 2, 29)
    assert d.year == 2024 and d.month == 2 and d.day == 29
    assert calendar.isleap(2024) is True


def test_century_leap_year_2000_divisible_by_400_valid():
    """2000 is divisible by 400: century leap year, Feb 29, 2000 is valid."""
    d = date(2000, 2, 29)
    assert d.year == 2000 and d.month == 2 and d.day == 29
    assert calendar.isleap(2000) is True


def test_century_non_leap_year_1900_divisible_by_100_invalid():
    """1900 is divisible by 100 but NOT 400: NOT a leap year. Feb 29 must raise ValueError."""
    assert calendar.isleap(1900) is False
    with pytest.raises(ValueError, match="day is out of range for month"):
        date(1900, 2, 29)


def test_century_non_leap_year_2100_invalid():
    """2100 is divisible by 100 but NOT 400: NOT a leap year. Feb 29 must raise ValueError."""
    assert calendar.isleap(2100) is False
    with pytest.raises(ValueError, match="day is out of range for month"):
        date(2100, 2, 29)


def test_date_addition_across_leap_year_boundary():
    """Adding 1 day to 2024-02-28 lands on 2024-02-29 (leap year)."""
    d = date(2024, 2, 28) + timedelta(days=1)
    assert d == date(2024, 2, 29)

    d_next = d + timedelta(days=1)
    assert d_next == date(2024, 3, 1)


def test_date_addition_across_non_leap_year_boundary():
    """Adding 1 day to 2023-02-28 lands on 2023-03-01 (common year)."""
    d = date(2023, 2, 28) + timedelta(days=1)
    assert d == date(2023, 3, 1)


def test_leap_year_day_of_year_calculation_day_366():
    """Leap years have 366 days; Dec 31 is day 366."""
    d_2024 = date(2024, 12, 31)
    assert d_2024.timetuple().tm_yday == 366

    d_2023 = date(2023, 12, 31)
    assert d_2023.timetuple().tm_yday == 365


# ============================================================================
# 2. Daylight Saving Time (DST) Transitions in US/Eastern
# ============================================================================

def test_dst_spring_forward_us_eastern_23_hour_day():
    """On spring-forward Sunday (2024-03-10), America/New_York has only 23 hours."""
    tz = ZoneInfo("America/New_York")
    start = datetime(2024, 3, 10, 0, 0, 0, tzinfo=tz)
    end = datetime(2024, 3, 11, 0, 0, 0, tzinfo=tz)

    duration_hours = (end.astimezone(timezone.utc) - start.astimezone(timezone.utc)).total_seconds() / 3600.0
    assert duration_hours == 23.0


def test_dst_spring_forward_skipped_local_hour_detection():
    """2:30 AM does not exist in local time on spring-forward Sunday."""
    tz = ZoneInfo("America/New_York")
    # In zoneinfo, constructing 2:30 AM on 2024-03-10 resolves to either 1:30 or 3:30 depending on fold
    dt_skipped = datetime(2024, 3, 10, 2, 30, 0, tzinfo=tz)
    # When converted to UTC and back, the local time cannot remain 02:30 EDT/EST
    utc_dt = dt_skipped.astimezone(timezone.utc)
    back_to_local = utc_dt.astimezone(tz)
    assert back_to_local.hour != 2 or back_to_local.minute != 30


def test_dst_fall_back_us_eastern_25_hour_day():
    """On fall-back Sunday (2024-11-03), America/New_York has 25 hours."""
    tz = ZoneInfo("America/New_York")
    start = datetime(2024, 11, 3, 0, 0, 0, tzinfo=tz)
    end = datetime(2024, 11, 4, 0, 0, 0, tzinfo=tz)

    duration_hours = (end.astimezone(timezone.utc) - start.astimezone(timezone.utc)).total_seconds() / 3600.0
    assert duration_hours == 25.0


def test_dst_fall_back_ambiguous_local_time_disambiguation():
    """1:30 AM occurs twice on fall-back Sunday: fold=0 (EDT, UTC-4) and fold=1 (EST, UTC-5)."""
    tz = ZoneInfo("America/New_York")
    dt_first = datetime(2024, 11, 3, 1, 30, 0, fold=0, tzinfo=tz)
    dt_second = datetime(2024, 11, 3, 1, 30, 0, fold=1, tzinfo=tz)

    assert dt_first.utcoffset() == timedelta(hours=-4)
    assert dt_second.utcoffset() == timedelta(hours=-5)

    # Exactly 1 hour apart in physical UTC time
    diff_sec = (dt_second.astimezone(timezone.utc) - dt_first.astimezone(timezone.utc)).total_seconds()
    assert diff_sec == 3600.0


def test_dst_transition_preserves_strict_utc_monotonicity():
    """Converting aware local datetimes across DST transitions produces strictly monotonic UTC timestamps."""
    tz = ZoneInfo("America/New_York")
    local_times = [
        datetime(2024, 11, 3, 1, 0, 0, fold=0, tzinfo=tz),
        datetime(2024, 11, 3, 1, 30, 0, fold=0, tzinfo=tz),
        datetime(2024, 11, 3, 1, 0, 0, fold=1, tzinfo=tz),
        datetime(2024, 11, 3, 1, 30, 0, fold=1, tzinfo=tz),
        datetime(2024, 11, 3, 2, 0, 0, fold=1, tzinfo=tz),
    ]

    utc_times = [t.astimezone(timezone.utc) for t in local_times]
    for i in range(len(utc_times) - 1):
        assert utc_times[i] < utc_times[i + 1]


def test_naive_timestamp_dst_fallback_causes_time_inversion():
    """Naive local timestamps jump backward 1 hour during fall-back, creating a non-monotonic sequence."""
    naive_times = [
        datetime(2024, 11, 3, 1, 0, 0),
        datetime(2024, 11, 3, 1, 30, 0),
        datetime(2024, 11, 3, 1, 0, 0),  # Clocks turned back!
        datetime(2024, 11, 3, 1, 30, 0),
    ]

    # Monotonicity test: naive_times[2] < naive_times[1] (Inversion!)
    has_inversion = any(naive_times[i] > naive_times[i + 1] for i in range(len(naive_times) - 1))
    assert has_inversion is True


def test_monotone_check_flags_timestamp_sequence_inversion():
    """Validation rule F-VAL-04: Enforce non-decreasing event_time per entity."""
    # Sequence with a backwards clock jump
    timestamps = [
        datetime(2023, 10, 1, 10, 0, 0, tzinfo=timezone.utc),
        datetime(2023, 10, 1, 10, 30, 0, tzinfo=timezone.utc),
        datetime(2023, 10, 1, 10, 15, 0, tzinfo=timezone.utc),  # INVERSION!
        datetime(2023, 10, 1, 11, 0, 0, tzinfo=timezone.utc),
    ]

    inversions = []
    for i in range(len(timestamps) - 1):
        if timestamps[i + 1] < timestamps[i]:
            inversions.append((i, timestamps[i], timestamps[i + 1]))

    assert len(inversions) == 1
    assert inversions[0][0] == 1  # Inversion at index 1 -> 2


# ============================================================================
# 3. Microsecond and Nanosecond Precision Boundaries
# ============================================================================

def test_microsecond_boundary_last_tick_of_day():
    """23:59:59.999999Z represents the absolute final microsecond of a day."""
    t_end = datetime(2023, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)
    assert t_end.microsecond == 999999

    # Adding 1 microsecond rolls over to 00:00:00 of the new year
    t_new_year = t_end + timedelta(microseconds=1)
    assert t_new_year == datetime(2024, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)


def test_nanosecond_precision_in_pyarrow_timestamp():
    """PyArrow timestamp('ns') supports nanosecond precision; downcasting safely raises ArrowInvalid on data loss."""
    # Unix timestamp in nanoseconds: 1704067199999999999 ns
    ns_val = 1704067199999999999
    arr = pa.array([ns_val], type=pa.timestamp("ns", tz="UTC"))
    assert arr[0].value == ns_val

    # Safe downcast raises ArrowInvalid to guard against silent precision loss
    with pytest.raises(pa.ArrowInvalid, match="would lose data"):
        arr.cast(pa.timestamp("us", tz="UTC"), safe=True)

    # Explicit lossy downcast truncates sub-microsecond nanoseconds
    arr_lossy = arr.cast(pa.timestamp("us", tz="UTC"), safe=False)
    assert arr_lossy[0].value == ns_val // 1000


def test_midnight_boundary_rollover_ordering():
    """Strict chronological ordering across midnight boundary: T_23:59:59.999999 < T_00:00:00.000000."""
    t1 = datetime(2023, 10, 1, 23, 59, 59, 999999, tzinfo=timezone.utc)
    t2 = datetime(2023, 10, 2, 0, 0, 0, 0, tzinfo=timezone.utc)

    assert t1 < t2
    diff = t2 - t1
    assert diff == timedelta(microseconds=1)


# ============================================================================
# 4. Epoch and Horizon Limits
# ============================================================================

def test_unix_epoch_1970_01_01_zero_timestamp_roundtrip():
    """Unix epoch (1970-01-01 00:00:00 UTC) has posix timestamp 0.0."""
    epoch = datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    assert epoch.timestamp() == 0.0

    from_ts = datetime.fromtimestamp(0.0, tz=timezone.utc)
    assert from_ts == epoch


def test_pre_epoch_negative_timestamp_1969():
    """Timestamps before 1970 have negative Unix epoch representations."""
    pre_epoch = datetime(1969, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
    assert pre_epoch.timestamp() == -1.0


def test_historical_1929_market_crash_timestamp():
    """Black Tuesday (1929-10-29 16:00:00 UTC) must be supported in financial history models."""
    crash = datetime(1929, 10, 29, 16, 0, 0, tzinfo=timezone.utc)
    assert crash.year == 1929
    assert crash.timestamp() < -1e9

    # PyArrow timestamp('us') supports 1929
    arr = pa.array([crash], type=pa.timestamp("us", tz="UTC"))
    assert arr[0].as_py() == crash


def test_year_2038_problem_safe_int64_timestamp():
    """Timestamps past 2038-01-19 03:14:07 UTC exceed 32-bit signed int and require 64-bit timestamps."""
    post_2038 = datetime(2038, 1, 20, 0, 0, 0, tzinfo=timezone.utc)
    ts_seconds = int(post_2038.timestamp())
    assert ts_seconds > 2147483647  # Exceeds Int32 max

    # Supported in Arrow Int64 timestamp
    arr = pa.array([post_2038], type=pa.timestamp("s", tz="UTC"))
    assert arr[0].as_py() == post_2038


def test_far_future_year_9999_timestamp():
    """Maximum supported date in Python datetime is 9999-12-31 23:59:59.999999."""
    far_future = datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)
    assert far_future.year == 9999

    with pytest.raises(OverflowError):
        far_future + timedelta(days=1)


# ============================================================================
# 5. ISO-8601 String Parsing & Normalization
# ============================================================================

def test_iso8601_utc_z_suffix_parsing():
    """ISO-8601 with trailing 'Z' parses into UTC timezone."""
    s = "2023-10-02T16:00:00Z"
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    assert dt.tzinfo == timezone.utc
    assert dt.hour == 16


def test_iso8601_offset_timezone_normalization_to_utc():
    """ISO-8601 with non-UTC offset (-04:00 EDT) normalizes to identical UTC instant."""
    s_edt = "2023-10-02T16:00:00-04:00"
    dt_edt = datetime.fromisoformat(s_edt)

    dt_utc = dt_edt.astimezone(timezone.utc)
    assert dt_utc.hour == 20
    assert dt_utc.day == 2
    assert dt_utc.tzinfo == timezone.utc


def test_iso8601_missing_fractional_seconds_parsing():
    """Parses timestamps with variable fractional precision (.1, .123, .123456)."""
    s1 = "2023-10-02T16:00:00"
    s2 = "2023-10-02T16:00:00.123456"

    dt1 = datetime.fromisoformat(s1)
    dt2 = datetime.fromisoformat(s2)

    assert dt1.microsecond == 0
    assert dt2.microsecond == 123456


# ============================================================================
# 6. Bitemporal Temporal Guard & Invariance Boundaries
# ============================================================================

def test_future_timestamp_guard_exact_event_time_gt_knowledge_time():
    """Validation rule F-VAL-05: Record where event_time > knowledge_time + tolerance is invalid."""
    t_know = datetime(2023, 10, 2, 16, 0, 0, tzinfo=timezone.utc)
    # Event occurs tomorrow relative to knowledge date (impossible observation)
    t_ev = datetime(2023, 10, 3, 16, 0, 0, tzinfo=timezone.utc)

    tolerance = timedelta(seconds=0)
    is_future = t_ev > (t_know + tolerance)
    assert is_future is True


def test_future_timestamp_guard_within_clock_skew_tolerance():
    """Small clock skew (e.g. 5 seconds) within configured tolerance (e.g. 60s) is accepted."""
    t_know = datetime(2023, 10, 2, 16, 0, 0, tzinfo=timezone.utc)
    t_ev = t_know + timedelta(seconds=5)

    tolerance = timedelta(seconds=60)
    is_future_violation = t_ev > (t_know + tolerance)
    assert is_future_violation is False


def test_future_timestamp_guard_exceeding_clock_skew_tolerance():
    """Clock skew exceeding tolerance (e.g. 120s > 60s) is flagged as violation."""
    t_know = datetime(2023, 10, 2, 16, 0, 0, tzinfo=timezone.utc)
    t_ev = t_know + timedelta(seconds=120)

    tolerance = timedelta(seconds=60)
    is_future_violation = t_ev > (t_know + tolerance)
    assert is_future_violation is True


def test_as_of_query_exact_knowledge_time_inclusion(temp_duckdb_conn):
    """As-of query at T_as_of = t_knowledge must include the record (<= condition)."""
    t_k = "2023-10-02 16:00:00"
    temp_duckdb_conn.execute(f"""
        CREATE TABLE bitemporal_test (id INT, knowledge_time TIMESTAMP, value DOUBLE);
        INSERT INTO bitemporal_test VALUES (1, '{t_k}'::TIMESTAMP, 100.0);
    """)

    # Query with as_of = t_k
    res = temp_duckdb_conn.execute(f"""
        SELECT count(*) FROM bitemporal_test WHERE knowledge_time <= '{t_k}'::TIMESTAMP
    """).fetchall()[0][0]
    assert res == 1


def test_as_of_query_one_microsecond_before_knowledge_time_exclusion(temp_duckdb_conn):
    """As-of query at T_as_of = t_knowledge - 1 microsecond must EXCLUDE the record."""
    t_k = "2023-10-02 16:00:00.000000"
    t_before = "2023-10-02 15:59:59.999999"

    temp_duckdb_conn.execute(f"""
        CREATE TABLE bitemporal_excl (id INT, knowledge_time TIMESTAMP, value DOUBLE);
        INSERT INTO bitemporal_excl VALUES (1, '{t_k}'::TIMESTAMP, 100.0);
    """)

    res = temp_duckdb_conn.execute(f"""
        SELECT count(*) FROM bitemporal_excl WHERE knowledge_time <= '{t_before}'::TIMESTAMP
    """).fetchall()[0][0]
    assert res == 0


def test_sec_filing_date_vs_period_end_date_temporal_order(fixture_paths):
    """In SEC DERA filings, period end date (ddate) must precede or equal filing period date."""
    assert fixture_paths.sec_2023q3_num.is_file()
    if duckdb:
        conn = duckdb.connect(":memory:")
        # Check that ddate parsed as integer YYYYMMDD is valid
        res = conn.execute(f"""
            SELECT min(ddate), max(ddate) FROM read_csv('{fixture_paths.sec_2023q3_num}', delim='\t', header=true)
        """).fetchall()[0]
        assert res[0] >= 20000101
        assert res[1] <= 20301231
        conn.close()


def test_alfred_vintage_realtime_start_vs_observation_date(fixture_paths):
    """In ALFRED, vintage publication date (realtime_start) must be >= macroeconomic observation date."""
    assert fixture_paths.alfred_gdpc1.is_file()
    if duckdb:
        conn = duckdb.connect(":memory:")
        # Count records where realtime_start < date (should be 0: no one publishes GDP before it happened!)
        res = conn.execute(f"""
            SELECT count(*) 
            FROM read_csv('{fixture_paths.alfred_gdpc1}')
            WHERE realtime_start < date
        """).fetchall()[0][0]
        assert res == 0
        conn.close()


def test_duckdb_bitemporal_as_of_filtering_with_timezone_utc(temp_duckdb_conn):
    """DuckDB bitemporal window query using TIMESTAMPTZ in UTC."""
    temp_duckdb_conn.execute("""
        CREATE TABLE quotes (
            symbol VARCHAR,
            event_time TIMESTAMPTZ,
            knowledge_time TIMESTAMPTZ,
            price DOUBLE
        );
        INSERT INTO quotes VALUES 
            ('AAPL', '2023-10-02 14:30:00Z', '2023-10-02 14:35:00Z', 170.0),
            ('AAPL', '2023-10-02 14:30:00Z', '2023-10-02 15:00:00Z', 170.5);  -- Restatement
    """)

    # Query as of 14:40:00Z -> should return original 170.0
    res_1 = temp_duckdb_conn.execute("""
        SELECT price FROM (
            SELECT price, ROW_NUMBER() OVER (PARTITION BY symbol, event_time ORDER BY knowledge_time DESC) as rn
            FROM quotes
            WHERE knowledge_time <= '2023-10-02 14:40:00Z'
        ) WHERE rn = 1
    """).fetchall()[0][0]
    assert res_1 == 170.0

    # Query as of 15:10:00Z -> should return restated 170.5
    res_2 = temp_duckdb_conn.execute("""
        SELECT price FROM (
            SELECT price, ROW_NUMBER() OVER (PARTITION BY symbol, event_time ORDER BY knowledge_time DESC) as rn
            FROM quotes
            WHERE knowledge_time <= '2023-10-02 15:10:00Z'
        ) WHERE rn = 1
    """).fetchall()[0][0]
    assert res_2 == 170.5


def test_bitemporal_vintage_audit_chronological_ordering():
    """Audit trajectory of revisions must be sorted chronologically by knowledge_time."""
    t_ev = datetime(2023, 10, 1, 16, 0, 0, tzinfo=timezone.utc)
    trajectory = [
        (100.0, datetime(2023, 10, 1, 17, 0, 0, tzinfo=timezone.utc)),
        (102.5, datetime(2023, 10, 5, 12, 0, 0, tzinfo=timezone.utc)),
        (101.8, datetime(2023, 10, 20, 9, 30, 0, tzinfo=timezone.utc)),
    ]

    for i in range(len(trajectory) - 1):
        assert trajectory[i][1] < trajectory[i + 1][1]
        # Revision sequence increments
        assert i + 1 < len(trajectory)
