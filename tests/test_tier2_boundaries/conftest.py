"""
Tier 2 Boundaries Pytest Configuration and Fixtures.

Exclusively scoped to tests/test_tier2_boundaries/** to ensure boundary test suite
isolation and resilience regardless of root conftest evolutions across milestones.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Generator, Sequence

import pytest

try:
    import duckdb
except ImportError:
    duckdb = None  # type: ignore

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    pa = None  # type: ignore
    pq = None  # type: ignore


# ============================================================================
# Marker Registration
# ============================================================================

def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers so --strict-markers succeeds."""
    config.addinivalue_line("markers", "tier1: Tier 1 feature isolation tests")
    config.addinivalue_line("markers", "tier2: Tier 2 boundary value analysis tests")
    config.addinivalue_line("markers", "tier3: Tier 3 cross-feature interaction tests")
    config.addinivalue_line("markers", "tier4: Tier 4 real-world workload scenarios")
    config.addinivalue_line("markers", "bitemporal: Point-in-time invariant tests")


# ============================================================================
# Fallback Canonical Record
# ============================================================================

try:
    from vendor_onboarding.models.record import CanonicalRecord
except ImportError:
    @dataclass(frozen=True)
    class CanonicalRecord:  # type: ignore[no-redef]
        entity_id: str
        event_time: datetime
        knowledge_time: datetime
        value: float | str | int | None
        unit: str
        source_symbol: str
        source_symbol_type: str
        vendor_id: str
        dataset_id: str
        drop_id: str
        is_restatement: bool = False
        revision_seq: int = 1


# ============================================================================
# Fixture Paths Registry
# ============================================================================

@dataclass(frozen=True)
class Tier2FixturePaths:
    root_dir: Path
    tests_dir: Path
    fixtures_dir: Path

    # SEC DERA
    sec_dir: Path
    sec_2023q3_sub: Path
    sec_2023q3_num: Path
    sec_2023q4_sub: Path
    sec_2023q4_num: Path
    sec_2024q1_sub: Path
    sec_2024q1_num: Path

    # ALFRED
    alfred_dir: Path
    alfred_gdpc1: Path
    alfred_payems: Path
    alfred_cpiaucsl: Path

    # Synthetic
    synth_dir: Path
    drop_001_csv: Path
    drop_001_parquet: Path
    drop_001_json: Path
    drop_002_csv: Path
    drop_002_parquet: Path
    drop_003_csv: Path
    drop_003_jsonl: Path
    schema_baseline: Path
    schema_drift_breaking: Path
    schema_drift_warning: Path
    truth_manifest_parquet: Path


@pytest.fixture(scope="session")
def fixture_paths() -> Tier2FixturePaths:
    """Returns absolute paths to all fixtures."""
    tests_dir = Path(__file__).resolve().parent.parent
    root_dir = tests_dir.parent
    fixtures_dir = tests_dir / "fixtures"

    sec_dir = fixtures_dir / "sec"
    alfred_dir = fixtures_dir / "alfred"
    synth_dir = fixtures_dir / "synthetic"

    return Tier2FixturePaths(
        root_dir=root_dir,
        tests_dir=tests_dir,
        fixtures_dir=fixtures_dir,
        sec_dir=sec_dir,
        sec_2023q3_sub=sec_dir / "2023q3_sub.txt",
        sec_2023q3_num=sec_dir / "2023q3_num.txt",
        sec_2023q4_sub=sec_dir / "2023q4_sub.txt",
        sec_2023q4_num=sec_dir / "2023q4_num.txt",
        sec_2024q1_sub=sec_dir / "2024q1_restatement_sub.txt",
        sec_2024q1_num=sec_dir / "2024q1_restatement_num.txt",
        alfred_dir=alfred_dir,
        alfred_gdpc1=alfred_dir / "GDPC1_vintages.csv",
        alfred_payems=alfred_dir / "PAYEMS_vintages.csv",
        alfred_cpiaucsl=alfred_dir / "CPIAUCSL_vintages.csv",
        synth_dir=synth_dir,
        drop_001_csv=synth_dir / "drop_001_baseline.csv",
        drop_001_parquet=synth_dir / "drop_001_baseline.parquet",
        drop_001_json=synth_dir / "drop_001_baseline.json",
        drop_002_csv=synth_dir / "drop_002_restatements.csv",
        drop_002_parquet=synth_dir / "drop_002_restatements.parquet",
        drop_003_csv=synth_dir / "drop_003_anomalies.csv",
        drop_003_jsonl=synth_dir / "drop_003_anomalies.jsonl",
        schema_baseline=synth_dir / "schema_baseline.json",
        schema_drift_breaking=synth_dir / "schema_drift_breaking.json",
        schema_drift_warning=synth_dir / "schema_drift_warning.json",
        truth_manifest_parquet=synth_dir / "truth_manifest.parquet",
    )


# ============================================================================
# DuckDB Fixtures
# ============================================================================

@pytest.fixture
def temp_duckdb_conn() -> Generator[Any, None, None]:
    """Isolated in-memory DuckDB connection."""
    if duckdb is None:
        pytest.skip("DuckDB not installed")
    conn = duckdb.connect(":memory:")
    conn.execute("SET timezone = 'UTC';")
    yield conn
    conn.close()


# ============================================================================
# Canonical Sample Data Fixtures
# ============================================================================

@pytest.fixture
def sample_canonical_records() -> list[CanonicalRecord]:
    """Canonical test records."""
    t_ev = datetime(2023, 10, 2, 16, 0, 0, tzinfo=timezone.utc)
    t_kn = datetime(2023, 10, 6, 20, 0, 0, tzinfo=timezone.utc)

    return [
        CanonicalRecord(
            entity_id="ENT_AAPL",
            event_time=t_ev,
            knowledge_time=t_kn,
            value=173.75,
            unit="USD",
            source_symbol="AAPL",
            source_symbol_type="TICKER",
            vendor_id="SYNTH_VENDOR_A",
            dataset_id="EQUITY_DAILY_FUNDAMENTALS",
            drop_id="DROP_001",
        ),
        CanonicalRecord(
            entity_id="ENT_MSFT",
            event_time=t_ev,
            knowledge_time=t_kn,
            value=321.80,
            unit="USD",
            source_symbol="MSFT",
            source_symbol_type="TICKER",
            vendor_id="SYNTH_VENDOR_A",
            dataset_id="EQUITY_DAILY_FUNDAMENTALS",
            drop_id="DROP_001",
        ),
    ]


@pytest.fixture
def synthetic_record_factory() -> Callable[..., CanonicalRecord]:
    """Factory for CanonicalRecord."""
    def _factory(
        entity_id: str = "ENT_AAPL",
        event_time: datetime | None = None,
        knowledge_time: datetime | None = None,
        value: Any = 100.0,
        unit: str = "USD",
        source_symbol: str = "AAPL",
        source_symbol_type: str = "TICKER",
        vendor_id: str = "TEST_VENDOR",
        dataset_id: str = "TEST_DATASET",
        drop_id: str = "DROP_TEST",
        is_restatement: bool = False,
        revision_seq: int = 1,
    ) -> CanonicalRecord:
        ev = event_time or datetime(2023, 10, 2, 16, 0, 0, tzinfo=timezone.utc)
        kn = knowledge_time or datetime(2023, 10, 6, 20, 0, 0, tzinfo=timezone.utc)
        return CanonicalRecord(
            entity_id=entity_id,
            event_time=ev,
            knowledge_time=kn,
            value=value,
            unit=unit,
            source_symbol=source_symbol,
            source_symbol_type=source_symbol_type,
            vendor_id=vendor_id,
            dataset_id=dataset_id,
            drop_id=drop_id,
            is_restatement=is_restatement,
            revision_seq=revision_seq,
        )
    return _factory


# ============================================================================
# PIT Invariance Assertion Helper
# ============================================================================

def _assert_point_in_time_invariance(
    h1: Any,
    h2: Any,
    key_columns: Sequence[str] | None = None,
    float_abs_tol: float = 1e-9,
) -> bool:
    def _to_records(obj: Any) -> list[dict[str, Any]]:
        if pa is not None and isinstance(obj, pa.Table):
            return obj.to_pylist()
        if isinstance(obj, list):
            if not obj:
                return []
            if hasattr(obj[0], "__dict__"):
                return [vars(x) if not hasattr(x, "__dataclass_fields__") else {f: getattr(x, f) for f in x.__dataclass_fields__} for x in obj]
            if isinstance(obj[0], dict):
                return obj
        raise TypeError(f"Unsupported type: {type(obj)}")

    rows1 = _to_records(h1)
    rows2 = _to_records(h2)

    assert len(rows1) == len(rows2), f"Row count mismatch: {len(rows1)} vs {len(rows2)}"
    if not rows1:
        return True

    keys = tuple(key_columns or ["entity_id", "event_time"])
    map1 = {tuple(str(r.get(k)) for k in keys): r for r in rows1}
    map2 = {tuple(str(r.get(k)) for k in keys): r for r in rows2}

    assert set(map1.keys()) == set(map2.keys()), "Key set mismatch"

    for k, r1 in map1.items():
        r2 = map2[k]
        for field, v1 in r1.items():
            v2 = r2.get(field)
            if isinstance(v1, float) and isinstance(v2, float):
                if abs(v1 - v2) > float_abs_tol:
                    raise AssertionError(f"PIT Invariance Violation: {field} diff {v1} != {v2}")
            elif v1 != v2:
                raise AssertionError(f"PIT Invariance Violation: {field} diff {v1} != {v2}")
    return True


@pytest.fixture
def pit_invariance_assert() -> Callable[..., bool]:
    return _assert_point_in_time_invariance
