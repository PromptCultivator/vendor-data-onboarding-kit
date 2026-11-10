"""
Local test configuration and fixtures for tests/test_tier1_features.

Provides strongly-typed FixturePaths, temp_duckdb_conn, sample records,
synthetic factory, and point-in-time (PIT) invariance assertion helper.
"""

from __future__ import annotations

import os
import json
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Generator, Sequence

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest


# ============================================================================
# Dataclasses & Types
# ============================================================================

@dataclass(frozen=True)
class CanonicalRecord:
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


@dataclass(frozen=True)
class DropManifest:
    drop_id: str
    vendor_id: str
    dataset_id: str
    file_path: str
    file_sha256: str
    file_size_bytes: int
    row_count: int
    ingest_timestamp: datetime


@dataclass
class ValidationResult:
    check_name: str
    passed: bool
    total_records: int
    violation_count: int
    violation_rate: float
    violations_sample: list[dict[str, Any]]
    threshold: float
    error_message: str | None = None


@dataclass
class DiffSummary:
    drop_id_baseline: str
    drop_id_current: str
    new_records: int
    unchanged_records: int
    restated_records: int
    dropped_records: int
    recall_rate: float


@dataclass(frozen=True)
class FixturePaths:
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
    sec_readme: Path

    # ALFRED
    alfred_dir: Path
    alfred_gdpc1: Path
    alfred_payems: Path
    alfred_cpiaucsl: Path
    alfred_readme: Path

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
    synth_readme: Path


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture(scope="session")
def fixture_paths() -> FixturePaths:
    tier1_dir = Path(__file__).resolve().parent
    tests_dir = tier1_dir.parent
    root_dir = tests_dir.parent
    fixtures_dir = tests_dir / "fixtures"

    sec_dir = fixtures_dir / "sec"
    alfred_dir = fixtures_dir / "alfred"
    synth_dir = fixtures_dir / "synthetic"

    return FixturePaths(
        root_dir=root_dir,
        tests_dir=tests_dir,
        fixtures_dir=fixtures_dir,
        # SEC
        sec_dir=sec_dir,
        sec_2023q3_sub=sec_dir / "2023q3_sub.txt",
        sec_2023q3_num=sec_dir / "2023q3_num.txt",
        sec_2023q4_sub=sec_dir / "2023q4_sub.txt",
        sec_2023q4_num=sec_dir / "2023q4_num.txt",
        sec_2024q1_sub=sec_dir / "2024q1_restatement_sub.txt",
        sec_2024q1_num=sec_dir / "2024q1_restatement_num.txt",
        sec_readme=sec_dir / "README.md",
        # ALFRED
        alfred_dir=alfred_dir,
        alfred_gdpc1=alfred_dir / "GDPC1_vintages.csv",
        alfred_payems=alfred_dir / "PAYEMS_vintages.csv",
        alfred_cpiaucsl=alfred_dir / "CPIAUCSL_vintages.csv",
        alfred_readme=alfred_dir / "README.md",
        # Synthetic
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
        synth_readme=synth_dir / "README.md",
    )


@pytest.fixture
def temp_duckdb_conn() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    conn = duckdb.connect(":memory:")
    conn.execute("SET timezone = 'UTC';")
    yield conn
    conn.close()


@pytest.fixture
def sample_canonical_records() -> list[CanonicalRecord]:
    t_event_1 = datetime(2023, 10, 2, 16, 0, 0, tzinfo=timezone.utc)
    t_event_2 = datetime(2023, 10, 3, 16, 0, 0, tzinfo=timezone.utc)
    t_known = datetime(2023, 10, 6, 20, 0, 0, tzinfo=timezone.utc)

    return [
        CanonicalRecord(
            entity_id="ENT_AAPL",
            event_time=t_event_1,
            knowledge_time=t_known,
            value=173.75,
            unit="USD",
            source_symbol="AAPL",
            source_symbol_type="TICKER",
            vendor_id="SYNTH_VENDOR_A",
            dataset_id="EQUITY_DAILY_FUNDAMENTALS",
            drop_id="DROP_001",
            is_restatement=False,
            revision_seq=1,
        ),
        CanonicalRecord(
            entity_id="ENT_MSFT",
            event_time=t_event_1,
            knowledge_time=t_known,
            value=321.80,
            unit="USD",
            source_symbol="MSFT",
            source_symbol_type="TICKER",
            vendor_id="SYNTH_VENDOR_A",
            dataset_id="EQUITY_DAILY_FUNDAMENTALS",
            drop_id="DROP_001",
            is_restatement=False,
            revision_seq=1,
        ),
        CanonicalRecord(
            entity_id="ENT_AAPL",
            event_time=t_event_2,
            knowledge_time=t_known,
            value=174.20,
            unit="USD",
            source_symbol="AAPL",
            source_symbol_type="TICKER",
            vendor_id="SYNTH_VENDOR_A",
            dataset_id="EQUITY_DAILY_FUNDAMENTALS",
            drop_id="DROP_001",
            is_restatement=False,
            revision_seq=1,
        ),
    ]


@pytest.fixture
def synthetic_record_factory() -> Callable[..., CanonicalRecord]:
    counter = 0

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
        nonlocal counter
        counter += 1
        ev_time = event_time or datetime(2023, 10, 2, 16, 0, 0, tzinfo=timezone.utc)
        kn_time = knowledge_time or datetime(2023, 10, 6, 20, 0, 0, tzinfo=timezone.utc)
        return CanonicalRecord(
            entity_id=entity_id,
            event_time=ev_time,
            knowledge_time=kn_time,
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


def assert_point_in_time_invariance(
    h1: Any,
    h2: Any,
    key_columns: Sequence[str] | None = None,
    float_abs_tol: float = 1e-9,
) -> bool:
    """Asserts PIT invariance: H_1 == H_2 with zero look-ahead bias."""
    def _to_records(obj: Any) -> list[dict[str, Any]]:
        if isinstance(obj, pa.Table):
            return obj.to_pylist()
        if isinstance(obj, duckdb.DuckDBPyRelation):
            return obj.arrow().read_all().to_pylist()
        if isinstance(obj, list):
            if not obj:
                return []
            if hasattr(obj[0], "__dataclass_fields__"):
                return [{f: getattr(x, f) for f in x.__dataclass_fields__} for x in obj]
            if isinstance(obj[0], dict):
                return obj
        raise TypeError(f"Unsupported type: {type(obj)}")

    rows1 = _to_records(h1)
    rows2 = _to_records(h2)

    assert len(rows1) == len(rows2), f"Row count mismatch: {len(rows1)} != {len(rows2)}"
    if not rows1:
        return True

    if key_columns is None:
        common = [c for c in ["entity_id", "event_time", "metric", "source_symbol"] if c in rows1[0]]
        keys = tuple(common if common else sorted(rows1[0].keys()))
    else:
        keys = tuple(key_columns)

    map1 = {tuple(str(r.get(k)) for k in keys): r for r in rows1}
    map2 = {tuple(str(r.get(k)) for k in keys): r for r in rows2}

    assert set(map1.keys()) == set(map2.keys()), "Keys mismatch between H_1 and H_2"

    for k in sorted(map1.keys()):
        r1, r2 = map1[k], map2[k]
        for field_name in r1:
            v1, v2 = r1[field_name], r2.get(field_name)
            if isinstance(v1, float) and isinstance(v2, float):
                assert abs(v1 - v2) <= float_abs_tol, f"Field {field_name} mismatch for key {k}: {v1} != {v2}"
            else:
                assert v1 == v2, f"Field {field_name} mismatch for key {k}: {v1} != {v2}"
    return True


@pytest.fixture
def pit_invariance_assert() -> Callable[..., bool]:
    return assert_point_in_time_invariance


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "tier1: Tier 1 feature isolation unit tests")
    config.addinivalue_line("markers", "tier2: Tier 2 boundary tests")
    config.addinivalue_line("markers", "tier3: Tier 3 interaction tests")
    config.addinivalue_line("markers", "tier4: Tier 4 real-world tests")
    config.addinivalue_line("markers", "bitemporal: Bitemporal invariant tests")
