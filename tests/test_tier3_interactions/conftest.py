"""
Tier 3 Cross-Feature Interactions Pytest Configuration and Fixtures.

Scoped to tests/test_tier3_interactions/**. Provides integration fixtures for
symbology master, bitemporal store, vectorized validation, and diffing engine.
"""

from __future__ import annotations

import sys
from collections.abc import Generator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# Ensure root tests directory is importable
_tests_dir = Path(__file__).resolve().parent.parent
if str(_tests_dir) not in sys.path:
    sys.path.insert(0, str(_tests_dir))


# Marker registration
def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "tier1: Tier 1 feature isolation tests")
    config.addinivalue_line("markers", "tier2: Tier 2 boundary value analysis tests")
    config.addinivalue_line("markers", "tier3: Tier 3 cross-feature interaction tests")
    config.addinivalue_line("markers", "tier4: Tier 4 real-world workload scenarios")
    config.addinivalue_line("markers", "bitemporal: Point-in-time invariant tests")


# ============================================================================
# Dataclasses & Models
# ============================================================================


@dataclass(frozen=True)
class SymbologyMapping:
    entity_id: str
    ticker: str
    cik: str
    cusip: str
    isin: str
    sedol: str
    figi: str
    effective_start: datetime
    effective_end: datetime


@dataclass
class QuarantineRecord:
    synthetic_id: str
    raw_symbol: str
    source_type: str
    vendor_id: str
    quarantine_time: datetime
    resolved_entity_id: str | None = None
    reason: str = "UNMAPPED_SYMBOL"


@dataclass
class DiffSummary:
    drop_id_baseline: str
    drop_id_current: str
    new_records: int
    unchanged_records: int
    restated_records: int
    dropped_records: int
    recall_rate: float


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
        common = [
            c for c in ["entity_id", "event_time", "metric", "source_symbol"] if c in rows1[0]
        ]
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
                assert abs(v1 - v2) <= float_abs_tol, (
                    f"Field {field_name} mismatch for key {k}: {v1} != {v2}"
                )
            else:
                assert v1 == v2, f"Field {field_name} mismatch for key {k}: {v1} != {v2}"
    return True


# ============================================================================
# Symbology Master
# ============================================================================


class SymbologyMaster:
    """Opaque-box reference implementation of Symbology Master and Quarantine."""

    def __init__(self) -> None:
        self.mappings: list[SymbologyMapping] = []
        self.quarantine_ledger: list[QuarantineRecord] = []

    def add_mapping(self, mapping: SymbologyMapping) -> None:
        self.mappings.append(mapping)

    def resolve(
        self,
        symbol: str,
        symbol_type: str,
        as_of: datetime,
        vendor_id: str = "SYNTH",
    ) -> str:
        """Resolves raw symbol to canonical entity_id as of a specific date."""
        for m in self.mappings:
            if m.effective_start <= as_of < m.effective_end:
                st = symbol_type.upper()
                if (
                    st == "TICKER"
                    and m.ticker == symbol
                    or st == "CIK"
                    and m.cik == symbol
                    or st == "CUSIP"
                    and m.cusip == symbol
                    or st == "ISIN"
                    and m.isin == symbol
                    or st == "SEDOL"
                    and m.sedol == symbol
                    or st == "FIGI"
                    and m.figi == symbol
                ):
                    return m.entity_id

        # Unmapped entity -> Quarantine
        clean_sym = symbol.replace("UNKNOWN_CORP_", "").replace("GHOST_TICKER_", "")
        synthetic_id = f"UNRESOLVED_{vendor_id}_{clean_sym}"
        self.quarantine_ledger.append(
            QuarantineRecord(
                synthetic_id=synthetic_id,
                raw_symbol=symbol,
                source_type=symbol_type,
                vendor_id=vendor_id,
                quarantine_time=as_of,
                reason="UNMAPPED_SYMBOL",
            )
        )
        return synthetic_id

    def lookup_cik_by_ticker(self, ticker: str, as_of: datetime) -> str | None:
        for m in self.mappings:
            if m.ticker == ticker and m.effective_start <= as_of < m.effective_end:
                return m.cik
        return None

    def lookup_ticker_by_cik(self, cik: str, as_of: datetime) -> str | None:
        for m in self.mappings:
            if m.cik == cik and m.effective_start <= as_of < m.effective_end:
                return m.ticker
        return None


# ============================================================================
# Diff Engine
# ============================================================================


class ReferenceDiffEngine:
    """Opaque-box reference diffing engine conforming to F-DIF-01..03."""

    def __init__(self, conn: duckdb.DuckDBPyConnection | None = None) -> None:
        self.conn = conn or duckdb.connect()

    def diff_drops(
        self,
        baseline_table: pa.Table,
        current_table: pa.Table,
        drop_id_baseline: str = "DROP_001",
        drop_id_current: str = "DROP_002",
        keys: list[str] | None = None,
        epsilon: float = 1e-9,
    ) -> tuple[pa.Table, DiffSummary]:
        key_cols = keys or ["entity_id", "event_time", "metric"]
        con = self.conn

        con.register("base", baseline_table)
        con.register("curr", current_table)

        join_on = " AND ".join([f'base."{k}" = curr."{k}"' for k in key_cols])

        has_sym = (
            "source_symbol" in baseline_table.column_names
            and "source_symbol" in current_table.column_names
        )
        sym_expr = (
            "COALESCE(curr.source_symbol, base.source_symbol) as source_symbol,"
            if has_sym
            else "'' as source_symbol,"
        )

        has_rec = (
            "record_id" in baseline_table.column_names and "record_id" in current_table.column_names
        )
        rec_expr = (
            "COALESCE(curr.record_id, base.record_id) as record_id,"
            if has_rec
            else "'' as record_id,"
        )

        base_key = f'base."{key_cols[0]}"'
        curr_key = f'curr."{key_cols[0]}"'

        diff_query = f"""
            WITH full_joined AS (
                SELECT
                    {rec_expr}
                    COALESCE(curr.entity_id, base.entity_id) as entity_id,
                    {sym_expr}
                    COALESCE(curr.event_time, base.event_time) as event_time,
                    COALESCE(curr.metric, base.metric) as metric,
                    base.value as value_old,
                    curr.value as value_new,
                    CASE
                        WHEN {base_key} IS NULL THEN 'NEW'
                        WHEN {curr_key} IS NULL THEN 'DELETED'
                        WHEN abs(curr.value - base.value) <= {epsilon} THEN 'UNCHANGED'
                        ELSE 'RESTATED'
                    END as diff_status,
                    CASE
                        WHEN curr.value IS NOT NULL AND base.value IS NOT NULL
                        THEN (curr.value - base.value)
                        ELSE NULL
                    END as restatement_delta,
                    CASE
                        WHEN curr.value IS NOT NULL AND base.value IS NOT NULL AND base.value != 0
                        THEN ((curr.value - base.value) / abs(base.value)) * 100.0
                        ELSE NULL
                    END as restatement_pct_delta
                FROM base
                FULL OUTER JOIN curr ON {join_on}
            )
            SELECT * FROM full_joined
        """
        diff_table = con.sql(diff_query).arrow().read_all()

        counts = {
            row[0]: row[1]
            for row in con.sql(
                "SELECT diff_status, count(*) FROM diff_table GROUP BY diff_status"
            ).fetchall()
        }

        new_cnt = counts.get("NEW", 0)
        unchanged_cnt = counts.get("UNCHANGED", 0)
        restated_cnt = counts.get("RESTATED", 0)
        dropped_cnt = counts.get("DELETED", 0)

        base_cnt = baseline_table.num_rows
        recall = 1.0 if base_cnt == 0 else min(1.0, (unchanged_cnt + restated_cnt) / base_cnt)

        summary = DiffSummary(
            drop_id_baseline=drop_id_baseline,
            drop_id_current=drop_id_current,
            new_records=new_cnt,
            unchanged_records=unchanged_cnt,
            restated_records=restated_cnt,
            dropped_records=dropped_cnt,
            recall_rate=recall,
        )
        return diff_table, summary


# ============================================================================
# Bitemporal Store
# ============================================================================


class BitemporalParquetStore:
    """Opaque-box reference implementation of partitioned append-only Parquet store."""

    def __init__(self, root_dir: Path, conn: duckdb.DuckDBPyConnection | None = None) -> None:
        self.root_dir = root_dir
        self.conn = conn or duckdb.connect()

    def append(self, table: pa.Table, dataset_id: str, drop_id: str) -> int:
        import os
        import stat

        dest_dir = self.root_dir / dataset_id / f"drop_id={drop_id}"
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_file = dest_dir / "data.parquet"
        pq.write_table(table, dest_file, compression="SNAPPY")
        os.chmod(dest_file, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        return table.num_rows

    def get_view_query(self, dataset_id: str) -> str:
        pattern = str(self.root_dir / dataset_id / "**" / "*.parquet")
        return f"SELECT * FROM read_parquet('{pattern}', hive_partitioning=true)"

    def as_of_query(
        self,
        dataset_id: str,
        as_of_time: datetime,
        entity_ids: list[str] | None = None,
    ) -> pa.Table:
        view_sql = self.get_view_query(dataset_id)
        as_of_iso = as_of_time.isoformat()
        entity_filter = ""
        if entity_ids:
            ent_list = ", ".join([f"'{e}'" for e in entity_ids])
            entity_filter = f"AND entity_id IN ({ent_list})"

        query = f"""
            WITH ranked AS (
                SELECT *,
                    row_number() OVER (
                        PARTITION BY entity_id, event_time, metric
                        ORDER BY knowledge_time DESC, revision_seq DESC
                    ) as rn
                FROM ({view_sql})
                WHERE knowledge_time <= TIMESTAMP '{as_of_iso}' {entity_filter}
            )
            SELECT entity_id, event_time, knowledge_time, metric, value, unit, source_symbol, source_symbol_type, vendor_id, dataset_id, is_restatement, revision_seq
            FROM ranked
            WHERE rn = 1
            ORDER BY entity_id, event_time, metric
        """
        return self.conn.sql(query).arrow().read_all()

    def audit_trail(
        self, dataset_id: str, entity_id: str, event_time: datetime, metric: str = "CLOSE_PRICE"
    ) -> pa.Table:
        view_sql = self.get_view_query(dataset_id)
        ev_iso = event_time.isoformat()
        query = f"""
            SELECT entity_id, event_time, knowledge_time, metric, value, is_restatement, revision_seq, drop_id
            FROM ({view_sql})
            WHERE entity_id = '{entity_id}'
              AND event_time = TIMESTAMP '{ev_iso}'
              AND metric = '{metric}'
            ORDER BY knowledge_time ASC, revision_seq ASC
        """
        return self.conn.sql(query).arrow().read_all()


# ============================================================================
# Vectorized Validation Engine
# ============================================================================


class VectorizedValidationEngine:
    def __init__(self, conn: duckdb.DuckDBPyConnection | None = None) -> None:
        self.conn = conn or duckdb.connect()

    def validate_completeness(
        self, table: pa.Table, mandatory_cols: list[str], threshold: float = 0.0
    ) -> ValidationResult:
        con = self.conn
        total = table.num_rows
        if total == 0:
            return ValidationResult("completeness", True, 0, 0, 0.0, [], threshold)

        con.register("tbl", table)
        null_conds = " OR ".join(
            [f'"{col}" IS NULL' for col in mandatory_cols if col in table.column_names]
        )
        query = f"SELECT * FROM tbl WHERE {null_conds}"
        violations_arrow = con.sql(query).arrow().read_all()
        viol_count = violations_arrow.num_rows
        viol_rate = viol_count / total
        samples = violations_arrow.slice(0, 5).to_pylist()
        passed = viol_rate <= threshold
        return ValidationResult(
            "completeness", passed, total, viol_count, viol_rate, samples, threshold
        )

    def validate_range_and_domain(
        self, table: pa.Table, value_col: str = "value", threshold: float = 0.0
    ) -> ValidationResult:
        con = self.conn
        total = table.num_rows
        con.register("tbl", table)
        query = f'SELECT * FROM tbl WHERE "{value_col}" < 0'
        violations_arrow = con.sql(query).arrow().read_all()
        viol_count = violations_arrow.num_rows
        viol_rate = viol_count / total if total > 0 else 0.0
        samples = violations_arrow.slice(0, 5).to_pylist()
        return ValidationResult(
            "range_domain", viol_count == 0, total, viol_count, viol_rate, samples, threshold
        )

    def validate_intra_drop_duplicates(
        self, table: pa.Table, key_cols: list[str], threshold: float = 0.0
    ) -> ValidationResult:
        con = self.conn
        total = table.num_rows
        con.register("tbl", table)
        keys_str = ", ".join([f'"{k}"' for k in key_cols])
        query = f"""
            SELECT {keys_str}, count(*) as cnt, min(value) as min_val, max(value) as max_val
            FROM tbl
            GROUP BY {keys_str}
            HAVING count(*) > 1
        """
        dups_arrow = con.sql(query).arrow().read_all()
        viol_count = dups_arrow.num_rows
        viol_rate = viol_count / total if total > 0 else 0.0
        samples = dups_arrow.slice(0, 5).to_pylist()
        return ValidationResult(
            "intra_drop_duplicates",
            viol_count == 0,
            total,
            viol_count,
            viol_rate,
            samples,
            threshold,
        )


# ============================================================================
# Pytest Fixtures
# ============================================================================


@pytest.fixture
def tier3_duckdb() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    conn = duckdb.connect(":memory:")
    conn.execute("SET timezone = 'UTC';")
    yield conn
    conn.close()


@pytest.fixture
def tier3_diff_engine(tier3_duckdb: duckdb.DuckDBPyConnection) -> ReferenceDiffEngine:
    return ReferenceDiffEngine(conn=tier3_duckdb)


@pytest.fixture
def tier3_validator(tier3_duckdb: duckdb.DuckDBPyConnection) -> VectorizedValidationEngine:
    return VectorizedValidationEngine(conn=tier3_duckdb)


@pytest.fixture
def standard_symbology_master() -> SymbologyMaster:
    master = SymbologyMaster()
    t_epoch = datetime(1990, 1, 1, tzinfo=UTC)
    t_fb_rename = datetime(2022, 6, 9, tzinfo=UTC)
    t_future = datetime(2099, 12, 31, tzinfo=UTC)

    # AAPL
    master.add_mapping(
        SymbologyMapping(
            entity_id="ENT_AAPL",
            ticker="AAPL",
            cik="0000320193",
            cusip="037833100",
            isin="US0378331005",
            sedol="2046251",
            figi="BBG000B9XRY4",
            effective_start=t_epoch,
            effective_end=t_future,
        )
    )
    # MSFT
    master.add_mapping(
        SymbologyMapping(
            entity_id="ENT_MSFT",
            ticker="MSFT",
            cik="0000789019",
            cusip="594918104",
            isin="US5949181045",
            sedol="2588173",
            figi="BBG000BPH459",
            effective_start=t_epoch,
            effective_end=t_future,
        )
    )
    # META: FB before 2022-06-09
    master.add_mapping(
        SymbologyMapping(
            entity_id="ENT_META",
            ticker="FB",
            cik="0001326801",
            cusip="30303M102",
            isin="US30303M1027",
            sedol="B7TL820",
            figi="BBG000MM2P62",
            effective_start=t_epoch,
            effective_end=t_fb_rename,
        )
    )
    # META: META on and after 2022-06-09
    master.add_mapping(
        SymbologyMapping(
            entity_id="ENT_META",
            ticker="META",
            cik="0001326801",
            cusip="30303M102",
            isin="US30303M1027",
            sedol="B7TL820",
            figi="BBG000MM2P62",
            effective_start=t_fb_rename,
            effective_end=t_future,
        )
    )
    return master
