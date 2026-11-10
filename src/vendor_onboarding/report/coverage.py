"""Daily Coverage Matrix and Anomaly Dip Detection (F-REP-01)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import duckdb
import pyarrow as pa


@dataclass
class CoverageDipAlert:
    """Alert for a day-over-day entity coverage drop exceeding threshold percentage."""

    date: str
    baseline_count: int
    current_count: int
    dip_percentage: float
    description: str


@dataclass
class CoverageGapAlert:
    """Alert for an entity missing expected trading dates within the observation period."""

    entity_id: str
    missing_dates: list[str]
    expected_count: int
    actual_count: int
    description: str


@dataclass
class DailyCoverageMatrix:
    """Daily coverage matrix mapping entity_id -> trade_date -> observation count."""

    matrix: dict[str, dict[str, int]] = field(default_factory=dict)

    def __init__(self, matrix: dict[str, dict[str, int]] | None = None, **kwargs: Any) -> None:
        if matrix is not None:
            self.matrix = matrix
        elif "data" in kwargs:
            self.matrix = kwargs["data"]
        else:
            self.matrix = {}

    def __getitem__(self, key: str) -> dict[str, int]:
        return self.matrix[key]

    def __setitem__(self, key: str, value: dict[str, int]) -> None:
        self.matrix[key] = value

    def __len__(self) -> int:
        return len(self.matrix)

    def __iter__(self) -> Any:
        return iter(self.matrix)

    def __contains__(self, key: str) -> bool:
        return key in self.matrix

    def items(self) -> Any:
        return self.matrix.items()

    def keys(self) -> Any:
        return self.matrix.keys()

    def values(self) -> Any:
        return self.matrix.values()

    def get(self, key: str, default: Any = None) -> Any:
        return self.matrix.get(key, default)

    def to_dict(self) -> dict[str, dict[str, int]]:
        return self.matrix


def build_coverage_matrix(
    table: pa.Table,
    entity_col: str = "entity_id",
    time_col: str = "event_time",
    conn: duckdb.DuckDBPyConnection | None = None,
) -> DailyCoverageMatrix:
    """Constructs entity_id x trade_date frequency matrix."""
    if entity_col not in table.column_names or time_col not in table.column_names:
        return DailyCoverageMatrix({})

    con = conn or duckdb.connect()
    con.register("tbl", table)
    query = f"""
        SELECT
            "{entity_col}",
            strftime(CAST("{time_col}" AS TIMESTAMP), '%Y-%m-%d') as trade_date,
            count(*) as cnt
        FROM tbl
        WHERE "{entity_col}" IS NOT NULL AND "{time_col}" IS NOT NULL
        GROUP BY "{entity_col}", trade_date
        ORDER BY trade_date, "{entity_col}"
    """
    rows = con.sql(query).fetchall()
    matrix: dict[str, dict[str, int]] = {}
    for ent, dt, cnt in rows:
        if ent not in matrix:
            matrix[ent] = {}
        matrix[ent][dt] = cnt
    return DailyCoverageMatrix(matrix)


def detect_coverage_dips(
    table: pa.Table,
    dip_threshold_pct: float = 20.0,
    entity_col: str = "entity_id",
    time_col: str = "event_time",
    conn: duckdb.DuckDBPyConnection | None = None,
) -> list[CoverageDipAlert]:
    """Detects day-over-day entity count dips meeting or exceeding threshold percentage."""
    if time_col not in table.column_names or entity_col not in table.column_names:
        return []

    con = conn or duckdb.connect()
    con.register("tbl", table)
    daily_counts = con.sql(f"""
        SELECT
            strftime(CAST("{time_col}" AS TIMESTAMP), '%Y-%m-%d') as trade_date,
            count(DISTINCT "{entity_col}") as entity_count
        FROM tbl
        WHERE "{time_col}" IS NOT NULL AND "{entity_col}" IS NOT NULL
        GROUP BY trade_date
        ORDER BY trade_date
    """).fetchall()

    alerts: list[CoverageDipAlert] = []
    for i in range(1, len(daily_counts)):
        _prev_date, prev_count = daily_counts[i - 1]
        curr_date, curr_count = daily_counts[i]
        if prev_count > 0:
            dip_pct = ((prev_count - curr_count) / prev_count) * 100.0
            if dip_pct >= dip_threshold_pct:
                alerts.append(
                    CoverageDipAlert(
                        date=curr_date,
                        baseline_count=prev_count,
                        current_count=curr_count,
                        dip_percentage=dip_pct,
                        description=(
                            f"Coverage dropped {dip_pct:.1f}% on {curr_date} "
                            f"({prev_count} -> {curr_count} entities)"
                        ),
                    )
                )
    return alerts


def detect_coverage_gaps(
    table: pa.Table,
    expected_dates: list[str] | None = None,
    entity_col: str = "entity_id",
    time_col: str = "event_time",
    conn: duckdb.DuckDBPyConnection | None = None,
) -> list[CoverageGapAlert]:
    """Identifies missing dates per entity against expected calendar dates."""
    matrix = build_coverage_matrix(table, entity_col=entity_col, time_col=time_col, conn=conn)
    if not matrix:
        return []

    if expected_dates is not None:
        target_dates = sorted(expected_dates)
    else:
        # Infer calendar dates as all unique dates present across all entities
        all_dts: set[str] = set()
        for d_map in matrix.values():
            all_dts.update(d_map.keys())
        target_dates = sorted(all_dts)

    exp_cnt = len(target_dates)
    alerts: list[CoverageGapAlert] = []
    for ent, d_map in sorted(matrix.items()):
        missing = [d for d in target_dates if d not in d_map]
        if missing:
            act_cnt = len(d_map)
            desc = (
                f"Entity '{ent}' missing {len(missing)} expected dates: {', '.join(missing)} "
                f"({act_cnt}/{exp_cnt} observed)"
            )
            alerts.append(
                CoverageGapAlert(
                    entity_id=ent,
                    missing_dates=missing,
                    expected_count=exp_cnt,
                    actual_count=act_cnt,
                    description=desc,
                )
            )
    return alerts


class CoverageAnalyzer:
    """Coverage matrix builder and anomaly detector coordinator."""

    def __init__(self, conn: duckdb.DuckDBPyConnection | None = None) -> None:
        self.conn = conn

    def build_matrix(
        self,
        table: pa.Table,
        entity_col: str = "entity_id",
        time_col: str = "event_time",
    ) -> DailyCoverageMatrix:
        """Constructs daily coverage matrix."""
        return build_coverage_matrix(
            table, entity_col=entity_col, time_col=time_col, conn=self.conn
        )

    def detect_dips(
        self,
        table: pa.Table,
        dip_threshold_pct: float = 20.0,
        entity_col: str = "entity_id",
        time_col: str = "event_time",
    ) -> list[CoverageDipAlert]:
        """Detects day-over-day entity coverage dips exceeding threshold."""
        return detect_coverage_dips(
            table,
            dip_threshold_pct=dip_threshold_pct,
            entity_col=entity_col,
            time_col=time_col,
            conn=self.conn,
        )

    def detect_gaps(
        self,
        table: pa.Table,
        expected_dates: list[str] | None = None,
        entity_col: str = "entity_id",
        time_col: str = "event_time",
    ) -> list[CoverageGapAlert]:
        """Detects individual entity date coverage gaps."""
        return detect_coverage_gaps(
            table,
            expected_dates=expected_dates,
            entity_col=entity_col,
            time_col=time_col,
            conn=self.conn,
        )
