"""Vectorized data quality and validation rules executed via DuckDB and PyArrow."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import duckdb
import pyarrow as pa


@dataclass
class ValidationResult:
    """Strongly-typed outcome of a data quality validation check.

    Attributes:
        check_name: Identifier for the validation check (e.g. completeness, range_domain).
        passed: Boolean indicating whether the check met or exceeded SLA threshold.
        total_records: Total count of evaluated records (or universe cardinality).
        violation_count: Count of records failing the validation check.
        violation_rate: Ratio of violations to total records (0.0 to 1.0).
        violations_sample: Up to 5 representative violation records as dicts.
        threshold: Permissible violation rate threshold or required coverage rate.
        error_message: Optional error message if the check failed.
    """

    check_name: str
    passed: bool
    total_records: int
    violation_count: int
    violation_rate: float
    violations_sample: list[dict[str, Any]]
    threshold: float
    error_message: str | None = None


def _to_arrow_table(table: pa.Table | duckdb.DuckDBPyRelation) -> pa.Table:
    """Coerces input to PyArrow Table."""
    if isinstance(table, duckdb.DuckDBPyRelation):
        return table.to_arrow_table()
    return table


def _get_conn(conn: duckdb.DuckDBPyConnection | None) -> duckdb.DuckDBPyConnection:
    """Returns provided connection or creates a new connection with UTC timezone."""
    if conn is not None:
        return conn
    c = duckdb.connect()
    c.execute("SET timezone = 'UTC';")
    return c


def validate_completeness(
    table: pa.Table | duckdb.DuckDBPyRelation,
    mandatory_cols: list[str],
    threshold: float = 0.0,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> ValidationResult:
    """Validates that mandatory columns do not contain null values beyond SLA threshold."""
    tbl = _to_arrow_table(table)
    total = tbl.num_rows
    if total == 0:
        return ValidationResult(
            check_name="completeness",
            passed=True,
            total_records=0,
            violation_count=0,
            violation_rate=0.0,
            violations_sample=[],
            threshold=threshold,
        )

    missing_cols = [c for c in mandatory_cols if c not in tbl.column_names]
    if missing_cols:
        return ValidationResult(
            check_name="completeness",
            passed=False,
            total_records=total,
            violation_count=total,
            violation_rate=1.0,
            violations_sample=[],
            threshold=threshold,
            error_message=f"Missing mandatory columns: {missing_cols}",
        )

    con = _get_conn(conn)
    con.register("tbl", tbl)

    if mandatory_cols:
        null_conds = " OR ".join([f'"{col}" IS NULL' for col in mandatory_cols])
    else:
        null_conds = "1=0"

    viol_count = int(
        con.sql(f"SELECT count(*) FROM tbl WHERE {null_conds}").fetchall()[0][0]
    )
    viol_rate = viol_count / total if total > 0 else 0.0
    samples = (
        con.sql(f"SELECT * FROM tbl WHERE {null_conds} LIMIT 5")
        .to_arrow_table()
        .to_pylist()
    )
    passed = viol_rate <= threshold
    error_msg = (
        None
        if passed
        else f"Completeness check failed: violation rate {viol_rate:.2%} exceeded threshold {threshold:.2%}"
    )
    return ValidationResult(
        check_name="completeness",
        passed=passed,
        total_records=total,
        violation_count=viol_count,
        violation_rate=viol_rate,
        violations_sample=samples,
        threshold=threshold,
        error_message=error_msg,
    )


def validate_range_and_domain(
    table: pa.Table | duckdb.DuckDBPyRelation,
    value_col: str = "value",
    threshold: float = 0.0,
    min_val: float | None = 0.0,
    max_val: float | None = None,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> ValidationResult:
    """Flags values < min_val, > max_val, isnan, and isinf."""
    tbl = _to_arrow_table(table)
    total = tbl.num_rows
    if total == 0:
        return ValidationResult(
            check_name="range_domain",
            passed=True,
            total_records=0,
            violation_count=0,
            violation_rate=0.0,
            violations_sample=[],
            threshold=threshold,
        )

    if value_col not in tbl.column_names:
        return ValidationResult(
            check_name="range_domain",
            passed=False,
            total_records=total,
            violation_count=total,
            violation_rate=1.0,
            violations_sample=[],
            threshold=threshold,
            error_message=f"Column '{value_col}' not found",
        )

    con = _get_conn(conn)
    con.register("tbl", tbl)

    col_type = tbl.schema.field(value_col).type
    is_str_col = pa.types.is_string(col_type) or pa.types.is_large_string(col_type)
    if is_str_col:
        val_expr = f'try_cast("{value_col}" AS DOUBLE)'
    else:
        val_expr = f'"{value_col}"'

    conds: list[str] = []
    if is_str_col:
        conds.append(f'("{value_col}" IS NOT NULL AND {val_expr} IS NULL)')
    if min_val is not None and not math.isinf(min_val) and not math.isnan(min_val):
        conds.append(f"{val_expr} < {min_val}")
    if max_val is not None and not math.isinf(max_val) and not math.isnan(max_val):
        conds.append(f"{val_expr} > {max_val}")
    conds.append(f"isnan({val_expr})")
    conds.append(f"isinf({val_expr})")

    where_clause = " OR ".join(conds)
    viol_count = int(
        con.sql(f"SELECT count(*) FROM tbl WHERE {where_clause}").fetchall()[0][0]
    )
    viol_rate = viol_count / total if total > 0 else 0.0
    samples = (
        con.sql(f"SELECT * FROM tbl WHERE {where_clause} LIMIT 5")
        .to_arrow_table()
        .to_pylist()
    )
    passed = viol_rate <= threshold
    error_msg = (
        None
        if passed
        else f"Range and domain check failed: violation rate {viol_rate:.2%} exceeded threshold {threshold:.2%}"
    )
    return ValidationResult(
        check_name="range_domain",
        passed=passed,
        total_records=total,
        violation_count=viol_count,
        violation_rate=viol_rate,
        violations_sample=samples,
        threshold=threshold,
        error_message=error_msg,
    )


def validate_intra_drop_duplicates(
    table: pa.Table | duckdb.DuckDBPyRelation,
    key_cols: list[str],
    threshold: float = 0.0,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> ValidationResult:
    """Detects duplicate primary key combinations within a single drop."""
    tbl = _to_arrow_table(table)
    total = tbl.num_rows
    if total == 0:
        return ValidationResult(
            check_name="intra_drop_duplicates",
            passed=True,
            total_records=0,
            violation_count=0,
            violation_rate=0.0,
            violations_sample=[],
            threshold=threshold,
        )

    if not key_cols:
        return ValidationResult(
            check_name="intra_drop_duplicates",
            passed=False,
            total_records=total,
            violation_count=total,
            violation_rate=1.0,
            violations_sample=[],
            threshold=threshold,
            error_message="No key columns specified for duplicate validation",
        )

    missing_keys = [k for k in key_cols if k not in tbl.column_names]
    if missing_keys:
        return ValidationResult(
            check_name="intra_drop_duplicates",
            passed=False,
            total_records=total,
            violation_count=total,
            violation_rate=1.0,
            violations_sample=[],
            threshold=threshold,
            error_message=f"Key columns {missing_keys} not found in table",
        )

    con = _get_conn(conn)
    con.register("tbl", tbl)

    keys_str = ", ".join([f'"{k}"' for k in key_cols])
    val_sel = (
        ", min(value) as min_val, max(value) as max_val"
        if "value" in tbl.column_names
        else ""
    )
    query = f"""
        WITH grouped AS (
            SELECT {keys_str}, count(*) as cnt{val_sel}
            FROM tbl
            GROUP BY {keys_str}
            HAVING count(*) > 1
        )
        SELECT count(*) as group_cnt, coalesce(sum(cnt), 0) as dup_rows FROM grouped
    """
    stats = con.sql(query).fetchall()[0]
    viol_count = int(stats[0])
    viol_rate = viol_count / total if total > 0 else 0.0
    samples_query = f"""
        WITH grouped AS (
            SELECT {keys_str}, count(*) as cnt{val_sel}
            FROM tbl
            GROUP BY {keys_str}
            HAVING count(*) > 1
        )
        SELECT * FROM grouped LIMIT 5
    """
    samples = con.sql(samples_query).to_arrow_table().to_pylist()
    passed = viol_rate <= threshold
    error_msg = (
        None
        if passed
        else f"Intra-drop duplicates check failed: {viol_count} duplicate key groups found"
    )
    return ValidationResult(
        check_name="intra_drop_duplicates",
        passed=passed,
        total_records=total,
        violation_count=viol_count,
        violation_rate=viol_rate,
        violations_sample=samples,
        threshold=threshold,
        error_message=error_msg,
    )


def validate_monotone_timestamps(
    table: pa.Table | duckdb.DuckDBPyRelation,
    entity_col: str = "entity_id",
    time_col: str = "event_time",
    conn: duckdb.DuckDBPyConnection | None = None,
) -> ValidationResult:
    """Verifies that timestamps for any given entity are non-decreasing."""
    tbl = _to_arrow_table(table)
    total = tbl.num_rows
    if total == 0:
        return ValidationResult(
            check_name="monotone_timestamps",
            passed=True,
            total_records=0,
            violation_count=0,
            violation_rate=0.0,
            violations_sample=[],
            threshold=0.0,
        )

    if entity_col not in tbl.column_names or time_col not in tbl.column_names:
        missing = [c for c in [entity_col, time_col] if c not in tbl.column_names]
        return ValidationResult(
            check_name="monotone_timestamps",
            passed=False,
            total_records=total,
            violation_count=total,
            violation_rate=1.0,
            violations_sample=[],
            threshold=0.0,
            error_message=f"Columns {missing} not found in table",
        )

    con = _get_conn(conn)
    con.register("tbl", tbl)

    cte = f"""
        WITH ordered AS (
            SELECT *,
                lag("{time_col}") OVER (PARTITION BY "{entity_col}" ORDER BY rowid) as prev_time
            FROM (SELECT row_number() OVER () as rowid, * FROM tbl)
        )
    """
    viol_count = int(
        con.sql(
            f'{cte} SELECT count(*) FROM ordered WHERE prev_time IS NOT NULL AND "{time_col}" < prev_time'
        ).fetchall()[0][0]
    )
    viol_rate = viol_count / total if total > 0 else 0.0
    samples = (
        con.sql(
            f'{cte} SELECT * FROM ordered WHERE prev_time IS NOT NULL AND "{time_col}" < prev_time LIMIT 5'
        )
        .to_arrow_table()
        .to_pylist()
    )
    passed = viol_count == 0
    error_msg = (
        None
        if passed
        else f"Monotone timestamps check failed: {viol_count} backwards time jumps detected"
    )
    return ValidationResult(
        check_name="monotone_timestamps",
        passed=passed,
        total_records=total,
        violation_count=viol_count,
        violation_rate=viol_rate,
        violations_sample=samples,
        threshold=0.0,
        error_message=error_msg,
    )


def validate_future_timestamps(
    table: pa.Table | duckdb.DuckDBPyRelation,
    event_col: str = "event_time",
    knowledge_col: str = "knowledge_time",
    tolerance_seconds: int = 300,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> ValidationResult:
    """Guards against lookahead bias: event_time must not exceed knowledge_time + tolerance."""
    tbl = _to_arrow_table(table)
    total = tbl.num_rows
    if total == 0:
        return ValidationResult(
            check_name="future_timestamps",
            passed=True,
            total_records=0,
            violation_count=0,
            violation_rate=0.0,
            violations_sample=[],
            threshold=0.0,
        )

    if event_col not in tbl.column_names or knowledge_col not in tbl.column_names:
        missing = [c for c in [event_col, knowledge_col] if c not in tbl.column_names]
        return ValidationResult(
            check_name="future_timestamps",
            passed=False,
            total_records=total,
            violation_count=total,
            violation_rate=1.0,
            violations_sample=[],
            threshold=0.0,
            error_message=f"Columns {missing} not found in table",
        )

    con = _get_conn(conn)
    con.register("tbl", tbl)

    where_clause = (
        f'try_cast("{event_col}" AS TIMESTAMPTZ) > '
        f'try_cast("{knowledge_col}" AS TIMESTAMPTZ) + to_seconds({int(tolerance_seconds)})'
    )
    viol_count = int(
        con.sql(f"SELECT count(*) FROM tbl WHERE {where_clause}").fetchall()[0][0]
    )
    viol_rate = viol_count / total if total > 0 else 0.0
    samples = (
        con.sql(f"SELECT * FROM tbl WHERE {where_clause} LIMIT 5")
        .to_arrow_table()
        .to_pylist()
        if viol_count > 0
        else []
    )
    passed = viol_count == 0
    error_msg = (
        None
        if passed
        else f"Future timestamps check failed: {viol_count} records violate event_time <= knowledge_time + {tolerance_seconds}s"
    )
    return ValidationResult(
        check_name="future_timestamps",
        passed=passed,
        total_records=total,
        violation_count=viol_count,
        violation_rate=viol_rate,
        violations_sample=samples,
        threshold=0.0,
        error_message=error_msg,
    )


def validate_universe_coverage(
    table: pa.Table | duckdb.DuckDBPyRelation,
    expected_universe: set[str] | list[str],
    entity_col: str = "entity_id",
    threshold: float = 0.90,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> ValidationResult:
    """Verifies that observed entities cover expected benchmark universe within threshold."""
    tbl = _to_arrow_table(table)
    total = tbl.num_rows
    expected = set(expected_universe)
    if total == 0:
        return ValidationResult(
            check_name="universe_coverage",
            passed=True,
            total_records=0,
            violation_count=0,
            violation_rate=0.0,
            violations_sample=[],
            threshold=threshold,
        )

    if entity_col not in tbl.column_names:
        return ValidationResult(
            check_name="universe_coverage",
            passed=False,
            total_records=len(expected),
            violation_count=len(expected),
            violation_rate=1.0,
            violations_sample=[{"missing": sorted(expected)[:5], "unlisted": []}],
            threshold=threshold,
            error_message=f"Column '{entity_col}' not found",
        )

    con = _get_conn(conn)
    con.register("tbl", tbl)

    res = con.sql(
        f'SELECT DISTINCT "{entity_col}" FROM tbl WHERE "{entity_col}" IS NOT NULL'
    ).fetchall()
    observed_entities = {row[0] for row in res}
    missing_entities = expected - observed_entities
    unlisted_additions = observed_entities - expected

    coverage_rate = (
        (len(expected) - len(missing_entities)) / len(expected)
        if expected
        else 1.0
    )
    passed = coverage_rate >= threshold
    sample = [
        {
            "missing": sorted(missing_entities)[:5],
            "unlisted": sorted(unlisted_additions)[:5],
        }
    ]
    viol_count = len(missing_entities)
    viol_rate = 1.0 - coverage_rate
    error_msg = (
        None
        if passed
        else f"Universe coverage check failed: coverage {coverage_rate:.2%} below threshold {threshold:.2%}"
    )
    return ValidationResult(
        check_name="universe_coverage",
        passed=passed,
        total_records=len(expected),
        violation_count=viol_count,
        violation_rate=viol_rate,
        violations_sample=sample,
        threshold=threshold,
        error_message=error_msg,
    )


def validate_positive_values(
    table: pa.Table | duckdb.DuckDBPyRelation,
    metric_col: str = "metric",
    value_col: str = "value",
    positive_metrics: list[str] | None = None,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> ValidationResult:
    """Verifies that intrinsically non-negative metrics do not take negative values."""
    tbl = _to_arrow_table(table)
    total = tbl.num_rows
    if total == 0:
        return ValidationResult(
            check_name="positive_values",
            passed=True,
            total_records=0,
            violation_count=0,
            violation_rate=0.0,
            violations_sample=[],
            threshold=0.0,
        )

    if metric_col not in tbl.column_names or value_col not in tbl.column_names:
        missing = [c for c in [metric_col, value_col] if c not in tbl.column_names]
        return ValidationResult(
            check_name="positive_values",
            passed=False,
            total_records=total,
            violation_count=total,
            violation_rate=1.0,
            violations_sample=[],
            threshold=0.0,
            error_message=f"Columns {missing} not found in table",
        )

    con = _get_conn(conn)
    con.register("tbl", tbl)

    metrics = positive_metrics or ["Assets", "Revenues", "PAYEMS", "GDPC1"]
    safe_metrics = ", ".join("'" + m.replace("'", "''") + "'" for m in metrics)
    query = f"""
        SELECT
            count(*) as total_metric,
            count(*) FILTER (WHERE "{value_col}" < 0) as viol_count
        FROM tbl
        WHERE "{metric_col}" IN ({safe_metrics})
    """
    stats = con.sql(query).fetchall()[0]
    total_metric = int(stats[0])
    viol_count = int(stats[1])
    viol_rate = viol_count / total_metric if total_metric > 0 else 0.0
    samples = (
        con.sql(
            f'SELECT * FROM tbl WHERE "{metric_col}" IN ({safe_metrics}) AND "{value_col}" < 0 LIMIT 5'
        )
        .to_arrow_table()
        .to_pylist()
    )
    passed = viol_count == 0
    error_msg = (
        None
        if passed
        else f"Positive values check failed: {viol_count} negative entries found in positive metrics"
    )
    return ValidationResult(
        check_name="positive_values",
        passed=passed,
        total_records=total_metric,
        violation_count=viol_count,
        violation_rate=viol_rate,
        violations_sample=samples,
        threshold=0.0,
        error_message=error_msg,
    )
