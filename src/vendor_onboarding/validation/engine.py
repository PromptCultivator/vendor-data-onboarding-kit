"""Vectorized Validation Engine coordinating data quality rule execution."""

from __future__ import annotations

from types import TracebackType
from typing import Any, Self

import duckdb
import pyarrow as pa

from vendor_onboarding.validation.rules import (
    ValidationResult,
    _to_arrow_table,
    validate_completeness,
    validate_future_timestamps,
    validate_intra_drop_duplicates,
    validate_monotone_timestamps,
    validate_positive_values,
    validate_range_and_domain,
    validate_universe_coverage,
)


class ValidationEngine:
    """High-throughput vectorized validation engine executing quality checks in DuckDB."""

    def __init__(self, conn: duckdb.DuckDBPyConnection | None = None) -> None:
        self.conn = conn or duckdb.connect()
        self.conn.execute("SET timezone = 'UTC';")

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        """Closes the underlying DuckDB connection."""
        if hasattr(self, "conn") and self.conn is not None:
            self.conn.close()

    def validate_completeness(
        self,
        table: pa.Table | duckdb.DuckDBPyRelation,
        mandatory_cols: list[str],
        threshold: float = 0.0,
    ) -> ValidationResult:
        """Validates that mandatory columns do not contain null values beyond SLA threshold."""
        return validate_completeness(
            table, mandatory_cols=mandatory_cols, threshold=threshold, conn=self.conn
        )

    def validate_range_and_domain(
        self,
        table: pa.Table | duckdb.DuckDBPyRelation,
        value_col: str = "value",
        threshold: float = 0.0,
        min_val: float | None = 0.0,
        max_val: float | None = None,
    ) -> ValidationResult:
        """Validates that values fall within acceptable numerical domains."""
        return validate_range_and_domain(
            table,
            value_col=value_col,
            threshold=threshold,
            min_val=min_val,
            max_val=max_val,
            conn=self.conn,
        )

    def validate_intra_drop_duplicates(
        self,
        table: pa.Table | duckdb.DuckDBPyRelation,
        key_cols: list[str],
        threshold: float = 0.0,
    ) -> ValidationResult:
        """Detects duplicate primary key combinations within a single drop."""
        return validate_intra_drop_duplicates(
            table, key_cols=key_cols, threshold=threshold, conn=self.conn
        )

    def validate_monotone_timestamps(
        self,
        table: pa.Table | duckdb.DuckDBPyRelation,
        entity_col: str = "entity_id",
        time_col: str = "event_time",
    ) -> ValidationResult:
        """Verifies that timestamps for any given entity are non-decreasing."""
        return validate_monotone_timestamps(
            table, entity_col=entity_col, time_col=time_col, conn=self.conn
        )

    def validate_future_timestamps(
        self,
        table: pa.Table | duckdb.DuckDBPyRelation,
        event_col: str = "event_time",
        knowledge_col: str = "knowledge_time",
        tolerance_seconds: int = 300,
    ) -> ValidationResult:
        """Guards against lookahead bias: event_time must not exceed knowledge_time + tolerance."""
        return validate_future_timestamps(
            table,
            event_col=event_col,
            knowledge_col=knowledge_col,
            tolerance_seconds=tolerance_seconds,
            conn=self.conn,
        )

    def validate_universe_coverage(
        self,
        table: pa.Table | duckdb.DuckDBPyRelation,
        expected_universe: set[str],
        entity_col: str = "entity_id",
        threshold: float = 0.90,
    ) -> ValidationResult:
        """Verifies that observed entities cover expected benchmark universe within threshold."""
        return validate_universe_coverage(
            table,
            expected_universe=expected_universe,
            entity_col=entity_col,
            threshold=threshold,
            conn=self.conn,
        )

    def validate_positive_values(
        self,
        table: pa.Table | duckdb.DuckDBPyRelation,
        metric_col: str = "metric",
        value_col: str = "value",
        positive_metrics: list[str] | None = None,
    ) -> ValidationResult:
        """Verifies that intrinsically non-negative metrics do not take negative values."""
        return validate_positive_values(
            table,
            metric_col=metric_col,
            value_col=value_col,
            positive_metrics=positive_metrics,
            conn=self.conn,
        )

    def validate(
        self,
        table: pa.Table | duckdb.DuckDBPyRelation,
        rules_config: dict[str, Any] | None = None,
    ) -> list[ValidationResult]:
        """Executes validation rules against the given table based on configuration."""
        tbl = _to_arrow_table(table)
        results: list[ValidationResult] = []

        if rules_config is None:
            # Default auto-discovery of applicable rules
            cols = tbl.column_names
            common_mandatory = [c for c in ["record_id", "entity_id", "value", "event_time"] if c in cols]
            if common_mandatory:
                results.append(self.validate_completeness(tbl, common_mandatory))
            if "value" in cols:
                results.append(self.validate_range_and_domain(tbl, "value"))
            if "entity_id" in cols and "event_time" in cols:
                results.append(self.validate_monotone_timestamps(tbl))
            if "event_time" in cols and "knowledge_time" in cols:
                results.append(self.validate_future_timestamps(tbl))
            return results

        for rule_name, config in rules_config.items():
            norm_name = rule_name.lower().replace("-", "_")
            cfg = config if isinstance(config, dict) else {}

            if norm_name in ("completeness", "validate_completeness"):
                mandatory = config if isinstance(config, list) else cfg.get("mandatory_cols", [])
                thresh = cfg.get("threshold", 0.0) if isinstance(config, dict) else 0.0
                results.append(self.validate_completeness(tbl, mandatory, threshold=thresh))
            elif norm_name in ("range_and_domain", "range_domain", "validate_range_and_domain"):
                results.append(self.validate_range_and_domain(tbl, **cfg))
            elif norm_name in ("intra_drop_duplicates", "duplicates", "validate_intra_drop_duplicates"):
                key_cols = config if isinstance(config, list) else cfg.get("key_cols", [])
                thresh = cfg.get("threshold", 0.0) if isinstance(config, dict) else 0.0
                results.append(self.validate_intra_drop_duplicates(tbl, key_cols, threshold=thresh))
            elif norm_name in ("monotone_timestamps", "monotone", "validate_monotone_timestamps"):
                results.append(self.validate_monotone_timestamps(tbl, **cfg))
            elif norm_name in ("future_timestamps", "future", "validate_future_timestamps"):
                results.append(self.validate_future_timestamps(tbl, **cfg))
            elif norm_name in ("universe_coverage", "universe", "validate_universe_coverage"):
                exp_univ = config if isinstance(config, set) else cfg.get("expected_universe", set())
                ent_col = cfg.get("entity_col", "entity_id")
                thresh = cfg.get("threshold", 0.90)
                results.append(self.validate_universe_coverage(tbl, exp_univ, entity_col=ent_col, threshold=thresh))
            elif norm_name in ("positive_values", "positive", "validate_positive_values"):
                results.append(self.validate_positive_values(tbl, **cfg))

        return results


VectorizedValidationEngine = ValidationEngine
