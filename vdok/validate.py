"""Validation rule compilation, execution engine, and severity tier routing."""

from __future__ import annotations

import datetime
import json
import statistics
import time
from enum import Enum
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from pydantic import BaseModel, Field

from vdok.manifest import ExpectedCalendar, VendorManifest
from vdok.normalize import CANONICAL_ARROW_SCHEMA
from vdok.schema import DriftSeverity, SchemaDiff

# 1. Target Schemas for Output Artifacts

QUARANTINE_ARROW_SCHEMA = pa.schema(
    [
        pa.field("entity_id", pa.string(), nullable=False),
        pa.field("entity_id_type", pa.string(), nullable=False),
        pa.field("field", pa.string(), nullable=False),
        pa.field("event_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("knowledge_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("value_num", pa.decimal128(38, 9), nullable=True),
        pa.field("value_str", pa.string(), nullable=True),
        pa.field("unit", pa.string(), nullable=True),
        pa.field("drop_id", pa.string(), nullable=False),
        pa.field("source_file", pa.string(), nullable=False),
        pa.field("source_row", pa.int64(), nullable=False),
        pa.field("row_hash", pa.string(), nullable=False),
        pa.field("unmapped", pa.bool_(), nullable=False),
        pa.field("quarantine_reasons", pa.list_(pa.string()), nullable=False),
        pa.field("quarantined_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)

HELD_DROPS_ARROW_SCHEMA = pa.schema(
    [
        pa.field("drop_id", pa.string(), nullable=False),
        pa.field("vendor", pa.string(), nullable=False),
        pa.field("held_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("reason", pa.string(), nullable=False),
        pa.field("rule_name", pa.string(), nullable=False),
        pa.field("details_json", pa.string(), nullable=False),
    ]
)


# 2. Enums


class RuleSeverity(str, Enum):
    """Operational severity tier for validation rules."""

    WARN = "warn"
    QUARANTINE = "quarantine"
    HOLD = "hold"


class RuleScope(str, Enum):
    """Scope of evaluation for validation rules."""

    ROW = "row"
    BATCH = "batch"


class RuleCheckType(str, Enum):
    """Classification of validation checks."""

    # Row checks
    NOT_NULL = "not_null"
    RANGE = "range"
    ALLOWED_VALUES = "allowed_values"
    REGEX_PATTERN = "regex_pattern"
    ALLOWED_UNITS = "allowed_units"
    DECIMAL_SCALE = "decimal_scale"
    TEMPORAL_BOUNDS = "temporal_bounds"
    TEMPORAL_ORDER = "temporal_order"
    SQL_EXPRESSION = "sql_expression"

    # Batch checks
    SCHEMA_DRIFT = "schema_drift"
    BATCH_VOLUME = "batch_volume"
    BATCH_DUPLICATE_RATIO = "batch_duplicate_ratio"
    BYTE_IDENTICAL = "byte_identical"
    COVERAGE_GAP = "coverage_gap"


# 3. Configuration Models


class RuleDefinition(BaseModel):
    """Declarative specification for an individual validation rule."""

    name: str
    check: RuleCheckType
    severity: RuleSeverity = RuleSeverity.QUARANTINE
    column: str | None = None
    filter: str | None = None

    # Range check parameters
    min: Any | None = None
    max: Any | None = None
    min_inclusive: bool = True
    max_inclusive: bool = True

    # Discrete value and unit check parameters
    values: list[Any] | None = None
    units: list[str] | None = None
    allow_null: bool = True

    # Regex parameter
    pattern: str | None = None

    # Decimal scale parameter
    max_scale: int | None = None

    # Temporal order parameters
    event_time_column: str = "event_time"
    knowledge_time_column: str = "knowledge_time"

    # SQL expression parameter
    expression: str | None = None

    # Batch volume parameters
    ratio_min: float = 0.5
    ratio_max: float = 2.0
    trailing_drops: int = 7
    first_drop_exempt: bool = True

    # Batch duplicate ratio parameters
    key_columns: list[str] = Field(
        default_factory=lambda: ["entity_id", "field", "event_time", "knowledge_time"]
    )
    max_ratio: float = 0.01

    # Schema drift parameters
    disallowed: list[str] = Field(default_factory=lambda: ["removed_column", "type_widened"])

    # Coverage gap parameters
    max_missing_periods: int = 1

    @property
    def scope(self) -> RuleScope:
        batch_checks = {
            RuleCheckType.SCHEMA_DRIFT,
            RuleCheckType.BATCH_VOLUME,
            RuleCheckType.BATCH_DUPLICATE_RATIO,
            RuleCheckType.BYTE_IDENTICAL,
            RuleCheckType.COVERAGE_GAP,
        }
        return RuleScope.BATCH if self.check in batch_checks else RuleScope.ROW


class VendorRules(BaseModel):
    """Collection of validation rules for a specific vendor."""

    vendor: str
    version: int = 1
    rules: list[RuleDefinition] = Field(default_factory=list)

    @classmethod
    def from_yaml(cls, content: str) -> VendorRules:
        data = yaml.safe_load(content)
        if not isinstance(data, dict):
            raise ValueError("Rules content must be a YAML mapping.")
        return cls.model_validate(data)

    @classmethod
    def from_file(cls, path: Path | str) -> VendorRules:
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Rules file not found: {p}")
        return cls.from_yaml(p.read_text(encoding="utf-8"))

    def to_yaml(self) -> str:
        return yaml.dump(self.model_dump(mode="json"), sort_keys=False)

    def to_file(self, path: Path | str) -> None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(self.to_yaml(), encoding="utf-8")


# 4. Context and Output Models


class ValidationContext(BaseModel):
    """Contextual execution parameters supplied to the validation engine."""

    drop_id: str
    vendor: str
    drop_time: datetime.datetime = Field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc)
    )
    manifest: VendorManifest | None = None
    schema_diff: SchemaDiff | None = None
    trailing_row_counts: list[int] = Field(default_factory=list)
    prior_drop_id: str | None = None
    source_files: list[str] = Field(default_factory=list)
    previous_drop_file_hashes: list[str] = Field(default_factory=list)
    current_drop_file_hashes: list[str] = Field(default_factory=list)


class HeldDropRecord(BaseModel):
    """Audit entry recording a batch ingestion hold."""

    drop_id: str
    vendor: str
    held_at: datetime.datetime
    reason: str
    rule_name: str
    details_json: str

    @property
    def severity(self) -> str:
        return "hold"

    @property
    def details(self) -> str:
        return self.details_json


class ValidationSummary(BaseModel):
    """Consolidated summary metrics produced by validation."""

    total_rows: int
    valid_count: int
    quarantine_count: int
    warn_count: int
    quarantine_share: float
    held: bool
    rule_counts: dict[str, int] = Field(default_factory=dict)
    severity_counts: dict[str, int] = Field(default_factory=dict)
    hold_reasons: list[str] = Field(default_factory=list)


class ValidationResult(BaseModel):
    """Complete outcome of drop validation."""

    model_config = {"arbitrary_types_allowed": True}

    drop_id: str
    vendor: str
    held: bool
    valid_table: pa.Table
    quarantine_table: pa.Table
    held_record: HeldDropRecord | None = None
    summary: ValidationSummary
    execution_time_seconds: float = 0.0

    @property
    def total_rows(self) -> int:
        return self.summary.total_rows

    @property
    def valid_count(self) -> int:
        return self.summary.valid_count

    @property
    def quarantine_count(self) -> int:
        return self.summary.quarantine_count

    @property
    def warn_count(self) -> int:
        return self.summary.warn_count

    @property
    def quarantine_share(self) -> float:
        return self.summary.quarantine_share

    @property
    def hold_reasons(self) -> list[str]:
        return self.summary.hold_reasons

    @property
    def rule_counts(self) -> dict[str, int]:
        return self.summary.rule_counts

    @property
    def severity_counts(self) -> dict[str, int]:
        return self.summary.severity_counts


# 5. Rule Compiler


class RuleCompiler:
    """Translates declarative rules into DuckDB SQL expressions."""

    @staticmethod
    def compile_row_predicate(
        rule: RuleDefinition, context: ValidationContext | None = None
    ) -> str:
        """Compile a row check into a boolean SQL failure condition."""
        check = rule.check

        if check == RuleCheckType.NOT_NULL:
            col = rule.column or "entity_id"
            fail_expr = f"{col} IS NULL"

        elif check == RuleCheckType.RANGE:
            col = rule.column or "value_num"
            parts: list[str] = []
            if rule.min is not None:
                op = "<" if rule.min_inclusive else "<="
                parts.append(f"{col} {op} {rule.min}")
            if rule.max is not None:
                op = ">" if rule.max_inclusive else ">="
                parts.append(f"{col} {op} {rule.max}")
            if parts:
                bounds_check = " OR ".join(parts)
                fail_expr = f"({col} IS NOT NULL AND ({bounds_check}))"
            else:
                fail_expr = "FALSE"

        elif check == RuleCheckType.ALLOWED_VALUES:
            col = rule.column or "value_str"
            vals = rule.values or []
            if vals:
                formatted = []
                for v in vals:
                    if isinstance(v, (int, float)):
                        formatted.append(str(v))
                    else:
                        escaped = str(v).replace("'", "''")
                        formatted.append(f"'{escaped}'")
                vals_csv = ", ".join(formatted)
                fail_expr = f"({col} IS NOT NULL AND {col} NOT IN ({vals_csv}))"
            else:
                fail_expr = "FALSE"

        elif check == RuleCheckType.REGEX_PATTERN:
            col = rule.column or "entity_id"
            pat = (rule.pattern or "").replace("'", "''")
            fail_expr = f"({col} IS NOT NULL AND NOT regexp_matches({col}, '{pat}'))"

        elif check == RuleCheckType.ALLOWED_UNITS:
            col = rule.column or "unit"
            units = rule.units or []
            if units:
                formatted = []
                for u in units:
                    escaped_u = str(u).replace("'", "''")
                    formatted.append(f"'{escaped_u}'")
                units_csv = ", ".join(formatted)
                fail_expr = f"({col} IS NOT NULL AND {col} NOT IN ({units_csv}))"
            else:
                fail_expr = "FALSE"

        elif check == RuleCheckType.DECIMAL_SCALE:
            col = rule.column or "value_num"
            scale = rule.max_scale if rule.max_scale is not None else 4
            fail_expr = f"({col} IS NOT NULL AND ROUND({col}, {scale}) != {col})"

        elif check == RuleCheckType.TEMPORAL_BOUNDS:
            col = rule.column or "event_time"
            min_val = str(rule.min or "1990-01-01")
            min_ts = (
                f"{min_val} 00:00:00+00" if (" " not in min_val and "T" not in min_val) else min_val
            )

            max_val = str(rule.max or "drop_time + 1 day")
            if "drop_time" in max_val or "now" in max_val:
                base_dt = (
                    context.drop_time
                    if context and context.drop_time
                    else datetime.datetime.now(datetime.timezone.utc)
                )
                max_dt = base_dt + datetime.timedelta(days=1)
                max_ts = max_dt.strftime("%Y-%m-%d %H:%M:%S+00")
            else:
                max_ts = max_val

            fail_expr = (
                f"({col} IS NOT NULL AND ({col} < '{min_ts}'::TIMESTAMPTZ "
                f"OR {col} > '{max_ts}'::TIMESTAMPTZ))"
            )

        elif check == RuleCheckType.TEMPORAL_ORDER:
            et = rule.event_time_column or "event_time"
            kt = rule.knowledge_time_column or "knowledge_time"
            fail_expr = f"({kt} IS NOT NULL AND {et} IS NOT NULL AND {kt} < {et})"

        elif check == RuleCheckType.SQL_EXPRESSION:
            expr = rule.expression or "TRUE"
            fail_expr = f"NOT ({expr})"

        else:
            fail_expr = "FALSE"

        if rule.filter:
            return f"({rule.filter}) AND ({fail_expr})"
        return fail_expr


# 6. Helper for Calendar Missing Period Calculation


def _calculate_missing_periods(
    dt1: datetime.date | datetime.datetime,
    dt2: datetime.date | datetime.datetime,
    cadence: str,
) -> int:
    """Calculate the number of missing reporting periods between two timestamps."""
    if cadence == "quarterly":
        q1 = (dt1.month - 1) // 3
        q2 = (dt2.month - 1) // 3
        total_quarters = (dt2.year - dt1.year) * 4 + (q2 - q1)
        return max(0, total_quarters - 1)
    elif cadence == "monthly":
        total_months = (dt2.year - dt1.year) * 12 + (dt2.month - dt1.month)
        return max(0, total_months - 1)
    elif cadence == "weekly":
        d1 = dt1 if not isinstance(dt1, datetime.datetime) else dt1.date()
        d2 = dt2 if not isinstance(dt2, datetime.datetime) else dt2.date()
        days = (d2 - d1).days
        total_weeks = days // 7
        return max(0, total_weeks - 1)
    elif cadence == "daily":
        d1 = dt1 if not isinstance(dt1, datetime.datetime) else dt1.date()
        d2 = dt2 if not isinstance(dt2, datetime.datetime) else dt2.date()
        days = (d2 - d1).days
        return max(0, days - 1)
    return 0


# 7. Validation Engine


class ValidationEngine:
    """Vectorized rule execution and multi-tier severity routing engine."""

    def __init__(self, rules: VendorRules | list[RuleDefinition] | None = None) -> None:
        if rules is None:
            self.rules: list[RuleDefinition] = []
        elif isinstance(rules, VendorRules):
            self.rules = list(rules.rules)
        else:
            self.rules = list(rules)

    def validate(self, table: pa.Table, context: ValidationContext) -> ValidationResult:
        """Execute validation across the table and produce a ValidationResult."""
        t_start = time.perf_counter()
        total_rows = len(table)

        con = duckdb.connect()
        try:
            con.register("canonical_table", table)

            # 1. Window ranking for duplicate key detection
            con.sql(
                """
                CREATE TEMPORARY VIEW numbered AS
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY entity_id, field, event_time, knowledge_time
                           ORDER BY source_file, source_row
                       ) AS _vdok_dup_rank
                FROM canonical_table;
                """
            )

            # 2. Count duplicate occurrences
            if total_rows > 0:
                dup_rows = con.sql(
                    "SELECT count(*) FROM numbered WHERE _vdok_dup_rank > 1"
                ).fetchall()
                dup_count = int(dup_rows[0][0]) if dup_rows else 0
            else:
                dup_count = 0
            dup_share = (dup_count / total_rows) if total_rows > 0 else 0.0

            # 3. Evaluate Batch Rules
            hold_reasons: list[str] = []
            failing_batch_rule_name: str | None = None
            batch_details: dict[str, Any] = {}

            # Identify duplicate ratio rule threshold
            dup_rule = next(
                (r for r in self.rules if r.check == RuleCheckType.BATCH_DUPLICATE_RATIO),
                None,
            )
            dup_threshold = dup_rule.max_ratio if dup_rule else 0.01

            if dup_share > dup_threshold:
                r_name = dup_rule.name if dup_rule else "duplicate_threshold"
                reason_str = (
                    f"Rule '{r_name}': Duplicate primary key share {dup_share:.4f} "
                    f"exceeds threshold {dup_threshold:.4f} "
                    f"({dup_count} duplicate rows out of {total_rows})"
                )
                hold_reasons.append(reason_str)
                if not failing_batch_rule_name:
                    failing_batch_rule_name = r_name
                batch_details["duplicate_share"] = dup_share
                batch_details["duplicate_threshold"] = dup_threshold
                batch_details["duplicate_count"] = dup_count

            # Evaluate other batch rules
            for rule in self.rules:
                if rule.scope != RuleScope.BATCH:
                    continue

                if rule.check == RuleCheckType.SCHEMA_DRIFT:
                    diff = context.schema_diff
                    if diff and (
                        diff.has_hold
                        or any(
                            d.category.value in rule.disallowed or d.severity == DriftSeverity.HOLD
                            for d in diff.differences
                        )
                    ):
                        violating = [
                            f"{d.column}:{d.category.value}"
                            for d in diff.differences
                            if d.category.value in rule.disallowed
                            or d.severity == DriftSeverity.HOLD
                        ]
                        reason_str = (
                            f"Rule '{rule.name}': Schema drift hold triggered on "
                            f"columns: {violating}"
                        )
                        hold_reasons.append(reason_str)
                        if not failing_batch_rule_name:
                            failing_batch_rule_name = rule.name
                        batch_details["schema_drift_columns"] = violating

                elif rule.check == RuleCheckType.BATCH_VOLUME:
                    trailing = context.trailing_row_counts
                    if len(trailing) == 0:
                        # First drop exempt
                        pass
                    else:
                        window = trailing[-rule.trailing_drops :]
                        med = statistics.median(window)
                        min_bound = rule.ratio_min * med
                        max_bound = rule.ratio_max * med
                        if total_rows < min_bound or total_rows > max_bound:
                            reason_str = (
                                f"Rule '{rule.name}': Batch row count {total_rows} is outside "
                                f"acceptable interval [{min_bound:.1f}, {max_bound:.1f}] based on "
                                f"trailing median {med:.1f}"
                            )
                            hold_reasons.append(reason_str)
                            if not failing_batch_rule_name:
                                failing_batch_rule_name = rule.name
                            batch_details["batch_rows"] = total_rows
                            batch_details["trailing_median"] = med
                            batch_details["expected_bounds"] = [min_bound, max_bound]

                elif rule.check == RuleCheckType.BYTE_IDENTICAL:
                    # Check drop ID equality
                    if context.prior_drop_id and context.prior_drop_id == context.drop_id:
                        reason_str = (
                            f"Rule '{rule.name}': Drop {context.drop_id} is byte-identical "
                            f"to predecessor drop"
                        )
                        hold_reasons.append(reason_str)
                        if not failing_batch_rule_name:
                            failing_batch_rule_name = rule.name
                        batch_details["prior_drop_id"] = context.prior_drop_id
                    elif (
                        context.previous_drop_file_hashes
                        and context.current_drop_file_hashes
                        and (context.previous_drop_file_hashes == context.current_drop_file_hashes)
                    ):
                        reason_str = (
                            f"Rule '{rule.name}': Current drop file hashes match "
                            f"predecessor drop hashes"
                        )
                        hold_reasons.append(reason_str)
                        if not failing_batch_rule_name:
                            failing_batch_rule_name = rule.name

                elif rule.check == RuleCheckType.COVERAGE_GAP:
                    man = context.manifest
                    if man and man.expected_calendar != ExpectedCalendar.NONE and total_rows > 0:
                        cadence = man.expected_calendar.value
                        trunc_map = {
                            "quarterly": "quarter",
                            "monthly": "month",
                            "weekly": "week",
                            "daily": "day",
                        }
                        trunc_unit = trunc_map.get(cadence)
                        if trunc_unit:
                            rows = con.sql(
                                f"""
                                SELECT DISTINCT
                                    strftime(
                                        date_trunc('{trunc_unit}', event_time), '%Y-%m-%d'
                                    ) AS p
                                FROM canonical_table
                                ORDER BY p
                                """
                            ).fetchall()
                            periods = [
                                datetime.date.fromisoformat(r[0]) for r in rows if r[0] is not None
                            ]
                            for i in range(len(periods) - 1):
                                gap = _calculate_missing_periods(
                                    periods[i], periods[i + 1], cadence
                                )
                                if gap > rule.max_missing_periods:
                                    reason_str = (
                                        f"Rule '{rule.name}': Coverage gap: {gap} "
                                        f"consecutive {cadence} reporting periods missing "
                                        f"between {periods[i]} and {periods[i + 1]}"
                                    )
                                    hold_reasons.append(reason_str)
                                    if not failing_batch_rule_name:
                                        failing_batch_rule_name = rule.name
                                    batch_details["missing_periods"] = gap
                                    batch_details["cadence"] = cadence
                                    break

            held = len(hold_reasons) > 0

            # 4. Compile Row-Level Expressions into Single Vectorized Query
            quarantine_clauses: list[str] = [
                "CASE WHEN _vdok_dup_rank > 1 THEN 'duplicate_key' ELSE NULL END"
            ]
            warn_clauses: list[str] = []

            for rule in self.rules:
                if rule.scope != RuleScope.ROW:
                    continue
                pred = RuleCompiler.compile_row_predicate(rule, context)
                clause = f"CASE WHEN ({pred}) THEN '{rule.name}' ELSE NULL END"
                if rule.severity == RuleSeverity.QUARANTINE:
                    quarantine_clauses.append(clause)
                elif rule.severity == RuleSeverity.WARN:
                    warn_clauses.append(clause)

            quarantine_list_expr = (
                f"list_filter([{', '.join(quarantine_clauses)}], lambda x: x IS NOT NULL)"
                if quarantine_clauses
                else "[]::VARCHAR[]"
            )

            warn_list_expr = (
                f"list_filter([{', '.join(warn_clauses)}], lambda x: x IS NOT NULL)"
                if warn_clauses
                else "[]::VARCHAR[]"
            )

            now_iso = datetime.datetime.now(datetime.timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S.%f+00"
            )

            con.sql(
                f"""
                CREATE TEMPORARY TABLE evaluated AS
                SELECT *,
                       {quarantine_list_expr} AS quarantine_reasons,
                       {warn_list_expr} AS warn_reasons,
                       '{now_iso}'::TIMESTAMPTZ AS quarantined_at
                FROM numbered;
                """
            )

            # 5. Extract Valid Table and Quarantine Table
            quarantine_rel = con.sql(
                """
                SELECT entity_id, entity_id_type, field, event_time, knowledge_time,
                       value_num, value_str, unit, drop_id, source_file, source_row,
                       row_hash, unmapped, quarantine_reasons, quarantined_at
                FROM evaluated
                WHERE len(quarantine_reasons) > 0;
                """
            )
            raw_quarantine_table = quarantine_rel.to_arrow_table()
            quarantine_table = raw_quarantine_table.cast(QUARANTINE_ARROW_SCHEMA)
            quarantine_count = len(quarantine_table)

            if held:
                # On hold: halt main ingestion, valid_table is empty
                valid_table = pa.Table.from_batches([], schema=CANONICAL_ARROW_SCHEMA)
                valid_count = 0
            else:
                valid_rel = con.sql(
                    """
                    SELECT entity_id, entity_id_type, field, event_time, knowledge_time,
                           value_num, value_str, unit, drop_id, source_file, source_row,
                           row_hash, unmapped
                    FROM evaluated
                    WHERE len(quarantine_reasons) = 0;
                    """
                )
                raw_valid_table = valid_rel.to_arrow_table()
                valid_table = raw_valid_table.cast(CANONICAL_ARROW_SCHEMA)
                valid_count = len(valid_table)

            # 6. Extract Rule Breakdown and Warn Count
            rule_counts: dict[str, int] = {}
            if total_rows > 0:
                breakdown = con.sql(
                    """
                    SELECT rule_name, count(*) AS cnt
                    FROM (
                        SELECT unnest(quarantine_reasons) AS rule_name FROM evaluated
                        UNION ALL
                        SELECT unnest(warn_reasons) AS rule_name FROM evaluated
                    )
                    GROUP BY rule_name;
                    """
                ).fetchall()
                for r_name, cnt in breakdown:
                    rule_counts[str(r_name)] = int(cnt)

                warn_rows = con.sql(
                    "SELECT count(*) FROM evaluated WHERE len(warn_reasons) > 0"
                ).fetchall()
                warn_count = int(warn_rows[0][0]) if warn_rows else 0
            else:
                warn_count = 0

            # Add failing batch rules to rule counts
            for hr in hold_reasons:
                rule_key = failing_batch_rule_name or "batch_rule"
                rule_counts[rule_key] = rule_counts.get(rule_key, 0) + 1

            severity_counts = {
                RuleSeverity.WARN.value: warn_count,
                RuleSeverity.QUARANTINE.value: quarantine_count,
                RuleSeverity.HOLD.value: len(hold_reasons),
            }

            # 7. Construct HeldDropRecord if batch held
            held_record: HeldDropRecord | None = None
            if held:
                held_record = HeldDropRecord(
                    drop_id=context.drop_id,
                    vendor=context.vendor,
                    held_at=datetime.datetime.now(datetime.timezone.utc),
                    reason="; ".join(hold_reasons),
                    rule_name=failing_batch_rule_name or "batch_hold",
                    details_json=json.dumps(batch_details),
                )

            t_end = time.perf_counter()
            summary = ValidationSummary(
                total_rows=total_rows,
                valid_count=valid_count,
                quarantine_count=quarantine_count,
                warn_count=warn_count,
                quarantine_share=(quarantine_count / total_rows) if total_rows > 0 else 0.0,
                held=held,
                rule_counts=rule_counts,
                severity_counts=severity_counts,
                hold_reasons=hold_reasons,
            )

            return ValidationResult(
                drop_id=context.drop_id,
                vendor=context.vendor,
                held=held,
                valid_table=valid_table,
                quarantine_table=quarantine_table,
                held_record=held_record,
                summary=summary,
                execution_time_seconds=(t_end - t_start),
            )

        finally:
            con.close()


# 8. Persistence Utilities


def write_quarantine_table(
    table: pa.Table, store_dir: Path | str, drop_id: str, vendor: str = ""
) -> Path:
    """Persist quarantined records to store/<vendor>/quarantine/part-<drop_id>.parquet."""
    base_path = Path(store_dir)
    if vendor and base_path.name != vendor:
        q_dir = base_path / vendor / "quarantine"
    else:
        q_dir = base_path / "quarantine"

    q_dir.mkdir(parents=True, exist_ok=True)
    target_file = q_dir / f"part-{drop_id}.parquet"
    casted = table.cast(QUARANTINE_ARROW_SCHEMA)
    pq.write_table(casted, target_file)
    return target_file


def write_held_record(record: HeldDropRecord, store_dir: Path | str) -> Path:
    """Persist or append a held drop record to store/<vendor>/metadata/held_drops.parquet."""
    base_path = Path(store_dir)
    if base_path.name == record.vendor:
        meta_dir = base_path / "metadata"
    else:
        meta_dir = base_path / record.vendor / "metadata"

    meta_dir.mkdir(parents=True, exist_ok=True)
    target_file = meta_dir / "held_drops.parquet"

    row_dict = {
        "drop_id": [record.drop_id],
        "vendor": [record.vendor],
        "held_at": [record.held_at],
        "reason": [record.reason],
        "rule_name": [record.rule_name],
        "details_json": [record.details_json],
    }
    new_table = pa.Table.from_pydict(row_dict, schema=HELD_DROPS_ARROW_SCHEMA)

    if target_file.exists():
        existing_table = pq.read_table(target_file)
        combined_table = pa.concat_tables([existing_table.cast(HELD_DROPS_ARROW_SCHEMA), new_table])
        pq.write_table(combined_table, target_file)
    else:
        pq.write_table(new_table, target_file)

    return target_file


# 9. Public API Entry Points


def load_rules(path: Path | str) -> VendorRules:
    """Load vendor rules from a YAML configuration file."""
    return VendorRules.from_file(path)


def get_default_rules(vendor: str) -> VendorRules:
    """Construct standard validation rules for a vendor feed."""
    return VendorRules(
        vendor=vendor,
        version=1,
        rules=[
            RuleDefinition(
                name="not_null_entity_id",
                check=RuleCheckType.NOT_NULL,
                column="entity_id",
                severity=RuleSeverity.QUARANTINE,
            ),
            RuleDefinition(
                name="not_null_field",
                check=RuleCheckType.NOT_NULL,
                column="field",
                severity=RuleSeverity.QUARANTINE,
            ),
            RuleDefinition(
                name="not_null_event_time",
                check=RuleCheckType.NOT_NULL,
                column="event_time",
                severity=RuleSeverity.QUARANTINE,
            ),
            RuleDefinition(
                name="not_null_knowledge_time",
                check=RuleCheckType.NOT_NULL,
                column="knowledge_time",
                severity=RuleSeverity.QUARANTINE,
            ),
            RuleDefinition(
                name="plausible_event_time",
                check=RuleCheckType.TEMPORAL_BOUNDS,
                column="event_time",
                min="1990-01-01",
                max="drop_time + 1 day",
                severity=RuleSeverity.QUARANTINE,
            ),
            RuleDefinition(
                name="knowledge_after_event",
                check=RuleCheckType.TEMPORAL_ORDER,
                event_time_column="event_time",
                knowledge_time_column="knowledge_time",
                severity=RuleSeverity.QUARANTINE,
            ),
            RuleDefinition(
                name="schema_drift_guard",
                check=RuleCheckType.SCHEMA_DRIFT,
                severity=RuleSeverity.HOLD,
            ),
            RuleDefinition(
                name="volume_stability",
                check=RuleCheckType.BATCH_VOLUME,
                ratio_min=0.5,
                ratio_max=2.0,
                trailing_drops=7,
                first_drop_exempt=True,
                severity=RuleSeverity.HOLD,
            ),
            RuleDefinition(
                name="duplicate_threshold",
                check=RuleCheckType.BATCH_DUPLICATE_RATIO,
                max_ratio=0.01,
                severity=RuleSeverity.HOLD,
            ),
            RuleDefinition(
                name="redundant_drop_guard",
                check=RuleCheckType.BYTE_IDENTICAL,
                severity=RuleSeverity.HOLD,
            ),
            RuleDefinition(
                name="coverage_gap_guard",
                check=RuleCheckType.COVERAGE_GAP,
                max_missing_periods=1,
                severity=RuleSeverity.HOLD,
            ),
        ],
    )


def validate_drop(
    table: pa.Table,
    manifest: VendorManifest,
    drop_id: str | None = None,
    drop_time: datetime.datetime | None = None,
    rules: VendorRules | Path | str | None = None,
    rules_path: Path | str | None = None,
    schema_diff: SchemaDiff | None = None,
    trailing_row_counts: list[int] | None = None,
    historical_drop_row_counts: list[int] | None = None,
    prior_drop_id: str | None = None,
    previous_drop_file_hashes: list[str] | None = None,
    current_drop_file_hashes: list[str] | None = None,
    source_files: list[str] | None = None,
    drop_meta: Any | None = None,
    store_dir: Path | str | None = None,
) -> ValidationResult:
    """Validate a normalized table against declarative rules with multi-tier severity."""
    # Resolve drop identifier
    effective_drop_id: str = "unknown"
    if drop_id:
        effective_drop_id = drop_id
    elif drop_meta and hasattr(drop_meta, "drop_id"):
        effective_drop_id = str(drop_meta.drop_id)
    elif len(table) > 0 and "drop_id" in table.column_names:
        effective_drop_id = str(table["drop_id"][0].as_py())

    # Resolve drop timestamp
    effective_drop_time: datetime.datetime
    if drop_time:
        effective_drop_time = drop_time
    elif drop_meta and hasattr(drop_meta, "drop_time") and drop_meta.drop_time:
        effective_drop_time = drop_meta.drop_time
    else:
        effective_drop_time = datetime.datetime.now(datetime.timezone.utc)

    # Resolve rules
    resolved_rules: VendorRules
    if isinstance(rules, VendorRules):
        resolved_rules = rules
    elif rules is not None and isinstance(rules, (str, Path)):
        resolved_rules = load_rules(rules)
    elif rules_path is not None:
        resolved_rules = load_rules(rules_path)
    elif manifest.rules and Path(manifest.rules).exists():
        resolved_rules = load_rules(manifest.rules)
    elif Path(f"rules/{manifest.vendor}.yaml").exists():
        resolved_rules = load_rules(f"rules/{manifest.vendor}.yaml")
    else:
        resolved_rules = get_default_rules(manifest.vendor)

    # Resolve trailing row counts
    counts = (
        trailing_row_counts
        if trailing_row_counts is not None
        else (historical_drop_row_counts or [])
    )

    # Resolve source files
    files = source_files or []
    if not files and drop_meta and hasattr(drop_meta, "files"):
        files = [str(f.path) for f in drop_meta.files]

    # Resolve file hashes
    cur_hashes = current_drop_file_hashes or []
    if not cur_hashes and drop_meta and hasattr(drop_meta, "files"):
        cur_hashes = [str(f.sha256) for f in drop_meta.files]

    context = ValidationContext(
        drop_id=effective_drop_id,
        vendor=manifest.vendor,
        drop_time=effective_drop_time,
        manifest=manifest,
        schema_diff=schema_diff,
        trailing_row_counts=counts,
        prior_drop_id=prior_drop_id,
        source_files=files,
        previous_drop_file_hashes=previous_drop_file_hashes or [],
        current_drop_file_hashes=cur_hashes,
    )

    engine = ValidationEngine(resolved_rules)
    result = engine.validate(table, context)

    # Optionally write persistent outputs when store_dir is supplied
    if store_dir:
        if result.held and result.held_record:
            write_held_record(result.held_record, store_dir)
        if len(result.quarantine_table) > 0:
            write_quarantine_table(
                result.quarantine_table, store_dir, effective_drop_id, manifest.vendor
            )

    return result
