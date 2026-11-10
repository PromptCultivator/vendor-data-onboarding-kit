"""Bitemporal revision ledger engine and rate calculations."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, Field

from vdok.normalize import CANONICAL_ARROW_SCHEMA

REVISIONS_ARROW_SCHEMA = pa.schema(
    [
        pa.field("entity_id", pa.string(), nullable=False),
        pa.field("field", pa.string(), nullable=False),
        pa.field("event_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("old_value_num", pa.decimal128(38, 9), nullable=True),
        pa.field("new_value_num", pa.decimal128(38, 9), nullable=True),
        pa.field("old_value_str", pa.string(), nullable=True),
        pa.field("new_value_str", pa.string(), nullable=True),
        pa.field("old_unit", pa.string(), nullable=True),
        pa.field("new_unit", pa.string(), nullable=True),
        pa.field("old_knowledge_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("new_knowledge_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("old_drop_id", pa.string(), nullable=False),
        pa.field("new_drop_id", pa.string(), nullable=False),
        pa.field("relative_change", pa.float64(), nullable=True),
    ]
)


class RevisionStats(BaseModel):
    """Statistical summary of detected revisions and silent overwrite indicators."""

    vendor: str
    drop_id: str | None = None
    total_revisions: int = 0
    total_valid_rows: int = 0
    drop_rate: float = 0.0
    field_rates: dict[str, float] = Field(default_factory=dict)
    silent_overwrite_fields: list[str] = Field(default_factory=list)
    largest_changes: list[dict[str, Any]] = Field(default_factory=list)

    @property
    def rate_per_field(self) -> dict[str, float]:
        return self.field_rates


RevisionSummary = RevisionStats


def detect_revisions(
    con: duckdb.DuckDBPyConnection,
    vendor: str,
    incoming_drop_id: str,
    base_path: Path | str = Path("data/store"),
    incoming_table: pa.Table | None = None,
) -> pa.Table:
    """Detect value restatements by comparing incoming drop rows against prior store events."""
    base = Path(base_path)
    vendor_dir = base if base.name == vendor else base / vendor

    # Verify if vendor_events view is registered, or create it
    views = [v[0] for v in con.execute("SHOW TABLES").fetchall()]
    if "vendor_events" not in views:
        resolved_path = vendor_dir.resolve()
        part_files = list(resolved_path.glob("*/*/*.parquet"))
        if part_files:
            glob_p = f"{resolved_path}/*/*/*.parquet"
            con.execute(
                "CREATE OR REPLACE VIEW vendor_events AS "
                f"SELECT * FROM read_parquet('{glob_p}', "
                "hive_partitioning=false)"
            )
        else:
            empty_canonical = CANONICAL_ARROW_SCHEMA.empty_table()
            con.register("_empty_canonical_for_rev", empty_canonical)
            con.execute(
                "CREATE OR REPLACE VIEW vendor_events AS SELECT * FROM _empty_canonical_for_rev"
            )

    if incoming_table is not None:
        con.register("_incoming_source_table", incoming_table)
        con.execute(
            "CREATE OR REPLACE TEMPORARY VIEW incoming_drop_source AS "
            "SELECT * FROM _incoming_source_table"
        )
    else:
        con.execute(
            "CREATE OR REPLACE TEMPORARY VIEW incoming_drop_source AS "
            f"SELECT * FROM vendor_events WHERE drop_id = '{incoming_drop_id}'"
        )

    # Core bitemporal revision detection query
    query = f"""
    WITH current_drop AS (
        SELECT * EXCLUDE (rn)
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY entity_id, field, event_time
                ORDER BY knowledge_time DESC, source_row DESC
            ) as rn
            FROM incoming_drop_source
        ) WHERE rn = 1
    ),
    prior_latest AS (
        SELECT
            entity_id,
            field,
            event_time,
            knowledge_time AS old_knowledge_time,
            value_num AS old_value_num,
            value_str AS old_value_str,
            unit AS old_unit,
            drop_id AS old_drop_id,
            ROW_NUMBER() OVER (
                PARTITION BY entity_id, field, event_time
                ORDER BY knowledge_time DESC, drop_id DESC
            ) AS rn
        FROM vendor_events
        WHERE drop_id != '{incoming_drop_id}'
    )
    SELECT
        curr.entity_id,
        curr.field,
        curr.event_time,
        prev.old_value_num,
        curr.value_num AS new_value_num,
        prev.old_value_str,
        curr.value_str AS new_value_str,
        prev.old_unit,
        curr.unit AS new_unit,
        prev.old_knowledge_time,
        curr.knowledge_time AS new_knowledge_time,
        prev.old_drop_id,
        curr.drop_id AS new_drop_id,
        CASE
            WHEN prev.old_value_num IS NOT NULL
                 AND prev.old_value_num != 0
                 AND curr.value_num IS NOT NULL
            THEN (curr.value_num::DOUBLE - prev.old_value_num::DOUBLE)
                 / ABS(prev.old_value_num::DOUBLE)
            ELSE NULL
        END AS relative_change
    FROM current_drop curr
    JOIN prior_latest prev
      ON curr.entity_id = prev.entity_id
     AND curr.field = prev.field
     AND curr.event_time = prev.event_time
     AND prev.rn = 1
    WHERE curr.knowledge_time > prev.old_knowledge_time
      AND (
          curr.value_num IS DISTINCT FROM prev.old_value_num
          OR curr.value_str IS DISTINCT FROM prev.old_value_str
          OR curr.unit IS DISTINCT FROM prev.old_unit
      );
    """

    raw_table = con.sql(query).to_arrow_table()
    casted_table = raw_table.cast(REVISIONS_ARROW_SCHEMA)
    return casted_table


def record_revisions(base_path: Path | str, vendor: str, revisions_table: pa.Table) -> None:
    """Persist detected revisions to metadata/revisions.parquet and sync DuckDB table."""
    base = Path(base_path)
    vendor_dir = base if base.name == vendor else base / vendor
    meta_dir = vendor_dir / "metadata"
    meta_dir.mkdir(parents=True, exist_ok=True)

    revisions_file = meta_dir / "revisions.parquet"

    # Idempotent write: filter out duplicates if already present
    if revisions_file.exists():
        existing_table = pq.read_table(revisions_file).cast(REVISIONS_ARROW_SCHEMA)
        if len(revisions_table) > 0:
            # Check if any new rows exist that are not in existing table
            con = duckdb.connect()
            try:
                con.register("existing_tbl", existing_table)
                con.register("new_tbl", revisions_table)
                deduped = con.sql(
                    """
                    SELECT * FROM existing_tbl
                    UNION ALL
                    SELECT * FROM new_tbl n
                    WHERE NOT EXISTS (
                        SELECT 1 FROM existing_tbl e
                        WHERE e.new_drop_id = n.new_drop_id
                          AND e.entity_id = n.entity_id
                          AND e.field = n.field
                          AND e.event_time = n.event_time
                    )
                    """
                ).to_arrow_table()
                final_table = deduped.cast(REVISIONS_ARROW_SCHEMA)
            finally:
                con.close()
        else:
            final_table = existing_table
    else:
        final_table = revisions_table.cast(REVISIONS_ARROW_SCHEMA)

    pq.write_table(final_table, revisions_file)

    # Sync with store.duckdb if catalog file exists
    db_file = vendor_dir / "store.duckdb"
    if db_file.exists():
        con = duckdb.connect(str(db_file))
        try:
            con.execute("DELETE FROM revisions;")
            if len(final_table) > 0:
                con.register("sync_rev", final_table)
                con.execute("INSERT INTO revisions SELECT * FROM sync_rev;")
        finally:
            con.close()


def compute_revision_rates(
    base_path: Path | str, vendor: str, drop_id: str | None = None
) -> RevisionStats:
    """Compute drop-level and field-level revision frequencies and silent overwrite diagnostics."""
    base = Path(base_path)
    vendor_dir = base if base.name == vendor else base / vendor
    meta_dir = vendor_dir / "metadata"
    revisions_file = meta_dir / "revisions.parquet"

    con = duckdb.connect()
    try:
        resolved_path = vendor_dir.resolve()
        part_files = list(resolved_path.glob("*/*/*.parquet"))
        if not part_files:
            return RevisionStats(vendor=vendor, drop_id=drop_id)

        con.execute(
            "CREATE VIEW vendor_events AS "
            f"SELECT * FROM read_parquet('{resolved_path}/*/*/*.parquet', hive_partitioning=false)"
        )

        has_revisions = revisions_file.exists()
        if has_revisions:
            con.execute(
                "CREATE VIEW revisions_view AS "
                f"SELECT * FROM read_parquet('{revisions_file.resolve()}')"
            )
        else:
            empty_rev = REVISIONS_ARROW_SCHEMA.empty_table()
            con.register("_empty_rev", empty_rev)
            con.execute("CREATE VIEW revisions_view AS SELECT * FROM _empty_rev")

        # Total valid rows
        if drop_id is not None:
            valid_rows_res = con.execute(
                "SELECT COUNT(*) FROM vendor_events WHERE drop_id = ?", [drop_id]
            ).fetchone()
            total_valid_rows = valid_rows_res[0] if valid_rows_res else 0

            rev_count_res = con.execute(
                "SELECT COUNT(*) FROM revisions_view WHERE new_drop_id = ?", [drop_id]
            ).fetchone()
            total_revisions = rev_count_res[0] if rev_count_res else 0

            field_stats = con.execute(
                """
                WITH v_counts AS (
                    SELECT field, COUNT(*) AS valid_cnt
                    FROM vendor_events
                    WHERE drop_id = ?
                    GROUP BY field
                ),
                r_counts AS (
                    SELECT field, COUNT(*) AS rev_cnt
                    FROM revisions_view
                    WHERE new_drop_id = ?
                    GROUP BY field
                )
                SELECT v.field, COALESCE(r.rev_cnt, 0) AS rev_cnt, v.valid_cnt
                FROM v_counts v
                LEFT JOIN r_counts r ON v.field = r.field
                """,
                [drop_id, drop_id],
            ).fetchall()

            largest_rows = con.execute(
                """
                SELECT entity_id, field,
                       strftime(event_time, '%Y-%m-%dT%H:%M:%SZ') AS event_time,
                       old_value_num, new_value_num, relative_change
                FROM revisions_view
                WHERE new_drop_id = ? AND relative_change IS NOT NULL
                ORDER BY ABS(relative_change) DESC
                LIMIT 10
                """,
                [drop_id],
            ).fetchall()

        else:
            valid_rows_res = con.execute("SELECT COUNT(*) FROM vendor_events").fetchone()
            total_valid_rows = valid_rows_res[0] if valid_rows_res else 0

            rev_count_res = con.execute("SELECT COUNT(*) FROM revisions_view").fetchone()
            total_revisions = rev_count_res[0] if rev_count_res else 0

            field_stats = con.execute(
                """
                WITH v_counts AS (
                    SELECT field, COUNT(*) AS valid_cnt
                    FROM vendor_events
                    GROUP BY field
                ),
                r_counts AS (
                    SELECT field, COUNT(*) AS rev_cnt
                    FROM revisions_view
                    GROUP BY field
                )
                SELECT v.field, COALESCE(r.rev_cnt, 0) AS rev_cnt, v.valid_cnt
                FROM v_counts v
                LEFT JOIN r_counts r ON v.field = r.field
                """
            ).fetchall()

            largest_rows = con.execute(
                """
                SELECT entity_id, field,
                       strftime(event_time, '%Y-%m-%dT%H:%M:%SZ') AS event_time,
                       old_value_num, new_value_num, relative_change
                FROM revisions_view
                WHERE relative_change IS NOT NULL
                ORDER BY ABS(relative_change) DESC
                LIMIT 10
                """
            ).fetchall()

        field_rates: dict[str, float] = {}
        for row in field_stats:
            f_name, r_cnt, v_cnt = row[0], row[1], row[2]
            field_rates[f_name] = (r_cnt / v_cnt) if v_cnt > 0 else 0.0

        drop_rate = (total_revisions / total_valid_rows) if total_valid_rows > 0 else 0.0

        # Silent overwrite diagnostic: fields with 0 revisions across > 3 distinct historical drops
        silent_rows = con.execute(
            """
            WITH drops_per_field AS (
                SELECT field, COUNT(DISTINCT drop_id) AS drop_count
                FROM vendor_events
                GROUP BY field
                HAVING COUNT(DISTINCT drop_id) > 3
            ),
            rev_per_field AS (
                SELECT field, COUNT(*) AS rev_count
                FROM revisions_view
                GROUP BY field
            )
            SELECT d.field
            FROM drops_per_field d
            LEFT JOIN rev_per_field r ON d.field = r.field
            WHERE COALESCE(r.rev_count, 0) = 0
            ORDER BY d.field
            """
        ).fetchall()

        silent_overwrite_fields = [r[0] for r in silent_rows]

        largest_changes: list[dict[str, Any]] = [
            {
                "entity_id": r[0],
                "field": r[1],
                "event_time": str(r[2]),
                "old_value_num": str(r[3]),
                "new_value_num": str(r[4]),
                "relative_change": r[5],
            }
            for r in largest_rows
        ]

        return RevisionStats(
            vendor=vendor,
            drop_id=drop_id,
            total_revisions=total_revisions,
            total_valid_rows=total_valid_rows,
            drop_rate=drop_rate,
            field_rates=field_rates,
            silent_overwrite_fields=silent_overwrite_fields,
            largest_changes=largest_changes,
        )

    finally:
        con.close()


def compute_revisions(
    store_or_base: Any,
    vendor: str,
    drop_id: str,
    incoming_table: pa.Table | None = None,
) -> tuple[RevisionStats, pa.Table]:
    """Convenience helper to detect and compute revision rates."""
    base_path = getattr(store_or_base, "base_dir", store_or_base)
    con = duckdb.connect()
    try:
        rev_table = detect_revisions(
            con=con,
            vendor=vendor,
            incoming_drop_id=drop_id,
            base_path=base_path,
            incoming_table=incoming_table,
        )
    finally:
        con.close()

    if len(rev_table) > 0:
        record_revisions(base_path=base_path, vendor=vendor, revisions_table=rev_table)

    stats = compute_revision_rates(base_path=base_path, vendor=vendor, drop_id=drop_id)
    return stats, rev_table
