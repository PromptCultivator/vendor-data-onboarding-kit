"""Point-in-time query API, brute-force reference oracle, and look-ahead leak detector."""

from __future__ import annotations

import datetime
import json
import secrets
import threading
import warnings
from collections import defaultdict
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.parquet as pq
from pydantic import BaseModel, Field

from vdok.normalize import normalize_identifier, parse_timestamp_utc
from vdok.store import resolve_field_bucket


class StoreRecord(dict[str, Any]):
    """Subscriptable and attribute-accessible record for query results."""

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as err:
            raise AttributeError(f"No attribute '{name}'") from err

    def __setattr__(self, name: str, value: Any) -> None:
        self[name] = value


class AuditLogger:
    """Thread-safe JSON Lines audit logger for query provenance and auditability."""

    def __init__(
        self,
        log_path: Path | str,
        session_id: str | None = None,
    ) -> None:
        self.log_path = Path(log_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.session_id = session_id or secrets.token_hex(4)
        self._lock = threading.Lock()

    def log_read(
        self,
        decision_time: datetime.datetime | str,
        as_of: datetime.datetime | str,
        key: str,
        knowledge_time_returned: datetime.datetime | str | None,
        query_type: str = "value",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Write single JSON Lines audit entry with immediate flush."""
        dt_str = (
            decision_time.isoformat()
            if isinstance(decision_time, datetime.datetime)
            else str(decision_time)
        )
        as_of_str = as_of.isoformat() if isinstance(as_of, datetime.datetime) else str(as_of)
        kt_str = (
            knowledge_time_returned.isoformat()
            if isinstance(knowledge_time_returned, datetime.datetime)
            else (str(knowledge_time_returned) if knowledge_time_returned is not None else None)
        )

        entry: dict[str, Any] = {
            "session_id": self.session_id,
            "decision_time": dt_str,
            "as_of": as_of_str,
            "key": key,
            "knowledge_time_returned": kt_str,
            "query_type": query_type,
        }
        if metadata:
            entry.update(metadata)

        line = json.dumps(entry) + "\n"
        with self._lock:
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(line)
                f.flush()


class LeakReport(BaseModel):
    """Structured summary detailing detected look-ahead leaks from audit logs."""

    total_reads: int = 0
    leaks_detected: int = 0
    as_of_leaks: int = 0
    knowledge_leaks: int = 0
    has_leaks: bool = False
    leak_records: list[dict[str, Any]] = Field(default_factory=list)


def detect_leaks(
    log_path: Path | str,
    decision_time_column: str = "decision_time",
    as_of_column: str = "as_of",
    knowledge_time_column: str = "knowledge_time_returned",
) -> LeakReport:
    """Analyze query audit log and flag reads exhibiting look-ahead bias."""
    path = Path(log_path)
    if not path.exists():
        raise FileNotFoundError(f"Audit log file not found at {path}")

    total_reads = 0
    as_of_leaks = 0
    knowledge_leaks = 0
    leak_records: list[dict[str, Any]] = []

    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line_str = line.strip()
            if not line_str:
                continue
            total_reads += 1
            record = json.loads(line_str)

            dt_raw = record.get(decision_time_column)
            as_of_raw = record.get(as_of_column)
            kt_raw = record.get(knowledge_time_column)

            if dt_raw is None or as_of_raw is None:
                continue

            dt_ts = parse_timestamp_utc(dt_raw)
            as_of_ts = parse_timestamp_utc(as_of_raw)
            kt_ts = parse_timestamp_utc(kt_raw) if kt_raw is not None else None

            is_as_of_leak = as_of_ts > dt_ts
            is_kt_leak = (kt_ts is not None) and (kt_ts > dt_ts)

            if is_as_of_leak or is_kt_leak:
                if is_as_of_leak:
                    as_of_leaks += 1
                if is_kt_leak:
                    knowledge_leaks += 1

                leak_info = {
                    "line": line_no,
                    "key": record.get("key"),
                    "decision_time": dt_ts.isoformat(),
                    "as_of": as_of_ts.isoformat(),
                    "knowledge_time_returned": kt_ts.isoformat() if kt_ts else None,
                    "is_as_of_leak": is_as_of_leak,
                    "is_knowledge_leak": is_kt_leak,
                }
                leak_records.append(leak_info)

    total_leak_count = len(leak_records)
    return LeakReport(
        total_reads=total_reads,
        leaks_detected=total_leak_count,
        as_of_leaks=as_of_leaks,
        knowledge_leaks=knowledge_leaks,
        has_leaks=total_leak_count > 0,
        leak_records=leak_records,
    )


class Store:
    """Bitemporal point-in-time query interface enforcing Guarantee 2."""

    def __init__(
        self,
        store_path: str | Path,
        audit: bool = False,
        audit_log_path: str | Path | None = None,
        default_decision_time: datetime.datetime | str | None = None,
    ) -> None:
        self.store_path = Path(store_path)
        self.audit = audit
        self.default_decision_time = (
            parse_timestamp_utc(default_decision_time)
            if default_decision_time is not None
            else None
        )

        if self.audit:
            log_dest = (
                Path(audit_log_path)
                if audit_log_path is not None
                else self.store_path / "audit" / "query_audit.jsonl"
            )
            self.logger: AuditLogger | None = AuditLogger(log_dest)
        else:
            self.logger = None

        self._con = duckdb.connect(":memory:")

    def close(self) -> None:
        """Close embedded DuckDB connection."""
        self._con.close()

    def __enter__(self) -> Store:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def value(
        self,
        entity_id: str,
        field: str,
        event_time: str | datetime.date | datetime.datetime,
        as_of: str | datetime.date | datetime.datetime,
        decision_time: str | datetime.date | datetime.datetime | None = None,
    ) -> StoreRecord | None:
        """Return row with greatest knowledge_time <= as_of for key, enforcing Guarantee 2."""
        event_ts = parse_timestamp_utc(event_time)
        as_of_ts = parse_timestamp_utc(as_of)

        if decision_time is not None:
            dec_ts = parse_timestamp_utc(decision_time)
        elif self.default_decision_time is not None:
            dec_ts = self.default_decision_time
        else:
            dec_ts = as_of_ts

        norm_id = normalize_identifier(entity_id)
        padded_id = norm_id.zfill(10) if (norm_id.isdigit() and len(norm_id) < 10) else norm_id

        bucket = resolve_field_bucket(field)
        year_str = str(event_ts.year)
        partition_dir = self.store_path / bucket / year_str

        res_record: StoreRecord | None = None

        if partition_dir.exists():
            files = list(partition_dir.glob("part-*.parquet"))
            if files:
                glob_path = str((partition_dir / "part-*.parquet").resolve())
                query = """
                SELECT entity_id, entity_id_type, field, event_time, knowledge_time,
                       value_num, value_str, unit, drop_id, source_file, source_row,
                       row_hash, unmapped
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY entity_id, field, event_time
                               ORDER BY knowledge_time DESC, drop_id DESC
                           ) AS rank_idx
                    FROM read_parquet(?, hive_partitioning=false)
                    WHERE (entity_id = ? OR entity_id = ?)
                      AND field = ?
                      AND event_time = ?
                      AND knowledge_time <= ?
                )
                WHERE rank_idx = 1
                """
                params = [glob_path, norm_id, padded_id, field, event_ts, as_of_ts]
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", category=UserWarning)
                    arr_tbl = self._con.execute(query, params).to_arrow_table()
                if arr_tbl is not None:
                    rows = arr_tbl.to_pylist()
                    if rows:
                        res_record = StoreRecord(rows[0])

        if self.audit and self.logger is not None:
            key_repr = f"{norm_id}|{field}|{event_ts.isoformat()}"
            kt_ret = res_record["knowledge_time"] if res_record is not None else None
            self.logger.log_read(
                decision_time=dec_ts,
                as_of=as_of_ts,
                key=key_repr,
                knowledge_time_returned=kt_ret,
                query_type="value",
            )

        return res_record

    def series(
        self,
        entity_id: str,
        field: str,
        as_of: str | datetime.date | datetime.datetime,
        start: str | datetime.date | datetime.datetime | None = None,
        end: str | datetime.date | datetime.datetime | None = None,
        decision_time: str | datetime.date | datetime.datetime | None = None,
    ) -> list[StoreRecord]:
        """Return point-in-time series ordered chronologically by event_time."""
        as_of_ts = parse_timestamp_utc(as_of)
        start_ts = parse_timestamp_utc(start) if start is not None else None
        end_ts = parse_timestamp_utc(end) if end is not None else None

        if decision_time is not None:
            dec_ts = parse_timestamp_utc(decision_time)
        elif self.default_decision_time is not None:
            dec_ts = self.default_decision_time
        else:
            dec_ts = as_of_ts

        norm_id = normalize_identifier(entity_id)
        padded_id = norm_id.zfill(10) if (norm_id.isdigit() and len(norm_id) < 10) else norm_id

        bucket = resolve_field_bucket(field)
        bucket_dir = self.store_path / bucket

        results: list[StoreRecord] = []

        if bucket_dir.exists():
            files = list(bucket_dir.glob("*/*.parquet"))
            if files:
                glob_path = str((bucket_dir / "*/*.parquet").resolve())
                query = """
                SELECT entity_id, entity_id_type, field, event_time, knowledge_time,
                       value_num, value_str, unit, drop_id, source_file, source_row,
                       row_hash, unmapped
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY entity_id, field, event_time
                               ORDER BY knowledge_time DESC, drop_id DESC
                           ) AS rank_idx
                    FROM read_parquet(?, hive_partitioning=false)
                    WHERE (entity_id = ? OR entity_id = ?)
                      AND field = ?
                      AND knowledge_time <= ?
                      AND (? IS NULL OR event_time >= ?)
                      AND (? IS NULL OR event_time <= ?)
                )
                WHERE rank_idx = 1
                ORDER BY event_time ASC
                """
                params = [
                    glob_path,
                    norm_id,
                    padded_id,
                    field,
                    as_of_ts,
                    start_ts,
                    start_ts,
                    end_ts,
                    end_ts,
                ]
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", category=UserWarning)
                    arr_tbl = self._con.execute(query, params).to_arrow_table()
                if arr_tbl is not None:
                    for r in arr_tbl.to_pylist():
                        results.append(StoreRecord(r))

        if self.audit and self.logger is not None:
            key_repr = f"{norm_id}|{field}|series"
            kt_ret = results[-1]["knowledge_time"] if results else None
            self.logger.log_read(
                decision_time=dec_ts,
                as_of=as_of_ts,
                key=key_repr,
                knowledge_time_returned=kt_ret,
                query_type="series",
            )

        return results


class BruteForceStore:
    """Exhaustive linear reference oracle verifying point-in-time correctness."""

    def __init__(
        self,
        store_path: str | Path,
        records: list[dict[str, Any]] | None = None,
    ) -> None:
        self.store_path = Path(store_path)
        if records is not None:
            self._records: list[dict[str, Any]] = list(records)
        else:
            self._records = self._load_all_records()

    def _load_all_records(self) -> list[dict[str, Any]]:
        if not self.store_path.exists():
            return []

        partition_files = [
            p
            for p in self.store_path.glob("*/*/*.parquet")
            if "quarantine" not in p.parts and "metadata" not in p.parts
        ]
        records: list[dict[str, Any]] = []
        for pf in partition_files:
            tbl = pq.read_table(pf)
            records.extend(tbl.to_pylist())
        return records

    def value(
        self,
        entity_id: str,
        field: str,
        event_time: str | datetime.date | datetime.datetime,
        as_of: str | datetime.date | datetime.datetime,
    ) -> StoreRecord | None:
        """Exhaustively scan records and return row with greatest knowledge_time <= as_of."""
        event_ts = parse_timestamp_utc(event_time)
        as_of_ts = parse_timestamp_utc(as_of)
        norm_id = normalize_identifier(entity_id)
        padded_id = norm_id.zfill(10) if (norm_id.isdigit() and len(norm_id) < 10) else norm_id

        candidates: list[dict[str, Any]] = []
        for r in self._records:
            r_ent = r["entity_id"]
            if r_ent != norm_id and r_ent != padded_id:
                continue
            if r["field"] != field:
                continue

            r_event = (
                r["event_time"]
                if isinstance(r["event_time"], datetime.datetime)
                else parse_timestamp_utc(r["event_time"])
            )
            if r_event != event_ts:
                continue

            r_kt = (
                r["knowledge_time"]
                if isinstance(r["knowledge_time"], datetime.datetime)
                else parse_timestamp_utc(r["knowledge_time"])
            )
            if r_kt <= as_of_ts:
                candidates.append(r)

        if not candidates:
            return None

        candidates.sort(
            key=lambda x: (
                x["knowledge_time"]
                if isinstance(x["knowledge_time"], datetime.datetime)
                else parse_timestamp_utc(x["knowledge_time"]),
                x.get("drop_id", ""),
            ),
            reverse=True,
        )
        return StoreRecord(candidates[0])

    def series(
        self,
        entity_id: str,
        field: str,
        as_of: str | datetime.date | datetime.datetime,
        start: str | datetime.date | datetime.datetime | None = None,
        end: str | datetime.date | datetime.datetime | None = None,
    ) -> list[StoreRecord]:
        """Exhaustively scan records and return chronologically sorted point-in-time series."""
        as_of_ts = parse_timestamp_utc(as_of)
        start_ts = parse_timestamp_utc(start) if start is not None else None
        end_ts = parse_timestamp_utc(end) if end is not None else None
        norm_id = normalize_identifier(entity_id)
        padded_id = norm_id.zfill(10) if (norm_id.isdigit() and len(norm_id) < 10) else norm_id

        candidates: list[dict[str, Any]] = []
        for r in self._records:
            r_ent = r["entity_id"]
            if r_ent != norm_id and r_ent != padded_id:
                continue
            if r["field"] != field:
                continue

            r_kt = (
                r["knowledge_time"]
                if isinstance(r["knowledge_time"], datetime.datetime)
                else parse_timestamp_utc(r["knowledge_time"])
            )
            if r_kt > as_of_ts:
                continue

            r_event = (
                r["event_time"]
                if isinstance(r["event_time"], datetime.datetime)
                else parse_timestamp_utc(r["event_time"])
            )
            if start_ts is not None and r_event < start_ts:
                continue
            if end_ts is not None and r_event > end_ts:
                continue

            candidates.append(r)

        by_event: dict[datetime.datetime, list[dict[str, Any]]] = defaultdict(list)
        for r in candidates:
            r_ev = (
                r["event_time"]
                if isinstance(r["event_time"], datetime.datetime)
                else parse_timestamp_utc(r["event_time"])
            )
            by_event[r_ev].append(r)

        result: list[StoreRecord] = []
        for ev_time in sorted(by_event.keys()):
            group = by_event[ev_time]
            group.sort(
                key=lambda x: (
                    x["knowledge_time"]
                    if isinstance(x["knowledge_time"], datetime.datetime)
                    else parse_timestamp_utc(x["knowledge_time"]),
                    x.get("drop_id", ""),
                ),
                reverse=True,
            )
            result.append(StoreRecord(group[0]))

        return result
