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


_UNIX_EPOCH_UTC = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


def _datetime_to_epoch_us(dt: datetime.datetime) -> int:
    """Convert timezone-aware datetime to integer microseconds since Unix epoch."""
    delta = dt.astimezone(datetime.timezone.utc) - _UNIX_EPOCH_UTC
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


def _compute_k64(entity_id: str, field: str, event_us: int) -> int:
    """Compute 64-bit unsigned MD5 prefix matching DuckDB lookup index."""
    import hashlib

    digest = hashlib.md5(f"{entity_id}\0{field}\0{event_us}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big", signed=False)


class _PartitionIndex:
    """Compact sorted Arrow partition cache supporting O(log N) point-in-time lookups."""

    def __init__(
        self,
        signature: tuple[tuple[str, int], ...],
        table: Any,
        k64_mv: memoryview,
        ev_mv: memoryview,
        kt_mv: memoryview,
        pinned_arrays: tuple[Any, ...],
    ) -> None:
        self.signature = signature
        self.table = table
        self.k64_mv = k64_mv
        self.ev_mv = ev_mv
        self.kt_mv = kt_mv
        self._pinned_arrays = pinned_arrays
        self.nbytes: int = int(table.nbytes) + sum(int(a.nbytes) for a in pinned_arrays)
        self._ent_col = table["entity_id"]
        self._fld_col = table["field"]
        self._drop_col = table["drop_id"]

    def lookup(
        self, entity_id: str, field: str, ev_us: int, as_of_us: int
    ) -> tuple[int, str, int] | None:
        """Return (knowledge_time_us, drop_id, row_idx) for greatest kt <= as_of."""
        import bisect

        target_k64 = _compute_k64(entity_id, field, ev_us)
        lo = bisect.bisect_left(self.k64_mv, target_k64)
        n = len(self.k64_mv)
        idx = lo
        while idx < n and self.k64_mv[idx] == target_k64:
            if self.kt_mv[idx] <= as_of_us and self.ev_mv[idx] == ev_us:
                if self._ent_col[idx].as_py() == entity_id and self._fld_col[idx].as_py() == field:
                    return (
                        int(self.kt_mv[idx]),
                        str(self._drop_col[idx].as_py()),
                        idx,
                    )
            idx += 1
        return None


class Store:
    """Bitemporal point-in-time query interface enforcing Guarantee 2."""

    _MAX_CACHED_PARTITIONS = 256
    _MAX_CACHED_BYTES = 2 * 1024 * 1024 * 1024

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
        self._con.execute("SET TimeZone = 'UTC';")
        self._partition_cache: dict[tuple[str, str], _PartitionIndex] = {}
        self._cached_bytes: int = 0

    def close(self) -> None:
        """Close embedded DuckDB connection and release partition caches."""
        self._partition_cache.clear()
        self._cached_bytes = 0
        self._con.close()

    def __enter__(self) -> Store:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def _get_partition_index(
        self, bucket: str, year_str: str, partition_dir: Path
    ) -> _PartitionIndex | None:
        files = sorted(partition_dir.glob("part-*.parquet"))
        if not files:
            return None
        sig = tuple((f.name, f.stat().st_size) for f in files)
        cache_key = (bucket, year_str)
        cached = self._partition_cache.get(cache_key)
        if cached is not None and cached.signature == sig:
            return cached
        if cached is not None:
            self._partition_cache.pop(cache_key, None)
            self._cached_bytes = max(0, self._cached_bytes - cached.nbytes)

        glob_escaped = str((partition_dir / "part-*.parquet").resolve()).replace("'", "''")
        sql = f"""
        SELECT
            entity_id, entity_id_type, field, event_time, knowledge_time,
            value_num, value_str, unit, drop_id, source_file, source_row,
            row_hash, unmapped,
            (
                '0x' || substring(
                    md5(entity_id || chr(0) || field || chr(0) || epoch_us(event_time)::VARCHAR),
                    1,
                    16
                )
            )::UBIGINT AS _k64
        FROM read_parquet('{glob_escaped}', hive_partitioning=false)
        ORDER BY _k64 ASC, knowledge_time DESC, drop_id DESC
        """
        arr_tbl = self._con.sql(sql).to_arrow_table()
        if len(arr_tbl) == 0:
            return None

        k64_col = arr_tbl["_k64"].combine_chunks()
        ev_col = arr_tbl["event_time"].combine_chunks()
        kt_col = arr_tbl["knowledge_time"].combine_chunks()

        k64_mv = memoryview(k64_col.buffers()[1]).cast("Q")
        ev_mv = memoryview(ev_col.buffers()[1]).cast("q")
        kt_mv = memoryview(kt_col.buffers()[1]).cast("q")
        data_tbl = arr_tbl.drop_columns(["_k64"])
        idx_obj = _PartitionIndex(sig, data_tbl, k64_mv, ev_mv, kt_mv, (k64_col, ev_col, kt_col))

        while self._partition_cache and (
            len(self._partition_cache) >= self._MAX_CACHED_PARTITIONS
            or self._cached_bytes + idx_obj.nbytes > self._MAX_CACHED_BYTES
        ):
            oldest_key = next(iter(self._partition_cache))
            evicted = self._partition_cache.pop(oldest_key)
            self._cached_bytes = max(0, self._cached_bytes - evicted.nbytes)

        self._partition_cache[cache_key] = idx_obj
        self._cached_bytes += idx_obj.nbytes
        return idx_obj

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
            p_idx = self._get_partition_index(bucket, year_str, partition_dir)
            if p_idx is not None:
                ev_us = _datetime_to_epoch_us(event_ts)
                as_of_us = _datetime_to_epoch_us(as_of_ts)
                hit = p_idx.lookup(norm_id, field, ev_us, as_of_us)
                if padded_id != norm_id:
                    hit_pad = p_idx.lookup(padded_id, field, ev_us, as_of_us)
                    if hit_pad is not None and (
                        hit is None or (hit_pad[0], hit_pad[1]) > (hit[0], hit[1])
                    ):
                        hit = hit_pad
                if hit is not None:
                    row_dict = p_idx.table.slice(hit[2], 1).to_pylist()[0]
                    res_record = StoreRecord(row_dict)

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

    @classmethod
    def batch_values_unpruned(
        cls,
        store_path: str | Path,
        queries: list[tuple[str, str, datetime.datetime, datetime.datetime]],
    ) -> list[StoreRecord | None]:
        """Verify batch of (entity_id, field, event_ts, as_of_ts) queries via unpruned scan.

        Reads every data Parquet file across all buckets and event years without directory
        pruning, DuckDB SQL, or MD5 indexing, selecting the record with greatest
        (knowledge_time, drop_id) where knowledge_time <= as_of_ts for each query.
        """
        import pyarrow as pa

        if not queries:
            return []

        base = Path(store_path)
        if not base.exists():
            return [None] * len(queries)

        partition_files = sorted(
            p
            for p in base.glob("*/*/*.parquet")
            if "quarantine" not in p.parts and "metadata" not in p.parts
        )
        if not partition_files:
            return [None] * len(queries)

        # Map each unique (entity_id, field, event_ts) to the query indices referencing it
        key_to_qindices: dict[tuple[str, str, datetime.datetime], list[int]] = defaultdict(list)
        q_as_of: list[datetime.datetime] = []

        for q_idx, (raw_ent, fld, ev_raw, ao_raw) in enumerate(queries):
            ev_ts = ev_raw if ev_raw.tzinfo is not None else parse_timestamp_utc(ev_raw)
            ev_ts = ev_ts.astimezone(datetime.timezone.utc)
            ao_ts = ao_raw if ao_raw.tzinfo is not None else parse_timestamp_utc(ao_raw)
            ao_ts = ao_ts.astimezone(datetime.timezone.utc)
            q_as_of.append(ao_ts)

            norm_id = normalize_identifier(raw_ent)
            padded_id = norm_id.zfill(10) if (norm_id.isdigit() and len(norm_id) < 10) else norm_id
            key_to_qindices[(norm_id, fld, ev_ts)].append(q_idx)
            if padded_id != norm_id:
                key_to_qindices[(padded_id, fld, ev_ts)].append(q_idx)

        unique_keys = list(key_to_qindices.keys())
        probe_tbl = pa.table(
            {
                "entity_id": pa.array([k[0] for k in unique_keys], type=pa.string()),
                "field": pa.array([k[1] for k in unique_keys], type=pa.string()),
                "event_time": pa.array([k[2] for k in unique_keys], type=pa.timestamp("us", "UTC")),
            }
        )

        winners: list[tuple[datetime.datetime, str, dict[str, Any]] | None] = [None] * len(queries)

        # Exhaustively stream every batch of every Parquet file in the store
        for pf_path in partition_files:
            pf = pq.ParquetFile(pf_path)
            for batch in pf.iter_batches(batch_size=262_144):
                batch_tbl = pa.Table.from_batches([batch])
                matched = batch_tbl.join(
                    probe_tbl, keys=["entity_id", "field", "event_time"], join_type="inner"
                )
                if len(matched) == 0:
                    continue
                for row in matched.to_pylist():
                    r_key = (row["entity_id"], row["field"], row["event_time"])
                    r_kt: datetime.datetime = row["knowledge_time"]
                    r_drop = str(row.get("drop_id", ""))
                    for q_idx in key_to_qindices.get(r_key, ()):
                        if r_kt <= q_as_of[q_idx]:
                            cur = winners[q_idx]
                            if cur is None or (r_kt, r_drop) > (cur[0], cur[1]):
                                winners[q_idx] = (r_kt, r_drop, row)

        return [StoreRecord(w[2]) if w is not None else None for w in winners]
