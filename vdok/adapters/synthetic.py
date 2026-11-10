"""Synthetic vendor drop sequence generator with planted data anomalies."""

from __future__ import annotations

import json
import math
import shutil
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import yaml

from vdok.normalize import CANONICAL_ARROW_SCHEMA

DEFAULT_ENTITIES = [
    "AAPL",
    "MSFT",
    "GOOGL",
    "AMZN",
    "NVDA",
    "META",
    "TSLA",
    "BRK.B",
    "JPM",
    "V",
]

CIK_MAPPING = {
    "AAPL": "0000320193",
    "MSFT": "0000789019",
    "GOOGL": "0001652044",
    "AMZN": "0001018724",
    "NVDA": "0001045810",
    "META": "0001326801",
    "TSLA": "0001318605",
    "BRK.B": "0001067983",
    "JPM": "0000019617",
    "V": "0001403161",
}


def parse_size_bytes(size: str | float | int) -> int:
    """Parse size specification string or number into bytes."""
    if isinstance(size, (int, float)):
        # If float < 10, assume gigabytes unless large
        val = float(size)
        if val < 1000:
            return int(val * 1024 * 1024 * 1024)
        return int(val)

    s = str(size).strip().lower()
    if s.endswith("gb") or s.endswith("g"):
        return int(float(s.rstrip("gb").strip()) * 1024 * 1024 * 1024)
    elif s.endswith("mb") or s.endswith("m"):
        return int(float(s.rstrip("mb").strip()) * 1024 * 1024)
    elif s.endswith("kb") or s.endswith("k"):
        return int(float(s.rstrip("kb").strip()) * 1024)
    elif s.endswith("b"):
        return int(float(s.rstrip("b").strip()))
    else:
        return int(float(s) * 1024 * 1024 * 1024)


class SyntheticGenerator:
    """Procedural generator creating reproducible multi-drop synthetic test feeds."""

    def __init__(
        self,
        seed: int = 1,
        size_bytes: int = 52_428_800,  # 50 MB default
        faults: Any = "default",
        format: str = "csv",
    ) -> None:
        self.seed = seed
        self.size_bytes = size_bytes
        self.faults_config = faults
        self.format = format.lower()

    def _resolve_fault_flags(self, drop_idx: int) -> dict[str, Any]:
        """Determine active fault injections for a given drop index (1-based)."""
        flags: dict[str, Any] = {
            "duplicate_row": False,
            "duplicate_share": 0.005,
            "type_flip": False,
            "type_flip_count": 10,
            "out_of_range": False,
            "out_of_range_count": 10,
            "repeat_drop": False,
            "schema_drift": False,
        }

        cfg = self.faults_config

        if cfg == "default":
            if drop_idx == 2:
                flags["duplicate_row"] = True
                flags["duplicate_share"] = 0.005  # sub-threshold
                flags["type_flip"] = True
                flags["type_flip_count"] = 10
                flags["out_of_range"] = True
                flags["out_of_range_count"] = 10
            elif drop_idx == 3:
                flags["schema_drift"] = True

        elif isinstance(cfg, list):
            for item in cfg:
                name = str(item).lower()
                if name in ("duplicate_row", "duplicate_row_sub") and drop_idx == 2:
                    flags["duplicate_row"] = True
                    flags["duplicate_share"] = 0.005
                elif name in ("duplicate_row_super", "super_duplicate") and drop_idx == 2:
                    flags["duplicate_row"] = True
                    flags["duplicate_share"] = 0.02
                elif name == "type_flip" and drop_idx == 2:
                    flags["type_flip"] = True
                    flags["type_flip_count"] = 10
                elif name == "out_of_range" and drop_idx == 2:
                    flags["out_of_range"] = True
                    flags["out_of_range_count"] = 10
                elif name == "repeat_drop" and drop_idx == 2:
                    flags["repeat_drop"] = True
                elif name == "schema_drift" and drop_idx == 3:
                    flags["schema_drift"] = True

        elif isinstance(cfg, dict):
            # Detailed custom configuration
            if "duplicate_row" in cfg and drop_idx == 2:
                val = cfg["duplicate_row"]
                flags["duplicate_row"] = True
                if isinstance(val, dict) and "share" in val:
                    flags["duplicate_share"] = float(val["share"])
                elif isinstance(val, (int, float)):
                    flags["duplicate_share"] = float(val)
                elif val == "super_threshold":
                    flags["duplicate_share"] = 0.02
                else:
                    flags["duplicate_share"] = 0.005

            if "type_flip" in cfg and drop_idx == 2:
                flags["type_flip"] = True
                val = cfg["type_flip"]
                if isinstance(val, dict) and "count" in val:
                    flags["type_flip_count"] = int(val["count"])
                elif isinstance(val, int):
                    flags["type_flip_count"] = val

            if "out_of_range" in cfg and drop_idx == 2:
                flags["out_of_range"] = True
                val = cfg["out_of_range"]
                if isinstance(val, dict) and "count" in val:
                    flags["out_of_range_count"] = int(val["count"])
                elif isinstance(val, int):
                    flags["out_of_range_count"] = val

            if "repeat_drop" in cfg and drop_idx == 2:
                flags["repeat_drop"] = bool(cfg["repeat_drop"])

            if "schema_drift" in cfg and drop_idx == 3:
                flags["schema_drift"] = bool(cfg["schema_drift"])

        return flags

    def generate_canonical_table(
        self,
        rows: int = 1000,
        faults: list[str] | dict[str, Any] | None = None,
        drop_id: str = "drop_1",
    ) -> tuple[pa.Table, dict[str, Any]]:
        """Generate a canonical Arrow table in memory with selected planted faults."""
        con = duckdb.connect()
        try:
            norm_seed = ((self.seed * 10007) % 100000) / 100000.0
            con.sql(f"SELECT setseed({norm_seed});")

            # Determine faults
            f_cfg = faults if faults is not None else self.faults_config
            has_dup = False
            dup_share = 0.005
            has_type_flip = False
            type_flip_cnt = 10
            has_out_of_range = False
            out_of_range_cnt = 10

            if f_cfg:
                if isinstance(f_cfg, list):
                    for item in f_cfg:
                        s = str(item).lower()
                        if s in ("duplicate_row", "duplicate_row_sub"):
                            has_dup = True
                            dup_share = 0.005
                        elif s in ("duplicate_row_super", "super_duplicate"):
                            has_dup = True
                            dup_share = 0.02
                        elif s == "type_flip":
                            has_type_flip = True
                        elif s == "out_of_range":
                            has_out_of_range = True
                elif isinstance(f_cfg, dict):
                    if "duplicate_row" in f_cfg:
                        has_dup = True
                        val = f_cfg["duplicate_row"]
                        if isinstance(val, dict) and "share" in val:
                            dup_share = float(val["share"])
                        elif isinstance(val, (int, float)):
                            dup_share = float(val)
                        elif val == "super_threshold":
                            dup_share = 0.02
                    if "type_flip" in f_cfg:
                        has_type_flip = True
                        val = f_cfg["type_flip"]
                        if isinstance(val, int):
                            type_flip_cnt = val
                    if "out_of_range" in f_cfg:
                        has_out_of_range = True
                        val = f_cfg["out_of_range"]
                        if isinstance(val, int):
                            out_of_range_cnt = val

            # Construct SQL expressions for planted faults
            price_cases: list[str] = []
            val_str_cases: list[str] = []

            cursor = 0
            if has_type_flip:
                end_tf = cursor + type_flip_cnt
                price_cases.append(f"WHEN i >= {cursor} AND i < {end_tf} THEN NULL::DECIMAL(38, 9)")
                val_str_cases.append(f"WHEN i >= {cursor} AND i < {end_tf} THEN 'BAD_STRING'")
                cursor = end_tf

            if has_out_of_range:
                end_oor = cursor + out_of_range_cnt
                price_cases.append(
                    f"WHEN i >= {cursor} AND i < {end_oor} THEN -50.00::DECIMAL(38, 9)"
                )
                cursor = end_oor

            seed_offset = (self.seed * 37) % 1000
            default_price = (
                f"ROUND(100.0 + ((i + {seed_offset}) % 500) + (i % 100) * 0.01, 4)::DECIMAL(38, 9)"
            )
            if price_cases:
                price_expr = f"CASE {' '.join(price_cases)} ELSE {default_price} END"
            else:
                price_expr = default_price

            if val_str_cases:
                val_str_expr = f"CASE {' '.join(val_str_cases)} ELSE NULL::VARCHAR END"
            else:
                val_str_expr = "NULL::VARCHAR"

            base_query = f"""
            SELECT
                CASE (i % 10)
                    WHEN 0 THEN '0000320193'
                    WHEN 1 THEN '0000789019'
                    WHEN 2 THEN '0001652044'
                    WHEN 3 THEN '0001018724'
                    WHEN 4 THEN '0001045810'
                    WHEN 5 THEN '0001326801'
                    WHEN 6 THEN '0001318605'
                    WHEN 7 THEN '0001067983'
                    WHEN 8 THEN '0000019617'
                    ELSE '0001403161'
                END AS entity_id,
                'cik' AS entity_id_type,
                'price' AS field,
                ('2024-01-01 00:00:00'::TIMESTAMPTZ + INTERVAL (i // 10) SECOND) AS event_time,
                ('2024-01-01 02:00:00'::TIMESTAMPTZ + INTERVAL (i // 10) SECOND) AS knowledge_time,
                {price_expr} AS value_num,
                {val_str_expr} AS value_str,
                'USD' AS unit,
                '{drop_id}' AS drop_id,
                'data.csv' AS source_file,
                i::BIGINT AS source_row,
                'hash_' || i::VARCHAR AS row_hash,
                false AS unmapped
            FROM range({rows}) t(i)
            """

            dup_count = 0
            if has_dup:
                dup_count = max(1, math.ceil(rows * dup_share))
                # Append duplicate rows matching the initial primary keys
                full_query = f"""
                ({base_query})
                UNION ALL
                (
                    SELECT
                        entity_id, entity_id_type, field, event_time, knowledge_time,
                        value_num, value_str, unit, drop_id, source_file,
                        ({rows} + row_number() OVER ())::BIGINT AS source_row,
                        'dup_hash_' || row_number() OVER () AS row_hash,
                        unmapped
                    FROM ({base_query} LIMIT {dup_count}) b
                )
                """
            else:
                full_query = base_query

            arrow_table = con.sql(full_query).to_arrow_table()
            casted = arrow_table.cast(CANONICAL_ARROW_SCHEMA)

            info = {
                "rows": len(casted),
                "has_duplicate": has_dup,
                "duplicate_count": dup_count,
                "duplicate_share": (dup_count / len(casted)) if len(casted) > 0 else 0.0,
                "has_type_flip": has_type_flip,
                "type_flip_count": type_flip_cnt if has_type_flip else 0,
                "has_out_of_range": has_out_of_range,
                "out_of_range_count": out_of_range_cnt if has_out_of_range else 0,
            }
            return casted, info

        finally:
            con.close()

    def generate_drop_data(
        self,
        drop_dir: Path | str,
        rows: int,
        flags: dict[str, Any],
        predecessor_dir: Path | None = None,
    ) -> dict[str, Any]:
        """Generate raw drop files on disk and return ground truth metadata."""
        target_dir = Path(drop_dir)
        target_dir.mkdir(parents=True, exist_ok=True)

        # Handle repeat_drop (byte-identical copy of predecessor)
        if flags.get("repeat_drop") and predecessor_dir and predecessor_dir.exists():
            for src_file in predecessor_dir.iterdir():
                if src_file.is_file() and not src_file.name.endswith(".json"):
                    shutil.copy2(src_file, target_dir / src_file.name)
            return {
                "rows": rows,
                "faults": [
                    {
                        "type": "repeat_drop",
                        "identical_to": predecessor_dir.name,
                        "expected_severity": "hold",
                    }
                ],
            }

        con = duckdb.connect()
        try:
            norm_seed = ((self.seed * 10007) % 100000) / 100000.0
            con.sql(f"SELECT setseed({norm_seed});")

            has_dup = flags.get("duplicate_row", False)
            dup_share = flags.get("duplicate_share", 0.005)
            has_tf = flags.get("type_flip", False)
            tf_count = flags.get("type_flip_count", 10)
            has_oor = flags.get("out_of_range", False)
            oor_count = flags.get("out_of_range_count", 10)
            has_drift = flags.get("schema_drift", False)

            # Planted faults in price column
            price_cases: list[str] = []
            cursor = 0
            if has_tf:
                end_tf = cursor + tf_count
                price_cases.append(f"WHEN i >= {cursor} AND i < {end_tf} THEN 'CORRUPT_STR'")
                cursor = end_tf

            if has_oor:
                end_oor = cursor + oor_count
                price_cases.append(f"WHEN i >= {cursor} AND i < {end_oor} THEN '-50.0000'")
                cursor = end_oor

            seed_offset = (self.seed * 37) % 1000
            default_price = (
                f"printf('%.4f', 100.0 + ((i + {seed_offset}) % 500) + (i % 100) * 0.01)"
            )
            if price_cases:
                price_expr = f"CASE {' '.join(price_cases)} ELSE {default_price} END"
            else:
                price_expr = default_price

            # Select column list: omit 'weight' if schema_drift active
            if has_drift:
                col_selection = f"""
                1000 + i AS record_id,
                CASE (i % 10)
                    WHEN 0 THEN 'AAPL'
                    WHEN 1 THEN 'MSFT'
                    WHEN 2 THEN 'GOOGL'
                    WHEN 3 THEN 'AMZN'
                    WHEN 4 THEN 'NVDA'
                    WHEN 5 THEN 'META'
                    WHEN 6 THEN 'TSLA'
                    WHEN 7 THEN 'BRK.B'
                    WHEN 8 THEN 'JPM'
                    ELSE 'V'
                END AS entity_id,
                {price_expr} AS price,
                CASE (i % 5)
                    WHEN 0 THEN 'ACTIVE'
                    WHEN 1 THEN 'ACTIVE'
                    WHEN 2 THEN 'ACTIVE'
                    WHEN 3 THEN 'PENDING'
                    ELSE 'HALTED'
                END AS status,
                strftime('2024-01-01'::DATE + INTERVAL ((i // 10) // 86400) DAY, '%Y-%m-%d')
                    AS trade_date,
                strftime(
                    '2024-01-01 00:00:00'::TIMESTAMPTZ + INTERVAL (i // 10) SECOND,
                    '%Y-%m-%dT%H:%M:%SZ'
                ) AS event_time,
                (i % 2 = 0) AS is_primary,
                'Regular observation' AS notes
                """
            else:
                col_selection = f"""
                1000 + i AS record_id,
                CASE (i % 10)
                    WHEN 0 THEN 'AAPL'
                    WHEN 1 THEN 'MSFT'
                    WHEN 2 THEN 'GOOGL'
                    WHEN 3 THEN 'AMZN'
                    WHEN 4 THEN 'NVDA'
                    WHEN 5 THEN 'META'
                    WHEN 6 THEN 'TSLA'
                    WHEN 7 THEN 'BRK.B'
                    WHEN 8 THEN 'JPM'
                    ELSE 'V'
                END AS entity_id,
                {price_expr} AS price,
                printf('%.4f', 0.01 + (i % 100) * 0.001) AS weight,
                CASE (i % 5)
                    WHEN 0 THEN 'ACTIVE'
                    WHEN 1 THEN 'ACTIVE'
                    WHEN 2 THEN 'ACTIVE'
                    WHEN 3 THEN 'PENDING'
                    ELSE 'HALTED'
                END AS status,
                strftime('2024-01-01'::DATE + INTERVAL ((i // 10) // 86400) DAY, '%Y-%m-%d')
                    AS trade_date,
                strftime(
                    '2024-01-01 00:00:00'::TIMESTAMPTZ + INTERVAL (i // 10) SECOND,
                    '%Y-%m-%dT%H:%M:%SZ'
                ) AS event_time,
                (i % 2 = 0) AS is_primary,
                'Regular observation' AS notes
                """

            base_sql = f"SELECT {col_selection} FROM range({rows}) t(i)"

            dup_count = 0
            if has_dup:
                dup_count = max(1, math.ceil(rows * dup_share))
                full_sql = f"""
                ({base_sql})
                UNION ALL
                ({base_sql} LIMIT {dup_count})
                """
            else:
                full_sql = base_sql

            # Output destination file
            ext = self.format
            dest_file = target_dir / f"data.{ext}"

            if ext == "csv":
                con.sql(f"COPY ({full_sql}) TO '{dest_file}' (HEADER, DELIMITER ',');")
            elif ext == "parquet":
                con.sql(f"COPY ({full_sql}) TO '{dest_file}' (FORMAT PARQUET);")
            elif ext == "jsonl":
                con.sql(f"COPY ({full_sql}) TO '{dest_file}' (FORMAT JSON);")
            else:
                con.sql(f"COPY ({full_sql}) TO '{dest_file}' (HEADER, DELIMITER ',');")

            # Record faults in ground truth
            planted_faults: list[dict[str, Any]] = []
            total_drop_rows = rows + dup_count

            if has_dup:
                exp_sev = "hold" if (dup_count / total_drop_rows > 0.01) else "quarantine"
                planted_faults.append(
                    {
                        "type": "duplicate_row",
                        "count": dup_count,
                        "share": dup_count / total_drop_rows,
                        "expected_severity": exp_sev,
                    }
                )

            if has_tf:
                planted_faults.append(
                    {
                        "type": "type_flip",
                        "column": "price",
                        "count": tf_count,
                        "expected_severity": "quarantine",
                    }
                )

            if has_oor:
                planted_faults.append(
                    {
                        "type": "out_of_range",
                        "column": "price",
                        "count": oor_count,
                        "min": 0.0,
                        "expected_severity": "quarantine",
                    }
                )

            if has_drift:
                planted_faults.append(
                    {
                        "type": "schema_drift",
                        "removed_column": "weight",
                        "expected_severity": "hold",
                    }
                )

            return {
                "rows": total_drop_rows,
                "faults": planted_faults,
            }

        finally:
            con.close()

    def generate_sequence(self, output_dir: Path | str, num_drops: int = 3) -> Path:
        """Generate multi-drop sequence with planted faults and ground_truth.json."""
        out_path = Path(output_dir)
        out_path.mkdir(parents=True, exist_ok=True)

        # Average row size across formats is roughly 85 bytes
        total_rows = max(300, self.size_bytes // 85)
        rows_per_drop = total_rows // num_drops

        ground_truth: dict[str, Any] = {
            "vendor": "synthetic",
            "seed": self.seed,
            "target_bytes": self.size_bytes,
            "format": self.format,
            "num_drops": num_drops,
            "drops": {},
        }

        prev_drop_dir: Path | None = None
        for idx in range(1, num_drops + 1):
            drop_name = f"drop_{idx}"
            drop_dir = out_path / drop_name
            flags = self._resolve_fault_flags(idx)
            info = self.generate_drop_data(
                drop_dir=drop_dir,
                rows=rows_per_drop,
                flags=flags,
                predecessor_dir=prev_drop_dir,
            )
            ground_truth["drops"][drop_name] = info
            prev_drop_dir = drop_dir

        # Write ground_truth.json
        gt_file = out_path / "ground_truth.json"
        gt_file.write_text(json.dumps(ground_truth, indent=2), encoding="utf-8")

        # Write synthetic manifest in output_dir
        manifest_data = {
            "vendor": "synthetic",
            "entity_columns": ["entity_id"],
            "entity_id_type": "ticker",
            "event_time_column": "event_time",
            "event_time_format": "%Y-%m-%dT%H:%M:%SZ",
            "knowledge_time": {"source": "drop_time"},
            "timezone": "UTC",
            "value_columns": ["price"],
            "unit_column": None,
            "field_columns": [],
            "expected_calendar": "daily",
            "rules": "rules/synthetic.yaml",
        }
        manifest_file = out_path / "manifest.yaml"
        manifest_file.write_text(yaml.dump(manifest_data, sort_keys=False), encoding="utf-8")

        return out_path


def generate_synthetic_sequence(
    output_dir: Path | str,
    size_gb: float | str = 0.05,
    seed: int = 1,
    faults: Any = "default",
    num_drops: int = 3,
    format: str = "csv",
) -> Path:
    """Generate synthetic vendor drops with reproducible planted faults."""
    size_bytes = parse_size_bytes(size_gb)
    generator = SyntheticGenerator(seed=seed, size_bytes=size_bytes, faults=faults, format=format)
    return generator.generate_sequence(output_dir=output_dir, num_drops=num_drops)
