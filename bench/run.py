"""Benchmark runner measuring onboarding throughput, memory, revisions, PIT, and leaks."""

from __future__ import annotations

import argparse
import csv
import datetime
import gc
import json
import os
import random
import resource
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from bench.hardware import format_hardware_block  # noqa: E402
from vdok.adapters.synthetic import (  # noqa: E402
    CIK_MAPPING,
    DEFAULT_ENTITIES,
    generate_synthetic_sequence,
)
from vdok.onboard import onboard_drop  # noqa: E402
from vdok.query import BruteForceStore, Store, detect_leaks  # noqa: E402

STAGE_NAMES = (
    "discovery",
    "schema",
    "normalization",
    "validation",
    "store_write",
    "revisions",
    "reporting",
)

CSV_COLUMNS = [
    "workload",
    "workload_label",
    "runs",
    "seed",
    "cpu_cores",
    "total_time_s_median",
    "total_time_s_min",
    "total_time_s_max",
    "stage_discovery_s_median",
    "stage_discovery_s_min",
    "stage_discovery_s_max",
    "stage_schema_s_median",
    "stage_schema_s_min",
    "stage_schema_s_max",
    "stage_normalization_s_median",
    "stage_normalization_s_min",
    "stage_normalization_s_max",
    "stage_validation_s_median",
    "stage_validation_s_min",
    "stage_validation_s_max",
    "stage_store_write_s_median",
    "stage_store_write_s_min",
    "stage_store_write_s_max",
    "stage_revisions_s_median",
    "stage_revisions_s_min",
    "stage_revisions_s_max",
    "stage_reporting_s_median",
    "stage_reporting_s_min",
    "stage_reporting_s_max",
    "val_gb_per_s_per_core_median",
    "val_gb_per_s_per_core_min",
    "val_gb_per_s_per_core_max",
    "val_rows_per_s_median",
    "val_rows_per_s_min",
    "val_rows_per_s_max",
    "write_rows_per_s_median",
    "write_rows_per_s_min",
    "write_rows_per_s_max",
    "write_mb_per_s_median",
    "write_mb_per_s_min",
    "write_mb_per_s_max",
    "peak_rss_mb_median",
    "peak_rss_mb_min",
    "peak_rss_mb_max",
    "revision_mode",
    "revision_recall_median",
    "revision_recall_min",
    "revision_recall_max",
    "revision_precision_median",
    "revision_precision_min",
    "revision_precision_max",
    "revisions_detected_median",
    "revisions_detected_min",
    "revisions_detected_max",
    "pit_queries",
    "pit_violations_median",
    "pit_violations_min",
    "pit_violations_max",
    "leak_queries_planted",
    "leak_recall_median",
    "leak_recall_min",
    "leak_recall_max",
    "leak_false_positives_max",
    "storage_factor_median",
    "storage_factor_min",
    "storage_factor_max",
    "storage_factor_ingested_median",
]

STANDARD_COLUMN_ORDER = [
    ("synthetic_50mb", "Synthetic (50 MB)"),
    ("synthetic_1gb", "Synthetic (1 GB)"),
    ("synthetic_10gb", "Synthetic (10 GB)"),
    ("synthetic_50gb", "Synthetic (50 GB)"),
    ("sec_fsds", "SEC FSDS"),
]


def purge_tree(path: Path) -> None:
    """Remove directory tree including read-only 0o444 Parquet partition files."""
    if not path.exists():
        return
    for root, dirs, files in os.walk(path):
        root_p = Path(root)
        try:
            os.chmod(root_p, 0o700)
        except OSError:
            pass
        for d in dirs:
            try:
                os.chmod(root_p / d, 0o700)
            except OSError:
                pass
        for f in files:
            try:
                os.chmod(root_p / f, 0o600)
            except OSError:
                pass
    shutil.rmtree(path, ignore_errors=True)


def parse_workload_label(size_str: str) -> tuple[str, str]:
    """Normalize size argument into (workload_id, human_label)."""
    raw = size_str.strip().upper()
    if raw.endswith("GB"):
        num = raw[:-2].strip()
        return f"synthetic_{num.lower()}gb", f"Synthetic ({num} GB)"
    if raw.endswith("MB"):
        num = raw[:-2].strip()
        return f"synthetic_{num.lower()}mb", f"Synthetic ({num} MB)"
    if raw.endswith("KB"):
        num = raw[:-2].strip()
        return f"synthetic_{num.lower()}kb", f"Synthetic ({num} KB)"
    return f"synthetic_{raw.lower()}gb", f"Synthetic ({raw} GB)"


def _get_process_tree_rss_mb() -> float:
    """Return max RSS in MiB across current process and any waited child processes."""
    ru_self = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    ru_child = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    # On Linux ru_maxrss is reported in KiB; on macOS it is bytes.
    divisor = 1024.0 * 1024.0 if sys.platform == "darwin" else 1024.0
    return (ru_self + ru_child) / divisor


def _resolve_symbol_master_path() -> Path | None:
    """Locate symbol master JSON or Parquet file for benchmark runs."""
    candidates = [
        REPO_ROOT / "data" / "symbols" / "company_tickers.json",
        REPO_ROOT / "tests" / "fixtures" / "symbols" / "company_tickers.json",
    ]
    for cand in candidates:
        if cand.exists():
            return cand
    return None


def _sample_store_keys(
    vendor_store_dir: Path,
    max_per_file: int = 250,
    max_total: int = 6000,
) -> list[tuple[str, str, datetime.datetime, datetime.datetime]]:
    """Sample valid (entity_id, field, event_time, knowledge_time) keys across partitions."""
    import pyarrow.parquet as pq

    part_files = sorted(
        p
        for p in vendor_store_dir.glob("*/*/*.parquet")
        if "quarantine" not in p.parts and "metadata" not in p.parts
    )
    sampled: list[tuple[str, str, datetime.datetime, datetime.datetime]] = []
    cols = ["entity_id", "field", "event_time", "knowledge_time"]

    for pf_path in part_files:
        pf = pq.ParquetFile(pf_path)
        for batch in pf.iter_batches(batch_size=max_per_file, columns=cols):
            rows = batch.to_pylist()
            for r in rows:
                sampled.append(
                    (
                        str(r["entity_id"]),
                        str(r["field"]),
                        r["event_time"],
                        r["knowledge_time"],
                    )
                )
            break
        if len(sampled) >= max_total:
            break

    return sampled[:max_total]


def _load_revision_rows(vendor_store_dir: Path) -> list[dict[str, Any]]:
    """Load revision ledger rows from metadata/revisions.parquet if present."""
    import pyarrow.parquet as pq

    rev_file = vendor_store_dir / "metadata" / "revisions.parquet"
    if not rev_file.exists():
        return []
    return pq.read_table(rev_file).to_pylist()


def _verify_pit_queries(
    vendor_store_dir: Path,
    sampled_keys: list[tuple[str, str, datetime.datetime, datetime.datetime]],
    rev_rows: list[dict[str, Any]],
    pit_queries: int,
    rng: random.Random,
) -> int:
    """Execute random Store.value() queries and compare against unpruned BruteForceStore."""
    if pit_queries <= 0 or not sampled_keys:
        return 0

    queries: list[tuple[str, str, datetime.datetime, datetime.datetime]] = []
    rev_pool = rev_rows[:2000] if rev_rows else []

    for idx in range(pit_queries):
        roll = rng.random()
        if rev_pool and roll < 0.25:
            rev = rev_pool[idx % len(rev_pool)]
            ent = str(rev["entity_id"])
            fld = str(rev["field"])
            ev_ts: datetime.datetime = rev["event_time"]
            old_kt: datetime.datetime = rev["old_knowledge_time"]
            new_kt: datetime.datetime = rev["new_knowledge_time"]
            mode = idx % 3
            if mode == 0:
                ao_ts = old_kt - datetime.timedelta(days=1)
            elif mode == 1:
                delta_us = max(1, int((new_kt - old_kt).total_seconds() * 500_000))
                ao_ts = old_kt + datetime.timedelta(microseconds=delta_us)
            else:
                ao_ts = new_kt + datetime.timedelta(days=1)
            queries.append((ent, fld, ev_ts, ao_ts))
        elif roll < 0.95:
            ent, fld, ev_ts, kt_ts = sampled_keys[rng.randrange(len(sampled_keys))]
            offset_days = rng.choice((-2, 0, 1, 5))
            ao_ts = kt_ts + datetime.timedelta(days=offset_days)
            queries.append((ent, fld, ev_ts, ao_ts))
        else:
            _, fld, ev_ts, kt_ts = sampled_keys[rng.randrange(len(sampled_keys))]
            ao_ts = kt_ts + datetime.timedelta(days=1)
            queries.append(("UNREGISTERED_ENTITY_999999", fld, ev_ts, ao_ts))

    got_results: list[Any] = []
    with Store(vendor_store_dir, audit=False) as store:
        for ent, fld, ev_ts, ao_ts in queries:
            got_results.append(store.value(ent, fld, ev_ts, ao_ts))

    expected_results = BruteForceStore.batch_values_unpruned(vendor_store_dir, queries)

    violations = 0
    for (_, _, _, ao_ts), got, exp in zip(queries, got_results, expected_results, strict=True):
        if got is None and exp is None:
            continue
        if (got is None) != (exp is None):
            violations += 1
            continue
        assert got is not None and exp is not None
        if got["knowledge_time"] > ao_ts:
            violations += 1
            continue
        if (
            got["value_num"] != exp["value_num"]
            or got["value_str"] != exp["value_str"]
            or got["knowledge_time"] != exp["knowledge_time"]
            or got["drop_id"] != exp["drop_id"]
        ):
            violations += 1

    return violations


def _verify_leak_detector(
    vendor_store_dir: Path,
    sampled_keys: list[tuple[str, str, datetime.datetime, datetime.datetime]],
    leak_queries: int,
    audit_log_path: Path,
    rng: random.Random,
) -> tuple[float, int]:
    """Execute compliant and planted-leaking audited reads and measure detector recall."""
    if leak_queries <= 0 or not sampled_keys:
        return 1.0, 0

    if audit_log_path.exists():
        audit_log_path.unlink()

    # Build balanced schedule: leak_queries compliant + leak_queries planted leaks
    schedule = [False] * leak_queries + [True] * leak_queries
    rng.shuffle(schedule)

    planted_lines: set[int] = set()
    with Store(vendor_store_dir, audit=True, audit_log_path=audit_log_path) as store:
        for line_no, is_leak in enumerate(schedule, start=1):
            ent, fld, ev_ts, kt_ts = sampled_keys[(line_no - 1) % len(sampled_keys)]
            if is_leak:
                planted_lines.add(line_no)
                if line_no % 2 == 0:
                    # Both as_of > decision_time and returned knowledge_time > decision_time
                    dec_ts = kt_ts - datetime.timedelta(days=5)
                    ao_ts = kt_ts + datetime.timedelta(days=1)
                else:
                    # Forward-looking as_of > decision_time
                    dec_ts = kt_ts + datetime.timedelta(days=1)
                    ao_ts = kt_ts + datetime.timedelta(days=5)
            else:
                # Strictly causal read: knowledge_time <= as_of <= decision_time
                ao_ts = kt_ts
                dec_ts = kt_ts + datetime.timedelta(days=1)

            store.value(ent, fld, ev_ts, ao_ts, decision_time=dec_ts)

    leak_rep = detect_leaks(audit_log_path)
    detected_lines = {int(rec["line"]) for rec in leak_rep.leak_records}

    tp = len(planted_lines & detected_lines)
    fp = len(detected_lines - planted_lines)
    recall = tp / len(planted_lines) if planted_lines else 1.0
    return recall, fp


def _execute_single_worker(config: dict[str, Any]) -> dict[str, Any]:
    """Run one complete isolated benchmark iteration inside a dedicated process."""
    mode = str(config["mode"])
    run_idx = int(config["run_idx"])
    seed = int(config["seed"])
    pit_queries = int(config["pit_queries"])
    leak_queries = int(config["leak_queries"])
    work_dir = Path(config["work_dir"])

    run_store_dir = work_dir / "store"
    run_schema_dir = work_dir / "schemas"
    run_results_dir = work_dir / "results"
    run_store_dir.mkdir(parents=True, exist_ok=True)
    run_schema_dir.mkdir(parents=True, exist_ok=True)
    run_results_dir.mkdir(parents=True, exist_ok=True)

    symbol_master_path = _resolve_symbol_master_path()
    cpu_cores = os.cpu_count() or 1

    stage_totals: dict[str, float] = {k: 0.0 for k in STAGE_NAMES}
    val_seconds = 0.0
    val_raw_bytes = 0
    val_rows = 0
    write_seconds = 0.0
    written_rows = 0
    raw_total_bytes = 0
    raw_ingested_bytes = 0

    if mode == "synthetic":
        vendor = "synthetic"
        raw_seq_dir = Path(config["raw_seq_dir"])
        manifest_path = raw_seq_dir / "manifest.yaml"
        rules_path = REPO_ROOT / "rules" / "synthetic.yaml"
        gt_data = json.loads((raw_seq_dir / "ground_truth.json").read_text(encoding="utf-8"))
        drop_names = sorted(gt_data["drops"].keys())
        drops_spec: list[tuple[Path, datetime.datetime | None]] = [
            (
                raw_seq_dir / d_name,
                datetime.datetime(2026, 1, idx, 0, 0, 0, tzinfo=datetime.timezone.utc),
            )
            for idx, d_name in enumerate(drop_names, start=1)
        ]
    elif mode == "sec_fsds":
        vendor = "sec_fsds"
        manifest_path = REPO_ROOT / "manifests" / "sec_fsds.yaml"
        rules_path = REPO_ROOT / "rules" / "sec_fsds.yaml"
        pinned_sec_schema = REPO_ROOT / "schemas" / "sec_fsds.yaml"
        if pinned_sec_schema.exists():
            shutil.copy2(pinned_sec_schema, run_schema_dir / "sec_fsds.yaml")
        gt_data = None
        quarter_dirs = [Path(p) for p in config["quarter_dirs"]]
        drops_spec = [(q_dir, None) for q_dir in quarter_dirs]
    else:
        raise ValueError(f"Unsupported benchmark mode: {mode}")

    t_wall_start = time.perf_counter()
    for drop_path, as_of_dt in drops_spec:
        rep, _ = onboard_drop(
            vendor=vendor,
            drop_path=drop_path,
            manifest_path=manifest_path,
            schema_dir=run_schema_dir,
            rules_path=rules_path,
            symbol_master_path=symbol_master_path,
            store_dir=run_store_dir,
            results_dir=run_results_dir,
            as_of=as_of_dt,
            force=True,
        )
        stage_map = rep.timings.timings_per_stage_seconds
        for stg in STAGE_NAMES:
            stage_totals[stg] += float(stage_map.get(stg, 0.0))

        drop_bytes = int(rep.drop_summary.total_bytes)
        raw_total_bytes += drop_bytes

        schema_held = any(d.severity.lower() == "hold" for d in rep.schema_report.differences)
        if not schema_held:
            val_seconds += float(stage_map.get("validation", 0.0))
            val_raw_bytes += drop_bytes
            val_rows += int(rep.validation_report.total_evaluated)

        if not rep.validation_report.held:
            write_seconds += float(stage_map.get("store_write", 0.0))
            written_rows += int(
                rep.validation_report.valid_count + rep.validation_report.quarantine_count
            )
            raw_ingested_bytes += drop_bytes

    total_wall_s = time.perf_counter() - t_wall_start
    onboarding_rss_mb = _get_process_tree_rss_mb()

    vendor_store_dir = run_store_dir / vendor
    parquet_bytes = sum(
        p.stat().st_size for p in vendor_store_dir.rglob("*.parquet") if p.is_file()
    )
    store_total_bytes = sum(p.stat().st_size for p in vendor_store_dir.rglob("*") if p.is_file())

    val_gb_per_s_per_core = (
        (val_raw_bytes / (1024.0**3)) / val_seconds / cpu_cores if val_seconds > 0 else 0.0
    )
    val_rows_per_s = val_rows / val_seconds if val_seconds > 0 else 0.0
    write_rows_per_s = written_rows / write_seconds if write_seconds > 0 else 0.0
    write_mb_per_s = (parquet_bytes / (1024.0**2)) / write_seconds if write_seconds > 0 else 0.0
    storage_factor = store_total_bytes / raw_total_bytes if raw_total_bytes > 0 else 0.0
    storage_factor_ingested = (
        store_total_bytes / raw_ingested_bytes if raw_ingested_bytes > 0 else storage_factor
    )

    rev_rows = _load_revision_rows(vendor_store_dir)
    revisions_detected = len(rev_rows)

    if gt_data is not None:
        revision_mode = "ground_truth"
        planted_keys: set[tuple[str, str, datetime.datetime]] = set()
        base_ev = datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        for d_info in gt_data["drops"].values():
            for fault in d_info.get("faults", []):
                if fault.get("type") == "restatement":
                    fld_name = str(fault.get("field", "price"))
                    for i in fault.get("indices", []):
                        idx_int = int(i)
                        ticker = DEFAULT_ENTITIES[idx_int % len(DEFAULT_ENTITIES)]
                        cik = CIK_MAPPING[ticker]
                        ev_dt = base_ev + datetime.timedelta(seconds=idx_int // 10)
                        planted_keys.add((cik, fld_name, ev_dt))

        detected_keys = {(str(r["entity_id"]), str(r["field"]), r["event_time"]) for r in rev_rows}
        tp = len(planted_keys & detected_keys)
        rev_recall: float | None = tp / len(planted_keys) if planted_keys else 1.0
        rev_precision: float | None = tp / len(detected_keys) if detected_keys else 0.0
    else:
        revision_mode = "unlabeled"
        rev_recall = None
        rev_precision = None

    # Release onboarding intermediates prior to query verification
    gc.collect()

    rng = random.Random(seed * 10_007 + run_idx)
    sampled_keys = _sample_store_keys(vendor_store_dir)
    pit_violations = _verify_pit_queries(
        vendor_store_dir=vendor_store_dir,
        sampled_keys=sampled_keys,
        rev_rows=rev_rows,
        pit_queries=pit_queries,
        rng=rng,
    )

    audit_log_path = work_dir / "audit_leaks.jsonl"
    leak_recall, leak_fp = _verify_leak_detector(
        vendor_store_dir=vendor_store_dir,
        sampled_keys=sampled_keys,
        leak_queries=leak_queries,
        audit_log_path=audit_log_path,
        rng=rng,
    )

    peak_rss_mb = max(onboarding_rss_mb, _get_process_tree_rss_mb())

    return {
        "run_idx": run_idx,
        "cpu_cores": cpu_cores,
        "total_time_s": total_wall_s,
        "stages": stage_totals,
        "val_gb_per_s_per_core": val_gb_per_s_per_core,
        "val_rows_per_s": val_rows_per_s,
        "write_rows_per_s": write_rows_per_s,
        "write_mb_per_s": write_mb_per_s,
        "peak_rss_mb": peak_rss_mb,
        "revision_mode": revision_mode,
        "revision_recall": rev_recall,
        "revision_precision": rev_precision,
        "revisions_detected": revisions_detected,
        "pit_queries": pit_queries,
        "pit_violations": pit_violations,
        "leak_queries_planted": leak_queries,
        "leak_recall": leak_recall,
        "leak_false_positives": leak_fp,
        "storage_factor": storage_factor,
        "storage_factor_ingested": storage_factor_ingested,
    }


def _stat_triplet(values: list[float]) -> tuple[float, float, float]:
    """Compute (median, min, max) over non-empty numeric list."""
    med = float(statistics.median(values))
    return med, float(min(values)), float(max(values))


def _aggregate_runs(
    workload: str,
    workload_label: str,
    seed: int,
    run_records: list[dict[str, Any]],
) -> dict[str, str]:
    """Aggregate per-iteration metrics into a CSV row dictionary (median and min-max)."""
    runs_n = len(run_records)
    cpu_cores = int(run_records[0]["cpu_cores"])

    tot_med, tot_min, tot_max = _stat_triplet([float(r["total_time_s"]) for r in run_records])
    vgb_med, vgb_min, vgb_max = _stat_triplet(
        [float(r["val_gb_per_s_per_core"]) for r in run_records]
    )
    vrw_med, vrw_min, vrw_max = _stat_triplet([float(r["val_rows_per_s"]) for r in run_records])
    wrw_med, wrw_min, wrw_max = _stat_triplet([float(r["write_rows_per_s"]) for r in run_records])
    wmb_med, wmb_min, wmb_max = _stat_triplet([float(r["write_mb_per_s"]) for r in run_records])
    rss_med, rss_min, rss_max = _stat_triplet([float(r["peak_rss_mb"]) for r in run_records])
    pit_med, pit_min, pit_max = _stat_triplet([float(r["pit_violations"]) for r in run_records])
    lrk_med, lrk_min, lrk_max = _stat_triplet([float(r["leak_recall"]) for r in run_records])
    sf_med, sf_min, sf_max = _stat_triplet([float(r["storage_factor"]) for r in run_records])
    sfi_med, _, _ = _stat_triplet([float(r["storage_factor_ingested"]) for r in run_records])
    rev_cnt_med, rev_cnt_min, rev_cnt_max = _stat_triplet(
        [float(r["revisions_detected"]) for r in run_records]
    )

    rev_mode = str(run_records[0]["revision_mode"])
    if rev_mode == "ground_truth":
        rr_med, rr_min, rr_max = _stat_triplet([float(r["revision_recall"]) for r in run_records])
        rp_med, rp_min, rp_max = _stat_triplet(
            [float(r["revision_precision"]) for r in run_records]
        )
        rr_s = (f"{rr_med:.4f}", f"{rr_min:.4f}", f"{rr_max:.4f}")
        rp_s = (f"{rp_med:.4f}", f"{rp_min:.4f}", f"{rp_max:.4f}")
    else:
        rr_s = ("N/A", "N/A", "N/A")
        rp_s = ("N/A", "N/A", "N/A")

    row: dict[str, str] = {
        "workload": workload,
        "workload_label": workload_label,
        "runs": str(runs_n),
        "seed": str(seed),
        "cpu_cores": str(cpu_cores),
        "total_time_s_median": f"{tot_med:.3f}",
        "total_time_s_min": f"{tot_min:.3f}",
        "total_time_s_max": f"{tot_max:.3f}",
        "val_gb_per_s_per_core_median": f"{vgb_med:.4f}",
        "val_gb_per_s_per_core_min": f"{vgb_min:.4f}",
        "val_gb_per_s_per_core_max": f"{vgb_max:.4f}",
        "val_rows_per_s_median": f"{vrw_med:.1f}",
        "val_rows_per_s_min": f"{vrw_min:.1f}",
        "val_rows_per_s_max": f"{vrw_max:.1f}",
        "write_rows_per_s_median": f"{wrw_med:.1f}",
        "write_rows_per_s_min": f"{wrw_min:.1f}",
        "write_rows_per_s_max": f"{wrw_max:.1f}",
        "write_mb_per_s_median": f"{wmb_med:.2f}",
        "write_mb_per_s_min": f"{wmb_min:.2f}",
        "write_mb_per_s_max": f"{wmb_max:.2f}",
        "peak_rss_mb_median": f"{rss_med:.2f}",
        "peak_rss_mb_min": f"{rss_min:.2f}",
        "peak_rss_mb_max": f"{rss_max:.2f}",
        "revision_mode": rev_mode,
        "revision_recall_median": rr_s[0],
        "revision_recall_min": rr_s[1],
        "revision_recall_max": rr_s[2],
        "revision_precision_median": rp_s[0],
        "revision_precision_min": rp_s[1],
        "revision_precision_max": rp_s[2],
        "revisions_detected_median": f"{int(round(rev_cnt_med))}",
        "revisions_detected_min": f"{int(round(rev_cnt_min))}",
        "revisions_detected_max": f"{int(round(rev_cnt_max))}",
        "pit_queries": str(int(run_records[0]["pit_queries"])),
        "pit_violations_median": f"{int(round(pit_med))}",
        "pit_violations_min": f"{int(round(pit_min))}",
        "pit_violations_max": f"{int(round(pit_max))}",
        "leak_queries_planted": str(int(run_records[0]["leak_queries_planted"])),
        "leak_recall_median": f"{lrk_med:.4f}",
        "leak_recall_min": f"{lrk_min:.4f}",
        "leak_recall_max": f"{lrk_max:.4f}",
        "leak_false_positives_max": str(max(int(r["leak_false_positives"]) for r in run_records)),
        "storage_factor_median": f"{sf_med:.3f}",
        "storage_factor_min": f"{sf_min:.3f}",
        "storage_factor_max": f"{sf_max:.3f}",
        "storage_factor_ingested_median": f"{sfi_med:.3f}",
    }

    for stg in STAGE_NAMES:
        s_med, s_min, s_max = _stat_triplet([float(r["stages"][stg]) for r in run_records])
        row[f"stage_{stg}_s_median"] = f"{s_med:.3f}"
        row[f"stage_{stg}_s_min"] = f"{s_min:.3f}"
        row[f"stage_{stg}_s_max"] = f"{s_max:.3f}"

    return row


def _load_existing_csv(csv_path: Path) -> dict[str, dict[str, str]]:
    """Read previously recorded workload rows from bench.csv if present."""
    if not csv_path.exists():
        return {}
    rows_by_key: dict[str, dict[str, str]] = {}
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            wk = r.get("workload")
            if wk:
                rows_by_key[wk] = dict(r)
    return rows_by_key


def _write_csv(csv_path: Path, rows_by_key: dict[str, dict[str, str]]) -> None:
    """Write ordered benchmark summary rows to bench.csv."""
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    ordered_keys: list[str] = []
    for wk, _ in STANDARD_COLUMN_ORDER:
        if wk in rows_by_key:
            ordered_keys.append(wk)
    for wk in sorted(rows_by_key.keys()):
        if wk not in ordered_keys:
            ordered_keys.append(wk)

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for wk in ordered_keys:
            r = rows_by_key[wk]
            writer.writerow({col: r.get(col, "") for col in CSV_COLUMNS})


def _fmt_cell_time(r: dict[str, str]) -> str:
    med = float(r["total_time_s_median"])
    mn = float(r["total_time_s_min"])
    mx = float(r["total_time_s_max"])
    if int(r["runs"]) == 1:
        return f"{med:.2f} s"
    return f"{med:.2f} s ({mn:.2f}–{mx:.2f} s)"


def _fmt_cell_val(r: dict[str, str]) -> str:
    gb_med = float(r["val_gb_per_s_per_core_median"])
    gb_min = float(r["val_gb_per_s_per_core_min"])
    gb_max = float(r["val_gb_per_s_per_core_max"])
    rw_med = float(r["val_rows_per_s_median"])
    cores = r["cpu_cores"]
    if int(r["runs"]) == 1:
        return f"{gb_med:.3f} GB/s/core ({rw_med:,.0f} rows/s, cores={cores})"
    return (
        f"{gb_med:.3f} GB/s/core ({gb_min:.3f}–{gb_max:.3f}; {rw_med:,.0f} rows/s, cores={cores})"
    )


def _fmt_cell_write(r: dict[str, str]) -> str:
    rw_med = float(r["write_rows_per_s_median"])
    mb_med = float(r["write_mb_per_s_median"])
    mb_min = float(r["write_mb_per_s_min"])
    mb_max = float(r["write_mb_per_s_max"])
    if int(r["runs"]) == 1:
        return f"{rw_med:,.0f} rows/s ({mb_med:.1f} MB/s)"
    return f"{rw_med:,.0f} rows/s ({mb_med:.1f} MB/s, {mb_min:.1f}–{mb_max:.1f})"


def _fmt_cell_rss(r: dict[str, str]) -> str:
    med = float(r["peak_rss_mb_median"])
    mn = float(r["peak_rss_mb_min"])
    mx = float(r["peak_rss_mb_max"])
    if int(r["runs"]) == 1:
        return f"{med:.1f} MiB"
    return f"{med:.1f} MiB ({mn:.1f}–{mx:.1f} MiB)"


def _fmt_cell_revisions(r: dict[str, str]) -> str:
    cnt = int(r["revisions_detected_median"])
    if r.get("revision_mode") == "ground_truth":
        rec = float(r["revision_recall_median"]) * 100.0
        prec = float(r["revision_precision_median"]) * 100.0
        return f"recall {rec:.1f}%, precision {prec:.1f}% ({cnt} revisions)"
    return f"{cnt:,} revisions detected (unlabeled)"


def _fmt_cell_pit(r: dict[str, str]) -> str:
    viol = int(r["pit_violations_max"])
    q_cnt = int(r["pit_queries"])
    return f"{viol} / {q_cnt:,}"


def _fmt_cell_leaks(r: dict[str, str]) -> str:
    rec = float(r["leak_recall_median"]) * 100.0
    planted = int(r["leak_queries_planted"])
    fp = int(r["leak_false_positives_max"])
    return f"{rec:.1f}% ({planted:,}/{planted:,}, {fp} false positives)"


def _fmt_cell_storage(r: dict[str, str]) -> str:
    med = float(r["storage_factor_median"])
    mn = float(r["storage_factor_min"])
    mx = float(r["storage_factor_max"])
    if int(r["runs"]) == 1 or abs(mx - mn) < 0.001:
        return f"{med:.3f}×"
    return f"{med:.3f}× ({mn:.3f}–{mx:.3f}×)"


def format_bench_markdown(
    rows_by_key: dict[str, dict[str, str]],
    hardware_block: str,
) -> str:
    """Render benchmark results and stage timings as Markdown report."""
    columns: list[tuple[str, str]] = []
    for wk, default_lbl in STANDARD_COLUMN_ORDER:
        if wk in rows_by_key:
            columns.append((wk, rows_by_key[wk]["workload_label"]))
        else:
            columns.append((wk, default_lbl))
    for wk in sorted(rows_by_key.keys()):
        if all(wk != existing_wk for existing_wk, _ in columns):
            columns.append((wk, rows_by_key[wk]["workload_label"]))

    lines: list[str] = [
        "# Benchmark Results",
        "",
        "Values report median and min–max range across isolated subprocess runs.",
        "",
    ]

    header = "| Metric | " + " | ".join(lbl for _, lbl in columns) + " |"
    sep = "|---|" + "|".join("---" for _ in columns) + "|"
    lines.append(header)
    lines.append(sep)

    metric_formatters = [
        ("Time from raw drop to report", _fmt_cell_time),
        ("Validation throughput", _fmt_cell_val),
        ("Store write throughput", _fmt_cell_write),
        ("Peak resident memory", _fmt_cell_rss),
        ("Revision detection recall and precision", _fmt_cell_revisions),
        ("Point-in-time violations", _fmt_cell_pit),
        ("Leak detector recall", _fmt_cell_leaks),
        ("Storage factor", _fmt_cell_storage),
    ]

    for metric_name, fn in metric_formatters:
        cells: list[str] = []
        for wk, _ in columns:
            if wk in rows_by_key:
                cells.append(fn(rows_by_key[wk]))
            else:
                cells.append("TBD")
        lines.append(f"| {metric_name} | " + " | ".join(cells) + " |")

    lines.append("")
    lines.append("## Per-Stage Wall-Clock Breakdown (Median)")
    lines.append("")
    measured_cols = [(wk, lbl) for wk, lbl in columns if wk in rows_by_key]
    if measured_cols:
        stg_header = "| Stage | " + " | ".join(lbl for _, lbl in measured_cols) + " |"
        stg_sep = "|---|" + "|".join("---" for _ in measured_cols) + "|"
        lines.append(stg_header)
        lines.append(stg_sep)
        for stg in STAGE_NAMES:
            stg_label = stg.replace("_", " ").capitalize()
            vals = []
            for wk, _ in measured_cols:
                r = rows_by_key[wk]
                med = float(r[f"stage_{stg}_s_median"])
                mn = float(r[f"stage_{stg}_s_min"])
                mx = float(r[f"stage_{stg}_s_max"])
                if int(r["runs"]) == 1:
                    vals.append(f"{med:.3f} s")
                else:
                    vals.append(f"{med:.3f} s ({mn:.3f}–{mx:.3f} s)")
            lines.append(f"| {stg_label} | " + " | ".join(vals) + " |")
        tot_vals = [_fmt_cell_time(rows_by_key[wk]) for wk, _ in measured_cols]
        lines.append("| **Total** | " + " | ".join(tot_vals) + " |")
        lines.append("")

    lines.append("## Hardware and Environment")
    lines.append("")
    lines.append(hardware_block)
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append(
        "- Each iteration executes inside a fresh Python subprocess so `resource.getrusage` "
        "captures an independent resident memory high-water mark."
    )
    lines.append(
        "- Synthetic sequences comprise 3 drops (`drop_1` baseline, `drop_2` with planted "
        "duplicates, type flips, out-of-range prices, and 25 restatements, and `drop_3` with "
        "schema column removal held at the schema gate). Storage factor divides vendor store "
        "bytes by total raw drop sequence bytes."
    )
    lines.append("")
    return "\n".join(lines)


def _discover_sec_quarters(sec_dir: Path, quarters_arg: str | None) -> list[Path]:
    """Locate extracted SEC FSDS quarterly directories ordered chronologically."""
    if quarters_arg:
        names = [q.strip() for q in quarters_arg.split(",") if q.strip()]
        dirs = [sec_dir / name for name in names]
        missing = [str(d) for d in dirs if not (d / "sub.txt").exists()]
        if missing:
            raise FileNotFoundError(
                f"Requested SEC quarter directories missing or incomplete: {', '.join(missing)}"
            )
        return dirs

    if not sec_dir.exists():
        raise FileNotFoundError(f"SEC raw directory does not exist: {sec_dir}")

    dirs = sorted(
        d
        for d in sec_dir.iterdir()
        if d.is_dir() and (d / "sub.txt").exists() and (d / "num.txt").exists()
    )
    if not dirs:
        raise FileNotFoundError(f"No extracted SEC FSDS quarters found under {sec_dir}")
    return dirs


def run_benchmark(
    *,
    mode: str = "synthetic",
    size: str = "50MB",
    runs: int = 5,
    seed: int = 1,
    sec_dir: Path = REPO_ROOT / "data" / "raw" / "sec_fsds",
    quarters: str | None = None,
    output_dir: Path = REPO_ROOT / "bench" / "results",
    pit_queries: int = 100_000,
    leak_queries: int = 1_000,
    merge_existing: bool = True,
    in_process: bool = False,
) -> dict[str, str]:
    """Run benchmark suite across `runs` iterations and persist bench.csv and bench.md."""
    if runs < 1:
        raise ValueError("runs must be >= 1")

    base_tmp = Path(tempfile.mkdtemp(prefix="vdok_bench_"))
    run_records: list[dict[str, Any]] = []

    try:
        if mode == "synthetic":
            workload, workload_label = parse_workload_label(size)
            raw_seq_dir = base_tmp / "raw_synthetic"
            generate_synthetic_sequence(
                output_dir=raw_seq_dir,
                size_gb=size,
                seed=seed,
                faults="default",
                num_drops=3,
                format="csv",
            )
            quarter_dirs_str: list[str] = []
        elif mode == "sec_fsds":
            q_dirs = _discover_sec_quarters(sec_dir, quarters)
            q_names = [d.name for d in q_dirs]
            span = f"{q_names[0]}–{q_names[-1]}" if len(q_names) > 1 else q_names[0]
            workload = "sec_fsds"
            workload_label = f"SEC FSDS ({span})"
            raw_seq_dir = base_tmp / "unused"
            quarter_dirs_str = [str(d.resolve()) for d in q_dirs]
        else:
            raise ValueError(f"Unsupported benchmark mode: {mode}")

        for r_idx in range(1, runs + 1):
            iter_dir = base_tmp / f"iter_{r_idx}"
            iter_dir.mkdir(parents=True, exist_ok=True)
            cfg = {
                "mode": mode,
                "run_idx": r_idx,
                "seed": seed,
                "pit_queries": pit_queries,
                "leak_queries": leak_queries,
                "work_dir": str(iter_dir),
                "raw_seq_dir": str(raw_seq_dir),
                "quarter_dirs": quarter_dirs_str,
            }

            try:
                if in_process:
                    rec = _execute_single_worker(cfg)
                else:
                    cfg_file = iter_dir / "worker_cfg.json"
                    out_file = iter_dir / "worker_out.json"
                    cfg_file.write_text(json.dumps(cfg), encoding="utf-8")
                    subprocess.run(
                        [
                            sys.executable,
                            str(Path(__file__).resolve()),
                            "--internal-worker",
                            str(cfg_file),
                            str(out_file),
                        ],
                        check=True,
                        cwd=str(REPO_ROOT),
                    )
                    rec = json.loads(out_file.read_text(encoding="utf-8"))
                run_records.append(rec)
            finally:
                purge_tree(iter_dir)

        summary_row = _aggregate_runs(
            workload=workload,
            workload_label=workload_label,
            seed=seed,
            run_records=run_records,
        )

        csv_path = output_dir / "bench.csv"
        md_path = output_dir / "bench.md"
        rows_by_key = _load_existing_csv(csv_path) if merge_existing else {}
        rows_by_key[workload] = summary_row
        _write_csv(csv_path, rows_by_key)

        hw_block = format_hardware_block()
        md_content = format_bench_markdown(rows_by_key, hw_block)
        md_path.parent.mkdir(parents=True, exist_ok=True)
        md_path.write_text(md_content, encoding="utf-8")

        return summary_row
    finally:
        purge_tree(base_tmp)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for synthetic, SEC FSDS, and CI smoke benchmark runs."""
    parser = argparse.ArgumentParser(description="Benchmark execution runner")
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="Run 50 MB single-iteration CI smoke benchmark and verify 0 violations",
    )
    parser.add_argument(
        "--sec",
        action="store_true",
        help="Run benchmark on real SEC FSDS quarterly archives in data/raw/sec_fsds",
    )
    parser.add_argument(
        "--sec-dir",
        default=str(REPO_ROOT / "data" / "raw" / "sec_fsds"),
        help="Path to extracted SEC FSDS archives directory",
    )
    parser.add_argument(
        "--quarters",
        default=None,
        help="Optional comma-separated SEC quarters (e.g. 2023q3,2023q4)",
    )
    parser.add_argument(
        "--size",
        default="10GB",
        help="Dataset size for synthetic benchmark run (e.g. 50MB, 1GB, 10GB)",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=5,
        help="Number of isolated iterations",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=1,
        help="Seed for pseudorandom generator",
    )
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "bench" / "results"),
        help="Output directory for bench.csv and bench.md",
    )
    parser.add_argument(
        "--pit-queries",
        type=int,
        default=100_000,
        help="Number of random Store.value() queries verified against brute-force oracle",
    )
    parser.add_argument(
        "--leak-queries",
        type=int,
        default=1_000,
        help="Number of planted leaking reads verified by detect_leaks",
    )
    parser.add_argument(
        "--no-merge",
        action="store_true",
        help="Overwrite bench.csv instead of merging with prior workloads",
    )
    parser.add_argument(
        "--internal-worker",
        nargs=2,
        metavar=("CFG_JSON", "OUT_JSON"),
        help=argparse.SUPPRESS,
    )

    args = parser.parse_args(argv)

    if args.internal_worker:
        cfg_p = Path(args.internal_worker[0])
        out_p = Path(args.internal_worker[1])
        cfg_obj = json.loads(cfg_p.read_text(encoding="utf-8"))
        res_obj = _execute_single_worker(cfg_obj)
        out_p.write_text(json.dumps(res_obj, indent=2), encoding="utf-8")
        return 0

    if args.smoke:
        smoke_out = Path(tempfile.mkdtemp(prefix="vdok_smoke_out_"))
        try:
            summary = run_benchmark(
                mode="synthetic",
                size="50MB",
                runs=1,
                seed=args.seed,
                output_dir=smoke_out,
                pit_queries=args.pit_queries,
                leak_queries=args.leak_queries,
                merge_existing=False,
            )
            viol = int(summary["pit_violations_max"])
            leak_rec = float(summary["leak_recall_median"])
            print(
                f"Benchmark smoke test complete: time={summary['total_time_s_median']}s, "
                f"rss={summary['peak_rss_mb_median']} MiB, "
                f"leak_recall={leak_rec:.2%}, {viol} violations observed."
            )
            return 0 if viol == 0 else 1
        finally:
            purge_tree(smoke_out)

    mode = "sec_fsds" if args.sec else "synthetic"
    summary = run_benchmark(
        mode=mode,
        size=args.size,
        runs=args.runs,
        seed=args.seed,
        sec_dir=Path(args.sec_dir),
        quarters=args.quarters,
        output_dir=Path(args.output_dir),
        pit_queries=args.pit_queries,
        leak_queries=args.leak_queries,
        merge_existing=not args.no_merge,
    )

    viol = int(summary["pit_violations_max"])
    print(
        f"Benchmark [{summary['workload_label']}] complete across {summary['runs']} runs: "
        f"median_time={summary['total_time_s_median']}s, "
        f"median_rss={summary['peak_rss_mb_median']} MiB, "
        f"pit_violations={viol}, "
        f"results -> {args.output_dir}"
    )
    return 0 if viol == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
