"""End-to-end ingestion and onboarding pipeline.

Executes discovery, schema contract validation, normalization to canonical Arrow
representation, identifier resolution, multi-tier validation routing, append-only
bitemporal store persistence, drop-over-drop revision detection, and readiness
report generation.
"""

from __future__ import annotations

import datetime
import json
import resource
import time
from pathlib import Path
from typing import Optional

from vdok.discover import scan
from vdok.manifest import VendorManifest
from vdok.normalize import CANONICAL_ARROW_SCHEMA, normalize_drop
from vdok.report import ReadinessReport, compile_readiness_report, load_report
from vdok.revisions import RevisionSummary, compute_revisions
from vdok.schema import DriftSeverity, SchemaDiff, check_schema, pin_schema
from vdok.store import BitemporalStore
from vdok.symbols import SymbolMaster
from vdok.validate import (
    QUARANTINE_ARROW_SCHEMA,
    HeldDropRecord,
    ValidationResult,
    ValidationSummary,
    validate_drop,
    write_held_record,
    write_quarantine_table,
)


def onboard_drop(
    vendor: str,
    drop_path: Path | str,
    manifest_path: Path | str | None = None,
    schema_dir: Path | str = "schemas",
    rules_path: Path | str | None = None,
    symbol_master_path: Path | str | None = None,
    store_dir: Path | str = "data/store",
    results_dir: Path | str = "data/results",
    as_of: datetime.datetime | None = None,
    force: bool = False,
) -> tuple[ReadinessReport, int]:
    """Execute end-to-end onboarding pipeline for a vendor drop.

    Returns tuple of (ReadinessReport, exit_code), where exit_code is:
    0: READY or READY WITH WARNINGS
    1: HELD
    """
    t_start = time.perf_counter()
    timings: dict[str, float] = {}

    # 1. Resolve and load vendor manifest
    if manifest_path is None:
        cand_manifest = Path("manifests") / f"{vendor}.yaml"
        if not cand_manifest.exists():
            raise FileNotFoundError(
                f"Manifest for vendor '{vendor}' not found at {cand_manifest}. "
                "Specify manifest_path explicitly."
            )
        manifest_path = cand_manifest

    manifest = VendorManifest.from_file(manifest_path)

    # 2. File discovery and drop fingerprinting
    t0 = time.perf_counter()
    discovery_res = scan(drop_path)
    drop_id = discovery_res.drop_id
    timings["discovery"] = time.perf_counter() - t0

    # 3. Store initialization and G3 idempotency verification
    store = BitemporalStore(base_dir=store_dir, vendor=vendor)
    if not force:
        history = store.get_manifest_history()
        existing_drop_ids = [h.drop_id for h in history]
        if drop_id in existing_drop_ids:
            existing = load_report(vendor=vendor, drop_id=drop_id, base_dir=results_dir)
            if existing is not None:
                return existing, 0

    # 4. Schema verification and pinning
    t0 = time.perf_counter()
    schema_file = Path(schema_dir) / f"{vendor}.yaml"
    schema_diff: SchemaDiff
    if not schema_file.exists():
        pin_schema(vendor=vendor, drop_path=drop_path, schema_dir=Path(schema_dir))
        schema_diff = SchemaDiff(
            vendor=vendor,
            pinned_version=1,
            has_hold=False,
            differences=[],
        )
    else:
        schema_diff = check_schema(vendor=vendor, drop_path=drop_path, schema_dir=Path(schema_dir))
    timings["schema"] = time.perf_counter() - t0

    # 5. Identifier symbol master resolution
    symbol_master: Optional[SymbolMaster] = None
    if symbol_master_path is not None and Path(symbol_master_path).exists():
        p_sym = Path(symbol_master_path)
        if p_sym.suffix == ".json":
            from vdok.symbols import build_symbol_master

            out_dir = Path(store_dir) / "symbols"
            master_path = build_symbol_master(p_sym, output_dir=out_dir)
            symbol_master = SymbolMaster.load(master_path)
        else:
            symbol_master = SymbolMaster.load(p_sym)
    else:
        # Check standard symbol master locations
        candidates = [
            Path("data/symbols/master.parquet"),
            Path("tests/fixtures/symbols/company_tickers.json"),
        ]
        for c in candidates:
            if c.exists():
                try:
                    if c.suffix == ".parquet":
                        symbol_master = SymbolMaster.load(c)
                        break
                    elif c.suffix == ".json":
                        from vdok.symbols import build_symbol_master

                        # build in-memory / cache
                        out_dir = Path(store_dir) / "symbols"
                        master_path = build_symbol_master(c, output_dir=out_dir)
                        symbol_master = SymbolMaster.load(master_path)
                        break
                except Exception:
                    pass

    # 6. Normalization
    t0 = time.perf_counter()
    canonical_table = normalize_drop(
        drop_dir=drop_path,
        manifest=manifest,
        drop_id=drop_id,
        symbol_master=symbol_master,
        as_of=as_of,
    )
    timings["normalization"] = time.perf_counter() - t0

    # 7. Validation
    t0 = time.perf_counter()
    resolved_rules = rules_path or manifest.rules
    val_res: ValidationResult

    if schema_diff.has_hold:
        hold_reasons = [
            f"Schema drift: {d.category.value} on column '{d.column}' ({d.proposed_change})"
            for d in schema_diff.differences
            if d.severity == DriftSeverity.HOLD
        ]
        val_res = ValidationResult(
            drop_id=drop_id,
            vendor=vendor,
            held=True,
            valid_table=CANONICAL_ARROW_SCHEMA.empty_table(),
            quarantine_table=QUARANTINE_ARROW_SCHEMA.empty_table(),
            summary=ValidationSummary(
                total_rows=len(canonical_table),
                valid_count=0,
                quarantine_count=0,
                warn_count=0,
                quarantine_share=0.0,
                held=True,
                rule_counts={"schema_drift_hold": len(hold_reasons)},
                severity_counts={"hold": len(hold_reasons)},
                hold_reasons=hold_reasons,
            ),
        )
    else:
        val_res = validate_drop(
            canonical_table,
            manifest=manifest,
            drop_id=drop_id,
            rules_path=resolved_rules,
            schema_diff=schema_diff,
        )
    timings["validation"] = time.perf_counter() - t0

    # 8. Store and quarantine persistence
    t0 = time.perf_counter()
    if val_res.held:
        if val_res.held_record is not None:
            write_held_record(val_res.held_record, store_dir=Path(store_dir))
        else:
            rec = HeldDropRecord(
                drop_id=drop_id,
                vendor=vendor,
                held_at=datetime.datetime.now(datetime.timezone.utc),
                reason="; ".join(val_res.held_reasons),
                rule_name="batch_hold",
                details_json=json.dumps({"reasons": val_res.held_reasons}),
            )
            write_held_record(rec, store_dir=Path(store_dir))
    else:
        if val_res.clean_table is not None and len(val_res.clean_table) > 0:
            store.write_drop(val_res.clean_table, manifest=manifest)
        if val_res.quarantine_table is not None and len(val_res.quarantine_table) > 0:
            write_quarantine_table(
                val_res.quarantine_table,
                store_dir=Path(store_dir),
                vendor=vendor,
                drop_id=drop_id,
            )
    timings["store_write"] = time.perf_counter() - t0

    # 9. Revision detection
    t0 = time.perf_counter()
    rev_summary: Optional[RevisionSummary] = None
    if not val_res.held:
        rev_summary, rev_tbl = compute_revisions(store, vendor, drop_id)
        if rev_tbl is not None and len(rev_tbl) > 0:
            store.write_revisions(rev_tbl)
    else:
        rev_summary = RevisionSummary(
            vendor=vendor,
            drop_id=drop_id,
            total_revisions=0,
            field_rates={},
            largest_changes=[],
        )
    timings["revisions"] = time.perf_counter() - t0

    # 10. Readiness report generation and serialization
    t0 = time.perf_counter()
    peak_rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    peak_mb = peak_rss_kb / 1024.0

    total_drops = len(store.get_manifest_history())

    report = compile_readiness_report(
        vendor=vendor,
        drop_id=drop_id,
        discovery_result=discovery_res,
        schema_diff=schema_diff,
        manifest=manifest,
        canonical_table=canonical_table,
        validation_result=val_res,
        revision_summary=rev_summary,
        timings=timings,
        peak_memory_mb=peak_mb,
        store_drops_count=total_drops,
    )
    timings["reporting"] = time.perf_counter() - t0
    timings["total"] = time.perf_counter() - t_start
    report.timings.timings_per_stage_seconds = timings
    report.timings.total_seconds = timings["total"]

    # Persist report artifacts
    report.save(base_dir=results_dir)

    exit_code = 1 if val_res.held else 0
    return report, exit_code
