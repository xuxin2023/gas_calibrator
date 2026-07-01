"""Build a V1.5 formal archive closure from existing offline evidence.

The closure is an index over already-generated V1.5 artifacts. It regenerates
reports, rebuilds the evidence bundle so those reports are indexed, and can
optionally import the final bundle into PostgreSQL. It never opens COM ports,
controls gas/water routes, controls valves/PACE, or writes coefficients.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Mapping, Sequence

from ..storage.v1_5_evidence.bundle import (
    build_evidence_bundle,
    bundle_summary,
    bundle_traceability_summary,
    sha256_file,
    write_bundle_json,
)
from ..storage.v1_5_evidence.repository import apply_migrations, import_bundle, mask_dsn
from .formal_reports import write_v1_5_calibration_reports
from .v1_5_calibration_capability import (
    build_v1_5_calibration_capability,
    render_v1_5_calibration_capability_markdown,
)
from .v1_5_run_evidence_status import (
    build_v1_5_run_evidence_status,
    render_v1_5_run_evidence_status_markdown,
)


SCHEMA = "v1_5_formal_archive_closure_v1"
_WINDOWS_LEGACY_PATH_LIMIT = 260
_LONGEST_PER_DEVICE_REPORT_RELATIVE_PATH = Path(
    "reports/per_device_certificates/device_000_calibration_certificate.docx"
)
DB_MODE_CHOICES = ("skip", "dry_run", "import")


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _write_json(path: str | Path, payload: Mapping[str, Any]) -> Path:
    target = Path(path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return target


def _write_markdown(path: str | Path, text: str) -> Path:
    target = Path(path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8-sig")
    return target


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True


_ARCHIVE_MARKERS = (
    "v1_5_formal_archive_closure_index.json",
    "initial_evidence_bundle.json",
    "traceability_summary.json",
    "database_import_summary.json",
)


def _latest_named_artifact(root: Path, name: str, *, exclude_dirs: Sequence[Path] = ()) -> Path | None:
    matches: list[Path] = []
    resolved_excludes = tuple(path.resolve() for path in exclude_dirs)
    for path in root.rglob(name):
        if not path.is_file():
            continue
        if any(_is_relative_to(path, excluded) for excluded in resolved_excludes):
            continue
        matches.append(path.resolve())
    if not matches:
        return None
    return max(matches, key=lambda item: item.stat().st_mtime)


def _is_archive_closure_dir(path: Path) -> bool:
    return path.is_dir() and any((path / marker).exists() for marker in _ARCHIVE_MARKERS)


def _archive_dirs_to_exclude(root: Path, *, keep: Path | None = None) -> Sequence[Path]:
    keep_resolved = keep.resolve() if keep else None
    excluded = []
    for child in root.iterdir():
        if keep_resolved is not None and child.resolve() == keep_resolved:
            continue
        if _is_archive_closure_dir(child):
            excluded.append(child.resolve())
    return excluded


def _validate_report_path_budget(closure_dir: Path) -> None:
    longest_expected = closure_dir / _LONGEST_PER_DEVICE_REPORT_RELATIVE_PATH
    if len(str(longest_expected)) >= _WINDOWS_LEGACY_PATH_LIMIT:
        raise ValueError(
            "output_dir path is too long for V1.5 per-device certificate artifacts on Windows; "
            "use a shorter archive output directory name, for example 'arc_stdgas'. "
            f"longest_expected_path_length={len(str(longest_expected))}, "
            f"limit={_WINDOWS_LEGACY_PATH_LIMIT}, "
            f"longest_expected_path={longest_expected}"
        )


def _artifact_record(role: str, path: str | Path) -> Dict[str, Any]:
    target = Path(path).resolve()
    return {
        "role": role,
        "path": str(target),
        "sha256": sha256_file(target),
        "size_bytes": target.stat().st_size,
    }


def _artifact_records(paths: Mapping[str, Path]) -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    for role, path in sorted(paths.items()):
        if Path(path).exists():
            rows.append(_artifact_record(role, path))
    return rows


def _load_reviewed_standard_gases(path: str | Path) -> list[Dict[str, Any]]:
    source = Path(path).resolve()
    payload = json.loads(source.read_text(encoding="utf-8-sig"))
    gases = payload.get("standard_gases") if isinstance(payload, Mapping) else payload
    if not isinstance(gases, Sequence) or isinstance(gases, (str, bytes, bytearray)):
        raise ValueError("standard_gases_json must contain a list or a 'standard_gases' list")
    rows = [dict(item) for item in gases if isinstance(item, Mapping)]
    if not rows:
        raise ValueError("standard_gases_json does not contain any standard gas rows")
    return rows


def _identity_getco_traceability_summary(root: Path, *, closure_dir: Path) -> Dict[str, Any]:
    source = _latest_named_artifact(
        root,
        "v1_5_getco_identity_readiness.json",
        exclude_dirs=tuple(_archive_dirs_to_exclude(root, keep=closure_dir)) + (closure_dir,),
    )
    if not source:
        return {
            "status": "missing",
            "ready_for_archive_release": False,
            "traceability_review_required": True,
            "evidence_path": "",
            "overall_status": "missing",
            "reasons": ["v1_5_getco_identity_readiness_json_missing"],
            "next_action": "generate identity/GETCO readiness with SN/device_code traceability before archive release",
        }
    payload = json.loads(source.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, Mapping):
        return {
            "status": "invalid",
            "ready_for_archive_release": False,
            "traceability_review_required": True,
            "evidence_path": str(source),
            "overall_status": "invalid",
            "reasons": ["v1_5_getco_identity_readiness_json_not_object"],
            "next_action": "regenerate identity/GETCO readiness sidecar",
        }
    checks = [row for row in payload.get("checks") or [] if isinstance(row, Mapping)]
    review_reasons: list[str] = []
    for row in checks:
        if str(row.get("status") or "") == "review_required":
            review_reasons.extend(str(reason) for reason in row.get("reasons") or [] if str(reason))
    traceability_review_required = bool(payload.get("traceability_review_required")) or bool(review_reasons)
    overall_status = str(payload.get("overall_status") or "")
    if overall_status != "identity_getco_ready_for_auxiliary_neutralization":
        status = "not_ready"
        review_reasons.append(f"identity_getco_overall_status={overall_status or 'missing'}")
    elif traceability_review_required:
        status = "review_required"
    else:
        status = "ready"
    return {
        "status": status,
        "ready_for_archive_release": status == "ready",
        "traceability_review_required": traceability_review_required,
        "evidence_path": str(source),
        "overall_status": overall_status,
        "active_analyzer_count": payload.get("active_analyzer_count"),
        "analyzer_device_ids": payload.get("analyzer_device_ids") or [],
        "reasons": review_reasons,
        "next_action": (
            "carry_forward"
            if status == "ready"
            else "resolve SN/device_code traceability review before database import or formal archive release"
        ),
    }


def _plan_with_reviewed_standard_gases(
    *,
    plan_path: Path,
    standard_gases_json: str | Path | None,
    closure_dir: Path,
) -> tuple[Path, Dict[str, Path]]:
    """Return an archive-local plan snapshot with explicit reviewed gas evidence."""

    if not standard_gases_json:
        return plan_path, {}
    gases_source = Path(standard_gases_json).resolve()
    gases = _load_reviewed_standard_gases(gases_source)
    gas_snapshot = {
        "schema": "v1_5_reviewed_standard_gases_snapshot_v1",
        "created_at": _now(),
        "source_path": str(gases_source),
        "source_sha256": sha256_file(gases_source),
        "standard_gases": gases,
        "physical_meaning": (
            "Reviewed standard-gas and H2O reference rows used to bind the V1.5 evidence "
            "bundle to traceable CO2/H2O input quantities. This snapshot is archive-only "
            "and does not infer certificate values from sample data."
        ),
    }
    gas_snapshot_path = _write_json(closure_dir / "standard_gases_reviewed_snapshot.json", gas_snapshot)

    plan = json.loads(plan_path.read_text(encoding="utf-8-sig"))
    if not isinstance(plan, Mapping):
        raise ValueError("plan_json must contain a JSON object")
    merged_plan = dict(plan)
    merged_plan["standard_gases"] = gases
    traceability_sources = dict(merged_plan.get("traceability_sources") or {})
    traceability_sources["standard_gases_json"] = {
        "path": str(gases_source),
        "sha256": sha256_file(gases_source),
        "snapshot_path": str(gas_snapshot_path),
        "reviewed": True,
    }
    merged_plan["traceability_sources"] = traceability_sources
    merged_plan["archive_standard_gases_snapshot"] = str(gas_snapshot_path)
    merged_plan_path = _write_json(closure_dir / "formal_plan_snapshot_with_standard_gases.json", merged_plan)
    return merged_plan_path, {
        "standard_gases_reviewed_snapshot": gas_snapshot_path,
        "formal_plan_with_standard_gases": merged_plan_path,
    }


def _render_markdown_zh(index: Mapping[str, Any]) -> str:
    lines = [
        "# V1.5 \u6b63\u5f0f\u5f52\u6863\u95ed\u73af\u7d22\u5f15",
        "",
        f"- \u8fd0\u884c\u76ee\u5f55\uff1a`{index.get('run_dir')}`",
        f"- \u8bc1\u636e\u72b6\u6001\uff1a`{index.get('evidence_status')}`",
        f"- \u8bc1\u636e\u5305\u72b6\u6001\uff1a`{index.get('package_status')}`",
        f"- \u6570\u636e\u5e93\u6a21\u5f0f\uff1a`{(index.get('database') or {}).get('mode')}`",
        "",
        "## \u7269\u7406\u8fb9\u754c",
        "",
    ]
    for key, value in (index.get("physical_boundaries") or {}).items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## \u6d41\u7a0b\u6b65\u9aa4", ""])
    for step in index.get("workflow_steps") or []:
        lines.append(f"- `{step.get('step')}`: `{step.get('status')}` - {step.get('meaning')}")
    lines.extend(["", "## \u53ef\u8ffd\u6eaf\u68c0\u67e5", ""])
    for key, value in (index.get("traceability_checks") or {}).items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## \u5173\u952e\u4ea7\u7269", ""])
    lines.append("| role | sha256 | path |")
    lines.append("| --- | --- | --- |")
    for row in index.get("artifacts") or []:
        lines.append(f"| {row.get('role')} | {row.get('sha256')} | {row.get('path')} |")
    return "\n".join(lines).rstrip() + "\n"


def normalize_archive_db_mode(value: str | None) -> str:
    return str(value or "dry_run").strip().lower().replace("-", "_")


def _archive_workflow_steps(database: Mapping[str, Any], db_mode: str) -> list[Dict[str, Any]]:
    return [
        {
            "step": "initial_bundle",
            "status": "completed",
            "meaning": "\u51bb\u7ed3\u5df2\u6709\u539f\u59cb\u5e27\u3001\u6807\u51c6\u6c14\u3001\u538b\u529b\u53c2\u8003\u3001QC \u548c\u8fd0\u884c\u8bc1\u636e\u72b6\u6001\u3002",
        },
        {
            "step": "report_generation",
            "status": "completed",
            "meaning": "\u4ece\u8bc1\u636e\u5305\u751f\u6210\u8fd0\u884c\u62a5\u544a\u3001\u6280\u672f\u62a5\u544a\u548c\u6b63\u5f0f\u6821\u51c6\u62a5\u544a\u3002",
        },
        {
            "step": "final_bundle_with_reports",
            "status": "completed",
            "meaning": "\u62a5\u544a\u843d\u5728\u8fd0\u884c\u76ee\u5f55\u5185\u5e76\u8fdb\u5165\u6700\u7ec8 evidence bundle \u7684 report/sample_file \u7d22\u5f15\u3002",
        },
        {
            "step": "database_import",
            "status": "completed" if database.get("database_imported") else db_mode,
            "meaning": "\u6570\u636e\u5e93\u5bfc\u5165\u662f\u8bc1\u636e\u94fe\u7d22\u5f15\u52a8\u4f5c\uff1b\u4e0d\u4ee3\u8868\u8bbe\u5907\u63a7\u5236\u6216 real acceptance\u3002",
        },
    ]


def build_v1_5_formal_archive_closure(
    *,
    run_dir: str | Path,
    plan_json: str | Path,
    pressure_reference_json: str | Path,
    standard_gases_json: str | Path | None = None,
    contract_json: str | Path | None = None,
    output_dir: str | Path | None = None,
    pressure_check_csv: str | Path | None = None,
    component: str = "both",
    analyzer_prefix: str = "ga01",
    today: str | None = None,
    allow_pressure_fallback: bool = False,
    report_no: str = "",
    reviewer: str = "",
    approver: str = "",
    location: str = "",
    calibration_date: str = "",
    uncertainty_json: str | Path | None = None,
    db_mode: str = "dry_run",
    dsn: str | None = None,
    apply_db_migrations: bool = False,
    capability_verification_csvs: Sequence[str | Path] = (),
    capability_candidate_csvs: Sequence[str | Path] = (),
    co2_limit_pct: float = 1.5,
    h2o_limit_pct: float = 2.0,
) -> Dict[str, Any]:
    """Run the offline report/database/archive closure for one V1.5 run."""

    root = Path(run_dir).resolve()
    plan_path = Path(plan_json).resolve()
    pressure_reference_path = Path(pressure_reference_json).resolve()
    contract_path = Path(contract_json).resolve() if contract_json else None
    pressure_check_path = Path(pressure_check_csv).resolve() if pressure_check_csv else None
    closure_dir = Path(output_dir).resolve() if output_dir else root / "formal_archive_closure"
    if not _is_relative_to(closure_dir, root):
        raise ValueError("output_dir must be inside run_dir so generated reports can be indexed by the final bundle")
    _validate_report_path_budget(closure_dir)
    closure_dir.mkdir(parents=True, exist_ok=True)
    working_plan_path, traceability_snapshot_paths = _plan_with_reviewed_standard_gases(
        plan_path=plan_path,
        standard_gases_json=standard_gases_json,
        closure_dir=closure_dir,
    )

    initial_bundle = build_evidence_bundle(
        run_dir=root,
        plan_path=working_plan_path,
        pressure_reference_path=pressure_reference_path,
        component=component,
        analyzer_prefix=analyzer_prefix,
        require_quick_check_artifact=not bool(allow_pressure_fallback),
        pressure_check_path=pressure_check_path,
        today=today,
        artifact_exclude_dirs=_archive_dirs_to_exclude(root),
    )
    initial_bundle_json = write_bundle_json(initial_bundle, closure_dir / "initial_evidence_bundle.json")

    reports = write_v1_5_calibration_reports(
        evidence_bundle_path=initial_bundle_json,
        output_dir=closure_dir / "reports",
        report_no=report_no,
        reviewer=reviewer,
        approver=approver,
        location=location,
        calibration_date=calibration_date,
        analyzer_prefix=analyzer_prefix,
        uncertainty_json=uncertainty_json,
    )

    bundle_for_status = build_evidence_bundle(
        run_dir=root,
        plan_path=working_plan_path,
        pressure_reference_path=pressure_reference_path,
        component=component,
        analyzer_prefix=analyzer_prefix,
        require_quick_check_artifact=not bool(allow_pressure_fallback),
        pressure_check_path=pressure_check_path,
        today=today,
        artifact_exclude_dirs=_archive_dirs_to_exclude(root, keep=closure_dir),
    )
    bundle_for_status_json = write_bundle_json(bundle_for_status, closure_dir / "evidence_bundle.json")
    run_status = build_v1_5_run_evidence_status(
        run_dir=root,
        contract_json=contract_path,
        evidence_bundle_json=bundle_for_status_json,
        component=component,
    )
    run_status_json = _write_json(closure_dir / "v1_5_run_evidence_status.json", run_status)
    run_status_md = _write_markdown(
        closure_dir / "v1_5_run_evidence_status.md",
        render_v1_5_run_evidence_status_markdown(run_status),
    )
    capability = build_v1_5_calibration_capability(
        run_status=run_status,
        verification_csvs=capability_verification_csvs,
        candidate_csvs=capability_candidate_csvs,
        component=component,
        co2_limit_pct=co2_limit_pct,
        h2o_limit_pct=h2o_limit_pct,
    )
    capability_json = _write_json(closure_dir / "v1_5_calibration_capability.json", capability)
    capability_md = _write_markdown(
        closure_dir / "v1_5_calibration_capability.md",
        render_v1_5_calibration_capability_markdown(capability),
    )

    final_bundle = build_evidence_bundle(
        run_dir=root,
        plan_path=working_plan_path,
        pressure_reference_path=pressure_reference_path,
        component=component,
        analyzer_prefix=analyzer_prefix,
        require_quick_check_artifact=not bool(allow_pressure_fallback),
        pressure_check_path=pressure_check_path,
        today=today,
        artifact_exclude_dirs=_archive_dirs_to_exclude(root, keep=closure_dir),
    )
    final_bundle_json = write_bundle_json(final_bundle, closure_dir / "evidence_bundle.json")
    summary = bundle_summary(final_bundle)
    traceability = bundle_traceability_summary(final_bundle)
    identity_getco_traceability = _identity_getco_traceability_summary(root, closure_dir=closure_dir)
    traceability_checks = dict(traceability.get("traceability_checks") or {})
    traceability_checks["identity_getco_sn_device_code_traceability_ready"] = bool(
        identity_getco_traceability.get("ready_for_archive_release")
    )
    traceability_json = _write_json(closure_dir / "traceability_summary.json", traceability)

    db_mode_normalized = normalize_archive_db_mode(db_mode)
    if db_mode_normalized not in DB_MODE_CHOICES:
        raise ValueError("db_mode must be one of: skip, dry_run, import")
    database: Dict[str, Any] = {
        "mode": db_mode_normalized,
        "database_imported": False,
        "dsn": "",
        "migrations_applied": [],
        "import_result": {},
    }
    if db_mode_normalized == "import":
        if identity_getco_traceability.get("ready_for_archive_release") is not True:
            raise ValueError(
                "db_mode=import requires identity GETCO SN/device_code traceability to be ready; "
                f"status={identity_getco_traceability.get('status')}"
            )
        resolved_dsn = dsn or os.environ.get("GAS_CAL_DB_DSN", "")
        if not resolved_dsn:
            raise ValueError("db_mode=import requires --dsn or GAS_CAL_DB_DSN")
        database["dsn"] = mask_dsn(resolved_dsn)
        if apply_db_migrations:
            database["migrations_applied"] = apply_migrations(resolved_dsn)
        database["import_result"] = import_bundle(resolved_dsn, final_bundle)
        database["database_imported"] = True
    elif db_mode_normalized == "dry_run":
        database["reason"] = "database import intentionally skipped; final bundle is ready for import"
    else:
        database["reason"] = "database import skipped by caller"
    db_summary_json = _write_json(closure_dir / "database_import_summary.json", database)

    output_paths: Dict[str, Path] = {
        "initial_evidence_bundle": initial_bundle_json,
        "evidence_bundle": final_bundle_json,
        "traceability_summary": traceability_json,
        "database_import_summary": db_summary_json,
        "run_evidence_status_json": run_status_json,
        "run_evidence_status_markdown": run_status_md,
        "calibration_capability_json": capability_json,
        "calibration_capability_markdown": capability_md,
    }
    identity_getco_path = identity_getco_traceability.get("evidence_path")
    if identity_getco_path:
        output_paths["identity_getco_readiness"] = Path(str(identity_getco_path)).resolve()
    output_paths.update(traceability_snapshot_paths)
    output_paths.update({f"report_{key}": value for key, value in reports.items()})

    index_without_self = {
        "schema": SCHEMA,
        "created_at": _now(),
        "run_dir": str(root),
        "plan_json": str(working_plan_path),
        "source_plan_json": str(plan_path),
        "standard_gases_json": str(Path(standard_gases_json).resolve()) if standard_gases_json else "",
        "pressure_reference_json": str(pressure_reference_path),
        "contract_json": str(contract_path) if contract_path else "",
        "output_dir": str(closure_dir),
        "run_id": final_bundle.get("run_id"),
        "run_db_id": final_bundle.get("run_db_id"),
        "evidence_status": summary.get("evidence_status"),
        "package_status": summary.get("package_status"),
        "physical_boundaries": {
            "offline_archive_only": True,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "controls_valves_or_pace": False,
            "writes_coefficients": False,
            "not_real_acceptance_evidence": True,
        },
        "database": database,
        "identity_getco_traceability": identity_getco_traceability,
        "calibration_capability": {
            "json_path": str(capability_json),
            "markdown_path": str(capability_md),
            "capability_status": capability.get("capability_status"),
            "method_backbone_ready": capability.get("method_backbone_ready"),
            "formal_release_ready": capability.get("formal_release_ready"),
            "formal_release_blockers": capability.get("formal_release_blockers"),
        },
        "table_counts": summary.get("table_counts") or {},
        "traceability_checks": traceability_checks,
        "reports": {key: str(Path(value).resolve()) for key, value in reports.items()},
        "workflow_steps": _archive_workflow_steps(database, db_mode_normalized),
        "artifacts": _artifact_records(output_paths),
    }
    index_json = _write_json(closure_dir / "v1_5_formal_archive_closure_index.json", index_without_self)
    index_md = closure_dir / "v1_5_formal_archive_closure_index.md"
    final_index = dict(index_without_self)
    final_output_paths = dict(output_paths)
    final_output_paths["archive_index_json"] = index_json
    index_md.write_text(_render_markdown_zh(final_index), encoding="utf-8-sig")
    final_output_paths["archive_index_markdown"] = index_md
    final_index["artifacts"] = _artifact_records(final_output_paths)
    _write_json(index_json, final_index)
    index_md.write_text(_render_markdown_zh(final_index), encoding="utf-8-sig")

    return {
        "index": final_index,
        "paths": {key: Path(value).resolve() for key, value in final_output_paths.items()},
    }
