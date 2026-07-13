"""Audit P1 legacy evidence gaps within a bounded same-run lineage.

The audit reads the legacy evidence-gap task plan and inspects only the point's
parent run plus its direct sibling run directories. It may identify a retry or
recovery candidate, but it never copies files, derives QC, binds cross-run
quality, authorizes fitting, or changes the original failed attempt.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_legacy_evidence_gap_task_plan import SCHEMA as TASK_PLAN_SCHEMA


SCHEMA = "v1_5_p1_evidence_lineage_audit_v1"
_CO2_POINT_RE = re.compile(r"^p\d+_T(?P<temp>m?\d+)_(?P<ppm>\d+)ppm", re.IGNORECASE)
_RECOVERY_MARKER_RE = re.compile(r"(?:^|[_-])(retry\d*|direct|recovery)(?:[_-]|$)", re.IGNORECASE)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return payload


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "ok", "pass"}


def _task_plan_blockers(plan: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    if plan.get("schema") != TASK_PLAN_SCHEMA:
        reasons.append("task_plan_schema_mismatch")
    if plan.get("overall_status") != "review_required_manual_offline_evidence_tasks":
        reasons.append("task_plan_status_not_review_required")
    if plan.get("artifact_integrity_mismatch_count") != 0:
        reasons.append("task_plan_artifact_integrity_not_clean")
    false_locks = (
        "opens_com_ports",
        "controls_pressure",
        "controls_water_or_gas_routes",
        "writes_coefficients",
        "writes_sn_or_device_code",
        "connects_postgresql",
        "automatic_repair_allowed",
        "continuous_route_attestation_allowed",
        "historical_fit_allowed",
        "formal_release_allowed",
        "database_import_allowed",
    )
    for key in false_locks:
        if plan.get(key) is not False:
            reasons.append(f"task_plan_{key}_not_false")
    if plan.get("not_real_acceptance_evidence") is not True:
        reasons.append("task_plan_real_acceptance_lock_missing")
    return sorted(set(reasons))


def _physical_key(name: str) -> tuple[str, str] | None:
    match = _CO2_POINT_RE.match(name)
    if not match:
        return None
    return match.group("temp").lower(), match.group("ppm")


def _bounded_candidate_dirs(point_dir: Path) -> list[Path]:
    """Return depth-two point directories under one reviewed lineage root."""
    lineage_root = point_dir.parent.parent
    found: dict[str, Path] = {}
    if not lineage_root.is_dir():
        return []
    for run_dir in lineage_root.iterdir():
        if not run_dir.is_dir():
            continue
        for candidate in run_dir.iterdir():
            if candidate.is_dir():
                found[str(candidate.resolve()).casefold()] = candidate.resolve()
    return [found[key] for key in sorted(found)]


def _bounded_manifest_paths(point_dir: Path) -> list[Path]:
    lineage_root = point_dir.parent.parent
    found: dict[str, Path] = {}
    if not lineage_root.is_dir():
        return []
    for run_dir in lineage_root.iterdir():
        if not run_dir.is_dir():
            continue
        for manifest in run_dir.glob("queue_manifest*.csv"):
            if manifest.is_file():
                found[str(manifest.resolve()).casefold()] = manifest.resolve()
        for queue_dir in run_dir.iterdir():
            if not queue_dir.is_dir():
                continue
            for manifest in queue_dir.glob("queue_manifest*.csv"):
                if manifest.is_file():
                    found[str(manifest.resolve()).casefold()] = manifest.resolve()
    return [found[key] for key in sorted(found)]


def _route_timing(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        return _read_json(path)
    except (OSError, ValueError, json.JSONDecodeError):
        return {}


def _artifact_row(point_name: str, role: str, path: Path) -> dict[str, Any]:
    return {
        "point_name": point_name,
        "artifact_role": role,
        "artifact_path": str(path.resolve()),
        "size_bytes": path.stat().st_size,
        "sha256": _sha256(path),
    }


def _candidate_record(original: Path, candidate: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    sample = candidate / "samples_machine_readable.csv"
    frame_qc = candidate / "frame_quality_summary.csv"
    component_qc = candidate / "formal_open_flow_data_quality_by_analyzer.csv"
    sidecar = candidate / "formal_open_flow_sidecar_metadata.json"
    timing_path = candidate / "formal_open_flow_route_timing.json"
    timing = _route_timing(timing_path)
    same_physical = _physical_key(candidate.name) == _physical_key(original.name)
    recovery_marker = bool(_RECOVERY_MARKER_RE.search(candidate.name))
    lineage_root = original.parent.parent
    try:
        lineage_parts = candidate.relative_to(lineage_root).parts[:-1]
    except ValueError:
        lineage_parts = ()
    dry_run_source = any("dry_run" in part.casefold() for part in lineage_parts)
    sample_window_completed = (
        timing.get("sample_window_started_at") not in (None, "")
        and timing.get("sample_window_ended_at") not in (None, "")
        and timing.get("sampling_before_route_close") is True
    )
    core_candidate = bool(
        candidate.resolve() != original.resolve()
        and same_physical
        and recovery_marker
        and not dry_run_source
        and sample.is_file()
        and frame_qc.is_file()
        and sidecar.is_file()
        and sample_window_completed
    )
    artifacts: list[dict[str, Any]] = []
    for role, path in (
        ("samples", sample),
        ("frame_qc", frame_qc),
        ("component_qc", component_qc),
        ("sidecar", sidecar),
        ("route_timing", timing_path),
    ):
        if path.is_file():
            artifacts.append(_artifact_row(candidate.name, role, path))
    for io_path in sorted(candidate.glob("io_*.csv")):
        if io_path.is_file():
            artifacts.append(_artifact_row(candidate.name, "raw_io_diagnostic_only", io_path))
    return (
        {
            "original_point_name": original.name,
            "candidate_point_name": candidate.name,
            "candidate_point_dir": str(candidate.resolve()),
            "same_lineage": True,
            "same_physical_point": same_physical,
            "recovery_marker_present": recovery_marker,
            "source_run_classification": (
                "dry_run_reference_only" if dry_run_source else "same_lineage_real_attempt"
            ),
            "has_samples": sample.is_file(),
            "has_frame_qc": frame_qc.is_file(),
            "has_component_qc": component_qc.is_file(),
            "has_sidecar": sidecar.is_file(),
            "sample_window_completed": sample_window_completed,
            "core_evidence_recovery_candidate": core_candidate,
            "component_qc_still_required": core_candidate and not component_qc.is_file(),
            "cross_run_direct_bind_allowed": False,
            "formal_fit_allowed": False,
        },
        artifacts,
    )


def _manifest_records(point_dir: Path, physical_key: tuple[str, str] | None) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    records: list[dict[str, Any]] = []
    artifacts: list[dict[str, Any]] = []
    for manifest in _bounded_manifest_paths(point_dir):
        matched = False
        for row in _read_csv(manifest):
            point_name = str(row.get("point_run_id") or "")
            if point_name == point_dir.name or (physical_key and _physical_key(point_name) == physical_key):
                records.append(
                    {
                        "original_point_name": point_dir.name,
                        "manifest_path": str(manifest),
                        "manifest_point_name": point_name,
                        "status": str(row.get("status") or ""),
                        "returncode": str(row.get("returncode") or ""),
                        "failure_category": str(row.get("failure_category") or ""),
                        "failure_reason": str(row.get("failure_reason") or ""),
                        "quality_grade": str(row.get("quality_grade") or ""),
                        "manifest_record_classification": (
                            "dry_run_reference_only"
                            if str(row.get("status") or "").casefold() == "dry_run"
                            else "real_attempt_diagnostic_record"
                        ),
                        "real_physical_evidence_allowed": False,
                        "same_physical_point": _physical_key(point_name) == physical_key,
                        "formal_fit_allowed": False,
                    }
                )
                matched = True
        if matched:
            artifacts.append(_artifact_row(point_dir.name, "queue_manifest", manifest))
    return records, artifacts


def _point_result(task: Mapping[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    point_dir = Path(str(task.get("point_dir") or "")).resolve()
    physical = _physical_key(point_dir.name)
    candidates: list[dict[str, Any]] = []
    artifacts: list[dict[str, Any]] = []
    for candidate_dir in _bounded_candidate_dirs(point_dir):
        if _physical_key(candidate_dir.name) != physical:
            continue
        candidate, candidate_artifacts = _candidate_record(point_dir, candidate_dir)
        candidates.append(candidate)
        artifacts.extend(candidate_artifacts)
    manifest_records, manifest_artifacts = _manifest_records(point_dir, physical)
    artifacts.extend(manifest_artifacts)
    alternatives = [row for row in candidates if row["core_evidence_recovery_candidate"]]
    original_timing = _route_timing(point_dir / "formal_open_flow_route_timing.json")
    io_files = sorted(point_dir.glob("io_*.csv")) if point_dir.is_dir() else []
    if alternatives:
        conclusion = "core_gap_resolved_by_same_lineage_retry_reference"
        next_action = "use_retry_as_explicit_diagnostic_candidate_then_run_separate_component_qc_review"
    else:
        conclusion = "unrecoverable_from_reviewed_lineage"
        next_action = "retain_failed_attempt_as_raw_diagnostic_only_do_not_borrow_cross_run_samples"
    result = {
        "task_id": str(task.get("task_id") or ""),
        "point_name": point_dir.name,
        "point_dir": str(point_dir),
        "lineage_root": str(point_dir.parent.parent),
        "route_kind": str(task.get("route_kind") or ""),
        "audit_conclusion": conclusion,
        "next_action": next_action,
        "candidate_count": len(candidates),
        "core_evidence_recovery_candidate_count": len(alternatives),
        "original_attempt_has_samples": (point_dir / "samples_machine_readable.csv").is_file(),
        "original_attempt_io_file_count": len(io_files),
        "original_sample_window_started": original_timing.get("sample_window_started_at") not in (None, ""),
        "original_sampling_before_route_close": original_timing.get("sampling_before_route_close") is True,
        "manifest_failure_categories": sorted(
            {str(row["failure_category"]) for row in manifest_records if row["failure_category"]}
        ),
        "same_lineage_only": True,
        "cross_run_search_performed": False,
        "cross_run_direct_bind_allowed": False,
        "automatic_file_copy_allowed": False,
        "automatic_qc_derivation_allowed": False,
        "continuous_route_attestation_allowed": False,
        "formal_fit_allowed": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
    }
    return result, candidates, manifest_records + artifacts


def _safety_locks() -> dict[str, bool]:
    return {
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_sn_or_device_code": False,
        "connects_postgresql": False,
        "automatic_file_copy_allowed": False,
        "automatic_qc_derivation_allowed": False,
        "continuous_route_attestation_allowed": False,
        "historical_fit_allowed": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
    }


def build_v1_5_p1_evidence_lineage_audit(*, task_plan_json_path: str | Path) -> dict[str, Any]:
    task_path = Path(task_plan_json_path).resolve()
    plan = _read_json(task_path)
    blockers = _task_plan_blockers(plan)
    if blockers:
        return {
            "schema": SCHEMA,
            "generated_at": _now(),
            "overall_status": "blocked_invalid_task_plan",
            "task_plan_blocker_codes": blockers,
            "point_count": 0,
            "points": [],
            "candidates": [],
            "manifest_records": [],
            "artifact_inventory": [],
            **_safety_locks(),
        }
    p1_tasks = [row for row in plan.get("tasks") or [] if row.get("priority") == "P1_core_evidence"]
    points: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    manifest_records: list[dict[str, Any]] = []
    artifact_inventory: list[dict[str, Any]] = []
    for task in p1_tasks:
        result, point_candidates, combined_records = _point_result(task)
        points.append(result)
        candidates.extend(point_candidates)
        for row in combined_records:
            if "artifact_role" in row:
                artifact_inventory.append(row)
            else:
                manifest_records.append(row)
    conclusion_counts = Counter(str(row["audit_conclusion"]) for row in points)
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": "review_required_p1_lineage_audit_complete",
        "task_plan_path": str(task_path),
        "task_plan_sha256": _sha256(task_path),
        "task_plan_blocker_codes": [],
        "point_count": len(points),
        "recoverable_reference_count": conclusion_counts[
            "core_gap_resolved_by_same_lineage_retry_reference"
        ],
        "unrecoverable_count": conclusion_counts["unrecoverable_from_reviewed_lineage"],
        "conclusion_counts": dict(sorted(conclusion_counts.items())),
        "points": points,
        "candidates": candidates,
        "manifest_records": manifest_records,
        "artifact_inventory": artifact_inventory,
        **_safety_locks(),
    }


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields: list[str] = []
    flattened: list[dict[str, Any]] = []
    for source in rows:
        row = dict(source)
        for key, value in list(row.items()):
            if isinstance(value, (dict, list)):
                row[key] = json.dumps(value, ensure_ascii=False, sort_keys=True)
        flattened.append(row)
        for key in row:
            if key not in fields:
                fields.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields or ["empty"])
        writer.writeheader()
        writer.writerows(flattened)


def write_v1_5_p1_evidence_lineage_audit(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    outputs = {
        "json": out / "v1_5_p1_evidence_lineage_audit.json",
        "points_csv": out / "v1_5_p1_evidence_lineage_points.csv",
        "candidates_csv": out / "v1_5_p1_evidence_lineage_candidates.csv",
        "manifest_csv": out / "v1_5_p1_evidence_lineage_manifest_records.csv",
        "artifacts_csv": out / "v1_5_p1_evidence_lineage_artifacts.csv",
        "markdown": out / "V1_5_P1_EVIDENCE_LINEAGE_AUDIT.md",
    }
    outputs["json"].write_text(
        json.dumps(dict(model), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    _write_csv(outputs["points_csv"], model.get("points") or [])
    _write_csv(outputs["candidates_csv"], model.get("candidates") or [])
    _write_csv(outputs["manifest_csv"], model.get("manifest_records") or [])
    _write_csv(outputs["artifacts_csv"], model.get("artifact_inventory") or [])
    lines = [
        "# V1.5 P1 Evidence Lineage Audit",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- point_count: `{model.get('point_count')}`",
        f"- recoverable_reference_count: `{model.get('recoverable_reference_count')}`",
        f"- unrecoverable_count: `{model.get('unrecoverable_count')}`",
        "- same_lineage_only: `true`",
        "- cross_run_direct_bind_allowed: `false`",
        "- automatic_file_copy_allowed: `false`",
        "- automatic_qc_derivation_allowed: `false`",
        "- historical_fit_allowed: `false`",
        "- offline_only: `true`",
        "",
        "| Point | Conclusion | Next action |",
        "| --- | --- | --- |",
    ]
    for row in model.get("points") or []:
        lines.append(
            f"| `{row.get('point_name')}` | `{row.get('audit_conclusion')}` | `{row.get('next_action')}` |"
        )
    outputs["markdown"].write_text("\n".join(lines) + "\n", encoding="utf-8")
    return outputs


__all__ = [
    "SCHEMA",
    "build_v1_5_p1_evidence_lineage_audit",
    "write_v1_5_p1_evidence_lineage_audit",
]
