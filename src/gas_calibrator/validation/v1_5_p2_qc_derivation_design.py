"""Classify legacy P2 points for a future component-QC derivation review.

This module validates same-point input structure only. It does not calculate
quality grades, create QC files, authorize fitting, or use 0624/migration QC as
the mature 0613/0620/0621 grading authority.
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
from .v1_5_p1_evidence_lineage_audit import SCHEMA as P1_AUDIT_SCHEMA


SCHEMA = "v1_5_p2_qc_derivation_design_v1"
_PREFIX_RE = re.compile(r"^(ga\d+)_analyzer_device_id$", re.IGNORECASE)


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


def _read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), [dict(row) for row in reader]


def _boolish(value: Any) -> bool | None:
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "y", "pass", "ok"}:
        return True
    if text in {"0", "false", "no", "n", "fail", "blocked"}:
        return False
    return None


def _number(value: Any) -> float | None:
    try:
        return float(str(value)) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _upstream_blockers(task_plan: Mapping[str, Any], p1_audit: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    if task_plan.get("schema") != TASK_PLAN_SCHEMA:
        reasons.append("task_plan_schema_mismatch")
    if task_plan.get("overall_status") != "review_required_manual_offline_evidence_tasks":
        reasons.append("task_plan_status_invalid")
    if task_plan.get("artifact_integrity_mismatch_count") != 0:
        reasons.append("task_plan_artifact_integrity_not_clean")
    if p1_audit.get("schema") != P1_AUDIT_SCHEMA:
        reasons.append("p1_audit_schema_mismatch")
    if p1_audit.get("overall_status") != "review_required_p1_lineage_audit_complete":
        reasons.append("p1_audit_status_invalid")
    false_locks = (
        "opens_com_ports",
        "controls_pressure",
        "controls_water_or_gas_routes",
        "writes_coefficients",
        "writes_sn_or_device_code",
        "connects_postgresql",
        "historical_fit_allowed",
        "formal_release_allowed",
        "database_import_allowed",
    )
    for source_name, payload in (("task_plan", task_plan), ("p1_audit", p1_audit)):
        for key in false_locks:
            if payload.get(key) is not False:
                reasons.append(f"{source_name}_{key}_not_false")
        if payload.get("not_real_acceptance_evidence") is not True:
            reasons.append(f"{source_name}_real_acceptance_lock_missing")
    if task_plan.get("automatic_repair_allowed") is not False:
        reasons.append("task_plan_automatic_repair_allowed_not_false")
    for key in ("automatic_file_copy_allowed", "automatic_qc_derivation_allowed"):
        if p1_audit.get(key) is not False:
            reasons.append(f"p1_audit_{key}_not_false")
    return sorted(set(reasons))


def _artifact(role: str, path: Path, point_dir: Path) -> dict[str, Any]:
    return {
        "point_dir": str(point_dir),
        "artifact_role": role,
        "artifact_path": str(path.resolve()),
        "size_bytes": path.stat().st_size,
        "sha256": _sha256(path),
    }


def _candidate_sources(
    task_plan: Mapping[str, Any], p1_audit: Mapping[str, Any]
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for task in task_plan.get("tasks") or []:
        if task.get("priority") != "P2_quality_traceability":
            continue
        candidates.append(
            {
                "source_role": "p2_catalog_point",
                "route_kind": str(task.get("route_kind") or ""),
                "point_name": str(task.get("point_name") or ""),
                "point_dir": str(task.get("point_dir") or ""),
                "accepted_manifest_warning": str(task.get("accepted_manifest_warning") or ""),
            }
        )
    route_by_original = {
        str(row.get("point_name") or ""): str(row.get("route_kind") or "")
        for row in p1_audit.get("points") or []
    }
    for row in p1_audit.get("candidates") or []:
        if row.get("core_evidence_recovery_candidate") is not True:
            continue
        if row.get("component_qc_still_required") is not True:
            continue
        original = str(row.get("original_point_name") or "")
        candidates.append(
            {
                "source_role": "p1_same_lineage_retry_reference",
                "route_kind": route_by_original.get(original, ""),
                "point_name": str(row.get("candidate_point_name") or ""),
                "point_dir": str(row.get("candidate_point_dir") or ""),
                "accepted_manifest_warning": "",
            }
        )
    unique: dict[str, dict[str, Any]] = {}
    for row in candidates:
        unique[str(Path(row["point_dir"]).resolve()).casefold()] = row
    return [unique[key] for key in sorted(unique)]


def _route_files(point_dir: Path, route_kind: str) -> dict[str, Path]:
    common = {
        "samples": point_dir / "samples_machine_readable.csv",
        "frame_qc": point_dir / "frame_quality_summary.csv",
        "runtime_config": point_dir / "runtime_config_snapshot.json",
    }
    if route_kind == "co2":
        common.update(
            {
                "sidecar": point_dir / "formal_open_flow_sidecar_metadata.json",
                "route_timing": point_dir / "formal_open_flow_route_timing.json",
            }
        )
    else:
        common.update(
            {
                "sidecar": point_dir / "formal_h2o_open_flow_sidecar_metadata.json",
                "hgen_flow_set": point_dir / "formal_h2o_open_flow_hgen_flow_set.json",
                "humidity_reference_review": point_dir / "h2o_humidity_reference_review.json",
                "point_timing_summary": point_dir / "point_timing_summary.csv",
            }
        )
    return common


def _analyzer_prefixes(sample_fields: Sequence[str]) -> list[str]:
    return sorted(
        {
            match.group(1).lower()
            for field in sample_fields
            if (match := _PREFIX_RE.match(field)) is not None
        }
    )


def _classify_candidate(source: Mapping[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    point_dir = Path(str(source.get("point_dir") or "")).resolve()
    route_kind = str(source.get("route_kind") or "")
    files = _route_files(point_dir, route_kind)
    reasons = [f"missing_{role}" for role, path in files.items() if not path.is_file()]
    artifacts = [
        _artifact(role, path, point_dir) for role, path in files.items() if path.is_file()
    ]
    sample_fields: list[str] = []
    sample_rows: list[dict[str, str]] = []
    frame_fields: list[str] = []
    frame_rows: list[dict[str, str]] = []
    if files["samples"].is_file():
        sample_fields, sample_rows = _read_csv(files["samples"])
    if files["frame_qc"].is_file():
        frame_fields, frame_rows = _read_csv(files["frame_qc"])
    prefixes = _analyzer_prefixes(sample_fields)
    frame_prefixes = sorted(
        {
            str(row.get("Analyzer") or "").strip().lower()
            for row in frame_rows
            if str(row.get("Analyzer") or "").strip()
        }
    )
    for field in ("sample_index", "point_phase", "sample_alignment_ok", "point_quality_blocked"):
        if field not in sample_fields:
            reasons.append(f"sample_column_missing:{field}")
    for field in ("Analyzer", "AnalyzerId", "TotalFrames", "ValidFrames", "ValidRatio"):
        if field not in frame_fields:
            reasons.append(f"frame_qc_column_missing:{field}")
    if not sample_rows:
        reasons.append("sample_rows_empty")
    if not frame_rows:
        reasons.append("frame_qc_rows_empty")
    if not prefixes:
        reasons.append("sample_analyzer_prefixes_missing")
    if prefixes != frame_prefixes:
        reasons.append("sample_frame_analyzer_prefix_mismatch")
    ratio_suffix = "co2_ratio_f" if route_kind == "co2" else "h2o_ratio_f"
    for prefix in prefixes:
        for field in (
            f"{prefix}_analyzer_device_id",
            f"{prefix}_frame_usable",
            f"{prefix}_{ratio_suffix}",
        ):
            if field not in sample_fields:
                reasons.append(f"sample_column_missing:{field}")
    if any(not str(row.get("AnalyzerId") or "").strip() for row in frame_rows):
        reasons.append("frame_qc_analyzer_id_missing")
    if any((_number(row.get("TotalFrames")) or 0) <= 0 for row in frame_rows):
        reasons.append("frame_qc_total_frames_not_positive")
    sidecar = _read_json(files["sidecar"]) if files.get("sidecar", Path()).is_file() else {}
    if sidecar:
        if sidecar.get("writes_senco") is not False or sidecar.get("writes_device_id") is not False:
            reasons.append("sidecar_no_write_boundary_invalid")
        if sidecar.get("route_open_until_sample_end") is not True:
            reasons.append("sidecar_route_open_until_sample_end_missing")
        if _number(sidecar.get("formal_sample_anchor_interval_s")) != 1.0:
            reasons.append("sidecar_sample_anchor_not_1s")
    if route_kind == "co2" and files.get("route_timing", Path()).is_file():
        timing = _read_json(files["route_timing"])
        if timing.get("sampling_before_route_close") is not True:
            reasons.append("co2_sampling_before_route_close_not_true")
    input_complete = not reasons
    alignment_false_count = sum(
        1 for row in sample_rows if _boolish(row.get("sample_alignment_ok")) is False
    )
    point_blocked_count = sum(
        1 for row in sample_rows if _boolish(row.get("point_quality_blocked")) is True
    )
    purge_actual = _number(sidecar.get("actual_purge_s"))
    purge_minimum = _number(sidecar.get("minimum_purge_s"))
    purge_below_declared_minimum = bool(
        purge_actual is not None and purge_minimum is not None and purge_actual < purge_minimum
    )
    warning = str(source.get("accepted_manifest_warning") or "")
    manual_gate_review = bool(
        warning or alignment_false_count or point_blocked_count or purge_below_declared_minimum
    )
    if not input_complete:
        classification = "input_incomplete"
    elif manual_gate_review:
        classification = "input_complete_manual_gate_review_generator_missing"
    else:
        classification = "input_complete_generator_contract_missing"
    return (
        {
            **dict(source),
            "point_dir": str(point_dir),
            "classification": classification,
            "input_complete": input_complete,
            "input_gap_codes": sorted(set(reasons)),
            "sample_row_count": len(sample_rows),
            "frame_qc_row_count": len(frame_rows),
            "analyzer_prefixes": prefixes,
            "frame_analyzer_prefixes": frame_prefixes,
            "analyzer_prefix_sets_match": prefixes == frame_prefixes and bool(prefixes),
            "sample_alignment_false_count": alignment_false_count,
            "point_quality_blocked_count": point_blocked_count,
            "purge_below_declared_minimum": purge_below_declared_minimum,
            "manual_gate_review_required": manual_gate_review,
            "reviewed_generator_available": False,
            "derivation_design_review_candidate": input_complete,
            "qc_derivation_execution_allowed": False,
            "generated_qc_write_allowed": False,
            "cross_run_qc_direct_bind_allowed": False,
            "formal_fit_allowed": False,
            "formal_release_allowed": False,
            "database_import_allowed": False,
        },
        artifacts,
    )


def _safety_locks() -> dict[str, bool]:
    return {
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_sn_or_device_code": False,
        "connects_postgresql": False,
        "reviewed_generator_available": False,
        "qc_derivation_execution_allowed": False,
        "generated_qc_write_allowed": False,
        "cross_run_qc_direct_bind_allowed": False,
        "historical_fit_allowed": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
    }


def build_v1_5_p2_qc_derivation_design(
    *, task_plan_json_path: str | Path, p1_audit_json_path: str | Path
) -> dict[str, Any]:
    task_path = Path(task_plan_json_path).resolve()
    audit_path = Path(p1_audit_json_path).resolve()
    task_plan = _read_json(task_path)
    p1_audit = _read_json(audit_path)
    blockers = _upstream_blockers(task_plan, p1_audit)
    if blockers:
        return {
            "schema": SCHEMA,
            "generated_at": _now(),
            "overall_status": "blocked_invalid_upstream_evidence",
            "upstream_blocker_codes": blockers,
            "candidate_count": 0,
            "candidates": [],
            "artifact_inventory": [],
            **_safety_locks(),
        }
    p2_source_reference_count = sum(
        1
        for task in task_plan.get("tasks") or []
        if task.get("priority") == "P2_quality_traceability"
    )
    p1_recovery_source_reference_count = sum(
        1
        for row in p1_audit.get("candidates") or []
        if row.get("core_evidence_recovery_candidate") is True
        and row.get("component_qc_still_required") is True
    )
    source_reference_count = p2_source_reference_count + p1_recovery_source_reference_count
    candidates: list[dict[str, Any]] = []
    artifacts: list[dict[str, Any]] = []
    for source in _candidate_sources(task_plan, p1_audit):
        candidate, candidate_artifacts = _classify_candidate(source)
        candidates.append(candidate)
        artifacts.extend(candidate_artifacts)
    class_counts = Counter(str(row["classification"]) for row in candidates)
    route_counts = Counter(str(row["route_kind"]) for row in candidates)
    source_role_counts = Counter(str(row["source_role"]) for row in candidates)
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": "blocked_missing_reviewed_qc_generator_contract",
        "upstream_blocker_codes": [],
        "task_plan_path": str(task_path),
        "task_plan_sha256": _sha256(task_path),
        "p1_audit_path": str(audit_path),
        "p1_audit_sha256": _sha256(audit_path),
        "candidate_count": len(candidates),
        "p2_source_reference_count": p2_source_reference_count,
        "p1_recovery_source_reference_count": p1_recovery_source_reference_count,
        "source_reference_count": source_reference_count,
        "duplicate_source_reference_count": source_reference_count - len(candidates),
        "source_role_counts": dict(sorted(source_role_counts.items())),
        "co2_candidate_count": route_counts["co2"],
        "h2o_candidate_count": route_counts["h2o"],
        "input_complete_count": sum(1 for row in candidates if row["input_complete"]),
        "input_incomplete_count": sum(1 for row in candidates if not row["input_complete"]),
        "manual_gate_review_count": sum(
            1 for row in candidates if row["manual_gate_review_required"]
        ),
        "classification_counts": dict(sorted(class_counts.items())),
        "generator_contract": {
            "status": "missing_reviewed_0613_0620_0621_component_qc_generator",
            "0624_component_qc_files_are_threshold_authority": False,
            "observed_output_schema_is_reference_only": True,
            "future_writer_must_hash_all_same_point_inputs": True,
            "future_writer_must_emit_per_analyzer_reason_and_fit_flags": True,
            "future_writer_must_not_change_original_samples_or_frame_qc": True,
        },
        "candidates": candidates,
        "artifact_inventory": artifacts,
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


def write_v1_5_p2_qc_derivation_design(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    outputs = {
        "json": out / "v1_5_p2_qc_derivation_design.json",
        "candidates_csv": out / "v1_5_p2_qc_derivation_candidates.csv",
        "artifacts_csv": out / "v1_5_p2_qc_derivation_artifacts.csv",
        "summary_csv": out / "v1_5_p2_qc_derivation_summary.csv",
        "markdown": out / "V1_5_P2_QC_DERIVATION_DESIGN.md",
    }
    json_payload = dict(model)
    json_payload.pop("artifact_inventory", None)
    json_payload["artifact_inventory_csv"] = outputs["artifacts_csv"].name
    outputs["json"].write_text(
        json.dumps(json_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    _write_csv(outputs["candidates_csv"], model.get("candidates") or [])
    _write_csv(outputs["artifacts_csv"], model.get("artifact_inventory") or [])
    summary = [
        {"metric": "overall_status", "value": model.get("overall_status")},
        {"metric": "candidate_count", "value": model.get("candidate_count")},
        {"metric": "source_reference_count", "value": model.get("source_reference_count")},
        {
            "metric": "duplicate_source_reference_count",
            "value": model.get("duplicate_source_reference_count"),
        },
        {"metric": "input_complete_count", "value": model.get("input_complete_count")},
        {"metric": "input_incomplete_count", "value": model.get("input_incomplete_count")},
        {"metric": "manual_gate_review_count", "value": model.get("manual_gate_review_count")},
    ]
    summary.extend(
        {"metric": f"classification:{key}", "value": value}
        for key, value in (model.get("classification_counts") or {}).items()
    )
    _write_csv(outputs["summary_csv"], summary)
    lines = [
        "# V1.5 P2 QC Derivation Design",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- candidate_count: `{model.get('candidate_count')}`",
        f"- source_reference_count: `{model.get('source_reference_count')}`",
        f"- duplicate_source_reference_count: `{model.get('duplicate_source_reference_count')}`",
        f"- CO2 / H2O: `{model.get('co2_candidate_count')} / {model.get('h2o_candidate_count')}`",
        f"- input_complete_count: `{model.get('input_complete_count')}`",
        f"- input_incomplete_count: `{model.get('input_incomplete_count')}`",
        f"- manual_gate_review_count: `{model.get('manual_gate_review_count')}`",
        "- reviewed_generator_available: `false`",
        "- qc_derivation_execution_allowed: `false`",
        "- generated_qc_write_allowed: `false`",
        "- historical_fit_allowed: `false`",
        "- offline_only: `true`",
        "",
        "Inputs may be structurally complete, but no reviewed 0613/0620/0621 component-QC generator exists in the repository.",
        "Observed 0624 QC files are schema references only and cannot supply mature grading thresholds.",
    ]
    outputs["markdown"].write_text("\n".join(lines) + "\n", encoding="utf-8")
    return outputs


__all__ = [
    "SCHEMA",
    "build_v1_5_p2_qc_derivation_design",
    "write_v1_5_p2_qc_derivation_design",
]
