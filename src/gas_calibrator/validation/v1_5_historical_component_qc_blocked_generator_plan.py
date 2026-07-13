"""Build a blocked, no-evaluation plan for historical V1.5 component QC."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_historical_component_qc_generator_preflight import (
    SCHEMA as PREFLIGHT_SCHEMA,
)


SCHEMA = "v1_5_historical_component_qc_blocked_generator_plan_v1"
READY_STATUS = "ready_for_historical_component_qc_blocked_generator_plan_review"
BLOCKED_STATUS = "blocked_historical_component_qc_generator_plan"
EXPECTED_PREFLIGHT_STATUS = (
    "ready_for_historical_component_qc_generator_preflight_manual_review"
)
REVIEW_OUTPUT_SUFFIX = (
    "docs",
    "v1_5_flow_contract",
    "historical_component_qc_blocked_generator_plan",
)

_FALSE_LOCKS = (
    "production_component_qc_generator_available",
    "historical_component_qc_generation_allowed",
    "historical_component_qc_write_allowed",
    "component_qc_backfill_allowed",
    "historical_fit_allowed",
    "formal_release_allowed",
    "database_import_allowed",
    "opens_com_ports",
    "controls_pressure",
    "controls_water_or_gas_routes",
    "writes_coefficients",
    "writes_sn_or_device_code",
    "connects_postgresql",
)
_SOURCE_HASH_FIELDS = {
    "p2_design_json": "p2_design_sha256",
    "p2_artifact_inventory_csv": "p2_artifact_inventory_sha256",
    "contract_json": "contract_file_sha256",
    "reference_evaluation_json": "reference_evaluation_sha256",
}
_OUTPUT_FILENAME = "formal_open_flow_data_quality_by_analyzer.csv"


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return payload


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_value(value: Any) -> str:
    raw = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _path_key(value: Any) -> str:
    raw = str(value or "").strip()
    return str(Path(raw).resolve()).casefold() if raw else ""


def _global_reasons(preflight: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    if preflight.get("schema") != PREFLIGHT_SCHEMA:
        reasons.append("preflight_schema_mismatch")
    if preflight.get("overall_status") != EXPECTED_PREFLIGHT_STATUS:
        reasons.append("preflight_status_not_ready_for_manual_review")
    if preflight.get("production_state") != "preflight_only_generator_and_writer_blocked":
        reasons.append("preflight_production_state_invalid")
    if preflight.get("global_blocker_codes") not in ([], ()):
        reasons.append("preflight_global_blockers_present")
    candidates = preflight.get("candidates")
    checks = preflight.get("artifact_checks")
    if not isinstance(candidates, list) or not candidates:
        reasons.append("preflight_candidates_missing_or_empty")
    elif preflight.get("candidate_count") != len(candidates):
        reasons.append("preflight_candidate_count_mismatch")
    if not isinstance(checks, list) or not checks:
        reasons.append("preflight_artifact_checks_missing_or_empty")
    elif preflight.get("artifact_check_count") != len(checks):
        reasons.append("preflight_artifact_check_count_mismatch")
    if preflight.get("candidate_blocked_count") != 0:
        reasons.append("preflight_candidate_blockers_present")
    if isinstance(candidates, list) and preflight.get("candidate_preflight_ready_count") != len(
        candidates
    ):
        reasons.append("preflight_not_all_candidates_ready")
    if isinstance(candidates, list) and preflight.get("manual_gate_review_count") != len(candidates):
        reasons.append("preflight_manual_gate_count_mismatch")
    if preflight.get("artifact_check_blocked_count") != 0:
        reasons.append("preflight_artifact_blockers_present")
    locks = preflight.get("locks")
    if not isinstance(locks, Mapping):
        reasons.append("preflight_locks_missing")
    else:
        if locks.get("preflight_available") is not True:
            reasons.append("preflight_available_flag_missing")
        for key in _FALSE_LOCKS:
            if locks.get(key) is not False:
                reasons.append(f"preflight_lock_not_false:{key}")
    if preflight.get("evidence_source") != "historical_replay":
        reasons.append("preflight_evidence_source_invalid")
    if preflight.get("not_real_acceptance_evidence") is not True:
        reasons.append("preflight_real_acceptance_lock_missing")
    return reasons


def _source_reasons(source_paths: Mapping[str, Any]) -> tuple[list[str], list[dict[str, Any]]]:
    reasons: list[str] = []
    checks: list[dict[str, Any]] = []
    for path_key, hash_key in _SOURCE_HASH_FIELDS.items():
        path = Path(str(source_paths.get(path_key) or "")).resolve()
        expected_sha = str(source_paths.get(hash_key) or "").lower()
        row_reasons: list[str] = []
        actual_sha: str | None = None
        if not str(source_paths.get(path_key) or "").strip():
            row_reasons.append("source_path_missing")
        elif not path.is_file():
            row_reasons.append("source_file_missing")
        else:
            actual_sha = _sha256_file(path)
            if not expected_sha:
                row_reasons.append("source_recorded_sha256_missing")
            elif actual_sha != expected_sha:
                row_reasons.append("source_sha256_mismatch")
        reasons.extend(f"{path_key}:{reason}" for reason in row_reasons)
        checks.append(
            {
                "source_role": path_key,
                "path": str(path),
                "recorded_sha256": expected_sha,
                "actual_sha256": actual_sha,
                "status": "pass" if not row_reasons else "blocked",
                "blocker_codes": sorted(set(row_reasons)),
            }
        )
    return reasons, checks


def _artifact_groups(
    rows: Sequence[Any],
) -> tuple[dict[str, list[Mapping[str, Any]]], list[str]]:
    groups: dict[str, list[Mapping[str, Any]]] = {}
    reasons: list[str] = []
    for index, row in enumerate(rows, start=1):
        if not isinstance(row, Mapping):
            reasons.append(f"preflight_artifact_check_not_object:row_{index}")
            continue
        point_key = _path_key(row.get("point_dir"))
        if not point_key:
            reasons.append(f"preflight_artifact_check_point_dir_missing:row_{index}")
            continue
        groups.setdefault(point_key, []).append(row)
    return groups, reasons


def _candidate_review(
    candidate: Mapping[str, Any], artifact_rows: Sequence[Mapping[str, Any]]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    reasons: list[str] = []
    point_dir = Path(str(candidate.get("point_dir") or "")).resolve()
    point_key = _path_key(point_dir)
    route_kind = str(candidate.get("route_kind") or "").strip().lower()
    target = Path(str(candidate.get("planned_output_path") or "")).resolve()
    expected_target = point_dir / _OUTPUT_FILENAME
    if not str(candidate.get("point_dir") or "").strip():
        reasons.append("candidate_point_dir_missing")
    if route_kind not in {"co2", "h2o"}:
        reasons.append("candidate_route_kind_invalid")
    if candidate.get("preflight_ready") is not True:
        reasons.append("candidate_preflight_ready_not_true")
    if candidate.get("preflight_status") != "input_packet_ready_for_manual_review":
        reasons.append("candidate_preflight_status_invalid")
    if candidate.get("blocker_codes") not in ([], ()):
        reasons.append("candidate_preflight_blockers_present")
    if candidate.get("manual_gate_review_required") is not True:
        reasons.append("candidate_manual_gate_missing")
    for key in (
        "historical_component_qc_generation_allowed",
        "historical_component_qc_write_allowed",
        "formal_fit_allowed",
    ):
        if candidate.get(key) is not False:
            reasons.append(f"candidate_lock_not_false:{key}")
    if target != expected_target.resolve():
        reasons.append("candidate_planned_output_path_invalid")
    if candidate.get("planned_output_exists") is not False:
        reasons.append("candidate_preflight_output_exists_flag_not_false")
    if target.exists():
        reasons.append("component_qc_output_target_now_exists")
    if not artifact_rows:
        reasons.append("candidate_artifact_checks_missing")

    source_rows: list[dict[str, Any]] = []
    seen_roles: set[str] = set()
    for index, row in enumerate(artifact_rows, start=1):
        role = str(row.get("role") or "").strip()
        path = Path(str(row.get("path") or "")).resolve()
        row_reasons: list[str] = []
        if not role:
            row_reasons.append("artifact_role_missing")
        elif role in seen_roles:
            row_reasons.append("artifact_role_duplicate")
        seen_roles.add(role)
        if _path_key(row.get("point_dir")) != point_key:
            row_reasons.append("artifact_point_dir_mismatch")
        try:
            path.relative_to(point_dir)
        except ValueError:
            row_reasons.append("artifact_path_outside_point_dir")
        actual_sha: str | None = None
        actual_size: int | None = None
        if not path.is_file():
            row_reasons.append("artifact_file_missing")
        else:
            actual_size = path.stat().st_size
            actual_sha = _sha256_file(path)
            if actual_size != row.get("actual_size_bytes"):
                row_reasons.append("artifact_size_drift_after_preflight")
            if actual_sha != row.get("actual_sha256"):
                row_reasons.append("artifact_sha256_drift_after_preflight")
        if row.get("status") != "pass" or row.get("blocker_codes") not in ([], ()):
            row_reasons.append("artifact_preflight_status_not_pass")
        reasons.extend(f"{role or index}:{reason}" for reason in row_reasons)
        source_rows.append(
            {
                "role": role,
                "path": str(path),
                "size_bytes": actual_size,
                "sha256": actual_sha,
                "status": "pass" if not row_reasons else "blocked",
                "blocker_codes": sorted(set(row_reasons)),
            }
        )
    source_packet = [
        {
            "role": row["role"],
            "path": row["path"],
            "size_bytes": row["size_bytes"],
            "sha256": row["sha256"],
        }
        for row in sorted(source_rows, key=lambda value: (value["role"], value["path"]))
    ]
    reasons = sorted(set(reasons))
    review = {
        "source_role": candidate.get("source_role"),
        "route_kind": route_kind,
        "point_name": candidate.get("point_name"),
        "point_dir": str(point_dir),
        "planned_output_path": str(target),
        "source_artifact_count": len(source_rows),
        "source_packet_sha256": _sha256_value(source_packet),
        "plan_candidate_ready": not reasons,
        "blocker_codes": reasons,
        "manual_gate_review_required": True,
        "operation_role": "review_only_would_write_preview",
        "would_evaluate": False,
        "would_derive_grades": False,
        "would_write": False,
        "overwrite_allowed": False,
        "requires_distinct_authorization": True,
        "formal_fit_allowed": False,
    }
    return review, source_rows


def build_v1_5_historical_component_qc_blocked_generator_plan(
    *, preflight_json_path: str | Path
) -> dict[str, Any]:
    """Revalidate a preflight and produce only a blocked would-write preview."""

    preflight_path = Path(preflight_json_path).resolve()
    preflight = _read_json(preflight_path)
    global_reasons = _global_reasons(preflight)
    source_paths = preflight.get("source_paths")
    if not isinstance(source_paths, Mapping):
        global_reasons.append("preflight_source_paths_missing")
        source_checks: list[dict[str, Any]] = []
    else:
        source_reasons, source_checks = _source_reasons(source_paths)
        global_reasons.extend(source_reasons)

    raw_artifact_checks = preflight.get("artifact_checks")
    artifact_rows = raw_artifact_checks if isinstance(raw_artifact_checks, list) else []
    groups, group_reasons = _artifact_groups(artifact_rows)
    global_reasons.extend(group_reasons)

    candidate_reviews: list[dict[str, Any]] = []
    source_artifact_checks: list[dict[str, Any]] = []
    seen_points: set[str] = set()
    raw_candidates = preflight.get("candidates")
    candidates = raw_candidates if isinstance(raw_candidates, list) else []
    for index, candidate in enumerate(candidates, start=1):
        if not isinstance(candidate, Mapping):
            global_reasons.append(f"preflight_candidate_not_object:row_{index}")
            continue
        point_key = _path_key(candidate.get("point_dir"))
        if point_key in seen_points:
            global_reasons.append(f"duplicate_preflight_candidate_point_dir:{point_key}")
        seen_points.add(point_key)
        review, checks = _candidate_review(candidate, groups.get(point_key, []))
        candidate_reviews.append(review)
        source_artifact_checks.extend(
            {"point_dir": review["point_dir"], **row} for row in checks
        )

    if set(groups).difference(seen_points):
        global_reasons.append("orphan_preflight_artifact_check_group")
    global_reasons = sorted(set(global_reasons))
    blocked_count = sum(not row["plan_candidate_ready"] for row in candidate_reviews)
    ready = not global_reasons and blocked_count == 0 and bool(candidate_reviews)
    preflight_sha = _sha256_file(preflight_path)
    for row in candidate_reviews:
        row["preflight_json_sha256"] = preflight_sha
    operation_plan = [dict(row) for row in candidate_reviews] if ready else []
    return {
        "schema": SCHEMA,
        "overall_status": READY_STATUS if ready else BLOCKED_STATUS,
        "production_state": "blocked_plan_only_no_evaluation_no_write",
        "blocked_generator_plan_ready": ready,
        "global_blocker_codes": global_reasons,
        "candidate_count": len(candidate_reviews),
        "candidate_plan_ready_count": sum(
            row["plan_candidate_ready"] is True for row in candidate_reviews
        ),
        "candidate_blocked_count": blocked_count,
        "manual_gate_review_count": sum(
            row["manual_gate_review_required"] is True for row in candidate_reviews
        ),
        "source_evidence_check_count": len(source_checks),
        "source_evidence_check_blocked_count": sum(
            row["status"] == "blocked" for row in source_checks
        ),
        "source_artifact_check_count": len(source_artifact_checks),
        "source_artifact_check_blocked_count": sum(
            row["status"] == "blocked" for row in source_artifact_checks
        ),
        "preflight_json_path": str(preflight_path),
        "preflight_json_sha256": preflight_sha,
        "candidate_reviews": candidate_reviews,
        "operation_plan": operation_plan,
        "source_evidence_checks": source_checks,
        "source_artifact_checks": source_artifact_checks,
        "locks": {
            "blocked_generator_plan_available": True,
            "execution_supported": False,
            "component_qc_evaluation_allowed": False,
            "component_qc_grade_derivation_allowed": False,
            "production_component_qc_generator_available": False,
            "historical_component_qc_generation_allowed": False,
            "historical_component_qc_write_allowed": False,
            "component_qc_overwrite_allowed": False,
            "component_qc_backfill_allowed": False,
            "historical_fit_allowed": False,
            "formal_release_allowed": False,
            "database_import_allowed": False,
            "opens_com_ports": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "writes_sn_or_device_code": False,
            "connects_postgresql": False,
        },
        "evidence_source": "historical_replay",
        "not_real_acceptance_evidence": True,
    }


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(str(key))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields or ["empty"])
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    key: json.dumps(value, ensure_ascii=False, sort_keys=True)
                    if isinstance(value, (list, dict))
                    else value
                    for key, value in row.items()
                }
            )


def write_v1_5_historical_component_qc_blocked_generator_plan(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    """Write review artifacts only; never write candidate component-QC targets."""

    out = Path(output_dir).resolve()
    suffix = tuple(part.lower() for part in out.parts[-len(REVIEW_OUTPUT_SUFFIX) :])
    if suffix != REVIEW_OUTPUT_SUFFIX:
        raise ValueError(
            "output_dir_must_be_historical_component_qc_blocked_generator_plan_review_directory"
        )
    out.mkdir(parents=True, exist_ok=True)
    outputs = {
        "json": out / "v1_5_historical_component_qc_blocked_generator_plan.json",
        "operation_plan_csv": out
        / "v1_5_historical_component_qc_blocked_generator_operation_plan.csv",
        "candidate_reviews_csv": out
        / "v1_5_historical_component_qc_blocked_generator_candidate_reviews.csv",
        "source_checks_csv": out
        / "v1_5_historical_component_qc_blocked_generator_source_checks.csv",
        "markdown": out / "V1_5_HISTORICAL_COMPONENT_QC_BLOCKED_GENERATOR_PLAN.md",
    }
    outputs["json"].write_text(
        json.dumps(dict(model), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    _write_csv(outputs["operation_plan_csv"], model.get("operation_plan") or [])
    _write_csv(outputs["candidate_reviews_csv"], model.get("candidate_reviews") or [])
    source_rows = [
        {"check_scope": "preflight_source", **row}
        for row in model.get("source_evidence_checks") or []
    ] + [
        {"check_scope": "candidate_artifact", **row}
        for row in model.get("source_artifact_checks") or []
    ]
    _write_csv(outputs["source_checks_csv"], source_rows)
    locks = model.get("locks") or {}
    lines = [
        "# V1.5 Historical Component-QC Blocked Generator Plan",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- production_state: `{model.get('production_state')}`",
        f"- candidate_count: `{model.get('candidate_count')}`",
        f"- candidate_plan_ready_count: `{model.get('candidate_plan_ready_count')}`",
        f"- candidate_blocked_count: `{model.get('candidate_blocked_count')}`",
        f"- source_evidence_check_blocked_count: `{model.get('source_evidence_check_blocked_count')}`",
        f"- source_artifact_check_count: `{model.get('source_artifact_check_count')}`",
        f"- operation_plan_count: `{len(model.get('operation_plan') or [])}`",
        f"- component_qc_evaluation_allowed: `{locks.get('component_qc_evaluation_allowed')}`",
        f"- component_qc_grade_derivation_allowed: `{locks.get('component_qc_grade_derivation_allowed')}`",
        f"- historical_component_qc_write_allowed: `{locks.get('historical_component_qc_write_allowed')}`",
        f"- component_qc_overwrite_allowed: `{locks.get('component_qc_overwrite_allowed')}`",
        "- evidence_source: `historical_replay`",
        "- not_real_acceptance_evidence: `true`",
        "",
        "This artifact is a deterministic would-write preview only. It does not evaluate samples, derive A/B/C grades, create or overwrite historical component-QC files, fit coefficients, open COM, control routes, or connect PostgreSQL.",
    ]
    outputs["markdown"].write_text("\n".join(lines) + "\n", encoding="utf-8")
    return outputs


__all__ = [
    "BLOCKED_STATUS",
    "READY_STATUS",
    "REVIEW_OUTPUT_SUFFIX",
    "SCHEMA",
    "build_v1_5_historical_component_qc_blocked_generator_plan",
    "write_v1_5_historical_component_qc_blocked_generator_plan",
]
