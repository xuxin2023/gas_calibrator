from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Optional


DEFAULT_SOFTWARE_BUILD_ID = "dev-build"
PROMOTION_TARGET_PRIMARY_LATEST = "real_primary_latest"


def build_version_snapshot(
    *,
    config_snapshot: Any,
    source_points_file: Optional[str | Path],
    profile_name: Optional[str],
    profile_version: Optional[str],
    software_build_id: Optional[str],
) -> dict[str, Any]:
    effective_build = str(software_build_id or "").strip() or DEFAULT_SOFTWARE_BUILD_ID
    return {
        "software_build_id": effective_build,
        "config_version": f"cfg-{_stable_digest(config_snapshot)}",
        "points_version": _path_version(source_points_file, prefix="pts"),
        "profile_name": str(profile_name or "").strip() or None,
        "profile_version": (
            str(profile_version or "").strip()
            or (f"profile-{_stable_digest({'profile_name': profile_name})}" if str(profile_name or "").strip() else None)
        ),
    }


def build_validation_acceptance_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    route_execution = dict(payload.get("route_execution_summary") or {})
    evidence_source = normalize_evidence_source(payload.get("evidence_source"))
    evidence_state = normalize_evidence_state(payload.get("evidence_state"))
    compare_status = str(payload.get("compare_status") or "--")
    diagnostic_only = bool(payload.get("diagnostic_only", False))
    acceptance_evidence = bool(payload.get("acceptance_evidence", False))
    not_real_acceptance_evidence = bool(payload.get("not_real_acceptance_evidence", False)) or evidence_source != "real_acceptance"
    primary_missing = compare_status == "PRIMARY_REAL_VALIDATION_LATEST_MISSING"
    acceptance_level = "diagnostic"
    if evidence_source == "real_probe":
        acceptance_level = "real_probe"
    elif evidence_source == "real_acceptance" and acceptance_evidence and not not_real_acceptance_evidence:
        acceptance_level = "real_acceptance"
    elif evidence_source in {"simulated_protocol", "replay"}:
        acceptance_level = "offline_regression"
    if diagnostic_only:
        acceptance_level = "diagnostic"

    review_state = "pending"
    approval_state = "blocked"
    if acceptance_level == "real_acceptance":
        review_state = "required"
        approval_state = "pending"

    gate_map = {
        "offline_validation_artifacts_present": compare_status not in {"--", ""},
        "route_execution_evaluable": bool(route_execution.get("valid_for_route_diff", False)),
        "reference_quality_gate": reference_quality_ok(payload.get("reference_quality")),
        "relay_physical_mismatch_gate": not relay_mismatch_present(route_execution),
        "real_probe_evidence_present": evidence_source in {"real_probe", "real_acceptance"} and not primary_missing,
        "real_acceptance_evidence_present": evidence_source == "real_acceptance" and acceptance_evidence and not not_real_acceptance_evidence,
        "review_completed": False,
        "approval_granted": False,
    }
    missing_conditions = [_gate_display_name(name) for name, passed in gate_map.items() if not passed]
    ready_for_promotion = all(gate_map.values())
    promotion_state = "ready" if ready_for_promotion else ("dry_run_only" if primary_missing or not_real_acceptance_evidence else "blocked")
    role_views = build_role_views(
        operator_status="attention" if compare_status not in {"MATCH", "PRIMARY_REAL_VALIDATION_LATEST_MISSING"} else "waiting_real_acceptance",
        operator_failures=[str(payload.get("first_failure_phase") or route_execution.get("first_failure_phase") or "--")],
        reviewer_complete=gate_map["offline_validation_artifacts_present"]
        and gate_map["reference_quality_gate"]
        and gate_map["relay_physical_mismatch_gate"],
        reviewer_notes=missing_conditions,
        approver_ready=ready_for_promotion,
        approver_missing=missing_conditions,
    )
    gates = [
        gate_payload(
            name=name,
            required=True,
            passed=passed,
            evidence_source=evidence_source,
            current_state="passed" if passed else "missing",
        )
        for name, passed in gate_map.items()
    ]
    return {
        "evidence_source": evidence_source,
        "evidence_state": evidence_state,
        "acceptance_level": acceptance_level,
        "acceptance_scope": "validation_latest",
        "promotion_state": promotion_state,
        "review_state": review_state,
        "approval_state": approval_state,
        "ready_for_promotion": ready_for_promotion,
        "missing_conditions": missing_conditions,
        "role_views": role_views,
        "gates": gates,
        "promotion_plan": build_promotion_plan(
            evidence_source=evidence_source,
            promotion_state=promotion_state,
            missing_conditions=missing_conditions,
            ready_for_promotion=ready_for_promotion,
        ),
        "readiness_summary": build_acceptance_readiness_summary(
            acceptance_level=acceptance_level,
            promotion_state=promotion_state,
            ready_for_promotion=ready_for_promotion,
            missing_conditions=missing_conditions,
            simulated_only=not_real_acceptance_evidence,
        ),
        "state_machine": build_acceptance_state_machine(
            evidence_source=evidence_source,
            ready_for_promotion=ready_for_promotion,
            simulated_only=not_real_acceptance_evidence,
        ),
    }


def build_run_acceptance_plan(
    *,
    run_id: str,
    simulation_mode: bool,
    reference_quality_ok_flag: bool,
    export_error_count: int,
    parity_status: str,
) -> dict[str, Any]:
    evidence_source = "simulated_protocol" if simulation_mode else "real_probe"
    gates = [
        gate_payload("offline_execution_complete", True, export_error_count == 0, evidence_source, "passed" if export_error_count == 0 else "failed"),
        gate_payload("reference_quality_gate", True, reference_quality_ok_flag, evidence_source, "passed" if reference_quality_ok_flag else "degraded"),
        gate_payload("summary_parity_available", False, parity_status in {"MATCH", "ok"}, "diagnostic", parity_status or "missing"),
        gate_payload("real_probe_evidence_present", True, not simulation_mode, evidence_source, "passed" if not simulation_mode else "missing"),
        gate_payload("real_acceptance_evidence_present", True, False, "real_acceptance", "missing"),
        gate_payload("review_completed", True, False, "diagnostic", "missing"),
        gate_payload("approval_granted", True, False, "diagnostic", "missing"),
        gate_payload("promote_primary_latest", True, False, evidence_source, "dry_run_only"),
    ]
    missing_conditions = [_gate_display_name(item["name"]) for item in gates if item["required"] and not item["passed"]]
    ready_for_promotion = False
    role_views = build_role_views(
        operator_status="healthy" if export_error_count == 0 else "attention",
        operator_failures=["export_errors"] if export_error_count else [],
        reviewer_complete=export_error_count == 0 and reference_quality_ok_flag,
        reviewer_notes=missing_conditions,
        approver_ready=ready_for_promotion,
        approver_missing=missing_conditions,
    )
    return {
        "run_id": run_id,
        "evidence_source": evidence_source,
        "evidence_state": "collected",
        "acceptance_level": "offline_regression" if simulation_mode else "real_probe",
        "acceptance_scope": "run",
        "promotion_state": "dry_run_only",
        "review_state": "pending",
        "approval_state": "blocked",
        "ready_for_promotion": ready_for_promotion,
        "not_real_acceptance_evidence": evidence_source != "real_acceptance",
        "required_gates": gates,
        "missing_conditions": missing_conditions,
        "promotion_plan": build_promotion_plan(
            evidence_source=evidence_source,
            promotion_state="dry_run_only",
            missing_conditions=missing_conditions,
            ready_for_promotion=ready_for_promotion,
        ),
        "role_views": role_views,
        "readiness_summary": build_acceptance_readiness_summary(
            acceptance_level="offline_regression" if simulation_mode else "real_probe",
            promotion_state="dry_run_only",
            ready_for_promotion=ready_for_promotion,
            missing_conditions=missing_conditions,
            simulated_only=simulation_mode,
        ),
        "state_machine": build_acceptance_state_machine(
            evidence_source=evidence_source,
            ready_for_promotion=ready_for_promotion,
            simulated_only=simulation_mode,
        ),
    }


def build_user_visible_evidence_boundary(
    *,
    evidence_source: Any = None,
    simulation_mode: Optional[bool] = None,
    not_real_acceptance_evidence: Optional[bool] = None,
    acceptance_level: Optional[str] = None,
    promotion_state: Optional[str] = None,
) -> dict[str, Any]:
    if evidence_source is None:
        if simulation_mode is True:
            evidence_source = "simulated_protocol"
        elif simulation_mode is False:
            evidence_source = "real_probe"
        else:
            evidence_source = "diagnostic"

    normalized_source = normalize_evidence_source(evidence_source)
    boundary_is_not_real = (
        normalized_source != "real_acceptance"
        if not_real_acceptance_evidence is None
        else bool(not_real_acceptance_evidence)
    )

    resolved_acceptance_level = acceptance_level
    if resolved_acceptance_level is None:
        if normalized_source in {"simulated_protocol", "replay"}:
            resolved_acceptance_level = "offline_regression"
        elif normalized_source == "real_probe":
            resolved_acceptance_level = "real_probe"
        elif normalized_source == "real_acceptance" and not boundary_is_not_real:
            resolved_acceptance_level = "real_acceptance"
        else:
            resolved_acceptance_level = "diagnostic"

    resolved_promotion_state = promotion_state
    if resolved_promotion_state is None:
        resolved_promotion_state = "dry_run_only" if boundary_is_not_real else "blocked"

    return {
        "evidence_source": normalized_source,
        "not_real_acceptance_evidence": boundary_is_not_real,
        "acceptance_level": resolved_acceptance_level,
        "promotion_state": resolved_promotion_state,
    }


def build_suite_acceptance_plan(
    *,
    suite_name: str,
    offline_green: bool,
    parity_green: bool,
    resilience_green: bool,
    evidence_sources_present: list[str],
) -> dict[str, Any]:
    normalized_sources: list[str] = []
    for value in list(evidence_sources_present or []):
        text = str(value or "").strip()
        if not text:
            continue
        source = normalize_evidence_source(text)
        if source not in normalized_sources:
            normalized_sources.append(source)
    gates = [
        gate_payload("offline_suite_green", True, offline_green, "simulated_protocol", "passed" if offline_green else "failed"),
        gate_payload("summary_parity_green", False, parity_green, "diagnostic", "passed" if parity_green else "missing"),
        gate_payload("export_resilience_green", False, resilience_green, "diagnostic", "passed" if resilience_green else "missing"),
        gate_payload("real_probe_evidence_present", True, False, "real_probe", "missing"),
        gate_payload("real_acceptance_evidence_present", True, False, "real_acceptance", "missing"),
        gate_payload("review_completed", True, False, "diagnostic", "missing"),
        gate_payload("approval_granted", True, False, "diagnostic", "missing"),
        gate_payload("promote_primary_latest", True, False, "diagnostic", "dry_run_only"),
    ]
    missing_conditions = [_gate_display_name(item["name"]) for item in gates if item["required"] and not item["passed"]]
    role_views = build_role_views(
        operator_status="healthy" if offline_green else "attention",
        operator_failures=["suite_failures"] if not offline_green else [],
        reviewer_complete=offline_green and parity_green,
        reviewer_notes=missing_conditions,
        approver_ready=False,
        approver_missing=missing_conditions,
    )
    return {
        "suite": suite_name,
        "evidence_source": normalized_sources[0] if normalized_sources else "simulated_protocol",
        "evidence_sources_present": normalized_sources,
        "evidence_state": "collected",
        "acceptance_level": "offline_regression",
        "acceptance_scope": "suite",
        "promotion_state": "dry_run_only",
        "review_state": "pending",
        "approval_state": "blocked",
        "ready_for_promotion": False,
        "required_gates": gates,
        "missing_conditions": missing_conditions,
        "promotion_plan": build_promotion_plan(
            evidence_source="simulated_protocol",
            promotion_state="dry_run_only",
            missing_conditions=missing_conditions,
            ready_for_promotion=False,
        ),
        "role_views": role_views,
        "readiness_summary": build_acceptance_readiness_summary(
            acceptance_level="offline_regression",
            promotion_state="dry_run_only",
            ready_for_promotion=False,
            missing_conditions=missing_conditions,
            simulated_only=True,
        ),
    }


def build_role_views(
    *,
    operator_status: str,
    operator_failures: list[str],
    reviewer_complete: bool,
    reviewer_notes: list[str],
    approver_ready: bool,
    approver_missing: list[str],
) -> dict[str, Any]:
    return {
        "operator": {
            "status": operator_status,
            "failure_points": [item for item in operator_failures if item and item != "--"],
            "summary": "run health ready" if operator_status == "healthy" else operator_status,
        },
        "reviewer": {
            "evidence_complete": reviewer_complete,
            "notes": [item for item in reviewer_notes if item],
            "summary": "evidence complete" if reviewer_complete else "evidence incomplete",
        },
        "approver": {
            "ready": approver_ready,
            "missing_conditions": [item for item in approver_missing if item],
            "summary": "ready for promotion" if approver_ready else "promotion blocked",
        },
    }


def build_promotion_plan(
    *,
    evidence_source: str,
    promotion_state: str,
    missing_conditions: list[str],
    ready_for_promotion: bool,
) -> dict[str, Any]:
    return {
        "mode": "dry_run",
        "target": PROMOTION_TARGET_PRIMARY_LATEST,
        "allowed": False,
        "requested": False,
        "current_state": promotion_state,
        "ready_for_promotion": ready_for_promotion,
        "blocking_conditions": missing_conditions,
        "publish_primary_latest_allowed": False,
        "promote_primary_latest_permitted": evidence_source == "real_acceptance" and ready_for_promotion,
    }


def build_acceptance_readiness_summary(
    *,
    acceptance_level: str,
    promotion_state: str,
    ready_for_promotion: bool,
    missing_conditions: list[str],
    simulated_only: bool,
) -> dict[str, Any]:
    return {
        "acceptance_level": acceptance_level,
        "promotion_state": promotion_state,
        "ready_for_promotion": ready_for_promotion,
        "simulated_readiness_only": simulated_only,
        "missing_conditions": missing_conditions,
        "summary": (
            f"{acceptance_level} | {promotion_state} | "
            f"{'ready' if ready_for_promotion else f'missing {len(missing_conditions)} gates'}"
        ),
    }


def build_acceptance_state_machine(
    *,
    evidence_source: str,
    ready_for_promotion: bool,
    simulated_only: bool,
) -> dict[str, Any]:
    current_state = "simulated_readiness_only" if simulated_only else ("promotion_ready" if ready_for_promotion else evidence_source)
    return {
        "current_state": current_state,
        "states": [
            "simulated_readiness_only",
            "replay_review_only",
            "real_probe_collected",
            "real_probe_reviewed",
            "real_acceptance_collected",
            "review_completed",
            "approval_granted",
            "promotion_ready",
            "promoted_primary_latest",
        ],
        "transitions": [
            {"from": "real_probe_collected", "to": "real_probe_reviewed", "condition": "review_state=completed"},
            {"from": "real_probe_reviewed", "to": "real_acceptance_collected", "condition": "real acceptance executed"},
            {"from": "real_acceptance_collected", "to": "review_completed", "condition": "review_state=completed"},
            {"from": "review_completed", "to": "approval_granted", "condition": "approval_state=approved"},
            {"from": "approval_granted", "to": "promotion_ready", "condition": "all promotion gates green"},
            {"from": "promotion_ready", "to": "promoted_primary_latest", "condition": "promotion action explicitly allowed"},
        ],
    }


def gate_payload(name: str, required: bool, passed: bool, evidence_source: str, current_state: str) -> dict[str, Any]:
    return {
        "name": name,
        "required": required,
        "passed": passed,
        "current_state": current_state,
        "evidence_source": evidence_source,
    }


def reference_quality_ok(value: Any) -> bool:
    if isinstance(value, dict):
        return str(value.get("reference_quality") or "").strip().lower() == "healthy"
    return str(value or "").strip().lower() == "healthy"


def relay_mismatch_present(route_execution: dict[str, Any]) -> bool:
    mismatch = dict(route_execution.get("relay_physical_mismatch") or {})
    return any(bool(item) for item in mismatch.values())


def normalize_evidence_source(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"simulated_protocol", "replay", "diagnostic", "real_probe", "real_acceptance"}:
        return normalized
    if normalized == "real":
        return "real_probe"
    if normalized == "simulated":
        return "simulated_protocol"
    return "diagnostic"


def normalize_evidence_state(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    return normalized or "planned"


def gate_display_name(value: str) -> str:
    return _gate_display_name(value)


def _gate_display_name(value: str) -> str:
    return str(value or "").replace("_", " ")


def _stable_digest(value: Any) -> str:
    payload = json.dumps(_json_safe(value), ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


def _path_version(path: Optional[str | Path], *, prefix: str) -> str:
    if path in (None, ""):
        return f"{prefix}-missing"
    target = Path(path)
    if not target.exists():
        return f"{prefix}-{_stable_digest(str(target))}"
    stats = target.stat()
    try:
        sample = target.read_bytes()[:4096]
    except Exception:
        sample = str(target).encode("utf-8")
    digest = hashlib.sha256(sample).hexdigest()[:12]
    return f"{prefix}-{target.name}-{stats.st_size}-{stats.st_mtime_ns}-{digest}"


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "__dict__"):
        return {str(key): _json_safe(item) for key, item in vars(value).items() if not str(key).startswith("_")}
    return str(value)
