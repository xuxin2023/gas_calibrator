"""Single read-only snapshot contract for the final V1.5 workstation.

The builder consumes already-produced workstation results and bounded local
artifact presence.  It never opens COM ports, controls routes, writes
coefficients, mutates a database, or promotes dry-run evidence.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping


SCHEMA_VERSION = "v1_5_workstation_snapshot_v1"
EXPECTED_POINT_COUNTS = {"co2": 45, "h2o": 13}
CONFIGURED_CHANNEL_COUNT = 6
PRODUCTION_PROFILE_ID = "legacy_ratio_production"
SHADOW_PROFILE_ID = "absorption_ratio_shadow"
ARTIFACT_ROLES = (
    "execution_rows",
    "execution_summary",
    "diagnostic_analysis",
    "formal_analysis",
)
EXPORT_STATUSES = ("ok", "skipped", "missing", "error")
REPORT_AUTHORITY = "mature_v1_5_runner_artifacts"
ARTIFACT_DEFINITIONS = (
    (
        "workstation_result_json",
        "V1.5 工作站结果 JSON",
        "v1_5_operator_workstation_dry_run.json",
        "execution_summary",
    ),
    (
        "workstation_result_markdown",
        "V1.5 工作站结果摘要",
        "V1_5_OPERATOR_WORKSTATION_DRY_RUN.md",
        "execution_summary",
    ),
)
SAFETY_FLAGS = (
    "opens_com_ports",
    "controls_water_or_gas_routes",
    "writes_coefficients",
    "writes_device_id",
)


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _clean_list(values: Iterable[Any] | None) -> list[str]:
    if isinstance(values, (str, bytes)):
        values = [values]
    return [
        text
        for value in values or ()
        if (text := str(value or "").strip())
    ]


def _point_counts(execution: Mapping[str, Any]) -> dict[str, int]:
    provided = dict(execution.get("point_counts") or {})
    counts: dict[str, int] = {}
    for route_kind, default in EXPECTED_POINT_COUNTS.items():
        if route_kind not in provided:
            counts[route_kind] = default
            continue
        try:
            counts[route_kind] = int(provided[route_kind])
        except (TypeError, ValueError):
            counts[route_kind] = 0
    return counts


def _route_rows(execution: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw in execution.get("route_results") or ():
        if not isinstance(raw, Mapping):
            continue
        route_kind = str(raw.get("route_kind") or "").strip().lower()
        if route_kind not in EXPECTED_POINT_COUNTS:
            continue
        try:
            point_count = int(
                raw.get("dry_run_points")
                or EXPECTED_POINT_COUNTS[route_kind]
            )
        except (TypeError, ValueError):
            point_count = EXPECTED_POINT_COUNTS[route_kind]
        rows.append(
            {
                "route_kind": route_kind,
                "status": str(raw.get("status") or "pending"),
                "point_count": point_count,
                "blockers": _clean_list(raw.get("blockers")),
            }
        )
    return rows


def _plan_route_rows(
    execution: Mapping[str, Any],
    *,
    route_results: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    result_statuses = {
        str(row.get("route_kind") or "").strip().lower(): str(
            row.get("status") or "pending"
        )
        for row in route_results
        if str(row.get("route_kind") or "").strip().lower()
        in EXPECTED_POINT_COUNTS
    }
    provided_routes = execution.get("routes")
    source_rows = (
        provided_routes
        if isinstance(provided_routes, (list, tuple))
        else [
            {
                "route_kind": route_kind,
                "expected_point_count": point_count,
            }
            for route_kind, point_count in EXPECTED_POINT_COUNTS.items()
        ]
    )
    rows: list[dict[str, Any]] = []
    observed: set[str] = set()
    for raw in source_rows:
        if not isinstance(raw, Mapping):
            continue
        route_kind = str(raw.get("route_kind") or "").strip().lower()
        if route_kind not in EXPECTED_POINT_COUNTS or route_kind in observed:
            continue
        observed.add(route_kind)
        try:
            point_count = int(
                raw.get("expected_point_count")
                or raw.get("point_count")
                or EXPECTED_POINT_COUNTS[route_kind]
            )
        except (TypeError, ValueError):
            point_count = EXPECTED_POINT_COUNTS[route_kind]
        rows.append(
            {
                "route_kind": route_kind,
                "point_count": point_count,
                "execution_mode": "mature_runner_dry_run",
                "status": result_statuses.get(route_kind, "planned"),
            }
        )
    for route_kind, point_count in EXPECTED_POINT_COUNTS.items():
        if route_kind not in observed:
            rows.append(
                {
                    "route_kind": route_kind,
                    "point_count": point_count,
                    "execution_mode": "mature_runner_dry_run",
                    "status": result_statuses.get(route_kind, "planned"),
                }
            )
    return rows


def _plan_summary(
    *,
    execution: Mapping[str, Any],
    execution_status: str,
    point_counts: Mapping[str, int],
    route_results: list[dict[str, Any]],
    safety: Mapping[str, Any],
    certificate: Mapping[str, Any],
    warnings: list[str],
    blockers: list[str],
) -> dict[str, Any]:
    routes = _plan_route_rows(execution, route_results=route_results)
    if safety.get("status") != "pass" or blockers:
        status = "blocked"
    elif execution_status == "pass":
        status = "executed_dry_run"
    elif execution_status in {"failed", "error"}:
        status = "failed"
    else:
        status = "planned"
    return {
        "status": status,
        "profile_id": str(
            execution.get("profile_id") or "legacy_ratio_production"
        ),
        "calibration_kernel": str(
            execution.get("calibration_kernel")
            or "v1_5_legacy_ratio_0613_0620_0621"
        ),
        "route_order": ["co2", "h2o"],
        "routes": routes,
        "point_counts": dict(point_counts),
        "total_points": sum(int(value) for value in point_counts.values()),
        "execution_mode": "mature_runner_dry_run",
        "editable": False,
        "point_table_edit_allowed": False,
        "certificate_start_gate": str(
            certificate.get("start_gate") or "non_blocking"
        ),
        "no_write": safety.get("status") == "pass",
        "warnings": list(warnings),
        "blockers": list(blockers),
        "contains_paths": False,
    }


def _qc_summary(
    *,
    execution_status: str,
    point_counts: Mapping[str, int],
    route_results: list[dict[str, Any]],
    safety: Mapping[str, Any],
    blockers: list[str],
) -> dict[str, Any]:
    point_contract_pass = dict(point_counts) == EXPECTED_POINT_COUNTS
    route_statuses = {
        str(row.get("route_kind") or ""): (
            str(row.get("status") or ""),
            int(row.get("point_count") or 0),
        )
        for row in route_results
    }
    route_closure_pass = (
        execution_status == "pass"
        and route_statuses
        == {
            route_kind: ("pass", point_count)
            for route_kind, point_count in EXPECTED_POINT_COUNTS.items()
        }
    )
    if route_closure_pass:
        route_closure_status = "pass"
    elif execution_status in {"pass", "failed", "error", "blocked"}:
        route_closure_status = "fail"
    else:
        route_closure_status = "pending"

    checks = [
        {
            "check_id": "point_count_contract",
            "status": "pass" if point_contract_pass else "fail",
            "evidence_basis": "mature_45_13_queue_contract",
        },
        {
            "check_id": "anchor_separation",
            "status": "pass",
            "evidence_basis": "co2_zero_gas_and_h2o_dry_gas_are_distinct",
        },
        {
            "check_id": "no_write_safety",
            "status": str(safety.get("status") or "blocked"),
            "evidence_basis": "workstation_safety_flags",
        },
        {
            "check_id": "route_dry_run_closure",
            "status": route_closure_status,
            "evidence_basis": "mature_runner_route_results",
        },
        {
            "check_id": "sample_stability",
            "status": "not_evaluated",
            "evidence_basis": "real_samples_not_available",
        },
        {
            "check_id": "real_device_readback",
            "status": "not_evaluated",
            "evidence_basis": "real_device_not_connected",
        },
    ]
    qc_blockers = list(blockers)
    if not point_contract_pass:
        qc_blockers.append("point_count_contract_failed")
    if safety.get("status") != "pass":
        qc_blockers.extend(str(item) for item in safety.get("violations") or ())
    if route_closure_status == "fail":
        qc_blockers.append("route_dry_run_closure_failed")
    qc_blockers = list(dict.fromkeys(qc_blockers))

    if qc_blockers or execution_status in {"failed", "error", "blocked"}:
        overall_status = "blocked"
    elif execution_status == "pass" and route_closure_pass:
        overall_status = "dry_run_pass"
    else:
        overall_status = "pending"
    return {
        "overall_status": overall_status,
        "checks": checks,
        "warnings": [
            "sample_stability_not_evaluated",
            "real_device_readback_not_evaluated",
        ],
        "blockers": qc_blockers,
        "formal_acceptance_status": "not_evaluated",
        "release_status": "not_released",
        "point_evidence_contract": {
            "status": "not_evaluated",
            "authority": "mature_v1_5_runner_artifacts",
            "artifact_roles": [
                "execution_rows",
                "execution_summary",
                "formal_analysis",
            ],
            "available_row_count": 0,
            "reason": "real_samples_not_available",
            "required_fields": [
                "point_id",
                "route_kind",
                "analyzer_id",
                "reference_temperature_c",
                "reference_pressure_hpa",
                "reference_dewpoint_or_humidity",
                "sample_count",
                "stability_status",
                "decision",
                "reason_code",
                "qc_policy_version",
                "threshold_profile_hash",
                "evidence_source",
            ],
        },
        "rule_threshold_governance": {
            "status": "runner_owned_read_only",
            "source": "reviewed_runtime_config_and_mature_runner_qc",
            "ui_edit_allowed": False,
            "policy_version_required": True,
            "threshold_profile_hash_required": True,
        },
        "reject_reason_summary": {
            "status": "not_evaluated",
            "source_artifact_role": "execution_summary",
            "reason_code_required": True,
        },
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
    }


def _device_summary() -> dict[str, Any]:
    channels = [
        {
            "channel_id": f"CH{index:02d}",
            "display_name": f"通道 {index:02d}",
            "mode": "simulation_only",
            "connection_status": "not_connected",
            "identity_status": "not_evaluated",
            "health_status": "not_evaluated",
            "last_frame_status": "not_evaluated",
        }
        for index in range(1, CONFIGURED_CHANNEL_COUNT + 1)
    ]
    return {
        "overall_status": "simulation_only",
        "ui_mode": "read_only_configured_slots",
        "runtime_state_authority": "mature_v1_5_runner_only",
        "real_device_state": "not_evaluated",
        "connection_policy": "no_com_no_scan",
        "configured_channel_count": CONFIGURED_CHANNEL_COUNT,
        "connected_count": 0,
        "identity_evaluated_count": 0,
        "health_evaluated_count": 0,
        "unknown_health_count": CONFIGURED_CHANNEL_COUNT,
        "channels": channels,
        "device_control_actions_available": False,
        "hardware_refresh_actions_available": False,
        "simulation_preset_actions_available": False,
        "fault_injection_actions_available": False,
        "route_control_actions_available": False,
        "device_configuration_actions_available": False,
        "initialization_contract": {
            "owner": "mature_v1_5_initialization_flow",
            "runtime_mode": "MODE2",
            "upload_rate_hz": 1,
            "temperature_coefficients": "SENCO7_SENCO8_neutral",
            "neutralization_evidence_required": True,
            "readback_verification_required": True,
            "performed_by_read_only_workstation": False,
        },
        "contains_ports": False,
        "contains_serial_numbers": False,
        "contains_runtime_device_data": False,
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
    }


def _algorithm_summary(
    *,
    execution: Mapping[str, Any],
    point_counts: Mapping[str, int],
) -> dict[str, Any]:
    observed_profile_id = str(
        execution.get("profile_id") or PRODUCTION_PROFILE_ID
    )
    profile_matches = observed_profile_id == PRODUCTION_PROFILE_ID
    point_contract_matches = dict(point_counts) == EXPECTED_POINT_COUNTS
    blockers: list[str] = []
    if not profile_matches:
        blockers.extend(
            [
                "observed_profile_is_not_locked_production_default",
                "implicit_profile_switch_forbidden",
            ]
        )
    if not point_contract_matches:
        blockers.append("production_profile_point_count_contract_failed")
    return {
        "overall_status": (
            "locked_production_default" if not blockers else "blocked"
        ),
        "observed_profile_id": observed_profile_id,
        "production_profile": {
            "profile_id": PRODUCTION_PROFILE_ID,
            "algorithm_mode": "legacy_ratio_R",
            "production_default": True,
            "review_status": "reviewed_mature_baseline",
            "point_counts": dict(point_counts),
            "fit_inputs": {
                "co2": "R_CO2",
                "h2o": "R_H2O",
            },
        },
        "shadow_candidates": [
            {
                "profile_id": SHADOW_PROFILE_ID,
                "algorithm_mode": "absorption_ratio_A",
                "production_default": False,
                "evaluation_scope": "offline_shadow",
                "promotion_state": "blocked",
                "writeback_state": "blocked",
                "fit_equation": "A=-ln(R/R0(T))/(P_kPa/100)",
            }
        ],
        "physical_contract": {
            "pressure_sequence": "SENCO9_first",
            "temperature_coefficients": "SENCO7_SENCO8_neutral_by_default",
            "co2_anchor": "co2_zero_gas",
            "h2o_anchor": "h2o_dry_gas",
            "anchors_are_distinct": True,
            "route_behavior": "preserve_mature_v1_5_0620_0621",
        },
        "auto_select": False,
        "profile_selection_actions_available": False,
        "coefficient_write_actions_available": False,
        "warnings": [
            "shadow_candidate_is_not_the_production_default",
            "real_acceptance_not_performed",
        ],
        "blockers": blockers,
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
    }


def _certificate_summary(
    records: Iterable[Mapping[str, Any]] | None,
    *,
    error: str = "",
) -> dict[str, Any]:
    clean_records = [dict(item) for item in records or () if isinstance(item, Mapping)]
    state_counts = Counter(
        str(item.get("review_state") or "draft") for item in clean_records
    )
    return {
        "record_count": len(clean_records),
        "review_state_counts": dict(sorted(state_counts.items())),
        "load_error": str(error or ""),
        "start_gate": "non_blocking",
        "connected_to_calibration": False,
        "formal_release_requires_independent_review": True,
    }


def _artifact_summary(output_dir: str | Path | None) -> dict[str, Any]:
    root = Path(output_dir).resolve() if output_dir else None
    rows: list[dict[str, Any]] = []
    for artifact_id, display_name, filename, role in ARTIFACT_DEFINITIONS:
        present = bool(root is not None and (root / filename).is_file())
        rows.append(
            {
                "artifact_id": artifact_id,
                "display_name": display_name,
                "role": role,
                "export_status": "ok" if present else "missing",
                "present": present,
            }
        )
    return {
        "artifacts": rows,
        "artifact_count": len(rows),
        "present_count": sum(bool(row["present"]) for row in rows),
        "allowed_roles": list(ARTIFACT_ROLES),
        "allowed_export_statuses": list(EXPORT_STATUSES),
        "authority": REPORT_AUTHORITY,
        "ui_mode": "read_only_inventory",
        "export_actions_available": False,
        "formal_release_status": "not_evaluated",
        "formal_release_requires_independent_review": True,
        "formal_certificate_signing_available": False,
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "contains_paths": False,
    }


def _safety_summary(execution: Mapping[str, Any]) -> dict[str, Any]:
    observed = {
        flag: bool(execution.get(flag, False))
        for flag in SAFETY_FLAGS
    }
    violations = [f"{flag}_true" for flag, value in observed.items() if value]
    if execution.get("not_real_acceptance_evidence") is False:
        violations.append("dry_run_claimed_as_real_acceptance")
    return {
        "status": "pass" if not violations else "blocked",
        **observed,
        "not_real_acceptance_evidence": True,
        "violations": violations,
    }


def _review_summary(
    *,
    execution_status: str,
    safety: Mapping[str, Any],
    certificate: Mapping[str, Any],
    blockers: list[str],
) -> dict[str, Any]:
    if safety.get("status") != "pass" or blockers:
        status = "blocked"
    elif execution_status == "pass":
        status = "dry_run_review_ready"
    else:
        status = "pending"
    sections = [
        {
            "key": "execution",
            "status": execution_status,
            "blockers": list(blockers),
        },
        {
            "key": "physical_anchors",
            "status": "preserved",
            "co2_anchor": "co2_zero_gas",
            "h2o_anchor": "h2o_dry_gas",
        },
        {
            "key": "certificate",
            "status": (
                "load_error"
                if certificate.get("load_error")
                else "advisory"
            ),
            "blockers": [],
        },
        {
            "key": "safety",
            "status": safety.get("status"),
            "blockers": list(safety.get("violations") or []),
        },
        {
            "key": "release",
            "status": "not_real_acceptance",
            "blockers": ["real_acceptance_not_performed"],
        },
    ]
    next_actions = (
        ["复核阻断项后重新执行仿真演练。"]
        if status == "blocked"
        else ["继续人工审核；不得把 dry-run 解释为真机验收。"]
    )
    return {
        "overall_status": status,
        "sections": sections,
        "next_actions": next_actions,
        "approval_actions_available": False,
        "coefficient_write_actions_available": False,
    }


def build_workstation_snapshot(
    *,
    execution: Mapping[str, Any] | None = None,
    output_dir: str | Path | None = None,
    certificate_records: Iterable[Mapping[str, Any]] | None = None,
    certificate_error: str = "",
) -> dict[str, Any]:
    """Build the one read model consumed by V1.5 result/review surfaces."""

    payload = dict(execution or {})
    execution_status = str(payload.get("overall_status") or "not_started")
    point_counts = _point_counts(payload)
    routes = _route_rows(payload)
    warnings = _clean_list(payload.get("warnings"))
    blockers = [
        *_clean_list(payload.get("blockers")),
        *_clean_list(payload.get("execution_blockers")),
    ]
    certificate = _certificate_summary(
        certificate_records,
        error=certificate_error,
    )
    safety = _safety_summary(payload)
    reports = _artifact_summary(output_dir)
    plan = _plan_summary(
        execution=payload,
        execution_status=execution_status,
        point_counts=point_counts,
        route_results=routes,
        safety=safety,
        certificate=certificate,
        warnings=warnings,
        blockers=blockers,
    )
    qc = _qc_summary(
        execution_status=execution_status,
        point_counts=point_counts,
        route_results=routes,
        safety=safety,
        blockers=blockers,
    )
    devices = _device_summary()
    algorithm = _algorithm_summary(
        execution=payload,
        point_counts=point_counts,
    )
    review = _review_summary(
        execution_status=execution_status,
        safety=safety,
        certificate=certificate,
        blockers=blockers,
    )
    snapshot = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _now(),
        "product_name": "V1.5 气体分析仪校准工作站",
        "product_version": "V1.5",
        "display_mode": "simulated_read_only",
        "channel_count": CONFIGURED_CHANNEL_COUNT,
        "overall_status": execution_status,
        "point_counts": point_counts,
        "run": {
            "run_id": str(payload.get("run_id") or ""),
            "status": execution_status,
            "mode": "dry_run",
            "calibration_kernel": str(
                payload.get("calibration_kernel")
                or "v1_5_legacy_ratio_0613_0620_0621"
            ),
            "profile_id": str(
                payload.get("profile_id") or "legacy_ratio_production"
            ),
            "point_counts": point_counts,
            "route_order": ["co2", "h2o"],
            "route_results": routes,
            "warnings": warnings,
            "blockers": blockers,
        },
        "results": {
            "status": execution_status,
            "point_counts": point_counts,
            "route_results": routes,
            "anchors": {
                "co2": {
                    "kind": "co2_zero_gas",
                    "independent_from_h2o": True,
                },
                "h2o": {
                    "kind": "h2o_dry_gas",
                    "independent_from_co2": True,
                },
            },
            "warnings": warnings,
            "blockers": blockers,
        },
        "plan": plan,
        "qc": qc,
        "devices": devices,
        "algorithm": algorithm,
        "reports": reports,
        "review": review,
        "certificate": certificate,
        "safety": safety,
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "opens_com_ports": safety["opens_com_ports"],
        "controls_water_or_gas_routes": safety[
            "controls_water_or_gas_routes"
        ],
        "writes_coefficients": safety["writes_coefficients"],
        "writes_device_id": safety["writes_device_id"],
    }
    return snapshot


__all__ = [
    "ARTIFACT_DEFINITIONS",
    "ARTIFACT_ROLES",
    "CONFIGURED_CHANNEL_COUNT",
    "EXPORT_STATUSES",
    "EXPECTED_POINT_COUNTS",
    "PRODUCTION_PROFILE_ID",
    "REPORT_AUTHORITY",
    "SCHEMA_VERSION",
    "SHADOW_PROFILE_ID",
    "build_workstation_snapshot",
]
