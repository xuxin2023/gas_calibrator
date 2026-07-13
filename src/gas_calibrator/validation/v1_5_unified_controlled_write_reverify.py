"""Unify V1.5 coefficient write, readback, and short-reverify evidence.

This module is deliberately offline.  It reviews immutable fitting, snapshot,
authorization, write-event, readback, and reverification artifacts.  It never
opens a COM port and never turns a reviewed plan into an executable command.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "v1_5_unified_controlled_write_readback_reverify_v1"
APPROVED_CANDIDATE_SCHEMA = "v1_5_reviewed_coefficient_candidate_packet_v1"
SNAPSHOT_SCHEMA = "v1_5_getco_snapshot_v1"
AUTHORIZATION_SCHEMA = "v1_5_unified_controlled_write_authorization_v1"
CONFIRMATION_TEXT = "AUTHORIZE_V1_5_UNIFIED_CONTROLLED_WRITE_REVIEW"
EVIDENCE_SOURCE = "historical_replay"

LEGACY_PROFILE = "legacy_ratio_production"
NEW_PROFILE = "new_absorption_candidate"

GROUP_CONTRACTS: tuple[dict[str, Any], ...] = (
    {"group": "SENCO1", "readback": "GETCO1", "component": "co2", "role": "main_ratio_model", "width": 6, "format": "scientific_5e", "profiles": (LEGACY_PROFILE, NEW_PROFILE)},
    {"group": "SENCO3", "readback": "GETCO3", "component": "co2", "role": "main_temperature_model_T1", "width": 6, "format": "scientific_5e", "profiles": (LEGACY_PROFILE, NEW_PROFILE)},
    {"group": "SENCO2", "readback": "GETCO2", "component": "h2o", "role": "main_ratio_model", "width": 6, "format": "scientific_5e", "profiles": (LEGACY_PROFILE, NEW_PROFILE)},
    {"group": "SENCO4", "readback": "GETCO4", "component": "h2o", "role": "main_temperature_model_T1", "width": 6, "format": "scientific_5e", "profiles": (LEGACY_PROFILE, NEW_PROFILE)},
    {"group": "SENCO5", "readback": "GETCO5", "component": "co2", "role": "final_affine_output_layer", "width": 2, "format": "decimal_3", "profiles": (LEGACY_PROFILE, NEW_PROFILE), "composed_layer": True, "clear_command": "CLEARSENCO5"},
    {"group": "SENCO6", "readback": "GETCO6", "component": "h2o", "role": "final_affine_output_layer", "width": 2, "format": "decimal_3", "profiles": (LEGACY_PROFILE, NEW_PROFILE), "composed_layer": True, "clear_command": "CLEARSENCO6"},
    {"group": "SENCO7", "readback": "GETCO7", "component": "temperature", "role": "temperature_calibration_disabled_neutral_only", "width": 4, "format": "neutral_only", "profiles": (LEGACY_PROFILE, NEW_PROFILE), "neutral_values": (0.0, 1.0, 0.0, 0.0)},
    {"group": "SENCO8", "readback": "GETCO8", "component": "temperature", "role": "temperature_calibration_disabled_neutral_only", "width": 4, "format": "neutral_only", "profiles": (LEGACY_PROFILE, NEW_PROFILE), "neutral_values": (0.0, 1.0, 0.0, 0.0)},
    {"group": "SENCO9", "readback": "GETCO9", "component": "pressure", "role": "pressure_model", "width": 4, "format": "decimal_6", "profiles": (LEGACY_PROFILE, NEW_PROFILE), "default_model": "offset_only"},
    {"group": "SENCOA", "readback": "GETCOA", "component": "co2", "role": "R0_CO2_T", "width": 4, "format": "scientific_5e", "profiles": (NEW_PROFILE,), "writer_state": "blocked_design_only"},
    {"group": "SENCOB", "readback": "GETCOB", "component": "h2o", "role": "R0_H2O_T", "width": 4, "format": "scientific_5e", "profiles": (NEW_PROFILE,), "writer_state": "blocked_design_only"},
)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return payload


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _finite_values(value: Any) -> list[float]:
    if not isinstance(value, list):
        return []
    result: list[float] = []
    for item in value:
        try:
            number = float(item)
        except (TypeError, ValueError):
            return []
        if not math.isfinite(number):
            return []
        result.append(number)
    return result


def _device_id(value: Any) -> str:
    text = str(value or "").strip()
    return text.zfill(3) if text.isdigit() else text


def _format_value(value: float, format_name: str) -> str:
    if format_name == "scientific_5e":
        return format(value, ".5e").replace("e+", "e")
    if format_name == "decimal_3":
        return f"{value:.3f}"
    if format_name == "decimal_6":
        return f"{value:.6f}"
    raise ValueError(f"Unsupported payload format: {format_name}")


def _compose_affine(old_values: Sequence[float], layer_values: Sequence[float]) -> list[float]:
    return [old_values[0] + old_values[1] * layer_values[0], old_values[1] * layer_values[1]]


def _candidate_rows(packet: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for device in packet.get("device_candidates") or []:
        if not isinstance(device, Mapping):
            continue
        device_id = _device_id(device.get("device_id"))
        sn_code = str(device.get("sn_code") or device.get("device_code") or "").strip()
        for candidate in device.get("groups") or []:
            if isinstance(candidate, Mapping):
                rows.append({"device_id": device_id, "sn_code": sn_code, **dict(candidate)})
    return rows


def _snapshot_index(payload: Mapping[str, Any]) -> dict[tuple[str, str], list[float]]:
    result: dict[tuple[str, str], list[float]] = {}
    for device in payload.get("devices") or []:
        if not isinstance(device, Mapping):
            continue
        device_id = _device_id(device.get("device_id"))
        values = device.get("values") or {}
        if not isinstance(values, Mapping):
            continue
        for group, raw in values.items():
            parsed = _finite_values(raw)
            if parsed:
                result[(device_id, str(group).strip().upper())] = parsed
    return result


def _snapshot_identity(payload: Mapping[str, Any]) -> dict[str, str]:
    result: dict[str, str] = {}
    for device in payload.get("devices") or []:
        if not isinstance(device, Mapping):
            continue
        device_id = _device_id(device.get("device_id"))
        sn_code = str(device.get("sn_code") or device.get("device_code") or "").strip()
        if device_id:
            result[device_id] = sn_code
    return result


def _authorization_reasons(
    authorization: Mapping[str, Any],
    *,
    matrix_sha: str,
    candidate_sha: str,
    device_ids: Sequence[str],
) -> list[str]:
    reasons: list[str] = []
    if authorization.get("schema") != AUTHORIZATION_SCHEMA:
        reasons.append("authorization_schema_invalid")
    if authorization.get("confirmation_text") != CONFIRMATION_TEXT:
        reasons.append("authorization_confirmation_invalid")
    for field in ("operator", "reviewer", "approver"):
        if not str(authorization.get(field) or "").strip():
            reasons.append(f"authorization_{field}_missing")
    if authorization.get("reviewer") == authorization.get("approver"):
        reasons.append("authorization_reviewer_equals_approver")
    if authorization.get("fit_matrix_sha256") != matrix_sha:
        reasons.append("authorization_fit_matrix_sha256_mismatch")
    if authorization.get("candidate_packet_sha256") != candidate_sha:
        reasons.append("authorization_candidate_packet_sha256_mismatch")
    authorized_ids = sorted(_device_id(value) for value in authorization.get("device_ids") or [])
    if authorized_ids != sorted(device_ids):
        reasons.append("authorization_device_ids_mismatch")
    try:
        gap = float(authorization.get("minimum_serial_command_gap_s"))
    except (TypeError, ValueError):
        gap = 0.0
    if gap < 1.0:
        reasons.append("authorization_serial_gap_below_1s")
    for key in ("no_sn_write", "no_device_id_write", "no_postgresql", "no_route_control"):
        if authorization.get(key) is not True:
            reasons.append(f"authorization_boundary_not_confirmed:{key}")
    return reasons


def _candidate_contract_reasons(
    *,
    matrix: Mapping[str, Any],
    matrix_sha: str,
    packet: Mapping[str, Any],
) -> list[str]:
    reasons: list[str] = []
    if matrix.get("production_fit_allowed") is not True:
        reasons.append("production_fit_not_allowed")
    if int(matrix.get("fit_ready_strategy_count") or 0) <= 0:
        reasons.append("fit_ready_candidate_missing")
    if packet.get("schema") != APPROVED_CANDIDATE_SCHEMA:
        reasons.append("approved_candidate_packet_schema_invalid")
    if packet.get("review_status") != "approved_for_unified_controlled_write_review":
        reasons.append("approved_candidate_packet_not_approved")
    if packet.get("fit_matrix_sha256") != matrix_sha:
        reasons.append("candidate_fit_matrix_sha256_mismatch")
    if packet.get("algorithm_profile_id") not in {LEGACY_PROFILE, NEW_PROFILE}:
        reasons.append("candidate_algorithm_profile_invalid")
    return reasons


def _state_machine_contract() -> list[dict[str, Any]]:
    return [
        {"order": 1, "stage": "fit_candidate_approval", "success_proves": "fit candidate reviewed", "does_not_prove": "device write"},
        {"order": 2, "stage": "old_getco_snapshot", "success_proves": "current device coefficient epoch captured", "does_not_prove": "candidate compatibility"},
        {"order": 3, "stage": "absolute_payload_derivation", "success_proves": "payload composed against current S5/S6 layer where required", "does_not_prove": "write authorization"},
        {"order": 4, "stage": "dual_review_authorization", "success_proves": "operator, reviewer, and approver bound exact artifacts", "does_not_prove": "write attempted"},
        {"order": 5, "stage": "paced_write_attempt", "success_proves": "writer attempted exact reviewed payload", "does_not_prove": "stored coefficients"},
        {"order": 6, "stage": "getco_readback", "success_proves": "stored coefficients match reviewed target", "does_not_prove": "physical calibration accuracy"},
        {"order": 7, "stage": "independent_short_reverify", "success_proves": "component-specific physical errors pass limits", "does_not_prove": "formal archive release"},
        {"order": 8, "stage": "rollback_or_archive_hold", "success_proves": "failure state preserved or successful evidence closed", "does_not_prove": "database import"},
    ]


def _failure_holds() -> list[dict[str, Any]]:
    return [
        {"trigger": "candidate_or_fit_lineage_invalid", "hold": "no_operation_plan", "rollback": "none"},
        {"trigger": "old_GETCO_snapshot_missing_or_changed", "hold": "do_not_write", "rollback": "none"},
        {"trigger": "authorization_invalid_or_artifact_hash_mismatch", "hold": "do_not_write", "rollback": "none"},
        {"trigger": "first_group_write_or_readback_failure", "hold": "stop_pair", "rollback": "restore_changed_group_from_old_snapshot"},
        {"trigger": "second_group_write_or_readback_failure", "hold": "pair_incomplete", "rollback": "restore_both_groups_in_reverse_order"},
        {"trigger": "S5_or_S6_clear_readback_not_neutral", "hold": "do_not_apply_absolute_target", "rollback": "restore_old_affine_layer"},
        {"trigger": "write_readback_pass_but_short_reverify_fail", "hold": "write_success_validation_failed", "rollback": "reviewed_manual_rollback_only"},
        {"trigger": "short_reverify_environment_safety_gate", "hold": "write_success_validation_not_completed", "rollback": "none_automatic"},
    ]


def _write_evidence_status(write_events: Mapping[str, Any], plan: Sequence[Mapping[str, Any]]) -> tuple[str, str]:
    if not plan:
        return "not_attempted", "not_attempted"
    events = [row for row in write_events.get("events") or [] if isinstance(row, Mapping)]
    if not events:
        return "not_attempted", "not_attempted"
    expected = {(str(row["device_id"]), str(row["group"])) for row in plan if row.get("action") == "write_reviewed_target"}
    written = {
        (_device_id(row.get("device_id")), str(row.get("group") or "").upper())
        for row in events
        if str(row.get("write_status") or "") == "success"
    }
    readback = {
        (_device_id(row.get("device_id")), str(row.get("group") or "").upper())
        for row in events
        if str(row.get("readback_status") or "") == "match"
    }
    return (
        "complete" if expected and expected <= written else "partial_or_failed",
        "complete" if expected and expected <= readback else "partial_or_failed",
    )


def _reverify_status(payload: Mapping[str, Any], *, readback_status: str) -> str:
    if readback_status != "complete":
        return "not_attempted"
    summaries = [row for row in payload.get("device_component_summary") or [] if isinstance(row, Mapping)]
    if not summaries:
        return "not_attempted"
    if all(str(row.get("status") or "") == "pass" for row in summaries):
        return "complete_pass"
    return "complete_fail_or_incomplete"


def build_v1_5_unified_controlled_write_reverify(
    *,
    production_fit_matrix_json: str | Path,
    approved_candidate_packet_json: str | Path | None = None,
    current_getco_snapshot_json: str | Path | None = None,
    authorization_json: str | Path | None = None,
    write_events_json: str | Path | None = None,
    short_reverify_json: str | Path | None = None,
) -> dict[str, Any]:
    matrix_path = Path(production_fit_matrix_json).resolve()
    matrix = _read_json(matrix_path)
    matrix_sha = _sha256(matrix_path)
    input_paths = {
        "production_fit_matrix": matrix_path,
        "approved_candidate_packet": Path(approved_candidate_packet_json).resolve() if approved_candidate_packet_json else None,
        "current_getco_snapshot": Path(current_getco_snapshot_json).resolve() if current_getco_snapshot_json else None,
        "authorization": Path(authorization_json).resolve() if authorization_json else None,
        "write_events": Path(write_events_json).resolve() if write_events_json else None,
        "short_reverify": Path(short_reverify_json).resolve() if short_reverify_json else None,
    }
    payloads: dict[str, dict[str, Any]] = {"production_fit_matrix": matrix}
    bindings: list[dict[str, Any]] = []
    global_reasons: list[str] = []
    for role, path in input_paths.items():
        if path is None:
            bindings.append({"role": role, "path": "", "status": "not_supplied", "sha256": ""})
            continue
        if not path.is_file():
            global_reasons.append(f"input_missing:{role}")
            bindings.append({"role": role, "path": str(path), "status": "missing", "sha256": ""})
            continue
        try:
            payloads[role] = _read_json(path)
        except (OSError, ValueError, json.JSONDecodeError):
            global_reasons.append(f"input_invalid:{role}")
            bindings.append({"role": role, "path": str(path), "status": "invalid", "sha256": _sha256(path)})
            continue
        bindings.append({"role": role, "path": str(path), "status": "bound", "sha256": _sha256(path)})

    packet = payloads.get("approved_candidate_packet", {})
    snapshot = payloads.get("current_getco_snapshot", {})
    authorization = payloads.get("authorization", {})
    candidate_path = input_paths["approved_candidate_packet"]
    candidate_sha = _sha256(candidate_path) if candidate_path and candidate_path.is_file() else ""
    profile = str(packet.get("algorithm_profile_id") or LEGACY_PROFILE)
    if approved_candidate_packet_json:
        contract_reasons = _candidate_contract_reasons(
            matrix=matrix, matrix_sha=matrix_sha, packet=packet
        )
    else:
        contract_reasons = []
        if matrix.get("production_fit_allowed") is not True:
            contract_reasons.append("production_fit_not_allowed")
        if int(matrix.get("fit_ready_strategy_count") or 0) <= 0:
            contract_reasons.append("fit_ready_candidate_missing")
        contract_reasons.append("approved_candidate_packet_missing")
    if snapshot.get("schema") != SNAPSHOT_SCHEMA:
        contract_reasons.append("current_getco_snapshot_missing_or_invalid")

    candidates = _candidate_rows(packet)
    device_ids = sorted({_device_id(row.get("device_id")) for row in candidates if _device_id(row.get("device_id"))})
    auth_reasons = (
        _authorization_reasons(
            authorization,
            matrix_sha=matrix_sha,
            candidate_sha=candidate_sha,
            device_ids=device_ids,
        )
        if authorization_json
        else ["authorization_missing"]
    )
    snapshot_index = _snapshot_index(snapshot)
    snapshot_identity = _snapshot_identity(snapshot)
    contract_by_group = {row["group"]: row for row in GROUP_CONTRACTS}
    group_reviews: list[dict[str, Any]] = []
    operation_plan: list[dict[str, Any]] = []

    for group in ("SENCO7", "SENCO8"):
        neutral = list(contract_by_group[group]["neutral_values"])
        readback = f"GETCO{group[-1]}"
        for device_id in device_ids:
            values = snapshot_index.get((device_id, readback), [])
            if len(values) != len(neutral):
                global_reasons.append(
                    f"temperature_neutral_snapshot_missing:{device_id}:{group}"
                )
            elif any(abs(a - b) > 1e-12 for a, b in zip(values, neutral)):
                global_reasons.append(
                    f"temperature_neutral_snapshot_failed:{device_id}:{group}"
                )

    candidate_groups_by_device: dict[str, set[str]] = {}
    candidate_group_keys: set[tuple[str, str]] = set()
    seen_device_rows: set[str] = set()
    seen_sn_codes: set[str] = set()
    for device in packet.get("device_candidates") or []:
        if not isinstance(device, Mapping):
            continue
        device_id = _device_id(device.get("device_id"))
        sn_code = str(device.get("sn_code") or device.get("device_code") or "").strip()
        if not device_id:
            global_reasons.append("candidate_device_id_missing")
        elif device_id in seen_device_rows:
            global_reasons.append(f"candidate_device_id_duplicate:{device_id}")
        else:
            seen_device_rows.add(device_id)
        if len(sn_code) != 8 or not sn_code.isdigit():
            global_reasons.append(f"candidate_sn_code_invalid:{device_id or 'missing'}")
        elif sn_code in seen_sn_codes:
            global_reasons.append(f"candidate_sn_code_duplicate:{sn_code}")
        else:
            seen_sn_codes.add(sn_code)
        snapshot_sn = snapshot_identity.get(device_id, "")
        if not snapshot_sn:
            global_reasons.append(f"snapshot_sn_code_missing:{device_id or 'missing'}")
        elif snapshot_sn != sn_code:
            global_reasons.append(f"snapshot_sn_code_mismatch:{device_id}:{sn_code}:{snapshot_sn}")
    for candidate in candidates:
        device_id = candidate["device_id"]
        group = str(candidate.get("group") or "").upper()
        key = (device_id, group)
        if key in candidate_group_keys:
            global_reasons.append(f"candidate_group_duplicate:{device_id}:{group}")
        candidate_group_keys.add(key)
        candidate_groups_by_device.setdefault(device_id, set()).add(group)
    for device_id, groups in candidate_groups_by_device.items():
        if bool({"SENCO1", "SENCO3"} & groups) and not {"SENCO1", "SENCO3"} <= groups:
            global_reasons.append(f"paired_candidate_incomplete:{device_id}:SENCO1_SENCO3")
        if bool({"SENCO2", "SENCO4"} & groups) and not {"SENCO2", "SENCO4"} <= groups:
            global_reasons.append(f"paired_candidate_incomplete:{device_id}:SENCO2_SENCO4")
        if profile == NEW_PROFILE and bool({"SENCOA", "SENCOB"} & groups) and not {"SENCOA", "SENCOB"} <= groups:
            global_reasons.append(f"paired_candidate_incomplete:{device_id}:SENCOA_SENCOB")

    for candidate in candidates:
        device_id = candidate["device_id"]
        group = str(candidate.get("group") or "").strip().upper()
        contract = contract_by_group.get(group)
        reasons: list[str] = []
        if not contract:
            reasons.append("unsupported_senco_group")
            group_reviews.append({"device_id": device_id, "group": group, "status": "blocked", "reasons": ";".join(reasons)})
            continue
        if profile not in contract["profiles"]:
            reasons.append("group_not_applicable_to_algorithm_profile")
        if candidate.get("candidate_status") != "approved":
            reasons.append("candidate_group_not_approved")
        if group in {"SENCO7", "SENCO8"}:
            reasons.append("temperature_coefficients_are_neutral_only_not_fit_candidates")
        if contract.get("writer_state") == "blocked_design_only":
            reasons.append("sencoa_sencob_real_writer_not_implemented")
        raw_values = candidate.get("candidate_layer_values") if contract.get("composed_layer") else candidate.get("candidate_values")
        values = _finite_values(raw_values)
        if len(values) != int(contract["width"]):
            reasons.append(f"candidate_width_{len(values)}_expected_{contract['width']}")
        old_values = snapshot_index.get((device_id, str(contract["readback"])), [])
        if len(old_values) != int(contract["width"]):
            reasons.append(f"old_snapshot_width_{len(old_values)}_expected_{contract['width']}")
        final_values = values
        if contract.get("composed_layer") and len(old_values) == 2 and len(values) == 2:
            final_values = _compose_affine(old_values, values)
        if group == "SENCO9" and len(values) == 4:
            model_kind = str(candidate.get("model_kind") or "offset_only")
            if model_kind == "offset_only" and not (
                abs(values[1] - 1.0) <= 1e-12 and abs(values[2]) <= 1e-12 and abs(values[3]) <= 1e-12
            ):
                reasons.append("senco9_offset_only_shape_invalid")
            if model_kind == "linear" and candidate.get("linear_exception_evidence_status") != "approved":
                reasons.append("senco9_linear_exception_not_approved")
            if model_kind not in {"offset_only", "linear"}:
                reasons.append("senco9_model_kind_invalid")

        blocked = sorted(set(contract_reasons + auth_reasons + global_reasons + reasons))
        group_reviews.append(
            {
                "device_id": device_id,
                "group": group,
                "readback_group": contract["readback"],
                "component": contract["component"],
                "role": contract["role"],
                "status": "review_ready" if not blocked else "blocked",
                "old_values": json.dumps(old_values),
                "candidate_values": json.dumps(values),
                "absolute_target_values": json.dumps(final_values),
                "payload_format": contract["format"],
                "reasons": ";".join(blocked),
            }
        )
        if blocked:
            continue
        if contract.get("clear_command"):
            operation_plan.append({"order": len(operation_plan) + 1, "device_id": device_id, "group": group, "action": "clear_existing_affine_layer", "command_contract": f"{contract['clear_command']},YGAS,FFF", "execution_allowed": False})
            operation_plan.append({"order": len(operation_plan) + 1, "device_id": device_id, "group": group, "action": "verify_neutral_readback", "command_contract": f"{contract['readback']},YGAS,FFF", "execution_allowed": False})
        formatted = [_format_value(value, str(contract["format"])) for value in final_values]
        operation_plan.append({"order": len(operation_plan) + 1, "device_id": device_id, "group": group, "action": "write_reviewed_target", "command_contract": f"{group},YGAS,FFF," + ",".join(formatted), "execution_allowed": False})
        operation_plan.append({"order": len(operation_plan) + 1, "device_id": device_id, "group": group, "action": "verify_getco_readback", "command_contract": f"{contract['readback']},YGAS,FFF", "execution_allowed": False})

    plan_ready = bool(operation_plan) and not global_reasons and all(
        row["status"] == "review_ready" for row in group_reviews
    )
    if plan_ready:
        write_status, readback_status = _write_evidence_status(
            payloads.get("write_events", {}), operation_plan
        )
    else:
        write_status, readback_status = "not_authorized", "not_authorized"
    reverify_status = _reverify_status(
        payloads.get("short_reverify", {}), readback_status=readback_status
    )
    if not candidates:
        overall_status = "blocked_no_fit_approved_candidate"
    elif plan_ready:
        overall_status = "unified_controlled_write_plan_review_ready_execution_locked"
    else:
        overall_status = "unified_controlled_write_review_required"
    frozen_gap_closed = (
        plan_ready
        and write_status == "complete"
        and readback_status == "complete"
        and reverify_status == "complete_pass"
    )
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": overall_status,
        "unified_contract_available": True,
        "frozen_gap_id": "unified_controlled_write_readback_reverify",
        "frozen_gap_program_contract_closed": True,
        "frozen_gap_production_evidence_closed": frozen_gap_closed,
        "algorithm_profile_id": profile,
        "fit_baseline": "0613 V1.5 fitting path",
        "mature_route_baseline": "0620/0621 clean-worktree mature physical route path",
        "fit_matrix_status": matrix.get("overall_status"),
        "production_fit_allowed": matrix.get("production_fit_allowed") is True,
        "fit_ready_strategy_count": int(matrix.get("fit_ready_strategy_count") or 0),
        "candidate_device_count": len(device_ids),
        "candidate_group_count": len(candidates),
        "operation_plan_ready": plan_ready,
        "operation_plan_count": len(operation_plan),
        "write_transaction_status": write_status,
        "getco_readback_status": readback_status,
        "physical_short_reverify_status": reverify_status,
        "write_success_separate_from_validation_success": True,
        "review_reasons": sorted(set(global_reasons + contract_reasons + auth_reasons)),
        "evidence_bindings": bindings,
        "group_contracts": [
            {**row, "profiles": ";".join(row["profiles"]), "neutral_values": json.dumps(row.get("neutral_values") or [])}
            for row in GROUP_CONTRACTS
        ],
        "group_reviews": group_reviews,
        "state_machine": _state_machine_contract(),
        "operation_plan": operation_plan,
        "failure_holds": _failure_holds(),
        "evidence_source": EVIDENCE_SOURCE,
        "not_real_acceptance_evidence": True,
        "opens_com_ports": False,
        "controlled_write_allowed": False,
        "writes_coefficients": False,
        "writes_sn_or_device_code": False,
        "connects_postgresql": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
    }


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(str(key))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _markdown(model: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# V1.5 统一系数受控写入、读回与短复验合同",
            "",
            f"- overall_status: `{model['overall_status']}`",
            f"- fit baseline: `{model['fit_baseline']}`",
            f"- mature route baseline: `{model['mature_route_baseline']}`",
            f"- production_fit_allowed: `{str(model['production_fit_allowed']).lower()}`",
            f"- fit_ready_strategy_count: `{model['fit_ready_strategy_count']}`",
            f"- operation_plan_count: `{model['operation_plan_count']}`",
            f"- write_transaction_status: `{model['write_transaction_status']}`",
            f"- getco_readback_status: `{model['getco_readback_status']}`",
            f"- physical_short_reverify_status: `{model['physical_short_reverify_status']}`",
            "",
            "本工件只统一证据和状态机，不执行串口命令。当前历史数据没有合格拟合候选，因此操作计划为空，写入、读回、复验继续锁定。",
            "",
            "## 物理边界",
            "",
            "- S1/S3 与 S2/S4 必须成对评审，使用科学计数法写入合同。",
            "- S5/S6 是最终仿射层，必须读取当前 GETCO5/6 并按层叠关系计算绝对目标；清除、读回中性、写入、再读回缺一不可。",
            "- S7/S8 不做温度校准，只允许保持 `[0,1,0,0]` 中性状态。",
            "- S9 默认 offset-only；linear 只允许有明确特例证据的设备。",
            "- SENCOA/B 只属于新算法 R0(T)，真实 writer 仍是 blocked-design-only。",
            "- 写入成功、GETCO 读回成功和独立物理复验成功是三个不同结论。",
            "",
            "## 当前 blockers",
            "",
            *[f"- `{reason}`" for reason in model.get("review_reasons") or ["none"]],
            "",
        ]
    )


def write_v1_5_unified_controlled_write_reverify(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths = {
        "manifest": out / "v1_5_unified_controlled_write_readback_reverify.json",
        "summary": out / "V1_5_UNIFIED_CONTROLLED_WRITE_READBACK_REVERIFY.md",
        "bindings": out / "v1_5_unified_controlled_write_evidence_bindings.csv",
        "groups": out / "v1_5_unified_controlled_write_group_contracts.csv",
        "reviews": out / "v1_5_unified_controlled_write_group_reviews.csv",
        "state_machine": out / "v1_5_unified_controlled_write_state_machine.csv",
        "operation_plan": out / "v1_5_unified_controlled_write_operation_plan.csv",
        "failure_holds": out / "v1_5_unified_controlled_write_failure_holds.csv",
    }
    paths["manifest"].write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    paths["summary"].write_text(_markdown(model), encoding="utf-8")
    _write_csv(paths["bindings"], model.get("evidence_bindings") or [])
    _write_csv(paths["groups"], model.get("group_contracts") or [])
    _write_csv(paths["reviews"], model.get("group_reviews") or [])
    _write_csv(paths["state_machine"], model.get("state_machine") or [])
    _write_csv(paths["operation_plan"], model.get("operation_plan") or [])
    _write_csv(paths["failure_holds"], model.get("failure_holds") or [])
    return paths


__all__ = [
    "APPROVED_CANDIDATE_SCHEMA",
    "AUTHORIZATION_SCHEMA",
    "CONFIRMATION_TEXT",
    "EVIDENCE_SOURCE",
    "GROUP_CONTRACTS",
    "SCHEMA",
    "SNAPSHOT_SCHEMA",
    "build_v1_5_unified_controlled_write_reverify",
    "write_v1_5_unified_controlled_write_reverify",
]
