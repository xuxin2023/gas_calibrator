"""Offline contract for a future controlled V1.5 read-only COM execution packet.

This module does not implement real COM execution. It consumes the controlled
read-only COM blocked-executor evidence and freezes the operator/reviewer
payload, serial pacing, and no-write boundaries that a future executor must
satisfy before it may read analyzer identity, GETCO, runtime, or CHECK data.
"""

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "v1_5_formal_readonly_com_execution_contract_v1"
CONTROLLED_BLOCKED_EXECUTOR_SCHEMA = (
    "v1_5_formal_initialization_readonly_com_preflight_controlled_blocked_executor_v1"
)
CONTROLLED_BLOCKED_STATUS = "blocked_pending_controlled_readonly_com_preflight_executor_implementation"
READY_STATUS = "ready_for_readonly_com_execution_contract_review"
REVIEW_STATUS = "review_required"
MIN_SERIAL_COMMAND_GAP_S = 1.0


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _load_json(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return {}
    source = Path(path)
    if not source.exists() or not source.is_file():
        return {}
    payload = json.loads(source.read_text(encoding="utf-8-sig"))
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8-sig")


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows([dict(row) for row in rows])


def _write_markdown(path: Path, model: Mapping[str, Any]) -> None:
    lines = [
        "# V1.5 Read-Only COM Execution Contract",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- contract_ready: `{model.get('contract_ready')}`",
        f"- execution_supported: `{model.get('execution_supported')}`",
        f"- read_only_real_com_execution_allowed: `{model.get('read_only_real_com_execution_allowed')}`",
        f"- opens_com_ports: `{model.get('opens_com_ports')}`",
        f"- writes_sn: `{model.get('writes_sn')}`",
        f"- writes_coefficients: `{model.get('writes_coefficients')}`",
        f"- connects_postgresql: `{model.get('connects_postgresql')}`",
        "",
        "This artifact is a contract-only review sidecar. It is not real COM execution evidence.",
        "",
        "## Required future execution packet",
    ]
    for row in model.get("execution_packet_contract") or []:
        lines.append(f"- `{row.get('field_or_flag')}`: {row.get('contract')}")
    lines.extend(["", "## Denied actions"])
    for row in model.get("denied_action_contract") or []:
        lines.append(f"- `{row.get('action')}`: {row.get('failure_policy')}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")


def _controlled_blocked_executor_reasons(payload: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    if not payload:
        return ["controlled_blocked_executor_missing"]
    if payload.get("schema") != CONTROLLED_BLOCKED_EXECUTOR_SCHEMA:
        reasons.append(f"schema={payload.get('schema') or 'missing'}")
    if payload.get("overall_status") != CONTROLLED_BLOCKED_STATUS:
        reasons.append(f"overall_status={payload.get('overall_status') or 'missing'}")
    if int(payload.get("review_required_count") or 0):
        reasons.append(f"review_required_count={payload.get('review_required_count')}")
    if int(payload.get("blocker_count") or 0):
        reasons.append(f"blocker_count={payload.get('blocker_count')}")
    if payload.get("blocked_executor_ready") is not True:
        reasons.append(f"blocked_executor_ready={payload.get('blocked_executor_ready')!r}")
    if float(payload.get("minimum_serial_command_gap_s") or 0.0) < MIN_SERIAL_COMMAND_GAP_S:
        reasons.append(f"minimum_serial_command_gap_s={payload.get('minimum_serial_command_gap_s')!r}")
    for field in (
        "execution_supported",
        "live_execution_allowed",
        "read_only_real_com_execution_allowed",
        "controlled_write_execution_allowed",
        "real_com_execution_allowed",
        "execute_flag_allowed",
        "opens_com_ports",
        "connects_postgresql",
        "controls_pressure",
        "controls_water_or_gas_routes",
        "writes_sn",
        "writes_device_id",
        "writes_coefficients",
        "database_written",
        "formal_release_allowed",
        "database_import_allowed",
    ):
        if payload.get(field) is not False:
            reasons.append(f"boundary_{field}={payload.get(field)!r}")
    if payload.get("not_real_acceptance_evidence") is not True:
        reasons.append(f"not_real_acceptance_evidence={payload.get('not_real_acceptance_evidence')!r}")
    return reasons


def build_v1_5_formal_readonly_com_execution_contract(
    *,
    formal_initialization_readonly_com_preflight_controlled_blocked_executor_json: str | Path | None,
) -> dict[str, Any]:
    blocked_path = (
        Path(formal_initialization_readonly_com_preflight_controlled_blocked_executor_json).resolve()
        if formal_initialization_readonly_com_preflight_controlled_blocked_executor_json
        else None
    )
    blocked_payload = _load_json(blocked_path)
    source_reasons = _controlled_blocked_executor_reasons(blocked_payload)

    execution_packet_contract = [
        {
            "field_or_flag": "--execute-read-only-real-com",
            "required": True,
            "contract": "The future real reader may only run when this exact flag is present.",
            "failure_policy": "abort_before_opening_any_com_port",
        },
        {
            "field_or_flag": "authorization_id",
            "required": True,
            "contract": "A unique authorization id must be serialized into the evidence packet.",
            "failure_policy": "hold_without_com",
        },
        {
            "field_or_flag": "operator_confirmation_text",
            "required": True,
            "contract": "The operator confirmation must bind run id, active devices, reviewed ports, no-write scope, and date.",
            "failure_policy": "hold_without_com",
        },
        {
            "field_or_flag": "reviewer",
            "required": True,
            "contract": "The reviewer must be recorded before real analyzer contact.",
            "failure_policy": "hold_without_com",
        },
        {
            "field_or_flag": "approver",
            "required": True,
            "contract": "The approver must be recorded and must not be the same empty implicit value as reviewer.",
            "failure_policy": "hold_without_com",
        },
        {
            "field_or_flag": "reviewed_port_inventory_json",
            "required": True,
            "contract": "Only ports present in the reviewed inventory may be opened by a future executor.",
            "failure_policy": "abort_before_opening_any_com_port",
        },
        {
            "field_or_flag": "active_analyzer_list_json",
            "required": True,
            "contract": "The active analyzer list must contain 1 to 6 analyzers and must not assume a six-device round.",
            "failure_policy": "hold_without_com",
        },
    ]

    denied_action_contract = [
        {
            "action": "--execute",
            "failure_policy": "reject_generic_execute_flag",
            "physical_meaning": "Read-only COM preflight must use a specific real-read flag, not a generic execute unlock.",
        },
        {
            "action": "--allow-real-com",
            "failure_policy": "reject_ambiguous_real_com_unlock",
            "physical_meaning": "The future unlock must be scoped to read-only initialization COM evidence.",
        },
        {
            "action": "--execute-controlled-writes",
            "failure_policy": "reject_any_write_unlock",
            "physical_meaning": "SN/device_code writes, SENCO writes, runtime writes, and database imports are out of scope.",
        },
        {
            "action": "postgresql_dsn",
            "failure_policy": "reject_database_connection_inputs",
            "physical_meaning": "Database import is governed by the PostgreSQL 18 import chain, not by read-only COM preflight.",
        },
        {
            "action": "route_or_pressure_control",
            "failure_policy": "reject_route_pressure_inputs",
            "physical_meaning": "Pressure, CO2, H2O, and route control stay in mature V1.5 route stages.",
        },
    ]

    future_read_sequence_contract = [
        {
            "order": 1,
            "read": "protocol_device_id",
            "serial_spacing_s": MIN_SERIAL_COMMAND_GAP_S,
            "hold_policy": "hold_on_timeout_or_unparseable_frame",
        },
        {
            "order": 2,
            "read": "sn_code_device_code",
            "serial_spacing_s": MIN_SERIAL_COMMAND_GAP_S,
            "hold_policy": "hold_on_missing_non_numeric_or_duplicate_sn",
        },
        {
            "order": 3,
            "read": "getco1_through_getco9_epoch0",
            "serial_spacing_s": MIN_SERIAL_COMMAND_GAP_S,
            "hold_policy": "hold_on_missing_group_raw_response_or_parse_error",
        },
        {
            "order": 4,
            "read": "runtime_state_mode2_1hz_average",
            "serial_spacing_s": MIN_SERIAL_COMMAND_GAP_S,
            "hold_policy": "hold_on_runtime_mismatch_without_writing_repair",
        },
        {
            "order": 5,
            "read": "check_monitor_for_check_capable_only",
            "serial_spacing_s": MIN_SERIAL_COMMAND_GAP_S,
            "hold_policy": "old_algorithm_skip_is_allowed; check_capable_failure_requires_review",
        },
    ]

    source_ready = not source_reasons
    model: dict[str, Any] = {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": READY_STATUS if source_ready else REVIEW_STATUS,
        "blocker_count": 0,
        "review_required_count": 0 if source_ready else 1,
        "contract_ready": source_ready,
        "production_state": "contract_only",
        "source_controlled_blocked_executor_path": str(blocked_path) if blocked_path else "",
        "source_controlled_blocked_executor_status": str(blocked_payload.get("overall_status") or ""),
        "source_review_reasons": source_reasons,
        "execution_supported": False,
        "live_execution_allowed": False,
        "read_only_real_com_execution_allowed": False,
        "controlled_write_execution_allowed": False,
        "real_com_execution_allowed": False,
        "execute_flag_allowed": False,
        "opens_com_ports": False,
        "connects_postgresql": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "database_written": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
        "required_future_read_only_real_com_flag": "--execute-read-only-real-com",
        "forbidden_future_flags": "--execute;--allow-real-com;--execute-controlled-writes",
        "minimum_serial_command_gap_s": MIN_SERIAL_COMMAND_GAP_S,
        "supports_old_algorithm_check_skip": True,
        "supported_active_analyzer_count": "1_to_6",
        "execution_packet_contract": execution_packet_contract,
        "denied_action_contract": denied_action_contract,
        "future_read_sequence_contract": future_read_sequence_contract,
    }
    return model


def write_v1_5_formal_readonly_com_execution_contract_outputs(
    model: Mapping[str, Any],
    *,
    output_dir: str | Path,
) -> dict[str, str]:
    out = Path(output_dir).resolve()
    outputs = {
        "json": out / "v1_5_formal_readonly_com_execution_contract.json",
        "markdown": out / "V1_5_FORMAL_READONLY_COM_EXECUTION_CONTRACT.md",
        "execution_packet_contract_csv": out / "v1_5_formal_readonly_com_execution_packet_contract.csv",
        "denied_action_contract_csv": out / "v1_5_formal_readonly_com_denied_action_contract.csv",
        "future_read_sequence_contract_csv": out / "v1_5_formal_readonly_com_future_read_sequence_contract.csv",
    }
    _write_json(outputs["json"], model)
    _write_markdown(outputs["markdown"], model)
    _write_csv(outputs["execution_packet_contract_csv"], model.get("execution_packet_contract") or [])
    _write_csv(outputs["denied_action_contract_csv"], model.get("denied_action_contract") or [])
    _write_csv(outputs["future_read_sequence_contract_csv"], model.get("future_read_sequence_contract") or [])
    return {key: str(path) for key, path in outputs.items()}
