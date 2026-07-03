"""Offline design review for a future V1.5 initialization executor.

This package is deliberately non-executable. It defines the authorization,
real-COM, write/readback, and hold/rollback contracts that a later controlled
initialization executor must satisfy before it may touch analyzers.
"""

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "v1_5_formal_initialization_controlled_executor_design_v1"
BLOCKED_EXECUTOR_SCHEMA = "v1_5_formal_initialization_blocked_executor_v1"
READY_STATUS = "ready_for_controlled_initialization_executor_design_review"
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


def _blocked_executor_review_reasons(payload: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    if not payload:
        return ["blocked_executor_evidence_missing"]
    if payload.get("schema") != BLOCKED_EXECUTOR_SCHEMA:
        reasons.append(f"blocked_executor_schema={payload.get('schema') or 'missing'}")
    if payload.get("blocked_executor_ready") is not True:
        reasons.append(f"blocked_executor_ready={payload.get('blocked_executor_ready')!r}")
    for field in (
        "execution_supported",
        "live_execution_allowed",
        "read_only_real_com_execution_allowed",
        "controlled_write_execution_allowed",
        "real_com_execution_allowed",
        "execute_flag_allowed",
        "opens_com_ports",
        "connects_postgresql",
        "controls_water_or_gas_routes",
        "controls_pressure",
        "writes_sn",
        "writes_device_id",
        "writes_coefficients",
        "database_written",
    ):
        if payload.get(field) is not False:
            reasons.append(f"blocked_executor_boundary_{field}={payload.get(field)!r}")
    return reasons


def build_v1_5_formal_initialization_controlled_executor_design(
    *,
    formal_initialization_blocked_executor_json: str | Path | None = None,
) -> dict[str, Any]:
    """Build a no-COM design package for a future controlled initialization executor."""

    blocked_path = (
        Path(formal_initialization_blocked_executor_json).resolve()
        if formal_initialization_blocked_executor_json
        else None
    )
    blocked_payload = _load_json(blocked_path)
    review_reasons = _blocked_executor_review_reasons(blocked_payload)

    authorization_contract = [
        {
            "gate": "explicit_controlled_initialization_flag",
            "required": True,
            "future_flag": "--execute-controlled-initialization",
            "contract": "Real initialization must be impossible unless a future controlled executor explicitly exposes and receives this flag.",
        },
        {
            "gate": "read_only_real_com_unlock",
            "required": True,
            "future_flag": "--execute-read-only-real-com",
            "contract": "Read-only analyzer contact must be separately unlocked before protocol ID, SN, GETCO, runtime setup, or CHECK reads.",
        },
        {
            "gate": "controlled_write_unlock",
            "required": True,
            "future_flag": "--execute-controlled-writes",
            "contract": "SN/device_code and auxiliary SENCO writes require an additional controlled-write unlock.",
        },
        {
            "gate": "operator_confirmation_text",
            "required": True,
            "future_field": "operator_confirmation_text",
            "contract": "Operator confirmation must bind run_id, active device count, SN/device_code targets, and serial-port mapping.",
        },
        {
            "gate": "reviewer_approver_dual_authorization",
            "required": True,
            "future_fields": "reviewer;approver;authorization_id",
            "contract": "Reviewer and approver must be present, distinct, and serialized in the initialization evidence.",
        },
        {
            "gate": "active_device_scope",
            "required": True,
            "future_scope": "1_to_6_active_analyzers",
            "contract": "The future executor must use the reviewed active device list, not assume six analyzers.",
        },
    ]

    real_com_contract = [
        {
            "step": "open_reviewed_serial_ports",
            "order": 1,
            "required": True,
            "physical_meaning": "Open only the reviewed active analyzer ports after read-only real-COM authorization.",
            "failure_policy": "abort_before_any_write",
        },
        {
            "step": "mode2_1hz_runtime_setup",
            "order": 2,
            "required": True,
            "physical_meaning": "Set analyzers to MODE2, 1Hz active upload, and AVERAGE1/2 filter policy before identity and GETCO evidence.",
            "failure_policy": "stop_affected_device_and_record_runtime_setup_failure",
        },
        {
            "step": "identity_binding",
            "order": 3,
            "required": True,
            "physical_meaning": "Bind COM/GA transport to protocol device ID plus 8-digit SN/device_code primary identity.",
            "failure_policy": "abort_all_writes_on_any_identity_mismatch",
        },
        {
            "step": "getco_epoch0_snapshot",
            "order": 4,
            "required": True,
            "physical_meaning": "Freeze GETCO1-9 before any neutralization or write so all later changes are traceable.",
            "failure_policy": "abort_writes_for_device_without_complete_snapshot",
        },
        {
            "step": "chamber_temperature_stable_then_check",
            "order": 5,
            "required": True,
            "physical_meaning": "After all active analyzer chamber temperatures are stable, read CHECK only for CHECK-capable/new-algorithm analyzers.",
            "failure_policy": "record_check_review_required_without_writing_coefficients",
        },
    ]

    controlled_write_contract = [
        {
            "write_scope": "sn_device_code",
            "required": True,
            "allowed_when": "new_device_or_missing_sn_and_reviewed_8_digit_numeric_target",
            "readback": "SN,YGAS,FFF must match target before database identity bundle can be release-ready",
            "excluded": "do not overwrite an existing matching production SN/device_code",
        },
        {
            "write_scope": "runtime_mode_filter",
            "required": True,
            "allowed_when": "read-only runtime setup evidence is present",
            "readback": "MODE2, 1Hz, and AVERAGE1/2 evidence preserved in runtime setup artifact",
            "excluded": "do not lower serial command spacing below 1.0s",
        },
        {
            "write_scope": "senco5_senco6_output_linear_neutral",
            "required": True,
            "allowed_when": "old GETCO5/GETCO6 snapshot exists and policy requires neutral output layer before fitting",
            "readback": "GETCO5/GETCO6 after write must match neutral target",
            "excluded": "do not mix old output trims with newly fitted S1/S2/S3/S4 candidates",
        },
        {
            "write_scope": "senco7_senco8_temperature_neutral",
            "required": True,
            "allowed_when": "all algorithms keep temperature calibration disabled at initialization",
            "readback": "GETCO7/GETCO8 must confirm neutral coefficients",
            "excluded": "do not perform temperature calibration in initialization for old or new algorithm devices",
        },
        {
            "write_scope": "senco9_pressure",
            "required": True,
            "allowed_when": "pressure channel evaluation has approved a controlled S9 update",
            "readback": "GETCO9 and pressure-channel reverify evidence",
            "excluded": "pressure S9 is not fitted from CO2/H2O component residuals",
        },
        {
            "write_scope": "component_and_r0_coefficients",
            "required": False,
            "allowed_when": "never in initialization",
            "readback": "handled by later controlled coefficient write stages",
            "excluded": "S1/S2/S3/S4/S5/S6/SENCOA/SENCOB production candidates are outside initialization executor scope",
        },
    ]

    readback_contract = [
        {
            "readback": "sn_device_code",
            "required": True,
            "expected": "8 numeric digits; device_code equals sn_code; protocol ID retained as compatibility alias",
        },
        {
            "readback": "getco_epoch0_and_after_write",
            "required": True,
            "expected": "old GETCO1-9 snapshot plus after-write GETCO rows for every controlled write target",
        },
        {
            "readback": "runtime_setup",
            "required": True,
            "expected": "MODE2, 1Hz, AVERAGE1/2, and >=1s command-spacing evidence",
        },
        {
            "readback": "check_monitor",
            "required": True,
            "expected": "CHECK monitor CSV only for CHECK-capable/new-algorithm analyzers after all active chambers are stable",
        },
        {
            "readback": "database_preflight_bundle",
            "required": True,
            "expected": "PostgreSQL 18 dry-run bundle with SN/device_code unique identity; no connection or import here",
        },
    ]

    hold_contract = [
        {
            "trigger": "identity_mismatch",
            "hold_action": "abort all writes and preserve raw identity frames",
            "release_policy": "manual traceability review before retry",
        },
        {
            "trigger": "write_readback_mismatch",
            "hold_action": "stop affected device, do not continue to route readiness, and require operator review",
            "release_policy": "controlled rollback or corrected write package required",
        },
        {
            "trigger": "serial_timeout_or_frame_schema_mismatch",
            "hold_action": "close affected COM port and mark device initialization incomplete",
            "release_policy": "rerun read-only probe only after cabling/protocol review",
        },
        {
            "trigger": "check_monitor_out_of_range",
            "hold_action": "record CHECK review_required without changing coefficients",
            "release_policy": "hardware/thermal review before new-algorithm formal flow",
        },
        {
            "trigger": "postgresql_preflight_missing",
            "hold_action": "do not enter open-flow sampling as production-ready",
            "release_policy": "refresh database dry-run/preflight evidence; do not auto-import",
        },
    ]

    boundary_gates = [
        {
            "gate": "design_only_no_com",
            "status": "pass",
            "evidence": "opens_com_ports=false; execution_supported=false; live_execution_allowed=false",
        },
        {
            "gate": "blocked_executor_consumed",
            "status": "review_required" if review_reasons else "pass",
            "evidence": ";".join(review_reasons) if review_reasons else str(blocked_path),
        },
        {
            "gate": "future_live_execution_still_locked",
            "status": "pass",
            "evidence": "current package does not add --execute-controlled-initialization runtime behavior",
        },
        {
            "gate": "serial_command_gap_contract",
            "status": "pass",
            "evidence": f"minimum_serial_command_gap_s={MIN_SERIAL_COMMAND_GAP_S:g}",
        },
        {
            "gate": "no_route_pressure_database_side_effects",
            "status": "pass",
            "evidence": "controls_pressure=false; controls_water_or_gas_routes=false; connects_postgresql=false; database_written=false",
        },
    ]
    review_required_count = sum(1 for row in boundary_gates if row["status"] == "review_required")
    manifest = {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": READY_STATUS if review_required_count == 0 else REVIEW_STATUS,
        "blocker_count": 0,
        "review_required_count": review_required_count,
        "production_state": "blocked_design_only",
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
        "minimum_serial_command_gap_s": MIN_SERIAL_COMMAND_GAP_S,
        "supported_active_analyzer_count": "1_to_6",
        "formal_initialization_blocked_executor_json": str(blocked_path) if blocked_path else "",
        "required_future_execute_flag": "--execute-controlled-initialization",
        "required_future_read_only_real_com_flag": "--execute-read-only-real-com",
        "required_future_controlled_write_flag": "--execute-controlled-writes",
        "next_action": (
            "Keep live initialization locked. Implement a separate controlled executor only after this "
            "authorization, real-COM, write/readback, CHECK, and hold contract is reviewed."
        ),
    }
    return {
        "manifest": manifest,
        "authorization_contract": authorization_contract,
        "real_com_contract": real_com_contract,
        "controlled_write_contract": controlled_write_contract,
        "readback_contract": readback_contract,
        "hold_contract": hold_contract,
        "boundary_gates": boundary_gates,
    }


def write_v1_5_formal_initialization_controlled_executor_design(
    output_dir: str | Path,
    *,
    formal_initialization_blocked_executor_json: str | Path | None = None,
) -> dict[str, str]:
    tables = build_v1_5_formal_initialization_controlled_executor_design(
        formal_initialization_blocked_executor_json=formal_initialization_blocked_executor_json,
    )
    out = Path(output_dir)
    outputs = {
        "manifest": out / "v1_5_formal_initialization_controlled_executor_design.json",
        "authorization_contract": out / "v1_5_formal_initialization_controlled_executor_authorization_contract.csv",
        "real_com_contract": out / "v1_5_formal_initialization_controlled_executor_real_com_contract.csv",
        "controlled_write_contract": out
        / "v1_5_formal_initialization_controlled_executor_controlled_write_contract.csv",
        "readback_contract": out / "v1_5_formal_initialization_controlled_executor_readback_contract.csv",
        "hold_contract": out / "v1_5_formal_initialization_controlled_executor_hold_contract.csv",
        "boundary_gates": out / "v1_5_formal_initialization_controlled_executor_boundary_gates.csv",
        "summary": out / "V1_5_FORMAL_INITIALIZATION_CONTROLLED_EXECUTOR_DESIGN.md",
    }
    _write_json(outputs["manifest"], tables["manifest"])
    _write_csv(outputs["authorization_contract"], tables["authorization_contract"])
    _write_csv(outputs["real_com_contract"], tables["real_com_contract"])
    _write_csv(outputs["controlled_write_contract"], tables["controlled_write_contract"])
    _write_csv(outputs["readback_contract"], tables["readback_contract"])
    _write_csv(outputs["hold_contract"], tables["hold_contract"])
    _write_csv(outputs["boundary_gates"], tables["boundary_gates"])
    summary = [
        "# V1.5 formal initialization controlled executor design",
        "",
        "This is an offline design review for a future controlled initialization executor.",
        "",
        f"- overall_status: `{tables['manifest'].get('overall_status')}`",
        f"- production_state: `{tables['manifest'].get('production_state')}`",
        f"- execution_supported: `{tables['manifest'].get('execution_supported')}`",
        f"- live_execution_allowed: `{tables['manifest'].get('live_execution_allowed')}`",
        f"- opens_com_ports: `{tables['manifest'].get('opens_com_ports')}`",
        f"- writes_sn: `{tables['manifest'].get('writes_sn')}`",
        f"- writes_coefficients: `{tables['manifest'].get('writes_coefficients')}`",
        f"- connects_postgresql: `{tables['manifest'].get('connects_postgresql')}`",
        "",
        "Future executor requirements:",
        "",
        "- Explicit `--execute-controlled-initialization`, read-only real-COM unlock, and controlled-write unlock.",
        "- Exact operator confirmation plus distinct reviewer and approver.",
        "- 1 to 6 reviewed active analyzers with stable SN/device_code, protocol ID alias, and COM/GA transport mapping.",
        "- MODE2, 1Hz, AVERAGE1/2, and serial command spacing of at least 1.0s.",
        "- GETCO1-9 epoch-0 snapshot before any neutralization or write.",
        "- CHECK monitor only for CHECK-capable/new-algorithm analyzers after all active chambers are stable.",
        "- Readback and hold policy for identity mismatch, write mismatch, serial timeout, CHECK review, and database preflight gaps.",
        "",
        "Current package remains blocked and does not implement the real executor.",
    ]
    outputs["summary"].parent.mkdir(parents=True, exist_ok=True)
    outputs["summary"].write_text("\n".join(summary) + "\n", encoding="utf-8")
    return {key: str(path) for key, path in outputs.items()}
