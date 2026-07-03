"""Offline controlled-executor design for V1.5 read-only initialization COM preflight.

This module deliberately does not implement real COM execution. It consumes the
blocked read-only COM preflight executor evidence and freezes the contract that
a future controlled read-only COM executor must satisfy before it may contact
analyzers.
"""

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "v1_5_formal_initialization_readonly_com_preflight_controlled_executor_design_v1"
BLOCKED_EXECUTOR_SCHEMA = "v1_5_formal_initialization_readonly_com_preflight_blocked_executor_v1"
READY_STATUS = "ready_for_readonly_com_preflight_controlled_executor_design_review"
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
        return ["readonly_com_preflight_blocked_executor_missing"]
    if payload.get("schema") != BLOCKED_EXECUTOR_SCHEMA:
        reasons.append(f"blocked_executor_schema={payload.get('schema') or 'missing'}")
    if payload.get("overall_status") != "blocked_pending_readonly_real_com_preflight_implementation":
        reasons.append(f"blocked_executor_status={payload.get('overall_status') or 'missing'}")
    if payload.get("blocked_executor_ready") is not True:
        reasons.append(f"blocked_executor_ready={payload.get('blocked_executor_ready')!r}")
    if int(payload.get("review_required_count") or 0):
        reasons.append(f"blocked_executor_review_required_count={payload.get('review_required_count')}")
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


def build_v1_5_formal_initialization_readonly_com_preflight_controlled_executor_design(
    *,
    formal_initialization_readonly_com_preflight_blocked_executor_json: str | Path | None = None,
) -> dict[str, Any]:
    """Build no-COM design artifacts for a future read-only COM preflight executor."""

    blocked_path = (
        Path(formal_initialization_readonly_com_preflight_blocked_executor_json).resolve()
        if formal_initialization_readonly_com_preflight_blocked_executor_json
        else None
    )
    blocked_payload = _load_json(blocked_path)
    review_reasons = _blocked_executor_review_reasons(blocked_payload)

    authorization_contract = [
        {
            "gate": "explicit_read_only_real_com_execute_flag",
            "required": True,
            "future_flag": "--execute-read-only-real-com",
            "contract": "Future executor must be impossible to run unless this flag is explicitly provided.",
        },
        {
            "gate": "operator_confirmation_record",
            "required": True,
            "future_field": "operator_confirmation_text",
            "contract": "Operator confirmation must bind run_id, active analyzers, reviewed COM ports, and no-write scope.",
        },
        {
            "gate": "reviewer_approver_dual_authorization",
            "required": True,
            "future_fields": "reviewer;approver;authorization_id",
            "contract": "Reviewer and approver must be distinct and serialized before any analyzer COM opens.",
        },
        {
            "gate": "controlled_write_flags_excluded",
            "required": True,
            "future_flag_excluded": "--execute-controlled-writes",
            "contract": "Read-only preflight must not include SN/device_code, runtime, SENCO, or database write unlocks.",
        },
        {
            "gate": "active_device_scope",
            "required": True,
            "future_scope": "1_to_6_active_analyzers",
            "contract": "The future executor must use the reviewed active analyzer list and must not assume six devices.",
        },
    ]

    port_inventory_contract = [
        {
            "step": "load_reviewed_port_inventory",
            "order": 1,
            "required": True,
            "physical_meaning": "Only reviewer-approved analyzer COM ports may be opened.",
            "failure_policy": "abort_before_opening_any_com_port",
        },
        {
            "step": "bind_transport_aliases",
            "order": 2,
            "required": True,
            "physical_meaning": "COM and GA labels are run-local transport aliases, not production identity.",
            "failure_policy": "hold_on_duplicate_or_missing_transport_alias",
        },
        {
            "step": "enforce_serial_command_spacing",
            "order": 3,
            "required": True,
            "physical_meaning": "Every command, retry, and cross-device read must preserve at least 1.0s spacing.",
            "failure_policy": "reject_preflight_evidence_on_pacing_violation",
        },
    ]

    read_sequence_contract = [
        {
            "read": "protocol_device_id",
            "order": 1,
            "required": True,
            "command_scope": "existing read-only identity path",
            "expected": "protocol ID retained only as compatibility alias",
        },
        {
            "read": "sn_code_device_code",
            "order": 2,
            "required": True,
            "command_scope": "SN,YGAS,FFF",
            "expected": "8 numeric digits; device_code equals sn_code",
        },
        {
            "read": "getco_epoch0",
            "order": 3,
            "required": True,
            "command_scope": "GETCO1 through GETCO9",
            "expected": "complete raw response plus parsed values before any later neutralization or write",
        },
        {
            "read": "runtime_state",
            "order": 4,
            "required": True,
            "command_scope": "read-only runtime evidence",
            "expected": "MODE2, 1Hz, AVERAGE1/2 expectation captured without writing runtime state",
        },
        {
            "read": "check_monitor",
            "order": 5,
            "required": True,
            "command_scope": "CHECK,YGAS,FFF for CHECK-capable/new-algorithm analyzers only",
            "expected": "old-algorithm devices without CHECK support are skipped, not failed",
        },
    ]

    evidence_contract = [
        {
            "artifact": "readonly_com_authorization.json",
            "required": True,
            "contents": "operator confirmation, reviewer, approver, authorization_id, active analyzer list",
        },
        {
            "artifact": "readonly_com_port_inventory.csv",
            "required": True,
            "contents": "reviewed COM, GA label, expected protocol ID, expected SN/device_code if known",
        },
        {
            "artifact": "readonly_com_identity_getco_snapshot.csv",
            "required": True,
            "contents": "protocol ID, SN/device_code, GETCO1-9 values, raw responses, timestamps",
        },
        {
            "artifact": "analyzer_check_monitor.csv",
            "required": True,
            "contents": "CHECK monitor voltages only for CHECK-capable/new-algorithm analyzers after chamber stability",
        },
        {
            "artifact": "readonly_com_hold_report.csv",
            "required": True,
            "contents": "serial timeouts, schema mismatches, duplicate SN, pacing violations, CHECK review rows",
        },
    ]

    hold_contract = [
        {
            "trigger": "execute_flag_missing",
            "hold_action": "do not open COM",
            "release_policy": "rerun only with explicit read-only COM execution authorization",
        },
        {
            "trigger": "operator_or_dual_authorization_missing",
            "hold_action": "do not open COM",
            "release_policy": "complete operator/reviewer/approver record",
        },
        {
            "trigger": "serial_timeout_or_schema_mismatch",
            "hold_action": "close affected COM and mark preflight incomplete",
            "release_policy": "review cabling/protocol before retry; never write recovery commands",
        },
        {
            "trigger": "identity_mismatch_or_duplicate_sn",
            "hold_action": "hold all write and database stages for the run",
            "release_policy": "manual traceability review and corrected identity bundle",
        },
        {
            "trigger": "check_monitor_out_of_range",
            "hold_action": "record hardware/thermal review_required without changing coefficients",
            "release_policy": "hardware review before new-algorithm formal route",
        },
    ]

    boundary_gates = [
        {
            "gate": "design_only_no_com",
            "status": "pass",
            "evidence": "opens_com_ports=false; execution_supported=false; read_only_real_com_execution_allowed=false",
        },
        {
            "gate": "blocked_executor_consumed",
            "status": "review_required" if review_reasons else "pass",
            "evidence": ";".join(review_reasons) if review_reasons else str(blocked_path),
        },
        {
            "gate": "future_readonly_com_still_locked",
            "status": "pass",
            "evidence": "current package does not add --execute-read-only-real-com runtime behavior",
        },
        {
            "gate": "no_write_no_database_no_route",
            "status": "pass",
            "evidence": "writes_sn=false; writes_coefficients=false; connects_postgresql=false; controls_water_or_gas_routes=false",
        },
        {
            "gate": "serial_command_gap_contract",
            "status": "pass",
            "evidence": f"minimum_serial_command_gap_s={MIN_SERIAL_COMMAND_GAP_S:g}",
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
        "formal_initialization_readonly_com_preflight_blocked_executor_json": (
            str(blocked_path) if blocked_path else ""
        ),
        "required_future_read_only_real_com_flag": "--execute-read-only-real-com",
        "required_future_controlled_write_flag_excluded": "--execute-controlled-writes",
        "next_action": (
            "Keep analyzer contact locked. Implement a separate controlled read-only COM executor only after "
            "this authorization, port inventory, pacing, identity, GETCO, CHECK, evidence, and hold contract is reviewed."
        ),
    }
    return {
        "manifest": manifest,
        "authorization_contract": authorization_contract,
        "port_inventory_contract": port_inventory_contract,
        "read_sequence_contract": read_sequence_contract,
        "evidence_contract": evidence_contract,
        "hold_contract": hold_contract,
        "boundary_gates": boundary_gates,
    }


def write_v1_5_formal_initialization_readonly_com_preflight_controlled_executor_design(
    output_dir: str | Path,
    *,
    formal_initialization_readonly_com_preflight_blocked_executor_json: str | Path | None = None,
) -> dict[str, str]:
    tables = build_v1_5_formal_initialization_readonly_com_preflight_controlled_executor_design(
        formal_initialization_readonly_com_preflight_blocked_executor_json=(
            formal_initialization_readonly_com_preflight_blocked_executor_json
        ),
    )
    out = Path(output_dir)
    outputs = {
        "manifest": out / "v1_5_formal_initialization_readonly_com_preflight_controlled_executor_design.json",
        "authorization_contract": out
        / "v1_5_formal_initialization_readonly_com_preflight_controlled_executor_authorization_contract.csv",
        "port_inventory_contract": out
        / "v1_5_formal_initialization_readonly_com_preflight_controlled_executor_port_inventory_contract.csv",
        "read_sequence_contract": out
        / "v1_5_formal_initialization_readonly_com_preflight_controlled_executor_read_sequence_contract.csv",
        "evidence_contract": out
        / "v1_5_formal_initialization_readonly_com_preflight_controlled_executor_evidence_contract.csv",
        "hold_contract": out
        / "v1_5_formal_initialization_readonly_com_preflight_controlled_executor_hold_contract.csv",
        "boundary_gates": out
        / "v1_5_formal_initialization_readonly_com_preflight_controlled_executor_boundary_gates.csv",
        "summary": out / "V1_5_FORMAL_INITIALIZATION_READONLY_COM_PREFLIGHT_CONTROLLED_EXECUTOR_DESIGN.md",
    }
    _write_json(outputs["manifest"], tables["manifest"])
    _write_csv(outputs["authorization_contract"], tables["authorization_contract"])
    _write_csv(outputs["port_inventory_contract"], tables["port_inventory_contract"])
    _write_csv(outputs["read_sequence_contract"], tables["read_sequence_contract"])
    _write_csv(outputs["evidence_contract"], tables["evidence_contract"])
    _write_csv(outputs["hold_contract"], tables["hold_contract"])
    _write_csv(outputs["boundary_gates"], tables["boundary_gates"])
    summary = [
        "# V1.5 formal initialization read-only COM preflight controlled executor design",
        "",
        "This is an offline design review for a future controlled read-only COM preflight executor.",
        "",
        f"- overall_status: `{tables['manifest'].get('overall_status')}`",
        f"- production_state: `{tables['manifest'].get('production_state')}`",
        f"- execution_supported: `{tables['manifest'].get('execution_supported')}`",
        f"- read_only_real_com_execution_allowed: `{tables['manifest'].get('read_only_real_com_execution_allowed')}`",
        f"- live_execution_allowed: `{tables['manifest'].get('live_execution_allowed')}`",
        f"- opens_com_ports: `{tables['manifest'].get('opens_com_ports')}`",
        f"- writes_sn: `{tables['manifest'].get('writes_sn')}`",
        f"- writes_coefficients: `{tables['manifest'].get('writes_coefficients')}`",
        f"- connects_postgresql: `{tables['manifest'].get('connects_postgresql')}`",
        "",
        "Future controlled read-only COM preflight requirements:",
        "",
        "- Explicit `--execute-read-only-real-com`; no controlled-write flag in this executor.",
        "- Operator confirmation plus distinct reviewer and approver.",
        "- Reviewed 1 to 6 active analyzers and reviewed COM/GA transport inventory.",
        "- Serial command spacing, retry, and cross-device reads at least 1.0s apart.",
        "- Protocol ID, 8-digit SN/device_code, GETCO1-9 epoch-0, and runtime evidence read-only.",
        "- `CHECK,YGAS,FFF` only for CHECK-capable/new-algorithm analyzers after chamber stability.",
        "- Old-algorithm devices without CHECK support are skipped without failing the run.",
        "- Holds never issue recovery writes and never authorize database import or route execution.",
        "",
        "Current package remains blocked and does not implement real COM execution.",
    ]
    outputs["summary"].parent.mkdir(parents=True, exist_ok=True)
    outputs["summary"].write_text("\n".join(summary) + "\n", encoding="utf-8")
    return {key: str(path) for key, path in outputs.items()}
