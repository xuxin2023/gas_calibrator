"""Offline design review for a future V1.5 read-only initialization COM preflight.

This package deliberately does not open serial ports. It freezes the contract a
future read-only real-COM preflight must satisfy before any initialization
executor can read protocol IDs, SN/device_code, GETCO, runtime state, or CHECK
monitor evidence from analyzers.
"""

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "v1_5_formal_initialization_readonly_com_preflight_design_v1"
CONTROLLED_EXECUTOR_DESIGN_SCHEMA = "v1_5_formal_initialization_controlled_executor_design_v1"
READY_STATUS = "ready_for_readonly_real_com_preflight_design_review"
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


def _controlled_design_review_reasons(payload: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    if not payload:
        return ["controlled_executor_design_evidence_missing"]
    if payload.get("schema") != CONTROLLED_EXECUTOR_DESIGN_SCHEMA:
        reasons.append(f"controlled_design_schema={payload.get('schema') or 'missing'}")
    if payload.get("overall_status") != "ready_for_controlled_initialization_executor_design_review":
        reasons.append(f"controlled_design_status={payload.get('overall_status') or 'missing'}")
    if int(payload.get("review_required_count") or 0):
        reasons.append(f"controlled_design_review_required_count={payload.get('review_required_count')}")
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
            reasons.append(f"controlled_design_boundary_{field}={payload.get(field)!r}")
    if float(payload.get("minimum_serial_command_gap_s") or 0.0) < MIN_SERIAL_COMMAND_GAP_S:
        reasons.append(
            f"controlled_design_command_gap_s={payload.get('minimum_serial_command_gap_s')!r}"
        )
    return reasons


def build_v1_5_formal_initialization_readonly_com_preflight_design(
    *,
    formal_initialization_controlled_executor_design_json: str | Path | None = None,
) -> dict[str, Any]:
    """Build a no-COM design package for a future read-only initialization preflight."""

    controlled_path = (
        Path(formal_initialization_controlled_executor_design_json).resolve()
        if formal_initialization_controlled_executor_design_json
        else None
    )
    controlled_payload = _load_json(controlled_path)
    review_reasons = _controlled_design_review_reasons(controlled_payload)

    authorization_contract = [
        {
            "gate": "explicit_read_only_real_com_flag",
            "required": True,
            "future_flag": "--execute-read-only-real-com",
            "contract": "Read-only analyzer contact remains impossible unless a future tool exposes and receives this flag.",
        },
        {
            "gate": "operator_confirmation_record",
            "required": True,
            "future_field": "operator_confirmation_text",
            "contract": "Operator confirmation must bind run_id, reviewed ports, active analyzer count, and no-write scope.",
        },
        {
            "gate": "active_device_scope",
            "required": True,
            "future_scope": "1_to_6_active_analyzers",
            "contract": "The preflight must use the reviewed active list; it must not assume six analyzers.",
        },
        {
            "gate": "controlled_writes_stay_locked",
            "required": True,
            "future_flag_excluded": "--execute-controlled-writes",
            "contract": "Read-only COM preflight must not write SN/device_code, runtime state, SENCO, or database rows.",
        },
    ]

    serial_preflight_contract = [
        {
            "step": "use_reviewed_port_inventory",
            "order": 1,
            "required": True,
            "physical_meaning": "Open only reviewer-approved analyzer ports and preserve COM/GA as transport aliases.",
            "failure_policy": "abort_before_any_device_contact_if_inventory_is_missing_or_ambiguous",
        },
        {
            "step": "serial_command_spacing",
            "order": 2,
            "required": True,
            "physical_meaning": "All commands, retries, and inter-device reads must preserve at least 1.0s spacing.",
            "failure_policy": "hold_run_if_scheduler_cannot_guarantee_spacing",
        },
        {
            "step": "bounded_timeout_and_retry",
            "order": 3,
            "required": True,
            "physical_meaning": "Retries are allowed only as read-only retries with retry_gap_s >= 1.0.",
            "failure_policy": "mark_device_incomplete_after_bounded_retry_budget",
        },
        {
            "step": "mode2_1hz_filter_precondition",
            "order": 4,
            "required": True,
            "physical_meaning": "The reviewed initialization contract expects MODE2, 1Hz active upload, and AVERAGE1/2 before point evidence.",
            "failure_policy": "record_runtime_setup_gap_without_writing_runtime_state",
        },
    ]

    identity_read_contract = [
        {
            "read": "protocol_device_id",
            "required": True,
            "command": "ID/read identity command from existing protocol path",
            "expected": "protocol device ID is retained as compatibility alias, not production primary key",
        },
        {
            "read": "sn_code",
            "required": True,
            "command": "SN,YGAS,FFF",
            "expected": "8 numeric digits",
        },
        {
            "read": "device_code",
            "required": True,
            "command": "derived from SN/device_code identity bundle",
            "expected": "device_code equals sn_code for production identity",
        },
        {
            "read": "transport_mapping",
            "required": True,
            "command": "reviewed COM/GA mapping",
            "expected": "COM and GA label are run transport aliases only",
        },
    ]

    getco_read_contract = [
        {
            "read": "getco_epoch0",
            "required_groups": "GETCO1,GETCO2,GETCO3,GETCO4,GETCO5,GETCO6,GETCO7,GETCO8,GETCO9",
            "physical_meaning": "Freeze all existing coefficients before S5/S6/S7/S8/S9 neutralization or later component writes.",
            "failure_policy": "incomplete snapshot blocks all controlled writes for that analyzer",
        },
        {
            "read": "raw_responses",
            "required_groups": "all GETCO groups attempted",
            "physical_meaning": "Preserve raw protocol frames so later fitting and write decisions can be audited.",
            "failure_policy": "missing raw response marks snapshot review_required",
        },
    ]

    check_read_contract = [
        {
            "read": "check_monitor",
            "applies_to": "CHECK-capable or new-algorithm analyzers only",
            "command": "CHECK,YGAS,FFF",
            "timing": "after all active analyzer chamber temperatures are stable and before point sampling evidence is sealed",
            "expected": "two monitor voltages plus raw response in analyzer_check_monitor.csv",
        },
        {
            "read": "legacy_algorithm_skip",
            "applies_to": "old-algorithm analyzers without CHECK support",
            "command": "none",
            "timing": "do not send CHECK",
            "expected": "skip is not a failure when the device is not CHECK-capable",
        },
    ]

    failure_hold_contract = [
        {
            "trigger": "serial_timeout_or_frame_schema_mismatch",
            "hold_action": "close affected COM port and mark read-only preflight incomplete",
            "release_policy": "review cabling/protocol/schema before retry; do not write recovery commands",
        },
        {
            "trigger": "identity_mismatch_or_duplicate_sn",
            "hold_action": "hold all writes and database import for affected run",
            "release_policy": "manual traceability review and new identity bundle required",
        },
        {
            "trigger": "command_gap_violation",
            "hold_action": "reject preflight evidence because serial pacing contract was violated",
            "release_policy": "rerun with scheduler guaranteeing >=1.0s command spacing",
        },
        {
            "trigger": "check_out_of_range",
            "hold_action": "record hardware/thermal review_required without changing coefficients",
            "release_policy": "hardware review before new-algorithm formal flow",
        },
        {
            "trigger": "getco_epoch0_incomplete",
            "hold_action": "block controlled write stages for the incomplete analyzer",
            "release_policy": "rerun read-only GETCO snapshot before any neutralization/write",
        },
    ]

    boundary_gates = [
        {
            "gate": "design_only_no_com",
            "status": "pass",
            "evidence": "opens_com_ports=false; execution_supported=false; read_only_real_com_execution_allowed=false",
        },
        {
            "gate": "controlled_design_consumed",
            "status": "review_required" if review_reasons else "pass",
            "evidence": ";".join(review_reasons) if review_reasons else str(controlled_path),
        },
        {
            "gate": "future_real_com_still_locked",
            "status": "pass",
            "evidence": "current package does not add --execute-read-only-real-com runtime behavior",
        },
        {
            "gate": "no_writes",
            "status": "pass",
            "evidence": "writes_sn=false; writes_device_id=false; writes_coefficients=false",
        },
        {
            "gate": "no_route_pressure_database",
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
        "formal_initialization_controlled_executor_design_json": str(controlled_path) if controlled_path else "",
        "required_future_read_only_real_com_flag": "--execute-read-only-real-com",
        "required_future_controlled_write_flag_excluded": "--execute-controlled-writes",
        "next_action": (
            "Keep read-only analyzer contact locked. A later controlled tool may implement this preflight only "
            "after the port, pacing, identity, GETCO, CHECK, and hold contracts are reviewed."
        ),
    }
    return {
        "manifest": manifest,
        "authorization_contract": authorization_contract,
        "serial_preflight_contract": serial_preflight_contract,
        "identity_read_contract": identity_read_contract,
        "getco_read_contract": getco_read_contract,
        "check_read_contract": check_read_contract,
        "failure_hold_contract": failure_hold_contract,
        "boundary_gates": boundary_gates,
    }


def write_v1_5_formal_initialization_readonly_com_preflight_design(
    output_dir: str | Path,
    *,
    formal_initialization_controlled_executor_design_json: str | Path | None = None,
) -> dict[str, str]:
    tables = build_v1_5_formal_initialization_readonly_com_preflight_design(
        formal_initialization_controlled_executor_design_json=(
            formal_initialization_controlled_executor_design_json
        ),
    )
    out = Path(output_dir)
    outputs = {
        "manifest": out / "v1_5_formal_initialization_readonly_com_preflight_design.json",
        "authorization_contract": out
        / "v1_5_formal_initialization_readonly_com_preflight_authorization_contract.csv",
        "serial_preflight_contract": out
        / "v1_5_formal_initialization_readonly_com_preflight_serial_contract.csv",
        "identity_read_contract": out
        / "v1_5_formal_initialization_readonly_com_preflight_identity_read_contract.csv",
        "getco_read_contract": out
        / "v1_5_formal_initialization_readonly_com_preflight_getco_read_contract.csv",
        "check_read_contract": out
        / "v1_5_formal_initialization_readonly_com_preflight_check_read_contract.csv",
        "failure_hold_contract": out
        / "v1_5_formal_initialization_readonly_com_preflight_failure_hold_contract.csv",
        "boundary_gates": out / "v1_5_formal_initialization_readonly_com_preflight_boundary_gates.csv",
        "summary": out / "V1_5_FORMAL_INITIALIZATION_READONLY_COM_PREFLIGHT_DESIGN.md",
    }
    _write_json(outputs["manifest"], tables["manifest"])
    _write_csv(outputs["authorization_contract"], tables["authorization_contract"])
    _write_csv(outputs["serial_preflight_contract"], tables["serial_preflight_contract"])
    _write_csv(outputs["identity_read_contract"], tables["identity_read_contract"])
    _write_csv(outputs["getco_read_contract"], tables["getco_read_contract"])
    _write_csv(outputs["check_read_contract"], tables["check_read_contract"])
    _write_csv(outputs["failure_hold_contract"], tables["failure_hold_contract"])
    _write_csv(outputs["boundary_gates"], tables["boundary_gates"])
    summary = [
        "# V1.5 formal initialization read-only COM preflight design",
        "",
        "This is an offline design review for a future read-only real-COM initialization preflight.",
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
        "Future read-only COM preflight requirements:",
        "",
        "- Explicit `--execute-read-only-real-com` plus operator confirmation and reviewed 1 to 6 active analyzers.",
        "- Reviewed serial-port inventory only; no implicit port discovery or root dirty-tree probing.",
        "- Serial command spacing and retry gaps at least 1.0s.",
        "- Protocol ID, `SN,YGAS,FFF`, 8-digit SN/device_code, and COM/GA transport mapping evidence.",
        "- GETCO1-9 epoch-0 snapshot and raw responses before any later neutralization or write.",
        "- `CHECK,YGAS,FFF` only for CHECK-capable/new-algorithm analyzers after all active chambers are stable.",
        "- Old-algorithm analyzers that do not support CHECK must be skipped without turning the skip into a failure.",
        "- Timeout, schema mismatch, duplicate SN, CHECK out-of-range, or pacing violations hold the run and never write recovery commands.",
        "",
        "Current package remains blocked and does not implement read-only COM execution.",
    ]
    outputs["summary"].parent.mkdir(parents=True, exist_ok=True)
    outputs["summary"].write_text("\n".join(summary) + "\n", encoding="utf-8")
    return {key: str(path) for key, path in outputs.items()}
