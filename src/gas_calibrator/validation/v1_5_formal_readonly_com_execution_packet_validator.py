"""Offline validator for a future V1.5 read-only COM execution packet.

This module validates the *shape* of future authorization, reviewed-port, and
active-analyzer inputs. It intentionally never opens analyzer COM ports and
never treats a valid packet as permission to execute the real reader.
"""

from __future__ import annotations

import csv
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "v1_5_formal_readonly_com_execution_packet_validator_v1"
BLOCKED_EXECUTOR_SCHEMA = "v1_5_formal_readonly_com_execution_blocked_executor_v1"
BLOCKED_EXECUTOR_STATUS = "blocked_pending_readonly_com_real_executor_implementation"
READY_WITH_PACKET_STATUS = "ready_for_readonly_com_execution_packet_review"
LOCKED_NO_PACKET_STATUS = "blocked_pending_readonly_com_execution_authorization_packet"
REVIEW_STATUS = "review_required"
MIN_SERIAL_COMMAND_GAP_S = 1.0
SN_PATTERN = re.compile(r"^\d{8}$")
CONFIRMATION_TEMPLATE_ID = "v1_5_readonly_com_no_write_reviewed_ports_v1"
STRUCTURED_CONFIRMATION_FIELDS = (
    "read_only",
    "no_write",
    "reviewed_ports",
    "no_senco_write",
    "no_database_import",
    "no_route_control",
)
LEGACY_CONFIRMATION_TOKENS = ("read-only", "no-write", "reviewed ports")


@dataclass(frozen=True)
class ReadonlyComExecutionPacketCheck:
    check: str
    status: str
    evidence_role: str
    reasons: tuple[str, ...]
    physical_meaning: str
    next_action: str
    details: Mapping[str, Any]

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


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
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})


def _rows(payload: Mapping[str, Any], key: str) -> list[Mapping[str, Any]]:
    values = payload.get(key) or []
    return [item for item in values if isinstance(item, Mapping)]


def _field(row: Mapping[str, Any], *names: str) -> str:
    for name in names:
        value = row.get(name)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _bool(row: Mapping[str, Any], *names: str) -> bool:
    for name in names:
        value = row.get(name)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"true", "1", "yes", "y"}
    return False


def _mapping(row: Mapping[str, Any], *names: str) -> Mapping[str, Any]:
    for name in names:
        value = row.get(name)
        if isinstance(value, Mapping):
            return value
    return {}


def _check(
    *,
    check: str,
    status: str,
    evidence_role: str,
    reasons: Sequence[str] = (),
    physical_meaning: str,
    next_action: str,
    details: Mapping[str, Any],
) -> ReadonlyComExecutionPacketCheck:
    return ReadonlyComExecutionPacketCheck(
        check=check,
        status=status,
        evidence_role=evidence_role,
        reasons=tuple(reasons),
        physical_meaning=physical_meaning,
        next_action=next_action,
        details=details,
    )


def _blocked_executor_reasons(payload: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    if not payload:
        return ["blocked_executor_missing"]
    if payload.get("schema") != BLOCKED_EXECUTOR_SCHEMA:
        reasons.append(f"schema={payload.get('schema') or 'missing'}")
    if payload.get("overall_status") != BLOCKED_EXECUTOR_STATUS:
        reasons.append(f"overall_status={payload.get('overall_status') or 'missing'}")
    if payload.get("blocked_executor_ready") is not True:
        reasons.append(f"blocked_executor_ready={payload.get('blocked_executor_ready')!r}")
    if int(payload.get("review_required_count") or 0):
        reasons.append(f"review_required_count={payload.get('review_required_count')}")
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
            reasons.append(f"blocked_executor_boundary_{field}={payload.get(field)!r}")
    if payload.get("not_real_acceptance_evidence") is not True:
        reasons.append(f"not_real_acceptance_evidence={payload.get('not_real_acceptance_evidence')!r}")
    return reasons


def _structured_confirmation_reasons(payload: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    if payload.get("confirmation_template_id") != CONFIRMATION_TEMPLATE_ID:
        reasons.append(f"confirmation_template_id={payload.get('confirmation_template_id')!r}")
    fields = _mapping(payload, "confirmation_fields", "structured_confirmation")
    if not fields:
        reasons.append("confirmation_fields=missing")
        return reasons
    for field in STRUCTURED_CONFIRMATION_FIELDS:
        if fields.get(field) is not True:
            reasons.append(f"confirmation_fields_{field}={fields.get(field)!r}")
    return reasons


def _legacy_confirmation_reasons(payload: Mapping[str, Any]) -> list[str]:
    confirmation = str(payload.get("operator_confirmation_text") or "").lower()
    if not confirmation.strip():
        return ["operator_confirmation_text=missing"]
    reasons: list[str] = []
    for token in LEGACY_CONFIRMATION_TOKENS:
        if token not in confirmation:
            reasons.append(f"operator_confirmation_missing_{token.replace('-', '_').replace(' ', '_')}")
    return reasons


def _authorization_reasons(payload: Mapping[str, Any], *, required: bool) -> list[str]:
    reasons: list[str] = []
    if not payload:
        return ["authorization_packet_missing"] if required else []
    required_fields = (
        "authorization_id",
        "operator",
        "reviewer",
        "approver",
    )
    for field in required_fields:
        if not str(payload.get(field) or "").strip():
            reasons.append(f"{field}=missing")
    if payload.get("requested_flag") != "--execute-read-only-real-com":
        reasons.append(f"requested_flag={payload.get('requested_flag')!r}")
    if str(payload.get("reviewer") or "").strip() == str(payload.get("approver") or "").strip():
        reasons.append("reviewer_and_approver_must_be_distinct")
    structured_reasons = _structured_confirmation_reasons(payload)
    legacy_reasons = _legacy_confirmation_reasons(payload)
    if structured_reasons and legacy_reasons:
        reasons.append("operator_confirmation_missing_structured_template_or_legacy_text")
        reasons.extend(structured_reasons)
        reasons.extend(legacy_reasons)
    if float(payload.get("minimum_serial_command_gap_s") or 0.0) < MIN_SERIAL_COMMAND_GAP_S:
        reasons.append(f"minimum_serial_command_gap_s={payload.get('minimum_serial_command_gap_s')!r}")
    if float(payload.get("retry_gap_s") or 0.0) < MIN_SERIAL_COMMAND_GAP_S:
        reasons.append(f"retry_gap_s={payload.get('retry_gap_s')!r}")
    for field in (
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
            reasons.append(f"authorization_boundary_{field}={payload.get(field)!r}")
    if payload.get("not_real_acceptance_evidence") is not True:
        reasons.append(f"not_real_acceptance_evidence={payload.get('not_real_acceptance_evidence')!r}")
    return reasons


def _port_inventory_reasons(payload: Mapping[str, Any], *, required: bool) -> list[str]:
    reasons: list[str] = []
    if not payload:
        return ["reviewed_port_inventory_missing"] if required else []
    rows = _rows(payload, "reviewed_ports")
    if not rows:
        reasons.append("reviewed_ports=missing")
        return reasons
    ports: set[str] = set()
    labels: set[str] = set()
    for index, row in enumerate(rows, start=1):
        port = _field(row, "port", "com_port")
        label = _field(row, "ga_label", "label")
        if not port.upper().startswith("COM"):
            reasons.append(f"port_{index}={port or 'missing'}")
        if not label:
            reasons.append(f"ga_label_{index}=missing")
        if port in ports:
            reasons.append(f"duplicate_port={port}")
        if label in labels:
            reasons.append(f"duplicate_ga_label={label}")
        ports.add(port)
        labels.add(label)
    return reasons


def _active_analyzer_reasons(
    payload: Mapping[str, Any],
    inventory: Mapping[str, Any],
    *,
    required: bool,
) -> list[str]:
    reasons: list[str] = []
    if not payload:
        return ["active_analyzer_list_missing"] if required else []
    active_rows = _rows(payload, "active_analyzers")
    inventory_rows = _rows(inventory, "reviewed_ports")
    inventory_pairs = {
        (_field(row, "ga_label", "label"), _field(row, "port", "com_port"))
        for row in inventory_rows
    }
    if not (1 <= len(active_rows) <= 6):
        reasons.append(f"active_analyzer_count={len(active_rows)}")
    labels: set[str] = set()
    ports: set[str] = set()
    sn_values: set[str] = set()
    for index, row in enumerate(active_rows, start=1):
        label = _field(row, "ga_label", "label")
        port = _field(row, "port", "com_port")
        protocol_id = _field(row, "protocol_device_id", "device_id")
        sn_code = _field(row, "sn_code", "device_code")
        algorithm = _field(row, "algorithm", "algorithm_profile").lower() or "legacy"
        check_capable = _bool(row, "check_capable")
        check_required = _bool(row, "check_required")
        if not label:
            reasons.append(f"active_{index}_ga_label=missing")
        if not port.upper().startswith("COM"):
            reasons.append(f"active_{index}_port={port or 'missing'}")
        if not protocol_id:
            reasons.append(f"active_{index}_protocol_device_id=missing")
        if not SN_PATTERN.match(sn_code):
            reasons.append(f"active_{index}_sn_code={sn_code or 'missing'}")
        if (label, port) not in inventory_pairs:
            reasons.append(f"active_{index}_not_in_reviewed_port_inventory={label}/{port}")
        if label in labels:
            reasons.append(f"duplicate_active_ga_label={label}")
        if port in ports:
            reasons.append(f"duplicate_active_port={port}")
        if sn_code in sn_values:
            reasons.append(f"duplicate_active_sn_code={sn_code}")
        labels.add(label)
        ports.add(port)
        sn_values.add(sn_code)
        if algorithm in {"new", "new_absorption", "absorption"}:
            if not check_capable or not check_required:
                reasons.append(f"active_{index}_new_algorithm_check_must_be_required")
        else:
            if check_required or check_capable:
                reasons.append(f"active_{index}_old_algorithm_check_must_be_skipped")
    return reasons


def build_v1_5_formal_readonly_com_execution_packet_validator(
    *,
    formal_readonly_com_execution_blocked_executor_json: str | Path | None,
    authorization_packet_json: str | Path | None = None,
    reviewed_port_inventory_json: str | Path | None = None,
    active_analyzer_list_json: str | Path | None = None,
) -> dict[str, Any]:
    blocked_path = (
        Path(formal_readonly_com_execution_blocked_executor_json).resolve()
        if formal_readonly_com_execution_blocked_executor_json
        else None
    )
    authorization_path = Path(authorization_packet_json).resolve() if authorization_packet_json else None
    inventory_path = Path(reviewed_port_inventory_json).resolve() if reviewed_port_inventory_json else None
    active_path = Path(active_analyzer_list_json).resolve() if active_analyzer_list_json else None
    blocked_payload = _load_json(blocked_path)
    authorization_payload = _load_json(authorization_path)
    inventory_payload = _load_json(inventory_path)
    active_payload = _load_json(active_path)

    packet_inputs_present = any((authorization_path, inventory_path, active_path))
    packet_inputs_complete = all((authorization_path, inventory_path, active_path))
    blocked_reasons = _blocked_executor_reasons(blocked_payload)
    authorization_reasons = _authorization_reasons(
        authorization_payload,
        required=packet_inputs_present,
    )
    inventory_reasons = _port_inventory_reasons(
        inventory_payload,
        required=packet_inputs_present,
    )
    active_reasons = _active_analyzer_reasons(
        active_payload,
        inventory_payload,
        required=packet_inputs_present,
    )
    packet_completeness_reasons: list[str] = []
    if packet_inputs_present and not packet_inputs_complete:
        packet_completeness_reasons.append("authorization_port_inventory_and_active_analyzer_inputs_must_arrive_together")

    checks = [
        _check(
            check="blocked_executor_evidence_consumed",
            status="ready" if not blocked_reasons else "review_required",
            evidence_role="required_prior_blocked_executor_evidence",
            reasons=blocked_reasons,
            physical_meaning=(
                "Packet validation must be downstream of the no-COM blocked executor evidence; "
                "it cannot replace the lock that keeps analyzer COM closed."
            ),
            next_action="Regenerate the blocked executor artifact until it is ready and no-COM.",
            details={"source_path": str(blocked_path) if blocked_path else ""},
        ),
        _check(
            check="packet_input_completeness",
            status="ready" if not packet_completeness_reasons else "review_required",
            evidence_role="packet_input_set_boundary",
            reasons=packet_completeness_reasons,
            physical_meaning=(
                "Authorization, reviewed port inventory, and active analyzer list are one execution packet. "
                "Partial packet inputs cannot be treated as a future real-COM unlock."
            ),
            next_action="Provide all packet inputs together or leave all future packet inputs absent.",
            details={
                "authorization_packet_present": bool(authorization_path),
                "reviewed_port_inventory_present": bool(inventory_path),
                "active_analyzer_list_present": bool(active_path),
            },
        ),
        _check(
            check="authorization_packet_shape",
            status="ready" if not authorization_reasons else "review_required",
            evidence_role="future_authorization_packet",
            reasons=authorization_reasons,
            physical_meaning=(
                "A future read-only COM run requires explicit structured operator confirmation or legacy "
                "confirmation text fallback, distinct reviewer and approver, no-write boundaries, and >=1s "
                "command/retry pacing before any serial port can open."
            ),
            next_action="Fix authorization packet fields before a future executor can consume them.",
            details={"source_path": str(authorization_path) if authorization_path else ""},
        ),
        _check(
            check="reviewed_port_inventory_shape",
            status="ready" if not inventory_reasons else "review_required",
            evidence_role="future_reviewed_port_inventory",
            reasons=inventory_reasons,
            physical_meaning=(
                "Only operator-reviewed COM/GA mappings may be eligible for a future read-only execution packet."
            ),
            next_action="Provide a reviewed port inventory with unique COM ports and GA labels.",
            details={"source_path": str(inventory_path) if inventory_path else ""},
        ),
        _check(
            check="active_analyzer_list_shape",
            status="ready" if not active_reasons else "review_required",
            evidence_role="future_active_analyzer_list",
            reasons=active_reasons,
            physical_meaning=(
                "The future active analyzer list must support 1 to 6 devices, bind protocol ID to 8-digit SN, "
                "and keep old-algorithm CHECK skipped while requiring CHECK for new/CHECK-capable analyzers."
            ),
            next_action="Fix active analyzer identity, SN, port, algorithm, or CHECK fields before live reads.",
            details={"source_path": str(active_path) if active_path else ""},
        ),
    ]

    review_required_count = sum(1 for row in checks if row.status == "review_required")
    packet_validated = packet_inputs_complete and review_required_count == 0
    validator_ready = not blocked_reasons and (
        not packet_inputs_present or (packet_inputs_complete and review_required_count == 0)
    )
    if review_required_count:
        overall_status = REVIEW_STATUS
    elif packet_validated:
        overall_status = READY_WITH_PACKET_STATUS
    else:
        overall_status = LOCKED_NO_PACKET_STATUS

    active_count = len(_rows(active_payload, "active_analyzers"))
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": overall_status,
        "blocker_count": 0,
        "review_required_count": review_required_count,
        "packet_validator_ready": validator_ready,
        "packet_inputs_present": packet_inputs_present,
        "packet_inputs_complete": packet_inputs_complete,
        "packet_validated_offline": packet_validated,
        "production_state": "offline_packet_validator_only",
        "formal_readonly_com_execution_blocked_executor_json": str(blocked_path) if blocked_path else "",
        "authorization_packet_json": str(authorization_path) if authorization_path else "",
        "reviewed_port_inventory_json": str(inventory_path) if inventory_path else "",
        "active_analyzer_list_json": str(active_path) if active_path else "",
        "execution_supported": False,
        "execution_requested": False,
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
        "structured_confirmation_template_id": CONFIRMATION_TEMPLATE_ID,
        "structured_confirmation_required_fields": list(STRUCTURED_CONFIRMATION_FIELDS),
        "legacy_confirmation_text_tokens": list(LEGACY_CONFIRMATION_TOKENS),
        "minimum_serial_command_gap_s": MIN_SERIAL_COMMAND_GAP_S,
        "retry_gap_s": MIN_SERIAL_COMMAND_GAP_S,
        "supported_active_analyzer_count": "1_to_6",
        "active_analyzer_count": active_count,
        "supports_old_algorithm_check_skip": True,
        "checks": [row.to_json() for row in checks],
        "next_action": (
            "Keep COM locked. A future real read-only executor may only consume a fully validated packet "
            "in a separate implementation PR; this validator never opens COM or executes reads."
        ),
    }


def write_v1_5_formal_readonly_com_execution_packet_validator_outputs(
    model: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, Path]:
    out = Path(output_dir)
    paths = {
        "json": out / "v1_5_formal_readonly_com_execution_packet_validator.json",
        "checks_csv": out / "v1_5_formal_readonly_com_execution_packet_validator_checks.csv",
        "summary_csv": out / "v1_5_formal_readonly_com_execution_packet_validator_summary.csv",
        "markdown": out / "V1_5_FORMAL_READONLY_COM_EXECUTION_PACKET_VALIDATOR.md",
    }
    _write_json(paths["json"], model)
    _write_csv(paths["checks_csv"], model.get("checks", []))
    _write_csv(
        paths["summary_csv"],
        [
            {
                "overall_status": model.get("overall_status"),
                "packet_validator_ready": model.get("packet_validator_ready"),
                "packet_inputs_present": model.get("packet_inputs_present"),
                "packet_inputs_complete": model.get("packet_inputs_complete"),
                "packet_validated_offline": model.get("packet_validated_offline"),
                "active_analyzer_count": model.get("active_analyzer_count"),
                "execution_supported": model.get("execution_supported"),
                "live_execution_allowed": model.get("live_execution_allowed"),
                "read_only_real_com_execution_allowed": model.get("read_only_real_com_execution_allowed"),
                "opens_com_ports": model.get("opens_com_ports"),
                "writes_sn": model.get("writes_sn"),
                "writes_coefficients": model.get("writes_coefficients"),
                "connects_postgresql": model.get("connects_postgresql"),
                "database_written": model.get("database_written"),
            }
        ],
    )
    lines = [
        "# V1.5 formal read-only COM execution packet validator",
        "",
        "This is an offline validator for future authorization, reviewed-port, and active-analyzer inputs.",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- packet_validator_ready: `{model.get('packet_validator_ready')}`",
        f"- packet_inputs_present: `{model.get('packet_inputs_present')}`",
        f"- packet_validated_offline: `{model.get('packet_validated_offline')}`",
        f"- execution_supported: `{model.get('execution_supported')}`",
        f"- live_execution_allowed: `{model.get('live_execution_allowed')}`",
        f"- read_only_real_com_execution_allowed: `{model.get('read_only_real_com_execution_allowed')}`",
        f"- opens_com_ports: `{model.get('opens_com_ports')}`",
        f"- writes_sn: `{model.get('writes_sn')}`",
        f"- writes_coefficients: `{model.get('writes_coefficients')}`",
        f"- connects_postgresql: `{model.get('connects_postgresql')}`",
        "- A valid packet is still not live execution authorization in this package.",
        "",
        "## Checks",
        "",
    ]
    for row in model.get("checks", []):
        reasons = ";".join(row.get("reasons") or ())
        lines.append(f"- `{row.get('check')}`: `{row.get('status')}` {reasons}".rstrip())
    paths["markdown"].parent.mkdir(parents=True, exist_ok=True)
    paths["markdown"].write_text("\n".join(lines) + "\n", encoding="utf-8")
    return paths
