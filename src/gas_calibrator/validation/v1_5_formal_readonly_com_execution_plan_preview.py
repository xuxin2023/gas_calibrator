"""Plan-only preview for future V1.5 read-only COM execution.

This module turns a validated future read-only COM execution packet into a
human-reviewable command/read sequence. It intentionally never opens COM ports,
never reads analyzers, and never treats a plan preview as live authorization.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "v1_5_formal_readonly_com_execution_plan_preview_v1"
PACKET_VALIDATOR_SCHEMA = "v1_5_formal_readonly_com_execution_packet_validator_v1"
PACKET_READY_STATUS = "ready_for_readonly_com_execution_packet_review"
READY_STATUS = "ready_for_readonly_com_execution_plan_preview_review"
LOCKED_STATUS = "blocked_pending_validated_readonly_com_execution_packet"
REVIEW_STATUS = "review_required"
MIN_SERIAL_COMMAND_GAP_S = 1.0


@dataclass(frozen=True)
class ReadonlyComExecutionPlanCheck:
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
    if not fields:
        fields = ["message"]
        rows = [{"message": "no_rows"}]
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


def _check(
    *,
    check: str,
    status: str,
    evidence_role: str,
    reasons: Sequence[str] = (),
    physical_meaning: str,
    next_action: str,
    details: Mapping[str, Any],
) -> ReadonlyComExecutionPlanCheck:
    return ReadonlyComExecutionPlanCheck(
        check=check,
        status=status,
        evidence_role=evidence_role,
        reasons=tuple(reasons),
        physical_meaning=physical_meaning,
        next_action=next_action,
        details=details,
    )


def _packet_reasons(payload: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    if not payload:
        return ["packet_validator_missing"]
    if payload.get("schema") != PACKET_VALIDATOR_SCHEMA:
        reasons.append(f"schema={payload.get('schema') or 'missing'}")
    if payload.get("overall_status") != PACKET_READY_STATUS:
        reasons.append(f"overall_status={payload.get('overall_status') or 'missing'}")
    if payload.get("packet_validated_offline") is not True:
        reasons.append(f"packet_validated_offline={payload.get('packet_validated_offline')!r}")
    if payload.get("packet_inputs_complete") is not True:
        reasons.append(f"packet_inputs_complete={payload.get('packet_inputs_complete')!r}")
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
            reasons.append(f"packet_boundary_{field}={payload.get(field)!r}")
    if payload.get("not_real_acceptance_evidence") is not True:
        reasons.append(f"not_real_acceptance_evidence={payload.get('not_real_acceptance_evidence')!r}")
    return reasons


def _inventory_map(payload: Mapping[str, Any]) -> dict[tuple[str, str], Mapping[str, Any]]:
    return {
        (_field(row, "ga_label", "label"), _field(row, "port", "com_port")): row
        for row in _rows(payload, "reviewed_ports")
    }


def _active_input_reasons(
    inventory_payload: Mapping[str, Any],
    active_payload: Mapping[str, Any],
) -> list[str]:
    reasons: list[str] = []
    if not inventory_payload:
        reasons.append("reviewed_port_inventory_missing_for_plan_preview")
    if not active_payload:
        reasons.append("active_analyzer_list_missing_for_plan_preview")
    active_rows = _rows(active_payload, "active_analyzers")
    if active_payload and not (1 <= len(active_rows) <= 6):
        reasons.append(f"active_analyzer_count={len(active_rows)}")
    inventory = _inventory_map(inventory_payload)
    labels: set[str] = set()
    ports: set[str] = set()
    for index, row in enumerate(active_rows, start=1):
        label = _field(row, "ga_label", "label")
        port = _field(row, "port", "com_port")
        algorithm = _field(row, "algorithm", "algorithm_profile").lower() or "legacy"
        check_required = _bool(row, "check_required")
        check_capable = _bool(row, "check_capable")
        if not label:
            reasons.append(f"active_{index}_ga_label=missing")
        if not port.upper().startswith("COM"):
            reasons.append(f"active_{index}_port={port or 'missing'}")
        if (label, port) not in inventory:
            reasons.append(f"active_{index}_not_in_reviewed_port_inventory={label}/{port}")
        if label in labels:
            reasons.append(f"duplicate_active_ga_label={label}")
        if port in ports:
            reasons.append(f"duplicate_active_port={port}")
        labels.add(label)
        ports.add(port)
        if algorithm in {"new", "new_absorption", "absorption"}:
            if not check_capable or not check_required:
                reasons.append(f"active_{index}_new_algorithm_check_must_be_required")
        elif check_required:
            reasons.append(f"active_{index}_old_algorithm_check_must_be_skipped")
    return reasons


def _add_plan_row(
    rows: list[dict[str, Any]],
    *,
    order: int,
    analyzer_index: int,
    row: Mapping[str, Any],
    read_role: str,
    command_or_source: str,
    expected_response: str,
    hold_condition: str,
    serial_command: bool = True,
    enabled: bool = True,
    note: str = "",
) -> int:
    algorithm = _field(row, "algorithm", "algorithm_profile") or "legacy_ratio"
    rows.append(
        {
            "order": order,
            "analyzer_index": analyzer_index,
            "ga_label": _field(row, "ga_label", "label"),
            "port": _field(row, "port", "com_port"),
            "protocol_device_id": _field(row, "protocol_device_id", "device_id"),
            "sn_code": _field(row, "sn_code", "device_code"),
            "algorithm": algorithm,
            "read_role": read_role,
            "command_or_source": command_or_source if enabled else "",
            "serial_command": serial_command and enabled,
            "enabled": enabled,
            "serial_gap_before_s": MIN_SERIAL_COMMAND_GAP_S if serial_command and enabled else "",
            "expected_response": expected_response,
            "hold_condition": hold_condition,
            "opens_com_ports_in_this_package": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "connects_postgresql": False,
            "controls_pressure": False,
            "controls_routes": False,
            "note": note,
        }
    )
    return order + 1


def _build_command_plan(active_payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    plan: list[dict[str, Any]] = []
    order = 1
    for analyzer_index, row in enumerate(_rows(active_payload, "active_analyzers"), start=1):
        order = _add_plan_row(
            plan,
            order=order,
            analyzer_index=analyzer_index,
            row=row,
            read_role="protocol_device_id_from_mode2_frame",
            command_or_source="MODE2_FRAME_DEVICE_ID_FIELD",
            serial_command=False,
            expected_response="protocol device ID observed from active 1Hz MODE2 stream",
            hold_condition="hold_on_missing_protocol_device_id_or_frame_timeout",
            note="passive frame field, not a serial write/read command",
        )
        order = _add_plan_row(
            plan,
            order=order,
            analyzer_index=analyzer_index,
            row=row,
            read_role="sn_code_device_code",
            command_or_source="SN,YGAS,FFF",
            expected_response="8 digit numeric SN/device_code",
            hold_condition="hold_on_missing_non_numeric_duplicate_or_mismatched_sn",
        )
        for index in range(1, 10):
            order = _add_plan_row(
                plan,
                order=order,
                analyzer_index=analyzer_index,
                row=row,
                read_role=f"getco{index}_epoch0",
                command_or_source=f"GETCO{index},YGAS,FFF",
                expected_response=f"GETCO{index} coefficient group raw response",
                hold_condition=f"hold_on_getco{index}_timeout_or_parse_error",
            )
        order = _add_plan_row(
            plan,
            order=order,
            analyzer_index=analyzer_index,
            row=row,
            read_role="runtime_state_mode2_1hz_average",
            command_or_source="MODE2_RUNTIME_FRAME",
            serial_command=False,
            expected_response="MODE2 active stream at 1Hz with AVERAGE1/2 evidence",
            hold_condition="hold_on_runtime_mismatch_without_repair_write",
            note="passive runtime evidence; repair writes are outside this read-only plan",
        )
        algorithm = _field(row, "algorithm", "algorithm_profile").lower() or "legacy"
        check_required = _bool(row, "check_required")
        check_capable = _bool(row, "check_capable")
        if algorithm in {"new", "new_absorption", "absorption"} or check_required or check_capable:
            order = _add_plan_row(
                plan,
                order=order,
                analyzer_index=analyzer_index,
                row=row,
                read_role="check_monitor_after_all_active_chambers_stable",
                command_or_source="CHECK,YGAS,FFF",
                expected_response="two monitor voltages and lock-temperature monitor values",
                hold_condition="hold_on_check_timeout_parse_error_or_voltage_state_review",
                note="only after all active analyzer chamber temperatures are stable",
            )
        else:
            order = _add_plan_row(
                plan,
                order=order,
                analyzer_index=analyzer_index,
                row=row,
                read_role="check_monitor_skipped_old_algorithm",
                command_or_source="",
                serial_command=False,
                enabled=False,
                expected_response="old algorithm device does not support CHECK",
                hold_condition="skip_expected_for_legacy_ratio_algorithm",
                note="old algorithm analyzers must not receive CHECK,YGAS,FFF",
            )
    return plan


def build_v1_5_formal_readonly_com_execution_plan_preview(
    *,
    formal_readonly_com_execution_packet_validator_json: str | Path | None,
    reviewed_port_inventory_json: str | Path | None = None,
    active_analyzer_list_json: str | Path | None = None,
) -> dict[str, Any]:
    packet_path = (
        Path(formal_readonly_com_execution_packet_validator_json).resolve()
        if formal_readonly_com_execution_packet_validator_json
        else None
    )
    inventory_path = Path(reviewed_port_inventory_json).resolve() if reviewed_port_inventory_json else None
    active_path = Path(active_analyzer_list_json).resolve() if active_analyzer_list_json else None
    packet_payload = _load_json(packet_path)
    inventory_payload = _load_json(inventory_path)
    active_payload = _load_json(active_path)

    packet_reasons = _packet_reasons(packet_payload)
    input_reasons = _active_input_reasons(inventory_payload, active_payload) if not packet_reasons else []
    command_plan = _build_command_plan(active_payload) if not packet_reasons and not input_reasons else []
    actual_command_rows = [row for row in command_plan if row.get("serial_command") is True]
    check_command_rows = [
        row for row in actual_command_rows if row.get("command_or_source") == "CHECK,YGAS,FFF"
    ]
    skipped_check_rows = [
        row for row in command_plan if row.get("read_role") == "check_monitor_skipped_old_algorithm"
    ]
    pacing_reasons: list[str] = []
    for row in actual_command_rows:
        if float(row.get("serial_gap_before_s") or 0.0) < MIN_SERIAL_COMMAND_GAP_S:
            pacing_reasons.append(f"order_{row.get('order')}_serial_gap_before_s={row.get('serial_gap_before_s')}")

    checks = [
        _check(
            check="packet_validator_ready_for_plan",
            status="ready" if not packet_reasons else "review_required",
            evidence_role="required_prior_packet_validator_evidence",
            reasons=packet_reasons,
            physical_meaning=(
                "The read-only COM plan preview can only be generated from a validated offline packet; "
                "a packet validator result is still not live COM authorization."
            ),
            next_action="Validate the authorization, reviewed-port, and active-analyzer packet first.",
            details={"source_path": str(packet_path) if packet_path else ""},
        ),
        _check(
            check="detailed_plan_inputs_present",
            status="ready" if not input_reasons else "review_required",
            evidence_role="future_read_plan_inputs",
            reasons=input_reasons,
            physical_meaning=(
                "A command/read plan needs the same reviewed port inventory and active analyzer list that "
                "the packet validator approved, so the preview cannot invent ports or devices."
            ),
            next_action="Provide reviewed-port and active-analyzer JSON from the same reviewed execution packet.",
            details={
                "reviewed_port_inventory_json": str(inventory_path) if inventory_path else "",
                "active_analyzer_list_json": str(active_path) if active_path else "",
            },
        ),
        _check(
            check="serial_pacing_preview",
            status="ready" if not pacing_reasons else "review_required",
            evidence_role="future_serial_pacing_plan",
            reasons=pacing_reasons,
            physical_meaning="Every future serial command row is spaced at >=1s; passive frame observations are not serial sends.",
            next_action="Keep command spacing at least 1s before any future executor implementation.",
            details={"minimum_serial_command_gap_s": MIN_SERIAL_COMMAND_GAP_S},
        ),
    ]
    review_required_count = sum(1 for row in checks if row.status == "review_required")
    if packet_reasons and packet_payload.get("overall_status") in {
        "blocked_pending_readonly_com_execution_authorization_packet",
        "",
        None,
    }:
        overall_status = LOCKED_STATUS
    elif review_required_count:
        overall_status = REVIEW_STATUS
    else:
        overall_status = READY_STATUS

    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": overall_status,
        "blocker_count": 0,
        "review_required_count": review_required_count,
        "plan_preview_ready": overall_status == READY_STATUS,
        "packet_validated_offline": not packet_reasons,
        "production_state": "offline_plan_preview_only",
        "formal_readonly_com_execution_packet_validator_json": str(packet_path) if packet_path else "",
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
        "does_not_execute_commands": True,
        "minimum_serial_command_gap_s": MIN_SERIAL_COMMAND_GAP_S,
        "active_analyzer_count": len(_rows(active_payload, "active_analyzers")),
        "future_command_count": len(actual_command_rows),
        "future_check_command_count": len(check_command_rows),
        "old_algorithm_check_skip_count": len(skipped_check_rows),
        "supports_1_to_6_active_analyzers": True,
        "supports_old_algorithm_check_skip": True,
        "checks": [row.to_json() for row in checks],
        "command_plan": command_plan,
        "next_action": (
            "Keep COM locked. Review the plan-only read sequence, then implement any future real read-only "
            "executor as a separate controlled package."
        ),
    }


def write_v1_5_formal_readonly_com_execution_plan_preview_outputs(
    model: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, Path]:
    out = Path(output_dir)
    paths = {
        "json": out / "v1_5_formal_readonly_com_execution_plan_preview.json",
        "checks_csv": out / "v1_5_formal_readonly_com_execution_plan_preview_checks.csv",
        "commands_csv": out / "v1_5_formal_readonly_com_execution_plan_preview_commands.csv",
        "summary_csv": out / "v1_5_formal_readonly_com_execution_plan_preview_summary.csv",
        "markdown": out / "V1_5_FORMAL_READONLY_COM_EXECUTION_PLAN_PREVIEW.md",
    }
    _write_json(paths["json"], model)
    _write_csv(paths["checks_csv"], model.get("checks", []))
    _write_csv(paths["commands_csv"], model.get("command_plan", []))
    _write_csv(
        paths["summary_csv"],
        [
            {
                "overall_status": model.get("overall_status"),
                "plan_preview_ready": model.get("plan_preview_ready"),
                "packet_validated_offline": model.get("packet_validated_offline"),
                "active_analyzer_count": model.get("active_analyzer_count"),
                "future_command_count": model.get("future_command_count"),
                "future_check_command_count": model.get("future_check_command_count"),
                "old_algorithm_check_skip_count": model.get("old_algorithm_check_skip_count"),
                "execution_supported": model.get("execution_supported"),
                "read_only_real_com_execution_allowed": model.get("read_only_real_com_execution_allowed"),
                "opens_com_ports": model.get("opens_com_ports"),
                "writes_sn": model.get("writes_sn"),
                "writes_coefficients": model.get("writes_coefficients"),
                "connects_postgresql": model.get("connects_postgresql"),
            }
        ],
    )
    lines = [
        "# V1.5 formal read-only COM execution plan preview",
        "",
        "This is a plan-only preview for a future read-only COM executor. It never opens COM.",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- plan_preview_ready: `{model.get('plan_preview_ready')}`",
        f"- packet_validated_offline: `{model.get('packet_validated_offline')}`",
        f"- active_analyzer_count: `{model.get('active_analyzer_count')}`",
        f"- future_command_count: `{model.get('future_command_count')}`",
        f"- future_check_command_count: `{model.get('future_check_command_count')}`",
        f"- old_algorithm_check_skip_count: `{model.get('old_algorithm_check_skip_count')}`",
        f"- read_only_real_com_execution_allowed: `{model.get('read_only_real_com_execution_allowed')}`",
        f"- opens_com_ports: `{model.get('opens_com_ports')}`",
        f"- writes_sn: `{model.get('writes_sn')}`",
        f"- writes_coefficients: `{model.get('writes_coefficients')}`",
        f"- connects_postgresql: `{model.get('connects_postgresql')}`",
        "",
        "## Checks",
        "",
    ]
    for row in model.get("checks", []):
        reasons = ";".join(row.get("reasons") or ())
        lines.append(f"- `{row.get('check')}`: `{row.get('status')}` {reasons}".rstrip())
    lines.extend(
        [
            "",
            "## Command plan",
            "",
            "See `v1_5_formal_readonly_com_execution_plan_preview_commands.csv` for the per-device read order.",
        ]
    )
    paths["markdown"].parent.mkdir(parents=True, exist_ok=True)
    paths["markdown"].write_text("\n".join(lines) + "\n", encoding="utf-8")
    return paths
