"""Minimal real read-only COM executor for V1.5 initialization evidence.

The executor is deliberately narrow: it may open reviewed analyzer COM ports
only when explicitly requested, reads identity/SN/GETCO/runtime/CHECK evidence,
and never writes analyzer state, PostgreSQL, pressure, gas, or water routes.
"""

from __future__ import annotations

import csv
import json
import re
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence


SCHEMA = "v1_5_formal_readonly_com_minimal_executor_v1"
PACKET_VALIDATOR_SCHEMA = "v1_5_formal_readonly_com_execution_packet_validator_v1"
PLAN_PREVIEW_SCHEMA = "v1_5_formal_readonly_com_execution_plan_preview_v1"
STUB_SCHEMA = "v1_5_formal_readonly_com_minimal_executor_stub_v1"
READY_STATUS = "readonly_com_minimal_executor_completed_no_write"
HOLD_STATUS = "readonly_com_minimal_executor_hold"
LOCKED_STATUS = "blocked_missing_execute_readonly_real_com"
MIN_SERIAL_COMMAND_GAP_S = 1.0
SN_PATTERN = re.compile(r"^\d{8}$")
GETCO_ROLE_PATTERN = re.compile(r"^getco(?P<index>\d+)_", re.IGNORECASE)
GETCO_COMMAND_PATTERN = re.compile(r"^GETCO,YGAS,[^,]+,(?P<index>\d+)$", re.IGNORECASE)
LEGACY_GETCO_COMMAND_PATTERN = re.compile(r"^GETCO(?P<index>\d+),", re.IGNORECASE)
COEFFICIENT_TOKEN_PATTERN = re.compile(
    r"C(?P<index>\d+)\s*:\s*(?P<value>[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)"
)
NEW_ALGORITHM_ALIASES = {"new", "new_absorption", "absorption", "absorption_ratio"}
LEGACY_ALGORITHM_ALIASES = {"legacy", "legacy_ratio", "old", "ratio"}
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


class ReadOnlySerialClient(Protocol):
    def query(self, command: str, *, timeout_s: float) -> str:
        """Send one read-only command and return the raw response line."""

    def close(self) -> None:
        """Close the serial resource."""


@dataclass(frozen=True)
class ReadonlyComHoldEvent:
    stage: str
    ga_label: str
    port: str
    protocol_device_id: str
    sn_code: str
    reason: str
    command_or_source: str = ""
    order: int | str = ""

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


class PySerialReadOnlyClient:
    """Tiny pyserial adapter loaded lazily so offline tests do not need pyserial."""

    def __init__(self, port: str, *, baudrate: int, timeout_s: float) -> None:
        try:
            import serial  # type: ignore[import-not-found]
        except Exception as exc:  # pragma: no cover - exercised only on live hosts without pyserial
            raise RuntimeError("pyserial is required for live read-only COM execution") from exc
        self._serial = serial.Serial(port=port, baudrate=baudrate, timeout=timeout_s)

    def _drain_input_buffer(self) -> None:
        """Discard active MODE2 frames queued before issuing a read-only command."""

        reset_input_buffer = getattr(self._serial, "reset_input_buffer", None)
        if callable(reset_input_buffer):
            reset_input_buffer()
            return
        flush_input = getattr(self._serial, "flushInput", None)
        if callable(flush_input):  # pragma: no cover - pyserial compatibility fallback
            flush_input()

    def query(self, command: str, *, timeout_s: float) -> str:
        self._serial.timeout = timeout_s
        self._drain_input_buffer()
        self._serial.write((command + "\r\n").encode("ascii", errors="strict"))
        raw = self._serial.readline()
        return raw.decode("utf-8", errors="replace").strip()

    def query_getco(self, command: str, *, timeout_s: float) -> str:
        """Send one GETCO read and scan past active MODE2 frames for coefficient tokens."""

        deadline = time.time() + max(0.05, float(timeout_s))
        self._serial.timeout = min(0.1, max(0.05, float(timeout_s)))
        self._drain_input_buffer()
        self._serial.write((command + "\r\n").encode("ascii", errors="strict"))
        first_response = ""
        while time.time() < deadline:
            raw = self._serial.readline()
            text = raw.decode("utf-8", errors="replace").strip()
            if not text:
                continue
            for candidate in [part.strip() for part in text.splitlines() if part.strip()]:
                if not first_response:
                    first_response = candidate
                if COEFFICIENT_TOKEN_PATTERN.search(candidate):
                    return candidate
        return first_response

    def close(self) -> None:
        self._serial.close()


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


def _mapping(row: Mapping[str, Any], *names: str) -> Mapping[str, Any]:
    for name in names:
        value = row.get(name)
        if isinstance(value, Mapping):
            return value
    return {}


def _resolve(path: str | Path | None) -> str:
    return str(Path(path).resolve()) if path else ""


def _algorithm(row: Mapping[str, Any]) -> str:
    return (_field(row, "algorithm", "algorithm_profile") or "legacy_ratio").lower()


def _is_legacy(row: Mapping[str, Any]) -> bool:
    return _algorithm(row) in LEGACY_ALGORITHM_ALIASES


def _is_new_algorithm(row: Mapping[str, Any]) -> bool:
    return _algorithm(row) in NEW_ALGORITHM_ALIASES


def _active_by_key(active_payload: Mapping[str, Any]) -> dict[tuple[str, str], Mapping[str, Any]]:
    return {
        (_field(row, "ga_label", "label"), _field(row, "port", "com_port")): row
        for row in _rows(active_payload, "active_analyzers")
    }


def _active_input_reasons(
    active_payload: Mapping[str, Any],
    inventory_payload: Mapping[str, Any],
) -> list[str]:
    reasons: list[str] = []
    active_rows = _rows(active_payload, "active_analyzers")
    inventory_pairs = {
        (_field(row, "ga_label", "label"), _field(row, "port", "com_port"))
        for row in _rows(inventory_payload, "reviewed_ports")
    }
    if not (1 <= len(active_rows) <= 6):
        reasons.append(f"active_analyzer_count={len(active_rows)}")
    labels: set[str] = set()
    ports: set[str] = set()
    sns: set[str] = set()
    for index, row in enumerate(active_rows, start=1):
        label = _field(row, "ga_label", "label")
        port = _field(row, "port", "com_port")
        protocol_id = _field(row, "protocol_device_id", "device_id")
        sn_code = _field(row, "sn_code", "device_code")
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
        if sn_code in sns:
            reasons.append(f"duplicate_active_sn_code={sn_code}")
        labels.add(label)
        ports.add(port)
        sns.add(sn_code)
        if _is_legacy(row) and (_bool(row, "check_capable") or _bool(row, "check_required")):
            reasons.append(f"active_{index}_old_algorithm_check_must_be_skipped")
        if _is_new_algorithm(row) and not (_bool(row, "check_capable") and _bool(row, "check_required")):
            reasons.append(f"active_{index}_new_algorithm_check_must_be_required")
        runtime = row.get("runtime_evidence")
        ftd_hz = row.get("ftd_hz", row.get("runtime_hz", row.get("mode2_upload_hz")))
        average1 = row.get("average1", row.get("average1_setting"))
        average2 = row.get("average2", row.get("average2_setting"))
        if isinstance(runtime, Mapping):
            ftd_hz = runtime.get("ftd_hz", runtime.get("runtime_hz", ftd_hz))
            average1 = runtime.get("average1", runtime.get("average1_setting", average1))
            average2 = runtime.get("average2", runtime.get("average2_setting", average2))
        try:
            ftd_ok = abs(float(ftd_hz) - 1.0) < 1e-9
        except (TypeError, ValueError):
            ftd_ok = False
        if not ftd_ok:
            reasons.append(f"active_{index}_runtime_1hz_evidence_missing")
        if average1 in (None, "") or average2 in (None, ""):
            reasons.append(f"active_{index}_average1_average2_evidence_missing")
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


def _authorization_shape_reasons(payload: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    if not payload:
        return ["authorization_packet_missing"]
    for field in ("authorization_id", "operator", "reviewer", "approver"):
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
    try:
        minimum_gap = float(payload.get("minimum_serial_command_gap_s") or 0.0)
    except (TypeError, ValueError):
        minimum_gap = 0.0
    try:
        retry_gap = float(payload.get("retry_gap_s") or 0.0)
    except (TypeError, ValueError):
        retry_gap = 0.0
    if minimum_gap < MIN_SERIAL_COMMAND_GAP_S:
        reasons.append(f"minimum_serial_command_gap_s={payload.get('minimum_serial_command_gap_s')!r}")
    if retry_gap < MIN_SERIAL_COMMAND_GAP_S:
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


def _source_reasons(
    *,
    authorization_path: Path | None,
    authorization_payload: Mapping[str, Any],
    packet_payload: Mapping[str, Any],
    plan_payload: Mapping[str, Any],
    stub_payload: Mapping[str, Any],
    inventory_path: Path | None,
    active_path: Path | None,
    inventory_payload: Mapping[str, Any],
    active_payload: Mapping[str, Any],
) -> list[str]:
    reasons: list[str] = []
    if packet_payload.get("schema") != PACKET_VALIDATOR_SCHEMA:
        reasons.append(f"packet_schema={packet_payload.get('schema') or 'missing'}")
    if packet_payload.get("overall_status") != "ready_for_readonly_com_execution_packet_review":
        reasons.append(f"packet_overall_status={packet_payload.get('overall_status') or 'missing'}")
    if packet_payload.get("packet_validated_offline") is not True:
        reasons.append(f"packet_validated_offline={packet_payload.get('packet_validated_offline')!r}")
    expected_authorization = str(packet_payload.get("authorization_packet_json") or "").strip()
    if not expected_authorization:
        reasons.append("packet_validator_authorization_packet_json_missing")
    elif _resolve(expected_authorization) != _resolve(authorization_path):
        reasons.append("authorization_packet_json_mismatch_with_packet_validator")
    reasons.extend(_authorization_shape_reasons(authorization_payload))
    if plan_payload.get("schema") != PLAN_PREVIEW_SCHEMA:
        reasons.append(f"plan_schema={plan_payload.get('schema') or 'missing'}")
    if plan_payload.get("overall_status") != "ready_for_readonly_com_execution_plan_preview_review":
        reasons.append(f"plan_overall_status={plan_payload.get('overall_status') or 'missing'}")
    if plan_payload.get("plan_preview_ready") is not True:
        reasons.append(f"plan_preview_ready={plan_payload.get('plan_preview_ready')!r}")
    if stub_payload.get("schema") != STUB_SCHEMA:
        reasons.append(f"stub_schema={stub_payload.get('schema') or 'missing'}")
    if stub_payload.get("overall_status") != "blocked_plan_only_minimal_readonly_com_executor_stub":
        reasons.append(f"stub_overall_status={stub_payload.get('overall_status') or 'missing'}")
    if stub_payload.get("minimal_executor_stub_ready") is not True:
        reasons.append(f"minimal_executor_stub_ready={stub_payload.get('minimal_executor_stub_ready')!r}")
    for label, payload in (("packet", packet_payload), ("plan", plan_payload), ("stub", stub_payload)):
        for field in (
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
                reasons.append(f"{label}_boundary_{field}={payload.get(field)!r}")
    expected_inventory = str(packet_payload.get("reviewed_port_inventory_json") or "")
    expected_active = str(packet_payload.get("active_analyzer_list_json") or "")
    if expected_inventory and _resolve(expected_inventory) != _resolve(inventory_path):
        reasons.append("reviewed_port_inventory_json_mismatch_with_packet_validator")
    if expected_active and _resolve(expected_active) != _resolve(active_path):
        reasons.append("active_analyzer_list_json_mismatch_with_packet_validator")
    if str(plan_payload.get("reviewed_port_inventory_json") or "") and (
        _resolve(plan_payload.get("reviewed_port_inventory_json")) != _resolve(inventory_path)
    ):
        reasons.append("reviewed_port_inventory_json_mismatch_with_plan_preview")
    if str(plan_payload.get("active_analyzer_list_json") or "") and (
        _resolve(plan_payload.get("active_analyzer_list_json")) != _resolve(active_path)
    ):
        reasons.append("active_analyzer_list_json_mismatch_with_plan_preview")
    reasons.extend(_active_input_reasons(active_payload, inventory_payload))
    command_plan = plan_payload.get("command_plan") if isinstance(plan_payload.get("command_plan"), list) else []
    if not command_plan:
        reasons.append("command_plan=missing")
    for row in command_plan:
        if not isinstance(row, Mapping):
            continue
        if row.get("command_or_source") == "CHECK,YGAS,FFF":
            active = _active_by_key(active_payload).get(
                (_field(row, "ga_label", "label"), _field(row, "port", "com_port")),
                {},
            )
            if not _is_new_algorithm(active):
                reasons.append(f"legacy_check_command_planned={row.get('ga_label')}/{row.get('port')}")
        if row.get("serial_command") is True and float(row.get("serial_gap_before_s") or 0.0) < MIN_SERIAL_COMMAND_GAP_S:
            reasons.append(f"plan_serial_gap_below_1s_order={row.get('order')}")
    return reasons


def _parse_sn(raw: str) -> str:
    for token in reversed([part.strip() for part in raw.split(",")]):
        if SN_PATTERN.match(token):
            return token
    match = re.search(r"(?<!\d)(\d{8})(?!\d)", raw)
    return match.group(1) if match else ""


def _getco_group(plan_row: Mapping[str, Any], command_or_source: str) -> str:
    read_role = str(plan_row.get("read_role") or "")
    role_match = GETCO_ROLE_PATTERN.match(read_role)
    if role_match:
        return f"GETCO{int(role_match.group('index'))}"
    for pattern in (GETCO_COMMAND_PATTERN, LEGACY_GETCO_COMMAND_PATTERN):
        match = pattern.match(str(command_or_source or "").strip())
        if match:
            return f"GETCO{int(match.group('index'))}"
    return ""


def _expected_getco_indexes(group: str) -> tuple[int, ...]:
    if group in {"GETCO5", "GETCO6"}:
        return (0, 1)
    if group in {"GETCO7", "GETCO8"}:
        return (0, 1, 2, 3)
    return ()


def _parse_coefficient_values(raw: str, group: str) -> list[float]:
    """Parse mature V1.5 GETCO coefficient tokens and reject malformed or active frames."""

    text = str(raw or "").strip().strip("<>")
    matches = list(COEFFICIENT_TOKEN_PATTERN.finditer(text))
    if not matches:
        return []
    values_by_index: dict[int, float] = {}
    for match in matches:
        index = int(match.group("index"))
        if index in values_by_index:
            return []
        values_by_index[index] = float(match.group("value"))
    expected_indexes = _expected_getco_indexes(group)
    if expected_indexes:
        if tuple(sorted(values_by_index)) != expected_indexes:
            return []
        return [values_by_index[index] for index in expected_indexes]
    sorted_indexes = tuple(sorted(values_by_index))
    if sorted_indexes != tuple(range(len(sorted_indexes))):
        return []
    return [values_by_index[index] for index in sorted_indexes]


def _neutral_status(group: str, values: Sequence[float]) -> str:
    tolerance = 1e-3
    if group in {"GETCO5", "GETCO6"}:
        if len(values) < 2:
            return "missing"
        return "neutral" if abs(values[0]) <= tolerance and abs(values[1] - 1.0) <= tolerance else "non_neutral"
    if group in {"GETCO7", "GETCO8"}:
        if len(values) < 4:
            return "missing"
        target = (0.0, 1.0, 0.0, 0.0)
        return "neutral" if all(abs(values[index] - target[index]) <= tolerance for index in range(4)) else "non_neutral"
    return ""


def _runtime_evidence(row: Mapping[str, Any]) -> dict[str, Any]:
    runtime = row.get("runtime_evidence") if isinstance(row.get("runtime_evidence"), Mapping) else {}
    return {
        "ftd_hz": runtime.get("ftd_hz", row.get("ftd_hz", row.get("runtime_hz", row.get("mode2_upload_hz")))),
        "average1": runtime.get("average1", row.get("average1", row.get("average1_setting"))),
        "average2": runtime.get("average2", row.get("average2", row.get("average2_setting"))),
        "filter": runtime.get("filter", row.get("filter")),
        "source": "active_analyzer_list_passive_runtime_evidence",
    }


def _append_hold(
    holds: list[dict[str, Any]],
    *,
    stage: str,
    row: Mapping[str, Any] | None,
    reason: str,
    command_or_source: str = "",
    order: int | str = "",
) -> None:
    row = row or {}
    holds.append(
        ReadonlyComHoldEvent(
            stage=stage,
            ga_label=_field(row, "ga_label", "label"),
            port=_field(row, "port", "com_port"),
            protocol_device_id=_field(row, "protocol_device_id", "device_id"),
            sn_code=_field(row, "sn_code", "device_code"),
            reason=reason,
            command_or_source=command_or_source,
            order=order,
        ).to_json()
    )


def _attempt_row(
    *,
    plan_row: Mapping[str, Any],
    result_status: str,
    command_or_source: str,
    started_at: str,
    ended_at: str,
    raw_response: str = "",
    error: str = "",
    gap_wait_s: float | str = "",
    retry_count: int = 0,
    retry_gap_s: float | str = "",
) -> dict[str, Any]:
    return {
        "order": plan_row.get("order", ""),
        "ga_label": plan_row.get("ga_label", ""),
        "port": plan_row.get("port", ""),
        "protocol_device_id": plan_row.get("protocol_device_id", ""),
        "sn_code": plan_row.get("sn_code", ""),
        "algorithm": plan_row.get("algorithm", ""),
        "read_role": plan_row.get("read_role", ""),
        "command_or_source": command_or_source,
        "serial_command": plan_row.get("serial_command", False),
        "started_at": started_at,
        "ended_at": ended_at,
        "gap_wait_s": gap_wait_s,
        "result_status": result_status,
        "raw_response": raw_response,
        "error": error,
        "retry_count": retry_count,
        "retry_gap_s": retry_gap_s,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "connects_postgresql": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
    }


def _real_client_factory(*, baudrate: int, timeout_s: float) -> Callable[[str], ReadOnlySerialClient]:
    def factory(port: str) -> ReadOnlySerialClient:
        return PySerialReadOnlyClient(port, baudrate=baudrate, timeout_s=timeout_s)

    return factory


def _query_readonly_command(
    client: ReadOnlySerialClient,
    command_or_source: str,
    *,
    timeout_s: float,
    getco_group: str,
    retry_gap_s: float,
    sleeper: Callable[[float], None],
) -> tuple[str, int]:
    def query_once() -> str:
        if getco_group:
            query_getco = getattr(client, "query_getco", None)
            if callable(query_getco):
                return str(query_getco(command_or_source, timeout_s=timeout_s)).strip()
        return client.query(command_or_source, timeout_s=timeout_s).strip()

    def valid_response(raw_response: str) -> bool:
        if command_or_source == "SN,YGAS,FFF":
            return bool(_parse_sn(raw_response))
        if getco_group:
            return bool(_parse_coefficient_values(raw_response, getco_group))
        return True

    raw = query_once()
    if (command_or_source == "SN,YGAS,FFF" or getco_group) and not valid_response(raw):
        gap = max(MIN_SERIAL_COMMAND_GAP_S, float(retry_gap_s or 0.0))
        sleeper(gap)
        return query_once(), 1
    return raw, 0


def build_v1_5_formal_readonly_com_minimal_executor(
    *,
    execute_read_only_real_com: bool,
    authorization_packet_json: str | Path | None,
    reviewed_port_inventory_json: str | Path | None,
    active_analyzer_list_json: str | Path | None,
    formal_readonly_com_execution_packet_validator_json: str | Path | None,
    formal_readonly_com_execution_plan_preview_json: str | Path | None,
    formal_readonly_com_minimal_executor_stub_json: str | Path | None,
    baudrate: int = 115200,
    timeout_s: float = 2.0,
    command_gap_s: float = MIN_SERIAL_COMMAND_GAP_S,
    client_factory: Callable[[str], ReadOnlySerialClient] | None = None,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Run the minimal read-only executor or return a locked/hold artifact."""

    authorization_path = Path(authorization_packet_json).resolve() if authorization_packet_json else None
    inventory_path = Path(reviewed_port_inventory_json).resolve() if reviewed_port_inventory_json else None
    active_path = Path(active_analyzer_list_json).resolve() if active_analyzer_list_json else None
    packet_path = (
        Path(formal_readonly_com_execution_packet_validator_json).resolve()
        if formal_readonly_com_execution_packet_validator_json
        else None
    )
    plan_path = (
        Path(formal_readonly_com_execution_plan_preview_json).resolve()
        if formal_readonly_com_execution_plan_preview_json
        else None
    )
    stub_path = (
        Path(formal_readonly_com_minimal_executor_stub_json).resolve()
        if formal_readonly_com_minimal_executor_stub_json
        else None
    )

    authorization_payload = _load_json(authorization_path)
    inventory_payload = _load_json(inventory_path)
    active_payload = _load_json(active_path)
    packet_payload = _load_json(packet_path)
    plan_payload = _load_json(plan_path)
    stub_payload = _load_json(stub_path)
    command_plan = plan_payload.get("command_plan") if isinstance(plan_payload.get("command_plan"), list) else []
    active_map = _active_by_key(active_payload)
    command_gap = max(MIN_SERIAL_COMMAND_GAP_S, float(command_gap_s or 0.0))
    try:
        retry_gap = max(MIN_SERIAL_COMMAND_GAP_S, float(authorization_payload.get("retry_gap_s") or 0.0))
    except (TypeError, ValueError):
        retry_gap = MIN_SERIAL_COMMAND_GAP_S
    attempts: list[dict[str, Any]] = []
    raw_responses: list[dict[str, Any]] = []
    holds: list[dict[str, Any]] = []
    snapshots: list[dict[str, Any]] = []

    preflight_reasons = _source_reasons(
        authorization_path=authorization_path,
        authorization_payload=authorization_payload,
        packet_payload=packet_payload,
        plan_payload=plan_payload,
        stub_payload=stub_payload,
        inventory_path=inventory_path,
        active_path=active_path,
        inventory_payload=inventory_payload,
        active_payload=active_payload,
    )
    if float(command_gap_s or 0.0) < MIN_SERIAL_COMMAND_GAP_S:
        preflight_reasons.append(f"command_gap_s={command_gap_s!r}")
    if not execute_read_only_real_com:
        preflight_reasons.append("execute_read_only_real_com_flag_missing")
    if authorization_payload.get("requested_flag") != "--execute-read-only-real-com":
        preflight_reasons.append(f"authorization_requested_flag={authorization_payload.get('requested_flag')!r}")

    if preflight_reasons:
        for reason in preflight_reasons:
            _append_hold(holds, stage="pre_open_preflight", row=None, reason=reason)
        overall_status = LOCKED_STATUS if not execute_read_only_real_com else HOLD_STATUS
        executed = False
    else:
        executed = True
        factory = client_factory or _real_client_factory(baudrate=baudrate, timeout_s=timeout_s)
        client_by_port: dict[str, ReadOnlySerialClient] = {}
        snapshot_by_key: dict[tuple[str, str], dict[str, Any]] = {}
        try:
            for plan_row in command_plan:
                if not isinstance(plan_row, Mapping):
                    continue
                key = (_field(plan_row, "ga_label", "label"), _field(plan_row, "port", "com_port"))
                active_row = active_map.get(key, {})
                snapshot = snapshot_by_key.setdefault(
                    key,
                    {
                        "ga_label": key[0],
                        "port": key[1],
                        "protocol_device_id_expected": _field(active_row, "protocol_device_id", "device_id"),
                        "sn_code_expected": _field(active_row, "sn_code", "device_code"),
                        "algorithm": _algorithm(active_row),
                        "sn_code_read": "",
                        "getco": {},
                        "auxiliary_neutrality": {},
                        "runtime_evidence": _runtime_evidence(active_row),
                        "check_monitor_raw": "",
                        "holds": [],
                    },
                )
                command_or_source = str(plan_row.get("command_or_source") or "")
                started_at = _now()
                if plan_row.get("serial_command") is not True:
                    attempts.append(
                        _attempt_row(
                            plan_row=plan_row,
                            result_status="passive_recorded",
                            command_or_source=command_or_source,
                            started_at=started_at,
                            ended_at=_now(),
                        )
                    )
                    raw_responses.append(
                        {
                            "order": plan_row.get("order", ""),
                            "ga_label": key[0],
                            "port": key[1],
                            "read_role": plan_row.get("read_role", ""),
                            "command_or_source": command_or_source,
                            "raw_response": json.dumps(snapshot.get("runtime_evidence"), ensure_ascii=False)
                            if plan_row.get("read_role") == "runtime_state_mode2_1hz_average"
                            else "",
                            "parsed_value": "",
                            "result_status": "passive_recorded",
                        }
                    )
                    continue
                if command_or_source == "CHECK,YGAS,FFF" and not _is_new_algorithm(active_row):
                    reason = "legacy_algorithm_check_command_blocked_before_send"
                    _append_hold(
                        holds,
                        stage="check_monitor",
                        row=active_row,
                        reason=reason,
                        command_or_source=command_or_source,
                        order=plan_row.get("order", ""),
                    )
                    snapshot["holds"].append(reason)
                    attempts.append(
                        _attempt_row(
                            plan_row=plan_row,
                            result_status="hold_before_send",
                            command_or_source=command_or_source,
                            started_at=started_at,
                            ended_at=_now(),
                            error=reason,
                        )
                    )
                    continue
                sleeper(command_gap)
                try:
                    client = client_by_port.get(key[1])
                    if client is None:
                        client = factory(key[1])
                        client_by_port[key[1]] = client
                    raw, retry_count = _query_readonly_command(
                        client,
                        command_or_source,
                        timeout_s=timeout_s,
                        getco_group=_getco_group(plan_row, command_or_source),
                        retry_gap_s=retry_gap,
                        sleeper=sleeper,
                    )
                    ended_at = _now()
                except Exception as exc:
                    raw = ""
                    retry_count = 0
                    ended_at = _now()
                    reason = f"serial_query_failed:{exc}"
                    _append_hold(
                        holds,
                        stage="serial_query",
                        row=active_row,
                        reason=reason,
                        command_or_source=command_or_source,
                        order=plan_row.get("order", ""),
                    )
                    snapshot["holds"].append(reason)
                    attempts.append(
                        _attempt_row(
                            plan_row=plan_row,
                            result_status="hold",
                            command_or_source=command_or_source,
                            started_at=started_at,
                            ended_at=ended_at,
                            error=reason,
                            gap_wait_s=command_gap,
                            retry_count=retry_count,
                            retry_gap_s=retry_gap if retry_count else "",
                        )
                    )
                    continue
                result_status = "ok" if raw else "hold"
                parsed_value: Any = ""
                if not raw:
                    reason = "empty_raw_response"
                    _append_hold(
                        holds,
                        stage="serial_response",
                        row=active_row,
                        reason=reason,
                        command_or_source=command_or_source,
                        order=plan_row.get("order", ""),
                    )
                    snapshot["holds"].append(reason)
                elif command_or_source == "SN,YGAS,FFF":
                    sn_read = _parse_sn(raw)
                    parsed_value = sn_read
                    snapshot["sn_code_read"] = sn_read
                    if sn_read != snapshot["sn_code_expected"]:
                        reason = f"sn_mismatch_expected_{snapshot['sn_code_expected']}_read_{sn_read or 'missing'}"
                        _append_hold(
                            holds,
                            stage="sn_device_code",
                            row=active_row,
                            reason=reason,
                            command_or_source=command_or_source,
                            order=plan_row.get("order", ""),
                        )
                        snapshot["holds"].append(reason)
                        result_status = "hold"
                elif _getco_group(plan_row, command_or_source):
                    group = _getco_group(plan_row, command_or_source)
                    values = _parse_coefficient_values(raw, group)
                    parsed_value = json.dumps(values, ensure_ascii=False)
                    snapshot["getco"][group] = {"raw": raw, "values": values}
                    neutral = _neutral_status(group, values)
                    if neutral:
                        snapshot["auxiliary_neutrality"][group] = neutral
                        if neutral != "neutral":
                            reason = f"{group.lower()}_{neutral}"
                            _append_hold(
                                holds,
                                stage="auxiliary_neutrality",
                                row=active_row,
                                reason=reason,
                                command_or_source=command_or_source,
                                order=plan_row.get("order", ""),
                            )
                            snapshot["holds"].append(reason)
                            result_status = "hold"
                    if not values:
                        reason = f"{group.lower()}_parse_error"
                        _append_hold(
                            holds,
                            stage="getco_epoch0",
                            row=active_row,
                            reason=reason,
                            command_or_source=command_or_source,
                            order=plan_row.get("order", ""),
                        )
                        snapshot["holds"].append(reason)
                        result_status = "hold"
                elif command_or_source == "CHECK,YGAS,FFF":
                    snapshot["check_monitor_raw"] = raw
                    parsed_value = raw
                attempts.append(
                    _attempt_row(
                        plan_row=plan_row,
                        result_status=result_status,
                        command_or_source=command_or_source,
                        started_at=started_at,
                        ended_at=ended_at,
                        raw_response=raw,
                        gap_wait_s=command_gap,
                        retry_count=retry_count,
                        retry_gap_s=retry_gap if retry_count else "",
                    )
                )
                raw_responses.append(
                    {
                        "order": plan_row.get("order", ""),
                        "ga_label": key[0],
                        "port": key[1],
                        "read_role": plan_row.get("read_role", ""),
                        "command_or_source": command_or_source,
                        "raw_response": raw,
                        "parsed_value": parsed_value,
                        "result_status": result_status,
                        "retry_count": retry_count,
                        "retry_gap_s": retry_gap if retry_count else "",
                    }
                )
        finally:
            for client in client_by_port.values():
                client.close()
        snapshots = list(snapshot_by_key.values())
        overall_status = HOLD_STATUS if holds else READY_STATUS

    if not snapshots:
        for row in _rows(active_payload, "active_analyzers"):
            snapshots.append(
                {
                    "ga_label": _field(row, "ga_label", "label"),
                    "port": _field(row, "port", "com_port"),
                    "protocol_device_id_expected": _field(row, "protocol_device_id", "device_id"),
                    "sn_code_expected": _field(row, "sn_code", "device_code"),
                    "algorithm": _algorithm(row),
                    "sn_code_read": "",
                    "getco": {},
                    "auxiliary_neutrality": {},
                    "runtime_evidence": _runtime_evidence(row),
                    "check_monitor_raw": "",
                    "holds": [],
                }
            )

    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": overall_status,
        "blocker_count": 0,
        "hold_count": len(holds),
        "review_required_count": len(holds),
        "minimal_readonly_com_executor_ready": overall_status == READY_STATUS,
        "production_state": "manual_authorized_read_only_com_no_write",
        "execution_supported": True,
        "execution_requested": execute_read_only_real_com,
        "execution_attempted": executed,
        "live_execution_allowed": execute_read_only_real_com and executed,
        "read_only_real_com_execution_allowed": execute_read_only_real_com and executed,
        "controlled_write_execution_allowed": False,
        "real_com_execution_allowed": execute_read_only_real_com and executed,
        "execute_flag_allowed": True,
        "opens_com_ports": execute_read_only_real_com and executed,
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
        "required_read_only_real_com_flag": "--execute-read-only-real-com",
        "minimum_serial_command_gap_s": MIN_SERIAL_COMMAND_GAP_S,
        "command_gap_s": command_gap,
        "retry_gap_s": retry_gap,
        "baudrate": baudrate,
        "timeout_s": timeout_s,
        "authorization_packet_json": str(authorization_path) if authorization_path else "",
        "reviewed_port_inventory_json": str(inventory_path) if inventory_path else "",
        "active_analyzer_list_json": str(active_path) if active_path else "",
        "formal_readonly_com_execution_packet_validator_json": str(packet_path) if packet_path else "",
        "formal_readonly_com_execution_plan_preview_json": str(plan_path) if plan_path else "",
        "formal_readonly_com_minimal_executor_stub_json": str(stub_path) if stub_path else "",
        "active_analyzer_count": len(_rows(active_payload, "active_analyzers")),
        "command_attempt_count": len(attempts),
        "raw_response_count": len(raw_responses),
        "read_retry_count": sum(int(row.get("retry_count") or 0) for row in attempts),
        "identity_getco_snapshot_count": len(snapshots),
        "check_command_attempt_count": sum(1 for row in attempts if row.get("command_or_source") == "CHECK,YGAS,FFF"),
        "legacy_check_command_attempt_count": sum(
            1
            for row in attempts
            if row.get("command_or_source") == "CHECK,YGAS,FFF"
            and _algorithm(active_map.get((str(row.get("ga_label")), str(row.get("port"))), {}))
            in LEGACY_ALGORITHM_ALIASES
        ),
        "command_attempts": attempts,
        "raw_responses": raw_responses,
        "hold_events": holds,
        "identity_getco_snapshots": snapshots,
        "next_action": (
            "Review hold events before physical flow."
            if holds
            else "Use this read-only evidence for initialization readiness review only; it is not release/import evidence."
        ),
    }


def write_v1_5_formal_readonly_com_minimal_executor_outputs(
    model: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, Path]:
    out = Path(output_dir)
    paths = {
        "json": out / "v1_5_formal_readonly_com_minimal_executor.json",
        "invocation_json": out / "readonly_com_executor_invocation.json",
        "command_attempts_csv": out / "readonly_com_command_attempts.csv",
        "raw_responses_csv": out / "readonly_com_raw_responses.csv",
        "hold_events_csv": out / "readonly_com_hold_events.csv",
        "identity_getco_snapshot_json": out / "readonly_com_identity_getco_snapshot.json",
        "markdown": out / "V1_5_FORMAL_READONLY_COM_MINIMAL_EXECUTOR.md",
    }
    _write_json(paths["json"], model)
    invocation = {
        key: model.get(key)
        for key in (
            "schema",
            "generated_at",
            "overall_status",
            "execution_requested",
            "execution_attempted",
            "opens_com_ports",
            "writes_sn",
            "writes_coefficients",
            "connects_postgresql",
            "controls_pressure",
            "controls_water_or_gas_routes",
            "active_analyzer_count",
            "command_attempt_count",
            "hold_count",
            "authorization_packet_json",
            "reviewed_port_inventory_json",
            "active_analyzer_list_json",
        )
    }
    _write_json(paths["invocation_json"], invocation)
    _write_csv(paths["command_attempts_csv"], model.get("command_attempts", []))
    _write_csv(paths["raw_responses_csv"], model.get("raw_responses", []))
    _write_csv(paths["hold_events_csv"], model.get("hold_events", []))
    _write_json(
        paths["identity_getco_snapshot_json"],
        {
            "schema": "v1_5_readonly_com_identity_getco_snapshot_v1",
            "generated_at": model.get("generated_at"),
            "overall_status": model.get("overall_status"),
            "not_real_acceptance_evidence": True,
            "snapshots": model.get("identity_getco_snapshots", []),
        },
    )
    lines = [
        "# V1.5 formal read-only COM minimal executor",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- execution_attempted: `{model.get('execution_attempted')}`",
        f"- opens_com_ports: `{model.get('opens_com_ports')}`",
        f"- command_attempt_count: `{model.get('command_attempt_count')}`",
        f"- hold_count: `{model.get('hold_count')}`",
        f"- writes_sn: `{model.get('writes_sn')}`",
        f"- writes_coefficients: `{model.get('writes_coefficients')}`",
        f"- connects_postgresql: `{model.get('connects_postgresql')}`",
        f"- controls_water_or_gas_routes: `{model.get('controls_water_or_gas_routes')}`",
        "",
        "This artifact is read-only initialization evidence. It is not formal release or database import evidence.",
    ]
    if model.get("hold_events"):
        lines.extend(["", "## Hold Events", ""])
        for row in model.get("hold_events", []):
            lines.append(
                f"- `{row.get('stage')}` `{row.get('ga_label')}` `{row.get('port')}`: {row.get('reason')}"
            )
    paths["markdown"].parent.mkdir(parents=True, exist_ok=True)
    paths["markdown"].write_text("\n".join(lines) + "\n", encoding="utf-8")
    return paths
