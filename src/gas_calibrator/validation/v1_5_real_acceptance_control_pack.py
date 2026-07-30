"""Build the offline V1.5 real-acceptance control pack.

The pack connects three existing governance surfaces without executing them:
the operator-reviewed site profile, the mature workstation/certificate gates,
and the formal evidence lifecycle.  It never opens COM, sends device commands,
controls routes, writes analyzer state, or promotes evidence automatically.
"""

from __future__ import annotations

import copy
import csv
import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "v1_5_real_acceptance_control_pack_v1"
SITE_PROFILE_SCHEMA = "v1_5_real_acceptance_site_profile_v1"
READONLY_EXECUTOR_SCHEMA = "v1_5_formal_readonly_com_minimal_executor_v1"
CURRENT_PROBE_BINDING_SCHEMA = "v1_5_current_site_probe_evidence_binding_v1"
PASSIVE_PROBE_SCHEMA = "v1_5_passive_site_inventory_probe_v1"
POWERED_IDENTITY_QUERY_SCHEMA = "v1_5_powered_analyzer_identity_query_v1"
CURRENT_INITIALIZATION_PROBE_SCHEMA = (
    "v1_5_current_powered_initialization_probe_v1"
)
RUNTIME_SETTING_READABILITY_REVIEW_SCHEMA = (
    "v1_5_runtime_setting_readability_review_v1"
)
ALGORITHM_CLASSIFICATION_EVIDENCE_SCHEMA = (
    "v1_5_algorithm_classification_evidence_v1"
)
RUNTIME_SETUP_EVIDENCE_SCHEMA = "v1_5_runtime_setup_evidence_binding_v1"
RUNTIME_SETUP_RESULT_SCHEMA = "v1_5_analyzer_runtime_setup_result_v0"
ALGORITHM_EVIDENCE_SOURCE_TYPES = {
    "production_batch_record",
    "firmware_manifest",
    "manufacturer_device_record",
}
SN_PATTERN = re.compile(r"^\d{8}$")
ANALYZER_BANK = tuple(f"COM{index}" for index in range(35, 43))
LEGACY_ALGORITHMS = {"legacy", "legacy_ratio", "old", "ratio"}
NEW_ALGORITHMS = {"new", "new_absorption", "absorption", "absorption_ratio"}
INITIALIZATION_QUERY_WHITELIST = tuple(
    f"GETCO,YGAS,FFF,{group}" for group in (5, 6, 7, 8)
)
NEUTRAL_TEMPERATURE_INPUT = (0.0, 1.0, 0.0, 0.0)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return {}
    source = Path(path)
    if not source.is_file():
        return {}
    payload = json.loads(source.read_text(encoding="utf-8-sig"))
    return payload if isinstance(payload, dict) else {}


def _sha256(path: str | Path | None) -> str:
    if not path:
        return ""
    source = Path(path)
    return hashlib.sha256(source.read_bytes()).hexdigest() if source.is_file() else ""


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
        writer.writerows({key: row.get(key, "") for key in fields} for row in rows)


def _rows(payload: Mapping[str, Any], key: str) -> list[Mapping[str, Any]]:
    value = payload.get(key)
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return [row for row in value if isinstance(row, Mapping)]


def _text(row: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = row.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _runtime_evidence(row: Mapping[str, Any]) -> Mapping[str, Any]:
    value = row.get("runtime_evidence")
    return value if isinstance(value, Mapping) else {}


def build_v1_5_real_acceptance_site_profile_template(
    *,
    runtime_port_inventory_json: str | Path,
    reported_connected_count: int = 4,
    reported_powered_count: int = 2,
    observation_id: str = "operator_report_unverified",
) -> dict[str, Any]:
    """Create an editable, deliberately non-executable site profile template."""

    if not 0 <= reported_powered_count <= reported_connected_count <= len(ANALYZER_BANK):
        raise ValueError("reported counts must satisfy 0 <= powered <= connected <= 8")
    inventory_path = Path(runtime_port_inventory_json).resolve()
    inventory = _load_json(inventory_path)
    visible = {
        str(row.get("port") or "").upper()
        for row in _rows(inventory, "ports")
        if str(row.get("port") or "").strip()
    }
    candidates = [
        {
            "port": port,
            "os_visible": port in visible,
            "connected": None,
            "powered": None,
            "operator_confirmed": False,
            "ga_label": "",
            "protocol_device_id": "",
            "sn_code": "",
            "algorithm": "",
            "algorithm_evidence": {},
            "check_capable": None,
            "check_required": None,
            "runtime_evidence": {
                "ftd_hz": None,
                "average1": "",
                "average2": "",
                "filter": "",
            },
        }
        for port in ANALYZER_BANK
    ]
    return {
        "schema": SITE_PROFILE_SCHEMA,
        "generated_at": _now(),
        "observation_id": observation_id,
        "observation_basis": "operator_report_only_not_reverified",
        "reported_connected_count": reported_connected_count,
        "reported_powered_count": reported_powered_count,
        "runtime_port_inventory_json": str(inventory_path),
        "runtime_port_inventory_sha256": _sha256(inventory_path),
        "candidate_analyzers": candidates,
        "profile_status": "operator_mapping_required",
        "active_analyzer_policy": "only_powered_and_operator_confirmed_rows_may_enter_readonly_packet",
        "opens_com_ports": False,
        "sends_device_commands": False,
        "writes_sn": False,
        "writes_coefficients": False,
        "controls_water_or_gas_routes": False,
        "not_real_acceptance_evidence": True,
    }


def prefill_v1_5_site_profile_from_historical_identity(
    *,
    site_profile: Mapping[str, Any],
    historical_identity_csv: str | Path,
    historical_runtime_config_json: str | Path,
) -> dict[str, Any]:
    """Prefill traceable identity fields without inferring current site state."""

    identity_path = Path(historical_identity_csv).resolve()
    runtime_path = Path(historical_runtime_config_json).resolve()
    if not identity_path.is_file():
        raise ValueError(f"historical identity CSV missing: {identity_path}")
    runtime_config = _load_json(runtime_path)
    if not runtime_config:
        raise ValueError(f"historical runtime config missing or invalid: {runtime_path}")

    with identity_path.open("r", encoding="utf-8-sig", newline="") as handle:
        identity_rows = [dict(row) for row in csv.DictReader(handle)]
    identity_sha256 = _sha256(identity_path)
    runtime_sha256 = _sha256(runtime_path)
    profile = copy.deepcopy(dict(site_profile))
    profile_rows = _rows(profile, "candidate_analyzers")
    by_port = {str(row.get("port") or "").upper(): row for row in profile_rows}
    runtime_devices = runtime_config.get("devices")
    runtime_devices = runtime_devices if isinstance(runtime_devices, Mapping) else {}
    runtime_rows = _rows(runtime_devices, "gas_analyzers")
    runtime_by_port = {
        str(row.get("port") or "").upper(): row
        for row in runtime_rows
        if str(row.get("port") or "").strip()
    }

    reasons: list[str] = []
    if not identity_rows:
        reasons.append("historical_identity_rows_missing")
    applied_ports: list[str] = []
    seen_ports: set[str] = set()
    for source_row in identity_rows:
        port = _text(source_row, "old_port", "port").upper()
        if port in seen_ports:
            reasons.append(f"duplicate_historical_identity_port={port}")
            continue
        seen_ports.add(port)
        target = by_port.get(port)
        if target is None:
            reasons.append(f"historical_identity_port_outside_candidate_bank={port}")
            continue
        runtime = runtime_by_port.get(port)
        if runtime is None:
            reasons.append(f"{port}_historical_runtime_missing")
            continue

        old_device_id = _text(source_row, "old_device_id")
        runtime_device_id = _text(runtime, "device_id")
        protocol_device_id = _text(source_row, "sn_write_protocol_device_id")
        sn_code = _text(source_row, "final_sn")
        sn_readback = _text(source_row, "sn_readback")
        evidence_level = _text(source_row, "evidence_level")
        status = _text(source_row, "status")
        row_reasons: list[str] = []
        if old_device_id != runtime_device_id:
            row_reasons.append(f"{port}_historical_old_device_id_mismatch")
        if status != "formal_identity_ready":
            row_reasons.append(f"{port}_historical_identity_status={status or 'missing'}")
        if evidence_level != "owner_attested_traceable":
            row_reasons.append(
                f"{port}_historical_evidence_level={evidence_level or 'missing'}"
            )
        if sn_code != sn_readback or not SN_PATTERN.fullmatch(sn_code):
            row_reasons.append(f"{port}_historical_sn_readback_mismatch")
        if not protocol_device_id:
            row_reasons.append(f"{port}_historical_protocol_device_id_missing")
        if not _text(source_row, "old_slot"):
            row_reasons.append(f"{port}_historical_ga_label_missing")

        proposed = {
            "ga_label": _text(source_row, "old_slot").upper(),
            "protocol_device_id": protocol_device_id,
            "sn_code": sn_code,
            "algorithm": "legacy_ratio",
            "check_capable": False,
            "check_required": False,
        }
        for field, value in proposed.items():
            current = target.get(field)
            if current not in (None, "") and current != value:
                row_reasons.append(f"{port}_{field}_conflicts_with_historical_identity")
        if row_reasons:
            reasons.extend(row_reasons)
            continue

        target.update(proposed)
        target["identity_evidence"] = {
            "scope": "historical_identity_prefill_only",
            "source_csv": str(identity_path),
            "source_csv_sha256": identity_sha256,
            "evidence_level": evidence_level,
            "binding_basis": _text(source_row, "binding_basis"),
            "owner_confirmation_date": _text(
                source_row,
                "owner_confirmation_date",
            ),
            "historical_old_device_id": old_device_id,
            "historical_runtime_config_json": str(runtime_path),
            "historical_runtime_config_sha256": runtime_sha256,
            "historical_runtime_reference": {
                "ftd_hz": runtime.get("ftd_hz"),
                "average1": runtime.get("average_co2"),
                "average2": runtime.get("average_h2o"),
                "filter": runtime.get("average_filter"),
            },
            "current_connection_state_inferred": False,
            "current_power_state_inferred": False,
            "current_operator_confirmation_inferred": False,
            "current_runtime_evidence_inferred": False,
        }
        applied_ports.append(port)

    profile["historical_identity_prefill"] = {
        "schema": "v1_5_historical_identity_prefill_v1",
        "generated_at": _now(),
        "status": (
            "operator_current_state_confirmation_required"
            if applied_ports and not reasons
            else "review_required"
        ),
        "applied_ports": applied_ports,
        "applied_count": len(applied_ports),
        "reasons": reasons,
        "historical_identity_csv": str(identity_path),
        "historical_identity_csv_sha256": identity_sha256,
        "historical_runtime_config_json": str(runtime_path),
        "historical_runtime_config_sha256": runtime_sha256,
        "current_connection_state_inferred": False,
        "current_power_state_inferred": False,
        "current_operator_confirmation_inferred": False,
        "current_runtime_evidence_inferred": False,
        "opens_com_ports": False,
        "sends_device_commands": False,
        "writes_sn": False,
        "writes_coefficients": False,
    }
    return profile


def _current_site_state_payload(site_profile: Mapping[str, Any]) -> dict[str, Any]:
    rows = {
        str(row.get("port") or "").upper(): row
        for row in _rows(site_profile, "candidate_analyzers")
    }
    payload = {
        "reported_connected_count": site_profile.get("reported_connected_count"),
        "reported_powered_count": site_profile.get("reported_powered_count"),
        "candidate_analyzers": [
            {
                "port": port,
                "connected": row.get("connected"),
                "powered": row.get("powered"),
                "operator_confirmed": row.get("operator_confirmed"),
                "ga_label": row.get("ga_label"),
                "protocol_device_id": row.get("protocol_device_id"),
                "sn_code": row.get("sn_code"),
                "algorithm": row.get("algorithm"),
                "algorithm_evidence": row.get("algorithm_evidence"),
                "check_capable": row.get("check_capable"),
                "check_required": row.get("check_required"),
                "runtime_evidence": row.get("runtime_evidence"),
            }
            for port in ANALYZER_BANK
            for row in [rows.get(port, {})]
        ],
    }
    if isinstance(site_profile.get("current_probe_evidence"), Mapping):
        payload["current_probe_evidence"] = site_profile["current_probe_evidence"]
    return payload


def _current_site_state_sha256(site_profile: Mapping[str, Any]) -> str:
    payload = json.dumps(
        _current_site_state_payload(site_profile),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _is_neutral_temperature_input(values: Any, *, atol: float = 1e-9) -> bool:
    if not isinstance(values, Sequence) or isinstance(
        values,
        (str, bytes, bytearray),
    ):
        return False
    if len(values) < len(NEUTRAL_TEMPERATURE_INPUT):
        return False
    try:
        return all(
            abs(float(value) - expected) <= atol
            for value, expected in zip(values, NEUTRAL_TEMPERATURE_INPUT)
        )
    except (TypeError, ValueError):
        return False


def _algorithm_evidence_reasons(
    *,
    row: Mapping[str, Any],
    port: str,
    protocol_id: str,
    sn_code: str,
    algorithm: str,
) -> list[str]:
    evidence = row.get("algorithm_evidence")
    if not isinstance(evidence, Mapping) or not evidence:
        return [f"{port}_algorithm_evidence_missing"]
    family = (
        "legacy_ratio"
        if algorithm in LEGACY_ALGORITHMS
        else "new_absorption"
    )
    required = (
        evidence.get("schema") == ALGORITHM_CLASSIFICATION_EVIDENCE_SCHEMA
        and _text(evidence, "source_type") in ALGORITHM_EVIDENCE_SOURCE_TYPES
        and bool(_text(evidence, "reference"))
        and _text(evidence, "algorithm_family") == family
        and evidence.get("classification_inferred") is False
    )
    if not required:
        return [f"{port}_algorithm_evidence_invalid"]
    if (
        _text(evidence, "bound_port").upper() != port
        or _text(evidence, "bound_protocol_device_id") != protocol_id
        or _text(evidence, "bound_sn_code") != sn_code
    ):
        return [f"{port}_algorithm_evidence_identity_mismatch"]
    source_path = _text(evidence, "source_file_path")
    source_sha256 = _text(evidence, "source_file_sha256")
    if bool(source_path) != bool(source_sha256):
        return [f"{port}_algorithm_evidence_file_binding_invalid"]
    if source_path and _sha256(source_path) != source_sha256:
        return [f"{port}_algorithm_evidence_file_hash_mismatch"]
    return []


def _runtime_setup_expected_commands() -> list[tuple[str, str]]:
    return [
        ("set_comm_way_inactive", "SETCOMWAY,YGAS,FFF,0"),
        ("set_mode2", "MODE,YGAS,FFF,2"),
        ("set_active_frequency", "FTD,YGAS,FFF,01"),
        ("set_average1_filter", "AVERAGE1,YGAS,FFF,49"),
        ("set_average2_filter", "AVERAGE2,YGAS,FFF,49"),
        ("set_comm_way_active", "SETCOMWAY,YGAS,FFF,1"),
    ]


def bind_v1_5_runtime_setup_result(
    *,
    site_profile: Mapping[str, Any],
    runtime_setup_result_json: str | Path,
) -> dict[str, Any]:
    """Bind a controlled runtime-setup result to matching powered identities."""

    result_path = Path(runtime_setup_result_json).resolve()
    payload = _load_json(result_path)
    if payload.get("schema_version") != RUNTIME_SETUP_RESULT_SCHEMA:
        raise ValueError("runtime setup result schema is invalid")
    plan = payload.get("plan")
    if not isinstance(plan, Mapping):
        raise ValueError("runtime setup result is missing its command plan")
    contract = plan.get("contract")
    if not isinstance(contract, Mapping):
        raise ValueError("runtime setup result is missing its runtime contract")
    result_rows = _rows(payload, "results")
    if not result_rows:
        raise ValueError("runtime setup result has no device rows")

    profile = copy.deepcopy(dict(site_profile))
    profile_rows = _rows(profile, "candidate_analyzers")
    powered_rows = [row for row in profile_rows if row.get("powered") is True]
    if not powered_rows:
        raise ValueError("site profile has no powered analyzer to bind")
    by_identity = {
        (
            _text(row, "port").upper(),
            _text(row, "protocol_device_id"),
            _text(row, "sn_code"),
        ): row
        for row in result_rows
    }
    result_sha256 = _sha256(result_path)
    bound_ports: list[str] = []
    for row in powered_rows:
        port = _text(row, "port").upper()
        protocol_id = _text(row, "protocol_device_id", "device_id")
        sn_code = _text(row, "sn_code", "device_code")
        result_row = by_identity.get((port, protocol_id, sn_code))
        if result_row is None:
            raise ValueError(
                f"{port}: runtime setup result does not match current port, protocol ID, and SN"
            )
        current = _runtime_evidence(row)
        row["runtime_evidence"] = {
            **dict(current),
            "schema": RUNTIME_SETUP_EVIDENCE_SCHEMA,
            "source_type": "controlled_runtime_setup_result",
            "runtime_setup_result_json": str(result_path),
            "runtime_setup_result_sha256": result_sha256,
            "runtime_setup_run_id": _text(payload, "run_id"),
            "bound_port": port,
            "bound_protocol_device_id": protocol_id,
            "bound_sn_code": sn_code,
            "ftd_hz": contract.get("ftd_hz"),
            "average1": contract.get("average1_target"),
            "average2": contract.get("average2_target"),
            "filter": contract.get("average1_target"),
            "result_status": _text(result_row, "status"),
            "values_derived_from_result": True,
            "operator_free_text_values_accepted": False,
        }
        bound_ports.append(port)

    profile["runtime_setup_evidence_binding"] = {
        "schema": RUNTIME_SETUP_EVIDENCE_SCHEMA,
        "status": "attached_for_validation",
        "runtime_setup_result_json": str(result_path),
        "runtime_setup_result_sha256": result_sha256,
        "runtime_setup_run_id": _text(payload, "run_id"),
        "bound_ports": bound_ports,
        "opens_com_ports": False,
        "sends_device_commands": False,
        "writes_device_settings": False,
    }
    confirmation = profile.get("current_site_confirmation")
    if isinstance(confirmation, dict) and confirmation.get("status") == "confirmed":
        confirmation["status"] = "stale_after_runtime_setup_evidence_attachment"
    return profile


def _runtime_setup_evidence_reasons(
    *,
    runtime: Mapping[str, Any],
    port: str,
    protocol_id: str,
    sn_code: str,
) -> list[str]:
    if runtime.get("schema") != RUNTIME_SETUP_EVIDENCE_SCHEMA:
        return [f"{port}_average1_average2_evidence_missing"]
    if (
        _text(runtime, "bound_port").upper() != port
        or _text(runtime, "bound_protocol_device_id") != protocol_id
        or _text(runtime, "bound_sn_code") != sn_code
    ):
        return [f"{port}_runtime_setup_evidence_identity_mismatch"]
    path = _text(runtime, "runtime_setup_result_json")
    declared_sha256 = _text(runtime, "runtime_setup_result_sha256")
    if not path or not declared_sha256:
        return [f"{port}_runtime_setup_evidence_file_binding_invalid"]
    if _sha256(path) != declared_sha256:
        return [f"{port}_runtime_setup_evidence_file_hash_mismatch"]

    payload = _load_json(path)
    plan = payload.get("plan")
    plan = plan if isinstance(plan, Mapping) else {}
    contract = plan.get("contract")
    contract = contract if isinstance(contract, Mapping) else {}
    boundary = payload.get("boundary")
    boundary = boundary if isinstance(boundary, Mapping) else {}
    reasons: list[str] = []
    if (
        payload.get("schema_version") != RUNTIME_SETUP_RESULT_SCHEMA
        or payload.get("status") != "ready"
        or payload.get("not_real_acceptance_evidence") is not True
        or not _text(payload, "run_id")
    ):
        reasons.append(f"{port}_runtime_setup_evidence_result_invalid")
    if (
        boundary.get("opens_com_ports") is not True
        or boundary.get("sends_device_commands") is not True
        or boundary.get("writes_runtime_settings") is not True
        or boundary.get("all_configuration_commands_require_ack") is not True
    ):
        reasons.append(f"{port}_runtime_setup_evidence_execution_boundary_invalid")
    for field in ("writes_senco", "writes_device_id", "writes_sn"):
        if boundary.get(field) is not False:
            reasons.append(f"{port}_runtime_setup_evidence_forbidden_write")
            break
    try:
        contract_ok = (
            int(contract.get("mode")) == 2
            and contract.get("active_send") is True
            and int(contract.get("ftd_hz")) == 1
            and int(contract.get("average1_target")) == 49
            and int(contract.get("average2_target")) == 49
            and float(contract.get("command_gap_s")) >= 1.0
        )
    except (TypeError, ValueError):
        contract_ok = False
    if not contract_ok:
        reasons.append(f"{port}_runtime_setup_evidence_contract_invalid")

    expected_commands = _runtime_setup_expected_commands()
    commands = _rows(plan, "commands")
    actual_commands = [
        (
            _text(command, "action"),
            _text(command, "command_preview"),
            command.get("ack_required"),
        )
        for command in commands
    ]
    if actual_commands != [
        (action, command, True) for action, command in expected_commands
    ]:
        reasons.append(f"{port}_runtime_setup_evidence_plan_invalid")

    matching = [
        row
        for row in _rows(payload, "results")
        if _text(row, "port").upper() == port
        and _text(row, "protocol_device_id") == protocol_id
        and _text(row, "sn_code") == sn_code
    ]
    if len(matching) != 1:
        reasons.append(f"{port}_runtime_setup_evidence_identity_mismatch")
        return reasons
    result_row = matching[0]
    identity_before = result_row.get("identity_before")
    identity_before = identity_before if isinstance(identity_before, Mapping) else {}
    identity_after = result_row.get("identity_after")
    identity_after = identity_after if isinstance(identity_after, Mapping) else {}
    if (
        result_row.get("status") != "ready"
        or _text(result_row, "sn_readback") != sn_code
        or _text(identity_before, "id") != protocol_id
        or _text(identity_after, "id") != protocol_id
    ):
        reasons.append(f"{port}_runtime_setup_evidence_identity_verification_invalid")
    events = _rows(result_row, "runtime_setup_events")
    actual_events = [
        (
            _text(event, "action"),
            _text(event, "command_preview"),
            event.get("ack_required"),
            event.get("ack_received"),
            event.get("ok"),
        )
        for event in events
    ]
    if actual_events != [
        (action, command, True, True, True)
        for action, command in expected_commands
    ]:
        reasons.append(f"{port}_runtime_setup_evidence_ack_sequence_invalid")
    if (
        _text(runtime, "source_type") != "controlled_runtime_setup_result"
        or runtime.get("values_derived_from_result") is not True
        or runtime.get("operator_free_text_values_accepted") is not False
        or _text(runtime, "runtime_setup_run_id") != _text(payload, "run_id")
        or runtime.get("ftd_hz") != contract.get("ftd_hz")
        or runtime.get("average1") != contract.get("average1_target")
        or runtime.get("average2") != contract.get("average2_target")
    ):
        reasons.append(f"{port}_runtime_setup_evidence_binding_invalid")
    return reasons


def _validate_initialization_probe(
    *,
    evidence: Mapping[str, Any],
    by_port: Mapping[str, Mapping[str, Any]],
    streaming_ports: Sequence[str],
) -> dict[str, Any]:
    path = _text(evidence, "initialization_probe_json")
    declared_sha256 = _text(evidence, "initialization_probe_sha256")
    if not path and not declared_sha256:
        return {
            "present": False,
            "status": "not_supplied",
            "reasons": [],
            "path": "",
            "sha256": "",
            "opens_com_ports": False,
            "sends_read_only_commands": False,
            "sends_write_commands": False,
        }

    reasons: list[str] = []
    payload = _load_json(path)
    actual_sha256 = _sha256(path)
    if not payload:
        reasons.append("current_probe_initialization_probe_missing")
    if not declared_sha256:
        reasons.append("current_probe_initialization_probe_sha256_missing")
    elif declared_sha256 != actual_sha256:
        reasons.append("current_probe_initialization_probe_sha256_mismatch")
    if payload:
        if payload.get("schema") != CURRENT_INITIALIZATION_PROBE_SCHEMA:
            reasons.append("current_probe_initialization_probe_schema_invalid")
        if (
            payload.get("overall_status")
            != "effective_1hz_and_senco78_already_neutral"
        ):
            reasons.append("current_probe_initialization_probe_status_invalid")
        if payload.get("engineering_probe_only") is not True:
            reasons.append(
                "current_probe_initialization_engineering_only_marker_missing"
            )
        if payload.get("promotion_state") != "blocked":
            reasons.append(
                "current_probe_initialization_promotion_state_must_be_blocked"
            )
        if payload.get("not_real_acceptance_evidence") is not True:
            reasons.append(
                "current_probe_initialization_not_real_acceptance_marker_missing"
            )
        if payload.get("sends_read_only_commands") is not True:
            reasons.append(
                "current_probe_initialization_read_only_command_marker_missing"
            )
        if payload.get("sends_write_commands") is not False:
            reasons.append("current_probe_initialization_sent_write_commands")
        if payload.get("sets_comm_way") is not False:
            reasons.append("current_probe_initialization_set_comm_way")
        for field in (
            "writes_sn",
            "writes_device_id",
            "writes_coefficients",
            "connects_postgresql",
            "controls_pressure",
            "controls_temperature",
            "controls_water_or_gas_routes",
            "database_written",
        ):
            if payload.get(field) is not False:
                reasons.append(
                    f"current_probe_initialization_{field}_must_be_false"
                )
        query_whitelist = tuple(payload.get("query_command_whitelist") or ())
        if len(query_whitelist) != len(INITIALIZATION_QUERY_WHITELIST) or set(
            query_whitelist
        ) != set(INITIALIZATION_QUERY_WHITELIST):
            reasons.append(
                "current_probe_initialization_query_whitelist_invalid"
            )
        if int(payload.get("query_command_count") or 0) != (
            len(streaming_ports) * len(INITIALIZATION_QUERY_WHITELIST)
        ):
            reasons.append("current_probe_initialization_query_count_invalid")
        try:
            command_gap_ok = (
                float(payload.get("minimum_inter_command_gap_s")) >= 1.0
            )
        except (TypeError, ValueError):
            command_gap_ok = False
        if not command_gap_ok:
            reasons.append(
                "current_probe_initialization_command_gap_below_1s"
            )

        sources = payload.get("source_artifacts")
        sources = sources if isinstance(sources, Mapping) else {}
        source_pairs = (
            ("cadence_json", "cadence_json_sha256"),
            ("identity_json", "identity_json_sha256"),
            ("getco_snapshot_json", "getco_snapshot_json_sha256"),
            ("getco_rows_csv", "getco_rows_csv_sha256"),
            ("getco_identity_csv", "getco_identity_csv_sha256"),
            ("getco_conclusion_csv", "getco_conclusion_csv_sha256"),
            ("getco_meta_json", "getco_meta_json_sha256"),
            ("getco_probe_source_py", "getco_probe_source_py_sha256"),
        )
        for source_key, hash_key in source_pairs:
            source_path = _text(sources, source_key)
            source_hash = _text(sources, hash_key)
            if not source_path or not source_hash:
                reasons.append(
                    f"current_probe_initialization_source_{source_key}_missing"
                )
            elif _sha256(source_path) != source_hash:
                reasons.append(
                    f"current_probe_initialization_source_{source_key}_sha256_mismatch"
                )

        raw_result_rows = _rows(payload, "results")
        result_rows = {
            str(row.get("port") or "").upper(): row
            for row in raw_result_rows
        }
        if len(raw_result_rows) != len(streaming_ports) or set(
            result_rows
        ) != set(streaming_ports):
            reasons.append(
                "current_probe_initialization_result_ports_mismatch"
            )
        for port in streaming_ports:
            result = result_rows.get(port, {})
            profile_row = by_port.get(port, {})
            if _text(result, "protocol_device_id") != _text(
                profile_row,
                "protocol_device_id",
            ):
                reasons.append(
                    f"current_probe_initialization_{port}_protocol_identity_mismatch"
                )
            if _text(result, "sn_code") != _text(profile_row, "sn_code"):
                reasons.append(
                    f"current_probe_initialization_{port}_sn_mismatch"
                )
            if result.get("effective_ftd_hz") != 1:
                reasons.append(
                    f"current_probe_initialization_{port}_effective_1hz_missing"
                )
            runtime = _runtime_evidence(profile_row)
            try:
                profile_ftd_ok = abs(float(runtime.get("ftd_hz")) - 1.0) < 1e-9
            except (TypeError, ValueError):
                profile_ftd_ok = False
            if not profile_ftd_ok:
                reasons.append(
                    f"current_probe_initialization_{port}_profile_1hz_not_bound"
                )
            if not _is_neutral_temperature_input(result.get("GETCO7")):
                reasons.append(
                    f"current_probe_initialization_{port}_senco7_not_neutral"
                )
            if not _is_neutral_temperature_input(result.get("GETCO8")):
                reasons.append(
                    f"current_probe_initialization_{port}_senco8_not_neutral"
                )
            if result.get("senco7_write_required") is not False:
                reasons.append(
                    f"current_probe_initialization_{port}_senco7_write_not_skipped"
                )
            if result.get("senco8_write_required") is not False:
                reasons.append(
                    f"current_probe_initialization_{port}_senco8_write_not_skipped"
                )
            if (
                result.get("initialization_action")
                != "already_neutral_readback_only_skip_senco78_write"
            ):
                reasons.append(
                    f"current_probe_initialization_{port}_action_invalid"
                )
            if result.get("status") != "pass":
                reasons.append(
                    f"current_probe_initialization_{port}_status_not_pass"
                )

    return {
        "present": True,
        "status": "valid_no_write_initialization_probe"
        if not reasons
        else "review_required",
        "reasons": reasons,
        "path": path,
        "sha256": actual_sha256,
        "opens_com_ports": payload.get("opens_com_ports") is True,
        "sends_read_only_commands": (
            payload.get("sends_read_only_commands") is True
        ),
        "sends_write_commands": payload.get("sends_write_commands") is True,
    }


def _validate_runtime_setting_readability_review(
    *,
    evidence: Mapping[str, Any],
    by_port: Mapping[str, Mapping[str, Any]],
    streaming_ports: Sequence[str],
) -> dict[str, Any]:
    path = _text(evidence, "runtime_setting_readability_review_json")
    declared_sha256 = _text(
        evidence,
        "runtime_setting_readability_review_sha256",
    )
    if not path and not declared_sha256:
        return {
            "present": False,
            "status": "not_supplied",
            "reasons": [],
            "path": "",
            "sha256": "",
        }

    payload = _load_json(path)
    actual_sha256 = _sha256(path)
    prefix = "current_probe_runtime_readability"
    reasons: list[str] = []
    if not payload:
        reasons.append(f"{prefix}_review_missing")
    if declared_sha256 != actual_sha256:
        reasons.append(f"{prefix}_review_sha256_mismatch")
    if payload:
        false_fields = (
            "real_com_execution_attempted",
            "opens_com_ports",
            "sends_device_commands",
            "writes_sn",
            "writes_device_id",
            "writes_coefficients",
            "controls_pressure",
            "controls_temperature",
            "controls_water_or_gas_routes",
            "connects_postgresql",
            "database_written",
        )
        contract_ok = (
            payload.get("schema")
            == RUNTIME_SETTING_READABILITY_REVIEW_SCHEMA
            and payload.get("overall_status")
            == "safe_hold_no_supported_read_command"
            and payload.get("engineering_review_only") is True
            and payload.get("promotion_state") == "blocked"
            and payload.get("not_real_acceptance_evidence") is True
            and payload.get("command_attempt_count") == 0
            and payload.get("bytes_written") == 0
            and all(payload.get(field) is False for field in false_fields)
        )
        if not contract_ok:
            reasons.append(f"{prefix}_safe_hold_contract_invalid")

        capabilities = payload.get("capabilities")
        capabilities = (
            capabilities if isinstance(capabilities, Mapping) else {}
        )
        capability_contract_ok = all(
            isinstance(capabilities.get(key), Mapping)
            and capabilities[key].get("directly_readable") is False
            and not _text(capabilities[key], "supported_read_command")
            for key in ("average1", "average2", "filter", "algorithm")
        )
        if not capability_contract_ok:
            reasons.append(f"{prefix}_capability_contract_invalid")

        expected_identities = {
            (
                port,
                _text(by_port.get(port, {}), "protocol_device_id"),
                _text(by_port.get(port, {}), "sn_code"),
            )
            for port in streaming_ports
        }
        actual_identities = {
            (
                str(row.get("port") or "").upper(),
                _text(row, "protocol_device_id"),
                _text(row, "sn_code"),
            )
            for row in _rows(payload, "results")
            if row.get("status") == "safe_hold"
            and row.get("historical_port_values_reused") is False
            and row.get("inferred_from_coefficient_shape") is False
        }
        if (
            actual_identities != expected_identities
            or len(_rows(payload, "results")) != len(expected_identities)
        ):
            reasons.append(f"{prefix}_safe_hold_identity_set_mismatch")

    return {
        "present": True,
        "status": "valid_safe_hold_review" if not reasons else "review_required",
        "reasons": reasons,
        "path": path,
        "sha256": actual_sha256,
    }


def confirm_v1_5_current_site_state(
    *,
    site_profile: Mapping[str, Any],
    operator_name: str,
    observation_basis: str,
    confirmed_at: str | None = None,
) -> dict[str, Any]:
    """Bind an operator confirmation to the exact current 4/2 site mapping."""

    operator = str(operator_name or "").strip()
    basis = str(observation_basis or "").strip()
    if not operator:
        raise ValueError("operator name is required")
    if not basis:
        raise ValueError("current-site observation basis is required")
    profile = copy.deepcopy(dict(site_profile))
    rows = _rows(profile, "candidate_analyzers")
    by_port = {str(row.get("port") or "").upper(): row for row in rows}
    if len(rows) != len(ANALYZER_BANK) or set(by_port) != set(ANALYZER_BANK):
        raise ValueError("candidate analyzer bank must be exactly COM35 through COM42")
    connected_ports = [
        port for port in ANALYZER_BANK if by_port[port].get("connected") is True
    ]
    powered_ports = [
        port for port in ANALYZER_BANK if by_port[port].get("powered") is True
    ]
    try:
        expected_connected = int(profile.get("reported_connected_count"))
        expected_powered = int(profile.get("reported_powered_count"))
    except (TypeError, ValueError) as exc:
        raise ValueError("reported connected/powered counts are invalid") from exc
    if len(connected_ports) != expected_connected:
        raise ValueError(
            f"connected ports must match reported count {expected_connected}"
        )
    if len(powered_ports) != expected_powered:
        raise ValueError(f"powered ports must match reported count {expected_powered}")
    if not set(powered_ports).issubset(connected_ports):
        raise ValueError("powered ports must be a subset of connected ports")
    unconfirmed = [
        port
        for port in connected_ports
        if by_port[port].get("operator_confirmed") is not True
    ]
    if unconfirmed:
        raise ValueError(
            "connected ports require row confirmation: " + ", ".join(unconfirmed)
        )

    profile["current_site_confirmation"] = {
        "schema": "v1_5_current_site_confirmation_v1",
        "status": "confirmed",
        "operator_name": operator,
        "confirmed_at": str(confirmed_at or _now()),
        "observation_basis": basis,
        "connected_ports": connected_ports,
        "powered_ports": powered_ports,
        "reported_connected_count": expected_connected,
        "reported_powered_count": expected_powered,
        "candidate_state_sha256": _current_site_state_sha256(profile),
        "opens_com_ports": False,
        "sends_device_commands": False,
        "writes_sn": False,
        "writes_coefficients": False,
    }
    return profile


def _validate_current_probe_evidence(
    site_profile: Mapping[str, Any],
    by_port: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    evidence = site_profile.get("current_probe_evidence")
    if not isinstance(evidence, Mapping):
        return {
            "present": False,
            "status": "not_supplied",
            "reasons": [],
            "opens_com_ports": False,
            "sends_read_only_device_commands": False,
            "sends_write_commands": False,
            "engineering_probe_only": False,
        }

    reasons: list[str] = []
    if evidence.get("schema") != CURRENT_PROBE_BINDING_SCHEMA:
        reasons.append("current_probe_binding_schema_invalid")
    if evidence.get("engineering_probe_only") is not True:
        reasons.append("current_probe_engineering_only_marker_missing")
    if evidence.get("promotion_state") != "blocked":
        reasons.append("current_probe_promotion_state_must_be_blocked")
    if evidence.get("not_real_acceptance_evidence") is not True:
        reasons.append("current_probe_not_real_acceptance_marker_missing")
    for field in ("writes_sn", "writes_device_id", "writes_coefficients"):
        if evidence.get(field) is not False:
            reasons.append(f"current_probe_binding_{field}_must_be_false")

    passive_path = _text(evidence, "passive_inventory_json")
    identity_path = _text(evidence, "identity_query_json")
    passive = _load_json(passive_path)
    identity = _load_json(identity_path)
    if not passive:
        reasons.append("current_probe_passive_inventory_missing")
    elif evidence.get("passive_inventory_sha256") != _sha256(passive_path):
        reasons.append("current_probe_passive_inventory_sha256_mismatch")
    if not identity:
        reasons.append("current_probe_identity_query_missing")
    elif evidence.get("identity_query_sha256") != _sha256(identity_path):
        reasons.append("current_probe_identity_query_sha256_mismatch")

    if passive:
        if passive.get("schema") != PASSIVE_PROBE_SCHEMA:
            reasons.append("current_probe_passive_inventory_schema_invalid")
        if passive.get("engineering_probe_only") is not True:
            reasons.append("current_probe_passive_engineering_only_marker_missing")
        if passive.get("not_real_acceptance_evidence") is not True:
            reasons.append("current_probe_passive_not_real_acceptance_marker_missing")
        if passive.get("bytes_written") != 0:
            reasons.append("current_probe_passive_bytes_written_nonzero")
        if passive.get("sends_device_commands") is not False:
            reasons.append("current_probe_passive_sent_device_commands")
        for field in ("writes_sn", "writes_device_id", "writes_coefficients"):
            if passive.get(field) is not False:
                reasons.append(f"current_probe_passive_{field}_must_be_false")

    if identity:
        if identity.get("schema") != POWERED_IDENTITY_QUERY_SCHEMA:
            reasons.append("current_probe_identity_query_schema_invalid")
        if identity.get("engineering_probe_only") is not True:
            reasons.append("current_probe_identity_engineering_only_marker_missing")
        if identity.get("not_real_acceptance_evidence") is not True:
            reasons.append("current_probe_identity_not_real_acceptance_marker_missing")
        if identity.get("sends_write_commands") is not False:
            reasons.append("current_probe_identity_sent_write_commands")
        for field in ("writes_sn", "writes_device_id", "writes_coefficients"):
            if identity.get(field) is not False:
                reasons.append(f"current_probe_identity_{field}_must_be_false")

    streaming_ports = [
        str(port).upper()
        for port in evidence.get("streaming_powered_ports") or []
    ]
    passive_ports = [
        str(port).upper()
        for port in passive.get("streaming_powered_ports") or []
    ]
    if streaming_ports != passive_ports:
        reasons.append("current_probe_streaming_ports_mismatch")
    passive_rows = {
        str(row.get("port") or "").upper(): row
        for row in _rows(passive, "port_results")
    }
    identity_rows = {
        str(row.get("port") or "").upper(): row
        for row in _rows(identity, "results")
    }
    for port in streaming_ports:
        profile_row = by_port.get(port, {})
        observed_ids = [
            str(value).strip()
            for value in passive_rows.get(port, {}).get("observed_device_ids") or []
            if str(value).strip()
        ]
        observed_sn = _text(identity_rows.get(port, {}), "sn_code_read")
        if len(observed_ids) != 1:
            reasons.append(f"current_probe_{port}_protocol_identity_not_unique")
        elif _text(profile_row, "protocol_device_id") != observed_ids[0]:
            reasons.append(f"current_probe_{port}_protocol_identity_mismatch")
        if not SN_PATTERN.match(observed_sn):
            reasons.append(f"current_probe_{port}_sn_missing")
        elif _text(profile_row, "sn_code") != observed_sn:
            reasons.append(f"current_probe_{port}_sn_mismatch")
        if profile_row.get("connected") is not True:
            reasons.append(f"current_probe_{port}_must_be_connected")
        if profile_row.get("powered") is not True:
            reasons.append(f"current_probe_{port}_must_be_powered")

    initialization_probe = _validate_initialization_probe(
        evidence=evidence,
        by_port=by_port,
        streaming_ports=streaming_ports,
    )
    reasons.extend(initialization_probe["reasons"])
    runtime_readability_review = _validate_runtime_setting_readability_review(
        evidence=evidence,
        by_port=by_port,
        streaming_ports=streaming_ports,
    )
    reasons.extend(runtime_readability_review["reasons"])
    opens_com_ports = any(
        row.get("open_succeeded") is True
        for row in _rows(passive, "port_results") + _rows(identity, "results")
    ) or initialization_probe["opens_com_ports"]
    sends_write_commands = (
        identity.get("sends_write_commands") is True
        or initialization_probe["sends_write_commands"]
    )
    sends_read_only_commands = bool(
        (
            identity.get("sends_device_commands") is True
            and int(identity.get("command_attempt_count") or 0) > 0
        )
        or initialization_probe["sends_read_only_commands"]
    ) and not sends_write_commands
    return {
        "present": True,
        "status": "valid_engineering_probe_binding" if not reasons else "review_required",
        "reasons": reasons,
        "passive_inventory_json": passive_path,
        "passive_inventory_sha256": _sha256(passive_path),
        "identity_query_json": identity_path,
        "identity_query_sha256": _sha256(identity_path),
        "initialization_probe": initialization_probe,
        "initialization_probe_json": initialization_probe["path"],
        "initialization_probe_sha256": initialization_probe["sha256"],
        "runtime_setting_readability_review": runtime_readability_review,
        "runtime_setting_readability_review_json": runtime_readability_review[
            "path"
        ],
        "runtime_setting_readability_review_sha256": runtime_readability_review[
            "sha256"
        ],
        "streaming_powered_ports": streaming_ports,
        "opens_com_ports": opens_com_ports,
        "sends_read_only_device_commands": sends_read_only_commands,
        "sends_write_commands": sends_write_commands,
        "engineering_probe_only": evidence.get("engineering_probe_only") is True,
        "not_real_acceptance_evidence": (
            evidence.get("not_real_acceptance_evidence") is True
        ),
    }


def validate_v1_5_real_acceptance_site_profile(
    *,
    site_profile: Mapping[str, Any],
    runtime_port_inventory_json: str | Path,
) -> dict[str, Any]:
    """Validate the operator mapping and derive existing read-only packet inputs."""

    inventory_path = Path(runtime_port_inventory_json).resolve()
    inventory = _load_json(inventory_path)
    visible = {
        str(row.get("port") or "").upper()
        for row in _rows(inventory, "ports")
        if str(row.get("port") or "").strip()
    }
    reasons: list[str] = []
    if site_profile.get("schema") != SITE_PROFILE_SCHEMA:
        reasons.append(f"site_profile_schema={site_profile.get('schema') or 'missing'}")
    if site_profile.get("runtime_port_inventory_sha256") != _sha256(inventory_path):
        reasons.append("runtime_port_inventory_sha256_mismatch")

    rows = _rows(site_profile, "candidate_analyzers")
    by_port = {str(row.get("port") or "").upper(): row for row in rows}
    if len(rows) != len(ANALYZER_BANK) or set(by_port) != set(ANALYZER_BANK):
        reasons.append("candidate_analyzer_bank_must_be_exactly_com35_to_com42")
    current_probe = _validate_current_probe_evidence(site_profile, by_port)
    reasons.extend(current_probe["reasons"])
    try:
        reported_connected = int(site_profile.get("reported_connected_count"))
        reported_powered = int(site_profile.get("reported_powered_count"))
    except (TypeError, ValueError):
        reported_connected = -1
        reported_powered = -1
        reasons.append("reported_counts_invalid")

    connected_rows = [row for row in rows if row.get("connected") is True]
    powered_rows = [row for row in rows if row.get("powered") is True]
    if len(connected_rows) != reported_connected:
        reasons.append(f"connected_count_expected_{reported_connected}_actual_{len(connected_rows)}")
    if len(powered_rows) != reported_powered:
        reasons.append(f"powered_count_expected_{reported_powered}_actual_{len(powered_rows)}")
    if not (1 <= len(powered_rows) <= 6):
        reasons.append(f"active_powered_analyzer_count={len(powered_rows)}")

    confirmation = site_profile.get("current_site_confirmation")
    confirmation = confirmation if isinstance(confirmation, Mapping) else {}
    if confirmation.get("schema") != "v1_5_current_site_confirmation_v1":
        reasons.append("current_site_confirmation_missing")
    elif confirmation.get("status") != "confirmed":
        reasons.append("current_site_confirmation_not_confirmed")
    else:
        if not _text(confirmation, "operator_name"):
            reasons.append("current_site_confirmation_operator_missing")
        if not _text(confirmation, "confirmed_at"):
            reasons.append("current_site_confirmation_time_missing")
        if not _text(confirmation, "observation_basis"):
            reasons.append("current_site_confirmation_basis_missing")
        connected_port_set = {
            str(row.get("port") or "").upper() for row in connected_rows
        }
        powered_port_set = {
            str(row.get("port") or "").upper() for row in powered_rows
        }
        connected_ports = [
            port for port in ANALYZER_BANK if port in connected_port_set
        ]
        powered_ports = [
            port for port in ANALYZER_BANK if port in powered_port_set
        ]
        if list(confirmation.get("connected_ports") or []) != connected_ports:
            reasons.append("current_site_confirmation_connected_ports_mismatch")
        if list(confirmation.get("powered_ports") or []) != powered_ports:
            reasons.append("current_site_confirmation_powered_ports_mismatch")
        if confirmation.get("reported_connected_count") != reported_connected:
            reasons.append("current_site_confirmation_connected_count_mismatch")
        if confirmation.get("reported_powered_count") != reported_powered:
            reasons.append("current_site_confirmation_powered_count_mismatch")
        if confirmation.get("candidate_state_sha256") != _current_site_state_sha256(
            site_profile
        ):
            reasons.append("current_site_confirmation_state_sha256_mismatch")

    labels: set[str] = set()
    sns: set[str] = set()
    reviewed_ports: list[dict[str, Any]] = []
    active_analyzers: list[dict[str, Any]] = []
    for index, row in enumerate(rows, start=1):
        port = str(row.get("port") or "").upper()
        connected = row.get("connected") is True
        powered = row.get("powered") is True
        confirmed = row.get("operator_confirmed") is True
        label = _text(row, "ga_label", "label")
        if powered and not connected:
            reasons.append(f"{port}_powered_without_connected")
        if connected and not confirmed:
            reasons.append(f"{port}_connected_not_operator_confirmed")
        if connected and port not in visible:
            reasons.append(f"{port}_connected_but_not_os_visible")
        if connected and not label:
            reasons.append(f"{port}_connected_ga_label_missing")
        if label:
            if label in labels:
                reasons.append(f"duplicate_ga_label={label}")
            labels.add(label)
        if not connected:
            continue
        reviewed_ports.append(
            {
                "ga_label": label,
                "port": port,
                "operator_confirmed": confirmed,
                "powered": powered,
                "source_row": index,
            }
        )
        if not powered:
            continue
        protocol_id = _text(row, "protocol_device_id", "device_id")
        sn_code = _text(row, "sn_code", "device_code")
        algorithm = _text(row, "algorithm", "algorithm_profile").lower()
        check_capable = row.get("check_capable")
        check_required = row.get("check_required")
        runtime = _runtime_evidence(row)
        if not protocol_id:
            reasons.append(f"{port}_protocol_device_id_missing")
        if not SN_PATTERN.match(sn_code):
            reasons.append(f"{port}_sn_code_invalid")
        elif sn_code in sns:
            reasons.append(f"duplicate_sn_code={sn_code}")
        sns.add(sn_code)
        if algorithm not in LEGACY_ALGORITHMS | NEW_ALGORITHMS:
            reasons.append(f"{port}_algorithm_invalid")
        else:
            reasons.extend(
                _algorithm_evidence_reasons(
                    row=row,
                    port=port,
                    protocol_id=protocol_id,
                    sn_code=sn_code,
                    algorithm=algorithm,
                )
            )
            if algorithm in LEGACY_ALGORITHMS and (
                check_capable is not False or check_required is not False
            ):
                reasons.append(f"{port}_legacy_check_must_be_false")
            elif algorithm in NEW_ALGORITHMS and (
                check_capable is not True or check_required is not True
            ):
                reasons.append(f"{port}_new_algorithm_check_must_be_true")
        try:
            ftd_ok = abs(float(runtime.get("ftd_hz")) - 1.0) < 1e-9
        except (TypeError, ValueError):
            ftd_ok = False
        if not ftd_ok:
            reasons.append(f"{port}_runtime_1hz_evidence_missing")
        reasons.extend(
            _runtime_setup_evidence_reasons(
                runtime=runtime,
                port=port,
                protocol_id=protocol_id,
                sn_code=sn_code,
            )
        )
        active_analyzers.append(
            {
                "ga_label": label,
                "port": port,
                "protocol_device_id": protocol_id,
                "sn_code": sn_code,
                "algorithm": algorithm,
                "algorithm_evidence": dict(
                    row.get("algorithm_evidence")
                    if isinstance(row.get("algorithm_evidence"), Mapping)
                    else {}
                ),
                "check_capable": check_capable,
                "check_required": check_required,
                "runtime_evidence": dict(runtime),
            }
        )

    return {
        "status": "ready_for_readonly_packet_build" if not reasons else "review_required",
        "ready_for_readonly_packet_build": not reasons,
        "reasons": reasons,
        "reported_connected_count": reported_connected,
        "reported_powered_count": reported_powered,
        "mapped_connected_count": len(connected_rows),
        "mapped_powered_count": len(powered_rows),
        "current_probe_evidence_validation": current_probe,
        "reviewed_port_inventory": {
            "schema": "v1_5_readonly_com_reviewed_port_inventory_v1",
            "reviewed_ports": reviewed_ports,
        },
        "active_analyzer_list": {
            "schema": "v1_5_readonly_com_active_analyzer_list_v1",
            "active_analyzers": active_analyzers,
        },
    }


def _artifact(role: str, path: str | Path | None) -> dict[str, Any]:
    source = Path(path).resolve() if path else None
    return {
        "role": role,
        "path": str(source) if source else "",
        "present": bool(source and source.is_file()),
        "sha256": _sha256(source),
        "size_bytes": source.stat().st_size if source and source.is_file() else 0,
    }


def _gate(gate: str, reasons: Sequence[str], meaning: str) -> dict[str, Any]:
    return {
        "gate": gate,
        "status": "pass" if not reasons else "blocked",
        "reasons": list(reasons),
        "physical_meaning": meaning,
    }


def build_v1_5_real_acceptance_control_pack(
    *,
    runtime_port_inventory_json: str | Path,
    certificate_registry_json: str | Path,
    certificate_reconciliation_json: str | Path,
    certificate_admission_json: str | Path,
    workstation_dry_run_json: str | Path,
    site_profile: Mapping[str, Any],
    readonly_com_executor_json: str | Path | None = None,
    formal_archive_closure_json: str | Path | None = None,
) -> dict[str, Any]:
    """Bind preflight and post-run lifecycle evidence without promoting it."""

    site = validate_v1_5_real_acceptance_site_profile(
        site_profile=site_profile,
        runtime_port_inventory_json=runtime_port_inventory_json,
    )
    registry = _load_json(certificate_registry_json)
    reconciliation = _load_json(certificate_reconciliation_json)
    admission = _load_json(certificate_admission_json)
    workstation = _load_json(workstation_dry_run_json)
    readonly = _load_json(readonly_com_executor_json)
    archive = _load_json(formal_archive_closure_json)

    registry_reasons: list[str] = []
    if not _rows(registry, "records"):
        registry_reasons.append("certificate_registry_records_missing")
    boundary = registry.get("boundary") if isinstance(registry.get("boundary"), Mapping) else {}
    for field in ("calibration_input_connected", "device_io_allowed", "coefficient_write_allowed"):
        if boundary.get(field) is not False:
            registry_reasons.append(f"certificate_registry_boundary_{field}={boundary.get(field)!r}")

    reconciliation_reasons: list[str] = []
    if int(reconciliation.get("mismatch_count") or 0):
        reconciliation_reasons.append(
            f"certificate_value_mismatch_count={reconciliation.get('mismatch_count')}"
        )
    if reconciliation.get("automatic_value_binding_allowed") is not True:
        reconciliation_reasons.append("automatic_certificate_value_binding_not_allowed")

    admission_reasons: list[str] = []
    if admission.get("ready_for_real_execution") is not True:
        admission_reasons.append("operational_certificate_gate_not_ready")
    if admission.get("strict_original_certificate_gate_passed") is not True:
        admission_reasons.append("strict_original_certificate_gate_not_passed")

    workstation_reasons: list[str] = []
    if workstation.get("overall_status") != "pass":
        workstation_reasons.append(f"workstation_status={workstation.get('overall_status') or 'missing'}")
    if workstation.get("point_counts") != {"co2": 45, "h2o": 13}:
        workstation_reasons.append(f"mature_point_counts={workstation.get('point_counts')!r}")
    for field in ("opens_com_ports", "writes_coefficients", "writes_device_id", "controls_water_or_gas_routes"):
        if workstation.get(field) is not False:
            workstation_reasons.append(f"workstation_boundary_{field}={workstation.get(field)!r}")

    readonly_reasons: list[str] = []
    if not readonly:
        readonly_reasons.append("readonly_com_executor_evidence_not_supplied")
    else:
        if readonly.get("schema") != READONLY_EXECUTOR_SCHEMA:
            readonly_reasons.append(f"readonly_executor_schema={readonly.get('schema') or 'missing'}")
        if readonly.get("overall_status") != "readonly_com_minimal_executor_completed_no_write":
            readonly_reasons.append(f"readonly_executor_status={readonly.get('overall_status') or 'missing'}")
        if readonly.get("execution_attempted") is not True:
            readonly_reasons.append("readonly_executor_not_attempted")
        for field in (
            "writes_sn",
            "writes_device_id",
            "writes_coefficients",
            "connects_postgresql",
            "controls_pressure",
            "controls_water_or_gas_routes",
            "formal_release_allowed",
            "database_import_allowed",
        ):
            if readonly.get(field) is not False:
                readonly_reasons.append(f"readonly_executor_boundary_{field}={readonly.get(field)!r}")
        if readonly.get("not_real_acceptance_evidence") is not True:
            readonly_reasons.append("readonly_executor_must_not_be_real_acceptance_evidence")

    archive_reasons: list[str] = []
    if not archive:
        archive_reasons.append("formal_archive_closure_not_supplied")
    else:
        formal_status = archive.get("formal_run_status")
        formal_status = formal_status if isinstance(formal_status, Mapping) else {}
        if formal_status.get("formal_release_allowed") is not True:
            archive_reasons.append("formal_archive_not_ready_for_human_release_review")

    gates = [
        _gate("site_profile", site["reasons"], "Only mapped, powered, operator-confirmed analyzers may enter the read-only packet."),
        _gate("certificate_registry", registry_reasons, "Certificate records stay traceable and disconnected from automatic fitting/writes."),
        _gate("certificate_value_reconciliation", reconciliation_reasons, "Each physical cylinder must match its current certificate value before use."),
        _gate("operational_certificate_admission", admission_reasons, "Original reference certificates must pass the formal execution gate."),
        _gate("mature_workstation_dry_run", workstation_reasons, "The 0613/0620/0621 mature CO2/H2O queues must remain intact."),
        _gate("readonly_initialization_evidence", readonly_reasons, "Read-only identity/GETCO evidence is initialization evidence, not acceptance."),
        _gate("formal_archive_closure", archive_reasons, "Post-run evidence must be archived before human acceptance review."),
    ]
    readonly_preflight_blockers = [
        reason
        for gate in (gates[0], gates[4])
        for reason in gate["reasons"]
    ]
    calibration_preflight_blockers = [
        reason
        for gate in gates[:5]
        for reason in gate["reasons"]
    ]
    if readonly_preflight_blockers:
        lifecycle_status = "blocked_before_readonly_initialization"
    elif readonly_reasons:
        lifecycle_status = "preflight_ready_for_explicit_readonly_authorization"
    elif calibration_preflight_blockers:
        lifecycle_status = "readonly_complete_calibration_preflight_blocked"
    elif archive_reasons:
        lifecycle_status = "readonly_complete_calibration_and_archive_pending"
    else:
        lifecycle_status = "ready_for_human_acceptance_review"

    artifacts = [
        _artifact("runtime_port_inventory", runtime_port_inventory_json),
        _artifact("certificate_registry", certificate_registry_json),
        _artifact("certificate_value_reconciliation", certificate_reconciliation_json),
        _artifact("certificate_operational_admission", certificate_admission_json),
        _artifact("mature_workstation_dry_run", workstation_dry_run_json),
        _artifact("readonly_com_executor", readonly_com_executor_json),
        _artifact("formal_archive_closure", formal_archive_closure_json),
    ]
    current_probe = site.get("current_probe_evidence_validation")
    current_probe = current_probe if isinstance(current_probe, Mapping) else {}
    if current_probe.get("present") is True:
        artifacts.extend(
            [
                _artifact(
                    "current_site_passive_inventory_probe",
                    current_probe.get("passive_inventory_json"),
                ),
                _artifact(
                    "current_site_powered_identity_query",
                    current_probe.get("identity_query_json"),
                ),
            ]
        )
        initialization_probe = current_probe.get("initialization_probe")
        initialization_probe = (
            initialization_probe
            if isinstance(initialization_probe, Mapping)
            else {}
        )
        if initialization_probe.get("present") is True:
            artifacts.append(
                _artifact(
                    "current_powered_initialization_probe",
                    initialization_probe.get("path"),
                )
            )
        runtime_readability_review = current_probe.get(
            "runtime_setting_readability_review"
        )
        runtime_readability_review = (
            runtime_readability_review
            if isinstance(runtime_readability_review, Mapping)
            else {}
        )
        if runtime_readability_review.get("present") is True:
            artifacts.append(
                _artifact(
                    "current_runtime_setting_readability_review",
                    runtime_readability_review.get("path"),
                )
            )
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "lifecycle_status": lifecycle_status,
        "preflight_ready_for_explicit_readonly_authorization": not readonly_preflight_blockers,
        "calibration_preflight_ready": not calibration_preflight_blockers,
        "certificate_gates_do_not_block_offline_or_readonly_program_progress": True,
        "ready_for_human_acceptance_review": lifecycle_status == "ready_for_human_acceptance_review",
        "real_acceptance_complete": False,
        "promotion_state": "blocked_pending_human_acceptance_and_release",
        "formal_release_allowed": False,
        "real_primary_latest_refresh_allowed": False,
        "default_entry_switch_allowed": False,
        "site_profile_validation": site,
        "source_probe_evidence": current_probe,
        "gates": gates,
        "blocker_count": sum(gate["status"] == "blocked" for gate in gates),
        "artifacts": artifacts,
        "opens_com_ports": False,
        "sends_device_commands": False,
        "source_evidence_opened_com_ports": current_probe.get("opens_com_ports") is True,
        "source_evidence_sent_read_only_device_commands": (
            current_probe.get("sends_read_only_device_commands") is True
        ),
        "source_evidence_sent_write_commands": current_probe.get("sends_write_commands") is True,
        "source_evidence_engineering_probe_only": (
            current_probe.get("engineering_probe_only") is True
        ),
        "connects_postgresql": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "database_written": False,
        "not_real_acceptance_evidence": True,
        "next_action": (
            "Complete the operator-reviewed active analyzer mapping and mature dry-run before read-only authorization."
            if readonly_preflight_blockers
            else "Obtain separate explicit authorization before any read-only real-COM execution."
            if readonly_reasons
            else "Resolve certificate/cylinder preflight blockers before gas-flow calibration."
            if calibration_preflight_blockers
            else "Complete calibration/archive evidence and regenerate this pack."
            if archive_reasons
            else "Submit the immutable pack to independent human acceptance review."
        ),
    }


def write_v1_5_real_acceptance_control_pack_outputs(
    *,
    model: Mapping[str, Any],
    site_profile: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, Path]:
    out = Path(output_dir)
    site = model.get("site_profile_validation")
    site = site if isinstance(site, Mapping) else {}
    paths = {
        "control_pack": out / "v1_5_real_acceptance_control_pack.json",
        "site_profile": out / "v1_5_real_acceptance_site_profile.json",
        "reviewed_ports": out / "v1_5_readonly_com_reviewed_port_inventory.json",
        "active_analyzers": out / "v1_5_readonly_com_active_analyzer_list.json",
        "checks": out / "v1_5_real_acceptance_control_pack_checks.csv",
        "markdown": out / "V1_5_REAL_ACCEPTANCE_CONTROL_PACK.md",
        "sha256": out / "SHA256SUMS.txt",
    }
    _write_json(paths["control_pack"], model)
    _write_json(paths["site_profile"], site_profile)
    _write_json(paths["reviewed_ports"], site.get("reviewed_port_inventory") or {})
    _write_json(paths["active_analyzers"], site.get("active_analyzer_list") or {})
    _write_csv(paths["checks"], model.get("gates") or [])
    lines = [
        "# V1.5 真实验收控制包",
        "",
        f"- 生命周期状态：`{model.get('lifecycle_status')}`",
        f"- 阻断门数量：`{model.get('blocker_count')}`",
        f"- 只读授权前置条件：`{model.get('preflight_ready_for_explicit_readonly_authorization')}`",
        "- 本工具不打开串口、不发送设备命令、不写系数、不控制气路、不自动放行。",
        (
            "- 警告：上游现场证据声明发送了写命令，控制包已阻断。"
            if model.get("source_evidence_sent_write_commands")
            else "- 上游现场证据曾打开串口并发送只读查询；已单独标记为工程探针，不是正式验收证据。"
            if model.get("source_evidence_sent_read_only_device_commands")
            else "- 未绑定发送设备命令的上游现场证据。"
        ),
        "",
        "## 门禁",
        "",
    ]
    for gate in model.get("gates") or []:
        reasons = ", ".join(str(reason) for reason in gate.get("reasons") or []) or "none"
        lines.append(f"- `{gate.get('gate')}`：`{gate.get('status')}`；{reasons}")
    lines.extend(["", "## 下一步", "", str(model.get("next_action") or "")])
    paths["markdown"].parent.mkdir(parents=True, exist_ok=True)
    paths["markdown"].write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8-sig")

    hash_lines = []
    for key in ("control_pack", "site_profile", "reviewed_ports", "active_analyzers", "checks", "markdown"):
        path = paths[key]
        hash_lines.append(f"{_sha256(path)}  {path.name}")
    paths["sha256"].write_text("\n".join(hash_lines) + "\n", encoding="utf-8")
    return paths
