"""Build the offline V1.5 real-acceptance control pack.

The pack connects three existing governance surfaces without executing them:
the operator-reviewed site profile, the mature workstation/certificate gates,
and the formal evidence lifecycle.  It never opens COM, sends device commands,
controls routes, writes analyzer state, or promotes evidence automatically.
"""

from __future__ import annotations

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
SN_PATTERN = re.compile(r"^\d{8}$")
ANALYZER_BANK = tuple(f"COM{index}" for index in range(35, 43))
LEGACY_ALGORITHMS = {"legacy", "legacy_ratio", "old", "ratio"}
NEW_ALGORITHMS = {"new", "new_absorption", "absorption", "absorption_ratio"}


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
        elif algorithm in LEGACY_ALGORITHMS and (check_capable is not False or check_required is not False):
            reasons.append(f"{port}_legacy_check_must_be_false")
        elif algorithm in NEW_ALGORITHMS and (check_capable is not True or check_required is not True):
            reasons.append(f"{port}_new_algorithm_check_must_be_true")
        try:
            ftd_ok = abs(float(runtime.get("ftd_hz")) - 1.0) < 1e-9
        except (TypeError, ValueError):
            ftd_ok = False
        if not ftd_ok:
            reasons.append(f"{port}_runtime_1hz_evidence_missing")
        if not _text(runtime, "average1") or not _text(runtime, "average2"):
            reasons.append(f"{port}_average1_average2_evidence_missing")
        active_analyzers.append(
            {
                "ga_label": label,
                "port": port,
                "protocol_device_id": protocol_id,
                "sn_code": sn_code,
                "algorithm": algorithm,
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
        "gates": gates,
        "blocker_count": sum(gate["status"] == "blocked" for gate in gates),
        "artifacts": artifacts,
        "opens_com_ports": False,
        "sends_device_commands": False,
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
