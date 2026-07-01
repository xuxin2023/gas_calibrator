"""Offline V1.5 identity/GETCO evidence readiness review.

This validator consumes the artifacts produced by
``probe_v1_5_getco_component_snapshot``. It does not open COM ports, control
routes, connect to PostgreSQL, or write analyzer IDs/SENCO coefficients.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "v1_5_getco_identity_readiness_v1"
REQUIRED_GETCO_GROUPS = tuple(range(1, 10))
SNAPSHOT_JSON = "old_component_coefficients_snapshot.json"
IDENTITY_CSV = "getco_component_snapshot_identity.csv"
CONCLUSION_CSV = "getco_component_snapshot_conclusion.csv"
RUNTIME_CONFIG_JSON = "runtime_identity_bound_config.json"


@dataclass(frozen=True)
class GetcoIdentityReadinessCheck:
    check: str
    status: str
    evidence_role: str
    evidence_path: str
    reasons: tuple[str, ...]
    physical_meaning: str
    next_action: str
    details: Mapping[str, Any]

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "pass", "ready"}


def _falsey(value: Any) -> bool:
    if isinstance(value, bool):
        return not value
    text = str(value or "").strip().lower()
    return text in {"", "0", "false", "no", "n", "none"}


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    return payload if isinstance(payload, dict) else {}


def _load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists() or not path.is_file():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    keys: list[str] = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _csv_cell(row.get(key, "")) for key in keys})


def _csv_cell(value: Any) -> Any:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return value


def _path_text(path: Path) -> str:
    return str(path.resolve()) if path.exists() else ""


def _groups_from_text(value: Any) -> tuple[int, ...]:
    groups: list[int] = []
    for item in str(value or "").split(","):
        text = item.strip()
        if not text:
            continue
        try:
            group = int(text)
        except ValueError:
            continue
        if group not in groups:
            groups.append(group)
    return tuple(groups)


def _device_id(row: Mapping[str, Any]) -> str:
    return str(row.get("analyzer_device_id") or row.get("runtime_device_id") or "").strip()


def _required_groups_present(groups: Sequence[int]) -> bool:
    return set(REQUIRED_GETCO_GROUPS).issubset(set(groups))


def _analyzer_entries(config: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    devices = config.get("devices") if isinstance(config.get("devices"), Mapping) else {}
    entries: list[Mapping[str, Any]] = []
    gas_analyzers = devices.get("gas_analyzers") if isinstance(devices, Mapping) else None
    if isinstance(gas_analyzers, list):
        entries.extend(item for item in gas_analyzers if isinstance(item, Mapping))
    single = devices.get("gas_analyzer") if isinstance(devices, Mapping) else None
    if isinstance(single, Mapping):
        entries.append(single)
    return entries


def _check_missing_artifacts(paths: Mapping[str, Path]) -> GetcoIdentityReadinessCheck:
    missing = tuple(name for name, path in paths.items() if not path.exists())
    status = "pending_live_gate" if missing else "ready"
    return GetcoIdentityReadinessCheck(
        check="required_artifacts_present",
        status=status,
        evidence_role="identity_getco_epoch0_artifacts",
        evidence_path=str(paths["getco_dir"].resolve()),
        reasons=missing,
        physical_meaning=(
            "The live read-only GETCO probe must leave a complete artifact set before any auxiliary "
            "neutralization or pressure/component sampling can be interpreted."
        ),
        next_action="Run probe_v1_5_getco_component_snapshot with groups 1-9 and >=1 s command spacing.",
        details={name: _path_text(path) for name, path in paths.items() if name != "getco_dir"},
    )


def _check_identity_rows(identity_rows: Sequence[Mapping[str, Any]]) -> GetcoIdentityReadinessCheck:
    reasons: list[str] = []
    device_ids: list[str] = []
    for index, row in enumerate(identity_rows, start=1):
        device_id = _device_id(row)
        if not device_id:
            reasons.append(f"row{index}_missing_analyzer_device_id")
        elif device_id not in device_ids:
            device_ids.append(device_id)
        if not str(row.get("port") or "").strip():
            reasons.append(f"row{index}_missing_port")
        if not _truthy(row.get("identity_verified")):
            reasons.append(f"row{index}_identity_not_verified")
        if not _truthy(row.get("all_groups_found")):
            reasons.append(f"row{index}_not_all_groups_found")
        if not _required_groups_present(_groups_from_text(row.get("requested_groups"))):
            reasons.append(f"row{index}_requested_groups_not_1_to_9")
        if not _required_groups_present(_groups_from_text(row.get("found_groups"))):
            reasons.append(f"row{index}_found_groups_not_1_to_9")
        for key in ("writes_senco", "writes_device_id", "controls_water_or_gas_routes", "controls_pace"):
            if not _falsey(row.get(key)):
                reasons.append(f"row{index}_{key}_must_be_false")
    if not identity_rows:
        reasons.append("identity_csv_empty")
    if not 1 <= len(identity_rows) <= 6:
        reasons.append("active_analyzer_count_must_be_1_to_6")
    return GetcoIdentityReadinessCheck(
        check="identity_rows_bound_and_verified",
        status="ready" if not reasons else "blocked",
        evidence_role="getco_component_snapshot_identity",
        evidence_path="getco_component_snapshot_identity.csv",
        reasons=tuple(reasons),
        physical_meaning=(
            "COM and GA labels are transport only; the runtime analyzer device ID from MODE2/stream evidence "
            "must be bound before pressure, CO2, H2O, or coefficient-write stages."
        ),
        next_action="Fix missing identity rows or rerun the read-only snapshot; do not proceed with guessed COM aliases.",
        details={"active_analyzer_count": len(identity_rows), "analyzer_device_ids": device_ids},
    )


def _check_conclusion(conclusion_rows: Sequence[Mapping[str, Any]], identity_rows: Sequence[Mapping[str, Any]]) -> GetcoIdentityReadinessCheck:
    reasons: list[str] = []
    row = conclusion_rows[0] if conclusion_rows else {}
    if not row:
        reasons.append("conclusion_csv_empty")
    if str(row.get("status") or "").strip().lower() != "pass":
        reasons.append("conclusion_status_not_pass")
    if _as_int(row.get("analyzer_count")) != len(identity_rows):
        reasons.append("conclusion_analyzer_count_mismatch")
    if not _required_groups_present(_groups_from_text(row.get("groups"))):
        reasons.append("conclusion_groups_not_1_to_9")
    if not _truthy(row.get("all_devices_bound")):
        reasons.append("conclusion_all_devices_bound_false")
    if not _truthy(row.get("all_identity_verified")):
        reasons.append("conclusion_all_identity_verified_false")
    for key in ("writes_senco", "writes_device_id", "controls_water_or_gas_routes", "controls_pace"):
        if not _falsey(row.get(key)):
            reasons.append(f"conclusion_{key}_must_be_false")
    return GetcoIdentityReadinessCheck(
        check="snapshot_conclusion_passes_no_write_contract",
        status="ready" if not reasons else "blocked",
        evidence_role="getco_component_snapshot_conclusion",
        evidence_path="getco_component_snapshot_conclusion.csv",
        reasons=tuple(reasons),
        physical_meaning="The snapshot conclusion must prove identity binding and GETCO completeness without any writes or route/pressure control.",
        next_action="Review the probe output and rerun the read-only snapshot if conclusion is not pass.",
        details=dict(row),
    )


def _check_snapshot(snapshot: Mapping[str, Any], identity_rows: Sequence[Mapping[str, Any]]) -> GetcoIdentityReadinessCheck:
    reasons: list[str] = []
    device_ids = tuple(_device_id(row) for row in identity_rows if _device_id(row))
    if not snapshot:
        reasons.append("old_component_coefficients_snapshot_empty")
    for device_id in device_ids:
        device = snapshot.get(device_id)
        if not isinstance(device, Mapping):
            reasons.append(f"{device_id}_missing_from_snapshot")
            continue
        for group in REQUIRED_GETCO_GROUPS:
            values = device.get(f"GETCO{group}_before")
            if not isinstance(values, list) or not values:
                reasons.append(f"{device_id}_GETCO{group}_before_missing")
            command = str(device.get(f"GETCO{group}_before_command") or "")
            if command and f",{group}" not in command:
                reasons.append(f"{device_id}_GETCO{group}_command_mismatch")
    return GetcoIdentityReadinessCheck(
        check="old_getco1_to_getco9_snapshot_complete",
        status="ready" if not reasons else "blocked",
        evidence_role="old_component_coefficients_snapshot",
        evidence_path="old_component_coefficients_snapshot.json",
        reasons=tuple(reasons),
        physical_meaning="GETCO1-9 is coefficient epoch 0. Auxiliary clears, pressure recovery, and component fitting must reference this immutable baseline.",
        next_action="Rerun the GETCO probe for missing groups before any SENCO5/6/7/8/9 neutralization.",
        details={"analyzer_device_ids": device_ids, "required_groups": REQUIRED_GETCO_GROUPS},
    )


def _check_runtime_config(config: Mapping[str, Any], identity_rows: Sequence[Mapping[str, Any]]) -> GetcoIdentityReadinessCheck:
    reasons: list[str] = []
    entries = _analyzer_entries(config)
    rows_by_port = {
        str(row.get("port") or "").strip(): row
        for row in identity_rows
        if str(row.get("port") or "").strip()
    }
    entries_by_port = {
        str(entry.get("port") or "").strip(): entry
        for entry in entries
        if str(entry.get("port") or "").strip()
    }
    if not config:
        reasons.append("runtime_identity_bound_config_empty")
    for port, row in rows_by_port.items():
        entry = entries_by_port.get(port)
        if not isinstance(entry, Mapping):
            reasons.append(f"{port}_missing_runtime_bound_analyzer")
            continue
        if str(entry.get("device_id") or "") != _device_id(row):
            reasons.append(f"{port}_device_id_not_bound_to_runtime_identity")
        if entry.get("runtime_identity_bound") is not True:
            reasons.append(f"{port}_runtime_identity_bound_false")
        if entry.get("identity_binding_frozen") is not True:
            reasons.append(f"{port}_identity_binding_frozen_false")
        if entry.get("identity_binding_source") != "v1_5_getco_component_snapshot":
            reasons.append(f"{port}_identity_binding_source_wrong")
    workflow = config.get("workflow") if isinstance(config.get("workflow"), Mapping) else {}
    analyzer_init = workflow.get("analyzer_mode2_init") if isinstance(workflow.get("analyzer_mode2_init"), Mapping) else {}
    if _as_float(analyzer_init.get("command_gap_s")) < 1.0:
        reasons.append("analyzer_mode2_init_command_gap_below_1s")
    if analyzer_init.get("fragile_serial_contract") != "minimum_1s_command_gap":
        reasons.append("fragile_serial_contract_missing")
    binding = config.get("v1_5_identity_binding") if isinstance(config.get("v1_5_identity_binding"), Mapping) else {}
    if binding.get("frozen_for_run") is not True:
        reasons.append("identity_binding_not_frozen_for_run")
    if binding.get("writes_device_id") is not False:
        reasons.append("identity_binding_must_not_write_device_id")
    if _as_float(binding.get("analyzer_command_gap_s")) < 1.0:
        reasons.append("identity_binding_command_gap_below_1s")
    return GetcoIdentityReadinessCheck(
        check="runtime_identity_bound_config_frozen",
        status="ready" if not reasons else "blocked",
        evidence_role="runtime_identity_bound_config",
        evidence_path="runtime_identity_bound_config.json",
        reasons=tuple(reasons),
        physical_meaning="All downstream pressure, CO2, and H2O physical stages must use the runtime-bound config from the GETCO snapshot.",
        next_action="Regenerate runtime_identity_bound_config.json from the read-only snapshot before pressure or route stages.",
        details={"bound_ports": sorted(rows_by_port), "runtime_config_analyzers": len(entries)},
    )


def build_getco_identity_readiness_model(
    *,
    getco_dir: str | Path,
) -> dict[str, Any]:
    root = Path(getco_dir).resolve()
    paths = {
        "getco_dir": root,
        "snapshot_json": root / SNAPSHOT_JSON,
        "identity_csv": root / IDENTITY_CSV,
        "conclusion_csv": root / CONCLUSION_CSV,
        "runtime_config_json": root / RUNTIME_CONFIG_JSON,
    }
    snapshot = _load_json(paths["snapshot_json"])
    identity_rows = _load_csv(paths["identity_csv"])
    conclusion_rows = _load_csv(paths["conclusion_csv"])
    runtime_config = _load_json(paths["runtime_config_json"])

    missing_check = _check_missing_artifacts(paths)
    artifact_paths = (
        paths["snapshot_json"],
        paths["identity_csv"],
        paths["conclusion_csv"],
        paths["runtime_config_json"],
    )
    if missing_check.status == "pending_live_gate" and not any(path.exists() for path in artifact_paths):
        checks = [missing_check]
    else:
        checks = [
            missing_check,
            _check_identity_rows(identity_rows),
            _check_conclusion(conclusion_rows, identity_rows),
            _check_snapshot(snapshot, identity_rows),
            _check_runtime_config(runtime_config, identity_rows),
        ]
    statuses = {check.status for check in checks}
    if "blocked" in statuses:
        overall = "identity_getco_blocked"
    elif "pending_live_gate" in statuses:
        overall = "identity_getco_pending_live_gate"
    else:
        overall = "identity_getco_ready_for_auxiliary_neutralization"

    analyzer_ids = tuple(_device_id(row) for row in identity_rows if _device_id(row))
    return {
        "schema": SCHEMA,
        "created_at": _now(),
        "overall_status": overall,
        "getco_dir": str(root),
        "active_analyzer_count": len(identity_rows),
        "analyzer_device_ids": analyzer_ids,
        "required_getco_groups": REQUIRED_GETCO_GROUPS,
        "opens_com_ports": False,
        "connects_postgresql": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_device_id": False,
        "not_real_acceptance_evidence": True,
        "required_before_auxiliary_neutralization": [
            "required_artifacts_present",
            "identity_rows_bound_and_verified",
            "snapshot_conclusion_passes_no_write_contract",
            "old_getco1_to_getco9_snapshot_complete",
            "runtime_identity_bound_config_frozen",
        ],
        "next_controlled_gate": "auxiliary_senco56789_neutralization_gate",
        "checks": [check.to_json() for check in checks],
    }


def render_getco_identity_readiness_markdown(model: Mapping[str, Any]) -> str:
    lines = [
        "# V1.5 Identity/GETCO Readiness",
        "",
        f"- schema: `{model.get('schema')}`",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- active_analyzer_count: `{model.get('active_analyzer_count')}`",
        f"- opens_com_ports: `{model.get('opens_com_ports')}`",
        f"- controls_water_or_gas_routes: `{model.get('controls_water_or_gas_routes')}`",
        f"- writes_coefficients: `{model.get('writes_coefficients')}`",
        f"- next_controlled_gate: `{model.get('next_controlled_gate')}`",
        "",
        "## Required Before Auxiliary Neutralization",
        "",
    ]
    for item in model.get("required_before_auxiliary_neutralization") or []:
        lines.append(f"- `{item}`")
    lines.extend(["", "## Checks", "", "| check | status | evidence_role | next_action |", "|---|---|---|---|"])
    for row in model.get("checks") or []:
        lines.append(
            f"| `{row.get('check', '')}` | `{row.get('status', '')}` | "
            f"`{row.get('evidence_role', '')}` | {row.get('next_action', '')} |"
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- This sidecar only reads GETCO snapshot artifacts produced by the authorized live probe.",
            "- It does not open COM, write analyzer IDs, write SENCO, connect PostgreSQL, or control routes.",
            "- A ready result means the evidence is complete enough to review the next controlled gate; it is not release acceptance.",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def write_getco_identity_readiness_outputs(model: Mapping[str, Any], output_dir: str | Path) -> dict[str, Path]:
    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    json_path = root / "v1_5_getco_identity_readiness.json"
    md_path = root / "v1_5_getco_identity_readiness.md"
    checks_path = root / "v1_5_getco_identity_readiness_checks.csv"
    json_path.write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_getco_identity_readiness_markdown(model), encoding="utf-8")
    _write_csv(checks_path, model.get("checks") or [])
    return {"json": json_path, "markdown": md_path, "checks_csv": checks_path}
