"""Build the V1.5 formal initialization contract.

The formal initializer is intentionally a planner in this first step. It
stitches together the already-validated V1.5 snapshot and controlled-write
tools, records the physical meaning of each gate, and refuses unsafe analyzer
command pacing. It does not open COM ports, write SENCO, control PACE, or touch
gas/water routes by itself.
"""

from __future__ import annotations

import argparse
import csv
import json
import shlex
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from ..config import load_config
from ..storage.v1_5_evidence.bundle import sha256_file, sha256_json, stable_id


SCHEMA = "v1_5_formal_initialization_plan_v0"
MIN_ANALYZER_COMMAND_GAP_S = 1.0
DEFAULT_ANALYZER_COMMAND_GAP_S = 1.2
DB_BUNDLE_SCHEMA = "v1_5_formal_initialization_db_bundle_v0"
DEFAULT_COM22_PRESSURE_REFERENCE_JSON = (
    "logs/v1_5_6ch_initialization_20260617_r2/"
    "com22_pressure_reference_FRGsz25038057_118288.json"
)


INITIALIZATION_TOOL_OWNERSHIP: Mapping[str, Mapping[str, str]] = {
    "formal_initialization_runner": {
        "tool": "gas_calibrator.tools.run_v1_5_formal_initialization_runner",
        "role": "single_formal_initialization_entrypoint",
        "allowed_use": "Plan and gate identity, GETCO1-9 epoch-0 snapshot, S5/S6/S7/S8/S9 handling, startup acquisition settings, readiness, and database evidence indexing.",
        "forbidden_use": "Does not open COM ports, write SENCO, write device IDs, control PACE, or control gas/water routes by itself.",
    },
    "getco_snapshot_probe": {
        "tool": "gas_calibrator.tools.probe_v1_5_getco_component_snapshot",
        "role": "subordinate_read_only_identity_and_getco_snapshot",
        "allowed_use": "When V1.5 real hardware is explicitly authorized, bind device ID to transport and read GETCO1-9 with a slow analyzer command gap.",
        "forbidden_use": "Not a top-level formal initialization entrypoint; never writes device ID or SENCO.",
    },
    "sn_identity_initialization": {
        "tool": "gas_calibrator.tools.run_v1_5_sn_identity_initialization",
        "role": "subordinate_first_discovery_sn_device_code_planner",
        "allowed_use": "Plan first-discovery 8-digit numeric SN/device_code allocation before GETCO evidence; real SN writes require the dedicated SN authorization phrase.",
        "forbidden_use": "Never writes SN/device_code from the formal planner or readiness audit; not a CO2/H2O sampling runner.",
    },
    "controlled_writers": {
        "tool": "gas_calibrator.tools.run_v1_5_*_controlled_write",
        "role": "subordinate_authorized_write_tools",
        "allowed_use": "Run only the reviewed S5/S6/S7/S8/S9 or main coefficient write action with old snapshot, explicit confirmation, readback, and rollback evidence.",
        "forbidden_use": "Never called automatically by readiness, report, archive, or the planner.",
    },
    "readiness_exporters": {
        "tool": "gas_calibrator.tools.export_v1_5_initialization_readiness",
        "role": "subordinate_offline_readiness_audit",
        "allowed_use": "Check existing evidence and explain missing initialization proof before open-flow sampling.",
        "forbidden_use": "Does not open COM or repair missing evidence.",
    },
    "formal_route_readiness_probe": {
        "tool": "gas_calibrator.tools.run_v1_5_formal_route_readiness_probe",
        "role": "subordinate_initialization_route_readiness_probe",
        "allowed_use": "Before chamber soak, prove relay_map completeness, relay/relay_8 read-write availability, dewpoint-meter online status, and N2 prepurge valve open/close when enabled.",
        "forbidden_use": "Not a formal CO2/H2O sampling runner; does not open CO2/H2O source valves, write SENCO, write device IDs, or run V2 real COM.",
    },
    "historical_artifacts": {
        "tool": "logs, reports, archived artifacts",
        "role": "traceability_reference_only",
        "allowed_use": "Keep for reconstruction, audit, and rollback context.",
        "forbidden_use": "Do not delete and do not use as a default formal entrypoint.",
    },
}


@dataclass(frozen=True)
class InitializationStep:
    step_id: str
    title: str
    phase: str
    command: tuple[str, ...] = ()
    execution_mode: str = "planned_only"
    opens_com_ports: bool = False
    writes_coefficients: bool = False
    controls_pressure: bool = False
    controls_gas_route: bool = False
    controls_water_route: bool = False
    writes_device_id: bool = False
    required_inputs: tuple[str, ...] = ()
    expected_outputs: tuple[str, ...] = ()
    physical_meaning: str = ""
    safety_notes: tuple[str, ...] = ()
    gate: str = "review"

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class InitializationPlan:
    schema: str
    run_id: str
    created_at: str
    config_path: str
    output_dir: str
    dry_run_only: bool
    analyzer_command_gap_s: float
    operator: str = ""
    reviewer: str = ""
    approver: str = ""
    expected_device_ids: tuple[str, ...] = ()
    analyzer_identities: tuple[Mapping[str, Any], ...] = ()
    safety_contract: Mapping[str, Any] = field(default_factory=dict)
    physical_contract: Mapping[str, Any] = field(default_factory=dict)
    coefficient_policy: Mapping[str, Any] = field(default_factory=dict)
    tool_ownership: Mapping[str, Any] = field(default_factory=dict)
    steps: tuple[InitializationStep, ...] = ()
    warnings: tuple[str, ...] = ()

    def to_json(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["steps"] = [step.to_json() for step in self.steps]
        return payload


@dataclass(frozen=True)
class InitializationExecutionStepResult:
    step_id: str
    title: str
    status: str
    reason: str = ""
    returncode: int | None = None
    command: tuple[str, ...] = ()
    command_text: str = ""
    started_at: str = ""
    ended_at: str = ""
    stdout_path: str = ""
    stderr_path: str = ""
    opens_com_ports: bool = False
    writes_coefficients: bool = False
    controls_pressure: bool = False
    controls_gas_route: bool = False
    controls_water_route: bool = False
    writes_device_id: bool = False

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class InitializationExecutionReport:
    schema: str
    run_id: str
    created_at: str
    status: str
    allow_read_only_real_com: bool
    allow_controlled_writes: bool
    stop_on_failure: bool
    selected_steps: tuple[str, ...]
    step_results: tuple[InitializationExecutionStepResult, ...]

    def to_json(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["step_results"] = [row.to_json() for row in self.step_results]
        return payload


def _cmd(*parts: object) -> tuple[str, ...]:
    return tuple(str(part) for part in parts if str(part) != "")


def _python_module(module: str, *args: object) -> tuple[str, ...]:
    return _cmd("python", "-m", module, *args)


def _artifact(root: Path, *parts: str) -> str:
    return str(root.joinpath(*parts))


def _quote_command(command: Sequence[str]) -> str:
    if not command:
        return ""
    return " ".join(shlex.quote(str(part)) for part in command)


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _device_id(value: Any) -> str:
    text = str(value or "").strip()
    if text.isdigit():
        return f"{int(text):03d}"
    return text.upper()


def _enabled_analyzers(cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    devices = cfg.get("devices", {}) if isinstance(cfg, Mapping) else {}
    source = devices.get("gas_analyzers") if isinstance(devices, Mapping) else None
    if isinstance(source, list) and source:
        raw_analyzers = source
    else:
        top_level = cfg.get("analyzers") if isinstance(cfg, Mapping) else None
        if isinstance(top_level, list) and top_level:
            raw_analyzers = top_level
        else:
            single = devices.get("gas_analyzer") if isinstance(devices, Mapping) else None
            raw_analyzers = [single] if isinstance(single, Mapping) and single.get("enabled", False) else []
    analyzers = [
        _normalize_analyzer_identity(item, index=idx)
        for idx, item in enumerate(raw_analyzers, start=1)
        if isinstance(item, Mapping) and item.get("enabled", True)
    ]
    return analyzers


def _normalize_analyzer_identity(item: Mapping[str, Any], *, index: int) -> dict[str, Any]:
    row = dict(item)
    slot = str(row.get("slot") or row.get("slot_id") or row.get("name") or f"GA{index:02d}").strip()
    slot_id = slot.upper() if slot.lower().startswith("ga") else slot
    protocol_id = _device_id(
        row.get("protocol_device_id")
        or row.get("device_id")
        or row.get("runtime_device_id")
        or row.get("configured_device_id")
    )
    sn_code = _sn_code(row.get("sn_code") or row.get("current_sn"))
    device_code = _sn_code(row.get("device_code") or sn_code)
    row["slot_id"] = slot_id
    row["slot"] = slot_id
    row.setdefault("name", slot_id.lower())
    if protocol_id:
        row["protocol_device_id"] = protocol_id
        row.setdefault("device_id", protocol_id)
    if sn_code:
        row["sn_code"] = sn_code
        row["device_code"] = device_code or sn_code
    elif device_code:
        row["sn_code"] = device_code
        row["device_code"] = device_code
    return row


def _sn_code(value: Any) -> str:
    text = str(value or "").strip()
    return text if len(text) == 8 and text.isdigit() and text != "00000000" else ""


def _analyzer_identities(config_path: Path) -> tuple[Mapping[str, Any], ...]:
    try:
        cfg = load_config(config_path)
    except Exception:
        return ()
    rows: list[Mapping[str, Any]] = []
    for analyzer in _enabled_analyzers(cfg):
        protocol_id = _device_id(analyzer.get("protocol_device_id") or analyzer.get("device_id"))
        if not protocol_id and not analyzer.get("sn_code"):
            continue
        rows.append(
            {
                "slot_id": analyzer.get("slot_id") or analyzer.get("slot") or "",
                "name": analyzer.get("name") or "",
                "port": analyzer.get("port") or "",
                "baud": analyzer.get("baud"),
                "protocol_device_id": protocol_id,
                "device_id": protocol_id,
                "sn_code": analyzer.get("sn_code") or "",
                "device_code": analyzer.get("device_code") or analyzer.get("sn_code") or "",
                "identity_source": analyzer.get("identity_binding_source")
                or analyzer.get("runtime_identity_source")
                or "runtime_config",
            }
        )
    return tuple(rows)


def _expected_device_ids_from_identities(analyzer_identities: Sequence[Mapping[str, Any]]) -> tuple[str, ...]:
    ids: list[str] = []
    for analyzer in analyzer_identities:
        device_id = _device_id(analyzer.get("protocol_device_id") or analyzer.get("device_id"))
        if device_id and device_id not in ids:
            ids.append(device_id)
    return tuple(ids)


def _expected_device_ids(config_path: Path) -> tuple[str, ...]:
    return _expected_device_ids_from_identities(_analyzer_identities(config_path))


def _read_json(path: Path) -> Any | None:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return None


def _modified_at(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(timespec="seconds")


def _artifact_role(path: Path) -> str:
    name = path.name.lower()
    if name == "v1_5_formal_initialization_plan.json":
        return "initialization_plan_snapshot"
    if name == "v1_5_formal_initialization_plan.md":
        return "initialization_plan_report"
    if name == "v1_5_formal_initialization_commands.ps1":
        return "initialization_command_plan"
    if name == "v1_5_formal_initialization_contract.json":
        return "initialization_contract"
    if name == "old_component_coefficients_snapshot.json":
        return "coefficient_snapshot"
    if name == "getco_component_snapshot_identity.csv":
        return "initialization_identity_snapshot"
    if name == "runtime_identity_bound_config.json":
        return "initialization_runtime_identity_config"
    if name.endswith("_write_events.csv"):
        return "coefficient_write_log"
    if name == "temperature_current_point_review.json":
        return "temperature_current_point_review_model"
    if name == "temperature_current_point_review.md":
        return "temperature_current_point_review_report"
    if name == "temperature_current_point_review.csv":
        return "temperature_current_point_review_table"
    if name == "temperature_senco78_projection_check.csv":
        return "temperature_senco78_projection_check"
    if name == "temperature_current_point_reference_samples.csv":
        return "temperature_reference_samples"
    if name == "temperature_current_point_analyzer_samples.csv":
        return "temperature_analyzer_samples"
    if name == "formal_route_readiness.json":
        return "formal_route_readiness_model"
    if name == "v1_5_initialization_readiness.json":
        return "initialization_readiness_model"
    if name == "v1_5_initialization_readiness.md":
        return "initialization_readiness_report"
    if name == "v1_5_formal_initialization_execution.json":
        return "initialization_execution_log"
    if name == "v1_5_formal_initialization_execution.csv":
        return "initialization_execution_table"
    return "initialization_supporting_artifact"


def _required_artifact_role(role: str) -> bool:
    return role in {
        "initialization_plan_snapshot",
        "initialization_contract",
        "coefficient_snapshot",
        "initialization_identity_snapshot",
        "initialization_runtime_identity_config",
        "temperature_current_point_review_model",
        "formal_route_readiness_model",
    }


def _relative_to(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except Exception:
        return str(path.resolve())


def _known_initialization_artifacts(
    plan: InitializationPlan,
    outputs: Mapping[str, Path],
) -> list[Path]:
    root = Path(plan.output_dir).resolve()
    candidates: list[Path] = [Path(path) for path in outputs.values()]
    for step in plan.steps:
        for value in step.expected_outputs:
            if value:
                candidates.append(Path(value))
    candidates.extend(
        [
            root / "v1_5_initialization_readiness.json",
            root / "v1_5_initialization_readiness.md",
        ]
    )
    seen: set[str] = set()
    paths: list[Path] = []
    for path in candidates:
        resolved = path.resolve()
        if resolved.name.lower() == "v1_5_formal_initialization_db_bundle.json":
            continue
        key = str(resolved).lower()
        if key in seen or not resolved.exists() or not resolved.is_file():
            continue
        seen.add(key)
        paths.append(resolved)
    return paths


def _build_initialization_sample_files(
    *,
    run_db_id: str,
    plan: InitializationPlan,
    outputs: Mapping[str, Path],
) -> list[dict[str, Any]]:
    root = Path(plan.output_dir).resolve()
    rows: list[dict[str, Any]] = []
    for path in _known_initialization_artifacts(plan, outputs):
        role = _artifact_role(path)
        stat = path.stat()
        rows.append(
            {
                "id": stable_id("sample_file", run_db_id, str(path)),
                "run_db_id": run_db_id,
                "artifact_role": role,
                "path": str(path),
                "sha256": sha256_file(path),
                "size_bytes": int(stat.st_size),
                "modified_at": _modified_at(path),
                "required": _required_artifact_role(role),
                "metadata": {
                    "relative_to_initialization_dir": _relative_to(path, root),
                    "extension": path.suffix.lower(),
                    "initialization_artifact": True,
                },
            }
        )
    return rows


def _device_rows(plan: InitializationPlan, run_db_id: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    devices: list[dict[str, Any]] = []
    run_devices: list[dict[str, Any]] = []
    identity_rows = list(plan.analyzer_identities or ())
    if not identity_rows:
        identity_rows = [{"protocol_device_id": device_id, "device_id": device_id} for device_id in plan.expected_device_ids]
    for index, identity in enumerate(identity_rows, start=1):
        device_id = _device_id(identity.get("protocol_device_id") or identity.get("device_id"))
        sn_code = str(identity.get("sn_code") or "").strip()
        device_code = str(identity.get("device_code") or sn_code).strip()
        slot_id = str(identity.get("slot_id") or identity.get("slot") or f"GA{index:02d}").strip()
        formal_key = f"gas_analyzer:sn:{sn_code}" if sn_code else f"gas_analyzer:id:{device_id}"
        display_name = sn_code or device_id
        identity_key = "sn_code/device_code" if sn_code else "analyzer_internal_mode2_id"
        metadata = {
            "identity_source": identity.get("identity_source")
            or "runtime_config_expected_id_pending_live_mode2_confirmation",
            "identity_key": identity_key,
            "com_port_is_transport_only": True,
            "initialization_expected_device": True,
            "slot_id": slot_id,
            "port_at_initialization": identity.get("port") or "",
            "protocol_device_id_current": device_id,
            "sn_code": sn_code,
            "device_code": device_code or sn_code,
        }
        row = {
            "id": stable_id("device", "analyzer", sn_code or device_id),
            "device_type": "gas_analyzer",
            "device_role": "device_under_test",
            "display_name": display_name,
            "serial_number": sn_code or device_id,
            "device_key": formal_key,
            "sn_code": sn_code,
            "device_code": device_code or sn_code,
            "protocol_device_id_current": device_id,
            "metadata": metadata,
            "metadata_json": metadata,
        }
        devices.append(row)
        run_devices.append(
            {
                "id": stable_id("run_device", run_db_id, row["id"], "device_under_test"),
                "run_db_id": run_db_id,
                "device_id": row["id"],
                "role": "device_under_test",
                "slot_id": slot_id,
                "port": identity.get("port") or "",
                "sn_code": sn_code,
                "device_code": device_code or sn_code,
                "protocol_device_id_at_run": device_id,
                "mode_at_run": "2" if sn_code else "",
                "status": "formal_initialization_planned_identity_bound" if sn_code else "formal_initialization_planned_device_id_only",
                "metadata": {
                    "identity_key": identity_key,
                    "planned_device_id": device_id,
                    "sn_code": sn_code,
                    "device_code": device_code or sn_code,
                    "com_port_is_transport_only": True,
                },
            }
        )
    return devices, run_devices


def _file_by_role(files: Sequence[Mapping[str, Any]], role: str) -> Mapping[str, Any] | None:
    for row in files:
        if row.get("artifact_role") == role:
            return row
    return None


def _getco_groups(coefficients: Mapping[str, Any]) -> list[str]:
    groups: set[str] = set()
    for key in coefficients:
        text = str(key)
        if text.startswith("GETCO") and ("_before" in text or "_after" in text):
            groups.add(text.split("_", 1)[0])
    return sorted(groups, key=lambda item: int(item.replace("GETCO", "") or "0"))


def _build_initialization_coefficient_snapshots(
    *,
    run_db_id: str,
    files: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    file_row = _file_by_role(files, "coefficient_snapshot")
    if not file_row:
        return []
    path = Path(str(file_row.get("path") or ""))
    payload = _read_json(path) if path.exists() else None
    if not isinstance(payload, Mapping):
        return []

    rows: list[dict[str, Any]] = []
    for key, value in payload.items():
        if not isinstance(value, Mapping):
            continue
        getco_keys = [item for item in value if str(item).startswith("GETCO")]
        if not getco_keys:
            continue
        analyzer_id = _device_id(value.get("analyzer_device_id") or key)
        groups = _getco_groups(value)
        rows.append(
            {
                "id": stable_id("coefficient_snapshot", run_db_id, str(path), analyzer_id),
                "run_db_id": run_db_id,
                "analyzer_id": analyzer_id,
                "snapshot_type": "initialization_epoch0_getco1_9",
                "coefficients": dict(value),
                "coefficients_hash": sha256_json(value),
                "source_artifact_id": str(file_row.get("id") or ""),
                "metadata": {
                    "path": str(path),
                    "artifact_sha256": file_row.get("sha256"),
                    "getco_groups": groups,
                    "missing_getco_groups": [
                        f"GETCO{index}"
                        for index in range(1, 10)
                        if f"GETCO{index}" not in groups
                    ],
                    "physical_meaning": "GETCO1-9 freezes coefficient epoch 0 before any S5/S6/S7/S8/S9 handling.",
                },
            }
        )
    return rows


def _read_event_csv(path: Path) -> list[dict[str, str]]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    except Exception:
        return []


def _build_initialization_write_events(
    *,
    run_db_id: str,
    plan: InitializationPlan,
    files: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = [
        {
            "id": stable_id("coefficient_write_event", run_db_id, "formal_initialization_planner_no_write"),
            "run_db_id": run_db_id,
            "analyzer_id": None,
            "event_type": "formal_initialization_planner_no_write",
            "status": "not_attempted",
            "approved_by": None,
            "command_summary": "The formal initialization planner only builds evidence and gated commands; it does not write SENCO.",
            "old_coefficients_hash": None,
            "candidate_id": None,
            "readback": {},
            "metadata": {
                "opens_com_ports": False,
                "writes_coefficients": False,
                "controls_gas_route": False,
                "controls_water_route": False,
                "controls_pressure": False,
                "minimum_analyzer_command_gap_s": plan.analyzer_command_gap_s,
            },
        }
    ]
    for file_row in files:
        if file_row.get("artifact_role") != "coefficient_write_log":
            continue
        path = Path(str(file_row.get("path") or ""))
        event_type = path.stem
        records = _read_event_csv(path)
        if not records:
            rows.append(
                {
                    "id": stable_id("coefficient_write_event", run_db_id, str(path), "empty_or_unparsed"),
                    "run_db_id": run_db_id,
                    "analyzer_id": None,
                    "event_type": event_type,
                    "status": "review_required",
                    "approved_by": None,
                    "command_summary": "Initialization write-event artifact exists but could not be parsed as rows.",
                    "old_coefficients_hash": None,
                    "candidate_id": None,
                    "readback": {},
                    "metadata": {"source_artifact_id": file_row.get("id"), "path": str(path)},
                }
            )
            continue
        for index, record in enumerate(records, start=1):
            analyzer_id = _device_id(
                record.get("analyzer_device_id")
                or record.get("device_id")
                or record.get("analyzer_id")
                or record.get("target")
                or ""
            ) or None
            rows.append(
                {
                    "id": stable_id("coefficient_write_event", run_db_id, str(path), index, analyzer_id),
                    "run_db_id": run_db_id,
                    "analyzer_id": analyzer_id,
                    "event_type": event_type,
                    "status": record.get("status") or record.get("result") or "review_required",
                    "approved_by": record.get("approved_by") or plan.approver or None,
                    "command_summary": record.get("command") or record.get("write_command") or event_type,
                    "old_coefficients_hash": record.get("old_coefficients_hash") or None,
                    "candidate_id": None,
                    "readback": dict(record),
                    "metadata": {"source_artifact_id": file_row.get("id"), "path": str(path), "row_index": index},
                }
            )
    return rows


def _integrity_row(
    *,
    run_db_id: str,
    name: str,
    status: str,
    severity: str,
    details: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "id": stable_id("integrity_check", run_db_id, name),
        "run_db_id": run_db_id,
        "check_name": name,
        "status": status,
        "severity": severity,
        "details": dict(details),
    }


def _build_initialization_integrity_checks(
    *,
    run_db_id: str,
    plan: InitializationPlan,
    files: Sequence[Mapping[str, Any]],
    coefficient_snapshots: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    roles = {str(row.get("artifact_role") or "") for row in files}
    snapshot_devices = {str(row.get("analyzer_id") or "") for row in coefficient_snapshots if row.get("analyzer_id")}
    expected_devices = set(plan.expected_device_ids)
    return [
        _integrity_row(
            run_db_id=run_db_id,
            name="formal_initialization_planner_no_real_com",
            status="pass",
            severity="error",
            details=plan.safety_contract,
        ),
        _integrity_row(
            run_db_id=run_db_id,
            name="analyzer_command_gap_at_least_one_second",
            status="pass" if plan.analyzer_command_gap_s >= MIN_ANALYZER_COMMAND_GAP_S else "fail",
            severity="error",
            details={
                "configured_analyzer_command_gap_s": plan.analyzer_command_gap_s,
                "minimum_analyzer_command_gap_s": MIN_ANALYZER_COMMAND_GAP_S,
            },
        ),
        _integrity_row(
            run_db_id=run_db_id,
            name="initialization_plan_artifacts_hashed",
            status="pass"
            if {"initialization_plan_snapshot", "initialization_contract"}.issubset(roles)
            else "fail",
            severity="error",
            details={"artifact_roles": sorted(roles)},
        ),
        _integrity_row(
            run_db_id=run_db_id,
            name="expected_device_ids_available",
            status="pass" if plan.expected_device_ids else "warn",
            severity="warning",
            details={"expected_device_ids": list(plan.expected_device_ids)},
        ),
        _integrity_row(
            run_db_id=run_db_id,
            name="epoch0_getco1_9_snapshot_indexed",
            status="pass" if coefficient_snapshots else "warn",
            severity="warning",
            details={
                "snapshot_count": len(coefficient_snapshots),
                "expected_device_ids": sorted(expected_devices),
                "snapshot_device_ids": sorted(snapshot_devices),
                "missing_snapshot_device_ids": sorted(expected_devices - snapshot_devices),
                "physical_meaning": "GETCO1-9 epoch-0 is required before any coefficient write or formal coefficient review.",
            },
        ),
        _integrity_row(
            run_db_id=run_db_id,
            name="s5_s6_s78_s9_actions_gated",
            status="pass",
            severity="error",
            details={
                "senco5": plan.coefficient_policy.get("senco5"),
                "senco6": plan.coefficient_policy.get("senco6"),
                "senco78": plan.coefficient_policy.get("senco78"),
                "senco78_current_point_review": plan.coefficient_policy.get("senco78_current_point_review"),
                "senco78_single_point_repair": plan.coefficient_policy.get("senco78_single_point_repair"),
                "senco9": plan.coefficient_policy.get("senco9"),
                "note": "Neutralize/clear/repair commands are recorded as gated commands; the planner itself does not execute them.",
            },
        ),
    ]


def build_formal_initialization_database_bundle(
    plan: InitializationPlan,
    outputs: Mapping[str, Path],
) -> dict[str, Any]:
    """Build a database-import bundle for initialization traceability.

    The bundle uses the existing V1.5 evidence registry schema. Raw files remain
    in the evidence directory; the database receives IDs, paths, SHA-256 hashes,
    coefficient snapshots, audit events, and integrity checks so future reviews
    can query by analyzer device ID.
    """

    root = Path(plan.output_dir).resolve()
    run_db_id = stable_id("formal_initialization_run", plan.run_id, str(root))
    files = _build_initialization_sample_files(run_db_id=run_db_id, plan=plan, outputs=outputs)
    coefficient_snapshots = _build_initialization_coefficient_snapshots(run_db_id=run_db_id, files=files)
    write_events = _build_initialization_write_events(run_db_id=run_db_id, plan=plan, files=files)
    devices, run_devices = _device_rows(plan, run_db_id)
    config_path = Path(plan.config_path)
    now = _now_utc()
    tables = {
        "runs": [
            {
                "id": run_db_id,
                "run_id": plan.run_id,
                "run_dir": str(root),
                "plan_id": "v1_5_formal_initialization",
                "plan_version": plan.schema,
                "analyzer_id": ",".join(plan.expected_device_ids) or None,
                "operator_name": plan.operator or None,
                "config_hash": sha256_file(config_path) if config_path.exists() else None,
                "package_status": "initialization_planned",
                "package_blockers": [],
                "evidence_status": "indexed",
                "metadata": {
                    "schema": DB_BUNDLE_SCHEMA,
                    "expected_device_ids": list(plan.expected_device_ids),
                    "analyzer_identities": [dict(row) for row in plan.analyzer_identities],
                    "dry_run_only": plan.dry_run_only,
                    "no_real_com_executed_by_planner": True,
                    "database_role": "initialization_traceability_index",
                    "physical_contract": dict(plan.physical_contract),
                    "coefficient_policy": dict(plan.coefficient_policy),
                },
            }
        ],
        "devices": devices,
        "run_devices": run_devices,
        "standard_gases": [],
        "reference_certificates": [],
        "calibration_points": [],
        "sample_files": files,
        "qc_results": [],
        "coefficient_snapshots": coefficient_snapshots,
        "coefficient_candidates": [],
        "coefficient_write_events": write_events,
        "reports": [],
        "audit_events": [
            {
                "id": stable_id("audit", run_db_id, "formal_initialization_plan_created"),
                "run_db_id": run_db_id,
                "event_type": "formal_initialization_plan_created",
                "actor": plan.operator or None,
                "event_at": now,
                "payload": {
                    "run_id": plan.run_id,
                    "output_dir": str(root),
                    "expected_device_ids": list(plan.expected_device_ids),
                    "analyzer_identities": [dict(row) for row in plan.analyzer_identities],
                    "opens_com_ports": False,
                    "writes_coefficients": False,
                },
            },
            {
                "id": stable_id("audit", run_db_id, "formal_initialization_database_bundle_built"),
                "run_db_id": run_db_id,
                "event_type": "formal_initialization_database_bundle_built",
                "actor": plan.operator or None,
                "event_at": now,
                "payload": {
                    "artifact_count": len(files),
                    "coefficient_snapshot_count": len(coefficient_snapshots),
                    "write_event_count": len(write_events),
                    "opens_com_ports": False,
                    "writes_coefficients": False,
                },
            },
        ],
        "evidence_integrity_checks": _build_initialization_integrity_checks(
            run_db_id=run_db_id,
            plan=plan,
            files=files,
            coefficient_snapshots=coefficient_snapshots,
        ),
    }
    return {
        "schema": DB_BUNDLE_SCHEMA,
        "generated_at": now,
        "run_db_id": run_db_id,
        "run_id": plan.run_id,
        "tables": tables,
        "import_notes": [
            "Use gas_calibrator.storage.v1_5_evidence.repository.import_bundle to import this JSON into PostgreSQL.",
            "The planner itself did not open COM ports or write any SENCO group.",
            "Initialization evidence should be queried by analyzer device ID, not COM port or GA alias.",
        ],
    }


def write_formal_initialization_database_bundle(
    plan: InitializationPlan,
    outputs: Mapping[str, Path],
    output_dir: str | Path | None = None,
) -> Path:
    root = Path(output_dir or plan.output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    bundle = build_formal_initialization_database_bundle(plan, outputs)
    path = root / "v1_5_formal_initialization_db_bundle.json"
    path.write_text(json.dumps(bundle, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _normalize_selected_steps(selected_steps: str | Sequence[str] | None) -> tuple[str, ...]:
    if selected_steps is None:
        return ()
    if isinstance(selected_steps, str):
        parts = selected_steps.split(",")
    else:
        parts = list(selected_steps)
    return tuple(part.strip() for part in parts if str(part).strip())


def _step_is_selected(step: InitializationStep, selected_steps: tuple[str, ...]) -> bool:
    return not selected_steps or step.step_id in selected_steps


def _execution_block_reason(
    step: InitializationStep,
    *,
    allow_read_only_real_com: bool,
    allow_controlled_writes: bool,
) -> str:
    if not step.command:
        return "no_standalone_command"
    if step.controls_gas_route or step.controls_water_route:
        return "blocked_route_control_forbidden_in_initialization_executor"
    if step.writes_device_id:
        return "blocked_device_id_write_forbidden"
    if step.controls_pressure:
        return "blocked_pressure_control_not_owned_by_initialization_executor"
    if step.writes_coefficients:
        if not allow_controlled_writes:
            return "skipped_controlled_write_locked"
        if "<reviewer>" in step.command or "<approver>" in step.command:
            return "blocked_missing_reviewer_or_approver"
    if step.opens_com_ports and not step.writes_coefficients and not allow_read_only_real_com:
        return "skipped_read_only_real_com_locked"
    return ""


def _command_for_subprocess(command: Sequence[str]) -> list[str]:
    parts = [str(part) for part in command]
    if parts and parts[0].lower() == "python":
        parts[0] = sys.executable
    return parts


def _write_text(path: Path, text: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("" if text is None else str(text), encoding="utf-8")


def _execution_overall_status(results: Sequence[InitializationExecutionStepResult]) -> str:
    if any(row.status == "failed" for row in results):
        return "failed"
    if any(row.status.startswith("blocked") for row in results):
        return "blocked"
    if any(row.status.startswith("skipped") for row in results):
        return "partial"
    return "passed"


def write_formal_initialization_execution_report(
    report: InitializationExecutionReport,
    output_dir: str | Path,
) -> dict[str, Path]:
    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    json_path = root / "v1_5_formal_initialization_execution.json"
    csv_path = root / "v1_5_formal_initialization_execution.csv"
    json_path.write_text(json.dumps(report.to_json(), ensure_ascii=False, indent=2), encoding="utf-8")
    with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
        fieldnames = [
            "step_id",
            "title",
            "status",
            "reason",
            "returncode",
            "started_at",
            "ended_at",
            "stdout_path",
            "stderr_path",
            "opens_com_ports",
            "writes_coefficients",
            "controls_pressure",
            "controls_gas_route",
            "controls_water_route",
            "writes_device_id",
            "command_text",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in report.step_results:
            payload = row.to_json()
            payload["command_text"] = row.command_text
            writer.writerow({key: payload.get(key, "") for key in fieldnames})
    return {"execution_json": json_path, "execution_csv": csv_path}


def execute_formal_initialization_plan(
    plan: InitializationPlan,
    *,
    outputs: Mapping[str, Path] | None = None,
    allow_read_only_real_com: bool = False,
    allow_controlled_writes: bool = False,
    selected_steps: str | Sequence[str] | None = None,
    stop_on_failure: bool = True,
    command_runner: Any = subprocess.run,
) -> tuple[InitializationExecutionReport, dict[str, Path]]:
    """Execute the explicitly unlocked parts of a formal initialization plan.

    The executor is intentionally narrow. It never invents commands, never
    touches gas/water routes, never writes device IDs, and only runs real-COM or
    SENCO-write commands when the caller passes the matching explicit unlock.
    """

    _validate_command_gap(plan.analyzer_command_gap_s)
    selected = _normalize_selected_steps(selected_steps)
    root = Path(plan.output_dir).resolve()
    exec_dir = root / "formal_initialization_execution"
    exec_dir.mkdir(parents=True, exist_ok=True)
    step_results: list[InitializationExecutionStepResult] = []

    for step in plan.steps:
        if not _step_is_selected(step, selected):
            continue
        reason = _execution_block_reason(
            step,
            allow_read_only_real_com=allow_read_only_real_com,
            allow_controlled_writes=allow_controlled_writes,
        )
        if reason:
            if reason == "no_standalone_command":
                status = "not_applicable"
            else:
                status = "skipped" if reason.startswith("skipped") else "blocked"
            step_results.append(
                InitializationExecutionStepResult(
                    step_id=step.step_id,
                    title=step.title,
                    status=status,
                    reason=reason,
                    command=step.command,
                    command_text=_quote_command(step.command),
                    opens_com_ports=step.opens_com_ports,
                    writes_coefficients=step.writes_coefficients,
                    controls_pressure=step.controls_pressure,
                    controls_gas_route=step.controls_gas_route,
                    controls_water_route=step.controls_water_route,
                    writes_device_id=step.writes_device_id,
                )
            )
            if status == "blocked" and stop_on_failure:
                break
            continue

        started_at = _now_utc()
        stdout_path = exec_dir / f"{step.step_id}_stdout.log"
        stderr_path = exec_dir / f"{step.step_id}_stderr.log"
        completed = command_runner(
            _command_for_subprocess(step.command),
            capture_output=True,
            text=True,
        )
        ended_at = _now_utc()
        _write_text(stdout_path, getattr(completed, "stdout", ""))
        _write_text(stderr_path, getattr(completed, "stderr", ""))
        returncode = int(getattr(completed, "returncode", 1))
        status = "passed" if returncode == 0 else "failed"
        row = InitializationExecutionStepResult(
            step_id=step.step_id,
            title=step.title,
            status=status,
            reason="" if returncode == 0 else f"command_returncode_{returncode}",
            returncode=returncode,
            command=step.command,
            command_text=_quote_command(step.command),
            started_at=started_at,
            ended_at=ended_at,
            stdout_path=str(stdout_path),
            stderr_path=str(stderr_path),
            opens_com_ports=step.opens_com_ports,
            writes_coefficients=step.writes_coefficients,
            controls_pressure=step.controls_pressure,
            controls_gas_route=step.controls_gas_route,
            controls_water_route=step.controls_water_route,
            writes_device_id=step.writes_device_id,
        )
        step_results.append(row)
        if status == "failed" and stop_on_failure:
            break

    report = InitializationExecutionReport(
        schema="v1_5_formal_initialization_execution_v0",
        run_id=plan.run_id,
        created_at=_now_utc(),
        status=_execution_overall_status(step_results),
        allow_read_only_real_com=allow_read_only_real_com,
        allow_controlled_writes=allow_controlled_writes,
        stop_on_failure=stop_on_failure,
        selected_steps=selected,
        step_results=tuple(step_results),
    )
    execution_outputs = write_formal_initialization_execution_report(report, root)
    merged_outputs = dict(outputs or {})
    merged_outputs.update(execution_outputs)
    return report, merged_outputs


def _validate_command_gap(command_gap_s: float) -> float:
    gap = float(command_gap_s)
    if gap < MIN_ANALYZER_COMMAND_GAP_S:
        raise ValueError(
            f"Analyzer command gap must be >= {MIN_ANALYZER_COMMAND_GAP_S:.1f}s; "
            f"got {gap:.3f}s. Fragile sensors can lock up below this pace."
        )
    return gap


def _write_unlock_command(
    module: str,
    *,
    config_path: Path,
    output_dir: Path,
    enable_flag: str,
    confirmation_flag: str,
    confirmation: str,
    reviewer: str,
    approver: str,
    selector_flag: str,
    command_gap_s: float,
    extra_args: Sequence[object] = (),
) -> tuple[str, ...]:
    return _python_module(
        module,
        "--config",
        config_path,
        "--output-dir",
        output_dir,
        *extra_args,
        selector_flag,
        f"--{enable_flag}",
        "--operator-confirmation",
        confirmation,
        "--reviewer",
        reviewer or "<reviewer>",
        "--approver",
        approver or "<approver>",
        "--restore-command-gap-s",
        f"{command_gap_s:.1f}",
        "--readback-retry-delay-s",
        f"{command_gap_s:.1f}",
        "--coefficient-read-delay-s",
        f"{command_gap_s:.1f}",
    )


def _optional_path(value: str | Path | None) -> str:
    return "" if value is None or str(value).strip() == "" else str(value)


def _default_pressure_reference_json() -> Path:
    return Path(__file__).resolve().parents[3] / DEFAULT_COM22_PRESSURE_REFERENCE_JSON


def _resolve_pressure_reference_json(value: str | Path | None) -> Path:
    if _optional_path(value):
        return Path(value).resolve()
    return _default_pressure_reference_json().resolve()


def _pressure_completion_command(
    *,
    senco9_write_summary: str | Path | None,
    post_write_fit_summary: str | Path | None,
    pressure_reference_json: str | Path | None,
    output_dir: Path,
    pressure_reference_traceability: str | Path | None = None,
    old_getco_json: str | Path | None = None,
    selected_device_ids: Sequence[str] = (),
    known_limitations: Sequence[str] = (),
    max_abs_offset_kpa: float = 0.05,
    max_residual_hpa: float = 0.5,
    acceptance_policy_note: str = "",
    today: str = "",
) -> tuple[str, ...]:
    """Build the offline pressure-channel completion command when evidence is available."""

    if not (
        _optional_path(senco9_write_summary)
        and _optional_path(post_write_fit_summary)
        and _optional_path(pressure_reference_json)
    ):
        return ()

    parts: list[object] = [
        "python",
        "-m",
        "gas_calibrator.tools.export_v1_5_pressure_channel_completion",
        "--senco9-write-summary",
        senco9_write_summary,
        "--post-write-fit-summary",
        post_write_fit_summary,
        "--pressure-reference-json",
        pressure_reference_json,
        "--output-dir",
        output_dir,
        "--max-abs-offset-kpa",
        f"{float(max_abs_offset_kpa):.6g}",
        "--max-residual-hpa",
        f"{float(max_residual_hpa):.6g}",
    ]
    if _optional_path(pressure_reference_traceability):
        parts.extend(["--pressure-reference-traceability", pressure_reference_traceability])
    if _optional_path(old_getco_json):
        parts.extend(["--old-getco-json", old_getco_json])
    for device_id in selected_device_ids:
        normalized = _device_id(device_id)
        if normalized:
            parts.extend(["--device-id", normalized])
    for limitation in known_limitations:
        if str(limitation).strip():
            parts.extend(["--known-limitation", limitation])
    if acceptance_policy_note:
        parts.extend(["--acceptance-policy-note", acceptance_policy_note])
    if today:
        parts.extend(["--today", today])
    return _cmd(*parts)


def build_formal_initialization_plan(
    *,
    config_path: str | Path,
    output_dir: str | Path,
    run_id: str | None = None,
    operator: str = "",
    reviewer: str = "",
    approver: str = "",
    command_gap_s: float = DEFAULT_ANALYZER_COMMAND_GAP_S,
    senco78_policy: str = "neutralize_in_initialization",
    senco9_policy: str = "direct_pressure_calibration",
    average_filter: int = 49,
    ftd_hz: int = 1,
    pressure_completion_senco9_write_summary: str | Path | None = None,
    pressure_completion_post_write_fit_summary: str | Path | None = None,
    pressure_completion_reference_json: str | Path | None = None,
    pressure_completion_reference_traceability: str | Path | None = None,
    pressure_completion_old_getco_json: str | Path | None = None,
    pressure_completion_output_dir: str | Path | None = None,
    pressure_completion_device_ids: Sequence[str] = (),
    pressure_completion_known_limitations: Sequence[str] = (),
    pressure_completion_max_abs_offset_kpa: float = 0.05,
    pressure_completion_max_residual_hpa: float = 0.5,
    pressure_completion_policy_note: str = "",
    pressure_completion_today: str = "",
    pressure_reference_json: str | Path | None = None,
) -> InitializationPlan:
    gap = _validate_command_gap(command_gap_s)
    config = Path(config_path).resolve()
    root = Path(output_dir).resolve()
    now = datetime.now().isoformat(timespec="seconds")
    rid = run_id or f"v1_5_formal_initialization_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    analyzer_identities = _analyzer_identities(config)
    expected_ids = _expected_device_ids_from_identities(analyzer_identities)
    requested_senco78_policy = str(senco78_policy or "").strip() or "neutralize_in_initialization"
    senco78_policy = "neutralize_in_initialization"

    getco_dir = root / "coefficient_epoch_0_getco_snapshot"
    sn_identity_dir = root / "sn_identity_initialization"
    aux_dir = root / "auxiliary_senco56789_neutralization"
    route_readiness_dir = root / "formal_route_readiness"
    pressure_completion_dir = Path(pressure_completion_output_dir).resolve() if pressure_completion_output_dir else (
        root / "pressure_channel_completion"
    )
    pressure_completion_old_getco = (
        pressure_completion_old_getco_json
        if pressure_completion_old_getco_json is not None
        else getco_dir / "old_component_coefficients_snapshot.json"
    )
    pressure_reference = _resolve_pressure_reference_json(pressure_reference_json)
    pressure_senco9_preflight_dir = root / "pressure_senco9_no_write_preflight"

    getco_cmd = _python_module(
        "gas_calibrator.tools.probe_v1_5_getco_component_snapshot",
        "--config",
        config,
        "--output-dir",
        getco_dir,
        "--groups",
        "1,2,3,4,5,6,7,8,9",
        "--response-timeout-s",
        "2.5",
        "--command-gap-s",
        f"{gap:.1f}",
        "--attempts-per-group",
        "3",
        "--pre-drain-s",
        "0.5",
        "--identity-timeout-s",
        "5.0",
        "--include-legacy",
        "--allow-runtime-identity-rebind",
    )
    sn_identity_cmd = _python_module(
        "gas_calibrator.tools.run_v1_5_sn_identity_initialization",
        "--config",
        config,
        "--output-dir",
        sn_identity_dir,
        "--run-id",
        f"{rid}_sn_identity",
    )
    readiness_cmd = _python_module(
        "gas_calibrator.tools.export_v1_5_initialization_readiness",
        "--run-dir",
        root,
        "--config",
        config,
        "--getco-snapshot-dir",
        getco_dir,
        "--aux-neutralization-dir",
        aux_dir,
        "--output-dir",
        root,
    )
    route_readiness_cmd = _python_module(
        "gas_calibrator.tools.run_v1_5_formal_route_readiness_probe",
        "--config",
        config,
        "--output-dir",
        route_readiness_dir,
    )
    pressure_completion_cmd = _pressure_completion_command(
        senco9_write_summary=pressure_completion_senco9_write_summary,
        post_write_fit_summary=pressure_completion_post_write_fit_summary,
        pressure_reference_json=pressure_completion_reference_json,
        pressure_reference_traceability=pressure_completion_reference_traceability,
        old_getco_json=pressure_completion_old_getco,
        output_dir=pressure_completion_dir,
        selected_device_ids=pressure_completion_device_ids,
        known_limitations=pressure_completion_known_limitations,
        max_abs_offset_kpa=pressure_completion_max_abs_offset_kpa,
        max_residual_hpa=pressure_completion_max_residual_hpa,
        acceptance_policy_note=pressure_completion_policy_note,
        today=pressure_completion_today,
    )
    pressure_senco9_preflight_cmd = _python_module(
        "gas_calibrator.tools.export_v1_5_pressure_senco9_no_write_preflight",
        "--config",
        config,
        "--pressure-reference-json",
        pressure_reference,
        "--output-dir",
        pressure_senco9_preflight_dir,
        "--pressure-points",
        "ambient,1100,1000,900,800,700,600,500",
        "--count",
        "12",
        "--interval-s",
        "1.0",
    )

    steps = (
        InitializationStep(
            step_id="sn_identity_initialization_plan",
            title="Plan first-discovery SN/device_code binding before epoch-0 GETCO",
            phase="identity_precheck",
            command=sn_identity_cmd,
            execution_mode="offline_sn_identity_plan_only",
            opens_com_ports=False,
            writes_device_id=False,
            required_inputs=(str(config),),
            expected_outputs=(
                _artifact(sn_identity_dir, "00_plan", "v1_5_sn_identity_initialization_plan.json"),
                _artifact(sn_identity_dir, "v1_5_sn_identity_initialization_result.json"),
            ),
            physical_meaning=(
                "New analyzers may not have a stable 8-digit SN yet. This step allocates or confirms "
                "the numeric SN/device_code plan from the device protocol ID before GETCO evidence and "
                "database indexing. The generated command is dry-run only; actual SN writes remain in "
                "the dedicated SN tool and require its exact authorization phrase."
            ),
            safety_notes=(
                "No COM is opened unless the dedicated SN tool is run separately with --execute.",
                "SN format remains 8 numeric digits: hardware version, YYMM, and sequence.",
                "The formal initializer never writes device ID or SN by itself.",
            ),
            gate="required_before_identity_database_indexing",
        ),
        InitializationStep(
            step_id="identity_and_getco_epoch0_snapshot",
            title="Read analyzer identity and GETCO1-9 epoch-0 snapshot",
            phase="evidence_freeze",
            command=getco_cmd,
            execution_mode="read_only_real_com_when_operator_runs_command",
            opens_com_ports=True,
            required_inputs=(str(config),),
            expected_outputs=(
                _artifact(getco_dir, "old_component_coefficients_snapshot.json"),
                _artifact(getco_dir, "getco_component_snapshot_identity.csv"),
                _artifact(getco_dir, "runtime_identity_bound_config.json"),
            ),
            physical_meaning=(
                "Freeze the analyzer identity and all coefficient groups before any correction. "
                "Device identity must come from the analyzer MODE2 ID, not COM port or GA alias."
            ),
            safety_notes=(
                "No SENCO/ID writes are performed.",
                f"Analyzer command gap is forced to {gap:.1f}s.",
            ),
            gate="required_before_any_coefficient_or_route_step",
        ),
        InitializationStep(
            step_id="senco5_neutralization_gate",
            title="Neutralize CO2 final affine trim SENCO5 after snapshot",
            phase="auxiliary_coefficient_control",
            command=_write_unlock_command(
                "gas_calibrator.tools.run_v1_5_co2_senco5_neutral_controlled_write",
                config_path=config,
                output_dir=aux_dir,
                enable_flag="enable-senco5-write",
                confirmation_flag="operator-confirmation",
                confirmation="WRITE_SENCO5_NEUTRAL_V1_5_CO2_LINEAR_TRIM",
                reviewer=reviewer,
                approver=approver,
                selector_flag="--write-all-nonneutral",
                command_gap_s=gap,
            ),
            execution_mode="blocked_pending_explicit_write_unlock",
            opens_com_ports=True,
            writes_coefficients=True,
            required_inputs=(_artifact(getco_dir, "old_component_coefficients_snapshot.json"),),
            expected_outputs=(_artifact(aux_dir, "senco5_neutral_write_events.csv"),),
            physical_meaning=(
                "SENCO5 is an output-layer CO2 affine trim. Leaving an old trim active makes "
                "final displayed concentration differ from the ratio-based main fit."
            ),
            safety_notes=("Uses CLEARSENCO5 and readback verification; does not write SENCO1/SENCO3.",),
            gate="controlled_write_required_or_explicit_modeling_required",
        ),
        InitializationStep(
            step_id="senco6_neutralization_gate",
            title="Neutralize H2O final affine trim SENCO6 after snapshot",
            phase="auxiliary_coefficient_control",
            command=_write_unlock_command(
                "gas_calibrator.tools.run_v1_5_h2o_senco6_neutral_controlled_write",
                config_path=config,
                output_dir=aux_dir,
                enable_flag="enable-senco6-write",
                confirmation_flag="operator-confirmation",
                confirmation="WRITE_SENCO6_NEUTRAL_V1_5_H2O_LINEAR_TRIM",
                reviewer=reviewer,
                approver=approver,
                selector_flag="--write-all-nonneutral",
                command_gap_s=gap,
            ),
            execution_mode="blocked_pending_explicit_write_unlock",
            opens_com_ports=True,
            writes_coefficients=True,
            required_inputs=(_artifact(getco_dir, "old_component_coefficients_snapshot.json"),),
            expected_outputs=(_artifact(aux_dir, "senco6_neutral_write_events.csv"),),
            physical_meaning=(
                "SENCO6 is an output-layer H2O affine trim. It must be neutralized or explicitly "
                "modeled before H2O main fitting."
            ),
            safety_notes=("Uses CLEARSENCO6 and readback verification; does not write SENCO2/SENCO4.",),
            gate="controlled_write_required_or_explicit_modeling_required",
        ),
        InitializationStep(
            step_id="senco78_neutralization_gate",
            title="Neutralize temperature input coefficients SENCO7/SENCO8 after snapshot",
            phase="auxiliary_coefficient_control",
            command=_write_unlock_command(
                "gas_calibrator.tools.run_v1_5_temperature_senco78_neutral_controlled_write",
                config_path=config,
                output_dir=aux_dir,
                enable_flag="enable-senco78-write",
                confirmation_flag="operator-confirmation",
                confirmation="WRITE_SENCO78_NEUTRAL_V1_5_TEMPERATURE_INPUTS",
                reviewer=reviewer,
                approver=approver,
                selector_flag="--write-all-nonneutral",
                command_gap_s=gap,
            ),
            execution_mode="blocked_pending_explicit_write_unlock",
            opens_com_ports=True,
            writes_coefficients=True,
            required_inputs=(_artifact(getco_dir, "old_component_coefficients_snapshot.json"),),
            expected_outputs=(
                _artifact(aux_dir, "senco78_neutral_write_events.csv"),
                _artifact(aux_dir, "senco78_neutral_write_meta.json"),
            ),
            physical_meaning=(
                "SENCO7/SENCO8 affect analyzer temperature inputs. V1.5 no longer performs temperature "
                "calibration for either classic or new-algorithm analyzers; initialization restores both "
                "temperature coefficient groups to neutral after the epoch-0 GETCO backup so later CO2/H2O "
                "fits use the analyzer runtime temperature directly."
            ),
            safety_notes=(
                "Temperature calibration and single-point temperature repair are intentionally disabled.",
                "Uses explicit SENCO7/SENCO8 neutral payloads and readback verification.",
                f"Analyzer command gap is forced to {gap:.1f}s.",
            ),
            gate="controlled_write_required_before_open_flow_sampling",
        ),
        InitializationStep(
            step_id="mode2_1hz_filter_startup_contract",
            title="Set analyzer acquisition contract: MODE2, 1 Hz active stream, AVERAGE1/2 filter",
            phase="communication_conditioning",
            execution_mode="performed_by_sampling_runner_after_identity_binding",
            opens_com_ports=True,
            expected_outputs=("mode2_frame_evidence", "active_stream_1hz_evidence", "average_filter_evidence"),
            physical_meaning=(
                "Calibration samples must be synchronized, filtered, and interpretable. "
                "MODE2 provides normal plus factory evidence; 1 Hz avoids fragile serial over-commanding."
            ),
            safety_notes=(
                f"FTD target: {int(ftd_hz)} Hz.",
                f"AVERAGE1/2 target: {int(average_filter)}.",
                f"All analyzer commands must observe >= {gap:.1f}s spacing.",
            ),
            gate="required_before_open_flow_sampling",
        ),
        InitializationStep(
            step_id="senco9_pressure_policy_gate",
            title="Use direct pressure calibration instead of pressure quick-check acceptance",
            phase="input_quantity_control",
            command=_write_unlock_command(
                "gas_calibrator.tools.run_v1_5_pressure_senco9_clear_controlled_write",
                config_path=config,
                output_dir=aux_dir,
                enable_flag="enable-senco9-clear",
                confirmation_flag="operator-confirmation",
                confirmation="CLEAR_SENCO9_V1_5_PRESSURE_RECOVERY_ONLY",
                reviewer=reviewer,
                approver=approver,
                selector_flag="--clear-all-nonneutral",
                command_gap_s=gap,
            ),
            execution_mode="clear_only_when_pressure_channel_is_fixed_or_prior_s9_is_untrustworthy",
            opens_com_ports=True,
            writes_coefficients=True,
            required_inputs=(_artifact(getco_dir, "old_component_coefficients_snapshot.json"),),
            expected_outputs=(_artifact(aux_dir, "senco9_clear_write_events.csv"),),
            physical_meaning=(
                "SENCO9 affects the pressure input used by the analyzer algorithm. Production should run "
                "multi-pressure calibration and verification, not treat a single quick check as acceptance."
            ),
            safety_notes=(
                f"Policy: {senco9_policy}.",
                "CLEARSENCO9 is a recovery gate when pressure is clamped/fixed; the formal next step is multi-point pressure calibration.",
            ),
            gate="pressure_calibration_required_before_component_sampling",
        ),
        InitializationStep(
            step_id="pressure_senco9_no_write_preflight",
            title="Generate traceable no-write pressure/SENCO9 multi-point collection runbook",
            phase="input_quantity_control",
            command=pressure_senco9_preflight_cmd,
            execution_mode="offline_preflight_generates_no_write_pressure_collection_commands",
            required_inputs=(str(config), str(pressure_reference)),
            expected_outputs=(
                _artifact(pressure_senco9_preflight_dir, "pressure_senco9_no_write_preflight.xlsx"),
                _artifact(pressure_senco9_preflight_dir, "pressure_senco9_no_write_runbook.md"),
                _artifact(pressure_senco9_preflight_dir, "command_plan.csv"),
            ),
            physical_meaning=(
                "Plan the direct multi-pressure SENCO9 no-write acquisition with COM22 traceability before any "
                "pressure write decision. This freezes the exact pressure points, sampling cadence, and mature "
                "V1.5 pressure reference certificate path used by validate_pressure_only and the offline fit review."
            ),
            safety_notes=(
                "This step is offline and does not open COM or control PACE.",
                "The generated collection command is still no-write; SENCO9 write requires the dedicated controlled writer.",
            ),
            gate="required_before_pressure_collection_or_pressure_write_review",
        ),
        InitializationStep(
            step_id="pressure_channel_completion_audit",
            title="Export pressure-channel completion evidence after SENCO9 write and verification",
            phase="input_quantity_control",
            command=pressure_completion_cmd,
            execution_mode="offline_audit_when_pressure_write_and_reverify_evidence_exist",
            required_inputs=(
                "senco9_write_summary.csv",
                "post-write pressure_fit_summary.csv",
                "COM22 pressure reference certificate JSON",
                _artifact(getco_dir, "old_component_coefficients_snapshot.json"),
            ),
            expected_outputs=(
                _artifact(pressure_completion_dir, "pressure_channel_completion_summary.csv"),
                _artifact(pressure_completion_dir, "pressure_channel_device_readiness.csv"),
                _artifact(pressure_completion_dir, "pressure_channel_completion_report.md"),
            ),
            physical_meaning=(
                "After SENCO9 is written and independently verified, this offline gate freezes the traceable "
                "pressure-input evidence that readiness and later CO2/H2O reports consume. It does not "
                "control PACE, open COM, or write coefficients."
            ),
            safety_notes=(
                "Skipped as not_applicable until SENCO9 write and post-write pressure verification evidence paths are supplied.",
                "This is the bridge from pressure-channel calibration to component open-flow readiness.",
            ),
            gate="required_before_open_flow_sampling_when_pressure_channel_was_repaired",
        ),
        InitializationStep(
            step_id="formal_route_readiness_probe",
            title="Verify formal N2/CO2/H2O route readiness before chamber soak",
            phase="route_precheck",
            command=route_readiness_cmd,
            execution_mode="read_only_route_hardware_probe_when_operator_runs_command",
            opens_com_ports=True,
            required_inputs=(str(config),),
            expected_outputs=(_artifact(route_readiness_dir, "formal_route_readiness.json"),),
            physical_meaning=(
                "Before any temperature-chamber soak, prove that all N2/CO2/H2O logical valves "
                "are present in relay_map, relay and relay_8 ports are readable/writable, the "
                "dewpoint meter is online, and the N2 prepurge source valve can open/close when "
                "prepurge is enabled. This prevents wasting hours before discovering a missing "
                "route or offline reference device."
            ),
            safety_notes=(
                "Does not write SENCO or device ID.",
                "Does not open formal CO2/H2O source valves or run gas/water sampling routes.",
                "Only the N2 source valve is toggled, and only when N2 prepurge is enabled.",
            ),
            gate="required_before_open_flow_sampling",
        ),
        InitializationStep(
            step_id="initialization_readiness_audit",
            title="Export initialization readiness after evidence and gates",
            phase="readiness_audit",
            command=readiness_cmd,
            execution_mode="offline_audit_after_selected_initialization_steps",
            expected_outputs=(
                _artifact(root, "v1_5_initialization_readiness.json"),
                _artifact(root, "v1_5_initialization_readiness.md"),
            ),
            physical_meaning=(
                "Summarize whether device identity, coefficient epoch, S5/S6/S7/S8/S9 handling, "
                "and startup settings are ready before open-flow CO2/H2O sampling."
            ),
            safety_notes=("Offline audit only; it does not open COM, write coefficients, or control routes.",),
            gate="required_before_open_flow_sampling",
        ),
    )

    safety_contract = {
        "planner_opens_com_ports": False,
        "planner_writes_coefficients": False,
        "planner_controls_pressure": False,
        "planner_controls_gas_route": False,
        "planner_controls_water_route": False,
        "does_not_modify_run_app": True,
        "does_not_write_device_id": True,
        "minimum_analyzer_command_gap_s": MIN_ANALYZER_COMMAND_GAP_S,
        "configured_analyzer_command_gap_s": gap,
        "real_execution_requires_operator_to_run_gated_commands": True,
    }
    coefficient_policy = {
        "epoch0_snapshot_required": "GETCO1-9 before any coefficient change",
        "senco5": "neutralize_or_explicitly_model_before_co2_main_fit",
        "senco6": "neutralize_or_explicitly_model_before_h2o_main_fit",
        "senco78": senco78_policy,
        "senco78_requested_policy": requested_senco78_policy,
        "senco78_epoch0_snapshot": "old_GETCO7_GETCO8_required_before_any_temperature_write",
        "senco78_neutralization": "required_after_epoch0_for_classic_and_new_algorithm_analyzers",
        "senco78_temperature_calibration": "disabled; no single-point or multi-temperature SENCO7/SENCO8 calibration",
        "senco78_if_not_neutralized": "blocked_before_open_flow_sampling",
        "senco9": senco9_policy,
        "device_id": "never_rewrite_identity_during_initialization",
    }
    physical_contract = {
        "identity_key": "analyzer_internal_mode2_id",
        "pressure_before_components": "direct_multi_point_pressure_calibration_and_verification",
        "temperature_before_components": "SENCO7/SENCO8_neutralized; use analyzer runtime chamber/cell temperature directly",
        "co2_route": "open_flow_clean_dry_gas_after_initialization",
        "h2o_route": "open_flow_humidity_route_after_temperature_and_pressure_inputs_are_trusted",
        "s5_s6_reason": "output_layer_trims_can_hide_main_fit_errors_if_not_neutralized_or_modeled",
        "s7_s8_reason": "temperature_input_errors_pollute_R_T_P_component_model",
        "s7_s8_old_coefficient_handling": (
            "SENCO7_8_must_be_neutralized_after_epoch0_before_sampling; no temperature calibration path is used"
        ),
        "s7_s8_subzero_failure_guard": "neutralization removes old temperature-coefficient projection risk before sampling",
        "s9_reason": "pressure_input_errors_pollute_R_T_P_component_model",
        "route_readiness_before_components": (
            "relay_map, relay/relay_8, dewpoint meter, and N2 prepurge source must be proven in initialization "
            "before chamber soak or open-flow sampling."
        ),
        "sn_identity_before_getco": (
            "first-discovery SN/device_code allocation is planned before epoch-0 GETCO and database indexing; "
            "actual SN writes require dedicated SN authorization and are not performed by this planner"
        ),
    }
    warnings = (
        "First-discovery SN/device_code binding is planned before GETCO; real SN writes require the dedicated SN authorization phrase.",
        "This tool writes a plan only; controlled real writes still require the dedicated writer confirmations.",
        "SENCO7/SENCO8 must be neutralized during initialization; do not run temperature calibration or single-point repair.",
        "Production pressure handling should proceed to direct multi-point SENCO9 calibration/review before component sampling.",
        "Formal N2/CO2/H2O route readiness must pass in initialization before temperature chamber soak starts.",
    )
    return InitializationPlan(
        schema=SCHEMA,
        run_id=rid,
        created_at=now,
        config_path=str(config),
        output_dir=str(root),
        dry_run_only=True,
        analyzer_command_gap_s=gap,
        operator=operator,
        reviewer=reviewer,
        approver=approver,
        expected_device_ids=expected_ids,
        analyzer_identities=analyzer_identities,
        safety_contract=safety_contract,
        physical_contract=physical_contract,
        coefficient_policy=coefficient_policy,
        tool_ownership=INITIALIZATION_TOOL_OWNERSHIP,
        steps=steps,
        warnings=warnings,
    )


def _render_markdown(plan: InitializationPlan) -> str:
    lines = [
        "# V1.5 formal initialization plan",
        "",
        f"- run_id: `{plan.run_id}`",
        f"- created_at: `{plan.created_at}`",
        f"- dry_run_only: `{plan.dry_run_only}`",
        f"- analyzer_command_gap_s: `{plan.analyzer_command_gap_s:.1f}`",
        f"- expected_device_ids: `{', '.join(plan.expected_device_ids) or 'unknown_until_live_probe'}`",
        "",
        "## Physical contract",
    ]
    for key, value in plan.physical_contract.items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(["", "## Coefficient policy"])
    for key, value in plan.coefficient_policy.items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(["", "## Tool ownership"])
    for key, item in plan.tool_ownership.items():
        lines.extend(
            [
                f"- {key}:",
                f"  - tool: `{item.get('tool', '')}`",
                f"  - role: `{item.get('role', '')}`",
                f"  - allowed_use: {item.get('allowed_use', '')}",
                f"  - forbidden_use: {item.get('forbidden_use', '')}",
            ]
        )
    lines.extend(["", "## Steps"])
    for step in plan.steps:
        lines.extend(
            [
                "",
                f"### {step.step_id}",
                f"- title: {step.title}",
                f"- phase: `{step.phase}`",
                f"- execution_mode: `{step.execution_mode}`",
                f"- gate: `{step.gate}`",
                f"- opens_com_ports: `{step.opens_com_ports}`",
                f"- writes_coefficients: `{step.writes_coefficients}`",
                f"- controls_pressure: `{step.controls_pressure}`",
                f"- controls_gas_route: `{step.controls_gas_route}`",
                f"- controls_water_route: `{step.controls_water_route}`",
                f"- physical_meaning: {step.physical_meaning}",
            ]
        )
        if step.command:
            lines.extend(["", "```powershell", _quote_command(step.command), "```"])
        if step.safety_notes:
            lines.append("- safety_notes:")
            for note in step.safety_notes:
                lines.append(f"  - {note}")
    lines.extend(["", "## Warnings"])
    for warning in plan.warnings:
        lines.append(f"- {warning}")
    lines.append("")
    return "\n".join(lines)


def _render_powershell(plan: InitializationPlan) -> str:
    lines = [
        "# V1.5 formal initialization command plan.",
        "# Review each gate before running. Controlled writes require explicit confirmations.",
        "# This generated file is not executed by the planner.",
        "",
    ]
    for step in plan.steps:
        lines.append(f"# {step.step_id}: {step.title}")
        if step.command and step.writes_coefficients:
            lines.append("# CONTROLLED WRITE GATE. Do not run until reviewer/approver fields are filled and authorization is recorded.")
            lines.append(f"# {_quote_command(step.command)}")
        elif step.command:
            lines.append(_quote_command(step.command))
        else:
            lines.append(f"# No standalone command. Evidence is produced by the downstream V1.5 runner.")
        lines.append("")
    return "\n".join(lines)


def write_formal_initialization_plan(plan: InitializationPlan, output_dir: str | Path | None = None) -> dict[str, Path]:
    root = Path(output_dir or plan.output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    json_path = root / "v1_5_formal_initialization_plan.json"
    md_path = root / "v1_5_formal_initialization_plan.md"
    ps1_path = root / "v1_5_formal_initialization_commands.ps1"
    contract_path = root / "v1_5_formal_initialization_contract.json"

    payload = plan.to_json()
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_render_markdown(plan), encoding="utf-8")
    ps1_path.write_text(_render_powershell(plan), encoding="utf-8")
    contract_path.write_text(
        json.dumps(
            {
                "schema": "v1_5_formal_initialization_contract_v0",
                "run_id": plan.run_id,
                "status": "planned",
                "safety_contract": plan.safety_contract,
                "physical_contract": plan.physical_contract,
                "coefficient_policy": plan.coefficient_policy,
                "tool_ownership": plan.tool_ownership,
                "required_before_open_flow": [
                    "sn_identity_initialization_plan",
                    "identity_and_getco_epoch0_snapshot",
                    "senco5_neutralization_gate",
                    "senco6_neutralization_gate",
                    "senco78_neutralization_gate",
                    "senco9_pressure_policy_gate",
                    "pressure_senco9_no_write_preflight",
                    "pressure_channel_completion_audit",
                    "mode2_1hz_filter_startup_contract",
                    "formal_route_readiness_probe",
                    "initialization_readiness_audit",
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    outputs = {"json": json_path, "markdown": md_path, "powershell": ps1_path, "contract_json": contract_path}
    outputs["database_bundle_json"] = write_formal_initialization_database_bundle(plan, outputs, root)
    return outputs


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate a V1.5 formal initialization plan and safety contract.")
    parser.add_argument("--config", required=True, help="V1.5 runtime config JSON.")
    parser.add_argument("--output-dir", required=True, help="Output directory for initialization plan artifacts.")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--operator", default="")
    parser.add_argument("--reviewer", default="")
    parser.add_argument("--approver", default="")
    parser.add_argument("--command-gap-s", type=float, default=DEFAULT_ANALYZER_COMMAND_GAP_S)
    parser.add_argument(
        "--senco78-policy",
        choices=(
            "neutralize_in_initialization",
            "review_then_single_point_repair_if_abnormal",
            "review_then_neutralize_if_abnormal",
            "neutralize_before_single_point_temperature",
            "review_only",
        ),
        default="neutralize_in_initialization",
    )
    parser.add_argument(
        "--senco9-policy",
        choices=("direct_pressure_calibration", "clear_if_fixed_then_direct_pressure_calibration"),
        default="direct_pressure_calibration",
    )
    parser.add_argument("--average-filter", type=int, default=49)
    parser.add_argument("--ftd-hz", type=int, default=1)
    parser.add_argument(
        "--pressure-completion-senco9-write-summary",
        default=None,
        help="Existing senco9_write_summary.csv to convert into pressure-channel completion evidence.",
    )
    parser.add_argument(
        "--pressure-completion-post-write-fit-summary",
        default=None,
        help="Existing post-write pressure_fit_summary.csv used to verify SENCO9.",
    )
    parser.add_argument(
        "--pressure-completion-reference-json",
        default=None,
        help="COM22 pressure reference certificate JSON for pressure-channel completion.",
    )
    parser.add_argument("--pressure-completion-reference-traceability", default=None)
    parser.add_argument("--pressure-completion-old-getco-json", default=None)
    parser.add_argument("--pressure-completion-output-dir", default=None)
    parser.add_argument("--pressure-completion-device-id", action="append", default=[])
    parser.add_argument("--pressure-completion-known-limitation", action="append", default=[])
    parser.add_argument("--pressure-completion-max-abs-offset-kpa", type=float, default=0.05)
    parser.add_argument("--pressure-completion-max-residual-hpa", type=float, default=0.5)
    parser.add_argument("--pressure-completion-policy-note", default="")
    parser.add_argument("--pressure-completion-today", default="")
    parser.add_argument(
        "--pressure-reference-json",
        default=None,
        help=(
            "COM22 pressure reference certificate JSON for pressure/SENCO9 no-write preflight. "
            f"Defaults to {DEFAULT_COM22_PRESSURE_REFERENCE_JSON}."
        ),
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Execute explicitly unlocked initialization steps after writing the plan.",
    )
    parser.add_argument(
        "--execute-read-only-real-com",
        action="store_true",
        help="Allow read-only real-COM identity/GETCO snapshot steps.",
    )
    parser.add_argument(
        "--execute-controlled-writes",
        action="store_true",
        help="Allow controlled S5/S6/S7/S8/S9 writer steps. Reviewer and approver are required.",
    )
    parser.add_argument(
        "--execute-steps",
        default="",
        help="Optional comma-separated step IDs to execute; default executes all allowed steps.",
    )
    parser.add_argument(
        "--continue-on-failure",
        action="store_true",
        help="Continue executing later selected steps after a failed or blocked step.",
    )
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    try:
        plan = build_formal_initialization_plan(
            config_path=args.config,
            output_dir=args.output_dir,
            run_id=args.run_id,
            operator=args.operator,
            reviewer=args.reviewer,
            approver=args.approver,
            command_gap_s=args.command_gap_s,
            senco78_policy=args.senco78_policy,
            senco9_policy=args.senco9_policy,
            average_filter=args.average_filter,
            ftd_hz=args.ftd_hz,
            pressure_completion_senco9_write_summary=args.pressure_completion_senco9_write_summary,
            pressure_completion_post_write_fit_summary=args.pressure_completion_post_write_fit_summary,
            pressure_completion_reference_json=args.pressure_completion_reference_json,
            pressure_completion_reference_traceability=args.pressure_completion_reference_traceability,
            pressure_completion_old_getco_json=args.pressure_completion_old_getco_json,
            pressure_completion_output_dir=args.pressure_completion_output_dir,
            pressure_completion_device_ids=tuple(args.pressure_completion_device_id or ()),
            pressure_completion_known_limitations=tuple(args.pressure_completion_known_limitation or ()),
            pressure_completion_max_abs_offset_kpa=args.pressure_completion_max_abs_offset_kpa,
            pressure_completion_max_residual_hpa=args.pressure_completion_max_residual_hpa,
            pressure_completion_policy_note=args.pressure_completion_policy_note,
            pressure_completion_today=args.pressure_completion_today,
            pressure_reference_json=args.pressure_reference_json,
        )
        outputs = write_formal_initialization_plan(plan, args.output_dir)
        if args.execute:
            report, outputs = execute_formal_initialization_plan(
                plan,
                outputs=outputs,
                allow_read_only_real_com=args.execute_read_only_real_com,
                allow_controlled_writes=args.execute_controlled_writes,
                selected_steps=args.execute_steps,
                stop_on_failure=not args.continue_on_failure,
            )
            outputs["database_bundle_json"] = write_formal_initialization_database_bundle(plan, outputs, args.output_dir)
    except Exception as exc:
        print(f"V1.5 formal initialization plan failed: {exc}", file=sys.stderr, flush=True)
        return 1
    print(json.dumps({key: str(value.resolve()) for key, value in outputs.items()}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
