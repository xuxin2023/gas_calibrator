"""Offline V1.5 pre-gas readiness review.

This model summarizes the evidence required between formal initialization and
the first CO2/H2O open-flow route. It is a sidecar-only review: it never opens
COM ports, connects to PostgreSQL, controls pressure/routes, or writes analyzer
coefficients.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "v1_5_pre_gas_readiness_v1"
READINESS_JSON = "v1_5_initialization_readiness.json"
INITIALIZATION_CONTRACT_JSON = "v1_5_formal_initialization_contract.json"
INITIALIZATION_DB_SIDECAR_JSON = "v1_5_initialization_database_sidecar.json"


@dataclass(frozen=True)
class PreGasReadinessCheck:
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


def _load_json(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return {}
    source = Path(path)
    if not source.exists() or not source.is_file():
        return {}
    payload = json.loads(source.read_text(encoding="utf-8-sig"))
    return payload if isinstance(payload, dict) else {}


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
            writer.writerow({key: row.get(key, "") for key in keys})


def _path_text(path: Path | None) -> str:
    return str(path.resolve()) if path and path.exists() else ""


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


def _candidate(path: str | Path | None, fallback: Path) -> Path:
    if path:
        return Path(path).resolve()
    return fallback.resolve()


def _check_by_name(model: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    checks = model.get("checks")
    if not isinstance(checks, list):
        return {}
    for row in checks:
        if isinstance(row, Mapping) and row.get("check") == name:
            return row
    return {}


def _readiness_contract(readiness: Mapping[str, Any]) -> Mapping[str, Any]:
    contract = readiness.get("initialization_contract")
    return contract if isinstance(contract, Mapping) else {}


def _contract_database_ok(contract: Mapping[str, Any], sidecar: Mapping[str, Any]) -> tuple[bool, tuple[str, ...]]:
    database = contract.get("database") if isinstance(contract.get("database"), Mapping) else {}
    reasons: list[str] = []
    if database.get("backend") != "postgresql":
        reasons.append("database_backend_not_postgresql")
    if _as_int(database.get("required_major")) != 18:
        reasons.append("postgresql18_required_major_missing")
    if database.get("preflight_mutates_database") is not False:
        reasons.append("database_preflight_must_be_no_write")
    if sidecar and sidecar.get("sidecar_only") is not True:
        reasons.append("initialization_database_sidecar_not_sidecar_only")
    return not reasons, tuple(reasons)


def _contract_identity_ok(contract: Mapping[str, Any]) -> tuple[bool, tuple[str, ...]]:
    identity = contract.get("identity") if isinstance(contract.get("identity"), Mapping) else {}
    reasons: list[str] = []
    if identity.get("primary_key") != "sn_code/device_code":
        reasons.append("sn_code_device_code_not_primary_identity")
    if identity.get("compatibility_alias") != "protocol_device_id":
        reasons.append("protocol_device_id_not_declared_as_compatibility_alias")
    if "COM/GA" not in str(identity.get("transport_not_identity") or ""):
        reasons.append("transport_mapping_not_separated_from_identity")
    return not reasons, tuple(reasons)


def _contract_runtime_temperature_check_ok(contract: Mapping[str, Any]) -> tuple[bool, tuple[str, ...]]:
    runtime = contract.get("runtime") if isinstance(contract.get("runtime"), Mapping) else {}
    temperature = contract.get("temperature") if isinstance(contract.get("temperature"), Mapping) else {}
    check_monitor = contract.get("check_monitor") if isinstance(contract.get("check_monitor"), Mapping) else {}
    reasons: list[str] = []
    if _as_int(runtime.get("mode")) != 2:
        reasons.append("mode2_contract_missing")
    if _as_int(runtime.get("ftd_hz")) != 1:
        reasons.append("ftd_hz_1_contract_missing")
    if _as_float(runtime.get("minimum_command_gap_s")) < 1.0:
        reasons.append("minimum_command_gap_below_1s")
    if temperature.get("temperature_calibration") != "disabled":
        reasons.append("temperature_calibration_not_disabled")
    if "neutralize" not in str(temperature.get("senco7_senco8_policy") or ""):
        reasons.append("senco7_senco8_neutral_policy_missing")
    if check_monitor.get("command") != "CHECK,YGAS,FFF":
        reasons.append("check_monitor_command_missing")
    if check_monitor.get("read_only") is not True:
        reasons.append("check_monitor_not_read_only")
    if _as_float(check_monitor.get("minimum_command_gap_s")) < 1.0:
        reasons.append("check_monitor_gap_below_1s")
    return not reasons, tuple(reasons)


def _status_from_init_check(row: Mapping[str, Any]) -> str:
    status = str(row.get("status") or "").strip().lower()
    if status == "pass":
        return "ready"
    if status in {"warning", "blocked"}:
        return "review_required"
    if status == "fail":
        return "pending_live_gate"
    return "pending_live_gate"


def _reasons_from_init_check(row: Mapping[str, Any], *, fallback: str) -> tuple[str, ...]:
    reasons = str(row.get("reasons") or "").strip()
    if reasons:
        return tuple(item for item in reasons.split(";") if item)
    status = str(row.get("status") or "").strip().lower()
    if status == "pass":
        return ()
    return (fallback,)


def build_pre_gas_readiness_model(
    *,
    run_dir: str | Path,
    initialization_dir: str | Path | None = None,
    config_path: str | Path | None = None,
    initialization_readiness_json: str | Path | None = None,
    initialization_contract_json: str | Path | None = None,
    database_sidecar_json: str | Path | None = None,
) -> dict[str, Any]:
    root = Path(run_dir).resolve()
    init_dir = Path(initialization_dir).resolve() if initialization_dir else root / "formal_initialization"
    readiness_path = _candidate(initialization_readiness_json, init_dir / READINESS_JSON)
    contract_path = _candidate(initialization_contract_json, init_dir / INITIALIZATION_CONTRACT_JSON)
    sidecar_path = _candidate(database_sidecar_json, init_dir / INITIALIZATION_DB_SIDECAR_JSON)

    readiness = _load_json(readiness_path)
    formal_contract = _load_json(contract_path)
    sidecar = _load_json(sidecar_path)
    contract = _readiness_contract(readiness)

    checks: list[PreGasReadinessCheck] = []
    init_reasons: list[str] = []
    if not readiness_path.exists():
        init_reasons.append("initialization_readiness_json_missing")
    readiness_status = str(readiness.get("readiness_status") or "").strip()
    if readiness_status.startswith("initialization_blocked") or readiness_status == "pressure_hardware_blocked":
        init_reasons.append(f"initialization_readiness_status={readiness_status}")
    checks.append(
        PreGasReadinessCheck(
            check="initialization_readiness_snapshot",
            status="ready" if readiness_path.exists() and not init_reasons else "review_required",
            evidence_role="initialization_readiness",
            evidence_path=_path_text(readiness_path),
            reasons=tuple(init_reasons),
            physical_meaning="Initialization readiness collects runtime safety, identity, GETCO, auxiliary, route, pressure, database, and CHECK contracts before gas routing.",
            next_action="Review blocking rows before any open-flow route. Missing live evidence remains a controlled gate rather than an automatic repair.",
            details={"readiness_status": readiness_status, "expected_device_ids": readiness.get("expected_device_ids") or []},
        )
    )

    db_ok, db_reasons = _contract_database_ok(contract, sidecar)
    checks.append(
        PreGasReadinessCheck(
            check="postgresql18_initialization_db_preflight_contract",
            status="ready" if db_ok and sidecar_path.exists() else "review_required",
            evidence_role="initialization_database_sidecar",
            evidence_path=_path_text(sidecar_path),
            reasons=db_reasons if db_reasons else (() if sidecar_path.exists() else ("initialization_database_sidecar_missing",)),
            physical_meaning="The production database gate must preserve SN/device_code as primary identity while keeping protocol device ID as a query alias.",
            next_action="Run or review the PostgreSQL 18 initialization DB preflight before open-flow sampling; this exporter does not connect to PostgreSQL.",
            details={"sidecar_only": sidecar.get("sidecar_only") if sidecar else None},
        )
    )

    identity_ok, identity_reasons = _contract_identity_ok(contract)
    checks.append(
        PreGasReadinessCheck(
            check="sn_device_code_identity_contract",
            status="ready" if identity_ok else "review_required",
            evidence_role="initialization_contract",
            evidence_path=_path_text(readiness_path),
            reasons=identity_reasons,
            physical_meaning="Analyzer identity is SN/device_code for production traceability; protocol device ID remains command/query compatibility only.",
            next_action="Keep COM/GA labels as transport mapping and bind the actual device ID/SN during the live identity snapshot.",
            details={"identity": dict(contract.get("identity") or {}) if isinstance(contract.get("identity"), Mapping) else {}},
        )
    )

    runtime_ok, runtime_reasons = _contract_runtime_temperature_check_ok(contract)
    checks.append(
        PreGasReadinessCheck(
            check="mode2_1hz_senco78_neutral_check_contract",
            status="ready" if runtime_ok else "review_required",
            evidence_role="initialization_contract",
            evidence_path=_path_text(readiness_path),
            reasons=runtime_reasons,
            physical_meaning="Startup must leave analyzers in MODE2, 1 Hz upload, stable filter settings, and neutral S7/S8 temperature coefficients for both classic and new algorithms.",
            next_action="Do not enter CO2/H2O open-flow until runtime setup and S7/S8 neutral evidence are reviewed or produced by controlled tools.",
            details={
                "runtime": dict(contract.get("runtime") or {}) if isinstance(contract.get("runtime"), Mapping) else {},
                "temperature": dict(contract.get("temperature") or {}) if isinstance(contract.get("temperature"), Mapping) else {},
                "check_monitor": dict(contract.get("check_monitor") or {}) if isinstance(contract.get("check_monitor"), Mapping) else {},
            },
        )
    )

    gate_specs = (
        (
            "getco_epoch0_snapshot_gate",
            _check_by_name(readiness, "getco1_to_getco9_epoch0_snapshot"),
            "coefficient_epoch0_getco_snapshot",
            "GETCO1-9 is the coefficient epoch-0 baseline. It must be read with real COM before neutralization, fitting, or writes.",
            "Run the validated read-only GETCO snapshot with >=1 s command pacing and identity rebinding.",
            "getco1_to_getco9_epoch0_snapshot_missing_or_not_passed",
        ),
        (
            "auxiliary_senco56789_and_senco78_gate",
            _check_by_name(readiness, "senco78_neutralization_evidence"),
            "auxiliary_coefficient_neutralization",
            "S5/S6/S7/S8/S9 auxiliary layers must be backed up, neutralized, repaired, or modeled before CO2/H2O fitting.",
            "Use controlled auxiliary tools after GETCO backup; keep S7/S8 neutral for both algorithms.",
            "senco78_neutralization_evidence_missing_or_not_passed",
        ),
        (
            "pressure_senco9_pre_open_flow_gate",
            _check_by_name(readiness, "senco9_pressure_completion_evidence"),
            "pressure_channel_completion",
            "SENCO9 pressure input affects firmware CO2/H2O calculations and must be traceably complete before open-flow fitting.",
            "Complete pressure/S9 review and write-readback-postverify evidence before CO2/H2O open-flow release.",
            "senco9_pressure_completion_evidence_missing_or_not_passed",
        ),
        (
            "formal_route_readiness_before_chamber_soak_gate",
            _check_by_name(readiness, "formal_route_readiness_evidence"),
            "formal_route_readiness",
            "Relay mapping, dewpoint evidence, relay read-write state, and N2 prepurge must be proven before chamber soak/open-flow sampling.",
            "Run the dedicated V1.5 route readiness probe outside the mature CO2/H2O queues.",
            "formal_route_readiness_evidence_missing_or_not_passed",
        ),
    )
    for check_name, source, role, meaning, action, fallback in gate_specs:
        checks.append(
            PreGasReadinessCheck(
                check=check_name,
                status=_status_from_init_check(source),
                evidence_role=role,
                evidence_path=str(source.get("path") or ""),
                reasons=_reasons_from_init_check(source, fallback=fallback),
                physical_meaning=meaning,
                next_action=action,
                details=dict(source.get("details") or {}) if isinstance(source.get("details"), Mapping) else {},
            )
        )

    checks.append(
        PreGasReadinessCheck(
            check="check_monitor_after_chamber_temperature_stable_gate",
            status="pending_point_gate",
            evidence_role="analyzer_check_monitor",
            evidence_path="analyzer_check_monitor.csv",
            reasons=("generated_by_downstream_formal_runner_after_all_active_chambers_stable",),
            physical_meaning="CHECK,YGAS,FFF is point-level read-only evidence after all active analyzer chamber temperatures are stable and before sampling.",
            next_action="Do not collect point samples for CHECK-capable new-algorithm analyzers until the downstream runner records CHECK,YGAS,FFF with >=1 s analyzer command gaps.",
            details={"command": "CHECK,YGAS,FFF", "minimum_command_gap_s": 1.0},
        )
    )
    checks.append(
        PreGasReadinessCheck(
            check="mature_co2_h2o_queue_boundary",
            status="ready",
            evidence_role="flow_boundary",
            evidence_path="",
            reasons=(),
            physical_meaning="The pre-gas readiness sidecar must not modify mature V1.5 CO2/H2O route runners or sampling workers.",
            next_action="Keep CO2/H2O route control in the validated queue runners and only pass them explicit operator-approved evidence.",
            details={"touches_formal_co2_queue": False, "touches_formal_h2o_queue": False},
        )
    )

    statuses = {check.status for check in checks}
    if "review_required" in statuses:
        overall = "review_required_before_live_identity_or_open_flow"
    elif "pending_live_gate" in statuses or "pending_point_gate" in statuses:
        overall = "ready_for_identity_gate_with_later_live_gates"
    else:
        overall = "ready_for_open_flow_from_sidecar_evidence"

    return {
        "schema": SCHEMA,
        "created_at": _now(),
        "overall_status": overall,
        "run_dir": str(root),
        "initialization_dir": str(init_dir),
        "config_path": str(Path(config_path).resolve()) if config_path else "",
        "opens_com_ports": False,
        "connects_postgresql": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_device_id": False,
        "not_real_acceptance_evidence": True,
        "checks": [check.to_json() for check in checks],
        "next_live_gate": "device_identity_and_getco_snapshot",
        "required_before_open_flow": [
            "sn_device_code_identity_contract",
            "getco_epoch0_snapshot_gate",
            "mode2_1hz_senco78_neutral_check_contract",
            "pressure_senco9_pre_open_flow_gate",
            "formal_route_readiness_before_chamber_soak_gate",
            "postgresql18_initialization_db_preflight_contract",
        ],
        "required_before_point_sampling_after_chamber_temperature_stable": [
            "check_monitor_after_chamber_temperature_stable_gate"
        ],
    }


def render_pre_gas_readiness_markdown(model: Mapping[str, Any]) -> str:
    lines = [
        "# V1.5 Pre-Gas Readiness",
        "",
        f"- schema: `{model.get('schema')}`",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- opens_com_ports: `{model.get('opens_com_ports')}`",
        f"- connects_postgresql: `{model.get('connects_postgresql')}`",
        f"- controls_water_or_gas_routes: `{model.get('controls_water_or_gas_routes')}`",
        f"- writes_coefficients: `{model.get('writes_coefficients')}`",
        f"- next_live_gate: `{model.get('next_live_gate')}`",
        "",
        "## Required Before Open Flow",
        "",
    ]
    for item in model.get("required_before_open_flow") or []:
        lines.append(f"- `{item}`")
    lines.extend(["", "## Required After Chamber Temperature Stable", ""])
    for item in model.get("required_before_point_sampling_after_chamber_temperature_stable") or []:
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
            "- This sidecar does not run hardware, connect PostgreSQL, control routes, or write SENCO.",
            "- Pending live gates remain explicit V1.5 controlled-tool work, not automatic release evidence.",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def write_pre_gas_readiness_outputs(model: Mapping[str, Any], output_dir: str | Path) -> dict[str, Path]:
    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    json_path = root / "v1_5_pre_gas_readiness.json"
    md_path = root / "v1_5_pre_gas_readiness.md"
    checks_path = root / "v1_5_pre_gas_readiness_checks.csv"
    json_path.write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_pre_gas_readiness_markdown(model), encoding="utf-8")
    _write_csv(checks_path, model.get("checks") or [])
    return {"json": json_path, "markdown": md_path, "checks_csv": checks_path}
