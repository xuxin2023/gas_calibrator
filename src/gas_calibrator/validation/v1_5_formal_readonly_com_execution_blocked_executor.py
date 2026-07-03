"""Blocked executor for future V1.5 read-only COM execution.

This module intentionally does not open analyzer COM ports. It consumes the
read-only COM execution contract and records that the future real
``--execute-read-only-real-com`` path is still locked.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "v1_5_formal_readonly_com_execution_blocked_executor_v1"
CONTRACT_SCHEMA = "v1_5_formal_readonly_com_execution_contract_v1"
READY_CONTRACT_STATUS = "ready_for_readonly_com_execution_contract_review"
BLOCKED_STATUS = "blocked_pending_readonly_com_real_executor_implementation"
REVIEW_STATUS = "review_required"
MIN_SERIAL_COMMAND_GAP_S = 1.0


@dataclass(frozen=True)
class ReadonlyComExecutionBlockedExecutorCheck:
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


def _check(
    *,
    check: str,
    status: str,
    evidence_role: str,
    reasons: Sequence[str] = (),
    physical_meaning: str,
    next_action: str,
    details: Mapping[str, Any],
) -> ReadonlyComExecutionBlockedExecutorCheck:
    return ReadonlyComExecutionBlockedExecutorCheck(
        check=check,
        status=status,
        evidence_role=evidence_role,
        reasons=tuple(reasons),
        physical_meaning=physical_meaning,
        next_action=next_action,
        details=details,
    )


def _contract_reasons(payload: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    if not payload:
        return ["readonly_com_execution_contract_missing"]
    if payload.get("schema") != CONTRACT_SCHEMA:
        reasons.append(f"contract_schema={payload.get('schema') or 'missing'}")
    if payload.get("overall_status") != READY_CONTRACT_STATUS:
        reasons.append(f"contract_status={payload.get('overall_status') or 'missing'}")
    if int(payload.get("blocker_count") or 0):
        reasons.append(f"contract_blocker_count={payload.get('blocker_count')}")
    if int(payload.get("review_required_count") or 0):
        reasons.append(f"contract_review_required_count={payload.get('review_required_count')}")
    if payload.get("contract_ready") is not True:
        reasons.append(f"contract_ready={payload.get('contract_ready')!r}")
    if payload.get("production_state") != "contract_only":
        reasons.append(f"production_state={payload.get('production_state') or 'missing'}")
    if payload.get("required_future_read_only_real_com_flag") != "--execute-read-only-real-com":
        reasons.append(
            f"required_future_read_only_real_com_flag={payload.get('required_future_read_only_real_com_flag')!r}"
        )
    if float(payload.get("minimum_serial_command_gap_s") or 0.0) < MIN_SERIAL_COMMAND_GAP_S:
        reasons.append(f"minimum_serial_command_gap_s={payload.get('minimum_serial_command_gap_s')!r}")
    if payload.get("supports_old_algorithm_check_skip") is not True:
        reasons.append(f"supports_old_algorithm_check_skip={payload.get('supports_old_algorithm_check_skip')!r}")
    if payload.get("supported_active_analyzer_count") != "1_to_6":
        reasons.append(f"supported_active_analyzer_count={payload.get('supported_active_analyzer_count')!r}")
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
            reasons.append(f"contract_boundary_{field}={payload.get(field)!r}")
    if payload.get("not_real_acceptance_evidence") is not True:
        reasons.append(f"not_real_acceptance_evidence={payload.get('not_real_acceptance_evidence')!r}")
    return reasons


def build_v1_5_formal_readonly_com_execution_blocked_executor(
    *,
    formal_readonly_com_execution_contract_json: str | Path | None,
) -> dict[str, Any]:
    contract_path = (
        Path(formal_readonly_com_execution_contract_json).resolve()
        if formal_readonly_com_execution_contract_json
        else None
    )
    contract = _load_json(contract_path)
    contract_reasons = _contract_reasons(contract)

    checks: list[ReadonlyComExecutionBlockedExecutorCheck] = []
    checks.append(
        _check(
            check="readonly_com_execution_contract_consumed",
            status="ready" if not contract_reasons else "review_required",
            evidence_role="required_readonly_com_execution_contract",
            reasons=contract_reasons,
            physical_meaning=(
                "The future real read-only COM executor must consume the reviewed execution packet "
                "contract before any analyzer serial contact can be considered."
            ),
            next_action="Regenerate the read-only COM execution contract until it is ready.",
            details={
                "source_path": str(contract_path) if contract_path else "",
                "source_status": contract.get("overall_status", ""),
            },
        )
    )
    checks.append(
        _check(
            check="real_readonly_com_execution_lock_enforced",
            status="ready",
            evidence_role="hard_real_com_execution_lock",
            physical_meaning=(
                "This command is still a blocked stub: there is no supported real "
                "--execute-read-only-real-com path."
            ),
            next_action="Implement a separate real read-only COM executor only after this blocked evidence is reviewed.",
            details={
                "execution_supported": False,
                "execution_requested": False,
                "read_only_real_com_execution_allowed": False,
                "opens_com_ports": False,
            },
        )
    )
    checks.append(
        _check(
            check="authorization_packet_remains_inert",
            status="ready",
            evidence_role="no_authorization_unlock_boundary",
            physical_meaning=(
                "Authorization id, operator confirmation, reviewer, approver, reviewed port inventory, "
                "and active analyzer list are future packet fields only; this stub rejects them as unlocks."
            ),
            next_action="Keep authorization packet inputs inert until a separately reviewed real executor exists.",
            details={
                "authorization_id_consumed": False,
                "operator_confirmation_consumed": False,
                "reviewer_approver_consumed": False,
                "reviewed_port_inventory_consumed": False,
                "active_analyzer_list_consumed": False,
            },
        )
    )
    checks.append(
        _check(
            check="identity_getco_check_read_sequence_still_no_com",
            status="ready",
            evidence_role="no_serial_read_boundary",
            physical_meaning=(
                "Protocol ID, SN/device_code, GETCO1-9, MODE2/1Hz runtime, and CHECK remain a documented "
                "read sequence only. Old-algorithm CHECK skip behavior is not exercised by this stub."
            ),
            next_action="Future serial reads must keep >=1s command pacing and hold on schema, identity, GETCO, runtime, or CHECK failures.",
            details={
                "opens_com_ports": False,
                "minimum_serial_command_gap_s": MIN_SERIAL_COMMAND_GAP_S,
                "old_algorithm_check_skip_contract_retained": True,
            },
        )
    )
    checks.append(
        _check(
            check="write_database_route_side_effect_lock",
            status="ready",
            evidence_role="no_write_no_database_no_route_boundary",
            physical_meaning=(
                "Read-only COM execution must not turn into SN/device_code writes, SENCO writes, "
                "PostgreSQL import, pressure control, or gas/water route control."
            ),
            next_action="Keep writes, pressure, routes, and database import in their dedicated later V1.5 gates.",
            details={
                "writes_sn": False,
                "writes_device_id": False,
                "writes_coefficients": False,
                "connects_postgresql": False,
                "controls_pressure": False,
                "controls_water_or_gas_routes": False,
            },
        )
    )

    review_required_count = sum(1 for row in checks if row.status == "review_required")
    blocked_executor_ready = review_required_count == 0
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": BLOCKED_STATUS if blocked_executor_ready else REVIEW_STATUS,
        "blocker_count": 0,
        "review_required_count": review_required_count,
        "blocked_executor_ready": blocked_executor_ready,
        "contract_ready_for_future_readonly_com_executor_review": blocked_executor_ready,
        "production_state": "blocked_executor_only",
        "execution_supported": False,
        "execution_requested": False,
        "dry_run_only": True,
        "live_execution_allowed": False,
        "read_only_real_com_execution_allowed": False,
        "controlled_write_execution_allowed": False,
        "real_com_execution_allowed": False,
        "execute_flag_allowed": False,
        "formal_readonly_com_execution_contract_json": str(contract_path) if contract_path else "",
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
        "required_future_read_only_real_com_flag": "--execute-read-only-real-com",
        "required_future_controlled_write_flag_excluded": "--execute-controlled-writes",
        "supports_old_algorithm_check_skip": True,
        "checks": [row.to_json() for row in checks],
        "next_action": (
            "Keep real read-only COM execution locked. A later executor must explicitly implement "
            "authorization, reviewed ports, >=1s pacing, identity/SN/GETCO/runtime/CHECK reads, and hold "
            "evidence before any analyzer COM opens."
        ),
    }


def write_v1_5_formal_readonly_com_execution_blocked_executor_outputs(
    model: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, Path]:
    out = Path(output_dir)
    paths = {
        "json": out / "v1_5_formal_readonly_com_execution_blocked_executor.json",
        "checks_csv": out / "v1_5_formal_readonly_com_execution_blocked_executor_checks.csv",
        "summary_csv": out / "v1_5_formal_readonly_com_execution_blocked_executor_summary.csv",
        "markdown": out / "V1_5_FORMAL_READONLY_COM_EXECUTION_BLOCKED_EXECUTOR.md",
    }
    _write_json(paths["json"], model)
    _write_csv(paths["checks_csv"], model.get("checks", []))
    _write_csv(
        paths["summary_csv"],
        [
            {
                "overall_status": model.get("overall_status"),
                "blocker_count": model.get("blocker_count"),
                "review_required_count": model.get("review_required_count"),
                "blocked_executor_ready": model.get("blocked_executor_ready"),
                "execution_supported": model.get("execution_supported"),
                "live_execution_allowed": model.get("live_execution_allowed"),
                "read_only_real_com_execution_allowed": model.get("read_only_real_com_execution_allowed"),
                "controlled_write_execution_allowed": model.get("controlled_write_execution_allowed"),
                "opens_com_ports": model.get("opens_com_ports"),
                "writes_sn": model.get("writes_sn"),
                "writes_coefficients": model.get("writes_coefficients"),
                "connects_postgresql": model.get("connects_postgresql"),
                "database_written": model.get("database_written"),
            }
        ],
    )
    lines = [
        "# V1.5 formal read-only COM execution blocked executor",
        "",
        "This is a no-COM, no-write blocked executor stub for the future real read-only COM executor.",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- blocked_executor_ready: `{model.get('blocked_executor_ready')}`",
        f"- execution_supported: `{model.get('execution_supported')}`",
        f"- live_execution_allowed: `{model.get('live_execution_allowed')}`",
        f"- read_only_real_com_execution_allowed: `{model.get('read_only_real_com_execution_allowed')}`",
        f"- controlled_write_execution_allowed: `{model.get('controlled_write_execution_allowed')}`",
        f"- opens_com_ports: `{model.get('opens_com_ports')}`",
        f"- writes_sn: `{model.get('writes_sn')}`",
        f"- writes_coefficients: `{model.get('writes_coefficients')}`",
        f"- connects_postgresql: `{model.get('connects_postgresql')}`",
        "- This stub does not open COM, read analyzers, write SN/device_code, write SENCO, connect PostgreSQL, control pressure, or control gas/water routes.",
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
