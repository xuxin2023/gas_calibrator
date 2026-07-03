"""Blocked executor stub for future V1.5 formal initialization execution.

This module is intentionally no-COM and no-write. It consumes the reviewed
initialization executor dry-run sidecar and records that live initialization
execution remains locked until a later controlled executor package exists.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "v1_5_formal_initialization_blocked_executor_v1"
PLAN_SCHEMA = "v1_5_formal_initialization_plan_v0"
DRY_RUN_SCHEMA = "v1_5_formal_initialization_executor_dry_run_v1"
READY_DRY_RUN_STATUS = "ready_for_initialization_executor_dry_run_review"
BLOCKED_STATUS = "blocked_pending_controlled_initialization_executor_implementation"
REVIEW_STATUS = "review_required"


@dataclass(frozen=True)
class FormalInitializationBlockedExecutorCheck:
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


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]], *, fallback_fields: Sequence[str] = ()) -> None:
    fields: list[str] = [str(field) for field in fallback_fields]
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
) -> FormalInitializationBlockedExecutorCheck:
    return FormalInitializationBlockedExecutorCheck(
        check=check,
        status=status,
        evidence_role=evidence_role,
        reasons=tuple(reasons),
        physical_meaning=physical_meaning,
        next_action=next_action,
        details=details,
    )


def _dry_run_reasons(payload: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    if not payload:
        return ["formal_initialization_executor_dry_run_missing"]
    if payload.get("schema") != DRY_RUN_SCHEMA:
        reasons.append(f"dry_run_schema={payload.get('schema') or 'missing'}")
    if payload.get("overall_status") != READY_DRY_RUN_STATUS:
        reasons.append(f"dry_run_status={payload.get('overall_status') or 'missing'}")
    if int(payload.get("blocker_count") or 0):
        reasons.append(f"dry_run_blocker_count={payload.get('blocker_count')}")
    if int(payload.get("review_required_count") or 0):
        reasons.append(f"dry_run_review_required_count={payload.get('review_required_count')}")
    if payload.get("dry_run_review_allowed") is not True:
        reasons.append(f"dry_run_review_allowed={payload.get('dry_run_review_allowed')!r}")
    for field in (
        "live_execution_allowed",
        "read_only_real_com_execution_allowed",
        "controlled_write_execution_allowed",
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
            reasons.append(f"dry_run_boundary_{field}={payload.get(field)!r}")
    return reasons


def _plan_reasons(payload: Mapping[str, Any], expected: str | Path | None) -> list[str]:
    expected_path = str(Path(expected).resolve()) if expected else ""
    if not expected_path:
        return ["formal_initialization_plan_json_argument_missing"]
    if not Path(expected_path).exists():
        return ["formal_initialization_plan_json_missing"]
    if payload.get("schema") != PLAN_SCHEMA:
        return [f"plan_schema={payload.get('schema') or 'missing'}"]
    return []


def build_v1_5_formal_initialization_blocked_executor(
    *,
    formal_initialization_executor_dry_run_json: str | Path | None,
    formal_initialization_plan_json: str | Path | None = None,
) -> dict[str, Any]:
    dry_run_path = (
        Path(formal_initialization_executor_dry_run_json).resolve()
        if formal_initialization_executor_dry_run_json
        else None
    )
    plan_path = Path(formal_initialization_plan_json).resolve() if formal_initialization_plan_json else None
    dry_run = _load_json(dry_run_path)
    plan = _load_json(plan_path)

    checks: list[FormalInitializationBlockedExecutorCheck] = []
    dry_run_reasons = _dry_run_reasons(dry_run)
    checks.append(
        _check(
            check="formal_initialization_executor_dry_run_consumed",
            status="ready" if not dry_run_reasons else "review_required",
            evidence_role="required_executor_dry_run",
            reasons=dry_run_reasons,
            physical_meaning=(
                "A future live initialization executor must consume the reviewed dry-run classification before "
                "any real-COM read, SN/device_code write, or coefficient write can be considered."
            ),
            next_action="Regenerate the initialization executor dry-run sidecar until it is ready.",
            details={
                "source_path": str(dry_run_path) if dry_run_path else "",
                "source_status": dry_run.get("overall_status", ""),
            },
        )
    )

    plan_reasons = _plan_reasons(plan, plan_path)
    contract_plan = str(dry_run.get("formal_initialization_plan_json") or "")
    if plan_path and contract_plan and str(Path(contract_plan).resolve()) != str(plan_path):
        plan_reasons.append("formal_initialization_plan_json_differs_from_dry_run")
    checks.append(
        _check(
            check="formal_initialization_plan_bound",
            status="ready" if not plan_reasons else "review_required",
            evidence_role="required_initialization_plan",
            reasons=plan_reasons,
            physical_meaning=(
                "The blocked executor records the exact initialization plan reviewed by the dry-run sidecar; "
                "it does not discover mutable plan files at execution time."
            ),
            next_action="Pass the same reviewed initialization plan JSON that was consumed by the dry-run review.",
            details={
                "argument_path": str(plan_path) if plan_path else "",
                "dry_run_plan_path": contract_plan,
                "plan_schema": plan.get("schema", ""),
            },
        )
    )

    checks.append(
        _check(
            check="execution_lock_enforced",
            status="ready",
            evidence_role="hard_execution_lock",
            physical_meaning=(
                "The V1.5 initialization executor is still a blocked stub: there is no supported --execute path."
            ),
            next_action=(
                "Implement a separate controlled executor package before any live initialization command can run."
            ),
            details={
                "execution_supported": False,
                "execution_requested": False,
                "live_execution_allowed": False,
            },
        )
    )
    checks.append(
        _check(
            check="real_com_side_effect_lock",
            status="ready",
            evidence_role="no_com_boundary",
            physical_meaning=(
                "Read-only identity/GETCO/CHECK contact remains locked behind a future explicit real-COM authorization."
            ),
            next_action="Keep COM access in dedicated, reviewed real-COM tools with >=1s command spacing.",
            details={
                "opens_com_ports": False,
                "read_only_real_com_execution_allowed": False,
            },
        )
    )
    checks.append(
        _check(
            check="controlled_write_side_effect_lock",
            status="ready",
            evidence_role="no_write_boundary",
            physical_meaning=(
                "SN/device_code writes and S5/S6/S7/S8/S9 neutralization remain separate controlled-write actions."
            ),
            next_action="Require explicit operator/reviewer/approver authorization plus readback before any write path.",
            details={
                "controlled_write_execution_allowed": False,
                "writes_sn": False,
                "writes_device_id": False,
                "writes_coefficients": False,
            },
        )
    )
    checks.append(
        _check(
            check="route_pressure_database_side_effect_lock",
            status="ready",
            evidence_role="no_route_no_pressure_no_database_boundary",
            physical_meaning=(
                "Initialization execution must not become pressure control, gas/water routing, or PostgreSQL import."
            ),
            next_action="Keep these actions in their dedicated V1.5 stages and evidence gates.",
            details={
                "controls_pressure": False,
                "controls_water_or_gas_routes": False,
                "connects_postgresql": False,
                "database_written": False,
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
        "contract_ready_for_future_execution_review": blocked_executor_ready,
        "execution_supported": False,
        "execution_requested": False,
        "live_execution_allowed": False,
        "read_only_real_com_execution_allowed": False,
        "controlled_write_execution_allowed": False,
        "real_com_execution_allowed": False,
        "execute_flag_allowed": False,
        "formal_initialization_executor_dry_run_json": str(dry_run_path) if dry_run_path else "",
        "formal_initialization_plan_json": str(plan_path) if plan_path else "",
        "run_id": dry_run.get("run_id") or plan.get("run_id") or "",
        "opens_com_ports": False,
        "connects_postgresql": False,
        "controls_water_or_gas_routes": False,
        "controls_pressure": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "database_written": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
        "checks": [row.to_json() for row in checks],
        "next_action": (
            "Keep live initialization locked. A later controlled executor must add explicit real-COM and "
            "controlled-write authorization, run readback checks, and write separate evidence before any device action."
        ),
    }


def write_v1_5_formal_initialization_blocked_executor_outputs(
    model: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, Path]:
    out = Path(output_dir)
    paths = {
        "json": out / "v1_5_formal_initialization_blocked_executor.json",
        "checks_csv": out / "v1_5_formal_initialization_blocked_executor_checks.csv",
        "summary_csv": out / "v1_5_formal_initialization_blocked_executor_summary.csv",
        "markdown": out / "V1_5_FORMAL_INITIALIZATION_BLOCKED_EXECUTOR.md",
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
                "writes_coefficients": model.get("writes_coefficients"),
                "database_written": model.get("database_written"),
            }
        ],
    )
    lines = [
        "# V1.5 formal initialization blocked executor",
        "",
        "This is a no-COM, no-write executor stub for future V1.5 formal initialization automation.",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- blocked_executor_ready: `{model.get('blocked_executor_ready')}`",
        f"- execution_supported: `{model.get('execution_supported')}`",
        f"- live_execution_allowed: `{model.get('live_execution_allowed')}`",
        f"- read_only_real_com_execution_allowed: `{model.get('read_only_real_com_execution_allowed')}`",
        f"- controlled_write_execution_allowed: `{model.get('controlled_write_execution_allowed')}`",
        f"- opens_com_ports: `{model.get('opens_com_ports')}`",
        f"- connects_postgresql: `{model.get('connects_postgresql')}`",
        f"- writes_coefficients: `{model.get('writes_coefficients')}`",
        "- This stub does not open COM, write SN/device_code, write SENCO, connect PostgreSQL, control pressure, or control gas/water routes.",
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
