"""Blocked executor stub for future V1.5 read-only initialization COM preflight.

This module is intentionally no-COM and no-write. It consumes the reviewed
read-only COM preflight design sidecar and records that analyzer contact remains
locked until a later controlled read-only preflight package exists.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "v1_5_formal_initialization_readonly_com_preflight_blocked_executor_v1"
DESIGN_SCHEMA = "v1_5_formal_initialization_readonly_com_preflight_design_v1"
READY_DESIGN_STATUS = "ready_for_readonly_real_com_preflight_design_review"
BLOCKED_STATUS = "blocked_pending_readonly_real_com_preflight_implementation"
REVIEW_STATUS = "review_required"


@dataclass(frozen=True)
class ReadonlyComPreflightBlockedExecutorCheck:
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
) -> ReadonlyComPreflightBlockedExecutorCheck:
    return ReadonlyComPreflightBlockedExecutorCheck(
        check=check,
        status=status,
        evidence_role=evidence_role,
        reasons=tuple(reasons),
        physical_meaning=physical_meaning,
        next_action=next_action,
        details=details,
    )


def _design_reasons(payload: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    if not payload:
        return ["readonly_com_preflight_design_missing"]
    if payload.get("schema") != DESIGN_SCHEMA:
        reasons.append(f"design_schema={payload.get('schema') or 'missing'}")
    if payload.get("overall_status") != READY_DESIGN_STATUS:
        reasons.append(f"design_status={payload.get('overall_status') or 'missing'}")
    if int(payload.get("blocker_count") or 0):
        reasons.append(f"design_blocker_count={payload.get('blocker_count')}")
    if int(payload.get("review_required_count") or 0):
        reasons.append(f"design_review_required_count={payload.get('review_required_count')}")
    if payload.get("required_future_read_only_real_com_flag") != "--execute-read-only-real-com":
        reasons.append(
            f"required_future_read_only_real_com_flag={payload.get('required_future_read_only_real_com_flag')!r}"
        )
    if payload.get("required_future_controlled_write_flag_excluded") != "--execute-controlled-writes":
        reasons.append(
            "required_future_controlled_write_flag_excluded="
            f"{payload.get('required_future_controlled_write_flag_excluded')!r}"
        )
    if float(payload.get("minimum_serial_command_gap_s") or 0.0) < 1.0:
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
        "controls_water_or_gas_routes",
        "controls_pressure",
        "writes_sn",
        "writes_device_id",
        "writes_coefficients",
        "database_written",
    ):
        if payload.get(field) is not False:
            reasons.append(f"design_boundary_{field}={payload.get(field)!r}")
    return reasons


def build_v1_5_formal_initialization_readonly_com_preflight_blocked_executor(
    *,
    formal_initialization_readonly_com_preflight_design_json: str | Path | None,
) -> dict[str, Any]:
    design_path = (
        Path(formal_initialization_readonly_com_preflight_design_json).resolve()
        if formal_initialization_readonly_com_preflight_design_json
        else None
    )
    design = _load_json(design_path)

    checks: list[ReadonlyComPreflightBlockedExecutorCheck] = []
    design_reasons = _design_reasons(design)
    checks.append(
        _check(
            check="readonly_com_preflight_design_consumed",
            status="ready" if not design_reasons else "review_required",
            evidence_role="required_readonly_com_preflight_design",
            reasons=design_reasons,
            physical_meaning=(
                "A future read-only COM preflight must consume the reviewed design contract before any "
                "protocol ID, SN/device_code, GETCO, runtime, or CHECK read can be considered."
            ),
            next_action="Regenerate the read-only COM preflight design sidecar until it is ready.",
            details={
                "source_path": str(design_path) if design_path else "",
                "source_status": design.get("overall_status", ""),
            },
        )
    )
    checks.append(
        _check(
            check="read_only_real_com_execution_lock_enforced",
            status="ready",
            evidence_role="hard_real_com_execution_lock",
            physical_meaning=(
                "The V1.5 read-only COM preflight is still a blocked stub: there is no supported "
                "--execute-read-only-real-com path."
            ),
            next_action="Implement a separate controlled read-only preflight package before any analyzer contact.",
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
            check="serial_and_identity_side_effect_lock",
            status="ready",
            evidence_role="no_com_no_identity_write_boundary",
            physical_meaning=(
                "Reviewed ports, SN/device_code reads, GETCO1-9, and CHECK remain contract requirements only; "
                "this stub reads no analyzer bytes and writes no identity state."
            ),
            next_action="Keep future serial contact behind explicit authorization and >=1s command pacing.",
            details={
                "opens_com_ports": False,
                "writes_sn": False,
                "writes_device_id": False,
                "minimum_serial_command_gap_s": 1.0,
            },
        )
    )
    checks.append(
        _check(
            check="controlled_write_side_effect_lock",
            status="ready",
            evidence_role="no_senco_no_runtime_write_boundary",
            physical_meaning=(
                "Read-only COM preflight must not turn into SN/device_code writes, runtime setup writes, "
                "or SENCO neutralization."
            ),
            next_action="Keep writes in later controlled-write tools with readback evidence.",
            details={
                "controlled_write_execution_allowed": False,
                "writes_coefficients": False,
                "writes_sn": False,
                "writes_device_id": False,
            },
        )
    )
    checks.append(
        _check(
            check="route_pressure_database_side_effect_lock",
            status="ready",
            evidence_role="no_route_no_pressure_no_database_boundary",
            physical_meaning=(
                "Read-only COM preflight must not become pressure control, gas/water routing, or PostgreSQL import."
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
        "contract_ready_for_future_readonly_com_review": blocked_executor_ready,
        "execution_supported": False,
        "execution_requested": False,
        "live_execution_allowed": False,
        "read_only_real_com_execution_allowed": False,
        "controlled_write_execution_allowed": False,
        "real_com_execution_allowed": False,
        "execute_flag_allowed": False,
        "formal_initialization_readonly_com_preflight_design_json": str(design_path) if design_path else "",
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
        "minimum_serial_command_gap_s": 1.0,
        "checks": [row.to_json() for row in checks],
        "next_action": (
            "Keep read-only COM preflight locked. A later controlled package must add explicit authorization, "
            "reviewed port inventory, >=1s pacing, identity/GETCO/CHECK reads, and hold evidence before analyzer contact."
        ),
    }


def write_v1_5_formal_initialization_readonly_com_preflight_blocked_executor_outputs(
    model: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, Path]:
    out = Path(output_dir)
    paths = {
        "json": out / "v1_5_formal_initialization_readonly_com_preflight_blocked_executor.json",
        "checks_csv": out / "v1_5_formal_initialization_readonly_com_preflight_blocked_executor_checks.csv",
        "summary_csv": out / "v1_5_formal_initialization_readonly_com_preflight_blocked_executor_summary.csv",
        "markdown": out / "V1_5_FORMAL_INITIALIZATION_READONLY_COM_PREFLIGHT_BLOCKED_EXECUTOR.md",
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
                "database_written": model.get("database_written"),
            }
        ],
    )
    lines = [
        "# V1.5 formal initialization read-only COM preflight blocked executor",
        "",
        "This is a no-COM, no-write blocked executor stub for future V1.5 read-only initialization COM preflight.",
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
