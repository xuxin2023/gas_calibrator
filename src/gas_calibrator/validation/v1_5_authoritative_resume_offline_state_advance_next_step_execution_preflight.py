"""Immediate, no-side-effect preflight for one authorized V1.5 next step."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_authoritative_resume_offline_state_advance_next_step_execution_authorization import (
    READY_STATUS as AUTHORIZATION_READY_STATUS,
    SCHEMA as AUTHORIZATION_SCHEMA,
    build_v1_5_authoritative_resume_offline_state_advance_next_step_execution_authorization,
)
from .v1_5_authoritative_resume_offline_state_advance_post_write_verification import (
    _contains_reparse,
)

SCHEMA = (
    "v1_5_authoritative_resume_offline_state_advance_next_step_execution_preflight_v1"
)
READY_STATUS = "ready_for_controlled_next_step_execution"
REVIEW_STATUS = "review_required"
AUTHORIZATION_VALIDATION_FILENAME = (
    "v1_5_authoritative_resume_offline_state_advance_"
    "next_step_execution_authorization_validation.json"
)
MATURE_ROUTE_MODULES = {
    "co2_open_flow_sampling": "gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue",
    "h2o_open_flow_sampling": "gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue",
}
FORBIDDEN_COMMAND_TOKENS = (
    "_handoff",
    "0624",
    "migration",
    "diagnostic",
    "sampling_worker",
    "open_flow_sampling.py",
    "gas_calibrator.v2",
    "gas_calibrator.tools.run_v1_",
)


def _now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _iso(value: datetime) -> str:
    return (
        value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    )


def _load(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(value) if isinstance(value, Mapping) else {}


def _sha(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else ""
    except OSError:
        return ""


def _command_sha(command: Sequence[Any]) -> str:
    normalized = json.dumps(
        [str(value) for value in command],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
    except (OSError, RuntimeError, ValueError):
        return False
    return True


def _check(name: str, reasons: Sequence[str]) -> dict[str, Any]:
    return {
        "check": name,
        "status": "ready" if not reasons else "review_required",
        "reasons": list(reasons),
    }


def _step_and_outputs(
    validation: Mapping[str, Any], reasons: list[str]
) -> tuple[dict[str, Any], list[Path]]:
    plan_path = Path(str(validation.get("full_flow_plan_json") or "")).absolute()
    plan = _load(plan_path)
    next_step_id = str(validation.get("next_step_id") or "")
    matches = [
        dict(row)
        for row in plan.get("steps") or []
        if isinstance(row, Mapping) and str(row.get("step_id") or "") == next_step_id
    ]
    if len(matches) != 1:
        reasons.append("full_flow_plan_next_step_not_unique")
        return {}, []
    step = matches[0]
    root = plan_path.parent
    outputs: list[Path] = []
    for value in step.get("expected_outputs") or []:
        candidate = Path(str(value))
        candidate = candidate if candidate.is_absolute() else root / candidate
        candidate = candidate.absolute()
        if not _is_within(candidate, root):
            reasons.append(f"expected_output_outside_plan_root:{candidate}")
        outputs.append(candidate)
    if not outputs:
        reasons.append("next_step_expected_outputs_missing")
    return step, outputs


def build_v1_5_authoritative_resume_offline_state_advance_next_step_execution_preflight(
    *, execution_authorization_validation_json: str | Path, now: datetime | None = None
) -> dict[str, Any]:
    validation_path = Path(execution_authorization_validation_json).absolute()
    validation = _load(validation_path)
    evaluated_at = (now or _now()).astimezone(UTC).replace(microsecond=0)
    validation_reasons: list[str] = []
    if validation_path.name != AUTHORIZATION_VALIDATION_FILENAME:
        validation_reasons.append(
            "execution_authorization_validation_filename_not_canonical"
        )
    if _contains_reparse(validation_path):
        validation_reasons.append(
            "execution_authorization_validation_path_contains_reparse_point"
        )
    if validation.get("schema") != AUTHORIZATION_SCHEMA:
        validation_reasons.append("execution_authorization_validation_schema_invalid")
    if validation.get("overall_status") != AUTHORIZATION_READY_STATUS:
        validation_reasons.append("execution_authorization_validation_not_ready")
    if validation.get("execution_authorization_validated") is not True:
        validation_reasons.append(
            "execution_authorization_validation_ready_flag_not_true"
        )
    if int(validation.get("review_required_count") or 0) or validation.get(
        "review_reasons"
    ):
        validation_reasons.append(
            "execution_authorization_validation_contains_review_reasons"
        )
    if validation.get("next_step_execution_allowed") is not False:
        validation_reasons.append(
            "execution_authorization_validation_prematurely_allows_execution"
        )
    design_path = Path(
        str(validation.get("controlled_executor_design_json") or "")
    ).absolute()
    packet_path = Path(
        str(validation.get("execution_authorization_json") or "")
    ).absolute()
    if str(validation.get("controlled_executor_design_sha256") or "") != _sha(
        design_path
    ):
        validation_reasons.append("controlled_executor_design_sha256_drift")
    if str(validation.get("execution_authorization_sha256") or "") != _sha(packet_path):
        validation_reasons.append("execution_authorization_sha256_drift")
    try:
        recomputed = build_v1_5_authoritative_resume_offline_state_advance_next_step_execution_authorization(
            controlled_executor_design_json=design_path,
            execution_authorization_json=packet_path,
            now=evaluated_at,
        )
    except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError):
        recomputed = {}
    exact = bool(recomputed) and {
        key: value for key, value in validation.items() if key != "generated_at"
    } == {key: value for key, value in recomputed.items() if key != "generated_at"}
    if not recomputed:
        validation_reasons.append("execution_authorization_recompute_failed")
    elif not exact:
        validation_reasons.append("execution_authorization_recompute_mismatch")

    command_reasons: list[str] = []
    plan_path = Path(str(validation.get("next_step_plan_json") or "")).absolute()
    plan = _load(plan_path)
    command = [str(value) for value in plan.get("next_step_command") or []]
    module = str(plan.get("next_step_tool_module") or "")
    if _contains_reparse(plan_path):
        command_reasons.append("next_step_plan_path_contains_reparse_point")
    if str(validation.get("next_step_plan_sha256") or "") != _sha(plan_path):
        command_reasons.append("next_step_plan_sha256_drift")
    if _command_sha(command) != str(validation.get("next_step_command_sha256") or ""):
        command_reasons.append("next_step_command_sha256_drift")
    if len(command) < 3 or Path(command[0]).stem.lower() not in {"python", "python3"}:
        command_reasons.append("next_step_command_python_prefix_invalid")
    if len(command) < 3 or command[1] != "-m" or command[2] != module:
        command_reasons.append("next_step_command_not_exact_python_module")
    if not module.startswith("gas_calibrator.tools.run_v1_5_"):
        command_reasons.append("next_step_tool_module_not_v1_5_runner")
    lowered = " ".join(command).replace("\\", "/").lower()
    for token in FORBIDDEN_COMMAND_TOKENS:
        if token in lowered and token != "gas_calibrator.tools.run_v1_":
            command_reasons.append(f"forbidden_next_step_command_token:{token}")
    if (
        "gas_calibrator.tools.run_v1_" in lowered
        and "gas_calibrator.tools.run_v1_5_" not in lowered
    ):
        command_reasons.append("forbidden_next_step_command_token:legacy_v1")
    expected_route_module = MATURE_ROUTE_MODULES.get(
        str(plan.get("next_step_id") or "")
    )
    if expected_route_module and module != expected_route_module:
        command_reasons.append("mature_route_module_mismatch")
    step, expected_outputs = _step_and_outputs(validation, command_reasons)
    if step and [str(value) for value in step.get("command") or []] != command:
        command_reasons.append("full_flow_step_command_mismatch")
    if step and str(step.get("tool_module") or "") != module:
        command_reasons.append("full_flow_step_module_mismatch")

    reasons = [*validation_reasons, *command_reasons]
    ready = not reasons
    checks = [
        _check(
            "last_moment_authorization_hash_and_expiry_recompute", validation_reasons
        ),
        _check("exact_mature_command_and_output_boundary", command_reasons),
    ]
    capabilities = dict(validation.get("authorized_capabilities") or {})
    return {
        "schema": SCHEMA,
        "generated_at": _iso(evaluated_at),
        "overall_status": READY_STATUS if ready else REVIEW_STATUS,
        "controlled_next_step_execution_preflight_ready": ready,
        "hold_count": len(reasons),
        "hold_reasons": reasons,
        "execution_authorization_validation_json": str(validation_path),
        "execution_authorization_validation_sha256": _sha(validation_path),
        "controlled_executor_design_json": str(design_path),
        "controlled_executor_design_sha256": _sha(design_path),
        "execution_authorization_json": str(packet_path),
        "execution_authorization_sha256": _sha(packet_path),
        "authorization_id": str(validation.get("authorization_id") or ""),
        "authorization_expires_at": str(
            validation.get("authorization_expires_at") or ""
        ),
        "run_id": str(validation.get("run_id") or ""),
        "attempt_id": str(validation.get("attempt_id") or ""),
        "verified_step_id": str(validation.get("verified_step_id") or ""),
        "next_step_id": str(validation.get("next_step_id") or ""),
        "next_step_tool_module": module,
        "next_step_command": command,
        "next_step_command_sha256": _command_sha(command),
        "full_flow_plan_json": str(validation.get("full_flow_plan_json") or ""),
        "full_flow_plan_sha256": str(validation.get("full_flow_plan_sha256") or ""),
        "expected_output_paths": [str(path) for path in expected_outputs],
        "authorized_capabilities": capabilities,
        "planned_opens_com_ports": bool(step.get("opens_com_ports")),
        "planned_controls_pressure": bool(step.get("controls_pressure")),
        "planned_controls_gas_route": bool(step.get("controls_gas_route")),
        "planned_controls_water_route": bool(step.get("controls_water_route")),
        "planned_writes_device_id": bool(step.get("writes_device_id")),
        "planned_writes_coefficients": bool(step.get("writes_coefficients")),
        "single_process_launch_max": 1,
        "shell_execution_allowed": False,
        "automatic_retry_allowed": False,
        "fallback_entry_allowed": False,
        "automatic_state_advance_allowed": False,
        "execution_supported": True,
        "next_step_execution_allowed": ready,
        "resume_execution_allowed": ready,
        "would_execute": False,
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_authoritative_state": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "connects_postgresql": False,
        "database_written": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
        "checks": checks,
        "next_action": "Invoke the controlled executor once with the exact attempt id and confirmation, or let this authorization expire.",
    }


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=("check", "status", "reasons"))
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "check": row.get("check"),
                    "status": row.get("status"),
                    "reasons": ";".join(
                        str(value) for value in row.get("reasons") or []
                    ),
                }
            )


def write_v1_5_authoritative_resume_offline_state_advance_next_step_execution_preflight(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    stem = (
        "v1_5_authoritative_resume_offline_state_advance_next_step_execution_preflight"
    )
    paths = {
        "json": out / f"{stem}.json",
        "checks_csv": out / f"{stem}_checks.csv",
        "markdown": out / "V1_5_NEXT_STEP_EXECUTION_PREFLIGHT.md",
    }
    paths["json"].write_text(
        json.dumps(dict(model), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    _write_csv(paths["checks_csv"], model.get("checks") or [])
    paths["markdown"].write_text(
        "\n".join(
            [
                "# V1.5 Next-Step Execution Preflight",
                "",
                f"- overall_status: `{model.get('overall_status')}`",
                f"- preflight_ready: `{model.get('controlled_next_step_execution_preflight_ready')}`",
                f"- next_step_execution_allowed: `{model.get('next_step_execution_allowed')}`",
                f"- next_step_id: `{model.get('next_step_id')}`",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return paths


__all__ = [
    "AUTHORIZATION_VALIDATION_FILENAME",
    "FORBIDDEN_COMMAND_TOKENS",
    "READY_STATUS",
    "REVIEW_STATUS",
    "SCHEMA",
    "build_v1_5_authoritative_resume_offline_state_advance_next_step_execution_preflight",
    "write_v1_5_authoritative_resume_offline_state_advance_next_step_execution_preflight",
]
