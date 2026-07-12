"""Controlled executor for one canonical V1.5 offline resume step."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .v1_5_authoritative_resume_offline_candidate_gate import (
    READY_STATUS as GATE_READY_STATUS,
    SCHEMA as GATE_SCHEMA,
    build_v1_5_authoritative_resume_offline_candidate_gate,
)
from .v1_5_entrypoint_inventory import classify_v1_5_entrypoint

SCHEMA = "v1_5_authoritative_resume_offline_executor_v1"
LOCKED_STATUS = "locked_no_offline_execution_requested"
EXECUTED_STATUS = "offline_step_executed_pending_verification"
HOLD_STATUS = "offline_step_execution_hold"
CONFIRMATION_TEXT = "execute_v1_5_offline_canonical_step_only"
DEFAULT_TIMEOUT_S = 300.0

GATE_COMPARE_KEYS = (
    "overall_status",
    "offline_resume_candidate_ready",
    "review_required_count",
    "review_reasons",
    "execution_preflight_json",
    "execution_preflight_sha256",
    "execution_preflight_age_s",
    "attempt_id",
    "run_id",
    "full_flow_plan_json",
    "full_flow_plan_sha256",
    "next_step_id",
    "next_step_execution_mode",
    "next_step_tool_module",
    "next_step_command_recorded_only",
    "physical_or_write_step_must_use_dedicated_executor",
    "execution_supported",
    "resume_execution_allowed",
    "would_execute",
    "opens_com_ports",
    "controls_pressure",
    "controls_water_or_gas_routes",
    "writes_sn",
    "writes_device_id",
    "writes_coefficients",
    "connects_postgresql",
    "database_written",
    "formal_release_allowed",
    "database_import_allowed",
    "not_real_acceptance_evidence",
)

_FORBIDDEN_FLAGS = {
    "--execute",
    "--execute-read-only-real-com",
    "--execute-controlled-writes",
    "--allow-real-com",
    "--allow-pressure-control",
    "--allow-route-control",
    "--allow-writes",
    "--allow-database-import",
    "--write-state",
    "--replace-state",
}


def _now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(value) if isinstance(value, Mapping) else {}


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else ""


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
    except (OSError, ValueError):
        return False
    return True


def _canonical_step(gate: Mapping[str, Any]) -> tuple[dict[str, Any], Path]:
    plan_path = Path(str(gate.get("full_flow_plan_json") or "")).resolve()
    plan = _load(plan_path)
    step_id = str(gate.get("next_step_id") or "")
    step = next(
        (
            dict(row)
            for row in plan.get("steps") or []
            if isinstance(row, Mapping) and str(row.get("step_id") or "") == step_id
        ),
        {},
    )
    return step, plan_path


def _command_reasons(
    command: Sequence[str], step: Mapping[str, Any], *, plan_root: Path, repo_root: Path
) -> list[str]:
    reasons: list[str] = []
    values = [str(value) for value in command]
    if len(values) < 3:
        return ["offline_command_too_short"]
    if Path(values[0]).name.lower() not in {"python", "python.exe", "python3"}:
        reasons.append("offline_command_runtime_not_python")
    if values[1] != "-m":
        reasons.append("offline_command_not_module_invocation")
    module = values[2]
    if module != str(step.get("tool_module") or ""):
        reasons.append("offline_command_module_mismatch")
    if not module.startswith("gas_calibrator.tools."):
        reasons.append("offline_command_module_outside_tools")
    module_name = module.rsplit(".", 1)[-1]
    module_path = repo_root / "src" / "gas_calibrator" / "tools" / f"{module_name}.py"
    if not module_path.is_file():
        reasons.append("offline_command_module_file_missing")
    else:
        entry = classify_v1_5_entrypoint(module_path, root=repo_root)
        if entry.risk_level != "offline" or entry.opens_com_ports or entry.controls_routes:
            reasons.append("offline_command_entrypoint_not_offline")
        if entry.writes_coefficients:
            reasons.append("offline_command_entrypoint_writes_coefficients")
    for value in values[3:]:
        if value in _FORBIDDEN_FLAGS or value.startswith("--execute="):
            reasons.append(f"offline_command_forbidden_flag:{value}")
        if any(token in value for token in ("&&", "||", ";", "|", "$(", "`")):
            reasons.append("offline_command_shell_metacharacter")
    try:
        output_dir = Path(values[values.index("--output-dir") + 1]).resolve()
    except (ValueError, IndexError):
        reasons.append("offline_command_output_dir_missing")
    else:
        if not _is_within(output_dir, plan_root):
            reasons.append("offline_command_output_dir_outside_plan_root")
    return reasons


def _expected_output_paths(step: Mapping[str, Any], plan_root: Path) -> list[Path]:
    return [
        (plan_root / str(value)).resolve()
        for value in step.get("expected_outputs") or []
        if str(value).strip()
    ]


def _fresh_output_reasons(paths: Sequence[Path], before: Mapping[str, str]) -> list[str]:
    reasons: list[str] = []
    for path in paths:
        current_sha = _sha(path)
        if not current_sha:
            reasons.append(f"expected_output_missing:{path}")
        elif current_sha == before.get(str(path), ""):
            reasons.append(f"expected_output_not_fresh:{path}")
    return reasons


def run_v1_5_authoritative_resume_offline_executor(
    *,
    offline_candidate_gate_json: str | Path,
    execute_offline_step: bool = False,
    expected_attempt_id: str = "",
    operator_confirmation_text: str = "",
    now: datetime | None = None,
    timeout_s: float = DEFAULT_TIMEOUT_S,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    gate_path = Path(offline_candidate_gate_json).resolve()
    gate = _load(gate_path)
    evaluated_at = (now or _now()).astimezone(UTC).replace(microsecond=0)
    reasons: list[str] = []
    if gate.get("schema") != GATE_SCHEMA:
        reasons.append("offline_candidate_gate_schema_invalid")
    if gate.get("overall_status") != GATE_READY_STATUS:
        reasons.append("offline_candidate_gate_not_ready")
    if gate.get("offline_resume_candidate_ready") is not True:
        reasons.append("offline_candidate_gate_ready_flag_not_true")
    try:
        recomputed = build_v1_5_authoritative_resume_offline_candidate_gate(
            execution_preflight_json=gate.get("execution_preflight_json"),
            now=evaluated_at,
        )
    except (OSError, ValueError, json.JSONDecodeError):
        recomputed = {}
    if not recomputed:
        reasons.append("offline_candidate_gate_recompute_failed")
    else:
        for key in GATE_COMPARE_KEYS:
            if gate.get(key) != recomputed.get(key):
                reasons.append(f"offline_candidate_gate_recompute_mismatch:{key}")
        reasons.extend(
            f"offline_candidate_revalidation:{reason}"
            for reason in recomputed.get("review_reasons") or []
        )

    step, plan_path = _canonical_step(gate)
    plan_root = plan_path.parent
    repo_root = Path(__file__).resolve().parents[3]
    command = [str(value) for value in step.get("command") or []]
    if command != [str(value) for value in gate.get("next_step_command_recorded_only") or []]:
        reasons.append("offline_candidate_command_mismatch")
    reasons.extend(_command_reasons(command, step, plan_root=plan_root, repo_root=repo_root))
    expected_outputs = _expected_output_paths(step, plan_root)
    if not expected_outputs:
        reasons.append("offline_candidate_expected_outputs_missing")
    for path in expected_outputs:
        if not _is_within(path, plan_root):
            reasons.append(f"offline_candidate_expected_output_outside_plan_root:{path}")
    if execute_offline_step:
        if expected_attempt_id != str(gate.get("attempt_id") or ""):
            reasons.append("offline_execution_attempt_id_mismatch")
        if operator_confirmation_text != CONFIRMATION_TEXT:
            reasons.append("offline_execution_confirmation_invalid")

    started_at = evaluated_at
    finished_at = evaluated_at
    process_attempted = False
    return_code: int | None = None
    stdout = ""
    stderr = ""
    output_reasons: list[str] = []
    duration_s = 0.0
    before = {str(path): _sha(path) for path in expected_outputs}
    if execute_offline_step and not reasons:
        runtime_command = [sys.executable, *command[1:]]
        environment = dict(os.environ)
        src_path = str(repo_root / "src")
        environment["PYTHONPATH"] = os.pathsep.join(
            value
            for value in (src_path, environment.get("PYTHONPATH", ""))
            if value
        )
        process_attempted = True
        monotonic_started = time.monotonic()
        try:
            result = subprocess_runner(
                runtime_command,
                cwd=str(repo_root),
                env=environment,
                shell=False,
                capture_output=True,
                text=True,
                timeout=max(1.0, float(timeout_s)),
                check=False,
            )
            return_code = int(result.returncode)
            stdout = str(result.stdout or "")[-20000:]
            stderr = str(result.stderr or "")[-20000:]
        except subprocess.TimeoutExpired as exc:
            return_code = -1
            stdout = str(exc.stdout or "")[-20000:]
            stderr = str(exc.stderr or "")[-20000:]
            output_reasons.append("offline_execution_timeout")
        duration_s = max(0.0, time.monotonic() - monotonic_started)
        finished_at = _now()
        if return_code != 0:
            output_reasons.append(f"offline_execution_return_code:{return_code}")
        output_reasons.extend(_fresh_output_reasons(expected_outputs, before))

    all_reasons = reasons + output_reasons
    executed_ok = execute_offline_step and process_attempted and not all_reasons
    after = {str(path): _sha(path) for path in expected_outputs}
    status = (
        EXECUTED_STATUS
        if executed_ok
        else HOLD_STATUS
        if execute_offline_step
        else LOCKED_STATUS
    )
    return {
        "schema": SCHEMA,
        "generated_at": _iso(finished_at),
        "overall_status": status,
        "offline_execution_requested": execute_offline_step,
        "offline_step_executed": executed_ok,
        "hold_count": len(all_reasons),
        "hold_reasons": all_reasons,
        "offline_candidate_gate_json": str(gate_path),
        "offline_candidate_gate_sha256": _sha(gate_path),
        "attempt_id": str(gate.get("attempt_id") or ""),
        "run_id": str(gate.get("run_id") or ""),
        "next_step_id": str(step.get("step_id") or ""),
        "next_step_tool_module": str(step.get("tool_module") or ""),
        "executed_command": [sys.executable, *command[1:]] if process_attempted else [],
        "process_attempted": process_attempted,
        "process_return_code": return_code,
        "process_stdout_tail": stdout,
        "process_stderr_tail": stderr,
        "process_duration_s": duration_s,
        "started_at": _iso(started_at),
        "finished_at": _iso(finished_at),
        "expected_output_paths": [str(path) for path in expected_outputs],
        "expected_output_sha256_before": before,
        "expected_output_sha256_after": after,
        "expected_outputs_fresh": executed_ok,
        "authoritative_state_advanced": False,
        "execution_supported": True,
        "offline_execution_only": True,
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "connects_postgresql": False,
        "database_written": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
        "next_action": (
            "Run a separate post-execution verifier before advancing authoritative state."
            if executed_ok
            else "Keep authoritative state unchanged and review hold reasons."
        ),
    }


def write_v1_5_authoritative_resume_offline_executor(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "v1_5_authoritative_resume_offline_executor.json"
    summary_path = out / "v1_5_authoritative_resume_offline_executor_summary.csv"
    markdown_path = out / "V1_5_AUTHORITATIVE_RESUME_OFFLINE_EXECUTOR.md"
    json_path.write_text(
        json.dumps(dict(model), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    fields = (
        "overall_status",
        "offline_execution_requested",
        "offline_step_executed",
        "hold_count",
        "attempt_id",
        "run_id",
        "next_step_id",
        "process_attempted",
        "process_return_code",
        "expected_outputs_fresh",
        "authoritative_state_advanced",
    )
    with summary_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerow({key: model.get(key) for key in fields})
    markdown_path.write_text(
        "\n".join(
            [
                "# V1.5 Authoritative Resume Offline Executor",
                "",
                f"- overall_status: `{model.get('overall_status')}`",
                f"- offline_step_executed: `{model.get('offline_step_executed')}`",
                f"- next_step_id: `{model.get('next_step_id')}`",
                f"- process_return_code: `{model.get('process_return_code')}`",
                f"- expected_outputs_fresh: `{model.get('expected_outputs_fresh')}`",
                f"- authoritative_state_advanced: `{model.get('authoritative_state_advanced')}`",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return {"json": json_path, "summary_csv": summary_path, "markdown": markdown_path}


__all__ = [
    "CONFIRMATION_TEXT",
    "EXECUTED_STATUS",
    "GATE_COMPARE_KEYS",
    "HOLD_STATUS",
    "LOCKED_STATUS",
    "SCHEMA",
    "run_v1_5_authoritative_resume_offline_executor",
    "write_v1_5_authoritative_resume_offline_executor",
]
