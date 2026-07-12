"""Execute at most one hash-bound V1.5 next step under explicit authorization."""

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

from .v1_5_authoritative_resume_offline_state_advance_next_step_execution_preflight import (
    READY_STATUS as PREFLIGHT_READY_STATUS,
    SCHEMA as PREFLIGHT_SCHEMA,
    build_v1_5_authoritative_resume_offline_state_advance_next_step_execution_preflight,
)
from .v1_5_authoritative_resume_offline_state_advance_post_write_verification import (
    _contains_reparse,
)

SCHEMA = (
    "v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_v1"
)
LOCKED_STATUS = "locked_pending_explicit_next_step_execution"
EXECUTED_STATUS = "controlled_next_step_process_completed_pending_post_verification"
HOLD_STATUS = "hold"
PREFLIGHT_FILENAME = (
    "v1_5_authoritative_resume_offline_state_advance_next_step_execution_preflight.json"
)
CONFIRMATION_TEXT = (
    "execute exactly one hash-bound V1.5 next step; no shell; no retry; "
    "no fallback; no automatic state advance"
)
SubprocessRunner = Callable[..., subprocess.CompletedProcess[str]]


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


def _artifact_digest(path: Path) -> str:
    if path.is_file():
        return _sha(path)
    if not path.is_dir():
        return ""
    digest = hashlib.sha256()
    try:
        files = sorted(item for item in path.rglob("*") if item.is_file())
        for item in files:
            digest.update(item.relative_to(path).as_posix().encode("utf-8"))
            digest.update(b"\0")
            digest.update(_sha(item).encode("ascii"))
            digest.update(b"\n")
    except OSError:
        return ""
    return digest.hexdigest() if files else ""


def _fresh_output_reasons(
    paths: Sequence[Path], before: Mapping[str, str]
) -> list[str]:
    reasons: list[str] = []
    for path in paths:
        current = _artifact_digest(path)
        if not current:
            reasons.append(f"expected_output_missing_or_empty:{path}")
        elif current == str(before.get(str(path)) or ""):
            reasons.append(f"expected_output_not_fresh:{path}")
    return reasons


def run_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor(
    *,
    next_step_execution_preflight_json: str | Path,
    execute_next_step: bool = False,
    expected_attempt_id: str = "",
    operator_confirmation_text: str = "",
    timeout_s: float = 86400.0,
    now: datetime | None = None,
    subprocess_runner: SubprocessRunner = subprocess.run,
) -> dict[str, Any]:
    preflight_path = Path(next_step_execution_preflight_json).absolute()
    preflight = _load(preflight_path)
    evaluated_at = (now or _now()).astimezone(UTC).replace(microsecond=0)
    reasons: list[str] = []
    if preflight_path.name != PREFLIGHT_FILENAME:
        reasons.append("next_step_execution_preflight_filename_not_canonical")
    if _contains_reparse(preflight_path):
        reasons.append("next_step_execution_preflight_path_contains_reparse_point")
    if preflight.get("schema") != PREFLIGHT_SCHEMA:
        reasons.append("next_step_execution_preflight_schema_invalid")
    if preflight.get("overall_status") != PREFLIGHT_READY_STATUS:
        reasons.append("next_step_execution_preflight_not_ready")
    if preflight.get("controlled_next_step_execution_preflight_ready") is not True:
        reasons.append("next_step_execution_preflight_ready_flag_not_true")
    if preflight.get("next_step_execution_allowed") is not True:
        reasons.append("next_step_execution_preflight_does_not_allow_exact_step")
    if int(preflight.get("hold_count") or 0) or preflight.get("hold_reasons"):
        reasons.append("next_step_execution_preflight_contains_holds")
    validation_path = Path(
        str(preflight.get("execution_authorization_validation_json") or "")
    ).absolute()
    try:
        recomputed = build_v1_5_authoritative_resume_offline_state_advance_next_step_execution_preflight(
            execution_authorization_validation_json=validation_path,
            now=evaluated_at,
        )
    except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError):
        recomputed = {}
    exact = bool(recomputed) and {
        key: value for key, value in preflight.items() if key != "generated_at"
    } == {key: value for key, value in recomputed.items() if key != "generated_at"}
    if not recomputed:
        reasons.append("next_step_execution_preflight_recompute_failed")
    elif not exact:
        reasons.append("next_step_execution_preflight_recompute_mismatch")
    if execute_next_step:
        if expected_attempt_id != str(preflight.get("attempt_id") or ""):
            reasons.append("next_step_execution_attempt_id_mismatch")
        if operator_confirmation_text != CONFIRMATION_TEXT:
            reasons.append("next_step_execution_operator_confirmation_invalid")

    command = [str(value) for value in preflight.get("next_step_command") or []]
    runtime_python = Path(sys.executable).resolve()
    if str(preflight.get("runtime_python_executable") or "") != str(runtime_python):
        reasons.append("runtime_python_executable_mismatch")
    if str(preflight.get("runtime_python_executable_sha256") or "") != _sha(
        runtime_python
    ):
        reasons.append("runtime_python_executable_sha256_mismatch")
    runtime_command = [str(runtime_python), *command[1:]] if command else []
    expected_outputs = [
        Path(str(value)).absolute()
        for value in preflight.get("expected_output_paths") or []
    ]
    before = {str(path): _artifact_digest(path) for path in expected_outputs}
    process_attempted = False
    process_return_code: int | None = None
    stdout_tail = ""
    stderr_tail = ""
    duration_s = 0.0
    started_at = evaluated_at
    finished_at = evaluated_at
    runtime_reasons: list[str] = []
    if execute_next_step and not reasons:
        process_attempted = True
        monotonic_started = time.monotonic()
        repo_root = Path(__file__).resolve().parents[3]
        environment = dict(os.environ)
        environment["PYTHONPATH"] = os.pathsep.join(
            value
            for value in (str(repo_root / "src"), environment.get("PYTHONPATH", ""))
            if value
        )
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
            process_return_code = int(result.returncode)
            stdout_tail = str(result.stdout or "")[-20000:]
            stderr_tail = str(result.stderr or "")[-20000:]
        except subprocess.TimeoutExpired as exc:
            process_return_code = -1
            stdout_tail = str(exc.stdout or "")[-20000:]
            stderr_tail = str(exc.stderr or "")[-20000:]
            runtime_reasons.append("next_step_child_process_timeout")
        except OSError as exc:
            process_return_code = -1
            stderr_tail = str(exc)[-20000:]
            runtime_reasons.append("next_step_child_process_launch_failed")
        duration_s = max(0.0, time.monotonic() - monotonic_started)
        finished_at = _now()
        if process_return_code != 0:
            runtime_reasons.append(
                f"next_step_child_process_return_code:{process_return_code}"
            )
        runtime_reasons.extend(_fresh_output_reasons(expected_outputs, before))

    all_reasons = [*reasons, *runtime_reasons]
    executed_ok = execute_next_step and process_attempted and not all_reasons
    after = {str(path): _artifact_digest(path) for path in expected_outputs}
    status = (
        EXECUTED_STATUS
        if executed_ok
        else HOLD_STATUS
        if execute_next_step
        else LOCKED_STATUS
    )
    capabilities = dict(preflight.get("authorized_capabilities") or {})
    actual_physical_attempt = process_attempted
    return {
        "schema": SCHEMA,
        "generated_at": _iso(finished_at),
        "overall_status": status,
        "execution_requested": execute_next_step,
        "execution_attempted": process_attempted,
        "next_step_process_completed": executed_ok,
        "hold_count": len(all_reasons),
        "hold_reasons": all_reasons,
        "next_step_execution_preflight_json": str(preflight_path),
        "next_step_execution_preflight_sha256": _sha(preflight_path),
        "execution_authorization_validation_json": str(validation_path),
        "execution_authorization_validation_sha256": _sha(validation_path),
        "authorization_id": str(preflight.get("authorization_id") or ""),
        "run_id": str(preflight.get("run_id") or ""),
        "attempt_id": str(preflight.get("attempt_id") or ""),
        "verified_step_id": str(preflight.get("verified_step_id") or ""),
        "next_step_id": str(preflight.get("next_step_id") or ""),
        "next_step_tool_module": str(preflight.get("next_step_tool_module") or ""),
        "authorized_capabilities": capabilities,
        "planned_command": command,
        "executed_command": runtime_command if process_attempted else [],
        "process_launch_count": 1 if process_attempted else 0,
        "process_return_code": process_return_code,
        "process_stdout_tail": stdout_tail,
        "process_stderr_tail": stderr_tail,
        "process_duration_s": duration_s,
        "started_at": _iso(started_at),
        "finished_at": _iso(finished_at),
        "expected_output_paths": [str(path) for path in expected_outputs],
        "expected_output_sha256_before": before,
        "expected_output_sha256_after": after,
        "expected_outputs_fresh": executed_ok,
        "shell_used": False,
        "executor_retry_count": 0,
        "fallback_entry_used": False,
        "authoritative_state_advanced": False,
        "execution_supported": True,
        "next_step_execution_allowed": executed_ok,
        "resume_execution_allowed": False,
        "opens_com_ports": actual_physical_attempt
        and bool(capabilities.get("allow_real_com")),
        "controls_pressure": actual_physical_attempt
        and bool(capabilities.get("allow_pressure_control")),
        "controls_water_or_gas_routes": actual_physical_attempt
        and bool(capabilities.get("allow_route_control")),
        "writes_sn": actual_physical_attempt
        and bool(preflight.get("planned_writes_device_id")),
        "writes_device_id": actual_physical_attempt
        and bool(preflight.get("planned_writes_device_id")),
        "writes_coefficients": actual_physical_attempt
        and bool(preflight.get("planned_writes_coefficients")),
        "connects_postgresql": False,
        "database_written": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
        "pre_execution_revalidation": recomputed,
        "next_action": (
            "Run a separate post-execution verifier before any authoritative state advance."
            if executed_ok
            else "Keep the authoritative state unchanged and review hold evidence."
        ),
    }


def _write_csv(
    path: Path, fieldnames: Sequence[str], rows: Sequence[Mapping[str, Any]]
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(
            {field: row.get(field) for field in fieldnames} for row in rows
        )


def write_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths = {
        "invocation": out / "executor_invocation.json",
        "pre_execution_revalidation": out / "pre_execution_revalidation.json",
        "command_attempts": out / "command_attempts.csv",
        "child_process_result": out / "child_process_result.json",
        "hold_events": out / "hold_events.csv",
        "post_execution_evidence_index": out / "post_execution_evidence_index.json",
    }
    invocation = {
        key: model.get(key)
        for key in (
            "schema",
            "generated_at",
            "overall_status",
            "execution_requested",
            "execution_attempted",
            "authorization_id",
            "run_id",
            "attempt_id",
            "verified_step_id",
            "next_step_id",
            "next_step_tool_module",
            "next_step_execution_preflight_json",
            "next_step_execution_preflight_sha256",
            "shell_used",
            "executor_retry_count",
            "fallback_entry_used",
            "authoritative_state_advanced",
        )
    }
    paths["invocation"].write_text(
        json.dumps(invocation, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    paths["pre_execution_revalidation"].write_text(
        json.dumps(
            model.get("pre_execution_revalidation") or {}, ensure_ascii=False, indent=2
        )
        + "\n",
        encoding="utf-8",
    )
    attempts = []
    if model.get("execution_attempted"):
        attempts.append(
            {
                "attempt_number": 1,
                "command": json.dumps(
                    model.get("executed_command") or [], ensure_ascii=False
                ),
                "shell": False,
                "return_code": model.get("process_return_code"),
                "duration_s": model.get("process_duration_s"),
            }
        )
    _write_csv(
        paths["command_attempts"],
        ("attempt_number", "command", "shell", "return_code", "duration_s"),
        attempts,
    )
    child = {
        key: model.get(key)
        for key in (
            "execution_attempted",
            "next_step_process_completed",
            "process_launch_count",
            "process_return_code",
            "process_stdout_tail",
            "process_stderr_tail",
            "process_duration_s",
            "expected_output_paths",
            "expected_output_sha256_before",
            "expected_output_sha256_after",
            "expected_outputs_fresh",
        )
    }
    paths["child_process_result"].write_text(
        json.dumps(child, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    holds = [
        {"hold_number": index, "reason": reason, "state_advanced": False}
        for index, reason in enumerate(model.get("hold_reasons") or [], start=1)
    ]
    _write_csv(paths["hold_events"], ("hold_number", "reason", "state_advanced"), holds)
    evidence_index = {
        "schema": "v1_5_next_step_controlled_executor_evidence_index_v1",
        "generated_at": model.get("generated_at"),
        "overall_status": model.get("overall_status"),
        "next_step_process_completed": model.get("next_step_process_completed"),
        "authoritative_state_advanced": False,
        "not_real_acceptance_evidence": True,
        "artifacts": [],
    }
    for role, path in paths.items():
        if role == "post_execution_evidence_index":
            continue
        evidence_index["artifacts"].append(
            {"role": role, "path": str(path), "sha256": _sha(path)}
        )
    paths["post_execution_evidence_index"].write_text(
        json.dumps(evidence_index, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return paths


__all__ = [
    "CONFIRMATION_TEXT",
    "EXECUTED_STATUS",
    "HOLD_STATUS",
    "LOCKED_STATUS",
    "PREFLIGHT_FILENAME",
    "SCHEMA",
    "run_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor",
    "write_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor",
]
