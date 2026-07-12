"""Classify a verified V1.5 resume step as an offline-only candidate."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_authoritative_resume_execution_preflight import (
    READY_STATUS as PREFLIGHT_READY_STATUS,
    SCHEMA as PREFLIGHT_SCHEMA,
    build_v1_5_authoritative_resume_execution_preflight,
)

SCHEMA = "v1_5_authoritative_resume_offline_candidate_gate_v1"
READY_STATUS = "ready_for_offline_resume_candidate_review"
REVIEW_STATUS = "review_required"
MAX_PREFLIGHT_AGE_S = 60.0

_PREFLIGHT_COMPARE_KEYS = (
    "overall_status",
    "resume_execution_preflight_ready",
    "review_required_count",
    "review_reasons",
    "attempt_id",
    "authorization_validation_json",
    "authorization_validation_sha256",
    "authorization_id",
    "authorization_seconds_remaining",
    "run_id",
    "next_step_id_recorded_only",
    "next_step_command_recorded_only",
    "next_step_command_sha256_recorded_only",
    "capability_envelope_recorded_only",
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

_FORBIDDEN_MODULE_FRAGMENTS = (
    "readonly_com",
    "pressure",
    "open_flow",
    "controlled_write",
    "atomic_writer",
    "database_import",
    "postgresql",
)


def _now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _parse_time(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo is not None else None


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


def _canonical_step(preflight: Mapping[str, Any]) -> tuple[dict[str, Any], Path]:
    validation = _load(
        Path(str(preflight.get("authorization_validation_json") or "")).resolve()
    )
    design = _load(Path(str(validation.get("controlled_design_json") or "")).resolve())
    blocked = _load(
        Path(str(design.get("authoritative_resume_executor_blocked_json") or "")).resolve()
    )
    contract = _load(Path(str(blocked.get("consumer_contract_json") or "")).resolve())
    plan_path = Path(str(contract.get("full_flow_plan_json") or "")).resolve()
    plan = _load(plan_path)
    next_step_id = str(preflight.get("next_step_id_recorded_only") or "")
    step = next(
        (
            dict(row)
            for row in plan.get("steps") or []
            if isinstance(row, Mapping) and str(row.get("step_id") or "") == next_step_id
        ),
        {},
    )
    return step, plan_path


def _offline_step_reasons(
    step: Mapping[str, Any], preflight: Mapping[str, Any]
) -> list[str]:
    reasons: list[str] = []
    if not step:
        return ["canonical_next_step_missing"]
    if not str(step.get("execution_mode") or "").startswith("offline"):
        reasons.append("canonical_next_step_not_offline_mode")
    if not str(step.get("tool_module") or ""):
        reasons.append("canonical_next_step_tool_module_missing")
    for field in (
        "opens_com_ports",
        "controls_pressure",
        "controls_gas_route",
        "controls_water_route",
        "writes_device_id",
        "writes_coefficients",
    ):
        if step.get(field) is not False:
            reasons.append(f"canonical_next_step_side_effect:{field}")
    module = str(step.get("tool_module") or "").lower()
    for fragment in _FORBIDDEN_MODULE_FRAGMENTS:
        if fragment in module:
            reasons.append(f"canonical_next_step_forbidden_module:{fragment}")
    command = [str(value) for value in step.get("command") or []]
    if not command:
        reasons.append("canonical_next_step_command_missing")
    if command != [str(value) for value in preflight.get("next_step_command_recorded_only") or []]:
        reasons.append("canonical_next_step_command_mismatch")
    forbidden_text = " ".join(command).lower().replace("\\", "/")
    for fragment in ("_handoff", "0624", "/v2/", "diagnostic"):
        if fragment in forbidden_text:
            reasons.append(f"canonical_next_step_forbidden_surface:{fragment}")
    return reasons


def build_v1_5_authoritative_resume_offline_candidate_gate(
    *, execution_preflight_json: str | Path, now: datetime | None = None
) -> dict[str, Any]:
    preflight_path = Path(execution_preflight_json).resolve()
    preflight = _load(preflight_path)
    evaluated_at = (now or _now()).astimezone(UTC).replace(microsecond=0)
    preflight_reasons: list[str] = []
    if preflight.get("schema") != PREFLIGHT_SCHEMA:
        preflight_reasons.append("execution_preflight_schema_invalid")
    if preflight.get("overall_status") != PREFLIGHT_READY_STATUS:
        preflight_reasons.append("execution_preflight_not_ready")
    if preflight.get("resume_execution_preflight_ready") is not True:
        preflight_reasons.append("execution_preflight_ready_flag_not_true")
    for field in (
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
    ):
        if preflight.get(field) is not False:
            preflight_reasons.append(f"execution_preflight_boundary_invalid:{field}")
    if preflight.get("not_real_acceptance_evidence") is not True:
        preflight_reasons.append("execution_preflight_boundary_invalid:not_real_acceptance_evidence")

    recorded_at = _parse_time(preflight.get("generated_at"))
    if recorded_at is None:
        preflight_reasons.append("execution_preflight_generated_at_invalid")
        preflight_age_s = -1.0
    else:
        preflight_age_s = (evaluated_at - recorded_at).total_seconds()
        if preflight_age_s < 0 or preflight_age_s > MAX_PREFLIGHT_AGE_S:
            preflight_reasons.append("execution_preflight_not_fresh")
        try:
            recomputed = build_v1_5_authoritative_resume_execution_preflight(
                authorization_validation_json=preflight.get(
                    "authorization_validation_json"
                ),
                now=recorded_at,
            )
        except (OSError, ValueError, json.JSONDecodeError):
            recomputed = {}
        if not recomputed:
            preflight_reasons.append("execution_preflight_recompute_failed")
        else:
            for key in _PREFLIGHT_COMPARE_KEYS:
                if preflight.get(key) != recomputed.get(key):
                    preflight_reasons.append(f"execution_preflight_recompute_mismatch:{key}")
        current = build_v1_5_authoritative_resume_execution_preflight(
            authorization_validation_json=preflight.get("authorization_validation_json"),
            now=evaluated_at,
        )
        if current.get("resume_execution_preflight_ready") is not True:
            preflight_reasons.extend(
                f"current_preflight:{reason}"
                for reason in current.get("review_reasons") or []
            )

    step, plan_path = _canonical_step(preflight)
    offline_reasons = _offline_step_reasons(step, preflight)
    reasons = preflight_reasons + offline_reasons
    ready = not reasons
    checks = [
        {
            "check": "fresh_preflight_recompute",
            "status": "ready" if not preflight_reasons else "review_required",
            "reasons": preflight_reasons,
        },
        {
            "check": "canonical_step_offline_only",
            "status": "ready" if not offline_reasons else "review_required",
            "reasons": offline_reasons,
        },
        {
            "check": "execution_remains_unimplemented",
            "status": "ready",
            "reasons": [],
        },
    ]
    return {
        "schema": SCHEMA,
        "generated_at": _iso(evaluated_at),
        "overall_status": READY_STATUS if ready else REVIEW_STATUS,
        "offline_resume_candidate_ready": ready,
        "review_required_count": len(reasons),
        "review_reasons": reasons,
        "execution_preflight_json": str(preflight_path),
        "execution_preflight_sha256": _sha(preflight_path),
        "execution_preflight_age_s": preflight_age_s,
        "attempt_id": str(preflight.get("attempt_id") or ""),
        "run_id": str(preflight.get("run_id") or ""),
        "full_flow_plan_json": str(plan_path),
        "full_flow_plan_sha256": _sha(plan_path),
        "next_step_id": str(step.get("step_id") or ""),
        "next_step_execution_mode": str(step.get("execution_mode") or ""),
        "next_step_tool_module": str(step.get("tool_module") or ""),
        "next_step_command_recorded_only": [
            str(value) for value in step.get("command") or []
        ],
        "physical_or_write_step_must_use_dedicated_executor": bool(offline_reasons),
        "execution_supported": False,
        "resume_execution_allowed": False,
        "would_execute": False,
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
        "checks": checks,
        "next_action": (
            "Only an offline-only executor may consume a ready candidate. Physical, write, and database steps remain assigned to dedicated controlled executors."
        ),
    }


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields = ("check", "status", "reasons")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "check": row.get("check"),
                    "status": row.get("status"),
                    "reasons": ";".join(str(value) for value in row.get("reasons") or []),
                }
            )


def write_v1_5_authoritative_resume_offline_candidate_gate(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "v1_5_authoritative_resume_offline_candidate_gate.json"
    checks_path = out / "v1_5_authoritative_resume_offline_candidate_gate_checks.csv"
    markdown_path = out / "V1_5_AUTHORITATIVE_RESUME_OFFLINE_CANDIDATE_GATE.md"
    json_path.write_text(
        json.dumps(dict(model), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    _write_csv(checks_path, model.get("checks") or [])
    markdown_path.write_text(
        "\n".join(
            [
                "# V1.5 Authoritative Resume Offline Candidate Gate",
                "",
                "This classifies a canonical next step; it does not execute it.",
                "",
                f"- overall_status: `{model.get('overall_status')}`",
                f"- offline_resume_candidate_ready: `{model.get('offline_resume_candidate_ready')}`",
                f"- next_step_id: `{model.get('next_step_id')}`",
                f"- execution_supported: `{model.get('execution_supported')}`",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return {"json": json_path, "checks_csv": checks_path, "markdown": markdown_path}


__all__ = [
    "MAX_PREFLIGHT_AGE_S",
    "READY_STATUS",
    "REVIEW_STATUS",
    "SCHEMA",
    "build_v1_5_authoritative_resume_offline_candidate_gate",
    "write_v1_5_authoritative_resume_offline_candidate_gate",
]
