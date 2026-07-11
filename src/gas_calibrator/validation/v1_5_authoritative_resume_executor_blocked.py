"""Fail-closed evidence for a future V1.5 authoritative resume executor."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_authoritative_resume_executor_plan_preview import (
    READY_STATUS as PREVIEW_READY_STATUS,
    SCHEMA as PREVIEW_SCHEMA,
    build_v1_5_authoritative_resume_executor_plan_preview,
)

SCHEMA = "v1_5_authoritative_resume_executor_blocked_v1"
BLOCKED_STATUS = "blocked_pending_authorized_resume_executor_implementation"
REVIEW_STATUS = "review_required"

_PREVIEW_COMPARE_KEYS = (
    "overall_status",
    "resume_executor_plan_preview_ready",
    "blocker_count",
    "blocker_reasons",
    "consumer_contract_json",
    "consumer_contract_sha256",
    "next_step_id",
    "next_step_title",
    "next_step_phase",
    "next_step_tool_module",
    "next_step_command",
    "requires_real_com_authorization",
    "requires_pressure_authorization",
    "requires_route_authorization",
    "requires_write_authorization",
    "execution_supported",
    "resume_execution_allowed",
    "would_execute",
    "opens_com_ports",
    "controls_pressure",
    "controls_water_or_gas_routes",
    "writes_coefficients",
    "connects_postgresql",
    "formal_release_allowed",
    "database_import_allowed",
    "not_real_acceptance_evidence",
)

_LOCKED_PREVIEW_FIELDS = {
    "execution_supported": False,
    "resume_execution_allowed": False,
    "would_execute": False,
    "opens_com_ports": False,
    "controls_pressure": False,
    "controls_water_or_gas_routes": False,
    "writes_coefficients": False,
    "connects_postgresql": False,
    "formal_release_allowed": False,
    "database_import_allowed": False,
    "not_real_acceptance_evidence": True,
}

REJECTED_EXECUTION_FLAGS = (
    "--execute",
    "--resume",
    "--execute-read-only-real-com",
    "--allow-real-com",
    "--allow-pressure-control",
    "--allow-route-control",
    "--allow-writes",
    "--allow-database-import",
)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(value) if isinstance(value, Mapping) else {}


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else ""


def _preview_boundary_reasons(preview: Mapping[str, Any]) -> list[str]:
    return [
        f"preview_boundary_not_locked:{key}"
        for key, expected in _LOCKED_PREVIEW_FIELDS.items()
        if preview.get(key) is not expected
    ]


def build_v1_5_authoritative_resume_executor_blocked(
    *, resume_executor_plan_preview_json: str | Path
) -> dict[str, Any]:
    preview_path = Path(resume_executor_plan_preview_json).resolve()
    preview = _load(preview_path)
    reasons: list[str] = []
    if preview.get("schema") != PREVIEW_SCHEMA:
        reasons.append("resume_executor_plan_preview_schema_invalid")
    if preview.get("overall_status") != PREVIEW_READY_STATUS:
        reasons.append("resume_executor_plan_preview_not_ready")
    if preview.get("resume_executor_plan_preview_ready") is not True:
        reasons.append("resume_executor_plan_preview_ready_flag_not_true")
    reasons.extend(_preview_boundary_reasons(preview))

    consumer_contract = Path(str(preview.get("consumer_contract_json") or "")).resolve()
    try:
        recomputed = build_v1_5_authoritative_resume_executor_plan_preview(
            consumer_contract_json=consumer_contract
        )
    except (OSError, ValueError, json.JSONDecodeError):
        recomputed = {}
    if not recomputed:
        reasons.append("resume_executor_plan_preview_recompute_failed")
    else:
        for key in _PREVIEW_COMPARE_KEYS:
            if preview.get(key) != recomputed.get(key):
                reasons.append(f"resume_executor_plan_preview_recompute_mismatch:{key}")

    ready = not reasons
    checks = [
        {
            "check": "plan_preview_hash_and_recompute_binding",
            "status": "ready" if ready else "review_required",
            "reasons": reasons,
            "physical_meaning": (
                "A blocked executor may describe only the exact independently recomputed "
                "plan preview; it cannot trust a copied ready flag."
            ),
        },
        {
            "check": "resume_execution_lock",
            "status": "ready",
            "reasons": [],
            "physical_meaning": (
                "This package cannot execute the next step or unlock COM, pressure, routes, "
                "writes, PostgreSQL, release, or import."
            ),
        },
    ]
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": BLOCKED_STATUS if ready else REVIEW_STATUS,
        "blocked_executor_ready": ready,
        "review_required_count": len(reasons),
        "review_reasons": reasons,
        "production_state": "blocked_executor_only",
        "resume_executor_plan_preview_json": str(preview_path),
        "resume_executor_plan_preview_sha256": _sha(preview_path),
        "consumer_contract_json": str(consumer_contract),
        "next_step_id_recorded_only": str(preview.get("next_step_id") or ""),
        "next_step_command_recorded_only": [
            str(value) for value in preview.get("next_step_command") or []
        ],
        "authorization_requirements_recorded_only": {
            "real_com": bool(preview.get("requires_real_com_authorization")),
            "pressure": bool(preview.get("requires_pressure_authorization")),
            "route": bool(preview.get("requires_route_authorization")),
            "write": bool(preview.get("requires_write_authorization")),
        },
        "rejected_execution_flags": list(REJECTED_EXECUTION_FLAGS),
        "execution_supported": False,
        "resume_execution_allowed": False,
        "execution_requested": False,
        "does_not_execute_commands": True,
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
            "Keep resume execution locked. A later executor requires a separate authorization, "
            "physical-boundary review, and dedicated tests."
        ),
    }


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = ("check", "status", "reasons", "physical_meaning")
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            normalized = dict(row)
            normalized["reasons"] = ";".join(str(value) for value in row.get("reasons") or [])
            writer.writerow({key: normalized.get(key, "") for key in fields})


def write_v1_5_authoritative_resume_executor_blocked(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "v1_5_authoritative_resume_executor_blocked.json"
    checks_path = out / "v1_5_authoritative_resume_executor_blocked_checks.csv"
    markdown_path = out / "V1_5_AUTHORITATIVE_RESUME_EXECUTOR_BLOCKED.md"
    json_path.write_text(
        json.dumps(dict(model), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    _write_csv(checks_path, model.get("checks") or [])
    markdown_path.write_text(
        "\n".join(
            [
                "# V1.5 Authoritative Resume Executor Blocked",
                "",
                "This artifact proves that the future resume executor remains unavailable.",
                "",
                f"- overall_status: `{model.get('overall_status')}`",
                f"- blocked_executor_ready: `{model.get('blocked_executor_ready')}`",
                f"- execution_supported: `{model.get('execution_supported')}`",
                f"- resume_execution_allowed: `{model.get('resume_execution_allowed')}`",
                f"- opens_com_ports: `{model.get('opens_com_ports')}`",
                f"- connects_postgresql: `{model.get('connects_postgresql')}`",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return {"json": json_path, "checks_csv": checks_path, "markdown": markdown_path}


__all__ = [
    "BLOCKED_STATUS",
    "REJECTED_EXECUTION_FLAGS",
    "REVIEW_STATUS",
    "SCHEMA",
    "build_v1_5_authoritative_resume_executor_blocked",
    "write_v1_5_authoritative_resume_executor_blocked",
]
