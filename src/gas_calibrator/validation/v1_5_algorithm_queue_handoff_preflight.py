"""Offline preflight before profile-generated runlists reach mature queues.

This guard consumes the profile runner dry-run bundle and answers a narrow
question: is the new-algorithm 47/14 runlist ready for a dry-run/no-prompt
handoff review? It must never authorize live queue execution by itself.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "v1_5_algorithm_queue_handoff_preflight_v1"
PROFILE_DRY_RUN_JSON = "v1_5_algorithm_profile_runner_dry_run.json"
RUNNER_DRY_RUN_JSON = "v1_5_algorithm_runner_integration_dry_run.json"
CO2_QUEUE_RUNNER = "gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue"
H2O_QUEUE_RUNNER = "gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue"
FORBIDDEN_LIVE_FLAGS = (
    "--execute-controlled-writes",
    "--execute-real-run",
    "--write-coefficients",
    "--write-senco",
    "--live",
)


@dataclass(frozen=True)
class QueueHandoffPreflightCheck:
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


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
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


def _path_text(path: Path) -> str:
    return str(path.resolve()) if path.exists() else ""


def _nested_runner_json_from_profile(profile_model: Mapping[str, Any], profile_json: Path) -> Path:
    raw = (
        (profile_model.get("outputs") or {})
        .get("runner_integration_dry_run", {})
        .get("json")
    )
    if raw:
        return Path(str(raw)).resolve()
    return profile_json.parent / "algorithm_runner_integration_dry_run" / RUNNER_DRY_RUN_JSON


def _check(
    *,
    check: str,
    status: str,
    evidence_role: str,
    evidence_path: Path | str,
    reasons: Sequence[str] = (),
    physical_meaning: str,
    next_action: str,
    details: Mapping[str, Any],
) -> QueueHandoffPreflightCheck:
    path_text = str(evidence_path) if isinstance(evidence_path, str) else _path_text(evidence_path)
    return QueueHandoffPreflightCheck(
        check=check,
        status=status,
        evidence_role=evidence_role,
        evidence_path=path_text,
        reasons=tuple(reasons),
        physical_meaning=physical_meaning,
        next_action=next_action,
        details=details,
    )


def _route_plan_by_kind(runner_model: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    out: dict[str, Mapping[str, Any]] = {}
    for row in runner_model.get("runner_integration_plan") or []:
        if not isinstance(row, Mapping):
            continue
        route = str(row.get("route_kind") or "").strip().lower()
        if route:
            out[route] = row
    return out


def _route_reasons(row: Mapping[str, Any], *, route: str, runner: str, count: int) -> list[str]:
    reasons: list[str] = []
    command = str(row.get("command_preview") or "")
    if not row:
        return [f"{route}_runner_plan_missing"]
    if row.get("runner_module") != runner:
        reasons.append(f"{route}_unexpected_runner_module")
    if int(row.get("observed_point_count") or 0) != count:
        reasons.append(f"{route}_observed_point_count={row.get('observed_point_count')}")
    if "--dry-run" not in command:
        reasons.append(f"{route}_dry_run_flag_missing")
    if "--no-prompt" not in command:
        reasons.append(f"{route}_no_prompt_flag_missing")
    joined = " ".join(
        str(row.get(key) or "")
        for key in ("command_preview", "required_cli_flags", "forbidden_cli_flags")
    )
    for flag in FORBIDDEN_LIVE_FLAGS:
        if flag in command or flag in str(row.get("required_cli_flags") or ""):
            reasons.append(f"{route}_live_flag_present={flag}")
    if row.get("runner_integration_status") != "dry_run_preview_only_not_runner_wired":
        reasons.append(f"{route}_runner_integration_status_not_dry_run_preview")
    if row.get("opens_com_ports") is not False:
        reasons.append(f"{route}_plan_must_not_open_com_ports")
    if row.get("controls_water_or_gas_routes") is not False:
        reasons.append(f"{route}_plan_must_not_control_routes")
    if row.get("writes_coefficients") is not False:
        reasons.append(f"{route}_plan_must_not_write_coefficients")
    if "--execute-controlled-writes" not in joined:
        reasons.append(f"{route}_forbidden_write_flag_not_documented")
    return reasons


def build_v1_5_algorithm_queue_handoff_preflight(
    *,
    profile_runner_dry_run_json: str | Path,
    runner_integration_dry_run_json: str | Path | None = None,
) -> dict[str, Any]:
    profile_json = Path(profile_runner_dry_run_json).resolve()
    profile_model = _read_json(profile_json)
    runner_json = (
        Path(runner_integration_dry_run_json).resolve()
        if runner_integration_dry_run_json
        else _nested_runner_json_from_profile(profile_model, profile_json)
    )
    runner_model = _read_json(runner_json)
    checks: list[QueueHandoffPreflightCheck] = []

    profile_reasons: list[str] = []
    if not profile_json.exists():
        profile_reasons.append("profile_runner_dry_run_json_missing")
    if profile_model.get("overall_status") != "ready_for_profile_driven_runner_dry_run_review":
        profile_reasons.append(f"profile_status={profile_model.get('overall_status') or 'missing'}")
    if int(profile_model.get("blocker_count") or 0) != 0:
        profile_reasons.append(f"profile_blocker_count={profile_model.get('blocker_count')}")
    if profile_model.get("co2_runlist_count") != 47:
        profile_reasons.append("profile_co2_count_not_47")
    if profile_model.get("h2o_runlist_count") != 14:
        profile_reasons.append("profile_h2o_count_not_14")
    if profile_model.get("runner_integration_status") != "profile_driven_dry_run_bundle_only_not_runner_wired":
        profile_reasons.append("profile_runner_status_not_dry_run_bundle")
    for key in ("opens_com_ports", "connects_postgresql", "controls_water_or_gas_routes", "writes_coefficients", "writes_device_id"):
        if profile_model.get(key) is not False:
            profile_reasons.append(f"profile_{key}_must_be_false")
    for key in ("not_real_acceptance_evidence", "does_not_execute_commands", "does_not_modify_runners"):
        if profile_model.get(key) is not True:
            profile_reasons.append(f"profile_{key}_must_be_true")
    checks.append(
        _check(
            check="profile_runner_dry_run_bundle_gate",
            status="ready" if not profile_reasons else "blocker",
            evidence_role="algorithm_profile_runner_dry_run",
            evidence_path=profile_json,
            reasons=profile_reasons,
            physical_meaning="The queue handoff preflight starts from the offline profile dry-run bundle, not live queue execution.",
            next_action="Regenerate the profile runner dry-run bundle before any queue handoff review.",
            details={
                "overall_status": profile_model.get("overall_status"),
                "co2_runlist_count": profile_model.get("co2_runlist_count"),
                "h2o_runlist_count": profile_model.get("h2o_runlist_count"),
                "runner_integration_status": profile_model.get("runner_integration_status"),
            },
        )
    )

    runner_reasons: list[str] = []
    if not runner_json.exists():
        runner_reasons.append("runner_integration_dry_run_json_missing")
    if runner_model.get("overall_status") != "ready_for_runner_integration_dry_run_review":
        runner_reasons.append(f"runner_status={runner_model.get('overall_status') or 'missing'}")
    if int(runner_model.get("blocker_count") or 0) != 0:
        runner_reasons.append(f"runner_blocker_count={runner_model.get('blocker_count')}")
    if runner_model.get("runner_integration_status") != "dry_run_preview_only_not_runner_wired":
        runner_reasons.append("runner_integration_status_not_dry_run_preview")
    for key in ("opens_com_ports", "connects_postgresql", "controls_water_or_gas_routes", "writes_coefficients", "writes_device_id"):
        if runner_model.get(key) is not False:
            runner_reasons.append(f"runner_{key}_must_be_false")
    for key in ("not_real_acceptance_evidence", "does_not_execute_commands", "does_not_modify_runners"):
        if runner_model.get(key) is not True:
            runner_reasons.append(f"runner_{key}_must_be_true")
    checks.append(
        _check(
            check="runner_integration_dry_run_gate",
            status="ready" if not runner_reasons else "blocker",
            evidence_role="algorithm_runner_integration_dry_run",
            evidence_path=runner_json,
            reasons=runner_reasons,
            physical_meaning="The mature CO2/H2O queues may only be considered through a dry-run/no-prompt preview at this stage.",
            next_action="Do not wire or run mature queues until the dry-run gate is reviewed and separately authorized.",
            details={
                "overall_status": runner_model.get("overall_status"),
                "blocker_count": runner_model.get("blocker_count"),
                "planned_route_order": runner_model.get("planned_route_order"),
            },
        )
    )

    by_route = _route_plan_by_kind(runner_model)
    route_specs = (
        ("co2", CO2_QUEUE_RUNNER, 47),
        ("h2o", H2O_QUEUE_RUNNER, 14),
    )
    for route, runner, count in route_specs:
        row = by_route.get(route, {})
        reasons = _route_reasons(row, route=route, runner=runner, count=count)
        checks.append(
            _check(
                check=f"{route}_dry_run_no_prompt_handoff_gate",
                status="ready" if not reasons else "blocker",
                evidence_role=f"{route}_queue_handoff_preflight",
                evidence_path=str(row.get("queue_csv") or ""),
                reasons=reasons,
                physical_meaning=(
                    f"The new-algorithm {route.upper()} profile runlist may be handed to the mature "
                    "queue only as a dry-run/no-prompt review artifact in this phase."
                ),
                next_action=(
                    f"Keep {route.upper()} profile queue handoff in dry-run review until a separate "
                    "operator-authorized live package exists."
                ),
                details=dict(row),
            )
        )

    live_reasons: list[str] = []
    if runner_model.get("does_not_execute_commands") is not True:
        live_reasons.append("runner_model_allows_command_execution")
    if runner_model.get("does_not_modify_runners") is not True:
        live_reasons.append("runner_model_allows_runner_modification")
    plan_command_text = " ".join(
        str(row.get(key) or "")
        for row in runner_model.get("runner_integration_plan") or []
        if isinstance(row, Mapping)
        for key in ("command_preview", "required_cli_flags")
    )
    if any(flag in plan_command_text for flag in FORBIDDEN_LIVE_FLAGS):
        live_reasons.append("live_or_write_flag_found_in_executable_command_preview")
    checks.append(
        _check(
            check="live_queue_execution_lock",
            status="ready" if not live_reasons else "blocker",
            evidence_role="queue_handoff_live_lock",
            evidence_path=runner_json,
            reasons=live_reasons,
            physical_meaning="Passing this preflight permits only dry-run handoff review; it does not permit live CO2/H2O queue execution.",
            next_action="Create a separate reviewed live-run package before any real COM, route, gas, water, or queue execution.",
            details={
                "live_queue_execution_allowed": False,
                "dry_run_handoff_review_allowed": True,
                "required_cli_flags_before_live_review": ("--dry-run", "--no-prompt"),
            },
        )
    )

    blocker_count = sum(1 for row in checks if row.status == "blocker")
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": "blocked" if blocker_count else "ready_for_dry_run_queue_handoff_review",
        "blocker_count": blocker_count,
        "review_required_count": 0,
        "profile_id": profile_model.get("profile_id") or "absorption_ratio_shadow",
        "not_real_acceptance_evidence": True,
        "no_write": True,
        "opens_com_ports": False,
        "connects_postgresql": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_device_id": False,
        "does_not_execute_commands": True,
        "does_not_modify_runners": True,
        "dry_run_handoff_review_allowed": blocker_count == 0,
        "live_queue_execution_allowed": False,
        "database_import_allowed": False,
        "formal_release_allowed": False,
        "co2_runlist_count": profile_model.get("co2_runlist_count"),
        "h2o_runlist_count": profile_model.get("h2o_runlist_count"),
        "required_cli_flags": ("--dry-run", "--no-prompt"),
        "forbidden_live_flags": FORBIDDEN_LIVE_FLAGS,
        "next_action": (
            "Review the dry-run/no-prompt queue handoff evidence. Passing this gate does not authorize "
            "live mature queue execution, real COM, route control, coefficient writes, archive release, or database import."
        ),
        "source_paths": {
            "profile_runner_dry_run_json": _path_text(profile_json),
            "runner_integration_dry_run_json": _path_text(runner_json),
        },
        "checks": [row.to_json() for row in checks],
    }


def write_v1_5_algorithm_queue_handoff_preflight_outputs(
    model: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, Path]:
    out = Path(output_dir)
    paths = {
        "json": out / "v1_5_algorithm_queue_handoff_preflight.json",
        "checks_csv": out / "v1_5_algorithm_queue_handoff_preflight_checks.csv",
        "markdown": out / "V1_5_ALGORITHM_QUEUE_HANDOFF_PREFLIGHT.md",
    }
    _write_json(paths["json"], model)
    _write_csv(paths["checks_csv"], model.get("checks", []))
    lines = [
        "# V1.5 algorithm queue handoff preflight",
        "",
        "This offline guard prevents profile-generated 47/14 runlists from being mistaken for authorized live queue execution.",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- blocker_count: `{model.get('blocker_count')}`",
        f"- profile: `{model.get('profile_id')}`",
        f"- CO2/H2O runlist counts: `{model.get('co2_runlist_count')}` / `{model.get('h2o_runlist_count')}`",
        f"- dry_run_handoff_review_allowed: `{model.get('dry_run_handoff_review_allowed')}`",
        f"- live_queue_execution_allowed: `{model.get('live_queue_execution_allowed')}`",
        "- Required pre-live-review flags: `--dry-run --no-prompt`.",
        "- This guard does not execute queues, open COM ports, connect PostgreSQL, control gas/water routes, write SN/device IDs, write coefficients, release archives, or import databases.",
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
