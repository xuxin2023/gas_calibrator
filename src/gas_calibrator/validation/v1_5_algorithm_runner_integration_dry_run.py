"""Offline V1.5 new-algorithm runner integration dry-run.

This sidecar turns a ready 47/14 runlist preview into a reviewer-facing runner
integration plan. It does not invoke the formal queue runners. It only records
which mature queue entrypoint would receive which CSV under dry-run/no-prompt
conditions in a later integration step.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "v1_5_algorithm_runner_integration_dry_run_v1"
READINESS_JSON = "v1_5_algorithm_runlist_readiness.json"
CO2_RUNLIST = "v1_5_new_algorithm_formal_co2_runlist_preview.csv"
H2O_RUNLIST = "v1_5_new_algorithm_formal_h2o_runlist_preview.csv"
CO2_QUEUE_RUNNER = "gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue"
H2O_QUEUE_RUNNER = "gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue"


@dataclass(frozen=True)
class RunnerIntegrationDryRunCheck:
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
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    return payload if isinstance(payload, dict) else {}


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


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


def _command_preview(*, runner_module: str, queue_csv: Path, route_kind: str) -> str:
    output = f"<future-output-dir>\\{route_kind}_queue_dry_run"
    return (
        f"python -m {runner_module} --config <runtime-config-json> "
        f"--queue-csv {queue_csv} --output-dir {output} --dry-run --no-prompt"
    )


def _runner_plan_row(
    *,
    route_kind: str,
    stage_order: int,
    runner_module: str,
    queue_csv: Path,
    expected_point_count: int,
    observed_point_count: int,
) -> dict[str, Any]:
    return {
        "stage_order": stage_order,
        "route_kind": route_kind,
        "runner_module": runner_module,
        "queue_csv": _path_text(queue_csv),
        "expected_point_count": expected_point_count,
        "observed_point_count": observed_point_count,
        "required_cli_flags": "--dry-run;--no-prompt",
        "forbidden_cli_flags": "--execute-controlled-writes;--execute-real-run;--write-coefficients",
        "command_preview": _command_preview(
            runner_module=runner_module,
            queue_csv=queue_csv,
            route_kind=route_kind,
        ),
        "runner_integration_status": "dry_run_preview_only_not_runner_wired",
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "physical_meaning": (
            "Use the mature V1.5 queue entrypoint with the profile-generated "
            "runlist CSV under dry-run/no-prompt conditions before any live runner wiring."
        ),
    }


def _check_queue_plan(row: Mapping[str, Any], *, expected_runner: str, expected_count: int) -> tuple[bool, tuple[str, ...]]:
    reasons: list[str] = []
    command = str(row.get("command_preview") or "")
    if row.get("runner_module") != expected_runner:
        reasons.append("unexpected_runner_module")
    if int(row.get("observed_point_count") or 0) != expected_count:
        reasons.append(f"observed_point_count={row.get('observed_point_count')}")
    if "--dry-run" not in command:
        reasons.append("dry_run_flag_missing")
    if "--no-prompt" not in command:
        reasons.append("no_prompt_flag_missing")
    forbidden_text = " ".join(str(row.get(key) or "") for key in ("command_preview", "required_cli_flags"))
    if "--execute-controlled-writes" in forbidden_text or "--write-coefficients" in forbidden_text:
        reasons.append("write_or_execute_flag_present")
    if row.get("opens_com_ports") is not False:
        reasons.append("plan_must_not_open_com_ports")
    if row.get("controls_water_or_gas_routes") is not False:
        reasons.append("plan_must_not_control_routes")
    if row.get("writes_coefficients") is not False:
        reasons.append("plan_must_not_write_coefficients")
    return not reasons, tuple(reasons)


def build_v1_5_algorithm_runner_integration_dry_run(
    *,
    readiness_dir: str | Path,
    runlist_dir: str | Path,
    readiness_json: str | Path | None = None,
    co2_runlist_csv: str | Path | None = None,
    h2o_runlist_csv: str | Path | None = None,
) -> dict[str, Any]:
    readiness_root = Path(readiness_dir).resolve()
    runlist_root = Path(runlist_dir).resolve()
    readiness_file = Path(readiness_json).resolve() if readiness_json else readiness_root / READINESS_JSON
    co2_file = Path(co2_runlist_csv).resolve() if co2_runlist_csv else runlist_root / CO2_RUNLIST
    h2o_file = Path(h2o_runlist_csv).resolve() if h2o_runlist_csv else runlist_root / H2O_RUNLIST

    readiness = _read_json(readiness_file)
    co2_rows = _read_csv(co2_file)
    h2o_rows = _read_csv(h2o_file)
    checks: list[RunnerIntegrationDryRunCheck] = []

    readiness_reasons: list[str] = []
    if not readiness_file.exists():
        readiness_reasons.append("algorithm_runlist_readiness_json_missing")
    if readiness.get("overall_status") != "ready_for_new_algorithm_runner_integration_review":
        readiness_reasons.append(f"readiness_status={readiness.get('overall_status') or 'missing'}")
    if int(readiness.get("blocker_count") or 0) != 0:
        readiness_reasons.append(f"readiness_blocker_count={readiness.get('blocker_count')}")
    if readiness.get("opens_com_ports") is not False:
        readiness_reasons.append("readiness_must_not_open_com")
    if readiness.get("controls_water_or_gas_routes") is not False:
        readiness_reasons.append("readiness_must_not_control_routes")
    if readiness.get("writes_coefficients") is not False:
        readiness_reasons.append("readiness_must_not_write_coefficients")
    checks.append(
        RunnerIntegrationDryRunCheck(
            check="algorithm_runlist_readiness_gate",
            status="ready" if not readiness_reasons else "blocker",
            evidence_role="algorithm_runlist_readiness",
            evidence_path=_path_text(readiness_file),
            reasons=tuple(readiness_reasons),
            physical_meaning="Runner integration dry-run is only meaningful after the 47/14 runlist readiness gate is clean.",
            next_action="Regenerate or repair runlist readiness before planning any runner integration.",
            details={
                "overall_status": readiness.get("overall_status"),
                "blocker_count": readiness.get("blocker_count"),
            },
        )
    )

    plan = [
        _runner_plan_row(
            route_kind="co2",
            stage_order=1,
            runner_module=CO2_QUEUE_RUNNER,
            queue_csv=co2_file,
            expected_point_count=47,
            observed_point_count=len(co2_rows),
        ),
        _runner_plan_row(
            route_kind="h2o",
            stage_order=2,
            runner_module=H2O_QUEUE_RUNNER,
            queue_csv=h2o_file,
            expected_point_count=14,
            observed_point_count=len(h2o_rows),
        ),
    ]

    co2_ok, co2_reasons = _check_queue_plan(plan[0], expected_runner=CO2_QUEUE_RUNNER, expected_count=47)
    checks.append(
        RunnerIntegrationDryRunCheck(
            check="co2_queue_runner_dry_run_plan",
            status="ready" if co2_ok else "blocker",
            evidence_role="co2_runner_integration_dry_run_plan",
            evidence_path=_path_text(co2_file),
            reasons=co2_reasons,
            physical_meaning="The new-algorithm CO2 47-point runlist should feed the mature CO2 queue only under dry-run review conditions.",
            next_action="Do not wire CO2 runner until this dry-run plan is reviewed and the readiness gate remains clean.",
            details=plan[0],
        )
    )

    h2o_ok, h2o_reasons = _check_queue_plan(plan[1], expected_runner=H2O_QUEUE_RUNNER, expected_count=14)
    checks.append(
        RunnerIntegrationDryRunCheck(
            check="h2o_queue_runner_dry_run_plan",
            status="ready" if h2o_ok else "blocker",
            evidence_role="h2o_runner_integration_dry_run_plan",
            evidence_path=_path_text(h2o_file),
            reasons=h2o_reasons,
            physical_meaning="The new-algorithm H2O 14-point runlist should feed the mature H2O queue only under dry-run review conditions.",
            next_action="Do not wire H2O runner until this dry-run plan is reviewed and humidity reference bridge requirements are preserved.",
            details=plan[1],
        )
    )

    boundary_reasons: list[str] = []
    if any(row.get("runner_integration_status") != "dry_run_preview_only_not_runner_wired" for row in plan):
        boundary_reasons.append("runner_integration_status_not_dry_run_preview")
    if any(row.get("opens_com_ports") is not False for row in plan):
        boundary_reasons.append("plan_opens_com_ports")
    if any(row.get("controls_water_or_gas_routes") is not False for row in plan):
        boundary_reasons.append("plan_controls_routes")
    if any(row.get("writes_coefficients") is not False for row in plan):
        boundary_reasons.append("plan_writes_coefficients")
    checks.append(
        RunnerIntegrationDryRunCheck(
            check="integration_dry_run_boundary",
            status="ready" if not boundary_reasons else "blocker",
            evidence_role="runner_integration_boundary",
            evidence_path=";".join(path for path in (_path_text(co2_file), _path_text(h2o_file)) if path),
            reasons=tuple(boundary_reasons),
            physical_meaning="This integration artifact must remain a plan, not an executable production run.",
            next_action="Keep formal runner wiring as a separate reviewed package after dry-run evidence is accepted.",
            details={
                "not_real_acceptance_evidence": True,
                "does_not_modify_runners": True,
                "does_not_execute_commands": True,
            },
        )
    )

    blocker_count = sum(1 for row in checks if row.status == "blocker")
    status = "blocked" if blocker_count else "ready_for_runner_integration_dry_run_review"
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": status,
        "blocker_count": blocker_count,
        "review_required_count": 0,
        "profile_id": readiness.get("profile_id") or "absorption_ratio_shadow",
        "not_real_acceptance_evidence": True,
        "no_write": True,
        "opens_com_ports": False,
        "connects_postgresql": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_device_id": False,
        "does_not_execute_commands": True,
        "does_not_modify_runners": True,
        "runner_integration_status": "dry_run_preview_only_not_runner_wired",
        "co2_runlist_count": len(co2_rows),
        "h2o_runlist_count": len(h2o_rows),
        "planned_route_order": ["co2", "h2o"],
        "next_action": (
            "Review this dry-run plan before any profile-driven runner wiring. Passing it does not authorize "
            "COM, route control, coefficient writes, archive release, or database import."
        ),
        "runner_integration_plan": plan,
        "checks": [row.to_json() for row in checks],
    }


def write_v1_5_algorithm_runner_integration_dry_run_outputs(
    model: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, Path]:
    out = Path(output_dir)
    paths = {
        "json": out / "v1_5_algorithm_runner_integration_dry_run.json",
        "plan_csv": out / "v1_5_algorithm_runner_integration_dry_run_plan.csv",
        "checks_csv": out / "v1_5_algorithm_runner_integration_dry_run_checks.csv",
        "markdown": out / "V1_5_ALGORITHM_RUNNER_INTEGRATION_DRY_RUN.md",
    }
    _write_json(paths["json"], model)
    _write_csv(paths["plan_csv"], model.get("runner_integration_plan", []))
    _write_csv(paths["checks_csv"], model.get("checks", []))
    lines = [
        "# V1.5 algorithm runner integration dry-run",
        "",
        "This is an offline runner integration plan for the new-algorithm 47/14 runlist preview.",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- blocker_count: `{model.get('blocker_count')}`",
        f"- CO2/H2O runlist counts: `{model.get('co2_runlist_count')}` / `{model.get('h2o_runlist_count')}`",
        "- Planned route order: `co2 -> h2o`.",
        "- Commands are preview strings only and include `--dry-run --no-prompt`.",
        "- This sidecar does not execute commands, open COM ports, control routes, write coefficients, release archives, or import databases.",
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
