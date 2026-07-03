"""Offline V1.5 profile-driven new-algorithm runner dry-run bundle.

This sidecar composes the existing point-plan, runlist readiness, and runner
integration dry-run layers from the algorithm profile. It is intentionally
offline: it does not execute formal queue runners, open COM ports, control
routes, connect PostgreSQL, write SN/device IDs, or write SENCO coefficients.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_algorithm_route_profiles import write_v1_5_algorithm_formal_runlist_preview
from .v1_5_algorithm_runlist_readiness import (
    build_v1_5_algorithm_runlist_readiness,
    write_v1_5_algorithm_runlist_readiness_outputs,
)
from .v1_5_algorithm_runner_integration_dry_run import (
    build_v1_5_algorithm_runner_integration_dry_run,
    write_v1_5_algorithm_runner_integration_dry_run_outputs,
)


SCHEMA = "v1_5_algorithm_profile_runner_dry_run_v1"
RUNLIST_DIR = "algorithm_formal_runlist_preview"
READINESS_DIR = "algorithm_runlist_readiness"
DRY_RUN_DIR = "algorithm_runner_integration_dry_run"


@dataclass(frozen=True)
class ProfileRunnerDryRunCheck:
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


def _status_from_blockers(blocker_count: int) -> str:
    return "blocked" if blocker_count else "ready_for_profile_driven_runner_dry_run_review"


def _check(
    *,
    check: str,
    status: str,
    evidence_role: str,
    evidence_path: str | Path,
    reasons: Sequence[str] = (),
    physical_meaning: str,
    next_action: str,
    details: Mapping[str, Any],
) -> ProfileRunnerDryRunCheck:
    path_text = str(evidence_path) if isinstance(evidence_path, str) else _path_text(evidence_path)
    return ProfileRunnerDryRunCheck(
        check=check,
        status=status,
        evidence_role=evidence_role,
        evidence_path=path_text,
        reasons=tuple(reasons),
        physical_meaning=physical_meaning,
        next_action=next_action,
        details=details,
    )


def build_v1_5_algorithm_profile_runner_dry_run(
    *,
    profile_path: str | Path,
    output_dir: str | Path,
    profile_id: str = "absorption_ratio_shadow",
) -> dict[str, Any]:
    """Build a no-COM profile-to-runner dry-run bundle for reviewer use."""

    root = Path(output_dir)
    runlist_dir = root / RUNLIST_DIR
    readiness_dir = root / READINESS_DIR
    dry_run_dir = root / DRY_RUN_DIR

    runlist_outputs = write_v1_5_algorithm_formal_runlist_preview(
        profile_path,
        runlist_dir,
        profile_id=profile_id,
    )
    readiness = build_v1_5_algorithm_runlist_readiness(runlist_dir=runlist_dir)
    readiness_outputs = write_v1_5_algorithm_runlist_readiness_outputs(readiness, readiness_dir)
    dry_run = build_v1_5_algorithm_runner_integration_dry_run(
        readiness_dir=readiness_dir,
        runlist_dir=runlist_dir,
    )
    dry_run_outputs = write_v1_5_algorithm_runner_integration_dry_run_outputs(dry_run, dry_run_dir)

    checks: list[ProfileRunnerDryRunCheck] = []
    runlist_reasons: list[str] = []
    manifest_path = Path(runlist_outputs["manifest"])
    if not manifest_path.exists():
        runlist_reasons.append("runlist_preview_manifest_missing")
    if Path(runlist_outputs["co2_runlist"]).exists() is not True:
        runlist_reasons.append("co2_runlist_missing")
    if Path(runlist_outputs["h2o_runlist"]).exists() is not True:
        runlist_reasons.append("h2o_runlist_missing")
    checks.append(
        _check(
            check="formal_runlist_preview_generation",
            status="ready" if not runlist_reasons else "blocker",
            evidence_role="algorithm_formal_runlist_preview",
            evidence_path=manifest_path,
            reasons=runlist_reasons,
            physical_meaning="The profile must first produce queue-compatible 47/14 preview CSVs without touching mature runners.",
            next_action="Regenerate the profile runlist preview before any readiness or runner dry-run review.",
            details={"outputs": runlist_outputs},
        )
    )

    readiness_reasons: list[str] = []
    if readiness.get("overall_status") != "ready_for_new_algorithm_runner_integration_review":
        readiness_reasons.append(f"readiness_status={readiness.get('overall_status') or 'missing'}")
    if int(readiness.get("blocker_count") or 0) != 0:
        readiness_reasons.append(f"readiness_blocker_count={readiness.get('blocker_count')}")
    checks.append(
        _check(
            check="runlist_readiness_gate",
            status="ready" if not readiness_reasons else "blocker",
            evidence_role="algorithm_runlist_readiness",
            evidence_path=Path(readiness_outputs["json"]),
            reasons=readiness_reasons,
            physical_meaning="The new-algorithm 47/14 runlist must pass the readiness gate before runner integration review.",
            next_action="Fix blocked readiness checks before planning any queue handoff.",
            details={
                "overall_status": readiness.get("overall_status"),
                "blocker_count": readiness.get("blocker_count"),
                "outputs": {key: str(value) for key, value in readiness_outputs.items()},
            },
        )
    )

    dry_run_reasons: list[str] = []
    if dry_run.get("overall_status") != "ready_for_runner_integration_dry_run_review":
        dry_run_reasons.append(f"dry_run_status={dry_run.get('overall_status') or 'missing'}")
    if int(dry_run.get("blocker_count") or 0) != 0:
        dry_run_reasons.append(f"dry_run_blocker_count={dry_run.get('blocker_count')}")
    checks.append(
        _check(
            check="runner_integration_dry_run_plan",
            status="ready" if not dry_run_reasons else "blocker",
            evidence_role="algorithm_runner_integration_dry_run",
            evidence_path=Path(dry_run_outputs["json"]),
            reasons=dry_run_reasons,
            physical_meaning="The runner handoff remains a dry-run/no-prompt command preview, not live queue execution.",
            next_action="Review the dry-run plan before any future profile-driven runner wiring package.",
            details={
                "overall_status": dry_run.get("overall_status"),
                "blocker_count": dry_run.get("blocker_count"),
                "planned_route_order": dry_run.get("planned_route_order"),
                "outputs": {key: str(value) for key, value in dry_run_outputs.items()},
            },
        )
    )

    boundary_reasons: list[str] = []
    if any(model.get("opens_com_ports") is not False for model in (readiness, dry_run)):
        boundary_reasons.append("sub_artifact_opens_com_ports")
    if any(model.get("connects_postgresql") is not False for model in (readiness, dry_run)):
        boundary_reasons.append("sub_artifact_connects_postgresql")
    if any(model.get("controls_water_or_gas_routes") is not False for model in (readiness, dry_run)):
        boundary_reasons.append("sub_artifact_controls_routes")
    if any(model.get("writes_coefficients") is not False for model in (readiness, dry_run)):
        boundary_reasons.append("sub_artifact_writes_coefficients")
    if dry_run.get("does_not_execute_commands") is not True:
        boundary_reasons.append("dry_run_command_execution_not_disabled")
    if dry_run.get("does_not_modify_runners") is not True:
        boundary_reasons.append("dry_run_runner_modification_not_disabled")
    checks.append(
        _check(
            check="profile_runner_dry_run_offline_boundary",
            status="ready" if not boundary_reasons else "blocker",
            evidence_role="profile_runner_dry_run_boundary",
            evidence_path=str(root.resolve()),
            reasons=boundary_reasons,
            physical_meaning="The profile-driven bundle must remain offline evidence, not a hidden production runner.",
            next_action="Keep live runner wiring as a separate reviewed package after this bundle is accepted.",
            details={
                "not_real_acceptance_evidence": True,
                "does_not_execute_commands": True,
                "does_not_modify_runners": True,
            },
        )
    )

    blocker_count = sum(1 for row in checks if row.status == "blocker")
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": _status_from_blockers(blocker_count),
        "blocker_count": blocker_count,
        "review_required_count": 0,
        "profile_id": profile_id,
        "not_real_acceptance_evidence": True,
        "no_write": True,
        "opens_com_ports": False,
        "connects_postgresql": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_device_id": False,
        "does_not_execute_commands": True,
        "does_not_modify_runners": True,
        "runner_integration_status": "profile_driven_dry_run_bundle_only_not_runner_wired",
        "co2_runlist_count": dry_run.get("co2_runlist_count"),
        "h2o_runlist_count": dry_run.get("h2o_runlist_count"),
        "planned_route_order": dry_run.get("planned_route_order"),
        "next_action": (
            "Review this profile-driven dry-run bundle. Passing it does not authorize COM, route control, "
            "coefficient writes, archive release, database import, or mature runner modification."
        ),
        "output_directories": {
            "runlist_preview": str(runlist_dir.resolve()),
            "runlist_readiness": str(readiness_dir.resolve()),
            "runner_integration_dry_run": str(dry_run_dir.resolve()),
        },
        "outputs": {
            "runlist_preview": runlist_outputs,
            "runlist_readiness": {key: str(value) for key, value in readiness_outputs.items()},
            "runner_integration_dry_run": {key: str(value) for key, value in dry_run_outputs.items()},
        },
        "checks": [row.to_json() for row in checks],
    }


def write_v1_5_algorithm_profile_runner_dry_run_outputs(
    model: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, Path]:
    out = Path(output_dir)
    paths = {
        "json": out / "v1_5_algorithm_profile_runner_dry_run.json",
        "checks_csv": out / "v1_5_algorithm_profile_runner_dry_run_checks.csv",
        "markdown": out / "V1_5_ALGORITHM_PROFILE_RUNNER_DRY_RUN.md",
    }
    _write_json(paths["json"], model)
    _write_csv(paths["checks_csv"], model.get("checks", []))
    lines = [
        "# V1.5 algorithm profile runner dry-run",
        "",
        "This is an offline profile-driven bundle for the new-algorithm 47/14 formal runlist path.",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- blocker_count: `{model.get('blocker_count')}`",
        f"- profile: `{model.get('profile_id')}`",
        f"- CO2/H2O runlist counts: `{model.get('co2_runlist_count')}` / `{model.get('h2o_runlist_count')}`",
        f"- runner_integration_status: `{model.get('runner_integration_status')}`",
        "- This bundle generates runlist preview, runlist readiness, and runner integration dry-run artifacts.",
        "- It does not execute commands, open COM ports, connect PostgreSQL, control routes, write SN/device IDs, write coefficients, release archives, import databases, or modify mature runners.",
        "",
        "## Output directories",
        "",
    ]
    for key, value in (model.get("output_directories") or {}).items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## Checks", ""])
    for row in model.get("checks", []):
        reasons = ";".join(row.get("reasons") or ())
        lines.append(f"- `{row.get('check')}`: `{row.get('status')}` {reasons}".rstrip())
    paths["markdown"].parent.mkdir(parents=True, exist_ok=True)
    paths["markdown"].write_text("\n".join(lines) + "\n", encoding="utf-8")
    return paths
