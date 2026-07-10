"""Offline V1.5 full-flow automation closure map.

This reviewer keeps the V1.5 automation conversation grounded in the mature
0613/0620/0621 path. It does not execute COM, pressure, gas/water routes,
database imports, or coefficient writes. Its purpose is to make the current
automation boundary explicit: what is structurally closed, what is controlled
but live-capable only with authorization, and what remains locked.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


SCHEMA = "v1_5_full_flow_automation_closure_v1"

MATURE_FITTING_BASELINE = "0613 V1.5 fitting path"
MATURE_ROUTE_BASELINE = "0620/0621 clean-worktree mature physical route path"

PROHIBITED_FORMAL_SURFACES = (
    "_handoff",
    "D:/gas_calibrator root dirty/migration surface",
    "0624 migrated route path",
    "diagnostic-only tools",
    "sampling workers as top-level launchers",
    "legacy V1 launchers",
    "V2 launchers",
)

PROTECTED_CORE_FILES = (
    "src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py",
    "src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_queue.py",
    "src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py",
    "src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_sampling.py",
    "src/gas_calibrator/workflow/runner.py",
    "src/gas_calibrator/devices/gas_analyzer.py",
    "configs/default_config.json",
    "run_app.py",
)


@dataclass(frozen=True)
class AutomationClosureStage:
    stage_id: str
    title: str
    mature_baseline: str
    canonical_entrypoint: str
    automation_state: str
    live_boundary: str
    production_rule: str
    next_gap: str
    blocks_full_auto: bool

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AutomationClosureCheck:
    check_id: str
    status: str
    severity: str
    topic: str
    requirement: str
    evidence_rule: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _stages() -> list[AutomationClosureStage]:
    return [
        AutomationClosureStage(
            stage_id="01_initialization_identity_runtime_closeout",
            title="Initialization identity, runtime, and auxiliary neutralization",
            mature_baseline="V1.5 initialization owner plus controlled read-only COM closeout",
            canonical_entrypoint="run_v1_5_formal_initialization_runner.py",
            automation_state="controlled_real_readonly_available_with_authorization",
            live_boundary=(
                "Read-only COM can collect SN/GETCO/runtime evidence with explicit authorization; "
                "SN/device_code and S5/S6/S7/S8/S9 writes remain separate controlled actions."
            ),
            production_rule="Do not enter CO2/H2O routes until SN/device_code, protocol ID, GETCO1-9, S5-S8, and S9 are closed.",
            next_gap="Package the per-batch initialization closeout into the pre-gas evidence index automatically.",
            blocks_full_auto=True,
        ),
        AutomationClosureStage(
            stage_id="02_pressure_s9_readiness",
            title="Pressure and SENCO9 readiness",
            mature_baseline="PACE INL absolute pressure and V1.5 pressure/S9 review path",
            canonical_entrypoint="validate_pressure_only.py",
            automation_state="controlled_live_pressure_when_authorized",
            live_boundary="Pressure control is allowed only in the pressure stage; SENCO9 write/readback/reverify remains controlled.",
            production_rule="Use PACE INL absolute pressure. Do not let CO2/H2O fitting absorb pressure error.",
            next_gap="Keep offset-only default and linear S9 exceptions explicitly tagged with readback and pressure-only reverify.",
            blocks_full_auto=True,
        ),
        AutomationClosureStage(
            stage_id="03_route_physical_readiness_guard",
            title="Route physical readiness and recovery evidence",
            mature_baseline=MATURE_ROUTE_BASELINE,
            canonical_entrypoint="export_v1_5_route_physical_recovery_readiness.py",
            automation_state="offline_guard_only",
            live_boundary="The recovery binder consumes reviewed traces only; it does not collect dewpoint, vent, pressure, or route data.",
            production_rule="Route recovery evidence can unblock a fresh run, but diagnostic/smoke data cannot become formal fit data.",
            next_gap="For future continuous runs, gather live physical recovery evidence through reviewed mature-path smoke, then bind it offline.",
            blocks_full_auto=True,
        ),
        AutomationClosureStage(
            stage_id="04_mature_legacy_co2_45_route",
            title="CO2 mature open-flow route",
            mature_baseline=MATURE_ROUTE_BASELINE,
            canonical_entrypoint="run_v1_5_formal_co2_open_flow_queue.py",
            automation_state="mature_runner_real_route_when_authorized",
            live_boundary="Uses mature queue/worker only; do not use root migration or 0624 route kernel.",
            production_rule=(
                "Legacy analyzers run CO2 45 points. Single-analyzer ratio instability downgrades that analyzer/point; "
                "public physical gates block the point."
            ),
            next_gap="Add a pre-run continuity guard that refuses segmented carry-over as a formal continuous run.",
            blocks_full_auto=True,
        ),
        AutomationClosureStage(
            stage_id="05_mature_legacy_h2o_13_route",
            title="H2O mature open-flow route",
            mature_baseline=MATURE_ROUTE_BASELINE,
            canonical_entrypoint="run_v1_5_formal_h2o_open_flow_queue.py",
            automation_state="mature_runner_real_route_when_authorized",
            live_boundary="Uses mature water-route queue/worker only; do not use 0624 handoff queues as the mature baseline.",
            production_rule="Legacy analyzers run H2O 13 wet points; new algorithm 14-point plans stay profile/dry-run until reviewed live wiring.",
            next_gap="Require run manifests before declaring a water route segment formal; empty queue attempts remain rejected diagnostics.",
            blocks_full_auto=True,
        ),
        AutomationClosureStage(
            stage_id="06_fit_strategy_review",
            title="No-write fitting strategy review",
            mature_baseline=MATURE_FITTING_BASELINE,
            canonical_entrypoint="export_v1_5_fit_input_quality.py",
            automation_state="offline_no_write_review",
            live_boundary="No coefficient write is implied by a fit candidate.",
            production_rule=(
                "Use physical fit roles, current GETCO state, dry/low-water anchors with dewpoint/pressure evidence, "
                "and explicit reject/supersede reasoning."
            ),
            next_gap="Codify the 0613 multi-strategy fit review into one canonical no-write strategy matrix for CO2 and H2O.",
            blocks_full_auto=True,
        ),
        AutomationClosureStage(
            stage_id="07_controlled_write_readback",
            title="Controlled coefficient write and readback",
            mature_baseline="V1.5 controlled SENCO write tools with live GETCO snapshots",
            canonical_entrypoint="run_v1_5_*_controlled_write.py",
            automation_state="manual_authorized_controlled_write",
            live_boundary="Writes require explicit authorization, old-value snapshot, write, GETCO readback, and rollback/reverify plan.",
            production_rule="S5/S6 are final linear layers; S5 composition must use current GETCO5 and clear-before-write where required.",
            next_gap="Unify per-coefficient write summaries into one post-fit write package without hiding per-device exceptions.",
            blocks_full_auto=True,
        ),
        AutomationClosureStage(
            stage_id="08_short_reverify",
            title="Post-write short reverification",
            mature_baseline="0613/0620/0621 mature short-reverify practice",
            canonical_entrypoint="export_v1_5_post_write_reverification.py",
            automation_state="offline_review_from_real_samples",
            live_boundary="Reverify samples are collected by reviewed mature route/reverify commands; exporter only reviews evidence.",
            production_rule="Write success and validation success must be reported separately.",
            next_gap="Bind short reverify points to the exact coefficient write package and reject stale reverify evidence.",
            blocks_full_auto=True,
        ),
        AutomationClosureStage(
            stage_id="09_archive_report_database",
            title="Archive, reports, and PostgreSQL 18 import gate",
            mature_baseline="V1.5 archive closure plus PostgreSQL 18 dry-run/import lock chain",
            canonical_entrypoint="run_v1_5_formal_archive_closure.py",
            automation_state="archive_offline_ready_database_import_locked",
            live_boundary="Archive/report is offline; PostgreSQL import remains blocked unless the controlled import chain is explicitly completed.",
            production_rule="SN/device_code traceability, readback, reverify, archive release, and DB dry-run must all pass before import.",
            next_gap="Implement real PostgreSQL import only after archive release and controlled executor review are complete.",
            blocks_full_auto=True,
        ),
    ]


def _checks(stages: list[AutomationClosureStage]) -> list[AutomationClosureCheck]:
    return [
        AutomationClosureCheck(
            check_id="AUTO-CLOSURE-001",
            status="pass",
            severity="blocker",
            topic="baseline",
            requirement="Mature gas/water execution baseline is 0613 fitting plus 0620/0621 clean-worktree route behavior.",
            evidence_rule="Do not treat root migration, 0624, or segmented diagnostics as the formal mature baseline.",
        ),
        AutomationClosureCheck(
            check_id="AUTO-CLOSURE-002",
            status="pass",
            severity="blocker",
            topic="forbidden_surfaces",
            requirement="Formal automation plans must not reference _handoff, root migration, 0624, diagnostic, worker, V1, or V2 surfaces.",
            evidence_rule="Use production entrypoint gate before starting or accepting a formal plan.",
        ),
        AutomationClosureCheck(
            check_id="AUTO-CLOSURE-003",
            status="pass",
            severity="blocker",
            topic="route_core",
            requirement="This package must not edit mature route queue/worker, runner, analyzer protocol, default config, or app entry.",
            evidence_rule="Protected files remain review hotspots and must stay unchanged in this automation-map package.",
        ),
        AutomationClosureCheck(
            check_id="AUTO-CLOSURE-004",
            status="pass",
            severity="blocker",
            topic="legacy_vs_new_algorithm",
            requirement="Legacy remains CO2 45 and H2O 13; new algorithm 47/14 remains profile/dry-run unless separately live-reviewed.",
            evidence_rule="New-algorithm runlists must not contaminate legacy mature queues.",
        ),
        AutomationClosureCheck(
            check_id="AUTO-CLOSURE-005",
            status="pass",
            severity="blocker",
            topic="fit_and_write",
            requirement="Fit review is no-write; controlled write requires live old-value snapshot, write, readback, and short reverify.",
            evidence_rule="Do not use no-write math or diagnostic smoke data as release evidence.",
        ),
        AutomationClosureCheck(
            check_id="AUTO-CLOSURE-006",
            status="pass",
            severity="blocker",
            topic="full_auto_state",
            requirement="Current V1.5 is structurally organized but still not one-click full production automation.",
            evidence_rule="Every stage with blocks_full_auto=true remains a deliberate handoff/authorization/review point.",
        ),
        AutomationClosureCheck(
            check_id="AUTO-CLOSURE-007",
            status="pass",
            severity="blocker",
            topic="database",
            requirement="PostgreSQL 18 import remains after archive/release and controlled import gates.",
            evidence_rule="No database import is allowed from route, fit, smoke, or no-write evidence alone.",
        ),
    ]


def build_v1_5_full_flow_automation_closure() -> dict[str, Any]:
    stages = _stages()
    checks = _checks(stages)
    blocker_count = sum(1 for check in checks if check.status == "blocker")
    live_stage_count = sum(1 for stage in stages if "real" in stage.automation_state or "live" in stage.live_boundary)
    remaining_gap_count = sum(1 for stage in stages if stage.blocks_full_auto)
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": "review_ready",
        "automation_closure_status": "structure_closed_live_full_auto_still_gated",
        "mature_fitting_baseline": MATURE_FITTING_BASELINE,
        "mature_route_baseline": MATURE_ROUTE_BASELINE,
        "legacy_point_counts": {"co2": 45, "h2o": 13},
        "new_algorithm_profile_point_counts": {"co2": 47, "h2o": 14},
        "blocker_count": blocker_count,
        "remaining_full_auto_gap_count": remaining_gap_count,
        "controlled_or_live_capable_stage_count": live_stage_count,
        "full_production_auto_allowed": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "connects_postgresql": False,
        "writes_coefficients": False,
        "not_real_acceptance_evidence": True,
        "protected_core_files": list(PROTECTED_CORE_FILES),
        "prohibited_formal_surfaces": list(PROHIBITED_FORMAL_SURFACES),
        "stages": [stage.to_json() for stage in stages],
        "checks": [check.to_json() for check in checks],
    }


def _markdown(model: dict[str, Any]) -> str:
    lines = [
        "# V1.5 Full-Flow Automation Closure Map",
        "",
        f"- schema: `{model['schema']}`",
        f"- overall_status: `{model['overall_status']}`",
        f"- automation_closure_status: `{model['automation_closure_status']}`",
        f"- mature_fitting_baseline: `{model['mature_fitting_baseline']}`",
        f"- mature_route_baseline: `{model['mature_route_baseline']}`",
        f"- remaining_full_auto_gap_count: `{model['remaining_full_auto_gap_count']}`",
        f"- full_production_auto_allowed: `{str(model['full_production_auto_allowed']).lower()}`",
        "",
        "## Meaning",
        "",
        "This map says V1.5 structure and guardrails are organized, but full production automation is still gated by explicit live handoffs, controlled writes, short reverify, archive release, and PostgreSQL 18 import locks.",
        "",
        "## Automation Stages",
        "",
        "| stage | automation_state | entrypoint | production_rule | next_gap |",
        "|---|---|---|---|---|",
    ]
    for stage in model["stages"]:
        lines.append(
            "| `{stage_id}` | `{automation_state}` | `{canonical_entrypoint}` | {production_rule} | {next_gap} |".format(
                stage_id=stage["stage_id"],
                automation_state=stage["automation_state"],
                canonical_entrypoint=stage["canonical_entrypoint"],
                production_rule=stage["production_rule"].replace("|", "/"),
                next_gap=stage["next_gap"].replace("|", "/"),
            )
        )
    lines.extend(
        [
            "",
            "## Forbidden Formal Surfaces",
            "",
        ]
    )
    lines.extend(f"- `{item}`" for item in model["prohibited_formal_surfaces"])
    lines.extend(
        [
            "",
            "## Protected Core Files",
            "",
        ]
    )
    lines.extend(f"- `{path}`" for path in model["protected_core_files"])
    lines.extend(
        [
            "",
            "## Checks",
            "",
            "| check_id | status | severity | topic | requirement |",
            "|---|---|---|---|---|",
        ]
    )
    for check in model["checks"]:
        lines.append(
            "| `{check_id}` | `{status}` | `{severity}` | `{topic}` | {requirement} |".format(
                check_id=check["check_id"],
                status=check["status"],
                severity=check["severity"],
                topic=check["topic"],
                requirement=check["requirement"].replace("|", "/"),
            )
        )
    lines.extend(
        [
            "",
            "## Non-Execution Boundary",
            "",
            "- opens_com_ports: `false`",
            "- controls_water_or_gas_routes: `false`",
            "- connects_postgresql: `false`",
            "- writes_coefficients: `false`",
            "- formal_release_allowed: `false`",
            "- database_import_allowed: `false`",
            "- not_real_acceptance_evidence: `true`",
            "",
        ]
    )
    return "\n".join(lines)


def write_v1_5_full_flow_automation_closure(*, output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    model = build_v1_5_full_flow_automation_closure()
    paths = {
        "manifest": out / "v1_5_full_flow_automation_closure.json",
        "stages": out / "v1_5_full_flow_automation_closure_stages.csv",
        "checks": out / "v1_5_full_flow_automation_closure_checks.csv",
        "markdown": out / "V1_5_FULL_FLOW_AUTOMATION_CLOSURE.md",
    }
    paths["manifest"].write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    with paths["stages"].open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=(
                "stage_id",
                "title",
                "mature_baseline",
                "canonical_entrypoint",
                "automation_state",
                "live_boundary",
                "production_rule",
                "next_gap",
                "blocks_full_auto",
            ),
        )
        writer.writeheader()
        writer.writerows(model["stages"])
    with paths["checks"].open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=("check_id", "status", "severity", "topic", "requirement", "evidence_rule"),
        )
        writer.writeheader()
        writer.writerows(model["checks"])
    paths["markdown"].write_text(_markdown(model), encoding="utf-8")
    return paths


__all__ = [
    "MATURE_FITTING_BASELINE",
    "MATURE_ROUTE_BASELINE",
    "PROHIBITED_FORMAL_SURFACES",
    "PROTECTED_CORE_FILES",
    "SCHEMA",
    "build_v1_5_full_flow_automation_closure",
    "write_v1_5_full_flow_automation_closure",
]
