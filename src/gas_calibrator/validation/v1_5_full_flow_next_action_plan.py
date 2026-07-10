"""Offline V1.5 full-flow next-action plan.

This reviewer turns the automation closure map into an ordered next-action
plan. It is intentionally offline: it does not open COM ports, control
gas/water routes, connect PostgreSQL, or write coefficients. Its job is to make
the next automation PR explicit without changing the 0613/0620/0621 mature
execution path.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from .v1_5_full_flow_automation_closure import (
    MATURE_FITTING_BASELINE,
    MATURE_ROUTE_BASELINE,
    build_v1_5_full_flow_automation_closure,
)


SCHEMA = "v1_5_full_flow_next_action_plan_v1"

NEXT_ACTIONS: tuple[dict[str, str], ...] = (
    {
        "action_id": "batch_initialization_closeout_pre_gas_evidence_index",
        "source_stage_id": "01_initialization_identity_runtime_closeout",
        "action_type": "offline_evidence_binder",
        "recommended_pr_scope": (
            "Bind per-batch SN/device_code, protocol ID, GETCO1-9, runtime, S5-S8 neutralization, "
            "S9 pressure readiness, and read-only COM closeout into one pre-gas evidence index."
        ),
        "done_when": (
            "A generated pre-gas evidence index can prove which batch is ready for mature CO2/H2O routes "
            "without reopening COM or writing any coefficient."
        ),
    },
    {
        "action_id": "pressure_s9_exception_and_reverify_evidence_index",
        "source_stage_id": "02_pressure_s9_readiness",
        "action_type": "offline_evidence_binder",
        "recommended_pr_scope": (
            "Normalize offset-only S9 and explicit linear-S9 exception evidence into pressure readiness "
            "without changing the mature route runners."
        ),
        "done_when": "S9 write/readback/reverify evidence is explicit per device and cannot be confused with route data.",
    },
    {
        "action_id": "route_physical_recovery_live_smoke_binding_contract",
        "source_stage_id": "03_route_physical_readiness_guard",
        "action_type": "offline_contract",
        "recommended_pr_scope": (
            "Define the reviewed mature-path smoke evidence packet required before a fresh continuous CO2/H2O run."
        ),
        "done_when": "PACE vent, pressure gauge, dewpoint, and fresh queue readiness can be reviewed without using diagnostic points as fit data.",
    },
    {
        "action_id": "mature_route_continuity_run_manifest_gate",
        "source_stage_id": "04_mature_legacy_co2_45_route",
        "action_type": "offline_guard",
        "recommended_pr_scope": (
            "Require a fresh continuous mature-run manifest before CO2/H2O route evidence can feed fitting or release."
        ),
        "done_when": "Segmented, retry, direct-recovery, and empty-manifest route attempts are blocked from formal continuous-run status.",
    },
    {
        "action_id": "0613_fit_strategy_matrix_no_write",
        "source_stage_id": "06_fit_strategy_review",
        "action_type": "offline_no_write_review",
        "recommended_pr_scope": (
            "Codify the 0613 multi-strategy fit review for CO2/H2O, including physical reject/supersede rules and anchor roles."
        ),
        "done_when": "Candidate coefficients are reviewed through one no-write strategy matrix before any controlled write package.",
    },
    {
        "action_id": "controlled_write_readback_reverify_bundle",
        "source_stage_id": "07_controlled_write_readback",
        "action_type": "controlled_write_contract",
        "recommended_pr_scope": (
            "Unify old-value snapshot, controlled write, GETCO readback, and short reverify references into one post-fit write bundle."
        ),
        "done_when": "Write success and validation success are reported separately per device and coefficient family.",
    },
    {
        "action_id": "archive_release_postgresql18_import_unlock_sequence",
        "source_stage_id": "09_archive_report_database",
        "action_type": "offline_import_gate",
        "recommended_pr_scope": (
            "Keep PostgreSQL 18 import locked behind archive release, dry-run, authorization, controlled executor, and readback evidence."
        ),
        "done_when": "Database import cannot run from route, fit, smoke, or no-write evidence alone.",
    },
)


@dataclass(frozen=True)
class NextActionRow:
    priority: int
    action_id: str
    source_stage_id: str
    action_type: str
    recommended_pr_scope: str
    allowed_scope: str
    forbidden_scope: str
    done_when: str
    blocks_full_auto: bool

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_closure(path: str | Path | None = None) -> dict[str, Any]:
    if not path:
        return build_v1_5_full_flow_automation_closure()
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("automation closure JSON must contain an object")
    return payload


def _closure_review_reasons(closure: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    if closure.get("overall_status") != "review_ready":
        reasons.append(f"closure_overall_status={closure.get('overall_status') or 'missing'}")
    if closure.get("automation_closure_status") != "structure_closed_live_full_auto_still_gated":
        reasons.append(f"automation_closure_status={closure.get('automation_closure_status') or 'missing'}")
    if closure.get("mature_fitting_baseline") != MATURE_FITTING_BASELINE:
        reasons.append("mature_fitting_baseline_mismatch")
    if closure.get("mature_route_baseline") != MATURE_ROUTE_BASELINE:
        reasons.append("mature_route_baseline_mismatch")
    if closure.get("legacy_point_counts") != {"co2": 45, "h2o": 13}:
        reasons.append("legacy_point_counts_not_45_13")
    if closure.get("new_algorithm_profile_point_counts") != {"co2": 47, "h2o": 14}:
        reasons.append("new_algorithm_point_counts_not_47_14")
    if closure.get("remaining_full_auto_gap_count", 0) <= 0:
        reasons.append("remaining_full_auto_gap_count_not_positive")
    for key in (
        "full_production_auto_allowed",
        "formal_release_allowed",
        "database_import_allowed",
        "opens_com_ports",
        "controls_water_or_gas_routes",
        "connects_postgresql",
        "writes_coefficients",
    ):
        if closure.get(key) is not False:
            reasons.append(f"{key}_not_false")
    if closure.get("not_real_acceptance_evidence") is not True:
        reasons.append("not_real_acceptance_evidence_not_true")
    return reasons


def _stage_lookup(closure: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {str(row.get("stage_id")): row for row in closure.get("stages") or [] if isinstance(row, Mapping)}


def build_v1_5_full_flow_next_action_plan(
    *,
    automation_closure_json: str | Path | None = None,
) -> dict[str, Any]:
    closure = _load_closure(automation_closure_json)
    reasons = _closure_review_reasons(closure)
    stages = _stage_lookup(closure)
    rows: list[NextActionRow] = []
    for idx, spec in enumerate(NEXT_ACTIONS, start=1):
        stage = stages.get(spec["source_stage_id"], {})
        rows.append(
            NextActionRow(
                priority=idx,
                action_id=spec["action_id"],
                source_stage_id=spec["source_stage_id"],
                action_type=spec["action_type"],
                recommended_pr_scope=spec["recommended_pr_scope"],
                allowed_scope="offline plan/evidence/status only unless a later dedicated controlled executor is explicitly reviewed",
                forbidden_scope="no COM, no gas/water route, no coefficient write, no PostgreSQL import, no formal release unlock",
                done_when=spec["done_when"],
                blocks_full_auto=bool(stage.get("blocks_full_auto", True)),
            )
        )
    overall_status = "review_ready" if not reasons else "review_required"
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": overall_status,
        "source_automation_closure_status": closure.get("automation_closure_status", ""),
        "source_blocker_count": closure.get("blocker_count", 0),
        "source_remaining_full_auto_gap_count": closure.get("remaining_full_auto_gap_count", 0),
        "mature_fitting_baseline": closure.get("mature_fitting_baseline", ""),
        "mature_route_baseline": closure.get("mature_route_baseline", ""),
        "legacy_point_counts": closure.get("legacy_point_counts", {}),
        "new_algorithm_profile_point_counts": closure.get("new_algorithm_profile_point_counts", {}),
        "recommended_next_action_id": rows[0].action_id if rows else "",
        "recommended_next_pr_scope": rows[0].recommended_pr_scope if rows else "",
        "review_reasons": reasons,
        "full_production_auto_allowed": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "connects_postgresql": False,
        "writes_coefficients": False,
        "not_real_acceptance_evidence": True,
        "next_actions": [row.to_json() for row in rows],
    }


def _markdown(model: Mapping[str, Any]) -> str:
    lines = [
        "# V1.5 Full-Flow Next-Action Plan",
        "",
        f"- schema: `{model['schema']}`",
        f"- overall_status: `{model['overall_status']}`",
        f"- recommended_next_action_id: `{model['recommended_next_action_id']}`",
        f"- mature_fitting_baseline: `{model['mature_fitting_baseline']}`",
        f"- mature_route_baseline: `{model['mature_route_baseline']}`",
        f"- full_production_auto_allowed: `{str(model['full_production_auto_allowed']).lower()}`",
        "",
        "## Meaning",
        "",
        "This plan ranks the remaining V1.5 automation handoffs. It does not execute the handoffs.",
        "",
        "## Next Actions",
        "",
        "| priority | action_id | action_type | recommended_pr_scope | done_when |",
        "|---:|---|---|---|---|",
    ]
    for row in model["next_actions"]:
        lines.append(
            "| {priority} | `{action_id}` | `{action_type}` | {scope} | {done_when} |".format(
                priority=row["priority"],
                action_id=row["action_id"],
                action_type=row["action_type"],
                scope=row["recommended_pr_scope"].replace("|", "/"),
                done_when=row["done_when"].replace("|", "/"),
            )
        )
    if model.get("review_reasons"):
        lines.extend(["", "## Review Reasons", ""])
        lines.extend(f"- `{reason}`" for reason in model["review_reasons"])
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


def write_v1_5_full_flow_next_action_plan(
    *,
    output_dir: str | Path,
    automation_closure_json: str | Path | None = None,
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    model = build_v1_5_full_flow_next_action_plan(automation_closure_json=automation_closure_json)
    paths = {
        "manifest": out / "v1_5_full_flow_next_action_plan.json",
        "actions": out / "v1_5_full_flow_next_action_plan_actions.csv",
        "markdown": out / "V1_5_FULL_FLOW_NEXT_ACTION_PLAN.md",
    }
    paths["manifest"].write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    with paths["actions"].open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=(
                "priority",
                "action_id",
                "source_stage_id",
                "action_type",
                "recommended_pr_scope",
                "allowed_scope",
                "forbidden_scope",
                "done_when",
                "blocks_full_auto",
            ),
        )
        writer.writeheader()
        writer.writerows(model["next_actions"])
    paths["markdown"].write_text(_markdown(model), encoding="utf-8")
    return paths


__all__ = [
    "NEXT_ACTIONS",
    "SCHEMA",
    "build_v1_5_full_flow_next_action_plan",
    "write_v1_5_full_flow_next_action_plan",
]
