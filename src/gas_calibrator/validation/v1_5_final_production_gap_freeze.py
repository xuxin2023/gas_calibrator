"""Freeze the final V1.5 production gaps from a reviewed origin/main baseline."""

from __future__ import annotations

import csv
import json
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_full_flow_automation_closure import MATURE_FITTING_BASELINE, MATURE_ROUTE_BASELINE


SCHEMA = "v1_5_final_production_gap_freeze_v1"
READY_STATUS = "production_gap_scope_frozen_offline_replay_next"
REVIEW_STATUS = "production_gap_scope_freeze_review_required"

_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_EVIDENCE_PATHS = {
    "mature_route_contract": Path(
        "docs/v1_5_flow_contract/mature_route_contract/v1_5_mature_route_contract.json"
    ),
    "production_entrypoint_gate": Path(
        "docs/v1_5_flow_contract/production_entrypoint_gate/v1_5_production_entrypoint_gate.json"
    ),
    "component_qc_controlled_writer_design": Path(
        "docs/v1_5_flow_contract/historical_component_qc_controlled_writer_design/"
        "v1_5_historical_component_qc_controlled_writer_design.json"
    ),
    "algorithm_queue_handoff_preflight": Path(
        "docs/v1_5_flow_contract/algorithm_queue_handoff_preflight/"
        "v1_5_algorithm_queue_handoff_preflight.json"
    ),
    "postgresql18_controlled_import_design": Path(
        "docs/v1_5_flow_contract/formal_database_import_controlled_executor_design/"
        "v1_5_formal_database_import_controlled_executor_design.json"
    ),
    "superseded_full_flow_next_action_plan": Path(
        "docs/v1_5_flow_contract/full_flow_next_action_plan/v1_5_full_flow_next_action_plan.json"
    ),
}


@dataclass(frozen=True)
class GapRow:
    priority: int
    gap_id: str
    title: str
    status: str
    production_effect: str
    next_package: str
    done_when: str
    execution_boundary: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


CRITICAL_GAPS: tuple[GapRow, ...] = (
    GapRow(
        1,
        "legacy_full_flow_orchestrator_offline_replay",
        "旧算法全流程 orchestrator 离线 replay",
        "next",
        "Prove that one state machine interprets initialization through archive in the mature order.",
        "Add a historical-replay-only legacy 45/13 orchestrator binder.",
        "A single replay explains every stage, exact evidence consumed, hold reason, and next action without promoting replay to real acceptance.",
        "offline replay only; no COM, route, write, PostgreSQL, release, or import",
    ),
    GapRow(
        2,
        "production_component_qc_and_0613_fit_matrix",
        "生产 component-QC evaluator 与 0613 拟合策略矩阵",
        "pending",
        "Replace historical/manual QC ambiguity with canonical per-analyzer inputs for CO2/H2O fitting.",
        "Implement reviewed production QC evaluation and the 0613 multi-strategy no-write fit selector.",
        "Every fit input has component-matched QC, physical supersede/reject reasons, anchor roles, and a no-write candidate decision.",
        "calc first; no coefficient write in evaluator or strategy selection",
    ),
    GapRow(
        3,
        "unified_controlled_write_readback_reverify",
        "统一系数受控写入、读回与短复验闭环",
        "pending",
        "Close the gap between accepted coefficients and per-device post-write evidence.",
        "Unify old-value snapshot, authorization, write, GETCO readback, rollback, and short reverify.",
        "Write success and validation success are reported separately for S1-S9 and SENCOA/B as applicable.",
        "explicit authorization; paced writes; readback required; no implicit overwrite",
    ),
    GapRow(
        4,
        "new_algorithm_47_14_live_mature_queue_handoff",
        "新算法 47/14 点 live mature-queue 接入",
        "pending",
        "Move the reviewed absorption profile from dry-run handoff to live use without forking the physical route.",
        "Add a separately authorized profile-to-mature-queue live adapter.",
        "47 CO2 and 14 H2O points run through the same 0620/0621 runners; only fit input/R0/write contracts differ.",
        "do not modify legacy 45/13 default or mature point-internal route logic",
    ),
    GapRow(
        5,
        "postgresql18_controlled_import",
        "PostgreSQL 18 真实受控入库",
        "pending",
        "Persist released calibration evidence with SN/device_code identity and protocol-ID aliases.",
        "Implement the authorized transaction executor behind archive closure and dry-run validation.",
        "Schema, uniqueness, preview, transaction, readback, rollback, and import lineage pass before database_import_allowed becomes true.",
        "no import from route, fit, replay, or no-write evidence alone",
    ),
    GapRow(
        6,
        "final_offline_acceptance_suite",
        "最终 simulation/replay/parity/resilience 验收套件",
        "pending",
        "Demonstrate program-level closure without confusing offline evidence with real acceptance.",
        "Run the frozen legacy replay plus entrypoint, mature-route, fit-parity, writer, archive, and DB-lock suites.",
        "All offline suites pass and every artifact retains not_real_acceptance_evidence=true.",
        "offline evidence cannot unlock formal release, database import, or live execution",
    ),
    GapRow(
        7,
        "real_batch_acceptance_when_hardware_available",
        "真机批次 acceptance（设备可用时）",
        "hardware_deferred",
        "Provide the only valid final evidence that the complete production chain works on a real batch.",
        "Run a separately authorized 1-6 analyzer legacy batch, then a new-algorithm batch after its live handoff closes.",
        "Continuous mature routes, fitting, controlled writes, reverify, archive, and PostgreSQL import close with real evidence.",
        "requires explicit hardware authorization; not part of this offline package",
    ),
)

DEFERRED_ITEMS: tuple[dict[str, str], ...] = (
    {
        "item_id": "historical_component_qc_backfill_writer",
        "reason": "Historical 125-point repair is useful for archaeology but is not required to finish the current production orchestrator.",
        "resume_condition": "Only resume by explicit user approval or when a frozen replay blocker cannot be resolved from canonical evidence.",
    },
    {
        "item_id": "root_pollution_cleanup_and_v1_v2_deletion",
        "reason": "The root worktree remains quarantined; deleting historical folders is not needed for V1.5 production closure.",
        "resume_condition": "Handle as a separate retention/cleanup review after production automation closes.",
    },
    {
        "item_id": "noncritical_ui_report_polish",
        "reason": "Cosmetic expansion must not displace orchestration, fitting, writing, database, or acceptance work.",
        "resume_condition": "Resume after the final offline suite or for a concrete production usability defect.",
    },
)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return payload


def _evidence_checks(repo_root: Path) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    reasons: list[str] = []
    payloads: dict[str, Mapping[str, Any]] = {}
    for role, relative in _EVIDENCE_PATHS.items():
        path = repo_root / relative
        if not path.is_file():
            reasons.append(f"source_evidence_missing:{role}")
            rows.append({"role": role, "path": str(relative), "status": "missing"})
            continue
        try:
            payload = _read_json(path)
        except (OSError, ValueError, json.JSONDecodeError):
            reasons.append(f"source_evidence_invalid:{role}")
            rows.append({"role": role, "path": str(relative), "status": "invalid"})
            continue
        payloads[role] = payload
        rows.append({"role": role, "path": str(relative), "status": "bound"})

    mature = payloads.get("mature_route_contract", {}).get("manifest", {})
    if not isinstance(mature, Mapping) or mature.get("status") != "pass":
        reasons.append("mature_route_contract_not_pass")
    else:
        contract = mature.get("mature_route_contract", {})
        if not isinstance(contract, Mapping):
            reasons.append("mature_route_contract_payload_missing")
        else:
            if contract.get("legacy_co2_point_count") != 45:
                reasons.append("legacy_co2_point_count_not_45")
            if contract.get("legacy_h2o_wet_point_count") != 13:
                reasons.append("legacy_h2o_point_count_not_13")

    entrypoint = payloads.get("production_entrypoint_gate", {}).get("manifest", {})
    if not isinstance(entrypoint, Mapping) or entrypoint.get("status") != "pass":
        reasons.append("production_entrypoint_gate_not_pass")

    component = payloads.get("component_qc_controlled_writer_design", {})
    locks = component.get("locks", {}) if isinstance(component, Mapping) else {}
    if component.get("controlled_writer_design_ready") is not True:
        reasons.append("component_qc_controlled_writer_design_not_ready")
    if not isinstance(locks, Mapping):
        reasons.append("component_qc_controlled_writer_locks_missing")
    else:
        for key in (
            "component_qc_payload_evaluator_available",
            "atomic_create_only_writer_available",
            "writer_execution_supported",
            "historical_fit_allowed",
        ):
            if locks.get(key) is not False:
                reasons.append(f"component_qc_lock_not_false:{key}")

    algorithm = payloads.get("algorithm_queue_handoff_preflight", {})
    if algorithm.get("co2_runlist_count") != 47 or algorithm.get("h2o_runlist_count") != 14:
        reasons.append("new_algorithm_point_counts_not_47_14")
    if algorithm.get("live_queue_execution_allowed") is not False:
        reasons.append("new_algorithm_live_queue_not_locked")

    database = payloads.get("postgresql18_controlled_import_design", {})
    if database.get("production_postgresql_major") != 18:
        reasons.append("production_postgresql_major_not_18")
    for key in (
        "execution_supported",
        "real_import_execution_allowed",
        "database_import_allowed",
        "connects_postgresql",
    ):
        if database.get(key) is not False:
            reasons.append(f"database_design_lock_not_false:{key}")
    return rows, sorted(set(reasons))


def build_v1_5_final_production_gap_freeze(
    *, repository_root: str | Path, source_origin_main_commit: str
) -> dict[str, Any]:
    repo_root = Path(repository_root).resolve()
    source_commit = source_origin_main_commit.strip().lower()
    reasons: list[str] = []
    if not _SHA_RE.fullmatch(source_commit):
        reasons.append("source_origin_main_commit_invalid")
    evidence_rows, evidence_reasons = _evidence_checks(repo_root)
    reasons.extend(evidence_reasons)
    if len(CRITICAL_GAPS) not in {6, 7}:
        reasons.append("critical_gap_count_not_frozen_to_6_or_7")
    if CRITICAL_GAPS[0].gap_id != "legacy_full_flow_orchestrator_offline_replay":
        reasons.append("offline_legacy_orchestrator_replay_not_first")
    priorities = [row.priority for row in CRITICAL_GAPS]
    if priorities != list(range(1, len(CRITICAL_GAPS) + 1)):
        reasons.append("critical_gap_priorities_not_contiguous")
    reasons = sorted(set(reasons))
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": READY_STATUS if not reasons else REVIEW_STATUS,
        "scope_frozen": not reasons,
        "new_gap_requires_user_approval": True,
        "source_origin_main_commit": source_commit,
        "source_snapshot_policy": "latest_origin_main_before_this_freeze_package",
        "supersedes": [
            "v1_5_full_flow_automation_closure_v1_generated_20260710",
            "v1_5_full_flow_next_action_plan_v1_generated_20260710",
        ],
        "mature_fitting_baseline": MATURE_FITTING_BASELINE,
        "mature_route_baseline": MATURE_ROUTE_BASELINE,
        "legacy_point_counts": {"co2": 45, "h2o": 13},
        "new_algorithm_profile_point_counts": {"co2": 47, "h2o": 14},
        "critical_gap_count": len(CRITICAL_GAPS),
        "deferred_item_count": len(DEFERRED_ITEMS),
        "recommended_next_gap_id": CRITICAL_GAPS[0].gap_id,
        "review_reasons": reasons,
        "source_evidence": evidence_rows,
        "critical_gaps": [row.to_json() for row in CRITICAL_GAPS],
        "deferred_items": list(DEFERRED_ITEMS),
        "evidence_source": "offline_repository_review",
        "full_production_auto_allowed": False,
        "live_queue_execution_allowed": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_sn_or_device_code": False,
        "connects_postgresql": False,
        "not_real_acceptance_evidence": True,
    }


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(str(key))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _markdown(model: Mapping[str, Any]) -> str:
    lines = [
        "# V1.5 最终生产缺口冻结清单",
        "",
        f"- source origin/main: `{model['source_origin_main_commit']}`",
        f"- overall_status: `{model['overall_status']}`",
        f"- scope_frozen: `{str(model['scope_frozen']).lower()}`",
        f"- critical_gap_count: `{model['critical_gap_count']}`",
        f"- recommended_next_gap_id: `{model['recommended_next_gap_id']}`",
        f"- mature fitting baseline: `{model['mature_fitting_baseline']}`",
        f"- mature route baseline: `{model['mature_route_baseline']}`",
        "",
        "这份清单取代 2026-07-10 的旧 closure/next-action 快照。新增生产缺口必须得到用户明确批准。",
        "",
        "## 冻结的生产关键缺口",
        "",
        "| 优先级 | 状态 | gap_id | 工作 | 完成标准 |",
        "|---:|---|---|---|---|",
    ]
    for row in model["critical_gaps"]:
        lines.append(
            f"| {row['priority']} | `{row['status']}` | `{row['gap_id']}` | {row['title']} | {row['done_when']} |"
        )
    lines.extend(["", "## 延后项", ""])
    for row in model["deferred_items"]:
        lines.append(f"- `{row['item_id']}`: {row['reason']} {row['resume_condition']}")
    lines.extend(
        [
            "",
            "## 当前锁",
            "",
            "- 旧算法生产物理基准仅认 `0613` 拟合与 `0620/0621` clean mature route。",
            "- 旧算法保持 `45 CO2 / 13 H2O`；新算法候选保持 `47 / 14`。",
            "- `full_production_auto_allowed=false`。",
            "- `live_queue_execution_allowed=false`。",
            "- `formal_release_allowed=false`。",
            "- `database_import_allowed=false`。",
            "- 本包不开 COM、不控压力/气水路、不写系数、不连 PostgreSQL。",
            "- `not_real_acceptance_evidence=true`。",
            "",
        ]
    )
    if model.get("review_reasons"):
        lines.extend(["## Review Reasons", ""])
        lines.extend(f"- `{reason}`" for reason in model["review_reasons"])
        lines.append("")
    return "\n".join(lines)


def write_v1_5_final_production_gap_freeze(
    *, output_dir: str | Path, repository_root: str | Path, source_origin_main_commit: str
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    model = build_v1_5_final_production_gap_freeze(
        repository_root=repository_root,
        source_origin_main_commit=source_origin_main_commit,
    )
    paths = {
        "manifest": out / "v1_5_final_production_gap_freeze.json",
        "critical_gaps": out / "v1_5_final_production_critical_gaps.csv",
        "deferred_items": out / "v1_5_final_production_deferred_items.csv",
        "markdown": out / "V1_5_FINAL_PRODUCTION_GAP_FREEZE.md",
    }
    paths["manifest"].write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(paths["critical_gaps"], model["critical_gaps"])
    _write_csv(paths["deferred_items"], model["deferred_items"])
    paths["markdown"].write_text(_markdown(model), encoding="utf-8")
    return paths


__all__ = [
    "CRITICAL_GAPS",
    "DEFERRED_ITEMS",
    "READY_STATUS",
    "SCHEMA",
    "build_v1_5_final_production_gap_freeze",
    "write_v1_5_final_production_gap_freeze",
]
