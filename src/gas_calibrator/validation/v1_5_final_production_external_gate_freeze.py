"""Freeze the remaining external gates after V1.5 program closure."""

from __future__ import annotations

import csv
import json
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "v1_5_final_production_external_gate_freeze_v1"
READY_STATUS = "program_automation_complete_real_production_evidence_pending"
REVIEW_STATUS = "final_production_external_gate_freeze_review_required"

_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_JSON_EVIDENCE_PATHS = {
    "previous_gap_freeze": Path(
        "docs/v1_5_flow_contract/final_production_gap_freeze/"
        "v1_5_final_production_gap_freeze.json"
    ),
    "legacy_full_flow_offline_replay": Path(
        "docs/v1_5_flow_contract/legacy_full_flow_offline_replay/"
        "v1_5_legacy_full_flow_offline_replay.json"
    ),
    "production_component_qc_fit_matrix": Path(
        "docs/v1_5_flow_contract/production_component_qc_fit_matrix/"
        "v1_5_production_component_qc_fit_matrix.json"
    ),
    "unified_controlled_write_reverify": Path(
        "docs/v1_5_flow_contract/unified_controlled_write_reverify/"
        "v1_5_unified_controlled_write_readback_reverify.json"
    ),
    "new_algorithm_mature_queue_live_handoff": Path(
        "docs/v1_5_flow_contract/new_algorithm_mature_queue_live_handoff/"
        "v1_5_new_algorithm_mature_queue_live_handoff.json"
    ),
    "final_offline_acceptance_suite": Path(
        "docs/v1_5_flow_contract/final_offline_acceptance_suite/"
        "v1_5_final_offline_acceptance_suite.json"
    ),
    "production_migration_executor": Path(
        "docs/v1_5_flow_contract/formal_database_migration_production_controlled_executor/"
        "v1_5_formal_database_migration_production_controlled_executor.json"
    ),
}
_STAGING_INTEGRATION_EVIDENCE = Path(
    "docs/v1_5_flow_contract/formal_database_import_staging_executor/"
    "V1_5_FORMAL_DATABASE_IMPORT_STAGING_REAL_INTEGRATION_TEST_EVIDENCE_20260714.md"
)


@dataclass(frozen=True)
class ProgramCapability:
    capability_id: str
    title: str
    status: str
    evidence_role: str
    production_meaning: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExternalGate:
    priority: int
    gate_id: str
    title: str
    status: str
    blocked_by: str
    done_when: str
    execution_boundary: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


PROGRAM_CAPABILITIES: tuple[ProgramCapability, ...] = (
    ProgramCapability(
        "mature_route_and_entrypoint_protection",
        "0613 拟合与 0620/0621 成熟物理路径保护",
        "implemented_and_tested",
        "previous_gap_freeze",
        "正式流程只认成熟 V1.5 路径，迁移版、0624、diagnostic、worker、V1/V2 均不得升格。",
    ),
    ProgramCapability(
        "legacy_full_flow_orchestrator_offline_replay",
        "旧算法 45/13 全流程 orchestrator 离线 replay",
        "implemented_and_tested_real_evidence_held",
        "legacy_full_flow_offline_replay",
        "程序可解释初始化到归档的完整顺序，但不会把离线 replay 冒充真机 acceptance。",
    ),
    ProgramCapability(
        "production_component_qc_and_0613_fit_matrix",
        "生产 component-QC 与 0613 多策略拟合矩阵",
        "implemented_and_tested_current_batch_continuity_required",
        "production_component_qc_fit_matrix",
        "程序已具备逐设备、逐组件 QC 和 no-write 策略评审；真实拟合仍需连续批次证据。",
    ),
    ProgramCapability(
        "unified_controlled_write_readback_reverify",
        "统一系数写入、GETCO 读回、回滚和短复验状态机",
        "implemented_contract_ready_candidate_required",
        "unified_controlled_write_reverify",
        "写入成功与复验成功已分离建模；没有获批候选时绝不允许写设备。",
    ),
    ProgramCapability(
        "new_algorithm_47_14_mature_queue_handoff",
        "新算法 47/14 成熟队列 handoff 合同",
        "implemented_contract_ready_live_locked",
        "new_algorithm_mature_queue_live_handoff",
        "新旧算法共用 0620/0621 点内物理路径；新算法 live 执行仍需独立授权与真机 smoke。",
    ),
    ProgramCapability(
        "postgresql18_staging_migration_import_chain",
        "PostgreSQL 18 staging、migration 002 与 production import 链",
        "implemented_and_real_staging_verified_production_locked",
        "production_migration_executor",
        "真实隔离 staging 已验证原子性、幂等、查询和回滚；生产迁移与入库仍分别授权。",
    ),
    ProgramCapability(
        "final_offline_acceptance_suite",
        "最终 simulation/replay/parity/resilience 离线验收",
        "passed_real_acceptance_locked",
        "final_offline_acceptance_suite",
        "程序级验收已通过，但所有离线证据继续标记为非 real acceptance。",
    ),
)


REMAINING_EXTERNAL_GATES: tuple[ExternalGate, ...] = (
    ExternalGate(
        1,
        "production_postgresql18_migration_002_authorization_and_execution",
        "生产 PostgreSQL 18 migration 002 三方授权与受控执行",
        "external_authorization_required",
        "current operator/reviewer/approver identities and signed authorization packet",
        "受控 executor 在固定 gas_calibrator/v1_5_evidence 目标执行 migration 002，保留 pre/apply/post、cluster system_identifier 和确认 artifact。",
        "不得虚构三方身份；不得由 production importer 代做 migration；失败或 commit uncertain 必须 hold。",
    ),
    ExternalGate(
        2,
        "current_batch_continuous_mature_route_evidence",
        "当前 1-6 台真实批次连续成熟路径证据",
        "hardware_required",
        "connected analyzers and separately authorized production run",
        "旧算法批次完成连续 45 CO2/13 H2O；新算法批次在获批后完成 47/14，且点内路径均为 0620/0621。",
        "不能用 segmented/retry/direct-recovery/0624/migration 路径拼成连续正式证据。",
    ),
    ExternalGate(
        3,
        "current_batch_fit_candidate_approval",
        "当前批次 QC、0613 多策略拟合与候选批准",
        "blocked_by_current_batch_evidence",
        "gate 2 continuous evidence and reviewer-approved fit candidates",
        "每台设备的 CO2/H2O 输入、剔除/替代原因、低端锚点和最大误差均可追溯，并产生 no-write approved candidate。",
        "CO2 zero gas 与 H2O dry-gas anchor 必须分开；没有候选不得进入写入。",
    ),
    ExternalGate(
        4,
        "device_controlled_write_readback_short_reverify",
        "真实设备受控写入、读回与短复验",
        "blocked_by_fit_candidate_and_hardware",
        "gate 3 approved candidates, live hardware, and explicit write authorization",
        "按槽位快照旧值、节拍写入、GETCO 读回、必要时回滚，并将 write success 与 validation success 分开记录。",
        "不得隐式覆盖；SENCO5 必须基于当前 GETCO5 做组合变换；失败不得刷新 release/import。",
    ),
    ExternalGate(
        5,
        "new_algorithm_47_14_live_smoke_and_batch_acceptance",
        "新算法 47/14 live handoff smoke 与批次 acceptance",
        "hardware_and_separate_authorization_required",
        "new-algorithm hardware, R0/write readiness, and separate live authorization",
        "先以最小 smoke 证明 profile 只改变点表/拟合/R0/write contract，再完成真实 47/14 批次。",
        "不得修改旧算法 45/13 默认队列或 0620/0621 点内物理动作。",
    ),
    ExternalGate(
        6,
        "production_evidence_import_archive_and_release",
        "生产证据入库、读回、归档和正式 release",
        "blocked_by_migration_real_archive_and_separate_authorization",
        "gate 1 migration artifact, real archive closure, and a fresh import authorization packet",
        "production importer 绑定 promotion/plan/bundle/migration 四个哈希和同一 cluster system_identifier，原子入库并查询读回后再刷新归档/状态。",
        "migration、import、formal release 是独立门禁；入库成功本身不等于校准 release。",
    ),
)


DEFERRED_ITEMS: tuple[dict[str, str], ...] = (
    {
        "item_id": "root_pollution_cleanup_and_v1_v2_deletion",
        "reason": "根目录继续隔离，不影响 clean V1.5 正式路径。",
        "resume_condition": "生产闭环后单独做保留策略和删除评审。",
    },
    {
        "item_id": "noncritical_ui_report_polish",
        "reason": "非阻塞 UI/报告美化不应打断真实生产门禁。",
        "resume_condition": "出现明确生产可用性缺陷或 real acceptance 完成后恢复。",
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
    for role, relative in _JSON_EVIDENCE_PATHS.items():
        path = repo_root / relative
        if not path.is_file():
            rows.append({"role": role, "path": str(relative), "status": "missing"})
            reasons.append(f"source_evidence_missing:{role}")
            continue
        try:
            payload = _read_json(path)
        except (OSError, ValueError, json.JSONDecodeError):
            rows.append({"role": role, "path": str(relative), "status": "invalid"})
            reasons.append(f"source_evidence_invalid:{role}")
            continue
        payloads[role] = payload
        rows.append({"role": role, "path": str(relative), "status": "bound"})

    staging_path = repo_root / _STAGING_INTEGRATION_EVIDENCE
    if not staging_path.is_file():
        rows.append(
            {"role": "postgresql18_real_staging_integration", "path": str(_STAGING_INTEGRATION_EVIDENCE), "status": "missing"}
        )
        reasons.append("source_evidence_missing:postgresql18_real_staging_integration")
    else:
        text = staging_path.read_text(encoding="utf-8-sig")
        required = (
            "1 passed in 22.63s",
            "13 passed in 5.79s",
            "226 passed, 1 warning in 93.39s",
            "temporary test database was dropped",
            "does not authorize",
        )
        if not all(token in text for token in required):
            rows.append(
                {"role": "postgresql18_real_staging_integration", "path": str(_STAGING_INTEGRATION_EVIDENCE), "status": "invalid"}
            )
            reasons.append("postgresql18_real_staging_integration_not_confirmed")
        else:
            rows.append(
                {"role": "postgresql18_real_staging_integration", "path": str(_STAGING_INTEGRATION_EVIDENCE), "status": "bound"}
            )

    freeze = payloads.get("previous_gap_freeze", {})
    if freeze.get("scope_frozen") is not True:
        reasons.append("previous_gap_scope_not_frozen")
    if freeze.get("mature_fitting_baseline") != "0613 V1.5 fitting path":
        reasons.append("mature_fitting_baseline_not_0613")
    if freeze.get("mature_route_baseline") != "0620/0621 clean-worktree mature physical route path":
        reasons.append("mature_route_baseline_not_0620_0621")

    replay = payloads.get("legacy_full_flow_offline_replay", {})
    if replay.get("orchestrator_replay_complete") is not True:
        reasons.append("legacy_orchestrator_replay_not_complete")
    if replay.get("production_flow_complete") is not False:
        reasons.append("legacy_replay_real_evidence_lock_missing")

    component = payloads.get("production_component_qc_fit_matrix", {})
    for key in (
        "production_component_qc_evaluator_available",
        "canonical_0613_strategy_matrix_available",
        "production_component_qc_evaluation_complete",
    ):
        if component.get(key) is not True:
            reasons.append(f"production_component_capability_not_ready:{key}")
    if component.get("production_fit_allowed") is not False:
        reasons.append("production_fit_not_locked_without_continuity")

    writer = payloads.get("unified_controlled_write_reverify", {})
    if writer.get("unified_contract_available") is not True:
        reasons.append("unified_controlled_write_contract_not_ready")
    if writer.get("frozen_gap_program_contract_closed") is not True:
        reasons.append("unified_controlled_write_program_gap_not_closed")
    if writer.get("frozen_gap_production_evidence_closed") is not False:
        reasons.append("unified_controlled_write_real_evidence_lock_missing")
    if writer.get("controlled_write_allowed") is not False:
        reasons.append("controlled_write_not_locked")

    handoff = payloads.get("new_algorithm_mature_queue_live_handoff", {})
    if handoff.get("offline_handoff_contract_ready") is not True:
        reasons.append("new_algorithm_handoff_contract_not_ready")
    if handoff.get("legacy_default_preserved") is not True:
        reasons.append("legacy_default_not_preserved")
    if handoff.get("production_live_gap_closed") is not False:
        reasons.append("new_algorithm_live_gap_not_locked")
    if handoff.get("live_queue_execution_allowed") is not False:
        reasons.append("new_algorithm_live_queue_not_locked")

    acceptance = payloads.get("final_offline_acceptance_suite", {})
    if acceptance.get("offline_program_acceptance_ready") is not True:
        reasons.append("final_offline_acceptance_not_ready")
    if acceptance.get("offline_suite_tests_passed") is not True:
        reasons.append("final_offline_suite_not_passed")
    if acceptance.get("production_acceptance_ready") is not False:
        reasons.append("real_acceptance_lock_missing")

    migration = payloads.get("production_migration_executor", {})
    if migration.get("migration_execution_package_ready") is not True:
        reasons.append("production_migration_execution_package_not_ready")
    for key in ("connects_postgresql", "applies_migrations", "migration_execution_confirmed"):
        if migration.get(key) is not False:
            reasons.append(f"production_migration_preview_lock_not_false:{key}")
    return rows, sorted(set(reasons))


def build_v1_5_final_production_external_gate_freeze(
    *, repository_root: str | Path, source_origin_main_commit: str
) -> dict[str, Any]:
    repo_root = Path(repository_root).resolve()
    source_commit = source_origin_main_commit.strip().lower()
    reasons: list[str] = []
    if not _SHA_RE.fullmatch(source_commit):
        reasons.append("source_origin_main_commit_invalid")
    evidence_rows, evidence_reasons = _evidence_checks(repo_root)
    reasons.extend(evidence_reasons)
    priorities = [row.priority for row in REMAINING_EXTERNAL_GATES]
    if priorities != list(range(1, len(REMAINING_EXTERNAL_GATES) + 1)):
        reasons.append("remaining_external_gate_priorities_not_contiguous")
    if REMAINING_EXTERNAL_GATES[0].gate_id != "production_postgresql18_migration_002_authorization_and_execution":
        reasons.append("production_migration_not_first_executable_external_gate")
    reasons = sorted(set(reasons))
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": READY_STATUS if not reasons else REVIEW_STATUS,
        "scope_frozen": not reasons,
        "source_origin_main_commit": source_commit,
        "supersedes": ["v1_5_final_production_gap_freeze_v1"],
        "mature_fitting_baseline": "0613 V1.5 fitting path",
        "mature_route_baseline": "0620/0621 clean-worktree mature physical route path",
        "legacy_point_counts": {"co2": 45, "h2o": 13},
        "new_algorithm_point_counts": {"co2": 47, "h2o": 14},
        "program_capability_count": len(PROGRAM_CAPABILITIES),
        "remaining_external_gate_count": len(REMAINING_EXTERNAL_GATES),
        "recommended_next_gate_id": REMAINING_EXTERNAL_GATES[0].gate_id,
        "program_structure_and_offline_automation_complete": not reasons,
        "postgresql18_real_staging_integration_verified": not reasons,
        "real_production_acceptance_complete": False,
        "review_reasons": reasons,
        "source_evidence": evidence_rows,
        "program_capabilities": [row.to_json() for row in PROGRAM_CAPABILITIES],
        "remaining_external_gates": [row.to_json() for row in REMAINING_EXTERNAL_GATES],
        "deferred_items": list(DEFERRED_ITEMS),
        "full_production_auto_allowed": False,
        "live_queue_execution_allowed": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "production_database_migrated": False,
        "production_database_written": False,
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
        "# V1.5 最终生产外部门禁冻结清单",
        "",
        f"- source origin/main: `{model['source_origin_main_commit']}`",
        f"- overall_status: `{model['overall_status']}`",
        f"- program capability count: `{model['program_capability_count']}`",
        f"- remaining external gate count: `{model['remaining_external_gate_count']}`",
        f"- recommended next gate: `{model['recommended_next_gate_id']}`",
        f"- mature fitting baseline: `{model['mature_fitting_baseline']}`",
        f"- mature route baseline: `{model['mature_route_baseline']}`",
        "",
        "本清单取代 2026-07-13 的七项程序缺口清单。程序结构与离线自动化已经闭合；以下未完成项都需要真实硬件、当前批次证据或独立三方授权。",
        "",
        "## 已完成的程序能力",
        "",
        "| capability_id | 状态 | 能力 | 生产含义 |",
        "|---|---|---|---|",
    ]
    for row in model["program_capabilities"]:
        lines.append(
            f"| `{row['capability_id']}` | `{row['status']}` | {row['title']} | {row['production_meaning']} |"
        )
    lines.extend(
        [
            "",
            "## 剩余真实生产外部门禁",
            "",
            "| 优先级 | gate_id | 状态 | 工作 | 完成标准 |",
            "|---:|---|---|---|---|",
        ]
    )
    for row in model["remaining_external_gates"]:
        lines.append(
            f"| {row['priority']} | `{row['gate_id']}` | `{row['status']}` | {row['title']} | {row['done_when']} |"
        )
    lines.extend(
        [
            "",
            "## 当前结论",
            "",
            "- V1.5 程序结构和离线自动化能力已经完成，不再把已实现的小包列为待开发。",
            "- 生产 PostgreSQL 18 staging 已真实验证；生产 migration 002 和 production import 从未执行。",
            "- 当前没有真机批次证据时，不允许拟合候选、写系数、live queue、入库或 release。",
            "- 下一项是收集真实 operator/reviewer/approver 身份并审核 migration 002 授权包；不得由程序虚构身份。",
            "- `full_production_auto_allowed=false`、`formal_release_allowed=false`、`database_import_allowed=false`。",
            "- 本包不开 COM、不控压力/气水路、不写设备、不连接生产 PostgreSQL。",
            "- `not_real_acceptance_evidence=true`。",
            "",
        ]
    )
    if model.get("review_reasons"):
        lines.extend(["## Review Reasons", ""])
        lines.extend(f"- `{reason}`" for reason in model["review_reasons"])
        lines.append("")
    return "\n".join(lines)


def write_v1_5_final_production_external_gate_freeze(
    *, output_dir: str | Path, repository_root: str | Path, source_origin_main_commit: str
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    model = build_v1_5_final_production_external_gate_freeze(
        repository_root=repository_root,
        source_origin_main_commit=source_origin_main_commit,
    )
    paths = {
        "manifest": out / "v1_5_final_production_external_gate_freeze.json",
        "program_capabilities": out / "v1_5_final_production_program_capabilities.csv",
        "remaining_external_gates": out / "v1_5_final_production_remaining_external_gates.csv",
        "deferred_items": out / "v1_5_final_production_deferred_items.csv",
        "markdown": out / "V1_5_FINAL_PRODUCTION_EXTERNAL_GATE_FREEZE.md",
    }
    paths["manifest"].write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(paths["program_capabilities"], model["program_capabilities"])
    _write_csv(paths["remaining_external_gates"], model["remaining_external_gates"])
    _write_csv(paths["deferred_items"], model["deferred_items"])
    paths["markdown"].write_text(_markdown(model), encoding="utf-8")
    return paths


__all__ = [
    "DEFERRED_ITEMS",
    "PROGRAM_CAPABILITIES",
    "READY_STATUS",
    "REMAINING_EXTERNAL_GATES",
    "SCHEMA",
    "build_v1_5_final_production_external_gate_freeze",
    "write_v1_5_final_production_external_gate_freeze",
]
