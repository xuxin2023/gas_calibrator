"""Replay the legacy V1.5 production state machine from checked-in evidence."""

from __future__ import annotations

import csv
import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence


SCHEMA = "v1_5_legacy_full_flow_offline_replay_v1"
READY_STATUS = "legacy_full_flow_replay_complete_production_evidence_incomplete"
REVIEW_STATUS = "legacy_full_flow_replay_review_required"
EVIDENCE_SOURCE = "historical_replay"
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")

_SOURCE_PATHS = {
    "production_gap_freeze": Path(
        "docs/v1_5_flow_contract/final_production_gap_freeze/v1_5_final_production_gap_freeze.json"
    ),
    "full_flow_plan": Path("docs/v1_5_flow_contract/v1_5_full_flow_plan.json"),
    "batch_initialization_closeout": Path(
        "docs/v1_5_flow_contract/batch_initialization_closeout_index/"
        "v1_5_batch_initialization_closeout_index.json"
    ),
    "pressure_s9_readiness": Path(
        "docs/v1_5_flow_contract/pressure_s9_readiness_index/v1_5_pressure_s9_readiness_index.json"
    ),
    "production_entrypoint_gate": Path(
        "docs/v1_5_flow_contract/production_entrypoint_gate/v1_5_production_entrypoint_gate.json"
    ),
    "mature_route_contract": Path(
        "docs/v1_5_flow_contract/mature_route_contract/v1_5_mature_route_contract.json"
    ),
    "historical_replay_contract": Path(
        "docs/v1_5_flow_contract/historical_replay_contract/v1_5_historical_replay_contract.json"
    ),
    "legacy_evidence_catalog": Path(
        "docs/v1_5_flow_contract/legacy_historical_evidence_catalog/"
        "v1_5_legacy_historical_evidence_catalog.json"
    ),
    "historical_mature_root_discovery": Path(
        "docs/v1_5_flow_contract/historical_mature_root_discovery/"
        "v1_5_historical_mature_root_discovery.json"
    ),
    "component_qc_writer_design": Path(
        "docs/v1_5_flow_contract/historical_component_qc_controlled_writer_design/"
        "v1_5_historical_component_qc_controlled_writer_design.json"
    ),
    "production_component_qc_fit_matrix": Path(
        "docs/v1_5_flow_contract/production_component_qc_fit_matrix/"
        "v1_5_production_component_qc_fit_matrix.json"
    ),
    "formal_run_status": Path(
        "docs/v1_5_flow_contract/final_acceptance_status/v1_5_formal_run_status.json"
    ),
    "postgresql18_import_design": Path(
        "docs/v1_5_flow_contract/formal_database_import_controlled_executor_design/"
        "v1_5_formal_database_import_controlled_executor_design.json"
    ),
}


@dataclass(frozen=True)
class StageAssessment:
    order: int
    stage_id: str
    title: str
    evidence_status: str
    effective_status: str
    blocker_codes: tuple[str, ...]
    evidence_roles: tuple[str, ...]
    observed: str
    physical_meaning: str
    next_action: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return payload


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_sources(repo_root: Path) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]], list[str]]:
    payloads: dict[str, dict[str, Any]] = {}
    rows: list[dict[str, Any]] = []
    reasons: list[str] = []
    for role, relative in _SOURCE_PATHS.items():
        path = repo_root / relative
        if not path.is_file():
            reasons.append(f"source_evidence_missing:{role}")
            rows.append({"role": role, "relative_path": str(relative), "status": "missing", "sha256": ""})
            continue
        try:
            payload = _read_json(path)
        except (OSError, ValueError, json.JSONDecodeError):
            reasons.append(f"source_evidence_invalid:{role}")
            rows.append({"role": role, "relative_path": str(relative), "status": "invalid", "sha256": _sha256(path)})
            continue
        payloads[role] = payload
        rows.append({"role": role, "relative_path": str(relative), "status": "bound", "sha256": _sha256(path)})
    return payloads, rows, reasons


def _gate(payload: Mapping[str, Any], gate_id: str) -> Mapping[str, Any]:
    for row in payload.get("gates") or []:
        if isinstance(row, Mapping) and row.get("gate_id") == gate_id:
            return row
    return {}


def _assessment(
    order: int,
    stage_id: str,
    title: str,
    blockers: Sequence[str],
    roles: Sequence[str],
    observed: str,
    physical_meaning: str,
    next_action: str,
) -> dict[str, Any]:
    clean_blockers = tuple(sorted(set(str(value) for value in blockers if str(value))))
    return {
        "order": order,
        "stage_id": stage_id,
        "title": title,
        "evidence_status": "pass" if not clean_blockers else "hold",
        "blocker_codes": clean_blockers,
        "evidence_roles": tuple(roles),
        "observed": observed,
        "physical_meaning": physical_meaning,
        "next_action": next_action,
    }


def _initialization(payloads: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    source = payloads.get("batch_initialization_closeout", {})
    blockers = [] if source.get("overall_status") == "ready" else list(source.get("review_reasons") or [])
    count = int(source.get("device_count") or 0)
    if not 1 <= count <= 6:
        blockers.append("active_device_count_not_1_to_6")
    return _assessment(
        1,
        "initialization_identity_runtime",
        "初始化、身份、GETCO 与 runtime closeout",
        blockers,
        ("batch_initialization_closeout",),
        f"overall_status={source.get('overall_status')}; device_count={count}",
        "A replay batch must bind 1-6 analyzers to SN/device_code, protocol ID, GETCO1-9, and neutral runtime state.",
        "Bind a concrete historical or future batch initialization closeout artifact; do not infer identity from route files.",
    )


def _pressure(payloads: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    source = payloads.get("pressure_s9_readiness", {})
    blockers = [] if source.get("overall_status") == "ready" else list(source.get("review_reasons") or [])
    count = int(source.get("device_count") or 0)
    if not 1 <= count <= 6:
        blockers.append("pressure_device_count_not_1_to_6")
    return _assessment(
        2,
        "pressure_s9_readiness",
        "压力/S9 no-write、写入读回与复验",
        blockers,
        ("pressure_s9_readiness",),
        f"overall_status={source.get('overall_status')}; device_count={count}",
        "Pressure must be traceable before gas or water fitting so S1-S4 do not absorb pressure bias.",
        "Bind per-device mature pressure evidence, including any explicit linear-S9 exception and post-write reverify.",
    )


def _route_readiness(payloads: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    entrypoint = payloads.get("production_entrypoint_gate", {}).get("manifest", {})
    mature = payloads.get("mature_route_contract", {}).get("manifest", {})
    discovery = payloads.get("historical_mature_root_discovery", {})
    blockers: list[str] = []
    if not isinstance(entrypoint, Mapping) or entrypoint.get("status") != "pass":
        blockers.append("production_entrypoint_gate_not_pass")
    if not isinstance(mature, Mapping) or mature.get("status") != "pass":
        blockers.append("mature_route_contract_not_pass")
    if discovery.get("overall_status") != "complete_mature_root_found":
        blockers.append("no_complete_continuous_mature_route_root")
    return _assessment(
        3,
        "mature_route_readiness",
        "0613/0620/0621 成熟入口与连续 route 根",
        blockers,
        ("production_entrypoint_gate", "mature_route_contract", "historical_mature_root_discovery"),
        (
            f"entrypoint={entrypoint.get('status')}; mature_contract={mature.get('status')}; "
            f"root_discovery={discovery.get('overall_status')}; complete_roots={discovery.get('attestation_input_candidate_count', 0)}"
        ),
        "The queue and point-internal route contract may pass while the historical evidence still lacks one continuous mature run root.",
        "Keep the mature runners unchanged; bind one fresh continuous queue root rather than promoting segmented or recovery evidence.",
    )


def _co2(payloads: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    catalog = payloads.get("legacy_evidence_catalog", {})
    blockers: list[str] = []
    accepted = int(catalog.get("accepted_composite_member_count") or 0)
    missing_qc = int((catalog.get("missing_component_qc_counts") or {}).get("co2") or 0)
    if accepted != 45:
        blockers.append("accepted_co2_composite_count_not_45")
    if catalog.get("continuous_route_attestation_allowed") is not True:
        blockers.append("co2_composite_not_continuous_route_attestation")
    if catalog.get("historical_fit_allowed") is not True:
        blockers.append("co2_historical_fit_not_allowed")
    if missing_qc:
        blockers.append(f"co2_component_qc_missing={missing_qc}")
    return _assessment(
        4,
        "legacy_co2_45",
        "旧算法成熟 CO2 45 点",
        blockers,
        ("legacy_evidence_catalog", "mature_route_contract"),
        f"accepted_composite_members={accepted}; co2_points={catalog.get('co2_point_count')}; missing_component_qc={missing_qc}",
        "A 45-point accepted composite can support diagnosis but cannot prove that one continuous mature queue completed.",
        "Use one continuous 45-point mature queue for production; keep the historical composite diagnostic-only.",
    )


def _h2o(payloads: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    catalog = payloads.get("legacy_evidence_catalog", {})
    discovery = payloads.get("historical_mature_root_discovery", {})
    h2o_points = int(catalog.get("h2o_point_count") or 0)
    missing_qc = int((catalog.get("missing_component_qc_counts") or {}).get("h2o") or 0)
    blockers: list[str] = []
    if int(discovery.get("attestation_input_candidate_count") or 0) <= 0:
        blockers.append("legacy_h2o_continuous_13_point_root_missing")
    if missing_qc:
        blockers.append(f"h2o_component_qc_missing={missing_qc}")
    if catalog.get("historical_fit_allowed") is not True:
        blockers.append("h2o_historical_fit_not_allowed")
    return _assessment(
        5,
        "legacy_h2o_13",
        "旧算法成熟 H2O 13 湿点",
        blockers,
        ("legacy_evidence_catalog", "historical_mature_root_discovery", "mature_route_contract"),
        f"cataloged_h2o_points={h2o_points}; missing_component_qc={missing_qc}; complete_mature_roots={discovery.get('attestation_input_candidate_count', 0)}",
        "Segmented wet points are not equivalent to one mature 13-point H2O queue, and CO2 zero gas is not an interchangeable H2O dry-gas anchor.",
        "Bind one continuous mature 13-point H2O root with component-matched QC and separately traceable dry-gas anchor evidence.",
    )


def _fit(payloads: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    catalog = payloads.get("legacy_evidence_catalog", {})
    matrix = payloads.get("production_component_qc_fit_matrix", {})
    blockers: list[str] = []
    if matrix.get("production_component_qc_evaluator_available") is not True:
        blockers.append("production_component_qc_evaluator_missing")
    if matrix.get("canonical_0613_strategy_matrix_available") is not True:
        blockers.append("canonical_0613_multi_strategy_fit_selector_not_closed")
    if matrix.get("production_component_qc_evaluation_complete") is not True:
        blockers.append("production_component_qc_evaluation_incomplete")
    if matrix.get("production_fit_allowed") is not True:
        blockers.append("production_fit_input_not_eligible")
    if catalog.get("historical_fit_allowed") is not True:
        blockers.append("catalog_not_fit_eligible")
    return _assessment(
        6,
        "component_qc_and_0613_fit_review",
        "component-QC 与 0613 多策略 no-write 拟合",
        blockers,
        (
            "legacy_evidence_catalog",
            "production_component_qc_fit_matrix",
            "component_qc_writer_design",
            "historical_replay_contract",
        ),
        (
            f"catalog_fit_allowed={catalog.get('historical_fit_allowed')}; "
            f"component_qc_evaluator_available={matrix.get('production_component_qc_evaluator_available')}; "
            f"strategy_matrix_available={matrix.get('canonical_0613_strategy_matrix_available')}; "
            f"evaluated_qc_rows={matrix.get('analyzer_qc_row_count')}; "
            f"fit_ready_strategies={matrix.get('fit_ready_strategy_count')}"
        ),
        "Fitting must consume per-analyzer physical QC and the 0613 strategy rules, not merely all available points.",
        "Bind a continuous mature 45/13 route root and separate H2O dry-gas anchor evidence, then rerun the now-available no-write matrix.",
    )


def _controlled_write(payloads: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    formal = payloads.get("formal_run_status", {})
    write_gate = _gate(formal, "post_run_write_package")
    blockers = [] if write_gate.get("status") == "ready" else [
        f"post_run_write_package={write_gate.get('status') or 'missing'}"
    ]
    return _assessment(
        7,
        "controlled_write_readback",
        "系数受控写入与 GETCO 读回",
        blockers,
        ("formal_run_status",),
        f"post_run_write_package={write_gate.get('status') or 'missing'}",
        "Calculation, authorization, old-value snapshot, write, and readback are separate evidence events.",
        "Close the unified S1-S9/SENCOA-B controlled-write bundle without treating a successful write as a successful validation.",
    )


def _reverify(payloads: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    formal = payloads.get("formal_run_status", {})
    gate = _gate(formal, "controlled_write_and_reverification")
    blockers = [] if gate.get("status") == "ready" else [
        f"controlled_write_and_reverification={gate.get('status') or 'missing'}"
    ]
    return _assessment(
        8,
        "post_write_short_reverify",
        "写后独立短复验",
        blockers,
        ("formal_run_status",),
        f"controlled_write_and_reverification={gate.get('status') or 'missing'}",
        "Readback proves stored bytes; independent reverify proves the physical calibration result.",
        "Bind component-specific short reverify evidence after every authorized write before archive closure.",
    )


def _archive_database(payloads: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    formal = payloads.get("formal_run_status", {})
    gate = _gate(formal, "formal_archive_database_release")
    database = payloads.get("postgresql18_import_design", {})
    blockers: list[str] = []
    if gate.get("status") != "ready":
        blockers.append(f"formal_archive_database_release={gate.get('status') or 'missing'}")
    for key in ("execution_supported", "real_import_execution_allowed", "database_import_allowed"):
        if database.get(key) is not True:
            blockers.append(f"postgresql18_{key}=false")
    return _assessment(
        9,
        "archive_release_postgresql18",
        "归档、release 与 PostgreSQL 18 受控入库",
        blockers,
        ("formal_run_status", "postgresql18_import_design"),
        (
            f"archive_gate={gate.get('status') or 'missing'}; db_execution_supported={database.get('execution_supported')}; "
            f"database_import_allowed={database.get('database_import_allowed')}"
        ),
        "Database import is the last transaction after traceable archive release, never a side effect of route, fitting, replay, or coefficient writing.",
        "Implement the controlled PostgreSQL 18 transaction only after archive, identity, readback, and reverify evidence close.",
    )


_STAGE_BUILDERS: tuple[Callable[[Mapping[str, Mapping[str, Any]]], dict[str, Any]], ...] = (
    _initialization,
    _pressure,
    _route_readiness,
    _co2,
    _h2o,
    _fit,
    _controlled_write,
    _reverify,
    _archive_database,
)


def _source_contract_reasons(payloads: Mapping[str, Mapping[str, Any]]) -> list[str]:
    reasons: list[str] = []
    freeze = payloads.get("production_gap_freeze", {})
    if freeze.get("scope_frozen") is not True:
        reasons.append("production_gap_scope_not_frozen")
    if freeze.get("recommended_next_gap_id") != "legacy_full_flow_orchestrator_offline_replay":
        reasons.append("legacy_replay_not_frozen_next_gap")
    if freeze.get("legacy_point_counts") != {"co2": 45, "h2o": 13}:
        reasons.append("legacy_point_counts_not_45_13")
    if freeze.get("mature_fitting_baseline") != "0613 V1.5 fitting path":
        reasons.append("mature_fitting_baseline_not_0613")
    if freeze.get("mature_route_baseline") != "0620/0621 clean-worktree mature physical route path":
        reasons.append("mature_route_baseline_not_0620_0621")
    replay_contract = payloads.get("historical_replay_contract", {}).get("manifest", {})
    if not isinstance(replay_contract, Mapping) or replay_contract.get("status") != "pass":
        reasons.append("historical_replay_contract_not_pass")
    plan = payloads.get("full_flow_plan", {})
    step_ids = {str(row.get("step_id")) for row in plan.get("steps") or [] if isinstance(row, Mapping)}
    for required in (
        "formal_initialization_contract_plan",
        "pressure_senco9_no_write_review",
        "co2_open_flow_sampling",
        "h2o_open_flow_sampling",
        "fit_input_quality_review",
        "controlled_component_write_placeholder",
        "post_write_reverification_placeholder",
        "database_import",
    ):
        if required not in step_ids:
            reasons.append(f"full_flow_plan_step_missing:{required}")
    return reasons


def build_v1_5_legacy_full_flow_offline_replay(
    *, repository_root: str | Path, source_origin_main_commit: str
) -> dict[str, Any]:
    repo_root = Path(repository_root).resolve()
    source_commit = source_origin_main_commit.strip().lower()
    payloads, source_rows, reasons = _load_sources(repo_root)
    if not _SHA_RE.fullmatch(source_commit):
        reasons.append("source_origin_main_commit_invalid")
    reasons.extend(_source_contract_reasons(payloads))

    stage_inputs = [builder(payloads) for builder in _STAGE_BUILDERS]
    prior_hold = ""
    stages: list[StageAssessment] = []
    for row in stage_inputs:
        evidence_status = str(row["evidence_status"])
        if prior_hold:
            effective_status = "blocked_by_previous_stage"
        elif evidence_status == "hold":
            effective_status = "hold"
            prior_hold = str(row["stage_id"])
        else:
            effective_status = "replay_pass"
        stages.append(
            StageAssessment(
                order=int(row["order"]),
                stage_id=str(row["stage_id"]),
                title=str(row["title"]),
                evidence_status=evidence_status,
                effective_status=effective_status,
                blocker_codes=tuple(row["blocker_codes"]),
                evidence_roles=tuple(row["evidence_roles"]),
                observed=str(row["observed"]),
                physical_meaning=str(row["physical_meaning"]),
                next_action=str(row["next_action"]),
            )
        )

    reasons = sorted(set(reasons))
    first_hold = next((row for row in stages if row.effective_status == "hold"), None)
    assessed_holds = [row.stage_id for row in stages if row.evidence_status == "hold"]
    replay_complete = not reasons and len(stages) == len(_STAGE_BUILDERS)
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": READY_STATUS if replay_complete else REVIEW_STATUS,
        "source_origin_main_commit": source_commit,
        "algorithm_profile_id": "legacy_ratio_production",
        "mature_fitting_baseline": "0613 V1.5 fitting path",
        "mature_route_baseline": "0620/0621 clean-worktree mature physical route path",
        "expected_point_counts": {"co2": 45, "h2o": 13},
        "state_machine_stage_count": len(stages),
        "source_evidence_count": len(source_rows),
        "source_review_reasons": reasons,
        "orchestrator_replay_complete": replay_complete,
        "production_flow_complete": False,
        "frozen_gap_assessment": "program_level_replay_complete_not_production_complete" if replay_complete else "replay_package_review_required",
        "current_stage_id": first_hold.stage_id if first_hold else "offline_replay_complete",
        "current_status": "hold" if first_hold else "offline_replay_complete",
        "evidence_hold_stage_ids": assessed_holds,
        "effective_blocked_stage_ids": [row.stage_id for row in stages if row.effective_status == "blocked_by_previous_stage"],
        "source_evidence": source_rows,
        "stages": [row.to_json() for row in stages],
        "next_action": first_hold.next_action if first_hold else "Proceed to the next frozen production gap.",
        "evidence_source": EVIDENCE_SOURCE,
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
        for row in rows:
            writer.writerow(
                {
                    key: ";".join(str(item) for item in value)
                    if isinstance(value, (list, tuple))
                    else value
                    for key, value in row.items()
                }
            )


def _markdown(model: Mapping[str, Any]) -> str:
    lines = [
        "# V1.5 旧算法全流程 Orchestrator 离线 Replay",
        "",
        f"- source origin/main: `{model['source_origin_main_commit']}`",
        f"- overall_status: `{model['overall_status']}`",
        f"- orchestrator_replay_complete: `{str(model['orchestrator_replay_complete']).lower()}`",
        f"- production_flow_complete: `{str(model['production_flow_complete']).lower()}`",
        f"- current_stage_id: `{model['current_stage_id']}`",
        f"- evidence_source: `{model['evidence_source']}`",
        f"- not_real_acceptance_evidence: `{str(model['not_real_acceptance_evidence']).lower()}`",
        "",
        "本 replay 只验证一个旧算法状态机能否按 0613/0620/0621 口径解释现有证据。它不会把分段、retry、diagnostic composite 或缺 QC 的历史数据提升为连续生产 run。",
        "",
        "## Stage Replay",
        "",
        "| 顺序 | stage | evidence | effective | observed | blockers |",
        "|---:|---|---|---|---|---|",
    ]
    for row in model["stages"]:
        blockers = "; ".join(row["blocker_codes"]) or "none"
        lines.append(
            f"| {row['order']} | `{row['stage_id']}` | `{row['evidence_status']}` | `{row['effective_status']}` | {row['observed']} | {blockers} |"
        )
    lines.extend(
        [
            "",
            "## Conclusion",
            "",
            "- 程序级 replay 已遍历初始化到归档/数据库的全部 9 个阶段。",
            "- 当前生产流在初始化批次证据处 hold；后续阶段仍被只读检查并列出自身证据缺口。",
            "- 45 点 CO2 composite 只可诊断，不能证明一条连续 mature route。",
            "- H2O 分段历史点不能替代一条连续 13 点 mature route，且 CO2 zero gas 与 H2O dry-gas anchor 不可互换。",
            "- 下一冻结缺口仍是生产 component-QC evaluator 与 0613 多策略拟合矩阵；真机未连接时不做 live acceptance。",
            "- 本包不开 COM、不控压力/气水路、不写系数、不连 PostgreSQL、不授权 release/import。",
            "",
        ]
    )
    if model.get("source_review_reasons"):
        lines.extend(["## Source Review Reasons", ""])
        lines.extend(f"- `{reason}`" for reason in model["source_review_reasons"])
        lines.append("")
    return "\n".join(lines)


def write_v1_5_legacy_full_flow_offline_replay(
    *, output_dir: str | Path, repository_root: str | Path, source_origin_main_commit: str
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    model = build_v1_5_legacy_full_flow_offline_replay(
        repository_root=repository_root,
        source_origin_main_commit=source_origin_main_commit,
    )
    paths = {
        "manifest": out / "v1_5_legacy_full_flow_offline_replay.json",
        "stages": out / "v1_5_legacy_full_flow_offline_replay_stages.csv",
        "sources": out / "v1_5_legacy_full_flow_offline_replay_sources.csv",
        "markdown": out / "V1_5_LEGACY_FULL_FLOW_OFFLINE_REPLAY.md",
    }
    paths["manifest"].write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(paths["stages"], model["stages"])
    _write_csv(paths["sources"], model["source_evidence"])
    paths["markdown"].write_text(_markdown(model), encoding="utf-8")
    return paths


__all__ = [
    "EVIDENCE_SOURCE",
    "READY_STATUS",
    "SCHEMA",
    "build_v1_5_legacy_full_flow_offline_replay",
    "write_v1_5_legacy_full_flow_offline_replay",
]
