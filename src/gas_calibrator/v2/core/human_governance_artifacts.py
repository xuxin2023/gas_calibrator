from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


RUN_METADATA_PROFILE_FILENAME = "run_metadata_profile.json"
OPERATOR_AUTHORIZATION_PROFILE_FILENAME = "operator_authorization_profile.json"
TRAINING_RECORD_FILENAME = "training_record.json"
SOP_VERSION_BINDING_FILENAME = "sop_version_binding.json"
QC_FLAG_CATALOG_FILENAME = "qc_flag_catalog.json"
RECOVERY_ACTION_LOG_FILENAME = "recovery_action_log.json"
REVIEWER_DUAL_CHECK_PLACEHOLDER_FILENAME = "reviewer_dual_check_placeholder.json"

HUMAN_GOVERNANCE_FIXTURE_ROOT_ENV = "GC_V2_HUMAN_GOVERNANCE_FIXTURE_ROOT"
HUMAN_GOVERNANCE_FIXTURE_SCHEMA_VERSION = "step2-human-governance-fixtures-v1"
OPERATOR_ROSTER_FIXTURE_PATH = ("governance", "operator_roster.json")
AUTHORIZATION_SCOPE_FIXTURE_PATH = ("governance", "authorization_scope.json")
TRAINING_RECORDS_FIXTURE_PATH = ("governance", "training_records.json")
SOP_VERSIONS_FIXTURE_PATH = ("governance", "sop_versions.json")
QC_FLAG_CATALOG_FIXTURE_PATH = ("governance", "qc_flag_catalog.json")

_BOUNDARY_FLAGS: dict[str, Any] = {
    "reviewer_only": True,
    "readiness_mapping_only": True,
    "not_real_acceptance_evidence": True,
    "not_ready_for_formal_claim": True,
    "file_artifact_first_preserved": True,
    "main_chain_dependency": False,
}
_BOUNDARY_SUMMARY = (
    "reviewer_only=true | readiness_mapping_only=true | "
    "not_real_acceptance_evidence=true | not_ready_for_formal_claim=true | "
    "file_artifact_first_preserved=true | main_chain_dependency=false"
)
_REQUIRED_OPERATOR_MODULES = {
    "base": {"STEP2_SIMULATION_OPERATOR", "STEP2_DUAL_CHECK_AWARENESS"},
    "gas": {"STEP2_ROUTE_GAS"},
    "water": {"STEP2_ROUTE_WATER"},
}
_DEFAULT_ENVIRONMENT_SCOPE = ("simulation", "offline", "headless")


def build_human_governance_artifacts(
    *,
    run_id: str = "",
    run_dir: str | Path | None = None,
    summary: dict[str, Any] | None = None,
    manifest: dict[str, Any] | None = None,
    acceptance_plan: dict[str, Any] | None = None,
    workbench_action_report: dict[str, Any] | None = None,
    fixtures_root: str | Path | None = None,
) -> dict[str, Any]:
    normalized_run_id = str(run_id or "").strip()
    normalized_run_dir = str(Path(run_dir)) if run_dir is not None else ""
    summary_payload = dict(summary or {})
    manifest_payload = dict(manifest or {})
    acceptance_plan_payload = dict(acceptance_plan or {})
    workbench_payload = dict(workbench_action_report or {})
    fixtures = load_human_governance_fixtures(fixtures_root=fixtures_root)
    fixture_paths = dict(fixtures.get("fixture_paths") or {})
    route_scope = _collect_route_scope(summary_payload, manifest_payload)
    environment_scope = _collect_environment_scope(summary_payload, manifest_payload)
    operator = _pick_person(
        manifest_payload.get("operator"),
        fixtures.get("operator_roster"),
        role="operator",
        environment_scope=environment_scope,
        route_scope=route_scope,
    )
    reviewer = _pick_person(
        (
            dict(acceptance_plan_payload.get("reviewer") or {}).get("person_id")
            or dict(acceptance_plan_payload.get("reviewer") or {}).get("name")
            or dict(acceptance_plan_payload.get("role_assignments") or {}).get("reviewer")
            or dict(acceptance_plan_payload.get("role_views") or {}).get("reviewer")
        ),
        fixtures.get("operator_roster"),
        role="reviewer",
        environment_scope=environment_scope,
        route_scope=route_scope,
    )
    authorization_row = _pick_authorization_row(
        operator.get("person_id"),
        fixtures.get("authorization_scopes"),
        environment_scope=environment_scope,
        route_scope=route_scope,
    )
    operator_training_rows = _person_rows(fixtures.get("training_records"), operator.get("person_id"))
    reviewer_training_rows = _person_rows(fixtures.get("training_records"), reviewer.get("person_id"))
    sop_rows = _active_sop_rows(fixtures.get("sop_versions"), route_scope=route_scope)
    qc_flag_rows = _applicable_qc_flags(fixtures.get("qc_flag_catalog_rows"), route_scope=route_scope)
    dual_check_rows = [
        _build_dual_check_row(row, reviewer=reviewer)
        for row in qc_flag_rows
        if bool(dict(row).get("requires_dual_check"))
    ]
    recovery_rows = _build_recovery_rows(
        summary_payload=summary_payload,
        workbench_payload=workbench_payload,
        reviewer=reviewer,
    )
    operator_required_modules = _required_training_modules(route_scope)
    operator_completed_modules = {
        str(dict(row).get("module_id") or "").strip()
        for row in operator_training_rows
        if str(dict(row).get("record_state") or "").strip() == "current"
    }
    missing_training_modules = sorted(
        module_id
        for module_id in operator_required_modules
        if module_id and module_id not in operator_completed_modules
    )
    authorization_ready = (
        bool(authorization_row)
        and str(authorization_row.get("authorization_state") or "").strip() in {"current", "authorized"}
        and set(route_scope).issubset(set(_text_list(authorization_row.get("route_scope") or [])))
        and set(environment_scope).issubset(set(_text_list(authorization_row.get("environment_scope") or [])))
    )
    sop_binding_summary = " | ".join(
        f"{row.get('sop_id')} {row.get('version')}"
        for row in sop_rows
        if str(dict(row).get("sop_id") or "").strip()
    ) or "未绑定"
    dual_check_summary = (
        f"{len(dual_check_rows)} 项关键动作要求双人复核占位，当前仅 reviewer note / placeholder。"
        if dual_check_rows
        else "当前目录下未配置双人复核占位动作。"
    )
    operator_summary = _person_summary(operator, fallback="未绑定操作员")
    reviewer_summary = _person_summary(reviewer, fallback="未绑定 reviewer")
    run_binding_summary = (
        f"操作员 {operator_summary} | reviewer {reviewer_summary} | "
        f"SOP {len(sop_rows)} 份 | route {', '.join(route_scope) or '--'}"
    )
    shared_artifact_paths = {
        "governance_fixture_root": str(fixtures.get("fixture_root") or ""),
        "governance_fixture_schema_version": str(
            fixtures.get("schema_version") or HUMAN_GOVERNANCE_FIXTURE_SCHEMA_VERSION
        ),
        "governance_fixture_operator_roster": str(fixture_paths.get("operator_roster") or ""),
        "governance_fixture_authorization_scope": str(fixture_paths.get("authorization_scope") or ""),
        "governance_fixture_training_records": str(fixture_paths.get("training_records") or ""),
        "governance_fixture_sop_versions": str(fixture_paths.get("sop_versions") or ""),
        "governance_fixture_qc_flag_catalog": str(fixture_paths.get("qc_flag_catalog") or ""),
        "summary_json": _existing_artifact_path(normalized_run_dir, "summary.json"),
        "manifest_json": _existing_artifact_path(normalized_run_dir, "manifest.json"),
        "acceptance_plan_json": _existing_artifact_path(normalized_run_dir, "acceptance_plan.json"),
        "workbench_action_report_json": _existing_artifact_path(normalized_run_dir, "workbench_action_report.json"),
    }

    run_metadata_profile = _build_governance_payload(
        artifact_type="run_metadata_profile",
        title_text="人员 / reviewer / SOP 绑定",
        run_id=normalized_run_id,
        run_dir=normalized_run_dir,
        primary_artifact_path=str(fixture_paths.get("operator_roster") or ""),
        artifact_paths=shared_artifact_paths,
        summary_line=run_binding_summary,
        summary_lines=[
            run_binding_summary,
            f"运行环境: {', '.join(environment_scope) or '--'}",
            f"授权摘要: {str(authorization_row.get('authorization_id') or '未绑定授权')} | {'就绪' if authorization_ready else '缺失'}",
            f"培训摘要: 已完成 {len(operator_completed_modules)}/{len(operator_required_modules)} 个必修模块",
            f"SOP 绑定: {sop_binding_summary}",
            dual_check_summary,
            "边界: reviewer-only / placeholder-only / 非真实签批。",
        ],
        detail_lines=[
            f"run_id: {normalized_run_id or '--'}",
            f"operator: {operator_summary}",
            f"reviewer: {reviewer_summary}",
            f"route_scope: {', '.join(route_scope) or '--'}",
            f"environment_scope: {', '.join(environment_scope) or '--'}",
            f"required_training_modules: {', '.join(sorted(operator_required_modules)) or '--'}",
            f"missing_training_modules: {', '.join(missing_training_modules) or 'none'}",
            f"placeholder_mode: reviewer_note_only",
        ],
        digest={
            "summary": run_binding_summary,
            "operator_summary": operator_summary,
            "reviewer_summary": reviewer_summary,
            "authorization_scope_summary": str(
                authorization_row.get("summary_text")
                or _authorization_summary(authorization_row)
                or "未绑定授权范围"
            ),
            "training_status_summary": (
                f"missing {', '.join(missing_training_modules)}"
                if missing_training_modules
                else "required operator training current"
            ),
            "sop_binding_summary": sop_binding_summary,
            "dual_check_summary": dual_check_summary,
            "recovery_action_summary": _recovery_summary(recovery_rows),
            "placeholder_summary": "reviewer note only / 非真实签批",
            "readiness_status_summary": "reviewer-only governance capsule ready",
        },
        rows=[
            {
                "person_id": operator.get("person_id"),
                "person_name": operator.get("display_name"),
                "role": "operator",
            },
            {
                "person_id": reviewer.get("person_id"),
                "person_name": reviewer.get("display_name"),
                "role": "reviewer",
            },
        ],
        extra_fields={
            "operator": dict(operator),
            "reviewer": dict(reviewer),
            "route_scope": list(route_scope),
            "environment_scope": list(environment_scope),
            "authorization_ready": authorization_ready,
            "missing_training_modules": list(missing_training_modules),
            "bound_sops": [dict(row) for row in sop_rows],
            "dual_check_required_count": len(dual_check_rows),
            "placeholder_mode": "reviewer_note_only",
        },
    )
    operator_authorization_profile = _build_governance_payload(
        artifact_type="operator_authorization_profile",
        title_text="操作员授权范围",
        run_id=normalized_run_id,
        run_dir=normalized_run_dir,
        primary_artifact_path=str(fixture_paths.get("authorization_scope") or ""),
        artifact_paths=shared_artifact_paths,
        summary_line=(
            f"操作员授权: {operator_summary} | "
            f"route {', '.join(_text_list(authorization_row.get('route_scope') or [])) or '--'} | "
            f"env {', '.join(_text_list(authorization_row.get('environment_scope') or [])) or '--'}"
        ),
        summary_lines=[
            f"授权记录: {str(authorization_row.get('authorization_id') or '未命中')} | {str(authorization_row.get('authorization_state') or 'missing')}",
            f"授权动作: {', '.join(_text_list(authorization_row.get('action_scope') or [])) or '--'}",
            f"运行 route 需求: {', '.join(route_scope) or '--'}",
            f"运行环境需求: {', '.join(environment_scope) or '--'}",
            "边界: 仅 reviewer-facing 授权映射，不进入真实审批链。",
        ],
        detail_lines=[
            f"authorization_ready: {authorization_ready}",
            f"authorization_note: {str(authorization_row.get('reviewer_only_note') or '--')}",
        ],
        digest={
            "summary": _authorization_summary(authorization_row) or "未命中授权范围",
            "operator_summary": operator_summary,
            "authorization_scope_summary": _authorization_summary(authorization_row) or "未命中授权范围",
            "readiness_status_summary": (
                "authorization current for Step 2 reviewer mapping"
                if authorization_ready
                else "authorization scope missing for current run"
            ),
        },
        rows=[dict(authorization_row)] if authorization_row else [],
        extra_fields={
            "authorization_ready": authorization_ready,
            "operator": dict(operator),
            "required_route_scope": list(route_scope),
            "required_environment_scope": list(environment_scope),
        },
    )
    training_record = _build_governance_payload(
        artifact_type="training_record",
        title_text="培训记录",
        run_id=normalized_run_id,
        run_dir=normalized_run_dir,
        primary_artifact_path=str(fixture_paths.get("training_records") or ""),
        artifact_paths=shared_artifact_paths,
        summary_line=(
            f"培训记录: {operator_summary} | current {len(operator_completed_modules)}/{len(operator_required_modules)}"
        ),
        summary_lines=[
            f"operator 必修模块: {', '.join(sorted(operator_required_modules)) or '--'}",
            f"operator 当前模块: {', '.join(sorted(operator_completed_modules)) or '--'}",
            f"reviewer 培训条目: {len(reviewer_training_rows)}",
            f"缺口: {', '.join(missing_training_modules) or 'none'}",
            "边界: 仅 file-backed reviewer 培训台账，不构成真实放行凭据。",
        ],
        detail_lines=[
            f"operator_training_records: {len(operator_training_rows)}",
            f"reviewer_training_records: {len(reviewer_training_rows)}",
        ],
        digest={
            "summary": (
                f"operator training current {len(operator_completed_modules)}/{len(operator_required_modules)}"
            ),
            "operator_summary": operator_summary,
            "training_status_summary": (
                f"missing {', '.join(missing_training_modules)}"
                if missing_training_modules
                else "required training current"
            ),
            "readiness_status_summary": (
                "operator training mapped for reviewer use"
                if not missing_training_modules
                else "operator training still incomplete"
            ),
        },
        rows=[dict(row) for row in operator_training_rows + reviewer_training_rows],
        extra_fields={
            "operator_required_modules": sorted(operator_required_modules),
            "operator_completed_modules": sorted(operator_completed_modules),
            "missing_training_modules": list(missing_training_modules),
        },
    )
    sop_version_binding = _build_governance_payload(
        artifact_type="sop_version_binding",
        title_text="SOP 版本绑定",
        run_id=normalized_run_id,
        run_dir=normalized_run_dir,
        primary_artifact_path=str(fixture_paths.get("sop_versions") or ""),
        artifact_paths=shared_artifact_paths,
        summary_line=f"SOP 绑定: {sop_binding_summary}",
        summary_lines=[
            f"绑定数量: {len(sop_rows)}",
            f"适用 route: {', '.join(route_scope) or '--'}",
            "placeholder 说明: 当前仅 reviewer note / 非真实电子签批。",
        ],
        detail_lines=[
            f"{row.get('sop_id')} {row.get('version')} | {row.get('category')} | {row.get('title')}"
            for row in sop_rows
        ] or ["未命中 SOP 版本。"],
        digest={
            "summary": sop_binding_summary,
            "sop_binding_summary": sop_binding_summary,
            "readiness_status_summary": (
                "SOP binding mapped for reviewer readiness"
                if sop_rows
                else "SOP binding missing"
            ),
        },
        rows=[dict(row) for row in sop_rows],
        extra_fields={
            "route_scope": list(route_scope),
            "bound_sops": [dict(row) for row in sop_rows],
        },
    )
    qc_flag_catalog = _build_governance_payload(
        artifact_type="qc_flag_catalog",
        title_text="QC 标记目录",
        run_id=normalized_run_id,
        run_dir=normalized_run_dir,
        primary_artifact_path=str(fixture_paths.get("qc_flag_catalog") or ""),
        artifact_paths=shared_artifact_paths,
        summary_line=f"QC flag 目录: {len(qc_flag_rows)} 条 | 双人复核占位 {len(dual_check_rows)} 条",
        summary_lines=[
            f"适用 route: {', '.join(route_scope) or '--'}",
            f"高优先级标记: {', '.join(_text_list(_critical_action_labels(qc_flag_rows)[:3])) or '--'}",
            "当前目录仅用于 reviewer-facing metadata 治理。",
        ],
        detail_lines=[
            f"{row.get('flag_code')}: {row.get('display_name')} | dual_check={bool(row.get('requires_dual_check'))}"
            for row in qc_flag_rows
        ] or ["未命中 QC flag 条目。"],
        digest={
            "summary": f"QC flags {len(qc_flag_rows)} | dual-check {len(dual_check_rows)}",
            "dual_check_summary": dual_check_summary,
            "recovery_action_summary": _recovery_summary(recovery_rows),
            "readiness_status_summary": "reviewer-only QC catalog loaded",
        },
        rows=[dict(row) for row in qc_flag_rows],
        extra_fields={
            "route_scope": list(route_scope),
            "critical_action_labels": _critical_action_labels(qc_flag_rows),
            "dual_check_required_count": len(dual_check_rows),
        },
    )
    recovery_action_log = _build_governance_payload(
        artifact_type="recovery_action_log",
        title_text="恢复动作日志",
        run_id=normalized_run_id,
        run_dir=normalized_run_dir,
        primary_artifact_path=_existing_artifact_path(normalized_run_dir, "summary.json"),
        artifact_paths=shared_artifact_paths,
        summary_line=_recovery_summary(recovery_rows),
        summary_lines=[
            _recovery_summary(recovery_rows),
            "当前 recovery log 仅做 reviewer-facing 元数据留痕，不做真实闭环放行。",
        ],
        detail_lines=[
            f"{row.get('action_id')}: {row.get('status')} | {row.get('summary')}"
            for row in recovery_rows
        ] or ["当前 run 未记录恢复动作。"],
        digest={
            "summary": _recovery_summary(recovery_rows),
            "recovery_action_summary": _recovery_summary(recovery_rows),
            "readiness_status_summary": "recovery log placeholder retained for reviewer use",
        },
        rows=[dict(row) for row in recovery_rows],
        extra_fields={
            "open_action_items": [
                str(row.get("summary") or "").strip()
                for row in recovery_rows
                if str(row.get("status") or "").strip() not in {"closed", "not_required", "none"}
                and str(row.get("summary") or "").strip()
            ],
        },
    )
    reviewer_dual_check_placeholder = _build_governance_payload(
        artifact_type="reviewer_dual_check_placeholder",
        title_text="关键动作双人复核占位",
        run_id=normalized_run_id,
        run_dir=normalized_run_dir,
        primary_artifact_path=str(fixture_paths.get("qc_flag_catalog") or ""),
        artifact_paths=shared_artifact_paths,
        summary_line=dual_check_summary,
        summary_lines=[
            dual_check_summary,
            f"reviewer: {reviewer_summary}",
            "注意: 当前仅 placeholder / reviewer note，非真实电子签批或审批链。",
        ],
        detail_lines=[
            f"{row.get('flag_code')}: {row.get('critical_action_label')} | reviewer {row.get('reviewer_summary')}"
            for row in dual_check_rows
        ] or ["当前未命中需要双人复核占位的动作。"],
        digest={
            "summary": dual_check_summary,
            "dual_check_summary": dual_check_summary,
            "placeholder_summary": "reviewer note only / placeholder only / 非真实签批",
            "readiness_status_summary": "dual-check placeholder retained for reviewer verification",
        },
        rows=[dict(row) for row in dual_check_rows],
        extra_fields={
            "placeholder_mode": "reviewer_note_only",
            "required_action_rows": [dict(row) for row in dual_check_rows],
        },
    )
    return {
        "fixtures": fixtures,
        "run_metadata_profile": run_metadata_profile,
        "operator_authorization_profile": operator_authorization_profile,
        "training_record": training_record,
        "sop_version_binding": sop_version_binding,
        "qc_flag_catalog": qc_flag_catalog,
        "recovery_action_log": recovery_action_log,
        "reviewer_dual_check_placeholder": reviewer_dual_check_placeholder,
    }


def load_human_governance_fixtures(
    *,
    fixtures_root: str | Path | None = None,
) -> dict[str, Any]:
    fixture_root = _resolve_human_governance_fixture_root(fixtures_root)
    fixture_paths = {
        "operator_roster": fixture_root / OPERATOR_ROSTER_FIXTURE_PATH[0] / OPERATOR_ROSTER_FIXTURE_PATH[1],
        "authorization_scope": fixture_root / AUTHORIZATION_SCOPE_FIXTURE_PATH[0] / AUTHORIZATION_SCOPE_FIXTURE_PATH[1],
        "training_records": fixture_root / TRAINING_RECORDS_FIXTURE_PATH[0] / TRAINING_RECORDS_FIXTURE_PATH[1],
        "sop_versions": fixture_root / SOP_VERSIONS_FIXTURE_PATH[0] / SOP_VERSIONS_FIXTURE_PATH[1],
        "qc_flag_catalog": fixture_root / QC_FLAG_CATALOG_FIXTURE_PATH[0] / QC_FLAG_CATALOG_FIXTURE_PATH[1],
    }
    operator_roster_payload = _read_fixture_json(fixture_paths["operator_roster"])
    authorization_scope_payload = _read_fixture_json(fixture_paths["authorization_scope"])
    training_records_payload = _read_fixture_json(fixture_paths["training_records"])
    sop_versions_payload = _read_fixture_json(fixture_paths["sop_versions"])
    qc_flag_catalog_payload = _read_fixture_json(fixture_paths["qc_flag_catalog"])
    return {
        "schema_version": HUMAN_GOVERNANCE_FIXTURE_SCHEMA_VERSION,
        "fixture_root": str(fixture_root),
        "fixture_paths": {name: str(path) for name, path in fixture_paths.items()},
        "operator_roster": _fixture_rows(operator_roster_payload, "people", "operators"),
        "authorization_scopes": _fixture_rows(authorization_scope_payload, "authorizations", "rows"),
        "training_records": _fixture_rows(training_records_payload, "records", "training_records"),
        "sop_versions": _fixture_rows(sop_versions_payload, "sop_versions", "rows"),
        "qc_flag_catalog_rows": _fixture_rows(qc_flag_catalog_payload, "flags", "rows"),
    }


def _build_governance_payload(
    *,
    artifact_type: str,
    title_text: str,
    run_id: str,
    run_dir: str,
    primary_artifact_path: str,
    artifact_paths: dict[str, Any],
    summary_line: str,
    summary_lines: Iterable[str],
    detail_lines: Iterable[str],
    digest: dict[str, Any],
    rows: list[dict[str, Any]],
    extra_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    normalized_summary_lines = [str(item).strip() for item in summary_lines if str(item).strip()]
    normalized_detail_lines = [str(item).strip() for item in detail_lines if str(item).strip()]
    normalized_artifact_paths = {
        artifact_type: primary_artifact_path,
        **{
            key: str(value).strip()
            for key, value in dict(artifact_paths or {}).items()
            if str(value).strip()
        },
    }
    review_surface = {
        "summary_text": summary_line,
        "summary_lines": list(normalized_summary_lines),
        "detail_lines": list(normalized_detail_lines),
        "artifact_paths": dict(normalized_artifact_paths),
        "phase_filters": ["step2_tail_stage3_bridge"],
        "artifact_role_filters": ["diagnostic_analysis"],
        "standard_family_filters": ["step2_human_governance"],
        "evidence_category_filters": ["readiness_governance", "human_governance"],
        "boundary_filters": ["reviewer_only", "not_real_acceptance_evidence", "placeholder_only"],
        "non_claim_filters": ["reviewer_only", "not_formal_claim"],
        "evidence_source_filters": ["simulated"],
        "anchor_id": artifact_type.replace("_", "-"),
        "anchor_label": title_text,
    }
    payload = {
        "schema_version": "step2-human-governance-artifact-v1",
        "artifact_type": artifact_type,
        "generated_at": generated_at,
        "run_id": run_id,
        "run_dir": run_dir,
        "title_text": title_text,
        "summary_line": summary_line,
        "reviewer_summary_line": summary_line,
        "summary_lines": list(normalized_summary_lines),
        "rows": [dict(row) for row in rows],
        "digest": {
            "summary": summary_line,
            "boundary_summary": _BOUNDARY_SUMMARY,
            **dict(digest or {}),
        },
        "review_surface": review_surface,
        "artifact_paths": dict(normalized_artifact_paths),
        "path": primary_artifact_path,
        "evidence_source": "simulated",
        "evidence_state": "reviewer_note_only",
        "overall_status": "reviewer_ready",
        "placeholder_only": True,
        "reviewer_note_only": True,
        **_BOUNDARY_FLAGS,
    }
    if extra_fields:
        payload.update(dict(extra_fields))
    return payload


def _resolve_human_governance_fixture_root(fixtures_root: str | Path | None = None) -> Path:
    if fixtures_root is not None:
        return Path(fixtures_root).expanduser().resolve()
    env_value = os.environ.get(HUMAN_GOVERNANCE_FIXTURE_ROOT_ENV)
    if str(env_value or "").strip():
        return Path(str(env_value)).expanduser().resolve()
    return (Path(__file__).resolve().parents[1] / "configs").resolve()


def _read_fixture_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _fixture_rows(payload: dict[str, Any], *keys: str) -> list[dict[str, Any]]:
    for key in keys:
        values = payload.get(key)
        if isinstance(values, list):
            return [dict(item) for item in values if isinstance(item, dict)]
    return []


def _pick_person(
    candidate: Any,
    roster_rows: Any,
    *,
    role: str,
    environment_scope: list[str],
    route_scope: list[str],
) -> dict[str, Any]:
    rows = [dict(item) for item in list(roster_rows or []) if isinstance(item, dict)]
    candidate_texts = _candidate_texts(candidate)
    if candidate_texts:
        for row in rows:
            row_role = str(row.get("role") or "").strip().lower()
            if row_role != role:
                continue
            person_id = str(row.get("person_id") or "").strip()
            display_name = str(row.get("display_name") or "").strip()
            aliases = {person_id, display_name, str(row.get("alias") or "").strip()}
            if any(text for text in aliases if text in candidate_texts):
                return row
    for row in rows:
        if str(row.get("role") or "").strip().lower() != role:
            continue
        if str(row.get("active_state") or "").strip() not in {"active", "current", ""}:
            continue
        row_environment_scope = set(_text_list(row.get("environment_scope") or []))
        row_route_scope = set(_text_list(row.get("route_scope") or []))
        if set(environment_scope).issubset(row_environment_scope) and (
            not route_scope or not row_route_scope or set(route_scope).issubset(row_route_scope)
        ):
            return row
    return next(
        (
            row
            for row in rows
            if str(row.get("role") or "").strip().lower() == role
        ),
        {},
    )


def _pick_authorization_row(
    person_id: Any,
    authorization_rows: Any,
    *,
    environment_scope: list[str],
    route_scope: list[str],
) -> dict[str, Any]:
    target_person_id = str(person_id or "").strip()
    rows = [
        dict(item)
        for item in list(authorization_rows or [])
        if isinstance(item, dict) and str(dict(item).get("person_id") or "").strip() == target_person_id
    ]
    for row in rows:
        if set(route_scope).issubset(set(_text_list(row.get("route_scope") or []))) and set(environment_scope).issubset(
            set(_text_list(row.get("environment_scope") or []))
        ):
            return row
    return dict(rows[0]) if rows else {}


def _person_rows(rows: Any, person_id: Any) -> list[dict[str, Any]]:
    target_person_id = str(person_id or "").strip()
    return [
        dict(item)
        for item in list(rows or [])
        if isinstance(item, dict) and str(dict(item).get("person_id") or "").strip() == target_person_id
    ]


def _active_sop_rows(rows: Any, *, route_scope: list[str]) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for item in list(rows or []):
        if not isinstance(item, dict):
            continue
        row = dict(item)
        if str(row.get("active_state") or "").strip() not in {"current", "active", ""}:
            continue
        row_routes = set(_text_list(row.get("route_scope") or []))
        if row_routes and "all" not in row_routes and not set(route_scope).intersection(row_routes):
            continue
        selected.append(row)
    return selected


def _applicable_qc_flags(rows: Any, *, route_scope: list[str]) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for item in list(rows or []):
        if not isinstance(item, dict):
            continue
        row = dict(item)
        row_routes = set(_text_list(row.get("route_scope") or []))
        if row_routes and "all" not in row_routes and not set(route_scope).intersection(row_routes):
            continue
        selected.append(row)
    return selected


def _build_dual_check_row(row: dict[str, Any], *, reviewer: dict[str, Any]) -> dict[str, Any]:
    return {
        "flag_code": str(row.get("flag_code") or "").strip(),
        "critical_action_label": str(
            row.get("critical_action_label")
            or row.get("display_name")
            or row.get("flag_code")
            or "--"
        ).strip(),
        "requires_dual_check": True,
        "placeholder_mode": "reviewer_note_only",
        "approval_state": "not_started",
        "reviewer_summary": _person_summary(reviewer, fallback="未绑定 reviewer"),
        "reviewer_note": str(
            row.get("reviewer_note")
            or "Step 2 reviewer placeholder only; not electronic signoff."
        ).strip(),
    }


def _build_recovery_rows(
    *,
    summary_payload: dict[str, Any],
    workbench_payload: dict[str, Any],
    reviewer: dict[str, Any],
) -> list[dict[str, Any]]:
    stats = dict(summary_payload.get("stats") or {})
    warning_count = int(
        summary_payload.get("warnings", stats.get("warning_count", 0)) or 0
    )
    error_count = int(
        summary_payload.get("errors", stats.get("error_count", 0)) or 0
    )
    rows: list[dict[str, Any]] = []
    if error_count > 0:
        rows.append(
            {
                "action_id": "recovery-errors-review",
                "status": "review_required",
                "summary": f"存在 {error_count} 条错误，需 reviewer 复核恢复动作。",
                "reviewer": _person_summary(reviewer, fallback="未绑定 reviewer"),
            }
        )
    if warning_count > 0:
        rows.append(
            {
                "action_id": "recovery-warnings-review",
                "status": "review_required",
                "summary": f"存在 {warning_count} 条 warning，需 reviewer 确认恢复记录。",
                "reviewer": _person_summary(reviewer, fallback="未绑定 reviewer"),
            }
        )
    workbench_summary = str(
        workbench_payload.get("summary_line")
        or workbench_payload.get("review_summary")
        or ""
    ).strip()
    if workbench_summary:
        rows.append(
            {
                "action_id": "recovery-workbench-review",
                "status": "not_required",
                "summary": f"workbench 记录: {workbench_summary}",
                "reviewer": _person_summary(reviewer, fallback="未绑定 reviewer"),
            }
        )
    return rows


def _collect_route_scope(summary_payload: dict[str, Any], manifest_payload: dict[str, Any]) -> list[str]:
    enabled_devices = _text_list(
        dict(summary_payload.get("stats") or {}).get("enabled_devices")
        or dict(manifest_payload.get("config_snapshot") or {}).get("enabled_devices")
        or []
    )
    scopes: list[str] = []
    if any("gas_analyzer" in item or "pressure" in item for item in enabled_devices):
        scopes.append("gas")
    if any(item in {"humidity_generator", "dewpoint_meter"} for item in enabled_devices):
        scopes.append("water")
    if not scopes:
        scopes = ["gas", "water"]
    return sorted(set(scopes))


def _collect_environment_scope(summary_payload: dict[str, Any], manifest_payload: dict[str, Any]) -> list[str]:
    simulation_markers = [
        summary_payload.get("simulation_mode"),
        dict(manifest_payload.get("features") or {}).get("simulation_mode"),
    ]
    environment_scope = list(_DEFAULT_ENVIRONMENT_SCOPE)
    if any(bool(item) for item in simulation_markers):
        return environment_scope
    return environment_scope


def _required_training_modules(route_scope: list[str]) -> set[str]:
    modules = set(_REQUIRED_OPERATOR_MODULES["base"])
    if "gas" in route_scope:
        modules.update(_REQUIRED_OPERATOR_MODULES["gas"])
    if "water" in route_scope:
        modules.update(_REQUIRED_OPERATOR_MODULES["water"])
    return modules


def _authorization_summary(row: dict[str, Any]) -> str:
    if not row:
        return ""
    route_scope = ", ".join(_text_list(row.get("route_scope") or [])) or "--"
    environment_scope = ", ".join(_text_list(row.get("environment_scope") or [])) or "--"
    return (
        f"{str(row.get('authorization_id') or '--')} | "
        f"route {route_scope} | env {environment_scope} | "
        f"{str(row.get('authorization_state') or 'missing')}"
    )


def _person_summary(row: dict[str, Any], *, fallback: str) -> str:
    if not row:
        return fallback
    display_name = str(row.get("display_name") or "").strip()
    person_id = str(row.get("person_id") or "").strip()
    if display_name and person_id:
        return f"{display_name} ({person_id})"
    return display_name or person_id or fallback


def _critical_action_labels(rows: list[dict[str, Any]]) -> list[str]:
    labels: list[str] = []
    for row in rows:
        label = str(
            row.get("critical_action_label")
            or row.get("display_name")
            or row.get("flag_code")
            or ""
        ).strip()
        if label and label not in labels:
            labels.append(label)
    return labels


def _recovery_summary(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "恢复动作日志: 当前 run 未记录恢复动作，仅保留 reviewer-only 占位。"
    open_count = sum(
        1
        for row in rows
        if str(row.get("status") or "").strip() not in {"closed", "not_required", "none"}
    )
    return f"恢复动作日志: {len(rows)} 条，其中待 reviewer 关注 {open_count} 条。"


def _existing_artifact_path(run_dir: str, filename: str) -> str:
    if not run_dir:
        return ""
    path = Path(run_dir) / filename
    return str(path) if path.exists() else ""


def _text_list(values: Iterable[Any]) -> list[str]:
    rows: list[str] = []
    for item in list(values or []):
        text = str(item or "").strip()
        if text:
            rows.append(text)
    return rows


def _candidate_texts(candidate: Any) -> set[str]:
    if isinstance(candidate, dict):
        values = []
        for key in ("person_id", "display_name", "name", "id"):
            text = str(candidate.get(key) or "").strip()
            if text:
                values.append(text)
        return set(values)
    text = str(candidate or "").strip()
    return {text} if text else set()
