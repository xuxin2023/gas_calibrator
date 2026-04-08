from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .engineering_isolation_admission_checklist import (
    build_engineering_isolation_admission_checklist,
)
from .stage_admission_review_pack import build_stage_admission_review_pack
from .stage3_real_validation_plan import (
    STAGE3_REAL_VALIDATION_PLAN_FILENAME,
    STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME,
    _artifact_ref,
    _normalize_artifact_paths,
    build_stage3_real_validation_plan,
)
from .stage3_real_validation_plan_artifact_entry import _VALIDATION_CATEGORY_LABELS


STAGE3_STANDARDS_ALIGNMENT_MATRIX_FILENAME = "stage3_standards_alignment_matrix.json"
STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_FILENAME = "stage3_standards_alignment_matrix.md"

_BOUNDARY_STATEMENTS = [
    "readiness mapping only",
    "not accreditation claim",
    "not compliance certification",
    "not real acceptance",
    "cannot replace real metrology validation",
    "simulation / offline / headless only",
]


def build_stage3_standards_alignment_matrix(
    *,
    run_id: str,
    step2_readiness_summary: dict[str, Any] | None,
    metrology_calibration_contract: dict[str, Any] | None,
    phase_transition_bridge: dict[str, Any] | None,
    stage_admission_review_pack: dict[str, Any] | None = None,
    engineering_isolation_admission_checklist: dict[str, Any] | None = None,
    stage3_real_validation_plan: dict[str, Any] | None = None,
    artifact_paths: dict[str, Any] | None = None,
) -> dict[str, Any]:
    readiness = dict(step2_readiness_summary or {})
    metrology = dict(metrology_calibration_contract or {})
    bridge = dict(phase_transition_bridge or {})

    review_pack = dict(stage_admission_review_pack or {})
    if not review_pack:
        review_pack = build_stage_admission_review_pack(
            run_id=run_id,
            step2_readiness_summary=readiness,
            metrology_calibration_contract=metrology,
            phase_transition_bridge=bridge,
            artifact_paths=artifact_paths,
        )

    checklist = dict(engineering_isolation_admission_checklist or {})
    if not checklist:
        checklist = build_engineering_isolation_admission_checklist(
            run_id=run_id,
            step2_readiness_summary=readiness,
            metrology_calibration_contract=metrology,
            phase_transition_bridge=bridge,
            stage_admission_review_pack=review_pack,
            artifact_paths=artifact_paths,
        )

    stage3_plan = dict(stage3_real_validation_plan or {})
    if not stage3_plan:
        stage3_plan = build_stage3_real_validation_plan(
            run_id=run_id,
            step2_readiness_summary=readiness,
            metrology_calibration_contract=metrology,
            phase_transition_bridge=bridge,
            stage_admission_review_pack=review_pack,
            engineering_isolation_admission_checklist=checklist,
            artifact_paths=artifact_paths,
        )

    pack_raw = dict(review_pack.get("raw") or {})
    pack_display = dict(review_pack.get("display") or {})
    checklist_raw = dict(checklist.get("raw") or {})
    checklist_display = dict(checklist.get("display") or {})
    plan_raw = dict(stage3_plan.get("raw") or {})
    plan_display = dict(stage3_plan.get("display") or {})
    bridge_display = dict(bridge.get("reviewer_display") or {})
    readiness_display = dict(readiness.get("reviewer_display") or {})
    metrology_display = dict(metrology.get("reviewer_display") or {})

    artifact_path_map = _normalize_artifact_paths(
        artifact_paths=artifact_paths,
        checklist_artifact_paths=checklist_raw.get("artifact_paths"),
        pack_artifact_paths=pack_raw.get("artifact_paths"),
    )
    direct_artifact_paths = dict(artifact_paths or {})
    artifact_path_map["stage3_real_validation_plan"] = str(
        direct_artifact_paths.get("stage3_real_validation_plan") or STAGE3_REAL_VALIDATION_PLAN_FILENAME
    )
    artifact_path_map["stage3_real_validation_plan_reviewer_artifact"] = str(
        direct_artifact_paths.get("stage3_real_validation_plan_reviewer_artifact")
        or STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME
    )

    artifact_refs = {
        "step2_readiness_summary": _artifact_ref(
            readiness,
            artifact_path_map["step2_readiness_summary"],
            str(readiness_display.get("summary_text") or "").strip(),
        ),
        "metrology_calibration_contract": _artifact_ref(
            metrology,
            artifact_path_map["metrology_calibration_contract"],
            str(metrology_display.get("summary_text") or "").strip(),
        ),
        "phase_transition_bridge": _artifact_ref(
            bridge,
            artifact_path_map["phase_transition_bridge"],
            str(bridge_display.get("summary_text") or "").strip(),
        ),
        "phase_transition_bridge_reviewer_artifact": {
            "artifact_type": "phase_transition_bridge_reviewer_artifact",
            "phase": str(bridge.get("phase") or ""),
            "overall_status": str(bridge.get("overall_status") or ""),
            "path": artifact_path_map["phase_transition_bridge_reviewer_artifact"],
            "summary_text": str(bridge_display.get("summary_text") or "").strip(),
        },
        "stage_admission_review_pack": _artifact_ref(
            pack_raw,
            artifact_path_map["stage_admission_review_pack"],
            str(pack_display.get("summary_text") or "").strip(),
        ),
        "stage_admission_review_pack_reviewer_artifact": {
            "artifact_type": "stage_admission_review_pack_reviewer_artifact",
            "phase": str(pack_raw.get("phase") or ""),
            "overall_status": str(pack_raw.get("overall_status") or ""),
            "path": artifact_path_map["stage_admission_review_pack_reviewer_artifact"],
            "summary_text": str(pack_display.get("summary_text") or "").strip(),
        },
        "engineering_isolation_admission_checklist": _artifact_ref(
            checklist_raw,
            artifact_path_map["engineering_isolation_admission_checklist"],
            str(checklist_display.get("summary_text") or "").strip(),
        ),
        "engineering_isolation_admission_checklist_reviewer_artifact": {
            "artifact_type": "engineering_isolation_admission_checklist_reviewer_artifact",
            "phase": str(checklist_raw.get("phase") or ""),
            "overall_status": str(checklist_raw.get("overall_status") or ""),
            "path": artifact_path_map["engineering_isolation_admission_checklist_reviewer_artifact"],
            "summary_text": str(checklist_display.get("summary_text") or "").strip(),
        },
        "stage3_real_validation_plan": _artifact_ref(
            plan_raw,
            artifact_path_map["stage3_real_validation_plan"],
            str(plan_display.get("summary_text") or "").strip(),
        ),
        "stage3_real_validation_plan_reviewer_artifact": {
            "artifact_type": "stage3_real_validation_plan_reviewer_artifact",
            "phase": str(plan_raw.get("phase") or ""),
            "overall_status": str(plan_raw.get("overall_status") or ""),
            "path": artifact_path_map["stage3_real_validation_plan_reviewer_artifact"],
            "summary_text": str(plan_display.get("summary_text") or "").strip(),
        },
    }

    rows = _build_mapping_rows(
        artifact_refs=artifact_refs,
        plan_raw=plan_raw,
        plan_display=plan_display,
        checklist_display=checklist_display,
        pack_display=pack_display,
        metrology_display=metrology_display,
    )
    readiness_status_counts = _count_by_key(rows, key_name="readiness_status")
    standard_families = _dedupe(str(item.get("standard_id_or_family") or "").strip() for item in rows)
    required_evidence_categories = _dedupe(
        label
        for row in rows
        for label in list(row.get("required_evidence_categories") or [])
    )

    summary_seed = str(plan_display.get("summary_text") or checklist_display.get("summary_text") or "").strip()
    reviewer_note_text = (
        "Step 2 tail / Stage 3 bridge：本矩阵只补足 standards family 的 readiness mapping 与证据覆盖梳理，"
        "明确依赖 engineering-isolation handoff 与 Stage 3 real validation plan；"
        "不是 accreditation claim，不是 compliance certification，不是 real acceptance，"
        "也不能替代真实计量验证。"
    )
    display = {
        "title_text": "Stage 3 Standards Alignment Matrix / 第三阶段标准符合性映射与证据覆盖矩阵",
        "summary_text": (
            f"{summary_seed} 第三阶段标准符合性映射与证据覆盖矩阵：把 standards family / topic-level "
            "mapping、required evidence categories、当前仓库已有治理工件覆盖与缺口统一收口为 reviewer-facing readiness mapping。"
        ).strip(),
        "reviewer_note_text": reviewer_note_text,
        "status_line": str(plan_display.get("status_line") or checklist_display.get("status_line") or "").strip(),
        "current_stage_text": str(
            plan_display.get("current_stage_text")
            or checklist_display.get("current_stage_text")
            or pack_display.get("current_stage_text")
            or ""
        ).strip(),
        "next_stage_text": str(
            plan_display.get("next_stage_text")
            or checklist_display.get("next_stage_text")
            or pack_display.get("next_stage_text")
            or ""
        ).strip(),
        "engineering_isolation_text": str(
            plan_display.get("engineering_isolation_text")
            or checklist_display.get("engineering_isolation_text")
            or pack_display.get("engineering_isolation_text")
            or ""
        ).strip(),
        "real_acceptance_text": str(
            plan_display.get("real_acceptance_text")
            or checklist_display.get("real_acceptance_text")
            or pack_display.get("real_acceptance_text")
            or ""
        ).strip(),
        "stage_bridge_text": "定位：Step 2 tail / Stage 3 bridge；本轮只做 standards alignment / evidence coverage readiness mapping。",
        "artifact_role_text": "execution_summary + formal_analysis",
        "standard_family_lines": [f"标准家族：{item}" for item in standard_families],
        "required_evidence_lines": [f"required evidence categories：{item}" for item in required_evidence_categories],
        "matrix_lines": [_build_matrix_line(row) for row in rows],
        "boundary_lines": [f"边界：{line}" for line in _BOUNDARY_STATEMENTS],
        "artifact_lines": _build_artifact_lines(artifact_refs),
    }
    markdown = _render_stage3_standards_alignment_matrix_markdown(display)

    raw = {
        "schema_version": "1.0",
        "artifact_type": "stage3_standards_alignment_matrix",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": str(
            run_id
            or plan_raw.get("run_id")
            or checklist_raw.get("run_id")
            or pack_raw.get("run_id")
            or bridge.get("run_id")
            or ""
        ),
        "phase": str(plan_raw.get("phase") or checklist_raw.get("phase") or "step2_tail_stage3_bridge"),
        "mode": str(plan_raw.get("mode") or checklist_raw.get("mode") or "simulation_only"),
        "overall_status": str(plan_raw.get("overall_status") or checklist_raw.get("overall_status") or "step2_tail_in_progress"),
        "recommended_next_stage": str(
            plan_raw.get("recommended_next_stage")
            or checklist_raw.get("recommended_next_stage")
            or "engineering_isolation"
        ),
        "mapping_scope": "family_topic_level_only",
        "not_real_acceptance_evidence": True,
        "boundary_statements": list(_BOUNDARY_STATEMENTS),
        "artifact_refs": artifact_refs,
        "artifact_paths": artifact_path_map,
        "standard_families": standard_families,
        "required_evidence_categories": required_evidence_categories,
        "readiness_status_counts": readiness_status_counts,
        "rows": rows,
        "notes": [
            "stage3_standards_alignment_matrix",
            "step2_tail_stage3_bridge",
            "engineering_isolation_dependency",
            "readiness_mapping_only",
            "not_accreditation_claim",
            "not_compliance_certification",
            "simulation_offline_headless_only",
            "not_real_acceptance_evidence",
            "default_path_unchanged",
        ],
    }
    return {
        "available": True,
        "artifact_type": "stage3_standards_alignment_matrix",
        "filename": STAGE3_STANDARDS_ALIGNMENT_MATRIX_FILENAME,
        "reviewer_filename": STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_FILENAME,
        "raw": raw,
        "display": display,
        "markdown": markdown,
    }


def _build_mapping_rows(
    *,
    artifact_refs: dict[str, dict[str, Any]],
    plan_raw: dict[str, Any],
    plan_display: dict[str, Any],
    checklist_display: dict[str, Any],
    pack_display: dict[str, Any],
    metrology_display: dict[str, Any],
) -> list[dict[str, Any]]:
    validation_items = [dict(item) for item in list(plan_raw.get("validation_items") or []) if isinstance(item, dict)]
    categories_by_source = _collect_category_sets(validation_items)
    plan_summary = str(plan_display.get("summary_text") or "").strip()
    checklist_summary = str(checklist_display.get("summary_text") or "").strip()
    pack_summary = str(pack_display.get("summary_text") or "").strip()
    metrology_summary = str(metrology_display.get("summary_text") or "").strip()

    return [
        _mapping_row(
            mapping_id="cma_meteorology_observation_qc",
            standard_family="中国气象 / 气象观测质量控制",
            standard_id_or_family="中国气象局 / 气象行业观测与质量控制相关要求",
            topic_or_control_object="观测流程质量控制、参考链留痕、审阅导出与数据质量门禁",
            reviewer_note=reviewer_note(
                "聚焦观测流程质量控制、审阅留痕和 reviewer evidence plan；当前仍处于 Step 2 tail / Stage 3 bridge。"
            ),
            linked_existing_artifacts=[
                "step2_readiness_summary",
                "phase_transition_bridge",
                "stage_admission_review_pack",
                "engineering_isolation_admission_checklist",
            ],
            required_evidence_categories=categories_by_source["quality_review"],
            current_evidence_coverage=[
                _first_text(pack_summary, checklist_summary),
                coverage_line("phase_transition_bridge", artifact_refs),
                coverage_line("stage_admission_review_pack", artifact_refs),
                coverage_line("engineering_isolation_admission_checklist", artifact_refs),
            ],
            gap_note=gap_note("仓库当前只补足 family/topic-level 质量控制映射，未包含具体条款编号、限值或真实观测站点 acceptance 结论。"),
        ),
        _mapping_row(
            mapping_id="cnas_cl01_general_competence",
            standard_family="CNAS / 实验室认可",
            standard_id_or_family="CNAS-CL01",
            topic_or_control_object="能力要求、方法确认、记录完整性、结果报告与审阅留痕",
            reviewer_note=reviewer_note(
                "复用 stage_admission_review_pack / checklist / stage3 plan 的 canonical wording，只做 readiness mapping。"
            ),
            linked_existing_artifacts=[
                "stage_admission_review_pack",
                "engineering_isolation_admission_checklist",
                "stage3_real_validation_plan",
            ],
            required_evidence_categories=_dedupe(
                categories_by_source["traceability_review"]
                + categories_by_source["quality_review"]
                + categories_by_source["decision_review"]
            ),
            current_evidence_coverage=[
                coverage_line("stage_admission_review_pack", artifact_refs),
                coverage_line("engineering_isolation_admission_checklist", artifact_refs),
                coverage_line("stage3_real_validation_plan", artifact_refs),
            ],
            gap_note=gap_note("repo 中没有 CL01 精确条文文本，本轮只做能力主题和证据覆盖准备，不输出符合性结论。"),
        ),
        _mapping_row(
            mapping_id="cnas_cl01_g002_traceability",
            standard_family="CNAS / 计量溯源性",
            standard_id_or_family="CNAS-CL01-G002",
            topic_or_control_object="reference traceability chain、证书有效性、参考标准与 run 绑定",
            reviewer_note=reviewer_note(
                "traceability 依赖 metrology contract 与 Stage 3 real validation plan；当前仍是 engineering-isolation dependency。"
            ),
            linked_existing_artifacts=[
                "metrology_calibration_contract",
                "phase_transition_bridge",
                "stage3_real_validation_plan",
            ],
            required_evidence_categories=_dedupe(
                categories_by_source["reference_instrument_enforcement"]
                + categories_by_source["traceability_review"]
            ),
            current_evidence_coverage=[
                coverage_line("metrology_calibration_contract", artifact_refs),
                coverage_line("phase_transition_bridge", artifact_refs),
                coverage_line("stage3_real_validation_plan", artifact_refs),
            ],
            gap_note=gap_note("只建立 traceability readiness mapping；真实证书、检定周期与实验室链条复核仍待 Stage 3。"),
        ),
        _mapping_row(
            mapping_id="cnas_cl01_g003_uncertainty",
            standard_family="CNAS / 测量不确定度",
            standard_id_or_family="CNAS-CL01-G003",
            topic_or_control_object="不确定度预算模板、最终结果依赖真实 run、拟合残差与结果评审",
            reviewer_note=reviewer_note(
                "当前只固化 uncertainty budget template 与 reviewer evidence categories，不能把 simulation 结果写成最终不确定度结论。"
            ),
            linked_existing_artifacts=[
                "metrology_calibration_contract",
                "stage3_real_validation_plan",
            ],
            required_evidence_categories=categories_by_source["uncertainty_result"],
            current_evidence_coverage=[
                coverage_line("metrology_calibration_contract", artifact_refs),
                plan_summary,
            ],
            gap_note=gap_note("未附 G003 精确条文，本轮不输出不确定度限值或真实实验室判定，只保留 topic-level mapping。"),
        ),
        _mapping_row(
            mapping_id="iso_iec_17025_family",
            standard_family="ISO/IEC 17025",
            standard_id_or_family="ISO/IEC 17025",
            topic_or_control_object="competence、traceability、method validation、records、reporting、decision review",
            reviewer_note=reviewer_note(
                "使用 Stage 3 plan / pack / checklist 的现有 reviewer wording 衔接 readiness mapping，明确 not accreditation claim。"
            ),
            linked_existing_artifacts=[
                "metrology_calibration_contract",
                "stage_admission_review_pack",
                "engineering_isolation_admission_checklist",
                "stage3_real_validation_plan",
            ],
            required_evidence_categories=_dedupe(
                categories_by_source["reference_instrument_enforcement"]
                + categories_by_source["traceability_review"]
                + categories_by_source["uncertainty_result"]
                + categories_by_source["decision_review"]
            ),
            current_evidence_coverage=[
                metrology_summary,
                pack_summary,
                checklist_summary,
                plan_summary,
            ],
            gap_note=gap_note("无精确 clause 文本时只保留 family/topic-level mapping，不生成认证、认可或符合性结论。"),
        ),
        _mapping_row(
            mapping_id="iso_6142_family",
            standard_family="ISO gas standards",
            standard_id_or_family="ISO 6142 family",
            topic_or_control_object="标准气体配制、批次证书、reference gas source traceability",
            reviewer_note=reviewer_note(
                "仅对齐 gas source traceability / batch evidence / reviewer evidence plan，不宣称 mixture 已经满足任何标准限值。"
            ),
            linked_existing_artifacts=[
                "metrology_calibration_contract",
                "stage3_real_validation_plan",
            ],
            required_evidence_categories=_dedupe(
                categories_by_source["reference_instrument_enforcement"]
                + categories_by_source["traceability_review"]
            ),
            current_evidence_coverage=[
                coverage_line("metrology_calibration_contract", artifact_refs),
                coverage_line("stage3_real_validation_plan", artifact_refs),
            ],
            gap_note=gap_note("repo 仅有 standard_gas_source contract/schema，未提供 6142 family 具体条款、配制计算或认证结果。"),
        ),
        _mapping_row(
            mapping_id="iso_6143_family",
            standard_family="ISO gas standards",
            standard_id_or_family="ISO 6143",
            topic_or_control_object="比较方法、标定函数、拟合残差、结果回归与 reviewer digest",
            reviewer_note=reviewer_note(
                "当前只把 calibration function / fit residual / uncertainty review 作为 Stage 3 evidence coverage 准备，不是 compliance certification。"
            ),
            linked_existing_artifacts=[
                "metrology_calibration_contract",
                "stage3_real_validation_plan",
            ],
            required_evidence_categories=_dedupe(
                categories_by_source["uncertainty_result"] + categories_by_source["decision_review"]
            ),
            current_evidence_coverage=[
                coverage_line("metrology_calibration_contract", artifact_refs),
                coverage_line("stage3_real_validation_plan", artifact_refs),
            ],
            gap_note=gap_note("仓库没有 ISO 6143 精确条文或方法参数，本轮只对 topic/control-object 级别做 reviewer mapping。"),
        ),
        _mapping_row(
            mapping_id="iso_6145_family",
            standard_family="ISO gas standards",
            standard_id_or_family="ISO 6145 family",
            topic_or_control_object="动态生成、流量控制、湿度/气体源路由与运行证据",
            reviewer_note=reviewer_note(
                "当前只把 humidity / gas source contract 与运行路由上下文纳入 standards readiness mapping，不能替代真实 bench validation。"
            ),
            linked_existing_artifacts=[
                "metrology_calibration_contract",
                "engineering_isolation_admission_checklist",
                "stage3_real_validation_plan",
            ],
            required_evidence_categories=_dedupe(
                categories_by_source["reference_instrument_enforcement"]
                + categories_by_source["real_world_repeatability"]
            ),
            current_evidence_coverage=[
                coverage_line("metrology_calibration_contract", artifact_refs),
                coverage_line("engineering_isolation_admission_checklist", artifact_refs),
                coverage_line("stage3_real_validation_plan", artifact_refs),
            ],
            gap_note=gap_note("repo 当前只具备 route context / source contract / evidence coverage 骨架，没有 6145 family 条款级动态发生器验证结果。"),
        ),
        _mapping_row(
            mapping_id="wmo_gaw_quality_assurance",
            standard_family="WMO / GAW",
            standard_id_or_family="WMO / GAW QA",
            topic_or_control_object="可追溯、质量保证、数据质量、comparability、异常复核与 reviewer evidence plan",
            reviewer_note=reviewer_note(
                "聚焦全球观测网络常见 QA/QC 主题的 readiness mapping，只说明证据覆盖准备，不暗示已被任何网络认可。"
            ),
            linked_existing_artifacts=[
                "phase_transition_bridge",
                "stage_admission_review_pack",
                "engineering_isolation_admission_checklist",
                "stage3_real_validation_plan",
            ],
            required_evidence_categories=_dedupe(
                categories_by_source["traceability_review"]
                + categories_by_source["real_world_repeatability"]
                + categories_by_source["anomaly_retest"]
                + categories_by_source["decision_review"]
            ),
            current_evidence_coverage=[
                coverage_line("phase_transition_bridge", artifact_refs),
                coverage_line("stage_admission_review_pack", artifact_refs),
                coverage_line("engineering_isolation_admission_checklist", artifact_refs),
                coverage_line("stage3_real_validation_plan", artifact_refs),
            ],
            gap_note=gap_note("未附 WMO/GAW 精确规范文本，本轮只对 traceability / QA/QC / comparability 主题做 family-level mapping。"),
        ),
    ]


def _mapping_row(
    *,
    mapping_id: str,
    standard_family: str,
    standard_id_or_family: str,
    topic_or_control_object: str,
    reviewer_note: str,
    linked_existing_artifacts: list[str],
    required_evidence_categories: list[str],
    current_evidence_coverage: list[str],
    gap_note: str,
) -> dict[str, Any]:
    return {
        "mapping_id": mapping_id,
        "mapping_level": "family_topic_level_only",
        "standard_family": standard_family,
        "standard_id_or_family": standard_id_or_family,
        "topic_or_control_object": topic_or_control_object,
        "applicability": "Step 2 tail / Stage 3 bridge reviewer readiness mapping",
        "reviewer_role": "reviewer / approver",
        "reviewer_note": reviewer_note,
        "linked_existing_artifacts": list(linked_existing_artifacts),
        "required_evidence_categories": list(required_evidence_categories),
        "current_evidence_coverage": list(current_evidence_coverage),
        "readiness_status": "mapping_ready_evidence_pending",
        "gap_note": gap_note,
        "non_claim": list(_BOUNDARY_STATEMENTS),
        "digest": (
            f"{standard_id_or_family} | {topic_or_control_object} | "
            f"当前覆盖 {len(current_evidence_coverage)} | 证据类别 {len(required_evidence_categories)} | "
            "readiness mapping only"
        ),
    }


def _build_artifact_lines(artifact_refs: dict[str, dict[str, Any]]) -> list[str]:
    order = (
        ("step2_readiness_summary", "step2_readiness_summary.json"),
        ("metrology_calibration_contract", "metrology_calibration_contract.json"),
        ("phase_transition_bridge", "phase_transition_bridge.json"),
        ("phase_transition_bridge_reviewer_artifact", "phase_transition_bridge_reviewer.md"),
        ("stage_admission_review_pack", "stage_admission_review_pack.json"),
        ("stage_admission_review_pack_reviewer_artifact", "stage_admission_review_pack.md"),
        ("engineering_isolation_admission_checklist", "engineering_isolation_admission_checklist.json"),
        ("engineering_isolation_admission_checklist_reviewer_artifact", "engineering_isolation_admission_checklist.md"),
        ("stage3_real_validation_plan", "stage3_real_validation_plan.json"),
        ("stage3_real_validation_plan_reviewer_artifact", "stage3_real_validation_plan.md"),
    )
    rows: list[str] = []
    for key, label in order:
        payload = dict(artifact_refs.get(key) or {})
        summary_text = str(payload.get("summary_text") or "").strip()
        path_text = str(payload.get("path") or "").strip()
        line = f"`{label}`"
        if summary_text:
            line = f"{line}：{summary_text}"
        if path_text:
            line = f"{line}（{path_text}）"
        rows.append(line)
    return rows


def _build_matrix_line(row: dict[str, Any]) -> str:
    evidence_text = "；".join(str(item).strip() for item in list(row.get("required_evidence_categories") or []) if str(item).strip())
    coverage_text = "；".join(str(item).strip() for item in list(row.get("current_evidence_coverage") or []) if str(item).strip())
    return (
        f"{row.get('standard_id_or_family') or row.get('standard_family')}"
        f"：{row.get('topic_or_control_object') or '--'}；"
        f"适用性：{row.get('applicability') or '--'}；"
        f"证据类别：{evidence_text or '--'}；"
        f"当前覆盖：{coverage_text or '--'}；"
        f"缺口：{row.get('gap_note') or '--'}"
    )


def _render_stage3_standards_alignment_matrix_markdown(display: dict[str, Any]) -> str:
    lines = [
        f"# {display.get('title_text') or 'Stage 3 Standards Alignment Matrix / 第三阶段标准符合性映射与证据覆盖矩阵'}",
        "",
        "> 离线 reviewer artifact：本工件只做 readiness mapping only / family-topic-level standards alignment / evidence coverage preparation；"
        "不是 accreditation claim，不是 compliance certification，不是 real acceptance，也不能替代真实计量验证。",
        "",
        "## 当前阶段",
        "",
        f"- {display.get('current_stage_text') or '--'}",
        f"- {display.get('next_stage_text') or '--'}",
        f"- {display.get('status_line') or '--'}",
        f"- {display.get('engineering_isolation_text') or '--'}",
        f"- {display.get('real_acceptance_text') or '--'}",
        f"- {display.get('stage_bridge_text') or '--'}",
        "",
        "## 审阅定位",
        "",
        f"- 角色：{display.get('artifact_role_text') or '--'}",
        f"- reviewer_note：{display.get('reviewer_note_text') or '--'}",
        "",
        "## 标准家族与主题映射",
        "",
    ]
    lines.extend(f"- {line}" for line in list(display.get("matrix_lines") or []))
    lines.extend(["", "## Required Evidence Categories", ""])
    lines.extend(f"- {line}" for line in list(display.get("required_evidence_lines") or []))
    lines.extend(["", "## 非声明边界", ""])
    lines.extend(f"- {line}" for line in list(display.get("boundary_lines") or []))
    lines.extend(["", "## 关联工件", ""])
    lines.extend(f"- {line}" for line in list(display.get("artifact_lines") or []))
    return "\n".join(line for line in lines if str(line).strip() or line == "") + "\n"


def _collect_category_sets(validation_items: list[dict[str, Any]]) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {
        "reference_instrument_enforcement": [],
        "traceability_review": [],
        "uncertainty_result": [],
        "device_acceptance": [],
        "real_world_repeatability": [],
        "pass_fail_contract": [],
        "anomaly_retest": [],
    }
    for item in validation_items:
        category = str(item.get("category") or "").strip()
        label = _VALIDATION_CATEGORY_LABELS.get(category) or str(item.get("title_text") or category).strip()
        if not category or not label:
            continue
        grouped.setdefault(category, [])
        if label not in grouped[category]:
            grouped[category].append(label)
    grouped["quality_review"] = _dedupe(
        grouped["reference_instrument_enforcement"]
        + grouped["traceability_review"]
        + grouped["uncertainty_result"]
    )
    grouped["decision_review"] = _dedupe(
        grouped["pass_fail_contract"] + grouped["anomaly_retest"] + grouped["device_acceptance"]
    )
    return grouped


def _count_by_key(rows: list[dict[str, Any]], *, key_name: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key_name) or "").strip()
        if not value:
            continue
        counts[value] = int(counts.get(value, 0) or 0) + 1
    return counts


def _dedupe(values: Any) -> list[str]:
    rows: list[str] = []
    for value in list(values or []):
        text = str(value or "").strip()
        if text and text not in rows:
            rows.append(text)
    return rows


def reviewer_note(message: str) -> str:
    return (
        f"{message} readiness mapping only；engineering-isolation dependency；"
        "simulation / offline / headless only；not real acceptance；cannot replace real metrology validation。"
    )


def gap_note(message: str) -> str:
    return (
        f"{message} readiness mapping only；not accreditation claim；not compliance certification；"
        "cannot replace real metrology validation。"
    )


def coverage_line(artifact_key: str, artifact_refs: dict[str, dict[str, Any]]) -> str:
    payload = dict(artifact_refs.get(artifact_key) or {})
    summary_text = str(payload.get("summary_text") or "").strip()
    if summary_text:
        return f"{artifact_key}：{summary_text}"
    path_text = str(payload.get("path") or "").strip()
    if path_text:
        return f"{artifact_key}：{path_text}"
    return artifact_key


def _first_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return "--"
