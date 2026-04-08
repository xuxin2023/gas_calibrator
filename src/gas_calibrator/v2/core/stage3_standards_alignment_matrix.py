from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .engineering_isolation_admission_checklist import (
    ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME,
    ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME,
    build_engineering_isolation_admission_checklist,
)
from .metrology_calibration_contract import METROLOGY_CALIBRATION_CONTRACT_FILENAME
from .phase_transition_bridge import PHASE_TRANSITION_BRIDGE_FILENAME
from .phase_transition_bridge_reviewer_artifact import PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME
from .stage_admission_review_pack import (
    STAGE_ADMISSION_REVIEW_PACK_FILENAME,
    STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME,
    build_stage_admission_review_pack,
)
from .stage3_real_validation_plan import (
    STAGE3_REAL_VALIDATION_PLAN_FILENAME,
    STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME,
    _artifact_ref,
    _normalize_artifact_paths,
    build_stage3_real_validation_plan,
)
from .stage3_real_validation_plan_artifact_entry import _VALIDATION_CATEGORY_LABELS
from .step2_readiness import STEP2_READINESS_SUMMARY_FILENAME


STAGE3_STANDARDS_ALIGNMENT_MATRIX_FILENAME = "stage3_standards_alignment_matrix.json"
STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_FILENAME = "stage3_standards_alignment_matrix.md"

_NON_CLAIM_LINES = [
    "readiness mapping only",
    "not accreditation claim",
    "not compliance certification",
    "not real acceptance",
    "cannot replace real metrology validation",
    "simulation / offline / headless only",
]

_STANDARD_ROW_TEMPLATES: tuple[dict[str, Any], ...] = (
    {
        "standard_family_id": "cma_observation_qc",
        "standard_family": "中国气象局 / 气象行业观测与质量控制相关要求",
        "standard_id_or_family": "中国气象局 / 气象行业观测与质量控制相关要求",
        "topic_or_control_object": "观测仪器校准治理、质量控制、数据可追溯与审阅闭环准备",
        "linked_artifacts": [
            "phase_transition_bridge",
            "stage_admission_review_pack",
            "engineering_isolation_admission_checklist",
            "stage3_real_validation_plan",
        ],
        "required_evidence_category_ids": [
            "traceability_review",
            "real_world_repeatability",
            "anomaly_retest",
        ],
        "gap_note": "仓库当前只提供 family/topic-level readiness mapping，不宣称已满足任何具体条款、限值或认可结论。",
    },
    {
        "standard_family_id": "cnas_cl01",
        "standard_family": "CNAS-CL01",
        "standard_id_or_family": "CNAS-CL01",
        "topic_or_control_object": "实验室能力通用要求的 reviewer evidence plan、记录链与审阅入口准备",
        "linked_artifacts": [
            "stage_admission_review_pack",
            "engineering_isolation_admission_checklist",
            "stage3_real_validation_plan",
        ],
        "required_evidence_category_ids": [
            "traceability_review",
            "uncertainty_result",
            "pass_fail_contract",
        ],
        "gap_note": "repo 中没有 CNAS-CL01 精确条文文本时，只保留 family-level / control-object-level mapping。",
    },
    {
        "standard_family_id": "cnas_cl01_g002",
        "standard_family": "CNAS-CL01-G002（计量溯源性）",
        "standard_id_or_family": "CNAS-CL01-G002",
        "topic_or_control_object": "reference instrument enforcement、证书有效性与计量溯源链审阅准备",
        "linked_artifacts": [
            "metrology_calibration_contract",
            "engineering_isolation_admission_checklist",
            "stage3_real_validation_plan",
        ],
        "required_evidence_category_ids": [
            "reference_instrument_enforcement",
            "traceability_review",
        ],
        "gap_note": "当前只映射 traceability control-object，不伪造条款编号，也不输出任何 accreditation 断言。",
    },
    {
        "standard_family_id": "cnas_cl01_g003",
        "standard_family": "CNAS-CL01-G003（测量不确定度）",
        "standard_id_or_family": "CNAS-CL01-G003",
        "topic_or_control_object": "uncertainty budget template、real-run result 与 reviewer evidence 覆盖准备",
        "linked_artifacts": [
            "metrology_calibration_contract",
            "stage_admission_review_pack",
            "stage3_real_validation_plan",
        ],
        "required_evidence_category_ids": [
            "uncertainty_result",
            "pass_fail_contract",
        ],
        "gap_note": "当前 repo 只保留不确定度模板与 reviewer contract，不能把 simulation/replay 结果写成最终不确定度结论。",
    },
    {
        "standard_family_id": "iso_iec_17025",
        "standard_family": "ISO/IEC 17025",
        "standard_id_or_family": "ISO/IEC 17025",
        "topic_or_control_object": "method validity、traceability、uncertainty、records 与 reviewer evidence readiness",
        "linked_artifacts": [
            "metrology_calibration_contract",
            "stage_admission_review_pack",
            "engineering_isolation_admission_checklist",
            "stage3_real_validation_plan",
        ],
        "required_evidence_category_ids": [
            "traceability_review",
            "uncertainty_result",
            "real_world_repeatability",
            "pass_fail_contract",
        ],
        "gap_note": "若仓库未提供精确条款文本，则只做 family/topic-level mapping，不构成合规或认可主张。",
    },
    {
        "standard_family_id": "iso_6142_family",
        "standard_family": "ISO 6142 family",
        "standard_id_or_family": "ISO 6142 family",
        "topic_or_control_object": "标准气体制备、批次标识、溯源链与 reviewer evidence coverage 准备",
        "linked_artifacts": [
            "metrology_calibration_contract",
            "stage_admission_review_pack",
            "stage3_real_validation_plan",
        ],
        "required_evidence_category_ids": [
            "reference_instrument_enforcement",
            "traceability_review",
        ],
        "gap_note": "这里只映射 gas mixture family-level reviewer controls，不伪造制备方法条款或批次限值。",
    },
    {
        "standard_family_id": "iso_6143",
        "standard_family": "ISO 6143",
        "standard_id_or_family": "ISO 6143",
        "topic_or_control_object": "comparison / calibration function / fit residual reviewer evidence 准备",
        "linked_artifacts": [
            "metrology_calibration_contract",
            "stage3_real_validation_plan",
        ],
        "required_evidence_category_ids": [
            "uncertainty_result",
            "pass_fail_contract",
        ],
        "gap_note": "repo 当前只保留 comparison / fit reviewer readiness mapping，不生成任何已符合算法要求的结论。",
    },
    {
        "standard_family_id": "iso_6145_family",
        "standard_family": "ISO 6145 family",
        "standard_id_or_family": "ISO 6145 family",
        "topic_or_control_object": "dynamic generation、flow / humidification / reference gas generation evidence 准备",
        "linked_artifacts": [
            "metrology_calibration_contract",
            "engineering_isolation_admission_checklist",
            "stage3_real_validation_plan",
        ],
        "required_evidence_category_ids": [
            "reference_instrument_enforcement",
            "real_world_repeatability",
        ],
        "gap_note": "当前 repo 只做 family/topic-level readiness mapping，不输出动态配气方法合规声明。",
    },
    {
        "standard_family_id": "wmo_gaw_qa",
        "standard_family": "WMO / GAW 质量保证、可追溯、数据质量相关要求",
        "standard_id_or_family": "WMO / GAW QA",
        "topic_or_control_object": "QA/QC、traceability、data quality、anomaly review 与 retest governance 准备",
        "linked_artifacts": [
            "phase_transition_bridge",
            "stage_admission_review_pack",
            "engineering_isolation_admission_checklist",
            "stage3_real_validation_plan",
        ],
        "required_evidence_category_ids": [
            "traceability_review",
            "real_world_repeatability",
            "anomaly_retest",
        ],
        "gap_note": "这里只保留 WMO / GAW QA family/topic mapping，不宣称任何全球网络认可或观测结果放行。",
    },
)


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
    review_pack_bundle = dict(stage_admission_review_pack or {})
    if not review_pack_bundle:
        review_pack_bundle = build_stage_admission_review_pack(
            run_id=run_id,
            step2_readiness_summary=readiness,
            metrology_calibration_contract=metrology,
            phase_transition_bridge=bridge,
            artifact_paths=artifact_paths,
        )
    checklist_bundle = dict(engineering_isolation_admission_checklist or {})
    if not checklist_bundle:
        checklist_bundle = build_engineering_isolation_admission_checklist(
            run_id=run_id,
            step2_readiness_summary=readiness,
            metrology_calibration_contract=metrology,
            phase_transition_bridge=bridge,
            stage_admission_review_pack=review_pack_bundle,
            artifact_paths=artifact_paths,
        )
    plan_bundle = dict(stage3_real_validation_plan or {})
    if not plan_bundle:
        plan_bundle = build_stage3_real_validation_plan(
            run_id=run_id,
            step2_readiness_summary=readiness,
            metrology_calibration_contract=metrology,
            phase_transition_bridge=bridge,
            stage_admission_review_pack=review_pack_bundle,
            engineering_isolation_admission_checklist=checklist_bundle,
            artifact_paths=artifact_paths,
        )

    pack_raw = dict(review_pack_bundle.get("raw") or {})
    pack_display = dict(review_pack_bundle.get("display") or {})
    checklist_raw = dict(checklist_bundle.get("raw") or {})
    checklist_display = dict(checklist_bundle.get("display") or {})
    plan_raw = dict(plan_bundle.get("raw") or {})
    plan_display = dict(plan_bundle.get("display") or {})
    bridge_display = dict(bridge.get("reviewer_display") or {})

    artifact_path_map = _normalize_artifact_paths(
        artifact_paths=artifact_paths,
        checklist_artifact_paths=checklist_raw.get("artifact_paths"),
        pack_artifact_paths=pack_raw.get("artifact_paths"),
    )
    artifact_path_map["stage3_real_validation_plan"] = str(
        dict(plan_raw.get("artifact_paths") or {}).get("stage3_real_validation_plan")
        or dict(artifact_path_map).get("stage3_real_validation_plan")
        or STAGE3_REAL_VALIDATION_PLAN_FILENAME
    )
    artifact_path_map["stage3_real_validation_plan_reviewer_artifact"] = str(
        dict(plan_raw.get("artifact_paths") or {}).get("stage3_real_validation_plan_reviewer_artifact")
        or dict(artifact_path_map).get("stage3_real_validation_plan_reviewer_artifact")
        or STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME
    )

    artifact_refs = {
        "step2_readiness_summary": _artifact_ref(
            readiness,
            artifact_path_map["step2_readiness_summary"],
            str(readiness.get("reviewer_display", {}).get("summary_text") or "").strip(),
        ),
        "metrology_calibration_contract": _artifact_ref(
            metrology,
            artifact_path_map["metrology_calibration_contract"],
            str(metrology.get("reviewer_display", {}).get("summary_text") or "").strip(),
        ),
        "phase_transition_bridge": _artifact_ref(
            bridge,
            artifact_path_map["phase_transition_bridge"],
            str(bridge_display.get("summary_text") or "").strip(),
        ),
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

    validation_items = [
        dict(item)
        for item in list(plan_raw.get("validation_items") or [])
        if isinstance(item, dict)
    ]
    category_id_order = _dedupe(
        [str(item.get("category") or "").strip() for item in validation_items if str(item.get("category") or "").strip()]
    )
    category_filters = [
        {
            "id": category_id,
            "label": str(_VALIDATION_CATEGORY_LABELS.get(category_id) or category_id).strip(),
        }
        for category_id in category_id_order
    ]
    matrix_rows = _build_matrix_rows(
        category_filters=category_filters,
        artifact_refs=artifact_refs,
    )
    standard_family_filters = [
        {"id": str(row.get("standard_family_id") or ""), "label": str(row.get("standard_family") or "")}
        for row in matrix_rows
        if str(row.get("standard_family_id") or "").strip() and str(row.get("standard_family") or "").strip()
    ]
    summary_seed = str(
        plan_display.get("summary_text")
        or checklist_display.get("summary_text")
        or pack_display.get("summary_text")
        or bridge_display.get("summary_text")
        or ""
    ).strip()
    stage_bridge_text = (
        "定位：Step 2 tail / Stage 3 bridge，补齐 standards-alignment / evidence-coverage reviewer capability，"
        "不提前执行第三阶段真实验证。"
    )
    reviewer_note_text = (
        f"{stage_bridge_text} {str(checklist_display.get('engineering_isolation_text') or plan_display.get('engineering_isolation_text') or '').strip()} "
        "readiness mapping only；not accreditation claim；not compliance certification；"
        "not real acceptance；cannot replace real metrology validation。"
    ).strip()
    status_line = (
        "阶段状态：Step 2 tail / Stage 3 bridge 的标准符合性映射与证据覆盖矩阵已进入 reviewer capability；"
        "engineering-isolation dependency 已保留；不是 real acceptance。"
    )
    digest = _build_digest(
        matrix_rows=matrix_rows,
        standard_family_filters=standard_family_filters,
        category_filters=category_filters,
        artifact_refs=artifact_refs,
    )
    display = {
        "title_text": "Stage 3 Standards Alignment Matrix / 第三阶段标准符合性映射矩阵",
        "summary_text": (
            f"{summary_seed} 标准符合性映射 + 证据覆盖矩阵：仅做 readiness mapping only，"
            "帮助 reviewer 在 Step 2 tail / Stage 3 bridge 阶段识别标准家族、证据类别与缺口。"
        ).strip(),
        "role_text": "formal_analysis / reviewer standards-alignment capability",
        "reviewer_note_text": reviewer_note_text,
        "status_line": status_line,
        "current_stage_text": str(plan_display.get("current_stage_text") or checklist_display.get("current_stage_text") or "").strip(),
        "next_stage_text": str(plan_display.get("next_stage_text") or checklist_display.get("next_stage_text") or "").strip(),
        "stage_bridge_text": stage_bridge_text,
        "engineering_isolation_text": str(
            checklist_display.get("engineering_isolation_text")
            or plan_display.get("engineering_isolation_text")
            or pack_display.get("engineering_isolation_text")
            or ""
        ).strip(),
        "matrix_lines": _build_matrix_lines(matrix_rows),
        "artifact_lines": _build_artifact_lines(artifact_refs),
        "non_claim_lines": list(_NON_CLAIM_LINES),
        "standard_family_lines": [
            f"{item['label']} ({item['id']})" for item in standard_family_filters if str(item.get("label") or "").strip()
        ],
        "required_evidence_categories_text": " / ".join(
            str(item.get("label") or "").strip() for item in category_filters if str(item.get("label") or "").strip()
        )
        or "--",
        "digest_text": _build_digest_text(digest),
    }
    markdown = _render_stage3_standards_alignment_matrix_markdown(display)

    raw = {
        "schema_version": "1.0",
        "artifact_type": "stage3_standards_alignment_matrix",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": str(run_id or plan_raw.get("run_id") or checklist_raw.get("run_id") or ""),
        "phase": str(plan_raw.get("phase") or checklist_raw.get("phase") or "step2_tail_stage3_bridge"),
        "mode": str(plan_raw.get("mode") or checklist_raw.get("mode") or "simulation_only"),
        "overall_status": str(plan_raw.get("overall_status") or checklist_raw.get("overall_status") or "step2_tail_in_progress"),
        "recommended_next_stage": str(
            plan_raw.get("recommended_next_stage")
            or checklist_raw.get("recommended_next_stage")
            or "engineering_isolation"
        ),
        "not_real_acceptance_evidence": True,
        "reviewer_role": "reviewer",
        "reviewer_note": reviewer_note_text,
        "stage_bridge_text": stage_bridge_text,
        "engineering_isolation_dependency": str(
            display.get("engineering_isolation_text") or ""
        ).strip(),
        "standards_matrix_rows": matrix_rows,
        "standard_families": [dict(item) for item in standard_family_filters],
        "required_evidence_categories": [dict(item) for item in category_filters],
        "artifact_refs": artifact_refs,
        "artifact_paths": {
            **artifact_path_map,
            "stage3_standards_alignment_matrix": STAGE3_STANDARDS_ALIGNMENT_MATRIX_FILENAME,
            "stage3_standards_alignment_matrix_reviewer_artifact": STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_FILENAME,
        },
        "digest": digest,
        "non_claims": list(_NON_CLAIM_LINES),
        "notes": [
            "stage3_standards_alignment_matrix",
            "step2_tail_stage3_bridge",
            "readiness_mapping_only",
            "not_accreditation_claim",
            "not_compliance_certification",
            "not_real_acceptance_evidence",
            "simulation_offline_headless_only",
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


def _build_matrix_rows(
    *,
    category_filters: list[dict[str, str]],
    artifact_refs: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    category_labels = {
        str(item.get("id") or ""): str(item.get("label") or "")
        for item in category_filters
        if str(item.get("id") or "").strip()
    }
    rows: list[dict[str, Any]] = []
    for template in _STANDARD_ROW_TEMPLATES:
        linked_artifacts = [str(item) for item in list(template.get("linked_artifacts") or []) if str(item).strip()]
        required_category_ids = [
            str(item) for item in list(template.get("required_evidence_category_ids") or []) if str(item).strip()
        ]
        required_category_filters = [
            {"id": category_id, "label": str(category_labels.get(category_id) or category_id).strip()}
            for category_id in required_category_ids
        ]
        linked_labels = [
            _artifact_filename_for_key(artifact_key, artifact_refs)
            for artifact_key in linked_artifacts
            if _artifact_filename_for_key(artifact_key, artifact_refs)
        ]
        coverage_text = (
            "repo 当前已有离线 reviewer 工件覆盖："
            + " / ".join(linked_labels)
            + "；真实参考仪器、真实 run、真实 bench 证据仍待第三阶段。"
        )
        digest_text = (
            f"{template['standard_id_or_family']} | {template['topic_or_control_object']} | "
            f"{' / '.join(item['label'] for item in required_category_filters) or '--'}"
        )
        rows.append(
            {
                "standard_family_id": str(template.get("standard_family_id") or ""),
                "standard_family": str(template.get("standard_family") or ""),
                "standard_id_or_family": str(template.get("standard_id_or_family") or ""),
                "mapping_level": "family_topic_level_only",
                "topic_or_control_object": str(template.get("topic_or_control_object") or ""),
                "applicability": (
                    "family-level / topic-level readiness mapping only；"
                    "offline governance and reviewer evidence coverage preparation。"
                ),
                "reviewer_role": "reviewer",
                "reviewer_note": (
                    "Step 2 tail / Stage 3 bridge reviewer mapping；"
                    "只展示当前仓库已有治理工件覆盖，不构成条款级合规结论。"
                ),
                "linked_existing_artifacts": linked_artifacts,
                "linked_existing_artifact_labels": linked_labels,
                "required_evidence_categories": required_category_filters,
                "current_evidence_coverage": coverage_text,
                "readiness_status": "mapping_ready_real_evidence_pending",
                "gap_note": str(template.get("gap_note") or "").strip(),
                "non_claim": "; ".join(_NON_CLAIM_LINES),
                "digest": digest_text,
            }
        )
    return rows


def _build_artifact_lines(artifact_refs: dict[str, dict[str, Any]]) -> list[str]:
    labels = {
        "step2_readiness_summary": "step2_readiness_summary.json",
        "metrology_calibration_contract": "metrology_calibration_contract.json",
        "phase_transition_bridge": "phase_transition_bridge.json",
        "stage_admission_review_pack": "stage_admission_review_pack.json",
        "stage_admission_review_pack_reviewer_artifact": "stage_admission_review_pack.md",
        "engineering_isolation_admission_checklist": "engineering_isolation_admission_checklist.json",
        "engineering_isolation_admission_checklist_reviewer_artifact": "engineering_isolation_admission_checklist.md",
        "stage3_real_validation_plan": "stage3_real_validation_plan.json",
        "stage3_real_validation_plan_reviewer_artifact": "stage3_real_validation_plan.md",
        "stage3_standards_alignment_matrix": STAGE3_STANDARDS_ALIGNMENT_MATRIX_FILENAME,
        "stage3_standards_alignment_matrix_reviewer_artifact": STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_FILENAME,
    }
    rows: list[str] = []
    for key, label in labels.items():
        payload = dict(artifact_refs.get(key) or {})
        path_text = str(payload.get("path") or "").strip()
        summary_text = str(payload.get("summary_text") or "").strip()
        if key.startswith("stage3_standards_alignment_matrix"):
            rows.append(f"`{label}`：{summary_text or 'reviewer capability output'}")
            continue
        line = f"`{label}`：{summary_text}".strip("：")
        if path_text:
            line = f"{line}（{path_text}）"
        rows.append(line)
    return rows


def _build_matrix_lines(matrix_rows: list[dict[str, Any]]) -> list[str]:
    rows: list[str] = []
    for item in matrix_rows:
        required_categories = " / ".join(
            str(category.get("label") or "").strip()
            for category in list(item.get("required_evidence_categories") or [])
            if str(category.get("label") or "").strip()
        )
        linked_artifacts = " / ".join(
            str(label).strip()
            for label in list(item.get("linked_existing_artifact_labels") or [])
            if str(label).strip()
        )
        rows.append(
            (
                f"{str(item.get('standard_id_or_family') or '').strip()}："
                f"{str(item.get('topic_or_control_object') or '').strip()}；"
                f"适用性：{str(item.get('applicability') or '').strip()}；"
                f"证据类别：{required_categories or '--'}；"
                f"当前覆盖：{str(item.get('current_evidence_coverage') or '').strip()}；"
                f"关联工件：{linked_artifacts or '--'}；"
                f"差距：{str(item.get('gap_note') or '').strip()}"
            ).strip("；")
        )
    return rows


def _build_digest(
    *,
    matrix_rows: list[dict[str, Any]],
    standard_family_filters: list[dict[str, str]],
    category_filters: list[dict[str, str]],
    artifact_refs: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    linked_artifact_keys = _dedupe(
        [
            str(key)
            for row in matrix_rows
            for key in list(row.get("linked_existing_artifacts") or [])
            if str(key).strip()
        ]
    )
    return {
        "standard_family_count": len(standard_family_filters),
        "matrix_row_count": len(matrix_rows),
        "mapping_level": "family_topic_level_only",
        "required_evidence_category_count": len(category_filters),
        "required_evidence_categories": [dict(item) for item in category_filters],
        "linked_artifact_count": len(linked_artifact_keys),
        "linked_artifacts": linked_artifact_keys,
        "artifact_paths": {
            key: str(dict(artifact_refs.get(key) or {}).get("path") or "").strip()
            for key in linked_artifact_keys
            if str(dict(artifact_refs.get(key) or {}).get("path") or "").strip()
        },
        "non_claims": list(_NON_CLAIM_LINES),
        "coverage_status_counts": {
            "mapping_ready_real_evidence_pending": len(matrix_rows),
        },
    }


def _build_digest_text(digest: dict[str, Any]) -> str:
    return " | ".join(
        [
            f"标准家族 {int(digest.get('standard_family_count', 0) or 0)}",
            f"映射行 {int(digest.get('matrix_row_count', 0) or 0)}",
            f"证据类别 {int(digest.get('required_evidence_category_count', 0) or 0)}",
            f"关联工件 {int(digest.get('linked_artifact_count', 0) or 0)}",
            str(digest.get("mapping_level") or "family_topic_level_only"),
        ]
    )


def _render_stage3_standards_alignment_matrix_markdown(display: dict[str, Any]) -> str:
    lines = [
        f"# {display.get('title_text') or 'Stage 3 Standards Alignment Matrix / 第三阶段标准符合性映射矩阵'}",
        "",
        f"> {display.get('reviewer_note_text') or '--'}",
        "",
        "## 当前阶段",
        "",
        f"- {display.get('current_stage_text') or '--'}",
        f"- {display.get('next_stage_text') or '--'}",
        f"- {display.get('status_line') or '--'}",
        f"- {display.get('stage_bridge_text') or '--'}",
        f"- {display.get('engineering_isolation_text') or '--'}",
        "",
        "## 标准家族与证据覆盖",
        "",
    ]
    lines.extend(f"- {line}" for line in list(display.get("matrix_lines") or []))
    lines.extend(["", "## 关联工件", ""])
    lines.extend(f"- {line}" for line in list(display.get("artifact_lines") or []))
    lines.extend(["", "## 非声明边界", ""])
    lines.extend(f"- {line}" for line in list(display.get("non_claim_lines") or []))
    lines.extend(["", "## Digest", ""])
    lines.append(f"- {display.get('digest_text') or '--'}")
    lines.append("")
    return "\n".join(line for line in lines if str(line).strip() or line == "") + "\n"


def _artifact_filename_for_key(artifact_key: str, artifact_refs: dict[str, dict[str, Any]]) -> str:
    labels = {
        "step2_readiness_summary": STEP2_READINESS_SUMMARY_FILENAME,
        "metrology_calibration_contract": METROLOGY_CALIBRATION_CONTRACT_FILENAME,
        "phase_transition_bridge": PHASE_TRANSITION_BRIDGE_FILENAME,
        "phase_transition_bridge_reviewer_artifact": PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME,
        "stage_admission_review_pack": STAGE_ADMISSION_REVIEW_PACK_FILENAME,
        "stage_admission_review_pack_reviewer_artifact": STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME,
        "engineering_isolation_admission_checklist": ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME,
        "engineering_isolation_admission_checklist_reviewer_artifact": (
            ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME
        ),
        "stage3_real_validation_plan": STAGE3_REAL_VALIDATION_PLAN_FILENAME,
        "stage3_real_validation_plan_reviewer_artifact": STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME,
    }
    label = str(labels.get(artifact_key) or "").strip()
    if label:
        return label
    return str(dict(artifact_refs.get(artifact_key) or {}).get("path") or "").strip()


def _dedupe(values: list[Any]) -> list[str]:
    rows: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in rows:
            rows.append(text)
    return rows
