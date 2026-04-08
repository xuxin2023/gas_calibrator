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
from .step2_readiness import STEP2_READINESS_SUMMARY_FILENAME


STAGE3_REAL_VALIDATION_PLAN_FILENAME = "stage3_real_validation_plan.json"
STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME = "stage3_real_validation_plan.md"


def build_stage3_real_validation_plan(
    *,
    run_id: str,
    step2_readiness_summary: dict[str, Any] | None,
    metrology_calibration_contract: dict[str, Any] | None,
    phase_transition_bridge: dict[str, Any] | None,
    stage_admission_review_pack: dict[str, Any] | None = None,
    engineering_isolation_admission_checklist: dict[str, Any] | None = None,
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

    pack_raw = dict(review_pack.get("raw") or {})
    pack_display = dict(review_pack.get("display") or {})
    checklist_raw = dict(checklist.get("raw") or {})
    checklist_display = dict(checklist.get("display") or {})
    bridge_display = dict(bridge.get("reviewer_display") or {})

    artifact_path_map = _normalize_artifact_paths(
        artifact_paths=artifact_paths,
        checklist_artifact_paths=checklist_raw.get("artifact_paths"),
        pack_artifact_paths=pack_raw.get("artifact_paths"),
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
    }

    ready_for_engineering_isolation = bool(
        checklist_raw.get(
            "ready_for_engineering_isolation",
            pack_raw.get("ready_for_engineering_isolation", bridge.get("ready_for_engineering_isolation", False)),
        )
    )
    real_acceptance_ready = bool(
        checklist_raw.get(
            "real_acceptance_ready",
            pack_raw.get("real_acceptance_ready", bridge.get("real_acceptance_ready", False)),
        )
    )
    overall_status = str(
        checklist_raw.get("overall_status")
        or pack_raw.get("overall_status")
        or bridge.get("overall_status")
        or "step2_tail_in_progress"
    )
    recommended_next_stage = str(
        checklist_raw.get("recommended_next_stage")
        or pack_raw.get("recommended_next_stage")
        or bridge.get("recommended_next_stage")
        or "engineering_isolation"
    )
    blocking_items = _dedupe(
        [
            *list(checklist_raw.get("blocking_items") or []),
            *list(bridge.get("blocking_items") or []),
        ]
    )
    warning_items = _dedupe(
        [
            *list(checklist_raw.get("warning_items") or []),
            *list(pack_raw.get("warning_items") or []),
            *list(bridge.get("warning_items") or []),
            "stage3_real_validation_plan_only",
        ]
    )
    defer_to_stage3_real_validation = _dedupe(
        [
            *list(checklist_raw.get("defer_to_stage3_real_validation") or []),
            *list(pack_raw.get("defer_to_stage3_real_validation") or []),
            *list(bridge.get("defer_to_stage3_real_validation") or []),
        ]
    )

    validation_items = _build_validation_items()
    validation_status_counts = _count_statuses(validation_items)
    required_real_world_evidence = _dedupe(
        [
            *list(checklist_raw.get("missing_real_world_evidence") or []),
            *list(pack_raw.get("missing_real_world_evidence") or []),
            *list(bridge.get("missing_real_world_evidence") or []),
            *[
                evidence
                for item in validation_items
                for evidence in list(item.get("required_evidence") or [])
            ],
        ]
    )
    pass_fail_contract = {
        "decision_stage": "stage3_real_validation",
        "contract_only": True,
        "not_executable_offline": True,
        "pass_requires": [
            "all_required_real_world_evidence_collected",
            "reference_instruments_enforced_with_valid_traceability",
            "real_run_uncertainty_result_within_contract",
            "coefficient_writeback_readback_verified_on_real_device",
            "repeatability_and_drift_within_contract",
            "real_acceptance_review_and_approval_recorded",
        ],
        "fail_triggers": [
            "missing_required_real_world_evidence",
            "certificate_or_due_date_invalid",
            "reference_instrument_not_enforced",
            "final_uncertainty_out_of_contract",
            "coefficient_writeback_readback_mismatch",
            "repeatability_or_drift_out_of_contract",
            "unresolved_real_run_anomaly",
        ],
    }

    summary_seed = str(
        checklist_display.get("summary_text")
        or pack_display.get("summary_text")
        or bridge_display.get("summary_text")
        or ""
    ).strip()
    display = {
        "title_text": "Stage 3 Real Validation Plan / 第三阶段真实验证计划",
        "summary_text": (
            f"{summary_seed} 第三阶段真实验证计划：把真实参考表/参考仪器、证书与检定周期、"
            "真实 run 最终不确定度、真机系数写入 acceptance、多点复测/重复性/漂移、"
            "异常复核与 real acceptance pass/fail 收成正式执行矩阵。"
        ).strip(),
        "status_line": str(checklist_display.get("status_line") or pack_display.get("status_line") or "").strip(),
        "current_stage_text": str(
            checklist_display.get("current_stage_text")
            or pack_display.get("current_stage_text")
            or ""
        ).strip(),
        "next_stage_text": str(
            checklist_display.get("next_stage_text")
            or pack_display.get("next_stage_text")
            or ""
        ).strip(),
        "engineering_isolation_text": str(
            checklist_display.get("engineering_isolation_text")
            or pack_display.get("engineering_isolation_text")
            or ""
        ).strip(),
        "real_acceptance_text": str(
            checklist_display.get("real_acceptance_text")
            or pack_display.get("real_acceptance_text")
            or ""
        ).strip(),
        "execute_now_text": str(
            checklist_display.get("execute_now_text")
            or pack_display.get("execute_now_text")
            or ""
        ).strip(),
        "defer_to_stage3_text": str(
            checklist_display.get("defer_to_stage3_text")
            or pack_display.get("defer_to_stage3_text")
            or ""
        ).strip(),
        "blocking_text": str(
            checklist_display.get("blocking_text")
            or pack_display.get("blocking_text")
            or ""
        ).strip(),
        "warning_text": str(
            checklist_display.get("warning_text")
            or pack_display.get("warning_text")
            or ""
        ).strip(),
        "plan_boundary_text": "本工件只定义第三阶段真实验证计划，不代表验证已完成。",
        "validation_lines": _build_validation_lines(validation_items),
        "required_evidence_lines": [f"需要真实证据：{item}" for item in required_real_world_evidence],
        "pass_fail_lines": _build_pass_fail_lines(pass_fail_contract),
        "artifact_lines": _build_artifact_lines(artifact_refs),
    }
    markdown = _render_stage3_real_validation_plan_markdown(display)

    raw = {
        "schema_version": "1.0",
        "artifact_type": "stage3_real_validation_plan",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": str(run_id or checklist_raw.get("run_id") or pack_raw.get("run_id") or bridge.get("run_id") or ""),
        "phase": str(checklist_raw.get("phase") or pack_raw.get("phase") or bridge.get("phase") or "step2_tail_stage3_bridge"),
        "mode": str(checklist_raw.get("mode") or pack_raw.get("mode") or bridge.get("mode") or "simulation_only"),
        "overall_status": overall_status,
        "recommended_next_stage": recommended_next_stage,
        "ready_for_engineering_isolation": ready_for_engineering_isolation,
        "real_acceptance_ready": real_acceptance_ready,
        "not_real_acceptance_evidence": True,
        "validation_items": validation_items,
        "validation_status_counts": validation_status_counts,
        "required_real_world_evidence": required_real_world_evidence,
        "pass_fail_contract": pass_fail_contract,
        "blocking_items": blocking_items,
        "warning_items": warning_items,
        "artifact_refs": artifact_refs,
        "artifact_paths": artifact_path_map,
        "notes": [
            "stage3_real_validation_plan",
            "step2_tail_stage3_bridge",
            "simulation_offline_headless_only",
            "not_real_acceptance_evidence",
            "default_path_unchanged",
        ],
    }
    return {
        "available": True,
        "artifact_type": "stage3_real_validation_plan",
        "filename": STAGE3_REAL_VALIDATION_PLAN_FILENAME,
        "reviewer_filename": STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME,
        "raw": raw,
        "display": display,
        "markdown": markdown,
    }


def _normalize_artifact_paths(
    *,
    artifact_paths: dict[str, Any] | None,
    checklist_artifact_paths: dict[str, Any] | None = None,
    pack_artifact_paths: dict[str, Any] | None = None,
) -> dict[str, str]:
    payload = dict(pack_artifact_paths or {})
    payload.update(dict(checklist_artifact_paths or {}))
    payload.update(dict(artifact_paths or {}))
    return {
        "step2_readiness_summary": str(
            payload.get("step2_readiness_summary") or STEP2_READINESS_SUMMARY_FILENAME
        ),
        "metrology_calibration_contract": str(
            payload.get("metrology_calibration_contract") or METROLOGY_CALIBRATION_CONTRACT_FILENAME
        ),
        "phase_transition_bridge": str(
            payload.get("phase_transition_bridge") or PHASE_TRANSITION_BRIDGE_FILENAME
        ),
        "phase_transition_bridge_reviewer_artifact": str(
            payload.get("phase_transition_bridge_reviewer_artifact") or PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME
        ),
        "stage_admission_review_pack": str(
            payload.get("stage_admission_review_pack") or STAGE_ADMISSION_REVIEW_PACK_FILENAME
        ),
        "stage_admission_review_pack_reviewer_artifact": str(
            payload.get("stage_admission_review_pack_reviewer_artifact")
            or STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME
        ),
        "engineering_isolation_admission_checklist": str(
            payload.get("engineering_isolation_admission_checklist")
            or ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME
        ),
        "engineering_isolation_admission_checklist_reviewer_artifact": str(
            payload.get("engineering_isolation_admission_checklist_reviewer_artifact")
            or ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME
        ),
    }


def _artifact_ref(payload: dict[str, Any], path_text: str, summary_text: str) -> dict[str, Any]:
    return {
        "artifact_type": str(payload.get("artifact_type") or ""),
        "phase": str(payload.get("phase") or ""),
        "overall_status": str(payload.get("overall_status") or ""),
        "path": str(path_text or ""),
        "summary_text": summary_text,
    }


def _build_validation_items() -> list[dict[str, Any]]:
    return [
        _validation_item(
            item_id="dewpoint_reference_enforcement",
            title_text="真实 dewpoint reference 强制执行",
            category="reference_instrument_enforcement",
            status="blocked_until_stage3",
            required_evidence=[
                "dewpoint_reference_certificate_and_due_date",
                "dewpoint_reference_bound_run_record",
            ],
            acceptance_rule="必须使用真实 dewpoint 参考表/参考仪器并把 run、traceability、证书绑定到 acceptance evidence。",
            source_artifact="metrology_calibration_contract",
            stage_assignment="stage3_real_validation",
            details="当前只定义 contract；本轮不得伪执行真实 dewpoint reference。",
        ),
        _validation_item(
            item_id="pressure_reference_enforcement",
            title_text="真实 pressure reference 强制执行",
            category="reference_instrument_enforcement",
            status="blocked_until_stage3",
            required_evidence=[
                "pressure_reference_certificate_and_due_date",
                "pressure_reference_bound_run_record",
            ],
            acceptance_rule="必须使用真实 pressure 参考仪器并记录量程、证书、run 绑定关系。",
            source_artifact="metrology_calibration_contract",
            stage_assignment="stage3_real_validation",
            details="当前只定义 pressure reference contract；不得在本轮离线工件中伪造成真实执行。",
        ),
        _validation_item(
            item_id="temperature_reference_enforcement",
            title_text="真实 temperature reference 强制执行",
            category="reference_instrument_enforcement",
            status="blocked_until_stage3",
            required_evidence=[
                "temperature_reference_certificate_and_due_date",
                "temperature_reference_bound_run_record",
            ],
            acceptance_rule="必须使用真实 temperature 参考链并记录 run 内引用关系。",
            source_artifact="metrology_calibration_contract",
            stage_assignment="stage3_real_validation",
            details="当前只保留温度参考 contract/schema；真实执行必须留到第三阶段。",
        ),
        _validation_item(
            item_id="humidity_gas_reference_enforcement",
            title_text="真实 humidity / gas reference 强制执行",
            category="reference_instrument_enforcement",
            status="blocked_until_stage3",
            required_evidence=[
                "humidity_generator_traceability_record",
                "standard_gas_batch_certificate_and_batch_record",
            ],
            acceptance_rule="湿度/标准气源必须具备真实 traceability、批次证据与 run 绑定。",
            source_artifact="metrology_calibration_contract",
            stage_assignment="stage3_real_validation",
            details="当前只定义 humidity / gas source contract，不代表真实 reference 已准备完成。",
        ),
        _validation_item(
            item_id="traceability_certificate_validity_review",
            title_text="真实证书 / 检定周期 / traceability 有效性核验",
            category="traceability_review",
            status="requires_real_evidence",
            required_evidence=[
                "reference_certificate_cycle_validity_review",
                "traceability_chain_approval_record",
            ],
            acceptance_rule="所有 reference 证书、检定周期与 traceability 必须在真实 run 时有效且可复核。",
            source_artifact="phase_transition_bridge",
            stage_assignment="stage3_real_validation",
            details="这是第三阶段 hard blocking 条件，离线工件只能提前定义字段与审阅要求。",
        ),
        _validation_item(
            item_id="real_run_uncertainty_result",
            title_text="基于真实 run 的最终不确定度结果",
            category="uncertainty_result",
            status="requires_real_evidence",
            required_evidence=[
                "real_run_uncertainty_budget_result",
                "real_run_raw_dataset_and_fit_residuals",
            ],
            acceptance_rule="最终不确定度必须基于真实 run、真实 reference 数据与拟合残差计算，不允许用 simulation/replay 替代。",
            source_artifact="metrology_calibration_contract",
            stage_assignment="stage3_real_validation",
            details="当前只存在 uncertainty budget template；最终结果必须在第三阶段真实执行后生成。",
        ),
        _validation_item(
            item_id="coefficient_writeback_readback_acceptance",
            title_text="真机系数写入、回读和 acceptance",
            category="device_acceptance",
            status="not_executable_offline",
            required_evidence=[
                "coefficient_writeback_record",
                "coefficient_readback_verification_record",
                "acceptance_device_snapshot",
            ],
            acceptance_rule="必须在真实设备上完成系数写入、回读和 acceptance，且 write/read 一致。",
            source_artifact="metrology_calibration_contract",
            stage_assignment="stage3_real_validation",
            details="当前 V2 仍保持 simulation-only，不允许运行任何真机 writeback/acceptance。",
        ),
        _validation_item(
            item_id="multi_point_repeatability_and_drift",
            title_text="真实工况下的多点复测 / 重复性 / 漂移验证",
            category="real_world_repeatability",
            status="requires_real_evidence",
            required_evidence=[
                "multi_point_repeatability_report",
                "real_world_drift_recheck_report",
            ],
            acceptance_rule="必须在真实工况下完成多点复测、重复性和漂移验证，并保留复测轨迹。",
            source_artifact="engineering_isolation_admission_checklist",
            stage_assignment="stage3_real_validation",
            details="当前 checklist 只能定义必须补齐的真实证据，不能把离线结果解释成真实重复性/漂移结论。",
        ),
        _validation_item(
            item_id="real_acceptance_pass_fail_contract",
            title_text="real acceptance pass/fail 判定条件",
            category="pass_fail_contract",
            status="planned",
            required_evidence=[
                "real_acceptance_decision_record",
                "reviewer_approver_signoff_record",
            ],
            acceptance_rule="只有在全部 required real-world evidence 齐全、traceability 有效、结果满足 contract 后才能做 real acceptance pass/fail 判定。",
            source_artifact="stage_admission_review_pack",
            stage_assignment="stage3_real_validation",
            details="本工件只定义 pass/fail contract，不代表已经通过或失败。",
        ),
        _validation_item(
            item_id="real_anomaly_review_and_retest_flow",
            title_text="真实异常处置与复核流程",
            category="anomaly_retest",
            status="planned",
            required_evidence=[
                "real_run_anomaly_log",
                "root_cause_review_record",
                "retest_and_disposition_record",
            ],
            acceptance_rule="真实异常必须有处置、复核、复测与结论记录，未闭环异常不能进入 real acceptance pass。",
            source_artifact="engineering_isolation_admission_checklist",
            stage_assignment="stage3_real_validation",
            details="当前只能把异常复核流程制度化，真实异常处置必须等第三阶段真实执行。",
        ),
    ]


def _validation_item(
    *,
    item_id: str,
    title_text: str,
    category: str,
    status: str,
    required_evidence: list[str],
    acceptance_rule: str,
    source_artifact: str,
    stage_assignment: str,
    details: str,
) -> dict[str, Any]:
    return {
        "item_id": item_id,
        "title_text": title_text,
        "category": category,
        "status": status,
        "required_evidence": list(required_evidence),
        "acceptance_rule": acceptance_rule,
        "source_artifact": source_artifact,
        "stage_assignment": stage_assignment,
        "details": details,
    }


def _build_validation_lines(validation_items: list[dict[str, Any]]) -> list[str]:
    rows: list[str] = []
    for item in validation_items:
        title_text = str(item.get("title_text") or item.get("item_id") or "").strip()
        status = str(item.get("status") or "").strip()
        acceptance_rule = str(item.get("acceptance_rule") or "").strip()
        required_evidence = "、".join(str(value) for value in list(item.get("required_evidence") or []) if str(value).strip())
        rows.append(
            f"{title_text}：{status}；需要真实证据：{required_evidence or '无'}；判定规则：{acceptance_rule}"
        )
    return rows


def _build_pass_fail_lines(pass_fail_contract: dict[str, Any]) -> list[str]:
    rows = [
        f"判定阶段：{pass_fail_contract.get('decision_stage') or '--'}",
        "通过条件：" + "、".join(
            str(item) for item in list(pass_fail_contract.get("pass_requires") or []) if str(item).strip()
        ),
        "失败触发：" + "、".join(
            str(item) for item in list(pass_fail_contract.get("fail_triggers") or []) if str(item).strip()
        ),
        "边界说明：本 contract 只定义第三阶段真实判定边界，不代表当前已完成真实判定。",
    ]
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
    }
    rows: list[str] = []
    for key, label in labels.items():
        payload = dict(artifact_refs.get(key) or {})
        path_text = str(payload.get("path") or "").strip()
        summary_text = str(payload.get("summary_text") or "").strip()
        line = f"`{label}`：{summary_text}".strip("：")
        if path_text:
            line = f"{line}（{path_text}）"
        rows.append(line)
    return rows


def _render_stage3_real_validation_plan_markdown(display: dict[str, Any]) -> str:
    lines = [
        f"# {display.get('title_text') or 'Stage 3 Real Validation Plan / 第三阶段真实验证计划'}",
        "",
        "> 离线 reviewer artifact：本工件只定义第三阶段真实验证计划与真实证据矩阵，不代表验证已完成，不是 real acceptance，也不能替代真实计量验证。",
        "",
        "## 当前阶段",
        "",
        f"- {display.get('current_stage_text') or '--'}",
        f"- {display.get('next_stage_text') or '--'}",
        f"- {display.get('status_line') or '--'}",
        f"- {display.get('engineering_isolation_text') or '--'}",
        f"- {display.get('real_acceptance_text') or '--'}",
        "",
        "## 当前只能做到的内容",
        "",
        f"- {display.get('execute_now_text') or '--'}",
        "",
        "## 第三阶段必须完成的真实验证项",
        "",
    ]
    lines.extend(f"- {line}" for line in list(display.get("validation_lines") or []))
    lines.extend(["", "## 真实证据要求", ""])
    lines.extend(f"- {line}" for line in list(display.get("required_evidence_lines") or []))
    lines.extend(["", "## pass/fail 基本边界", ""])
    lines.extend(f"- {line}" for line in list(display.get("pass_fail_lines") or []))
    lines.extend(["", "## 关联工件", ""])
    lines.extend(f"- {line}" for line in list(display.get("artifact_lines") or []))
    lines.extend(
        [
            "",
            "## 审阅提示",
            "",
            f"- {display.get('blocking_text') or '--'}",
            f"- {display.get('warning_text') or '--'}",
            f"- {display.get('defer_to_stage3_text') or '--'}",
            f"- {display.get('plan_boundary_text') or '--'}",
        ]
    )
    return "\n".join(line for line in lines if str(line).strip() or line == "") + "\n"


def _count_statuses(items: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        status = str(item.get("status") or "").strip()
        if not status:
            continue
        counts[status] = int(counts.get(status, 0) or 0) + 1
    return counts


def _dedupe(values: list[Any]) -> list[str]:
    rows: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in rows:
            rows.append(text)
    return rows
