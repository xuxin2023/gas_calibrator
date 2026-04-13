from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .governance_handoff_contracts import GOVERNANCE_HANDOFF_FILENAMES as _GOV_FILENAMES
from .phase_evidence_display_contracts import (
    BRIDGE_SECTION_LABELS as _SECTION_LABELS,
    BRIDGE_REVIEWER_TEXTS as _BRIDGE_TEXTS,
)


PHASE_TRANSITION_BRIDGE_FILENAME = _GOV_FILENAMES["phase_transition_bridge"]
_METROLOGY_SECTION_IDS = [
    "reference_traceability_contract",
    "calibration_execution_contract",
    "data_quality_contract",
    "uncertainty_budget_template",
    "coefficient_verification_contract",
    "evidence_traceability_contract",
    "reporting_contract",
]


def build_phase_transition_bridge(
    *,
    run_id: str,
    step2_readiness_summary: dict[str, Any] | None,
    metrology_calibration_contract: dict[str, Any] | None,
) -> dict[str, Any]:
    readiness = dict(step2_readiness_summary or {})
    metrology = dict(metrology_calibration_contract or {})

    readiness_ref = {
        "artifact_type": str(readiness.get("artifact_type") or "step2_readiness_summary"),
        "phase": str(readiness.get("phase") or "step2_readiness_bridge"),
        "overall_status": str(readiness.get("overall_status") or "not_ready"),
        "ready_for_engineering_isolation": bool(readiness.get("ready_for_engineering_isolation", False)),
        "real_acceptance_ready": bool(readiness.get("real_acceptance_ready", False)),
    }
    metrology_ref = {
        "artifact_type": str(metrology.get("artifact_type") or "metrology_calibration_contract"),
        "phase": str(metrology.get("phase") or "step2_tail_step3_bridge"),
        "overall_status": str(metrology.get("overall_status") or "contract_missing"),
        "real_acceptance_ready": bool(metrology.get("real_acceptance_ready", False)),
    }

    ready_for_engineering_isolation = readiness_ref["ready_for_engineering_isolation"]
    real_acceptance_ready = readiness_ref["real_acceptance_ready"] or metrology_ref["real_acceptance_ready"]

    execute_now_in_step2_tail = _dedupe(
        [
            *list(
                dict(metrology.get("stage_assignment") or {}).get("execute_now_in_step2_tail")
                or []
            ),
            *[f"resolve_{item}" for item in list(readiness.get("blocking_items") or [])],
        ]
    )
    defer_to_stage3_real_validation = _dedupe(
        [
            *list(
                dict(metrology.get("stage_assignment") or {}).get("defer_to_stage3_real_validation")
                or []
            ),
            *list(metrology.get("stage3_execution_items") or []),
        ]
    )
    blocking_items = _dedupe(list(readiness.get("blocking_items") or []))
    warning_items = _dedupe(
        [
            *list(readiness.get("warning_items") or []),
            *list(metrology.get("warning_items") or []),
            "phase_transition_bridge_not_real_acceptance",
        ]
    )
    missing_real_world_evidence = _dedupe(
        defer_to_stage3_real_validation
        + [
            "real_reference_evidence",
            "real_certificate_cycle_enforcement",
            "real_run_uncertainty_result",
            "real_writeback_acceptance",
            "real_acceptance_decision",
        ]
    )

    gate_matrix = [
        {
            "gate_id": str(item.get("gate_id") or ""),
            "source_artifact": readiness_ref["artifact_type"],
            "status": str(item.get("status") or ""),
            "reason_code": str(item.get("reason_code") or ""),
        }
        for item in list(readiness.get("gates") or [])
    ]
    for section_id in _METROLOGY_SECTION_IDS:
        gate_matrix.append(
            {
                "gate_id": section_id,
                "source_artifact": metrology_ref["artifact_type"],
                "status": "defined" if section_id in metrology else "missing",
                "reason_code": "contract_defined" if section_id in metrology else "contract_missing",
            }
        )

    if real_acceptance_ready:
        overall_status = "blocked_before_stage3"
        recommended_next_stage = "audit_real_acceptance_claim"
    elif ready_for_engineering_isolation:
        overall_status = "ready_for_engineering_isolation"
        recommended_next_stage = "engineering_isolation"
    elif metrology_ref["overall_status"] == "contract_ready_for_stage3_bridge":
        overall_status = "step2_tail_in_progress"
        recommended_next_stage = "close_step2_tail_gaps"
    else:
        overall_status = "blocked_before_stage3"
        recommended_next_stage = "stabilize_step2_governance"

    reviewer_display = _build_reviewer_display(
        overall_status=overall_status,
        recommended_next_stage=recommended_next_stage,
        ready_for_engineering_isolation=ready_for_engineering_isolation,
        blocking_items=blocking_items,
        warning_items=warning_items,
        execute_now_in_step2_tail=execute_now_in_step2_tail,
        defer_to_stage3_real_validation=defer_to_stage3_real_validation,
        gate_matrix=gate_matrix,
    )

    return {
        "schema_version": "1.0",
        "artifact_type": "phase_transition_bridge",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": str(run_id or ""),
        "phase": "step2_tail_stage3_bridge",
        "mode": "simulation_only",
        "overall_status": overall_status,
        "recommended_next_stage": recommended_next_stage,
        "ready_for_engineering_isolation": ready_for_engineering_isolation,
        "real_acceptance_ready": False,
        "step2_readiness_ref": readiness_ref,
        "metrology_contract_ref": metrology_ref,
        "execute_now_in_step2_tail": execute_now_in_step2_tail,
        "defer_to_stage3_real_validation": defer_to_stage3_real_validation,
        "blocking_items": blocking_items,
        "warning_items": warning_items,
        "missing_real_world_evidence": missing_real_world_evidence,
        "gate_matrix": gate_matrix,
        "notes": [
            "phase_transition_bridge_artifact",
            "step2_tail_stage3_bridge",
            "simulation_offline_headless_only",
            "not_real_acceptance_evidence",
            "default_path_unchanged",
        ],
        "reviewer_display": reviewer_display,
    }


def _build_reviewer_display(
    *,
    overall_status: str,
    recommended_next_stage: str,
    ready_for_engineering_isolation: bool,
    blocking_items: list[str],
    warning_items: list[str],
    execute_now_in_step2_tail: list[str],
    defer_to_stage3_real_validation: list[str],
    gate_matrix: list[dict[str, Any]],
) -> dict[str, Any]:
    if overall_status == "ready_for_engineering_isolation":
        status_line = _BRIDGE_TEXTS["status_ready_for_engineering_isolation"]
    elif overall_status == "step2_tail_in_progress":
        status_line = _BRIDGE_TEXTS["status_step2_tail_in_progress"]
    else:
        status_line = _BRIDGE_TEXTS["status_blocked_before_stage3"]
    summary_text = _BRIDGE_TEXTS["summary_text"]
    current_stage_text = _BRIDGE_TEXTS["current_stage_text"]
    next_stage_text = (
        _BRIDGE_TEXTS["next_stage_ready"]
        if ready_for_engineering_isolation
        else _BRIDGE_TEXTS["next_stage_not_ready"]
    )
    execute_now_text = _BRIDGE_TEXTS["execute_now_prefix"] + "、".join(execute_now_in_step2_tail) + "。"
    defer_to_stage3_text = _BRIDGE_TEXTS["defer_to_stage3_prefix"] + "、".join(defer_to_stage3_real_validation) + "。"
    blocking_text = _BRIDGE_TEXTS["no_blocking"] if not blocking_items else _BRIDGE_TEXTS["blocking_prefix"] + "、".join(blocking_items) + "。"
    warning_text = _BRIDGE_TEXTS["warning_prefix"] + "、".join(warning_items) + "。"
    gate_lines = [
        _build_gate_line(item)
        for item in gate_matrix
    ]
    gate_lines.append(f"{_BRIDGE_TEXTS['recommended_next_stage_prefix']}{recommended_next_stage}")
    return {
        "summary_text": summary_text,
        "status_line": status_line,
        "current_stage_text": current_stage_text,
        "next_stage_text": next_stage_text,
        "execute_now_text": execute_now_text,
        "defer_to_stage3_text": defer_to_stage3_text,
        "blocking_text": blocking_text,
        "warning_text": warning_text,
        "gate_lines": gate_lines,
    }


def _build_gate_line(item: dict[str, Any]) -> str:
    gate_id = str(item.get("gate_id") or "")
    source = str(item.get("source_artifact") or "")
    status = str(item.get("status") or "")
    reason_code = str(item.get("reason_code") or "")
    if source == "metrology_calibration_contract":
        label = _SECTION_LABELS.get(gate_id, gate_id)
        return f"{label}：{_BRIDGE_TEXTS['gate_status_defined'] if status == 'defined' else _BRIDGE_TEXTS['gate_status_missing']}（{reason_code}）。"
    return f"{gate_id}：{status}（{reason_code}）。"


def _dedupe(values: list[str]) -> list[str]:
    rows: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in rows:
            rows.append(text)
    return rows
