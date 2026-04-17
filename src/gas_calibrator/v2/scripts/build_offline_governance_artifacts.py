from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Iterable, Optional

from ..core.offline_artifacts import (
    ANALYTICS_SUMMARY_FILENAME,
    export_run_offline_artifacts,
    export_suite_offline_artifacts,
    write_json,
)
from ..core.metrology_calibration_contract import (
    METROLOGY_CALIBRATION_CONTRACT_FILENAME,
    build_metrology_calibration_contract,
)
from ..core.engineering_isolation_admission_checklist import (
    ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME,
    ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME,
    build_engineering_isolation_admission_checklist,
)
from ..core.stage3_real_validation_plan import (
    STAGE3_REAL_VALIDATION_PLAN_FILENAME,
    STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME,
    build_stage3_real_validation_plan,
)
from ..core.stage3_standards_alignment_matrix import (
    STAGE3_STANDARDS_ALIGNMENT_MATRIX_FILENAME,
    STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_FILENAME,
    build_stage3_standards_alignment_matrix,
)
from ..core.phase_transition_bridge import (
    PHASE_TRANSITION_BRIDGE_FILENAME,
    build_phase_transition_bridge,
)
from ..core.phase_transition_bridge_presenter import build_phase_transition_bridge_panel_payload
from ..core.phase_transition_bridge_reviewer_artifact import (
    PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME,
    build_phase_transition_bridge_reviewer_artifact,
)
from ..core import recognition_readiness_artifacts as recognition_readiness
from ..core.stage_admission_review_pack import (
    STAGE_ADMISSION_REVIEW_PACK_FILENAME,
    STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME,
    build_stage_admission_review_pack,
)
from ..core.step2_readiness import (
    STEP2_READINESS_SUMMARY_FILENAME,
    build_step2_readiness_summary,
)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rebuild offline acceptance/analytics/lineage artifacts.")
    parser.add_argument("--run-dir", default=None, help="Run directory containing summary/manifest/results.")
    parser.add_argument("--suite-dir", default=None, help="Suite directory containing suite_summary.json.")
    return parser.parse_args(list(argv) if argv is not None else None)


def _load_json(path: Path) -> dict[str, object]:
    if not path.exists():
        raise FileNotFoundError(f"required artifact missing: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _objectify(value):
    if isinstance(value, dict):
        return SimpleNamespace(**{key: _objectify(item) for key, item in value.items()})
    if isinstance(value, list):
        return [_objectify(item) for item in value]
    return value


def _default_smoke_paths() -> tuple[Path, Path]:
    config_dir = Path(__file__).resolve().parents[1] / "configs"
    return config_dir / "smoke_v2_minimal.json", config_dir / "smoke_points_minimal.json"


def _augment_run_payload_with_step2_readiness(
    payload: dict[str, object],
    *,
    run_dir: Path,
    run_id: str,
    simulation_mode: bool,
) -> dict[str, object]:
    analytics_summary = dict(payload.get("summary_stats", {}).get("analytics_summary") or _load_json(run_dir / ANALYTICS_SUMMARY_FILENAME))
    smoke_config_path, smoke_points_path = _default_smoke_paths()
    readiness_summary = build_step2_readiness_summary(
        run_id=run_id,
        simulation_mode=simulation_mode,
        config_governance_handoff=dict(analytics_summary.get("config_governance_handoff") or {}),
        smoke_config_path=smoke_config_path,
        smoke_points_path=smoke_points_path,
    )
    analytics_summary["step2_readiness_summary"] = dict(readiness_summary)
    write_json(run_dir / ANALYTICS_SUMMARY_FILENAME, analytics_summary)
    readiness_path = write_json(run_dir / STEP2_READINESS_SUMMARY_FILENAME, readiness_summary)
    metrology_contract = build_metrology_calibration_contract(
        run_id=run_id,
        simulation_mode=simulation_mode,
        config_governance_handoff=dict(analytics_summary.get("config_governance_handoff") or {}),
    )
    analytics_summary["metrology_calibration_contract"] = dict(metrology_contract)
    write_json(run_dir / ANALYTICS_SUMMARY_FILENAME, analytics_summary)
    metrology_path = write_json(run_dir / METROLOGY_CALIBRATION_CONTRACT_FILENAME, metrology_contract)
    phase_transition_bridge = build_phase_transition_bridge(
        run_id=run_id,
        step2_readiness_summary=readiness_summary,
        metrology_calibration_contract=metrology_contract,
    )
    phase_transition_bridge_surface_bundle = build_phase_transition_bridge_panel_payload(phase_transition_bridge)
    phase_transition_bridge_reviewer_artifact = build_phase_transition_bridge_reviewer_artifact(phase_transition_bridge)
    analytics_summary["phase_transition_bridge"] = dict(phase_transition_bridge)
    analytics_summary["phase_transition_bridge_reviewer_section"] = dict(phase_transition_bridge_surface_bundle)
    write_json(run_dir / ANALYTICS_SUMMARY_FILENAME, analytics_summary)
    phase_transition_path = write_json(run_dir / PHASE_TRANSITION_BRIDGE_FILENAME, phase_transition_bridge)
    phase_transition_reviewer_path = run_dir / PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME
    phase_transition_reviewer_path.write_text(
        str(phase_transition_bridge_reviewer_artifact.get("markdown") or ""),
        encoding="utf-8",
    )
    stage_admission_review_pack = build_stage_admission_review_pack(
        run_id=run_id,
        step2_readiness_summary=readiness_summary,
        metrology_calibration_contract=metrology_contract,
        phase_transition_bridge=phase_transition_bridge,
        phase_transition_bridge_reviewer_artifact=phase_transition_bridge_reviewer_artifact,
        artifact_paths={
            "step2_readiness_summary": readiness_path,
            "metrology_calibration_contract": metrology_path,
            "phase_transition_bridge": phase_transition_path,
            "phase_transition_bridge_reviewer_artifact": phase_transition_reviewer_path,
        },
    )
    stage_admission_review_pack_path = write_json(
        run_dir / STAGE_ADMISSION_REVIEW_PACK_FILENAME,
        dict(stage_admission_review_pack.get("raw") or {}),
    )
    stage_admission_review_pack_reviewer_path = run_dir / STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME
    stage_admission_review_pack_reviewer_path.write_text(
        str(stage_admission_review_pack.get("markdown") or ""),
        encoding="utf-8",
    )
    analytics_summary["stage_admission_review_pack"] = dict(stage_admission_review_pack.get("raw") or {})
    engineering_isolation_admission_checklist = build_engineering_isolation_admission_checklist(
        run_id=run_id,
        step2_readiness_summary=readiness_summary,
        metrology_calibration_contract=metrology_contract,
        phase_transition_bridge=phase_transition_bridge,
        stage_admission_review_pack=stage_admission_review_pack,
        artifact_paths={
            "step2_readiness_summary": readiness_path,
            "metrology_calibration_contract": metrology_path,
            "phase_transition_bridge": phase_transition_path,
            "phase_transition_bridge_reviewer_artifact": phase_transition_reviewer_path,
            "stage_admission_review_pack": stage_admission_review_pack_path,
            "stage_admission_review_pack_reviewer_artifact": stage_admission_review_pack_reviewer_path,
        },
    )
    engineering_isolation_admission_checklist_path = write_json(
        run_dir / ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME,
        dict(engineering_isolation_admission_checklist.get("raw") or {}),
    )
    engineering_isolation_admission_checklist_reviewer_path = (
        run_dir / ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME
    )
    engineering_isolation_admission_checklist_reviewer_path.write_text(
        str(engineering_isolation_admission_checklist.get("markdown") or ""),
        encoding="utf-8",
    )
    analytics_summary["engineering_isolation_admission_checklist"] = dict(
        engineering_isolation_admission_checklist.get("raw") or {}
    )
    stage3_real_validation_plan = build_stage3_real_validation_plan(
        run_id=run_id,
        step2_readiness_summary=readiness_summary,
        metrology_calibration_contract=metrology_contract,
        phase_transition_bridge=phase_transition_bridge,
        stage_admission_review_pack=stage_admission_review_pack,
        engineering_isolation_admission_checklist=engineering_isolation_admission_checklist,
        artifact_paths={
            "step2_readiness_summary": readiness_path,
            "metrology_calibration_contract": metrology_path,
            "phase_transition_bridge": phase_transition_path,
            "phase_transition_bridge_reviewer_artifact": phase_transition_reviewer_path,
            "stage_admission_review_pack": stage_admission_review_pack_path,
            "stage_admission_review_pack_reviewer_artifact": stage_admission_review_pack_reviewer_path,
            "engineering_isolation_admission_checklist": engineering_isolation_admission_checklist_path,
            "engineering_isolation_admission_checklist_reviewer_artifact": (
                engineering_isolation_admission_checklist_reviewer_path
            ),
        },
    )
    stage3_real_validation_plan_path = write_json(
        run_dir / STAGE3_REAL_VALIDATION_PLAN_FILENAME,
        dict(stage3_real_validation_plan.get("raw") or {}),
    )
    stage3_real_validation_plan_reviewer_path = run_dir / STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME
    stage3_real_validation_plan_reviewer_path.write_text(
        str(stage3_real_validation_plan.get("markdown") or ""),
        encoding="utf-8",
    )
    analytics_summary["stage3_real_validation_plan"] = dict(stage3_real_validation_plan.get("raw") or {})
    scope_definition_pack_path = run_dir / recognition_readiness.SCOPE_DEFINITION_PACK_FILENAME
    decision_rule_profile_path = run_dir / recognition_readiness.DECISION_RULE_PROFILE_FILENAME
    scope_definition_pack = _load_json(scope_definition_pack_path) if scope_definition_pack_path.exists() else {}
    decision_rule_profile = _load_json(decision_rule_profile_path) if decision_rule_profile_path.exists() else {}
    stage3_standards_alignment_matrix = build_stage3_standards_alignment_matrix(
        run_id=run_id,
        step2_readiness_summary=readiness_summary,
        metrology_calibration_contract=metrology_contract,
        phase_transition_bridge=phase_transition_bridge,
        stage_admission_review_pack=stage_admission_review_pack,
        engineering_isolation_admission_checklist=engineering_isolation_admission_checklist,
        stage3_real_validation_plan=stage3_real_validation_plan,
        scope_definition_pack=scope_definition_pack,
        decision_rule_profile=decision_rule_profile,
        conformity_statement_profile=dict(decision_rule_profile.get("conformity_statement_profile") or {}),
        artifact_paths={
            "step2_readiness_summary": readiness_path,
            "metrology_calibration_contract": metrology_path,
            "phase_transition_bridge": phase_transition_path,
            "phase_transition_bridge_reviewer_artifact": phase_transition_reviewer_path,
            "stage_admission_review_pack": stage_admission_review_pack_path,
            "stage_admission_review_pack_reviewer_artifact": stage_admission_review_pack_reviewer_path,
            "engineering_isolation_admission_checklist": engineering_isolation_admission_checklist_path,
            "engineering_isolation_admission_checklist_reviewer_artifact": (
                engineering_isolation_admission_checklist_reviewer_path
            ),
            "stage3_real_validation_plan": stage3_real_validation_plan_path,
            "stage3_real_validation_plan_reviewer_artifact": stage3_real_validation_plan_reviewer_path,
            "scope_definition_pack": scope_definition_pack_path,
            "decision_rule_profile": decision_rule_profile_path,
        },
    )
    stage3_standards_alignment_matrix_path = write_json(
        run_dir / STAGE3_STANDARDS_ALIGNMENT_MATRIX_FILENAME,
        dict(stage3_standards_alignment_matrix.get("raw") or {}),
    )
    stage3_standards_alignment_matrix_reviewer_path = (
        run_dir / STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_FILENAME
    )
    stage3_standards_alignment_matrix_reviewer_path.write_text(
        str(stage3_standards_alignment_matrix.get("markdown") or ""),
        encoding="utf-8",
    )
    analytics_summary["stage3_standards_alignment_matrix"] = dict(
        stage3_standards_alignment_matrix.get("raw") or {}
    )
    write_json(run_dir / ANALYTICS_SUMMARY_FILENAME, analytics_summary)

    summary_stats = dict(payload.get("summary_stats") or {})
    summary_stats["analytics_summary"] = analytics_summary
    summary_stats["step2_readiness_summary"] = dict(readiness_summary)
    summary_stats["metrology_calibration_contract"] = dict(metrology_contract)
    summary_stats["phase_transition_bridge"] = dict(phase_transition_bridge)
    summary_stats["step2_readiness_digest"] = {
        "phase": readiness_summary.get("phase"),
        "overall_status": readiness_summary.get("overall_status"),
        "ready_for_engineering_isolation": bool(readiness_summary.get("ready_for_engineering_isolation", False)),
        "real_acceptance_ready": bool(readiness_summary.get("real_acceptance_ready", False)),
        "gate_status_counts": dict(readiness_summary.get("gate_status_counts") or {}),
        "blocking_items": list(readiness_summary.get("blocking_items") or []),
        "warning_items": list(readiness_summary.get("warning_items") or []),
        "evidence_mode": readiness_summary.get("evidence_mode"),
    }
    summary_stats["metrology_calibration_contract_digest"] = {
        "phase": metrology_contract.get("phase"),
        "overall_status": metrology_contract.get("overall_status"),
        "real_acceptance_ready": bool(metrology_contract.get("real_acceptance_ready", False)),
        "stage_assignment": dict(metrology_contract.get("stage_assignment") or {}),
        "stage3_execution_items": list(metrology_contract.get("stage3_execution_items") or []),
        "blocking_items": list(metrology_contract.get("blocking_items") or []),
        "warning_items": list(metrology_contract.get("warning_items") or []),
        "evidence_mode": metrology_contract.get("evidence_mode"),
    }
    summary_stats["phase_transition_bridge_digest"] = {
        "phase": phase_transition_bridge.get("phase"),
        "overall_status": phase_transition_bridge.get("overall_status"),
        "recommended_next_stage": phase_transition_bridge.get("recommended_next_stage"),
        "ready_for_engineering_isolation": bool(phase_transition_bridge.get("ready_for_engineering_isolation", False)),
        "real_acceptance_ready": bool(phase_transition_bridge.get("real_acceptance_ready", False)),
        "blocking_items": list(phase_transition_bridge.get("blocking_items") or []),
        "warning_items": list(phase_transition_bridge.get("warning_items") or []),
        "missing_real_world_evidence": list(phase_transition_bridge.get("missing_real_world_evidence") or []),
    }
    summary_stats["phase_transition_bridge_reviewer_section"] = dict(phase_transition_bridge_surface_bundle)
    summary_stats["stage_admission_review_pack"] = dict(stage_admission_review_pack.get("raw") or {})
    summary_stats["stage_admission_review_pack_digest"] = {
        "phase": stage_admission_review_pack["raw"].get("phase"),
        "overall_status": stage_admission_review_pack["raw"].get("overall_status"),
        "recommended_next_stage": stage_admission_review_pack["raw"].get("recommended_next_stage"),
        "ready_for_engineering_isolation": bool(
            stage_admission_review_pack["raw"].get("ready_for_engineering_isolation", False)
        ),
        "real_acceptance_ready": bool(
            stage_admission_review_pack["raw"].get("real_acceptance_ready", False)
        ),
        "artifact_paths": dict(stage_admission_review_pack["raw"].get("artifact_paths") or {}),
        "missing_real_world_evidence": list(
            stage_admission_review_pack["raw"].get("missing_real_world_evidence") or []
        ),
    }
    summary_stats["engineering_isolation_admission_checklist"] = dict(
        engineering_isolation_admission_checklist.get("raw") or {}
    )
    summary_stats["engineering_isolation_admission_checklist_digest"] = {
        "phase": engineering_isolation_admission_checklist["raw"].get("phase"),
        "overall_status": engineering_isolation_admission_checklist["raw"].get("overall_status"),
        "recommended_next_stage": engineering_isolation_admission_checklist["raw"].get("recommended_next_stage"),
        "ready_for_engineering_isolation": bool(
            engineering_isolation_admission_checklist["raw"].get("ready_for_engineering_isolation", False)
        ),
        "real_acceptance_ready": bool(
            engineering_isolation_admission_checklist["raw"].get("real_acceptance_ready", False)
        ),
        "artifact_paths": dict(engineering_isolation_admission_checklist["raw"].get("artifact_paths") or {}),
        "checklist_status_counts": dict(
            engineering_isolation_admission_checklist["raw"].get("checklist_status_counts") or {}
        ),
        "missing_real_world_evidence": list(
            engineering_isolation_admission_checklist["raw"].get("missing_real_world_evidence") or []
        ),
    }
    summary_stats["stage3_real_validation_plan"] = dict(stage3_real_validation_plan.get("raw") or {})
    summary_stats["stage3_real_validation_plan_digest"] = {
        "phase": stage3_real_validation_plan["raw"].get("phase"),
        "overall_status": stage3_real_validation_plan["raw"].get("overall_status"),
        "recommended_next_stage": stage3_real_validation_plan["raw"].get("recommended_next_stage"),
        "ready_for_engineering_isolation": bool(
            stage3_real_validation_plan["raw"].get("ready_for_engineering_isolation", False)
        ),
        "real_acceptance_ready": bool(
            stage3_real_validation_plan["raw"].get("real_acceptance_ready", False)
        ),
        "artifact_paths": dict(stage3_real_validation_plan["raw"].get("artifact_paths") or {}),
        "validation_status_counts": dict(
            stage3_real_validation_plan["raw"].get("validation_status_counts") or {}
        ),
        "required_real_world_evidence": list(
            stage3_real_validation_plan["raw"].get("required_real_world_evidence") or []
        ),
    }
    summary_stats["stage3_standards_alignment_matrix"] = dict(
        stage3_standards_alignment_matrix.get("raw") or {}
    )
    summary_stats["stage3_standards_alignment_matrix_digest"] = {
        "phase": stage3_standards_alignment_matrix["raw"].get("phase"),
        "overall_status": stage3_standards_alignment_matrix["raw"].get("overall_status"),
        "recommended_next_stage": stage3_standards_alignment_matrix["raw"].get("recommended_next_stage"),
        "mapping_scope": stage3_standards_alignment_matrix["raw"].get("mapping_scope"),
        "standard_family_count": len(stage3_standards_alignment_matrix["raw"].get("standard_families") or []),
        "mapping_row_count": len(stage3_standards_alignment_matrix["raw"].get("rows") or []),
        "required_evidence_category_count": len(
            stage3_standards_alignment_matrix["raw"].get("required_evidence_categories") or []
        ),
        "standard_families": list(stage3_standards_alignment_matrix["raw"].get("standard_families") or []),
        "required_evidence_categories": list(
            stage3_standards_alignment_matrix["raw"].get("required_evidence_categories") or []
        ),
        "readiness_status_counts": dict(
            stage3_standards_alignment_matrix["raw"].get("readiness_status_counts") or {}
        ),
        "boundary_statements": list(stage3_standards_alignment_matrix["raw"].get("boundary_statements") or []),
        "artifact_paths": dict(stage3_standards_alignment_matrix["raw"].get("artifact_paths") or {}),
    }
    payload["summary_stats"] = summary_stats

    artifact_statuses = dict(payload.get("artifact_statuses") or {})
    artifact_statuses["step2_readiness_summary"] = {
        "status": "ok",
        "role": "execution_summary",
        "path": str(readiness_path),
    }
    artifact_statuses["metrology_calibration_contract"] = {
        "status": "ok",
        "role": "formal_analysis",
        "path": str(metrology_path),
    }
    artifact_statuses["phase_transition_bridge"] = {
        "status": "ok",
        "role": "execution_summary",
        "path": str(phase_transition_path),
    }
    artifact_statuses["phase_transition_bridge_reviewer_artifact"] = {
        "status": "ok",
        "role": "formal_analysis",
        "path": str(phase_transition_reviewer_path),
    }
    artifact_statuses["stage_admission_review_pack"] = {
        "status": "ok",
        "role": "execution_summary",
        "path": str(stage_admission_review_pack_path),
    }
    artifact_statuses["stage_admission_review_pack_reviewer_artifact"] = {
        "status": "ok",
        "role": "formal_analysis",
        "path": str(stage_admission_review_pack_reviewer_path),
    }
    artifact_statuses["engineering_isolation_admission_checklist"] = {
        "status": "ok",
        "role": "execution_summary",
        "path": str(engineering_isolation_admission_checklist_path),
    }
    artifact_statuses["engineering_isolation_admission_checklist_reviewer_artifact"] = {
        "status": "ok",
        "role": "formal_analysis",
        "path": str(engineering_isolation_admission_checklist_reviewer_path),
    }
    artifact_statuses["stage3_real_validation_plan"] = {
        "status": "ok",
        "role": "execution_summary",
        "path": str(stage3_real_validation_plan_path),
    }
    artifact_statuses["stage3_real_validation_plan_reviewer_artifact"] = {
        "status": "ok",
        "role": "formal_analysis",
        "path": str(stage3_real_validation_plan_reviewer_path),
    }
    artifact_statuses["stage3_standards_alignment_matrix"] = {
        "status": "ok",
        "role": "execution_summary",
        "path": str(stage3_standards_alignment_matrix_path),
    }
    artifact_statuses["stage3_standards_alignment_matrix_reviewer_artifact"] = {
        "status": "ok",
        "role": "formal_analysis",
        "path": str(stage3_standards_alignment_matrix_reviewer_path),
    }
    payload["artifact_statuses"] = artifact_statuses

    manifest_sections = dict(payload.get("manifest_sections") or {})
    manifest_sections["step2_readiness"] = {
        "phase": readiness_summary.get("phase"),
        "overall_status": readiness_summary.get("overall_status"),
        "ready_for_engineering_isolation": bool(readiness_summary.get("ready_for_engineering_isolation", False)),
        "real_acceptance_ready": bool(readiness_summary.get("real_acceptance_ready", False)),
        "evidence_mode": readiness_summary.get("evidence_mode"),
        "blocking_items": list(readiness_summary.get("blocking_items") or []),
        "warning_items": list(readiness_summary.get("warning_items") or []),
        "gate_status_counts": dict(readiness_summary.get("gate_status_counts") or {}),
        "not_real_acceptance_evidence": bool(readiness_summary.get("not_real_acceptance_evidence", True)),
    }
    manifest_sections["metrology_calibration_contract"] = {
        "phase": metrology_contract.get("phase"),
        "overall_status": metrology_contract.get("overall_status"),
        "real_acceptance_ready": bool(metrology_contract.get("real_acceptance_ready", False)),
        "stage_assignment": dict(metrology_contract.get("stage_assignment") or {}),
        "stage3_execution_items": list(metrology_contract.get("stage3_execution_items") or []),
        "blocking_items": list(metrology_contract.get("blocking_items") or []),
        "warning_items": list(metrology_contract.get("warning_items") or []),
        "not_real_acceptance_evidence": bool(metrology_contract.get("not_real_acceptance_evidence", True)),
    }
    manifest_sections["phase_transition_bridge"] = {
        "phase": phase_transition_bridge.get("phase"),
        "overall_status": phase_transition_bridge.get("overall_status"),
        "recommended_next_stage": phase_transition_bridge.get("recommended_next_stage"),
        "ready_for_engineering_isolation": bool(phase_transition_bridge.get("ready_for_engineering_isolation", False)),
        "real_acceptance_ready": bool(phase_transition_bridge.get("real_acceptance_ready", False)),
        "blocking_items": list(phase_transition_bridge.get("blocking_items") or []),
        "warning_items": list(phase_transition_bridge.get("warning_items") or []),
        "not_real_acceptance_evidence": True,
    }
    manifest_sections["phase_transition_bridge_reviewer_section"] = dict(phase_transition_bridge_surface_bundle)
    manifest_sections["phase_transition_bridge_reviewer_artifact"] = {
        "artifact_type": str(phase_transition_bridge_reviewer_artifact.get("artifact_type") or ""),
        "path": str(phase_transition_reviewer_path),
        "available": bool(phase_transition_bridge_reviewer_artifact.get("available", False)),
        "summary_text": str(phase_transition_bridge_reviewer_artifact.get("display", {}).get("summary_text") or ""),
        "status_line": str(phase_transition_bridge_reviewer_artifact.get("display", {}).get("status_line") or ""),
        "current_stage_text": str(
            phase_transition_bridge_reviewer_artifact.get("display", {}).get("current_stage_text") or ""
        ),
        "next_stage_text": str(
            phase_transition_bridge_reviewer_artifact.get("display", {}).get("next_stage_text") or ""
        ),
        "engineering_isolation_text": str(
            phase_transition_bridge_reviewer_artifact.get("display", {}).get("engineering_isolation_text") or ""
        ),
        "real_acceptance_text": str(
            phase_transition_bridge_reviewer_artifact.get("display", {}).get("real_acceptance_text") or ""
        ),
        "execute_now_text": str(
            phase_transition_bridge_reviewer_artifact.get("display", {}).get("execute_now_text") or ""
        ),
        "defer_to_stage3_text": str(
            phase_transition_bridge_reviewer_artifact.get("display", {}).get("defer_to_stage3_text") or ""
        ),
        "blocking_text": str(phase_transition_bridge_reviewer_artifact.get("display", {}).get("blocking_text") or ""),
        "warning_text": str(phase_transition_bridge_reviewer_artifact.get("display", {}).get("warning_text") or ""),
        "not_real_acceptance_evidence": True,
    }
    manifest_sections["stage_admission_review_pack"] = {
        "artifact_type": str(stage_admission_review_pack["raw"].get("artifact_type") or ""),
        "path": str(stage_admission_review_pack_path),
        "reviewer_path": str(stage_admission_review_pack_reviewer_path),
        "phase": stage_admission_review_pack["raw"].get("phase"),
        "overall_status": stage_admission_review_pack["raw"].get("overall_status"),
        "recommended_next_stage": stage_admission_review_pack["raw"].get("recommended_next_stage"),
        "ready_for_engineering_isolation": bool(
            stage_admission_review_pack["raw"].get("ready_for_engineering_isolation", False)
        ),
        "real_acceptance_ready": bool(stage_admission_review_pack["raw"].get("real_acceptance_ready", False)),
        "artifact_paths": dict(stage_admission_review_pack["raw"].get("artifact_paths") or {}),
        "execute_now_in_step2_tail": list(
            stage_admission_review_pack["raw"].get("execute_now_in_step2_tail") or []
        ),
        "defer_to_stage3_real_validation": list(
            stage_admission_review_pack["raw"].get("defer_to_stage3_real_validation") or []
        ),
        "blocking_items": list(stage_admission_review_pack["raw"].get("blocking_items") or []),
        "warning_items": list(stage_admission_review_pack["raw"].get("warning_items") or []),
        "missing_real_world_evidence": list(
            stage_admission_review_pack["raw"].get("missing_real_world_evidence") or []
        ),
        "handoff_checklist": dict(stage_admission_review_pack["raw"].get("handoff_checklist") or {}),
        "notes": list(stage_admission_review_pack["raw"].get("notes") or []),
        "not_real_acceptance_evidence": True,
    }
    manifest_sections["stage_admission_review_pack_reviewer_artifact"] = {
        "artifact_type": "stage_admission_review_pack_reviewer_artifact",
        "path": str(stage_admission_review_pack_reviewer_path),
        "available": bool(stage_admission_review_pack.get("available", False)),
        "summary_text": str(stage_admission_review_pack["display"].get("summary_text") or ""),
        "status_line": str(stage_admission_review_pack["display"].get("status_line") or ""),
        "current_stage_text": str(stage_admission_review_pack["display"].get("current_stage_text") or ""),
        "next_stage_text": str(stage_admission_review_pack["display"].get("next_stage_text") or ""),
        "engineering_isolation_text": str(
            stage_admission_review_pack["display"].get("engineering_isolation_text") or ""
        ),
        "real_acceptance_text": str(stage_admission_review_pack["display"].get("real_acceptance_text") or ""),
        "execute_now_text": str(stage_admission_review_pack["display"].get("execute_now_text") or ""),
        "defer_to_stage3_text": str(stage_admission_review_pack["display"].get("defer_to_stage3_text") or ""),
        "blocking_text": str(stage_admission_review_pack["display"].get("blocking_text") or ""),
        "warning_text": str(stage_admission_review_pack["display"].get("warning_text") or ""),
        "not_real_acceptance_evidence": True,
    }
    manifest_sections["engineering_isolation_admission_checklist"] = {
        "artifact_type": str(engineering_isolation_admission_checklist["raw"].get("artifact_type") or ""),
        "path": str(engineering_isolation_admission_checklist_path),
        "reviewer_path": str(engineering_isolation_admission_checklist_reviewer_path),
        "phase": engineering_isolation_admission_checklist["raw"].get("phase"),
        "overall_status": engineering_isolation_admission_checklist["raw"].get("overall_status"),
        "recommended_next_stage": engineering_isolation_admission_checklist["raw"].get("recommended_next_stage"),
        "ready_for_engineering_isolation": bool(
            engineering_isolation_admission_checklist["raw"].get("ready_for_engineering_isolation", False)
        ),
        "real_acceptance_ready": bool(
            engineering_isolation_admission_checklist["raw"].get("real_acceptance_ready", False)
        ),
        "checklist_items": list(engineering_isolation_admission_checklist["raw"].get("checklist_items") or []),
        "checklist_status_counts": dict(
            engineering_isolation_admission_checklist["raw"].get("checklist_status_counts") or {}
        ),
        "artifact_paths": dict(engineering_isolation_admission_checklist["raw"].get("artifact_paths") or {}),
        "blocking_items": list(engineering_isolation_admission_checklist["raw"].get("blocking_items") or []),
        "warning_items": list(engineering_isolation_admission_checklist["raw"].get("warning_items") or []),
        "missing_real_world_evidence": list(
            engineering_isolation_admission_checklist["raw"].get("missing_real_world_evidence") or []
        ),
        "defer_to_stage3_real_validation": list(
            engineering_isolation_admission_checklist["raw"].get("defer_to_stage3_real_validation") or []
        ),
        "notes": list(engineering_isolation_admission_checklist["raw"].get("notes") or []),
        "not_real_acceptance_evidence": True,
    }
    manifest_sections["engineering_isolation_admission_checklist_reviewer_artifact"] = {
        "artifact_type": "engineering_isolation_admission_checklist_reviewer_artifact",
        "path": str(engineering_isolation_admission_checklist_reviewer_path),
        "available": bool(engineering_isolation_admission_checklist.get("available", False)),
        "summary_text": str(engineering_isolation_admission_checklist["display"].get("summary_text") or ""),
        "status_line": str(engineering_isolation_admission_checklist["display"].get("status_line") or ""),
        "current_stage_text": str(
            engineering_isolation_admission_checklist["display"].get("current_stage_text") or ""
        ),
        "next_stage_text": str(
            engineering_isolation_admission_checklist["display"].get("next_stage_text") or ""
        ),
        "engineering_isolation_text": str(
            engineering_isolation_admission_checklist["display"].get("engineering_isolation_text") or ""
        ),
        "real_acceptance_text": str(
            engineering_isolation_admission_checklist["display"].get("real_acceptance_text") or ""
        ),
        "execute_now_text": str(
            engineering_isolation_admission_checklist["display"].get("execute_now_text") or ""
        ),
        "defer_to_stage3_text": str(
            engineering_isolation_admission_checklist["display"].get("defer_to_stage3_text") or ""
        ),
        "blocking_text": str(engineering_isolation_admission_checklist["display"].get("blocking_text") or ""),
        "warning_text": str(engineering_isolation_admission_checklist["display"].get("warning_text") or ""),
        "not_real_acceptance_evidence": True,
    }
    manifest_sections["stage3_real_validation_plan"] = {
        "artifact_type": str(stage3_real_validation_plan["raw"].get("artifact_type") or ""),
        "path": str(stage3_real_validation_plan_path),
        "reviewer_path": str(stage3_real_validation_plan_reviewer_path),
        "phase": stage3_real_validation_plan["raw"].get("phase"),
        "overall_status": stage3_real_validation_plan["raw"].get("overall_status"),
        "recommended_next_stage": stage3_real_validation_plan["raw"].get("recommended_next_stage"),
        "ready_for_engineering_isolation": bool(
            stage3_real_validation_plan["raw"].get("ready_for_engineering_isolation", False)
        ),
        "real_acceptance_ready": bool(
            stage3_real_validation_plan["raw"].get("real_acceptance_ready", False)
        ),
        "validation_items": list(stage3_real_validation_plan["raw"].get("validation_items") or []),
        "validation_status_counts": dict(stage3_real_validation_plan["raw"].get("validation_status_counts") or {}),
        "required_real_world_evidence": list(
            stage3_real_validation_plan["raw"].get("required_real_world_evidence") or []
        ),
        "pass_fail_contract": dict(stage3_real_validation_plan["raw"].get("pass_fail_contract") or {}),
        "artifact_paths": dict(stage3_real_validation_plan["raw"].get("artifact_paths") or {}),
        "blocking_items": list(stage3_real_validation_plan["raw"].get("blocking_items") or []),
        "warning_items": list(stage3_real_validation_plan["raw"].get("warning_items") or []),
        "notes": list(stage3_real_validation_plan["raw"].get("notes") or []),
        "not_real_acceptance_evidence": True,
    }
    manifest_sections["stage3_real_validation_plan_reviewer_artifact"] = {
        "artifact_type": "stage3_real_validation_plan_reviewer_artifact",
        "path": str(stage3_real_validation_plan_reviewer_path),
        "available": bool(stage3_real_validation_plan.get("available", False)),
        "summary_text": str(stage3_real_validation_plan["display"].get("summary_text") or ""),
        "status_line": str(stage3_real_validation_plan["display"].get("status_line") or ""),
        "current_stage_text": str(stage3_real_validation_plan["display"].get("current_stage_text") or ""),
        "next_stage_text": str(stage3_real_validation_plan["display"].get("next_stage_text") or ""),
        "engineering_isolation_text": str(
            stage3_real_validation_plan["display"].get("engineering_isolation_text") or ""
        ),
        "real_acceptance_text": str(stage3_real_validation_plan["display"].get("real_acceptance_text") or ""),
        "execute_now_text": str(stage3_real_validation_plan["display"].get("execute_now_text") or ""),
        "defer_to_stage3_text": str(stage3_real_validation_plan["display"].get("defer_to_stage3_text") or ""),
        "blocking_text": str(stage3_real_validation_plan["display"].get("blocking_text") or ""),
        "warning_text": str(stage3_real_validation_plan["display"].get("warning_text") or ""),
        "plan_boundary_text": str(stage3_real_validation_plan["display"].get("plan_boundary_text") or ""),
        "not_real_acceptance_evidence": True,
    }
    manifest_sections["stage3_standards_alignment_matrix"] = {
        "artifact_type": str(stage3_standards_alignment_matrix["raw"].get("artifact_type") or ""),
        "path": str(stage3_standards_alignment_matrix_path),
        "reviewer_path": str(stage3_standards_alignment_matrix_reviewer_path),
        "phase": stage3_standards_alignment_matrix["raw"].get("phase"),
        "overall_status": stage3_standards_alignment_matrix["raw"].get("overall_status"),
        "recommended_next_stage": stage3_standards_alignment_matrix["raw"].get("recommended_next_stage"),
        "mapping_scope": stage3_standards_alignment_matrix["raw"].get("mapping_scope"),
        "artifact_paths": dict(stage3_standards_alignment_matrix["raw"].get("artifact_paths") or {}),
        "standard_families": list(stage3_standards_alignment_matrix["raw"].get("standard_families") or []),
        "required_evidence_categories": list(
            stage3_standards_alignment_matrix["raw"].get("required_evidence_categories") or []
        ),
        "readiness_status_counts": dict(
            stage3_standards_alignment_matrix["raw"].get("readiness_status_counts") or {}
        ),
        "boundary_statements": list(stage3_standards_alignment_matrix["raw"].get("boundary_statements") or []),
        "rows": list(stage3_standards_alignment_matrix["raw"].get("rows") or []),
        "not_real_acceptance_evidence": True,
    }
    manifest_sections["stage3_standards_alignment_matrix_reviewer_artifact"] = {
        "artifact_type": "stage3_standards_alignment_matrix_reviewer_artifact",
        "path": str(stage3_standards_alignment_matrix_reviewer_path),
        "available": bool(stage3_standards_alignment_matrix.get("available", False)),
        "summary_text": str(stage3_standards_alignment_matrix["display"].get("summary_text") or ""),
        "reviewer_note_text": str(stage3_standards_alignment_matrix["display"].get("reviewer_note_text") or ""),
        "status_line": str(stage3_standards_alignment_matrix["display"].get("status_line") or ""),
        "current_stage_text": str(stage3_standards_alignment_matrix["display"].get("current_stage_text") or ""),
        "next_stage_text": str(stage3_standards_alignment_matrix["display"].get("next_stage_text") or ""),
        "engineering_isolation_text": str(
            stage3_standards_alignment_matrix["display"].get("engineering_isolation_text") or ""
        ),
        "real_acceptance_text": str(stage3_standards_alignment_matrix["display"].get("real_acceptance_text") or ""),
        "stage_bridge_text": str(stage3_standards_alignment_matrix["display"].get("stage_bridge_text") or ""),
        "artifact_role_text": str(stage3_standards_alignment_matrix["display"].get("artifact_role_text") or ""),
        "not_real_acceptance_evidence": True,
    }
    payload["manifest_sections"] = manifest_sections

    remembered_files = [str(item) for item in list(payload.get("remembered_files") or [])]
    readiness_path_text = str(readiness_path)
    if readiness_path_text not in remembered_files:
        remembered_files.append(readiness_path_text)
    metrology_path_text = str(metrology_path)
    if metrology_path_text not in remembered_files:
        remembered_files.append(metrology_path_text)
    phase_transition_path_text = str(phase_transition_path)
    if phase_transition_path_text not in remembered_files:
        remembered_files.append(phase_transition_path_text)
    phase_transition_reviewer_path_text = str(phase_transition_reviewer_path)
    if phase_transition_reviewer_path_text not in remembered_files:
        remembered_files.append(phase_transition_reviewer_path_text)
    stage_admission_review_pack_path_text = str(stage_admission_review_pack_path)
    if stage_admission_review_pack_path_text not in remembered_files:
        remembered_files.append(stage_admission_review_pack_path_text)
    stage_admission_review_pack_reviewer_path_text = str(stage_admission_review_pack_reviewer_path)
    if stage_admission_review_pack_reviewer_path_text not in remembered_files:
        remembered_files.append(stage_admission_review_pack_reviewer_path_text)
    engineering_isolation_admission_checklist_path_text = str(engineering_isolation_admission_checklist_path)
    if engineering_isolation_admission_checklist_path_text not in remembered_files:
        remembered_files.append(engineering_isolation_admission_checklist_path_text)
    engineering_isolation_admission_checklist_reviewer_path_text = str(
        engineering_isolation_admission_checklist_reviewer_path
    )
    if engineering_isolation_admission_checklist_reviewer_path_text not in remembered_files:
        remembered_files.append(engineering_isolation_admission_checklist_reviewer_path_text)
    stage3_real_validation_plan_path_text = str(stage3_real_validation_plan_path)
    if stage3_real_validation_plan_path_text not in remembered_files:
        remembered_files.append(stage3_real_validation_plan_path_text)
    stage3_real_validation_plan_reviewer_path_text = str(stage3_real_validation_plan_reviewer_path)
    if stage3_real_validation_plan_reviewer_path_text not in remembered_files:
        remembered_files.append(stage3_real_validation_plan_reviewer_path_text)
    stage3_standards_alignment_matrix_path_text = str(stage3_standards_alignment_matrix_path)
    if stage3_standards_alignment_matrix_path_text not in remembered_files:
        remembered_files.append(stage3_standards_alignment_matrix_path_text)
    stage3_standards_alignment_matrix_reviewer_path_text = str(stage3_standards_alignment_matrix_reviewer_path)
    if stage3_standards_alignment_matrix_reviewer_path_text not in remembered_files:
        remembered_files.append(stage3_standards_alignment_matrix_reviewer_path_text)
    payload["remembered_files"] = remembered_files
    _persist_governance_handoff_metadata(run_dir=run_dir, payload=payload)
    return payload


def _persist_governance_handoff_metadata(*, run_dir: Path, payload: dict[str, object]) -> None:
    summary_path = run_dir / "summary.json"
    manifest_path = run_dir / "manifest.json"
    summary = _load_json(summary_path)
    manifest = _load_json(manifest_path)

    stats = dict(summary.get("stats") or {})
    output_files = _merge_unique_text_list(
        list(stats.get("output_files") or []),
        list(payload.get("remembered_files") or []),
    )
    artifact_exports = dict(stats.get("artifact_exports") or {})
    artifact_exports.update(dict(payload.get("artifact_statuses") or {}))
    stats["output_files"] = output_files
    stats["artifact_exports"] = artifact_exports
    for key, value in dict(payload.get("summary_stats") or {}).items():
        stats[str(key)] = value
    summary["stats"] = stats
    write_json(summary_path, summary)

    for key, value in dict(payload.get("manifest_sections") or {}).items():
        manifest[str(key)] = value
    write_json(manifest_path, manifest)


def _merge_unique_text_list(existing: Iterable[object], incoming: Iterable[object]) -> list[str]:
    rows: list[str] = []
    for collection in (existing, incoming):
        for item in list(collection or []):
            text = str(item or "").strip()
            if text and text not in rows:
                rows.append(text)
    return rows


def rebuild_run(run_dir: Path) -> dict[str, object]:
    for name in ("summary.json", "manifest.json", "results.json"):
        if not (run_dir / name).exists():
            raise FileNotFoundError(
                f"{run_dir} is not a formal V2 run directory. Missing {name}. "
                "Use a run directory that contains summary.json, manifest.json, and results.json."
            )
    summary = _load_json(run_dir / "summary.json")
    manifest = _load_json(run_dir / "manifest.json")
    results = _load_json(run_dir / "results.json")
    session = SimpleNamespace(
        run_id=str(summary.get("run_id") or manifest.get("run_id") or run_dir.name),
        config=_objectify(dict(manifest.get("config_snapshot") or {})),
    )
    payload = export_run_offline_artifacts(
        run_dir=run_dir,
        output_dir=run_dir.parent,
        run_id=str(session.run_id),
        session=session,
        samples=[_objectify(item) for item in list(results.get("samples") or [])],
        point_summaries=[dict(item) for item in list(results.get("point_summaries") or [])],
        output_files=list((summary.get("stats") or {}).get("output_files") or []),
        export_statuses=dict((summary.get("stats") or {}).get("artifact_exports") or {}),
        source_points_file=manifest.get("source_points_file"),
        software_build_id=str(manifest.get("software_build_id") or summary.get("software_build_id") or ""),
        config_safety=dict(summary.get("config_safety") or (summary.get("stats") or {}).get("config_safety") or {}),
        config_safety_review=dict(
            summary.get("config_safety_review") or (summary.get("stats") or {}).get("config_safety_review") or {}
        ),
    )
    simulation_mode = bool(getattr(getattr(session.config, "features", None), "simulation_mode", False))
    return _augment_run_payload_with_step2_readiness(
        payload,
        run_dir=run_dir,
        run_id=str(session.run_id),
        simulation_mode=simulation_mode,
    )


def rebuild_suite(suite_dir: Path) -> dict[str, object]:
    if not (suite_dir / "suite_summary.json").exists():
        raise FileNotFoundError(
            f"{suite_dir} is not a suite directory. Missing suite_summary.json."
        )
    summary = _load_json(suite_dir / "suite_summary.json")
    return export_suite_offline_artifacts(suite_dir=suite_dir, summary=summary)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    if bool(args.run_dir) == bool(args.suite_dir):
        print("Provide exactly one of --run-dir or --suite-dir.", file=sys.stderr)
        return 2
    try:
        if args.run_dir:
            payload = rebuild_run(Path(str(args.run_dir)).resolve())
            print(f"acceptance_plan: {Path(args.run_dir).resolve() / 'acceptance_plan.json'}")
            print(f"analytics_summary: {Path(args.run_dir).resolve() / 'analytics_summary.json'}")
            print(f"step2_readiness_summary: {Path(args.run_dir).resolve() / STEP2_READINESS_SUMMARY_FILENAME}")
            print(f"metrology_calibration_contract: {Path(args.run_dir).resolve() / METROLOGY_CALIBRATION_CONTRACT_FILENAME}")
            print(f"phase_transition_bridge: {Path(args.run_dir).resolve() / PHASE_TRANSITION_BRIDGE_FILENAME}")
            print(f"phase_transition_bridge_reviewer: {Path(args.run_dir).resolve() / PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME}")
            print(f"stage_admission_review_pack: {Path(args.run_dir).resolve() / STAGE_ADMISSION_REVIEW_PACK_FILENAME}")
            print(
                "stage_admission_review_pack_reviewer: "
                f"{Path(args.run_dir).resolve() / STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME}"
            )
            print(
                "engineering_isolation_admission_checklist: "
                f"{Path(args.run_dir).resolve() / ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME}"
            )
            print(
                "engineering_isolation_admission_checklist_reviewer: "
                f"{Path(args.run_dir).resolve() / ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME}"
            )
            print(f"stage3_real_validation_plan: {Path(args.run_dir).resolve() / STAGE3_REAL_VALIDATION_PLAN_FILENAME}")
            print(
                "stage3_real_validation_plan_reviewer: "
                f"{Path(args.run_dir).resolve() / STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME}"
            )
            print(
                "stage3_standards_alignment_matrix: "
                f"{Path(args.run_dir).resolve() / STAGE3_STANDARDS_ALIGNMENT_MATRIX_FILENAME}"
            )
            print(
                "stage3_standards_alignment_matrix_reviewer: "
                f"{Path(args.run_dir).resolve() / STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_FILENAME}"
            )
            print(f"lineage_summary: {Path(args.run_dir).resolve() / 'lineage_summary.json'}")
            print(f"trend_registry: {Path(args.run_dir).resolve() / 'trend_registry.json'}")
            print(f"evidence_registry: {Path(args.run_dir).resolve() / 'evidence_registry.json'}")
            print(f"coefficient_registry: {Path(args.run_dir).resolve() / 'coefficient_registry.json'}")
            return 0 if payload else 1
        payload = rebuild_suite(Path(str(args.suite_dir)).resolve())
        print(f"suite_analytics_summary: {Path(args.suite_dir).resolve() / 'suite_analytics_summary.json'}")
        print(f"suite_acceptance_plan: {Path(args.suite_dir).resolve() / 'suite_acceptance_plan.json'}")
        print(f"suite_evidence_registry: {Path(args.suite_dir).resolve() / 'suite_evidence_registry.json'}")
        return 0 if payload else 1
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
