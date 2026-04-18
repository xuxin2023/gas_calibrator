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
from ..core.engineering_isolation_gate_evaluator import (
    ENGINEERING_ISOLATION_BLOCKERS_FILENAME,
    ENGINEERING_ISOLATION_GATE_DIGEST_FILENAME,
    ENGINEERING_ISOLATION_GATE_RESULT_FILENAME,
    ENGINEERING_ISOLATION_WARNINGS_FILENAME,
    build_engineering_isolation_gate_evaluator,
)
from ..core.human_governance_artifacts import (
    OPERATOR_AUTHORIZATION_PROFILE_FILENAME,
    QC_FLAG_CATALOG_FILENAME,
    RECOVERY_ACTION_LOG_FILENAME,
    REVIEWER_DUAL_CHECK_PLACEHOLDER_FILENAME,
    RUN_METADATA_PROFILE_FILENAME,
    SOP_VERSION_BINDING_FILENAME,
    TRAINING_RECORD_FILENAME,
    build_human_governance_artifacts,
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
from ..core.step2_closeout_bundle_builder import (
    STEP2_CLOSEOUT_BUNDLE_FILENAME,
    STEP2_CLOSEOUT_EVIDENCE_INDEX_FILENAME,
    STEP2_CLOSEOUT_SUMMARY_FILENAME,
    build_step2_closeout_bundle,
)
from ..core.step2_reviewer_readiness_artifacts import (
    AI_RUN_SUMMARY_FILENAME,
    EVIDENCE_COVERAGE_MATRIX_FILENAME,
    EVIDENCE_LINEAGE_INDEX_FILENAME,
    RESULT_TRACEABILITY_TREE_FILENAME,
    REVIEWER_ANCHOR_NAVIGATION_FILENAME,
    STEP2_CLOSEOUT_DIGEST_FILENAME,
    build_step2_reviewer_readiness_artifacts,
)
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
    supplemental_paths = {
        "reference_asset_registry": run_dir / "reference_asset_registry.json",
        "certificate_lifecycle_summary": run_dir / "certificate_lifecycle_summary.json",
        "pre_run_readiness_gate": run_dir / "pre_run_readiness_gate.json",
        "uncertainty_report_pack": run_dir / "uncertainty_report_pack.json",
        "uncertainty_rollup": run_dir / "uncertainty_rollup.json",
        "uncertainty_method_readiness_summary": run_dir / "uncertainty_method_readiness_summary.json",
        "method_confirmation_protocol": run_dir / "method_confirmation_protocol.json",
        "verification_rollup": run_dir / "verification_rollup.json",
        "software_validation_traceability_matrix": run_dir / "software_validation_traceability_matrix.json",
        "requirement_design_code_test_links": run_dir / "requirement_design_code_test_links.json",
        "validation_evidence_index": run_dir / "validation_evidence_index.json",
        "software_validation_rollup": run_dir / "software_validation_rollup.json",
        "audit_readiness_digest": run_dir / "audit_readiness_digest.json",
        "pt_ilc_registry": run_dir / "pt_ilc_registry.json",
        "comparison_evidence_pack": run_dir / "comparison_evidence_pack.json",
        "scope_comparison_view": run_dir / "scope_comparison_view.json",
        "comparison_digest": run_dir / "comparison_digest.json",
        "comparison_rollup": run_dir / "comparison_rollup.json",
        "sidecar_index_summary": run_dir / "sidecar_index_summary.json",
        "review_copilot_payload": run_dir / "review_copilot_payload.json",
        "model_governance_summary": run_dir / "model_governance_summary.json",
    }
    supplemental_payloads = {
        key: (_load_json(path) if path.exists() else {})
        for key, path in supplemental_paths.items()
    }
    summary_payload = _load_json(run_dir / "summary.json")
    manifest_payload = _load_json(run_dir / "manifest.json")
    results_payload = _load_json(run_dir / "results.json")
    acceptance_plan_payload = (
        _load_json(run_dir / "acceptance_plan.json")
        if (run_dir / "acceptance_plan.json").exists()
        else {}
    )
    workbench_action_report_payload = (
        _load_json(run_dir / "workbench_action_report.json")
        if (run_dir / "workbench_action_report.json").exists()
        else {}
    )

    human_governance_payloads = build_human_governance_artifacts(
        run_id=run_id,
        run_dir=run_dir,
        summary=summary_payload,
        manifest=manifest_payload,
        acceptance_plan=acceptance_plan_payload,
        workbench_action_report=workbench_action_report_payload,
    )
    human_governance_files = {
        "run_metadata_profile": RUN_METADATA_PROFILE_FILENAME,
        "operator_authorization_profile": OPERATOR_AUTHORIZATION_PROFILE_FILENAME,
        "training_record": TRAINING_RECORD_FILENAME,
        "sop_version_binding": SOP_VERSION_BINDING_FILENAME,
        "qc_flag_catalog": QC_FLAG_CATALOG_FILENAME,
        "recovery_action_log": RECOVERY_ACTION_LOG_FILENAME,
        "reviewer_dual_check_placeholder": REVIEWER_DUAL_CHECK_PLACEHOLDER_FILENAME,
    }
    human_governance_paths = {
        key: write_json(run_dir / filename, dict(human_governance_payloads.get(key) or {}))
        for key, filename in human_governance_files.items()
    }

    ai_run_summary_path = run_dir / AI_RUN_SUMMARY_FILENAME
    existing_ai_run_summary_text = (
        ai_run_summary_path.read_text(encoding="utf-8")
        if ai_run_summary_path.exists()
        else ""
    )
    step2_reviewer_readiness_payloads = build_step2_reviewer_readiness_artifacts(
        run_id=run_id,
        run_dir=run_dir,
        summary=summary_payload,
        manifest=manifest_payload,
        results=results_payload,
        scope_definition_pack=scope_definition_pack,
        decision_rule_profile=decision_rule_profile,
        conformity_statement_profile=dict(decision_rule_profile.get("conformity_statement_profile") or {}),
        reference_asset_registry=dict(supplemental_payloads.get("reference_asset_registry") or {}),
        certificate_lifecycle_summary=dict(supplemental_payloads.get("certificate_lifecycle_summary") or {}),
        uncertainty_report_pack=dict(supplemental_payloads.get("uncertainty_report_pack") or {}),
        uncertainty_rollup=dict(supplemental_payloads.get("uncertainty_rollup") or {}),
        method_confirmation_protocol=dict(supplemental_payloads.get("method_confirmation_protocol") or {}),
        verification_rollup=dict(supplemental_payloads.get("verification_rollup") or {}),
        software_validation_traceability_matrix=dict(
            supplemental_payloads.get("software_validation_traceability_matrix") or {}
        ),
        release_manifest=dict(supplemental_payloads.get("release_manifest") or {}),
        comparison_evidence_pack=dict(supplemental_payloads.get("comparison_evidence_pack") or {}),
        comparison_rollup=dict(supplemental_payloads.get("comparison_rollup") or {}),
        stage3_standards_alignment_matrix=dict(stage3_standards_alignment_matrix.get("raw") or {}),
        run_metadata_profile=dict(human_governance_payloads.get("run_metadata_profile") or {}),
        operator_authorization_profile=dict(human_governance_payloads.get("operator_authorization_profile") or {}),
        training_record=dict(human_governance_payloads.get("training_record") or {}),
        sop_version_binding=dict(human_governance_payloads.get("sop_version_binding") or {}),
        qc_flag_catalog=dict(human_governance_payloads.get("qc_flag_catalog") or {}),
        recovery_action_log=dict(human_governance_payloads.get("recovery_action_log") or {}),
        reviewer_dual_check_placeholder=dict(
            human_governance_payloads.get("reviewer_dual_check_placeholder") or {}
        ),
        sidecar_index_summary=dict(supplemental_payloads.get("sidecar_index_summary") or {}),
        review_copilot_payload=dict(supplemental_payloads.get("review_copilot_payload") or {}),
        model_governance_summary=dict(supplemental_payloads.get("model_governance_summary") or {}),
        existing_ai_run_summary_text=existing_ai_run_summary_text,
    )
    step2_reviewer_paths = {
        "step2_closeout_digest": write_json(
            run_dir / STEP2_CLOSEOUT_DIGEST_FILENAME,
            dict(step2_reviewer_readiness_payloads.get("step2_closeout_digest") or {}),
        ),
        "evidence_coverage_matrix": write_json(
            run_dir / EVIDENCE_COVERAGE_MATRIX_FILENAME,
            dict(step2_reviewer_readiness_payloads.get("evidence_coverage_matrix") or {}),
        ),
        "result_traceability_tree": write_json(
            run_dir / RESULT_TRACEABILITY_TREE_FILENAME,
            dict(step2_reviewer_readiness_payloads.get("result_traceability_tree") or {}),
        ),
        "evidence_lineage_index": write_json(
            run_dir / EVIDENCE_LINEAGE_INDEX_FILENAME,
            dict(step2_reviewer_readiness_payloads.get("evidence_lineage_index") or {}),
        ),
        "reviewer_anchor_navigation": write_json(
            run_dir / REVIEWER_ANCHOR_NAVIGATION_FILENAME,
            dict(step2_reviewer_readiness_payloads.get("reviewer_anchor_navigation") or {}),
        ),
    }
    ai_run_summary_path.write_text(
        str(step2_reviewer_readiness_payloads.get("ai_run_summary_markdown") or ""),
        encoding="utf-8",
    )
    step2_reviewer_paths["ai_run_summary"] = ai_run_summary_path

    step2_closeout_bundle_payloads = build_step2_closeout_bundle(
        run_id=run_id,
        run_dir=run_dir,
        scope_definition_pack=scope_definition_pack,
        decision_rule_profile=decision_rule_profile,
        conformity_statement_profile=dict(decision_rule_profile.get("conformity_statement_profile") or {}),
        reference_asset_registry=dict(supplemental_payloads.get("reference_asset_registry") or {}),
        certificate_lifecycle_summary=dict(supplemental_payloads.get("certificate_lifecycle_summary") or {}),
        pre_run_readiness_gate=dict(supplemental_payloads.get("pre_run_readiness_gate") or {}),
        uncertainty_report_pack=dict(supplemental_payloads.get("uncertainty_report_pack") or {}),
        uncertainty_rollup=dict(supplemental_payloads.get("uncertainty_rollup") or {}),
        method_confirmation_protocol=dict(supplemental_payloads.get("method_confirmation_protocol") or {}),
        verification_rollup=dict(supplemental_payloads.get("verification_rollup") or {}),
        software_validation_traceability_matrix=dict(
            supplemental_payloads.get("software_validation_traceability_matrix") or {}
        ),
        requirement_design_code_test_links=dict(
            supplemental_payloads.get("requirement_design_code_test_links") or {}
        ),
        validation_evidence_index=dict(supplemental_payloads.get("validation_evidence_index") or {}),
        change_impact_summary=dict(supplemental_payloads.get("change_impact_summary") or {}),
        rollback_readiness_summary=dict(supplemental_payloads.get("rollback_readiness_summary") or {}),
        release_manifest=dict(supplemental_payloads.get("release_manifest") or {}),
        release_scope_summary=dict(supplemental_payloads.get("release_scope_summary") or {}),
        release_boundary_digest=dict(supplemental_payloads.get("release_boundary_digest") or {}),
        release_evidence_pack_index=dict(supplemental_payloads.get("release_evidence_pack_index") or {}),
        release_validation_manifest=dict(supplemental_payloads.get("release_validation_manifest") or {}),
        software_validation_rollup=dict(supplemental_payloads.get("software_validation_rollup") or {}),
        audit_readiness_digest=dict(supplemental_payloads.get("audit_readiness_digest") or {}),
        comparison_evidence_pack=dict(supplemental_payloads.get("comparison_evidence_pack") or {}),
        scope_comparison_view=dict(supplemental_payloads.get("scope_comparison_view") or {}),
        comparison_digest=dict(supplemental_payloads.get("comparison_digest") or {}),
        comparison_rollup=dict(supplemental_payloads.get("comparison_rollup") or {}),
        step2_closeout_digest=dict(step2_reviewer_readiness_payloads.get("step2_closeout_digest") or {}),
        evidence_coverage_matrix=dict(step2_reviewer_readiness_payloads.get("evidence_coverage_matrix") or {}),
        result_traceability_tree=dict(step2_reviewer_readiness_payloads.get("result_traceability_tree") or {}),
        evidence_lineage_index=dict(step2_reviewer_readiness_payloads.get("evidence_lineage_index") or {}),
        reviewer_anchor_navigation=dict(step2_reviewer_readiness_payloads.get("reviewer_anchor_navigation") or {}),
        sidecar_index_summary=dict(supplemental_payloads.get("sidecar_index_summary") or {}),
        review_copilot_payload=dict(supplemental_payloads.get("review_copilot_payload") or {}),
        model_governance_summary=dict(supplemental_payloads.get("model_governance_summary") or {}),
        ai_run_summary_payload=dict(step2_reviewer_readiness_payloads.get("ai_run_summary_payload") or {}),
        run_metadata_profile=dict(human_governance_payloads.get("run_metadata_profile") or {}),
        operator_authorization_profile=dict(human_governance_payloads.get("operator_authorization_profile") or {}),
        training_record=dict(human_governance_payloads.get("training_record") or {}),
        sop_version_binding=dict(human_governance_payloads.get("sop_version_binding") or {}),
        qc_flag_catalog=dict(human_governance_payloads.get("qc_flag_catalog") or {}),
        recovery_action_log=dict(human_governance_payloads.get("recovery_action_log") or {}),
        reviewer_dual_check_placeholder=dict(
            human_governance_payloads.get("reviewer_dual_check_placeholder") or {}
        ),
    )
    step2_closeout_bundle_path = write_json(
        run_dir / STEP2_CLOSEOUT_BUNDLE_FILENAME,
        dict(step2_closeout_bundle_payloads.get("step2_closeout_bundle") or {}),
    )
    step2_closeout_evidence_index_path = write_json(
        run_dir / STEP2_CLOSEOUT_EVIDENCE_INDEX_FILENAME,
        dict(step2_closeout_bundle_payloads.get("step2_closeout_evidence_index") or {}),
    )
    step2_closeout_summary_path = run_dir / STEP2_CLOSEOUT_SUMMARY_FILENAME
    step2_closeout_summary_path.write_text(
        str(step2_closeout_bundle_payloads.get("step2_closeout_summary_markdown") or ""),
        encoding="utf-8",
    )
    recognition_binding = _build_recognition_binding(
        scope_definition_pack=scope_definition_pack,
        decision_rule_profile=decision_rule_profile,
        conformity_statement_profile=dict(decision_rule_profile.get("conformity_statement_profile") or {}),
        step2_closeout_bundle=dict(step2_closeout_bundle_payloads.get("step2_closeout_bundle") or {}),
        step2_closeout_digest=dict(step2_reviewer_readiness_payloads.get("step2_closeout_digest") or {}),
    )
    uncertainty_binding = _build_uncertainty_binding(
        recognition_binding=recognition_binding,
        uncertainty_report_pack=dict(supplemental_payloads.get("uncertainty_report_pack") or {}),
        uncertainty_rollup=dict(supplemental_payloads.get("uncertainty_rollup") or {}),
        method_confirmation_protocol=dict(supplemental_payloads.get("method_confirmation_protocol") or {}),
        verification_rollup=dict(supplemental_payloads.get("verification_rollup") or {}),
        step2_closeout_bundle=dict(step2_closeout_bundle_payloads.get("step2_closeout_bundle") or {}),
    )

    engineering_isolation_gate = build_engineering_isolation_gate_evaluator(
        run_id=run_id,
        run_dir=run_dir,
        stage_admission_review_pack=dict(stage_admission_review_pack.get("raw") or {}),
        engineering_isolation_admission_checklist=dict(
            engineering_isolation_admission_checklist.get("raw") or {}
        ),
        pre_run_readiness_gate=dict(supplemental_payloads.get("pre_run_readiness_gate") or {}),
        step2_closeout_bundle=dict(step2_closeout_bundle_payloads.get("step2_closeout_bundle") or {}),
        step2_closeout_compact_section=dict(
            step2_closeout_bundle_payloads.get("step2_closeout_compact_section") or {}
        ),
        stage3_standards_alignment_matrix=dict(stage3_standards_alignment_matrix.get("raw") or {}),
        stage3_real_validation_plan=dict(stage3_real_validation_plan.get("raw") or {}),
        scope_definition_pack=scope_definition_pack,
        decision_rule_profile=decision_rule_profile,
        conformity_statement_profile=dict(decision_rule_profile.get("conformity_statement_profile") or {}),
        reference_asset_registry=dict(supplemental_payloads.get("reference_asset_registry") or {}),
        certificate_lifecycle_summary=dict(supplemental_payloads.get("certificate_lifecycle_summary") or {}),
        uncertainty_report_pack=dict(supplemental_payloads.get("uncertainty_report_pack") or {}),
        uncertainty_rollup=dict(supplemental_payloads.get("uncertainty_rollup") or {}),
        uncertainty_method_readiness_summary=dict(
            supplemental_payloads.get("uncertainty_method_readiness_summary") or {}
        ),
        method_confirmation_protocol=dict(supplemental_payloads.get("method_confirmation_protocol") or {}),
        verification_rollup=dict(supplemental_payloads.get("verification_rollup") or {}),
        software_validation_traceability_matrix=dict(
            supplemental_payloads.get("software_validation_traceability_matrix") or {}
        ),
        requirement_design_code_test_links=dict(
            supplemental_payloads.get("requirement_design_code_test_links") or {}
        ),
        validation_evidence_index=dict(supplemental_payloads.get("validation_evidence_index") or {}),
        software_validation_rollup=dict(supplemental_payloads.get("software_validation_rollup") or {}),
        audit_readiness_digest=dict(supplemental_payloads.get("audit_readiness_digest") or {}),
        pt_ilc_registry=dict(supplemental_payloads.get("pt_ilc_registry") or {}),
        comparison_evidence_pack=dict(supplemental_payloads.get("comparison_evidence_pack") or {}),
        scope_comparison_view=dict(supplemental_payloads.get("scope_comparison_view") or {}),
        comparison_digest=dict(supplemental_payloads.get("comparison_digest") or {}),
        comparison_rollup=dict(supplemental_payloads.get("comparison_rollup") or {}),
        sidecar_index_summary=dict(supplemental_payloads.get("sidecar_index_summary") or {}),
        review_copilot_payload=dict(supplemental_payloads.get("review_copilot_payload") or {}),
        model_governance_summary=dict(supplemental_payloads.get("model_governance_summary") or {}),
        run_metadata_profile=dict(human_governance_payloads.get("run_metadata_profile") or {}),
        operator_authorization_profile=dict(human_governance_payloads.get("operator_authorization_profile") or {}),
        training_record=dict(human_governance_payloads.get("training_record") or {}),
        sop_version_binding=dict(human_governance_payloads.get("sop_version_binding") or {}),
        qc_flag_catalog=dict(human_governance_payloads.get("qc_flag_catalog") or {}),
        recovery_action_log=dict(human_governance_payloads.get("recovery_action_log") or {}),
        reviewer_dual_check_placeholder=dict(
            human_governance_payloads.get("reviewer_dual_check_placeholder") or {}
        ),
    )
    engineering_isolation_gate_result = dict(
        engineering_isolation_gate.get("engineering_isolation_gate_result") or {}
    )
    engineering_isolation_blockers = dict(
        engineering_isolation_gate.get("engineering_isolation_blockers") or {}
    )
    engineering_isolation_warnings = dict(
        engineering_isolation_gate.get("engineering_isolation_warnings") or {}
    )
    engineering_isolation_gate_digest_path = run_dir / ENGINEERING_ISOLATION_GATE_DIGEST_FILENAME
    engineering_isolation_gate_result_path = write_json(
        run_dir / ENGINEERING_ISOLATION_GATE_RESULT_FILENAME,
        engineering_isolation_gate_result,
    )
    engineering_isolation_gate_digest_path.write_text(
        str(engineering_isolation_gate.get("engineering_isolation_gate_digest_markdown") or ""),
        encoding="utf-8",
    )
    engineering_isolation_blockers_path = write_json(
        run_dir / ENGINEERING_ISOLATION_BLOCKERS_FILENAME,
        engineering_isolation_blockers,
    )
    engineering_isolation_warnings_path = write_json(
        run_dir / ENGINEERING_ISOLATION_WARNINGS_FILENAME,
        engineering_isolation_warnings,
    )
    analytics_summary["engineering_isolation_gate_result"] = engineering_isolation_gate_result
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
    summary_stats["engineering_isolation_gate_result"] = dict(engineering_isolation_gate_result)
    summary_stats["engineering_isolation_gate_digest"] = {
        "phase": engineering_isolation_gate_result.get("phase"),
        "overall_status": engineering_isolation_gate_result.get("overall_status"),
        "gate_level": engineering_isolation_gate_result.get("gate_level"),
        "gate_level_display": engineering_isolation_gate_result.get("gate_level_display"),
        "blocker_count": int(engineering_isolation_gate_result.get("blocker_count") or 0),
        "warning_count": int(engineering_isolation_gate_result.get("warning_count") or 0),
        "formal_gap_count": int(engineering_isolation_gate_result.get("formal_gap_count") or 0),
        "required_evidence_categories": list(
            dict(engineering_isolation_gate_result.get("stage3_real_validation_plan_draft_input") or {}).get(
                "required_evidence_categories"
            )
            or []
        ),
        "missing_prerequisites": list(
            dict(engineering_isolation_gate_result.get("stage3_real_validation_plan_draft_input") or {}).get(
                "missing_prerequisites"
            )
            or []
        ),
        "artifact_paths": dict(engineering_isolation_gate_result.get("artifact_paths") or {}),
    }
    summary_stats["recognition_binding"] = dict(recognition_binding)
    summary_stats["uncertainty_binding"] = dict(uncertainty_binding)
    summary_stats["step2_closeout_digest"] = dict(step2_reviewer_readiness_payloads.get("step2_closeout_digest") or {})
    summary_stats["evidence_coverage_matrix"] = dict(
        step2_reviewer_readiness_payloads.get("evidence_coverage_matrix") or {}
    )
    summary_stats["result_traceability_tree"] = dict(
        step2_reviewer_readiness_payloads.get("result_traceability_tree") or {}
    )
    summary_stats["evidence_lineage_index"] = dict(
        step2_reviewer_readiness_payloads.get("evidence_lineage_index") or {}
    )
    summary_stats["reviewer_anchor_navigation"] = dict(
        step2_reviewer_readiness_payloads.get("reviewer_anchor_navigation") or {}
    )
    summary_stats["ai_run_summary_payload"] = dict(
        step2_reviewer_readiness_payloads.get("ai_run_summary_payload") or {}
    )
    summary_stats["run_metadata_profile"] = dict(human_governance_payloads.get("run_metadata_profile") or {})
    summary_stats["operator_authorization_profile"] = dict(
        human_governance_payloads.get("operator_authorization_profile") or {}
    )
    summary_stats["training_record"] = dict(human_governance_payloads.get("training_record") or {})
    summary_stats["sop_version_binding"] = dict(human_governance_payloads.get("sop_version_binding") or {})
    summary_stats["qc_flag_catalog"] = dict(human_governance_payloads.get("qc_flag_catalog") or {})
    summary_stats["recovery_action_log"] = dict(human_governance_payloads.get("recovery_action_log") or {})
    summary_stats["reviewer_dual_check_placeholder"] = dict(
        human_governance_payloads.get("reviewer_dual_check_placeholder") or {}
    )
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
    artifact_statuses["engineering_isolation_gate_result"] = {
        "status": "ok",
        "role": "execution_summary",
        "path": str(engineering_isolation_gate_result_path),
    }
    artifact_statuses["engineering_isolation_gate_digest"] = {
        "status": "ok",
        "role": "formal_analysis",
        "path": str(engineering_isolation_gate_digest_path),
    }
    artifact_statuses["engineering_isolation_blockers"] = {
        "status": "ok",
        "role": "diagnostic_analysis",
        "path": str(engineering_isolation_blockers_path),
    }
    artifact_statuses["engineering_isolation_warnings"] = {
        "status": "ok",
        "role": "diagnostic_analysis",
        "path": str(engineering_isolation_warnings_path),
    }
    for artifact_key, artifact_path in human_governance_paths.items():
        artifact_statuses[str(artifact_key)] = {
            "status": "ok",
            "role": "diagnostic_analysis",
            "path": str(artifact_path),
        }
    for artifact_key, artifact_path in step2_reviewer_paths.items():
        artifact_statuses[str(artifact_key)] = {
            "status": "ok",
            "role": "diagnostic_analysis",
            "path": str(artifact_path),
        }
    artifact_statuses["step2_closeout_bundle"] = {
        "status": "ok",
        "role": "diagnostic_analysis",
        "path": str(step2_closeout_bundle_path),
    }
    artifact_statuses["step2_closeout_evidence_index"] = {
        "status": "ok",
        "role": "diagnostic_analysis",
        "path": str(step2_closeout_evidence_index_path),
    }
    artifact_statuses["step2_closeout_summary_markdown"] = {
        "status": "ok",
        "role": "formal_analysis",
        "path": str(step2_closeout_summary_path),
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
    manifest_sections["engineering_isolation_gate_result"] = {
        "artifact_type": str(engineering_isolation_gate_result.get("artifact_type") or ""),
        "path": str(engineering_isolation_gate_result_path),
        "reviewer_path": str(engineering_isolation_gate_digest_path),
        "phase": engineering_isolation_gate_result.get("phase"),
        "overall_status": engineering_isolation_gate_result.get("overall_status"),
        "gate_level": engineering_isolation_gate_result.get("gate_level"),
        "gate_level_display": engineering_isolation_gate_result.get("gate_level_display"),
        "summary_text": str(
            dict(engineering_isolation_gate_result.get("review_surface") or {}).get("summary_text") or ""
        ),
        "status_line": str(
            dict(engineering_isolation_gate_result.get("review_surface") or {}).get("status_line") or ""
        ),
        "bridge_note_text": str(engineering_isolation_gate_result.get("note") or ""),
        "blocker_lines": list(
            dict(engineering_isolation_gate_result.get("review_surface") or {}).get("blocker_lines") or []
        ),
        "warning_lines": list(
            dict(engineering_isolation_gate_result.get("review_surface") or {}).get("warning_lines") or []
        ),
        "unresolved_gap_lines": list(
            dict(engineering_isolation_gate_result.get("review_surface") or {}).get("unresolved_gap_lines") or []
        ),
        "suggested_next_action_lines": list(
            dict(engineering_isolation_gate_result.get("review_surface") or {}).get("suggested_next_action_lines")
            or []
        ),
        "required_evidence_categories": list(
            dict(engineering_isolation_gate_result.get("stage3_real_validation_plan_draft_input") or {}).get(
                "required_evidence_categories"
            )
            or []
        ),
        "standard_families": list(
            dict(stage3_standards_alignment_matrix.get("raw") or {}).get("standard_families") or []
        ),
        "boundary_statements": list(engineering_isolation_gate_result.get("boundary_statements") or []),
        "artifact_paths": dict(engineering_isolation_gate_result.get("artifact_paths") or {}),
        "not_real_acceptance_evidence": True,
    }
    manifest_sections["engineering_isolation_gate_digest"] = {
        "artifact_type": "engineering_isolation_gate_digest",
        "path": str(engineering_isolation_gate_digest_path),
        "available": True,
        "title_text": str(
            dict(engineering_isolation_gate_result.get("review_surface") or {}).get("title_text") or ""
        ),
        "summary_text": str(
            dict(engineering_isolation_gate_result.get("review_surface") or {}).get("summary_text") or ""
        ),
        "status_line": str(
            dict(engineering_isolation_gate_result.get("review_surface") or {}).get("status_line") or ""
        ),
        "bridge_note_text": str(engineering_isolation_gate_result.get("note") or ""),
        "blocker_lines": list(
            dict(engineering_isolation_gate_result.get("review_surface") or {}).get("blocker_lines") or []
        ),
        "warning_lines": list(
            dict(engineering_isolation_gate_result.get("review_surface") or {}).get("warning_lines") or []
        ),
        "unresolved_gap_lines": list(
            dict(engineering_isolation_gate_result.get("review_surface") or {}).get("unresolved_gap_lines") or []
        ),
        "suggested_next_action_lines": list(
            dict(engineering_isolation_gate_result.get("review_surface") or {}).get("suggested_next_action_lines")
            or []
        ),
        "boundary_statements": list(engineering_isolation_gate_result.get("boundary_statements") or []),
        "not_real_acceptance_evidence": True,
    }
    manifest_sections["engineering_isolation_blockers"] = {
        "artifact_type": str(engineering_isolation_blockers.get("artifact_type") or ""),
        "path": str(engineering_isolation_blockers_path),
        "count": int(engineering_isolation_blockers.get("count") or 0),
        "items": list(engineering_isolation_blockers.get("items") or []),
        "not_real_acceptance_evidence": True,
    }
    manifest_sections["engineering_isolation_warnings"] = {
        "artifact_type": str(engineering_isolation_warnings.get("artifact_type") or ""),
        "path": str(engineering_isolation_warnings_path),
        "count": int(engineering_isolation_warnings.get("count") or 0),
        "items": list(engineering_isolation_warnings.get("items") or []),
        "not_real_acceptance_evidence": True,
    }
    for artifact_key, artifact_path in human_governance_paths.items():
        manifest_sections[str(artifact_key)] = _build_manifest_surface_section(
            artifact_key=str(artifact_key),
            path=artifact_path,
            payload=dict(human_governance_payloads.get(str(artifact_key)) or {}),
        )
    for artifact_key, artifact_path in step2_reviewer_paths.items():
        manifest_sections[str(artifact_key)] = _build_manifest_surface_section(
            artifact_key=str(artifact_key),
            path=artifact_path,
            payload=(
                dict(step2_reviewer_readiness_payloads.get(str(artifact_key)) or {})
                if str(artifact_key) != "ai_run_summary"
                else dict(step2_reviewer_readiness_payloads.get("ai_run_summary_payload") or {})
            ),
            extra_fields=(
                {"markdown_available": True}
                if str(artifact_key) == "ai_run_summary"
                else None
            ),
        )
    manifest_sections["step2_closeout_bundle"] = _build_manifest_surface_section(
        artifact_key="step2_closeout_bundle",
        path=step2_closeout_bundle_path,
        payload=dict(step2_closeout_bundle_payloads.get("step2_closeout_bundle") or {}),
    )
    manifest_sections["step2_closeout_evidence_index"] = _build_manifest_surface_section(
        artifact_key="step2_closeout_evidence_index",
        path=step2_closeout_evidence_index_path,
        payload=dict(step2_closeout_bundle_payloads.get("step2_closeout_evidence_index") or {}),
    )
    manifest_sections["step2_closeout_summary_markdown"] = {
        "artifact_type": "step2_closeout_summary_markdown",
        "path": str(step2_closeout_summary_path),
        "summary_text": str(
            dict(step2_closeout_bundle_payloads.get("step2_closeout_bundle") or {}).get("summary_line") or ""
        ),
        "markdown_available": True,
        "reviewer_only": True,
        "readiness_mapping_only": True,
        "not_real_acceptance_evidence": True,
        "not_ready_for_formal_claim": True,
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
    engineering_isolation_gate_result_path_text = str(engineering_isolation_gate_result_path)
    if engineering_isolation_gate_result_path_text not in remembered_files:
        remembered_files.append(engineering_isolation_gate_result_path_text)
    engineering_isolation_gate_digest_path_text = str(engineering_isolation_gate_digest_path)
    if engineering_isolation_gate_digest_path_text not in remembered_files:
        remembered_files.append(engineering_isolation_gate_digest_path_text)
    engineering_isolation_blockers_path_text = str(engineering_isolation_blockers_path)
    if engineering_isolation_blockers_path_text not in remembered_files:
        remembered_files.append(engineering_isolation_blockers_path_text)
    engineering_isolation_warnings_path_text = str(engineering_isolation_warnings_path)
    if engineering_isolation_warnings_path_text not in remembered_files:
        remembered_files.append(engineering_isolation_warnings_path_text)
    for artifact_path in list(human_governance_paths.values()) + list(step2_reviewer_paths.values()):
        artifact_path_text = str(artifact_path)
        if artifact_path_text not in remembered_files:
            remembered_files.append(artifact_path_text)
    step2_closeout_bundle_path_text = str(step2_closeout_bundle_path)
    if step2_closeout_bundle_path_text not in remembered_files:
        remembered_files.append(step2_closeout_bundle_path_text)
    step2_closeout_evidence_index_path_text = str(step2_closeout_evidence_index_path)
    if step2_closeout_evidence_index_path_text not in remembered_files:
        remembered_files.append(step2_closeout_evidence_index_path_text)
    step2_closeout_summary_path_text = str(step2_closeout_summary_path)
    if step2_closeout_summary_path_text not in remembered_files:
        remembered_files.append(step2_closeout_summary_path_text)
    payload["remembered_files"] = remembered_files
    _persist_governance_handoff_metadata(run_dir=run_dir, payload=payload)
    return payload


def _persist_governance_handoff_metadata(*, run_dir: Path, payload: dict[str, object]) -> None:
    summary_path = run_dir / "summary.json"
    manifest_path = run_dir / "manifest.json"
    results_path = run_dir / "results.json"
    summary = _load_json(summary_path)
    manifest = _load_json(manifest_path)
    results = _load_json(results_path)
    summary_stats = dict(payload.get("summary_stats") or {})

    stats = dict(summary.get("stats") or {})
    output_files = _merge_unique_text_list(
        list(stats.get("output_files") or []),
        list(payload.get("remembered_files") or []),
    )
    artifact_exports = dict(stats.get("artifact_exports") or {})
    artifact_exports.update(dict(payload.get("artifact_statuses") or {}))
    stats["output_files"] = output_files
    stats["artifact_exports"] = artifact_exports
    for key, value in summary_stats.items():
        stats[str(key)] = value
    summary["stats"] = stats
    recognition_binding = dict(summary_stats.get("recognition_binding") or {})
    uncertainty_binding = dict(summary_stats.get("uncertainty_binding") or {})
    if recognition_binding:
        summary["recognition_binding"] = dict(recognition_binding)
        summary["scope_id"] = str(recognition_binding.get("scope_id") or summary.get("scope_id") or "")
        summary["decision_rule_id"] = str(
            recognition_binding.get("decision_rule_id") or summary.get("decision_rule_id") or ""
        )
        summary["limitation_note"] = str(
            recognition_binding.get("limitation_note") or summary.get("limitation_note") or ""
        )
        summary["non_claim_note"] = str(
            recognition_binding.get("non_claim_note") or summary.get("non_claim_note") or ""
        )
    if uncertainty_binding:
        summary["uncertainty_binding"] = dict(uncertainty_binding)
        summary["uncertainty_case_id"] = str(
            uncertainty_binding.get("uncertainty_case_id") or summary.get("uncertainty_case_id") or ""
        )
        summary["method_confirmation_protocol_id"] = str(
            uncertainty_binding.get("method_confirmation_protocol_id")
            or summary.get("method_confirmation_protocol_id")
            or ""
        )
        summary["verification_rollup_id"] = str(
            uncertainty_binding.get("verification_rollup_id") or summary.get("verification_rollup_id") or ""
        )
    write_json(summary_path, summary)

    for key, value in dict(payload.get("manifest_sections") or {}).items():
        manifest[str(key)] = value
    write_json(manifest_path, manifest)

    if recognition_binding:
        results["recognition_binding"] = dict(recognition_binding)
        results["scope_id"] = str(recognition_binding.get("scope_id") or results.get("scope_id") or "")
        results["decision_rule_id"] = str(
            recognition_binding.get("decision_rule_id") or results.get("decision_rule_id") or ""
        )
        results["limitation_note"] = str(
            recognition_binding.get("limitation_note") or results.get("limitation_note") or ""
        )
        results["non_claim_note"] = str(
            recognition_binding.get("non_claim_note") or results.get("non_claim_note") or ""
        )
    if uncertainty_binding:
        results["uncertainty_binding"] = dict(uncertainty_binding)
        results["uncertainty_case_id"] = str(
            uncertainty_binding.get("uncertainty_case_id") or results.get("uncertainty_case_id") or ""
        )
        results["method_confirmation_protocol_id"] = str(
            uncertainty_binding.get("method_confirmation_protocol_id")
            or results.get("method_confirmation_protocol_id")
            or ""
        )
        results["verification_rollup_id"] = str(
            uncertainty_binding.get("verification_rollup_id") or results.get("verification_rollup_id") or ""
        )
    for key in (
        "step2_closeout_digest",
        "evidence_coverage_matrix",
        "result_traceability_tree",
        "evidence_lineage_index",
        "reviewer_anchor_navigation",
        "ai_run_summary_payload",
        "engineering_isolation_gate_result",
    ):
        value = summary_stats.get(key)
        if isinstance(value, dict) and value:
            results[str(key)] = dict(value)
    write_json(results_path, results)


def _merge_unique_text_list(existing: Iterable[object], incoming: Iterable[object]) -> list[str]:
    rows: list[str] = []
    for collection in (existing, incoming):
        for item in list(collection or []):
            text = str(item or "").strip()
            if text and text not in rows:
                rows.append(text)
    return rows


def _summary_text(payload: dict[str, object] | None) -> str:
    data = dict(payload or {})
    digest = dict(data.get("digest") or {})
    review_surface = dict(data.get("review_surface") or {})
    return str(
        data.get("summary_line")
        or digest.get("summary")
        or review_surface.get("summary_text")
        or data.get("summary")
        or ""
    ).strip()


def _review_surface_lines(payload: dict[str, object] | None) -> list[str]:
    review_surface = dict(dict(payload or {}).get("review_surface") or {})
    return [str(item).strip() for item in list(review_surface.get("summary_lines") or []) if str(item).strip()]


def _build_manifest_surface_section(
    *,
    artifact_key: str,
    path: Path,
    payload: dict[str, object] | None,
    extra_fields: dict[str, object] | None = None,
) -> dict[str, object]:
    data = dict(payload or {})
    section = {
        "artifact_type": str(data.get("artifact_type") or artifact_key),
        "path": str(path),
        "summary_text": _summary_text(data),
        "summary_lines": _review_surface_lines(data),
        "digest": dict(data.get("digest") or {}),
        "review_surface": dict(data.get("review_surface") or {}),
        "reviewer_only": bool(data.get("reviewer_only", True)),
        "readiness_mapping_only": bool(data.get("readiness_mapping_only", True)),
        "not_real_acceptance_evidence": bool(data.get("not_real_acceptance_evidence", True)),
        "not_ready_for_formal_claim": bool(data.get("not_ready_for_formal_claim", True)),
    }
    if extra_fields:
        section.update(extra_fields)
    return section


def _build_recognition_binding(
    *,
    scope_definition_pack: dict[str, object],
    decision_rule_profile: dict[str, object],
    conformity_statement_profile: dict[str, object],
    step2_closeout_bundle: dict[str, object],
    step2_closeout_digest: dict[str, object],
) -> dict[str, object]:
    scope_payload = dict(scope_definition_pack or {})
    decision_payload = dict(decision_rule_profile or {})
    conformity_payload = dict(conformity_statement_profile or {})
    closeout_bundle = dict(step2_closeout_bundle or {})
    closeout_digest = dict(step2_closeout_digest or {})
    scope_id = str(
        closeout_bundle.get("scope_id")
        or closeout_digest.get("scope_id")
        or scope_payload.get("scope_id")
        or dict(scope_payload.get("scope_export_pack") or {}).get("scope_id")
        or ""
    ).strip()
    decision_rule_id = str(
        closeout_bundle.get("decision_rule_id")
        or closeout_digest.get("decision_rule_id")
        or decision_payload.get("decision_rule_id")
        or ""
    ).strip()
    limitation_note = str(
        closeout_bundle.get("limitation_note")
        or closeout_digest.get("limitation_note")
        or decision_payload.get("limitation_note")
        or conformity_payload.get("limitation_note")
        or ""
    ).strip()
    non_claim_note = str(
        closeout_bundle.get("non_claim_note")
        or closeout_digest.get("non_claim_note")
        or decision_payload.get("non_claim_note")
        or conformity_payload.get("non_claim_note")
        or ""
    ).strip()
    applicability_scope_display = str(
        decision_payload.get("applicability_scope_display")
        or scope_payload.get("applicability_scope_display")
        or conformity_payload.get("applicability_scope_display")
        or ""
    ).strip()
    return {
        "scope_id": scope_id,
        "decision_rule_id": decision_rule_id,
        "limitation_note": limitation_note,
        "non_claim_note": non_claim_note,
        "applicability_scope_display": applicability_scope_display,
        "binding_summary": (
            f"scope {scope_id or '--'} | decision_rule {decision_rule_id or '--'} | "
            f"limitation {limitation_note or '--'}"
        ),
        "reviewer_only": True,
        "readiness_mapping_only": True,
        "not_real_acceptance_evidence": True,
        "not_ready_for_formal_claim": True,
    }


def _build_uncertainty_binding(
    *,
    recognition_binding: dict[str, object],
    uncertainty_report_pack: dict[str, object],
    uncertainty_rollup: dict[str, object],
    method_confirmation_protocol: dict[str, object],
    verification_rollup: dict[str, object],
    step2_closeout_bundle: dict[str, object],
) -> dict[str, object]:
    recognition = dict(recognition_binding or {})
    uncertainty_pack = dict(uncertainty_report_pack or {})
    uncertainty_rollup_payload = dict(uncertainty_rollup or {})
    method_payload = dict(method_confirmation_protocol or {})
    verification_payload = dict(verification_rollup or {})
    closeout_bundle = dict(step2_closeout_bundle or {})
    uncertainty_case_id = str(
        closeout_bundle.get("uncertainty_case_id")
        or uncertainty_rollup_payload.get("uncertainty_case_id")
        or uncertainty_pack.get("uncertainty_case_id")
        or ""
    ).strip()
    method_confirmation_protocol_id = str(
        closeout_bundle.get("method_confirmation_protocol_id")
        or method_payload.get("method_confirmation_protocol_id")
        or method_payload.get("protocol_id")
        or verification_payload.get("method_confirmation_protocol_id")
        or ""
    ).strip()
    verification_rollup_id = str(
        closeout_bundle.get("verification_rollup_id")
        or verification_payload.get("verification_rollup_id")
        or verification_payload.get("verification_digest_id")
        or ""
    ).strip()
    return {
        "scope_id": str(recognition.get("scope_id") or "").strip(),
        "decision_rule_id": str(recognition.get("decision_rule_id") or "").strip(),
        "limitation_note": str(recognition.get("limitation_note") or "").strip(),
        "non_claim_note": str(recognition.get("non_claim_note") or "").strip(),
        "uncertainty_case_id": uncertainty_case_id,
        "method_confirmation_protocol_id": method_confirmation_protocol_id,
        "verification_rollup_id": verification_rollup_id,
        "binding_summary": (
            f"uncertainty_case {uncertainty_case_id or '--'} | "
            f"method_protocol {method_confirmation_protocol_id or '--'} | "
            f"verification {verification_rollup_id or '--'}"
        ),
        "reviewer_only": True,
        "readiness_mapping_only": True,
        "not_real_acceptance_evidence": True,
        "not_ready_for_formal_claim": True,
    }


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
            print(
                "engineering_isolation_gate_result: "
                f"{Path(args.run_dir).resolve() / ENGINEERING_ISOLATION_GATE_RESULT_FILENAME}"
            )
            print(
                "engineering_isolation_gate_digest: "
                f"{Path(args.run_dir).resolve() / ENGINEERING_ISOLATION_GATE_DIGEST_FILENAME}"
            )
            print(
                "engineering_isolation_blockers: "
                f"{Path(args.run_dir).resolve() / ENGINEERING_ISOLATION_BLOCKERS_FILENAME}"
            )
            print(
                "engineering_isolation_warnings: "
                f"{Path(args.run_dir).resolve() / ENGINEERING_ISOLATION_WARNINGS_FILENAME}"
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
