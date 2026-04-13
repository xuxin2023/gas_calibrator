from gas_calibrator.v2.core.acceptance_model import (
    build_run_acceptance_plan,
    build_suite_acceptance_plan,
    build_user_visible_evidence_boundary,
    build_validation_acceptance_snapshot,
)
from gas_calibrator.v2.core.metrology_calibration_contract import (
    METROLOGY_CALIBRATION_CONTRACT_FILENAME,
    build_metrology_calibration_contract,
)
from gas_calibrator.v2.core.phase_transition_bridge import build_phase_transition_bridge
from gas_calibrator.v2.core.phase_transition_bridge_presenter import build_phase_transition_bridge_panel_payload
from gas_calibrator.v2.core.phase_transition_bridge_reviewer_artifact import (
    PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME,
    build_phase_transition_bridge_reviewer_artifact,
)
from gas_calibrator.v2.core.phase_transition_bridge_reviewer_artifact_entry import (
    PHASE_TRANSITION_BRIDGE_REVIEWER_ARTIFACT_KEY,
    build_phase_transition_bridge_reviewer_artifact_entry,
)
from gas_calibrator.v2.core.stage_admission_review_pack import (
    STAGE_ADMISSION_REVIEW_PACK_FILENAME,
    STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME,
    build_stage_admission_review_pack,
)
from gas_calibrator.v2.core.stage_admission_review_pack_artifact_entry import (
    STAGE_ADMISSION_REVIEW_PACK_ARTIFACT_KEY,
    STAGE_ADMISSION_REVIEW_PACK_REVIEWER_ARTIFACT_KEY,
    build_stage_admission_review_pack_artifact_entry,
)
from gas_calibrator.v2.core.engineering_isolation_admission_checklist import (
    ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME,
    ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME,
    build_engineering_isolation_admission_checklist,
)
from gas_calibrator.v2.core.engineering_isolation_admission_checklist_artifact_entry import (
    ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_ARTIFACT_KEY,
    ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_ARTIFACT_KEY,
    build_engineering_isolation_admission_checklist_artifact_entry,
)
from gas_calibrator.v2.core.stage3_real_validation_plan import (
    STAGE3_REAL_VALIDATION_PLAN_FILENAME,
    STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME,
    build_stage3_real_validation_plan,
)
from gas_calibrator.v2.core.stage3_real_validation_plan_artifact_entry import (
    STAGE3_REAL_VALIDATION_PLAN_ARTIFACT_KEY,
    STAGE3_REAL_VALIDATION_PLAN_REVIEWER_ARTIFACT_KEY,
    build_stage3_real_validation_plan_artifact_entry,
)
from gas_calibrator.v2.core.stage3_standards_alignment_matrix import (
    STAGE3_STANDARDS_ALIGNMENT_MATRIX_FILENAME,
    STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_FILENAME,
    build_stage3_standards_alignment_matrix,
)
from gas_calibrator.v2.core.stage3_standards_alignment_matrix_artifact_entry import (
    STAGE3_STANDARDS_ALIGNMENT_MATRIX_ARTIFACT_KEY,
    STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_ARTIFACT_KEY,
    build_stage3_standards_alignment_matrix_artifact_entry,
)
from gas_calibrator.v2.core.step2_readiness import build_step2_readiness_summary
from gas_calibrator.v2.core.step2_readiness import STEP2_READINESS_SUMMARY_FILENAME


def test_validation_acceptance_snapshot_does_not_allow_simulated_promotion() -> None:
    payload = {
        "evidence_source": "simulated_protocol",
        "evidence_state": "simulated_protocol",
        "compare_status": "MATCH",
        "diagnostic_only": False,
        "acceptance_evidence": False,
        "not_real_acceptance_evidence": True,
        "reference_quality": {"reference_quality": "healthy"},
        "route_execution_summary": {
            "valid_for_route_diff": True,
            "relay_physical_mismatch": {"v1": False, "v2": False},
        },
    }

    snapshot = build_validation_acceptance_snapshot(payload)

    assert snapshot["acceptance_level"] == "offline_regression"
    assert snapshot["promotion_state"] == "dry_run_only"
    assert snapshot["ready_for_promotion"] is False
    assert "real acceptance evidence present" in snapshot["missing_conditions"]
    assert snapshot["promotion_plan"]["publish_primary_latest_allowed"] is False


def test_run_acceptance_plan_stays_dry_run_without_real_acceptance() -> None:
    plan = build_run_acceptance_plan(
        run_id="run_test",
        simulation_mode=True,
        reference_quality_ok_flag=True,
        export_error_count=0,
        parity_status="MATCH",
    )

    assert plan["evidence_source"] == "simulated_protocol"
    assert plan["not_real_acceptance_evidence"] is True
    assert plan["promotion_state"] == "dry_run_only"
    assert plan["ready_for_promotion"] is False


def test_suite_acceptance_plan_reports_missing_real_acceptance() -> None:
    plan = build_suite_acceptance_plan(
        suite_name="nightly",
        offline_green=True,
        parity_green=True,
        resilience_green=True,
        evidence_sources_present=["simulated", "replay", "simulated_protocol"],
    )

    assert plan["acceptance_scope"] == "suite"
    assert plan["evidence_source"] == "simulated_protocol"
    assert plan["evidence_sources_present"] == ["simulated_protocol", "replay"]
    assert plan["promotion_state"] == "dry_run_only"
    assert "real acceptance evidence present" in plan["missing_conditions"]


def test_user_visible_evidence_boundary_defaults_to_step2_dry_run_fields() -> None:
    payload = build_user_visible_evidence_boundary(simulation_mode=True)

    assert payload["evidence_source"] == "simulated_protocol"
    assert payload["not_real_acceptance_evidence"] is True
    assert payload["acceptance_level"] == "offline_regression"
    assert payload["promotion_state"] == "dry_run_only"


def test_step2_readiness_summary_reports_engineering_isolation_preparation_without_claiming_real_acceptance() -> None:
    readiness = build_step2_readiness_summary(
        run_id="run_ready",
        simulation_mode=True,
        config_governance_handoff={
            "simulation_only": True,
            "operator_safe": True,
            "real_port_device_count": 0,
            "engineering_only_flag_count": 0,
            "enabled_engineering_flags": [],
            "risk_markers": [],
            "execution_gate": {"status": "open"},
            "step2_default_workflow_allowed": True,
            "requires_explicit_unlock": False,
        },
    )

    gate_ids = {str(item["gate_id"]) for item in readiness["gates"]}

    assert readiness["phase"] == "step2_readiness_bridge"
    assert readiness["mode"] == "simulation_only"
    assert readiness["overall_status"] == "ready_for_engineering_isolation"
    assert readiness["ready_for_engineering_isolation"] is True
    assert readiness["real_acceptance_ready"] is False
    assert readiness["evidence_mode"] == "simulation_offline_headless"
    assert readiness["not_real_acceptance_evidence"] is True
    assert readiness["gate_status_counts"]["pass"] >= 1
    assert readiness["blocking_items"] == []
    assert {
        "simulation_only_boundary",
        "real_bench_locked_by_default",
        "shared_experiment_flags_default_off",
        "offline_only_adapters_not_in_default_path",
        "reviewer_surface_hydration_chain_ready",
        "headless_smoke_path_available",
        "readiness_evidence_complete",
        "step2_gate_status",
    } <= gate_ids
    assert readiness["reviewer_display"]["status_line"].startswith("阶段状态：")
    assert "不是 real acceptance" in readiness["reviewer_display"]["summary_text"]
    assert all(str(item["reason_code"]).strip() for item in readiness["gates"])


def test_step2_readiness_summary_blocks_enabled_engineering_flags_but_keeps_raw_contract_machine_readable() -> None:
    readiness = build_step2_readiness_summary(
        run_id="run_blocked",
        simulation_mode=True,
        config_governance_handoff={
            "simulation_only": True,
            "operator_safe": False,
            "real_port_device_count": 0,
            "engineering_only_flag_count": 1,
            "enabled_engineering_flags": ["workflow.pressure.capture_then_hold_enabled"],
            "risk_markers": ["engineering_only_flags_enabled"],
            "execution_gate": {"status": "blocked"},
            "step2_default_workflow_allowed": False,
            "requires_explicit_unlock": True,
        },
    )

    step2_gate = next(item for item in readiness["gates"] if item["gate_id"] == "step2_gate_status")
    flag_gate = next(item for item in readiness["gates"] if item["gate_id"] == "shared_experiment_flags_default_off")

    assert readiness["overall_status"] == "not_ready"
    assert "shared_experiment_flags_default_off" in readiness["blocking_items"]
    assert step2_gate["status"] == "not_ready"
    assert step2_gate["reason_code"] == "execution_gate_blocked"
    assert flag_gate["status"] == "blocked"
    assert flag_gate["details"]["enabled_engineering_flags"] == ["workflow.pressure.capture_then_hold_enabled"]
    assert "不是 real acceptance" in readiness["reviewer_display"]["summary_text"]


def test_step2_readiness_summary_requires_governance_evidence_completeness_before_marking_ready() -> None:
    readiness = build_step2_readiness_summary(
        run_id="run_missing_governance",
        simulation_mode=True,
        config_governance_handoff={},
    )

    evidence_gate = next(item for item in readiness["gates"] if item["gate_id"] == "readiness_evidence_complete")
    step2_gate = next(item for item in readiness["gates"] if item["gate_id"] == "step2_gate_status")

    assert readiness["overall_status"] == "not_ready"
    assert readiness["ready_for_engineering_isolation"] is False
    assert readiness["real_acceptance_ready"] is False
    assert evidence_gate["status"] == "blocked"
    assert evidence_gate["reason_code"] == "config_governance_handoff_incomplete"
    assert evidence_gate["details"]["governance_handoff_present"] is False
    assert "execution_gate" in evidence_gate["details"]["missing_fields"]
    assert readiness["gate_status_counts"]["blocked"] >= 1
    assert readiness["gate_status_counts"]["not_ready"] == 1
    assert "readiness_evidence_complete" in readiness["blocking_items"]
    assert "config_governance_handoff_incomplete" in readiness["warning_items"]
    assert any("治理证据完整性：阻塞" in line for line in readiness["reviewer_display"]["gate_lines"])
    assert step2_gate["status"] == "not_ready"


def test_metrology_calibration_contract_reports_step2_tail_design_contract_without_claiming_real_acceptance() -> None:
    contract = build_metrology_calibration_contract(
        run_id="run_metrology",
        simulation_mode=True,
        config_governance_handoff={
            "simulation_only": True,
            "real_port_device_count": 0,
            "engineering_only_flag_count": 0,
            "enabled_engineering_flags": [],
        },
    )

    assert contract["artifact_type"] == "metrology_calibration_contract"
    assert contract["phase"] == "step2_tail_step3_bridge"
    assert contract["mode"] == "simulation_only"
    assert contract["overall_status"] == "contract_ready_for_stage3_bridge"
    assert contract["real_acceptance_ready"] is False
    assert contract["reference_traceability_contract"]["placeholder_only"] is True
    assert contract["reference_traceability_contract"]["certificate_cycle_hard_blocking_stage"] == "stage3_real_validation"
    assert contract["calibration_execution_contract"]["default_workflow_unchanged"] is True
    assert contract["uncertainty_budget_template"]["template_only"] is True
    assert "real_run_uncertainty_result" in contract["stage3_execution_items"]
    assert "reference_traceability_contract_schema" in contract["stage_assignment"]["execute_now_in_step2_tail"]
    assert "real_reference_instrument_enforcement" in contract["stage_assignment"]["defer_to_stage3_real_validation"]
    assert "不是 real acceptance" in contract["reviewer_display"]["summary_text"]
    assert "第三阶段再执行" in contract["reviewer_display"]["defer_to_stage3_text"]
    assert any("不确定度模板" in line for line in contract["reviewer_display"]["section_lines"])


def test_metrology_calibration_contract_keeps_raw_contract_machine_readable_when_boundary_is_not_simulation_only() -> None:
    contract = build_metrology_calibration_contract(
        run_id="run_non_sim_boundary",
        simulation_mode=False,
        config_governance_handoff={
            "simulation_only": False,
            "real_port_device_count": 1,
            "engineering_only_flag_count": 1,
            "enabled_engineering_flags": ["workflow.pressure.capture_then_hold_enabled"],
        },
    )

    assert contract["overall_status"] == "contract_ready_for_stage3_bridge"
    assert contract["blocking_items"] == []
    assert "simulation_only_boundary_not_satisfied" in contract["warning_items"]
    assert "engineering_only_flags_enabled" in contract["warning_items"]
    assert contract["reference_traceability_contract"]["device_classes"][0]["device_class"] == "dewpoint_meter"
    assert contract["data_quality_contract"]["gates"][0]["gate_id"] == "pressure_stability_gate"
    assert contract["reporting_contract"]["governance_notice_code"] == "metrology_design_contract_only"
    assert contract["reviewer_display"]["blocking_text"].startswith("阻塞项：")


def test_phase_transition_bridge_reports_step2_tail_gap_without_confusing_it_with_real_acceptance() -> None:
    readiness = build_step2_readiness_summary(
        run_id="run_bridge_blocked",
        simulation_mode=True,
        config_governance_handoff={
            "simulation_only": True,
            "operator_safe": True,
            "real_port_device_count": 0,
            "engineering_only_flag_count": 0,
            "enabled_engineering_flags": [],
            "risk_markers": [],
            "execution_gate": {"status": "blocked"},
            "step2_default_workflow_allowed": False,
            "requires_explicit_unlock": True,
        },
    )
    metrology = build_metrology_calibration_contract(
        run_id="run_bridge_blocked",
        simulation_mode=True,
        config_governance_handoff={
            "simulation_only": True,
            "real_port_device_count": 0,
            "engineering_only_flag_count": 0,
            "enabled_engineering_flags": [],
        },
    )

    bridge = build_phase_transition_bridge(
        run_id="run_bridge_blocked",
        step2_readiness_summary=readiness,
        metrology_calibration_contract=metrology,
    )

    assert bridge["artifact_type"] == "phase_transition_bridge"
    assert bridge["phase"] == "step2_tail_stage3_bridge"
    assert bridge["overall_status"] == "step2_tail_in_progress"
    assert bridge["recommended_next_stage"] == "close_step2_tail_gaps"
    assert bridge["ready_for_engineering_isolation"] is False
    assert bridge["real_acceptance_ready"] is False
    assert bridge["step2_readiness_ref"]["overall_status"] == "not_ready"
    assert bridge["metrology_contract_ref"]["overall_status"] == "contract_ready_for_stage3_bridge"
    assert "real_reference_instrument_enforcement" in bridge["defer_to_stage3_real_validation"]
    assert "reference_traceability_contract_schema" in bridge["execute_now_in_step2_tail"]
    assert "resolve_step2_gate_status" in bridge["execute_now_in_step2_tail"]
    assert any(item["gate_id"] == "simulation_only_boundary" for item in bridge["gate_matrix"])
    assert any(item["gate_id"] == "reference_traceability_contract" for item in bridge["gate_matrix"])
    assert "不是 real acceptance" in bridge["reviewer_display"]["summary_text"]
    assert "第三阶段执行" in bridge["reviewer_display"]["defer_to_stage3_text"]
    assert "不能替代真实计量验证" in bridge["reviewer_display"]["summary_text"]


def test_phase_transition_bridge_reports_engineering_isolation_without_marking_real_acceptance_ready() -> None:
    readiness = build_step2_readiness_summary(
        run_id="run_bridge_ready",
        simulation_mode=True,
        config_governance_handoff={
            "simulation_only": True,
            "operator_safe": True,
            "real_port_device_count": 0,
            "engineering_only_flag_count": 0,
            "enabled_engineering_flags": [],
            "risk_markers": [],
            "execution_gate": {"status": "open"},
            "step2_default_workflow_allowed": True,
            "requires_explicit_unlock": False,
        },
    )
    metrology = build_metrology_calibration_contract(
        run_id="run_bridge_ready",
        simulation_mode=True,
        config_governance_handoff={
            "simulation_only": True,
            "real_port_device_count": 0,
            "engineering_only_flag_count": 0,
            "enabled_engineering_flags": [],
        },
    )

    bridge = build_phase_transition_bridge(
        run_id="run_bridge_ready",
        step2_readiness_summary=readiness,
        metrology_calibration_contract=metrology,
    )

    assert bridge["overall_status"] == "ready_for_engineering_isolation"
    assert bridge["recommended_next_stage"] == "engineering_isolation"
    assert bridge["ready_for_engineering_isolation"] is True
    assert bridge["real_acceptance_ready"] is False
    assert "engineering-isolation" in bridge["reviewer_display"]["status_line"]
    assert "not_real_acceptance_evidence" in bridge["warning_items"]


def test_phase_transition_bridge_reviewer_artifact_reuses_canonical_panel_output_without_leaking_raw_keys() -> None:
    readiness = build_step2_readiness_summary(
        run_id="run_bridge_reviewer",
        simulation_mode=True,
        config_governance_handoff={
            "simulation_only": True,
            "operator_safe": True,
            "real_port_device_count": 0,
            "engineering_only_flag_count": 0,
            "enabled_engineering_flags": [],
            "risk_markers": [],
            "execution_gate": {"status": "open"},
            "step2_default_workflow_allowed": True,
            "requires_explicit_unlock": False,
        },
    )
    metrology = build_metrology_calibration_contract(
        run_id="run_bridge_reviewer",
        simulation_mode=True,
        config_governance_handoff={
            "simulation_only": True,
            "real_port_device_count": 0,
            "engineering_only_flag_count": 0,
            "enabled_engineering_flags": [],
        },
    )
    bridge = build_phase_transition_bridge(
        run_id="run_bridge_reviewer",
        step2_readiness_summary=readiness,
        metrology_calibration_contract=metrology,
    )

    artifact = build_phase_transition_bridge_reviewer_artifact(bridge)
    expected_panel = build_phase_transition_bridge_panel_payload(bridge)
    markdown = artifact["markdown"]

    assert artifact["available"] is True
    assert artifact["artifact_type"] == "phase_transition_bridge_reviewer_artifact"
    assert artifact["filename"] == PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME
    assert artifact["raw"]["ready_for_engineering_isolation"] is True
    assert artifact["raw"]["real_acceptance_ready"] is False
    assert artifact["section"]["display"] == expected_panel["display"]
    assert artifact["display"]["engineering_isolation_text"] == expected_panel["display"]["engineering_isolation_text"]
    assert artifact["display"]["real_acceptance_text"] == expected_panel["display"]["real_acceptance_text"]
    assert "engineering-isolation 准备：已具备。" in expected_panel["display"]["section_text"]
    assert "real acceptance 准备：尚未具备。" in expected_panel["display"]["section_text"]
    assert "Step 2 tail / Stage 3 bridge" in markdown
    assert "engineering-isolation" in markdown
    assert "engineering-isolation 准备：已具备。" in markdown
    assert "real acceptance 准备：尚未具备。" in markdown
    assert "当前执行" in markdown
    assert "第三阶段执行" in markdown
    assert "不是 real acceptance" in markdown
    assert "不能替代真实计量验证" in markdown
    assert "ready_for_engineering_isolation" not in markdown
    assert "real_acceptance_ready" not in markdown


def test_phase_transition_bridge_reviewer_artifact_entry_reuses_manifest_and_panel_wording_without_rejudging_stage_logic() -> None:
    readiness = build_step2_readiness_summary(
        run_id="run_bridge_entry",
        simulation_mode=True,
        config_governance_handoff={
            "simulation_only": True,
            "operator_safe": True,
            "real_port_device_count": 0,
            "engineering_only_flag_count": 0,
            "enabled_engineering_flags": [],
            "risk_markers": [],
            "execution_gate": {"status": "open"},
            "step2_default_workflow_allowed": True,
            "requires_explicit_unlock": False,
        },
    )
    metrology = build_metrology_calibration_contract(
        run_id="run_bridge_entry",
        simulation_mode=True,
        config_governance_handoff={
            "simulation_only": True,
            "real_port_device_count": 0,
            "engineering_only_flag_count": 0,
            "enabled_engineering_flags": [],
        },
    )
    bridge = build_phase_transition_bridge(
        run_id="run_bridge_entry",
        step2_readiness_summary=readiness,
        metrology_calibration_contract=metrology,
    )
    reviewer_artifact = build_phase_transition_bridge_reviewer_artifact(bridge)

    entry = build_phase_transition_bridge_reviewer_artifact_entry(
        artifact_path=f"D:/tmp/{PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME}",
        manifest_section={
            "artifact_type": reviewer_artifact["artifact_type"],
            "path": f"D:/tmp/{PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME}",
            "available": True,
            "summary_text": reviewer_artifact["display"]["summary_text"],
            "status_line": reviewer_artifact["display"]["status_line"],
            "current_stage_text": reviewer_artifact["display"]["current_stage_text"],
            "next_stage_text": reviewer_artifact["display"]["next_stage_text"],
            "engineering_isolation_text": reviewer_artifact["display"]["engineering_isolation_text"],
            "real_acceptance_text": reviewer_artifact["display"]["real_acceptance_text"],
            "execute_now_text": reviewer_artifact["display"]["execute_now_text"],
            "defer_to_stage3_text": reviewer_artifact["display"]["defer_to_stage3_text"],
            "blocking_text": reviewer_artifact["display"]["blocking_text"],
            "warning_text": reviewer_artifact["display"]["warning_text"],
            "not_real_acceptance_evidence": True,
        },
        reviewer_section=reviewer_artifact["section"],
    )

    assert entry["artifact_key"] == PHASE_TRANSITION_BRIDGE_REVIEWER_ARTIFACT_KEY
    assert entry["artifact_type"] == PHASE_TRANSITION_BRIDGE_REVIEWER_ARTIFACT_KEY
    assert entry["path"].endswith(PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME)
    assert entry["name_text"] == reviewer_artifact["section"]["display"]["title_text"]
    assert entry["summary_text"] == reviewer_artifact["display"]["summary_text"]
    assert entry["status_line"] == reviewer_artifact["display"]["status_line"]
    assert entry["stage_marker_text"] == reviewer_artifact["display"]["current_stage_text"]
    assert entry["engineering_isolation_text"] == reviewer_artifact["display"]["engineering_isolation_text"]
    assert entry["real_acceptance_text"] == reviewer_artifact["display"]["real_acceptance_text"]
    assert "Step 2 tail / Stage 3 bridge" in entry["entry_text"]
    assert "engineering-isolation" in entry["entry_text"]
    assert reviewer_artifact["display"]["execute_now_text"] in entry["entry_text"]
    assert reviewer_artifact["display"]["defer_to_stage3_text"] in entry["entry_text"]
    assert "不是 real acceptance" in entry["entry_text"]
    assert "不能替代真实计量验证" in entry["entry_text"]
    assert "ready_for_engineering_isolation" not in entry["entry_text"]
    assert "real_acceptance_ready" not in entry["entry_text"]


def test_stage3_standards_alignment_matrix_reuses_stage3_plan_pack_and_checklist_wording_without_fake_compliance() -> None:
    readiness = build_step2_readiness_summary(
        run_id="run_stage3_matrix",
        simulation_mode=True,
        config_governance_handoff={
            "simulation_only": True,
            "operator_safe": True,
            "real_port_device_count": 0,
            "engineering_only_flag_count": 0,
            "enabled_engineering_flags": [],
            "risk_markers": [],
            "execution_gate": {"status": "open"},
            "step2_default_workflow_allowed": True,
            "requires_explicit_unlock": False,
        },
    )
    metrology = build_metrology_calibration_contract(
        run_id="run_stage3_matrix",
        simulation_mode=True,
        config_governance_handoff={
            "simulation_only": True,
            "real_port_device_count": 0,
            "engineering_only_flag_count": 0,
            "enabled_engineering_flags": [],
        },
    )
    bridge = build_phase_transition_bridge(
        run_id="run_stage3_matrix",
        step2_readiness_summary=readiness,
        metrology_calibration_contract=metrology,
    )
    artifact_paths = {
        "step2_readiness_summary": f"D:/tmp/{STEP2_READINESS_SUMMARY_FILENAME}",
        "metrology_calibration_contract": f"D:/tmp/{METROLOGY_CALIBRATION_CONTRACT_FILENAME}",
        "phase_transition_bridge": "D:/tmp/phase_transition_bridge.json",
        "phase_transition_bridge_reviewer_artifact": f"D:/tmp/{PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME}",
        "stage_admission_review_pack": f"D:/tmp/{STAGE_ADMISSION_REVIEW_PACK_FILENAME}",
        "stage_admission_review_pack_reviewer_artifact": f"D:/tmp/{STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME}",
        "engineering_isolation_admission_checklist": f"D:/tmp/{ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME}",
        "engineering_isolation_admission_checklist_reviewer_artifact": (
            f"D:/tmp/{ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME}"
        ),
        "stage3_real_validation_plan": f"D:/tmp/{STAGE3_REAL_VALIDATION_PLAN_FILENAME}",
        "stage3_real_validation_plan_reviewer_artifact": f"D:/tmp/{STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME}",
        "stage3_standards_alignment_matrix": f"D:/tmp/{STAGE3_STANDARDS_ALIGNMENT_MATRIX_FILENAME}",
        "stage3_standards_alignment_matrix_reviewer_artifact": (
            f"D:/tmp/{STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_FILENAME}"
        ),
    }
    pack = build_stage_admission_review_pack(
        run_id="run_stage3_matrix",
        step2_readiness_summary=readiness,
        metrology_calibration_contract=metrology,
        phase_transition_bridge=bridge,
        phase_transition_bridge_reviewer_artifact=build_phase_transition_bridge_reviewer_artifact(bridge),
        artifact_paths=artifact_paths,
    )
    checklist = build_engineering_isolation_admission_checklist(
        run_id="run_stage3_matrix",
        step2_readiness_summary=readiness,
        metrology_calibration_contract=metrology,
        phase_transition_bridge=bridge,
        stage_admission_review_pack=pack,
        artifact_paths=artifact_paths,
    )
    plan = build_stage3_real_validation_plan(
        run_id="run_stage3_matrix",
        step2_readiness_summary=readiness,
        metrology_calibration_contract=metrology,
        phase_transition_bridge=bridge,
        stage_admission_review_pack=pack,
        engineering_isolation_admission_checklist=checklist,
        artifact_paths=artifact_paths,
    )
    matrix = build_stage3_standards_alignment_matrix(
        run_id="run_stage3_matrix",
        step2_readiness_summary=readiness,
        metrology_calibration_contract=metrology,
        phase_transition_bridge=bridge,
        stage_admission_review_pack=pack,
        engineering_isolation_admission_checklist=checklist,
        stage3_real_validation_plan=plan,
        artifact_paths=artifact_paths,
    )

    raw = matrix["raw"]
    markdown = matrix["markdown"]
    entry = build_stage3_standards_alignment_matrix_artifact_entry(
        artifact_path=artifact_paths["stage3_standards_alignment_matrix"],
        reviewer_artifact_path=artifact_paths["stage3_standards_alignment_matrix_reviewer_artifact"],
        manifest_section={
            **raw,
            "path": artifact_paths["stage3_standards_alignment_matrix"],
            "reviewer_path": artifact_paths["stage3_standards_alignment_matrix_reviewer_artifact"],
        },
        reviewer_manifest_section={
            "artifact_type": STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_ARTIFACT_KEY,
            "path": artifact_paths["stage3_standards_alignment_matrix_reviewer_artifact"],
            "summary_text": matrix["display"]["summary_text"],
            "reviewer_note_text": matrix["display"]["reviewer_note_text"],
            "status_line": matrix["display"]["status_line"],
            "current_stage_text": matrix["display"]["current_stage_text"],
            "next_stage_text": matrix["display"]["next_stage_text"],
            "engineering_isolation_text": matrix["display"]["engineering_isolation_text"],
            "real_acceptance_text": matrix["display"]["real_acceptance_text"],
            "stage_bridge_text": matrix["display"]["stage_bridge_text"],
            "artifact_role_text": matrix["display"]["artifact_role_text"],
            "not_real_acceptance_evidence": True,
        },
        digest_section={
            "overall_status": raw["overall_status"],
            "recommended_next_stage": raw["recommended_next_stage"],
            "mapping_scope": raw["mapping_scope"],
            "standard_family_count": len(raw["standard_families"]),
            "mapping_row_count": len(raw["rows"]),
            "required_evidence_category_count": len(raw["required_evidence_categories"]),
            "standard_families": raw["standard_families"],
            "required_evidence_categories": raw["required_evidence_categories"],
            "readiness_status_counts": raw["readiness_status_counts"],
            "boundary_statements": raw["boundary_statements"],
            "artifact_paths": raw["artifact_paths"],
        },
        reviewer_markdown_text=markdown,
    )

    assert matrix["artifact_type"] == "stage3_standards_alignment_matrix"
    assert matrix["filename"] == STAGE3_STANDARDS_ALIGNMENT_MATRIX_FILENAME
    assert matrix["reviewer_filename"] == STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_FILENAME
    assert raw["mapping_scope"] == "family_topic_level_only"
    assert raw["artifact_refs"]["stage3_real_validation_plan"]["summary_text"] == plan["display"]["summary_text"]
    assert raw["artifact_refs"]["engineering_isolation_admission_checklist"]["summary_text"] == (
        checklist["display"]["summary_text"]
    )
    assert raw["standard_families"] == [
        "中国气象局 / 气象行业观测与质量控制相关要求",
        "CNAS-CL01",
        "CNAS-CL01-G002",
        "CNAS-CL01-G003",
        "ISO/IEC 17025",
        "ISO 6142 family",
        "ISO 6143",
        "ISO 6145 family",
        "WMO / GAW QA",
    ]
    assert len(raw["rows"]) == 9
    assert all(row["mapping_level"] == "family_topic_level_only" for row in raw["rows"])
    assert all("clause_number" not in row and "clause_id" not in row for row in raw["rows"])
    assert matrix["display"]["status_line"] == plan["display"]["status_line"]
    assert matrix["display"]["engineering_isolation_text"] == plan["display"]["engineering_isolation_text"]
    assert matrix["display"]["real_acceptance_text"] == plan["display"]["real_acceptance_text"]
    assert "Step 2 tail / Stage 3 bridge" in markdown
    assert "readiness mapping only" in markdown
    assert "not accreditation claim" in markdown
    assert "not compliance certification" in markdown
    assert "not real acceptance" in markdown
    assert "cannot replace real metrology validation" in markdown
    assert "simulation / offline / headless only" in markdown
    assert "stage3_real_validation_plan.json" in markdown
    assert "ready_for_engineering_isolation" not in markdown
    assert "real_acceptance_ready" not in markdown
    assert entry["artifact_key"] == STAGE3_STANDARDS_ALIGNMENT_MATRIX_ARTIFACT_KEY
    assert entry["reviewer_artifact_key"] == STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_ARTIFACT_KEY
    assert entry["path"].endswith(STAGE3_STANDARDS_ALIGNMENT_MATRIX_FILENAME)
    assert entry["reviewer_path"].endswith(STAGE3_STANDARDS_ALIGNMENT_MATRIX_REVIEWER_FILENAME)
    assert entry["status_line"] == matrix["display"]["status_line"]
    assert entry["engineering_isolation_text"] == matrix["display"]["engineering_isolation_text"]
    assert entry["real_acceptance_text"] == matrix["display"]["real_acceptance_text"]
    assert entry["stage_bridge_text"] == matrix["display"]["stage_bridge_text"]
    assert entry["navigation_id"] == "stage3-standards-alignment-matrix"
    assert "Step 2 tail / Stage 3 bridge" in entry["card_text"]
    assert "readiness mapping only" in entry["card_text"]
    assert "not accreditation claim" in entry["card_text"]
    assert "not compliance certification" in entry["card_text"]
    assert "not real acceptance" in entry["card_text"]
    assert "cannot replace real metrology validation" in entry["card_text"]
    assert "ISO/IEC 17025" in entry["standard_families_text"]
    assert "CNAS-CL01-G003" in entry["standard_families_text"]
    assert "ready_for_engineering_isolation" not in entry["entry_text"]
    assert "real_acceptance_ready" not in entry["entry_text"]


def test_stage_admission_review_pack_reuses_existing_governance_artifacts_without_rejudging_stage_logic() -> None:
    readiness = build_step2_readiness_summary(
        run_id="run_stage_pack",
        simulation_mode=True,
        config_governance_handoff={
            "simulation_only": True,
            "operator_safe": True,
            "real_port_device_count": 0,
            "engineering_only_flag_count": 0,
            "enabled_engineering_flags": [],
            "risk_markers": [],
            "execution_gate": {"status": "open"},
            "step2_default_workflow_allowed": True,
            "requires_explicit_unlock": False,
        },
    )
    metrology = build_metrology_calibration_contract(
        run_id="run_stage_pack",
        simulation_mode=True,
        config_governance_handoff={
            "simulation_only": True,
            "real_port_device_count": 0,
            "engineering_only_flag_count": 0,
            "enabled_engineering_flags": [],
        },
    )
    bridge = build_phase_transition_bridge(
        run_id="run_stage_pack",
        step2_readiness_summary=readiness,
        metrology_calibration_contract=metrology,
    )
    reviewer_artifact = build_phase_transition_bridge_reviewer_artifact(bridge)

    pack = build_stage_admission_review_pack(
        run_id="run_stage_pack",
        step2_readiness_summary=readiness,
        metrology_calibration_contract=metrology,
        phase_transition_bridge=bridge,
        phase_transition_bridge_reviewer_artifact=reviewer_artifact,
        artifact_paths={
            "step2_readiness_summary": f"D:/tmp/{STEP2_READINESS_SUMMARY_FILENAME}",
            "metrology_calibration_contract": f"D:/tmp/{METROLOGY_CALIBRATION_CONTRACT_FILENAME}",
            "phase_transition_bridge": f"D:/tmp/phase_transition_bridge.json",
            "phase_transition_bridge_reviewer_artifact": f"D:/tmp/{PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME}",
        },
    )

    raw = pack["raw"]
    markdown = pack["markdown"]

    assert pack["artifact_type"] == "stage_admission_review_pack"
    assert pack["filename"] == STAGE_ADMISSION_REVIEW_PACK_FILENAME
    assert pack["reviewer_filename"] == STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME
    assert raw["artifact_type"] == "stage_admission_review_pack"
    assert raw["phase"] == bridge["phase"]
    assert raw["overall_status"] == bridge["overall_status"]
    assert raw["recommended_next_stage"] == bridge["recommended_next_stage"]
    assert raw["ready_for_engineering_isolation"] is True
    assert raw["real_acceptance_ready"] is False
    assert raw["artifact_paths"]["step2_readiness_summary"].endswith(STEP2_READINESS_SUMMARY_FILENAME)
    assert raw["artifact_paths"]["metrology_calibration_contract"].endswith(METROLOGY_CALIBRATION_CONTRACT_FILENAME)
    assert raw["artifact_paths"]["phase_transition_bridge"].endswith("phase_transition_bridge.json")
    assert raw["artifact_paths"]["phase_transition_bridge_reviewer_artifact"].endswith(
        PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME
    )
    assert raw["artifact_refs"]["step2_readiness_summary"]["overall_status"] == readiness["overall_status"]
    assert raw["artifact_refs"]["metrology_calibration_contract"]["overall_status"] == metrology["overall_status"]
    assert raw["artifact_refs"]["phase_transition_bridge"]["overall_status"] == bridge["overall_status"]
    assert raw["artifact_refs"]["phase_transition_bridge_reviewer_artifact"]["summary_text"] == (
        reviewer_artifact["display"]["summary_text"]
    )
    assert raw["execute_now_in_step2_tail"] == bridge["execute_now_in_step2_tail"]
    assert raw["defer_to_stage3_real_validation"] == bridge["defer_to_stage3_real_validation"]
    assert raw["missing_real_world_evidence"] == bridge["missing_real_world_evidence"]
    assert raw["handoff_checklist"]["stage3_prerequisites"] == bridge["missing_real_world_evidence"]
    assert reviewer_artifact["display"]["summary_text"] == pack["display"]["summary_text"]
    assert reviewer_artifact["display"]["status_line"] == pack["display"]["status_line"]
    assert reviewer_artifact["display"]["engineering_isolation_text"] == pack["display"]["engineering_isolation_text"]
    assert reviewer_artifact["display"]["real_acceptance_text"] == pack["display"]["real_acceptance_text"]
    assert reviewer_artifact["display"]["execute_now_text"] == pack["display"]["execute_now_text"]
    assert reviewer_artifact["display"]["defer_to_stage3_text"] == pack["display"]["defer_to_stage3_text"]
    assert "Step 2 tail / Stage 3 bridge" in markdown
    assert "engineering-isolation" in markdown
    assert "当前执行" in markdown
    assert "第三阶段执行" in markdown
    assert "不是 real acceptance" in markdown
    assert "不能替代真实计量验证" in markdown
    assert "step2_readiness_summary.json" in markdown
    assert "metrology_calibration_contract.json" in markdown
    assert "phase_transition_bridge.json" in markdown
    assert "phase_transition_bridge_reviewer.md" in markdown
    assert "ready_for_engineering_isolation" not in markdown
    assert "real_acceptance_ready" not in markdown


def test_stage_admission_review_pack_artifact_entry_reuses_pack_reviewer_display_without_new_stage_logic() -> None:
    readiness = build_step2_readiness_summary(
        run_id="run_stage_pack_entry",
        simulation_mode=True,
        config_governance_handoff={
            "simulation_only": True,
            "operator_safe": True,
            "real_port_device_count": 0,
            "engineering_only_flag_count": 0,
            "enabled_engineering_flags": [],
            "risk_markers": [],
            "execution_gate": {"status": "open"},
            "step2_default_workflow_allowed": True,
            "requires_explicit_unlock": False,
        },
    )
    metrology = build_metrology_calibration_contract(
        run_id="run_stage_pack_entry",
        simulation_mode=True,
        config_governance_handoff={
            "simulation_only": True,
            "real_port_device_count": 0,
            "engineering_only_flag_count": 0,
            "enabled_engineering_flags": [],
        },
    )
    bridge = build_phase_transition_bridge(
        run_id="run_stage_pack_entry",
        step2_readiness_summary=readiness,
        metrology_calibration_contract=metrology,
    )
    reviewer_artifact = build_phase_transition_bridge_reviewer_artifact(bridge)
    pack = build_stage_admission_review_pack(
        run_id="run_stage_pack_entry",
        step2_readiness_summary=readiness,
        metrology_calibration_contract=metrology,
        phase_transition_bridge=bridge,
        phase_transition_bridge_reviewer_artifact=reviewer_artifact,
        artifact_paths={
            "step2_readiness_summary": f"D:/tmp/{STEP2_READINESS_SUMMARY_FILENAME}",
            "metrology_calibration_contract": f"D:/tmp/{METROLOGY_CALIBRATION_CONTRACT_FILENAME}",
            "phase_transition_bridge": "D:/tmp/phase_transition_bridge.json",
            "phase_transition_bridge_reviewer_artifact": f"D:/tmp/{PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME}",
        },
    )

    entry = build_stage_admission_review_pack_artifact_entry(
        artifact_path=f"D:/tmp/{STAGE_ADMISSION_REVIEW_PACK_FILENAME}",
        reviewer_artifact_path=f"D:/tmp/{STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME}",
        manifest_section={
            **pack["raw"],
            "path": f"D:/tmp/{STAGE_ADMISSION_REVIEW_PACK_FILENAME}",
            "reviewer_path": f"D:/tmp/{STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME}",
        },
        reviewer_manifest_section={
            "artifact_type": STAGE_ADMISSION_REVIEW_PACK_REVIEWER_ARTIFACT_KEY,
            "path": f"D:/tmp/{STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME}",
            "summary_text": pack["display"]["summary_text"],
            "status_line": pack["display"]["status_line"],
            "current_stage_text": pack["display"]["current_stage_text"],
            "next_stage_text": pack["display"]["next_stage_text"],
            "engineering_isolation_text": pack["display"]["engineering_isolation_text"],
            "real_acceptance_text": pack["display"]["real_acceptance_text"],
            "execute_now_text": pack["display"]["execute_now_text"],
            "defer_to_stage3_text": pack["display"]["defer_to_stage3_text"],
            "blocking_text": pack["display"]["blocking_text"],
            "warning_text": pack["display"]["warning_text"],
            "not_real_acceptance_evidence": True,
        },
    )

    assert entry["artifact_key"] == STAGE_ADMISSION_REVIEW_PACK_ARTIFACT_KEY
    assert entry["reviewer_artifact_key"] == STAGE_ADMISSION_REVIEW_PACK_REVIEWER_ARTIFACT_KEY
    assert entry["path"].endswith(STAGE_ADMISSION_REVIEW_PACK_FILENAME)
    assert entry["reviewer_path"].endswith(STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME)
    assert entry["summary_text"] == pack["display"]["summary_text"]
    assert entry["status_line"] == pack["display"]["status_line"]
    assert entry["current_stage_text"] == pack["display"]["current_stage_text"]
    assert entry["next_stage_text"] == pack["display"]["next_stage_text"]
    assert entry["engineering_isolation_text"] == pack["display"]["engineering_isolation_text"]
    assert entry["real_acceptance_text"] == pack["display"]["real_acceptance_text"]
    assert "Step 2 tail / Stage 3 bridge" in entry["entry_text"]
    assert "engineering-isolation" in entry["entry_text"]
    assert entry["execute_now_text"] in entry["entry_text"]
    assert entry["defer_to_stage3_text"] in entry["entry_text"]
    assert "不是 real acceptance" in entry["entry_text"]
    assert "不能替代真实计量验证" in entry["entry_text"]
    assert "ready_for_engineering_isolation" not in entry["entry_text"]
    assert "real_acceptance_ready" not in entry["entry_text"]
    assert entry["ready_for_engineering_isolation"] is True
    assert entry["real_acceptance_ready"] is False


def test_engineering_isolation_admission_checklist_reuses_existing_pack_and_bridge_wording_without_new_stage_logic() -> None:
    readiness = build_step2_readiness_summary(
        run_id="run_checklist",
        simulation_mode=True,
        config_governance_handoff={
            "simulation_only": True,
            "operator_safe": True,
            "real_port_device_count": 0,
            "engineering_only_flag_count": 0,
            "enabled_engineering_flags": [],
            "risk_markers": [],
            "execution_gate": {"status": "open"},
            "step2_default_workflow_allowed": True,
            "requires_explicit_unlock": False,
        },
    )
    metrology = build_metrology_calibration_contract(
        run_id="run_checklist",
        simulation_mode=True,
        config_governance_handoff={
            "simulation_only": True,
            "real_port_device_count": 0,
            "engineering_only_flag_count": 0,
            "enabled_engineering_flags": [],
        },
    )
    bridge = build_phase_transition_bridge(
        run_id="run_checklist",
        step2_readiness_summary=readiness,
        metrology_calibration_contract=metrology,
    )
    reviewer_artifact = build_phase_transition_bridge_reviewer_artifact(bridge)
    pack = build_stage_admission_review_pack(
        run_id="run_checklist",
        step2_readiness_summary=readiness,
        metrology_calibration_contract=metrology,
        phase_transition_bridge=bridge,
        phase_transition_bridge_reviewer_artifact=reviewer_artifact,
        artifact_paths={
            "step2_readiness_summary": f"D:/tmp/{STEP2_READINESS_SUMMARY_FILENAME}",
            "metrology_calibration_contract": f"D:/tmp/{METROLOGY_CALIBRATION_CONTRACT_FILENAME}",
            "phase_transition_bridge": "D:/tmp/phase_transition_bridge.json",
            "phase_transition_bridge_reviewer_artifact": f"D:/tmp/{PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME}",
            "stage_admission_review_pack": f"D:/tmp/{STAGE_ADMISSION_REVIEW_PACK_FILENAME}",
            "stage_admission_review_pack_reviewer_artifact": f"D:/tmp/{STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME}",
        },
    )

    checklist = build_engineering_isolation_admission_checklist(
        run_id="run_checklist",
        step2_readiness_summary=readiness,
        metrology_calibration_contract=metrology,
        phase_transition_bridge=bridge,
        stage_admission_review_pack=pack,
        artifact_paths={
            "step2_readiness_summary": f"D:/tmp/{STEP2_READINESS_SUMMARY_FILENAME}",
            "metrology_calibration_contract": f"D:/tmp/{METROLOGY_CALIBRATION_CONTRACT_FILENAME}",
            "phase_transition_bridge": "D:/tmp/phase_transition_bridge.json",
            "phase_transition_bridge_reviewer_artifact": f"D:/tmp/{PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME}",
            "stage_admission_review_pack": f"D:/tmp/{STAGE_ADMISSION_REVIEW_PACK_FILENAME}",
            "stage_admission_review_pack_reviewer_artifact": f"D:/tmp/{STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME}",
        },
    )

    raw = checklist["raw"]
    markdown = checklist["markdown"]
    status_map = {item["item_id"]: item["status"] for item in raw["checklist_items"]}

    assert checklist["artifact_type"] == "engineering_isolation_admission_checklist"
    assert checklist["filename"] == ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME
    assert checklist["reviewer_filename"] == ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME
    assert raw["artifact_type"] == "engineering_isolation_admission_checklist"
    assert raw["phase"] == pack["raw"]["phase"]
    assert raw["overall_status"] == pack["raw"]["overall_status"]
    assert raw["recommended_next_stage"] == pack["raw"]["recommended_next_stage"]
    assert raw["ready_for_engineering_isolation"] is True
    assert raw["real_acceptance_ready"] is False
    assert raw["artifact_paths"]["step2_readiness_summary"].endswith(STEP2_READINESS_SUMMARY_FILENAME)
    assert raw["artifact_paths"]["metrology_calibration_contract"].endswith(METROLOGY_CALIBRATION_CONTRACT_FILENAME)
    assert raw["artifact_paths"]["phase_transition_bridge"].endswith("phase_transition_bridge.json")
    assert raw["artifact_paths"]["stage_admission_review_pack"].endswith(STAGE_ADMISSION_REVIEW_PACK_FILENAME)
    assert raw["artifact_paths"]["stage_admission_review_pack_reviewer_artifact"].endswith(
        STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME
    )
    assert raw["artifact_refs"]["stage_admission_review_pack"]["summary_text"] == pack["display"]["summary_text"]
    assert raw["checklist_status_counts"]["done"] >= 4
    assert raw["checklist_status_counts"]["pending"] >= 3
    assert raw["checklist_status_counts"]["stage3_only"] >= 1
    assert status_map["step2_readiness_bridge_formed"] == "done"
    assert status_map["metrology_contract_institutionalized"] == "done"
    assert status_map["handoff_pack_integrity_confirmation"] == "pending"
    assert status_map["reviewer_discoverability_confirmation"] == "pending"
    assert status_map["evidence_bundle_path_confirmation"] == "pending"
    assert checklist["display"]["status_line"] == pack["display"]["status_line"]
    assert checklist["display"]["engineering_isolation_text"] == pack["display"]["engineering_isolation_text"]
    assert checklist["display"]["real_acceptance_text"] == pack["display"]["real_acceptance_text"]
    assert checklist["display"]["execute_now_text"] == pack["display"]["execute_now_text"]
    assert checklist["display"]["defer_to_stage3_text"] == pack["display"]["defer_to_stage3_text"]
    assert "Step 2 tail / Stage 3 bridge" in markdown
    assert "engineering-isolation" in markdown
    assert "当前执行" in markdown
    assert "第三阶段执行" in markdown
    assert "不是 real acceptance" in markdown
    assert "不能替代真实计量验证" in markdown
    assert "step2_readiness_summary.json" in markdown
    assert "metrology_calibration_contract.json" in markdown
    assert "phase_transition_bridge.json" in markdown
    assert "stage_admission_review_pack.json" in markdown
    assert "stage_admission_review_pack.md" in markdown
    assert "ready_for_engineering_isolation" not in markdown
    assert "real_acceptance_ready" not in markdown


def test_engineering_isolation_admission_checklist_artifact_entry_reuses_checklist_reviewer_display_without_new_stage_logic() -> None:
    readiness = build_step2_readiness_summary(
        run_id="run_checklist_entry",
        simulation_mode=True,
        config_governance_handoff={
            "simulation_only": True,
            "operator_safe": True,
            "real_port_device_count": 0,
            "engineering_only_flag_count": 0,
            "enabled_engineering_flags": [],
            "risk_markers": [],
            "execution_gate": {"status": "open"},
            "step2_default_workflow_allowed": True,
            "requires_explicit_unlock": False,
        },
    )
    metrology = build_metrology_calibration_contract(
        run_id="run_checklist_entry",
        simulation_mode=True,
        config_governance_handoff={
            "simulation_only": True,
            "real_port_device_count": 0,
            "engineering_only_flag_count": 0,
            "enabled_engineering_flags": [],
        },
    )
    bridge = build_phase_transition_bridge(
        run_id="run_checklist_entry",
        step2_readiness_summary=readiness,
        metrology_calibration_contract=metrology,
    )
    pack = build_stage_admission_review_pack(
        run_id="run_checklist_entry",
        step2_readiness_summary=readiness,
        metrology_calibration_contract=metrology,
        phase_transition_bridge=bridge,
        phase_transition_bridge_reviewer_artifact=build_phase_transition_bridge_reviewer_artifact(bridge),
        artifact_paths={
            "step2_readiness_summary": f"D:/tmp/{STEP2_READINESS_SUMMARY_FILENAME}",
            "metrology_calibration_contract": f"D:/tmp/{METROLOGY_CALIBRATION_CONTRACT_FILENAME}",
            "phase_transition_bridge": "D:/tmp/phase_transition_bridge.json",
            "phase_transition_bridge_reviewer_artifact": f"D:/tmp/{PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME}",
        },
    )
    checklist = build_engineering_isolation_admission_checklist(
        run_id="run_checklist_entry",
        step2_readiness_summary=readiness,
        metrology_calibration_contract=metrology,
        phase_transition_bridge=bridge,
        stage_admission_review_pack=pack,
        artifact_paths={
            "step2_readiness_summary": f"D:/tmp/{STEP2_READINESS_SUMMARY_FILENAME}",
            "metrology_calibration_contract": f"D:/tmp/{METROLOGY_CALIBRATION_CONTRACT_FILENAME}",
            "phase_transition_bridge": "D:/tmp/phase_transition_bridge.json",
            "phase_transition_bridge_reviewer_artifact": f"D:/tmp/{PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME}",
            "stage_admission_review_pack": f"D:/tmp/{STAGE_ADMISSION_REVIEW_PACK_FILENAME}",
            "stage_admission_review_pack_reviewer_artifact": f"D:/tmp/{STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME}",
        },
    )

    entry = build_engineering_isolation_admission_checklist_artifact_entry(
        artifact_path=f"D:/tmp/{ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME}",
        reviewer_artifact_path=f"D:/tmp/{ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME}",
        manifest_section={
            **checklist["raw"],
            "path": f"D:/tmp/{ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME}",
            "reviewer_path": f"D:/tmp/{ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME}",
        },
        reviewer_manifest_section={
            "artifact_type": ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_ARTIFACT_KEY,
            "path": f"D:/tmp/{ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME}",
            "summary_text": checklist["display"]["summary_text"],
            "status_line": checklist["display"]["status_line"],
            "current_stage_text": checklist["display"]["current_stage_text"],
            "next_stage_text": checklist["display"]["next_stage_text"],
            "engineering_isolation_text": checklist["display"]["engineering_isolation_text"],
            "real_acceptance_text": checklist["display"]["real_acceptance_text"],
            "execute_now_text": checklist["display"]["execute_now_text"],
            "defer_to_stage3_text": checklist["display"]["defer_to_stage3_text"],
            "blocking_text": checklist["display"]["blocking_text"],
            "warning_text": checklist["display"]["warning_text"],
            "not_real_acceptance_evidence": True,
        },
    )

    assert entry["artifact_key"] == ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_ARTIFACT_KEY
    assert entry["reviewer_artifact_key"] == ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_ARTIFACT_KEY
    assert entry["path"].endswith(ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME)
    assert entry["reviewer_path"].endswith(ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME)
    assert entry["summary_text"] == checklist["display"]["summary_text"]
    assert entry["status_line"] == checklist["display"]["status_line"]
    assert entry["engineering_isolation_text"] == checklist["display"]["engineering_isolation_text"]
    assert entry["real_acceptance_text"] == checklist["display"]["real_acceptance_text"]
    assert entry["execute_now_text"] == checklist["display"]["execute_now_text"]
    assert entry["defer_to_stage3_text"] == checklist["display"]["defer_to_stage3_text"]
    assert "Step 2 tail / Stage 3 bridge" in entry["entry_text"]
    assert "engineering-isolation" in entry["entry_text"]
    assert "不是 real acceptance" in entry["entry_text"]
    assert "不能替代真实计量验证" in entry["entry_text"]
    assert "ready_for_engineering_isolation" not in entry["entry_text"]
    assert "real_acceptance_ready" not in entry["entry_text"]


def test_stage3_real_validation_plan_reuses_existing_checklist_and_pack_wording_without_new_stage_logic() -> None:
    readiness = build_step2_readiness_summary(
        run_id="run_stage3_plan",
        simulation_mode=True,
        config_governance_handoff={
            "simulation_only": True,
            "operator_safe": True,
            "real_port_device_count": 0,
            "engineering_only_flag_count": 0,
            "enabled_engineering_flags": [],
            "risk_markers": [],
            "execution_gate": {"status": "open"},
            "step2_default_workflow_allowed": True,
            "requires_explicit_unlock": False,
        },
    )
    metrology = build_metrology_calibration_contract(
        run_id="run_stage3_plan",
        simulation_mode=True,
        config_governance_handoff={
            "simulation_only": True,
            "real_port_device_count": 0,
            "engineering_only_flag_count": 0,
            "enabled_engineering_flags": [],
        },
    )
    bridge = build_phase_transition_bridge(
        run_id="run_stage3_plan",
        step2_readiness_summary=readiness,
        metrology_calibration_contract=metrology,
    )
    pack = build_stage_admission_review_pack(
        run_id="run_stage3_plan",
        step2_readiness_summary=readiness,
        metrology_calibration_contract=metrology,
        phase_transition_bridge=bridge,
        phase_transition_bridge_reviewer_artifact=build_phase_transition_bridge_reviewer_artifact(bridge),
        artifact_paths={
            "step2_readiness_summary": f"D:/tmp/{STEP2_READINESS_SUMMARY_FILENAME}",
            "metrology_calibration_contract": f"D:/tmp/{METROLOGY_CALIBRATION_CONTRACT_FILENAME}",
            "phase_transition_bridge": "D:/tmp/phase_transition_bridge.json",
            "phase_transition_bridge_reviewer_artifact": f"D:/tmp/{PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME}",
            "stage_admission_review_pack": f"D:/tmp/{STAGE_ADMISSION_REVIEW_PACK_FILENAME}",
            "stage_admission_review_pack_reviewer_artifact": f"D:/tmp/{STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME}",
        },
    )
    checklist = build_engineering_isolation_admission_checklist(
        run_id="run_stage3_plan",
        step2_readiness_summary=readiness,
        metrology_calibration_contract=metrology,
        phase_transition_bridge=bridge,
        stage_admission_review_pack=pack,
        artifact_paths={
            "step2_readiness_summary": f"D:/tmp/{STEP2_READINESS_SUMMARY_FILENAME}",
            "metrology_calibration_contract": f"D:/tmp/{METROLOGY_CALIBRATION_CONTRACT_FILENAME}",
            "phase_transition_bridge": "D:/tmp/phase_transition_bridge.json",
            "phase_transition_bridge_reviewer_artifact": f"D:/tmp/{PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME}",
            "stage_admission_review_pack": f"D:/tmp/{STAGE_ADMISSION_REVIEW_PACK_FILENAME}",
            "stage_admission_review_pack_reviewer_artifact": f"D:/tmp/{STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME}",
        },
    )

    plan = build_stage3_real_validation_plan(
        run_id="run_stage3_plan",
        step2_readiness_summary=readiness,
        metrology_calibration_contract=metrology,
        phase_transition_bridge=bridge,
        stage_admission_review_pack=pack,
        engineering_isolation_admission_checklist=checklist,
        artifact_paths={
            "step2_readiness_summary": f"D:/tmp/{STEP2_READINESS_SUMMARY_FILENAME}",
            "metrology_calibration_contract": f"D:/tmp/{METROLOGY_CALIBRATION_CONTRACT_FILENAME}",
            "phase_transition_bridge": "D:/tmp/phase_transition_bridge.json",
            "phase_transition_bridge_reviewer_artifact": f"D:/tmp/{PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME}",
            "stage_admission_review_pack": f"D:/tmp/{STAGE_ADMISSION_REVIEW_PACK_FILENAME}",
            "stage_admission_review_pack_reviewer_artifact": f"D:/tmp/{STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME}",
            "engineering_isolation_admission_checklist": f"D:/tmp/{ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME}",
            "engineering_isolation_admission_checklist_reviewer_artifact": (
                f"D:/tmp/{ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME}"
            ),
        },
    )

    raw = plan["raw"]
    markdown = plan["markdown"]
    item_map = {item["item_id"]: dict(item) for item in raw["validation_items"]}

    assert plan["artifact_type"] == "stage3_real_validation_plan"
    assert plan["filename"] == STAGE3_REAL_VALIDATION_PLAN_FILENAME
    assert plan["reviewer_filename"] == STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME
    assert raw["artifact_type"] == "stage3_real_validation_plan"
    assert raw["phase"] == checklist["raw"]["phase"]
    assert raw["overall_status"] == checklist["raw"]["overall_status"]
    assert raw["recommended_next_stage"] == checklist["raw"]["recommended_next_stage"]
    assert raw["ready_for_engineering_isolation"] is True
    assert raw["real_acceptance_ready"] is False
    assert raw["artifact_paths"]["stage_admission_review_pack"].endswith(STAGE_ADMISSION_REVIEW_PACK_FILENAME)
    assert raw["artifact_paths"]["engineering_isolation_admission_checklist"].endswith(
        ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME
    )
    assert raw["artifact_paths"]["engineering_isolation_admission_checklist_reviewer_artifact"].endswith(
        ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME
    )
    assert raw["artifact_refs"]["stage_admission_review_pack"]["summary_text"] == pack["display"]["summary_text"]
    assert raw["artifact_refs"]["engineering_isolation_admission_checklist"]["summary_text"] == (
        checklist["display"]["summary_text"]
    )
    assert raw["validation_status_counts"]["blocked_until_stage3"] >= 4
    assert raw["validation_status_counts"]["requires_real_evidence"] >= 3
    assert item_map["dewpoint_reference_enforcement"]["status"] == "blocked_until_stage3"
    assert item_map["real_run_uncertainty_result"]["status"] == "requires_real_evidence"
    assert item_map["coefficient_writeback_readback_acceptance"]["status"] == "not_executable_offline"
    assert item_map["real_acceptance_pass_fail_contract"]["status"] == "planned"
    assert "real_run_uncertainty_result" in raw["pass_fail_contract"]["pass_requires"][2]
    assert plan["display"]["status_line"] == checklist["display"]["status_line"]
    assert plan["display"]["engineering_isolation_text"] == checklist["display"]["engineering_isolation_text"]
    assert plan["display"]["real_acceptance_text"] == checklist["display"]["real_acceptance_text"]
    assert plan["display"]["execute_now_text"] == checklist["display"]["execute_now_text"]
    assert plan["display"]["defer_to_stage3_text"] == checklist["display"]["defer_to_stage3_text"]
    assert "Step 2 tail / Stage 3 bridge" in markdown
    assert "engineering-isolation" in markdown
    assert "第三阶段真实验证" in markdown
    assert "不是 real acceptance" in markdown
    assert "不能替代真实计量验证" in markdown
    assert "本工件只定义第三阶段真实验证计划，不代表验证已完成" in markdown
    assert "step2_readiness_summary.json" in markdown
    assert "metrology_calibration_contract.json" in markdown
    assert "phase_transition_bridge.json" in markdown
    assert "stage_admission_review_pack.json" in markdown
    assert "engineering_isolation_admission_checklist.json" in markdown
    assert "ready_for_engineering_isolation" not in markdown
    assert "real_acceptance_ready" not in markdown

    entry = build_stage3_real_validation_plan_artifact_entry(
        artifact_path=f"D:/tmp/{STAGE3_REAL_VALIDATION_PLAN_FILENAME}",
        reviewer_artifact_path=f"D:/tmp/{STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME}",
        manifest_section={
            **raw,
            "path": f"D:/tmp/{STAGE3_REAL_VALIDATION_PLAN_FILENAME}",
            "reviewer_path": f"D:/tmp/{STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME}",
        },
        reviewer_manifest_section={
            "artifact_type": STAGE3_REAL_VALIDATION_PLAN_REVIEWER_ARTIFACT_KEY,
            "path": f"D:/tmp/{STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME}",
            "summary_text": plan["display"]["summary_text"],
            "status_line": plan["display"]["status_line"],
            "current_stage_text": plan["display"]["current_stage_text"],
            "next_stage_text": plan["display"]["next_stage_text"],
            "engineering_isolation_text": plan["display"]["engineering_isolation_text"],
            "real_acceptance_text": plan["display"]["real_acceptance_text"],
            "execute_now_text": plan["display"]["execute_now_text"],
            "defer_to_stage3_text": plan["display"]["defer_to_stage3_text"],
            "blocking_text": plan["display"]["blocking_text"],
            "warning_text": plan["display"]["warning_text"],
            "plan_boundary_text": plan["display"]["plan_boundary_text"],
            "not_real_acceptance_evidence": True,
        },
        digest_section={
            "overall_status": raw["overall_status"],
            "recommended_next_stage": raw["recommended_next_stage"],
            "validation_status_counts": raw["validation_status_counts"],
            "required_real_world_evidence": raw["required_real_world_evidence"],
            "artifact_paths": raw["artifact_paths"],
        },
        reviewer_markdown_text=markdown,
    )

    assert entry["artifact_key"] == STAGE3_REAL_VALIDATION_PLAN_ARTIFACT_KEY
    assert entry["reviewer_artifact_key"] == STAGE3_REAL_VALIDATION_PLAN_REVIEWER_ARTIFACT_KEY
    assert entry["path"].endswith(STAGE3_REAL_VALIDATION_PLAN_FILENAME)
    assert entry["reviewer_path"].endswith(STAGE3_REAL_VALIDATION_PLAN_REVIEWER_FILENAME)
    assert entry["summary_text"] == plan["display"]["summary_text"]
    assert entry["status_line"] == plan["display"]["status_line"]
    assert entry["current_stage_text"] == plan["display"]["current_stage_text"]
    assert entry["next_stage_text"] == plan["display"]["next_stage_text"]
    assert entry["engineering_isolation_text"] == plan["display"]["engineering_isolation_text"]
    assert entry["real_acceptance_text"] == plan["display"]["real_acceptance_text"]
    assert entry["execute_now_text"] == plan["display"]["execute_now_text"]
    assert entry["defer_to_stage3_text"] == plan["display"]["defer_to_stage3_text"]
    assert entry["reviewer_note_text"] in entry["card_text"]
    assert entry["role_text"] in entry["card_text"]
    assert "Step 2 tail / Stage 3 bridge" in entry["card_text"]
    assert "engineering-isolation" in entry["card_text"]
    assert "第三阶段真实验证证据类别" in entry["card_text"]
    assert "pass/fail contract 摘要" in entry["card_text"]
    assert "Digest：" in entry["card_text"]
    assert "simulation / offline / headless only" in entry["card_text"]
    assert "不是 real acceptance" in entry["card_text"]
    assert "不能替代真实计量验证" in entry["card_text"]
    assert "JSON：D:/tmp/stage3_real_validation_plan.json" in entry["card_text"]
    assert "Markdown：D:/tmp/stage3_real_validation_plan.md" in entry["card_text"]
    assert "真实参考表 / 参考仪器强制执行" in entry["required_evidence_categories_text"]
    assert "真机系数写入 / 回读 / acceptance" in entry["required_evidence_categories_text"]
    assert "ready_for_engineering_isolation" not in entry["entry_text"]
    assert "real_acceptance_ready" not in entry["entry_text"]


# ---------------------------------------------------------------------------
# TestV12CompactSummaryGovernance (2.11)
# ---------------------------------------------------------------------------

class TestV12CompactSummaryGovernance:
    """Verify V1.2 compact summary does not introduce real acceptance language."""

    def test_v12_compact_summary_no_real_acceptance(self):
        """V1.2 compact summary must not contain real acceptance language."""
        from gas_calibrator.v2.core.reviewer_summary_builders import (
            build_v12_alignment_compact_summary,
            V12_COMPACT_SUMMARY_LABELS,
            V12_COMPACT_SUMMARY_LABELS_EN,
        )
        result = build_v12_alignment_compact_summary({})
        joined = " | ".join(result["summary_lines"])

        # Must NOT contain real acceptance language
        assert "real acceptance" not in joined.lower() or "not real acceptance" in joined.lower()
        assert "正式放行" not in joined or "不构成正式放行" in joined

        # Must contain simulated-only note
        assert "仿真" in joined or "Simulated" in joined

    def test_v12_compact_labels_no_formal_claim(self):
        """V1.2 compact labels must not contain formal claim language."""
        from gas_calibrator.v2.core.reviewer_summary_builders import (
            V12_COMPACT_SUMMARY_LABELS,
            V12_COMPACT_SUMMARY_LABELS_EN,
        )
        all_texts = list(V12_COMPACT_SUMMARY_LABELS.values()) + list(V12_COMPACT_SUMMARY_LABELS_EN.values())
        for text in all_texts:
            lower = text.lower()
            assert "formal acceptance" not in lower, f"Formal acceptance in: {text}"
            assert "formal claim" not in lower, f"Formal claim in: {text}"
            assert "正式放行" not in text or "不构成" in text, f"正式放行 without negation in: {text}"

    def test_v12_compact_boundary_markers_step2(self):
        """V1.2 compact summary boundary markers must match Step 2."""
        from gas_calibrator.v2.core.reviewer_summary_builders import build_v12_alignment_compact_summary
        from gas_calibrator.v2.core.phase_evidence_display_contracts import PHASE_EVIDENCE_STEP2_BOUNDARY

        result = build_v12_alignment_compact_summary({})
        markers = result["boundary_markers"]
        assert markers["evidence_source"] == "simulated"
        assert markers["not_real_acceptance_evidence"] is True
        assert markers["not_ready_for_formal_claim"] is True
        assert markers["reviewer_only"] is True
        assert markers["readiness_mapping_only"] is True
