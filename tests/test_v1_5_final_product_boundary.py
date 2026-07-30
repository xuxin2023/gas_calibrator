from __future__ import annotations

from pathlib import Path

from gas_calibrator.tools.export_v2_module_disposition_inventory import build_inventory

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_long_term_rules_make_v1_5_the_only_final_product() -> None:
    rules = (REPO_ROOT / "AGENTS.md").read_text(encoding="utf-8")

    assert "V1.5 是本仓库唯一最终产品版本" in rules
    assert "未来永远规划和完善 V1.5" in rules
    assert "禁止新增 V2 产品功能" in rules


def test_v1_5_product_paths_do_not_import_v2() -> None:
    payload = build_inventory(REPO_ROOT)

    assert payload["summary"]["v1_5_protected_import_violation_count"] == 0
    assert payload["v1_5_protected_import_violations"] == []


def test_all_extracted_v2_product_page_shells_are_retired() -> None:
    payload = build_inventory(REPO_ROOT)

    assert payload["summary"]["extracted_page_shell_expected_count"] == 5
    assert payload["summary"]["extracted_page_shell_deleted_count"] == 5
    assert payload["summary"]["extracted_page_shell_count"] == 0
    assert payload["summary"]["extracted_page_shell_delete_ready_count"] == 0
    assert payload["extracted_page_shell_audit"] == []


def test_v1_5_workstation_has_no_user_facing_v2_product_label() -> None:
    source = (
        REPO_ROOT
        / "src"
        / "gas_calibrator"
        / "v1_5"
        / "ui"
        / "operator_workstation_app.py"
    ).read_text(encoding="utf-8")

    assert "V2 分析" not in source
    assert "V2 驾驶舱" not in source
    assert '"nav.auxiliary": "分析、报告与证据"' in source


def test_v1_5_gui_and_cli_share_one_application_execution_seam() -> None:
    gui_source = (
        REPO_ROOT
        / "src"
        / "gas_calibrator"
        / "v1_5"
        / "ui"
        / "operator_workstation_app.py"
    ).read_text(encoding="utf-8")
    cli_source = (
        REPO_ROOT
        / "src"
        / "gas_calibrator"
        / "tools"
        / "run_v1_5_operator_workstation_dry_run.py"
    ).read_text(encoding="utf-8")

    shared_name = "run_v1_5_operator_workstation_application"
    assert shared_name in gui_source
    assert shared_name in cli_source
    assert "write_v1_5_operator_workstation_outputs" not in gui_source
    assert "write_v1_5_operator_workstation_outputs" not in cli_source


def test_runtime_package_does_not_ship_uncollected_v2_tests() -> None:
    runtime_tests = REPO_ROOT / "src" / "gas_calibrator" / "v2" / "tests"

    assert not runtime_tests.exists() or not list(runtime_tests.glob("*.py"))


def test_retired_v2_compatibility_paths_do_not_reappear() -> None:
    retired_paths = (
        "src/gas_calibrator/v2/adapters/historical_frame_parity_audit.py",
        "src/gas_calibrator/v2/adapters/method_confirmation_gateway.py",
        "src/gas_calibrator/v2/adapters/recognition_scope_gateway.py",
        "src/gas_calibrator/v2/adapters/software_validation_gateway.py",
        "src/gas_calibrator/v2/adapters/uncertainty_gateway.py",
        "src/gas_calibrator/v2/adapters/wp6_gateway.py",
        "src/gas_calibrator/v2/core/certificate_metrics_registry.py",
        "src/gas_calibrator/v2/ui_v2/pages/certificate_metrics_page.py",
        "src/gas_calibrator/v2/ui_v2/pages/visitor_showcase_page.py",
        "src/gas_calibrator/v2/ui_v2/theme/tokens.py",
        "src/gas_calibrator/v2/ui_v2/theme/ttk_theme.py",
        "src/gas_calibrator/v2/ui_v2/utils/screenshot.py",
        "src/gas_calibrator/v2/ui_v2/widgets/scrollable_page_frame.py",
        "src/gas_calibrator/v2/storage/coefficient_store.py",
        "src/gas_calibrator/v2/storage/database.py",
        "src/gas_calibrator/v2/storage/exporter.py",
        "src/gas_calibrator/v2/storage/import_run.py",
        "src/gas_calibrator/v2/storage/__init__.py",
        "src/gas_calibrator/v2/adapters/v1_sidecar_watcher.py",
        "src/gas_calibrator/v2/scripts/v1_postprocess_gui.py",
        "src/gas_calibrator/v2/scripts/launch_v1_postprocess_gui.cmd",
        "run_v1_postprocess.py",
        "src/gas_calibrator/tools/run_v1_no500_postprocess.py",
        "src/gas_calibrator/v2/adapters/v1_postprocess_runner.py",
        "src/gas_calibrator/v2/adapters/offline_refit_runner.py",
        "src/gas_calibrator/v2/config/offline_modeling.py",
        "src/gas_calibrator/v2/core/refit_filtering.py",
        "src/gas_calibrator/v2/analytics/feature_builder.py",
        "src/gas_calibrator/v2/analytics/exporters.py",
        "src/gas_calibrator/v2/analytics/service.py",
        "src/gas_calibrator/v2/analytics/marts/__init__.py",
        "src/gas_calibrator/v2/analytics/marts/run_kpis.py",
        "src/gas_calibrator/v2/analytics/marts/point_kpis.py",
        "src/gas_calibrator/v2/analytics/marts/drift_metrics.py",
        "src/gas_calibrator/v2/analytics/marts/control_charts.py",
        "src/gas_calibrator/v2/analytics/marts/fault_attribution.py",
        "src/gas_calibrator/v2/analytics/marts/traceability.py",
        "src/gas_calibrator/v2/analytics/measurement/__init__.py",
        "src/gas_calibrator/v2/analytics/measurement/service.py",
        "src/gas_calibrator/v2/analytics/measurement/schemas.py",
        "src/gas_calibrator/v2/analytics/measurement/exporters.py",
        "src/gas_calibrator/v2/analytics/measurement/feature_builder.py",
        "src/gas_calibrator/v2/analytics/measurement/marts/__init__.py",
        "src/gas_calibrator/v2/analytics/measurement/marts/measurement_quality.py",
        "src/gas_calibrator/v2/analytics/measurement/marts/measurement_drift.py",
        "src/gas_calibrator/v2/analytics/measurement/marts/signal_anomaly.py",
        "src/gas_calibrator/v2/analytics/measurement/marts/context_attribution.py",
        "src/gas_calibrator/v2/analytics/__init__.py",
        "src/gas_calibrator/v2/analytics/sidecar_views.py",
        "src/gas_calibrator/v2/intelligence/review_copilot.py",
        "src/gas_calibrator/storage/sidecar_index.py",
        "src/gas_calibrator/v2/adapters/results_gateway.py",
        "src/gas_calibrator/v2/ui_v2/artifact_registry_governance.py",
        "src/gas_calibrator/v2/core/engineering_isolation_gate_artifact_entry.py",
        "src/gas_calibrator/v2/core/engineering_isolation_gate_repository.py",
        "src/gas_calibrator/v2/core/step2_closeout_repository.py",
        "src/gas_calibrator/v2/core/stage_admission_review_pack_artifact_entry.py",
        "src/gas_calibrator/v2/core/stage3_standards_alignment_matrix_artifact_entry.py",
        "src/gas_calibrator/v2/core/phase_transition_bridge_reviewer_artifact_entry.py",
        "src/gas_calibrator/v2/core/engineering_isolation_admission_checklist_artifact_entry.py",
        "src/gas_calibrator/v2/core/stage3_real_validation_plan_artifact_entry.py",
        "src/gas_calibrator/v2/core/phase_transition_bridge_presenter.py",
        "src/gas_calibrator/v2/scripts/verify_v1_v2_h2o_only_replacement.py",
        "src/gas_calibrator/v2/scripts/verify_v1_v2_skip0_co2_only_diagnostic_relaxed.py",
        "src/gas_calibrator/v2/scripts/verify_v1_v2_skip0_co2_only_replacement.py",
        "src/gas_calibrator/v2/scripts/verify_v1_v2_skip0_replacement.py",
        "src/gas_calibrator/v2/core/workflow_steps/co2_route.py",
        "src/gas_calibrator/v2/core/workflow_steps/finalize.py",
        "src/gas_calibrator/v2/core/workflow_steps/h2o_route.py",
        "src/gas_calibrator/v2/core/workflow_steps/precheck.py",
        "src/gas_calibrator/v2/core/workflow_steps/sampling.py",
        "src/gas_calibrator/v2/core/workflow_steps/startup.py",
        "src/gas_calibrator/v2/core/workflow_steps/temperature_group.py",
        "src/gas_calibrator/v2/domain/point_models.py",
        "src/gas_calibrator/v2/domain/run_models.py",
        "src/gas_calibrator/v2/domain/enums.py",
        "src/gas_calibrator/v2/domain/qc_models.py",
        "src/gas_calibrator/v2/domain/result_models.py",
        "src/gas_calibrator/v2/domain/sample_models.py",
        "src/gas_calibrator/v2/algorithms/registry.py",
        "src/gas_calibrator/v2/algorithms/result_types.py",
        "src/gas_calibrator/v2/core/orchestration_context.py",
        "src/gas_calibrator/v2/core/event_bus.py",
        "src/gas_calibrator/v2/core/route_context.py",
        "src/gas_calibrator/v2/core/session.py",
        "src/gas_calibrator/v2/domain/explanation_models.py",
        "src/gas_calibrator/v2/domain/mode_models.py",
        "src/gas_calibrator/v2/core/runners/route_run_result.py",
        "src/gas_calibrator/v2/intelligence/advisors/algorithm_advisor.py",
        "src/gas_calibrator/v2/intelligence/advisors/anomaly_advisor.py",
        "src/gas_calibrator/v2/intelligence/context_builders/fit_context.py",
        "src/gas_calibrator/v2/intelligence/context_builders/qc_context.py",
        "src/gas_calibrator/v2/intelligence/context_builders/run_context.py",
        "src/gas_calibrator/v2/intelligence/explainers/fit_explainer.py",
        "src/gas_calibrator/v2/intelligence/explainers/qc_explainer.py",
        "src/gas_calibrator/v2/intelligence/explainers/run_explainer.py",
        "src/gas_calibrator/v2/intelligence/runtime.py",
        "src/gas_calibrator/v2/intelligence/prompts/__init__.py",
        "src/gas_calibrator/v2/review_surface_formatter.py",
        "src/gas_calibrator/v2/sim/certificate_operational_admission.py",
        "src/gas_calibrator/v2/sim/devices/models.py",
        "src/gas_calibrator/v2/core/services/ai_explanation_service.py",
        "src/gas_calibrator/v2/calibration/__init__.py",
        "src/gas_calibrator/v2/calibration/temperature_compensation.py",
        "src/gas_calibrator/v2/export/temperature_compensation_export.py",
        "src/gas_calibrator/v2/storage/import_v1_5_initialization.py",
        "src/gas_calibrator/v2/storage/import_v1_5_readiness_events.py",
        "src/gas_calibrator/v2/storage/importer.py",
        "src/gas_calibrator/v2/storage/models.py",
        "src/gas_calibrator/v2/storage/queries.py",
        "src/gas_calibrator/v2/storage/sidecar_index.py",
        "src/gas_calibrator/v2/storage/v1_5_initialization.py",
        "src/gas_calibrator/v2/utils/__init__.py",
        "src/gas_calibrator/v2/utils/converters.py",
    )

    assert all(not (REPO_ROOT / path).exists() for path in retired_paths)
