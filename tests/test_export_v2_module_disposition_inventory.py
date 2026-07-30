from __future__ import annotations

import json
from pathlib import Path

from gas_calibrator.tools.export_v2_module_disposition_inventory import (
    FINAL_VALIDATION_MIGRATION_MODULES,
    VALIDATION_CONFIGURATION_MODULES,
    VALIDATION_ENGINEERING_PROBE_MODULES,
    VALIDATION_METROLOGY_MODULES,
    VALIDATION_SIMULATION_RUNTIME_MODULES,
    build_inventory,
    classify_module,
    write_inventory,
)


def _write(path: Path, text: str = "") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_classification_routes_v2_assets_into_final_v1_5_ownership() -> None:
    assert (
        classify_module(
            "gas_calibrator.v2.ui_v2.pages.results_page",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.ui_v2.pages.reports_page",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.ui_v2.pages.qc_page",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.ui_v2.pages.devices_page",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.ui_v2.pages.algorithms_page",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.ui_v2.widgets.device_workbench",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.ui_v2.controllers.device_workbench",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.ui_v2.widgets.analyzer_health_panel",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.ui_v2.widgets.device_status_table",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.ui_v2.widgets.collapsible_section",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.ui_v2.widgets.metric_card",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.ui_v2.widgets.algorithm_compare_table",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.ui_v2.widgets.qc_overview_panel",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.ui_v2.widgets.qc_reject_reason_chart",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.ui_v2.widgets.winner_badge",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.ui_v2.widgets.residual_chart",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.ui_v2.widgets.export_bar",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.ui_v2.pages.plan_editor_page",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.storage",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.storage.profile_store",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.storage.exporter",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.storage.import_run",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.algorithms.robust",
            static_zero_reference=False,
        )[0]
        == "migrate_to_modeling"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.ui_v2.shell",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.scripts.run_v2",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.ui_v2.pages.run_control_page",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )


def test_runtime_tests_and_parallel_execution_have_explicit_exit_paths() -> None:
    assert (
        classify_module(
            "gas_calibrator.v2.tests.test_real_com_probe_gate",
            static_zero_reference=True,
        )[0]
        == "delete_now"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.core.orchestrator",
            static_zero_reference=False,
        )[0]
        == "migrate_to_validation"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.core.real_com_probe_gate",
            static_zero_reference=True,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.scripts.run001_a2_no_write_pressure_sweep",
            static_zero_reference=True,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.scripts.query_only_com_sanity_probe",
            static_zero_reference=True,
        )[0]
        == "retain_step3a_r0"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.core.run001_query_only_real_com_probe",
            static_zero_reference=False,
        )[0]
        == "retain_step3a_r0"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.core.run001_a2_no_write",
            static_zero_reference=False,
        )[0]
        == "migrate_to_validation"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.core.query_only_readers",
            static_zero_reference=False,
        )[0]
        == "retain_step3a_r0"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.core.run001_r0_1_reference_read_probe",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.scripts.audit_historical_frame_parity",
            static_zero_reference=True,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.scripts.build_certificate_evidence_census",
            static_zero_reference=True,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.core.certificate_evidence_census",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.scripts.build_certificate_operational_admission",
            static_zero_reference=True,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.core.certificate_operational_admission",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.scripts.build_regression_scoreboard",
            static_zero_reference=True,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.core.regression_scoreboard",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.core.golden_dataset_registry",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.scripts.build_rs485_v1_v2_alignment_matrix",
            static_zero_reference=True,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.core.run001_rs485_alignment",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.adapters.v1_sidecar_watcher",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.scripts.v1_postprocess_gui",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.core.run001_a1_analyzer_id_truth",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.core.run001_a1_analyzer_mapping",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.core.cutover_candidate_worksheet",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.core.run001_a1_analyzer_diagnostics",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.core.run001_a1_analyzer_mode2_setup",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    assert (
        classify_module(
            "gas_calibrator.v2.core.run001_a1_serial_assistant_probe",
            static_zero_reference=False,
        )[0]
        == "delete_after_extraction"
    )
    for retired_module in (
        "gas_calibrator.v2.adapters.historical_frame_parity_audit",
        "gas_calibrator.v2.core.certificate_metrics_registry",
        "gas_calibrator.v2.ui_v2.dialogs.about_dialog",
        "gas_calibrator.v2.ui_v2.dialogs.licenses_dialog",
        "gas_calibrator.v2.ui_v2.dialogs.preferences_dialog",
        "gas_calibrator.v2.ui_v2.dialogs.release_notes_dialog",
        "gas_calibrator.v2.ui_v2.diagnostics.redact_helpers",
        "gas_calibrator.v2.ui_v2.runtime.build_info_loader",
        "gas_calibrator.v2.ui_v2.runtime.crash_recovery",
        "gas_calibrator.v2.ui_v2.runtime.recovery_store",
        "gas_calibrator.v2.ui_v2.runtime.release_notes_loader",
        "gas_calibrator.v2.ui_v2.review_center_artifact_scope",
        "gas_calibrator.v2.ui_v2.review_center_scan_contracts",
        "gas_calibrator.v2.ui_v2.review_scope_export_index",
        "gas_calibrator.v2.ui_v2.styles",
        "gas_calibrator.v2.ui_v2.pages.certificate_metrics_page",
        "gas_calibrator.v2.ui_v2.pages.visitor_showcase_page",
        "gas_calibrator.v2.ui_v2.theme.tokens",
        "gas_calibrator.v2.ui_v2.theme.ttk_theme",
        "gas_calibrator.v2.storage.coefficient_store",
        "gas_calibrator.v2.storage.database",
        "gas_calibrator.v2.storage.exporter",
        "gas_calibrator.v2.storage.import_run",
        "gas_calibrator.v2.storage.import_v1_5_initialization",
        "gas_calibrator.v2.storage.import_v1_5_readiness_events",
        "gas_calibrator.v2.storage.importer",
        "gas_calibrator.v2.storage.models",
        "gas_calibrator.v2.storage.queries",
        "gas_calibrator.v2.storage.sidecar_index",
        "gas_calibrator.v2.storage",
        "gas_calibrator.v2.storage.v1_5_initialization",
        "gas_calibrator.v2.adapters.v1_sidecar_watcher",
        "gas_calibrator.v2.scripts.v1_postprocess_gui",
        "gas_calibrator.v2.utils",
        "gas_calibrator.v2.utils.converters",
        "gas_calibrator.v2.ui_v2.utils.screenshot",
        "gas_calibrator.v2.ui_v2.utils.app_info",
        "gas_calibrator.v2.ui_v2.utils.preferences_store",
        "gas_calibrator.v2.ui_v2.utils.recent_runs_store",
        "gas_calibrator.v2.ui_v2.utils.route_memory",
        "gas_calibrator.v2.ui_v2.utils.runtime_paths",
        "gas_calibrator.v2.ui_v2.widgets.busy_overlay",
        "gas_calibrator.v2.ui_v2.widgets.error_banner",
        "gas_calibrator.v2.ui_v2.widgets.empty_state",
        "gas_calibrator.v2.ui_v2.widgets.log_panel",
        "gas_calibrator.v2.ui_v2.widgets.notification_center",
        "gas_calibrator.v2.ui_v2.widgets.route_progress_timeline",
        "gas_calibrator.v2.ui_v2.widgets.scrollable_page_frame",
        "gas_calibrator.v2.ui_v2.widgets.timeseries_chart",
    ):
        assert classify_module(retired_module, static_zero_reference=False)[0] == "delete_after_extraction"
    assert (
        classify_module(
            "gas_calibrator.v2.sim.parity",
            static_zero_reference=False,
        )[0]
        == "migrate_to_validation"
    )


def test_validation_config_disposition_preserves_v1_fit_and_shared_dry_anchor_contracts() -> None:
    disposition, reason = classify_module(
        "gas_calibrator.v2.config.models",
        static_zero_reference=False,
    )

    assert disposition == "migrate_to_validation"
    assert "V2-only order/signal_keys coefficient mirrors" in reason
    assert "raw V1 AMT order/signal_keys" in reason
    assert "V2-only gas-level summary temperature/pressure mirrors are retired" in reason
    assert "raw V1 gas-level summary-column contracts remain production-owned" in reason
    assert "offline ratio-poly coefficient/report configuration subclosure is validation-owned" in reason
    assert "report_temperature_key/report_pressure_key" in reason
    assert "shared dewpoint-backed dry-anchor gate" in reason
    assert "active nine-field valve snapshot subclosure is validation-owned" in reason
    assert "pure Step 2 port/engineering-flag risk-inventory" in reason
    assert "classification/badge/inventory-detail presentation" in reason
    assert "pure run-mode alias normalization" in reason
    assert "pure sensor-precheck normalization" in reason
    assert "hydrate/review/governance-handoff leaves are validation-owned" in reason
    assert "raw valve-route dictionary" in reason
    assert "physical mapping remain frozen" in reason


def test_final_validation_migration_groups_are_complete_and_disjoint() -> None:
    groups = (
        VALIDATION_CONFIGURATION_MODULES,
        VALIDATION_SIMULATION_RUNTIME_MODULES,
        VALIDATION_ENGINEERING_PROBE_MODULES,
        VALIDATION_METROLOGY_MODULES,
    )

    assert len(FINAL_VALIDATION_MIGRATION_MODULES) == 15
    assert sum(len(group) for group in groups) == 15
    assert set().union(*groups) == FINAL_VALIDATION_MIGRATION_MODULES
    for index, group in enumerate(groups):
        assert all(group.isdisjoint(other) for other in groups[index + 1 :])
    assert all(
        classify_module(module, static_zero_reference=False)[0]
        == "migrate_to_validation"
        for module in FINAL_VALIDATION_MIGRATION_MODULES
    )


def test_inventory_counts_relative_imports_and_blocks_v1_5_v2_imports(
    tmp_path: Path,
) -> None:
    _write(tmp_path / "src/gas_calibrator/v2/__init__.py")
    _write(tmp_path / "src/gas_calibrator/v2/storage/__init__.py")
    _write(tmp_path / "src/gas_calibrator/v2/storage/database.py", "VALUE = 1\n")
    _write(
        tmp_path / "src/gas_calibrator/v2/storage/queries.py",
        "from .database import VALUE\n",
    )
    _write(
        tmp_path / "tests/test_queries.py",
        "from gas_calibrator.v2.storage.queries import VALUE\n",
    )
    _write(
        tmp_path / "src/gas_calibrator/v1_5/runtime.py",
        "from gas_calibrator.v2.storage import queries\n",
    )
    _write(
        tmp_path / "src/gas_calibrator/v2/sim/__init__.py",
        (
            '_EXPORTS = {"build": '
            '("gas_calibrator.v2.sim.protocol", "build_protocol")}\n'
        ),
    )
    _write(
        tmp_path / "src/gas_calibrator/v2/sim/protocol.py",
        "def build_protocol():\n    return {}\n",
    )
    _write(
        tmp_path / "src/gas_calibrator/v2/analytics/__init__.py",
        ('_LAZY_EXPORTS = {"AnalyticsService": (".service", "AnalyticsService")}\n'),
    )
    _write(
        tmp_path / "src/gas_calibrator/v2/analytics/service.py",
        "class AnalyticsService:\n    pass\n",
    )
    _write(
        tmp_path / "src/gas_calibrator/v2/core/__init__.py",
        ('_EXPORT_MAP = {"PlanCompiler": (".plan_compiler", "PlanCompiler")}\n'),
    )
    _write(
        tmp_path / "src/gas_calibrator/v2/core/plan_compiler.py",
        "class PlanCompiler:\n    pass\n",
    )
    _write(
        tmp_path / "src/gas_calibrator/v2/scripts/consumer.py",
        (
            "from gas_calibrator.v2.analytics import AnalyticsService\n"
            "from gas_calibrator.v2.core import PlanCompiler\n"
            "from gas_calibrator.v2.sim import build\n"
        ),
    )
    _write(
        tmp_path / "src/gas_calibrator/v2/scripts/query_only_com_sanity_probe.py",
        "ENTRYPOINT = True\n",
    )

    payload = build_inventory(tmp_path)
    rows = {row["module"]: row for row in payload["modules"]}

    assert (
        rows["gas_calibrator.v2.storage.database"]["v2_internal_reference_count"] == 1
    )
    assert rows["gas_calibrator.v2.storage.queries"]["test_reference_count"] == 1
    assert rows["gas_calibrator.v2.sim.protocol"]["v2_internal_reference_count"] == 1
    assert rows["gas_calibrator.v2.sim.protocol"]["static_zero_reference"] is False
    assert (
        rows["gas_calibrator.v2.analytics.service"]["v2_internal_reference_count"] == 1
    )
    assert rows["gas_calibrator.v2.analytics.service"]["static_zero_reference"] is False
    assert (
        rows["gas_calibrator.v2.core.plan_compiler"]["v2_internal_reference_count"] == 1
    )
    assert (
        rows["gas_calibrator.v2.core.plan_compiler"]["static_zero_reference"] is False
    )
    query_entry = rows["gas_calibrator.v2.scripts.query_only_com_sanity_probe"]
    assert query_entry["static_zero_reference"] is True
    assert query_entry["intentional_external_entrypoint"] is True
    assert query_entry["explained_static_zero_reference"] is True
    assert query_entry["unexplained_static_zero_reference"] is False
    assert query_entry["disposition"] == "retain_step3a_r0"
    package_root = rows["gas_calibrator.v2"]
    assert package_root["implicit_package_initializer"] is True
    assert package_root["explained_static_zero_reference"] is True
    assert payload["summary"]["intentional_external_entrypoint_count"] == 1
    assert payload["summary"]["explained_static_zero_reference_count"] >= 2
    assert payload["summary"]["v1_5_protected_import_violation_count"] >= 1
    assert payload["summary"]["final_validation_group_counts"] == {
        "simulation_config": 0,
        "simulation_runtime": 1,
        "engineering_probe": 0,
        "metrology": 0,
    }
    assert payload["summary"]["final_validation_migration_count"] == 1
    assert all(
        row["delete_allowed"] is False
        for row in rows.values()
        if row["disposition"] != "delete_now"
    )


def test_inventory_blocks_shared_validation_from_importing_v2(
    tmp_path: Path,
) -> None:
    _write(tmp_path / "src/gas_calibrator/v2/__init__.py")
    _write(tmp_path / "src/gas_calibrator/v2/analytics/__init__.py")
    _write(
        tmp_path / "src/gas_calibrator/v2/analytics/legacy_health.py",
        "VALUE = 1\n",
    )
    _write(
        tmp_path / "src/gas_calibrator/validation/analyzer_health.py",
        "from gas_calibrator.v2.analytics.legacy_health import VALUE\n",
    )

    payload = build_inventory(tmp_path)

    assert payload["v1_5_protected_import_violations"] == [
        {
            "path": "src\\gas_calibrator\\validation\\analyzer_health.py",
            "import_name": "gas_calibrator.v2.analytics.legacy_health",
        },
        {
            "path": "src\\gas_calibrator\\validation\\analyzer_health.py",
            "import_name": "gas_calibrator.v2.analytics.legacy_health.VALUE",
        },
    ]


def test_inventory_blocks_extracted_page_deletion_while_callers_remain(
    tmp_path: Path,
) -> None:
    _write(tmp_path / "src/gas_calibrator/v2/__init__.py")
    _write(tmp_path / "src/gas_calibrator/v2/ui_v2/__init__.py")
    _write(tmp_path / "src/gas_calibrator/v2/ui_v2/pages/__init__.py")
    _write(
        tmp_path / "src/gas_calibrator/v2/ui_v2/pages/devices_page.py",
        "class DevicesPage:\n    pass\n",
    )
    _write(
        tmp_path / "src/gas_calibrator/v2/ui_v2/shell.py",
        "from .pages.devices_page import DevicesPage\n",
    )
    _write(
        tmp_path / "tests/test_devices_page.py",
        "from gas_calibrator.v2.ui_v2.pages.devices_page import DevicesPage\n",
    )

    payload = build_inventory(tmp_path)
    audit = {row["module"]: row for row in payload["extracted_page_shell_audit"]}
    row = audit["gas_calibrator.v2.ui_v2.pages.devices_page"]

    assert row["delete_ready"] is False
    assert row["runtime_callers"] == ["src\\gas_calibrator\\v2\\ui_v2\\shell.py"]
    assert row["test_callers"] == ["tests\\test_devices_page.py"]
    assert row["blockers"] == [
        "runtime_callers_present",
        "tests_still_reference_page_shell",
    ]
    assert payload["summary"]["extracted_page_shell_count"] == 1
    assert payload["summary"]["extracted_page_shell_expected_count"] == 5
    assert payload["summary"]["extracted_page_shell_deleted_count"] == 4
    assert payload["summary"]["extracted_page_shell_delete_ready_count"] == 0


def test_write_inventory_emits_csv_json_and_markdown(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    _write(repo_root / "src/gas_calibrator/v2/__init__.py")
    _write(repo_root / "src/gas_calibrator/v2/ui_v2/shell.py", "VALUE = 1\n")
    payload = build_inventory(repo_root)

    outputs = write_inventory(payload, tmp_path / "output")

    assert Path(outputs["modules_csv"]).is_file()
    summary_path = Path(outputs["summary_json"])
    assert json.loads(summary_path.read_text(encoding="utf-8"))["schema"].endswith(
        "_v4"
    )
    assert "Automatic deletion permitted: `False`" in Path(
        outputs["summary_md"]
    ).read_text(encoding="utf-8")
