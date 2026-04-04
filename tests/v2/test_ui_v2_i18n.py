from __future__ import annotations

import json
from pathlib import Path

from gas_calibrator.v2.ui_v2.i18n import (
    DEFAULT_LOCALE,
    FALLBACK_LOCALE,
    LOCALES_DIR,
    get_locale,
    set_locale,
    t,
)


def _flatten_keys(payload: dict, prefix: str = "") -> set[str]:
    keys: set[str] = set()
    for key, value in payload.items():
        current = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, dict):
            keys.update(_flatten_keys(value, current))
        else:
            keys.add(current)
    return keys


def test_i18n_defaults_to_chinese_locale() -> None:
    set_locale(None)

    assert DEFAULT_LOCALE == "zh_CN"
    assert get_locale() == "zh_CN"
    assert t("shell.title") != "shell.title"
    assert t("shell.nav.run") != "shell.nav.run"


def test_i18n_falls_back_to_english_when_requested_locale_is_missing() -> None:
    assert FALLBACK_LOCALE == "en_US"
    assert t("shell.title", locale="missing_locale") == "Gas Calibrator V2 Cockpit"
    assert t("common.ready", locale="missing_locale") == "Ready"


def test_zh_cn_locale_covers_en_us_leaf_keys() -> None:
    zh_payload = json.loads((LOCALES_DIR / "zh_CN.json").read_text(encoding="utf-8"))
    en_payload = json.loads((LOCALES_DIR / "en_US.json").read_text(encoding="utf-8"))

    zh_keys = _flatten_keys(zh_payload)
    en_keys = _flatten_keys(en_payload)

    assert en_keys - zh_keys == set()


def test_i18n_covers_compare_review_preset_and_display_profile_labels() -> None:
    set_locale("zh_CN")

    keys = [
        "compare.profile",
        "suite.arg.suite",
        "enum.reference_quality.wrong_unit_configuration",
        "pages.devices.workbench.view.operator_view",
        "pages.devices.workbench.field.preset",
        "pages.devices.workbench.field.display_profile",
        "pages.devices.workbench.preset_center.favorite_button",
        "pages.devices.workbench.preset_center.editor.title",
        "pages.devices.workbench.preset_center.manager.title",
        "pages.devices.workbench.preset_center.manager.conflict_policy_label",
        "pages.devices.workbench.preset_center.manager.conflict_policy_summary",
        "pages.devices.workbench.preset_center.manager.conflict_policy.rename",
        "pages.devices.workbench.preset_center.manager.bundle_format_summary",
        "pages.devices.workbench.preset_center.manager.conflict_strategy_summary",
        "pages.devices.workbench.preset_center.manager.sharing_reserved_fields_summary",
        "pages.devices.workbench.preset_center.manager.bundle_profile_summary",
        "pages.devices.workbench.preset_center.manager.sharing_ready_summary",
        "pages.devices.workbench.preset_center.manager.import_conflict_summary",
        "pages.devices.workbench.preset_center.manager.directory.summary",
        "pages.devices.workbench.preset_center.manager.directory.builtin",
        "pages.devices.workbench.preset_center.manager.error.schema_invalid",
        "pages.devices.workbench.preset_center.origin.import_bundle",
        "pages.devices.workbench.preset_center.detail_capabilities",
        "pages.devices.workbench.preset_center.capability.pressure_gauge",
        "pages.devices.workbench.message.custom_preset_saved",
        "pages.devices.workbench.message.display_profile_context_refreshed",
        "pages.devices.workbench.display_profile_profile.1080p_compact",
        "pages.devices.workbench.display_profile_profile.1440p_standard",
        "pages.devices.workbench.display_profile_profile.4k_standard",
        "pages.devices.workbench.display_profile_profile.ultrawide_standard",
        "pages.devices.workbench.display_profile_family.1080p",
        "pages.devices.workbench.display_profile_family.1440p",
        "pages.devices.workbench.display_profile_family.4k",
        "pages.devices.workbench.display_profile_family.ultrawide",
        "pages.devices.workbench.display_profile_monitor.standard_monitor",
        "pages.devices.workbench.display_profile_resolution.full_hd",
        "pages.devices.workbench.display_profile_resolution.wide_resolution",
        "pages.devices.workbench.display_profile_resolution.ultrawide_resolution",
        "pages.devices.workbench.display_profile_window.standard_window",
        "pages.devices.workbench.display_profile_window.wide_window",
        "pages.devices.workbench.display_profile_reason.manual_dense_1080p",
        "pages.devices.workbench.display_profile_reason.manual_standard_display",
        "pages.devices.workbench.display_profile_reason.default_1080p",
        "pages.devices.workbench.display_profile_reason.wide_resolution",
        "pages.devices.workbench.display_profile_reason.simulated_1440p_canvas",
        "pages.devices.workbench.display_profile_reason.simulated_4k_canvas",
        "pages.devices.workbench.display_profile_reason.simulated_ultrawide_canvas",
        "pages.devices.workbench.display_profile_multi_monitor.single_monitor_baseline",
        "pages.devices.workbench.display_profile_multi_monitor.future_multi_monitor_ready",
        "pages.devices.workbench.engineer_data_state.no_data",
        "pages.devices.workbench.engineer_data_state.diagnostic_only",
        "pages.devices.workbench.engineer_block.route",
        "pages.devices.workbench.engineer_card.statistics",
        "pages.devices.workbench.engineer_card.suite_analytics",
        "pages.devices.workbench.engineer_card.artifact_lineage",
        "pages.devices.workbench.engineer_trend.evidence",
        "pages.devices.workbench.engineer_trend.reference_quality",
        "pages.devices.workbench.engineer_trend.suite_analytics",
        "pages.devices.workbench.engineer_trend.artifact_lineage",
        "pages.devices.workbench.engineer_section.trend_focus.title",
        "pages.devices.workbench.engineer_section.artifact_lineage.title",
        "pages.devices.workbench.engineer_section.suite_analytics.title",
        "pages.devices.workbench.engineer_severity.warning",
        "pages.devices.workbench.snapshot.title",
        "results.review_digest.suite",
        "results.review_center.filter.time",
        "results.review_center.filter.source",
        "results.review_center.filter.active_source",
        "results.review_center.filter.clear_source_drilldown",
        "results.review_center.source_kind.workbench",
        "results.review_center.source_kind.mixed",
        "results.review_center.type.workbench",
        "results.review_center.type.analytics",
        "results.review_center.index.source_kind_summary",
        "results.review_center.index.coverage_summary",
        "results.review_center.index.diagnostics_summary",
        "results.review_center.index.source_drilldown_summary",
        "results.review_center.index.source_scope_count",
        "results.review_center.index.source_disambiguation",
        "results.review_center.scope.readiness_summary",
        "results.review_center.scope.analytics_summary",
        "results.review_center.scope.lineage_summary",
        "results.review_center.scope.no_detail",
        "results.review_center.detail_panel.acceptance",
        "results.review_center.detail_panel.analytics",
        "results.review_center.detail_panel.lineage",
        "results.review_center.detail.acceptance_hint",
        "results.review_center.risk.high",
        "results.review_center.section.run_index",
        "results.review_center.focus.operator_source_summary",
        "results.review_center.focus.reviewer_source_summary",
        "results.review_center.focus.approver_source_summary",
        "widgets.artifact_list.origin",
        "widgets.artifact_list.role_status",
        "widgets.artifact_list.unclassified",
        "widgets.artifact_list.origin_current_run",
        "widgets.artifact_list.origin_review_reference",
        "widgets.artifact_list.origin_source_scan",
        "widgets.artifact_list.origin_missing_reference",
        "widgets.artifact_list.export_status_ok",
        "widgets.artifact_list.export_status_skipped",
        "widgets.artifact_list.export_status_missing",
        "widgets.artifact_list.export_status_error",
        "widgets.artifact_list.export_status_unregistered",
        "widgets.artifact_list.exportability_current_run",
        "widgets.artifact_list.exportability_current_run_missing",
        "widgets.artifact_list.exportability_review_reference",
        "widgets.artifact_list.exportability_source_scan",
        "widgets.artifact_list.exportability_missing_reference",
        "widgets.export_bar.review_manifest",
        "pages.reports.artifact_scope.summary_all",
        "pages.reports.artifact_scope.summary_source",
        "pages.reports.artifact_scope.summary_evidence",
        "pages.reports.artifact_scope.clear",
        "pages.reports.artifact_scope.empty",
        "pages.reports.artifact_scope.disclaimer",
        "pages.reports.artifact_scope.run_dir_note",
        "pages.reports.artifact_scope.catalog_note",
        "pages.reports.artifact_scope.scope_note",
        "pages.reports.artifact_scope.present_note",
        "pages.reports.artifact_scope.export_scope_warning",
        "pages.reports.review_scope_manifest.title",
        "pages.reports.review_scope_manifest.generated_at",
        "pages.reports.review_scope_manifest.scope",
        "pages.reports.review_scope_manifest.selection",
        "pages.reports.review_scope_manifest.counts",
        "pages.reports.review_scope_manifest.rows",
        "pages.reports.review_scope_manifest.note",
        "pages.reports.review_scope_manifest.disclaimer_label",
        "pages.reports.review_scope_manifest.disclaimer",
        "pages.reports.review_scope_manifest.selection_line",
        "pages.reports.review_scope_manifest.counts_line",
        "pages.reports.review_scope_manifest.note_current_run_status",
        "pages.reports.review_scope_manifest.note_current_run_unregistered",
        "pages.reports.review_scope_manifest.note_current_run_missing",
        "pages.reports.review_scope_manifest.note_review_reference",
        "pages.reports.review_scope_manifest.note_source_scan",
        "pages.reports.review_scope_manifest.note_missing_reference",
        "facade.role_summary_item",
        "facade.export_review_manifesting",
        "facade.review_scope_manifest_export_failed",
        "facade.review_scope_manifest_exported",
        "facade.results.overview.run_id",
        "facade.results.algorithm.default",
        "facade.results.result_summary.workbench_evidence",
        "dialogs.preferences.title",
        "pages.reports.artifact_list_title",
    ]

    for key in keys:
        assert t(key) != key
