from __future__ import annotations

import json
from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.i18n import t
from gas_calibrator.v2.ui_v2.widgets.device_workbench import DeviceWorkbench

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import build_fake_facade, make_root


def test_engineer_summary_exposes_cards_status_blocks_and_trends(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    run_dir = Path(facade.result_store.run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "suite_summary.json").write_text(
        json.dumps(
            {
                "suite": "regression",
                "counts": {"passed": 5, "total": 5},
                "all_passed": True,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (run_dir / "analytics_summary.json").write_text(
        json.dumps(
            {
                "analyzer_coverage": {"coverage_text": "1/1"},
                "reference_quality_statistics": {"reference_quality": "degraded", "reference_quality_trend": "drift"},
                "export_resilience_status": {"overall_status": "degraded"},
                "digest": {"summary": "coverage 1/1 | degraded", "health": "attention"},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (run_dir / "lineage_summary.json").write_text(
        json.dumps(
            {
                "config_version": "cfg-001",
                "points_version": "pts-001",
                "profile_version": "profile-001",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    facade.execute_device_workbench_action("relay", "run_preset", preset_id="route_h2o", relay_name="relay")
    facade.execute_device_workbench_action("pressure_gauge", "run_preset", preset_id="wrong_unit")
    facade.execute_device_workbench_action(
        "workbench",
        "generate_diagnostic_evidence",
        current_device="pressure_gauge",
        current_action="run_preset",
    )

    engineer = facade.get_device_workbench_snapshot()["engineer_summary"]
    card_titles = {str(item.get("title") or "") for item in engineer["cards"]}
    trend_blocks = [dict(item) for item in engineer["trend_blocks"]]
    trend_titles = {str(item.get("title") or "") for item in trend_blocks}
    suite_card = next(
        item for item in engineer["cards"] if str(item.get("title") or "") == t("pages.devices.workbench.engineer_card.suite_analytics")
    )
    suite_trend = next(
        item for item in trend_blocks if str(item.get("title") or "") == t("pages.devices.workbench.engineer_trend.suite_analytics")
    )

    assert len(engineer["cards"]) >= 6
    assert len(engineer["status_blocks"]) >= 4
    assert len(engineer["trend_blocks"]) >= 7
    assert all(str(item.get("title") or "").strip() for item in engineer["cards"])
    assert all(str(item.get("summary") or "").strip() for item in engineer["cards"])
    assert all(str(item.get("title") or "").strip() for item in engineer["status_blocks"])
    assert all(str(item.get("value") or "").strip() for item in engineer["trend_blocks"])
    assert all(str(item.get("severity_display") or "").strip() for item in engineer["status_blocks"])
    assert engineer["sections"]
    assert all(str(item.get("title") or "").strip() for item in engineer["sections"])
    assert any(bool(item.get("expanded", False)) for item in engineer["sections"])
    assert t("pages.devices.workbench.engineer_card.statistics") in card_titles
    assert t("pages.devices.workbench.engineer_card.suite_analytics") in card_titles
    assert t("pages.devices.workbench.engineer_card.artifact_lineage") in card_titles
    assert "1/1" in str(suite_card.get("summary") or "")
    assert "degraded" in str(suite_card.get("summary") or "")
    assert t("pages.devices.workbench.engineer_trend.evidence") in trend_titles
    assert t("pages.devices.workbench.engineer_trend.devices") in trend_titles
    assert t("pages.devices.workbench.engineer_trend.reference_quality") in trend_titles
    assert t("pages.devices.workbench.engineer_trend.suite_analytics") in trend_titles
    assert t("pages.devices.workbench.engineer_trend.artifact_lineage") in trend_titles
    assert "degraded" in str(suite_trend.get("note") or "")
    assert "drift" in str(suite_trend.get("note") or "")
    assert any(
        str(item.get("title") or "") == t("pages.devices.workbench.engineer_trend.reference_quality")
        and str(item.get("note") or "").strip()
        for item in trend_blocks
    )
    assert any(str(item.get("id") or "").strip() == "trend_focus" for item in engineer["sections"])
    assert any(str(item.get("id") or "").strip() == "exception_focus" for item in engineer["sections"])
    assert any(str(item.get("id") or "").strip() == "artifact_lineage" for item in engineer["sections"])
    assert any(str(item.get("id") or "").strip() == "suite_analytics" for item in engineer["sections"])
    suite_section = next(item for item in engineer["sections"] if str(item.get("id") or "") == "suite_analytics")
    assert "覆盖 1/1" in str(suite_section.get("body_text") or "")
    assert "drift" in str(suite_section.get("body_text") or "")
    assert "cfg-001" in str(suite_section.get("body_text") or "")


def test_engineer_summary_treats_missing_suite_analytics_and_lineage_as_no_data(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    run_dir = Path(facade.result_store.run_dir)
    for name in (
        "suite_summary.json",
        "suite_analytics_summary.json",
        "analytics_summary.json",
        "lineage_summary.json",
    ):
        path = run_dir / name
        if path.exists():
            path.unlink()

    engineer = facade.get_device_workbench_snapshot()["engineer_summary"]
    suite_card = next(
        item for item in engineer["cards"] if str(item.get("title") or "") == t("pages.devices.workbench.engineer_card.suite_analytics")
    )
    suite_trend = next(
        item for item in engineer["trend_blocks"] if str(item.get("title") or "") == t("pages.devices.workbench.engineer_trend.suite_analytics")
    )
    suite_section = next(item for item in engineer["sections"] if str(item.get("id") or "") == "suite_analytics")
    no_data_display = t("pages.devices.workbench.engineer_data_state.no_data")
    failed_display = t("pages.devices.workbench.engineer_data_state.failed")

    assert no_data_display in str(suite_card.get("summary") or "")
    assert no_data_display in str(suite_trend.get("value") or "")
    assert no_data_display in str(suite_trend.get("note") or "")
    assert no_data_display in str(suite_section.get("body_text") or "")
    assert failed_display not in str(suite_card.get("summary") or "")
    assert failed_display not in str(suite_trend.get("value") or "")
    assert failed_display not in str(suite_trend.get("note") or "")
    assert failed_display not in str(suite_section.get("body_text") or "")
    assert engineer["diagnostics"]["suite_analytics_state"] == {
        "suite": "no_data",
        "analytics": "no_data",
        "lineage": "no_data",
    }


def test_device_workbench_renders_engineer_blocks_and_respects_view_layering(tmp_path: Path) -> None:
    root = make_root()
    try:
        facade = build_fake_facade(tmp_path)
        widget = DeviceWorkbench(root, facade=facade)

        widget.render(facade.get_device_workbench_snapshot())
        assert widget.engineer_frame.winfo_manager() == ""
        assert widget.preset_manager_section.winfo_manager() == ""

        facade.execute_device_workbench_action("workbench", "set_view_mode", view_mode="engineer_view")
        facade.execute_device_workbench_action("workbench", "set_display_profile", display_profile="dense_1080p")
        widget.render(facade.get_device_workbench_snapshot())

        assert widget.engineer_frame.winfo_manager() != ""
        assert widget.engineer_status_frame.winfo_children()
        assert widget.preset_manager_section.winfo_manager() != ""
        summary_text = str(widget.preset_manager_summary_var.get() or "").strip()
        manager = facade.get_device_workbench_snapshot()["workbench"]["preset_center"]["manager"]
        assert summary_text
        assert str(manager.get("selected_preset_metadata_summary") or "") in summary_text
        assert str(manager.get("selected_preset_capability_summary") or "") in summary_text
        assert str(manager.get("conflict_policy_summary") or "") in summary_text
        assert str(manager.get("sharing_reserved_fields_summary") or "") in summary_text
        assert str(manager.get("bundle_profile_summary") or "") in summary_text
        assert widget.layout_mode_var.get()
        assert widget.display_profile_var.get()
    finally:
        root.destroy()
