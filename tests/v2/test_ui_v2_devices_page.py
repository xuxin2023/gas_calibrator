import sys
from pathlib import Path

from gas_calibrator.v2.ui_v2.i18n import t
from gas_calibrator.v2.ui_v2.pages.devices_page import DevicesPage

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import build_fake_facade, make_root


def test_devices_page_displays_snapshot(tmp_path: Path) -> None:
    root = make_root()
    try:
        facade = build_fake_facade(tmp_path)
        page = DevicesPage(root, facade=facade)
        page.render(facade.build_snapshot()["devices"])
        assert page.page_scaffold is not None
        assert page.enabled_card.value_var.get() == "2"
        assert len(page.table.tree.get_children()) >= 1
        assert len(page.health_panel.tree.get_children()) >= 1
        assert page.workbench.banner_var.get() == t("pages.devices.workbench.banner.simulation_mode")
        assert len(page.workbench.notebook.tabs()) == 7
        assert page.workbench.analyzer_selector.get() == "1"
        assert page.workbench.relay_tree["columns"] == ("channel", "desired", "actual", "input", "mapping")
        assert page.workbench.operator_view_button["text"] == t("pages.devices.workbench.view.operator_view")
        assert page.workbench.generate_evidence_button["text"] == t("pages.devices.workbench.button.generate_evidence")
        assert page.workbench.compact_layout_button["text"] == t("pages.devices.workbench.layout.compact")
        assert page.workbench.preset_group_selector["values"]
        assert page.workbench.engineer_frame.winfo_manager() == ""
        assert page.workbench.preset_manager_section.winfo_manager() == ""

        facade.execute_device_workbench_action("workbench", "set_view_mode", view_mode="engineer_view")
        facade.execute_device_workbench_action("pressure_gauge", "run_preset", preset_id="wrong_unit")
        facade.execute_device_workbench_action(
            "workbench",
            "generate_diagnostic_evidence",
            current_device="pressure_gauge",
            current_action="run_preset",
        )
        page.workbench.render(facade.get_device_workbench_snapshot())
        page.workbench._set_layout_mode("standard")

        assert page.workbench.engineer_frame.winfo_manager() == "grid"
        assert page.workbench.layout_mode_var.get() == t("pages.devices.workbench.layout.standard")
        assert page.workbench.recent_preset_selector["values"]
        assert page.workbench.engineer_notebook.tabs()
        assert page.workbench.history_tree["columns"] == ("sequence", "time", "device", "action", "result", "fault")
        assert page.workbench.preset_manager_section.winfo_manager() == "grid"
        assert page.workbench.preset_import_conflict_selector["values"]
    finally:
        root.destroy()
