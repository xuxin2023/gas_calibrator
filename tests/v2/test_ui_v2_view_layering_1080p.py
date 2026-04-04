from __future__ import annotations

from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.app import build_application
from gas_calibrator.v2.ui_v2.pages.devices_page import DevicesPage
from gas_calibrator.v2.ui_v2.styles import apply_styles

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import build_fake_facade, make_root


def _show_1080p(root) -> None:
    root.geometry("1920x1080+0+0")
    root.deiconify()
    root.update_idletasks()
    root.update()


def _mount_page(root, page) -> None:
    root.rowconfigure(0, weight=1)
    root.columnconfigure(0, weight=1)
    page.grid(row=0, column=0, sticky="nsew")
    _show_1080p(root)


def test_shell_remembers_workspace_log_split_position(tmp_path: Path) -> None:
    first_root = make_root()
    first_shell = None
    remembered = None
    try:
        first_root.geometry("1920x1080+0+0")
        facade = build_fake_facade(tmp_path)
        _, first_shell, _ = build_application(root=first_root, facade=facade, start_feed=False)
        _show_1080p(first_root)
        total_height = int(first_shell.main_split.winfo_height() or 0)
        assert total_height > 0
        target_position = max(520, total_height - 180)
        first_shell.main_split.sashpos(0, target_position)
        first_root.update_idletasks()
        first_shell._remember_workspace_split()
        remembered = int(facade.get_preferences()["shell_log_sash"])
        assert remembered >= 520
    finally:
        if first_shell is not None:
            first_shell.shutdown()
        first_root.destroy()

    second_root = make_root()
    second_shell = None
    try:
        second_root.geometry("1920x1080+0+0")
        restored_facade = build_fake_facade(tmp_path)
        _, second_shell, _ = build_application(root=second_root, facade=restored_facade, start_feed=False)
        _show_1080p(second_root)
        second_shell._set_default_workspace_split()
        second_root.update_idletasks()
        restored = int(second_shell.main_split.sashpos(0))
        assert remembered is not None
        assert abs(restored - remembered) <= 24
    finally:
        if second_shell is not None:
            second_shell.shutdown()
        second_root.destroy()


def test_devices_page_view_layering_keeps_operator_and_engineer_sections_reachable(tmp_path: Path) -> None:
    root = make_root()
    try:
        apply_styles(root)
        facade = build_fake_facade(tmp_path)
        page = DevicesPage(root, facade=facade)
        _mount_page(root, page)
        page.render(facade.get_devices_snapshot())
        _show_1080p(root)

        assert page.page_scaffold is not None
        assert page.workbench.notebook.winfo_ismapped()
        assert page.workbench.generate_evidence_button.winfo_ismapped()
        assert page.workbench.preset_center_frame.winfo_ismapped()
        assert page.workbench.operator_history_frame.winfo_ismapped()
        assert page.workbench.engineer_frame.winfo_manager() == ""
        assert (not page.page_scaffold.has_overflow()) or page.page_scaffold.is_scrollbar_visible()

        facade.execute_device_workbench_action("workbench", "set_view_mode", view_mode="engineer_view")
        facade.execute_device_workbench_action(
            "relay",
            "run_preset",
            preset_id="stuck_channel",
            relay_name="relay_8",
            channel=1,
        )
        page.render(facade.get_devices_snapshot())
        _show_1080p(root)

        assert page.workbench.engineer_frame.winfo_ismapped()
        assert page.workbench.engineer_notebook.index("end") >= 6
        page.workbench.engineer_notebook.select(page.workbench.history_tab)
        root.update_idletasks()
        assert page.workbench.history_tree.winfo_ismapped()
        page.workbench._set_layout_mode("standard")
        root.update_idletasks()
        page.workbench.engineer_notebook.select(page.workbench.compare_tab)
        root.update_idletasks()
        assert page.workbench.snapshot_compare_text.winfo_ismapped()
        assert (not page.page_scaffold.has_overflow()) or page.page_scaffold.is_scrollbar_visible()
    finally:
        root.destroy()
