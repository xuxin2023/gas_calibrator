from __future__ import annotations

import json
from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.widgets.device_workbench import DeviceWorkbench

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import build_fake_facade, make_root


def _custom_device_label(widget: DeviceWorkbench, device_kind: str) -> str:
    return next(label for label, item_id in widget._custom_device_lookup.items() if item_id == device_kind)


def _step_preset_label(widget: DeviceWorkbench, step_index: int, preset_id: str) -> str:
    return next(label for label, item_id in widget._custom_step_preset_lookup[step_index].items() if item_id == preset_id)


def test_custom_preset_editor_creates_updates_deletes_and_persists_simulation_only_presets(tmp_path: Path) -> None:
    root = make_root()
    try:
        facade = build_fake_facade(tmp_path)
        widget = DeviceWorkbench(root, facade=facade)
        widget.render(facade.get_device_workbench_snapshot())

        pressure_group_label = next(
            item["label"]
            for item in facade.get_device_workbench_snapshot()["workbench"]["preset_center"]["groups"]
            if item["id"] == "pressure"
        )
        widget.custom_preset_group_var.set(pressure_group_label)
        widget.custom_preset_name_var.set("压力链路诊断")
        widget.custom_preset_description_var.set("仅用于 fake/simulation 预置链")

        widget.custom_step_device_vars[0].set(_custom_device_label(widget, "pressure_gauge"))
        widget._refresh_custom_step_options(0)
        widget.custom_step_preset_vars[0].set(_step_preset_label(widget, 0, "wrong_unit"))

        widget.custom_step_device_vars[1].set(_custom_device_label(widget, "relay"))
        widget._refresh_custom_step_options(1)
        widget.custom_step_preset_vars[1].set(_step_preset_label(widget, 1, "route_h2o"))

        widget._save_custom_preset_from_editor()
        preset_id = widget._editor_preset_id()
        snapshot = facade.get_device_workbench_snapshot()
        custom_presets = snapshot["workbench"]["preset_center"]["custom_presets"]

        assert preset_id.startswith("custom_pressure_")
        assert widget.custom_preset_buttons.winfo_children()
        assert any(item["id"] == preset_id for item in custom_presets)

        widget.custom_preset_description_var.set("更新后的 simulation-only 说明")
        widget._save_custom_preset_from_editor()
        updated_snapshot = facade.get_device_workbench_snapshot()
        saved = next(item for item in updated_snapshot["workbench"]["preset_center"]["custom_presets"] if item["id"] == preset_id)

        assert saved["description"] == "更新后的 simulation-only 说明"
        assert saved["is_custom"] is True
        assert len(saved["steps"]) == 2

        facade.execute_device_workbench_action("pressure_gauge", "run_preset", preset_id=preset_id)
        run_snapshot = facade.get_device_workbench_snapshot()
        report_payload = json.loads(
            (Path(facade.result_store.run_dir) / "workbench_action_report.json").read_text(encoding="utf-8")
        )

        assert run_snapshot["workbench"]["preset_center"]["recent_presets"][0]["id"] == preset_id
        assert run_snapshot["history"]["all_items"][0]["params"]["custom_preset_id"] == preset_id
        assert run_snapshot["history"]["all_items"][0]["params"]["preset_source"] == "custom"
        assert run_snapshot["workbench"]["snapshot_compare"]["options"]
        assert any(
            dict(item.get("params", {}) or {}).get("custom_preset_id") == preset_id
            for item in report_payload["history"]
        )
        assert any(item["type"] == "workbench" for item in facade.build_results_snapshot()["review_center"]["evidence_items"])

        reloaded = build_fake_facade(tmp_path)
        reloaded_snapshot = reloaded.get_device_workbench_snapshot()
        assert any(item["id"] == preset_id for item in reloaded_snapshot["workbench"]["preset_center"]["custom_presets"])

        widget._delete_loaded_custom_preset()
        deleted_snapshot = facade.get_device_workbench_snapshot()
        assert all(item["id"] != preset_id for item in deleted_snapshot["workbench"]["preset_center"]["custom_presets"])

        reloaded_after_delete = build_fake_facade(tmp_path)
        assert all(
            item["id"] != preset_id
            for item in reloaded_after_delete.get_device_workbench_snapshot()["workbench"]["preset_center"]["custom_presets"]
        )
    finally:
        root.destroy()

