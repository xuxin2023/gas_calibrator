from __future__ import annotations

import json
from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.utils.preferences_store import PreferencesStore

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import build_fake_facade


def test_workbench_preferences_persist_view_layout_profile_and_preset_flags(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)

    facade.execute_device_workbench_action("workbench", "set_view_mode", view_mode="engineer_view")
    facade.execute_device_workbench_action("workbench", "set_layout_mode", layout_mode="standard")
    facade.execute_device_workbench_action("workbench", "set_display_profile", display_profile="dense_1080p")
    facade.execute_device_workbench_action("analyzer", "run_preset", preset_id="mode2_active_read", analyzer_index=0)
    facade.execute_device_workbench_action(
        "workbench",
        "toggle_preset_favorite",
        device_kind="analyzer",
        preset_id="mode2_active_read",
    )
    facade.execute_device_workbench_action(
        "workbench",
        "toggle_preset_pin",
        device_kind="analyzer",
        preset_id="mode2_active_read",
    )
    stored = facade.get_preferences()

    reloaded = build_fake_facade(tmp_path)
    snapshot = reloaded.get_device_workbench_snapshot()
    preset_center = snapshot["workbench"]["preset_center"]
    analyzer_group = next(item for item in preset_center["groups"] if item["id"] == "analyzer")
    analyzer_preset = next(item for item in analyzer_group["presets"] if item["id"] == "mode2_active_read")

    assert snapshot["meta"]["view_mode"] == "engineer_view"
    assert snapshot["meta"]["layout_mode"] == "standard"
    assert snapshot["meta"]["display_profile"] == "dense_1080p"
    assert snapshot["meta"]["display_profile_meta"]["resolved"] == "1080p_compact"
    assert snapshot["meta"]["display_profile_meta"]["profile_family"] == "1080p"
    assert snapshot["meta"]["display_profile_meta"]["resolution_bucket"] == "1080p"
    assert snapshot["meta"]["display_profile_meta"]["monitor_class"] == "standard_monitor"
    assert snapshot["meta"]["display_profile_meta"]["resolution"] == "1920x1080"
    assert analyzer_preset["is_favorite"] is True
    assert analyzer_preset["is_pinned"] is True
    assert preset_center["recent_presets"][0]["id"] == "mode2_active_read"
    assert preset_center["pinned_presets"][0]["id"] == "mode2_active_read"
    assert preset_center["favorite_presets"][0]["id"] == "mode2_active_read"
    assert stored["workbench"]["display_profile_context"]["selected"] == "dense_1080p"
    assert stored["workbench"]["display_profile_context"]["resolved"] == "1080p_compact"
    assert stored["workbench"]["display_profile_context"]["family"] == "1080p"
    assert stored["workbench"]["display_profile_context"]["resolution_bucket"] == "1080p"
    assert stored["workbench"]["display_profile_context"]["monitor_class"] == "standard_monitor"
    assert stored["workbench"]["display_profile_context"]["resolution"] == "1920x1080"
    assert stored["workbench"]["display_profile_context"]["multi_monitor_ready_hint"] == "single_monitor_baseline"
    assert "1080p_compact" in str(stored["workbench"]["display_profile_context"]["mapping_summary"] or "")
    assert stored["workbench"]["preset_preferences"]["import_conflict_policy"] == "rename"


def test_workbench_preferences_persist_import_conflict_policy(tmp_path: Path) -> None:
    source_facade = build_fake_facade(tmp_path / "source")
    saved = source_facade.execute_device_workbench_action(
        "workbench",
        "save_custom_preset",
        group_id="pressure",
        label="pressure_chain",
        description="source preset",
        steps=[{"device_kind": "pressure_gauge", "preset_id": "wrong_unit"}],
    )
    export_result = source_facade.execute_device_workbench_action(
        "workbench",
        "export_preset_bundle",
        scope="selected",
        device_kind=str(saved["custom_preset"]["device_kind"] or ""),
        preset_id=str(saved["custom_preset"]["id"] or ""),
    )

    target_facade = build_fake_facade(tmp_path / "target")
    target_facade.execute_device_workbench_action(
        "workbench",
        "save_custom_preset",
        group_id="pressure",
        label="pressure_chain",
        description="target preset",
        steps=[{"device_kind": "pressure_gauge", "preset_id": "wrong_unit"}],
    )
    target_facade.execute_device_workbench_action(
        "workbench",
        "import_preset_bundle",
        bundle_text=export_result["bundle_text"],
        conflict_policy="overwrite",
    )

    stored = target_facade.get_preferences()["workbench"]["preset_preferences"]
    reloaded = build_fake_facade(tmp_path / "target")
    manager = reloaded.get_device_workbench_snapshot()["workbench"]["preset_center"]["manager"]

    assert stored["import_conflict_policy"] == "overwrite"
    assert manager["selected_import_conflict_policy"] == "overwrite"


def test_preferences_store_recovers_nested_workbench_defaults_safely(tmp_path: Path) -> None:
    path = tmp_path / "preferences.json"
    path.write_text(
        json.dumps({"workbench": {"layout_mode": "standard"}}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    store = PreferencesStore(path)

    loaded = store.load()

    assert loaded["workbench"]["layout_mode"] == "standard"
    assert loaded["workbench"]["view_mode"] == "operator_view"
    assert loaded["workbench"]["display_profile"] == "auto"
    assert loaded["workbench"]["display_profile_context"]["selected"] == "auto"
    assert loaded["workbench"]["display_profile_context"]["resolved"] == "1080p_standard"
    assert loaded["workbench"]["display_profile_context"]["family"] == "1080p"
    assert loaded["workbench"]["display_profile_context"]["resolution_bucket"] == "1080p"
    assert loaded["workbench"]["display_profile_context"]["monitor_class"] == "standard_monitor"
    assert loaded["workbench"]["display_profile_context"]["multi_monitor_ready_hint"] == "single_monitor_baseline"
    assert "1080p_standard" in str(loaded["workbench"]["display_profile_context"]["mapping_summary"] or "")
    assert loaded["workbench"]["preset_preferences"]["favorites"] == []
    assert loaded["workbench"]["preset_preferences"]["import_conflict_policy"] == "rename"
    assert loaded["workbench"]["preset_preferences"]["custom_presets"] == []

    path.write_text("{invalid", encoding="utf-8")
    fallback = store.load()

    assert fallback["workbench"]["layout_mode"] == "compact"
    assert fallback["workbench"]["view_mode"] == "operator_view"
    assert fallback["workbench"]["display_profile"] == "auto"
    assert fallback["workbench"]["display_profile_context"]["selected"] == "auto"
    assert fallback["workbench"]["display_profile_context"]["resolved"] == "1080p_standard"
    assert fallback["workbench"]["display_profile_context"]["family"] == "1080p"
    assert fallback["workbench"]["display_profile_context"]["resolution_bucket"] == "1080p"
    assert fallback["workbench"]["display_profile_context"]["monitor_class"] == "standard_monitor"
    assert fallback["workbench"]["display_profile_context"]["multi_monitor_ready_hint"] == "single_monitor_baseline"
    assert "1080p_standard" in str(fallback["workbench"]["display_profile_context"]["mapping_summary"] or "")
    assert fallback["workbench"]["preset_preferences"]["import_conflict_policy"] == "rename"
