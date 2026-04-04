from __future__ import annotations

from pathlib import Path
import sys

from gas_calibrator.v2.ui_v2.i18n import t
from gas_calibrator.v2.ui_v2.utils.preferences_store import PreferencesStore

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import build_fake_facade


def test_display_profile_resolves_to_1080p_family_and_persists_context(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)

    facade.execute_device_workbench_action("workbench", "set_display_profile", display_profile="dense_1080p")
    snapshot = facade.get_device_workbench_snapshot()
    meta = snapshot["meta"]["display_profile_meta"]
    stored = facade.get_preferences()

    assert snapshot["meta"]["display_profile"] == "dense_1080p"
    assert meta["selected"] == "dense_1080p"
    assert meta["resolved"] == "1080p_compact"
    assert meta["strategy_version"] == "display_profile_v2"
    assert meta["profile_family"] == "1080p"
    assert meta["profile_family_label"] == t("pages.devices.workbench.display_profile_family.1080p")
    assert meta["resolution_bucket"] == "1080p"
    assert meta["resolution_bucket_label"] == t("pages.devices.workbench.display_profile_family.1080p")
    assert meta["monitor_class"] == "standard_monitor"
    assert meta["monitor_label"] == t("pages.devices.workbench.display_profile_monitor.standard_monitor")
    assert meta["resolution"] == "1920x1080"
    assert meta["resolution_class"] == "full_hd"
    assert meta["resolution_class_label"] == t("pages.devices.workbench.display_profile_resolution.full_hd")
    assert meta["window_class"] == "standard_window"
    assert meta["window_class_label"] == t("pages.devices.workbench.display_profile_window.standard_window")
    assert meta["layout_hint"] == "compact"
    assert meta["auto_reason"] == "manual_dense_1080p"
    assert meta["auto_reason_label"] == t("pages.devices.workbench.display_profile_reason.manual_dense_1080p")
    assert meta["multi_monitor_ready_hint"] == "single_monitor_baseline"
    assert meta["multi_monitor_ready_hint_label"] == t("pages.devices.workbench.display_profile_multi_monitor.single_monitor_baseline")
    assert meta["selection_mode"] == "manual"
    assert meta["profile_summary"]
    assert meta["aspect_ratio"] == "1.78"
    assert meta["screen_area"] == 2073600
    assert meta["window_area"] == meta["window_width"] * meta["window_height"]
    assert meta["metadata"]["strategy_version"] == "display_profile_v2"
    assert meta["metadata"]["selected_profile"] == "dense_1080p"
    assert meta["metadata"]["resolved_profile"] == "1080p_compact"
    assert meta["metadata"]["resolution_bucket"] == "1080p"
    assert meta["metadata"]["multi_monitor_ready_hint"] == "single_monitor_baseline"
    assert meta["metadata"]["resolution"] == "1920x1080"
    assert meta["metadata"]["selection_mode"] == "manual"
    assert meta["metadata"]["aspect_ratio"] == "1.78"
    assert meta["metadata"]["screen_area"] == 2073600
    assert meta["metadata"]["window_area"] == meta["window_width"] * meta["window_height"]
    assert meta["metadata"]["profile_summary"] == meta["profile_summary"]
    assert "1080p_compact" in str(meta["metadata"]["mapping_summary"])
    assert meta["metadata"]["resolution_class"] == "full_hd"
    assert stored["workbench"]["display_profile_context"]["selected"] == "dense_1080p"
    assert stored["workbench"]["display_profile_context"]["resolved"] == "1080p_compact"
    assert stored["workbench"]["display_profile_context"]["family"] == "1080p"
    assert stored["workbench"]["display_profile_context"]["resolution_bucket"] == "1080p"
    assert stored["workbench"]["display_profile_context"]["monitor_class"] == "standard_monitor"
    assert stored["workbench"]["display_profile_context"]["multi_monitor_ready_hint"] == "single_monitor_baseline"

    reloaded = build_fake_facade(tmp_path)
    reloaded_meta = reloaded.get_device_workbench_snapshot()["meta"]["display_profile_meta"]
    assert reloaded_meta["selected"] == "dense_1080p"
    assert reloaded_meta["resolved"] == "1080p_compact"


def test_display_profile_refresh_context_maps_window_and_monitor_to_profile_family(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)

    result = facade.execute_device_workbench_action(
        "workbench",
        "refresh_display_profile_context",
        screen_width=2560,
        screen_height=1440,
        window_width=2200,
        window_height=1200,
    )
    meta = result["snapshot"]["meta"]["display_profile_meta"]
    stored = facade.get_preferences()["workbench"]["display_profile_context"]

    assert result["ok"] is True
    assert meta["selected"] == "auto"
    assert meta["resolved"] == "1440p_standard"
    assert meta["strategy_version"] == "display_profile_v2"
    assert meta["profile_family"] == "1440p"
    assert meta["resolution_bucket"] == "1440p"
    assert meta["layout_hint"] == "standard"
    assert meta["monitor_class"] == "wide_monitor"
    assert meta["monitor_label"] == t("pages.devices.workbench.display_profile_monitor.wide_monitor")
    assert meta["resolution"] == "2560x1440"
    assert meta["resolution_class"] == "wide_resolution"
    assert meta["resolution_class_label"] == t("pages.devices.workbench.display_profile_resolution.wide_resolution")
    assert meta["window_class"] == "wide_window"
    assert meta["window_class_label"] == t("pages.devices.workbench.display_profile_window.wide_window")
    assert meta["auto_reason"] == "simulated_1440p_canvas"
    assert meta["auto_reason_label"] == t("pages.devices.workbench.display_profile_reason.simulated_1440p_canvas")
    assert meta["multi_monitor_ready_hint"] == "future_multi_monitor_ready"
    assert meta["multi_monitor_ready_hint_label"] == t("pages.devices.workbench.display_profile_multi_monitor.future_multi_monitor_ready")
    assert meta["selection_mode"] == "auto"
    assert meta["profile_summary"]
    assert meta["aspect_ratio"] == "1.78"
    assert meta["screen_area"] == 3686400
    assert meta["window_area"] == 2640000
    assert meta["screen_width"] == 2560
    assert meta["window_width"] == 2200
    assert meta["metadata"]["resolved_profile"] == "1440p_standard"
    assert meta["metadata"]["resolution_bucket"] == "1440p"
    assert meta["metadata"]["multi_monitor_ready_hint"] == "future_multi_monitor_ready"
    assert meta["metadata"]["selection_mode"] == "auto"
    assert meta["metadata"]["aspect_ratio"] == "1.78"
    assert meta["metadata"]["screen_area"] == 3686400
    assert meta["metadata"]["window_area"] == 2640000
    assert "wide_monitor" in str(meta["metadata"]["mapping_summary"])
    assert stored["resolved"] == "1440p_standard"
    assert stored["family"] == "1440p"
    assert stored["resolution_bucket"] == "1440p"
    assert stored["monitor_class"] == "wide_monitor"
    assert stored["resolution_class"] == "wide_resolution"
    assert stored["window_class"] == "wide_window"
    assert stored["multi_monitor_ready_hint"] == "future_multi_monitor_ready"


def test_display_profile_invalid_saved_value_falls_back_to_auto_profile(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    store = PreferencesStore(facade.runtime_paths.preferences_path)
    store.save(
        {
            "workbench": {
                "display_profile": "broken_profile",
                "display_profile_context": {
                    "selected": "broken_profile",
                    "resolved": "invalid_profile",
                    "resolution": "999x999",
                },
            }
        }
    )

    reloaded = build_fake_facade(tmp_path)
    meta = reloaded.get_device_workbench_snapshot()["meta"]["display_profile_meta"]

    assert reloaded.get_device_workbench_snapshot()["meta"]["display_profile"] == "auto"
    assert meta["selected"] == "auto"
    assert meta["resolved"] == "1080p_standard"
    assert meta["profile_family"] == "1080p"
    assert meta["resolution_bucket"] == "1080p"
    assert meta["monitor_class"] == "standard_monitor"
    assert meta["resolution"] == "1920x1080"
    assert meta["resolution_class"] == "full_hd"
    assert meta["window_class"] == "standard_window"
    assert meta["strategy_version"] == "display_profile_v2"
    assert meta["profile_summary"]
    assert meta["metadata"]["selected_profile"] == "auto"
    assert meta["metadata"]["resolved_profile"] == "1080p_standard"
    assert meta["metadata"]["resolution_bucket"] == "1080p"
    assert meta["metadata"]["aspect_ratio"] == "1.78"


def test_display_profile_auto_uses_4k_and_ultrawide_contract_fields(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)

    four_k = facade.execute_device_workbench_action(
        "workbench",
        "refresh_display_profile_context",
        screen_width=3840,
        screen_height=2160,
        window_width=2800,
        window_height=1600,
    )["snapshot"]["meta"]["display_profile_meta"]
    ultrawide = facade.execute_device_workbench_action(
        "workbench",
        "refresh_display_profile_context",
        screen_width=3440,
        screen_height=1440,
        window_width=2400,
        window_height=1200,
    )["snapshot"]["meta"]["display_profile_meta"]

    assert four_k["resolved"] == "4k_standard"
    assert four_k["profile_family"] == "4k"
    assert four_k["resolution_bucket"] == "4k"
    assert four_k["multi_monitor_ready_hint"] == "future_multi_monitor_ready"
    assert four_k["auto_reason"] == "simulated_4k_canvas"
    assert "4k_standard" in str(four_k["mapping_summary"])
    assert four_k["metadata"]["resolution_bucket"] == "4k"

    assert ultrawide["resolved"] == "ultrawide_standard"
    assert ultrawide["profile_family"] == "ultrawide"
    assert ultrawide["resolution_bucket"] == "ultrawide"
    assert ultrawide["multi_monitor_ready_hint"] == "future_multi_monitor_ready"
    assert ultrawide["auto_reason"] == "simulated_ultrawide_canvas"
    assert "ultrawide_standard" in str(ultrawide["mapping_summary"])
    assert ultrawide["metadata"]["resolution_bucket"] == "ultrawide"
