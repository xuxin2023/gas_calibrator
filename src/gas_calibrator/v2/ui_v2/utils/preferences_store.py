from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any


DEFAULT_PREFERENCES: dict[str, Any] = {
    "last_config_path": "",
    "simulation_default": False,
    "auto_start_feed": True,
    "screenshot_format": "png",
    "window_geometry": "",
    "shell_log_sash": None,
    "spectral_quality": {
        "enabled": False,
        "min_samples": 64,
        "min_duration_s": 30.0,
        "low_freq_max_hz": 0.01,
    },
    "workbench": {
        "layout_mode": "compact",
        "view_mode": "operator_view",
        "display_profile": "auto",
        "display_profile_context": {
            "selected": "auto",
            "resolved": "1080p_standard",
            "strategy_version": "display_profile_v2",
            "family": "1080p",
            "resolution_bucket": "1080p",
            "resolution": "1920x1080",
            "resolution_class": "full_hd",
            "layout_hint": "standard",
            "monitor_class": "standard_monitor",
            "window_class": "standard_window",
            "auto_reason": "default_1080p",
            "multi_monitor_ready_hint": "single_monitor_baseline",
            "mapping_summary": "auto->1080p_standard | 1080p | 1920x1080 | standard_monitor | standard_window | single_monitor_baseline",
            "screen_width": 1920,
            "screen_height": 1080,
            "window_width": 1720,
            "window_height": 940,
        },
        "preset_preferences": {
            "favorites": [],
            "pinned": [],
            "recent_presets": [],
            "usage": {},
            "import_conflict_policy": "rename",
            "custom_presets": [],
        },
    },
}


def merge_preferences(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in dict(override or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = merge_preferences(dict(merged[key]), value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


class PreferencesStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def load(self) -> dict[str, Any]:
        if not self.path.exists():
            return copy.deepcopy(DEFAULT_PREFERENCES)
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                return merge_preferences(DEFAULT_PREFERENCES, payload)
            return copy.deepcopy(DEFAULT_PREFERENCES)
        except Exception:
            return copy.deepcopy(DEFAULT_PREFERENCES)

    def save(self, preferences: dict[str, Any]) -> dict[str, Any]:
        payload = merge_preferences(DEFAULT_PREFERENCES, dict(preferences or {}))
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return payload
