from pathlib import Path

from gas_calibrator.v2.ui_v2.utils.preferences_store import PreferencesStore


def test_preferences_store_round_trips_json(tmp_path: Path) -> None:
    store = PreferencesStore(tmp_path / "preferences.json")

    saved = store.save(
        {
            "last_config_path": "demo.json",
            "simulation_default": True,
            "screenshot_format": "txt",
        }
    )
    loaded = store.load()

    assert saved["last_config_path"] == "demo.json"
    assert loaded["simulation_default"] is True
    assert loaded["screenshot_format"] == "txt"
    assert loaded["auto_start_feed"] is True
    assert loaded["shell_log_sash"] is None
    assert loaded["spectral_quality"]["enabled"] is False
    assert loaded["spectral_quality"]["min_samples"] == 64


def test_preferences_store_merges_spectral_quality_defaults(tmp_path: Path) -> None:
    store = PreferencesStore(tmp_path / "preferences.json")

    saved = store.save({"spectral_quality": {"enabled": True, "min_samples": 96}})

    assert saved["spectral_quality"]["enabled"] is True
    assert saved["spectral_quality"]["min_samples"] == 96
    assert saved["spectral_quality"]["min_duration_s"] == 30.0
    assert saved["spectral_quality"]["low_freq_max_hz"] == 0.01
