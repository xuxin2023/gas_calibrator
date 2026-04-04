from pathlib import Path

from gas_calibrator.v2.ui_v2.utils.runtime_paths import RuntimePaths


def test_runtime_paths_uses_expected_local_structure(tmp_path: Path) -> None:
    paths = RuntimePaths.from_base_dir(tmp_path / "client_state").ensure_dirs()

    assert paths.base_dir.exists()
    assert paths.config_dir.exists()
    assert paths.cache_dir.exists()
    assert paths.logs_dir.exists()
    assert paths.screenshots_dir.exists()
    assert paths.plan_profiles_dir.exists()
    assert paths.preferences_path.parent == paths.config_dir
    assert paths.recent_runs_path.parent == paths.config_dir
    assert paths.route_memory_path.parent == paths.config_dir
    assert paths.plan_profiles_dir.parent == paths.config_dir
