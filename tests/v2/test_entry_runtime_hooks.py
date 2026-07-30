from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from gas_calibrator.v2.config.models import AppConfig
from gas_calibrator.v2.entry import (
    create_calibration_service_from_config,
    load_config_bundle,
)


class _FakeService:
    def __init__(self, **kwargs) -> None:
        self.init_kwargs = kwargs
        self.runtime_hooks = None
        self.loaded_points = None
        self._raw_cfg = None

    def set_runtime_hooks(self, hooks) -> None:
        self.runtime_hooks = hooks

    def load_points(self, points_path, point_filter=None) -> None:
        self.loaded_points = (points_path, point_filter)


def _write_config(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_load_config_bundle_disables_adjacent_database_sidecar_in_simulation(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "simulation.json"
    _write_config(
        config_path,
        {
            "features": {"simulation_mode": True},
            "paths": {"points_excel": "points.json"},
        },
    )
    _write_config(
        tmp_path / "storage_config.json",
        {
            "storage": {
                "backend": "postgresql",
                "database": "gas_calibrator",
                "auto_import": True,
            }
        },
    )

    _, raw_config, config = load_config_bundle(str(config_path))

    assert config.storage.backend == "file"
    assert config.storage.auto_import is True
    assert config.storage.enabled is False
    assert config.storage.database_enabled is False
    assert raw_config["storage"]["enabled"] is False


def test_load_config_bundle_preserves_explicit_storage_disable_over_sidecar(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "offline.json"
    _write_config(
        config_path,
        {
            "features": {"simulation_mode": False},
            "storage": {"enabled": False, "backend": "file"},
        },
    )
    _write_config(
        tmp_path / "storage_config.json",
        {
            "storage": {
                "backend": "postgresql",
                "database": "gas_calibrator",
                "auto_import": True,
            }
        },
    )

    _, raw_config, config = load_config_bundle(str(config_path))

    assert raw_config["storage"] == {"enabled": False, "backend": "file"}
    assert config.storage.backend == "file"
    assert config.storage.enabled is False
    assert config.storage.database_enabled is False


def test_load_config_bundle_ignores_storage_sidecar_without_explicit_enable(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "offline.json"
    _write_config(config_path, {"features": {"simulation_mode": False}})
    _write_config(
        tmp_path / "storage_config.json",
        {
            "storage": {
                "backend": "sqlite",
                "database": "storage.sqlite",
                "auto_import": True,
            }
        },
    )

    _, raw_config, config = load_config_bundle(str(config_path))

    assert "storage" not in raw_config
    assert config.storage.backend == "file"
    assert config.storage.enabled is None
    assert config.storage.database_enabled is False


def test_load_config_bundle_keeps_non_simulation_storage_sidecar_opt_in(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "offline.json"
    _write_config(config_path, {"features": {"simulation_mode": False}})
    _write_config(
        tmp_path / "storage_config.json",
        {
            "storage": {
                "enabled": True,
                "backend": "sqlite",
                "database": "storage.sqlite",
                "auto_import": False,
            }
        },
    )

    _, raw_config, config = load_config_bundle(str(config_path))

    expected_database = str((tmp_path / "storage.sqlite").resolve())
    assert raw_config["storage"]["enabled"] is True
    assert raw_config["storage"]["database"] == expected_database
    assert config.storage.database == expected_database
    assert config.storage.database_enabled is True
    assert config.storage.auto_import is False


def test_create_calibration_service_from_config_applies_runtime_hooks_factory_before_preload() -> None:
    config = AppConfig.from_dict(
        {
            "devices": {},
            "workflow": {},
            "paths": {"points_excel": "points.json", "output_dir": "output", "logs_dir": "logs"},
            "features": {"simulation_mode": True},
        }
    )
    raw_cfg = {"paths": {"points_excel": "points.json"}}
    captured: dict[str, object] = {}

    def _runtime_hooks_factory(service, builder_raw_cfg):
        captured["service"] = service
        captured["raw_cfg"] = builder_raw_cfg
        return SimpleNamespace(name="bench_hooks")

    service = create_calibration_service_from_config(
        config,
        raw_cfg=raw_cfg,
        preload_points=True,
        service_cls=_FakeService,
        runtime_hooks_factory=_runtime_hooks_factory,
    )

    assert captured["service"] is service
    assert captured["raw_cfg"] == raw_cfg
    assert service.runtime_hooks.name == "bench_hooks"
    assert service.loaded_points == (config.paths.points_excel, None)

