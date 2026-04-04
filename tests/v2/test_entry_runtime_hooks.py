from __future__ import annotations

from types import SimpleNamespace

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.entry import create_calibration_service_from_config


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

