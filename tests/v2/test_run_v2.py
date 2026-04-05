import json
import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest

from gas_calibrator.v2.core.calibration_service import CalibrationService


def _load_run_v2_module():
    path = Path(__file__).resolve().parents[2] / "src" / "gas_calibrator" / "v2" / "scripts" / "run_v2.py"
    spec = spec_from_file_location("test_run_v2_module", path)
    module = module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_run_v2_headless_uses_calibration_service_without_ui(monkeypatch) -> None:
    sys.modules.pop("gas_calibrator.v2.ui_v2.app", None)
    module = _load_run_v2_module()
    calls: list[str] = []
    assert "gas_calibrator.v2.ui_v2.app" not in sys.modules

    class FakeService:
        def run(self) -> None:
            calls.append("run")

    monkeypatch.setattr(
        module,
        "create_calibration_service",
        lambda config_path, simulation_mode=False, allow_unsafe_step2_config=False: calls.append(
            f"create:{config_path}:{simulation_mode}:{allow_unsafe_step2_config}"
        ) or FakeService(),
    )
    monkeypatch.setattr(module, "_run_ui", lambda argv=None: calls.append("ui") or 0)

    result = module.main(["--config", "demo.json", "--simulation", "--headless"])

    assert result == 0
    assert calls[0] == "create:demo.json:True:False"
    assert calls[1] == "run"
    assert "ui" not in calls


def test_run_v2_ui_mode_delegates_to_ui_main(monkeypatch) -> None:
    module = _load_run_v2_module()
    captured = {}

    monkeypatch.setattr(module, "create_calibration_service", lambda *args, **kwargs: None)
    def _fake_run_ui(argv=None):
        captured["argv"] = list(argv or [])
        return 0

    monkeypatch.setattr(module, "_run_ui", _fake_run_ui)

    result = module.main(["--config", "demo.json", "--simulation"])

    assert result == 0
    assert captured["argv"] == ["--config", "demo.json", "--simulation"]


def test_run_v2_forwards_allow_unsafe_flag_to_headless_and_ui(monkeypatch) -> None:
    module = _load_run_v2_module()
    captured = {"headless": None, "ui": None}

    class FakeService:
        def run(self) -> None:
            return None

    def _fake_create(config_path, simulation_mode=False, allow_unsafe_step2_config=False):
        captured["headless"] = {
            "config_path": config_path,
            "simulation_mode": simulation_mode,
            "allow_unsafe_step2_config": allow_unsafe_step2_config,
        }
        return FakeService()

    monkeypatch.setattr(module, "create_calibration_service", _fake_create)
    monkeypatch.setattr(module, "_run_ui", lambda argv=None: captured.__setitem__("ui", list(argv or [])) or 0)

    assert module.main(["--config", "demo.json", "--headless", "--allow-unsafe-step2-config"]) == 0
    assert captured["headless"] == {
        "config_path": "demo.json",
        "simulation_mode": False,
        "allow_unsafe_step2_config": True,
    }

    assert module.main(["--config", "demo.json", "--allow-unsafe-step2-config"]) == 0
    assert captured["ui"] == ["--config", "demo.json", "--allow-unsafe-step2-config"]


def test_run_v2_headless_resolves_relative_config_paths(monkeypatch, tmp_path: Path) -> None:
    module = _load_run_v2_module()
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    points_path = config_dir / "points.json"
    points_path.write_text(
        json.dumps(
            {
                "points": [
                    {
                        "index": 1,
                        "temperature": 25.0,
                        "co2": 400.0,
                        "pressure": 1000.0,
                        "route": "co2",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    config_path = config_dir / "app.json"
    config_path.write_text(
        json.dumps(
            {
                "devices": {
                    "gas_analyzers": [{"port": "SIM-GA1", "enabled": True}],
                },
                "paths": {
                    "points_excel": "points.json",
                    "output_dir": "output",
                    "logs_dir": "logs",
                },
                "features": {"simulation_mode": True},
            }
        ),
        encoding="utf-8",
    )

    captured = {}

    def _fake_run(self) -> None:
        captured["points_excel"] = self.config.paths.points_excel
        captured["output_dir"] = self.config.paths.output_dir
        captured["logs_dir"] = self.config.paths.logs_dir
        captured["total_points"] = self.get_status().total_points

    monkeypatch.setattr(CalibrationService, "run", _fake_run)
    monkeypatch.chdir(tmp_path)

    result = module.main(["--config", "configs/app.json", "--simulation", "--headless"])

    assert result == 0
    assert Path(captured["points_excel"]) == points_path.resolve()
    assert Path(captured["output_dir"]) == (config_dir / "output").resolve()
    assert Path(captured["logs_dir"]) == (config_dir / "logs").resolve()
    assert captured["total_points"] == 1


def test_run_v2_headless_logs_step2_config_safety(monkeypatch, capsys) -> None:
    module = _load_run_v2_module()

    class FakeService:
        def __init__(self) -> None:
            self.config = type(
                "Cfg",
                (),
                {
                    "_config_safety": {
                        "review_lines": [
                            "配置安全提醒 1 项：检测到 non-default 工程配置。",
                        ],
                        "execution_gate": {
                            "summary": "Step 2 默认工作流已拦截当前配置；必须显式双重解锁。",
                        },
                    }
                },
            )()

        def run(self) -> None:
            return None

    monkeypatch.setattr(module, "create_calibration_service", lambda *args, **kwargs: FakeService())

    result = module.main(["--config", "demo.json", "--headless"])
    captured = capsys.readouterr().out

    assert result == 0
    assert "[Step2 config safety]" in captured
    assert "[Step2 execution gate]" in captured
    assert "Step 2 默认工作流已拦截当前配置" in captured


def test_run_v2_headless_blocks_unsafe_step2_config_without_dual_unlock(tmp_path: Path, monkeypatch) -> None:
    module = _load_run_v2_module()
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    points_path = config_dir / "points.json"
    points_path.write_text('{"points": []}', encoding="utf-8")
    config_path = config_dir / "unsafe.json"
    config_path.write_text(
        json.dumps(
            {
                "devices": {
                    "pressure_controller": {"port": "COM31", "enabled": True},
                    "gas_analyzers": [{"port": "SIM-GA1", "enabled": True}],
                },
                "paths": {
                    "points_excel": "points.json",
                    "output_dir": "output",
                    "logs_dir": "logs",
                },
                "features": {"simulation_mode": True},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("GAS_CALIBRATOR_V2_ALLOW_UNSAFE_CONFIG", raising=False)

    with pytest.raises(RuntimeError, match="Step 2 默认工作流已拦截当前配置"):
        module.main(["--config", str(config_path), "--headless"])


def test_run_v2_headless_blocks_capture_then_hold_without_dual_unlock(tmp_path: Path, monkeypatch) -> None:
    module = _load_run_v2_module()
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    points_path = config_dir / "points.json"
    points_path.write_text('{"points": []}', encoding="utf-8")
    config_path = config_dir / "capture_then_hold.json"
    config_path.write_text(
        json.dumps(
            {
                "devices": {
                    "pressure_controller": {"port": "SIM-PACE5000", "enabled": True},
                    "gas_analyzers": [{"port": "SIM-GA1", "enabled": True}],
                },
                "paths": {
                    "points_excel": "points.json",
                    "output_dir": "output",
                    "logs_dir": "logs",
                },
                "features": {"simulation_mode": True},
                "workflow": {
                    "pressure": {
                        "capture_then_hold_enabled": True,
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("GAS_CALIBRATOR_V2_ALLOW_UNSAFE_CONFIG", raising=False)

    with pytest.raises(RuntimeError, match="Step 2"):
        module.main(["--config", str(config_path), "--headless"])

    with pytest.raises(RuntimeError, match="Step 2"):
        module.main(["--config", str(config_path), "--headless", "--allow-unsafe-step2-config"])

    captured: dict[str, dict[str, object]] = {}
    real_create = module.create_calibration_service

    def _capturing_create(*args, **kwargs):
        service = real_create(*args, **kwargs)
        captured["gate"] = dict(getattr(service.config, "_step2_execution_gate", {}) or {})
        return service

    monkeypatch.setenv("GAS_CALIBRATOR_V2_ALLOW_UNSAFE_CONFIG", "1")
    monkeypatch.setattr(module, "create_calibration_service", _capturing_create)
    monkeypatch.setattr(CalibrationService, "run", lambda self: None)

    assert module.main(
        ["--config", str(config_path), "--headless", "--simulation", "--allow-unsafe-step2-config"]
    ) == 0
    assert captured["gate"]["status"] == "unlocked_override"
