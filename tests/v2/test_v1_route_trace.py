import importlib
import json
from pathlib import Path
import sys

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.v2.adapters.v1_route_trace import TracedCalibrationRunner
from gas_calibrator.workflow.runner import CalibrationRunner


def _point(*, route: str = "h2o") -> CalibrationPoint:
    return CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=None if route == "h2o" else 400.0,
        hgen_temp_c=20.0,
        hgen_rh_pct=50.0,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
        co2_group="A",
    )


def _runner(tmp_path: Path) -> TracedCalibrationRunner:
    logger = RunLogger(tmp_path)
    return TracedCalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)


def _import_run_v1_route_trace():
    return importlib.import_module("gas_calibrator.v2.scripts.run_v1_route_trace")


def test_traced_runner_writes_h2o_path_trace(monkeypatch, tmp_path: Path) -> None:
    point = _point(route="h2o")
    runner = _runner(tmp_path)

    monkeypatch.setattr(CalibrationRunner, "_set_h2o_path", lambda self, is_open, point=None: None)

    runner._set_h2o_path(True, point)
    runner.logger.close()

    trace_path = runner.route_trace_path
    payload = json.loads(trace_path.read_text(encoding="utf-8").splitlines()[0])
    assert payload["route"] == "h2o"
    assert payload["point_index"] == 1
    assert payload["point_tag"] == "h2o_20c_50rh_1000hpa"
    assert payload["action"] == "set_h2o_path"
    assert payload["target"]["open"] is True
    assert payload["result"] == "ok"


def test_traced_runner_writes_sample_start_and_end(monkeypatch, tmp_path: Path) -> None:
    point = _point(route="co2")
    runner = _runner(tmp_path)
    monkeypatch.setattr(CalibrationRunner, "_sampling_params", lambda self, phase="": (3, 0.5))
    monkeypatch.setattr(CalibrationRunner, "_sample_and_log", lambda self, point, phase="", point_tag="": None)

    runner._sample_and_log(point, phase="co2", point_tag="co2_groupa_400ppm_1000hpa")
    runner.logger.close()

    lines = runner.route_trace_path.read_text(encoding="utf-8").splitlines()
    actions = [json.loads(line)["action"] for line in lines]
    assert actions == ["sample_start", "sample_end"]


def test_run_v1_route_trace_uses_traced_runner(monkeypatch, tmp_path: Path) -> None:
    run_v1_route_trace = _import_run_v1_route_trace()
    cfg = {"paths": {"output_dir": str(tmp_path)}, "devices": {}, "workflow": {}}
    created = {}

    monkeypatch.setattr(run_v1_route_trace, "load_config", lambda path: cfg)
    monkeypatch.setattr(run_v1_route_trace, "run_self_test", lambda cfg, io_logger=None: {})
    monkeypatch.setattr(run_v1_route_trace, "_enabled_failures", lambda cfg, results: [])
    monkeypatch.setattr(run_v1_route_trace, "_build_devices", lambda cfg, io_logger=None: {})
    monkeypatch.setattr(run_v1_route_trace, "_close_devices", lambda devices: None)

    class _FakeRunner:
        def __init__(self, config, devices, logger, log_fn, status_fn):
            created["config"] = config
            created["logger"] = logger
            self.route_trace_path = logger.run_dir / "route_trace.jsonl"

        def run(self):
            self.route_trace_path.write_text("", encoding="utf-8")

    monkeypatch.setattr(run_v1_route_trace, "TracedCalibrationRunner", _FakeRunner)

    exit_code = run_v1_route_trace.main(["--config", "dummy.json", "--skip-connect-check"])
    assert exit_code == 0
    assert created["config"] is cfg
    assert created["logger"].run_dir.joinpath("route_trace.jsonl").exists()


def test_run_v1_route_trace_module_import_stays_lightweight() -> None:
    for name in [
        "gas_calibrator.v2.scripts.run_v1_route_trace",
        "gas_calibrator.diagnostics",
        "gas_calibrator.tools.run_headless",
    ]:
        sys.modules.pop(name, None)

    module = importlib.import_module("gas_calibrator.v2.scripts.run_v1_route_trace")

    assert hasattr(module, "main")
    assert "gas_calibrator.diagnostics" not in sys.modules
    assert "gas_calibrator.tools.run_headless" not in sys.modules
