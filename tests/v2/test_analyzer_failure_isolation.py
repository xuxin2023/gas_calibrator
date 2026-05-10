from __future__ import annotations

import json
from pathlib import Path

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.calibration_service import CalibrationService
from gas_calibrator.v2.core.device_manager import DeviceManager
from gas_calibrator.v2.core.models import CalibrationPoint
from gas_calibrator.v2.core.stability_checker import StabilityResult
from gas_calibrator.v2.exceptions import WorkflowValidationError


class FakeTemperatureChamber:
    def read_temp_c(self) -> float:
        return 25.0


class FakeAnalyzer:
    def __init__(self, co2_ppm: float = 400.0) -> None:
        self.co2_ppm = co2_ppm
        self.fetch_calls = 0
        self.write_calls: list[str] = []

    def fetch_all(self) -> dict:
        self.fetch_calls += 1
        return {
            "co2_ppm": self.co2_ppm,
            "h2o_mmol": 10.0,
            "co2_signal": self.co2_ppm,
            "h2o_signal": 10.0,
            "temperature_c": 25.0,
            "pressure_hpa": 1000.0,
        }

    def set_device_id_with_ack(self, value, require_ack=False) -> None:
        self.write_calls.append("set_device_id_with_ack")

    def set_mode_with_ack(self, value, require_ack=False) -> None:
        self.write_calls.append("set_mode_with_ack")

    def set_senco_with_ack(self, value, require_ack=False) -> None:
        self.write_calls.append("set_senco_with_ack")


class ImmediateStabilityChecker:
    def wait_for_stability(self, stability_type, read_func, stop_event):
        value = read_func() if read_func is not None else None
        return StabilityResult(
            stability_type=stability_type,
            stable=True,
            readings=[] if value is None else [float(value)],
            range_value=0.0,
            tolerance=1.0,
            elapsed_s=0.0,
            window_s=0.0,
            timeout_s=1.0,
            sample_count=1 if value is not None else 0,
            last_value=None if value is None else float(value),
        )


def _points_file(tmp_path: Path) -> Path:
    path = tmp_path / "points.json"
    path.write_text(
        json.dumps({"points": [{"index": 1, "temperature_c": 25.0, "h2o_mmol": 10.0, "route": "h2o"}]}),
        encoding="utf-8",
    )
    return path


def _make_service(tmp_path: Path, analyzer_count: int = 4) -> CalibrationService:
    points_path = _points_file(tmp_path)
    config = AppConfig.from_dict(
        {
            "devices": {
                "temperature_chamber": {"port": "SIM", "enabled": True},
                "gas_analyzers": [{"port": f"SIM{i}", "enabled": True} for i in range(analyzer_count)],
            },
            "workflow": {
                "route_mode": "h2o_only",
                "collect_only": True,
                "sampling": {"count": 1, "interval_s": 0.0, "discard_first_n": 0},
                "precheck": {
                    "enabled": True,
                    "device_connection": True,
                    "sensor_check": False,
                    "pressure_leak_test": False,
                },
                "stability": {"temperature": {"analyzer_chamber_temp_enabled": False}},
            },
            "run001_h2o_1_point": {"no_write": True},
            "paths": {"points_excel": str(points_path)},
        }
    )
    device_manager = DeviceManager(config.devices)
    device_manager.register_device("temperature_chamber", FakeTemperatureChamber())
    for index in range(analyzer_count):
        device_manager.register_device(f"gas_analyzer_{index}", FakeAnalyzer(400.0 + index))
    service = CalibrationService(
        config=config,
        device_manager=device_manager,
        stability_checker=ImmediateStabilityChecker(),
    )
    service._raw_cfg = {
        "workflow": {"route_mode": "h2o_only", "collect_only": True},
        "run001_h2o_1_point": {"no_write": True},
    }
    service.orchestrator._log = lambda message: None
    service.orchestrator._check_stop = lambda: None
    service.orchestrator._finish_point_timing = lambda point, phase="", point_tag="": {}
    service.orchestrator._append_result = lambda result: None
    return service


def test_single_gas_analyzer_failure_is_nonblocking_for_h2o_probe(tmp_path: Path) -> None:
    service = _make_service(tmp_path)

    policy = service.orchestrator._classify_device_failures(
        ["gas_analyzer_3"],
        all_devices=["temperature_chamber", "gas_analyzer_0", "gas_analyzer_1", "gas_analyzer_2", "gas_analyzer_3"],
        stage="precheck",
    )

    assert "gas_analyzer_3" not in policy["critical_devices_failed"]
    assert policy["analyzer_failed_nonblocking"] == ["gas_analyzer_3"]
    assert policy["analyzer_failure_blocks_probe"] is False


def test_single_gas_analyzer_failure_records_disabled_analyzer(tmp_path: Path) -> None:
    service = _make_service(tmp_path)

    policy = service.orchestrator._handle_device_failures(
        ["gas_analyzer_3"],
        all_devices=["temperature_chamber", "gas_analyzer_0", "gas_analyzer_1", "gas_analyzer_2", "gas_analyzer_3"],
        error_message="Device precheck failed",
        warning_prefix="Device precheck warnings",
        stage="precheck",
    )
    evidence = service.orchestrator._device_init_policy_summary()

    assert policy["analyzer_failed_nonblocking"] == ["gas_analyzer_3"]
    assert "gas_analyzer_3" in service.orchestrator.run_state.analyzers.disabled
    assert evidence["disabled_analyzers"] == ["gas_analyzer_3"]
    assert evidence["disabled_analyzer_reasons"]["gas_analyzer_3"] == "analyzer_unavailable"


def test_reference_device_failure_still_blocks(tmp_path: Path) -> None:
    service = _make_service(tmp_path)

    try:
        service.orchestrator._handle_device_failures(
            ["pressure_controller"],
            all_devices=["pressure_controller", "gas_analyzer_0"],
            error_message="Device precheck failed",
            warning_prefix="Device precheck warnings",
            stage="precheck",
        )
    except WorkflowValidationError as exc:
        assert "pressure_controller" in exc.context["critical_devices_failed"]
        assert exc.context["reference_devices_ready"] is False
    else:
        raise AssertionError("Expected WorkflowValidationError")


def test_all_analyzers_failed_blocks(tmp_path: Path) -> None:
    service = _make_service(tmp_path, analyzer_count=2)

    try:
        service.orchestrator._handle_device_failures(
            ["gas_analyzer_0", "gas_analyzer_1"],
            all_devices=["gas_analyzer_0", "gas_analyzer_1"],
            error_message="Device precheck failed",
            warning_prefix="Device precheck warnings",
            stage="precheck",
        )
    except WorkflowValidationError as exc:
        assert exc.context["all_analyzers_unavailable"] is True
        assert exc.context["device_failure_policy_result"] == "all_analyzers_unavailable"
        assert exc.context["device_precheck_result"] == "FAIL"
    else:
        raise AssertionError("Expected WorkflowValidationError")


def test_sampling_skips_disabled_analyzer(tmp_path: Path) -> None:
    service = _make_service(tmp_path, analyzer_count=2)
    disabled = service.orchestrator.device_manager.get_device("gas_analyzer_1")
    active = service.orchestrator.device_manager.get_device("gas_analyzer_0")
    service.orchestrator._disable_analyzers(["gas_analyzer_1"], "analyzer_unavailable")
    rows: list[dict] = []
    service.orchestrator.context.run_logger.log_sample = rows.append

    results = service.orchestrator.sampling_service.sample_point(
        CalibrationPoint(index=1, temperature_c=25.0, h2o_mmol=10.0, route="h2o"),
        phase="h2o",
    )

    assert len(results) == 1
    assert active.fetch_calls == 1
    assert disabled.fetch_calls == 0
    assert rows[0]["disabled_analyzers"] == ["gas_analyzer_1"]
    assert rows[0]["analyzer_skipped_labels"] == "GAS_ANALYZER_1"


def test_device_precheck_degraded_continue_evidence(tmp_path: Path) -> None:
    service = _make_service(tmp_path)

    service.orchestrator._handle_device_failures(
        ["gas_analyzer_3"],
        all_devices=["gas_analyzer_0", "gas_analyzer_1", "gas_analyzer_2", "gas_analyzer_3"],
        error_message="Device precheck failed",
        warning_prefix="Device precheck warnings",
        stage="precheck",
    )
    evidence = service.orchestrator._device_init_policy_summary()

    assert evidence["device_precheck_result"] == "DEGRADED_CONTINUE"
    assert evidence["failed_analyzers"] == ["gas_analyzer_3"]
    assert evidence["active_analyzers"] == ["gas_analyzer_0", "gas_analyzer_1", "gas_analyzer_2"]
    assert evidence["all_analyzers_unavailable"] is False
    assert evidence["reference_devices_ready"] is True


def test_no_write_flags_unchanged_by_analyzer_isolation(tmp_path: Path) -> None:
    service = _make_service(tmp_path)

    service.orchestrator._handle_device_failures(
        ["gas_analyzer_3"],
        all_devices=["gas_analyzer_0", "gas_analyzer_1", "gas_analyzer_2", "gas_analyzer_3"],
        error_message="Device precheck failed",
        warning_prefix="Device precheck warnings",
        stage="precheck",
    )
    analyzers = [device for _, device, _ in service.orchestrator._all_gas_analyzers()]

    assert service._raw_cfg["run001_h2o_1_point"]["no_write"] is True
    assert all(not analyzer.write_calls for analyzer in analyzers)


def test_co2_protection_not_touched(tmp_path: Path) -> None:
    service = _make_service(tmp_path)
    service._raw_cfg = {"workflow": {"route_mode": "co2_only", "collect_only": False}}

    policy = service.orchestrator._classify_device_failures(
        ["gas_analyzer_0"],
        all_devices=["gas_analyzer_0", "gas_analyzer_1"],
        stage="precheck",
    )

    assert policy["analyzer_failure_isolation_enabled"] is False
    assert policy["critical_devices_failed"] == ["gas_analyzer_0"]
