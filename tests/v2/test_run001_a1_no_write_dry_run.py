from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from gas_calibrator.v2.core.no_write_guard import (
    NoWriteGuard,
    NoWriteViolation,
    build_no_write_guard_from_raw_config,
)
from gas_calibrator.v2.core.run001_a1_dry_run import (
    RUN001_FAIL,
    RUN001_PASS,
    build_run001_a1_evidence_payload,
    evaluate_run001_a1_readiness,
    load_point_rows,
    write_run001_a1_artifacts,
)
from gas_calibrator.v2.entry import create_calibration_service_from_config, load_config_bundle


def _base_raw_config() -> dict:
    return {
        "run001_a1": {
            "mode": "real_machine_dry_run",
            "no_write": True,
            "co2_only": True,
            "single_route": True,
            "single_temperature_group": True,
            "allow_real_route": True,
            "allow_real_pressure": True,
            "allow_real_wait": True,
            "allow_real_sample": True,
            "allow_artifact": True,
            "allow_write_coefficients": False,
            "allow_write_zero": False,
            "allow_write_span": False,
            "allow_write_calibration_parameters": False,
            "default_cutover_to_v2": False,
            "disable_v1": False,
            "full_h2o_co2_group": False,
        },
        "workflow": {
            "route_mode": "co2_only",
            "selected_temps_c": [20.0],
            "skip_co2_ppm": [0],
            "sampling": {"count": 10, "stable_count": 10, "interval_s": 1.0},
        },
        "paths": {},
        "features": {"use_v2": True, "simulation_mode": False},
    }


def _co2_points() -> list[dict]:
    return [
        {"index": 1, "temperature_c": 20.0, "pressure_hpa": 1100.0, "route": "co2", "co2_ppm": 0.0},
        {"index": 2, "temperature_c": 20.0, "pressure_hpa": 1000.0, "route": "co2", "co2_ppm": 100.0},
    ]


class FakeAnalyzer:
    def __init__(self) -> None:
        self.ser = FakeAnalyzerSerial()

    def write(self, data: str) -> str:
        return f"wrote:{data}"

    def query(self, data: str) -> str:
        return f"query:{data}"

    def read(self) -> dict:
        return {"co2_ppm": 100.0, "h2o_mmol": 0.0}

    def read_latest_data(self) -> str:
        return "<YGAS,001,2,100.0,0.0>"

    def set_senco(self, *_args, **_kwargs):
        return True

    def write_coefficients(self):
        return True

    def set_coefficients(self):
        return True

    def write_zero(self):
        return True

    def write_span(self):
        return True

    def apply_calibration(self):
        return True

    def commit_calibration(self):
        return True

    def save_parameters(self):
        return True

    def writeback(self):
        return True

    def write_eeprom(self):
        return True

    def write_flash(self):
        return True

    def write_nvm(self):
        return True

    def eeprom_write(self):
        return True

    def flash_write(self):
        return True

    def nvm_write(self):
        return True

    def calibration_commit(self):
        return True


class FakeAnalyzerSerial:
    def write(self, data: str) -> str:
        return f"serial:{data}"

    def query(self, data: str) -> str:
        return f"serial_query:{data}"

    def readline(self) -> str:
        return "<YGAS,001,2,100.0,0.0>"


class FakeRelay:
    def write_coil(self, channel: int, value: bool) -> bool:
        return bool(value) or channel >= 0

    def write_register(self, address: int, value: int) -> int:
        return int(address) + int(value)

    def read_coils(self, start: int, count: int = 1) -> list[bool]:
        return [False] * int(count)


class FakePressureController:
    def set_pressure(self, pressure_hpa: float) -> float:
        return float(pressure_hpa)

    def set_output(self, enabled: bool) -> bool:
        return bool(enabled)

    def set_isolation(self, enabled: bool) -> bool:
        return bool(enabled)

    def vent(self) -> str:
        return "vented"

    def wait_until_stable(self) -> bool:
        return True

    def write(self, cmd: str) -> str:
        return f"pressure:{cmd}"

    def query(self, cmd: str) -> str:
        return f"pressure_query:{cmd}"

    def read_pressure(self) -> float:
        return 1000.0


def test_no_write_allows_runtime_route_pressure_wait_and_sample_actions() -> None:
    guard = NoWriteGuard()
    relay = guard.guard_device(FakeRelay(), device_name="relay_a", device_type="relay")
    pressure = guard.guard_device(
        FakePressureController(),
        device_name="pace",
        device_type="pressure_controller",
    )
    analyzer = guard.guard_device(FakeAnalyzer(), device_name="ga01", device_type="gas_analyzer")

    assert relay.write_coil(7, True) is True
    assert relay.write_register(1, 1) == 2
    assert relay.read_coils(0, 2) == [False, False]
    assert pressure.set_pressure(1000.0) == 1000.0
    assert pressure.set_output(True) is True
    assert pressure.set_isolation(False) is False
    assert pressure.vent() == "vented"
    assert pressure.wait_until_stable() is True
    assert pressure.write(":SOUR:PRES 1000") == "pressure::SOUR:PRES 1000"
    assert pressure.query(":MEAS:PRES?") == "pressure_query::MEAS:PRES?"
    assert pressure.read_pressure() == 1000.0
    assert analyzer.write("READDATA,YGAS,FFF\r\n") == "wrote:READDATA,YGAS,FFF\r\n"
    assert analyzer.read()["co2_ppm"] == 100.0

    assert guard.attempted_write_count == 0
    assert guard.blocked_events == []


def test_no_write_allows_analyzer_read_query_and_active_upload_sampling() -> None:
    guard = NoWriteGuard()
    analyzer = guard.guard_device(FakeAnalyzer(), device_name="ga01", device_type="gas_analyzer")

    assert analyzer.read_latest_data() == "<YGAS,001,2,100.0,0.0>"
    assert analyzer.read()["h2o_mmol"] == 0.0
    assert analyzer.query("READDATA,YGAS,FFF?") == "query:READDATA,YGAS,FFF?"
    assert analyzer.ser.readline() == "<YGAS,001,2,100.0,0.0>"
    assert analyzer.ser.write("GETCO9,YGAS,FFF\r\n") == "serial:GETCO9,YGAS,FFF\r\n"

    assert guard.attempted_write_count == 0
    assert guard.blocked_events == []


@pytest.mark.parametrize(
    "method_name",
    [
        "set_senco",
        "write_coefficients",
        "set_coefficients",
        "write_zero",
        "write_span",
        "apply_calibration",
        "commit_calibration",
        "save_parameters",
        "writeback",
        "write_eeprom",
        "write_flash",
        "write_nvm",
        "eeprom_write",
        "flash_write",
        "nvm_write",
        "calibration_commit",
    ],
)
def test_no_write_blocks_calibration_parameter_write_methods(method_name: str) -> None:
    guard = NoWriteGuard()
    analyzer = guard.guard_device(FakeAnalyzer(), device_name="ga01", device_type="gas_analyzer")

    with pytest.raises(NoWriteViolation):
        getattr(analyzer, method_name)()

    assert guard.attempted_write_count == 1
    assert guard.blocked_events[0]["method_name"] == method_name


@pytest.mark.parametrize("payload", ["SENCO9,YGAS,FFF,0,1,0,0\r\n", "SAVE_PARAMETERS", "WRITEBACK EEPROM"])
def test_no_write_blocks_raw_analyzer_calibration_write_payloads(payload: str) -> None:
    guard = NoWriteGuard()
    analyzer = guard.guard_device(FakeAnalyzer(), device_name="ga01", device_type="gas_analyzer")

    with pytest.raises(NoWriteViolation):
        analyzer.write(payload)

    assert guard.attempted_write_count == 1
    assert guard.blocked_events[0]["method_name"] == "write"


def test_no_write_blocks_raw_analyzer_serial_calibration_write_payloads() -> None:
    guard = NoWriteGuard()
    analyzer = guard.guard_device(FakeAnalyzer(), device_name="ga01", device_type="gas_analyzer")

    with pytest.raises(NoWriteViolation):
        analyzer.ser.write("SENCO9,YGAS,FFF,0,1,0,0\r\n")

    assert guard.attempted_write_count == 1
    assert guard.blocked_events[0]["device_type"] == "gas_analyzer_serial"


def test_no_write_true_blocks_coefficient_write() -> None:
    guard = NoWriteGuard()
    analyzer = guard.guard_device(FakeAnalyzer(), device_name="ga01", device_type="gas_analyzer")

    with pytest.raises(NoWriteViolation):
        analyzer.set_senco(1, [1.0, 2.0])

    assert guard.attempted_write_count == 1
    assert guard.blocked_events[0]["method_name"] == "set_senco"


def test_no_write_true_blocks_zero_and_span_writes() -> None:
    guard = NoWriteGuard()
    analyzer = guard.guard_device(FakeAnalyzer(), device_name="ga01", device_type="gas_analyzer")

    with pytest.raises(NoWriteViolation):
        analyzer.write_zero()
    with pytest.raises(NoWriteViolation):
        analyzer.write_span()

    assert guard.attempted_write_count == 2
    assert [event["method_name"] for event in guard.blocked_events] == ["write_zero", "write_span"]


def test_attempted_write_count_makes_readiness_fail() -> None:
    result = evaluate_run001_a1_readiness(
        _base_raw_config(),
        point_rows=_co2_points(),
        attempted_write_count=1,
    )

    assert result["readiness_result"] == RUN001_FAIL
    assert "attempted_write_count_gt_0" in result["hard_stop_reasons"]


def test_h2o_config_is_rejected_for_run001() -> None:
    raw = _base_raw_config()
    raw["workflow"]["route_mode"] = "h2o_only"

    result = evaluate_run001_a1_readiness(raw, point_rows=_co2_points())

    assert result["readiness_result"] == RUN001_FAIL
    assert "route_mode_not_co2_only" in result["hard_stop_reasons"]
    assert "h2o_scope_requested" in result["hard_stop_reasons"]


def test_full_h2o_co2_group_is_rejected_for_run001() -> None:
    raw = _base_raw_config()
    raw["run001_a1"]["full_h2o_co2_group"] = True

    result = evaluate_run001_a1_readiness(raw, point_rows=_co2_points())

    assert result["readiness_result"] == RUN001_FAIL
    assert "full_h2o_co2_group_requested" in result["hard_stop_reasons"]


def test_skip_co2_ppm_zero_is_retained() -> None:
    payload = build_run001_a1_evidence_payload(_base_raw_config(), point_rows=_co2_points())

    assert payload["skip_co2_ppm"] == [0]
    assert payload["final_decision"] == RUN001_PASS


def test_single_route_and_single_temperature_are_enforced() -> None:
    raw = _base_raw_config()
    raw["workflow"]["selected_temps_c"] = [20.0, 30.0]
    points = _co2_points() + [
        {"index": 3, "temperature_c": 30.0, "pressure_hpa": 1000.0, "route": "co2", "co2_ppm": 300.0}
    ]

    result = evaluate_run001_a1_readiness(raw, point_rows=points)

    assert result["readiness_result"] == RUN001_FAIL
    assert "not_single_temperature_group" in result["hard_stop_reasons"]
    assert "points_include_multiple_temperature_groups" in result["hard_stop_reasons"]


def test_no_write_false_cannot_enter_run001() -> None:
    raw = _base_raw_config()
    raw["run001_a1"]["no_write"] = False

    result = evaluate_run001_a1_readiness(raw, point_rows=_co2_points())

    assert result["readiness_result"] == RUN001_FAIL
    assert "no_write_not_true" in result["hard_stop_reasons"]


def test_default_v2_cutover_and_disable_v1_are_blocked() -> None:
    raw = _base_raw_config()
    raw["run001_a1"]["default_cutover_to_v2"] = True
    raw["run001_a1"]["disable_v1"] = True

    result = evaluate_run001_a1_readiness(raw, point_rows=_co2_points())

    assert result["readiness_result"] == RUN001_FAIL
    assert "default_cutover_to_v2_true" in result["hard_stop_reasons"]
    assert "v1_fallback_unavailable_or_disabled" in result["hard_stop_reasons"]


def test_v1_entry_and_main_workflow_changes_are_blocked() -> None:
    result = evaluate_run001_a1_readiness(
        _base_raw_config(),
        point_rows=_co2_points(),
        changed_paths=["run_app.py", "src/gas_calibrator/workflow/runner.py"],
    )

    assert result["readiness_result"] == RUN001_FAIL
    assert "v1_forbidden_change_detected" in result["hard_stop_reasons"]


def test_artifact_missing_makes_runtime_readiness_fail(tmp_path: Path) -> None:
    missing = tmp_path / "missing.json"
    result = evaluate_run001_a1_readiness(
        _base_raw_config(),
        point_rows=_co2_points(),
        artifact_paths={"summary": missing, "manifest": missing, "trace": missing},
        require_runtime_artifacts=True,
    )

    assert result["readiness_result"] == RUN001_FAIL
    assert "required_artifact_missing_summary" in result["hard_stop_reasons"]
    assert "required_artifact_missing_manifest" in result["hard_stop_reasons"]
    assert "required_artifact_missing_trace" in result["hard_stop_reasons"]


def test_normal_co2_only_skip0_no_write_preflight_passes_and_writes_artifacts(tmp_path: Path) -> None:
    payload = build_run001_a1_evidence_payload(_base_raw_config(), point_rows=_co2_points(), run_dir=tmp_path)
    written = write_run001_a1_artifacts(tmp_path, payload)

    assert payload["final_decision"] == RUN001_PASS
    assert set(written) == {"summary", "no_write_guard", "readiness", "trace", "manifest", "report"}
    readiness = json.loads((tmp_path / "readiness.json").read_text(encoding="utf-8"))
    guard = json.loads((tmp_path / "no_write_guard.json").read_text(encoding="utf-8"))
    assert readiness["final_decision"] == RUN001_PASS
    assert guard["attempted_write_count"] == 0


def test_config_template_passes_preflight() -> None:
    config_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "gas_calibrator"
        / "v2"
        / "configs"
        / "validation"
        / "run001_a1_co2_only_skip0_no_write_real_machine_dry_run.json"
    )
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    points = load_point_rows(config_path, raw)
    payload = build_run001_a1_evidence_payload(raw, config_path=config_path, point_rows=points)

    assert payload["final_decision"] == RUN001_PASS
    assert payload["mode"] == "real_machine_dry_run"
    assert payload["no_write"] is True
    assert payload["temperature_group"] == [20.0]
    assert payload["h2o_single_route_readiness"] == "yellow"
    assert payload["full_single_temperature_h2o_co2_group_readiness"] == "yellow"


def test_run001_guard_is_installed_when_service_is_created_from_config() -> None:
    config_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "gas_calibrator"
        / "v2"
        / "configs"
        / "validation"
        / "run001_a1_co2_only_skip0_no_write_real_machine_dry_run.json"
    )
    _, raw_cfg, app_config = load_config_bundle(
        str(config_path),
        simulation_mode=False,
        allow_unsafe_step2_config=True,
        enforce_step2_execution_gate=False,
    )
    service = create_calibration_service_from_config(app_config, raw_cfg=raw_cfg, preload_points=False)

    assert isinstance(service.no_write_guard, NoWriteGuard)
    assert service.config.workflow.route_mode == "co2_only"
    assert service.config.workflow.skip_co2_ppm == [0]


def test_no_write_guard_requires_true_for_real_machine_dry_run() -> None:
    raw = _base_raw_config()
    raw["run001_a1"]["no_write"] = False

    with pytest.raises(RuntimeError):
        build_no_write_guard_from_raw_config(raw)


def test_readiness_input_is_not_mutated() -> None:
    raw = _base_raw_config()
    original = copy.deepcopy(raw)

    evaluate_run001_a1_readiness(raw, point_rows=_co2_points())

    assert raw == original
