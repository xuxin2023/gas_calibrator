from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from gas_calibrator.v2.core.no_write_guard import (
    NoWriteDeviceFactory,
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
from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.entry import (
    Run001A1SafetyGateError,
    authorize_run001_a1_no_write_real_machine_dry_run,
    create_calibration_service,
    create_calibration_service_from_config,
    is_run001_a1_authorized_no_write_real_machine_dry_run,
    load_config_bundle,
)


def _base_raw_config() -> dict:
    return {
        "run001_a1": {
            "run_id": "Run-001/A1",
            "scenario": "Run-001/A1 CO2-only skip0 no-write real-machine dry-run",
            "mode": "real_machine_dry_run",
            "no_write": True,
            "co2_only": True,
            "skip_co2_ppm": [0],
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


def _cli_args(**overrides) -> dict:
    payload = {
        "execute": True,
        "confirm_real_machine_no_write": True,
        "allow_unsafe_step2_config": False,
    }
    payload.update(overrides)
    return payload


def _write_config(tmp_path: Path, raw: dict) -> Path:
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    payload = copy.deepcopy(raw)
    payload.setdefault("paths", {})
    payload["paths"]["points_excel"] = "points.json"
    payload["paths"]["output_dir"] = "output"
    payload["paths"]["logs_dir"] = "logs"
    (config_dir / "points.json").write_text(json.dumps({"points": _co2_points()}), encoding="utf-8")
    config_path = config_dir / "run001_a1.json"
    config_path.write_text(json.dumps(payload), encoding="utf-8")
    return config_path


def _authorize_or_raise(raw: dict, *, cli_args: dict | None = None, config_path: str = "") -> dict:
    app_config = AppConfig.from_dict(raw)
    return authorize_run001_a1_no_write_real_machine_dry_run(
        app_config,
        raw,
        cli_args or _cli_args(),
        config_path=config_path or str(Path(__file__).resolve()),
        config_safety={"execution_gate": {"status": "blocked"}},
        allow_unsafe_step2_config=False,
    )


def _assert_gate_rejects(raw: dict, expected_reason: str, *, cli_args: dict | None = None) -> None:
    with pytest.raises(Run001A1SafetyGateError) as exc_info:
        _authorize_or_raise(raw, cli_args=cli_args)
    assert expected_reason in exc_info.value.reasons


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
    assert payload["unsafe_step2_bypass_used"] is False


def test_run001_a1_safe_gate_allows_no_write_without_unsafe_bypass(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("GAS_CALIBRATOR_V2_ALLOW_UNSAFE_CONFIG", raising=False)
    config_path = _write_config(tmp_path, _base_raw_config())

    service = create_calibration_service(
        str(config_path),
        simulation_mode=False,
        allow_unsafe_step2_config=False,
        run001_a1_no_write_dry_run_cli_args=_cli_args(),
    )

    assert isinstance(service.no_write_guard, NoWriteGuard)
    gate = dict(getattr(service.config, "_run001_a1_safety_gate", {}) or {})
    assert gate["status"] == "authorized"
    assert gate["unsafe_step2_bypass_used"] is False
    assert service.config.workflow.skip_co2_ppm == [0]


def test_run001_a1_gate_requires_confirm_without_creating_service(tmp_path: Path, monkeypatch) -> None:
    import gas_calibrator.v2.entry as entry_module

    config_path = _write_config(tmp_path, _base_raw_config())
    created = {"service": False}

    def _unexpected_service(*_args, **_kwargs):
        created["service"] = True
        raise AssertionError("service creation should not be reached")

    monkeypatch.setattr(entry_module, "create_calibration_service_from_config", _unexpected_service)

    with pytest.raises(Run001A1SafetyGateError) as exc_info:
        entry_module.create_calibration_service(
            str(config_path),
            simulation_mode=False,
            allow_unsafe_step2_config=False,
            run001_a1_no_write_dry_run_cli_args=_cli_args(confirm_real_machine_no_write=False),
        )

    assert "confirm_real_machine_no_write_missing" in exc_info.value.reasons
    assert created["service"] is False


def test_run001_a1_gate_rejects_no_write_false_without_creating_service(tmp_path: Path, monkeypatch) -> None:
    import gas_calibrator.v2.entry as entry_module

    raw = _base_raw_config()
    raw["run001_a1"]["no_write"] = False
    config_path = _write_config(tmp_path, raw)
    created = {"service": False}

    def _unexpected_service(*_args, **_kwargs):
        created["service"] = True
        raise AssertionError("service creation should not be reached")

    monkeypatch.setattr(entry_module, "create_calibration_service_from_config", _unexpected_service)

    with pytest.raises(Run001A1SafetyGateError) as exc_info:
        entry_module.create_calibration_service(
            str(config_path),
            simulation_mode=False,
            allow_unsafe_step2_config=False,
            run001_a1_no_write_dry_run_cli_args=_cli_args(),
        )

    assert "no_write_not_true" in exc_info.value.reasons
    assert created["service"] is False


def test_run001_a1_gate_rejects_h2o_single_route_and_full_group() -> None:
    h2o = _base_raw_config()
    h2o["workflow"]["route_mode"] = "h2o_only"
    _assert_gate_rejects(h2o, "h2o_scope_requested")

    full_group = _base_raw_config()
    full_group["run001_a1"]["full_h2o_co2_group"] = True
    _assert_gate_rejects(full_group, "full_h2o_co2_group_requested")


@pytest.mark.parametrize("skip_value", [[], None, [200], [0, 200]])
def test_run001_a1_gate_requires_skip_co2_ppm_locked_to_zero(skip_value) -> None:
    raw = _base_raw_config()
    if skip_value is None:
        raw["run001_a1"].pop("skip_co2_ppm", None)
        raw["workflow"].pop("skip_co2_ppm", None)
        expected_reason = "policy_skip_co2_ppm_not_locked_to_0"
    else:
        raw["run001_a1"]["skip_co2_ppm"] = skip_value
        raw["workflow"]["skip_co2_ppm"] = skip_value
        expected_reason = "policy_skip_co2_ppm_not_locked_to_0"

    _assert_gate_rejects(raw, expected_reason)


def test_run001_a1_gate_rejects_non_single_route_and_non_single_temperature() -> None:
    raw = _base_raw_config()
    raw["run001_a1"]["single_route"] = False
    _assert_gate_rejects(raw, "single_route_not_true")

    raw = _base_raw_config()
    raw["run001_a1"]["single_temperature_group"] = False
    raw["workflow"]["selected_temps_c"] = [20.0, 30.0]
    _assert_gate_rejects(raw, "single_temperature_group_not_true")


@pytest.mark.parametrize(
    "flag",
    [
        "allow_write_coefficients",
        "allow_write_zero",
        "allow_write_span",
        "allow_write_calibration_parameters",
        "default_cutover_to_v2",
        "disable_v1",
    ],
)
def test_run001_a1_gate_rejects_any_write_or_cutover_flag(flag: str) -> None:
    raw = _base_raw_config()
    raw["run001_a1"][flag] = True

    expected_reason = f"{flag}_true" if flag != "disable_v1" else "disable_v1_true"
    _assert_gate_rejects(raw, expected_reason)


def test_run001_a1_gate_does_not_accept_unsafe_bypass_as_required_evidence() -> None:
    raw = _base_raw_config()

    assert is_run001_a1_authorized_no_write_real_machine_dry_run(
        AppConfig.from_dict(raw),
        raw,
        _cli_args(),
        config_path=str(Path(__file__).resolve()),
        allow_unsafe_step2_config=False,
    )
    _assert_gate_rejects(
        raw,
        "unsafe_step2_bypass_not_allowed_for_run001_a1",
        cli_args=_cli_args(allow_unsafe_step2_config=True),
    )


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
        allow_unsafe_step2_config=False,
        enforce_step2_execution_gate=False,
    )
    service = create_calibration_service_from_config(app_config, raw_cfg=raw_cfg, preload_points=False)

    assert isinstance(service.no_write_guard, NoWriteGuard)
    assert service.config.workflow.route_mode == "co2_only"
    assert service.config.workflow.skip_co2_ppm == [0]


def test_no_write_guard_is_wrapped_before_service_initialization(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, _base_raw_config())
    _, raw_cfg, app_config = load_config_bundle(
        str(config_path),
        simulation_mode=False,
        allow_unsafe_step2_config=False,
        enforce_step2_execution_gate=False,
    )
    captured: dict[str, object] = {}

    class CapturingService:
        def __init__(self, *, config, device_factory, point_parser, **_kwargs) -> None:
            captured["device_factory"] = device_factory
            captured["guard"] = getattr(device_factory, "guard", None)
            self.config = config

    service = create_calibration_service_from_config(
        app_config,
        raw_cfg=raw_cfg,
        preload_points=False,
        service_cls=CapturingService,
        require_no_write_guard=True,
    )

    assert service.config is app_config
    assert isinstance(captured["device_factory"], NoWriteDeviceFactory)
    assert isinstance(captured["guard"], NoWriteGuard)


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
