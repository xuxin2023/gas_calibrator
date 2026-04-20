from __future__ import annotations

import csv
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import gas_calibrator.tools.run_room_temp_co2_pressure_diagnostic as room_temp_diag_tool
from gas_calibrator.devices.pace5000 import Pace5000
from gas_calibrator.tools.run_room_temp_co2_pressure_diagnostic import VariantSpec, parse_args
from gas_calibrator.tools.safe_stop import perform_safe_stop, perform_safe_stop_with_retries


class _FakeLegacyDiagnosticPace:
    def __init__(self, *, ok: bool = True, legacy_identity: bool = True, reason: str = "") -> None:
        self.ok = bool(ok)
        self.legacy_identity = bool(legacy_identity)
        self.reason = reason or (
            "legacy_safe_vent_observed(action=safe_stop,last_status=1,vent_command_sent=true)"
            if ok
            else "legacy_safe_vent_blocked(action=safe_stop,step=observe_timeout,last_status=3,output_state=0,isolation_state=1,recoverable=true)"
        )
        self.actions: list[str] = []
        self.enter_atmosphere_mode_calls = 0

    def has_legacy_vent_status_model(self) -> bool:
        return self.legacy_identity

    def enter_legacy_diagnostic_safe_vent_mode(self, *, action: str, **_: object) -> dict[str, object]:
        self.actions.append(str(action))
        return {
            "action": action,
            "legacy_identity": self.legacy_identity,
            "ok": self.ok,
            "recoverable": True,
            "reason": self.reason,
        }

    def enter_atmosphere_mode(self) -> None:
        self.enter_atmosphere_mode_calls += 1

    def read_pressure(self) -> float:
        return 1002.0

    def query(self, cmd: str) -> str:
        mapping = {
            ":SOUR:PRES:LEV:IMM:AMPL:VENT?": ":SOUR:PRES:LEV:IMM:AMPL:VENT 2",
            ":OUTP:STAT?": ":OUTP:STAT 0",
            ":OUTP:ISOL:STAT?": ":OUTP:ISOL:STAT 1",
        }
        return mapping[cmd]


def test_pace_legacy_safe_vent_helper_never_sends_raw_vent0_when_observing_vent_on() -> None:
    pace = object.__new__(Pace5000)
    calls: list[tuple[str, object]] = []
    statuses = iter([Pace5000.VENT_STATUS_IDLE, Pace5000.VENT_STATUS_IN_PROGRESS])

    pace.has_legacy_vent_status_model = lambda: True
    pace.stop_atmosphere_hold = lambda: calls.append(("stop_hold", None))
    pace.set_output = lambda on: calls.append(("set_output", bool(on)))
    pace.set_isolation_open = lambda is_open: calls.append(("set_isolation_open", bool(is_open)))
    pace.get_output_state = lambda: 0
    pace.get_isolation_state = lambda: 1
    pace.get_vent_status = lambda: next(statuses)
    pace.vent = lambda on=True: calls.append(("vent", bool(on)))

    result = Pace5000.enter_legacy_diagnostic_safe_vent_mode(
        pace,
        timeout_s=0.5,
        poll_s=0.01,
        action="unit_test_safe_vent",
    )

    assert result["ok"] is True
    assert ("vent", True) in calls
    assert ("vent", False) not in calls


def test_pace_legacy_safe_vent_helper_blocks_status3_with_clear_reason() -> None:
    pace = object.__new__(Pace5000)
    calls: list[tuple[str, object]] = []

    pace.has_legacy_vent_status_model = lambda: True
    pace.stop_atmosphere_hold = lambda: calls.append(("stop_hold", None))
    pace.set_output = lambda on: calls.append(("set_output", bool(on)))
    pace.set_isolation_open = lambda is_open: calls.append(("set_isolation_open", bool(is_open)))
    pace.get_output_state = lambda: 0
    pace.get_isolation_state = lambda: 1
    pace.get_vent_status = lambda: Pace5000.VENT_STATUS_TRAPPED_PRESSURE
    pace.vent = lambda on=True: calls.append(("vent", bool(on)))

    result = Pace5000.enter_legacy_diagnostic_safe_vent_mode(
        pace,
        timeout_s=0.5,
        poll_s=0.01,
        action="unit_test_blocked",
    )

    assert result["ok"] is False
    assert "legacy_safe_vent_blocked" in str(result["reason"])
    assert ("vent", False) not in calls


def test_safe_stop_diagnostic_safe_vent_does_not_fallback_to_enter_atmosphere_mode_for_legacy() -> None:
    pace = _FakeLegacyDiagnosticPace(ok=True, legacy_identity=True)

    result = perform_safe_stop(
        {"pace": pace},
        log_fn=lambda *_: None,
        pace_mode="diagnostic_safe_vent",
    )

    assert result["pace_diagnostic_safe_vent"]["ok"] is True
    assert pace.actions == ["safe_stop"]
    assert pace.enter_atmosphere_mode_calls == 0


def test_safe_stop_diagnostic_safe_vent_returns_blocked_reason_without_retries() -> None:
    reason = (
        "legacy_safe_vent_blocked(action=safe_stop,step=observe_timeout,"
        "last_status=3,output_state=0,isolation_state=1,recoverable=true)"
    )
    pace = _FakeLegacyDiagnosticPace(ok=False, legacy_identity=True, reason=reason)

    result = perform_safe_stop_with_retries(
        {"pace": pace},
        log_fn=lambda *_: None,
        pace_mode="diagnostic_safe_vent",
        attempts=3,
        retry_delay_s=0.0,
    )

    assert result["safe_stop_verified"] is False
    assert result["safe_stop_attempt"] == 1
    assert result["safe_stop_issues"] == [reason]
    assert pace.actions == ["safe_stop"]
    assert pace.enter_atmosphere_mode_calls == 0


def test_non_legacy_diagnostic_safe_vent_mode_falls_back_to_existing_safe_stop_behavior() -> None:
    pace = _FakeLegacyDiagnosticPace(ok=False, legacy_identity=False, reason="non_legacy_identity")

    result = perform_safe_stop(
        {"pace": pace},
        log_fn=lambda *_: None,
        pace_mode="diagnostic_safe_vent",
    )

    assert result["pace_diagnostic_safe_vent"]["legacy_identity"] is False
    assert pace.actions == ["safe_stop"]
    assert pace.enter_atmosphere_mode_calls == 1


def test_parse_args_accepts_skip_gas_analyzer_flag() -> None:
    args = parse_args(
        [
            "--allow-live-hardware",
            "--smoke-level",
            "analyzer-chain-isolation",
            "--skip-gas-analyzer",
        ]
    )

    assert args.skip_gas_analyzer is True


def test_main_rejects_skip_gas_analyzer_outside_analyzer_chain_isolation() -> None:
    rc = room_temp_diag_tool.main(
        [
            "--allow-live-hardware",
            "--skip-gas-analyzer",
        ]
    )

    assert rc == 2


def test_build_devices_skip_gas_analyzer_does_not_construct_or_open_analyzer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gas_analyzer_constructed: list[str] = []

    class _UnexpectedGasAnalyzer:
        def __init__(self, *_args, **_kwargs) -> None:
            gas_analyzer_constructed.append("constructed")
            raise AssertionError("gas analyzer should not be constructed in skip mode")

    class _FakeDevice:
        def __init__(self, port: str, *_args, **_kwargs) -> None:
            self.port = port
            self.opened = False

        def open(self) -> None:
            self.opened = True

    monkeypatch.setattr(room_temp_diag_tool, "GasAnalyzer", _UnexpectedGasAnalyzer)
    monkeypatch.setattr(room_temp_diag_tool, "Pace5000", _FakeDevice)
    monkeypatch.setattr(room_temp_diag_tool, "ParoscientificGauge", _FakeDevice)
    monkeypatch.setattr(room_temp_diag_tool, "DewpointMeter", _FakeDevice)
    monkeypatch.setattr(room_temp_diag_tool, "RelayController", _FakeDevice)

    runtime_cfg = {
        "devices": {
            "pressure_controller": {"enabled": True, "port": "COM1"},
            "pressure_gauge": {"enabled": True, "port": "COM2"},
            "dewpoint_meter": {"enabled": True, "port": "COM3"},
            "relay": {"enabled": True, "port": "COM4"},
            "relay_8": {"enabled": True, "port": "COM5"},
        }
    }

    built = room_temp_diag_tool._build_devices(
        runtime_cfg,
        {"port": "COM99", "device_id": "099"},
        SimpleNamespace(),
        skip_gas_analyzer=True,
    )

    assert gas_analyzer_constructed == []
    assert "gas_analyzer" not in built
    assert built["pace"].opened is True
    assert built["pressure_gauge"].opened is True
    assert built["dewpoint"].opened is True
    assert built["relay"].opened is True
    assert built["relay_8"].opened is True


def test_apply_analyzer_chain_pressure_baseline_legacy_safe_path_skips_runner_vent_methods() -> None:
    pace = _FakeLegacyDiagnosticPace(ok=True, legacy_identity=True)

    class _FakeRunner:
        def __init__(self) -> None:
            self.devices = {"pace": pace}
            self.route_baseline_calls = 0
            self.co2_route_baseline_calls = 0

        def _apply_route_baseline_valves(self) -> None:
            self.route_baseline_calls += 1

        def _set_co2_route_baseline(self, reason: str) -> None:
            self.co2_route_baseline_calls += 1
            raise AssertionError(f"unexpected raw baseline path: {reason}")

    runner = _FakeRunner()

    result = room_temp_diag_tool._apply_analyzer_chain_pressure_baseline(
        runner,
        cfg={"workflow": {"pressure": {"vent_transition_timeout_s": 2.0}}},
        reason="unit test final restore",
        legacy_safe_vent_active=True,
        run_id="run-1",
        actuation_events=[],
        process_variant="B",
        layer=0,
        repeat_index=1,
        gas_ppm=0,
        chain_mode="analyzer_in_keep_rest",
        action="analyzer_chain_final_restore",
    )

    assert result["ok"] is True
    assert pace.actions == ["analyzer_chain_final_restore"]
    assert runner.route_baseline_calls == 1
    assert runner.co2_route_baseline_calls == 0


def test_analyzer_chain_legacy_safe_vent_policy_blocks_when_closed_pressure_swing_enabled() -> None:
    pace = _FakeLegacyDiagnosticPace(ok=True, legacy_identity=True)

    policy = room_temp_diag_tool._analyzer_chain_legacy_safe_vent_policy(
        {"diagnostics": {"precondition": {"closed_pressure_swing": {"enabled": True}}}},
        pace=pace,
        pace_in_path=True,
    )

    assert policy["legacy_identity_detected"] is True
    assert policy["legacy_safe_vent_required"] is True
    assert policy["legacy_safe_vent_active"] is False
    assert policy["legacy_safe_vent_fail_reason"] == "closed_pressure_swing_enabled"


def test_analyzer_chain_isolation_capture_uses_capture_only_startup_gate_and_skips_raw_vent0_routes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    pace = _FakeLegacyDiagnosticPace(ok=True, legacy_identity=True)
    analyzer = SimpleNamespace(active_send=False)

    class _FakeRunner:
        instances: list["_FakeRunner"] = []

        def __init__(self, _cfg, devices, _logger, _log_fn, _trace_cb) -> None:
            self.devices = devices
            self.apply_route_baseline_calls = 0
            self.apply_valve_states_calls: list[tuple[int, ...]] = []
            self.configure_devices_calls: list[bool] = []
            self.started_labels: list[str] = []
            self._disabled_analyzers: set[str] = set()
            self._disabled_analyzer_reasons: dict[str, str] = {}
            self._disabled_analyzer_last_reprobe_ts: dict[str, float] = {}
            type(self).instances.append(self)

        def _configure_devices(self, *, configure_gas_analyzers: bool = True) -> None:
            self.configure_devices_calls.append(bool(configure_gas_analyzers))
            if configure_gas_analyzers:
                raise AssertionError("analyzer-chain-isolation should not use public all-analyzer startup gate")
            return None

        def _gas_analyzer_runtime_settings(self, cfg) -> dict[str, object]:
            return {
                "mode": 2,
                "active_send": bool(cfg.get("active_send", False)),
                "ftd_hz": int(cfg.get("ftd_hz", 1)),
                "avg_co2": int(cfg.get("average_co2", 1)),
                "avg_h2o": int(cfg.get("average_h2o", 1)),
                "avg_filter": int(cfg.get("average_filter", 49)),
            }

        def _configure_gas_analyzer(self, ga, *, label: str, **_kwargs) -> None:
            assert ga is analyzer
            self.started_labels.append(str(label))

        def _disable_analyzers(self, labels, reason: str) -> None:
            for label in labels:
                self._disabled_analyzers.add(str(label))
                self._disabled_analyzer_reasons[str(label)] = str(reason)

        def _apply_valve_states(self, open_logical_valves) -> None:
            self.apply_valve_states_calls.append(tuple(int(v) for v in open_logical_valves))

        def _apply_route_baseline_valves(self) -> None:
            self.apply_route_baseline_calls += 1

        def _set_pressure_controller_vent(self, *_args, **_kwargs) -> None:
            raise AssertionError("analyzer-chain-isolation should not call _set_pressure_controller_vent on legacy safe vent path")

        def _set_co2_route_baseline(self, *_args, **_kwargs) -> None:
            raise AssertionError("analyzer-chain-isolation should not call _set_co2_route_baseline on legacy safe vent path")

    monkeypatch.setattr(room_temp_diag_tool, "_build_devices", lambda *_args, **_kwargs: {"gas_analyzer": analyzer, "pace": pace})
    monkeypatch.setattr(room_temp_diag_tool, "CalibrationRunner", _FakeRunner)
    monkeypatch.setattr(room_temp_diag_tool, "_configure_analyzer_stream", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        room_temp_diag_tool,
        "_route_for_point",
        lambda *_args, **_kwargs: {
            "source_valve": 7,
            "path_valve": 8,
            "open_logical_valves": [7, 8],
        },
    )
    monkeypatch.setattr(
        room_temp_diag_tool,
        "_run_closed_pressure_swing_predry",
        lambda *_args, **_kwargs: ([], [], room_temp_diag_tool._closed_pressure_swing_defaults({})),
    )
    monkeypatch.setattr(room_temp_diag_tool, "_run_sealed_hold_phase", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("sealed hold should stay disabled")))
    monkeypatch.setattr(room_temp_diag_tool, "_run_pressure_sweep_phase", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("pressure sweep should stay disabled")))

    def _fake_run_flush_phase(*_args, **kwargs):
        vent_on_handler = kwargs.get("vent_on_handler")
        assert callable(vent_on_handler)
        vent_on_handler("unit test legacy flush vent")
        return (
            [],
            {
                "flush_gate_status": "pass",
                "flush_gate_pass": True,
                "flush_gate_fail_reason": "",
                "flush_duration_s": 5.0,
            },
            {},
            "",
        )

    monkeypatch.setattr(room_temp_diag_tool, "_run_flush_phase", _fake_run_flush_phase)
    monkeypatch.setattr(
        room_temp_diag_tool,
        "_perform_runtime_safe_stop",
        lambda *_args, **_kwargs: {"safe_stop_verified": True, "safe_stop_issues": []},
    )
    monkeypatch.setattr(
        room_temp_diag_tool,
        "build_analyzer_chain_isolation_summary",
        lambda *_args, **_kwargs: {"classification": "pass"},
    )
    monkeypatch.setattr(
        room_temp_diag_tool,
        "build_analyzer_chain_isolation_comparison",
        lambda *_args, **_kwargs: {"dominant_isolation_conclusion": "pass"},
    )
    monkeypatch.setattr(room_temp_diag_tool, "build_analyzer_chain_pace_contribution_comparison", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(room_temp_diag_tool, "_latest_chain_mode_artifacts", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(room_temp_diag_tool, "_collect_4ch_chain_summaries", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(room_temp_diag_tool, "_load_reference_chain_summaries", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(room_temp_diag_tool, "build_analyzer_chain_compare_vs_8ch", lambda *_args, **_kwargs: {"rows": []})
    monkeypatch.setattr(room_temp_diag_tool, "build_analyzer_chain_compare_vs_baseline", lambda *_args, **_kwargs: {"rows": []})
    monkeypatch.setattr(
        room_temp_diag_tool,
        "export_analyzer_chain_isolation_results",
        lambda run_dir, **_kwargs: {key: run_dir / f"{key}.txt" for key in (
            "raw_timeseries",
            "flush_gate_trace",
            "actuation_events",
            "closed_pressure_swing_trace",
            "setup_metadata",
            "isolation_summary",
            "isolation_comparison_summary",
            "summary",
            "compare_vs_baseline_csv",
            "compare_vs_baseline_md",
            "compare_vs_8ch_csv",
            "compare_vs_8ch_md",
            "readable_report",
            "diagnostic_workbook",
        )},
    )
    monkeypatch.setattr(room_temp_diag_tool, "_close_devices", lambda *_args, **_kwargs: None)

    args = parse_args(
        [
            "--allow-live-hardware",
            "--smoke-level",
            "analyzer-chain-isolation",
            "--chain-mode",
            "analyzer_in_keep_rest",
        ]
    )
    logger = SimpleNamespace(run_id="run-1", run_dir=tmp_path)
    cfg = {
        "devices": {
            "gas_analyzers": [
                {
                    "name": "ga01",
                    "device_id": "001",
                    "port": "COM1",
                    "enabled": True,
                },
                {
                    "name": "ga02",
                    "device_id": "002",
                    "port": "COM2",
                    "enabled": True,
                }
            ]
        }
    }
    variant = VariantSpec(
        name="B",
        description="unit test",
        seal_trigger_hpa=None,
        ramp_down_rate_hpa_per_s=None,
        stable_gate_mode="default",
        comparison_factor="default",
    )

    result = room_temp_diag_tool._run_analyzer_chain_isolation_capture(
        args=args,
        cfg=cfg,
        logger=logger,
        analyzer_name="ga02",
        analyzer_cfg=cfg["devices"]["gas_analyzers"][1],
        variant=variant,
        chain_mode="analyzer_in_keep_rest",
    )

    runner = _FakeRunner.instances[0]
    assert result == 0
    assert runner.configure_devices_calls == [False]
    assert runner.started_labels == ["ga02"]
    assert pace.actions == [
        "analyzer_chain_initial_baseline",
        "analyzer_chain_flush_vent",
        "analyzer_chain_final_restore",
    ]
    assert runner.apply_route_baseline_calls == 2
    assert runner.apply_valve_states_calls == [(7, 8)]


def test_analyzer_chain_isolation_capture_fails_fast_when_capture_startup_fails_and_runs_legacy_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    pace = _FakeLegacyDiagnosticPace(ok=True, legacy_identity=True)
    analyzer = SimpleNamespace(active_send=False)

    class _FakeRunner:
        def __init__(self, _cfg, devices, _logger, _log_fn, _trace_cb) -> None:
            self.devices = devices
            self.apply_route_baseline_calls = 0
            self.started_labels: list[str] = []
            self._disabled_analyzers: set[str] = set()
            self._disabled_analyzer_reasons: dict[str, str] = {}
            self._disabled_analyzer_last_reprobe_ts: dict[str, float] = {}

        def _configure_devices(self, *, configure_gas_analyzers: bool = True) -> None:
            if configure_gas_analyzers:
                raise AssertionError("startup failure path should not use public all-analyzer startup gate")

        def _gas_analyzer_runtime_settings(self, cfg) -> dict[str, object]:
            return {
                "mode": 2,
                "active_send": bool(cfg.get("active_send", False)),
                "ftd_hz": int(cfg.get("ftd_hz", 1)),
                "avg_co2": int(cfg.get("average_co2", 1)),
                "avg_h2o": int(cfg.get("average_h2o", 1)),
                "avg_filter": int(cfg.get("average_filter", 49)),
            }

        def _configure_gas_analyzer(self, _ga, *, label: str, **_kwargs) -> None:
            self.started_labels.append(str(label))
            raise RuntimeError("MODE2 not ready (stream) last=")

        def _disable_analyzers(self, labels, reason: str) -> None:
            for label in labels:
                self._disabled_analyzers.add(str(label))
                self._disabled_analyzer_reasons[str(label)] = str(reason)

        def _apply_route_baseline_valves(self) -> None:
            self.apply_route_baseline_calls += 1

        def _set_pressure_controller_vent(self, *_args, **_kwargs) -> None:
            raise AssertionError("legacy cleanup should not fall back to raw vent path")

        def _set_co2_route_baseline(self, *_args, **_kwargs) -> None:
            raise AssertionError("legacy cleanup should not fall back to raw route baseline path")

    monkeypatch.setattr(room_temp_diag_tool, "_build_devices", lambda *_args, **_kwargs: {"gas_analyzer": analyzer, "pace": pace})
    monkeypatch.setattr(room_temp_diag_tool, "CalibrationRunner", _FakeRunner)
    monkeypatch.setattr(room_temp_diag_tool, "_close_devices", lambda *_args, **_kwargs: None)

    args = parse_args(
        [
            "--allow-live-hardware",
            "--smoke-level",
            "analyzer-chain-isolation",
            "--chain-mode",
            "analyzer_in_keep_rest",
        ]
    )
    logger = SimpleNamespace(run_id="run-1", run_dir=tmp_path)
    cfg = {
        "devices": {
            "gas_analyzers": [
                {
                    "name": "ga02",
                    "device_id": "002",
                    "port": "COM2",
                    "enabled": True,
                }
            ]
        }
    }
    variant = VariantSpec(
        name="B",
        description="unit test",
        seal_trigger_hpa=None,
        ramp_down_rate_hpa_per_s=None,
        stable_gate_mode="default",
        comparison_factor="default",
    )

    with pytest.raises(RuntimeError, match=r"capture_analyzer_startup_failed\(name=ga02,reason=MODE2 not ready \(stream\) last=\)"):
        room_temp_diag_tool._run_analyzer_chain_isolation_capture(
            args=args,
            cfg=cfg,
            logger=logger,
            analyzer_name="ga02",
            analyzer_cfg=cfg["devices"]["gas_analyzers"][0],
            variant=variant,
            chain_mode="analyzer_in_keep_rest",
        )

    assert pace.actions == ["safe_stop", "analyzer_chain_final_restore"]


def test_analyzer_chain_isolation_skip_gas_analyzer_generates_required_outputs_without_startup_gate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    pace = _FakeLegacyDiagnosticPace(ok=True, legacy_identity=True)

    class _FakeRunner:
        instances: list["_FakeRunner"] = []

        def __init__(self, _cfg, devices, _logger, _log_fn, _trace_cb) -> None:
            self.devices = devices
            self.apply_route_baseline_calls = 0
            self.apply_valve_states_calls: list[tuple[int, ...]] = []
            self.configure_devices_calls: list[bool] = []
            self._disabled_analyzers: set[str] = set()
            self._disabled_analyzer_reasons: dict[str, str] = {}
            self._disabled_analyzer_last_reprobe_ts: dict[str, float] = {}
            type(self).instances.append(self)

        def _configure_devices(self, *, configure_gas_analyzers: bool = True) -> None:
            self.configure_devices_calls.append(bool(configure_gas_analyzers))
            if configure_gas_analyzers:
                raise AssertionError("skip analyzer mode should not use analyzer startup gate")

        def _configure_gas_analyzer(self, *_args, **_kwargs) -> None:
            raise AssertionError("skip analyzer mode should not configure any gas analyzer")

        def _apply_valve_states(self, open_logical_valves) -> None:
            self.apply_valve_states_calls.append(tuple(int(v) for v in open_logical_valves))

        def _apply_route_baseline_valves(self) -> None:
            self.apply_route_baseline_calls += 1

        def _set_pressure_controller_vent(self, *_args, **_kwargs) -> None:
            raise AssertionError("legacy diagnostic surface must not fall back to raw vent path")

        def _set_co2_route_baseline(self, *_args, **_kwargs) -> None:
            raise AssertionError("legacy diagnostic surface must not fall back to raw route baseline path")

    def _fake_build_devices(_runtime_cfg, _analyzer_cfg, _logger, *, skip_gas_analyzer: bool = False):
        assert skip_gas_analyzer is True
        return {"pace": pace}

    def _fake_run_flush_phase(*_args, **kwargs):
        assert kwargs.get("require_ratio") is False
        assert _args[1] is None
        vent_on_handler = kwargs.get("vent_on_handler")
        assert callable(vent_on_handler)
        vent_on_handler("unit test skip-analyzer legacy flush vent")
        trace_rows = kwargs.get("flush_gate_trace_rows")
        assert isinstance(trace_rows, list)
        trace_rows.append(
            {
                "timestamp": "2026-04-19T22:05:23",
                "process_variant": "B",
                "layer": 0,
                "repeat_index": 1,
                "phase": "isolation_flush",
                "gas_ppm": 0,
                "pressure_target_hpa": None,
                "gate_name": "isolation_flush_gate",
                "gate_status": "pass",
                "gate_pass": True,
            }
        )
        rows = [
            {
                "timestamp": f"2026-04-19T22:05:{index:02d}",
                "process_variant": "B",
                "layer": 0,
                "repeat_index": 1,
                "phase": "isolation_flush_vent_on",
                "gas_ppm": 0,
                "gauge_pressure_hpa": 1013.2 + (index * 0.01),
                "controller_pressure_hpa": 1013.1 + (index * 0.01),
                "controller_vent_status_code": 1,
                "controller_output_state": 0,
                "controller_isolation_state": 1,
                "dewpoint_c": -58.0 + (index * 0.01),
                "dewpoint_temp_c": 20.0,
                "dewpoint_rh_percent": 2.0,
                "route_group": "A",
                "route_source_valve": 7,
                "route_path_valve": 8,
            }
            for index in range(12)
        ]
        summary = {
            "process_variant": "B",
            "layer": 0,
            "repeat_index": 1,
            "gas_ppm": 0,
            "flush_gate_status": "pass",
            "flush_gate_pass": True,
            "flush_gate_fail_reason": "",
            "flush_duration_s": 130.0,
            "vent_state_during_flush": "VENT_ON",
            "dewpoint_rebound_detected": False,
            "rebound_rise_c": 0.0,
            "flush_last60s_dewpoint_span": 0.08,
            "flush_last60s_dewpoint_slope": 0.0005,
            "flush_last60s_gauge_span_hpa": 0.12,
            "flush_last60s_gauge_slope_hpa_per_s": 0.002,
            "flush_last60s_ratio_span": None,
            "flush_last60s_ratio_slope": None,
            "analyzer_raw_sample_count": 0,
            "gauge_raw_sample_count": 12,
            "dewpoint_raw_sample_count": 12,
            "aligned_sample_count": 12,
            "flush_warning_flags": [],
        }
        gate_row = {
            "timestamp": "2026-04-19T22:05:40",
            "process_variant": "B",
            "layer": 0,
            "repeat_index": 1,
            "phase": "isolation_flush",
            "gas_ppm": 0,
            "pressure_target_hpa": None,
            "gate_name": "isolation_flush_gate",
            "gate_status": "pass",
            "gate_pass": True,
        }
        return rows, summary, gate_row, ""

    monkeypatch.setattr(room_temp_diag_tool, "_build_devices", _fake_build_devices)
    monkeypatch.setattr(room_temp_diag_tool, "CalibrationRunner", _FakeRunner)
    monkeypatch.setattr(
        room_temp_diag_tool,
        "_route_for_point",
        lambda *_args, **_kwargs: {
            "group": "A",
            "source_valve": 7,
            "path_valve": 8,
            "open_logical_valves": [7, 8],
        },
    )
    monkeypatch.setattr(
        room_temp_diag_tool,
        "_run_closed_pressure_swing_predry",
        lambda *_args, **_kwargs: ([], [], room_temp_diag_tool._closed_pressure_swing_defaults({})),
    )
    monkeypatch.setattr(room_temp_diag_tool, "_run_flush_phase", _fake_run_flush_phase)
    monkeypatch.setattr(
        room_temp_diag_tool,
        "_perform_runtime_safe_stop",
        lambda *_args, **kwargs: {
            "safe_stop_verified": kwargs.get("pace_mode") == "diagnostic_safe_vent",
            "safe_stop_issues": [],
        },
    )
    monkeypatch.setattr(
        room_temp_diag_tool,
        "_latest_chain_mode_artifacts",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(room_temp_diag_tool, "_collect_4ch_chain_summaries", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(room_temp_diag_tool, "_load_reference_chain_summaries", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(room_temp_diag_tool, "build_analyzer_chain_compare_vs_8ch", lambda *_args, **_kwargs: {"rows": []})
    monkeypatch.setattr(room_temp_diag_tool, "_resolve_compare_vs_baseline_reference_dir", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        room_temp_diag_tool,
        "_run_sealed_hold_phase",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("sealed hold should stay disabled")),
    )
    monkeypatch.setattr(
        room_temp_diag_tool,
        "_run_pressure_sweep_phase",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("pressure sweep should stay disabled")),
    )
    monkeypatch.setattr(room_temp_diag_tool, "_close_devices", lambda *_args, **_kwargs: None)

    args = parse_args(
        [
            "--allow-live-hardware",
            "--smoke-level",
            "analyzer-chain-isolation",
            "--chain-mode",
            "analyzer_in_keep_rest",
            "--skip-gas-analyzer",
            "--no-export-png",
            "--no-export-xlsx",
        ]
    )
    logger = SimpleNamespace(run_id="run-1", run_dir=tmp_path)
    cfg = {
        "devices": {
            "gas_analyzers": [
                {
                    "name": "ga01",
                    "device_id": "001",
                    "port": "COM1",
                    "enabled": True,
                },
                {
                    "name": "ga02",
                    "device_id": "002",
                    "port": "COM2",
                    "enabled": True,
                },
            ]
        }
    }
    variant = VariantSpec(
        name="B",
        description="unit test",
        seal_trigger_hpa=None,
        ramp_down_rate_hpa_per_s=None,
        stable_gate_mode="default",
        comparison_factor="default",
    )

    result = room_temp_diag_tool._run_analyzer_chain_isolation_capture(
        args=args,
        cfg=cfg,
        logger=logger,
        analyzer_name="ga02",
        analyzer_cfg=cfg["devices"]["gas_analyzers"][1],
        variant=variant,
        chain_mode="analyzer_in_keep_rest",
    )

    runner = _FakeRunner.instances[0]
    assert result == 0
    assert runner.configure_devices_calls == [False]
    assert pace.actions == [
        "analyzer_chain_initial_baseline",
        "analyzer_chain_flush_vent",
        "analyzer_chain_final_restore",
    ]
    assert runner.apply_route_baseline_calls == 2
    assert runner.apply_valve_states_calls == [(7, 8)]

    required_paths = {
        "summary": tmp_path / "summary.json",
        "setup": tmp_path / "setup_metadata.json",
        "isolation": tmp_path / "isolation_summary.csv",
        "actuation": tmp_path / "actuation_events.csv",
        "trace": tmp_path / "flush_gate_trace.csv",
    }
    for one_path in required_paths.values():
        assert one_path.exists(), f"missing export: {one_path}"

    setup_metadata = json.loads(required_paths["setup"].read_text(encoding="utf-8"))
    assert setup_metadata["gas_analyzer_skipped"] is True
    assert setup_metadata["gas_analyzer_skip_reason"] == "diagnostic_only_focus_non_analyzer"
    assert setup_metadata["analyzer_sampling_enabled"] is False
    assert setup_metadata["capture_analyzer_name"] == "ga02"
    assert setup_metadata["capture_analyzer_startup_status"] == "skipped"
    assert setup_metadata["closed_pressure_swing_enabled"] is False
    assert setup_metadata["flush_vent_refresh_interval_s"] == 0.0
    assert setup_metadata["flush_vent_refresh_count"] == 0

    summary_payload = json.loads(required_paths["summary"].read_text(encoding="utf-8"))
    first_summary = summary_payload["isolation_summaries"][0]
    assert first_summary["classification"] == "pass"
    assert first_summary["gas_analyzer_skipped"] is True
    assert first_summary["analyzer_sampling_enabled"] is False
    assert first_summary["flush_vent_refresh_interval_s"] == 0.0
    assert first_summary["flush_vent_refresh_count"] == 0
    assert first_summary["ratio_tail_span_60s"] is None
    assert first_summary["evidence_density_reason"] == ""

    with required_paths["isolation"].open("r", encoding="utf-8-sig", newline="") as fh:
        rows = list(csv.DictReader(fh))
    assert len(rows) == 1
    assert rows[0]["gas_analyzer_skipped"] == "True"
    assert rows[0]["gas_analyzer_skip_reason"] == "diagnostic_only_focus_non_analyzer"
    assert rows[0]["analyzer_sampling_enabled"] == "False"
    assert rows[0]["flush_vent_refresh_interval_s"] == "0.0"
    assert rows[0]["flush_vent_refresh_count"] == "0"


def test_parse_args_rejects_flush_vent_refresh_outside_skip_analyzer_chain_isolation() -> None:
    with pytest.raises(SystemExit):
        parse_args(
            [
                "--allow-live-hardware",
                "--smoke-level",
                "analyzer-chain-isolation",
                "--chain-mode",
                "analyzer_in_keep_rest",
                "--flush-vent-refresh-interval-s",
                "2",
            ]
        )


def test_run_flush_phase_refresh_records_trace_and_count(monkeypatch: pytest.MonkeyPatch) -> None:
    monotonic_values = iter([0.0, 0.0, 2.1, 2.1, 4.2])
    vent_on_calls: list[str] = []
    refresh_calls: list[str] = []
    actuation_events: list[dict[str, object]] = []
    flush_gate_trace_rows: list[dict[str, object]] = []
    setup_metadata: dict[str, object] = {}
    capture_count = {"value": 0}

    def _fake_capture_phase_rows(*_args, **kwargs):
        capture_count["value"] += 1
        index = capture_count["value"]
        return [
            {
                "timestamp": f"ts-{kwargs['phase_elapsed_offset_s']}",
                "phase_elapsed_s": float(kwargs["phase_elapsed_offset_s"]) + float(kwargs["duration_s"]),
                "gauge_pressure_hpa": 1600.0 + index,
                "controller_pressure_hpa": 1601.0 + index,
                "controller_vent_status_code": 2,
                "controller_output_state": 0,
                "controller_isolation_state": 1,
                "dewpoint_c": -55.0 + index,
            }
        ]

    monkeypatch.setattr(room_temp_diag_tool.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(room_temp_diag_tool, "_capture_phase_rows", _fake_capture_phase_rows)
    monkeypatch.setattr(
        room_temp_diag_tool,
        "_build_flush_gate_trace_row",
        lambda _rows, **kwargs: {
            "phase": kwargs["phase"],
            "elapsed_s_real": kwargs["elapsed_real_s"],
            "note": kwargs["note"],
        },
    )
    monkeypatch.setattr(room_temp_diag_tool, "_log_flush_gate_snapshot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        room_temp_diag_tool,
        "build_flush_summary",
        lambda *_args, **_kwargs: {
            "flush_gate_status": "pass",
            "flush_gate_pass": True,
            "flush_gate_fail_reason": "",
            "flush_duration_s": 4.2,
            "flush_last60s_ratio_span": None,
            "flush_last60s_ratio_slope": None,
            "flush_last60s_dewpoint_span": 0.1,
            "flush_last60s_dewpoint_slope": 0.001,
            "flush_last60s_gauge_span_hpa": 0.2,
            "flush_last60s_gauge_slope_hpa_per_s": 0.01,
            "analyzer_raw_sample_count": 0,
            "gauge_raw_sample_count": 2,
            "dewpoint_raw_sample_count": 2,
            "aligned_sample_count": 2,
        },
    )
    monkeypatch.setattr(
        room_temp_diag_tool,
        "build_phase_gate_row",
        lambda **kwargs: {
            "gate_status": kwargs["gate_status"],
            "gate_pass": kwargs["gate_pass"],
        },
    )

    rows, summary, gate_row, stop_reason = room_temp_diag_tool._run_flush_phase(
        runner=SimpleNamespace(
            _set_pressure_controller_vent=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("test should stay on handler path")
            )
        ),
        analyzer=None,
        devices={},
        process_variant="B",
        layer=0,
        repeat_index=1,
        gas_ppm=0,
        route={"group": "A"},
        gas_start_mono=0.0,
        min_flush_s=0.0,
        target_flush_s=4.0,
        max_flush_s=4.5,
        gate_window_s=6.0,
        rebound_window_s=30.0,
        rebound_min_rise_c=0.0,
        sample_poll_s=1.0,
        print_every_s=10.0,
        actual_deadtime_s=0.0,
        actuation_events=actuation_events,
        flush_gate_trace_rows=flush_gate_trace_rows,
        run_id="run-1",
        require_ratio=False,
        require_vent_on=True,
        controller_vent_state_label="VENT_ON",
        phase_name_override="isolation_flush_vent_on",
        gate_name_override="isolation_flush_gate",
        phase_label_override="isolation_flush",
        chain_mode="analyzer_in_keep_rest",
        vent_on_handler=lambda reason: vent_on_calls.append(reason),
        flush_vent_refresh_interval_s=2.0,
        vent_refresh_handler=lambda reason: refresh_calls.append(reason),
        setup_metadata=setup_metadata,
    )

    assert stop_reason == ""
    assert len(rows) == 2
    assert rows[0]["gauge_pressure_hpa"] != rows[1]["gauge_pressure_hpa"]
    assert rows[0]["controller_pressure_hpa"] != rows[1]["controller_pressure_hpa"]
    assert rows[0]["dewpoint_c"] != rows[1]["dewpoint_c"]
    assert summary["flush_gate_pass"] is True
    assert gate_row["gate_pass"] is True
    assert vent_on_calls == ["metrology isolation_flush gate"]
    assert refresh_calls == [
        "metrology isolation_flush refresh @2.1s",
        "metrology isolation_flush refresh @4.2s",
    ]
    assert setup_metadata["flush_vent_refresh_interval_s"] == 2.0
    assert setup_metadata["flush_vent_refresh_count"] == 2
    assert [row["note"] for row in flush_gate_trace_rows] == [
        "isolation_flush_start",
        "isolation_flush_vent_refresh_2.1s",
        "isolation_flush_vent_refresh_4.2s",
        "isolation_flush_first_gate_evaluation",
        "isolation_flush_gate_pass",
    ]
    refresh_events = [row for row in actuation_events if row["event_type"] == "vent_refresh"]
    assert len(refresh_events) == 2
    assert all(row["chain_mode"] == "analyzer_in_keep_rest" for row in refresh_events)
    assert [row["elapsed_s_real"] for row in flush_gate_trace_rows] == [0.0, 2.1, 4.2, 4.2, 4.2]


def test_run_flush_phase_refresh_does_not_replay_overdue_intervals_without_new_samples(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monotonic_values = iter([0.0, 0.0, 200.023])
    refresh_calls: list[str] = []
    actuation_events: list[dict[str, object]] = []
    flush_gate_trace_rows: list[dict[str, object]] = []
    setup_metadata: dict[str, object] = {}

    monkeypatch.setattr(room_temp_diag_tool.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(
        room_temp_diag_tool,
        "_capture_phase_rows",
        lambda *_args, **kwargs: [
            {
                "timestamp": "ts-200",
                "phase_elapsed_s": float(kwargs["phase_elapsed_offset_s"]) + float(kwargs["duration_s"]),
                "gauge_pressure_hpa": 1097.715,
                "controller_pressure_hpa": 1098.5,
                "controller_vent_status_code": 2,
                "controller_output_state": 0,
                "controller_isolation_state": 1,
                "dewpoint_c": -37.99,
            }
        ],
    )
    monkeypatch.setattr(
        room_temp_diag_tool,
        "_build_flush_gate_trace_row",
        lambda _rows, **kwargs: {
            "phase": kwargs["phase"],
            "elapsed_s_real": kwargs["elapsed_real_s"],
            "note": kwargs["note"],
        },
    )
    monkeypatch.setattr(room_temp_diag_tool, "_log_flush_gate_snapshot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        room_temp_diag_tool,
        "build_flush_summary",
        lambda *_args, **_kwargs: {
            "flush_gate_status": "pass",
            "flush_gate_pass": True,
            "flush_gate_fail_reason": "",
            "flush_duration_s": 200.023,
            "flush_last60s_ratio_span": None,
            "flush_last60s_ratio_slope": None,
            "flush_last60s_dewpoint_span": 0.1,
            "flush_last60s_dewpoint_slope": 0.001,
            "flush_last60s_gauge_span_hpa": 0.2,
            "flush_last60s_gauge_slope_hpa_per_s": 0.01,
            "analyzer_raw_sample_count": 0,
            "gauge_raw_sample_count": 1,
            "dewpoint_raw_sample_count": 1,
            "aligned_sample_count": 1,
        },
    )
    monkeypatch.setattr(
        room_temp_diag_tool,
        "build_phase_gate_row",
        lambda **kwargs: {
            "gate_status": kwargs["gate_status"],
            "gate_pass": kwargs["gate_pass"],
        },
    )

    rows, summary, gate_row, stop_reason = room_temp_diag_tool._run_flush_phase(
        runner=SimpleNamespace(_set_pressure_controller_vent=lambda *_args, **_kwargs: None),
        analyzer=None,
        devices={},
        process_variant="B",
        layer=0,
        repeat_index=1,
        gas_ppm=0,
        route={"group": "A"},
        gas_start_mono=0.0,
        min_flush_s=0.0,
        target_flush_s=180.0,
        max_flush_s=210.0,
        gate_window_s=6.0,
        rebound_window_s=30.0,
        rebound_min_rise_c=0.0,
        sample_poll_s=1.0,
        print_every_s=10.0,
        actual_deadtime_s=0.0,
        actuation_events=actuation_events,
        flush_gate_trace_rows=flush_gate_trace_rows,
        run_id="run-1",
        require_ratio=False,
        require_vent_on=False,
        controller_vent_state_label="VENT_ON",
        phase_name_override="isolation_flush_vent_on",
        gate_name_override="isolation_flush_gate",
        phase_label_override="isolation_flush",
        chain_mode="analyzer_in_keep_rest",
        flush_vent_refresh_interval_s=2.0,
        vent_refresh_handler=lambda reason: refresh_calls.append(reason),
        setup_metadata=setup_metadata,
    )

    assert stop_reason == ""
    assert len(rows) == 1
    assert summary["flush_gate_pass"] is True
    assert gate_row["gate_pass"] is True
    assert refresh_calls == ["metrology isolation_flush refresh @200.0s"]
    assert setup_metadata["flush_vent_refresh_count"] == 1
    refresh_notes = [row["note"] for row in flush_gate_trace_rows if "vent_refresh" in str(row["note"])]
    assert refresh_notes == ["isolation_flush_vent_refresh_200.0s"]
    refresh_events = [row for row in actuation_events if row["event_type"] == "vent_refresh"]
    assert len(refresh_events) == 1


def test_export_analyzer_chain_isolation_results_with_fallback_retries_without_png_when_matplotlib_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[bool] = []

    def _fake_export(_output_dir, **kwargs):
        calls.append(bool(kwargs.get("export_png")))
        if len(calls) == 1:
            raise ModuleNotFoundError("No module named 'matplotlib'", name="matplotlib")
        return {"summary": tmp_path / "summary.json"}

    monkeypatch.setattr(room_temp_diag_tool, "export_analyzer_chain_isolation_results", _fake_export)

    outputs = room_temp_diag_tool._export_analyzer_chain_isolation_results_with_fallback(
        tmp_path,
        raw_rows=[],
        flush_gate_trace_rows=[],
        actuation_events=[],
        setup_metadata={},
        isolation_summaries=[],
        comparison_summary={},
        operator_checklist="checklist",
        export_csv=True,
        export_xlsx=False,
        export_png=True,
    )

    assert calls == [True, False]
    assert outputs["summary"] == tmp_path / "summary.json"
