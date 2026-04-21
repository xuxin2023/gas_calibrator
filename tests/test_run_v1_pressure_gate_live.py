import json
from argparse import Namespace
from pathlib import Path

import pytest

from gas_calibrator.tools import run_v1_pressure_gate_live as live_tool


class _FakeRunner:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []
        self._last_atmosphere_gate_summary = {"ambient_hpa": 1012.0, "atmosphere_ready": True}
        self._last_route_pressure_guard_summary = {
            "route_pressure_guard_status": "pass",
            "analyzer_pressure_available": False,
            "analyzer_pressure_protection_active": False,
            "analyzer_pressure_status": "unavailable",
        }
        self._source_stage = {"co2_a": False, "co2_b": False, "h2o": False}
        self._atmosphere_safe = {"co2_a": False, "co2_b": False, "h2o": False}
        self._seal_safe = {"co2_a": False, "co2_b": False, "h2o": False}
        self._continuous_state = {
            "active": False,
            "route_flow_active": False,
            "route_key": "",
            "phase_name": "",
            "pressure_mode": "",
            "keepalive_count": 0,
            "last_keepalive_summary": {},
        }
        self.final_syst_err = '0,"No error"'
        self.attribution_counts = {
            "pace_error_attribution_count": 0,
            "optional_probe_error_count": 0,
            "hidden_syst_err_count": 0,
            "unclassified_syst_err_count": 0,
            "pre_route_drain_syst_err_count": 0,
        }
        self.attribution_log: list[dict[str, object]] = []
        self.active_analyzers: list[tuple[str, object, dict[str, object]]] = []
        self.analyzer_pressure_reading: float | None = None
        self.analyzer_pressure_label = "ga01"

    def _configure_devices(self, configure_gas_analyzers: bool = False) -> None:
        self.calls.append(("configure_devices", bool(configure_gas_analyzers)))

    def _drain_pace_system_errors(self, reason: str = "", **kwargs):
        self.calls.append(("drain", reason))
        return []

    def _read_pace_system_error_text(self):
        return self.final_syst_err

    def _pace_error_attribution_counts(self):
        return dict(self.attribution_counts)

    def _pace_error_attribution_log_snapshot(self):
        return list(self.attribution_log)

    def _capture_pace_capability_snapshot(self, **kwargs):
        return {
            "pressure_unit": "HPA",
            "pressure_unit_status": "known_hpa",
            "pressure_unit_allows_formal_setpoint": True,
        }

    def _source_stage_safety_snapshot(self):
        return dict(self._source_stage)

    def _route_final_stage_atmosphere_safety_snapshot(self):
        return dict(self._atmosphere_safe)

    def _route_final_stage_seal_safety_snapshot(self):
        return dict(self._seal_safe)

    def _continuous_atmosphere_state_snapshot(self):
        return dict(self._continuous_state)

    def _clear_last_sealed_pressure_route_context(self, reason: str = "") -> None:
        self.calls.append(("clear_last", reason))

    def _clear_pressure_sequence_context(self, reason: str = "") -> None:
        self.calls.append(("clear_sequence", reason))

    def _set_co2_route_baseline(self, reason: str = "") -> None:
        self.calls.append(("baseline", reason))

    def _read_current_pressure_hpa_for_atmosphere(self):
        return {
            "pressure_hpa": 1012.0,
            "pressure_gauge_hpa": 1012.0,
            "pace_pressure_hpa": 1012.0,
        }

    def _append_pressure_trace_row(self, *args, **kwargs) -> None:
        self.calls.append(("trace", str(kwargs.get("trace_stage") or "")))

    def _numeric_series_metrics(self, rows):
        values = [float(value) for _ts, value in list(rows or [])]
        if not values:
            return {"count": 0, "first_value": None, "last_value": None, "slope_per_s": None}
        return {
            "count": len(values),
            "first_value": values[0],
            "last_value": values[-1],
            "slope_per_s": 0.0,
        }

    def _as_float(self, value):
        if value is None:
            return None
        return float(value)

    def _current_valve_route_state_text(self) -> str:
        return "baseline"

    def _active_gas_analyzers(self):
        return list(self.active_analyzers)

    def _read_route_guard_analyzer_pressure_kpa(self):
        return self.analyzer_pressure_reading, self.analyzer_pressure_label

    def _apply_route_baseline_valves(self) -> None:
        self.calls.append(("apply_route_baseline", None))

    def _set_pressure_controller_vent(self, is_open: bool, reason: str = "") -> None:
        self.calls.append(("vent", bool(is_open)))

    def _co2_open_valves(self, point, include_total_valve: bool, *, include_source_valve: bool = True):
        if str(getattr(point, "co2_group", "") or "").upper() == "B":
            return [8, 11, 16, 24] if include_source_valve else [8, 11, 16]
        return [8, 11, 7, 4] if include_source_valve else [8, 11, 7]

    def _h2o_open_valves(self, point, *, include_final_stage: bool = True):
        return [8, 9, 10] if include_final_stage else [8, 9]

    def _open_route_with_pressure_guard(self, point, **kwargs):
        self.calls.append(("guard", list(kwargs.get("open_valves") or [])))
        return True

    def _open_co2_route_for_conditioning(self, point, *, point_tag: str = ""):
        self.calls.append(("conditioning", point_tag))
        return True

    def _point_runtime_state(self, point, *, phase: str):
        return {
            "pressure_delta_from_ambient_hpa": 0.25,
            "route_pressure_guard_status": "pass",
            "route_pressure_guard_reason": "",
            "continuous_atmosphere_active": self._continuous_state["active"],
            "vent_keepalive_count": self._continuous_state["keepalive_count"],
            "atmosphere_flow_safe": self._atmosphere_safe.get("co2_a", False),
            "seal_pressure_safe": self._seal_safe.get("co2_a", False),
        }

    def maintain_continuous_atmosphere_flowthrough(self, route_key, **kwargs):
        self._continuous_state["active"] = True
        self._continuous_state["route_flow_active"] = True
        self._continuous_state["route_key"] = str(route_key)
        self._continuous_state["phase_name"] = str(kwargs.get("phase_name") or "")
        self._continuous_state["keepalive_count"] = int(self._continuous_state.get("keepalive_count", 0)) + 1
        self._continuous_state["last_keepalive_summary"] = {
            "pace_vent_command_sent": ":SOUR:PRES:LEV:IMM:AMPL:VENT 1",
            "pace_vent_status_returned": 2,
            "pace_vent_status_text": "completed",
        }
        self.calls.append(("keepalive", str(route_key)))
        return True, dict(self._continuous_state)


def _args(**overrides):
    base = {
        "target_pressure_hpa": 1000.0,
        "co2_ppm": 600.0,
        "allow_source_open": False,
        "allow_h2o_final_stage_open": False,
    }
    base.update(overrides)
    return Namespace(**base)


def _config(
    *,
    gas_analyzers: list[dict[str, object]] | None = None,
    gas_analyzer_enabled: bool = False,
    mechanical_pressure_protection_confirmed: bool = False,
):
    return {
        "devices": {
            "pressure_controller": {"enabled": True},
            "pressure_gauge": {"enabled": True},
            "relay": {"enabled": True},
            "relay_8": {"enabled": True},
            "dewpoint_meter": {"enabled": False},
            "gas_analyzer": {
                "enabled": gas_analyzer_enabled,
                "port": "COM7",
                "baud": 115200,
                "device_id": "001",
            },
            "gas_analyzers": list(gas_analyzers or []),
        },
        "workflow": {
            "pressure": {
                "mechanical_pressure_protection_confirmed": mechanical_pressure_protection_confirmed,
            }
        },
        "paths": {},
    }


def _run_main(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    scenario: str,
    config: dict,
    allow_source_open: bool = False,
    allow_h2o_final_stage_open: bool = False,
    runner_factory=None,
):
    output_dir = tmp_path / "out"
    config_path = tmp_path / "cfg.yaml"
    config_path.write_text("devices: {}\nworkflow: {}\n", encoding="utf-8")
    runner_box: dict[str, _FakeRunner] = {}

    def _make_runner(*args, **kwargs):
        runner = runner_factory() if callable(runner_factory) else _FakeRunner()
        runner_box["runner"] = runner
        return runner

    monkeypatch.setattr(live_tool, "load_config", lambda _path: config)
    monkeypatch.setattr(live_tool, "_build_devices", lambda runtime_cfg, io_logger=None: {})
    monkeypatch.setattr(live_tool, "_close_devices", lambda devices: None)
    monkeypatch.setattr(
        live_tool,
        "perform_safe_stop_with_retries",
        lambda devices, log_fn=None, cfg=None: {"ok": True},
    )
    monkeypatch.setattr(live_tool, "CalibrationRunner", _make_runner)

    argv = [
        "--real-device",
        "--config",
        str(config_path),
        "--output-dir",
        str(output_dir),
        "--scenario",
        scenario,
    ]
    if allow_source_open:
        argv.append("--allow-source-open")
    if allow_h2o_final_stage_open:
        argv.append("--allow-h2o-final-stage-open")

    exit_code = live_tool.main(argv)
    summary_path = next(output_dir.rglob("pressure_gate_live_summary.json"))
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    return exit_code, summary, runner_box["runner"]


def test_source_open_live_scenario_requires_allow_source_open_flag(tmp_path: Path) -> None:
    runner = _FakeRunner()
    result = live_tool._run_route_synchronized_atmosphere_flush_co2_a_source_guarded(
        runner,
        tmp_path / "trace.csv",
        _args(allow_source_open=False),
    )

    assert result["status"] == "skipped"
    assert result["skipped_reason"] == "SourceOpenRequiresExplicitAllowFlag"
    assert result["operator_must_confirm_upstream_source_pressure_limited"] is True


def test_analyzer_pressure_required_preserves_analyzer_list() -> None:
    runtime_cfg = _config(
        gas_analyzers=[
            {"name": "ga01", "enabled": True, "port": "COM11"},
            {"name": "ga02", "enabled": True, "port": "COM12"},
        ]
    )

    live_tool._disable_unneeded_devices(
        runtime_cfg,
        need_dewpoint=False,
        need_analyzer_pressure=True,
    )

    assert len(runtime_cfg["devices"]["gas_analyzers"]) == 2
    assert [item["enabled"] for item in runtime_cfg["devices"]["gas_analyzers"]] == [True, True]


def test_analyzer_pressure_required_with_empty_analyzer_list_fails_fast(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exit_code, summary, runner = _run_main(
        tmp_path,
        monkeypatch,
        scenario="route_synchronized_atmosphere_flush_co2_a_source_guarded",
        config=_config(),
        allow_source_open=True,
    )

    assert exit_code == 0
    assert summary["scenario_result"]["status"] == "diagnostic_error"
    assert summary["scenario_result"]["abort_reason"] == "AnalyzerPressureRequiredButUnavailable"
    assert summary["scenario_result"]["analyzer_pressure_required"] is True
    assert summary["scenario_result"]["analyzer_pressure_available"] is False
    assert summary["scenario_result"]["analyzer_pressure_abort_reason"] == "AnalyzerPressureRequiredButUnavailable"
    assert all(call[0] not in {"conditioning", "guard"} for call in runner.calls)


def test_analyzer_pressure_optional_can_disable_analyzer_with_transparent_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exit_code, summary, runner = _run_main(
        tmp_path,
        monkeypatch,
        scenario="route_synchronized_atmosphere_flush_co2_a_no_source",
        config=_config(gas_analyzers=[{"name": "ga01", "enabled": True, "port": "COM11"}]),
    )

    assert exit_code == 0
    assert summary["scenario_result"]["status"] == "pass"
    assert summary["scenario_result"]["analyzer_pressure_required"] is False
    assert summary["scenario_result"]["analyzer_pressure_available"] is False
    assert summary["scenario_result"]["analyzer_pressure_protection_active"] is False
    assert summary["scenario_result"]["analyzer_disabled_reason"] == "AnalyzerPressureOptionalForScenario"
    assert ("guard", [8, 11, 7]) in runner.calls


def test_source_or_h2o_final_stage_requires_analyzer_pressure_or_mechanical_protection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exit_code, summary, runner = _run_main(
        tmp_path,
        monkeypatch,
        scenario="route_synchronized_atmosphere_flush_h2o",
        config=_config(),
        allow_h2o_final_stage_open=True,
    )

    assert exit_code == 0
    assert summary["scenario_result"]["status"] == "diagnostic_error"
    assert summary["scenario_result"]["abort_reason"] == "AnalyzerPressureRequiredButUnavailable"
    assert summary["scenario_result"]["analyzer_pressure_required"] is True
    assert summary["scenario_result"]["mechanical_pressure_protection_confirmed"] is False
    assert all(call[0] not in {"conditioning", "guard"} for call in runner.calls)


def test_mechanical_pressure_protection_confirmation_allows_analyzer_unavailable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exit_code, summary, runner = _run_main(
        tmp_path,
        monkeypatch,
        scenario="route_synchronized_atmosphere_flush_h2o",
        config=_config(mechanical_pressure_protection_confirmed=True),
        allow_h2o_final_stage_open=True,
    )

    assert exit_code == 0
    assert summary["scenario_result"]["status"] == "pass"
    assert summary["scenario_result"]["mechanical_pressure_protection_confirmed"] is True
    assert summary["scenario_result"]["analyzer_pressure_required"] is False
    assert summary["scenario_result"]["analyzer_pressure_available"] is False
    assert summary["scenario_result"]["analyzer_pressure_protection_active"] is False
    assert summary["scenario_result"]["analyzer_disabled_reason"] == "MechanicalPressureProtectionConfirmed"
    assert ("guard", [8, 9, 10]) in runner.calls


def test_no_source_co2_a_route_is_8_11_7(tmp_path: Path) -> None:
    runner = _FakeRunner()
    result = live_tool._run_route_synchronized_atmosphere_flush_co2_a_no_source(
        runner,
        tmp_path / "trace.csv",
        _args(),
    )

    assert result["open_valves"] == [8, 11, 7]
    assert ("guard", [8, 11, 7]) in runner.calls


def test_no_source_co2_b_route_is_8_11_16(tmp_path: Path) -> None:
    runner = _FakeRunner()
    result = live_tool._run_route_synchronized_atmosphere_flush_co2_b_no_source(
        runner,
        tmp_path / "trace.csv",
        _args(),
    )

    assert result["open_valves"] == [8, 11, 16]
    assert ("guard", [8, 11, 16]) in runner.calls


def test_h2o_no_final_route_is_8_9(tmp_path: Path) -> None:
    runner = _FakeRunner()
    result = live_tool._run_route_synchronized_atmosphere_flush_h2o_no_final(
        runner,
        tmp_path / "trace.csv",
        _args(),
    )

    assert result["open_valves"] == [8, 9]
    assert result["skipped_final_stage"] == 10
    assert result["skipped_reason"] == "H2OFinalStage10RequiresExplicitAllowFlag"
    assert ("guard", [8, 9]) in runner.calls


def test_h2o_full_route_requires_allow_h2o_final_stage_open(tmp_path: Path) -> None:
    runner = _FakeRunner()
    result = live_tool._run_route_synchronized_atmosphere_flush_h2o(
        runner,
        tmp_path / "trace.csv",
        _args(allow_h2o_final_stage_open=False),
    )

    assert result["status"] == "skipped"
    assert result["skipped_reason"] == "H2OFinalStage10RequiresExplicitAllowFlag"
    assert result["operator_must_confirm_h2o_upstream_pressure_limited"] is True


def test_hidden_syst_err_disqualifies_route_pass(tmp_path: Path) -> None:
    runner = _FakeRunner()
    runner.attribution_counts["hidden_syst_err_count"] = 1

    result = live_tool._enrich_live_result_with_pace_diagnostics(
        runner,
        {"scenario": "demo", "status": "pass", "route_open_passed": True},
    )

    assert result["status"] == "pass_with_diagnostic_error"


def test_route_pass_requires_zero_hidden_and_unclassified_syst_err(tmp_path: Path) -> None:
    runner = _FakeRunner()
    runner.attribution_counts["unclassified_syst_err_count"] = 1

    result = live_tool._enrich_live_result_with_pace_diagnostics(
        runner,
        {"scenario": "demo", "status": "pass", "route_open_passed": True},
    )

    assert result["status"] == "diagnostic_error"
    assert result["abort_reason"] == "UnclassifiedPaceSystErrDuringRouteStage"


def test_clean_route_pass_requires_zero_pre_route_drain_errors(tmp_path: Path) -> None:
    runner = _FakeRunner()
    runner.attribution_counts["pre_route_drain_syst_err_count"] = 1

    result = live_tool._enrich_live_result_with_pace_diagnostics(
        runner,
        {"scenario": "demo", "status": "pass", "route_open_passed": True},
    )

    assert result["status"] == "pass_with_diagnostic_error"
    assert result["not_real_acceptance_evidence"] is True


def test_unresolved_pre_route_error_marks_not_real_acceptance_evidence(tmp_path: Path) -> None:
    runner = _FakeRunner()
    runner.attribution_counts["pre_route_drain_syst_err_count"] = 3
    runner.attribution_log = [
        {
            "classification": "pre_route_drain",
            "syst_err": ':SYST:ERR -102,"Syntax error"',
            "suspected_command": ":SOUR:PRES:LEV:IMM:AMPL:VENT:ETIM?",
        }
    ]

    result = live_tool._enrich_live_result_with_pace_diagnostics(
        runner,
        {"scenario": "demo", "status": "pass", "route_open_passed": True},
    )

    assert result["status"] == "pass_with_diagnostic_error"
    assert result["not_real_acceptance_evidence"] is True
    assert result["pace_error_attribution_log"][0]["suspected_command"] == ":SOUR:PRES:LEV:IMM:AMPL:VENT:ETIM?"


def test_summary_extract_includes_point_runtime_pressure_delta() -> None:
    extract = live_tool._build_summary_extract(
        {
            "scenario_result": {
                "pressure_delta_from_ambient_hpa": None,
                "point_runtime_state": {
                    "pressure_delta_from_ambient_hpa": 0.291,
                    "route_pressure_guard_status": "pass",
                    "route_pressure_guard_reason": "",
                },
                "route_pressure_guard_summary": {},
            }
        }
    )

    assert extract["pressure_delta_from_ambient_hpa"] == 0.291


def test_summary_extract_includes_continuous_atmosphere_fields() -> None:
    extract = live_tool._build_summary_extract(
        {
            "scenario_result": {
                "source_stage_safety": {"co2_a": False, "co2_b": False, "h2o": False},
                "point_runtime_state": {
                    "continuous_atmosphere_active": True,
                    "vent_keepalive_count": 4,
                    "atmosphere_flow_safe": False,
                    "seal_pressure_safe": False,
                    "analyzer_pressure_available": False,
                },
                "continuous_atmosphere_state": {
                    "active": True,
                    "keepalive_count": 4,
                },
                "pre_route_drain_syst_err_count": 2,
            }
        }
    )

    assert extract["continuous_atmosphere_active"] is True
    assert extract["vent_keepalive_count"] == 4
    assert extract["atmosphere_flow_safe"] is False
    assert extract["seal_pressure_safe"] is False
    assert extract["analyzer_pressure_available"] is False
    assert extract["pre_route_drain_syst_err_count"] == 2


def test_summary_extract_does_not_drop_route_pressure_guard_status() -> None:
    extract = live_tool._build_summary_extract(
        {
            "scenario_result": {
                "point_runtime_state": {},
                "route_pressure_guard_summary": {
                    "route_pressure_guard_status": "fail",
                    "route_pressure_guard_reason": "RouteVentPathNotEffective",
                },
            }
        }
    )

    assert extract["route_pressure_guard_status"] == "fail"
    assert extract["route_pressure_guard_reason"] == "RouteVentPathNotEffective"


def test_route_summary_separates_vent_command_and_vent_status(tmp_path: Path) -> None:
    runner = _FakeRunner()

    result = live_tool._run_continuous_atmosphere_keepalive_probe_no_source(
        runner,
        tmp_path / "trace.csv",
        _args(continuous_keepalive_probe_s=0.25, continuous_keepalive_probe_poll_s=0.0),
    )

    state = result["continuous_atmosphere_state"]
    keepalive_summary = state["last_keepalive_summary"]
    assert keepalive_summary["pace_vent_command_sent"] == ":SOUR:PRES:LEV:IMM:AMPL:VENT 1"
    assert keepalive_summary["pace_vent_status_returned"] == 2
    assert keepalive_summary["pace_vent_status_text"] == "completed"
    assert ":SOUR:PRES:LEV:IMM:AMPL:VENT 2" not in str(result)


def test_continuous_atmosphere_keepalive_probe_no_source(tmp_path: Path) -> None:
    runner = _FakeRunner()

    result = live_tool._run_continuous_atmosphere_keepalive_probe_no_source(
        runner,
        tmp_path / "trace.csv",
        _args(continuous_keepalive_probe_s=0.25, continuous_keepalive_probe_poll_s=0.0),
    )

    assert result["status"] == "pass"
    assert result["open_valves"] == [8, 11, 7]
    assert result["continuous_atmosphere_state"]["active"] is True
    assert result["continuous_atmosphere_state"]["keepalive_count"] >= 1
    assert ("guard", [8, 11, 7]) in runner.calls
    assert any(call[0] == "keepalive" for call in runner.calls)


def test_baseline_final_syst_err_uses_post_cleanup_drain(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    output_dir = tmp_path / "out"
    config_path = tmp_path / "cfg.yaml"
    config_path.write_text("devices: {}\nworkflow: {}\n", encoding="utf-8")

    class _MainFakeRunner(_FakeRunner):
        def __init__(self, *args, **kwargs):
            super().__init__()
            self.final_syst_err = ':SYST:ERR -102,"Syntax error"'
            self.cleanup_drained = False

        def _capture_pace_capability_snapshot(self, **kwargs):
            return {
                "pressure_unit": "HPA",
                "pressure_unit_status": "known_hpa",
                "pressure_unit_allows_formal_setpoint": True,
                "sens_pres_cont_supported": False,
                "sens_pres_cont_error": ':SYST:ERR -113,"Undefined header"',
            }

        def _set_co2_route_baseline(self, reason: str = "") -> None:
            self.calls.append(("baseline", reason))

        def _drain_pace_system_errors(self, reason: str = "", **kwargs):
            self.calls.append(("drain", reason))
            if "post-cleanup final drain" in reason and not self.cleanup_drained:
                self.cleanup_drained = True
                self.final_syst_err = '0,"No error"'
                return [':SYST:ERR -102,"Syntax error"']
            return []

    monkeypatch.setattr(live_tool, "load_config", lambda _path: {"devices": {}, "workflow": {}, "paths": {"output_dir": str(output_dir)}})
    monkeypatch.setattr(live_tool, "_build_devices", lambda runtime_cfg, io_logger=None: {})
    monkeypatch.setattr(live_tool, "_close_devices", lambda devices: None)
    monkeypatch.setattr(live_tool, "perform_safe_stop_with_retries", lambda devices, log_fn=None, cfg=None: {"ok": True})
    monkeypatch.setattr(live_tool, "CalibrationRunner", _MainFakeRunner)

    exit_code = live_tool.main(
        [
            "--real-device",
            "--config",
            str(config_path),
            "--output-dir",
            str(output_dir),
            "--scenario",
            "baseline_atmosphere_hold_60s",
            "--baseline-hold-monitor-s",
            "0",
            "--baseline-hold-poll-s",
            "0",
        ]
    )

    assert exit_code == 0
    summary_files = list(output_dir.rglob("pressure_gate_live_summary.json"))
    assert summary_files
    data = summary_files[0].read_text(encoding="utf-8")
    assert '"final_syst_err": "0,\\"No error\\""' in data
    assert '"cleanup_syst_errs": [' in data
    assert '"status": "pass_with_diagnostic_error"' in data
