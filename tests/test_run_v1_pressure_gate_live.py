import json
from argparse import Namespace
from pathlib import Path

import pytest

from gas_calibrator.tools import run_v1_pressure_gate_live as live_tool


class _FakePacePressureReady:
    def __init__(self, *, results: list[dict[str, object]] | None = None) -> None:
        self.calls: list[dict[str, object]] = []
        self.events: list[tuple[str, object]] = []
        self._results = [dict(item) for item in list(results or [])]

    def set_setpoint(self, value_hpa: float) -> None:
        self.events.append(("set_setpoint", float(value_hpa)))

    def wait_for_pressure_ready(self, **kwargs):
        self.events.append(("wait_for_pressure_ready", dict(kwargs)))
        self.calls.append(dict(kwargs))
        if self._results:
            return dict(self._results.pop(0))
        target_hpa = float(kwargs.get("target_hpa", 0.0) or 0.0)
        return {
            "ok": True,
            "reason": "",
            "target_hpa": target_hpa,
            "setpoint_hpa": target_hpa,
            "output_state": 1,
            "last_pressure_hpa": target_hpa,
            "last_in_limit_flag": 1,
            "poll_count": 1,
            "timeout_s": float(kwargs.get("timeout_s", 0.0) or 0.0),
            "poll_s": float(kwargs.get("poll_s", 0.0) or 0.0),
            "consecutive_in_limits_required": int(kwargs.get("consecutive_in_limits_required", 1) or 1),
            "ready_dwell_s": float(kwargs.get("ready_dwell_s", 0.0) or 0.0),
            "ready_hold_elapsed_s": float(kwargs.get("ready_dwell_s", 0.0) or 0.0),
            "recent_states": [{"pressure_hpa": target_hpa, "in_limit_flag": 1}],
        }


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
        self._current_open_valves: tuple[int, ...] = ()
        self._pace_state_cache = {"in_limits_cache_valid": True, "sample_ts": "2026-04-22T17:00:00.000"}
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
        self.mechanical_pressure_protection_confirmed = True
        self.verify_result: dict[str, object] | None = None
        self.candidate_result: dict[str, object] | None = None
        self.apply_result: dict[str, object] | None = None
        self.raise_on_source_final_open = False
        self.route_open_guard_fail_reason = ""
        self.cleanup_calls = 0
        self.pace_commands_sent: list[str] = []
        self.vent_write_commands: list[str] = []
        self.vent_query_responses: list[object] = []
        self.trace_rows: list[dict[str, object]] = []
        self.devices = {"pace": _FakePacePressureReady()}

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
            "profile": "OLD_PACE5000",
            "pressure_unit": "HPA",
            "pressure_unit_status": "known_hpa",
            "pressure_unit_allows_formal_setpoint": True,
            "final_syst_err": self.final_syst_err,
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
        self.trace_rows.append(dict(kwargs))

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

    def _pace_state_snapshot(self, refresh: bool = True):
        self.calls.append(("pace_state_snapshot", bool(refresh)))
        return {"pace_oper_pres_in_limits_bit": 1, "pace_oper_pres_even_query": 0}

    def _set_pressure_controller_vent(self, is_open: bool, reason: str = "") -> None:
        self.calls.append(("vent", bool(is_open)))
        self.vent_write_commands.append(":SOUR:PRES:LEV:IMM:AMPL:VENT 1" if is_open else ":SOUR:PRES:LEV:IMM:AMPL:VENT 0")

    def exit_continuous_atmosphere_flowthrough(self, route_key: str = "", **kwargs):
        self._continuous_state["active"] = False
        self._continuous_state["route_flow_active"] = False
        self._continuous_state["route_key"] = str(route_key or self._continuous_state.get("route_key") or "")
        self.calls.append(("exit_flowthrough", str(route_key)))
        return dict(self._continuous_state)

    def _ensure_pressure_controller_ready_for_control(self, point, **kwargs) -> bool:
        self._continuous_state["active"] = False
        self._continuous_state["route_flow_active"] = False
        self.calls.append(("control_ready", float(kwargs.get("pressure_target_hpa", 0.0) or 0.0)))
        return True

    def _enable_pressure_controller_output(self, reason: str = "") -> bool:
        self.calls.append(("output_on", reason))
        return True

    def _co2_open_valves(self, point, include_total_valve: bool, *, include_source_valve: bool = True):
        if str(getattr(point, "co2_group", "") or "").upper() == "B":
            return [8, 11, 16, 24] if include_source_valve else [8, 11, 16]
        return [8, 11, 7, 4] if include_source_valve else [8, 11, 7]

    def _h2o_open_valves(self, point, *, include_final_stage: bool = True):
        return [8, 9, 10] if include_final_stage else [8, 9]

    def _source_stage_key_for_point(self, point, *, phase: str = ""):
        if str(phase or "").strip().lower() == "h2o":
            return "h2o"
        return "co2_b" if str(getattr(point, "co2_group", "") or "").upper() == "B" else "co2_a"

    def _seal_pressure_target_supported_by_hardware(self, point) -> bool:
        return True

    def _mechanical_pressure_protection_confirmed(self) -> bool:
        return bool(self.mechanical_pressure_protection_confirmed)

    def _open_route_with_pressure_guard(self, point, **kwargs):
        open_valves = list(kwargs.get("open_valves") or [])
        self.calls.append(("guard", open_valves))
        route_key = self._source_stage_key_for_point(point, phase=str(kwargs.get("phase") or "co2"))
        if self.raise_on_source_final_open and 4 in open_valves:
            raise RuntimeError("boom during staged valve 4 open")
        if self.route_open_guard_fail_reason:
            return False
        self._current_open_valves = tuple(open_valves)
        if 4 not in open_valves and 24 not in open_valves and 10 not in open_valves:
            self._source_stage[route_key] = True
            self._atmosphere_safe[route_key] = True
            self._seal_safe[route_key] = False
        self._last_route_pressure_guard_summary = {
            "route_pressure_guard_status": "pass",
            "route_pressure_guard_reason": "",
            "analyzer_pressure_available": False,
            "analyzer_pressure_protection_active": False,
            "analyzer_pressure_status": "unavailable",
            "pace_syst_err_query": self.final_syst_err,
        }
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

    def verify_seal_pressure_stage_preconditions(self, point, *, phase: str = "", evidence_source: str = "", verification_inputs=None):
        payload = {
            "eligible": True,
            "reason": "",
            "reasons": [],
            "source_stage_key": self._source_stage_key_for_point(point, phase=phase),
            "source_stage_safe": True,
            "route_final_stage_atmosphere_safe": True,
            "route_final_stage_seal_safe": False,
            "evidence_source": evidence_source,
            "not_real_acceptance_evidence": True,
            "pressure_read_fresh": True,
            "in_limits_cache_fresh": True,
            "target_pressure_supported": True,
            "analyzer_pressure_protection_active": False,
            "mechanical_pressure_protection_confirmed": True,
            "blocked_valves": [4, 24, 10],
            "source_final_valves_open": [],
        }
        if verification_inputs:
            payload.update(dict(verification_inputs))
        if self.verify_result is not None:
            payload.update(dict(self.verify_result))
        self.calls.append(("verify", evidence_source))
        return payload

    def evaluate_seal_pressure_verified_release_candidate(self, verification, **kwargs):
        payload = {
            "candidate_type": "seal_pressure_verified_release_candidate",
            "eligible_for_explicit_release": True,
            "release_performed": False,
            "route_final_stage_seal_safety_updated": False,
            "source_stage_key": str((verification or {}).get("source_stage_key") or "co2_a"),
            "required_conditions": {
                "explicit_allow": True,
                "fresh_pressure_read": True,
                "fresh_in_limits_cache": True,
                "target_pressure_supported": True,
                "analyzer_pressure_protection_confirmed": False,
                "mechanical_pressure_protection_confirmed": True,
                "no_active_atmosphere_keepalive": True,
                "no_post_exit_vent_leak": True,
                "hidden_syst_err_count_zero": True,
                "unclassified_syst_err_count_zero": True,
                "pre_route_drain_syst_err_count_zero": True,
                "no_vent2_tx": True,
                "source_final_stage_explicit_safety": True,
            },
            "blocked_valves_must_remain_blocked_until_apply": [4, 24, 10],
            "source_final_valves_open": [],
            "source_final_stage_opened": False,
            "co2_4_24_opened": False,
            "h2o_10_opened": False,
            "real_sealed_pressure_transition_started": False,
            "observations": ["LiveSafePreflightIsNotAcceptance"],
            "reason": "",
            "reasons": [],
        }
        if self.candidate_result is not None:
            payload.update(dict(self.candidate_result))
        self.calls.append(("candidate", bool(kwargs.get("explicit_allow"))))
        return payload

    def apply_seal_pressure_verified_release_candidate(self, **kwargs):
        key = str(kwargs.get("source_stage_key") or "co2_a")
        dry_run = bool(kwargs.get("dry_run"))
        current_value = bool(self._seal_safe.get(key, False))
        payload = {
            "apply_type": "seal_pressure_verified_release_apply",
            "dry_run": dry_run,
            "dry_run_release_suppressed": dry_run,
            "dry_run_authorized_for_staged_source_final": dry_run
            and str(kwargs.get("release_scope") or "") == live_tool.CO2_A_STAGED_RELEASE_SCOPE,
            "release_performed": not dry_run,
            "route_final_stage_seal_safety_updated": not dry_run,
            "route_final_stage_seal_safety_key": key,
            "route_final_stage_seal_safety_value": current_value if dry_run else True,
            "would_update_route_final_stage_seal_safety": not current_value,
            "opened_valves": [],
            "pace_commands_sent": [],
            "real_sealed_pressure_transition_started": False,
            "source_final_stage_opened": False,
            "co2_4_24_opened": False,
            "h2o_10_opened": False,
            "first_use_must_be_staged_dry_run": True,
            "reason": "",
            "reasons": [],
            "expected_source_final_valves": list(kwargs.get("expected_source_final_valves") or []),
        }
        if self.apply_result is not None:
            payload.update(dict(self.apply_result))
        if payload.get("release_performed"):
            self._seal_safe[key] = True
        self.calls.append(("apply", list(kwargs.get("expected_source_final_valves") or [])))
        return payload


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
    mechanical_pressure_protection_confirmed: bool | None = None,
    staged_pressure_protection: dict[str, object] | None = None,
    pressure_protection_approval_json: str | None = None,
):
    pressure_cfg: dict[str, object] = {}
    if mechanical_pressure_protection_confirmed is not None:
        pressure_cfg["mechanical_pressure_protection_confirmed"] = mechanical_pressure_protection_confirmed
    if staged_pressure_protection is not None:
        pressure_cfg["co2_a_staged_pressure_protection"] = dict(staged_pressure_protection)
    if pressure_protection_approval_json is not None:
        pressure_cfg["pressure_protection_approval_json"] = pressure_protection_approval_json
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
            "pressure": pressure_cfg
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
    safe_stop_impl=None,
    extra_args: list[str] | None = None,
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
    monkeypatch.setattr(live_tool, "_build_devices", lambda runtime_cfg, io_logger=None: {"pace": object()})
    monkeypatch.setattr(live_tool, "_close_devices", lambda devices: None)
    def _fake_safe_stop(devices, log_fn=None, cfg=None):
        runner = runner_box.get("runner")
        if callable(safe_stop_impl):
            return safe_stop_impl(devices, runner=runner, log_fn=log_fn, cfg=cfg)
        if runner is not None:
            runner.calls.append(("cleanup", list(runner._current_open_valves)))
            runner.cleanup_calls += 1
            runner._current_open_valves = tuple()
        return {"ok": True, "safe_stop_verified": True}

    monkeypatch.setattr(live_tool, "perform_safe_stop_with_retries", _fake_safe_stop)
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
    if extra_args:
        argv.extend(list(extra_args))

    exit_code = live_tool.main(argv)
    summary_path = sorted(
        output_dir.rglob("pressure_gate_live_summary.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )[0]
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    return exit_code, summary, runner_box["runner"]


def _set_required_staged_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALLOW_STAGED_SOURCE_FINAL_DRY_RUN", "CO2_A_VALVE_4_ONLY")
    monkeypatch.setenv("OPERATOR_INTENT_CONFIRMED", "YES")
    monkeypatch.setenv("RELEASE_REASON", "first staged dry run")
    monkeypatch.setenv("CONFIRM_NOT_FULL_PRODUCTION", "YES")
    monkeypatch.setenv("CONFIRM_NO_ROUTE_FLUSH_DEWPOINT_GATE", "YES")
    monkeypatch.setenv("CONFIRM_SINGLE_ROUTE_CO2_A_ONLY", "YES")


def _write_pressure_protection_approval_json(tmp_path: Path, **overrides) -> Path:
    payload = {
        "approval_type": "co2_a_staged_dry_run_pressure_protection_approval",
        "route": "CO2_A",
        "source_final_valve_under_test": 4,
        "approval_scope": "CO2_A_VALVE_4_STAGED_DRY_RUN_ONLY",
        "retry_scope": "staged_source_final_release_dry_run",
        "retry_allowed_for_scope": True,
        "analyzer_pressure_protection_active": False,
        "mechanical_pressure_protection_confirmed": True,
        "not_full_v1_production_approval": True,
        "not_full_formal_approval": True,
        "does_not_open_4_24_10": True,
        "does_not_run_real_sealed_pressure_transition": True,
    }
    payload.update(overrides)
    path = tmp_path / "pressure_protection_approval.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


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


def test_co2_a_pressure_switch_smoke_requires_allow_source_open_flag(tmp_path: Path) -> None:
    runner = _FakeRunner()
    result = live_tool._run_co2_a_pressure_switch_smoke_no_temp_wait(
        runner,
        tmp_path / "trace.csv",
        _args(allow_source_open=False, pressure_points_hpa="1100,1000,900"),
    )

    assert result["status"] == "skipped"
    assert result["skipped_reason"] == "SourceOpenRequiresExplicitAllowFlag"
    assert result["temperature_wait_skipped"] is True
    assert result["pressure_points_requested"] == [1100.0, 1000.0, 900.0]


def test_route_open_pressure_guard_requires_allow_source_open_flag(tmp_path: Path) -> None:
    runner = _FakeRunner()
    result = live_tool._run_route_open_pressure_guard(
        runner,
        tmp_path / "trace.csv",
        _args(allow_source_open=False),
    )

    assert result["status"] == "skipped"
    assert result["skipped_reason"] == "SourceOpenRequiresExplicitAllowFlag"
    assert result["operator_must_confirm_upstream_source_pressure_limited"] is True
    assert all(call[0] != "conditioning" for call in runner.calls)


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


def test_pressure_switch_smoke_requires_analyzer_pressure_or_mechanical_protection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exit_code, summary, runner = _run_main(
        tmp_path,
        monkeypatch,
        scenario=live_tool.CO2_A_PRESSURE_SWITCH_SMOKE_NO_TEMP_WAIT,
        config=_config(),
        allow_source_open=True,
        extra_args=["--pressure-points-hpa", "1100,1000"],
    )

    assert exit_code == 0
    assert summary["scenario_result"]["status"] == "diagnostic_error"
    assert summary["scenario_result"]["abort_reason"] == "AnalyzerPressureRequiredButUnavailable"
    assert summary["scenario_result"]["analyzer_pressure_required"] is True
    assert summary["scenario_result"]["mechanical_pressure_protection_confirmed"] is False
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


def test_co2_a_pressure_switch_smoke_runs_multi_point_ready_sequence(tmp_path: Path) -> None:
    runner = _FakeRunner()
    fake_pace = _FakePacePressureReady(
        results=[
            {"ok": True, "reason": "", "target_hpa": 1100.0, "setpoint_hpa": 1100.0, "output_state": 1, "last_pressure_hpa": 1100.0, "last_in_limit_flag": 1, "poll_count": 2},
            {"ok": True, "reason": "", "target_hpa": 1000.0, "setpoint_hpa": 1000.0, "output_state": 1, "last_pressure_hpa": 1000.0, "last_in_limit_flag": 1, "poll_count": 2},
            {"ok": True, "reason": "", "target_hpa": 900.0, "setpoint_hpa": 900.0, "output_state": 1, "last_pressure_hpa": 900.0, "last_in_limit_flag": 1, "poll_count": 2},
        ]
    )
    runner.devices = {"pace": fake_pace}

    result = live_tool._run_co2_a_pressure_switch_smoke_no_temp_wait(
        runner,
        tmp_path / "trace.csv",
        _args(
            allow_source_open=True,
            pressure_points_hpa="1100,1000,900",
            _runtime_cfg=_config(mechanical_pressure_protection_confirmed=True),
        ),
    )

    assert result["status"] == "pass"
    assert result["route_open_passed"] is True
    assert result["temperature_wait_skipped"] is True
    assert result["temperature_chamber_enabled_in_runtime"] is False
    assert result["pressure_points_requested"] == [1100.0, 1000.0, 900.0]
    assert result["pressure_points_completed"] == [1100.0, 1000.0, 900.0]
    assert result["pressure_point_switch_requested"] is True
    assert result["pressure_point_switch_executed"] is True
    assert [call["target_hpa"] for call in fake_pace.calls] == [1100.0, 1000.0, 900.0]
    first_step = result["pressure_point_results"][0]
    assert first_step["requested_target_hpa"] == 1100.0
    assert first_step["pace_events"] == ["set_setpoint", "wait_for_pressure_ready"]
    assert first_step["runner_calls"].index("control_ready") < first_step["runner_calls"].index("output_on")


def test_co2_a_pressure_switch_smoke_stops_on_first_pressure_ready_failure(tmp_path: Path) -> None:
    runner = _FakeRunner()
    fake_pace = _FakePacePressureReady(
        results=[
            {"ok": True, "reason": "", "target_hpa": 1100.0, "setpoint_hpa": 1100.0, "output_state": 1, "last_pressure_hpa": 1100.0, "last_in_limit_flag": 1, "poll_count": 2},
            {"ok": False, "reason": "PressureInLimitsTimeout", "target_hpa": 1000.0, "setpoint_hpa": 1000.0, "output_state": 1, "last_pressure_hpa": 996.0, "last_in_limit_flag": 0, "poll_count": 5},
            {"ok": True, "reason": "", "target_hpa": 900.0, "setpoint_hpa": 900.0, "output_state": 1, "last_pressure_hpa": 900.0, "last_in_limit_flag": 1, "poll_count": 2},
        ]
    )
    runner.devices = {"pace": fake_pace}

    result = live_tool._run_co2_a_pressure_switch_smoke_no_temp_wait(
        runner,
        tmp_path / "trace.csv",
        _args(
            allow_source_open=True,
            pressure_points_hpa="1100,1000,900",
            _runtime_cfg=_config(mechanical_pressure_protection_confirmed=True),
        ),
    )

    assert result["status"] == "diagnostic_error"
    assert result["abort_reason"] == "PressureInLimitsTimeout"
    assert result["pressure_points_completed"] == [1100.0]
    assert [call["target_hpa"] for call in fake_pace.calls] == [1100.0, 1000.0]
    assert len(result["pressure_point_results"]) == 2
    assert result["pressure_point_results"][-1]["requested_target_hpa"] == 1000.0
    assert result["pressure_point_results"][-1]["ok"] is False


def test_main_co2_a_pressure_switch_smoke_writes_not_acceptance_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exit_code, summary, _runner = _run_main(
        tmp_path,
        monkeypatch,
        scenario=live_tool.CO2_A_PRESSURE_SWITCH_SMOKE_NO_TEMP_WAIT,
        config=_config(mechanical_pressure_protection_confirmed=True),
        allow_source_open=True,
        extra_args=["--pressure-points-hpa", "1100,1000"],
    )

    assert exit_code == 0
    assert summary["not_real_acceptance_evidence"] is True
    assert summary["scenario_result"]["status"] == "pass"
    assert summary["scenario_result"]["temperature_wait_skipped"] is True
    assert summary["scenario_result"]["pressure_points_completed"] == [1100.0, 1000.0]


def _old_k0472_source_final_jump_trace_rows() -> list[dict[str, object]]:
    return [
        {
            "trace_stage": "route_open_pressure_guard_sample",
            "valve_route_state": "open:7|8|11",
            "pace_vent_status_query": "2",
            "pace_outp_state_query": "0",
            "pace_isol_state_query": "1",
            "pressure_gauge_hpa": "1011.8",
            "ambient_hpa": "1009.4",
            "pressure_delta_from_ambient_hpa": "2.4",
            "pace_vent_completed_latched": "True",
        },
        {
            "trace_stage": "route_open_stage",
            "valve_route_state": "open:4|7|8|11",
            "pace_vent_status_query": "2",
            "pace_outp_state_query": "0",
            "pace_isol_state_query": "1",
            "pressure_delta_from_ambient_hpa": "2.4",
            "pace_vent_completed_latched": "True",
            "offending_valve_or_group": "8|11|7|4",
        },
        {
            "trace_stage": "route_open_fresh_vent_end",
            "valve_route_state": "open:4|7|8|11",
            "pace_vent_status_query": "2",
            "pace_outp_state_query": "0",
            "pace_isol_state_query": "1",
            "ambient_hpa": "1009.5",
            "pressure_delta_from_ambient_hpa": "433.4",
            "pace_vent_completed_latched": "True",
            "offending_valve_or_group": "8|11|7|4",
        },
        {
            "trace_stage": "route_open_pressure_guard_sample",
            "valve_route_state": "open:4|7|8|11",
            "pace_vent_status_query": "2",
            "pace_outp_state_query": "0",
            "pace_isol_state_query": "1",
            "pressure_gauge_hpa": "1442.9",
            "ambient_hpa": "1009.5",
            "pressure_delta_from_ambient_hpa": "433.5",
            "pace_vent_completed_latched": "True",
            "route_pressure_guard_status": "fail",
            "route_pressure_guard_reason": "AnalyzerPressureTooHigh",
            "offending_valve_or_group": "8|11|7|4",
        },
    ]


def test_old_k0472_vent2_and_keepalive_do_not_prove_sustained_atmosphere() -> None:
    diagnostics = live_tool._co2_a_sustained_atmosphere_diagnostics(
        route_open_state={
            "pace_vent_status_query": 2,
            "pace_outp_state_query": 0,
            "pace_isol_state_query": 1,
            "pace_vent_completed_latched": True,
            "continuous_atmosphere_active": True,
            "vent_keepalive_count": 4,
        },
        point_state={
            "continuous_atmosphere_active": True,
            "vent_keepalive_count": 4,
        },
        route_guard_summary={},
        trace_rows=_old_k0472_source_final_jump_trace_rows(),
    )

    assert diagnostics["old_k0472_remote_sustained_atmosphere_proven"] is False
    assert diagnostics["old_k0472_remote_sustained_atmosphere_not_proven"] is True
    assert (
        diagnostics["pre_flush_sustained_atmosphere_evidence_basis"]
        == "vent_cycle_completed_and_pressure_window_only"
    )
    assert diagnostics["flush_phase_requires_continuous_atmosphere"] is True
    assert diagnostics["flush_phase_remote_sustained_atmosphere_proven"] is False
    assert diagnostics["flush_phase_pressure_rise_unexpected"] is True
    assert diagnostics["flush_phase_evidence_basis"] == "vent_cycle_completed_and_pressure_window_only"
    assert diagnostics["pre_source_final_vent_status"] == 2
    assert diagnostics["pre_source_final_outp_state"] == 0
    assert diagnostics["pre_source_final_isol_state"] == 1
    assert diagnostics["source_final_open_pressure_jump_hpa"] == pytest.approx(431.0)
    assert diagnostics["post_source_final_fresh_vent_recovery_effective"] is False
    assert diagnostics["post_seal_air_ingress_validation_status"] == "deferred"
    assert diagnostics["post_seal_vent_command_allowed"] is False


def test_pressure_switch_summary_and_trace_mark_sustained_atmosphere_not_proven(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _FakeRunner()
    runner._continuous_state["active"] = True
    runner._continuous_state["route_flow_active"] = True
    runner._continuous_state["keepalive_count"] = 4
    runner.devices = {
        "pace": _FakePacePressureReady(
            results=[
                {
                    "ok": True,
                    "reason": "",
                    "target_hpa": 1100.0,
                    "setpoint_hpa": 1100.0,
                    "output_state": 1,
                    "last_pressure_hpa": 1100.0,
                    "last_in_limit_flag": 1,
                    "poll_count": 1,
                }
            ]
        )
    }
    monkeypatch.setattr(
        live_tool,
        "_scenario_trace_rows",
        lambda trace_path, trace_start: list(_old_k0472_source_final_jump_trace_rows()),
    )

    result = live_tool._run_co2_a_pressure_switch_smoke_no_temp_wait(
        runner,
        tmp_path / "trace.csv",
        _args(
            allow_source_open=True,
            pressure_points_hpa="1100",
            _runtime_cfg=_config(mechanical_pressure_protection_confirmed=True),
        ),
    )

    assert result["old_k0472_remote_sustained_atmosphere_proven"] is False
    assert result["old_k0472_remote_sustained_atmosphere_not_proven"] is True
    assert (
        result["pre_flush_sustained_atmosphere_evidence_basis"]
        == "vent_cycle_completed_and_pressure_window_only"
    )
    assert result["flush_phase_requires_continuous_atmosphere"] is True
    assert result["flush_phase_remote_sustained_atmosphere_proven"] is False
    assert result["flush_phase_pressure_rise_unexpected"] is True
    assert result["post_seal_air_ingress_validation_status"] == "deferred"
    assert result["post_seal_vent_command_allowed"] is False
    stages = [str(row.get("trace_stage") or "") for row in runner.trace_rows]
    assert "pre_source_final_atmosphere_evidence" in stages
    assert "source_final_open_pressure_jump_detected" in stages
    assert "post_source_final_fresh_vent_recovery_effective" in stages


def test_1100_preseal_buildup_after_flush_exit_is_not_flush_pressure_failure(tmp_path: Path) -> None:
    runner = _FakeRunner()
    fake_pace = _FakePacePressureReady(
        results=[
            {
                "ok": True,
                "reason": "",
                "target_hpa": 1100.0,
                "setpoint_hpa": 1100.0,
                "output_state": 1,
                "last_pressure_hpa": 1105.0,
                "last_in_limit_flag": 1,
                "poll_count": 2,
            }
        ]
    )
    runner.devices = {"pace": fake_pace}

    result = live_tool._run_co2_a_pressure_switch_smoke_no_temp_wait(
        runner,
        tmp_path / "trace.csv",
        _args(
            allow_source_open=True,
            pressure_points_hpa="1100",
            _runtime_cfg=_config(mechanical_pressure_protection_confirmed=True),
        ),
    )

    assert result["status"] == "pass"
    assert result["flush_phase_pressure_rise_unexpected"] is False
    assert result["preseal_pressure_buildup_for_1100_allowed"] is True
    assert result["preseal_pressure_buildup_started"] is True
    assert result["preseal_pressure_buildup_threshold_reached"] is True
    assert result["preseal_pressure_buildup_reason"] == "prepare_for_1100_seal_control"
    assert result["sealed_control_started"] is False
    stages = [str(row.get("trace_stage") or "") for row in runner.trace_rows]
    assert "preseal_pressure_buildup_for_1100_begin" in stages
    assert "preseal_pressure_buildup_threshold_reached" in stages


def test_sealed_control_boundary_keeps_post_seal_vent_forbidden() -> None:
    diagnostics = live_tool._co2_a_sustained_atmosphere_diagnostics(
        route_open_state={},
        point_state={},
        route_guard_summary={},
        trace_rows=[
            {"trace_stage": "preseal_pressure_buildup_for_1100_begin"},
            {"trace_stage": "route_sealed"},
            {"trace_stage": "control_output_on_begin"},
        ],
        pressure_targets_hpa=[1100.0],
        pressure_point_results=[
            {
                "requested_target_hpa": 1100.0,
                "ok": True,
                "last_pressure_hpa": 1102.0,
            }
        ],
    )

    assert diagnostics["preseal_pressure_buildup_for_1100_allowed"] is True
    assert diagnostics["preseal_pressure_buildup_started"] is True
    assert diagnostics["preseal_pressure_buildup_threshold_reached"] is True
    assert diagnostics["sealed_control_started"] is True
    assert diagnostics["post_seal_vent_command_allowed"] is False


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


def test_seal_pressure_stage_not_verified_does_not_block_route_flush(tmp_path: Path) -> None:
    runner = _FakeRunner()

    co2_a = live_tool._run_route_synchronized_atmosphere_flush_co2_a_no_source(
        runner,
        tmp_path / "co2_a_trace.csv",
        _args(),
    )
    co2_b = live_tool._run_route_synchronized_atmosphere_flush_co2_b_no_source(
        runner,
        tmp_path / "co2_b_trace.csv",
        _args(),
    )
    h2o = live_tool._run_route_synchronized_atmosphere_flush_h2o_no_final(
        runner,
        tmp_path / "h2o_trace.csv",
        _args(),
    )

    assert co2_a["open_valves"] == [8, 11, 7]
    assert co2_b["open_valves"] == [8, 11, 16]
    assert h2o["open_valves"] == [8, 9]
    assert 4 not in co2_a["open_valves"]
    assert 24 not in co2_b["open_valves"]
    assert 10 not in h2o["open_valves"]
    assert ("guard", [8, 11, 7]) in runner.calls
    assert ("guard", [8, 11, 16]) in runner.calls
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


def test_co2_a_staged_source_final_dry_run_requires_operator_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in (
        "ALLOW_STAGED_SOURCE_FINAL_DRY_RUN",
        "OPERATOR_INTENT_CONFIRMED",
        "RELEASE_REASON",
        "CONFIRM_NOT_FULL_PRODUCTION",
        "CONFIRM_NO_ROUTE_FLUSH_DEWPOINT_GATE",
        "CONFIRM_SINGLE_ROUTE_CO2_A_ONLY",
    ):
        monkeypatch.delenv(name, raising=False)

    runner = _FakeRunner()
    result = live_tool._run_co2_a_staged_source_final_release_dry_run(
        runner,
        tmp_path / "trace.csv",
        _args(),
    )

    assert result["status"] == "skipped"
    assert result["abort_reason"] == "operator_confirmation_missing"
    assert "ALLOW_STAGED_SOURCE_FINAL_DRY_RUN" in result["missing_operator_env"]
    assert "RELEASE_REASON" in result["missing_operator_env"]
    assert all(call[0] not in {"verify", "candidate", "apply", "guard"} for call in runner.calls)
    assert result["pace_commands_sent"] == []


@pytest.mark.parametrize(
    ("kwargs", "expected_reason"),
    [
        ({"route": "CO2_B"}, "OnlyCO2ARouteSupported"),
        ({"route": "H2O"}, "OnlyCO2ARouteSupported"),
        ({"source_final_valve_under_test": 24}, "OnlyValve4SourceFinalStageSupported"),
        ({"source_final_valve_under_test": 10}, "OnlyValve4SourceFinalStageSupported"),
        ({"release_scope": "full_v1_production"}, "OnlyStagedSourceFinalReleaseDryRunSupported"),
    ],
)
def test_co2_a_staged_source_final_dry_run_rejects_non_co2_a_scope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kwargs: dict[str, object],
    expected_reason: str,
) -> None:
    _set_required_staged_env(monkeypatch)
    runner = _FakeRunner()

    result = live_tool._run_co2_a_staged_source_final_release_dry_run(
        runner,
        tmp_path / "trace.csv",
        _args(),
        **kwargs,
    )

    assert result["status"] == "skipped"
    assert result["abort_reason"] == expected_reason
    assert all(call[0] not in {"verify", "candidate", "apply", "guard"} for call in runner.calls)
    assert result["co2_4_opened"] is False


@pytest.mark.parametrize(
    ("verify_result", "candidate_result", "apply_result", "expected_reason"),
    [
        ({"eligible": False, "reason": "SourceStageNotVerified", "reasons": ["SourceStageNotVerified"]}, None, None, "SourceStageNotVerified"),
        (None, {"eligible_for_explicit_release": False, "reason": "CandidateNotEligible", "reasons": ["CandidateNotEligible"]}, None, "CandidateNotEligible"),
        (None, None, {"release_performed": False, "route_final_stage_seal_safety_updated": False, "reason": "ExplicitApplyFailed", "reasons": ["ExplicitApplyFailed"]}, "ExplicitApplyFailed"),
    ],
)
def test_co2_a_staged_source_final_dry_run_requires_candidate_and_apply(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    verify_result: dict[str, object] | None,
    candidate_result: dict[str, object] | None,
    apply_result: dict[str, object] | None,
    expected_reason: str,
) -> None:
    _set_required_staged_env(monkeypatch)
    runner = _FakeRunner()
    runner.verify_result = verify_result
    runner.candidate_result = candidate_result
    runner.apply_result = apply_result

    result = live_tool._run_co2_a_staged_source_final_release_dry_run(
        runner,
        tmp_path / "trace.csv",
        _args(),
    )

    assert result["dry_run_passed"] is False
    assert result["abort_reason"] == expected_reason
    assert result["co2_4_opened"] is False
    assert [8, 11, 7, 4] not in [call[1] for call in runner.calls if call[0] == "guard"]


def test_co2_a_staged_source_final_dry_run_opens_only_valve_4_after_apply(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required_staged_env(monkeypatch)

    exit_code, summary, runner = _run_main(
        tmp_path,
        monkeypatch,
        scenario=live_tool.CO2_A_STAGED_SOURCE_FINAL_RELEASE_DRY_RUN,
        config=_config(),
    )

    assert exit_code == 0
    assert summary["scenario_result"]["dry_run_passed"] is True
    assert summary["scenario_result"]["explicit_apply_succeeded"] is True
    assert summary["scenario_result"]["release_performed"] is False
    assert summary["scenario_result"]["route_final_stage_seal_safety_updated"] is False
    assert summary["scenario_result"]["dry_run_release_suppressed"] is True
    assert summary["scenario_result"]["dry_run_authorized_for_staged_source_final"] is True
    guard_calls = [call[1] for call in runner.calls if call[0] == "guard"]
    assert [8, 11, 7] in guard_calls
    assert [8, 11, 7, 4] in guard_calls
    assert summary["scenario_result"]["co2_4_opened"] is True
    assert summary["scenario_result"]["co2_24_opened"] is False
    assert summary["scenario_result"]["h2o_10_opened"] is False
    assert summary["scenario_result"]["pace_commands_sent"] == []
    assert summary["scenario_result"]["vent2_tx_observed"] is False
    assert runner._seal_safe["co2_a"] is False
    assert runner.cleanup_calls == 1
    assert ("cleanup", [8, 11, 7, 4]) in runner.calls
    assert runner._current_open_valves == tuple()


def test_co2_a_staged_source_final_dry_run_writes_not_acceptance_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required_staged_env(monkeypatch)

    exit_code, summary, _runner = _run_main(
        tmp_path,
        monkeypatch,
        scenario=live_tool.CO2_A_STAGED_SOURCE_FINAL_RELEASE_DRY_RUN,
        config=_config(),
    )

    assert exit_code == 0
    artifact_path = Path(summary["dry_run_summary_path"])
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))

    assert artifact["route"] == "CO2_A"
    assert artifact["real_sealed_pressure_transition_verified"] is False
    assert artifact["not_full_v1_production_acceptance"] is True
    assert artifact["not_full_formal_acceptance"] is True
    assert artifact["explicit_apply_succeeded"] is True
    assert artifact["release_performed"] is False
    assert artifact["route_final_stage_seal_safety_updated"] is False
    assert artifact["dry_run_release_suppressed"] is True
    assert artifact["dry_run_authorized_for_staged_source_final"] is True
    assert artifact["co2_4_opened"] is True
    assert artifact["co2_24_opened"] is False
    assert artifact["h2o_10_opened"] is False
    assert artifact["vent2_tx_observed"] is False
    assert artifact["dry_run_passed"] is True


def test_co2_a_staged_source_final_dry_run_cleanup_runs_on_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required_staged_env(monkeypatch)

    def _runner_factory() -> _FakeRunner:
        runner = _FakeRunner()
        runner.raise_on_source_final_open = True
        return runner

    def _safe_stop(devices, *, runner=None, log_fn=None, cfg=None):
        if runner is not None:
            runner.calls.append(("cleanup", list(runner._current_open_valves)))
            runner.cleanup_calls += 1
            runner._current_open_valves = tuple()
        return {"ok": True, "safe_stop_verified": True}

    exit_code, summary, runner = _run_main(
        tmp_path,
        monkeypatch,
        scenario=live_tool.CO2_A_STAGED_SOURCE_FINAL_RELEASE_DRY_RUN,
        config=_config(),
        runner_factory=_runner_factory,
        safe_stop_impl=_safe_stop,
    )

    assert exit_code == 0
    artifact = json.loads(Path(summary["dry_run_summary_path"]).read_text(encoding="utf-8"))
    assert summary["scenario_result"]["status"] == "fail"
    assert artifact["cleanup_completed"] is True
    assert artifact["closed_valves_final"] == [4, 24, 10]
    assert artifact["dry_run_passed"] is False
    assert runner.cleanup_calls == 1
    assert ("cleanup", [8, 11, 7]) in runner.calls or ("cleanup", [8, 11, 7, 4]) in runner.calls
    assert runner._current_open_valves == tuple()


def test_co2_a_pressure_protection_resolver_missing_blocks() -> None:
    resolved = live_tool.resolve_co2_a_staged_pressure_protection(
        _config(),
        approval_json_path=None,
        route="CO2_A",
        source_final_valve=4,
        release_scope=live_tool.CO2_A_STAGED_RELEASE_SCOPE,
    )

    assert resolved["pressure_protection_source"] == "missing"
    assert resolved["pressure_protection_precheck_satisfied"] is False
    assert "PressureProtectionApprovalMissing" in resolved["reasons"]


def test_co2_a_pressure_protection_resolver_accepts_existing_v1_analyzer_config() -> None:
    resolved = live_tool.resolve_co2_a_staged_pressure_protection(
        _config(gas_analyzer_enabled=True),
        approval_json_path=None,
        route="CO2_A",
        source_final_valve=4,
        release_scope=live_tool.CO2_A_STAGED_RELEASE_SCOPE,
    )

    assert resolved["pressure_protection_source"] == "existing_v1_analyzer_config"
    assert resolved["pressure_protection_precheck_satisfied"] is True
    assert resolved["analyzer_pressure_protection_active"] is True
    assert resolved["mechanical_pressure_protection_confirmed"] is False
    assert resolved["route"] == "CO2_A"
    assert resolved["source_final_valve_under_test"] == 4
    assert resolved["release_scope"] == live_tool.CO2_A_STAGED_RELEASE_SCOPE
    assert resolved["approval_scope"] == live_tool.CO2_A_STAGED_APPROVAL_SCOPE
    assert resolved["reasons"] == []


def test_co2_a_pressure_protection_resolver_accepts_existing_v1_analyzer_list() -> None:
    resolved = live_tool.resolve_co2_a_staged_pressure_protection(
        _config(gas_analyzers=[{"name": "ga01", "enabled": True}, {"name": "ga02", "enabled": False}]),
        approval_json_path=None,
        route="CO2_A",
        source_final_valve=4,
        release_scope=live_tool.CO2_A_STAGED_RELEASE_SCOPE,
    )

    assert resolved["pressure_protection_source"] == "existing_v1_analyzer_config"
    assert resolved["pressure_protection_precheck_satisfied"] is True
    assert resolved["analyzer_pressure_protection_active"] is True
    assert resolved["mechanical_pressure_protection_confirmed"] is False
    assert resolved["reasons"] == []


@pytest.mark.parametrize("drop_devices", [False, True])
def test_co2_a_pressure_protection_resolver_rejects_root_level_single_analyzer_without_devices_proof(
    drop_devices: bool,
) -> None:
    config = _config(gas_analyzer_enabled=False)
    config["gas_analyzer"] = {"enabled": True, "port": "COM99"}
    if drop_devices:
        config["devices"].pop("gas_analyzer", None)

    resolved = live_tool.resolve_co2_a_staged_pressure_protection(
        config,
        approval_json_path=None,
        route="CO2_A",
        source_final_valve=4,
        release_scope=live_tool.CO2_A_STAGED_RELEASE_SCOPE,
    )

    assert resolved["pressure_protection_source"] == "missing"
    assert resolved["pressure_protection_precheck_satisfied"] is False
    assert resolved["analyzer_pressure_protection_active"] is False
    assert "PressureProtectionApprovalMissing" in resolved["reasons"]


@pytest.mark.parametrize("drop_devices", [False, True])
def test_co2_a_pressure_protection_resolver_rejects_root_level_analyzer_list_without_devices_proof(
    drop_devices: bool,
) -> None:
    config = _config(gas_analyzers=[{"name": "ga01", "enabled": False}, {"name": "ga02", "enabled": False}])
    config["gas_analyzers"] = [{"name": "ga01", "enabled": True}, {"name": "ga02", "enabled": False}]
    if drop_devices:
        config["devices"].pop("gas_analyzers", None)

    resolved = live_tool.resolve_co2_a_staged_pressure_protection(
        config,
        approval_json_path=None,
        route="CO2_A",
        source_final_valve=4,
        release_scope=live_tool.CO2_A_STAGED_RELEASE_SCOPE,
    )

    assert resolved["pressure_protection_source"] == "missing"
    assert resolved["pressure_protection_precheck_satisfied"] is False
    assert resolved["analyzer_pressure_protection_active"] is False
    assert "PressureProtectionApprovalMissing" in resolved["reasons"]


def test_co2_a_pressure_protection_resolver_rejects_noanalyzers_as_protected() -> None:
    resolved = live_tool.resolve_co2_a_staged_pressure_protection(
        _config(
            gas_analyzer_enabled=False,
            gas_analyzers=[{"name": "ga01", "enabled": False}, {"name": "ga02", "enabled": False}],
        ),
        approval_json_path=None,
        route="CO2_A",
        source_final_valve=4,
        release_scope=live_tool.CO2_A_STAGED_RELEASE_SCOPE,
    )

    assert resolved["pressure_protection_source"] == "missing"
    assert resolved["pressure_protection_precheck_satisfied"] is False
    assert resolved["analyzer_pressure_protection_active"] is False
    assert resolved["mechanical_pressure_protection_confirmed"] is False
    assert "PressureProtectionApprovalMissing" in resolved["reasons"]


@pytest.mark.parametrize(
    ("route", "valve", "scope"),
    [
        ("CO2_B", 4, live_tool.CO2_A_STAGED_RELEASE_SCOPE),
        ("CO2_A", 24, live_tool.CO2_A_STAGED_RELEASE_SCOPE),
        ("CO2_A", 4, "wrong_scope"),
    ],
)
def test_co2_a_pressure_protection_resolver_existing_config_wrong_route_scope_blocks(
    route: str,
    valve: int,
    scope: str,
) -> None:
    resolved = live_tool.resolve_co2_a_staged_pressure_protection(
        _config(gas_analyzer_enabled=True),
        approval_json_path=None,
        route=route,
        source_final_valve=valve,
        release_scope=scope,
    )

    assert resolved["pressure_protection_source"] == "missing"
    assert resolved["pressure_protection_precheck_satisfied"] is False
    assert "PressureProtectionScopeInvalid" in resolved["reasons"]


@pytest.mark.parametrize("scope", ["full_v1_production", "route_flush_dewpoint_gate", "full_formal"])
def test_existing_v1_analyzer_mapping_does_not_unlock_full_production(scope: str) -> None:
    resolved = live_tool.resolve_co2_a_staged_pressure_protection(
        _config(gas_analyzer_enabled=True),
        approval_json_path=None,
        route="CO2_A",
        source_final_valve=4,
        release_scope=scope,
    )

    assert resolved["pressure_protection_source"] == "missing"
    assert resolved["pressure_protection_precheck_satisfied"] is False
    assert "PressureProtectionScopeInvalid" in resolved["reasons"]


def test_existing_v1_analyzer_mapping_does_not_affect_other_scenarios(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exit_code, summary, runner = _run_main(
        tmp_path,
        monkeypatch,
        scenario="route_synchronized_atmosphere_flush_co2_a_no_source",
        config=_config(gas_analyzer_enabled=True, gas_analyzers=[{"name": "ga01", "enabled": True}]),
    )

    assert exit_code == 0
    assert summary["scenario_result"]["status"] == "pass"
    assert summary["scenario_result"].get("pressure_protection_source") is None
    assert all(call[0] not in {"verify", "candidate", "apply"} for call in runner.calls)


def test_co2_a_staged_dry_run_uses_pressure_ready_gate_before_source_final_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required_staged_env(monkeypatch)
    runner = _FakeRunner()
    runner.mechanical_pressure_protection_confirmed = False
    fake_pace = _FakePacePressureReady(
        results=[
            {
                "ok": True,
                "reason": "",
                "target_hpa": 1000.0,
                "setpoint_hpa": 1000.0,
                "output_state": 1,
                "last_pressure_hpa": 1000.0,
                "last_in_limit_flag": 1,
                "poll_count": 3,
                "timeout_s": 10.0,
                "poll_s": 0.25,
                "consecutive_in_limits_required": 1,
                "ready_dwell_s": 0.0,
                "ready_hold_elapsed_s": 0.0,
                "recent_states": [
                    {"pressure_hpa": 990.0527344, "in_limit_flag": 0},
                    {"pressure_hpa": 995.0, "in_limit_flag": 0},
                    {"pressure_hpa": 1000.0, "in_limit_flag": 1},
                ],
            }
        ]
    )
    runner.devices = {"pace": fake_pace}

    result = live_tool._run_co2_a_staged_source_final_release_dry_run(
        runner,
        tmp_path / "trace.csv",
        _args(_runtime_cfg=_config(gas_analyzer_enabled=True)),
    )

    assert result["status"] == "pass"
    assert result["pressure_ready_gate"]["ok"] is True
    assert result["pressure_ready_gate"]["poll_count"] == 3
    assert fake_pace.calls and fake_pace.calls[0]["target_hpa"] == 1000.0
    assert ("trace", "staged_pressure_ready_gate") in runner.calls
    assert runner.calls.index(("apply", [4])) < runner.calls.index(("guard", [8, 11, 7, 4]))


def test_co2_a_staged_dry_run_blocks_when_pressure_ready_gate_times_out(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required_staged_env(monkeypatch)
    runner = _FakeRunner()
    runner.mechanical_pressure_protection_confirmed = False
    runner.devices = {
        "pace": _FakePacePressureReady(
            results=[
                {
                    "ok": False,
                    "reason": "PressureInLimitsTimeout",
                    "target_hpa": 1000.0,
                    "setpoint_hpa": 1000.0,
                    "output_state": 1,
                    "last_pressure_hpa": 996.25,
                    "last_in_limit_flag": 0,
                    "poll_count": 4,
                    "timeout_s": 10.0,
                    "poll_s": 0.25,
                    "consecutive_in_limits_required": 1,
                    "ready_dwell_s": 0.0,
                    "ready_hold_elapsed_s": 0.0,
                    "recent_states": [
                        {"pressure_hpa": 990.0527344, "in_limit_flag": 0},
                        {"pressure_hpa": 996.25, "in_limit_flag": 0},
                    ],
                }
            ]
        )
    }

    result = live_tool._run_co2_a_staged_source_final_release_dry_run(
        runner,
        tmp_path / "trace.csv",
        _args(_runtime_cfg=_config(gas_analyzer_enabled=True)),
    )

    assert result["status"] == "diagnostic_error"
    assert result["abort_reason"] == "PressureInLimitsTimeout"
    assert result["pressure_ready_gate"]["ok"] is False
    assert result["pressure_ready_gate"]["last_in_limit_flag"] == 0
    assert result["co2_4_opened"] is False
    assert ("verify", "live_safe_preflight") not in runner.calls
    assert ("apply", [4]) not in runner.calls
    assert ("guard", [8, 11, 7, 4]) not in runner.calls


def test_co2_a_staged_dry_run_arms_pressure_before_wait_gate_with_real_runner_stage_semantics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required_staged_env(monkeypatch)

    class _RealLikeStageSafetyRunner(_FakeRunner):
        def _open_route_with_pressure_guard(self, point, **kwargs):
            open_valves = list(kwargs.get("open_valves") or [])
            self.calls.append(("guard", open_valves))
            route_key = self._source_stage_key_for_point(point, phase=str(kwargs.get("phase") or "co2"))
            if self.route_open_guard_fail_reason:
                return False
            self._current_open_valves = tuple(open_valves)
            if 4 not in open_valves and 24 not in open_valves and 10 not in open_valves:
                self._continuous_state["active"] = True
                self._continuous_state["route_flow_active"] = True
                self._continuous_state["route_key"] = route_key
            else:
                self._source_stage[route_key] = True
                self._atmosphere_safe[route_key] = True
                self._seal_safe[route_key] = False
            self._last_route_pressure_guard_summary = {
                "route_pressure_guard_status": "pass",
                "route_pressure_guard_reason": "",
                "analyzer_pressure_available": False,
                "analyzer_pressure_protection_active": False,
                "analyzer_pressure_status": "unavailable",
                "pace_syst_err_query": self.final_syst_err,
            }
            return True

    fake_pace = _FakePacePressureReady()
    runner = _RealLikeStageSafetyRunner()
    runner.mechanical_pressure_protection_confirmed = False
    runner.devices = {"pace": fake_pace}

    result = live_tool._run_co2_a_staged_source_final_release_dry_run(
        runner,
        tmp_path / "trace.csv",
        _args(_runtime_cfg=_config(gas_analyzer_enabled=True)),
    )

    assert result["status"] == "pass"
    assert result["precheck"]["source_stage_safe"] is True
    assert result["precheck"]["route_final_stage_atmosphere_safe"] is True
    assert "ActiveAtmosphereKeepalive" not in result["precheck"]["blocked_reasons"]
    assert ("exit_flowthrough", "co2_a") in runner.calls
    assert ("control_ready", 1000.0) in runner.calls
    assert ("output_on", "before staged pressure-ready gate") in runner.calls
    assert fake_pace.events[0] == ("set_setpoint", 1000.0)
    assert fake_pace.events[1][0] == "wait_for_pressure_ready"


def test_co2_a_staged_dry_run_accepts_front_path_with_same_valves_in_different_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required_staged_env(monkeypatch)

    class _SortedValveRuntimeRunner(_FakeRunner):
        def _open_route_with_pressure_guard(self, point, **kwargs):
            open_valves = list(kwargs.get("open_valves") or [])
            self.calls.append(("guard", open_valves))
            route_key = self._source_stage_key_for_point(point, phase=str(kwargs.get("phase") or "co2"))
            if self.route_open_guard_fail_reason:
                return False
            self._current_open_valves = tuple(sorted(open_valves))
            if 4 not in open_valves and 24 not in open_valves and 10 not in open_valves:
                self._continuous_state["active"] = True
                self._continuous_state["route_flow_active"] = True
                self._continuous_state["route_key"] = route_key
            else:
                self._source_stage[route_key] = True
                self._atmosphere_safe[route_key] = True
                self._seal_safe[route_key] = False
            self._last_route_pressure_guard_summary = {
                "route_pressure_guard_status": "pass",
                "route_pressure_guard_reason": "",
                "analyzer_pressure_available": False,
                "analyzer_pressure_protection_active": False,
                "analyzer_pressure_status": "unavailable",
                "pace_syst_err_query": self.final_syst_err,
            }
            return True

    runner = _SortedValveRuntimeRunner()
    runner.mechanical_pressure_protection_confirmed = False
    runner.devices = {"pace": _FakePacePressureReady()}

    result = live_tool._run_co2_a_staged_source_final_release_dry_run(
        runner,
        tmp_path / "trace.csv",
        _args(_runtime_cfg=_config(gas_analyzer_enabled=True)),
    )

    assert result["status"] == "pass"
    assert result["precheck"]["current_open_valves_after_precheck"] == [7, 8, 11]
    assert "NoSourceFrontPathNotConfirmed" not in result["precheck"]["blocked_reasons"]
    assert result["precheck"]["source_stage_safe"] is True
    assert result["precheck"]["route_final_stage_atmosphere_safe"] is True


def test_co2_a_pressure_protection_resolver_accepts_valid_mechanical_approval_artifact(tmp_path: Path) -> None:
    approval_path = _write_pressure_protection_approval_json(tmp_path)

    resolved = live_tool.resolve_co2_a_staged_pressure_protection(
        _config(),
        approval_json_path=str(approval_path),
        route="CO2_A",
        source_final_valve=4,
        release_scope=live_tool.CO2_A_STAGED_RELEASE_SCOPE,
    )

    assert resolved["pressure_protection_source"] == "approval_artifact"
    assert resolved["pressure_protection_precheck_satisfied"] is True
    assert resolved["mechanical_pressure_protection_confirmed"] is True
    assert resolved["analyzer_pressure_protection_active"] is False


def test_co2_a_pressure_protection_resolver_accepts_valid_analyzer_approval_artifact(tmp_path: Path) -> None:
    approval_path = _write_pressure_protection_approval_json(
        tmp_path,
        analyzer_pressure_protection_active=True,
        mechanical_pressure_protection_confirmed=False,
    )

    resolved = live_tool.resolve_co2_a_staged_pressure_protection(
        _config(),
        approval_json_path=str(approval_path),
        route="CO2_A",
        source_final_valve=4,
        release_scope=live_tool.CO2_A_STAGED_RELEASE_SCOPE,
    )

    assert resolved["pressure_protection_source"] == "approval_artifact"
    assert resolved["pressure_protection_precheck_satisfied"] is True
    assert resolved["analyzer_pressure_protection_active"] is True
    assert resolved["mechanical_pressure_protection_confirmed"] is False


@pytest.mark.parametrize(
    ("route", "valve", "scope"),
    [
        ("CO2_B", 4, live_tool.CO2_A_STAGED_RELEASE_SCOPE),
        ("CO2_A", 24, live_tool.CO2_A_STAGED_RELEASE_SCOPE),
        ("CO2_A", 4, "full_v1_production"),
    ],
)
def test_co2_a_pressure_protection_resolver_rejects_wrong_scope_or_route(
    tmp_path: Path,
    route: str,
    valve: int,
    scope: str,
) -> None:
    approval_path = _write_pressure_protection_approval_json(tmp_path)

    resolved = live_tool.resolve_co2_a_staged_pressure_protection(
        _config(),
        approval_json_path=str(approval_path),
        route=route,
        source_final_valve=valve,
        release_scope=scope,
    )

    assert resolved["pressure_protection_precheck_satisfied"] is False
    assert "PressureProtectionScopeInvalid" in resolved["reasons"]


def test_co2_a_pressure_protection_resolver_accepts_explicit_config() -> None:
    resolved = live_tool.resolve_co2_a_staged_pressure_protection(
        _config(
            staged_pressure_protection={
                "route": "CO2_A",
                "source_final_valve_under_test": 4,
                "approval_scope": "CO2_A_VALVE_4_STAGED_DRY_RUN_ONLY",
                "release_scope": live_tool.CO2_A_STAGED_RELEASE_SCOPE,
                "retry_allowed_for_scope": True,
                "analyzer_pressure_protection_active": False,
                "mechanical_pressure_protection_confirmed": True,
                "not_full_v1_production_approval": True,
                "not_full_formal_approval": True,
                "does_not_open_4_24_10": True,
                "does_not_run_real_sealed_pressure_transition": True,
            }
        ),
        approval_json_path=None,
        route="CO2_A",
        source_final_valve=4,
        release_scope=live_tool.CO2_A_STAGED_RELEASE_SCOPE,
    )

    assert resolved["pressure_protection_source"] == "config"
    assert resolved["pressure_protection_precheck_satisfied"] is True
    assert resolved["mechanical_pressure_protection_confirmed"] is True


def test_co2_a_staged_dry_run_uses_resolver_before_candidate_apply(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required_staged_env(monkeypatch)
    runner = _FakeRunner()
    runner.mechanical_pressure_protection_confirmed = False

    result = live_tool._run_co2_a_staged_source_final_release_dry_run(
        runner,
        tmp_path / "trace.csv",
        _args(_runtime_cfg=_config(), pressure_protection_approval_json=None),
    )

    assert result["status"] == "diagnostic_error"
    assert result["abort_reason"] == "PressureProtectionApprovalMissing"
    assert result["pressure_protection_source"] == "missing"
    assert result["co2_4_opened"] is False
    assert all(call[0] not in {"verify", "candidate", "apply"} for call in runner.calls)


def test_co2_a_staged_dry_run_valid_approval_still_requires_operator_env_and_apply(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    approval_path = _write_pressure_protection_approval_json(tmp_path)

    def _runner_factory() -> _FakeRunner:
        runner = _FakeRunner()
        runner.mechanical_pressure_protection_confirmed = False
        return runner

    for name in (
        "ALLOW_STAGED_SOURCE_FINAL_DRY_RUN",
        "OPERATOR_INTENT_CONFIRMED",
        "RELEASE_REASON",
        "CONFIRM_NOT_FULL_PRODUCTION",
        "CONFIRM_NO_ROUTE_FLUSH_DEWPOINT_GATE",
        "CONFIRM_SINGLE_ROUTE_CO2_A_ONLY",
    ):
        monkeypatch.delenv(name, raising=False)

    exit_code, summary, runner = _run_main(
        tmp_path,
        monkeypatch,
        scenario=live_tool.CO2_A_STAGED_SOURCE_FINAL_RELEASE_DRY_RUN,
        config=_config(),
        runner_factory=_runner_factory,
        extra_args=["--pressure-protection-approval-json", str(approval_path)],
    )

    assert exit_code == 0
    assert summary["scenario_result"]["status"] == "skipped"
    assert summary["scenario_result"]["abort_reason"] == "operator_confirmation_missing"
    assert all(call[0] not in {"verify", "candidate", "apply"} for call in runner.calls)

    _set_required_staged_env(monkeypatch)
    exit_code, summary, runner = _run_main(
        tmp_path,
        monkeypatch,
        scenario=live_tool.CO2_A_STAGED_SOURCE_FINAL_RELEASE_DRY_RUN,
        config=_config(),
        runner_factory=_runner_factory,
        extra_args=["--pressure-protection-approval-json", str(approval_path)],
    )

    assert exit_code == 0
    assert summary["scenario_result"]["pressure_protection_source"] == "approval_artifact"
    assert summary["scenario_result"]["pressure_protection_precheck_satisfied"] is True
    assert summary["scenario_result"]["explicit_apply_succeeded"] is True
    assert summary["scenario_result"]["release_performed"] is False
    assert summary["scenario_result"]["route_final_stage_seal_safety_updated"] is False
    assert summary["scenario_result"]["dry_run_release_suppressed"] is True
    assert summary["scenario_result"]["dry_run_authorized_for_staged_source_final"] is True
    assert summary["scenario_result"]["dry_run_passed"] is True
    assert summary["scenario_result"]["co2_4_opened"] is True
    assert summary["scenario_result"]["co2_24_opened"] is False
    assert summary["scenario_result"]["h2o_10_opened"] is False
    assert summary["scenario_result"]["vent2_tx_observed"] is False
    assert runner._seal_safe["co2_a"] is False
    assert any(call[0] == "apply" for call in runner.calls)


def test_co2_a_staged_dry_run_valid_approval_does_not_flip_seal_safety(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    approval_path = _write_pressure_protection_approval_json(tmp_path)
    _set_required_staged_env(monkeypatch)

    def _runner_factory() -> _FakeRunner:
        runner = _FakeRunner()
        runner.mechanical_pressure_protection_confirmed = False
        return runner

    exit_code, summary, runner = _run_main(
        tmp_path,
        monkeypatch,
        scenario=live_tool.CO2_A_STAGED_SOURCE_FINAL_RELEASE_DRY_RUN,
        config=_config(),
        runner_factory=_runner_factory,
        extra_args=["--pressure-protection-approval-json", str(approval_path)],
    )

    assert exit_code == 0
    assert summary["scenario_result"]["status"] == "pass"
    assert summary["scenario_result"]["explicit_apply_succeeded"] is True
    assert summary["scenario_result"]["release_performed"] is False
    assert summary["scenario_result"]["route_final_stage_seal_safety_updated"] is False
    assert summary["scenario_result"]["dry_run_release_suppressed"] is True
    assert summary["scenario_result"]["dry_run_authorized_for_staged_source_final"] is True
    assert runner._seal_safe["co2_a"] is False


def test_co2_a_staged_dry_run_rejects_apply_result_that_releases_in_dry_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_required_staged_env(monkeypatch)
    runner = _FakeRunner()
    runner.apply_result = {
        "dry_run": True,
        "dry_run_release_suppressed": False,
        "dry_run_authorized_for_staged_source_final": True,
        "release_performed": True,
        "route_final_stage_seal_safety_updated": True,
    }

    result = live_tool._run_co2_a_staged_source_final_release_dry_run(
        runner,
        tmp_path / "trace.csv",
        _args(_runtime_cfg=_config()),
    )

    assert result["status"] == "diagnostic_error"
    assert result["abort_reason"] == "DryRunApplyMustNotReleaseSealSafety"
    assert result["release_performed"] is True
    assert result["route_final_stage_seal_safety_updated"] is True
    assert [8, 11, 7, 4] not in [call[1] for call in runner.calls if call[0] == "guard"]


def test_pressure_protection_resolver_does_not_release_seal_safety(tmp_path: Path) -> None:
    approval_path = _write_pressure_protection_approval_json(tmp_path)
    runner = _FakeRunner()
    before = dict(runner._seal_safe)

    resolved = live_tool.resolve_co2_a_staged_pressure_protection(
        _config(),
        approval_json_path=str(approval_path),
        route="CO2_A",
        source_final_valve=4,
        release_scope=live_tool.CO2_A_STAGED_RELEASE_SCOPE,
    )

    assert resolved["pressure_protection_precheck_satisfied"] is True
    assert runner._seal_safe == before


def test_pressure_protection_template_does_not_allow_retry() -> None:
    template_path = (
        live_tool.REPO_ROOT
        / "audit"
        / "pressure"
        / "protection_approval_template"
        / "co2_a_valve4_staged_dry_run_approval.template.json"
    )
    resolved = live_tool.resolve_co2_a_staged_pressure_protection(
        _config(),
        approval_json_path=str(template_path),
        route="CO2_A",
        source_final_valve=4,
        release_scope=live_tool.CO2_A_STAGED_RELEASE_SCOPE,
    )

    assert resolved["pressure_protection_precheck_satisfied"] is False
    assert "PressureProtectionScopeInvalid" in resolved["reasons"] or "PressureProtectionApprovalMissing" in resolved["reasons"]


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
