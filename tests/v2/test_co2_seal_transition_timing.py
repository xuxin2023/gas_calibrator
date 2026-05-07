from __future__ import annotations

import time
from types import SimpleNamespace

import pytest

from gas_calibrator.v2.core.services.pressure_control_service import (
    PressureControlService,
    PressureWaitResult,
)
from gas_calibrator.v2.core.models import CalibrationPoint


def _point(index=1, ppm=1000.0, pressure=800.0, route="co2"):
    return CalibrationPoint(
        index=index,
        temperature_c=20.0,
        co2_ppm=ppm,
        pressure_hpa=pressure,
        pressure_mode="sealed_controlled",
        pressure_selection_token="800hPa",
        route=route,
    )


def _make_host(overrides=None):
    overrides = dict(overrides or {})
    route_trace_records = []

    def cfg_get(path, default=None):
        return dict(overrides).get(path, default)

    host = SimpleNamespace(
        _cfg_get=cfg_get,
        _as_float=lambda v: None if v in (None, "") else float(v),
        _log=lambda msg: None,
        _check_stop=lambda: None,
        a2_hooks=SimpleNamespace(
            high_pressure_first_point_mode_enabled=False,
            co2_route_conditioning_completed=False,
            co2_route_conditioning_completed_at="",
            seal_allowed=False,
            co2_route_conditioning_at_atmosphere_context={},
        ),
        run_state=SimpleNamespace(humidity=SimpleNamespace(active_post_h2o_co2_zero_flush=False)),
        _capture_preseal_dewpoint_snapshot=lambda: None,
        _make_pressure_reader=lambda: None,
        _device=lambda name: None,
        _record_route_trace_records=route_trace_records,
    )

    def _set_pressure_controller_vent(vent_on, reason="", prefer_direct_command=False, **kw):
        return {"vent_on": vent_on, "reason": reason, "command_result": "ok"}

    host._set_pressure_controller_vent = _set_pressure_controller_vent

    apply_calls = []

    def _apply_valve_states(open_valves):
        apply_calls.append(list(open_valves))
        return {"relay_a": {"1": False, "2": False}, "relay_b": {"1": False, "2": False}}

    host._apply_valve_states = _apply_valve_states
    host.apply_calls = apply_calls

    return host, route_trace_records


def _make_service(host):
    service = PressureControlService(
        SimpleNamespace(),
        SimpleNamespace(humidity=SimpleNamespace(active_post_h2o_co2_zero_flush=False)),
        host=host,
    )
    service._make_preseal_observation_reader = lambda: None
    service._ambient_reference_payload = lambda: {}
    service._positive_preseal_enabled = lambda *a, **kw: False
    service._seal_transition_evidence = lambda *, route, relay_state: {
        "route": route,
        "seal_transition_completed": True,
        "seal_transition_status": "verified_closed",
        "seal_transition_reason": "all route valves closed",
        "seal_open_channels": [],
        "seal_relay_state": relay_state or {},
    }
    service._coerce_bool = lambda v: bool(v) if v is not None else None
    service._coerce_float = lambda v: None if v in (None, "") else float(v)
    service._record_route_trace = lambda **kw: host._record_route_trace_records.append(kw)
    service._mark_seal_transition = lambda d: None
    service._mark_preseal_final_atmosphere_exit = lambda d: None
    service._preseal_watchlist_snapshot = lambda *a, **kw: {
        "pressure_controller_vent_status": 0,
        "preseal_watchlist_status_seen": False,
        "preseal_watchlist_status_accepted": False,
        "preseal_watchlist_status_reason": "",
    }
    service._pressure_controller_is_simulated = lambda c: True
    service._pressure_controller_vent_status = lambda c: 0
    service._pressure_controller_fast_state_hint = lambda c: {}
    service._pressure_controller_state_snapshot = lambda c: {}
    service._pressure_controller_atmosphere_evidence = lambda *a, **kw: {}
    service._read_pressure_with_recovery = lambda: None
    service._read_pressure_sample = lambda r, **kw: {}
    service._preseal_capture_predictive_seal_latency_s = lambda: 0.0
    service._preseal_final_atmosphere_exit_gate = lambda *a, **kw: PressureWaitResult(
        ok=True, diagnostics={"preseal_final_atmosphere_exit_verified": True}
    )
    service._seal_transition_gate = lambda *a, **kw: PressureWaitResult(
        ok=True, diagnostics={"seal_transition_completed": True}
    )
    return service


class TestSealTransitionEvidenceRecorded:
    def test_vent_off_to_route_close_timing_recorded_for_co2(self):
        point = _point(pressure=800.0)
        host, traces = _make_host({
            "workflow.pressure.pressurize_wait_after_vent_off_s": 0.05,
            "workflow.pressure.co2_vent_off_to_route_close_max_s": 1.5,
        })
        service = _make_service(host)

        result = service.pressurize_and_hold(point, route="co2")
        assert result.ok

        evidence_records = [t for t in traces if t.get("action") == "co2_seal_transition_evidence"]
        assert len(evidence_records) >= 1
        ev = evidence_records[-1]["actual"]

        assert "co2_seal_transition_started_at" in ev
        assert ev["co2_seal_transition_started_at"]
        assert "vent_off_command_sent_at" in ev
        assert ev["vent_off_command_sent_at"]
        assert "co2_route_valve_close_command_sent_at" in ev
        assert ev["co2_route_valve_close_command_sent_at"]
        assert "co2_route_valve_close_confirmed_at" in ev
        assert ev["co2_route_valve_close_confirmed_at"]
        assert "vent_off_to_route_close_s" in ev
        assert ev["vent_off_to_route_close_s"] is not None
        assert ev["vent_off_to_route_close_s"] >= 0
        assert "vent_off_to_route_close_limit_s" in ev
        assert ev["vent_off_to_route_close_limit_s"] > 0
        assert "positive_preseal_peak_hpa" in ev
        assert "positive_preseal_peak_after_vent_off_s" in ev
        assert "pressure_read_between_vent_off_and_route_close" in ev
        assert "pressure_read_blocked_route_close" in ev
        assert ev["pressure_read_blocked_route_close"] is False

    def test_evidence_recorded_when_wait_after_vent_off_zero(self):
        point = _point(pressure=800.0)
        host, traces = _make_host({
            "workflow.pressure.pressurize_wait_after_vent_off_s": 0.0,
        })
        service = _make_service(host)

        result = service.pressurize_and_hold(point, route="co2")
        assert result.ok

        evidence_records = [t for t in traces if t.get("action") == "co2_seal_transition_evidence"]
        assert len(evidence_records) >= 1
        ev = evidence_records[-1]["actual"]
        assert ev["vent_off_to_route_close_s"] is not None
        assert ev["vent_off_to_route_close_s"] >= 0


class TestH2oNotAffected:
    def test_h2o_route_does_not_record_co2_evidence(self):
        point = _point(pressure=800.0, route="h2o")
        host, traces = _make_host({
            "workflow.pressure.pressurize_wait_after_vent_off_s": 0.05,
        })
        service = _make_service(host)

        result = service.pressurize_and_hold(point, route="h2o")
        assert result.ok

        evidence_records = [t for t in traces if t.get("action") == "co2_seal_transition_evidence"]
        assert len(evidence_records) == 0

    def test_h2o_route_still_sends_vent_off(self):
        point = _point(pressure=800.0, route="h2o")
        host, traces = _make_host({
            "workflow.pressure.pressurize_wait_after_vent_off_s": 0.05,
        })
        service = _make_service(host)

        result = service.pressurize_and_hold(point, route="h2o")
        assert result.ok


class TestPressurePeakRecorded:
    def test_pressure_peak_is_none_without_positive_preseal(self):
        point = _point(pressure=800.0)
        host, traces = _make_host({
            "workflow.pressure.pressurize_wait_after_vent_off_s": 0.05,
        })
        service = _make_service(host)

        result = service.pressurize_and_hold(point, route="co2")
        assert result.ok

        evidence_records = [t for t in traces if t.get("action") == "co2_seal_transition_evidence"]
        ev = evidence_records[-1]["actual"]
        assert ev["positive_preseal_peak_hpa"] is None
        assert ev["positive_preseal_peak_after_vent_off_s"] is None


class TestSmokeConfigLoadsCorrectly:
    def test_ambient_800_smoke_config_has_correct_preseal_settings(self):
        import json
        from pathlib import Path

        config_path = Path(__file__).resolve().parents[2] / "src" / "gas_calibrator" / "v2" / "configs" / "validation" / "run001_a2_co2_1000ppm_ambient_800hpa_smoke.json"
        with open(config_path, "r", encoding="utf-8") as fh:
            data = json.load(fh)

        pressure_cfg = data.get("workflow", {}).get("pressure", {})

        assert pressure_cfg.get("high_pressure_first_point_mode_enabled") is False, (
            "ambient+800 smoke MUST set high_pressure_first_point_mode_enabled=false"
        )
        assert pressure_cfg.get("positive_preseal_pressurization_enabled") is False, (
            "ambient+800 smoke MUST set positive_preseal_pressurization_enabled=false"
        )

    def test_ambient_800_smoke_config_has_all_no_write_guards(self):
        import json
        from pathlib import Path

        config_path = Path(__file__).resolve().parents[2] / "src" / "gas_calibrator" / "v2" / "configs" / "validation" / "run001_a2_co2_1000ppm_ambient_800hpa_smoke.json"
        with open(config_path, "r", encoding="utf-8") as fh:
            data = json.load(fh)

        run001 = data.get("run001_a2", {})

        assert run001.get("no_write") is True
        assert run001.get("allow_write_coefficients") is False
        assert run001.get("allow_write_zero") is False
        assert run001.get("allow_write_span") is False
        assert run001.get("allow_write_calibration_parameters") is False
        assert run001.get("default_cutover_to_v2") is False
        assert run001.get("disable_v1") is False

        a2_probe = data.get("a2_co2_7_pressure_no_write_probe", {})

        assert a2_probe.get("co2_only") is True
        assert a2_probe.get("no_write") is True
        assert a2_probe.get("skip0") is True
        assert a2_probe.get("single_route") is True
        assert a2_probe.get("single_temperature") is True

    def test_ambient_800_smoke_config_authorized_pressure_points(self):
        import json
        from pathlib import Path

        config_path = Path(__file__).resolve().parents[2] / "src" / "gas_calibrator" / "v2" / "configs" / "validation" / "run001_a2_co2_1000ppm_ambient_800hpa_smoke.json"
        with open(config_path, "r", encoding="utf-8") as fh:
            data = json.load(fh)

        run001 = data.get("run001_a2", {})
        authorized = run001.get("authorized_pressure_points_hpa", [])

        assert "ambient_open" in authorized
        assert 800 in authorized
        assert len(authorized) == 2
