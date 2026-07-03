from __future__ import annotations

import csv
from pathlib import Path

import pytest

from gas_calibrator.tools.run_v1_5_sealed_pressure_tune_900 import (
    QUERY_LATENCY_FIELDS,
    DEFAULT_TRIALS,
    TrialParams,
    V15SealedPressureTune900,
)


def _cfg(tmp_path: Path, *, collect_only: bool = True) -> dict:
    return {
        "paths": {"output_dir": str(tmp_path)},
        "workflow": {
            "collect_only": collect_only,
            "production": False,
            "controlled_write": False,
        },
        "coefficients": {"enabled": False, "sencos": {}},
        "devices": {
            "pressure_controller": {"enabled": True, "port": "COM23"},
            "pressure_gauge": {"enabled": True, "port": "COM22"},
            "humidity_generator": {"enabled": True, "port": "COM-HGEN"},
            "relay": {"enabled": True, "port": "COM-R1"},
            "relay_8": {"enabled": True, "port": "COM-R2"},
        },
        "valves": {
            "co2_path": 7,
            "gas_main": 11,
            "h2o_path": 8,
            "hold": 9,
            "flow_switch": 10,
            "co2_map": {"0": 1, "1000": 6},
            "co2_map_group2": {},
            "relay_map": {
                "1": {"device": "relay", "channel": 7},
                "6": {"device": "relay", "channel": 12},
                "7": {"device": "relay", "channel": 15},
                "8": {"device": "relay_8", "channel": 8},
                "9": {"device": "relay_8", "channel": 1},
                "10": {"device": "relay_8", "channel": 2},
                "11": {"device": "relay_8", "channel": 3},
            },
        },
    }


class FakePace:
    def __init__(
        self,
        *,
        pressures: list[float] | None = None,
        efforts: list[float] | None = None,
        vent_statuses: list[int] | None = None,
    ) -> None:
        self.pressures = list(pressures or [1007.0, 900.5, 900.4, 900.3, 900.2])
        self.efforts = list(efforts or [-0.1] * 20)
        self.vent_statuses = list(vent_statuses or [])
        self.calls: list[tuple] = []
        self.output_state = 0
        self.isolation_state = 1
        self.vent_status = 2
        self.setpoint = None

    def _next_pressure(self) -> float:
        if self.pressures:
            return float(self.pressures.pop(0))
        return 900.4

    def query(self, command: str) -> str:
        self.calls.append(("query", command))
        cmd = command.upper()
        if "SYST:ERR" in cmd:
            return "0,No error"
        if "EFF" in cmd:
            return str(self.efforts.pop(0) if self.efforts else -0.1)
        if "VENT?" in cmd:
            return str(self.get_vent_status())
        if "OUTP:STAT" in cmd:
            return str(self.output_state)
        if "OUTP:ISOL" in cmd:
            return str(self.isolation_state)
        if "PRES" in cmd:
            return str(self._next_pressure())
        return "0"

    def read_pressure(self) -> float:
        self.calls.append(("read_pressure",))
        return self._next_pressure()

    def set_setpoint(self, value: float) -> None:
        self.calls.append(("setpoint", float(value)))
        self.setpoint = float(value)

    def set_slew_mode_linear(self) -> None:
        self.calls.append(("set_slew_mode_linear",))

    def set_slew_rate(self, value: float) -> None:
        self.calls.append(("set_slew_rate", float(value)))

    def set_overshoot_allowed(self, enabled: bool) -> None:
        self.calls.append(("set_overshoot_allowed", bool(enabled)))

    def enable_control_output(self) -> None:
        self.calls.append(("enable_control_output",))
        self.output_state = 1

    def set_output(self, on: bool) -> None:
        self.calls.append(("set_output", bool(on)))
        self.output_state = 1 if on else 0

    def set_isolation_open(self, is_open: bool) -> None:
        self.calls.append(("set_isolation_open", bool(is_open)))
        self.isolation_state = 1 if is_open else 0

    def enter_atmosphere_mode(self) -> None:
        self.calls.append(("enter_atmosphere_mode",))
        self.output_state = 0
        self.isolation_state = 1
        self.vent_status = 2

    def get_vent_status(self) -> int:
        self.calls.append(("get_vent_status",))
        if self.vent_statuses:
            self.vent_status = int(self.vent_statuses.pop(0))
        return self.vent_status

    def get_output_state(self) -> int:
        return self.output_state

    def get_isolation_state(self) -> int:
        return self.isolation_state


class FakeGauge:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.calls = 0

    def read_pressure(self) -> float:
        self.calls += 1
        if self.fail:
            raise RuntimeError("COM22 should not block trigger")
        return 1007.0


class FakeRelay:
    def __init__(self) -> None:
        self.updates: list = []

    def set_valves_bulk(self, updates) -> None:
        self.updates.append(list(updates))

    def set_valve(self, channel: int, state: bool) -> None:
        self.updates.append([(channel, state)])


def _harness(tmp_path: Path, pace: FakePace | None = None, gauge: FakeGauge | None = None) -> V15SealedPressureTune900:
    return V15SealedPressureTune900(
        _cfg(tmp_path),
        devices={
            "pace": pace or FakePace(),
            "pressure_gauge": gauge or FakeGauge(),
            "relay": FakeRelay(),
            "relay_8": FakeRelay(),
        },
        output_dir=tmp_path / "run",
        max_trials=6,
        no_write=True,
        confirm_pressure_only_tuning=True,
    )


def _params(**overrides) -> TrialParams:
    data = {
        "trial_id": 1,
        "slew_rate_hpa_per_s": 3.0,
        "approach_slow_zone_hpa": 20.0,
        "slow_slew_rate_hpa_per_s": 0.5,
        "fast_monitor_interval_s": 0.0,
        "upper_window_hpa": 1.0,
        "burst_rows": 3,
        "burst_interval_s": 0.0,
        "max_monitor_s": 1.0,
    }
    data.update(overrides)
    return TrialParams(**data)


def test_900_tuning_harness_never_opens_gas_or_hgen(tmp_path: Path) -> None:
    harness = _harness(tmp_path)
    assert "humidity_generator" not in harness.devices
    row = harness.run_trial(_params())
    assert row["candidate_detected"] is True
    relay_updates = harness.devices["relay"].updates + harness.devices["relay_8"].updates
    assert relay_updates
    assert all(state is False for updates in relay_updates for _channel, state in updates)


def test_900_tuning_requires_collect_only_no_write(tmp_path: Path) -> None:
    harness = V15SealedPressureTune900(
        _cfg(tmp_path, collect_only=False),
        devices={"pace": FakePace(), "relay": FakeRelay()},
        output_dir=tmp_path / "run",
        confirm_pressure_only_tuning=True,
    )
    assert any("collect_only" in issue for issue in harness.preflight())


def test_900_fast_monitor_uses_pace_not_blocking_com22(tmp_path: Path) -> None:
    pace = FakePace(pressures=[1007.0, 900.5, 900.4, 900.3])
    harness = _harness(tmp_path, pace=pace, gauge=FakeGauge(fail=True))
    harness.selected_pressure_query = ":SENS:PRES:CONT?"
    row = harness.run_trial(_params())
    assert row["candidate_detected"] is True
    assert row["pressure_source_used_for_trigger"] == "PACE::SENS:PRES:CONT?"


def test_900_fast_candidate_triggers_short_burst(tmp_path: Path) -> None:
    harness = _harness(tmp_path, pace=FakePace(pressures=[1007.0, 900.5, 900.4, 900.3, 900.2]))
    row = harness.run_trial(_params(burst_rows=3))
    assert row["sample_rows"] == 3
    assert len(harness.sample_rows) == 3


def test_900_crossing_before_sample_invalidates(tmp_path: Path) -> None:
    harness = _harness(tmp_path, pace=FakePace(pressures=[1007.0, 899.9]))
    row = harness.run_trial(_params())
    assert row["candidate_detected"] is False
    assert row["final_decision"] == "FAIL_CLOSED_TARGET_CROSSING_BEFORE_SAMPLE"
    assert row["sample_rows"] == 0


def test_900_positive_effort_invalidates(tmp_path: Path) -> None:
    pace = FakePace(
        pressures=[1007.0, 900.5, 900.4, 900.3],
        efforts=[-0.1, 1.0, -0.1, -0.1],
    )
    harness = _harness(tmp_path, pace=pace)
    row = harness.run_trial(_params(burst_rows=1))
    assert row["candidate_detected"] is True
    assert row["sample_invalidated_reason"] == "positive_effort"
    assert harness.sample_rows[0]["sample_invalidated_by_positive_effort"] is True


def test_900_vent_in_active_sealed_fails(tmp_path: Path) -> None:
    pace = FakePace(pressures=[1007.0, 900.5], vent_statuses=[1])
    harness = _harness(tmp_path, pace=pace)
    row = harness.run_trial(_params())
    assert row["final_decision"] == "FAIL_CLOSED_VENT_STATUS_ACTIVE_OR_TRAPPED"


def test_900_cleanup_marks_line_contaminated(tmp_path: Path) -> None:
    harness = _harness(tmp_path)
    harness.run_trial(_params())
    assert harness.line_contaminated is True
    assert harness.rerun_requires_full_open_flow_flush is True


def test_query_latency_audit_outputs_required_fields(tmp_path: Path) -> None:
    harness = _harness(tmp_path, pace=FakePace(pressures=[1007.0] * 20))
    selected = harness.audit_query_latency(repeats=1)
    assert selected in {":SENS:PRES:CONT?", ":SENS:PRES:INL?", "read_pressure"}
    with harness.query_latency_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames == QUERY_LATENCY_FIELDS
        rows = list(reader)
    assert rows
    assert any(row["selected_for_fast_monitor"] == "True" for row in rows)


def test_tuning_loop_is_bounded_max_trials(tmp_path: Path) -> None:
    harness = V15SealedPressureTune900(
        _cfg(tmp_path),
        devices={"pace": FakePace(), "relay": FakeRelay()},
        output_dir=tmp_path / "run",
        max_trials=99,
        confirm_pressure_only_tuning=True,
        trials=list(DEFAULT_TRIALS) * 3,
    )
    assert harness.max_trials == 8
    assert len(harness.trials) == 8


def test_no_forbidden_calibration_writes(tmp_path: Path) -> None:
    harness = _harness(tmp_path)
    harness.run_trial(_params())
    counts = harness._count_io_commands()
    assert counts["forbidden_calibration_writes"] == 0
