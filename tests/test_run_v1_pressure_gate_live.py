from argparse import Namespace
from pathlib import Path

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

    def _drain_pace_system_errors(self, reason: str = ""):
        self.calls.append(("drain", reason))
        return []

    def _read_pace_system_error_text(self):
        return '0,"No error"'

    def _clear_last_sealed_pressure_route_context(self, reason: str = "") -> None:
        self.calls.append(("clear_last", reason))

    def _clear_pressure_sequence_context(self, reason: str = "") -> None:
        self.calls.append(("clear_sequence", reason))

    def _set_co2_route_baseline(self, reason: str = "") -> None:
        self.calls.append(("baseline", reason))

    def _co2_open_valves(self, point, include_total_valve: bool, *, include_source_valve: bool = True):
        if str(getattr(point, "co2_group", "") or "").upper() == "B":
            return [8, 11, 16, 24] if include_source_valve else [8, 11, 16]
        return [8, 11, 7, 4] if include_source_valve else [8, 11, 7]

    def _open_route_with_pressure_guard(self, point, **kwargs):
        self.calls.append(("guard", list(kwargs.get("open_valves") or [])))
        return True

    def _open_co2_route_for_conditioning(self, point, *, point_tag: str = ""):
        self.calls.append(("conditioning", point_tag))
        return True

    def _point_runtime_state(self, point, *, phase: str):
        return {}


def _args(**overrides):
    base = {
        "target_pressure_hpa": 1000.0,
        "co2_ppm": 600.0,
        "allow_source_open": False,
    }
    base.update(overrides)
    return Namespace(**base)


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
