from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from gas_calibrator.v2.core.models import CalibrationPoint
from gas_calibrator.v2.core.services.valve_routing_service import ValveRoutingService


class FakeRelay:
    def __init__(self, *, fail_channels: set[int] | None = None) -> None:
        self.fail_channels = fail_channels or set()
        self.states: dict[int, bool] = {}
        self.calls: list[tuple[int, bool]] = []

    def set_valve(self, channel: int, state: bool) -> bool:
        self.calls.append((channel, state))
        if channel not in self.fail_channels:
            self.states[int(channel)] = bool(state)
        return True


def _point() -> CalibrationPoint:
    return CalibrationPoint(index=1, temperature_c=20.0, humidity_pct=50.0, pressure_hpa=1100.0, route="h2o")


class Host:
    def __init__(self, relay_b: FakeRelay, *, h2o_path: int = 8, hold: int = 9) -> None:
        self.relay_b = relay_b
        self.traces: list[dict[str, Any]] = []
        self.cfg = {
            "valves": {
                "h2o_path": h2o_path,
                "hold": hold,
                "relay_map": {
                    "8": {"device": "relay_8", "channel": 8},
                    "9": {"device": "relay_8", "channel": 1},
                },
            }
        }

    def _cfg_get(self, path: str, default=None):
        current: Any = self.cfg
        for part in path.split("."):
            if not isinstance(current, dict) or part not in current:
                return default
            current = current[part]
        return current

    def _as_int(self, value):
        try:
            return int(value)
        except Exception:
            return None

    def _device(self, name: str):
        return self.relay_b if name == "relay_b" else None

    def _call_first(self, device, names, *args):
        for name in names:
            fn = getattr(device, name, None)
            if callable(fn):
                return fn(*args)
        return None

    def _log(self, message: str) -> None:
        return None


def _service(relay_b: FakeRelay) -> tuple[ValveRoutingService, Host]:
    host = Host(relay_b)
    service = ValveRoutingService(SimpleNamespace(), SimpleNamespace(), host=host)
    service._record_route_trace = lambda **kwargs: host.traces.append(kwargs)
    return service, host


def test_set_h2o_path_open_returns_ok_when_relay_command_and_physical_match() -> None:
    service, host = _service(FakeRelay())

    assert service.set_h2o_path(True, _point()) is True

    evidence = service.last_h2o_path_evidence
    assert evidence["relay_command_sent"] is True
    assert evidence["relay_command_result"] == "sent"
    assert evidence["h2o_path_return_value"] is True
    assert evidence["route_physical_state_match"] is True
    assert evidence["h2o_path_open_verified"] is True
    assert host.traces[-1]["result"] == "ok"


def test_set_h2o_path_open_records_failure_reason_when_return_false() -> None:
    service, host = _service(FakeRelay(fail_channels={8}))

    assert service.set_h2o_path(True, _point()) is False

    evidence = service.last_h2o_path_evidence
    assert evidence["relay_command_sent"] is True
    assert evidence["relay_command_result"] == "sent"
    assert evidence["h2o_path_return_value"] is False
    assert evidence["route_physical_state_match"] is False
    assert evidence["relay_physical_mismatch"] is True
    assert evidence["mismatched_channels"] == [{"logical_valve": 8, "relay": "relay_b", "channel": 8, "target": True, "actual": False}]
    assert evidence["h2o_path_open_failure_reason"] == "relay_physical_mismatch"
    assert host.traces[-1]["result"] == "fail"


def test_set_h2o_path_open_does_not_treat_none_as_ok_without_evidence() -> None:
    relay = FakeRelay()
    host = Host(relay, h2o_path=0, hold=0)
    service = ValveRoutingService(SimpleNamespace(), SimpleNamespace(), host=host)
    service._record_route_trace = lambda **kwargs: host.traces.append(kwargs)

    assert service.set_h2o_path(True, _point()) is False

    evidence = service.last_h2o_path_evidence
    assert evidence["relay_command_sent"] is False
    assert evidence["relay_command_result"] == "not_sent"
    assert evidence["h2o_path_open_verified"] is False
    assert host.traces[-1]["result"] == "fail"


class CoilListRelay:
    def __init__(self, *, fail_channels: set[int] | None = None) -> None:
        self.fail_channels = fail_channels or set()
        self._states: list[bool] = [False] * 32
        self.calls: list[tuple[int, bool]] = []

    def set_valve(self, channel: int, state: bool) -> bool:
        self.calls.append((channel, state))
        if channel not in self.fail_channels:
            self._states[int(channel) - 1] = bool(state)
        return True

    def read_coils(self, start: int, count: int = 1):
        end = start + count
        return list(self._states[start:end])


def _service_coil_list(relay_b: CoilListRelay) -> tuple[ValveRoutingService, Host]:
    host = Host(FakeRelay(), h2o_path=8, hold=9)
    host.relay_b = relay_b
    host.cfg["valves"]["relay_map"] = {
        "8": {"device": "relay_8", "channel": 8},
        "9": {"device": "relay_8", "channel": 1},
        "10": {"device": "relay_8", "channel": 2},
    }
    service = ValveRoutingService(SimpleNamespace(), SimpleNamespace(), host=host)
    service._record_route_trace = lambda **kwargs: host.traces.append(kwargs)
    return service, host


def test_set_h2o_path_open_ok_when_read_coils_returns_true_list() -> None:
    relay = CoilListRelay()
    service, host = _service_coil_list(relay)

    assert service.set_h2o_path(True, _point()) is True

    evidence = service.last_h2o_path_evidence
    assert evidence["relay_command_sent"] is True
    assert evidence["relay_command_result"] == "sent"
    assert evidence["h2o_path_return_value"] is True
    assert evidence["route_physical_state_match"] is True
    assert evidence["relay_physical_mismatch"] is False
    assert evidence["mismatched_channels"] == []
    assert evidence["h2o_path_open_verified"] is True
    assert host.traces[-1]["result"] == "ok"
    assert host.traces[-1]["message"] == "H2O route path set"


def test_set_h2o_path_open_fail_when_read_coils_returns_false_list() -> None:
    relay = CoilListRelay(fail_channels={8})
    service, host = _service_coil_list(relay)

    assert service.set_h2o_path(True, _point()) is False

    evidence = service.last_h2o_path_evidence
    assert evidence["relay_command_sent"] is True
    assert evidence["relay_command_result"] == "sent"
    assert evidence["h2o_path_return_value"] is False
    assert evidence["route_physical_state_match"] is False
    assert evidence["relay_physical_mismatch"] is True
    assert len(evidence["mismatched_channels"]) >= 1
    assert evidence["h2o_path_open_failure_reason"] == "relay_physical_mismatch"
    assert host.traces[-1]["result"] == "fail"
