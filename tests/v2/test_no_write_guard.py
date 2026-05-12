from __future__ import annotations

import pytest

from gas_calibrator.v2.core.no_write_guard import (
    NoWriteGuard,
    NoWriteViolation,
)


class FakeAnalyzerForRuntimeSetup:
    def __init__(self) -> None:
        self._mode = ""
        self._comm_way = ""
        self._active_freq = 0
        self._average_filter = 0
        self._average_co2 = 0
        self._average_h2o = 0
        self.ser = _FakeAnalyzerSerial()

    def set_mode(self, mode: int) -> str:
        self._mode = f"mode_{mode}"
        return self._mode

    def set_mode_with_ack(self, mode: int) -> str:
        self._mode = f"mode_{mode}_ack"
        return self._mode

    def set_comm_way(self, active: bool) -> str:
        self._comm_way = f"comm_{active}"
        return self._comm_way

    def set_comm_way_with_ack(self, active: bool) -> str:
        self._comm_way = f"comm_{active}_ack"
        return self._comm_way

    def set_active_freq(self, hz: float) -> str:
        self._active_freq = hz
        return f"freq_{hz}"

    def set_active_freq_with_ack(self, hz: float) -> str:
        self._active_freq = hz
        return f"freq_{hz}_ack"

    def set_average_filter(self, count: int) -> str:
        self._average_filter = count
        return f"filter_{count}"

    def set_average_filter_with_ack(self, count: int) -> str:
        self._average_filter = count
        return f"filter_{count}_ack"

    def set_average_filter_channel(self, channel: int, count: int) -> str:
        return f"ch_{channel}_filter_{count}"

    def set_average_filter_channel_with_ack(self, channel: int, count: int) -> str:
        return f"ch_{channel}_filter_{count}_ack"

    def set_average(self, co2: int, h2o: int) -> str:
        self._average_co2 = co2
        self._average_h2o = h2o
        return f"avg_{co2}_{h2o}"

    def set_average_with_ack(self, co2: int, h2o: int) -> str:
        self._average_co2 = co2
        self._average_h2o = h2o
        return f"avg_{co2}_{h2o}_ack"

    def set_device_id_with_ack(self, device_id: str) -> str:
        return f"id_{device_id}"

    def set_device_id(self, device_id: str) -> str:
        return f"id_{device_id}"

    def write_device_id(self, device_id: str) -> str:
        return f"id_{device_id}"

    def assign_device_id(self, device_id: str) -> str:
        return f"id_{device_id}"

    def set_id(self, device_id: str) -> str:
        return f"id_{device_id}"

    def write(self, data: str) -> str:
        return f"wrote:{data}"

    def query(self, data: str) -> str:
        return f"query:{data}"


class _FakeAnalyzerSerial:
    def write(self, data: str) -> str:
        return f"serial:{data}"

    def query(self, data: str) -> str:
        return f"serial_query:{data}"


class FakeAnalyzerWithFailingSetup:
    def set_mode(self, mode: int) -> str:
        raise RuntimeError("simulated communication failure")

    def set_mode_with_ack(self, mode: int) -> str:
        raise RuntimeError("simulated communication failure")

    def write(self, data: str) -> str:
        return f"wrote:{data}"

    def query(self, data: str) -> str:
        return f"query:{data}"


# ---------------------------------------------------------------------------
# Test 1: identity write methods are still blocked
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "method_name",
    [
        "set_device_id_with_ack",
        "set_device_id",
        "write_device_id",
        "assign_device_id",
        "set_id",
    ],
)
def test_identity_write_methods_are_still_blocked(method_name: str) -> None:
    guard = NoWriteGuard()
    analyzer = guard.guard_device(
        FakeAnalyzerForRuntimeSetup(),
        device_name="ga01",
        device_type="gas_analyzer",
    )

    with pytest.raises(NoWriteViolation):
        getattr(analyzer, method_name)("029")

    assert guard.attempted_write_count == 1
    assert guard.blocked_events[0]["method_name"] == method_name
    assert guard.blocked_events[0]["identity_write_command_sent"] is True
    assert guard.blocked_events[0]["persistent_write_command_sent"] is True
    assert guard.to_artifact()["identity_write_command_sent"] is True
    assert guard.runtime_setup_command_count == 0


# ---------------------------------------------------------------------------
# Test 2: runtime setup methods are recorded, not blocked
# ---------------------------------------------------------------------------
def test_runtime_setup_methods_are_recorded_not_blocked() -> None:
    guard = NoWriteGuard()
    analyzer = guard.guard_device(
        FakeAnalyzerForRuntimeSetup(),
        device_name="ga01",
        device_type="gas_analyzer",
    )

    results = [
        analyzer.set_mode(2),
        analyzer.set_comm_way(True),
        analyzer.set_active_freq(10.0),
        analyzer.set_average_filter(16),
        analyzer.set_average(32, 32),
    ]

    assert results == [
        "mode_2",
        "comm_True",
        "freq_10.0",
        "filter_16",
        "avg_32_32",
    ]

    assert guard.runtime_setup_command_count == 5
    assert guard.attempted_write_count == 0
    assert guard.to_artifact()["final_decision"] == "PASS"
    assert guard.to_artifact()["identity_write_command_sent"] is False
    assert guard.to_artifact()["persistent_write_command_sent"] is False
    assert guard.to_artifact()["runtime_setup_command_sent"] is True

    for event in guard.runtime_setup_events:
        assert event["command_category"] == "analyzer_runtime_setup"
        assert event["calibration_write_command_sent"] is False
        assert event["identity_write_command_sent"] is False
        assert event["persistent_write_command_sent"] is False
        assert event["reason"] == "allowed_runtime_setup_under_no_write_guard"
        assert event["success"] is True
        assert event["error"] == ""


# ---------------------------------------------------------------------------
# Test 3: runtime setup exception is recorded and re-raised
# ---------------------------------------------------------------------------
def test_runtime_setup_exception_is_recorded_and_reraised() -> None:
    guard = NoWriteGuard()
    analyzer = guard.guard_device(
        FakeAnalyzerWithFailingSetup(),
        device_name="ga01",
        device_type="gas_analyzer",
    )

    with pytest.raises(RuntimeError, match="simulated communication failure"):
        analyzer.set_mode(2)

    assert guard.runtime_setup_command_count == 1
    assert guard.attempted_write_count == 0
    assert guard.to_artifact()["final_decision"] == "PASS"

    event = guard.runtime_setup_events[0]
    assert event["method_name"] == "set_mode"
    assert event["command_category"] == "analyzer_runtime_setup"
    assert event["success"] is False
    assert "simulated communication failure" in event["error"]


# ---------------------------------------------------------------------------
# Test 4: raw identity payload still blocked
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "payload",
    [
        "ID,YGAS,003,029\r\n",
        "ID,YGAS,FFF,029\r\n",
    ],
)
def test_raw_identity_payload_still_blocked(payload: str) -> None:
    guard = NoWriteGuard()
    analyzer = guard.guard_device(
        FakeAnalyzerForRuntimeSetup(),
        device_name="ga01",
        device_type="gas_analyzer",
    )

    with pytest.raises(NoWriteViolation):
        analyzer.write(payload)

    assert guard.attempted_write_count == 1
    assert guard.blocked_events[0]["method_name"] == "write"
    assert guard.blocked_events[0]["write_category"] == "persistent_identity_write"
    assert guard.blocked_events[0]["identity_write_command_sent"] is True
    assert guard.to_artifact()["identity_write_command_sent"] is True
    assert guard.runtime_setup_command_count == 0


# ---------------------------------------------------------------------------
# Test 5: raw calibration payload still blocked
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "payload",
    [
        "SENCO9,YGAS,FFF,0,1,0,0\r\n",
        "COEFFWRITE,YGAS,FFF,1,0,0,0,0,0",
        "WRITEZERO,YGAS,FFF",
        "WRITESPAN,YGAS,FFF,1000",
        "APPLYCAL,YGAS,FFF",
        "SAVE_PARAMETERS",
    ],
)
def test_raw_calibration_payload_still_blocked(payload: str) -> None:
    guard = NoWriteGuard()
    analyzer = guard.guard_device(
        FakeAnalyzerForRuntimeSetup(),
        device_name="ga01",
        device_type="gas_analyzer",
    )

    with pytest.raises(NoWriteViolation):
        analyzer.write(payload)

    assert guard.attempted_write_count == 1
    assert guard.blocked_events[0]["method_name"] == "write"
    assert guard.blocked_events[0]["write_category"] == "calibration_or_parameter_write"
    assert guard.to_artifact()["identity_write_command_sent"] is False
    assert guard.runtime_setup_command_count == 0
