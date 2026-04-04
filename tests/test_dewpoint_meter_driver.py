from gas_calibrator.devices import dewpoint_meter
from gas_calibrator.devices.serial_base import ReplaySerial


def test_dewpoint_meter_supports_replay_read_and_status() -> None:
    replay = ReplaySerial(
        on_write=lambda data, transport: transport.queue_buffer(
            "001_GetCurData_1.20_23.40_0_0_0_0_0_45.60_TRUE_FALSE_TRUE_FALSE_END"
        )
    )
    dev = dewpoint_meter.DewpointMeter("COM1", station="001", serial_factory=lambda **_: replay)

    dev.open()
    row = dev.read()
    status = dev.status()
    dev.close()

    assert row["ok"] is True
    assert row["dewpoint_c"] == 1.2
    assert row["temp_c"] == 23.4
    assert row["rh_pct"] == 45.6
    assert status["station"] == "001"
    assert status["dewpoint_c"] == 1.2


def test_dewpoint_meter_returns_not_ok_when_no_frame_arrives() -> None:
    replay = ReplaySerial(on_write=lambda data, transport: transport.queue_line("noise_only"))
    dev = dewpoint_meter.DewpointMeter("COM1", station="001", serial_factory=lambda **_: replay)

    dev.open()
    row = dev.get_current(timeout_s=0.05, attempts=1)
    dev.close()

    assert row["ok"] is False
    assert row["raw"] == ""
    assert row["lines"]
    assert all(item == "noise_only" for item in row["lines"])


def test_dewpoint_meter_fast_read_uses_single_variant_and_single_attempt(monkeypatch) -> None:
    class FakeSerialDevice:
        def __init__(self, *args, **kwargs):
            self.calls = []

        def open(self):
            return None

        def close(self):
            return None

        def exchange_readlines(
            self,
            data: str,
            *,
            response_timeout_s: float,
            read_timeout_s: float = 0.1,
            clear_input: bool = False,
        ):
            self.calls.append(
                {
                    "data": data,
                    "response_timeout_s": response_timeout_s,
                    "read_timeout_s": read_timeout_s,
                    "clear_input": clear_input,
                }
            )
            return ["001_GetCurData_1.20_23.40_0_0_0_0_0_45.60_TRUE_FALSE_TRUE_FALSE_END"]

    monkeypatch.setattr(dewpoint_meter, "SerialDevice", FakeSerialDevice)
    dev = dewpoint_meter.DewpointMeter("COM1", station="001")

    row = dev.get_current_fast(timeout_s=0.35)

    assert row["ok"] is True
    assert row["dewpoint_c"] == 1.2
    assert row["temp_c"] == 23.4
    assert row["rh_pct"] == 45.6
    assert dev.ser.calls == [
        {
            "data": "001_GetCurData_END\r\n",
            "response_timeout_s": 0.35,
            "read_timeout_s": 0.05,
            "clear_input": False,
        }
    ]
