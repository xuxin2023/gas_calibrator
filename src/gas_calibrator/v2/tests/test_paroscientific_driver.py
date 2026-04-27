from __future__ import annotations

from gas_calibrator.devices.paroscientific import ParoscientificGauge
from gas_calibrator.devices.serial_base import ReplaySerial


def test_paroscientific_p3_command_preview() -> None:
    gauge = ParoscientificGauge("COM30", baudrate=9600, timeout=1.0, dest_id="01")
    assert gauge._cmd("P3") == "*0100P3\r\n"


def test_paroscientific_read_pressure_uses_p3_query() -> None:
    def factory(**kwargs):
        return ReplaySerial(
            port=kwargs["port"],
            baudrate=kwargs["baudrate"],
            timeout=kwargs["timeout"],
            script=[
                {
                    "expect": b"*0100P3\r\n",
                    "responses": ["*01001012.345"],
                }
            ],
        )

    gauge = ParoscientificGauge("COM30", baudrate=9600, timeout=0.01, dest_id="01", serial_factory=factory)
    try:
        gauge.open()
        assert gauge.read_pressure(response_timeout_s=0.02, retries=1, retry_sleep_s=0.0) == 1012.345
    finally:
        gauge.close()


def test_paroscientific_fast_read_prefers_buffered_frame() -> None:
    def factory(**kwargs):
        return ReplaySerial(
            port=kwargs["port"],
            baudrate=kwargs["baudrate"],
            timeout=kwargs["timeout"],
            read_lines=["*01001013.750"],
        )

    gauge = ParoscientificGauge("COM30", baudrate=9600, timeout=0.01, dest_id="01", serial_factory=factory)
    try:
        gauge.open()
        assert gauge.read_pressure_fast(response_timeout_s=0.02, retries=1, buffered_drain_s=0.01) == 1013.75
    finally:
        gauge.close()
