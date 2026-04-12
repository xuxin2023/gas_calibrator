import pytest

from gas_calibrator.devices import humidity_generator
from gas_calibrator.devices.serial_base import ReplaySerial


def test_humidity_generator_supports_replay_fetch_and_status() -> None:
    def _on_write(data: bytes, transport: ReplaySerial) -> None:
        text = data.decode("ascii", errors="ignore").strip().upper()
        if text == "FETC? (@ALL)":
            transport.queue_line("Uw= 26.4,Tc= 22.052,Td= 1.90,Flux= 5.3")

    replay = ReplaySerial(on_write=_on_write)
    dev = humidity_generator.HumidityGenerator("COM1", serial_factory=lambda **_: replay)

    dev.open()
    row = dev.fetch_all()
    status = dev.status()
    dev.close()

    assert row["data"]["Fl"] == 5.3
    assert row["data"]["Td"] == 1.9
    assert status["ok"] is True
    assert status["flow_lpm"] == 5.3
    assert status["dewpoint_c"] == 1.9


def test_humidity_generator_safe_stop_ignores_failures(monkeypatch) -> None:
    dev = humidity_generator.HumidityGenerator("COM1", serial_factory=lambda **_: ReplaySerial())

    monkeypatch.setattr(dev, "enable_control", lambda on: (_ for _ in ()).throw(RuntimeError("ctrl")))
    monkeypatch.setattr(dev, "cool_off", lambda: (_ for _ in ()).throw(RuntimeError("cool")))
    monkeypatch.setattr(dev, "heat_off", lambda: (_ for _ in ()).throw(RuntimeError("heat")))

    result = dev.safe_stop()

    assert result["ctrl_off"] == "failed"
    assert result["cool_off"] == "failed"
    assert result["heat_off"] == "failed"


def test_humidity_generator_safe_stop_sets_zero_flow_before_ctrl_off() -> None:
    replay = ReplaySerial()
    dev = humidity_generator.HumidityGenerator("COM1", serial_factory=lambda **_: replay)

    dev.open()
    result = dev.safe_stop()
    dev.close()

    writes = [payload.decode("ascii", errors="ignore").strip() for payload in replay.writes]
    assert writes[:4] == [
        "Target:FA=0.0",
        "Target:CTRL=OFF",
        "Target:COOL=OFF",
        "Target:HEAT=OFF",
    ]
    assert result["flow_off"] == "ok"
    assert result["ctrl_off"] == "ok"
    assert result["cool_off"] == "ok"
    assert result["heat_off"] == "ok"


def test_humidity_generator_fetch_tag_value_keeps_raw_text() -> None:
    replay = ReplaySerial(
        on_write=lambda data, transport: transport.queue_line("STATE= READY")
        if data.decode("ascii", errors="ignore").strip().upper() == "FETC? (@STATE)"
        else None
    )
    dev = humidity_generator.HumidityGenerator("COM1", serial_factory=lambda **_: replay)

    dev.open()
    row = dev.fetch_tag_value("STATE")
    dev.close()

    assert row["value"] == "READY"
    assert row["raw_pick"] == "STATE= READY"


def test_humidity_generator_supports_target_readback_and_wait_stopped() -> None:
    flow_values = iter(["Flux= 0.8", "Flux= 0.2", "Flux= 0.0"])

    def _on_write(data: bytes, transport: ReplaySerial) -> None:
        text = data.decode("ascii", errors="ignore").strip().upper()
        if text == "FETC? (@TA)":
            transport.queue_line("TA= 20.0")
        elif text == "FETC? (@UWA)":
            transport.queue_line("UwA= 30.0")
        elif text == "FETC? (@ALL)":
            transport.queue_line(next(flow_values, "Flux= 0.0"))

    replay = ReplaySerial(on_write=_on_write)
    dev = humidity_generator.HumidityGenerator("COM1", serial_factory=lambda **_: replay)

    dev.open()
    readback = dev.verify_target_readback(target_temp_c=20.0, target_rh_pct=30.0)
    stopped = dev.wait_stopped(max_flow_lpm=0.05, timeout_s=2.0, poll_s=0.0)
    dev.close()

    assert readback["ok"] is True
    assert readback["read_temp_c"] == 20.0
    assert readback["read_rh_pct"] == 30.0
    assert stopped["ok"] is True
    assert stopped["flow_lpm"] == 0.0


def test_humidity_generator_supports_dewpoint_target_derivation_and_command_order() -> None:
    replay = ReplaySerial()
    dev = humidity_generator.HumidityGenerator("COM1", serial_factory=lambda **_: replay)

    dev.open()
    result = dev.set_target_dewpoint(2.0)
    dev.close()

    writes = [payload.decode("ascii", errors="ignore").strip() for payload in replay.writes]
    assert writes[:2] == [
        f"Target:TA={result['target_temp_c']}",
        f"Target:UwA={result['target_rh_pct']}",
    ]
    assert result["target_dewpoint_c"] == 2.0
    assert result["target_temp_c"] == 20.0
    assert result["target_rh_pct"] == pytest.approx(30.19, abs=1e-3)


def test_humidity_generator_supports_runtime_activation_verify_with_cooling_evidence() -> None:
    rows = iter(
        [
            "Uw= 5.0,Tc= 22.0,Ts= 22.0,Flux= 0.0",
            "Uw= 5.0,Tc= 22.0,Ts= 21.9,Flux= 0.6",
            "Uw= 5.0,Tc= 22.0,Ts= 21.4,Flux= 0.8",
        ]
    )

    def _on_write(data: bytes, transport: ReplaySerial) -> None:
        text = data.decode("ascii", errors="ignore").strip().upper()
        if text == "FETC? (@ALL)":
            transport.queue_line(next(rows, "Uw= 5.0,Tc= 22.0,Ts= 21.4,Flux= 0.8"))

    replay = ReplaySerial(on_write=_on_write)
    dev = humidity_generator.HumidityGenerator("COM1", serial_factory=lambda **_: replay)

    dev.open()
    result = dev.verify_runtime_activation(
        min_flow_lpm=0.5,
        timeout_s=2.0,
        poll_s=0.0,
        target_temp_c=0.0,
        baseline_hot_temp_c=22.0,
        baseline_cold_temp_c=22.0,
        cooling_min_drop_c=0.2,
        cooling_min_delta_c=0.5,
    )
    dev.close()

    assert result["ok"] is True
    assert result["fully_confirmed"] is True
    assert result["flow_ok"] is True
    assert result["cooling_expected"] is True
    assert result["cooling_ok"] is True
    assert result["flow_lpm"] == 0.8
