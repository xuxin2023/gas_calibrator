from __future__ import annotations

from gas_calibrator.v2.core.simulated_devices import SimulationPlantState
from gas_calibrator.v2.sim.devices import AnalyzerFake


def test_analyzer_fake_mode2_continuous_frame_supports_active_and_passive_reads() -> None:
    plant = SimulationPlantState()
    fake = AnalyzerFake(
        plant_state=plant,
        device_id="001",
        mode=2,
        active_send=True,
        mode2_stream="stable",
    )

    fake.open()
    passive = fake.process_command("READDATA,YGAS,FFF").strip()
    active = fake.read_data_active()
    parsed_passive = fake.parse_line_mode2(passive)
    parsed_active = fake.parse_line_mode2(active)

    assert parsed_passive is not None
    assert parsed_active is not None
    assert parsed_passive["device_id"] == "001"
    assert parsed_active["mode"] == 2
    assert parsed_active["pressure_hpa"] > 0.0


def test_analyzer_fake_supports_broadcast_mode_switch_ftd_and_average_commands() -> None:
    fake = AnalyzerFake(device_id="002", active_send=False, mode=1)

    assert fake.process_command("MODE,YGAS,FFF,2") == "YGAS,002,T\r\n"
    assert fake.process_command("SETCOMWAY,YGAS,FFF,1") == "YGAS,002,T\r\n"
    assert fake.process_command("FTD,YGAS,FFF,20") == "YGAS,002,T\r\n"
    assert fake.process_command("AVERAGE1,YGAS,FFF,4") == "YGAS,002,T\r\n"
    assert fake.process_command("AVERAGE2,YGAS,FFF,6") == "YGAS,002,T\r\n"

    status = fake.status()

    assert status["mode_effective"] == 2
    assert status["active_send"] is True
    assert status["ftd_hz"] == 20
    assert status["average_h2o"] == 4
    assert status["average_co2"] == 6


def test_analyzer_fake_fault_modes_are_repeatable_and_relaxed_mode_can_recover() -> None:
    strict_fail = AnalyzerFake(device_id="003", mode2_stream="partial_frame", sensor_precheck="strict_fail")
    relaxed = AnalyzerFake(device_id="004", mode2_stream="partial_frame", sensor_precheck="relaxed_pass")
    truncated = AnalyzerFake(device_id="005", mode2_stream="truncated_frame", sensor_precheck="strict_fail")
    corrupted = AnalyzerFake(device_id="006", mode2_stream="corrupted_frame", sensor_precheck="strict_fail")

    strict_frames = [strict_fail.read_data_passive() for _ in range(3)]
    relaxed_frames = [relaxed.read_data_passive() for _ in range(3)]

    assert all(strict_fail.parse_line_mode2(frame) is None for frame in strict_frames)
    assert any(relaxed.parse_line_mode2(frame) is not None for frame in relaxed_frames)
    assert truncated.parse_line_mode2(truncated.read_data_passive()) is None
    assert corrupted.parse_line_mode2(corrupted.read_data_passive()) is None


def test_analyzer_fake_supports_multiple_analyzers_and_unique_ids() -> None:
    devices = [AnalyzerFake(device_id=f"{index:03d}") for index in range(1, 9)]
    ids = [device.fetch_all()["data"]["device_id"] for device in devices]
    co2_values = [device.fetch_all()["data"]["co2_ppm"] for device in devices]

    assert ids == [f"{index:03d}" for index in range(1, 9)]
    assert len(set(co2_values)) == len(co2_values)
