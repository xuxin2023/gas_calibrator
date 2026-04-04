from __future__ import annotations

from gas_calibrator.v2.core.simulated_devices import SimulationPlantState
from gas_calibrator.v2.sim.devices import RelayFake


def test_relay_fake_supports_single_and_multiple_coil_writes_for_16_and_8_channels() -> None:
    plant = SimulationPlantState()
    relay16 = RelayFake(plant_state=plant, name="relay", channel_count=16)
    relay8 = RelayFake(plant_state=plant, name="relay_8", channel_count=8)

    relay16.write_coil(0, True)
    relay16.write_coils(1, [True, False, True])
    relay8.write_coils(0, [True, False, True, False, False, False, False, True])

    assert relay16.read_coils(0, 4).bits == [True, True, False, True]
    assert relay8.read_coils(0, 8).bits == [True, False, True, False, False, False, False, True]
    assert relay16.read_discrete_inputs(0, 4).bits == [True, True, False, True]
    assert relay8.read_discrete_inputs(0, 8).bits == [True, False, True, False, False, False, False, True]


def test_relay_fake_stuck_channel_keeps_physical_state_and_route_out_of_scope() -> None:
    plant = SimulationPlantState()
    relay8 = RelayFake(
        plant_state=plant,
        name="relay_8",
        channel_count=8,
        mode="stuck_channel",
        stuck_channels=[8],
    )

    relay8.write_coil(7, True)
    relay8.set_logical_valve_state(8, True, physical_channel=8)

    assert relay8.read_coils(7, 1).bits == [False]
    assert plant.route == "ambient"


def test_relay_fake_close_all_restores_all_channels_off() -> None:
    plant = SimulationPlantState()
    relay16 = RelayFake(plant_state=plant, name="relay", channel_count=16)
    relay8 = RelayFake(plant_state=plant, name="relay_8", channel_count=8)

    relay16.write_coils(0, [True] * 16)
    relay8.write_coils(0, [True] * 8)
    relay16.close_all()
    relay8.close_all()

    assert all(bit is False for bit in relay16.read_coils(0, 16).bits)
    assert all(bit is False for bit in relay8.read_coils(0, 8).bits)
    assert plant.route == "ambient"
