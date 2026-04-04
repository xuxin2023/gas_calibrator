from gas_calibrator.v2.core.event_bus import EventBus
from gas_calibrator.v2.core.models import CalibrationPhase, CalibrationPoint
from gas_calibrator.v2.core.state_manager import StateManager


def test_state_manager_updates_phase_and_progress() -> None:
    manager = StateManager(EventBus())
    callback_phases: list[CalibrationPhase] = []
    manager.set_progress_callback(lambda status: callback_phases.append(status.phase))

    manager.load_points(2, "loaded", point_keys=["co2:a", "co2:b"])
    manager.prepare_run(2, point_keys=["co2:a", "co2:b"])
    manager.start()
    manager.update_status(phase=CalibrationPhase.INITIALIZING, message="init")
    manager.mark_point_completed(CalibrationPoint(index=1, temperature_c=25.0, route="co2"), point_key="co2:a")
    manager.complete()

    status = manager.status
    assert status.phase is CalibrationPhase.COMPLETED
    assert status.completed_points == 1
    assert status.total_points == 2
    assert callback_phases


def test_state_manager_deduplicates_completed_point_keys_and_clamps_progress() -> None:
    manager = StateManager(EventBus())
    manager.prepare_run(1, point_keys=["co2:tag-a"])
    manager.start()

    point = CalibrationPoint(index=1, temperature_c=25.0, route="co2")
    manager.mark_point_completed(point, point_key="co2:tag-a")
    manager.mark_point_completed(point, point_key="co2:tag-a")

    status = manager.status
    assert status.total_points == 1
    assert status.completed_points == 1
    assert status.progress == 1.0


def test_state_manager_sets_error() -> None:
    manager = StateManager(EventBus())
    manager.set_error("boom")

    status = manager.status
    assert status.phase is CalibrationPhase.ERROR
    assert status.error == "boom"
