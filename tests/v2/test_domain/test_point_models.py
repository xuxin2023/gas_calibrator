from gas_calibrator.validation.simulation.domain import (
    CalibrationPoint,
    PointExecutionState,
    PointStatus,
    WorkflowPhase,
)


def test_calibration_point_defaults() -> None:
    point = CalibrationPoint(index=1, name="P1")

    assert point.enabled is True
    assert point.sample_seconds == 30
    assert point.stability_seconds == 60
    assert point.remarks == ""


def test_point_execution_state_defaults() -> None:
    state = PointExecutionState(point_index=1)

    assert state.status is PointStatus.PENDING
    assert state.phase is WorkflowPhase.POINT_EXECUTION
    assert state.sample_count == 0
    assert state.rejected is False


def test_point_models_have_domain_package_as_their_single_owner() -> None:
    assert (
        CalibrationPoint.__module__
        == "gas_calibrator.validation.simulation.domain"
    )
    assert (
        PointExecutionState.__module__
        == "gas_calibrator.validation.simulation.domain"
    )
