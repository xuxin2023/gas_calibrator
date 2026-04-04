from gas_calibrator.v2.domain.enums import PointStatus, QCLevel, RunStatus, WorkflowPhase


def test_run_status_values() -> None:
    assert RunStatus.IDLE.value == "IDLE"
    assert RunStatus.FINISHED.value == "FINISHED"


def test_workflow_phase_values() -> None:
    assert WorkflowPhase.STARTUP.value == "STARTUP"
    assert WorkflowPhase.RUN_FINALIZE.value == "RUN_FINALIZE"


def test_point_and_qc_levels() -> None:
    assert PointStatus.PENDING.value == "PENDING"
    assert QCLevel.PASS.value == "PASS"
