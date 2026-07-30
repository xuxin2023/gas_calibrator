from gas_calibrator.validation.simulation.domain import (
    CleanedData,
    PointResult,
    PointStatus,
    QCDecision,
    QCLevel,
    RawSample,
    RunArtifactManifest,
    RunStatus,
    SampleWindow,
    WorkflowPhase,
)


def test_run_status_values() -> None:
    assert RunStatus.IDLE.value == "IDLE"
    assert RunStatus.FINISHED.value == "FINISHED"


def test_workflow_phase_values() -> None:
    assert WorkflowPhase.STARTUP.value == "STARTUP"
    assert WorkflowPhase.RUN_FINALIZE.value == "RUN_FINALIZE"


def test_point_and_qc_levels() -> None:
    assert PointStatus.PENDING.value == "PENDING"
    assert QCLevel.PASS.value == "PASS"


def test_foundational_domain_types_have_one_package_owner() -> None:
    owned_types = (
        RunStatus,
        WorkflowPhase,
        PointStatus,
        QCLevel,
        RawSample,
        SampleWindow,
        PointResult,
        RunArtifactManifest,
        QCDecision,
        CleanedData,
    )
    assert {item.__module__ for item in owned_types} == {
        "gas_calibrator.validation.simulation.domain"
    }
