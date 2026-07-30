from datetime import datetime

from gas_calibrator.validation.simulation.domain import (
    RunContext,
    RunStatus,
    RunSummary,
    WorkflowPhase,
)


def test_run_context_defaults() -> None:
    started_at = datetime(2026, 3, 18, 10, 0, 0)
    context = RunContext(
        run_id="run_001",
        task_name="bench",
        started_at=started_at,
        output_dir="output/run_001",
    )

    assert context.status is RunStatus.IDLE
    assert context.current_phase is WorkflowPhase.STARTUP
    assert context.current_point_index is None
    assert context.message == ""


def test_run_summary_fields() -> None:
    started_at = datetime(2026, 3, 18, 10, 0, 0)
    ended_at = datetime(2026, 3, 18, 10, 30, 0)
    summary = RunSummary(
        run_id="run_001",
        status=RunStatus.FINISHED,
        total_points=10,
        passed_points=8,
        failed_points=2,
        started_at=started_at,
        ended_at=ended_at,
        duration_sec=1800.0,
        warnings=["warn"],
        errors=["err"],
    )

    assert summary.status is RunStatus.FINISHED
    assert summary.warnings == ["warn"]
    assert summary.errors == ["err"]


def test_run_models_have_domain_package_as_their_single_owner() -> None:
    assert RunContext.__module__ == "gas_calibrator.validation.simulation.domain"
    assert RunSummary.__module__ == "gas_calibrator.validation.simulation.domain"
