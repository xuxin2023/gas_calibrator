from types import SimpleNamespace

from gas_calibrator.v2.intelligence.context_builders import (
    build_fit_context,
    build_qc_context,
    build_run_context,
)


def test_build_qc_context_from_validation_and_cleaned() -> None:
    validation = SimpleNamespace(valid=False, quality_score=0.42, reasons=["outlier_ratio_high"])
    cleaned = SimpleNamespace(cleaned_count=8, removed_count=2)

    context = build_qc_context(3, validation, cleaned)

    assert context.point_index == 3
    assert context.passed is False
    assert context.qc_score == 0.42
    assert context.sample_count == 8
    assert context.outlier_count == 2
    assert context.action == "剔除"


def test_build_fit_context_counts_valid_points() -> None:
    fit_result = SimpleNamespace(
        algorithm_name="linear",
        r_squared=0.98,
        rmse=0.1,
        mae=0.05,
        confidence=0.91,
    )
    points = [
        SimpleNamespace(accepted=True),
        SimpleNamespace(accepted=False),
        SimpleNamespace(accepted=True),
    ]

    context = build_fit_context(fit_result, points, quality_score=0.8)

    assert context.algorithm == "linear"
    assert context.point_count == 3
    assert context.valid_points == 2
    assert context.quality_score == 0.8


def test_build_run_context_uses_quality_score_and_fit_result() -> None:
    session = SimpleNamespace(run_id="run_001", total_points=5)
    fit_result = SimpleNamespace(algorithm_name="polynomial", r_squared=0.95, rmse=0.2)
    quality_score = SimpleNamespace(overall_score=0.88, valid_points=4)

    context = build_run_context(session, fit_result, quality_score)

    assert context.run_id == "run_001"
    assert context.total_points == 5
    assert context.valid_points == 4
    assert context.invalid_points == 1
    assert context.algorithm == "polynomial"
    assert context.overall_score == 0.88
