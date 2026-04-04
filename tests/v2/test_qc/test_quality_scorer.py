from gas_calibrator.v2.qc.point_validator import PointValidationResult
from gas_calibrator.v2.qc.quality_scorer import QualityScorer


def test_quality_scorer_scores_run_and_grade() -> None:
    scorer = QualityScorer()
    validations = [
        PointValidationResult(True, 1, 5, 0.0, 0.95, "use", "passed"),
        PointValidationResult(True, 2, 5, 0.0, 0.85, "use", "passed"),
        PointValidationResult(False, 3, 2, 0.4, 0.55, "exclude", "outlier_ratio_too_high"),
    ]

    score = scorer.score_run(validations)

    assert 0.0 <= score.overall_score <= 1.0
    assert score.grade in {"A", "B", "C", "D", "F"}
    assert 1 in score.point_scores
    assert score.recommendations
