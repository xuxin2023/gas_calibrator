from gas_calibrator.v2.domain.explanation_models import (
    AlgorithmRecommendation,
    PointRejection,
    Recommendation,
    RunExplanation,
)


def test_recommendation_and_algorithm_recommendation_explain() -> None:
    recommendation = Recommendation(
        action="use",
        reason="fit quality is strong",
        details=["R² above threshold", "residuals are stable"],
        confidence=0.9,
        alternatives=["polynomial"],
    )
    algo = AlgorithmRecommendation(
        selected_algorithm="linear",
        reason="highest combined score",
        comparison_summary="linear outperformed polynomial",
        ranking=["linear", "polynomial"],
        scores={"linear": 0.95, "polynomial": 0.82},
        recommendation=recommendation,
    )

    assert "Action=use" in recommendation.explain()
    assert "Selected linear" in algo.explain()


def test_point_rejection_and_run_explanation_report() -> None:
    rejection = PointRejection(
        point_index=3,
        rejected=True,
        reasons=["outlier_ratio_too_high", "sample_count_low"],
        qc_score=0.42,
        sample_count=2,
        outlier_count=1,
        recommendation=Recommendation(action="retry", reason="collect more stable samples"),
    )
    run = RunExplanation(
        run_id="run_003",
        total_points=5,
        valid_points=4,
        rejected_points=1,
        point_rejections=[rejection],
        overall_quality=0.81,
        overall_confidence=0.78,
        final_recommendation=Recommendation(action="use", reason="enough valid points remain"),
    )

    report = run.to_report()

    assert "Point 3 rejected" in rejection.explain()
    assert "Run run_003" in report
    assert "enough valid points remain" in report
