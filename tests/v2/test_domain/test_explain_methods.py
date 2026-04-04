from gas_calibrator.v2.algorithms.engine import AlgorithmEngine
from gas_calibrator.v2.algorithms.registry import AlgorithmRegistry
from gas_calibrator.v2.algorithms.result_types import ComparisonResult, FitResult
from gas_calibrator.v2.algorithms.validator import BackValidator


def test_validator_assesses_confidence_level() -> None:
    validator = BackValidator()
    fit_result = FitResult(
        algorithm_name="linear",
        coefficients={"slope": 1.0, "intercept": 0.0},
        r_squared=0.97,
        rmse=0.02,
        residuals=[0.01, -0.01, 0.0, 0.01],
    )

    confidence = validator.assess_confidence(fit_result)

    assert 0.0 <= confidence <= 1.0
    assert validator.get_confidence_level(confidence) in {"low", "medium", "high"}


def test_engine_explains_selection_and_rejection() -> None:
    registry = AlgorithmRegistry()
    registry.register_default_algorithms()
    engine = AlgorithmEngine(registry)
    comparison = ComparisonResult(
        best_algorithm="linear",
        results={
            "linear": FitResult(algorithm_name="linear", coefficients={"slope": 2.0}, r_squared=0.99, rmse=0.01, confidence=0.9, confidence_level="high"),
            "polynomial": FitResult(algorithm_name="polynomial", coefficients={"coef_2": 1.0}, r_squared=0.95, rmse=0.03, confidence=0.75, confidence_level="medium"),
        },
        ranking=["linear", "polynomial"],
        recommendation="linear ranked first",
    )

    selection = engine.explain_selection(comparison)
    rejection = engine.explain_rejection(
        7,
        type(
            "QCResult",
            (),
            {
                "valid": False,
                "reason": "outlier_ratio_too_high,sample_count_low",
                "quality_score": 0.4,
                "usable_sample_count": 2,
                "outlier_ratio": 0.5,
                "recommendation": "exclude",
            },
        )(),
    )

    assert selection.selected_algorithm == "linear"
    assert "linear" in selection.explain()
    assert rejection.rejected is True
    assert rejection.recommendation.action in {"exclude", "retry"}
