from pathlib import Path

from gas_calibrator.validation.simulation import domain
from gas_calibrator.v2.algorithms import base, linear
from gas_calibrator.v2.algorithms import (
    AlgorithmSpec,
    FitResult,
)


def test_algorithm_spec_and_fit_result_explain() -> None:
    spec = AlgorithmSpec(name="linear", display_name="Linear", description="Linear fit")
    result = FitResult(
        algorithm_name="linear",
        algorithm_spec=spec,
        coefficients={"slope": 2.0, "intercept": 1.0},
        r_squared=0.99,
        confidence=0.91,
        confidence_level="high",
    )

    assert "Linear" in spec.explain()
    assert "Confidence=0.91" in result.explain()
    assert result.coefficient_names == ["slope", "intercept"]


def test_domain_algorithm_models_do_not_reappear() -> None:
    source_root = Path(__file__).resolve().parents[3] / "src"

    assert not (
        source_root
        / "gas_calibrator/v2/domain/algorithm_models.py"
    ).exists()
    for name in (
        "AlgorithmSpec",
        "CoefficientSet",
        "FitDataset",
        "FitInput",
        "FitPoint",
        "FitResult",
    ):
        assert name not in domain.__all__
        assert not hasattr(domain, name)


def test_algorithm_models_keep_single_runtime_identity() -> None:
    assert base.AlgorithmSpec is AlgorithmSpec
    assert base.FitResult is FitResult
    assert linear.FitResult is FitResult
