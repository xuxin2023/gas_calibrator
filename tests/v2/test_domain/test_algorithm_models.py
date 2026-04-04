from datetime import datetime

from gas_calibrator.v2.domain.algorithm_models import (
    AlgorithmSpec,
    CoefficientSet,
    FitDataset,
    FitPoint,
    FitResult,
)


def test_fit_dataset_to_arrays_and_validate() -> None:
    dataset = FitDataset(
        run_id="run_001",
        gas_type="co2",
        points=[
            FitPoint(index=1, target=100.0, ratio=1.1, temperature_c=25.0, pressure_hpa=1013.25),
            FitPoint(index=2, target=200.0, ratio=2.1, temperature_c=25.0, pressure_hpa=1012.8),
        ],
    )

    x_values, y_values = dataset.to_arrays()

    assert dataset.validate() is True
    assert x_values.tolist() == [1.1, 2.1]
    assert y_values.tolist() == [100.0, 200.0]


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


def test_coefficient_set_from_fit_result_and_to_dict() -> None:
    fit_result = FitResult(
        algorithm_name="polynomial",
        coefficients={"coef_2": 1.0, "coef_1": 2.0, "coef_0": 3.0},
        r_squared=0.98,
        confidence=0.84,
        valid=True,
        message="ok",
        residuals=[0.1, -0.1, 0.0],
        created_at=datetime(2026, 3, 18, 12, 0, 0),
    )

    coefficient_set = CoefficientSet.from_fit_result(fit_result, run_id="run_002")
    payload = coefficient_set.to_dict()

    assert coefficient_set.run_id == "run_002"
    assert coefficient_set.point_count == 3
    assert payload["confidence"] == 0.84
    assert payload["algorithm_name"] == "polynomial"
