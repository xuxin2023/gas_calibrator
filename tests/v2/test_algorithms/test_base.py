from gas_calibrator.v2.algorithms.base import AlgorithmBase
from gas_calibrator.v2.algorithms.result_types import FitResult, ValidationResult


class DummyAlgorithm(AlgorithmBase):
    def fit(self, samples, point_results) -> FitResult:
        return FitResult(algorithm_name=self.name, coefficients={"k": 1.0}, r_squared=1.0, rmse=0.0)

    def validate(self, fit_result, samples) -> ValidationResult:
        return ValidationResult(
            algorithm_name=self.name,
            passed=True,
            r_squared=1.0,
            rmse=0.0,
            mae=0.0,
            sample_count=len(samples),
        )

    def predict(self, coefficients, inputs) -> float:
        return float(coefficients["k"]) * float(inputs.get("x", 0.0))


def test_algorithm_base_helpers() -> None:
    algo = DummyAlgorithm("dummy")
    fit_result = algo.fit([], [])

    assert algo.export_coefficients(fit_result) == {"k": 1.0}
    assert "Algorithm: dummy" in algo.explain(fit_result)
    assert algo.predict({"k": 2.0}, {"x": 3.0}) == 6.0
