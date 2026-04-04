from gas_calibrator.v2.algorithms.polynomial import PolynomialAlgorithm
from gas_calibrator.v2.domain.result_models import PointResult


def test_polynomial_fit_quadratic_curve() -> None:
    algorithm = PolynomialAlgorithm("polynomial", {"degree": 2})
    point_results = [
        PointResult(point_index=1, mean_co2=0.0, mean_h2o=1.0),
        PointResult(point_index=2, mean_co2=1.0, mean_h2o=6.0),
        PointResult(point_index=3, mean_co2=2.0, mean_h2o=17.0),
        PointResult(point_index=4, mean_co2=3.0, mean_h2o=34.0),
    ]

    fit_result = algorithm.fit([], point_results)

    assert fit_result.valid is True
    assert abs(algorithm.predict(fit_result.coefficients, {"x": 4.0}) - 57.0) < 1e-5
    assert fit_result.r_squared > 0.999
