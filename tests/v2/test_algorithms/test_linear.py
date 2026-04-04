from datetime import datetime

from gas_calibrator.v2.algorithms.linear import LinearAlgorithm
from gas_calibrator.v2.domain.result_models import PointResult
from gas_calibrator.v2.domain.sample_models import RawSample


def test_linear_fit_and_predict() -> None:
    algorithm = LinearAlgorithm("linear")
    point_results = [
        PointResult(point_index=1, mean_co2=1.0, mean_h2o=3.0),
        PointResult(point_index=2, mean_co2=2.0, mean_h2o=5.0),
        PointResult(point_index=3, mean_co2=3.0, mean_h2o=7.0),
    ]

    fit_result = algorithm.fit([], point_results)

    assert fit_result.valid is True
    assert abs(fit_result.coefficients["slope"] - 2.0) < 1e-6
    assert abs(fit_result.coefficients["intercept"] - 1.0) < 1e-6
    assert abs(algorithm.predict(fit_result.coefficients, {"x": 4.0}) - 9.0) < 1e-6


def test_linear_validate() -> None:
    algorithm = LinearAlgorithm("linear", {"tolerance": 0.2})
    fit_result = algorithm.fit(
        [],
        [
            PointResult(point_index=1, mean_co2=1.0, mean_h2o=3.0),
            PointResult(point_index=2, mean_co2=2.0, mean_h2o=5.0),
            PointResult(point_index=3, mean_co2=3.0, mean_h2o=7.0),
        ],
    )
    samples = [
        RawSample(timestamp=datetime(2026, 3, 18, 10, 0, 0), point_index=1, analyzer_name="ga01", co2=1.0, h2o=3.0),
        RawSample(timestamp=datetime(2026, 3, 18, 10, 0, 1), point_index=1, analyzer_name="ga01", co2=2.0, h2o=5.0),
    ]

    validation = algorithm.validate(fit_result, samples)

    assert validation.passed is True
    assert validation.sample_count == 2
