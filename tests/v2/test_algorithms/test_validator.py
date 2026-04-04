from datetime import datetime

from gas_calibrator.v2.algorithms.result_types import FitResult
from gas_calibrator.v2.algorithms.validator import BackValidator
from gas_calibrator.v2.domain.sample_models import RawSample


def test_back_validator_validates_linear_fit() -> None:
    validator = BackValidator()
    fit_result = FitResult(
        algorithm_name="linear",
        coefficients={"slope": 2.0, "intercept": 1.0},
        r_squared=1.0,
        rmse=0.0,
    )
    samples = [
        RawSample(timestamp=datetime(2026, 3, 18, 10, 0, 0), point_index=1, analyzer_name="ga01", co2=1.0, h2o=3.0),
        RawSample(timestamp=datetime(2026, 3, 18, 10, 0, 1), point_index=1, analyzer_name="ga01", co2=2.0, h2o=5.0),
        RawSample(timestamp=datetime(2026, 3, 18, 10, 0, 2), point_index=1, analyzer_name="ga01", co2=3.0, h2o=7.0),
    ]

    result = validator.validate(fit_result, samples, tolerance=0.1)

    assert result.passed is True
    assert result.sample_count == 3
    assert result.outliers == []
