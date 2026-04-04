import numpy as np

from gas_calibrator.coefficients.coefficient_analysis import analyze_coefficient_stability


def test_analyze_coefficient_stability_returns_expected_keys() -> None:
    x_matrix = np.array([[1.0, 1.0], [1.0, 2.0], [1.0, 3.0]])
    coefficients = np.array([2.0, 0.01])

    stats = analyze_coefficient_stability(x_matrix, coefficients)

    assert stats["condition_number"] > 0
    assert stats["max_abs_coefficient"] == 2.0
    assert stats["min_nonzero_coefficient"] == 0.01
    assert stats["nonzero_count"] == 2
