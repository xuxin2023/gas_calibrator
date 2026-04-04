import pandas as pd

from gas_calibrator.coefficients.outlier_detector import detect_iqr_outliers, filter_outliers


def test_detect_iqr_outliers_flags_extreme_value() -> None:
    mask = detect_iqr_outliers([10, 11, 12, 13, 100])

    assert mask.tolist() == [False, False, False, False, True]


def test_filter_outliers_removes_training_outlier() -> None:
    dataframe = pd.DataFrame(
        {
            "ppm_CO2_Tank": [100, 105, 110, 115, 6000],
            "R_CO2": [0.8, 0.82, 0.84, 0.86, 0.85],
            "T1": [20, 21, 22, 23, 22],
            "BAR": [100, 100, 101, 101, 100],
        }
    )

    result = filter_outliers(
        dataframe,
        target_column="ppm_CO2_Tank",
        ratio_column="R_CO2",
        temperature_column="T1",
        pressure_column="BAR",
        model_features=["intercept", "R", "R2", "T", "P"],
        temperature_offset_c=273.15,
        methods=("iqr",),
    )

    assert result.outlier_count == 1
    assert result.final_count == 4
