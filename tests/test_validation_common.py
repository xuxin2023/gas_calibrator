from __future__ import annotations

from types import SimpleNamespace

from gas_calibrator.coefficients.model_feature_policy import AMBIENT_ONLY_MODEL_FEATURES
from gas_calibrator.validation import common as validation_common


def test_fit_overview_rows_ambient_only_passes_seven_feature_model(monkeypatch) -> None:
    summary_rows = [
        {"Analyzer": "GA01", "PointPhase": "气路", "ppm_CO2_Tank": 400.0, "R_CO2": 1.0, "T1": 20.0, "BAR": 101.0},
        {"Analyzer": "GA01", "PointPhase": "气路", "ppm_CO2_Tank": 500.0, "R_CO2": 1.1, "T1": 21.0, "BAR": 101.1},
    ]
    captured: dict[str, object] = {}

    def fake_fit(rows, **kwargs):
        captured["model_features"] = kwargs.get("model_features")
        return SimpleNamespace(
            n=len(rows),
            residuals=[{"error_simplified": 0.1}, {"error_simplified": -0.1}],
            stats={"rmse_simplified": 0.1, "max_abs_simplified": 0.2},
        )

    monkeypatch.setattr(validation_common, "fit_ratio_poly_rt_p", fake_fit)

    rows, messages = validation_common.fit_overview_rows(
        summary_rows,
        cfg={"workflow": {"selected_pressure_points": ["ambient"]}, "coefficients": {}},
        gas="co2",
        mode="current",
    )

    assert messages is not None
    assert captured["model_features"] == AMBIENT_ONLY_MODEL_FEATURES
    assert rows[0]["status"] == "fit_ok"
    assert rows[0]["model_feature_policy"] == "ambient_only_fallback"
    assert rows[0]["model_feature_tokens"] == ",".join(AMBIENT_ONLY_MODEL_FEATURES)
