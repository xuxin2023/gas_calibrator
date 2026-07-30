from __future__ import annotations

import json
from types import SimpleNamespace

import pandas as pd

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


def test_pressure_diff_stats_normalizes_reference_hpa_to_analyzer_kpa() -> None:
    frame = pd.DataFrame(
        [
            {"P": 1000.0, "BAR": 100.2},
            {"P": 1100.0, "BAR": 109.7},
        ]
    )

    result = validation_common._pressure_diff_stats(frame)

    assert result["reference_pressure_unit"] == "hPa"
    assert result["reference_to_kpa_scale"] == 0.1
    assert result["analyzer_pressure_unit"] == "kPa"
    assert result["Overlap"] == 2
    assert abs(result["P_BAR_mean_abs_diff_kpa"] - 0.25) < 1e-12
    assert abs(result["P_BAR_max_abs_diff_kpa"] - 0.3) < 1e-12
    assert abs(result["P_minus_BAR_mean_kpa"] - 0.05) < 1e-12
    assert result["P_BAR_mean_abs_diff"] == result["P_BAR_mean_abs_diff_kpa"]


def test_same_frame_pressure_source_audit_uses_same_rows_and_kpa_for_both_fits() -> None:
    rows = [
        {"P": 1000.0, "BAR": 100.2, "target": 400.0, "ratio": 1.0, "temp": 20.0},
        {"P": 1100.0, "BAR": 109.7, "target": 800.0, "ratio": 1.1, "temp": 21.0},
        {"P": None, "BAR": 101.0, "target": 1200.0, "ratio": 1.2, "temp": 22.0},
    ]
    calls: list[dict[str, object]] = []

    def fake_fit(fit_rows, **kwargs):
        pressure_key = kwargs["pressure_keys"][0]
        calls.append(
            {
                "pressure_key": pressure_key,
                "rows": [dict(row) for row in fit_rows],
                "pressure_values": [row[pressure_key] for row in fit_rows],
            }
        )
        pressure_coefficient = 1.0 if pressure_key == "_pressure_audit_reference_kpa" else 1.25
        return SimpleNamespace(
            n=len(fit_rows),
            residuals=[
                {"target": 400.0, "error_simplified": 1.0},
                {"target": 800.0, "error_simplified": -1.0},
            ],
            stats={
                "rmse_simplified": 1.0,
                "mae_simplified": 1.0,
                "max_abs_simplified": 1.0,
                "original_coefficient_analysis": {
                    "condition_number": 12.0,
                    "max_abs_coefficient": 2.0,
                    "min_nonzero_coefficient": 0.5,
                },
            },
            simplified_coefficients={"Constant": 2.0, "P": pressure_coefficient},
            feature_terms={"Constant": "1", "P": "R*T_k*P"},
        )

    result = validation_common._same_frame_pressure_source_audit(
        rows,
        fit_fn=fake_fit,
        gas="co2",
        target_key="target",
        ratio_key="ratio",
        temp_keys=("temp",),
        common_kwargs={},
    )

    assert result["pressure_audit_status"] == "diagnostic_comparable"
    assert result["pressure_audit_same_frame_rows"] == 2
    assert result["pressure_audit_promotion_state"] == "blocked"
    assert result["pressure_audit_not_real_acceptance_evidence"] is True
    assert result["pressure_audit_equivalence_decision"] == "not_assessed_no_predefined_limits"
    assert len(calls) == 2
    assert calls[0]["pressure_values"] == [100.0, 110.0]
    assert calls[1]["pressure_values"] == [100.2, 109.7]
    assert calls[0]["rows"] == calls[1]["rows"]
    coefficient_deltas = json.loads(result["pressure_audit_analyzer_minus_reference_coefficients_json"])
    assert coefficient_deltas == {"Constant": 0.0, "P": 0.25}
    assert json.loads(result["pressure_audit_pressure_related_coefficient_deltas_json"]) == {"R*T_k*P": 0.25}
    assert result["pressure_audit_R_T_k_P_coefficient_delta"] == 0.25


def test_same_frame_pressure_source_audit_fails_closed_without_analyzer_pressure() -> None:
    result = validation_common._same_frame_pressure_source_audit(
        [{"P": 1000.0, "target": 400.0, "ratio": 1.0, "temp": 20.0}],
        fit_fn=lambda *_args, **_kwargs: None,
        gas="co2",
        target_key="target",
        ratio_key="ratio",
        temp_keys=("temp",),
        common_kwargs={},
    )

    assert result["pressure_audit_status"] == "evidence_insufficient"
    assert result["pressure_audit_reason"] == "missing_analyzer_pressure_column_BAR_or_P_fit"
    assert result["pressure_audit_promotion_state"] == "blocked"
