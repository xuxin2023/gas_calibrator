from __future__ import annotations

from gas_calibrator.v2.config import OfflineRefitConfig
from gas_calibrator.v2.core import run_refit_filtering


def _build_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    targets = [0.0, 180.0, 400.0, 800.0, 1200.0, 1800.0]
    for cycle in range(4):
        for offset, target in enumerate(targets):
            temperature = 20.0 + float(cycle)
            pressure = 110.0 + float(offset % 2)
            ratio = 1.0 + 0.0008 * target + 0.002 * (temperature + 273.15) + 0.0005 * pressure
            rows.append(
                {
                    "Analyzer": "GA01",
                    "PointRow": len(rows) + 1,
                    "PointPhase": "气路",
                    "PointTag": f"co2_{len(rows)}",
                    "PointTitle": f"point-{len(rows)}",
                    "ppm_CO2_Tank": target,
                    "R_CO2": ratio,
                    "T1": temperature,
                    "BAR": pressure,
                }
            )
    rows[5]["R_CO2"] = float(rows[5]["R_CO2"]) + 0.8
    rows[11]["R_CO2"] = float(rows[11]["R_CO2"]) - 0.9
    return rows


def test_run_refit_filtering_generates_audit_and_compare_tables() -> None:
    config = OfflineRefitConfig.from_dict(
        {
            "enabled": True,
            "gas_type": "co2",
            "filtering": {
                "enable_refit_filtering": True,
                "target_bins_co2": [0, 200, 500, 1000, 2000],
            },
        }
    )
    result = run_refit_filtering(_build_rows(), config=config, analyzer_id="GA01")
    assert not result.audit_frame.empty
    assert {
        "analyzer_id",
        "gas_type",
        "Y_true",
        "R",
        "T_k",
        "P",
        "target_bin",
        "first_fit_pred",
        "second_fit_pred",
        "keep_final",
        "remove_reason",
    }.issubset(result.audit_frame.columns)
    assert int(result.summary_frame.iloc[0]["总点数"]) == len(_build_rows())
    assert "RMSE_before" in result.compare_frame.columns
    assert "是否推荐采用" in result.compare_frame.columns


def test_run_refit_filtering_preserves_min_points_per_bin() -> None:
    config = OfflineRefitConfig.from_dict(
        {
            "enabled": True,
            "gas_type": "co2",
            "filtering": {
                "enable_refit_filtering": True,
                "target_bins_co2": [0, 200, 500, 1000, 2000],
                "max_remove_ratio_co2": 0.5,
                "max_remove_per_bin": 2,
                "min_points_per_bin": 3,
            },
        }
    )
    result = run_refit_filtering(_build_rows(), config=config)
    kept = result.audit_frame[result.audit_frame["keep_final"]]
    counts = kept.groupby("target_bin").size()
    assert (counts >= 3).all()
