import csv
import json

from gas_calibrator.validation.co2_s13_residual_root_cause_review import (
    build_co2_s13_residual_root_cause_review,
    write_co2_s13_residual_root_cause_review,
)


def _write_csv(path, rows):
    header = []
    for row in rows:
        for key in row:
            if key not in header:
                header.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def _fit_points():
    base = {
        "component": "co2",
        "analyzer_device_id": "123",
        "analyzer_prefix": "ga01",
        "source_role": "fit",
        "temperature_c": "20.0",
        "pressure_hpa": "1013.2",
        "temp_set_c": "20",
        "co2_ratio_f_std": "0.00003",
        "dewpoint_mean_c": "-35.0",
        "pressure_gauge_mean_hpa": "1013.2",
        "h2o_mmol_mean": "48.0",
        "ref_signal_mean": "3600",
        "co2_signal_mean": "2200",
        "sample_count": "20",
        "usable_sample_count": "20",
        "status_register_qc_values": "missing",
        "fit_inclusion_status": "included",
    }
    rows = []
    rows.append(
        {
            **base,
            "point_identity": "T20_0ppm",
            "target_value": "0",
            "ratio": "1.0",
            "zero_anchor_class": "estimated_zero_anchor",
        }
    )
    rows.append(
        {
            **base,
            "point_identity": "T20_100ppm",
            "target_value": "100",
            "ratio": "0.9",
            "zero_anchor_class": "standard_fit_point",
        }
    )
    return rows


def _residuals():
    common = {
        "device_id": "123",
        "objective_id": "absolute_lstsq",
        "zero_offset_ppm": "5",
        "model_id": "senco13_temperature_terms_pressure_zero",
        "source_role": "fit",
        "temperature_c": "20.0",
        "pressure_hpa": "1013.2",
        "h2o_mmol": "48.0",
    }
    return [
        {
            **common,
            "point_identity": "T20_0ppm",
            "target_ppm": "5",
            "zero_anchor_class": "estimated_zero_anchor",
            "prediction_ppm": "5.2",
            "error_ppm": "0.2",
            "relative_error_percent": "",
            "ratio": "1.0",
        },
        {
            **common,
            "point_identity": "T20_100ppm",
            "target_ppm": "100",
            "zero_anchor_class": "standard_fit_point",
            "prediction_ppm": "106",
            "error_ppm": "6",
            "relative_error_percent": "6.0",
            "ratio": "0.9",
        },
        {
            **common,
            "objective_id": "absolute_lstsq",
            "zero_offset_ppm": "0",
            "point_identity": "T20_100ppm",
            "target_ppm": "100",
            "zero_anchor_class": "standard_fit_point",
            "prediction_ppm": "110",
            "error_ppm": "10",
            "relative_error_percent": "10.0",
            "ratio": "0.9",
        },
    ]


def _summary():
    return [
        {
            "device_id": "123",
            "objective_id": "absolute_lstsq",
            "zero_offset_ppm": "0",
            "max_abs_relative_error_percent": "10",
        },
        {
            "device_id": "123",
            "objective_id": "absolute_lstsq",
            "zero_offset_ppm": "5",
            "max_abs_relative_error_percent": "6",
        },
        {
            "device_id": "123",
            "objective_id": "absolute_lstsq",
            "zero_offset_ppm": "10",
            "max_abs_relative_error_percent": "8",
        },
    ]


def _selected():
    return [
        {
            "device_id": "123",
            "best_objective_id": "absolute_lstsq",
            "best_zero_offset_ppm": "5",
        }
    ]


def test_root_cause_review_flags_zero_and_h2o_mismatch(tmp_path):
    fit = tmp_path / "fit.csv"
    residuals = tmp_path / "residuals.csv"
    summary = tmp_path / "summary.csv"
    selected = tmp_path / "selected.csv"
    _write_csv(fit, _fit_points())
    _write_csv(residuals, _residuals())
    _write_csv(summary, _summary())
    _write_csv(selected, _selected())

    tables = build_co2_s13_residual_root_cause_review(
        fit_points_csv=fit,
        objective_residuals_csv=residuals,
        objective_summary_csv=summary,
        selected_candidates_csv=selected,
    )

    zero_effect = tables["zero_offset_effect"][0]
    nonzero = [row for row in tables["selected_residuals"] if row["point_identity"] == "T20_100ppm"][0]
    zero = [row for row in tables["selected_residuals"] if row["point_identity"] == "T20_0ppm"][0]

    assert zero_effect["zero5_conclusion"] == "5ppm_zero_assumption_clearly_improves"
    assert tables["low_end_pattern_summary"]
    assert tables["device_low_end_temperature_bias"]
    assert nonzero["ratio_grade"] == "A"
    assert nonzero["dryness_grade"] == "deep_dry"
    assert nonzero["root_cause_hypothesis"] == "analyzer_h2o_output_not_physical_for_co2_bridge"
    assert zero["root_cause_hypothesis"] == "zero_anchor_not_traceably_zero"


def test_root_cause_review_writes_no_write_artifacts(tmp_path):
    fit = tmp_path / "fit.csv"
    residuals = tmp_path / "residuals.csv"
    summary = tmp_path / "summary.csv"
    selected = tmp_path / "selected.csv"
    _write_csv(fit, _fit_points())
    _write_csv(residuals, _residuals())
    _write_csv(summary, _summary())
    _write_csv(selected, _selected())

    outputs = write_co2_s13_residual_root_cause_review(
        fit_points_csv=fit,
        objective_residuals_csv=residuals,
        objective_summary_csv=summary,
        selected_candidates_csv=selected,
        output_dir=tmp_path / "out",
    )

    meta = json.loads(outputs["metadata"].read_text(encoding="utf-8"))
    assert meta["boundary"] == {
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "not_real_acceptance_evidence": True,
    }
    assert outputs["markdown"].read_bytes()[:3] == b"\xef\xbb\xbf"
    text = outputs["markdown"].read_text(encoding="utf-8-sig")
    assert "低端残差根因拆解报告" in text
    assert "乱码" not in text
    worst = list(csv.DictReader(outputs["worst_nonzero_points"].open(encoding="utf-8-sig")))
    assert worst
    assert worst[0]["controls_water_or_gas_routes"] == "False"
    assert worst[0]["writes_coefficients"] == "False"
    low_end = list(csv.DictReader(outputs["low_end_pattern_summary"].open(encoding="utf-8-sig")))
    assert low_end
    assert low_end[0]["writes_coefficients"] == "False"
