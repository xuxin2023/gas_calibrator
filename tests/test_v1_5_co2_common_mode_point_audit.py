import csv

from gas_calibrator.validation.co2_common_mode_point_audit import (
    AuditInputs,
    build_co2_common_mode_point_audit_tables,
    write_co2_common_mode_point_audit_report,
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


def _fit_rows():
    rows = []
    for device, prefix in (("001", "ga01"), ("002", "ga02"), ("003", "ga03")):
        rows.extend(
            [
                {
                    "component": "co2",
                    "analyzer_device_id": device,
                    "analyzer_prefix": prefix,
                    "point_identity": "T20_600ppm",
                    "target_value": "600",
                    "ratio": "1.2",
                    "co2_ratio_f_std": "0.00012",
                    "temperature_c": "20.0",
                    "pressure_hpa": "1010.0",
                    "h2o_mmol_mean": "0.36",
                    "dewpoint_mean_c": "-36.0",
                    "ref_signal_mean": "3800",
                    "co2_signal_mean": "4500",
                    "usable_sample_count": "120",
                    "zero_anchor_class": "standard_fit_point",
                    "status_register_qc_values": "missing",
                },
                {
                    "component": "co2",
                    "analyzer_device_id": device,
                    "analyzer_prefix": prefix,
                    "point_identity": "T20_400ppm",
                    "target_value": "400",
                    "ratio": "1.3",
                    "co2_ratio_f_std": "0.00010",
                    "temperature_c": "20.0",
                    "pressure_hpa": "1010.1",
                    "h2o_mmol_mean": "0.34",
                    "dewpoint_mean_c": "-36.5",
                    "ref_signal_mean": "3801",
                    "co2_signal_mean": "4501",
                    "usable_sample_count": "120",
                    "zero_anchor_class": "standard_fit_point",
                    "status_register_qc_values": "missing",
                },
            ]
        )
    return rows


def _prediction_rows():
    rows = []
    errors = {"001": 12.0, "002": 10.0, "003": 11.0}
    for device, prefix in (("001", "ga01"), ("002", "ga02"), ("003", "ga03")):
        rows.append(
            {
                "device_id": device,
                "analyzer_prefix": prefix,
                "model_id": "senco13_temperature_terms_pressure_zero",
                "source_role": "fit",
                "point_identity": "T20_600ppm",
                "target_ppm": "600",
                "prediction_ppm": str(600 + errors[device]),
                "error_ppm": str(errors[device]),
            }
        )
        rows.append(
            {
                "device_id": device,
                "analyzer_prefix": prefix,
                "model_id": "senco13_temperature_terms_pressure_zero",
                "source_role": "fit",
                "point_identity": "T20_400ppm",
                "target_ppm": "400",
                "prediction_ppm": str(400 + (-1) ** int(device) * 1.5),
                "error_ppm": str((-1) ** int(device) * 1.5),
            }
        )
        rows.append(
            {
                "device_id": device,
                "analyzer_prefix": prefix,
                "model_id": "senco13_temperature_terms_pressure_zero_h2o_bridge",
                "source_role": "fit",
                "point_identity": "T20_600ppm",
                "target_ppm": "600",
                "prediction_ppm": "600",
                "error_ppm": "0",
            }
        )
    return rows


def _recommendation_rows():
    return [
        {
            "recommendation_item": "device_001_next_candidate",
            "recommendation": "senco13_temperature_terms_pressure_zero",
        },
        {
            "recommendation_item": "device_002_next_candidate",
            "recommendation": "senco13_temperature_terms_pressure_zero",
        },
        {
            "recommendation_item": "device_003_next_candidate",
            "recommendation": "senco13_temperature_terms_pressure_zero",
        },
    ]


def test_common_mode_audit_flags_same_sign_multi_device_target(tmp_path):
    fit_path = tmp_path / "fit.csv"
    pred_path = tmp_path / "pred.csv"
    reco_path = tmp_path / "reco.csv"
    _write_csv(fit_path, _fit_rows())
    _write_csv(pred_path, _prediction_rows())
    _write_csv(reco_path, _recommendation_rows())

    tables = build_co2_common_mode_point_audit_tables(
        inputs=AuditInputs(
            fit_points_csv=fit_path,
            predictions_csv=pred_path,
            recommendation_csv=reco_path,
        )
    )
    by_point = {row["point_identity"]: row for row in tables["co2_common_mode_point_audit"]}

    assert by_point["T20_600ppm"]["common_mode_status"] == "common_mode_suspect"
    assert by_point["T20_600ppm"]["root_cause_hypothesis"] == "source_route_target_or_model_common_bias"
    assert by_point["T20_400ppm"]["common_mode_status"] == "not_common_mode"


def test_common_mode_audit_uses_only_recommended_model_per_device(tmp_path):
    fit_path = tmp_path / "fit.csv"
    pred_path = tmp_path / "pred.csv"
    reco_path = tmp_path / "reco.csv"
    _write_csv(fit_path, _fit_rows())
    _write_csv(pred_path, _prediction_rows())
    recommendation = _recommendation_rows()
    recommendation[1]["recommendation"] = "senco13_temperature_terms_pressure_zero_h2o_bridge"
    _write_csv(reco_path, recommendation)

    outputs = write_co2_common_mode_point_audit_report(
        inputs=AuditInputs(
            fit_points_csv=fit_path,
            predictions_csv=pred_path,
            recommendation_csv=reco_path,
        ),
        output_dir=tmp_path / "out",
    )

    assert outputs["markdown"].exists()
    audit_rows = list(csv.DictReader(outputs["co2_common_mode_point_audit_csv"].open(encoding="utf-8-sig")))
    row_600 = {row["point_identity"]: row for row in audit_rows}["T20_600ppm"]
    assert row_600["device_count"] == "3"
    assert float(row_600["mean_error_ppm"]) < 12.0
