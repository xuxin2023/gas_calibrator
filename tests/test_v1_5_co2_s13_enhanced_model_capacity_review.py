import csv

from gas_calibrator.validation.co2_s13_enhanced_model_capacity_review import (
    build_co2_s13_enhanced_model_capacity_review,
)


def _write_csv(path, rows):
    headers = []
    for row in rows:
        for key in row:
            if key not in headers:
                headers.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def test_enhanced_capacity_review_identifies_missing_temperature_ratio_term(tmp_path):
    rows = []
    ratio_center = 1.0
    temp_center_k = 293.15
    for temp_c in (-20.0, 0.0, 20.0, 40.0):
        for ratio, nominal in ((0.35, 100.0), (0.65, 300.0), (0.95, 600.0), (1.25, 900.0)):
            temp_k = temp_c + 273.15
            dr = ratio - ratio_center
            dt = temp_k - temp_center_k
            target = (
                10.0
                + 520.0 * ratio
                - 35.0 * ratio * ratio
                + 0.04 * temp_k
                + 0.20 * ratio * temp_k
                + 420.0 * dr * dr * dt
            )
            rows.append(
                {
                    "component": "co2",
                    "analyzer_device_id": "101",
                    "analyzer_prefix": "GA01",
                    "source_role": "fit",
                    "point_identity": f"T{int(temp_c)}_{int(nominal)}ppm",
                    "target_value": f"{target:.12f}",
                    "ratio": f"{ratio:.12f}",
                    "temperature_c": f"{temp_c:.3f}",
                    "pressure_hpa": "1013.25",
                    "zero_anchor_class": "standard_fit_point",
                }
            )
    evidence = tmp_path / "fit_points.csv"
    _write_csv(evidence, rows)

    tables = build_co2_s13_enhanced_model_capacity_review(
        fit_points_csv=evidence,
        zero_offsets_ppm=(0.0,),
        objectives=("absolute_lstsq",),
    )
    decisions = {row["device_id"]: row for row in tables["capacity_decision"]}
    assert decisions["101"]["recommendation"] == "root_cause_current_senco13_model_capacity_insufficient"

    current = [
        row
        for row in tables["capacity_summary"]
        if row["device_id"] == "101" and row["structure_id"] == "current_writable_senco13"
    ][0]
    enhanced = [
        row
        for row in tables["capacity_summary"]
        if row["device_id"] == "101" and row["structure_id"] == "diagnostic_add_R2T_RT2"
    ][0]
    assert float(current["max_abs_error_ppm"]) > 1.0
    assert float(enhanced["max_abs_error_ppm"]) < 1.0e-8
    assert enhanced["writes_coefficients"] is False
    assert enhanced["controls_water_or_gas_routes"] is False
