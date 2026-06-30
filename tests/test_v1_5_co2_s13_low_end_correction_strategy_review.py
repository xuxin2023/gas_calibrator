import csv
import json

from gas_calibrator.validation.co2_s13_low_end_correction_strategy_review import (
    build_co2_s13_low_end_correction_strategy_review,
    write_co2_s13_low_end_correction_strategy_review,
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


def _target_ppm(ratio, temp_c):
    temp_k = temp_c + 273.15
    return (
        7.0
        + 820.0 * ratio
        + 55.0 * ratio**2
        - 4.0 * ratio**3
        + 0.09 * temp_k
        - 0.00006 * temp_k**2
        + 0.12 * ratio * temp_k
    )


def _fit_rows():
    rows = []
    ratios = [
        ("100ppm", 0.08),
        ("200ppm", 0.18),
        ("300ppm", 0.30),
        ("600ppm", 0.60),
        ("900ppm", 0.90),
        ("1000ppm", 1.00),
    ]
    for temp_c in (-20.0, 0.0, 20.0, 30.0, 40.0):
        rows.append(
            {
                "component": "co2",
                "analyzer_device_id": "058",
                "analyzer_prefix": "GA01",
                "source_role": "fit",
                "point_identity": f"T{temp_c:g}_0ppm",
                "target_value": "0.0",
                "zero_anchor_class": "estimated_zero_anchor",
                "ratio": f"{0.0015 + 0.00001 * temp_c:.12f}",
                "temperature_c": f"{temp_c:.3f}",
                "pressure_hpa": "1013.2",
                "co2_ratio_f_std": "0.00020",
                "dewpoint_c_mean": "-31.0",
            }
        )
        for label, ratio in ratios:
            target = _target_ppm(ratio, temp_c)
            identity = f"T{temp_c:g}_{label}"
            if identity == "T20_100ppm":
                target -= 8.0
            rows.append(
                {
                    "component": "co2",
                    "analyzer_device_id": "058",
                    "analyzer_prefix": "GA01",
                    "source_role": "fit",
                    "point_identity": identity,
                    "target_value": f"{target:.12f}",
                    "ratio": f"{ratio:.12f}",
                    "temperature_c": f"{temp_c:.3f}",
                    "pressure_hpa": "1013.2",
                    "co2_ratio_f_std": "0.00020",
                    "dewpoint_c_mean": "-31.0",
                }
            )
    return rows


def test_low_end_strategy_review_stays_no_write_no_pressure_no_s5(tmp_path):
    evidence = tmp_path / "fit_points.csv"
    _write_csv(evidence, _fit_rows())

    tables = build_co2_s13_low_end_correction_strategy_review(
        fit_points_csv=evidence,
        zero_offsets_ppm=(0.0, 5.0),
        low_end_multipliers=(1.0, 5.0),
        diagnostic_holdout_points=("T20_100ppm",),
    )

    assert tables["run_summary"][0]["opens_com_ports"] is False
    assert tables["run_summary"][0]["writes_coefficients"] is False
    assert tables["run_summary"][0]["uses_pressure_terms"] is False
    assert tables["run_summary"][0]["uses_s5_output_trim"] is False
    best = tables["best_regular_by_device"][0]
    assert best["device_id"] == "058"
    assert best["auto_write_allowed"] is False
    assert best["s1_payload_scientific"]
    assert best["s3_payload_scientific"]
    assert all(row["uses_pressure_terms"] is False for row in tables["strategy_summary"])
    assert all(row["uses_s5_output_trim"] is False for row in tables["strategy_summary"])


def test_holdout_is_diagnostic_and_not_auto_exclusion(tmp_path):
    evidence = tmp_path / "fit_points.csv"
    _write_csv(evidence, _fit_rows())

    tables = build_co2_s13_low_end_correction_strategy_review(
        fit_points_csv=evidence,
        zero_offsets_ppm=(0.0,),
        low_end_multipliers=(1.0, 5.0),
        diagnostic_holdout_points=("T20_100ppm",),
    )

    holdout = tables["diagnostic_holdout_review"][0]
    assert holdout["held_out_point_identity"] == "T20_100ppm"
    assert holdout["diagnostic_only"] is True
    assert holdout["auto_exclude_allowed"] is False
    assert holdout["diagnostic_interpretation"]
    assert "剔除" not in holdout["diagnostic_interpretation"]


def test_low_end_strategy_review_writes_utf8_chinese_report(tmp_path):
    evidence = tmp_path / "fit_points.csv"
    _write_csv(evidence, _fit_rows())

    outputs = write_co2_s13_low_end_correction_strategy_review(
        fit_points_csv=evidence,
        output_dir=tmp_path / "review",
        zero_offsets_ppm=(0.0, 5.0),
        low_end_multipliers=(1.0, 5.0),
        diagnostic_holdout_points=("T20_100ppm",),
    )

    meta = json.loads(outputs["metadata"].read_text(encoding="utf-8"))
    assert meta["boundary"]["opens_com_ports"] is False
    assert meta["boundary"]["writes_coefficients"] is False
    assert meta["boundary"]["uses_pressure_terms"] is False
    assert meta["boundary"]["uses_s5_output_trim"] is False
    text = outputs["markdown"].read_text(encoding="utf-8-sig")
    assert "V1.5 CO2 S1/S3 低端修正策略评审" in text
    assert "S5 输出层修正不参与本轮主模型判断" in text
    assert "乱码" not in text
