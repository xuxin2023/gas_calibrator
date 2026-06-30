import csv
import json

from gas_calibrator.validation.co2_s13_multistrategy_fit_review import (
    StrategyPass,
    build_co2_s13_multistrategy_fit_review,
    write_co2_s13_multistrategy_fit_review,
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


def _co2_target(ratio, temp_c):
    temp_k = temp_c + 273.15
    return (
        4.5
        + 780.0 * ratio
        + 95.0 * ratio**2
        - 12.0 * ratio**3
        + 0.15 * temp_k
        - 0.0001 * temp_k**2
        + 0.18 * ratio * temp_k
    )


def _fit_rows():
    rows = []
    for temp_c in (-20.0, 0.0, 20.0, 40.0):
        rows.append(
            {
                "component": "co2",
                "analyzer_device_id": "058",
                "analyzer_prefix": "GA01",
                "source_role": "fit",
                "point_identity": f"T{temp_c:g}_0ppm",
                "target_value": "0.0",
                "zero_anchor_class": "estimated_zero_anchor",
                "target_uncertainty_ppm": "8.0",
                "ratio": f"{0.001 + 0.000012 * temp_c:.12f}",
                "temperature_c": f"{temp_c:.3f}",
                "pressure_hpa": "1013.2",
                "co2_ratio_f_std": "0.0002",
                "dewpoint_c_mean": "-31.5",
            }
        )
        for ratio in (0.07, 0.16, 0.31, 0.50, 0.74, 0.95):
            target = _co2_target(ratio, temp_c)
            rows.append(
                {
                    "component": "co2",
                    "analyzer_device_id": "058",
                    "analyzer_prefix": "GA01",
                    "source_role": "fit",
                    "point_identity": f"T{temp_c:g}_R{ratio:g}",
                    "target_value": f"{target:.12f}",
                    "ratio": f"{ratio:.12f}",
                    "temperature_c": f"{temp_c:.3f}",
                    "pressure_hpa": "1013.2",
                    "co2_ratio_f_std": "0.0002",
                    "dewpoint_c_mean": "-31.5",
                }
            )
    return rows


def test_multistrategy_review_selects_no_pressure_no_s5_candidate(tmp_path):
    evidence = tmp_path / "fit_points.csv"
    _write_csv(evidence, _fit_rows())

    tables = build_co2_s13_multistrategy_fit_review(
        fit_points_csv=evidence,
        strategy_passes=(
            StrategyPass(
                profile_id="baseline_full_temp_absolute_zero0",
                description="baseline",
                structures=("core_plus_full_temp",),
                objectives=("absolute_lstsq",),
                zero_offsets_ppm=(0.0,),
            ),
            StrategyPass(
                profile_id="low_end_priority_x5",
                description="low end",
                structures=("core_plus_full_temp",),
                objectives=("low_end_priority_lstsq",),
                zero_offsets_ppm=(0.0, 5.0),
                low_end_multiplier=5.0,
            ),
            StrategyPass(
                profile_id="ratio_only_diagnostic",
                description="diagnostic",
                structures=("core_ratio_only_diagnostic",),
                objectives=("absolute_lstsq",),
                zero_offsets_ppm=(0.0,),
            ),
        ),
    )

    best = tables["best_by_device"][0]
    assert best["device_id"] == "058"
    assert best["structure_id"] == "core_plus_full_temp"
    assert best["uses_pressure_terms"] is False
    assert best["uses_s5_output_trim"] is False
    assert best["writes_coefficients"] is False
    assert best["auto_write_allowed"] is False
    assert tables["top_candidates"]
    assert tables["best_residuals"]
    assert tables["segment_summary"]
    segments = {row["segment_id"] for row in tables["segment_summary"]}
    assert "zero_anchor" in segments
    assert "low_nonzero" in segments
    assert "high_700_plus" in segments
    assert tables["s5_best_by_device"]
    s5_best = tables["s5_best_by_device"][0]
    assert s5_best["uses_s5_output_trim"] is True
    assert s5_best["writes_coefficients"] is False
    assert s5_best["auto_write_allowed"] is False
    assert s5_best["s5_command_preview"].startswith("SENCO5,YGAS,FFF,")


def test_multistrategy_review_outputs_artifacts_with_chinese_markdown(tmp_path):
    evidence = tmp_path / "fit_points.csv"
    _write_csv(evidence, _fit_rows())

    outputs = write_co2_s13_multistrategy_fit_review(
        fit_points_csv=evidence,
        output_dir=tmp_path / "out",
        top_n=3,
    )

    meta = json.loads(outputs["metadata"].read_text(encoding="utf-8"))
    assert meta["boundary"] == {
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "uses_pressure_terms": False,
        "uses_s5_output_trim": "review_only_no_write",
        "not_real_acceptance_evidence": True,
    }
    best = list(csv.DictReader(outputs["best_by_device"].open(encoding="utf-8-sig")))
    assert best
    s5_best = list(csv.DictReader(outputs["s5_best_by_device"].open(encoding="utf-8-sig")))
    assert s5_best
    assert s5_best[0]["s5_command_preview"].startswith("SENCO5,YGAS,FFF,")
    assert best[0]["s1_payload_scientific"]
    assert best[0]["s3_payload_scientific"]
    text = outputs["markdown"].read_text(encoding="utf-8-sig")
    assert "V1.5 CO2 S1/S3 多策略拟合比较评审" in text
    assert "S5 输出层线性修正不参与" in text
