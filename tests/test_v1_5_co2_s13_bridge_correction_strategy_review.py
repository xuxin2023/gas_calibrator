import csv
import json

from gas_calibrator.validation.co2_s13_bridge_correction_strategy_review import (
    build_co2_s13_bridge_correction_strategy_review,
    write_co2_s13_bridge_correction_strategy_review,
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


def _state_rows():
    return [
        {
            "device_id": "058",
            "point_identity": "T20_0ppm",
            "temperature_group": "T20",
            "target_ppm": "0",
            "is_zero_anchor": "true",
            "error_ppm": "2",
            "relative_error_percent": "",
            "ratio_grade": "A",
            "dryness_grade": "deep_dry",
        },
        {
            "device_id": "058",
            "point_identity": "T20_100ppm",
            "temperature_group": "T20",
            "target_ppm": "100",
            "is_zero_anchor": "false",
            "error_ppm": "-3",
            "relative_error_percent": "-3",
            "ratio_grade": "A",
            "dryness_grade": "deep_dry",
        },
        {
            "device_id": "058",
            "point_identity": "T20_200ppm",
            "temperature_group": "T20",
            "target_ppm": "200",
            "is_zero_anchor": "false",
            "error_ppm": "-6",
            "relative_error_percent": "-3",
            "ratio_grade": "A",
            "dryness_grade": "deep_dry",
        },
        {
            "device_id": "058",
            "point_identity": "T30_100ppm",
            "temperature_group": "T30",
            "target_ppm": "100",
            "is_zero_anchor": "false",
            "error_ppm": "4",
            "relative_error_percent": "4",
            "ratio_grade": "A",
            "dryness_grade": "deep_dry",
        },
        {
            "device_id": "058",
            "point_identity": "T30_200ppm",
            "temperature_group": "T30",
            "target_ppm": "200",
            "is_zero_anchor": "false",
            "error_ppm": "8",
            "relative_error_percent": "4",
            "ratio_grade": "A",
            "dryness_grade": "deep_dry",
        },
    ]


def test_bridge_correction_review_compares_relative_segment_strategy(tmp_path):
    state_csv = tmp_path / "state.csv"
    _write_csv(state_csv, _state_rows())

    tables = build_co2_s13_bridge_correction_strategy_review(
        selected_residual_state_csv=state_csv,
    )

    summary = {
        row["candidate_id"]: row
        for row in tables["candidate_summary"]
        if row["device_id"] == "058"
    }
    baseline = float(summary["baseline_selected_s13"]["max_abs_relative_error_percent"])
    relative_bridge = float(
        summary["temperature_segment_relative_bridge_loo"]["max_abs_relative_error_percent"]
    )
    assert baseline == 4.0
    assert relative_bridge == 0.0
    assert summary["s5_relative_weighted_rounded"]["s5_c1"] != ""
    assert tables["device_recommendations"][0]["recommended_action"] in {
        "refit_s13_after_bridge_model_review",
        "s5_can_reduce_display_error_after_s13_review",
    }


def test_bridge_correction_review_writes_no_write_artifacts(tmp_path):
    state_csv = tmp_path / "state.csv"
    _write_csv(state_csv, _state_rows())

    outputs = write_co2_s13_bridge_correction_strategy_review(
        selected_residual_state_csv=state_csv,
        output_dir=tmp_path / "out",
    )

    meta = json.loads(outputs["metadata"].read_text(encoding="utf-8"))
    assert meta["boundary"]["opens_com_ports"] is False
    assert meta["boundary"]["writes_coefficients"] is False
    assert meta["boundary"]["s5_is_theoretical_output_trim_only"] is True
    assert outputs["markdown"].read_bytes()[:3] == b"\xef\xbb\xbf"
    rows = list(csv.DictReader(outputs["candidate_summary"].open(encoding="utf-8-sig")))
    assert {row["candidate_id"] for row in rows} >= {
        "baseline_selected_s13",
        "temperature_segment_relative_bridge_loo",
        "s5_relative_weighted_rounded",
    }
