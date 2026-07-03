import csv
import json

from gas_calibrator.validation.co2_s13_ratio_target_mapping_audit import (
    build_co2_s13_ratio_target_mapping_audit,
    write_co2_s13_ratio_target_mapping_audit,
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


def _rows(*, monotonic=True):
    ratio_200 = "1.19" if monotonic else "1.23"
    return [
        {
            "device_id": "058",
            "point_identity": "T20_0ppm",
            "temperature_group": "T20",
            "target_ppm": "0",
            "is_zero_anchor": "true",
            "ratio": "1.24",
            "ratio_grade": "A",
            "dryness_grade": "deep_dry",
            "error_ppm": "2",
            "relative_error_percent": "",
        },
        {
            "device_id": "058",
            "point_identity": "T20_100ppm",
            "temperature_group": "T20",
            "target_ppm": "99.94",
            "is_zero_anchor": "false",
            "ratio": "1.21",
            "ratio_grade": "A",
            "dryness_grade": "deep_dry",
            "error_ppm": "-2.5",
            "relative_error_percent": "-2.5",
        },
        {
            "device_id": "058",
            "point_identity": "T20_200ppm",
            "temperature_group": "T20",
            "target_ppm": "200.10",
            "is_zero_anchor": "false",
            "ratio": ratio_200,
            "ratio_grade": "A",
            "dryness_grade": "deep_dry",
            "error_ppm": "-5",
            "relative_error_percent": "-2.5",
        },
        {
            "device_id": "058",
            "point_identity": "T20_400ppm",
            "temperature_group": "T20",
            "target_ppm": "399.56",
            "is_zero_anchor": "false",
            "ratio": "1.10",
            "ratio_grade": "A",
            "dryness_grade": "deep_dry",
            "error_ppm": "-8",
            "relative_error_percent": "-2.0",
        },
    ]


def test_ratio_target_mapping_audit_accepts_monotonic_low_end_bias(tmp_path):
    csv_path = tmp_path / "state.csv"
    _write_csv(csv_path, _rows(monotonic=True))

    tables = build_co2_s13_ratio_target_mapping_audit(
        selected_residual_state_csv=csv_path,
    )

    mono = tables["ratio_target_monotonicity"][0]
    assert mono["ratio_target_status"] == "ratio_target_monotonic"
    low = tables["low_end_common_bias"][0]
    assert low["low_end_bias_status"] == "low_end_common_bias"
    summary = tables["device_summary"][0]
    assert summary["recommended_action"] == "review_s13_low_end_model_boundary"
    assert summary["ratio_monotonic_violation_count"] == 0


def test_ratio_target_mapping_audit_flags_ratio_reversal(tmp_path):
    csv_path = tmp_path / "state.csv"
    _write_csv(csv_path, _rows(monotonic=False))

    tables = build_co2_s13_ratio_target_mapping_audit(
        selected_residual_state_csv=csv_path,
    )

    mono = tables["ratio_target_monotonicity"][0]
    assert mono["ratio_target_status"] == "ratio_target_mapping_suspect"
    assert int(mono["adjacent_violation_count"]) == 1
    assert tables["device_summary"][0]["recommended_action"] == "block_write_review_route_or_point_mapping"


def test_ratio_target_mapping_audit_keeps_zero_assigned_value_separate(tmp_path):
    rows = _rows(monotonic=True)
    rows[0]["target_ppm"] = "10"
    csv_path = tmp_path / "state.csv"
    _write_csv(csv_path, rows)

    tables = build_co2_s13_ratio_target_mapping_audit(
        selected_residual_state_csv=csv_path,
    )

    mapping = tables["point_mapping_audit"][0]
    assert mapping["mapping_status"] == "zero_anchor_assigned_value_review"
    summary = tables["device_summary"][0]
    assert summary["mapping_suspect_count"] == 0
    assert summary["zero_anchor_assigned_value_review_count"] == 1
    assert summary["recommended_action"] == "review_zero_anchor_assigned_value_and_low_end_model_boundary"


def test_ratio_target_mapping_audit_writes_no_write_artifacts(tmp_path):
    csv_path = tmp_path / "state.csv"
    _write_csv(csv_path, _rows(monotonic=True))

    outputs = write_co2_s13_ratio_target_mapping_audit(
        selected_residual_state_csv=csv_path,
        output_dir=tmp_path / "out",
    )

    meta = json.loads(outputs["metadata"].read_text(encoding="utf-8"))
    assert meta["boundary"]["opens_com_ports"] is False
    assert meta["boundary"]["writes_coefficients"] is False
    assert outputs["markdown"].read_bytes()[:3] == b"\xef\xbb\xbf"
    assert "零气锚点" in outputs["markdown"].read_text(encoding="utf-8")
    rows = list(csv.DictReader(outputs["device_summary"].open(encoding="utf-8-sig")))
    assert rows[0]["device_id"] == "058"
