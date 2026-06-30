import csv
import json

from gas_calibrator.validation.co2_s13_low_end_temperature_boundary_decision import (
    build_co2_s13_low_end_temperature_boundary_decision,
    write_co2_s13_low_end_temperature_boundary_decision,
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


def _fixture(tmp_path, *, mapping_suspect=0, zero_reviews=1):
    mapping = tmp_path / "mapping.csv"
    capacity = tmp_path / "capacity.csv"
    segments = tmp_path / "segments.csv"
    _write_csv(
        mapping,
        [
            {
                "device_id": "083",
                "ratio_monotonic_violation_count": "0",
                "mapping_suspect_count": str(mapping_suspect),
                "zero_anchor_assigned_value_review_count": str(zero_reviews),
                "low_end_common_bias_group_count": "2",
            }
        ],
    )
    _write_csv(
        capacity,
        [
            {
                "device_id": "083",
                "baseline_max_abs_relative_error_percent": "5.2",
                "best_max_abs_relative_error_percent": "3.3",
                "best_low_end_max_abs_relative_error_percent": "3.3",
                "best_vs_baseline_improvement_fraction": "0.35",
                "low_end_temperature_bias_group_count": "1",
                "low_end_target_bias_group_count": "0",
            }
        ],
    )
    _write_csv(
        segments,
        [
            {
                "device_id": "083",
                "segment_id": "zero_anchor",
                "max_abs_error_ppm": "9.6",
            },
            {
                "device_id": "083",
                "segment_id": "low_nonzero_le_300ppm",
                "max_abs_relative_error_percent": "3.3",
            },
            {
                "device_id": "083",
                "segment_id": "high_gt_300ppm",
                "max_abs_relative_error_percent": "3.1",
            },
        ],
    )
    return mapping, capacity, segments


def test_low_end_temperature_boundary_decision_keeps_zero_anchor_before_s5(tmp_path):
    mapping, capacity, segments = _fixture(tmp_path)

    rows = build_co2_s13_low_end_temperature_boundary_decision(
        ratio_mapping_device_summary_csv=mapping,
        model_capacity_boundary_csv=capacity,
        segment_diagnostic_csv=segments,
    )

    row = rows[0]
    assert row["device_id"] == "083"
    assert row["boundary_diagnosis"] == "zero_anchor_and_low_end_temperature_boundary"
    assert row["uses_s5_output_trim"] is False
    assert row["writes_coefficients"] is False


def test_low_end_temperature_boundary_decision_blocks_real_mapping_suspect(tmp_path):
    mapping, capacity, segments = _fixture(tmp_path, mapping_suspect=1, zero_reviews=0)

    rows = build_co2_s13_low_end_temperature_boundary_decision(
        ratio_mapping_device_summary_csv=mapping,
        model_capacity_boundary_csv=capacity,
        segment_diagnostic_csv=segments,
    )

    assert rows[0]["boundary_diagnosis"] == "route_or_point_mapping_blocker"
    assert rows[0]["recommended_next_action"] == "block_coefficient_write_review_route_certificate_mapping"


def test_low_end_temperature_boundary_decision_writes_artifacts(tmp_path):
    mapping, capacity, segments = _fixture(tmp_path)

    outputs = write_co2_s13_low_end_temperature_boundary_decision(
        ratio_mapping_device_summary_csv=mapping,
        model_capacity_boundary_csv=capacity,
        segment_diagnostic_csv=segments,
        output_dir=tmp_path / "out",
    )

    meta = json.loads(outputs["metadata"].read_text(encoding="utf-8"))
    assert meta["boundary"]["writes_coefficients"] is False
    assert meta["boundary"]["uses_pressure_terms"] is False
    assert "低端温度边界" in outputs["markdown"].read_text(encoding="utf-8")
