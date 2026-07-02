import csv
import json

from gas_calibrator.validation.co2_intercept_root_cause import (
    Co2InterceptRootCauseConfig,
    build_co2_intercept_root_cause_tables,
)


def _write_csv(path, rows):
    keys = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def _candidate_dir(tmp_path, intercept=1.0):
    root = tmp_path / "candidate"
    root.mkdir()
    rows = [
        {"component": "co2", "analyzer_device_id": "022", "term": "intercept", "coefficient": intercept},
        {"component": "co2", "analyzer_device_id": "022", "term": "R", "coefficient": 2.0},
        {"component": "co2", "analyzer_device_id": "022", "term": "R2", "coefficient": 3.0},
        {"component": "co2", "analyzer_device_id": "022", "term": "R3", "coefficient": 4.0},
        {"component": "co2", "analyzer_device_id": "022", "term": "T", "coefficient": 5.0},
        {"component": "co2", "analyzer_device_id": "022", "term": "T2", "coefficient": 6.0},
        {"component": "co2", "analyzer_device_id": "022", "term": "RT", "coefficient": 7.0},
    ]
    _write_csv(root / "candidate_coefficients.csv", rows)
    _write_csv(
        root / "candidate_fit_residuals.csv",
        [
            {
                "component": "co2",
                "analyzer_device_id": "022",
                "point_identity": "p_T20_900ppm_fit",
                "target_value": 900.0,
                "ratio": 1.2500,
                "temperature_c": 22.5,
                "error": 0.0,
            }
        ],
    )
    return root


def _current_getco(path, intercept=1.0):
    path.write_text(
        json.dumps(
            {
                "022": {
                    "GETCO1_before_live": [intercept, 2.0, 3.0, 4.0, 0.0, 0.0],
                    "GETCO3_before_live": [5.0, 6.0, 7.0, 0.0, 0.0, 0.0],
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return path


def test_intercept_root_cause_marks_predominant_offset_when_getco_matches_candidate(tmp_path):
    rows = []
    for point, target in (("p700", 700.0), ("p900", 900.0), ("p1000", 1000.0)):
        rows.append(
            {
                "point": point,
                "device": "022",
                "target_ppm": target,
                "co2_ppm": target + 20.0,
                "h2o_mmol_mol": 0.1,
                "pressure_kpa": 101.3,
            }
        )
    point_csv = tmp_path / "points.csv"
    _write_csv(point_csv, rows)

    tables = build_co2_intercept_root_cause_tables(
        point_errors_csv=point_csv,
        candidate_dir=_candidate_dir(tmp_path, intercept=1.0),
        current_getco_json=_current_getco(tmp_path / "getco.json", intercept=1.0),
        cfg=Co2InterceptRootCauseConfig(target_device_ids=("022",), exclude_device_ids=()),
    )

    summary = tables["device_summary"][0]
    assert summary["root_cause_class"] == "predominant_intercept_offset"
    assert summary["current_vs_latest_candidate_status"] == "matches_latest_candidate"
    assert abs(float(summary["offset_only_C0_delta_ppm"]) + 20.0) < 1.0e-9
    assert float(summary["offset_only_max_abs_error_pct"]) == 0.0
    assert summary["h2o_status"] == "h2o_too_low_to_explain_bias"


def test_intercept_root_cause_blocks_when_current_getco_is_not_latest_candidate(tmp_path):
    rows = [
        {
            "point": "p900",
            "device": "022",
            "target_ppm": 900.0,
            "co2_ppm": 920.0,
            "h2o_mmol_mol": 0.0,
            "pressure_kpa": 101.3,
        }
    ]
    point_csv = tmp_path / "points.csv"
    _write_csv(point_csv, rows)

    tables = build_co2_intercept_root_cause_tables(
        point_errors_csv=point_csv,
        candidate_dir=_candidate_dir(tmp_path, intercept=21.0),
        current_getco_json=_current_getco(tmp_path / "getco.json", intercept=1.0),
        cfg=Co2InterceptRootCauseConfig(target_device_ids=("022",), exclude_device_ids=()),
    )

    summary = tables["device_summary"][0]
    coeffs = {row["term"]: row for row in tables["coefficient_deltas"]}
    assert summary["root_cause_class"] == "current_getco_not_latest_candidate_but_replay_missing"
    assert summary["current_vs_latest_candidate_status"] == "different_from_latest_candidate"
    assert float(coeffs["intercept"]["latest_minus_current"]) == 20.0


def test_intercept_root_cause_flags_same_point_ratio_state_conflict(tmp_path):
    rows = [
        {
            "point": "open_flow_900ppm",
            "device": "022",
            "target_ppm": 900.0,
            "co2_ppm": 930.0,
            "h2o_mmol_mol": 0.0,
            "pressure_kpa": 101.3,
            "r_co2": 1.2460,
            "t1_c": 22.4,
        }
    ]
    point_csv = tmp_path / "points.csv"
    _write_csv(point_csv, rows)

    tables = build_co2_intercept_root_cause_tables(
        point_errors_csv=point_csv,
        candidate_dir=_candidate_dir(tmp_path, intercept=1.0),
        current_getco_json=_current_getco(tmp_path / "getco.json", intercept=1.0),
        cfg=Co2InterceptRootCauseConfig(target_device_ids=("022",), exclude_device_ids=()),
    )

    summary = tables["device_summary"][0]
    ratio_rows = tables["ratio_state_diagnostics"]
    assert summary["root_cause_class"] == "ratio_state_conflict_between_fit_and_retest"
    assert summary["ratio_state_status"] == "ratio_state_conflict_blocks_final_write"
    assert ratio_rows[0]["ratio_state_status"] == "ratio_state_conflict_blocks_final_write"


def test_intercept_root_cause_reports_zero_anchor_and_temperature_weighting(tmp_path):
    point_csv = tmp_path / "points.csv"
    _write_csv(
        point_csv,
        [
            {
                "point": "p100",
                "device": "022",
                "target_ppm": 100.0,
                "co2_ppm": 101.0,
                "h2o_mmol_mol": 0.0,
                "pressure_kpa": 101.3,
            }
        ],
    )

    candidate = tmp_path / "candidate_grid"
    candidate.mkdir()
    _write_csv(
        candidate / "candidate_coefficients.csv",
        [
            {"component": "co2", "analyzer_device_id": "022", "term": "intercept", "coefficient": 0.0},
            {"component": "co2", "analyzer_device_id": "022", "term": "R", "coefficient": 1.0},
        ],
    )
    _write_csv(
        candidate / "candidate_fit_residuals.csv",
        [
            {
                "component": "co2",
                "analyzer_device_id": "022",
                "point_identity": "p000_T20_0ppm_fit",
                "target_value": 0.0,
                "ratio": 1.50,
                "temperature_c": 20.0,
                "error": 0.0,
            },
            {
                "component": "co2",
                "analyzer_device_id": "022",
                "point_identity": "p001_T20_100ppm_fit",
                "target_value": 100.0,
                "ratio": 1.49,
                "temperature_c": 20.0,
                "error": 0.0,
            },
            {
                "component": "co2",
                "analyzer_device_id": "022",
                "point_identity": "p002_T20_200ppm_fit",
                "target_value": 200.0,
                "ratio": 1.48,
                "temperature_c": 20.0,
                "error": 0.0,
            },
            {
                "component": "co2",
                "analyzer_device_id": "022",
                "point_identity": "p003_T40_900ppm_fit",
                "target_value": 900.0,
                "ratio": 1.25,
                "temperature_c": 40.0,
                "error": 0.0,
            },
        ],
    )

    tables = build_co2_intercept_root_cause_tables(
        point_errors_csv=point_csv,
        candidate_dir=candidate,
        current_getco_json=None,
        cfg=Co2InterceptRootCauseConfig(target_device_ids=("022",), exclude_device_ids=()),
    )

    summary = tables["device_summary"][0]
    assert summary["candidate_fit_grid_status"] == "imbalanced_temperature_target_grid_blocks_final_write"
    assert summary["candidate_fit_zero_anchor_count"] == 1
    assert summary["candidate_fit_zero_anchor_status"] == "zero_anchor_present_needs_certificate_trace_review"
    assert summary["candidate_fit_dominant_temperature_group"] == "20"
    assert float(summary["candidate_fit_dominant_temperature_weight_fraction"]) == 0.75
    coverage = {row["temperature_group"]: row for row in tables["candidate_fit_coverage"]}
    assert float(coverage["20"]["target_count_fraction_of_device_fit"]) == 0.75
    assert coverage["40"]["temperature_weighting_status"] == "imbalanced_temperature_target_grid_blocks_final_write"


def test_intercept_root_cause_accepts_candidate_residual_table_columns(tmp_path):
    point_csv = tmp_path / "candidate_fit_residuals.csv"
    _write_csv(
        point_csv,
        [
            {
                "component": "co2",
                "analyzer_device_id": "022",
                "point_identity": "p001_T20_900ppm_fit",
                "target_value": "900.0",
                "prediction": "918.0",
                "error": "18.0",
                "ratio": "1.25",
                "temperature_c": "20.0",
                "pressure_hpa": "1013.0",
                "h2o_mmol": "0.4",
            }
        ],
    )

    tables = build_co2_intercept_root_cause_tables(
        point_errors_csv=point_csv,
        candidate_dir=None,
        current_getco_json=None,
        cfg=Co2InterceptRootCauseConfig(target_device_ids=("022",), exclude_device_ids=()),
    )

    summary = tables["device_summary"][0]
    point = tables["point_diagnostics"][0]
    assert summary["point_count"] == 1
    assert float(summary["observed_mean_error_ppm"]) == 18.0
    assert float(point["pressure_kpa"]) == 101.3
    assert point["h2o_mmol_mol"] == "0.4"
