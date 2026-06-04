import csv
from pathlib import Path

from gas_calibrator.validation.co2_post_h2o_diagnostic import (
    build_co2_post_h2o_diagnostic,
    write_co2_post_h2o_diagnostic,
)


def _write_csv(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    keys = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def test_post_h2o_diagnostic_identifies_ratio_shift_not_h2o(tmp_path):
    verification = tmp_path / "verify.csv"
    fit = tmp_path / "fit.csv"
    replay = tmp_path / "replay.csv"
    compare = tmp_path / "compare.csv"
    _write_csv(
        verification,
        [
            {
                "point_run_id": "p004_T20_900ppm_verification",
                "source_nominal_ppm": "900",
                "certificate_co2_ppm": "897.04",
                "device_id": "022",
                "measured_co2_ppm": "938.35",
                "error_pct": "4.6",
                "status": "fail",
                "h2o_mmol_mol": "0.3",
                "co2_ratio_f": "1.231",
                "chamber_temp_c": "22.5",
            }
        ],
    )
    _write_csv(
        fit,
        [
            {
                "analyzer_device_id": "022",
                "sample_index": "p024_T20_900ppm_fit:open_flow_900ppm",
                "target_value": "897.04",
                "ratio": "1.240",
                "temperature_c": "23.0",
                "prediction": "899.0",
            }
        ],
    )
    _write_csv(
        replay,
        [
            {
                "source": "today_T20_900_fail",
                "device_id": "022",
                "replay_minus_measured_ppm": "0.02",
            }
        ],
    )
    _write_csv(
        compare,
        [
            {
                "device_id": "022",
                "yesterday_900_co2_ppm": "899.3",
                "today_900_co2_ppm": "938.3",
                "delta_error_pct": "4.3",
                "delta_R_CO2": "-0.008",
                "delta_T1": "-8.0",
            }
        ],
    )

    payload = build_co2_post_h2o_diagnostic(
        verification_summary_csv=verification,
        fit_residuals_csv=fit,
        firmware_replay_csv=replay,
        yesterday_today_csv=compare,
        target_device_ids=["022"],
    )

    summary = payload["run_summary"][0]
    point = payload["point_diagnostics"][0]
    device = payload["device_summary"][0]
    assert summary["overall_pass"] is False
    assert summary["firmware_replay_consistent"] is True
    assert summary["h2o_can_explain_current_co2_error"] is False
    assert "h2o_final_dry_correction_too_small_to_explain_error" in point["diagnostic_driver"]
    assert "co2_ratio_shift_vs_original_T20_fit_sample" in point["diagnostic_driver"]
    assert device["conclusion"] == "blocked_ratio_temperature_or_physical_state_shift"


def test_post_h2o_diagnostic_writes_artifacts(tmp_path):
    verification = tmp_path / "verify.csv"
    _write_csv(
        verification,
        [
            {
                "source_nominal_ppm": "900",
                "certificate_co2_ppm": "897.04",
                "device_id": "051",
                "measured_co2_ppm": "903.0",
                "error_pct": "0.66",
                "status": "pass",
            }
        ],
    )

    outputs = write_co2_post_h2o_diagnostic(
        verification_summary_csv=verification,
        output_dir=tmp_path / "out",
        target_device_ids=["051"],
    )

    assert outputs["summary_csv"].exists()
    assert outputs["device_csv"].exists()
    assert outputs["points_csv"].exists()
    assert outputs["markdown"].read_text(encoding="utf-8").startswith("# V1.5 CO2 Post-H2O")
