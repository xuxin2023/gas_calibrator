import csv

from gas_calibrator.tools.export_v1_5_fit_input_quality import main as cli_main
from gas_calibrator.validation.v1_5_fit_input_quality import (
    FitInputQualityConfig,
    build_fit_input_quality_tables,
    write_fit_input_quality_report,
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


def _read_csv(path):
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def test_fit_input_quality_marks_target_devices_a_and_excludes_scope_devices(tmp_path):
    co2_policy = tmp_path / "co2_policy.csv"
    co2_residuals = tmp_path / "co2_residuals.csv"
    h2o_policy = tmp_path / "h2o_policy.csv"
    h2o_residuals = tmp_path / "h2o_residuals.csv"
    h2o_inputs = tmp_path / "h2o_inputs.csv"

    _write_csv(
        co2_policy,
        [
            {
                "analyzer_device_id": "022",
                "candidate_status": "fit_ready_requires_verification",
                "allowed_to_fit": "True",
                "fit_sample_count": "20",
                "fit_point_count": "2",
                "formal_a_grade_count": "20",
                "preparation_rejected_count": "0",
            },
            {
                "analyzer_device_id": "023",
                "candidate_status": "verification_failed",
                "allowed_to_fit": "True",
                "fit_sample_count": "20",
                "fit_point_count": "2",
                "formal_a_grade_count": "20",
                "preparation_rejected_count": "0",
            },
        ],
    )
    _write_csv(
        co2_residuals,
        [
            {"analyzer_device_id": "022", "point_identity": "co2_100", "residual_role": "fit", "target_value": "99.94", "ratio": "1.4"},
            {"analyzer_device_id": "022", "point_identity": "co2_900", "residual_role": "fit", "target_value": "897.04", "ratio": "1.2"},
            {"analyzer_device_id": "023", "point_identity": "co2_100", "residual_role": "fit", "target_value": "99.94", "ratio": "1.4"},
        ],
    )
    _write_csv(
        h2o_policy,
        [
            {
                "analyzer_device_id": "022",
                "candidate_status": "candidate_fit_review_required",
                "complete_point_count": "10",
                "complete_wet_point_count": "6",
                "complete_dry_anchor_count": "4",
                "rejected_point_count": "0",
                "fit_design_qc": "review",
                "warning_reasons": "side_channel_cache_age_warning_kept_as_evidence_not_fit_blocker",
            },
        ],
    )
    _write_csv(
        h2o_residuals,
        [
            {
                "analyzer_device_id": "022",
                "point_run_id": "h2o_wet",
                "sample_role": "fit",
                "reference_h2o_mmol": "12.0",
                "h2o_ratio_f": "0.7",
                "chamber_temp_c": "20.0",
            }
        ],
    )
    _write_csv(
        h2o_inputs,
        [{"analyzer_device_id": "023", "point_run_id": "h2o_excluded", "sample_role": "fit"}],
    )

    tables = build_fit_input_quality_tables(
        co2_policy_csv=co2_policy,
        co2_residuals_csv=co2_residuals,
        h2o_policy_csv=h2o_policy,
        h2o_residuals_csv=h2o_residuals,
        h2o_point_inputs_csv=h2o_inputs,
        cfg=FitInputQualityConfig(target_device_ids=("022",), excluded_device_ids=("023",), co2_min_fit_samples=10),
    )

    devices = {(row["component"], row["device_id"]): row for row in tables["device_quality"]}
    assert devices[("co2", "022")]["fit_input_grade"] == "A"
    assert devices[("h2o", "022")]["fit_input_grade"] == "A"
    assert "h2o_model_fit_qc_review_not_input_quality_reject" in devices[("h2o", "022")]["warning_reasons"]
    assert tables["summary"][0]["run_status"] == "pass"
    excluded = tables["excluded_evidence"]
    assert {row["device_id"] for row in excluded} == {"023"}
    assert all(row["fit_input_grade"] == "EXCLUDED" for row in excluded)


def test_fit_input_quality_keeps_scope_exclusion_visible_without_source_rows(tmp_path):
    co2_policy = tmp_path / "co2_policy.csv"
    co2_residuals = tmp_path / "co2_residuals.csv"
    h2o_policy = tmp_path / "h2o_policy.csv"
    h2o_residuals = tmp_path / "h2o_residuals.csv"

    _write_csv(
        co2_policy,
        [
            {
                "analyzer_device_id": "022",
                "allowed_to_fit": "True",
                "fit_sample_count": "10",
                "formal_a_grade_count": "10",
                "preparation_rejected_count": "0",
            }
        ],
    )
    _write_csv(co2_residuals, [{"analyzer_device_id": "022", "point_identity": "co2_100"}])
    _write_csv(
        h2o_policy,
        [
            {
                "analyzer_device_id": "022",
                "complete_point_count": "8",
                "complete_wet_point_count": "3",
                "complete_dry_anchor_count": "5",
                "rejected_point_count": "0",
            }
        ],
    )
    _write_csv(h2o_residuals, [{"analyzer_device_id": "022", "point_run_id": "h2o_wet"}])

    tables = build_fit_input_quality_tables(
        co2_policy_csv=co2_policy,
        co2_residuals_csv=co2_residuals,
        h2o_policy_csv=h2o_policy,
        h2o_residuals_csv=h2o_residuals,
        cfg=FitInputQualityConfig(target_device_ids=("022",), excluded_device_ids=("023",)),
    )

    excluded = tables["excluded_evidence"]
    assert len(excluded) == 1
    assert excluded[0]["device_id"] == "023"
    assert excluded[0]["source_table"] == "scope_exclusion"
    assert excluded[0]["exclude_reason"] == "device_excluded_from_current_calibration_scope_no_source_rows_found"


def test_fit_input_quality_writer_and_cli_are_offline(tmp_path):
    co2_policy = tmp_path / "co2_policy.csv"
    co2_residuals = tmp_path / "co2_residuals.csv"
    h2o_policy = tmp_path / "h2o_policy.csv"
    h2o_residuals = tmp_path / "h2o_residuals.csv"

    _write_csv(
        co2_policy,
        [
            {
                "analyzer_device_id": "022",
                "allowed_to_fit": "True",
                "fit_sample_count": "10",
                "fit_point_count": "1",
                "formal_a_grade_count": "10",
                "preparation_rejected_count": "0",
            }
        ],
    )
    _write_csv(co2_residuals, [{"analyzer_device_id": "022", "point_identity": "co2"}])
    _write_csv(
        h2o_policy,
        [
            {
                "analyzer_device_id": "022",
                "complete_point_count": "8",
                "complete_wet_point_count": "3",
                "complete_dry_anchor_count": "5",
                "rejected_point_count": "0",
            }
        ],
    )
    _write_csv(h2o_residuals, [{"analyzer_device_id": "022", "point_run_id": "h2o"}])

    output = tmp_path / "out"
    outputs = write_fit_input_quality_report(
        co2_policy_csv=co2_policy,
        co2_residuals_csv=co2_residuals,
        h2o_policy_csv=h2o_policy,
        h2o_residuals_csv=h2o_residuals,
        output_dir=output,
        cfg=FitInputQualityConfig(target_device_ids=("022",), excluded_device_ids=()),
    )
    assert outputs["markdown"].exists()
    summary = _read_csv(output / "v1_5_fit_input_quality_summary.csv")
    assert summary[0]["opens_com_ports"] == "False"
    assert summary[0]["controls_water_or_gas_routes"] == "False"
    assert summary[0]["writes_coefficients"] == "False"

    cli_output = tmp_path / "cli"
    rc = cli_main(
        [
            "--co2-policy-csv",
            str(co2_policy),
            "--co2-residuals-csv",
            str(co2_residuals),
            "--h2o-policy-csv",
            str(h2o_policy),
            "--h2o-residuals-csv",
            str(h2o_residuals),
            "--output-dir",
            str(cli_output),
            "--target-device-id",
            "022",
        ]
    )
    assert rc == 0
    assert (cli_output / "v1_5_fit_input_quality_devices.csv").exists()
