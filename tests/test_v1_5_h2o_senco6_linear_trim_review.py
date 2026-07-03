import csv

from gas_calibrator.tools.export_v1_5_h2o_senco6_linear_trim_review import main as cli_main
from gas_calibrator.validation.h2o_senco6_linear_trim_review import (
    H2oSenco6LinearTrimConfig,
    build_h2o_senco6_linear_trim_review,
)


def _write_csv(path, rows):
    header = []
    for row in rows:
        for key in row:
            if key not in header:
                header.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path):
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def test_h2o_senco6_linear_trim_review_fits_dewpoint_reference_affine(tmp_path):
    source = tmp_path / "h2o_verify.csv"
    _write_csv(
        source,
        [
            {
                "device_id": "022",
                "reference_h2o_mmol": "7.7745",
                "measured_h2o_mmol": "7.1372",
                "source": "postwrite_firmware_output_verification",
            },
            {
                "device_id": "022",
                "reference_h2o_mmol": "16.6435",
                "measured_h2o_mmol": "15.8275",
                "source": "postwrite_firmware_output_verification",
            },
        ],
    )

    tables = build_h2o_senco6_linear_trim_review(
        verification_summary_csv=source,
        cfg=H2oSenco6LinearTrimConfig(target_device_ids=("022",), acceptance_pct=2.0),
    )

    summary = tables["candidate_summary"][0]
    coeff = tables["candidate_coefficients"][0]

    assert summary["candidate_status"] == "review_ready"
    assert summary["input_source_contract"] == "postwrite_firmware_output_verification"
    assert abs(float(summary["candidate_C0"]) - 0.490523) < 0.001
    assert abs(float(summary["candidate_C1"]) - 1.020563) < 0.001
    assert coeff["command_preview"] == "SENCO6,YGAS,FFF,0.7,1.0"
    assert coeff["auto_write_allowed"] is False
    assert tables["run_summary"][0]["opens_com_ports"] is False
    assert tables["run_summary"][0]["writes_coefficients"] is False


def test_h2o_senco6_linear_trim_review_blocks_model_prediction_source(tmp_path):
    source = tmp_path / "h2o_verify.csv"
    _write_csv(
        source,
        [
            {
                "device_id": "022",
                "reference_h2o_mmol": "7.7745",
                "measured_h2o_mmol": "7.1372",
                "senco6_input_contract": "measured_h2o_mmol_is_senco24_main_model_pred_not_current_firmware_reported_output",
            },
            {
                "device_id": "022",
                "reference_h2o_mmol": "16.6435",
                "measured_h2o_mmol": "15.8275",
                "senco6_input_contract": "measured_h2o_mmol_is_senco24_main_model_pred_not_current_firmware_reported_output",
            },
        ],
    )

    tables = build_h2o_senco6_linear_trim_review(
        verification_summary_csv=source,
        cfg=H2oSenco6LinearTrimConfig(target_device_ids=("022",), acceptance_pct=2.0),
    )

    summary = tables["candidate_summary"][0]
    assert summary["candidate_status"] == "blocked"
    assert "senco6_input_not_independent_firmware_output" in summary["blocked_reasons"]
    assert tables["candidate_coefficients"] == []


def test_h2o_senco6_linear_trim_review_blocks_missing_state_comparability_evidence(tmp_path):
    source = tmp_path / "h2o_verify.csv"
    _write_csv(
        source,
        [
            {
                "device_id": "084",
                "reference_h2o_mmol": "7.9",
                "measured_h2o_mmol": "7.9",
                "source": "postwrite_firmware_output_verification",
            },
            {
                "device_id": "084",
                "reference_h2o_mmol": "17.0",
                "measured_h2o_mmol": "17.0",
                "source": "postwrite_firmware_output_verification",
            },
        ],
    )

    tables = build_h2o_senco6_linear_trim_review(
        verification_summary_csv=source,
        cfg=H2oSenco6LinearTrimConfig(
            target_device_ids=("084",),
            acceptance_pct=2.0,
            require_state_comparability_evidence=True,
        ),
    )

    summary = tables["candidate_summary"][0]
    assert summary["candidate_status"] == "blocked"
    assert "missing_state_comparability_evidence" in summary["blocked_reasons"]
    assert tables["candidate_coefficients"] == []


def test_h2o_senco6_linear_trim_review_blocks_ratio_state_delta_outside_fit_support(tmp_path):
    source = tmp_path / "h2o_verify.csv"
    _write_csv(
        source,
        [
            {
                "device_id": "084",
                "reference_h2o_mmol": "7.9",
                "measured_h2o_mmol": "7.9",
                "source": "postwrite_firmware_output_verification",
                "h2o_ratio_delta_vs_fit": "-0.024",
                "temperature_delta_c_vs_fit": "0.5",
            },
            {
                "device_id": "084",
                "reference_h2o_mmol": "17.0",
                "measured_h2o_mmol": "17.0",
                "source": "postwrite_firmware_output_verification",
                "h2o_ratio_delta_vs_fit": "-0.018",
                "temperature_delta_c_vs_fit": "0.4",
            },
        ],
    )

    tables = build_h2o_senco6_linear_trim_review(
        verification_summary_csv=source,
        cfg=H2oSenco6LinearTrimConfig(
            target_device_ids=("084",),
            acceptance_pct=2.0,
            require_state_comparability_evidence=True,
            max_abs_h2o_ratio_delta_vs_fit=0.005,
        ),
    )

    summary = tables["candidate_summary"][0]
    assert summary["candidate_status"] == "blocked"
    assert "h2o_ratio_state_delta_exceeds_fit_support" in summary["blocked_reasons"]
    assert tables["candidate_coefficients"] == []


def test_h2o_senco6_linear_trim_review_accepts_explicit_state_comparability_pass(tmp_path):
    source = tmp_path / "h2o_verify.csv"
    _write_csv(
        source,
        [
            {
                "device_id": "084",
                "reference_h2o_mmol": "7.9",
                "measured_h2o_mmol": "7.9",
                "source": "postwrite_firmware_output_verification",
                "state_comparability_status": "pass",
            },
            {
                "device_id": "084",
                "reference_h2o_mmol": "17.0",
                "measured_h2o_mmol": "17.0",
                "source": "postwrite_firmware_output_verification",
                "state_comparability_status": "pass",
            },
        ],
    )

    tables = build_h2o_senco6_linear_trim_review(
        verification_summary_csv=source,
        cfg=H2oSenco6LinearTrimConfig(
            target_device_ids=("084",),
            acceptance_pct=2.0,
            require_state_comparability_evidence=True,
        ),
    )

    summary = tables["candidate_summary"][0]
    assert summary["candidate_status"] == "review_ready"
    assert summary["requires_state_comparability_evidence"] is True
    assert tables["candidate_coefficients"][0]["requires_state_comparability_evidence"] is True


def test_h2o_senco6_linear_trim_review_blocks_missing_device_point(tmp_path):
    source = tmp_path / "h2o_verify.csv"
    _write_csv(
        source,
        [
            {"device_id": "022", "reference_h2o_mmol": "7.0", "measured_h2o_mmol": "6.5"},
        ],
    )

    tables = build_h2o_senco6_linear_trim_review(
        verification_summary_csv=source,
        cfg=H2oSenco6LinearTrimConfig(target_device_ids=("022",), min_points=2),
    )

    summary = tables["candidate_summary"][0]
    assert summary["candidate_status"] == "blocked"
    assert "insufficient_points" in summary["blocked_reasons"]


def test_h2o_senco6_linear_trim_review_blocks_extreme_final_trim_scope(tmp_path):
    source = tmp_path / "h2o_verify.csv"
    _write_csv(
        source,
        [
            {
                "device_id": "030",
                "reference_h2o_mmol": "7.7745",
                "measured_h2o_mmol": "13.8301",
                "source": "postwrite_firmware_output_verification",
            },
            {
                "device_id": "030",
                "reference_h2o_mmol": "16.6435",
                "measured_h2o_mmol": "24.4026",
                "source": "postwrite_firmware_output_verification",
            },
        ],
    )

    tables = build_h2o_senco6_linear_trim_review(
        verification_summary_csv=source,
        cfg=H2oSenco6LinearTrimConfig(target_device_ids=("030",), acceptance_pct=2.0),
    )

    summary = tables["candidate_summary"][0]
    assert summary["candidate_status"] == "blocked"
    assert "senco6_c0_exceeds_final_trim_scope" in summary["blocked_reasons"]
    assert "senco6_c1_exceeds_final_trim_scope" in summary["blocked_reasons"]


def test_h2o_senco6_linear_trim_review_cli_writes_artifacts(tmp_path):
    source = tmp_path / "h2o_verify.csv"
    output = tmp_path / "out"
    _write_csv(
        source,
        [
            {
                "device_id": "022",
                "reference_h2o_mmol": "7.7745",
                "measured_h2o_mmol": "7.1372",
                "source": "postwrite_firmware_output_verification",
            },
            {
                "device_id": "022",
                "reference_h2o_mmol": "16.6435",
                "measured_h2o_mmol": "15.8275",
                "source": "postwrite_firmware_output_verification",
            },
        ],
    )

    rc = cli_main(
        [
            "--verification-summary-csv",
            str(source),
            "--output-dir",
            str(output),
            "--target-device-ids",
            "022",
        ]
    )

    assert rc == 0
    rows = _read_csv(output / "h2o_senco6_linear_trim_candidate_coefficients.csv")
    assert rows[0]["senco_group"] == "SENCO6"
    markdown = output / "h2o_senco6_linear_trim_review.md"
    assert markdown.exists()
    assert markdown.read_bytes().startswith(b"\xef\xbb\xbf")
    assert "V1.5 H2O SENCO6 最终线性修正评审" in markdown.read_text(encoding="utf-8-sig")
