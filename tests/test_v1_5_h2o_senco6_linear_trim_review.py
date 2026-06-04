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
            {"device_id": "022", "reference_h2o_mmol": "7.7745", "measured_h2o_mmol": "7.1372"},
            {"device_id": "022", "reference_h2o_mmol": "16.6435", "measured_h2o_mmol": "15.8275"},
        ],
    )

    tables = build_h2o_senco6_linear_trim_review(
        verification_summary_csv=source,
        cfg=H2oSenco6LinearTrimConfig(target_device_ids=("022",), acceptance_pct=2.0),
    )

    summary = tables["candidate_summary"][0]
    coeff = tables["candidate_coefficients"][0]

    assert summary["candidate_status"] == "review_ready"
    assert abs(float(summary["candidate_C0"]) - 0.490523) < 0.001
    assert abs(float(summary["candidate_C1"]) - 1.020563) < 0.001
    assert coeff["command_preview"] == "SENCO6,YGAS,FFF,0.7,1.0"
    assert coeff["auto_write_allowed"] is False
    assert tables["run_summary"][0]["opens_com_ports"] is False
    assert tables["run_summary"][0]["writes_coefficients"] is False


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
            {"device_id": "030", "reference_h2o_mmol": "7.7745", "measured_h2o_mmol": "13.8301"},
            {"device_id": "030", "reference_h2o_mmol": "16.6435", "measured_h2o_mmol": "24.4026"},
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
            {"device_id": "022", "reference_h2o_mmol": "7.7745", "measured_h2o_mmol": "7.1372"},
            {"device_id": "022", "reference_h2o_mmol": "16.6435", "measured_h2o_mmol": "15.8275"},
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
    assert (output / "h2o_senco6_linear_trim_review.md").exists()
