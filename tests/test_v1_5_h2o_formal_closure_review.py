import csv
import json

from gas_calibrator.tools.export_v1_5_h2o_formal_closure_review import main as cli_main
from gas_calibrator.validation.h2o_formal_closure_review import (
    H2OFormalClosureConfig,
    build_h2o_formal_closure_review,
    write_h2o_formal_closure_review,
)


def _write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    header = []
    for row in rows:
        for key in row:
            if key not in header:
                header.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def _base_artifacts(tmp_path, *, command="SENCO6,YGAS,FFF,0,0.947", s6_source="postwrite_firmware_output_verification"):
    s24 = tmp_path / "h2o_senco24_candidates.csv"
    s24_res = tmp_path / "h2o_senco24_residuals.csv"
    inputs = tmp_path / "h2o_senco24_point_inputs.csv"
    s6 = tmp_path / "h2o_senco6_candidates.csv"
    s6_res = tmp_path / "h2o_senco6_residuals.csv"

    _write_csv(
        s24,
        [
            {
                "device_id": "001",
                "candidate_status": "candidate_fit_ready_requires_independent_verification",
                "senco2_command_preview": "SENCO2,YGAS,FFF,1.0E+00,2.0E+00",
                "senco4_command_preview": "SENCO4,YGAS,FFF,3.0E+00,4.0E+00",
            }
        ],
    )
    _write_csv(
        s24_res,
        [
            {
                "device_id": "001",
                "point_id": "T20_H2O_01",
                "error_mmol": "0.004",
                "relative_error_pct": "0.025",
            }
        ],
    )
    _write_csv(
        inputs,
        [
            {
                "device_id": "001",
                "point_id": "T0_0ppm_dry_anchor",
                "sample_role": "dry_anchor",
                "reference_source": "dewpoint_meter_plus_COM22_pressure_dry_gas_anchor",
                "target_h2o_mmol": "0.18",
            }
        ],
    )
    _write_csv(
        s6,
        [
            {
                "device_id": "001",
                "candidate_status": "review_ready",
                "command_preview": command,
                "input_source_contract": s6_source,
            }
        ],
    )
    _write_csv(
        s6_res,
        [
            {
                "device_id": "001",
                "point_id": "postwrite_h2o_01",
                "corrected_abs_error_mmol": "0.002",
                "corrected_abs_relative_error_pct": "0.011",
            }
        ],
    )
    return s24, s24_res, inputs, s6, s6_res


def test_h2o_formal_closure_accepts_separate_senco6_and_dewpoint_dry_anchor(tmp_path):
    s24, s24_res, inputs, s6, s6_res = _base_artifacts(tmp_path)

    tables = build_h2o_formal_closure_review(
        senco24_candidate_csv=s24,
        senco24_residuals_csv=s24_res,
        senco24_point_inputs_csv=inputs,
        senco6_candidate_csv=s6,
        senco6_residuals_csv=s6_res,
        config=H2OFormalClosureConfig(dry_anchor_required=True),
    )

    row = tables["device_status"][0]
    assert tables["summary"]["opens_com_ports"] is False
    assert tables["summary"]["writes_coefficients"] is False
    assert row["closure_status"] == "ready_for_controlled_write_or_report_review"
    assert row["dry_anchor_status"] == "pass"
    assert row["blocked_reasons"] == ""
    assert "senco6_separate_final_affine_layer" in row["physical_contracts"]
    assert row["senco6_command_preview"] == "SENCO6,YGAS,FFF,0,0.947"


def test_h2o_formal_closure_blocks_senco6_bare_decimal_command(tmp_path):
    s24, s24_res, inputs, s6, s6_res = _base_artifacts(
        tmp_path,
        command="SENCO6,YGAS,FFF,0,.947",
    )

    tables = build_h2o_formal_closure_review(
        senco24_candidate_csv=s24,
        senco24_residuals_csv=s24_res,
        senco24_point_inputs_csv=inputs,
        senco6_candidate_csv=s6,
        senco6_residuals_csv=s6_res,
        config=H2OFormalClosureConfig(dry_anchor_required=True),
    )

    row = tables["device_status"][0]
    assert row["closure_status"] == "blocked"
    assert "senco6_command_uses_bare_decimal_without_leading_zero" in row["blocked_reasons"]


def test_h2o_formal_closure_blocks_senco6_model_only_input_source(tmp_path):
    s24, s24_res, inputs, s6, s6_res = _base_artifacts(
        tmp_path,
        s6_source="senco24_model_pred_wet_points_only",
    )

    tables = build_h2o_formal_closure_review(
        senco24_candidate_csv=s24,
        senco24_residuals_csv=s24_res,
        senco24_point_inputs_csv=inputs,
        senco6_candidate_csv=s6,
        senco6_residuals_csv=s6_res,
        config=H2OFormalClosureConfig(dry_anchor_required=True),
    )

    row = tables["device_status"][0]
    assert row["closure_status"] == "blocked"
    assert "senco6_input_not_independent_firmware_output" in row["blocked_reasons"]


def test_h2o_formal_closure_blocks_dry_anchor_forced_zero(tmp_path):
    s24, s24_res, inputs, s6, s6_res = _base_artifacts(tmp_path)
    _write_csv(
        inputs,
        [
            {
                "device_id": "001",
                "sample_role": "dry_anchor",
                "reference_source": "dewpoint_meter_plus_COM22_pressure_dry_gas_anchor",
                "target_h2o_mmol": "0",
            }
        ],
    )

    tables = build_h2o_formal_closure_review(
        senco24_candidate_csv=s24,
        senco24_residuals_csv=s24_res,
        senco24_point_inputs_csv=inputs,
        senco6_candidate_csv=s6,
        senco6_residuals_csv=s6_res,
        config=H2OFormalClosureConfig(dry_anchor_required=True),
    )

    row = tables["device_status"][0]
    assert row["closure_status"] == "blocked"
    assert "dry_anchor_target_was_forced_to_zero" in row["blocked_reasons"]


def test_h2o_formal_closure_verified_writes_allow_formal_report(tmp_path):
    s24, s24_res, inputs, s6, s6_res = _base_artifacts(tmp_path)
    s24_write = tmp_path / "s24_write_events.csv"
    s6_write = tmp_path / "s6_write_events.csv"
    _write_csv(s24_write, [{"device_id": "001", "status": "written_readback_verified"}])
    _write_csv(s6_write, [{"device_id": "001", "status": "written_readback_verified"}])

    tables = build_h2o_formal_closure_review(
        senco24_candidate_csv=s24,
        senco24_residuals_csv=s24_res,
        senco24_point_inputs_csv=inputs,
        senco6_candidate_csv=s6,
        senco6_residuals_csv=s6_res,
        senco24_write_events_csv=s24_write,
        senco6_write_events_csv=s6_write,
        config=H2OFormalClosureConfig(dry_anchor_required=True, require_verified_writes=True),
    )

    row = tables["device_status"][0]
    assert row["closure_status"] == "ready_for_formal_report"
    assert row["senco24_write_verified"] is True
    assert row["senco6_write_verified"] is True


def test_h2o_formal_closure_cli_writes_utf8_chinese_report(tmp_path):
    s24, s24_res, inputs, s6, s6_res = _base_artifacts(tmp_path)
    out = tmp_path / "out"

    rc = cli_main(
        [
            "--output-dir",
            str(out),
            "--senco24-candidate-csv",
            str(s24),
            "--senco24-residuals-csv",
            str(s24_res),
            "--senco24-point-inputs-csv",
            str(inputs),
            "--senco6-candidate-csv",
            str(s6),
            "--senco6-residuals-csv",
            str(s6_res),
            "--dry-anchor-required",
        ]
    )

    assert rc == 0
    report = out / "h2o_formal_closure_review.md"
    assert report.read_bytes().startswith(b"\xef\xbb\xbf")
    text = report.read_text(encoding="utf-8-sig")
    assert "V1.5 H2O 正式闭环评审" in text
    assert "干气锚点" in text
    manifest = json.loads((out / "h2o_formal_closure_manifest.json").read_text(encoding="utf-8-sig"))
    assert manifest["summary"]["run_status"] == "ready"
