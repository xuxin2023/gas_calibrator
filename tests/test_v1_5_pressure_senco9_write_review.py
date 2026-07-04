import csv
import json

from gas_calibrator.tools.export_v1_5_pressure_senco9_write_review import main as write_review_main
from gas_calibrator.validation.pressure_senco9_write_review import (
    build_pressure_senco9_write_review_tables,
    write_pressure_senco9_write_review_report,
)


def _fit_summary_rows():
    return [
        {
            "analyzer_prefix": "ga01",
            "analyzer_device_id": "023",
            "status": "pass",
            "recommendation": "review_senco9_offset_candidate_no_write",
            "reason": "offset_only_model_supported_no_write_review_required",
            "valid_pair_count": "96",
            "distinct_pressure_points": "8",
            "reference_span_hpa": "599.455",
            "offset_only_offset_kpa": "0.704736",
            "offset_only_residual_mean_abs_hpa": "0.093",
            "offset_only_residual_max_abs_hpa": "0.302",
            "linear_slope_bias": "-0.0004",
            "senco9_candidate_command": "SENCO9,YGAS,FFF,7.047,1.000,0.000,0.000",
            "write_allowed": "False",
        },
        {
            "analyzer_prefix": "ga02",
            "analyzer_device_id": "030",
            "status": "pass",
            "recommendation": "review_senco9_offset_candidate_no_write",
            "reason": "offset_only_model_supported_no_write_review_required",
            "valid_pair_count": "96",
            "distinct_pressure_points": "8",
            "reference_span_hpa": "599.455",
            "offset_only_offset_kpa": "-0.218805",
            "offset_only_residual_mean_abs_hpa": "0.104",
            "offset_only_residual_max_abs_hpa": "0.277",
            "linear_slope_bias": "-0.0005",
            "senco9_candidate_command": "SENCO9,YGAS,FFF,-2.188,1.000,0.000,0.000",
            "write_allowed": "False",
        },
    ]


def _point_rows():
    return [
        {
            "analyzer_prefix": "ga01",
            "analyzer_device_id": "023",
            "pressure_point_group": "500",
            "sample_count": "12",
            "com22_pressure_mean_hpa": "500.0",
            "analyzer_pressure_mean_hpa": "493.0",
            "analyzer_minus_com22_mean_hpa": "-7.0",
            "point_status": "pass",
        }
    ]


def _linear_exception_fit_summary_rows():
    return [
        {
            "analyzer_prefix": "ga04",
            "analyzer_device_id": "076",
            "status": "fail",
            "recommendation": "do_not_write_senco9_investigate_pressure_channel",
            "reason": "pressure_fit_residuals_exceed_limits",
            "valid_pair_count": "105",
            "distinct_pressure_points": "7",
            "reference_span_hpa": "599.592",
            "offset_only_offset_kpa": "-3.73598",
            "offset_only_residual_mean_abs_hpa": "1.598",
            "offset_only_residual_max_abs_hpa": "3.004",
            "linear_intercept_kpa": "-2.9616976234456303",
            "linear_slope": "0.9907505353405923",
            "linear_slope_bias": "-0.00924946465940768",
            "linear_residual_mean_abs_hpa": "0.0389",
            "linear_residual_max_abs_hpa": "0.2045",
            "senco9_candidate_command": "SENCO9,YGAS,FFF,-3.736,1.000,0.000,0.000",
            "write_allowed": "False",
        }
    ]


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


def test_pressure_senco9_write_review_blocks_until_single_device_old_snapshot_and_approval():
    tables, context = build_pressure_senco9_write_review_tables(
        fit_summary_rows=_fit_summary_rows(),
        point_mean_rows=_point_rows(),
    )

    summary = tables["pressure_senco9_write_review_summary"][0]
    failed = {
        row["check"]: row["reasons"]
        for row in tables["pressure_senco9_write_review_checks"]
        if row["status"] == "fail"
    }

    assert context["review_status"] == "blocked"
    assert summary["write_allowed_by_this_tool"] is False
    assert summary["controls_water_or_gas_routes"] is False
    assert summary["writes_senco9"] is False
    assert summary["writes_device_id"] is False
    assert summary["execution_command_generated"] is False
    assert "single_device_selection" in failed
    assert "old_getco9_snapshot" in failed
    assert "reviewer_approver" in failed
    assert "broadcast_address_guard" not in failed
    assert {row["candidate_status"] for row in tables["pressure_senco9_write_candidates"]} == {
        "supported_for_review"
    }
    assert {row["candidate_command_scope"] for row in tables["pressure_senco9_write_candidates"]} == {
        "review_only_not_execution_do_not_broadcast_fff"
    }


def test_pressure_senco9_write_review_can_be_ready_for_one_selected_device_without_writing():
    tables, context = build_pressure_senco9_write_review_tables(
        fit_summary_rows=_fit_summary_rows(),
        point_mean_rows=_point_rows(),
        selected_analyzer_device_id="023",
        old_getco_snapshot={"023": {"GETCO9": "GETCO9,YGAS,023,1.0,2.0,3.0,4.0"}},
        reviewer="reviewer-a",
        approver="approver-b",
    )

    summary = tables["pressure_senco9_write_review_summary"][0]
    checks = {row["check"]: row for row in tables["pressure_senco9_write_review_checks"]}
    selected = [row for row in tables["pressure_senco9_write_candidates"] if row["selected_for_controlled_write_review"]]

    assert context["review_status"] == "ready_for_controlled_single_device_write_review"
    assert summary["selected_analyzer_device_id"] == "023"
    assert summary["selected_candidate_command"].startswith("SENCO9")
    assert summary["selected_candidate_command_is_review_only"] is True
    assert summary["execution_command_generated"] is False
    assert summary["write_allowed_by_this_tool"] is False
    assert all(row["status"] == "pass" for row in checks.values())
    assert selected[0]["old_getco9_snapshot"].startswith("GETCO9")
    assert tables["rollback_plan"][0]["rollback_available"] is True


def test_pressure_senco9_write_review_keeps_linear_exception_blocked_by_default():
    tables, context = build_pressure_senco9_write_review_tables(
        fit_summary_rows=_linear_exception_fit_summary_rows(),
        selected_analyzer_device_id="076",
        old_getco_snapshot={"devices": {"076": {"GETCO9_before": [0.0, 1.0, 0.0, 0.0]}}},
        reviewer="reviewer-a",
        approver="approver-b",
    )

    assert context["review_status"] == "blocked"
    candidates = tables["pressure_senco9_write_candidates"]
    assert candidates[0]["candidate_status"] == "blocked"
    failed = {
        row["check"]: row["reasons"]
        for row in tables["pressure_senco9_write_review_checks"]
        if row["status"] == "fail"
    }
    assert failed["candidate_evidence_available"] == "no_supported_no_write_offset_candidate"


def test_pressure_senco9_write_review_supports_explicit_linear_exception_without_writing():
    tables, context = build_pressure_senco9_write_review_tables(
        fit_summary_rows=_linear_exception_fit_summary_rows(),
        selected_analyzer_device_id="076",
        old_getco_snapshot={"devices": {"076": {"GETCO9_before": [0.0, 1.0, 0.0, 0.0]}}},
        reviewer="reviewer-a",
        approver="approver-b",
        allow_linear_senco9_exception=True,
    )

    summary = tables["pressure_senco9_write_review_summary"][0]
    candidates = tables["pressure_senco9_write_candidates"]
    assert context["review_status"] == "ready_for_controlled_single_device_write_review"
    assert candidates[0]["candidate_status"] == "supported_linear_exception_for_review"
    assert candidates[0]["candidate_model"] == "linear_exception"
    assert "9.90751e-01" in candidates[0]["candidate_command"]
    assert summary["allow_linear_senco9_exception"] is True
    assert summary["write_allowed_by_this_tool"] is False
    assert summary["writes_senco9"] is False


def test_pressure_senco9_write_review_report_and_cli_write_artifacts(tmp_path):
    fit_dir = tmp_path / "fit"
    fit_dir.mkdir()
    _write_csv(fit_dir / "pressure_fit_summary.csv", _fit_summary_rows())
    _write_csv(fit_dir / "pressure_fit_point_means.csv", _point_rows())
    old_getco = tmp_path / "old_getco.json"
    old_getco.write_text(
        json.dumps({"023": {"GETCO9": "GETCO9,YGAS,023,1.0,2.0,3.0,4.0"}}, ensure_ascii=False),
        encoding="utf-8",
    )

    outputs = write_pressure_senco9_write_review_report(
        fit_dir=fit_dir,
        output_dir=tmp_path / "review",
        selected_analyzer_device_id="023",
        old_getco_snapshot_path=old_getco,
        reviewer="reviewer-a",
        approver="approver-b",
    )

    assert outputs["workbook"].exists()
    assert outputs["runbook"].exists()
    summary = _read_csv(outputs["pressure_senco9_write_review_summary_csv"])
    assert summary[0]["review_status"] == "ready_for_controlled_single_device_write_review"
    runbook = outputs["runbook"].read_text(encoding="utf-8")
    assert "write_allowed_by_this_tool: false" in runbook
    assert "execution_command_generated: false" in runbook
    assert "Do not execute the review command directly" in runbook
    assert "controls_water_or_gas_routes: false" in runbook

    cli_dir = tmp_path / "cli_review"
    rc = write_review_main(
        [
            "--fit-dir",
            str(fit_dir),
            "--output-dir",
            str(cli_dir),
            "--selected-analyzer-device-id",
            "023",
            "--old-getco-json",
            str(old_getco),
            "--reviewer",
            "reviewer-a",
            "--approver",
            "approver-b",
        ]
    )

    assert rc == 0
    assert (cli_dir / "pressure_senco9_write_review.xlsx").exists()
    assert (cli_dir / "pressure_senco9_controlled_write_runbook.md").exists()
