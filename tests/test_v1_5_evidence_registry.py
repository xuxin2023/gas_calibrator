import json
import csv

import gas_calibrator.tools.query_v1_5_evidence_run as query_tool
from gas_calibrator.storage.v1_5_evidence.bundle import (
    build_evidence_bundle,
    bundle_summary,
    bundle_traceability_summary,
    verify_evidence_bundle_integrity,
    write_bundle_json,
    _build_qc_rows,
    _point_group_key,
    _target_value,
)
from gas_calibrator.storage.v1_5_evidence.repository import TABLE_COLUMNS, mask_dsn
from gas_calibrator.storage.v1_5_evidence.schema import load_migrations
from gas_calibrator.tools.import_v1_5_evidence_package import main as import_main
from gas_calibrator.tools.verify_v1_5_evidence_bundle import main as verify_bundle_main
from gas_calibrator.validation.v1_5_canonical_evidence import write_canonical_v1_5_evidence_package
from gas_calibrator.validation.formal_calibration_package import write_formal_calibration_package
from gas_calibrator.validation.formal_reports import write_v1_5_calibration_reports
from gas_calibrator.validation.pressure_channel import write_pressure_quick_check_csv


def _plan():
    return {
        "plan_id": "v1_5_formal_db_demo",
        "plan_version": "2026-05-24",
        "config_hash": "config-hash",
        "operator": "operator-a",
        "analyzer_id": "GA-UNDER-TEST-001",
        "allow_device_write": False,
        "standard_gases": [
            {
                "component": "co2",
                "cylinder_id": "CO2-001",
                "certificate_value": 900.0,
                "certificate_uncertainty": 0.9,
                "valid_until": "2027-01-01",
                "supplier": "standard-lab",
                "certificate_hash": "co2-cert-hash",
            },
            {
                "component": "h2o",
                "cylinder_id": "H2O-001",
                "certificate_value": 0.5,
                "certificate_uncertainty": 0.01,
                "valid_until": "2027-01-01",
                "supplier": "standard-lab",
                "certificate_hash": "h2o-cert-hash",
            },
        ],
    }


def _pressure_reference():
    return {
        "device_id": "COM22-DPG-001",
        "certificate_id": "P-CERT-001",
        "certificate_uncertainty": 0.15,
        "valid_until": "2027-01-01",
        "certificate_hash": "pressure-cert-hash",
        "unit": "hPa",
    }


def _row(index: int, component: str):
    return {
        "sample_index": index,
        "sample_ts": f"2026-05-24T12:00:{index:02d}",
        "point_phase": component,
        "route": component,
        "pressure_mode": "ambient_open",
        "pressure_gauge_hpa": 1000.5 + index * 0.002,
        "controller_pressure": 1000.6 + index * 0.002,
        "pressure_atmosphere_hold_status": "verified",
        "pressure_atmosphere_hold_active": "true",
        "pressure_atmosphere_hold_strategy": "legacy_hold_thread",
        "dewpoint_c": -30.0 + index * 0.001,
        "ga01_frame_usable": "true",
        "ga01_mode2_contract_status": "pass",
        "ga01_mode2_qc_status": "pass",
        "ga01_mode2_tokens_json": json.dumps(
            ["YGAS", "001", "0900.000", "00.500", "1768.000", "00.410"],
            separators=(",", ":"),
        ),
        "ga01_raw": "YGAS,001,...",
        "ga01_ref_signal": 3322.0,
        "ga01_co2_signal": 4356.0,
        "ga01_h2o_signal": 2631.0,
        "ga01_chamber_temp_c": 25.0 + index * 0.001,
        "ga01_case_temp_c": 25.5,
        "ga01_pressure_kpa": 100.05 + index * 0.0002,
        "ga01_co2_ratio_f": 1.3000 + index * 0.0001,
        "ga01_co2_ppm": 900.0 + index * 0.01,
        "ga01_h2o_ratio_f": 0.7000 + index * 0.00001,
        "ga01_h2o_mmol": 0.5 + index * 0.0001,
    }


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


def _write_post_write_reverification_artifacts(run_dir):
    review_dir = run_dir / "post_write_reverification"
    review_dir.mkdir()
    _write_csv(
        review_dir / "post_write_reverification_device_summary.csv",
        [
            {
                "device_id": "100",
                "component": "co2",
                "point_count": 2,
                "pass_count": 2,
                "fail_count": 0,
                "not_evaluated_count": 0,
                "max_abs_error": 1.2,
                "max_abs_error_pct": 0.13,
                "status": "pass",
            }
        ],
    )
    _write_csv(
        review_dir / "post_write_reverification_points.csv",
        [
            {
                "device_id": "100",
                "component": "co2",
                "point_id": "post_write_900ppm",
                "standard_value": 900.0,
                "measured_value": 901.2,
                "unit": "ppm",
                "error": 1.2,
                "error_pct": 0.13,
                "limit_value": 1.5,
                "limit_basis": "co2_relative_pct",
                "status": "pass",
                "reason": "",
            }
        ],
    )
    (review_dir / "post_write_reverification_review.json").write_text(
        json.dumps(
            {
                "schema": "v1_5_post_write_reverification_review_v1",
                "created_at": "2026-05-24T12:30:00",
                "overall_status": "pass",
                "limits": {"co2_relative_pct": 1.5, "h2o_relative_pct": 2.0},
                "warnings": [],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (review_dir / "post_write_reverification_review.md").write_text(
        "# V1.5 post-write reverification\n\noverall_status: pass\n",
        encoding="utf-8",
    )


def _make_run(tmp_path, *, quick_check=True, write_package=True):
    run_dir = tmp_path / "run_20260524_120000"
    run_dir.mkdir()
    rows = [_row(i, "co2") for i in range(1, 11)] + [_row(i, "h2o") for i in range(11, 21)]
    _write_csv(run_dir / "samples_20260524.csv", rows)
    if quick_check:
        write_pressure_quick_check_csv(run_dir, rows[:10], run_id="20260524")
    plan_path = tmp_path / "formal_plan.json"
    plan_path.write_text(json.dumps(_plan(), ensure_ascii=False), encoding="utf-8")
    pressure_reference_path = tmp_path / "pressure_reference.json"
    pressure_reference_path.write_text(
        json.dumps(_pressure_reference(), ensure_ascii=False),
        encoding="utf-8",
    )
    if write_package:
        write_formal_calibration_package(
            run_dir=run_dir,
            plan_path=plan_path,
            pressure_reference_path=pressure_reference_path,
            output_dir=run_dir / "formal_calibration_package",
            today="2026-05-24",
        )
    return run_dir, plan_path, pressure_reference_path


def test_v1_5_evidence_migration_declares_required_tables():
    sql = "\n".join(migration.sql for migration in load_migrations())
    assert "CREATE SCHEMA IF NOT EXISTS v1_5_evidence" in sql
    for table in TABLE_COLUMNS:
        assert f"v1_5_evidence.{table}" in sql
    assert "run_evidence_summary" in sql
    assert "ix_v1_5_evidence_sample_files_sha256" in sql
    assert "secret-pass" not in sql


def test_evidence_bundle_indexes_traceable_artifacts_and_blocks_auto_write(tmp_path):
    run_dir, plan_path, pressure_reference_path = _make_run(tmp_path)
    (run_dir / "six_device_optical_root_cause_report_zh.md").write_text("光学根因", encoding="utf-8")
    (run_dir / "status_register_and_invalid_frame_summary.csv").write_text(
        "device_id,status_register\n079,0101\n",
        encoding="utf-8",
    )

    bundle = build_evidence_bundle(
        run_dir=run_dir,
        plan_path=plan_path,
        pressure_reference_path=pressure_reference_path,
        today="2026-05-24",
    )
    summary = bundle_summary(bundle)
    tables = bundle["tables"]
    roles = {row["artifact_role"] for row in tables["sample_files"]}

    assert summary["evidence_status"] == "ready_for_reviewer"
    assert roles >= {
        "raw_samples",
        "pressure_channel_quick_check",
        "formal_plan_snapshot",
        "pressure_reference_snapshot",
        "diagnostic_analysis",
    }
    assert all(row["sha256"] for row in tables["sample_files"])
    assert {row["component"] for row in tables["standard_gases"]} == {"co2", "h2o"}
    assert tables["reference_certificates"][0]["certificate_id"] == "P-CERT-001"
    assert {row["candidate_status"] for row in tables["coefficient_candidates"]} == {
        "ready_for_reviewer"
    }
    assert all(row["auto_write_allowed"] is False for row in tables["coefficient_candidates"])
    assert tables["coefficient_write_events"][0]["status"] == "not_attempted"
    assert any(row["check_name"] == "old_coefficients_snapshot_present" for row in tables["evidence_integrity_checks"])


def test_evidence_bundle_indexes_post_write_reverification_artifacts(tmp_path):
    run_dir, plan_path, pressure_reference_path = _make_run(tmp_path)
    _write_post_write_reverification_artifacts(run_dir)

    bundle = build_evidence_bundle(
        run_dir=run_dir,
        plan_path=plan_path,
        pressure_reference_path=pressure_reference_path,
        today="2026-05-24",
    )
    roles = {row["artifact_role"] for row in bundle["tables"]["sample_files"]}
    post_write_files = [
        row
        for row in bundle["tables"]["sample_files"]
        if str(row["artifact_role"]).startswith("post_write_reverification")
    ]
    summary = bundle_traceability_summary(bundle)

    assert roles >= {
        "post_write_reverification_review",
        "post_write_reverification_points",
        "post_write_reverification_device_summary",
    }
    assert len(post_write_files) == 4
    assert all(row["sha256"] for row in post_write_files)
    assert summary["traceability_checks"]["has_post_write_reverification"] is True


def test_evidence_bundle_indexes_generated_reports_as_traceable_artifacts(tmp_path):
    run_dir, plan_path, pressure_reference_path = _make_run(tmp_path)
    initial_bundle = build_evidence_bundle(
        run_dir=run_dir,
        plan_path=plan_path,
        pressure_reference_path=pressure_reference_path,
        today="2026-05-24",
    )
    initial_bundle_path = write_bundle_json(initial_bundle, run_dir / "evidence_bundle_for_reports.json")

    write_v1_5_calibration_reports(
        evidence_bundle_path=initial_bundle_path,
        output_dir=run_dir / "reports",
        report_no="RPT-TRACE-001",
        reviewer="reviewer-a",
        approver="approver-a",
        location="lab-a",
        calibration_date="2026-05-24",
    )
    final_bundle = build_evidence_bundle(
        run_dir=run_dir,
        plan_path=plan_path,
        pressure_reference_path=pressure_reference_path,
        today="2026-05-24",
    )
    tables = final_bundle["tables"]
    sample_files = tables["sample_files"]
    reports = tables["reports"]
    roles = {row["artifact_role"] for row in sample_files}
    report_roles = {row["report_type"] for row in reports}
    summary = bundle_traceability_summary(final_bundle)

    assert roles >= {"report_model", "run_report", "technical_report", "formal_calibration_report"}
    assert report_roles >= {"report_model", "run_report", "technical_report", "formal_calibration_report"}
    assert len([row for row in sample_files if row["artifact_role"] == "run_report"]) == 3
    assert len([row for row in sample_files if row["artifact_role"] == "technical_report"]) == 3
    assert len([row for row in sample_files if row["artifact_role"] == "formal_calibration_report"]) == 3
    assert all(row["sha256"] for row in reports)
    assert all(row["metadata"].get("source_artifact_id") for row in reports)
    source_ids = {row["id"]: row for row in sample_files}
    for row in reports:
        artifact = source_ids[row["metadata"]["source_artifact_id"]]
        assert artifact["sha256"] == row["sha256"]
        assert artifact["path"] == row["path"]
    assert {row["report_type"] for row in summary["reports"]} >= {
        "report_model",
        "run_report",
        "technical_report",
        "formal_calibration_report",
    }


def test_evidence_bundle_integrity_verifier_detects_report_tampering(tmp_path, capsys):
    run_dir, plan_path, pressure_reference_path = _make_run(tmp_path)
    initial_bundle = build_evidence_bundle(
        run_dir=run_dir,
        plan_path=plan_path,
        pressure_reference_path=pressure_reference_path,
        today="2026-05-24",
    )
    initial_bundle_path = write_bundle_json(initial_bundle, run_dir / "evidence_bundle_for_reports.json")
    write_v1_5_calibration_reports(
        evidence_bundle_path=initial_bundle_path,
        output_dir=run_dir / "reports",
        report_no="RPT-INTEGRITY-001",
    )
    final_bundle = build_evidence_bundle(
        run_dir=run_dir,
        plan_path=plan_path,
        pressure_reference_path=pressure_reference_path,
        today="2026-05-24",
    )
    final_bundle_path = write_bundle_json(final_bundle, run_dir / "evidence_bundle_with_reports.json")

    result = verify_evidence_bundle_integrity(final_bundle)
    assert result["status"] == "pass"
    assert all(row["status"] == "pass" for row in result["checks"])
    assert verify_bundle_main(["--evidence-bundle-json", str(final_bundle_path)]) == 0
    cli_result = json.loads(capsys.readouterr().out)
    assert cli_result["status"] == "pass"
    assert cli_result["physical_boundaries"]["opens_com_ports"] is False

    report_path = run_dir / "reports" / "formal_calibration_report.md"
    report_path.write_text(report_path.read_text(encoding="utf-8") + "\n\nTAMPERED\n", encoding="utf-8")

    tampered = verify_evidence_bundle_integrity(final_bundle)
    mismatch_check = next(
        row for row in tampered["checks"] if row["check_name"] == "sample_file_hashes_match_disk"
    )
    assert tampered["status"] == "fail"
    assert mismatch_check["details"]["mismatched_files"]
    assert verify_bundle_main(["--evidence-bundle-json", str(final_bundle_path)]) == 1
    cli_tampered = json.loads(capsys.readouterr().out)
    assert cli_tampered["status"] == "fail"


def test_evidence_target_value_keeps_standard_target_separate_from_analyzer_output():
    zero_row = {
        "point_tag": "open_flow_0ppm",
        "target_co2_ppm": "",
        "co2_ppm": "3000.0",
    }
    standard_row = {
        "point_title": "20°C环境，二氧化碳900ppm，气压未设",
        "co2_ppm": "897.5",
    }
    measured_only_row = {"co2_ppm": "897.5"}

    assert _target_value(zero_row, "co2") == 0.0
    assert _target_value(standard_row, "co2") == 900.0
    assert _target_value(measured_only_row, "co2") is None


def test_evidence_point_group_uses_route_when_component_is_missing():
    key = _point_group_key(
        {
            "route": "co2",
            "point_row": "1",
            "point_tag": "open_flow_0ppm",
            "pressure_mode": "ambient_open",
        }
    )

    assert key == ("co2", "1", "open_flow_0ppm", "ambient_open")


def test_evidence_bundle_blocks_when_pressure_quick_check_artifact_is_missing(tmp_path):
    run_dir, plan_path, pressure_reference_path = _make_run(
        tmp_path,
        quick_check=False,
        write_package=False,
    )

    bundle = build_evidence_bundle(
        run_dir=run_dir,
        plan_path=plan_path,
        pressure_reference_path=pressure_reference_path,
        today="2026-05-24",
    )
    checks = {row["check_name"]: row for row in bundle["tables"]["evidence_integrity_checks"]}

    assert bundle_summary(bundle)["evidence_status"] == "blocked"
    assert checks["pressure_quick_check_artifact_present"]["status"] == "fail"
    assert bundle["tables"]["runs"][0]["package_status"] == "blocked"


def test_evidence_bundle_all_analyzers_indexes_each_sensor_id(tmp_path):
    run_dir = tmp_path / "run_multi_analyzer"
    run_dir.mkdir()
    rows = []
    for index in range(1, 11):
        row = _row(index, "co2")
        row.update(
            {
                "ga02_frame_usable": "true",
                "ga02_mode2_contract_status": "pass",
                "ga02_mode2_qc_status": "pass",
                "ga02_mode2_tokens_json": json.dumps(
                    ["YGAS", "022", "0900.000", "00.500", "1768.000", "00.410"],
                    separators=(",", ":"),
                ),
                "ga02_raw": "YGAS,022,...",
                "ga02_ref_signal": 3322.0,
                "ga02_co2_signal": 4356.0,
                "ga02_h2o_signal": 2631.0,
                "ga02_chamber_temp_c": 25.0 + index * 0.001,
                "ga02_case_temp_c": 25.5,
                "ga02_pressure_kpa": 100.05 + index * 0.0002,
                "ga02_co2_ratio_f": 1.3100 + index * 0.0001,
                "ga02_co2_ppm": 900.0 + index * 0.02,
                "ga02_h2o_ratio_f": 0.7000 + index * 0.00001,
                "ga02_h2o_mmol": 0.5 + index * 0.0001,
            }
        )
        rows.append(row)
    _write_csv(run_dir / "samples_20260524.csv", rows)
    plan_path = tmp_path / "formal_plan.json"
    plan_path.write_text(json.dumps(_plan(), ensure_ascii=False), encoding="utf-8")
    pressure_reference_path = tmp_path / "pressure_reference.json"
    pressure_reference_path.write_text(
        json.dumps(_pressure_reference(), ensure_ascii=False),
        encoding="utf-8",
    )

    bundle = build_evidence_bundle(
        run_dir=run_dir,
        plan_path=plan_path,
        pressure_reference_path=pressure_reference_path,
        component="co2",
        analyzer_prefix="all",
        require_quick_check_artifact=False,
        today="2026-05-24",
    )

    tables = bundle["tables"]
    analyzer_serials = {
        row["serial_number"]
        for row in tables["devices"]
        if row["device_type"] == "gas_analyzer"
    }
    candidate_ids = [row["id"] for row in tables["coefficient_candidates"]]
    candidate_sensor_ids = {
        row["metadata"]["analyzer_device_id"]
        for row in tables["coefficient_candidates"]
    }

    assert analyzer_serials == {"001", "022"}
    assert candidate_sensor_ids == {"001", "022"}
    assert len(candidate_ids) == len(set(candidate_ids))
    assert tables["runs"][0]["metadata"]["analyzer_device_ids"] == ["001", "022"]


def test_h2o_traceability_accepts_device_id_prefixed_raw_fields(tmp_path):
    run_dir = tmp_path / "run_h2o_device_prefixed"
    run_dir.mkdir()
    rows = []
    for index in range(1, 11):
        rows.append(
            {
                "sample_index": index,
                "sample_ts": f"2026-05-30T02:00:{index:02d}",
                "point_phase": "h2o",
                "route": "h2o",
                "pressure_mode": "ambient_open",
                "pressure_gauge_hpa": 1000.5 + index * 0.002,
                "controller_pressure": 1000.6 + index * 0.002,
                "pressure_atmosphere_hold_status": "verified",
                "pressure_atmosphere_hold_active": "true",
                "dewpoint_c": -6.0 + index * 0.001,
                "h2o_mmol_target": 3.6,
                "h2o_wet_ppmv": 3600.0,
                "h2o_dry_ppmv": 3613.0,
                "ga022_frame_usable": "true",
                "ga022_mode2_contract_status": "pass",
                "ga022_mode2_qc_status": "pass",
                "ga022_mode2_tokens_json": json.dumps(
                    ["YGAS", "022", "0000.000", "03.600", "1768.000", "00.410"],
                    separators=(",", ":"),
                ),
                "ga022_h2o_signal": 2631.0,
                "ga022_h2o_ratio_f": 0.7000 + index * 0.00001,
                "ga022_h2o_mmol": 3.6 + index * 0.0001,
                "ga022_pressure_kpa": 100.05 + index * 0.0002,
                "ga022_chamber_temp_c": 20.0 + index * 0.001,
                "ga022_case_temp_c": 20.5,
            }
        )
    _write_csv(run_dir / "samples_20260530.csv", rows)
    (run_dir / "h2o_senco24_candidate_review.md").write_text(
        "# H2O SENCO2/4 candidate review\n",
        encoding="utf-8",
    )
    plan_path = tmp_path / "formal_plan.json"
    plan_path.write_text(json.dumps(_plan(), ensure_ascii=False), encoding="utf-8")
    pressure_reference_path = tmp_path / "pressure_reference.json"
    pressure_reference_path.write_text(
        json.dumps(_pressure_reference(), ensure_ascii=False),
        encoding="utf-8",
    )

    bundle = build_evidence_bundle(
        run_dir=run_dir,
        plan_path=plan_path,
        pressure_reference_path=pressure_reference_path,
        component="h2o",
        analyzer_prefix="all",
        require_quick_check_artifact=False,
        today="2026-05-30",
    )
    summary = bundle_traceability_summary(bundle)
    roles = {row["artifact_role"] for row in bundle["tables"]["sample_files"]}
    water = summary["water_route_evidence"]

    assert summary["traceability_checks"]["has_h2o_raw_signal_fields"] is True
    assert water["raw_h2o_fields_present"] is True
    assert water["missing_raw_h2o_fields"] == []
    assert set(water["h2o_analyzer_fields_by_suffix"]) == {
        "h2o_signal",
        "h2o_ratio_f",
        "h2o_mmol",
    }
    assert bundle["tables"]["calibration_points"][0]["target_value"] == 3.6
    assert "candidate_coefficient_review" in roles


def test_evidence_bundle_splits_device_keyed_old_getco_snapshots(tmp_path):
    run_dir, plan_path, pressure_reference_path = _make_run(tmp_path)
    snapshot_dir = run_dir / "old_getco_component_snapshot"
    snapshot_dir.mkdir()
    (snapshot_dir / "old_component_coefficients_snapshot.json").write_text(
        json.dumps(
            {
                "022": {
                    "analyzer_prefix": "ga022",
                    "analyzer_device_id": "022",
                    "port": "COM35",
                    "source": "read_only_getco_component_snapshot",
                    "GETCO2_before": [-1.0, 2.0, 3.0, 4.0, 0.0, 0.0],
                    "GETCO4_before": [5.0, 6.0, 7.0, 8.0, 0.0, 0.0],
                    "GETCO6_before": [0.0, 1.0],
                },
                "030": {
                    "analyzer_prefix": "ga030",
                    "analyzer_device_id": "030",
                    "port": "COM36",
                    "source": "read_only_getco_component_snapshot",
                    "GETCO2_before": [-11.0, 12.0, 13.0, 14.0, 0.0, 0.0],
                    "GETCO4_before": [15.0, 16.0, 17.0, 18.0, 0.0, 0.0],
                    "GETCO6_before": [-0.5, 1.2],
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    bundle = build_evidence_bundle(
        run_dir=run_dir,
        plan_path=plan_path,
        pressure_reference_path=pressure_reference_path,
        today="2026-05-30",
    )
    snapshots = bundle["tables"]["coefficient_snapshots"]
    by_analyzer = {row["analyzer_id"]: row for row in snapshots}
    check = next(
        row
        for row in bundle["tables"]["evidence_integrity_checks"]
        if row["check_name"] == "old_coefficients_snapshot_present"
    )

    assert set(by_analyzer) == {"022", "030"}
    assert all(row["snapshot_type"] == "old_component_getco_coefficients" for row in snapshots)
    assert by_analyzer["022"]["metadata"]["getco_groups"] == ["GETCO2", "GETCO4", "GETCO6"]
    assert by_analyzer["030"]["coefficients"]["GETCO6_before"] == [-0.5, 1.2]
    assert check["status"] == "pass"


def test_h2o_sidecar_candidate_rollup_keeps_warnings_out_of_blockers(tmp_path):
    run_dir, plan_path, pressure_reference_path = _make_run(tmp_path)
    sidecar_path = run_dir / "h2o_senco24_database_sidecar.json"
    sidecar_path.write_text(
        json.dumps(
            {
                "schema": "v1_5_h2o_senco24_database_sidecar_v1",
                "suggested_rows": [
                    {
                        "db_table": "coefficient_candidates",
                        "record_key": "h2o_senco2_senco4_candidate_022",
                        "component": "h2o",
                        "analyzer_device_id": "022",
                        "candidate_status": "candidate_fit_review_required",
                        "auto_write_allowed": False,
                        "fit_rmse": 0.131,
                        "fit_max_abs_relative_error_pct": 6.4,
                        "blocked_reasons": "",
                        "warning_reasons": "side_channel_cache_age_warning_kept_as_evidence_not_fit_blocker",
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    bundle = build_evidence_bundle(
        run_dir=run_dir,
        plan_path=plan_path,
        pressure_reference_path=pressure_reference_path,
        today="2026-05-30",
    )
    sidecar_candidate = next(
        row
        for row in bundle["tables"]["coefficient_candidates"]
        if row["metadata"].get("record_key") == "h2o_senco2_senco4_candidate_022"
    )
    traceability = bundle_traceability_summary(bundle)
    rollup = next(
        row
        for row in traceability["h2o_candidate_review_rollup"]
        if row["analyzer_device_id"] == "022"
    )

    assert sidecar_candidate["blockers"] == []
    assert "co2_senco_pair_review_required" not in sidecar_candidate["blockers"]
    assert sidecar_candidate["metadata"]["warning_reasons_list"] == [
        "side_channel_cache_age_warning_kept_as_evidence_not_fit_blocker"
    ]
    assert rollup["consolidated_status"] == "review_required"
    assert rollup["blockers"] == []
    assert rollup["warnings"] == ["side_channel_cache_age_warning_kept_as_evidence_not_fit_blocker"]
    assert rollup["review_action"] == "review_candidate_metrics_warnings_and_post_write_plan"


def test_evidence_import_cli_dry_run_writes_bundle_json_without_database(tmp_path):
    run_dir, plan_path, pressure_reference_path = _make_run(tmp_path)
    bundle_path = tmp_path / "bundle.json"
    summary_path = tmp_path / "summary.json"

    rc = import_main(
        [
            "--run-dir",
            str(run_dir),
            "--plan-json",
            str(plan_path),
            "--pressure-reference-json",
            str(pressure_reference_path),
            "--output-json",
            str(bundle_path),
            "--summary-json",
            str(summary_path),
            "--dry-run",
            "--today",
            "2026-05-24",
        ]
    )

    assert rc == 0
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert bundle["schema"] == "v1_5_evidence_registry"
    assert summary["database_imported"] is False
    assert summary["table_counts"]["sample_files"] >= 4


def test_evidence_import_cli_dry_run_indexes_run_evidence_status_artifacts(tmp_path):
    outputs = write_canonical_v1_5_evidence_package(
        tmp_path / "canonical_with_status",
        include_reports=False,
    )
    bundle_path = tmp_path / "bundle_with_status.json"
    summary_path = tmp_path / "summary_with_status.json"

    rc = import_main(
        [
            "--run-dir",
            str(outputs["root"] / "run"),
            "--plan-json",
            str(outputs["plan"]),
            "--pressure-reference-json",
            str(outputs["pressure_reference"]),
            "--output-json",
            str(bundle_path),
            "--summary-json",
            str(summary_path),
            "--dry-run",
            "--today",
            "2026-05-24",
        ]
    )

    assert rc == 0
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    traceability = bundle_traceability_summary(bundle)

    assert summary["database_imported"] is False
    assert summary["run_evidence_status"]["present"] is True
    assert summary["run_evidence_status"]["all_hashed"] is True
    assert summary["run_evidence_status"]["roles"] == [
        "run_evidence_status",
        "run_evidence_status_report",
    ]
    assert traceability["traceability_checks"]["has_run_evidence_status"] is True
    assert {
        row["artifact_role"]
        for row in traceability["run_evidence_status_artifacts"]
    } == {"run_evidence_status", "run_evidence_status_report"}


def test_traceability_summary_explains_canonical_physical_evidence_chain(tmp_path):
    outputs = write_canonical_v1_5_evidence_package(
        tmp_path / "canonical",
        include_reports=False,
    )
    bundle = json.loads(outputs["evidence_bundle"].read_text(encoding="utf-8"))

    summary = bundle_traceability_summary(bundle)
    artifact_roles = {row["artifact_role"] for row in summary["artifacts"]}
    qc_scopes = {row["physical_scope"] for row in summary["qc_results"]}
    gases = {row["component"]: row for row in summary["standard_gases"]}

    assert summary["evidence_status"] == "ready_for_reviewer"
    assert summary["package_status"] == "ready_for_reviewer"
    assert summary["physical_boundaries"]["opens_com_ports"] is False
    assert summary["physical_boundaries"]["controls_water_or_gas_routes"] is False
    assert summary["physical_boundaries"]["controls_valves_or_pace"] is False
    assert summary["physical_boundaries"]["writes_coefficients"] is False
    assert summary["physical_boundaries"]["not_real_acceptance_evidence"] is True
    assert summary["traceability_checks"]["all_required_artifacts_have_sha256"] is True
    assert summary["traceability_checks"]["no_coefficient_write_attempted"] is True
    assert summary["traceability_checks"]["has_standard_gas_traceability"] is True
    assert summary["traceability_checks"]["has_pressure_reference_traceability"] is True
    assert summary["traceability_checks"]["has_raw_samples"] is True
    assert summary["traceability_checks"]["has_pressure_quick_check"] is True
    assert summary["traceability_checks"]["has_water_route_traceability"] is True
    assert summary["traceability_checks"]["has_h2o_open_flow_qc"] is True
    assert summary["traceability_checks"]["has_h2o_raw_signal_fields"] is True
    assert summary["traceability_checks"]["has_run_evidence_status"] is True
    assert {
        row["artifact_role"]
        for row in summary["run_evidence_status_artifacts"]
    } == {"run_evidence_status", "run_evidence_status_report"}
    assert {"raw_samples", "pressure_channel_quick_check", "formal_plan_snapshot", "pressure_reference_snapshot"}.issubset(
        artifact_roles
    )
    assert gases["co2"]["certificate_value"] == 900.0
    assert gases["h2o"]["certificate_value"] == 0.5
    assert gases["h2o"]["certificate_hash"] == "canonical-h2o-reference-hash"
    assert summary["reference_certificates"][0]["certificate_id"] == "P-CERT-CANONICAL-001"
    assert "pressure_input_validation" in qc_scopes
    water = summary["water_route_evidence"]
    assert water["h2o_standard_reference_present"] is True
    assert water["h2o_open_flow_points_present"] is True
    assert water["h2o_a_grade_count"] == 10
    assert water["h2o_qc_present"] is True
    assert water["h2o_open_flow_qc_present"] is True
    assert water["h2o_candidate_review_present"] is True
    assert water["raw_sample_header_readable"] is True
    assert water["raw_h2o_fields_present"] is True
    assert water["missing_raw_h2o_fields"] == []
    assert all(row["auto_write_allowed"] is False for row in summary["coefficient_candidates"])


def test_query_cli_supports_traceability_and_artifact_hash_modes(monkeypatch, capsys):
    monkeypatch.setenv("GAS_CAL_DB_DSN", "postgresql://postgres:secret-pass@localhost:5432/gas_calibrator")
    monkeypatch.setattr(
        query_tool,
        "query_run_traceability",
        lambda dsn, run_id: {
            "run_id": run_id,
            "physical_boundaries": {"opens_com_ports": False, "writes_coefficients": False},
        },
    )
    monkeypatch.setattr(
        query_tool,
        "query_artifacts_by_sha256",
        lambda dsn, sha256: [{"run_id": "run-a", "sha256": sha256, "artifact_role": "raw_samples"}],
    )

    assert query_tool.main(["--run-id", "run-a", "--traceability"]) == 0
    traceability_out = json.loads(capsys.readouterr().out)
    assert traceability_out["run_id"] == "run-a"
    assert traceability_out["physical_boundaries"]["opens_com_ports"] is False

    assert query_tool.main(["--artifact-sha256", "abc123"]) == 0
    artifact_out = json.loads(capsys.readouterr().out)
    assert artifact_out["rows"][0]["artifact_role"] == "raw_samples"
    assert artifact_out["rows"][0]["sha256"] == "abc123"


def test_pressure_reference_traceability_qc_deduplicates_shared_reference_rows():
    duplicate_reference = {
        "device_id": "118288",
        "status": "pass",
        "reasons": "",
        "certificate_id": "FRGsz25038057",
    }

    rows = _build_qc_rows(
        run_db_id="run-db-id",
        tables={
            "pressure_reference_traceability": [
                duplicate_reference,
                dict(duplicate_reference),
                dict(duplicate_reference),
            ]
        },
        source_artifacts={"pressure_reference_snapshot": "pressure-reference-artifact-id"},
    )

    pressure_rows = [row for row in rows if row["rule_name"] == "pressure_reference_traceability"]
    assert len(pressure_rows) == 1
    assert pressure_rows[0]["subject_id"] == "118288"
    assert pressure_rows[0]["metadata"]["deduped_shared_reference"] is True
    assert pressure_rows[0]["metadata"]["source_row_count"] == 3
    assert len({row["id"] for row in rows}) == len(rows)


def test_dsn_masking_never_exposes_password():
    masked = mask_dsn("postgresql://postgres:secret-pass@localhost:5432/gas_calibrator")
    assert "secret-pass" not in masked
    assert masked == "postgresql://postgres:***@localhost:5432/gas_calibrator"
