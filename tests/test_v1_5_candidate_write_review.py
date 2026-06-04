import csv
import json

from gas_calibrator.tools.export_v1_5_candidate_write_review import main as write_review_cli
from gas_calibrator.validation.formal_candidate_write_review import (
    FORMULA_CONTRACT_MANUAL_SENCO13_RT_PRESSURE_SEPARATE,
    SENCO5_POLICY_INTEGRATED_OUTPUT_LAYER_REVIEWED,
    SENCO5_POLICY_PRESERVE_EXISTING,
    build_candidate_write_review_tables,
    write_candidate_write_review_report,
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


def _plan():
    return {
        "plan_id": "v1_5_co2_review",
        "standard_gases": [
            {
                "component": "co2",
                "cylinder_id": "CO2-500",
                "certificate_id": "GBW(E)063611",
                "certificate_value": 500.13,
                "certificate_uncertainty": 10.0026,
                "certificate_uncertainty_unit": "ppm",
                "valid_until": "2027-03-12",
                "traceability_level": "photo_certificate",
            }
        ],
    }


def _candidate_dir(tmp_path, *, blocked=False):
    root = tmp_path / "candidate"
    root.mkdir()
    _write_csv(
        root / "candidate_run_summary.csv",
        [
            {
                "candidate_run_status": "verification_passed",
                "auto_write_allowed": "False",
                "opens_com_ports": "False",
                "controls_water_or_gas_routes": "False",
                "writes_coefficients": "False",
            }
        ],
    )
    status = "blocked" if blocked else "verification_passed"
    allowed = "False" if blocked else "True"
    _write_csv(
        root / "candidate_policy_summary.csv",
        [
            {
                "component": "co2",
                "analyzer_prefix": "ga01",
                "analyzer_device_id": "023",
                "candidate_status": status,
                "allowed_for_review": allowed,
                "fit_sample_count": "100",
                "fit_point_count": "5",
                "fit_rmse": "0.1",
                "fit_max_error": "0.2",
                "verification_status": "pass",
                "verification_max_error": "1.0",
                "verification_error_limit": "10.0026",
                "blocked_reasons": "distinct_fit_targets<5" if blocked else "",
                "warning_reasons": "",
            }
        ],
    )
    _write_csv(
        root / "candidate_coefficients.csv",
        [
            {"component": "co2", "analyzer_prefix": "ga01", "analyzer_device_id": "023", "term": "intercept", "coefficient": "1.0"},
            {"component": "co2", "analyzer_prefix": "ga01", "analyzer_device_id": "023", "term": "R", "coefficient": "2.0"},
            {"component": "co2", "analyzer_prefix": "ga01", "analyzer_device_id": "023", "term": "R2", "coefficient": "3.0"},
            {"component": "co2", "analyzer_prefix": "ga01", "analyzer_device_id": "023", "term": "R3", "coefficient": "4.0"},
        ],
    )
    _write_csv(
        root / "candidate_verification_summary.csv",
        [
            {
                "component": "co2",
                "analyzer_prefix": "ga01",
                "analyzer_device_id": "023",
                "verification_status": "pass",
                "verification_sample_count": "30",
                "verification_point_count": "1",
                "verification_max_error": "1.0",
                "verification_error_limit": "10.0026",
                "verification_error_limit_source": "max(fixed_abs_error=2,certificate_expanded_uncertainty=10.0026)",
            }
        ],
    )
    return root


def _candidate_dir_with_secondary_terms(tmp_path):
    root = _candidate_dir(tmp_path)
    rows = _read_csv(root / "candidate_coefficients.csv")
    rows.extend(
        [
            {"component": "co2", "analyzer_prefix": "ga01", "analyzer_device_id": "023", "term": "T", "coefficient": "5.0"},
            {"component": "co2", "analyzer_prefix": "ga01", "analyzer_device_id": "023", "term": "T2", "coefficient": "6.0"},
            {"component": "co2", "analyzer_prefix": "ga01", "analyzer_device_id": "023", "term": "RT", "coefficient": "7.0"},
        ]
    )
    _write_csv(root / "candidate_coefficients.csv", rows)
    return root


def _candidate_dir_with_secondary_pressure_terms(tmp_path):
    root = _candidate_dir_with_secondary_terms(tmp_path)
    rows = _read_csv(root / "candidate_coefficients.csv")
    rows.extend(
        [
            {"component": "co2", "analyzer_prefix": "ga01", "analyzer_device_id": "023", "term": "P", "coefficient": "0.5"},
            {"component": "co2", "analyzer_prefix": "ga01", "analyzer_device_id": "023", "term": "RTP", "coefficient": "-0.25"},
        ]
    )
    _write_csv(root / "candidate_coefficients.csv", rows)
    return root


def _candidate_dir_fit_ready_requires_verification(tmp_path):
    root = _candidate_dir_with_secondary_terms(tmp_path)
    rows = _read_csv(root / "candidate_run_summary.csv")
    rows[0]["candidate_run_status"] = "fit_ready_requires_verification"
    _write_csv(root / "candidate_run_summary.csv", rows)
    policies = _read_csv(root / "candidate_policy_summary.csv")
    policies[0]["candidate_status"] = "fit_ready_requires_verification"
    policies[0]["allowed_for_review"] = "False"
    policies[0]["allowed_to_fit"] = "True"
    policies[0]["verification_status"] = "missing"
    _write_csv(root / "candidate_policy_summary.csv", policies)
    verifications = _read_csv(root / "candidate_verification_summary.csv")
    verifications[0]["verification_status"] = "missing"
    verifications[0]["verification_max_error"] = ""
    _write_csv(root / "candidate_verification_summary.csv", verifications)
    return root


def test_candidate_write_review_is_review_ready_but_write_blocked_without_old_coefficients(tmp_path):
    tables, context = build_candidate_write_review_tables(
        candidate_dir=_candidate_dir(tmp_path),
        plan=_plan(),
        component="co2",
        min_fit_points=5,
    )

    summary = tables["candidate_write_review_summary"][0]
    checks = {row["check"]: row for row in tables["candidate_write_review_checks"]}
    mapping = tables["candidate_senco_mapping_review"][0]
    algorithm = {row["topic"]: row for row in tables["candidate_algorithm_alignment"]}
    purge = tables["candidate_write_review_purge_policy"][0]
    assert context["review_status"] == "ready_for_human_candidate_review"
    assert summary["write_status"] == "blocked_until_getco_backup_senco_mapping_approval_and_post_write_plan"
    assert summary["writes_coefficients"] is False
    assert checks["old_coefficients_snapshot_bound"]["status"] == "block_write"
    assert checks["component_senco_mapping_reviewed"]["status"] == "review_only"
    assert mapping["primary_senco"] == "SENCO1"
    assert mapping["secondary_senco"] == "SENCO3"
    assert mapping["primary_command_preview"].startswith("SENCO1,YGAS,FFF,1.00000e00,2.00000e00")
    assert mapping["secondary_command_preview"] == ""
    assert mapping["write_allowed"] is False
    assert algorithm["firmware_formula_contract"]["alignment_status"] == "not_confirmed_for_firmware_final_output_model"
    assert algorithm["coefficient_family"]["alignment_status"] == "requires_firmware_formula_confirmation"
    assert algorithm["senco_channel_mapping"]["alignment_status"] == "mapping_consistent"
    assert (
        algorithm["co2_senco5_senco6_linear_correction_scope"]["alignment_status"]
        == "in_scope_requires_integrated_output_layer_review"
    )
    assert algorithm["terms_written_by_candidate"]["alignment_status"] == "primary_or_identifiable_secondary_only"
    assert algorithm["pressure_temperature_terms"]["alignment_status"] == "intentional_v1_5_scope_reduction"
    assert checks["firmware_formula_contract_confirmed"]["status"] == "block_write"
    assert checks["co2_senco5_senco6_linear_correction_contract"]["status"] == "block_write"
    assert purge["recommended_default_min_purge_s"] == 360
    assert purge["recommended_low_ppm_or_large_switch_min_purge_s"] == 360


def test_candidate_write_review_previews_senco1_senco3_pair_when_secondary_terms_exist(tmp_path):
    old_snapshot = {
        "023": {
            "GETCO1_before": [1, 2, 3, 4, 0, 0],
            "GETCO3_before": [0.1, 0.2, 0.3, 8, 9, 10],
        }
    }

    tables, context = build_candidate_write_review_tables(
        candidate_dir=_candidate_dir_with_secondary_terms(tmp_path),
        plan=_plan(),
        component="co2",
        min_fit_points=5,
        old_coefficients_snapshot=old_snapshot,
    )

    mapping = tables["candidate_senco_mapping_review"][0]
    assert context["review_status"] == "ready_for_human_candidate_review"
    assert mapping["mapping_status"] == "review_only_primary_secondary_preview_ready"
    assert mapping["candidate_terms"] == "intercept;R;R2;R3;T;T2;RT"
    assert "P(" not in mapping["candidate_terms"]
    assert mapping["primary_command_preview"].startswith("SENCO1,YGAS,FFF,1.00000e00,2.00000e00")
    assert mapping["secondary_command_preview"].startswith("SENCO3,YGAS,FFF,5.00000e00,6.00000e00,7.00000e00")
    assert mapping["secondary_candidate_values"].endswith("0.00000e00,0.00000e00,0.00000e00")
    assert mapping["write_allowed"] is False


def test_candidate_write_review_blocks_nonzero_pressure_terms_from_current_atmosphere_candidate(tmp_path):
    old_snapshot = {
        "023": {
            "GETCO1_before": [1, 2, 3, 4, 0, 0],
            "GETCO3_before": [0.1, 0.2, 0.3, 0, 0, 0],
        }
    }

    tables, context = build_candidate_write_review_tables(
        candidate_dir=_candidate_dir_with_secondary_pressure_terms(tmp_path),
        plan=_plan(),
        component="co2",
        min_fit_points=5,
        old_coefficients_snapshot=old_snapshot,
    )

    checks = {row["check"]: row for row in tables["candidate_write_review_checks"]}
    mapping = tables["candidate_senco_mapping_review"][0]
    assert context["review_status"] == "blocked"
    assert checks["secondary_pressure_terms_frozen"]["status"] == "fail"
    assert mapping["mapping_status"] == "blocked_pressure_terms_present_current_atmosphere_contract"
    assert mapping["secondary_action"] == "blocked_pressure_terms_present_current_atmosphere_contract"
    assert mapping["secondary_command_preview"] == ""
    assert "P=0.5" in mapping["secondary_pressure_terms_nonzero"]
    assert "RTP=-0.25" in mapping["secondary_pressure_terms_nonzero"]


def test_candidate_write_review_accepts_fit_ready_candidates_for_prewrite_review_only(tmp_path):
    tables, context = build_candidate_write_review_tables(
        candidate_dir=_candidate_dir_fit_ready_requires_verification(tmp_path),
        plan=_plan(),
        component="co2",
        min_fit_points=5,
    )

    checks = {row["check"]: row for row in tables["candidate_write_review_checks"]}
    summary = tables["candidate_write_review_summary"][0]
    device = tables["candidate_write_review_devices"][0]
    assert context["review_status"] == "ready_for_human_candidate_review"
    assert summary["write_status"] == "blocked_until_getco_backup_senco_mapping_approval_and_post_write_plan"
    assert checks["candidate_run_verified"]["status"] == "review_only_requires_post_write_verification"
    assert checks["all_devices_ready_for_reviewer"]["status"] == "pass"
    assert device["review_ready"] is True
    assert device["verification_status"] == "missing"


def test_candidate_write_review_can_bind_old_component_snapshot_without_enabling_write(tmp_path):
    old_snapshot = {
        "023": {
            "GETCO1_before": [1, 2, 3, 4, 0, 0],
            "GETCO3_before": [5, 6, 7, 8, 9, 0],
        }
    }

    tables, _ = build_candidate_write_review_tables(
        candidate_dir=_candidate_dir(tmp_path),
        plan=_plan(),
        component="co2",
        min_fit_points=5,
        old_coefficients_snapshot=old_snapshot,
    )

    checks = {row["check"]: row for row in tables["candidate_write_review_checks"]}
    mapping = tables["candidate_senco_mapping_review"][0]
    assert checks["old_coefficients_snapshot_bound"]["status"] == "pass"
    assert mapping["old_snapshot_status"] == "primary_and_secondary_bound"
    assert mapping["old_primary_snapshot"] == "[1,2,3,4,0,0]"
    assert mapping["old_secondary_snapshot"] == "[5,6,7,8,9,0]"
    assert mapping["write_allowed"] is False


def test_candidate_write_review_binds_live_getco_snapshot_and_preserves_trailing_senco3_slots(tmp_path):
    old_snapshot = {
        "023": {
            "GETCO1_before_live": [1, 2, 3, 4, 0, 0],
            "GETCO3_before_live": [0.1, 0.2, 0.3, -40.6241, 0.0856692, 0.0],
        }
    }

    tables, _ = build_candidate_write_review_tables(
        candidate_dir=_candidate_dir_with_secondary_terms(tmp_path),
        plan=_plan(),
        component="co2",
        min_fit_points=5,
        old_coefficients_snapshot=old_snapshot,
    )

    checks = {row["check"]: row for row in tables["candidate_write_review_checks"]}
    mapping = tables["candidate_senco_mapping_review"][0]
    assert checks["old_coefficients_snapshot_bound"]["status"] == "pass"
    assert mapping["old_snapshot_status"] == "primary_and_secondary_bound"
    assert mapping["secondary_candidate_values"] == (
        "5.00000e00,6.00000e00,7.00000e00,0.00000e00,0.00000e00,0.00000e00"
    )
    assert mapping["secondary_command_preview"].endswith("0.00000e00,0.00000e00,0.00000e00")


def test_candidate_write_review_can_fix_manual_formula_contract_without_writing_senco5(tmp_path):
    old_snapshot = {
        "023": {
            "GETCO1_before": [1, 2, 3, 4, 0, 0],
            "GETCO3_before": [5, 6, 7, 8, 9, 0],
        }
    }

    tables, _ = build_candidate_write_review_tables(
        candidate_dir=_candidate_dir_with_secondary_terms(tmp_path),
        plan=_plan(),
        component="co2",
        min_fit_points=5,
        old_coefficients_snapshot=old_snapshot,
        formula_contract=FORMULA_CONTRACT_MANUAL_SENCO13_RT_PRESSURE_SEPARATE,
        senco5_policy=SENCO5_POLICY_PRESERVE_EXISTING,
    )

    checks = {row["check"]: row for row in tables["candidate_write_review_checks"]}
    algorithm = {row["topic"]: row for row in tables["candidate_algorithm_alignment"]}
    summary = tables["candidate_write_review_summary"][0]
    assert checks["firmware_formula_contract_confirmed"]["status"] == "pass"
    assert checks["co2_senco5_senco6_linear_correction_contract"]["status"] == "pass"
    assert algorithm["firmware_formula_contract"]["alignment_status"] == (
        "confirmed_manual_senco13_rt_pressure_separate"
    )
    assert algorithm["co2_senco5_senco6_linear_correction_scope"]["alignment_status"] == (
        "preserve_existing_not_part_of_concentration_candidate"
    )
    assert summary["formula_contract"] == FORMULA_CONTRACT_MANUAL_SENCO13_RT_PRESSURE_SEPARATE
    assert summary["senco5_policy"] == SENCO5_POLICY_PRESERVE_EXISTING


def test_candidate_write_review_can_accept_integrated_senco5_output_layer_review(tmp_path):
    old_snapshot = {
        "023": {
            "GETCO1_before": [1, 2, 3, 4, 0, 0],
            "GETCO3_before": [5, 6, 7, 8, 9, 0],
            "GETCO5_before": [0, 1],
        }
    }

    tables, _ = build_candidate_write_review_tables(
        candidate_dir=_candidate_dir_with_secondary_terms(tmp_path),
        plan=_plan(),
        component="co2",
        min_fit_points=5,
        old_coefficients_snapshot=old_snapshot,
        formula_contract=FORMULA_CONTRACT_MANUAL_SENCO13_RT_PRESSURE_SEPARATE,
        senco5_policy=SENCO5_POLICY_INTEGRATED_OUTPUT_LAYER_REVIEWED,
    )

    checks = {row["check"]: row for row in tables["candidate_write_review_checks"]}
    algorithm = {row["topic"]: row for row in tables["candidate_algorithm_alignment"]}
    summary = tables["candidate_write_review_summary"][0]
    assert checks["firmware_formula_contract_confirmed"]["status"] == "pass"
    assert checks["co2_senco5_senco6_linear_correction_contract"]["status"] == "pass"
    assert algorithm["co2_senco5_senco6_linear_correction_scope"]["alignment_status"] == (
        "integrated_final_output_layer_reviewed"
    )
    assert summary["senco5_policy"] == SENCO5_POLICY_INTEGRATED_OUTPUT_LAYER_REVIEWED


def test_candidate_write_review_blocks_when_candidate_policy_is_not_ready(tmp_path):
    tables, context = build_candidate_write_review_tables(
        candidate_dir=_candidate_dir(tmp_path, blocked=True),
        plan=_plan(),
        component="co2",
        min_fit_points=5,
    )

    checks = {row["check"]: row for row in tables["candidate_write_review_checks"]}
    assert context["review_status"] == "blocked"
    assert checks["all_devices_ready_for_reviewer"]["status"] == "fail"


def test_candidate_write_review_report_and_cli_write_artifacts(tmp_path):
    candidate_dir = _candidate_dir(tmp_path)
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(_plan(), ensure_ascii=False), encoding="utf-8")
    output_dir = tmp_path / "review"

    outputs = write_candidate_write_review_report(
        candidate_dir=candidate_dir,
        plan_path=plan_path,
        output_dir=output_dir,
        component="co2",
    )

    assert (output_dir / "candidate_write_review.xlsx").exists()
    assert (output_dir / "candidate_write_review_runbook.md").exists()
    summary = _read_csv(outputs["candidate_write_review_summary_csv"])
    assert summary[0]["review_status"] == "ready_for_human_candidate_review"

    cli_dir = tmp_path / "review_cli"
    rc = write_review_cli(
        [
            "--candidate-dir",
            str(candidate_dir),
            "--plan-json",
            str(plan_path),
            "--output-dir",
            str(cli_dir),
            "--component",
            "co2",
        ]
    )
    assert rc == 0
    assert (cli_dir / "candidate_write_review.xlsx").exists()
