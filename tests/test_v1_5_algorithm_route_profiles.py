import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PROFILE_PATH = ROOT / "configs" / "v1_5_algorithm_route_profiles.json"


def _load_profiles() -> dict:
    return json.loads(PROFILE_PATH.read_text(encoding="utf-8"))


def test_v1_5_algorithm_profiles_define_legacy_and_absorption_modes() -> None:
    config = _load_profiles()
    profiles = {profile["profile_id"]: profile for profile in config["profiles"]}

    assert config["default_profile_id"] == "legacy_ratio_production"
    assert set(profiles) == {"legacy_ratio_production", "absorption_ratio_shadow"}
    assert profiles["legacy_ratio_production"]["production_default"] is True
    assert profiles["absorption_ratio_shadow"]["production_default"] is False
    assert profiles["legacy_ratio_production"]["algorithm_mode"] == "legacy_ratio_R"
    assert profiles["absorption_ratio_shadow"]["algorithm_mode"] == "absorption_ratio_A"


def test_v1_5_algorithm_profiles_keep_mature_route_runners_shared() -> None:
    config = _load_profiles()
    shared = config["shared_route_contract"]

    assert shared["co2_runner"] == "gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue"
    assert shared["h2o_runner"] == "gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue"
    assert shared["route_behavior"] == "preserve_mature_v1_5_0620_route_timing_and_quality_gates"

    for profile in config["profiles"]:
        assert profile["co2_route"]["runner"] == shared["co2_runner"]
        assert profile["h2o_route"]["runner"] == shared["h2o_runner"]


def test_v1_5_absorption_profile_separates_co2_zero_and_h2o_dry_anchors() -> None:
    profile = next(
        p for p in _load_profiles()["profiles"] if p["profile_id"] == "absorption_ratio_shadow"
    )

    assert profile["fit_input"]["formula"] == "A=-ln(R/R0(T))/(P_kPa/100)"
    assert profile["co2_route"]["r0_policy"]["r0_source"] == "co2_zero_gas_only"
    assert (
        profile["h2o_route"]["r0_policy"]["r0_source"]
        == "co2_zero_gas_h2o_ratio_plus_dewpoint_pressure_evidence"
    )
    assert profile["h2o_route"]["r0_policy"]["must_not_use_co2_zero_gas_as_h2o_zero"] is True
    assert profile["h2o_route"]["r0_policy"]["must_not_force_residual_water_to_zero"] is True
    assert (
        profile["h2o_route"]["r0_policy"]["offline_anchor_tool"]
        == "gas_calibrator.tools.export_v1_5_h2o_low_anchor_from_co2_zero"
    )


def test_v1_5_algorithm_profiles_define_co2_write_contracts() -> None:
    profiles = {profile["profile_id"]: profile for profile in _load_profiles()["profiles"]}
    legacy = profiles["legacy_ratio_production"]["co2_route"]["write_contract"]
    absorption = profiles["absorption_ratio_shadow"]["co2_route"]["write_contract"]

    assert legacy["algorithm_contract"] == "old_ratio_temperature"
    assert legacy["main_chain_coefficients"] == ["SENCO1", "SENCO3"]
    assert (
        legacy["main_chain_controlled_writer"]
        == "gas_calibrator.tools.run_v1_5_co2_senco13_controlled_write"
    )
    assert legacy["final_linear_trim"]["coefficient"] == "SENCO5"
    assert legacy["final_linear_trim"]["must_not_fold_into_main_chain"] is True
    assert (
        legacy["final_linear_trim"]["clear_command_required_for_neutralization"]
        == "CLEARSENCO5,YGAS,FFF"
    )

    assert absorption["algorithm_contract"] == "old7_absorption_A_TK_zero1ppm"
    assert absorption["fit_input"] == "A=-ln(R/R0(T))/(P_kPa/100)"
    assert absorption["temperature_feature"] == "absolute_chamber_temperature_kelvin"
    assert "exclusion_fit_gate" in absorption["required_review_checks"]
    assert "low_temperature_coverage_guard" in absorption["required_review_checks"]
    assert absorption["final_linear_trim"]["review_after_main_chain_reverification"] is True
    assert absorption["final_linear_trim"]["must_not_fold_into_main_chain"] is True
    assert not absorption["candidate_write_pack_evidence"].startswith("D:/")


def test_v1_5_algorithm_profiles_define_h2o_write_contracts() -> None:
    profiles = {profile["profile_id"]: profile for profile in _load_profiles()["profiles"]}
    legacy = profiles["legacy_ratio_production"]["h2o_route"]["write_contract"]
    absorption = profiles["absorption_ratio_shadow"]["h2o_route"]["write_contract"]

    assert legacy["algorithm_contract"] == "old_ratio_temperature"
    assert legacy["main_chain_coefficients"] == ["SENCO2", "SENCO4"]
    assert (
        legacy["main_chain_controlled_writer"]
        == "gas_calibrator.tools.run_v1_5_h2o_senco24_controlled_write"
    )
    assert legacy["main_chain_cli_algorithm_flag"] == "--h2o-senco24-algorithm old_ratio_temperature"
    assert legacy["final_linear_trim"]["coefficient"] == "SENCO6"
    assert legacy["final_linear_trim"]["must_not_fold_into_main_chain"] is True
    assert (
        legacy["final_linear_trim"]["clear_command_required_for_neutralization"]
        == "CLEARSENCO6,YGAS,FFF"
    )

    assert absorption["status"] == "blocked_pending_firmware_input_scale_confirmation"
    assert absorption["fit_input"] == "A_H2O=-ln(R_H2O/R0_H2O(T))/(P_kPa/100)"
    assert "R0_H2O_T_contract_confirmed" in absorption["required_review_checks"]
    assert absorption["alternate_absorption_slot_contract"]["status"] == (
        "diagnostic_only_not_default_production"
    )
    assert absorption["final_linear_trim"]["review_after_main_chain_reverification"] is True


def test_v1_5_absorption_profile_marks_sencoa_sencob_r0_contract_blocked() -> None:
    profile = next(
        p for p in _load_profiles()["profiles"] if p["profile_id"] == "absorption_ratio_shadow"
    )

    contract = profile["r0_write_contract"]
    components = {item["component"]: item for item in contract["components"]}

    assert contract["status"] == "blocked_until_controlled_sencoa_sencob_writer_exists"
    assert set(components) == {"co2", "h2o"}
    assert components["co2"]["coefficient_group"] == "SENCOA"
    assert components["co2"]["readback_group"] == "GETCOA"
    assert components["co2"]["physical_quantity"] == "R0_CO2(T)"
    assert components["h2o"]["coefficient_group"] == "SENCOB"
    assert components["h2o"]["readback_group"] == "GETCOB"
    assert components["h2o"]["physical_quantity"] == "R0_H2O(T)"
    assert {item["controlled_writer_status"] for item in components.values()} == {
        "missing_controlled_writer"
    }
    assert {item["production_blocker"] for item in components.values()} == {True}


def test_v1_5_absorption_profile_documents_targeted_supplements_only() -> None:
    profile = next(
        p for p in _load_profiles()["profiles"] if p["profile_id"] == "absorption_ratio_shadow"
    )

    co2_policy = profile["co2_route"]["supplement_policy"]
    h2o_supplement_policy = profile["h2o_route"]["supplement_policy"]
    h2o_policy = profile["h2o_route"]["dry_anchor_policy"]

    assert co2_policy["do_not_expand_low_temperature_to_dense_11_point_grid_by_default"] is True
    assert co2_policy["supplemental_points_are_candidate_only"] is True
    assert co2_policy["must_not_modify_legacy_ratio_production_queue"] is True
    assert co2_policy["physical_process"] == "all_items_are_gas_points_when_run"
    assert {
        item["temperature_c"] for item in co2_policy["required_new_algorithm_supplemental_gas_points"]
    } == {-20, -10}
    assert {
        item["co2_ppm"] for item in co2_policy["required_new_algorithm_supplemental_gas_points"]
    } == {600}
    assert {
        item["fit_role"] for item in co2_policy["required_new_algorithm_supplemental_gas_points"]
    } == {"low_temperature_curvature_constraint"}
    assert profile["co2_route"]["production_candidate_point_count_with_supplements"] == 47
    assert (
        co2_policy["device_specific_recheck_policy"]
        == "run_full_new_algorithm_candidate_points_then_generate_diagnostic_recheck_points_from_each_device_residual_review"
    )
    assert co2_policy["do_not_apply_sn01260607_conflict_points_to_other_devices"] is True
    assert co2_policy["sn01260607_observed_conflict_gas_points_for_diagnostic_recheck"] == [
        {"temperature_c": -20, "co2_ppm": 400},
        {"temperature_c": -10, "co2_ppm": 1000},
        {"temperature_c": 20, "co2_ppm": 100},
        {"temperature_c": 0, "co2_ppm": 400},
    ]
    assert (
        co2_policy["sn01260607_fit_evidence_20260629"][
            "kept_max_abs_rel_error_nonzero_pct"
        ]
        < 0.6
    )
    assert (
        co2_policy["sn01260607_fit_evidence_20260629"][
            "all_points_replay_max_abs_rel_error_nonzero_pct"
        ]
        > 1.0
    )
    assert h2o_policy["humidity_generator_low_humidity_required"] is False
    assert h2o_supplement_policy["supplemental_wet_points_are_candidate_only"] is True
    assert h2o_supplement_policy["must_not_modify_legacy_ratio_production_queue"] is True
    assert h2o_supplement_policy["do_not_add_humidity_generator_low_humidity_points_by_default"] is True
    assert (
        h2o_supplement_policy["device_specific_recheck_policy"]
        == "run_full_new_algorithm_candidate_points_then_generate_diagnostic_recheck_points_from_each_device_residual_review"
    )
    assert h2o_supplement_policy["do_not_apply_sn01260607_high_residual_points_to_other_devices"] is True
    assert h2o_policy["recommended_future_supplement"] == [
        {
            "temperature_c": 40,
            "route": "co2_open_flow_zero_gas_route",
            "purpose": "close_high_temperature_R0_H2O_T_extrapolation",
        }
    ]

    h2o_evidence = profile["h2o_route"]["sn01260607_fit_evidence_20260629"]
    assert profile["h2o_route"]["production_candidate_wet_point_count_with_supplements"] == 14
    assert profile["h2o_route"]["required_new_algorithm_supplemental_wet_points"] == [
        {
            "temperature_c": 40,
            "humidity_generator": "HGEN30C",
            "relative_humidity_pct": 30,
            "fit_role": "high_temperature_mid_water_shape_constraint",
        }
    ]
    assert h2o_evidence["wet_point_count"] == 13
    assert h2o_evidence["max_abs_relative_error_pct"] > 2.0
    assert {
        point["point"] for point in h2o_evidence["highest_residual_points"]
    } == {"p009_T30_HG20C_50RH_h2o", "p012_T40_HG30C_50RH_h2o"}
    assert {
        point["point"]
        for point in h2o_supplement_policy[
            "sn01260607_observed_high_residual_wet_points_for_diagnostic_recheck"
        ]
    } == {"p009_T30_HG20C_50RH_h2o", "p012_T40_HG30C_50RH_h2o"}
