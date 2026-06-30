import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_new_algorithm_test_point_plan import main as cli_main
from gas_calibrator.validation.v1_5_algorithm_route_profiles import (
    build_v1_5_new_algorithm_test_point_plan,
    write_v1_5_new_algorithm_test_point_plan,
)


ROOT = Path(__file__).resolve().parents[1]
PROFILE_PATH = ROOT / "configs" / "v1_5_algorithm_route_profiles.json"


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def test_new_algorithm_test_point_plan_keeps_supplements_out_of_legacy_queue() -> None:
    tables = build_v1_5_new_algorithm_test_point_plan(PROFILE_PATH)
    manifest = tables["manifest"]
    rows = tables["point_plan"]

    assert manifest["no_write"] is True
    assert manifest["opens_com_ports"] is False
    assert manifest["controls_water_or_gas_routes"] is False
    assert manifest["writes_coefficients"] is False
    assert manifest["legacy_co2_formal_point_count"] == 45
    assert manifest["legacy_h2o_formal_point_count"] == 13
    assert manifest["new_algorithm_co2_candidate_point_count"] == 47
    assert manifest["new_algorithm_h2o_candidate_wet_point_count"] == 14

    supplemental_rows = [
        row for row in rows if str(row["point_role"]).startswith("new_algorithm_supplemental")
    ]
    assert len(supplemental_rows) == 3
    assert {row["included_in_legacy_default_queue"] for row in supplemental_rows} == {False}
    assert {row["included_in_new_algorithm_candidate"] for row in supplemental_rows} == {True}
    assert {row["do_not_modify_mature_runner"] for row in supplemental_rows} == {True}


def test_new_algorithm_test_point_plan_lists_exact_co2_h2o_candidate_points() -> None:
    rows = build_v1_5_new_algorithm_test_point_plan(PROFILE_PATH)["point_plan"]

    co2_supplements = {
        (row["temperature_c"], row["co2_ppm"], row["fit_role"])
        for row in rows
        if row["point_role"] == "new_algorithm_supplemental_gas_point"
    }
    assert co2_supplements == {
        (-20, 600, "low_temperature_curvature_constraint"),
        (-10, 600, "low_temperature_curvature_constraint"),
    }

    h2o_supplements = [
        row for row in rows if row["point_role"] == "new_algorithm_supplemental_wet_point"
    ]
    assert h2o_supplements == [
        {
            "plan_id": "h2o_T40_HGEN30C_30RH_supplement",
            "component": "h2o",
            "route_component": "h2o",
            "point_role": "new_algorithm_supplemental_wet_point",
            "physical_process": "water_point",
            "temperature_c": 40,
            "co2_ppm": "",
            "hgen_temp": "HGEN30C",
            "hgen_rh_pct": 30,
            "fit_role": "high_temperature_mid_water_shape_constraint",
            "included_in_legacy_default_queue": False,
            "included_in_new_algorithm_candidate": True,
            "counts_as_new_physical_point": True,
            "applies_to_all_new_algorithm_devices": True,
            "diagnostic_source_device_sn": "",
            "diagnostic_source_device_id": "",
            "source_runner": "gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue",
            "do_not_modify_mature_runner": True,
            "release_gate_role": "required_before_new_algorithm_release",
            "notes": "Supplemental high-temperature mid-water shape constraint.",
        }
    ]


def test_new_algorithm_test_point_plan_captures_device_specific_rechecks_and_low_anchor() -> None:
    rows = build_v1_5_new_algorithm_test_point_plan(PROFILE_PATH)["point_plan"]

    co2_rechecks = {
        (row["temperature_c"], row["co2_ppm"])
        for row in rows
        if row["point_role"] == "sn01260607_observed_conflict_gas_point"
    }
    assert co2_rechecks == {(-20, 400), (-10, 1000), (20, 100), (0, 400)}
    co2_recheck_rows = [
        row for row in rows if row["point_role"] == "sn01260607_observed_conflict_gas_point"
    ]
    assert {row["applies_to_all_new_algorithm_devices"] for row in co2_recheck_rows} == {False}
    assert {row["diagnostic_source_device_sn"] for row in co2_recheck_rows} == {"01260607"}
    assert {row["release_gate_role"] for row in co2_recheck_rows} == {
        "device_specific_diagnostic_only_not_generic_release_gate"
    }
    assert all("Observed conflict for SN01260607 only" in row["notes"] for row in co2_recheck_rows)

    h2o_rechecks = {
        (row["temperature_c"], row["hgen_temp"], row["hgen_rh_pct"])
        for row in rows
        if row["point_role"] == "sn01260607_observed_high_residual_wet_point"
    }
    assert h2o_rechecks == {(30, "HGEN20C", 50), (40, "HGEN30C", 50)}
    h2o_recheck_rows = [
        row for row in rows if row["point_role"] == "sn01260607_observed_high_residual_wet_point"
    ]
    assert {row["applies_to_all_new_algorithm_devices"] for row in h2o_recheck_rows} == {False}
    assert {row["diagnostic_source_device_sn"] for row in h2o_recheck_rows} == {"01260607"}
    assert {row["release_gate_role"] for row in h2o_recheck_rows} == {
        "device_specific_diagnostic_only_not_generic_release_gate"
    }
    assert all("Observed high residual for SN01260607 only" in row["notes"] for row in h2o_recheck_rows)

    low_anchors = [
        row for row in rows if row["point_role"] == "h2o_low_water_anchor_from_co2_zero_gas"
    ]
    assert len(low_anchors) == 1
    assert low_anchors[0]["temperature_c"] == 40
    assert low_anchors[0]["route_component"] == "co2"
    assert low_anchors[0]["co2_ppm"] == 0
    assert "residual water is not forced to zero" in low_anchors[0]["notes"]


def test_new_algorithm_test_point_plan_writer_and_cli_create_artifacts(tmp_path: Path) -> None:
    output = tmp_path / "plan"
    outputs = write_v1_5_new_algorithm_test_point_plan(PROFILE_PATH, output)

    manifest = json.loads(
        output.joinpath("v1_5_new_algorithm_test_point_plan_manifest.json").read_text(
            encoding="utf-8-sig"
        )
    )
    assert manifest["profile_id"] == "absorption_ratio_shadow"
    assert Path(outputs["point_plan"]).exists()
    rows = _read_csv(output / "v1_5_new_algorithm_test_point_plan.csv")
    assert len(rows) == 10

    cli_output = tmp_path / "cli_plan"
    rc = cli_main(
        [
            "--profile-path",
            str(PROFILE_PATH),
            "--output-dir",
            str(cli_output),
        ]
    )
    assert rc == 0
    assert cli_output.joinpath("V1_5_NEW_ALGORITHM_TEST_POINT_PLAN.md").exists()
    role_rows = _read_csv(cli_output / "v1_5_new_algorithm_test_point_role_counts.csv")
    assert {row["point_role"] for row in role_rows} >= {
        "new_algorithm_supplemental_gas_point",
        "new_algorithm_supplemental_wet_point",
        "h2o_low_water_anchor_from_co2_zero_gas",
        "sn01260607_observed_conflict_gas_point",
        "sn01260607_observed_high_residual_wet_point",
    }
