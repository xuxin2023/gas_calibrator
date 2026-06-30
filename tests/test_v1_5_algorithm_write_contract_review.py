import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_algorithm_write_contract_review import main as cli_main
from gas_calibrator.validation.v1_5_algorithm_route_profiles import (
    build_v1_5_algorithm_write_contract_tables,
    write_v1_5_algorithm_write_contract_review,
)


ROOT = Path(__file__).resolve().parents[1]
PROFILE_PATH = ROOT / "configs" / "v1_5_algorithm_route_profiles.json"


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def test_algorithm_write_contract_review_is_offline_and_separates_co2_layers() -> None:
    tables = build_v1_5_algorithm_write_contract_tables(PROFILE_PATH)
    manifest = tables["manifest"]
    rows = {row["profile_id"]: row for row in tables["co2_write_contracts"]}

    assert manifest["no_write"] is True
    assert manifest["opens_com_ports"] is False
    assert manifest["controls_water_or_gas_routes"] is False
    assert manifest["writes_coefficients"] is False
    assert manifest["contract_scope"] == "co2_main_chain_h2o_main_chain_final_linear_trims_and_r0_blockers"

    assert rows["legacy_ratio_production"]["algorithm_contract"] == "old_ratio_temperature"
    assert rows["legacy_ratio_production"]["main_chain_coefficients"] == "SENCO1;SENCO3"
    assert rows["legacy_ratio_production"]["senco5_must_not_fold_into_main_chain"] is True
    assert (
        rows["legacy_ratio_production"]["senco5_clear_command_required_for_neutralization"]
        == "CLEARSENCO5,YGAS,FFF"
    )

    new_row = rows["absorption_ratio_shadow"]
    assert new_row["algorithm_contract"] == "old7_absorption_A_TK_zero1ppm"
    assert new_row["fit_input"] == "A=-ln(R/R0(T))/(P_kPa/100)"
    assert new_row["temperature_feature"] == "absolute_chamber_temperature_kelvin"
    assert "exclusion_fit_gate" in new_row["required_review_checks"]
    assert "low_temperature_coverage_guard" in new_row["required_review_checks"]
    assert new_row["senco5_review_after_main_chain_reverification"] is True
    assert new_row["senco5_controlled_writer"] == (
        "gas_calibrator.tools.run_v1_5_co2_senco5_linear_controlled_write"
    )


def test_algorithm_write_contract_review_includes_h2o_layers_and_r0_blockers() -> None:
    tables = build_v1_5_algorithm_write_contract_tables(PROFILE_PATH)
    h2o_rows = {row["profile_id"]: row for row in tables["h2o_write_contracts"]}
    r0_rows = {(row["profile_id"], row["component"]): row for row in tables["r0_write_contracts"]}

    legacy_h2o = h2o_rows["legacy_ratio_production"]
    assert legacy_h2o["algorithm_contract"] == "old_ratio_temperature"
    assert legacy_h2o["main_chain_coefficients"] == "SENCO2;SENCO4"
    assert (
        legacy_h2o["main_chain_controlled_writer"]
        == "gas_calibrator.tools.run_v1_5_h2o_senco24_controlled_write"
    )
    assert legacy_h2o["senco6_clear_command_required_for_neutralization"] == "CLEARSENCO6,YGAS,FFF"

    new_h2o = h2o_rows["absorption_ratio_shadow"]
    assert new_h2o["contract_status"] == "blocked_pending_firmware_input_scale_confirmation"
    assert new_h2o["fit_input"] == "A_H2O=-ln(R_H2O/R0_H2O(T))/(P_kPa/100)"
    assert "R0_H2O_T_contract_confirmed" in new_h2o["required_review_checks"]
    assert new_h2o["alternate_absorption_status"] == "diagnostic_only_not_default_production"
    assert new_h2o["senco6_review_after_main_chain_reverification"] is True

    co2_r0 = r0_rows[("absorption_ratio_shadow", "co2")]
    h2o_r0 = r0_rows[("absorption_ratio_shadow", "h2o")]
    assert co2_r0["coefficient_group"] == "SENCOA"
    assert co2_r0["readback_group"] == "GETCOA"
    assert h2o_r0["coefficient_group"] == "SENCOB"
    assert h2o_r0["readback_group"] == "GETCOB"
    assert {co2_r0["controlled_writer_status"], h2o_r0["controlled_writer_status"]} == {
        "missing_controlled_writer"
    }
    assert co2_r0["production_blocker"] is True
    assert h2o_r0["production_blocker"] is True


def test_algorithm_write_contract_writer_and_cli_create_artifacts(tmp_path: Path) -> None:
    output = tmp_path / "write_contract"
    outputs = write_v1_5_algorithm_write_contract_review(PROFILE_PATH, output)

    manifest = json.loads(
        output.joinpath("v1_5_algorithm_write_contract_manifest.json").read_text(
            encoding="utf-8-sig"
        )
    )
    assert manifest["default_profile_id"] == "legacy_ratio_production"
    assert Path(outputs["co2_write_contracts"]).exists()
    assert Path(outputs["h2o_write_contracts"]).exists()
    assert Path(outputs["r0_write_contracts"]).exists()
    rows = _read_csv(output / "v1_5_co2_algorithm_write_contracts.csv")
    assert {row["profile_id"] for row in rows} == {
        "legacy_ratio_production",
        "absorption_ratio_shadow",
    }
    h2o_rows = _read_csv(output / "v1_5_h2o_algorithm_write_contracts.csv")
    assert any(row["algorithm_contract"] == "old_ratio_temperature" for row in h2o_rows)
    r0_rows = _read_csv(output / "v1_5_r0_algorithm_write_contracts.csv")
    assert {row["coefficient_group"] for row in r0_rows} == {"SENCOA", "SENCOB"}

    cli_output = tmp_path / "cli_write_contract"
    rc = cli_main(
        [
            "--profile-path",
            str(PROFILE_PATH),
            "--output-dir",
            str(cli_output),
        ]
    )
    assert rc == 0
    assert cli_output.joinpath("V1_5_ALGORITHM_WRITE_CONTRACT_REVIEW.md").exists()
    cli_rows = _read_csv(cli_output / "v1_5_co2_algorithm_write_contracts.csv")
    assert any(row["algorithm_contract"] == "old7_absorption_A_TK_zero1ppm" for row in cli_rows)
    cli_r0_rows = _read_csv(cli_output / "v1_5_r0_algorithm_write_contracts.csv")
    assert all(row["controlled_writer_status"] == "missing_controlled_writer" for row in cli_r0_rows)
