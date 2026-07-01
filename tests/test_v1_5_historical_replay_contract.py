import copy
import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_historical_replay_contract import main as cli_main
from gas_calibrator.validation.v1_5_historical_replay_contract import (
    DEFAULT_REPLAY_SOURCE_FAMILIES,
    EXPECTED_ABSORPTION_FORMULA,
    build_v1_5_historical_replay_contract,
    write_v1_5_historical_replay_contract,
)


ROOT = Path(__file__).resolve().parents[1]
PROFILE_PATH = ROOT / "configs" / "v1_5_algorithm_route_profiles.json"


def _load_profile() -> dict:
    return json.loads(PROFILE_PATH.read_text(encoding="utf-8"))


def _write_profile(tmp_path: Path, payload: dict) -> Path:
    path = tmp_path / "v1_5_algorithm_route_profiles.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _check_by_id(model: dict) -> dict[str, dict]:
    return {row["check_id"]: row for row in model["checks"]}


def _source_families() -> list[dict]:
    return copy.deepcopy(list(DEFAULT_REPLAY_SOURCE_FAMILIES))


def test_v1_5_historical_replay_contract_current_profile_passes() -> None:
    model = build_v1_5_historical_replay_contract(profile_path=PROFILE_PATH)
    checks = _check_by_id(model)

    assert model["manifest"]["status"] == "pass"
    assert model["manifest"]["blocker_count"] == 0
    assert model["manifest"]["opens_com_ports"] is False
    assert model["manifest"]["connects_postgresql"] is False
    assert model["manifest"]["controls_water_or_gas_routes"] is False
    assert model["manifest"]["writes_coefficients"] is False
    assert model["manifest"]["not_real_acceptance_evidence"] is True
    assert model["manifest"]["historical_replay_contract"]["legacy_replay_fit_input"] == "R_CO2/R_H2O"
    assert (
        model["manifest"]["historical_replay_contract"]["new_algorithm_replay_fit_input"]
        == EXPECTED_ABSORPTION_FORMULA
    )

    assert checks["historical_replay_is_offline_only"]["status"] == "pass"
    assert checks["legacy_replay_uses_mature_ratio_profile"]["status"] == "pass"
    assert checks["legacy_replay_preserves_45_13_counts"]["status"] == "pass"
    assert checks["new_algorithm_replay_uses_absorption_shadow"]["status"] == "pass"
    assert checks["new_algorithm_replay_requires_r0_evidence"]["status"] == "pass"
    assert checks["replay_qc_rejections_remain_non_fit"]["status"] == "pass"
    assert checks["replay_does_not_authorize_archive_or_database_release"]["status"] == "pass"


def test_v1_5_historical_replay_contract_blocks_legacy_replay_absorption_drift(tmp_path: Path) -> None:
    config = copy.deepcopy(_load_profile())
    legacy = next(profile for profile in config["profiles"] if profile["profile_id"] == "legacy_ratio_production")
    legacy["algorithm_mode"] = "absorption_ratio_A"
    legacy["fit_input"]["co2"] = "A_CO2_from_R_CO2_and_R0_CO2_T"
    profile_path = _write_profile(tmp_path, config)

    model = build_v1_5_historical_replay_contract(profile_path=profile_path)
    checks = _check_by_id(model)

    assert model["manifest"]["status"] == "blocked"
    assert checks["legacy_replay_uses_mature_ratio_profile"]["status"] == "blocker"


def test_v1_5_historical_replay_contract_blocks_missing_qc_role() -> None:
    families = _source_families()
    families[0]["required_roles"].remove("point_qc_and_quality_grade")

    model = build_v1_5_historical_replay_contract(
        profile_path=PROFILE_PATH,
        replay_source_families=families,
    )
    checks = _check_by_id(model)

    assert model["manifest"]["status"] == "blocked"
    assert checks["replay_source_families_have_required_roles"]["status"] == "blocker"
    assert checks["legacy_replay_uses_mature_ratio_profile"]["status"] == "pass"


def test_v1_5_historical_replay_contract_blocks_rejected_points_becoming_fit_eligible() -> None:
    families = _source_families()
    families[1]["qc_policy"]["sample_quality_rejects_remain_rejected"] = False

    model = build_v1_5_historical_replay_contract(
        profile_path=PROFILE_PATH,
        replay_source_families=families,
    )
    checks = _check_by_id(model)

    assert model["manifest"]["status"] == "blocked"
    assert checks["replay_qc_rejections_remain_non_fit"]["status"] == "blocker"
    assert checks["replay_does_not_authorize_archive_or_database_release"]["status"] == "pass"


def test_v1_5_historical_replay_contract_blocks_replay_release_promotion() -> None:
    families = _source_families()
    families[0]["release_policy"]["formal_release_allowed_from_replay"] = True
    families[0]["release_policy"]["database_import_allowed_from_replay"] = True

    model = build_v1_5_historical_replay_contract(
        profile_path=PROFILE_PATH,
        replay_source_families=families,
    )
    checks = _check_by_id(model)

    assert model["manifest"]["status"] == "blocked"
    assert checks["replay_does_not_authorize_archive_or_database_release"]["status"] == "blocker"


def test_v1_5_historical_replay_contract_blocks_absorption_formula_drift(tmp_path: Path) -> None:
    config = copy.deepcopy(_load_profile())
    absorption = next(profile for profile in config["profiles"] if profile["profile_id"] == "absorption_ratio_shadow")
    absorption["fit_input"]["formula"] = "A=-ln(R)/(P_kPa/100)"
    profile_path = _write_profile(tmp_path, config)

    model = build_v1_5_historical_replay_contract(profile_path=profile_path)
    checks = _check_by_id(model)

    assert model["manifest"]["status"] == "blocked"
    assert checks["new_algorithm_replay_uses_absorption_shadow"]["status"] == "blocker"
    assert checks["legacy_replay_uses_mature_ratio_profile"]["status"] == "pass"


def test_v1_5_historical_replay_contract_blocks_r0_release_promotion(tmp_path: Path) -> None:
    config = copy.deepcopy(_load_profile())
    absorption = next(profile for profile in config["profiles"] if profile["profile_id"] == "absorption_ratio_shadow")
    absorption["r0_write_contract"]["status"] = "ready"
    absorption["r0_write_contract"]["components"][0]["production_blocker"] = False
    absorption["r0_write_contract"]["components"][0]["controlled_writer_status"] = "ready"
    profile_path = _write_profile(tmp_path, config)

    model = build_v1_5_historical_replay_contract(profile_path=profile_path)
    checks = _check_by_id(model)

    assert model["manifest"]["status"] == "blocked"
    assert checks["new_algorithm_replay_requires_r0_evidence"]["status"] == "blocker"


def test_v1_5_historical_replay_contract_writer_and_cli(tmp_path: Path) -> None:
    output = tmp_path / "contract"
    outputs = write_v1_5_historical_replay_contract(profile_path=PROFILE_PATH, output_dir=output)

    manifest = json.loads(Path(outputs["manifest"]).read_text(encoding="utf-8"))
    assert manifest["manifest"]["status"] == "pass"
    markdown = Path(outputs["markdown"]).read_text(encoding="utf-8")
    assert markdown.startswith("# V1.5 Historical Replay Contract")
    assert "replay_pass_does_not_authorize_archive_or_database_import" in markdown
    with Path(outputs["checks"]).open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert {row["check_id"] for row in rows} >= {
        "legacy_replay_uses_mature_ratio_profile",
        "new_algorithm_replay_uses_absorption_shadow",
        "replay_does_not_authorize_archive_or_database_release",
    }

    cli_output = tmp_path / "cli"
    rc = cli_main(
        [
            "--profile-path",
            str(PROFILE_PATH),
            "--output-dir",
            str(cli_output),
            "--fail-on-blocker",
        ]
    )

    assert rc == 0
    assert (cli_output / "v1_5_historical_replay_contract.json").exists()
