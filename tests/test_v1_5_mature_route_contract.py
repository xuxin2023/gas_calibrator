import copy
import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_mature_route_contract import main as cli_main
from gas_calibrator.validation.v1_5_mature_route_contract import (
    build_v1_5_mature_route_contract,
    write_v1_5_mature_route_contract,
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


def test_v1_5_mature_route_contract_current_profile_passes() -> None:
    model = build_v1_5_mature_route_contract(profile_path=PROFILE_PATH)
    checks = _check_by_id(model)

    assert model["manifest"]["status"] == "pass"
    assert model["manifest"]["blocker_count"] == 0
    assert model["manifest"]["opens_com_ports"] is False
    assert model["manifest"]["connects_postgresql"] is False
    assert model["manifest"]["controls_water_or_gas_routes"] is False
    assert model["manifest"]["writes_coefficients"] is False
    assert model["manifest"]["not_real_acceptance_evidence"] is True
    assert model["manifest"]["mature_route_contract"]["legacy_co2_point_count"] == 45
    assert model["manifest"]["mature_route_contract"]["legacy_h2o_wet_point_count"] == 13

    assert checks["shared_route_behavior_0620"]["status"] == "pass"
    assert checks["legacy_co2_45_point_contract"]["status"] == "pass"
    assert checks["legacy_h2o_13_point_contract"]["status"] == "pass"
    assert checks["absorption_profile_fit_input_only"]["status"] == "pass"
    assert checks["r0_writer_contract_blocks_absorption_release"]["status"] == "pass"
    assert checks["canonical_entrypoint_guard"]["status"] == "pass"


def test_v1_5_mature_route_contract_blocks_legacy_co2_point_drift(tmp_path: Path) -> None:
    config = copy.deepcopy(_load_profile())
    legacy = next(profile for profile in config["profiles"] if profile["profile_id"] == "legacy_ratio_production")
    legacy["co2_route"]["temperature_plan"]["-20"] = [0, 400, 600, 1000]
    legacy["co2_route"]["formal_point_count"] = 46
    profile_path = _write_profile(tmp_path, config)

    model = build_v1_5_mature_route_contract(profile_path=profile_path)
    checks = _check_by_id(model)

    assert model["manifest"]["status"] == "blocked"
    assert checks["legacy_co2_45_point_contract"]["status"] == "blocker"
    assert checks["legacy_h2o_13_point_contract"]["status"] == "pass"


def test_v1_5_mature_route_contract_blocks_absorption_runner_fork(tmp_path: Path) -> None:
    config = copy.deepcopy(_load_profile())
    absorption = next(profile for profile in config["profiles"] if profile["profile_id"] == "absorption_ratio_shadow")
    absorption["co2_route"]["runner"] = "gas_calibrator.tools.experimental_absorption_co2_queue"
    profile_path = _write_profile(tmp_path, config)

    model = build_v1_5_mature_route_contract(profile_path=profile_path)
    checks = _check_by_id(model)

    assert model["manifest"]["status"] == "blocked"
    assert checks["shared_route_runner_names"]["status"] == "blocker"
    assert checks["absorption_profile_fit_input_only"]["status"] == "pass"


def test_v1_5_mature_route_contract_blocks_r0_writer_promotion_without_contract(tmp_path: Path) -> None:
    config = copy.deepcopy(_load_profile())
    absorption = next(profile for profile in config["profiles"] if profile["profile_id"] == "absorption_ratio_shadow")
    absorption["r0_write_contract"]["status"] = "ready"
    absorption["r0_write_contract"]["components"][0]["production_blocker"] = False
    absorption["r0_write_contract"]["components"][0]["controlled_writer_status"] = "ready"
    profile_path = _write_profile(tmp_path, config)

    model = build_v1_5_mature_route_contract(profile_path=profile_path)
    checks = _check_by_id(model)

    assert model["manifest"]["status"] == "blocked"
    assert checks["r0_writer_contract_blocks_absorption_release"]["status"] == "blocker"


def test_v1_5_mature_route_contract_writer_and_cli(tmp_path: Path) -> None:
    output = tmp_path / "contract"
    outputs = write_v1_5_mature_route_contract(profile_path=PROFILE_PATH, output_dir=output)

    manifest = json.loads(Path(outputs["manifest"]).read_text(encoding="utf-8"))
    assert manifest["manifest"]["status"] == "pass"
    assert Path(outputs["markdown"]).read_text(encoding="utf-8").startswith("# V1.5 Mature Route Contract")
    with Path(outputs["checks"]).open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert {row["check_id"] for row in rows} >= {
        "legacy_co2_45_point_contract",
        "legacy_h2o_13_point_contract",
        "supplement_points_do_not_modify_legacy_queue",
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
    assert (cli_output / "v1_5_mature_route_contract.json").exists()
