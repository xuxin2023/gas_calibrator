import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_production_entrypoint_map import main as cli_main
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_production_entrypoint_map import (
    LEGACY_CO2_POINT_COUNT,
    LEGACY_H2O_WET_POINT_COUNT,
    MATURE_FITTING_BASELINE,
    MATURE_PHYSICAL_BASELINE,
    NEW_ALGORITHM_CO2_POINT_COUNT,
    NEW_ALGORITHM_H2O_WET_POINT_COUNT,
    build_v1_5_production_entrypoint_map,
    write_v1_5_production_entrypoint_map,
)


ROOT = Path(__file__).resolve().parents[1]


def _entrypoints_by_id(model: dict) -> dict[str, dict]:
    return {row["entrypoint_id"]: row for row in model["production_entrypoints"]}


def _forbidden_by_id(model: dict) -> dict[str, dict]:
    return {row["surface_id"]: row for row in model["forbidden_surfaces"]}


def _checks_by_id(model: dict) -> dict[str, dict]:
    return {row["check_id"]: row for row in model["checks"]}


def test_production_entrypoint_map_is_offline_and_locks_point_contracts() -> None:
    model = build_v1_5_production_entrypoint_map()
    manifest = model["manifest"]

    assert manifest["status"] == "pass"
    assert manifest["blocker_count"] == 0
    assert manifest["mature_fitting_baseline"] == MATURE_FITTING_BASELINE
    assert manifest["mature_physical_baseline"] == MATURE_PHYSICAL_BASELINE
    assert manifest["legacy_point_contract"] == {
        "co2": LEGACY_CO2_POINT_COUNT,
        "h2o_wet": LEGACY_H2O_WET_POINT_COUNT,
    }
    assert manifest["new_algorithm_profile_contract"] == {
        "co2": NEW_ALGORITHM_CO2_POINT_COUNT,
        "h2o_wet": NEW_ALGORITHM_H2O_WET_POINT_COUNT,
    }
    assert manifest["opens_com_ports"] is False
    assert manifest["controls_pressure"] is False
    assert manifest["controls_water_or_gas_routes"] is False
    assert manifest["connects_postgresql"] is False
    assert manifest["writes_coefficients"] is False
    assert manifest["writes_sn_or_device_code"] is False
    assert manifest["formal_release_allowed"] is False
    assert manifest["database_import_allowed"] is False
    assert manifest["not_real_acceptance_evidence"] is True


def test_production_entrypoints_separate_formal_routes_from_support_and_writes() -> None:
    model = build_v1_5_production_entrypoint_map()
    entries = _entrypoints_by_id(model)

    assert entries["co2_mature_legacy_queue"]["path"].endswith("run_v1_5_formal_co2_open_flow_queue.py")
    assert entries["co2_mature_legacy_queue"]["point_contract"] == "legacy CO2 45 points"
    assert entries["co2_mature_legacy_queue"]["mature_baseline"] == MATURE_PHYSICAL_BASELINE
    assert entries["co2_mature_legacy_queue"]["controls_routes_when_authorized"] is True
    assert "0624" in entries["co2_mature_legacy_queue"]["forbidden_substitutes"]

    assert entries["h2o_mature_legacy_queue"]["path"].endswith("run_v1_5_formal_h2o_open_flow_queue.py")
    assert entries["h2o_mature_legacy_queue"]["point_contract"] == "legacy H2O 13 wet points"
    assert "0624 handoff H2O queue" in entries["h2o_mature_legacy_queue"]["forbidden_substitutes"]

    assert entries["readonly_com_identity_getco_closeout"]["opens_com_when_authorized"] is True
    assert entries["readonly_com_identity_getco_closeout"]["writes_coefficients_when_authorized"] is False
    assert "Legacy analyzers must not receive CHECK" in entries["readonly_com_identity_getco_closeout"]["notes"]

    assert entries["controlled_coefficient_writes"]["writes_coefficients_when_authorized"] is True
    assert entries["controlled_coefficient_writes"]["launch_policy"] == "manual_authorized_controlled_write_only"
    assert "CLEARSENCO5,YGAS,FFF" in entries["controlled_coefficient_writes"]["notes"]


def test_forbidden_surfaces_block_handoff_migration_workers_and_v1_v2() -> None:
    model = build_v1_5_production_entrypoint_map()
    forbidden = _forbidden_by_id(model)
    checks = _checks_by_id(model)

    assert forbidden["handoff_evidence"]["policy"] == "not_a_production_launcher"
    assert forbidden["root_migration_area"]["policy"] == "not_a_mature_baseline"
    assert "0624 handoff logic" in forbidden["root_migration_area"]["examples"]
    assert forbidden["sampling_workers"]["policy"] == "worker_not_top_level"
    assert "run_v1_5_formal_open_flow_sampling.py" in forbidden["sampling_workers"]["examples"]
    assert forbidden["legacy_v1_or_v2_surfaces"]["policy"] == "not_v1_5_production_entry"

    assert checks["ENTRY-MAP-003"]["status"] == "pass"
    assert checks["ENTRY-MAP-004"]["status"] == "pass"
    assert "Root migration and 0624 handoff" in checks["ENTRY-MAP-004"]["requirement"]


def test_production_entrypoint_map_writer_and_cli(tmp_path: Path) -> None:
    output_dir = tmp_path / "entrypoint_map"
    paths = write_v1_5_production_entrypoint_map(output_dir=output_dir)

    assert paths["manifest"].exists()
    assert paths["entrypoints"].exists()
    assert paths["forbidden"].exists()
    assert paths["checks"].exists()
    assert paths["markdown"].exists()

    model = json.loads(paths["manifest"].read_text(encoding="utf-8"))
    markdown = paths["markdown"].read_text(encoding="utf-8")
    entrypoints_csv = paths["entrypoints"].read_text(encoding="utf-8")
    forbidden_csv = paths["forbidden"].read_text(encoding="utf-8")

    assert model["manifest"]["status"] == "pass"
    assert "V1.5 Production Entrypoint Map" in markdown
    assert "legacy CO2 45 points" in entrypoints_csv
    assert "worker_not_top_level" in forbidden_csv

    cli_dir = tmp_path / "cli"
    assert cli_main(["--output-dir", str(cli_dir), "--fail-on-blocker"]) == 0
    assert (cli_dir / "v1_5_production_entrypoint_map.json").exists()


def test_production_entrypoint_map_exporter_is_offline_review_evidence() -> None:
    entry = classify_v1_5_entrypoint(
        ROOT / "src/gas_calibrator/tools/export_v1_5_production_entrypoint_map.py",
        root=ROOT,
    )

    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
    assert any("production entrypoint map" in note for note in entry.notes)
