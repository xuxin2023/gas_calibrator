import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_automation_control_contract import main as cli_main
from gas_calibrator.validation.v1_5_automation_control_contract import (
    CANONICAL_AUTOMATION_STAGES,
    PROTECTED_CORE_FILES,
    build_v1_5_automation_control_contract,
    write_v1_5_automation_control_contract,
)
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint


ROOT = Path(__file__).resolve().parents[1]


def _checks_by_id(model: dict) -> dict[str, dict]:
    return {row["check_id"]: row for row in model["checks"]}


def test_v1_5_automation_contract_locks_mature_core_and_no_live_actions() -> None:
    model = build_v1_5_automation_control_contract()
    manifest = model["manifest"]
    checks = _checks_by_id(model)

    assert manifest["status"] == "pass"
    assert manifest["blocker_count"] == 0
    assert manifest["automation_model"] == "mature_core_with_automation_shell"
    assert manifest["mature_fitting_baseline"] == "0613-style V1.5 fitting method"
    assert manifest["mature_physical_baseline"] == "0620/0621 mature physical execution path"
    assert manifest["legacy_co2_point_count"] == 45
    assert manifest["legacy_h2o_wet_point_count"] == 13
    assert manifest["new_algorithm_profile_point_count"] == {"co2": 47, "h2o": 14}
    assert manifest["opens_com_ports"] is False
    assert manifest["controls_water_or_gas_routes"] is False
    assert manifest["connects_postgresql"] is False
    assert manifest["writes_coefficients"] is False
    assert manifest["writes_sn_or_device_code"] is False
    assert manifest["formal_release_allowed"] is False
    assert manifest["database_import_allowed"] is False
    assert manifest["not_real_acceptance_evidence"] is True

    assert checks["AUTO-CORE-001"]["status"] == "pass"
    assert "automation is a shell" in checks["AUTO-CORE-001"]["requirement"]
    assert "Do not start production from diagnostic" in checks["AUTO-ENTRY-001"]["forbidden_failure_mode"]
    assert "PACE INL absolute pressure" in checks["AUTO-PRESS-001"]["requirement"]
    assert "CLEARSENCO5,YGAS,FFF" in checks["AUTO-FIT-001"]["requirement"]
    assert "Keep CO2 zero gas and H2O dry-gas" in checks["AUTO-FIT-003"]["requirement"]
    assert "no_write_candidate" in checks["AUTO-EVID-001"]["requirement"]


def test_v1_5_automation_contract_stage_order_and_protected_files() -> None:
    model = build_v1_5_automation_control_contract()
    manifest = model["manifest"]

    assert tuple(manifest["canonical_automation_stages"]) == CANONICAL_AUTOMATION_STAGES
    assert manifest["canonical_automation_stages"].index("03_mature_legacy_co2_45_route") < manifest[
        "canonical_automation_stages"
    ].index("04_mature_legacy_h2o_13_route")
    assert "src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py" in PROTECTED_CORE_FILES
    assert "src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_queue.py" in PROTECTED_CORE_FILES
    assert "src/gas_calibrator/workflow/runner.py" in PROTECTED_CORE_FILES
    assert "configs/default_config.json" in PROTECTED_CORE_FILES
    assert "run_app.py" in PROTECTED_CORE_FILES


def test_v1_5_automation_contract_writer_and_cli(tmp_path: Path) -> None:
    output_dir = tmp_path / "automation_contract"
    paths = write_v1_5_automation_control_contract(output_dir=output_dir)

    assert paths["manifest"].exists()
    assert paths["checks"].exists()
    assert paths["markdown"].exists()

    model = json.loads(paths["manifest"].read_text(encoding="utf-8"))
    markdown = paths["markdown"].read_text(encoding="utf-8")
    checks_csv = paths["checks"].read_text(encoding="utf-8")

    assert model["manifest"]["status"] == "pass"
    assert "V1.5 automation is an orchestration shell" in markdown
    assert "0620/0621 mature physical execution path" in markdown
    assert "AUTO-CORE-001" in checks_csv
    assert "AUTO-DB-001" in checks_csv

    cli_dir = tmp_path / "cli"
    assert cli_main(["--output-dir", str(cli_dir)]) == 0
    assert (cli_dir / "v1_5_automation_control_contract.json").exists()


def test_v1_5_automation_exporter_is_offline_review_evidence() -> None:
    entry = classify_v1_5_entrypoint(
        ROOT / "src/gas_calibrator/tools/export_v1_5_automation_control_contract.py",
        root=ROOT,
    )

    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
    assert any("automation control contract" in note for note in entry.notes)
