import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_full_flow_automation_closure import main as cli_main
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_full_flow_automation_closure import (
    MATURE_FITTING_BASELINE,
    MATURE_ROUTE_BASELINE,
    PROHIBITED_FORMAL_SURFACES,
    PROTECTED_CORE_FILES,
    build_v1_5_full_flow_automation_closure,
    write_v1_5_full_flow_automation_closure,
)


ROOT = Path(__file__).resolve().parents[1]


def _stage_by_id(model: dict) -> dict[str, dict]:
    return {row["stage_id"]: row for row in model["stages"]}


def _check_by_id(model: dict) -> dict[str, dict]:
    return {row["check_id"]: row for row in model["checks"]}


def test_full_flow_automation_closure_locks_mature_baseline_and_boundaries() -> None:
    model = build_v1_5_full_flow_automation_closure()
    stages = _stage_by_id(model)
    checks = _check_by_id(model)

    assert model["overall_status"] == "review_ready"
    assert model["automation_closure_status"] == "structure_closed_live_full_auto_still_gated"
    assert model["mature_fitting_baseline"] == MATURE_FITTING_BASELINE == "0613 V1.5 fitting path"
    assert model["mature_route_baseline"] == MATURE_ROUTE_BASELINE == "0620/0621 clean-worktree mature physical route path"
    assert model["legacy_point_counts"] == {"co2": 45, "h2o": 13}
    assert model["new_algorithm_profile_point_counts"] == {"co2": 47, "h2o": 14}
    assert model["full_production_auto_allowed"] is False
    assert model["formal_release_allowed"] is False
    assert model["database_import_allowed"] is False
    assert model["opens_com_ports"] is False
    assert model["controls_water_or_gas_routes"] is False
    assert model["connects_postgresql"] is False
    assert model["writes_coefficients"] is False
    assert model["not_real_acceptance_evidence"] is True

    assert stages["04_mature_legacy_co2_45_route"]["canonical_entrypoint"] == (
        "run_v1_5_formal_co2_open_flow_queue.py"
    )
    assert stages["05_mature_legacy_h2o_13_route"]["canonical_entrypoint"] == (
        "run_v1_5_formal_h2o_open_flow_queue.py"
    )
    assert "0624" in checks["AUTO-CLOSURE-001"]["evidence_rule"]
    assert "root migration" in checks["AUTO-CLOSURE-002"]["requirement"]
    assert "not one-click full production automation" in checks["AUTO-CLOSURE-006"]["requirement"]


def test_full_flow_automation_closure_stage_order_and_remaining_gaps() -> None:
    model = build_v1_5_full_flow_automation_closure()
    stage_ids = [row["stage_id"] for row in model["stages"]]

    assert stage_ids == [
        "01_initialization_identity_runtime_closeout",
        "02_pressure_s9_readiness",
        "03_route_physical_readiness_guard",
        "04_mature_legacy_co2_45_route",
        "05_mature_legacy_h2o_13_route",
        "06_fit_strategy_review",
        "07_controlled_write_readback",
        "08_short_reverify",
        "09_archive_report_database",
    ]
    assert model["remaining_full_auto_gap_count"] == len(model["stages"])
    assert all(row["blocks_full_auto"] is True for row in model["stages"])
    assert "_handoff" in PROHIBITED_FORMAL_SURFACES
    assert "0624 migrated route path" in PROHIBITED_FORMAL_SURFACES
    assert "src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py" in PROTECTED_CORE_FILES
    assert "src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_queue.py" in PROTECTED_CORE_FILES
    assert "run_app.py" in PROTECTED_CORE_FILES


def test_full_flow_automation_closure_writer_and_cli(tmp_path: Path) -> None:
    output_dir = tmp_path / "closure"
    paths = write_v1_5_full_flow_automation_closure(output_dir=output_dir)

    assert paths["manifest"].exists()
    assert paths["stages"].exists()
    assert paths["checks"].exists()
    assert paths["markdown"].exists()

    model = json.loads(paths["manifest"].read_text(encoding="utf-8"))
    markdown = paths["markdown"].read_text(encoding="utf-8")
    stages_csv = paths["stages"].read_text(encoding="utf-8")
    checks_csv = paths["checks"].read_text(encoding="utf-8")

    assert model["automation_closure_status"] == "structure_closed_live_full_auto_still_gated"
    assert "V1.5 structure and guardrails are organized" in markdown
    assert "04_mature_legacy_co2_45_route" in stages_csv
    assert "AUTO-CLOSURE-006" in checks_csv

    cli_dir = tmp_path / "cli"
    assert cli_main(["--output-dir", str(cli_dir)]) == 0
    assert (cli_dir / "v1_5_full_flow_automation_closure.json").exists()


def test_full_flow_automation_closure_exporter_is_offline_review_evidence() -> None:
    entry = classify_v1_5_entrypoint(
        ROOT / "src/gas_calibrator/tools/export_v1_5_full_flow_automation_closure.py",
        root=ROOT,
    )

    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
    assert any("full-flow automation closure map" in note for note in entry.notes)
