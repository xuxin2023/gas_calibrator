import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_full_flow_next_action_plan import main as cli_main
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_full_flow_automation_closure import (
    MATURE_FITTING_BASELINE,
    MATURE_ROUTE_BASELINE,
    build_v1_5_full_flow_automation_closure,
)
from gas_calibrator.validation.v1_5_full_flow_next_action_plan import (
    SCHEMA,
    build_v1_5_full_flow_next_action_plan,
    write_v1_5_full_flow_next_action_plan,
)


ROOT = Path(__file__).resolve().parents[1]


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def test_next_action_plan_ranks_initialization_closeout_first() -> None:
    model = build_v1_5_full_flow_next_action_plan()
    first = model["next_actions"][0]

    assert model["schema"] == SCHEMA
    assert model["overall_status"] == "review_ready"
    assert model["mature_fitting_baseline"] == MATURE_FITTING_BASELINE == "0613 V1.5 fitting path"
    assert model["mature_route_baseline"] == MATURE_ROUTE_BASELINE
    assert model["legacy_point_counts"] == {"co2": 45, "h2o": 13}
    assert model["new_algorithm_profile_point_counts"] == {"co2": 47, "h2o": 14}
    assert model["recommended_next_action_id"] == "batch_initialization_closeout_pre_gas_evidence_index"
    assert first["source_stage_id"] == "01_initialization_identity_runtime_closeout"
    assert first["action_type"] == "offline_evidence_binder"
    assert "SN/device_code" in first["recommended_pr_scope"]
    assert "pre-gas evidence index" in first["recommended_pr_scope"]
    assert "no COM" in first["forbidden_scope"]
    assert "no gas/water route" in first["forbidden_scope"]
    assert model["full_production_auto_allowed"] is False
    assert model["formal_release_allowed"] is False
    assert model["database_import_allowed"] is False
    assert model["opens_com_ports"] is False
    assert model["controls_water_or_gas_routes"] is False
    assert model["connects_postgresql"] is False
    assert model["writes_coefficients"] is False
    assert model["not_real_acceptance_evidence"] is True


def test_next_action_plan_reviews_dirty_or_mismatched_closure(tmp_path: Path) -> None:
    closure = build_v1_5_full_flow_automation_closure()
    closure["full_production_auto_allowed"] = True
    closure["legacy_point_counts"] = {"co2": 47, "h2o": 14}
    closure_path = _write_json(tmp_path / "closure" / "v1_5_full_flow_automation_closure.json", closure)

    model = build_v1_5_full_flow_next_action_plan(automation_closure_json=closure_path)

    assert model["overall_status"] == "review_required"
    assert "full_production_auto_allowed_not_false" in model["review_reasons"]
    assert "legacy_point_counts_not_45_13" in model["review_reasons"]
    assert model["full_production_auto_allowed"] is False
    assert model["opens_com_ports"] is False


def test_next_action_plan_writer_and_cli(tmp_path: Path) -> None:
    closure_path = _write_json(
        tmp_path / "inputs" / "v1_5_full_flow_automation_closure.json",
        build_v1_5_full_flow_automation_closure(),
    )
    output_dir = tmp_path / "next_action"
    paths = write_v1_5_full_flow_next_action_plan(
        output_dir=output_dir,
        automation_closure_json=closure_path,
    )

    assert paths["manifest"].exists()
    assert paths["actions"].exists()
    assert paths["markdown"].exists()
    model = json.loads(paths["manifest"].read_text(encoding="utf-8"))
    actions_csv = paths["actions"].read_text(encoding="utf-8")
    markdown = paths["markdown"].read_text(encoding="utf-8")

    assert model["recommended_next_action_id"] == "batch_initialization_closeout_pre_gas_evidence_index"
    assert "pressure_s9_exception_and_reverify_evidence_index" in actions_csv
    assert "This plan ranks the remaining V1.5 automation handoffs" in markdown

    cli_dir = tmp_path / "cli"
    assert cli_main(["--output-dir", str(cli_dir), "--automation-closure-json", str(closure_path)]) == 0
    assert (cli_dir / "v1_5_full_flow_next_action_plan.json").exists()


def test_next_action_plan_exporter_is_offline_review_evidence() -> None:
    entry = classify_v1_5_entrypoint(
        ROOT / "src/gas_calibrator/tools/export_v1_5_full_flow_next_action_plan.py",
        root=ROOT,
    )

    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
    assert any("next-action plan" in note for note in entry.notes)
