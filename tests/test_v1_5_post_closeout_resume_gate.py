import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_post_closeout_resume_gate import main as cli_main
from gas_calibrator.v1_5.orchestration.full_flow import build_full_flow_plan, write_full_flow_plan
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_post_closeout_resume_gate import (
    BLOCKED_STATUS,
    READY_STATUS,
    SCHEMA,
    build_v1_5_post_closeout_resume_gate,
)


ROOT = Path(__file__).resolve().parents[1]


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _ready_batch_payload() -> dict:
    return {
        "schema": "v1_5_batch_initialization_closeout_index_v1",
        "overall_status": "ready_for_mature_open_flow_from_initialization_index",
        "batch_initialization_closeout_ready": True,
        "ready_for_mature_open_flow_from_initialization_index": True,
        "device_count": 6,
        "device_ready_count": 6,
        "mature_route_baseline": "0620/0621 clean worktree mature physical route",
        "mature_fitting_baseline": "0613 V1.5 fitting path",
        "opens_com_ports": False,
        "read_only_real_com_execution_allowed": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "connects_postgresql": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "database_written": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
        "review_reasons": [],
    }


def _plan_and_batch(tmp_path: Path) -> tuple[Path, Path]:
    config = _write_json(tmp_path / "config.json", {})
    output_dir = tmp_path / "flow"
    plan = build_full_flow_plan(config_path=config, output_dir=output_dir, run_id="resume-demo")
    plan_path = write_full_flow_plan(plan, output_dir)["json"]
    batch_path = _write_json(
        output_dir / "batch_initialization_closeout_index" / "v1_5_batch_initialization_closeout_index.json",
        _ready_batch_payload(),
    )
    return plan_path, batch_path


def test_post_closeout_resume_gate_binds_ready_prefix_without_applying_it(tmp_path: Path) -> None:
    plan_path, batch_path = _plan_and_batch(tmp_path)

    model = build_v1_5_post_closeout_resume_gate(
        full_flow_plan_json=plan_path,
        batch_initialization_closeout_json=batch_path,
    )

    assert model["schema"] == SCHEMA
    assert model["overall_status"] == READY_STATUS
    assert model["resume_gate_ready"] is True
    assert model["ready_for_resume_state_application_review"] is True
    assert model["resume_completed_step_ids"][-2:] == [
        "batch_initialization_closeout_index",
        "post_closeout_resume_gate_snapshot",
    ]
    assert model["next_step_id"] == "temperature_channel_fast_review"
    assert model["downstream_route_step_ids"] == ["co2_open_flow_sampling", "h2o_open_flow_sampling"]
    assert model["route_authorization_still_required"] is True
    assert len(model["full_flow_plan_sha256"]) == 64
    assert len(model["batch_initialization_closeout_sha256"]) == 64
    assert model["does_not_execute_commands"] is True
    assert model["applies_completed_steps"] is False
    assert model["live_resume_execution_allowed"] is False
    assert model["opens_com_ports"] is False
    assert model["controls_water_or_gas_routes"] is False
    assert model["writes_coefficients"] is False
    assert model["connects_postgresql"] is False


def test_post_closeout_resume_gate_blocks_incomplete_batch(tmp_path: Path) -> None:
    plan_path, batch_path = _plan_and_batch(tmp_path)
    batch = _ready_batch_payload()
    batch["overall_status"] = "review_required"
    batch["batch_initialization_closeout_ready"] = False
    batch["device_ready_count"] = 5
    _write_json(batch_path, batch)

    model = build_v1_5_post_closeout_resume_gate(
        full_flow_plan_json=plan_path,
        batch_initialization_closeout_json=batch_path,
    )

    assert model["overall_status"] == BLOCKED_STATUS
    assert model["resume_gate_ready"] is False
    assert model["resume_completed_step_ids"] == []
    assert "batch_closeout_status_not_ready" in model["review_reasons"]
    assert "batch_device_readiness_incomplete" in model["review_reasons"]


def test_post_closeout_resume_gate_blocks_0624_or_noncanonical_queue(tmp_path: Path) -> None:
    plan_path, batch_path = _plan_and_batch(tmp_path)
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    co2 = next(row for row in plan["steps"] if row["step_id"] == "co2_open_flow_sampling")
    co2["tool_module"] = "gas_calibrator.tools.run_v1_5_formal_co2_0624_queue"
    _write_json(plan_path, plan)

    model = build_v1_5_post_closeout_resume_gate(
        full_flow_plan_json=plan_path,
        batch_initialization_closeout_json=batch_path,
    )

    assert model["overall_status"] == BLOCKED_STATUS
    assert "canonical_module_mismatch:co2_open_flow_sampling" in model["review_reasons"]
    assert any("forbidden_resume_surface:co2_open_flow_sampling:0624" == row for row in model["review_reasons"])


def test_post_closeout_resume_gate_blocks_batch_path_not_declared_by_plan(tmp_path: Path) -> None:
    plan_path, batch_path = _plan_and_batch(tmp_path)
    alternate_batch = _write_json(tmp_path / "other_batch" / "closeout.json", _ready_batch_payload())

    model = build_v1_5_post_closeout_resume_gate(
        full_flow_plan_json=plan_path,
        batch_initialization_closeout_json=alternate_batch,
    )

    assert model["overall_status"] == BLOCKED_STATUS
    assert model["resume_gate_ready"] is False
    assert "resume_gate_batch_path_mismatch_with_full_flow_plan" in model["review_reasons"]
    assert batch_path != alternate_batch


def test_post_closeout_resume_gate_cli_fails_closed_and_is_offline_entrypoint(tmp_path: Path) -> None:
    plan_path, batch_path = _plan_and_batch(tmp_path)
    output_dir = tmp_path / "resume_gate"

    assert (
        cli_main(
            [
                "--full-flow-plan-json",
                str(plan_path),
                "--batch-initialization-closeout-json",
                str(batch_path),
                "--output-dir",
                str(output_dir),
                "--fail-on-blocked",
            ]
        )
        == 0
    )
    exported = json.loads((output_dir / "v1_5_post_closeout_resume_gate.json").read_text(encoding="utf-8"))
    assert exported["resume_gate_ready"] is True
    assert (output_dir / "v1_5_post_closeout_resume_steps.csv").exists()

    batch = _ready_batch_payload()
    batch["ready_for_mature_open_flow_from_initialization_index"] = False
    _write_json(batch_path, batch)
    assert (
        cli_main(
            [
                "--full-flow-plan-json",
                str(plan_path),
                "--batch-initialization-closeout-json",
                str(batch_path),
                "--output-dir",
                str(tmp_path / "blocked_gate"),
                "--fail-on-blocked",
            ]
        )
        == 2
    )

    entry = classify_v1_5_entrypoint(
        ROOT / "src/gas_calibrator/tools/export_v1_5_post_closeout_resume_gate.py",
        root=ROOT,
    )
    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
    assert any("post-closeout resume gate" in note for note in entry.notes)
