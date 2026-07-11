import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_resume_prefix_application_review import main as cli_main
from gas_calibrator.v1_5.orchestration.full_flow import build_full_flow_plan, write_full_flow_plan
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_post_closeout_resume_gate import (
    write_v1_5_post_closeout_resume_gate,
)
from gas_calibrator.validation.v1_5_resume_prefix_application_review import (
    BLOCKED_STATUS,
    READY_STATUS,
    SCHEMA,
    build_v1_5_resume_prefix_application_review,
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


def _plan_gate_and_batch(tmp_path: Path) -> tuple[Path, Path, Path]:
    config = _write_json(tmp_path / "config.json", {})
    output_dir = tmp_path / "flow"
    plan = build_full_flow_plan(config_path=config, output_dir=output_dir, run_id="resume-application-demo")
    plan_path = write_full_flow_plan(plan, output_dir)["json"]
    batch_path = _write_json(
        output_dir / "batch_initialization_closeout_index" / "v1_5_batch_initialization_closeout_index.json",
        _ready_batch_payload(),
    )
    gate_paths = write_v1_5_post_closeout_resume_gate(
        output_dir=output_dir / "post_closeout_resume_gate",
        full_flow_plan_json=plan_path,
        batch_initialization_closeout_json=batch_path,
    )
    return plan_path, gate_paths["manifest"], batch_path


def test_resume_prefix_application_review_consumes_exact_prefix_without_applying_state(tmp_path: Path) -> None:
    plan_path, gate_path, _batch_path = _plan_gate_and_batch(tmp_path)

    model = build_v1_5_resume_prefix_application_review(
        full_flow_plan_json=plan_path,
        post_closeout_resume_gate_json=gate_path,
    )

    assert model["schema"] == SCHEMA
    assert model["overall_status"] == READY_STATUS
    assert model["resume_prefix_application_review_ready"] is True
    assert model["resume_prefix_consumed_for_review"] is True
    assert model["reviewed_resume_completed_step_ids"][-1] == "post_closeout_resume_gate_snapshot"
    assert model["reviewed_completed_step_ids_after_application"][-1] == (
        "post_closeout_resume_prefix_application_review"
    )
    assert model["reviewed_state_application_cli_arguments"][-2:] == [
        "--completed-step",
        "post_closeout_resume_prefix_application_review",
    ]
    assert model["state_preview_current_step_id"] == "authoritative_resume_state_writer_design"
    assert model["route_authorization_still_required"] is True
    assert model["does_not_execute_commands"] is True
    assert model["applies_completed_steps"] is False
    assert model["writes_authoritative_state"] is False
    assert model["would_execute"] is False
    assert model["live_resume_execution_allowed"] is False
    assert model["opens_com_ports"] is False
    assert model["controls_water_or_gas_routes"] is False
    assert model["writes_coefficients"] is False
    assert model["connects_postgresql"] is False


def test_resume_prefix_application_review_blocks_noncontiguous_or_extra_prefix(tmp_path: Path) -> None:
    plan_path, gate_path, _batch_path = _plan_gate_and_batch(tmp_path)
    gate = json.loads(gate_path.read_text(encoding="utf-8"))
    gate["resume_completed_step_ids"] = [
        "batch_initialization_closeout_index",
        "post_closeout_resume_gate_snapshot",
    ]
    gate["resume_cli_arguments"] = [
        "--completed-step",
        "batch_initialization_closeout_index",
        "--completed-step",
        "post_closeout_resume_gate_snapshot",
    ]
    _write_json(gate_path, gate)

    model = build_v1_5_resume_prefix_application_review(
        full_flow_plan_json=plan_path,
        post_closeout_resume_gate_json=gate_path,
    )

    assert model["overall_status"] == BLOCKED_STATUS
    assert model["resume_prefix_application_review_ready"] is False
    assert model["reviewed_completed_step_ids_after_application"] == []
    assert "resume_completed_step_prefix_not_exact_or_contiguous" in model["review_reasons"]


def test_resume_prefix_application_review_blocks_stale_plan_or_batch_hash(tmp_path: Path) -> None:
    plan_path, gate_path, batch_path = _plan_gate_and_batch(tmp_path)
    batch = json.loads(batch_path.read_text(encoding="utf-8"))
    batch["device_ready_count"] = 5
    _write_json(batch_path, batch)

    model = build_v1_5_resume_prefix_application_review(
        full_flow_plan_json=plan_path,
        post_closeout_resume_gate_json=gate_path,
    )

    assert model["overall_status"] == BLOCKED_STATUS
    assert "resume_gate_batch_closeout_sha256_mismatch" in model["review_reasons"]


def test_resume_prefix_application_review_blocks_gate_path_not_declared_by_plan(tmp_path: Path) -> None:
    plan_path, gate_path, _batch_path = _plan_gate_and_batch(tmp_path)
    alternate_gate = _write_json(
        tmp_path / "alternate" / "gate.json",
        json.loads(gate_path.read_text(encoding="utf-8")),
    )

    model = build_v1_5_resume_prefix_application_review(
        full_flow_plan_json=plan_path,
        post_closeout_resume_gate_json=alternate_gate,
    )

    assert model["overall_status"] == BLOCKED_STATUS
    assert "application_review_gate_path_mismatch_with_full_flow_plan" in model["review_reasons"]


def test_resume_prefix_application_review_rejects_state_or_execution_flags(tmp_path: Path) -> None:
    plan_path, gate_path, _batch_path = _plan_gate_and_batch(tmp_path)
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    step = next(
        row
        for row in plan["steps"]
        if row["step_id"] == "post_closeout_resume_prefix_application_review"
    )
    step["command"].extend(["--completed-step", "unreviewed_stage"])
    _write_json(plan_path, plan)

    model = build_v1_5_resume_prefix_application_review(
        full_flow_plan_json=plan_path,
        post_closeout_resume_gate_json=gate_path,
    )

    assert model["overall_status"] == BLOCKED_STATUS
    assert (
        "resume_prefix_application_review_forbidden_flag:--completed-step"
        in model["review_reasons"]
    )


def test_resume_prefix_application_review_blocks_unreviewed_step_inserted_before_application(
    tmp_path: Path,
) -> None:
    plan_path, gate_path, batch_path = _plan_gate_and_batch(tmp_path)
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    application_index = next(
        index
        for index, row in enumerate(plan["steps"])
        if row["step_id"] == "post_closeout_resume_prefix_application_review"
    )
    plan["steps"].insert(
        application_index,
        {
            "step_id": "unreviewed_inserted_step",
            "tool_module": "gas_calibrator.tools.export_v1_5_formal_run_status",
            "command": [],
            "execution_mode": "offline_sidecar",
        },
    )
    _write_json(plan_path, plan)
    gate_path = write_v1_5_post_closeout_resume_gate(
        output_dir=gate_path.parent,
        full_flow_plan_json=plan_path,
        batch_initialization_closeout_json=batch_path,
    )["manifest"]

    model = build_v1_5_resume_prefix_application_review(
        full_flow_plan_json=plan_path,
        post_closeout_resume_gate_json=gate_path,
    )

    assert model["overall_status"] == BLOCKED_STATUS
    assert "resume_prefix_application_steps_not_adjacent" in model["review_reasons"]
    assert model["reviewed_completed_step_ids_after_application"] == []


def test_resume_prefix_application_review_cli_is_offline_and_fails_closed(tmp_path: Path) -> None:
    plan_path, gate_path, _batch_path = _plan_gate_and_batch(tmp_path)
    output_dir = tmp_path / "application_review"

    assert (
        cli_main(
            [
                "--full-flow-plan-json",
                str(plan_path),
                "--post-closeout-resume-gate-json",
                str(gate_path),
                "--output-dir",
                str(output_dir),
                "--fail-on-blocked",
            ]
        )
        == 0
    )
    exported = json.loads(
        (output_dir / "v1_5_resume_prefix_application_review.json").read_text(encoding="utf-8")
    )
    assert exported["resume_prefix_application_review_ready"] is True
    assert (output_dir / "v1_5_resume_prefix_state_preview.csv").exists()

    gate = json.loads(gate_path.read_text(encoding="utf-8"))
    gate["applies_completed_steps"] = True
    _write_json(gate_path, gate)
    assert (
        cli_main(
            [
                "--full-flow-plan-json",
                str(plan_path),
                "--post-closeout-resume-gate-json",
                str(gate_path),
                "--output-dir",
                str(tmp_path / "blocked_review"),
                "--fail-on-blocked",
            ]
        )
        == 2
    )

    entry = classify_v1_5_entrypoint(
        ROOT / "src/gas_calibrator/tools/export_v1_5_resume_prefix_application_review.py",
        root=ROOT,
    )
    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
    assert any("without writing state" in note for note in entry.notes)
