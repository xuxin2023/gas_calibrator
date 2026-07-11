import json
from pathlib import Path

from gas_calibrator.tools.run_v1_5_authoritative_resume_state_writer_blocked_executor import (
    main as blocked_main,
)
from gas_calibrator.v1_5.orchestration.full_flow import build_full_flow_plan, write_full_flow_plan
from gas_calibrator.validation.v1_5_authoritative_resume_state_writer_blocked_executor import (
    BLOCKED_STATUS,
    REVIEW_STATUS,
    SCHEMA,
    build_v1_5_authoritative_resume_state_writer_blocked_executor,
    write_v1_5_authoritative_resume_state_writer_blocked_executor_outputs,
)
from gas_calibrator.validation.v1_5_authoritative_resume_state_writer_design import (
    write_v1_5_authoritative_resume_state_writer_design,
)
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_post_closeout_resume_gate import (
    write_v1_5_post_closeout_resume_gate,
)
from gas_calibrator.validation.v1_5_resume_prefix_application_review import (
    write_v1_5_resume_prefix_application_review,
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


def _ready_sources(tmp_path: Path) -> tuple[Path, Path, Path]:
    config = _write_json(tmp_path / "config.json", {})
    output_dir = tmp_path / "flow"
    plan = build_full_flow_plan(config_path=config, output_dir=output_dir, run_id="blocked-stub-demo")
    plan_path = write_full_flow_plan(plan, output_dir)["json"]
    batch_path = _write_json(
        output_dir
        / "batch_initialization_closeout_index"
        / "v1_5_batch_initialization_closeout_index.json",
        _ready_batch_payload(),
    )
    gate_path = write_v1_5_post_closeout_resume_gate(
        output_dir=output_dir / "post_closeout_resume_gate",
        full_flow_plan_json=plan_path,
        batch_initialization_closeout_json=batch_path,
    )["manifest"]
    application_path = write_v1_5_resume_prefix_application_review(
        output_dir=output_dir / "resume_prefix_application_review",
        full_flow_plan_json=plan_path,
        post_closeout_resume_gate_json=gate_path,
    )["manifest"]
    design_path = write_v1_5_authoritative_resume_state_writer_design(
        output_dir=output_dir / "authoritative_resume_state_writer_design",
        full_flow_plan_json=plan_path,
        resume_prefix_application_review_json=application_path,
    )["manifest"]
    return plan_path, application_path, design_path


def test_blocked_executor_recomputes_design_and_keeps_state_unwritten(tmp_path: Path) -> None:
    plan_path, application_path, design_path = _ready_sources(tmp_path)

    model = build_v1_5_authoritative_resume_state_writer_blocked_executor(
        full_flow_plan_json=plan_path,
        resume_prefix_application_review_json=application_path,
        authoritative_resume_state_writer_design_json=design_path,
    )

    assert model["schema"] == SCHEMA
    assert model["overall_status"] == BLOCKED_STATUS
    assert model["blocked_executor_ready"] is True
    assert model["review_required_count"] == 0
    assert model["execution_supported"] is False
    assert model["authoritative_state_write_allowed"] is False
    assert model["write_state_flag_allowed"] is False
    assert model["state_target_argument_allowed"] is False
    assert model["expected_state_sha_argument_allowed"] is False
    assert model["authorization_inputs_allowed"] is False
    assert model["writes_authoritative_state"] is False
    assert model["state_file_created"] is False
    assert model["state_file_replaced"] is False
    assert model["state_snapshot_created"] is False
    assert model["rollback_executed"] is False
    assert model["opens_com_ports"] is False
    assert model["controls_water_or_gas_routes"] is False
    assert model["writes_coefficients"] is False
    assert model["connects_postgresql"] is False
    assert model["next_step_id_after_blocked_executor_review"] == "temperature_channel_fast_review"
    assert not Path(model["proposed_authoritative_state_json_recorded_only"]).exists()
    checks = {row["check"]: row for row in model["checks"]}
    assert checks["blocked_executor_bound_to_canonical_plan"]["status"] == "ready"
    assert checks["writer_design_independently_recomputed"]["status"] == "ready"


def test_blocked_executor_rejects_forged_design(tmp_path: Path) -> None:
    plan_path, application_path, design_path = _ready_sources(tmp_path)
    design = json.loads(design_path.read_text(encoding="utf-8"))
    design["proposed_completed_step_ids"].insert(-1, "database_import")
    design["proposed_completed_step_cli_arguments"].extend(
        ["--completed-step", "database_import"]
    )
    _write_json(design_path, design)

    model = build_v1_5_authoritative_resume_state_writer_blocked_executor(
        full_flow_plan_json=plan_path,
        resume_prefix_application_review_json=application_path,
        authoritative_resume_state_writer_design_json=design_path,
    )

    assert model["overall_status"] == REVIEW_STATUS
    assert model["blocked_executor_ready"] is False
    checks = {row["check"]: row for row in model["checks"]}
    assert "design_independent_recompute_mismatch" in checks[
        "writer_design_independently_recomputed"
    ]["reasons"]


def test_blocked_executor_rejects_same_hash_design_copy_not_declared_by_plan(tmp_path: Path) -> None:
    plan_path, application_path, design_path = _ready_sources(tmp_path)
    alternate_design = _write_json(
        tmp_path / "alternate" / "design.json",
        json.loads(design_path.read_text(encoding="utf-8")),
    )

    model = build_v1_5_authoritative_resume_state_writer_blocked_executor(
        full_flow_plan_json=plan_path,
        resume_prefix_application_review_json=application_path,
        authoritative_resume_state_writer_design_json=alternate_design,
    )

    assert model["overall_status"] == REVIEW_STATUS
    assert model["blocked_executor_ready"] is False
    checks = {row["check"]: row for row in model["checks"]}
    assert (
        "authoritative_state_blocked_executor_path_mismatch:--authoritative-resume-state-writer-design-json"
        in checks["blocked_executor_bound_to_canonical_plan"]["reasons"]
    )


def test_blocked_executor_cli_writes_lock_evidence_but_no_state(tmp_path: Path, capsys) -> None:
    plan_path, application_path, design_path = _ready_sources(tmp_path)
    output_dir = tmp_path / "blocked"

    rc = blocked_main(
        [
            "--full-flow-plan-json",
            str(plan_path),
            "--resume-prefix-application-review-json",
            str(application_path),
            "--authoritative-resume-state-writer-design-json",
            str(design_path),
            "--output-dir",
            str(output_dir),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert payload["overall_status"] == BLOCKED_STATUS
    assert payload["blocked_executor_ready"] is True
    assert payload["execution_supported"] is False
    assert payload["authoritative_state_write_allowed"] is False
    assert payload["writes_authoritative_state"] is False
    assert payload["state_file_created"] is False
    assert payload["state_file_replaced"] is False
    assert (output_dir / "v1_5_authoritative_resume_state_writer_blocked_executor.json").exists()
    assert not (plan_path.parent / "v1_5_full_flow_state.json").exists()


def test_blocked_executor_cli_rejects_write_target_and_authorization_before_artifact(
    tmp_path: Path,
    capsys,
) -> None:
    plan_path, application_path, design_path = _ready_sources(tmp_path)
    base = [
        "--full-flow-plan-json",
        str(plan_path),
        "--resume-prefix-application-review-json",
        str(application_path),
        "--authoritative-resume-state-writer-design-json",
        str(design_path),
    ]
    attempts = (
        ["--execute"],
        ["--write-state"],
        ["--replace-state"],
        ["--authoritative-state-json", str(tmp_path / "state.json")],
        ["--expected-existing-state-sha256", "a" * 64],
        ["--authorization-id", "AUTH-1"],
        ["--reviewer", "reviewer"],
        ["--approver", "approver"],
        ["--allow-route-control"],
        ["--allow-database-import"],
    )
    for index, extra in enumerate(attempts):
        output_dir = tmp_path / f"blocked_{index}"
        rc = blocked_main([*base, "--output-dir", str(output_dir), *extra])
        assert rc == 2
        assert not (
            output_dir / "v1_5_authoritative_resume_state_writer_blocked_executor.json"
        ).exists()
    assert "authoritative resume-state writing is locked" in capsys.readouterr().err


def test_blocked_executor_output_and_entrypoint_are_offline(tmp_path: Path) -> None:
    plan_path, application_path, design_path = _ready_sources(tmp_path)
    model = build_v1_5_authoritative_resume_state_writer_blocked_executor(
        full_flow_plan_json=plan_path,
        resume_prefix_application_review_json=application_path,
        authoritative_resume_state_writer_design_json=design_path,
    )
    outputs = write_v1_5_authoritative_resume_state_writer_blocked_executor_outputs(
        model, tmp_path / "outputs"
    )
    assert all(path.exists() for path in outputs.values())
    assert "no-state-write blocked executor stub" in outputs["markdown"].read_text(
        encoding="utf-8"
    )

    entry = classify_v1_5_entrypoint(
        ROOT
        / "src/gas_calibrator/tools/run_v1_5_authoritative_resume_state_writer_blocked_executor.py",
        root=ROOT,
    )
    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
    assert any("without creating or replacing state" in note for note in entry.notes)
