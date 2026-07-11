import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_authoritative_resume_state_writer_design import (
    main as cli_main,
)
from gas_calibrator.v1_5.orchestration.full_flow import build_full_flow_plan, write_full_flow_plan
from gas_calibrator.validation.v1_5_authoritative_resume_state_writer_design import (
    BLOCKED_STATUS,
    READY_STATUS,
    SCHEMA,
    build_v1_5_authoritative_resume_state_writer_design,
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
    plan = build_full_flow_plan(config_path=config, output_dir=output_dir, run_id="writer-design-demo")
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
    return plan_path, application_path, batch_path


def test_writer_design_binds_exact_prefix_and_keeps_state_locked(tmp_path: Path) -> None:
    plan_path, application_path, _batch_path = _ready_sources(tmp_path)

    model = build_v1_5_authoritative_resume_state_writer_design(
        full_flow_plan_json=plan_path,
        resume_prefix_application_review_json=application_path,
    )

    assert model["schema"] == SCHEMA
    assert model["overall_status"] == READY_STATUS
    assert model["authoritative_resume_state_writer_design_ready"] is True
    assert model["proposed_completed_step_ids"][-1] == (
        "authoritative_resume_state_writer_design"
    )
    assert model["proposed_current_step_id"] == (
        "authoritative_resume_state_writer_blocked_executor"
    )
    assert model["proposed_authorization_state"] == {
        "allow_real_com": False,
        "allow_pressure_control": False,
        "allow_route_control": False,
        "allow_writes": False,
        "allow_database_import": False,
    }
    assert model["transaction_contract"]["existing_state_sha256_compare_and_swap_required"] is True
    assert model["transaction_contract"]["atomic_replace_required"] is True
    assert model["transaction_contract"]["rollback_snapshot_required"] is True
    assert model["execution_supported"] is False
    assert model["authoritative_state_write_allowed"] is False
    assert model["writes_authoritative_state"] is False
    assert model["opens_com_ports"] is False
    assert model["controls_water_or_gas_routes"] is False
    assert model["connects_postgresql"] is False
    assert not Path(model["proposed_authoritative_state_json"]).exists()


def test_writer_design_blocks_tampered_application_prefix(tmp_path: Path) -> None:
    plan_path, application_path, _batch_path = _ready_sources(tmp_path)
    application = json.loads(application_path.read_text(encoding="utf-8"))
    application["reviewed_completed_step_ids_after_application"].append("rogue_step")
    _write_json(application_path, application)

    model = build_v1_5_authoritative_resume_state_writer_design(
        full_flow_plan_json=plan_path,
        resume_prefix_application_review_json=application_path,
    )

    assert model["overall_status"] == BLOCKED_STATUS
    assert model["proposed_completed_step_ids"] == []
    assert "resume_prefix_application_completed_steps_not_exact" in model["review_reasons"]


def test_writer_design_blocks_stale_or_alternate_application_source(tmp_path: Path) -> None:
    plan_path, application_path, batch_path = _ready_sources(tmp_path)
    alternate = _write_json(
        tmp_path / "alternate" / "application.json",
        json.loads(application_path.read_text(encoding="utf-8")),
    )
    alternate_model = build_v1_5_authoritative_resume_state_writer_design(
        full_flow_plan_json=plan_path,
        resume_prefix_application_review_json=alternate,
    )
    assert alternate_model["overall_status"] == BLOCKED_STATUS
    assert "authoritative_writer_design_application_path_mismatch" in alternate_model["review_reasons"]

    batch = json.loads(batch_path.read_text(encoding="utf-8"))
    batch["device_ready_count"] = 5
    _write_json(batch_path, batch)
    stale_model = build_v1_5_authoritative_resume_state_writer_design(
        full_flow_plan_json=plan_path,
        resume_prefix_application_review_json=application_path,
    )
    assert stale_model["overall_status"] == BLOCKED_STATUS
    assert "batch_closeout_artifact_sha256_mismatch" in stale_model["review_reasons"]


def test_writer_design_recomputes_application_source_chain_and_rejects_alternate_gate(
    tmp_path: Path,
) -> None:
    plan_path, application_path, _batch_path = _ready_sources(tmp_path)
    application = json.loads(application_path.read_text(encoding="utf-8"))
    canonical_gate = Path(application["post_closeout_resume_gate_json"])
    alternate_gate = _write_json(
        tmp_path / "alternate" / "resume_gate.json",
        json.loads(canonical_gate.read_text(encoding="utf-8")),
    )
    application["post_closeout_resume_gate_json"] = str(alternate_gate.resolve())
    application["post_closeout_resume_gate_sha256"] = application[
        "post_closeout_resume_gate_sha256"
    ]
    _write_json(application_path, application)

    model = build_v1_5_authoritative_resume_state_writer_design(
        full_flow_plan_json=plan_path,
        resume_prefix_application_review_json=application_path,
    )

    assert model["overall_status"] == BLOCKED_STATUS
    assert model["authoritative_resume_state_writer_design_ready"] is False
    assert model["proposed_completed_step_ids"] == []
    assert "resume_prefix_application_source_chain_recompute_mismatch" in model[
        "review_reasons"
    ]


def test_writer_design_rejects_execution_or_state_write_flags(tmp_path: Path) -> None:
    plan_path, application_path, _batch_path = _ready_sources(tmp_path)
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    writer = next(
        row for row in plan["steps"] if row["step_id"] == "authoritative_resume_state_writer_design"
    )
    writer["command"].extend(["--write-state", "--allow-route-control"])
    _write_json(plan_path, plan)

    model = build_v1_5_authoritative_resume_state_writer_design(
        full_flow_plan_json=plan_path,
        resume_prefix_application_review_json=application_path,
    )

    assert model["overall_status"] == BLOCKED_STATUS
    assert "authoritative_writer_design_forbidden_flag:--write-state" in model["review_reasons"]
    assert "authoritative_writer_design_forbidden_flag:--allow-route-control" in model[
        "review_reasons"
    ]


def test_writer_design_cli_is_offline_and_fails_closed(tmp_path: Path) -> None:
    plan_path, application_path, _batch_path = _ready_sources(tmp_path)
    output_dir = tmp_path / "writer_design"

    assert (
        cli_main(
            [
                "--full-flow-plan-json",
                str(plan_path),
                "--resume-prefix-application-review-json",
                str(application_path),
                "--output-dir",
                str(output_dir),
                "--fail-on-blocked",
            ]
        )
        == 0
    )
    manifest = json.loads(
        (output_dir / "v1_5_authoritative_resume_state_writer_design.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest["authoritative_resume_state_writer_design_ready"] is True
    assert (output_dir / "v1_5_authoritative_resume_state_transaction_contract.csv").exists()
    assert not (plan_path.parent / "v1_5_full_flow_state.json").exists()

    entry = classify_v1_5_entrypoint(
        ROOT
        / "src/gas_calibrator/tools/export_v1_5_authoritative_resume_state_writer_design.py",
        root=ROOT,
    )
    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
    assert any("without writing state" in note for note in entry.notes)
