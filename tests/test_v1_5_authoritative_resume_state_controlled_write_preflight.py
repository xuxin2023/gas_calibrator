import hashlib
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_authoritative_resume_state_controlled_write_preflight import (
    main as preflight_main,
)
from gas_calibrator.v1_5.orchestration.full_flow import build_full_flow_plan, write_full_flow_plan
from gas_calibrator.validation.v1_5_authoritative_resume_state_controlled_write_preflight import (
    AUTHORIZATION_OPERATION,
    AUTHORIZATION_SCHEMA,
    BLOCKED_STATUS,
    CONFIRMATION_TEMPLATE,
    READY_STATUS,
    REVIEW_STATUS,
    SCHEMA,
    build_v1_5_authoritative_resume_state_controlled_write_preflight,
    write_v1_5_authoritative_resume_state_controlled_write_preflight_outputs,
)
from gas_calibrator.validation.v1_5_authoritative_resume_state_writer_blocked_executor import (
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


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


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


def _ready_sources(tmp_path: Path) -> tuple[Path, Path, Path, Path, Path]:
    config = _write_json(tmp_path / "config.json", {})
    output_dir = tmp_path / "flow"
    plan = build_full_flow_plan(config_path=config, output_dir=output_dir, run_id="preflight-demo")
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
    blocked_model = build_v1_5_authoritative_resume_state_writer_blocked_executor(
        full_flow_plan_json=plan_path,
        resume_prefix_application_review_json=application_path,
        authoritative_resume_state_writer_design_json=design_path,
    )
    blocked_path = write_v1_5_authoritative_resume_state_writer_blocked_executor_outputs(
        blocked_model,
        output_dir / "authoritative_resume_state_writer_blocked_executor",
    )["json"]
    authorization_path = (
        output_dir
        / "authoritative_resume_state_write_authorization"
        / "v1_5_authoritative_resume_state_write_authorization.json"
    )
    return plan_path, application_path, design_path, blocked_path, authorization_path


def _authorization_payload(
    *,
    plan_path: Path,
    application_path: Path,
    design_path: Path,
    blocked_path: Path,
    expected_existing_state_sha256: str = "absent",
    expected_candidate_state_sha256: str = "",
) -> dict:
    return {
        "schema": AUTHORIZATION_SCHEMA,
        "requested_operation": AUTHORIZATION_OPERATION,
        "confirmation_template": CONFIRMATION_TEMPLATE,
        "authorization_id": "resume-state-preflight-001",
        "authorized_at": "2026-07-11T12:00:00Z",
        "operator": "operator-a",
        "reviewer": "reviewer-b",
        "approver": "approver-c",
        "preflight_only": True,
        "full_flow_plan_json": str(plan_path.resolve()),
        "full_flow_plan_sha256": _sha256(plan_path),
        "resume_prefix_application_review_json": str(application_path.resolve()),
        "resume_prefix_application_review_sha256": _sha256(application_path),
        "authoritative_resume_state_writer_design_json": str(design_path.resolve()),
        "authoritative_resume_state_writer_design_sha256": _sha256(design_path),
        "authoritative_resume_state_writer_blocked_executor_json": str(
            blocked_path.resolve()
        ),
        "authoritative_resume_state_writer_blocked_executor_sha256": _sha256(
            blocked_path
        ),
        "authoritative_state_json": str(
            (plan_path.parent / "v1_5_full_flow_state.json").resolve()
        ),
        "expected_existing_state_sha256": expected_existing_state_sha256,
        "expected_candidate_state_sha256": expected_candidate_state_sha256,
        "authoritative_state_write_allowed": False,
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "connects_postgresql": False,
        "database_import_allowed": False,
        "formal_release_allowed": False,
    }


def _ready_model(tmp_path: Path) -> tuple[dict, tuple[Path, Path, Path, Path, Path]]:
    sources = _ready_sources(tmp_path)
    plan_path, application_path, design_path, blocked_path, authorization_path = sources
    packet = _authorization_payload(
        plan_path=plan_path,
        application_path=application_path,
        design_path=design_path,
        blocked_path=blocked_path,
    )
    _write_json(authorization_path, packet)
    first = build_v1_5_authoritative_resume_state_controlled_write_preflight(
        full_flow_plan_json=plan_path,
        resume_prefix_application_review_json=application_path,
        authoritative_resume_state_writer_design_json=design_path,
        authoritative_resume_state_writer_blocked_executor_json=blocked_path,
        authorization_packet_json=authorization_path,
    )
    assert first["overall_status"] == REVIEW_STATUS
    packet["expected_candidate_state_sha256"] = first["candidate_state_sha256"]
    _write_json(authorization_path, packet)
    model = build_v1_5_authoritative_resume_state_controlled_write_preflight(
        full_flow_plan_json=plan_path,
        resume_prefix_application_review_json=application_path,
        authoritative_resume_state_writer_design_json=design_path,
        authoritative_resume_state_writer_blocked_executor_json=blocked_path,
        authorization_packet_json=authorization_path,
    )
    return model, sources


def test_controlled_write_preflight_ready_without_writing_state(tmp_path: Path) -> None:
    model, sources = _ready_model(tmp_path)
    plan_path = sources[0]

    assert model["schema"] == SCHEMA
    assert model["overall_status"] == READY_STATUS
    assert model["blocker_count"] == 0
    assert model["review_required_count"] == 0
    assert model["controlled_write_preflight_ready"] is True
    assert model["candidate_state"]["current_step_id"] == "temperature_channel_fast_review"
    assert model["candidate_state"]["completed_step_ids"][-1] == (
        "authoritative_resume_state_controlled_write_preflight"
    )
    assert model["observed_existing_state_sha256"] == "absent"
    assert model["execution_supported"] is False
    assert model["authoritative_state_write_allowed"] is False
    assert model["writes_authoritative_state"] is False
    assert model["state_file_created"] is False
    assert model["state_file_replaced"] is False
    assert model["opens_com_ports"] is False
    assert model["connects_postgresql"] is False
    assert not (plan_path.parent / "v1_5_full_flow_state.json").exists()


def test_controlled_write_preflight_first_pass_exposes_candidate_hash_only(
    tmp_path: Path,
) -> None:
    plan_path, application_path, design_path, blocked_path, authorization_path = (
        _ready_sources(tmp_path)
    )
    _write_json(
        authorization_path,
        _authorization_payload(
            plan_path=plan_path,
            application_path=application_path,
            design_path=design_path,
            blocked_path=blocked_path,
        ),
    )

    model = build_v1_5_authoritative_resume_state_controlled_write_preflight(
        full_flow_plan_json=plan_path,
        resume_prefix_application_review_json=application_path,
        authoritative_resume_state_writer_design_json=design_path,
        authoritative_resume_state_writer_blocked_executor_json=blocked_path,
        authorization_packet_json=authorization_path,
    )
    outputs = write_v1_5_authoritative_resume_state_controlled_write_preflight_outputs(
        model, tmp_path / "preflight"
    )

    assert model["overall_status"] == REVIEW_STATUS
    assert model["review_required_count"] == 1
    assert len(model["candidate_state_sha256"]) == 64
    assert outputs["candidate_preview"].exists()
    payload = json.loads(outputs["json"].read_text(encoding="utf-8"))
    assert payload["candidate_state_preview_sha256"] == model["candidate_state_sha256"]
    assert not (plan_path.parent / "v1_5_full_flow_state.json").exists()


def test_controlled_write_preflight_blocks_changed_existing_state(tmp_path: Path) -> None:
    model, sources = _ready_model(tmp_path)
    plan_path, application_path, design_path, blocked_path, authorization_path = sources
    target = plan_path.parent / "v1_5_full_flow_state.json"
    target.write_text(json.dumps(model["candidate_state"], indent=2), encoding="utf-8")

    stale = build_v1_5_authoritative_resume_state_controlled_write_preflight(
        full_flow_plan_json=plan_path,
        resume_prefix_application_review_json=application_path,
        authoritative_resume_state_writer_design_json=design_path,
        authoritative_resume_state_writer_blocked_executor_json=blocked_path,
        authorization_packet_json=authorization_path,
    )

    assert stale["overall_status"] == BLOCKED_STATUS
    assert stale["controlled_write_preflight_ready"] is False
    checks = {row["check"]: row for row in stale["checks"]}
    assert "expected_existing_state_sha256_mismatch" in checks[
        "manual_authorization_packet_binds_sources_target_and_candidate"
    ]["reasons"]


def test_controlled_write_preflight_accepts_matching_existing_state_read_only(
    tmp_path: Path,
) -> None:
    model, sources = _ready_model(tmp_path)
    plan_path, application_path, design_path, blocked_path, authorization_path = sources
    target = plan_path.parent / "v1_5_full_flow_state.json"
    target.write_bytes((json.dumps(model["candidate_state"], indent=2) + "\n").encode("utf-8"))
    before = target.read_bytes()
    packet = json.loads(authorization_path.read_text(encoding="utf-8"))
    packet["expected_existing_state_sha256"] = hashlib.sha256(before).hexdigest()
    _write_json(authorization_path, packet)

    current = build_v1_5_authoritative_resume_state_controlled_write_preflight(
        full_flow_plan_json=plan_path,
        resume_prefix_application_review_json=application_path,
        authoritative_resume_state_writer_design_json=design_path,
        authoritative_resume_state_writer_blocked_executor_json=blocked_path,
        authorization_packet_json=authorization_path,
    )

    assert current["overall_status"] == READY_STATUS
    assert current["state_target_exists"] is True
    assert target.read_bytes() == before


def test_controlled_write_preflight_blocks_forged_lock_evidence(tmp_path: Path) -> None:
    model, sources = _ready_model(tmp_path)
    plan_path, application_path, design_path, blocked_path, authorization_path = sources
    forged = json.loads(blocked_path.read_text(encoding="utf-8"))
    forged["state_file_created"] = True
    _write_json(blocked_path, forged)

    result = build_v1_5_authoritative_resume_state_controlled_write_preflight(
        full_flow_plan_json=plan_path,
        resume_prefix_application_review_json=application_path,
        authoritative_resume_state_writer_design_json=design_path,
        authoritative_resume_state_writer_blocked_executor_json=blocked_path,
        authorization_packet_json=authorization_path,
    )

    assert result["overall_status"] == BLOCKED_STATUS
    assert result["controlled_write_preflight_ready"] is False
    checks = {row["check"]: row for row in result["checks"]}
    assert "blocked_executor_independent_recompute_mismatch" in checks[
        "blocked_executor_lock_evidence_independently_recomputed"
    ]["reasons"]


def test_controlled_write_preflight_blocks_non_distinct_authorizers(tmp_path: Path) -> None:
    model, sources = _ready_model(tmp_path)
    plan_path, application_path, design_path, blocked_path, authorization_path = sources
    packet = json.loads(authorization_path.read_text(encoding="utf-8"))
    packet["approver"] = packet["reviewer"]
    _write_json(authorization_path, packet)

    result = build_v1_5_authoritative_resume_state_controlled_write_preflight(
        full_flow_plan_json=plan_path,
        resume_prefix_application_review_json=application_path,
        authoritative_resume_state_writer_design_json=design_path,
        authoritative_resume_state_writer_blocked_executor_json=blocked_path,
        authorization_packet_json=authorization_path,
    )

    assert model["controlled_write_preflight_ready"] is True
    assert result["overall_status"] == BLOCKED_STATUS
    checks = {row["check"]: row for row in result["checks"]}
    assert "operator_reviewer_approver_must_be_distinct" in checks[
        "manual_authorization_packet_binds_sources_target_and_candidate"
    ]["reasons"]


def test_controlled_write_preflight_blocks_malformed_existing_state(tmp_path: Path) -> None:
    model, sources = _ready_model(tmp_path)
    plan_path, application_path, design_path, blocked_path, authorization_path = sources
    target = plan_path.parent / "v1_5_full_flow_state.json"
    target.write_text("not-json", encoding="utf-8")
    packet = json.loads(authorization_path.read_text(encoding="utf-8"))
    packet["expected_existing_state_sha256"] = _sha256(target)
    _write_json(authorization_path, packet)

    result = build_v1_5_authoritative_resume_state_controlled_write_preflight(
        full_flow_plan_json=plan_path,
        resume_prefix_application_review_json=application_path,
        authoritative_resume_state_writer_design_json=design_path,
        authoritative_resume_state_writer_blocked_executor_json=blocked_path,
        authorization_packet_json=authorization_path,
    )

    assert model["controlled_write_preflight_ready"] is True
    assert result["overall_status"] == BLOCKED_STATUS
    checks = {row["check"]: row for row in result["checks"]}
    assert "existing_state_schema_invalid" in checks[
        "authoritative_state_target_compare_and_swap_preflight"
    ]["reasons"]


def test_controlled_write_preflight_rejects_alternate_authorization_path(
    tmp_path: Path,
) -> None:
    model, sources = _ready_model(tmp_path)
    plan_path, application_path, design_path, blocked_path, authorization_path = sources
    alternate = _write_json(
        tmp_path / "alternate" / "authorization.json",
        json.loads(authorization_path.read_text(encoding="utf-8")),
    )

    result = build_v1_5_authoritative_resume_state_controlled_write_preflight(
        full_flow_plan_json=plan_path,
        resume_prefix_application_review_json=application_path,
        authoritative_resume_state_writer_design_json=design_path,
        authoritative_resume_state_writer_blocked_executor_json=blocked_path,
        authorization_packet_json=alternate,
    )

    assert result["overall_status"] == BLOCKED_STATUS
    checks = {row["check"]: row for row in result["checks"]}
    assert (
        "controlled_write_preflight_path_mismatch:--authorization-packet-json"
        in checks["controlled_write_preflight_bound_to_canonical_plan"]["reasons"]
    )


def test_controlled_write_preflight_cli_rejects_execution_before_artifact(
    tmp_path: Path,
    capsys,
) -> None:
    model, sources = _ready_model(tmp_path)
    plan_path, application_path, design_path, blocked_path, authorization_path = sources
    base = [
        "--full-flow-plan-json",
        str(plan_path),
        "--resume-prefix-application-review-json",
        str(application_path),
        "--authoritative-resume-state-writer-design-json",
        str(design_path),
        "--authoritative-resume-state-writer-blocked-executor-json",
        str(blocked_path),
        "--authorization-packet-json",
        str(authorization_path),
    ]
    for index, extra in enumerate(
        (["--execute"], ["--write-state"], ["--replace-state"], ["--allow-writes"])
    ):
        out = tmp_path / f"cli_{index}"
        assert preflight_main([*base, "--output-dir", str(out), *extra]) == 2
        assert not (
            out / "v1_5_resume_state_write_preflight.json"
        ).exists()
    assert "preflight is no-write" in capsys.readouterr().err
    assert model["controlled_write_preflight_ready"] is True


def test_controlled_write_preflight_entrypoint_is_offline() -> None:
    entry = classify_v1_5_entrypoint(
        ROOT
        / "src/gas_calibrator/tools/export_v1_5_authoritative_resume_state_controlled_write_preflight.py",
        root=ROOT,
    )
    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
    assert any("candidate state" in note for note in entry.notes)
