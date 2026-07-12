import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import gas_calibrator.validation.v1_5_formal_run_status as formal_status_module
from gas_calibrator.tools import (
    export_v1_5_authoritative_resume_offline_state_advance_consumer_readiness as consumer_tool,
)
from gas_calibrator.tools import (
    export_v1_5_authoritative_resume_offline_state_advance_post_write_verification as verify_tool,
)
from gas_calibrator.v1_5.orchestration.full_flow import (
    build_full_flow_plan,
    build_full_flow_state,
    write_full_flow_plan,
)
from gas_calibrator.validation import (
    v1_5_authoritative_resume_offline_state_advance_atomic_writer as writer_module,
)
from gas_calibrator.validation import (
    v1_5_authoritative_resume_offline_state_advance_post_write_verification as verification_module,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_atomic_writer import (
    CONFIRMATION_TEMPLATE,
    execute_v1_5_authoritative_resume_offline_state_advance_atomic_write,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_authorization import (
    AUTHORIZATION_OPERATION,
    AUTHORIZATION_SCHEMA,
    READY_STATUS as AUTHORIZATION_READY_STATUS,
    SCHEMA as AUTHORIZATION_VALIDATION_SCHEMA,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_blocked_executor import (
    AUTHORIZATION_COMPARE_KEYS,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_consumer_readiness import (
    BLOCKED_STATUS as CONSUMER_BLOCKED_STATUS,
    READY_STATUS as CONSUMER_READY_STATUS,
    build_v1_5_authoritative_resume_offline_state_advance_consumer_readiness,
    write_v1_5_authoritative_resume_offline_state_advance_consumer_readiness,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_next_step_plan import (
    READY_STATUS as NEXT_STEP_PLAN_READY_STATUS,
    build_v1_5_authoritative_resume_offline_state_advance_next_step_plan,
    write_v1_5_authoritative_resume_offline_state_advance_next_step_plan,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_post_write_verification import (
    BLOCKED_STATUS,
    READY_STATUS,
    build_v1_5_authoritative_resume_offline_state_advance_post_write_verification,
    write_v1_5_authoritative_resume_offline_state_advance_post_write_verification,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_preflight import (
    READY_STATUS as PREFLIGHT_READY_STATUS,
    SCHEMA as PREFLIGHT_SCHEMA,
)

AUTHORIZATION_ID = "state-advance-postverify-109"


def _write(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(
        (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    )
    return path


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _bundle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Path, Path, Path, Path, Path, list[str]]:
    root = tmp_path / "flow"
    config = _write(root / "config.json", {})
    plan = build_full_flow_plan(config_path=config, output_dir=root, run_id="run-109")
    plan_path = write_full_flow_plan(plan, root)["json"]
    step_ids = [step.step_id for step in plan.steps]
    original_state = build_full_flow_state(plan, completed_steps=step_ids[:2]).to_json()
    candidate_state = build_full_flow_state(plan, completed_steps=step_ids[:3]).to_json()
    state_path = _write(root / "v1_5_full_flow_state.json", original_state)
    candidate_path = _write(
        root
        / "state-advance"
        / "v1_5_authoritative_resume_offline_state_candidate.json",
        candidate_state,
    )
    preflight = {
        "schema": PREFLIGHT_SCHEMA,
        "overall_status": PREFLIGHT_READY_STATUS,
        "offline_state_advance_preflight_ready": True,
        "blocker_count": 0,
        "blocker_reasons": [],
        "run_id": "run-109",
        "attempt_id": "attempt-109",
        "verified_step_id": step_ids[2],
        "next_step_id_after_advance": step_ids[3],
        "full_flow_plan_json": str(plan_path.resolve()),
        "full_flow_plan_sha256": _sha(plan_path),
        "authoritative_state_json": str(state_path.resolve()),
        "expected_current_state_sha256": _sha(state_path),
        "candidate_state_preview_json": str(candidate_path.resolve()),
        "candidate_state_preview_sha256": _sha(candidate_path),
        "candidate_state_sha256": _sha(candidate_path),
        "compare_and_swap_required": True,
        "execution_supported": False,
        "would_execute": False,
        "authoritative_state_write_allowed": False,
        "writes_authoritative_state": False,
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "connects_postgresql": False,
        "database_written": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
    }
    preflight_path = _write(
        candidate_path.parent
        / "v1_5_authoritative_resume_offline_state_advance_preflight.json",
        preflight,
    )
    now = datetime.now(UTC).replace(microsecond=0)
    packet = {
        "schema": AUTHORIZATION_SCHEMA,
        "requested_operation": AUTHORIZATION_OPERATION,
        "confirmation_template": CONFIRMATION_TEMPLATE,
        "authorization_id": AUTHORIZATION_ID,
        "issued_at": _iso(now - timedelta(minutes=5)),
        "expires_at": _iso(now + timedelta(minutes=20)),
        "operator": "operator-a",
        "reviewer": "reviewer-b",
        "approver": "approver-c",
        "offline_state_advance_preflight_json": str(preflight_path.resolve()),
        "offline_state_advance_preflight_sha256": _sha(preflight_path),
        "authoritative_state_json": str(state_path.resolve()),
        "expected_current_state_sha256": _sha(state_path),
        "candidate_state_preview_json": str(candidate_path.resolve()),
        "candidate_state_sha256": _sha(candidate_path),
        "run_id": "run-109",
        "attempt_id": "attempt-109",
        "verified_step_id": step_ids[2],
        "next_step_id_after_advance": step_ids[3],
        "compare_and_swap_required": True,
        "structured_confirmation": {
            "exact_preflight_only": True,
            "one_verified_offline_step_only": True,
            "compare_and_swap_before_write": True,
            "atomic_replace_and_readback_required": True,
            "rollback_required": True,
            "no_com": True,
            "no_pressure_or_route": True,
            "no_device_or_coefficient_write": True,
            "no_postgresql_or_release": True,
        },
        "allow_authoritative_state_write": True,
        "allow_real_com": False,
        "allow_pressure_control": False,
        "allow_route_control": False,
        "allow_device_or_coefficient_write": False,
        "allow_postgresql_import": False,
    }
    packet_path = _write(root / "authorization-packet.json", packet)
    values = {key: None for key in AUTHORIZATION_COMPARE_KEYS}
    values.update(
        {
            "overall_status": AUTHORIZATION_READY_STATUS,
            "offline_state_advance_authorization_validated": True,
            "review_required_count": 0,
            "review_reasons": [],
            "offline_state_advance_preflight_json": str(preflight_path.resolve()),
            "offline_state_advance_preflight_sha256": _sha(preflight_path),
            "authorization_packet_json": str(packet_path.resolve()),
            "authorization_packet_sha256": _sha(packet_path),
            "authorization_id": AUTHORIZATION_ID,
            "authorization_expires_at": packet["expires_at"],
            "run_id": "run-109",
            "attempt_id": "attempt-109",
            "verified_step_id": step_ids[2],
            "next_step_id_after_advance": step_ids[3],
            "authoritative_state_json": str(state_path.resolve()),
            "expected_current_state_sha256": _sha(state_path),
            "candidate_state_preview_json": str(candidate_path.resolve()),
            "candidate_state_sha256": _sha(candidate_path),
            "compare_and_swap_required": True,
            "execution_supported": False,
            "state_write_execution_allowed": False,
            "would_execute": False,
            "writes_authoritative_state": False,
            "opens_com_ports": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "connects_postgresql": False,
            "database_written": False,
            "formal_release_allowed": False,
            "database_import_allowed": False,
            "not_real_acceptance_evidence": True,
            "checks": [{"check": "ready", "status": "ready", "reasons": []}],
        }
    )
    validation = {
        "schema": AUTHORIZATION_VALIDATION_SCHEMA,
        "generated_at": _iso(now),
        **values,
    }
    validation_path = _write(
        root
        / "authorization-review"
        / "v1_5_authoritative_resume_offline_state_advance_authorization.json",
        validation,
    )
    monkeypatch.setattr(
        writer_module,
        "build_v1_5_authoritative_resume_offline_state_advance_authorization",
        lambda **_kwargs: dict(validation),
    )
    result = execute_v1_5_authoritative_resume_offline_state_advance_atomic_write(
        state_advance_authorization_json=validation_path,
        authorization_id=AUTHORIZATION_ID,
        confirmation_template=CONFIRMATION_TEMPLATE,
        output_dir=root / "writer-output",
    )
    assert result["authoritative_state_write_committed"] is True
    writer_path = (
        root
        / "writer-output"
        / "v1_5_authoritative_resume_offline_state_advance_atomic_writer.json"
    )
    return writer_path, state_path, candidate_path, packet_path, plan_path, step_ids


def _rebind_forged_packet(writer_path: Path, packet_path: Path, packet: dict) -> None:
    _write(packet_path, packet)
    writer = json.loads(writer_path.read_text(encoding="utf-8"))
    validation_path = Path(writer["state_advance_authorization_json"])
    validation = json.loads(validation_path.read_text(encoding="utf-8"))
    validation["authorization_packet_sha256"] = _sha(packet_path)
    _write(validation_path, validation)
    writer["state_advance_authorization_sha256"] = _sha(validation_path)
    _write(writer_path, writer)
    invocation_path = Path(writer["invocation_json"])
    invocation = json.loads(invocation_path.read_text(encoding="utf-8"))
    invocation["state_advance_authorization_sha256"] = _sha(validation_path)
    _write(invocation_path, invocation)


def test_post_write_verification_and_consumer_gate_accept_exact_chain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer, state, candidate, _packet, _plan, steps = _bundle(tmp_path, monkeypatch)
    verification = build_v1_5_authoritative_resume_offline_state_advance_post_write_verification(
        atomic_write_json=writer
    )
    assert verification["overall_status"] == READY_STATUS
    assert verification["post_write_verification_ready"] is True
    assert verification["authoritative_state_sha256"] == _sha(state) == _sha(candidate)
    assert verification["writer_lock_released"] is True
    assert verification["state_consumption_allowed"] is False
    verification_path = write_v1_5_authoritative_resume_offline_state_advance_post_write_verification(
        verification, tmp_path / "post-write"
    )["json"]
    consumer = build_v1_5_authoritative_resume_offline_state_advance_consumer_readiness(
        post_write_verification_json=verification_path
    )
    assert consumer["overall_status"] == CONSUMER_READY_STATUS
    assert consumer["resume_state_consumer_readiness_ready"] is True
    assert consumer["state_consumption_allowed"] is True
    assert consumer["resume_execution_allowed"] is False
    assert consumer["verified_step_id"] == steps[2]
    assert consumer["next_step_id"] == steps[3]
    consumer_path = write_v1_5_authoritative_resume_offline_state_advance_consumer_readiness(
        consumer, tmp_path / "consumer"
    )
    preview = build_v1_5_authoritative_resume_offline_state_advance_next_step_plan(
        consumer_readiness_json=consumer_path
    )
    assert preview["overall_status"] == NEXT_STEP_PLAN_READY_STATUS
    assert preview["next_step_id"] == steps[3]
    assert preview["plan_consumption_allowed"] is True
    assert preview["next_step_execution_allowed"] is False
    assert preview["resume_execution_allowed"] is False


@pytest.mark.parametrize(
    ("artifact_index", "reason"),
    [
        (2, "candidate_preview_sha256_mismatch"),
        (3, "state_advance_authorization_packet_sha256_mismatch"),
    ],
)
def test_post_write_verification_rejects_candidate_or_packet_tamper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    artifact_index: int,
    reason: str,
) -> None:
    bundle = _bundle(tmp_path, monkeypatch)
    writer = bundle[0]
    artifact = bundle[artifact_index]
    artifact.write_text('{"tampered":true}\n', encoding="utf-8")
    model = build_v1_5_authoritative_resume_offline_state_advance_post_write_verification(
        atomic_write_json=writer
    )
    assert model["overall_status"] == BLOCKED_STATUS
    assert reason in model["blocker_reasons"]


def test_post_write_verification_rejects_snapshot_or_lock_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer, state, _candidate, _packet, _plan, _steps = _bundle(
        tmp_path, monkeypatch
    )
    payload = json.loads(writer.read_text(encoding="utf-8"))
    snapshot = Path(payload["rollback_snapshot_path"])
    snapshot.write_text("tampered-snapshot\n", encoding="utf-8")
    lock = state.with_name(f"{state.name}.lock")
    lock.write_text("still-locked", encoding="utf-8")
    model = build_v1_5_authoritative_resume_offline_state_advance_post_write_verification(
        atomic_write_json=writer
    )
    assert "rollback_snapshot_does_not_match_expected_old_state" in model[
        "blocker_reasons"
    ]
    assert "writer_lock_still_present" in model["blocker_reasons"]


def test_post_write_verification_rejects_writer_outside_authorization_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer, _state, _candidate, _packet, _plan, _steps = _bundle(
        tmp_path, monkeypatch
    )
    payload = json.loads(writer.read_text(encoding="utf-8"))
    payload["generated_at"] = "2000-01-01T00:00:00Z"
    _write(writer, payload)
    model = build_v1_5_authoritative_resume_offline_state_advance_post_write_verification(
        atomic_write_json=writer
    )
    assert "writer_timeline_outside_authorization_window" in model[
        "blocker_reasons"
    ]


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        ("long_ttl", "authorization_packet_ttl_out_of_range"),
        (
            "missing_confirmation",
            "authorization_packet_confirmation_missing:rollback_required",
        ),
        ("real_com", "authorization_packet_boundary_invalid:allow_real_com"),
    ],
)
def test_post_write_verification_rejects_semantically_forged_rebound_packet(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
    reason: str,
) -> None:
    writer, _state, _candidate, packet_path, _plan, _steps = _bundle(
        tmp_path, monkeypatch
    )
    packet = json.loads(packet_path.read_text(encoding="utf-8"))
    if mutation == "long_ttl":
        issued = datetime.fromisoformat(packet["issued_at"].replace("Z", "+00:00"))
        packet["expires_at"] = _iso(issued + timedelta(hours=2))
    elif mutation == "missing_confirmation":
        packet["structured_confirmation"]["rollback_required"] = False
    else:
        packet["allow_real_com"] = True
    _rebind_forged_packet(writer, packet_path, packet)
    model = build_v1_5_authoritative_resume_offline_state_advance_post_write_verification(
        atomic_write_json=writer
    )
    assert model["overall_status"] == BLOCKED_STATUS
    assert reason in model["blocker_reasons"]


def test_consumer_gate_recomputes_verification_and_blocks_state_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer, state, _candidate, _packet, _plan, _steps = _bundle(
        tmp_path, monkeypatch
    )
    verification = build_v1_5_authoritative_resume_offline_state_advance_post_write_verification(
        atomic_write_json=writer
    )
    verification_path = write_v1_5_authoritative_resume_offline_state_advance_post_write_verification(
        verification, tmp_path / "post-write"
    )["json"]
    state.write_text('{"tampered":true}\n', encoding="utf-8")
    model = build_v1_5_authoritative_resume_offline_state_advance_consumer_readiness(
        post_write_verification_json=verification_path
    )
    assert model["overall_status"] == CONSUMER_BLOCKED_STATUS
    assert model["state_consumption_allowed"] is False
    assert "post_write_verification_recompute_mismatch" in model["blocker_reasons"]
    assert "authoritative_state_sha256_mismatch" in model["blocker_reasons"]


def test_consumer_gate_rejects_reparse_point_in_plan_ancestor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer, _state, _candidate, _packet, plan, _steps = _bundle(
        tmp_path, monkeypatch
    )
    verification = build_v1_5_authoritative_resume_offline_state_advance_post_write_verification(
        atomic_write_json=writer
    )
    verification_path = write_v1_5_authoritative_resume_offline_state_advance_post_write_verification(
        verification, tmp_path / "post-write"
    )["json"]
    original = verification_module._has_reparse_point
    monkeypatch.setattr(
        verification_module,
        "_has_reparse_point",
        lambda path: path == plan.parent or original(path),
    )
    model = build_v1_5_authoritative_resume_offline_state_advance_consumer_readiness(
        post_write_verification_json=verification_path
    )
    assert model["overall_status"] == CONSUMER_BLOCKED_STATUS
    assert "full_flow_plan_path_contains_reparse_point" in model["blocker_reasons"]
    assert model["state_consumption_allowed"] is False


def test_post_write_and_consumer_clis_export_offline_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer, _state, _candidate, _packet, _plan, _steps = _bundle(
        tmp_path, monkeypatch
    )
    verify_out = tmp_path / "verify-cli"
    assert verify_tool.main(
        [
            "--atomic-write-json",
            str(writer),
            "--output-dir",
            str(verify_out),
            "--fail-on-blocker",
        ]
    ) == 0
    verification = (
        verify_out
        / "v1_5_authoritative_resume_offline_state_advance_post_write_verification.json"
    )
    consumer_out = tmp_path / "consumer-cli"
    assert consumer_tool.main(
        [
            "--post-write-verification-json",
            str(verification),
            "--output-dir",
            str(consumer_out),
            "--fail-on-blocker",
        ]
    ) == 0
    payload = json.loads(
        (
            consumer_out
            / "v1_5_authoritative_resume_offline_state_advance_consumer_readiness.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["state_consumption_allowed"] is True
    assert payload["resume_execution_allowed"] is False


def test_formal_status_accepts_real_post_write_and_consumer_chain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer, _state, _candidate, _packet, _plan, _steps = _bundle(
        tmp_path, monkeypatch
    )
    verification_model = build_v1_5_authoritative_resume_offline_state_advance_post_write_verification(
        atomic_write_json=writer
    )
    verification_path = write_v1_5_authoritative_resume_offline_state_advance_post_write_verification(
        verification_model,
        tmp_path / "formal-status-verification",
    )["json"]
    consumer_model = build_v1_5_authoritative_resume_offline_state_advance_consumer_readiness(
        post_write_verification_json=verification_path
    )
    consumer_path = write_v1_5_authoritative_resume_offline_state_advance_consumer_readiness(
        consumer_model,
        tmp_path / "formal-status-consumer",
    )
    next_step_model = build_v1_5_authoritative_resume_offline_state_advance_next_step_plan(
        consumer_readiness_json=consumer_path
    )
    next_step_path = write_v1_5_authoritative_resume_offline_state_advance_next_step_plan(
        next_step_model,
        tmp_path / "formal-status-next-step-plan",
    )

    post_gate = formal_status_module._authoritative_resume_offline_state_advance_post_write_verification_gate(
        verification_path,
        verification_model,
        writer,
    )
    consumer_gate = formal_status_module._authoritative_resume_offline_state_advance_consumer_readiness_gate(
        consumer_path,
        consumer_model,
        verification_path,
    )
    next_step_gate = formal_status_module._authoritative_resume_offline_state_advance_next_step_plan_gate(
        next_step_path,
        next_step_model,
        consumer_path,
    )

    assert post_gate.status == "ready"
    assert post_gate.blocks_physical_flow is False
    assert consumer_gate.status == "ready"
    assert consumer_gate.blocks_physical_flow is False
    assert next_step_gate.status == "ready"
    assert next_step_gate.blocks_physical_flow is False
    assert consumer_model["state_consumption_allowed"] is True
    assert consumer_model["resume_execution_allowed"] is False
    assert next_step_model["next_step_execution_allowed"] is False
