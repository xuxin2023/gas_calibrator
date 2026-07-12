import hashlib
import json
import sys
from dataclasses import replace
from pathlib import Path

import gas_calibrator.validation.v1_5_formal_run_status as formal_status_module
from gas_calibrator.v1_5.orchestration.full_flow import build_full_flow_plan
from gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_consumer_readiness import (
    READY_STATUS as CONSUMER_READY_STATUS,
    SCHEMA as CONSUMER_SCHEMA,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_post_write_verification import (
    READY_STATUS as VERIFICATION_READY_STATUS,
    SCHEMA as VERIFICATION_SCHEMA,
)
from gas_calibrator.validation.v1_5_formal_flow_contract import (
    discover_current_v1_5_inventory,
    validate_v1_5_formal_flow_contract,
)
from gas_calibrator.validation.v1_5_formal_run_status import build_v1_5_formal_run_status


def _write(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _flag_value(command: tuple[str, ...], flag: str) -> str:
    values = [str(part) for part in command]
    return values[values.index(flag) + 1]


def _plan(tmp_path: Path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    return build_full_flow_plan(
        config_path=config,
        output_dir=tmp_path / "flow",
        run_id="offline-state-advance-status",
        operator="operator-a",
        analyzer_id="multi-device",
    )


def _verification_payload(atomic: Path) -> dict:
    payload = {
        key: "" for key in formal_status_module.OFFLINE_STATE_ADVANCE_VERIFICATION_COMPARE_KEYS
    }
    payload.update(
        {
            "schema": VERIFICATION_SCHEMA,
            "overall_status": VERIFICATION_READY_STATUS,
            "post_write_verification_ready": True,
            "blocker_count": 0,
            "blocker_reasons": [],
            "atomic_write_json": str(atomic.resolve()),
            "atomic_write_sha256": _sha(atomic),
            "state_consumption_allowed": False,
            "execution_supported": False,
            "resume_execution_allowed": False,
            "opens_com_ports": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "writes_authoritative_state": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "connects_postgresql": False,
            "database_written": False,
            "formal_release_allowed": False,
            "database_import_allowed": False,
            "not_real_acceptance_evidence": True,
        }
    )
    return payload


def _consumer_payload(verification: Path) -> dict:
    return {
        "schema": CONSUMER_SCHEMA,
        "generated_at": "2026-07-12T07:00:00Z",
        "overall_status": CONSUMER_READY_STATUS,
        "resume_state_consumer_readiness_ready": True,
        "blocker_count": 0,
        "blocker_reasons": [],
        "post_write_verification_json": str(verification.resolve()),
        "post_write_verification_sha256": _sha(verification),
        "atomic_write_json": "writer.json",
        "atomic_write_sha256": "a" * 64,
        "full_flow_plan_json": "plan.json",
        "full_flow_plan_sha256": "b" * 64,
        "authoritative_state_json": "state.json",
        "authoritative_state_sha256": "c" * 64,
        "run_id": "run-001",
        "attempt_id": "attempt-001",
        "verified_step_id": "temperature_channel_fast_review",
        "completed_step_ids": ["temperature_channel_fast_review"],
        "next_step_id": "co2_open_flow_sampling",
        "state_consumption_allowed": True,
        "execution_supported": False,
        "resume_execution_allowed": False,
        "would_execute": False,
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_authoritative_state": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "connects_postgresql": False,
        "database_written": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
    }


def test_full_flow_keeps_state_advance_evidence_out_of_canonical_steps(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    step_ids = [step.step_id for step in plan.steps]
    post_id = "authoritative_resume_offline_state_advance_post_write_verification"
    consumer_id = "authoritative_resume_offline_state_advance_consumer_readiness"
    assert post_id not in step_ids
    assert consumer_id not in step_ids
    assert step_ids.index("temperature_channel_fast_review") < step_ids.index(
        "co2_open_flow_sampling"
    )
    assert all(
        "authoritative_resume_offline_state_advance" not in str(step.tool_module or "")
        for step in plan.steps
    )

    status = next(step for step in plan.steps if step.step_id == "formal_run_status_snapshot")
    assert _flag_value(
        status.command,
        "--authoritative-resume-offline-state-advance-atomic-write-json",
    ).endswith(
        "authoritative_resume_offline_state_advance_atomic_writer\\v1_5_authoritative_resume_offline_state_advance_atomic_writer.json"
    )
    assert _flag_value(
        status.command,
        "--authoritative-resume-offline-state-advance-post-write-verification-json",
    ).endswith(
        "authoritative_resume_offline_state_advance_post_write_verification\\v1_5_authoritative_resume_offline_state_advance_post_write_verification.json"
    )
    assert _flag_value(
        status.command,
        "--authoritative-resume-offline-state-advance-consumer-readiness-json",
    ).endswith(
        "authoritative_resume_offline_state_advance_consumer_readiness\\v1_5_authoritative_resume_offline_state_advance_consumer_readiness.json"
    )


def test_formal_flow_contract_rejects_state_advance_tools_as_canonical_steps(
    tmp_path: Path,
) -> None:
    plan = _plan(tmp_path)
    report = validate_v1_5_formal_flow_contract(
        plan,
        inventory_entries=discover_current_v1_5_inventory(anchor_paths=(Path.cwd(),)),
    )
    assert report.status == "pass"

    steps = list(plan.steps)
    index = next(
        index for index, step in enumerate(steps) if step.step_id == "temperature_channel_fast_review"
    )
    steps[index] = replace(
        steps[index],
        step_id="authoritative_resume_offline_state_advance_consumer_readiness",
        tool_module=(
            "gas_calibrator.tools.export_v1_5_authoritative_resume_offline_state_advance_consumer_readiness"
        ),
        command=(
            sys.executable,
            "-m",
            "gas_calibrator.tools.export_v1_5_authoritative_resume_offline_state_advance_consumer_readiness",
            "--post-write-verification-json",
            "verification.json",
            "--output-dir",
            "consumer",
            "--fail-on-blocker",
        ),
    )
    blocked = validate_v1_5_formal_flow_contract(
        replace(plan, steps=tuple(steps)),
        inventory_entries=discover_current_v1_5_inventory(anchor_paths=(Path.cwd(),)),
    )
    assert blocked.status == "blocked"
    assert any(
        issue.code == "offline_state_advance_evidence_must_remain_out_of_band"
        for issue in blocked.issues
    )


def test_formal_status_gates_accept_exact_read_only_chain(tmp_path: Path, monkeypatch) -> None:
    atomic = _write(tmp_path / "writer.json", {"committed": True})
    verification = _verification_payload(atomic)
    verification_path = _write(
        tmp_path
        / "v1_5_authoritative_resume_offline_state_advance_post_write_verification.json",
        verification,
    )
    consumer = _consumer_payload(verification_path)
    consumer_path = _write(
        tmp_path / "v1_5_authoritative_resume_offline_state_advance_consumer_readiness.json",
        consumer,
    )
    monkeypatch.setattr(
        formal_status_module,
        "build_v1_5_authoritative_resume_offline_state_advance_post_write_verification",
        lambda **_kwargs: dict(verification),
    )
    monkeypatch.setattr(
        formal_status_module,
        "build_v1_5_authoritative_resume_offline_state_advance_consumer_readiness",
        lambda **_kwargs: dict(consumer),
    )
    post_gate = formal_status_module._authoritative_resume_offline_state_advance_post_write_verification_gate(
        verification_path,
        verification,
        atomic,
    )
    consumer_gate = formal_status_module._authoritative_resume_offline_state_advance_consumer_readiness_gate(
        consumer_path,
        consumer,
        verification_path,
    )
    assert post_gate.status == "ready"
    assert post_gate.blocks_physical_flow is False
    assert post_gate.release_gate is False
    assert consumer_gate.status == "ready"
    assert consumer_gate.blocks_physical_flow is False
    assert consumer_gate.release_gate is False


def test_formal_status_blocks_missing_or_execution_capable_consumer(tmp_path: Path) -> None:
    root = tmp_path / "run"
    atomic = _write(
        root
        / "authoritative_resume_offline_state_advance_atomic_writer"
        / "v1_5_authoritative_resume_offline_state_advance_atomic_writer.json",
        {"schema": "writer"},
    )
    model = build_v1_5_formal_run_status(
        run_dir=root,
        authoritative_resume_offline_state_advance_atomic_write_json=atomic,
    )
    gates = {row["gate_id"]: row for row in model["gates"]}
    assert gates[
        "authoritative_resume_offline_state_advance_post_write_verification"
    ]["status"] == "missing"
    assert gates[
        "authoritative_resume_offline_state_advance_consumer_readiness"
    ]["status"] == "missing"
    assert model["can_continue_physical_flow"] is False

    verification = _write(
        root
        / "authoritative_resume_offline_state_advance_post_write_verification"
        / "v1_5_authoritative_resume_offline_state_advance_post_write_verification.json",
        {"schema": VERIFICATION_SCHEMA},
    )
    consumer_payload = _consumer_payload(verification)
    consumer_payload["resume_execution_allowed"] = True
    consumer = _write(
        root
        / "authoritative_resume_offline_state_advance_consumer_readiness"
        / "v1_5_authoritative_resume_offline_state_advance_consumer_readiness.json",
        consumer_payload,
    )
    gate = formal_status_module._authoritative_resume_offline_state_advance_consumer_readiness_gate(
        consumer,
        consumer_payload,
        verification,
    )
    assert gate.status == "blocked"
    assert gate.blocks_physical_flow is True
