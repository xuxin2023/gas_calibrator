import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

import gas_calibrator.tools.export_v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight as tool_module
import gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight as auth_module
from gas_calibrator.tools.export_v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight import (
    main,
)
from gas_calibrator.v1_5.orchestration.full_flow import build_full_flow_plan
from gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight import (
    AUTHORIZATION_FILENAME,
    AUTHORIZATION_OPERATION,
    AUTHORIZATION_SCHEMA,
    CONFIRMATION_TEMPLATE,
    READY_STATUS,
    REVIEW_STATUS,
    build_v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_next_step_plan import (
    READY_STATUS as PLAN_READY_STATUS,
    SCHEMA as PLAN_SCHEMA,
)
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_formal_flow_contract import (
    discover_current_v1_5_inventory,
    validate_v1_5_formal_flow_contract,
)

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 7, 12, 12, 0, tzinfo=UTC)
PLAN_NAME = "v1_5_authoritative_resume_offline_state_advance_next_step_plan.json"


def _write(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return path


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fixture(tmp_path: Path, monkeypatch) -> tuple[Path, dict, Path, dict]:
    consumer_path = _write(
        tmp_path / "consumer" / "v1_5_authoritative_resume_offline_state_advance_consumer_readiness.json",
        {"schema": "consumer"},
    )
    plan = {
        "schema": PLAN_SCHEMA,
        "generated_at": "2026-07-12T11:59:00Z",
        "overall_status": PLAN_READY_STATUS,
        "next_step_plan_review_ready": True,
        "blocker_count": 0,
        "blocker_reasons": [],
        "consumer_readiness_json": str(consumer_path.absolute()),
        "consumer_readiness_sha256": _sha(consumer_path),
        "post_write_verification_json": "verification.json",
        "post_write_verification_sha256": "a" * 64,
        "full_flow_plan_json": "plan.json",
        "full_flow_plan_sha256": "b" * 64,
        "authoritative_state_json": "state.json",
        "authoritative_state_sha256": "c" * 64,
        "run_id": "run-113",
        "attempt_id": "attempt-113",
        "verified_step_id": "temperature_channel_fast_review",
        "completed_step_ids": ["temperature_channel_fast_review"],
        "next_step_id": "co2_open_flow_sampling",
        "next_step_title": "CO2 open-flow sampling",
        "next_step_phase": "co2",
        "next_step_tool_module": "gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue",
        "next_step_command": [],
        "next_step_execution_mode": "real_com_route_requires_authorization",
        "requires_real_com_authorization": True,
        "requires_pressure_authorization": False,
        "requires_route_authorization": True,
        "requires_write_authorization": False,
        "mature_route_module_verified": True,
        "plan_consumption_allowed": True,
        "execution_supported": False,
        "next_step_execution_allowed": False,
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
    plan_path = _write(tmp_path / "plan" / PLAN_NAME, plan)
    recomputed_plan = dict(plan)
    packet = {
        "schema": AUTHORIZATION_SCHEMA,
        "requested_operation": AUTHORIZATION_OPERATION,
        "confirmation_template": CONFIRMATION_TEMPLATE,
        "authorization_id": "next-step-review-113",
        "issued_at": "2026-07-12T11:55:00Z",
        "expires_at": "2026-07-12T12:15:00Z",
        "operator": "operator-a",
        "reviewer": "reviewer-b",
        "approver": "approver-c",
        "next_step_plan_json": str(plan_path.absolute()),
        "next_step_plan_sha256": _sha(plan_path),
        "consumer_readiness_json": str(consumer_path.absolute()),
        "consumer_readiness_sha256": _sha(consumer_path),
        "run_id": "run-113",
        "attempt_id": "attempt-113",
        "verified_step_id": "temperature_channel_fast_review",
        "next_step_id": "co2_open_flow_sampling",
        "next_step_tool_module": "gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue",
        "structured_confirmation": {
            "exact_plan_only": True,
            "review_only": True,
            "no_execution": True,
            "no_com": True,
            "no_pressure_control": True,
            "no_route_control": True,
            "no_device_or_coefficient_write": True,
            "no_postgresql_or_release": True,
            "mature_route_unchanged": True,
        },
        "allow_plan_review": True,
        "allow_next_step_execution": False,
        "allow_real_com": False,
        "allow_pressure_control": False,
        "allow_route_control": False,
        "allow_device_or_coefficient_write": False,
        "allow_postgresql_import": False,
        "next_step_execution_allowed": False,
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
    packet_path = _write(tmp_path / "authorization" / AUTHORIZATION_FILENAME, packet)
    monkeypatch.setattr(
        auth_module,
        "build_v1_5_authoritative_resume_offline_state_advance_next_step_plan",
        lambda **_kwargs: dict(recomputed_plan),
    )
    return plan_path, plan, packet_path, packet


def test_authorization_preflight_accepts_exact_review_only_packet(
    tmp_path: Path, monkeypatch
) -> None:
    plan_path, _plan, packet_path, _packet = _fixture(tmp_path, monkeypatch)
    model = build_v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight(
        next_step_plan_json=plan_path,
        authorization_packet_json=packet_path,
        now=NOW,
    )
    assert model["overall_status"] == READY_STATUS
    assert model["next_step_authorization_preflight_ready"] is True
    assert model["plan_review_allowed"] is True
    assert model["next_step_execution_allowed"] is False
    assert model["resume_execution_allowed"] is False
    assert model["opens_com_ports"] is False
    assert model["controls_water_or_gas_routes"] is False
    assert model["writes_coefficients"] is False
    assert model["connects_postgresql"] is False


@pytest.mark.parametrize(
    ("mutator", "reason"),
    [
        (lambda packet: packet.update(reviewer="approver-c"), "authorization_identities_must_be_distinct"),
        (lambda packet: packet.update(next_step_plan_sha256="bad"), "authorization_binding_mismatch:next_step_plan_sha256"),
        (lambda packet: packet.update(allow_next_step_execution=True), "authorization_capability_mismatch:allow_next_step_execution"),
        (lambda packet: packet.update(opens_com_ports=True), "authorization_boundary_opens_com_ports_not_false"),
        (lambda packet: packet.update(expires_at="2026-07-12T11:59:00Z"), "authorization_expired"),
    ],
)
def test_authorization_preflight_rejects_identity_binding_execution_or_expiry(
    tmp_path: Path, monkeypatch, mutator, reason: str
) -> None:
    plan_path, _plan, packet_path, packet = _fixture(tmp_path, monkeypatch)
    mutator(packet)
    _write(packet_path, packet)
    model = build_v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight(
        next_step_plan_json=plan_path,
        authorization_packet_json=packet_path,
        now=NOW,
    )
    assert model["overall_status"] == REVIEW_STATUS
    assert reason in model["review_reasons"]
    assert model["plan_review_allowed"] is False
    assert model["next_step_execution_allowed"] is False


def test_authorization_preflight_rejects_plan_tamper(
    tmp_path: Path, monkeypatch
) -> None:
    plan_path, plan, packet_path, _packet = _fixture(tmp_path, monkeypatch)
    plan["next_step_id"] = "h2o_open_flow_sampling"
    _write(plan_path, plan)
    model = build_v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight(
        next_step_plan_json=plan_path,
        authorization_packet_json=packet_path,
        now=NOW,
    )
    assert model["overall_status"] == REVIEW_STATUS
    assert "next_step_plan_recompute_mismatch" in model["review_reasons"]
    assert "authorization_binding_mismatch:next_step_plan_sha256" in model["review_reasons"]
    assert model["next_step_execution_allowed"] is False


def test_cli_and_inventory_remain_offline(tmp_path: Path, monkeypatch) -> None:
    plan_path, _plan, packet_path, _packet = _fixture(tmp_path, monkeypatch)
    model = build_v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight(
        next_step_plan_json=plan_path,
        authorization_packet_json=packet_path,
        now=NOW,
    )
    monkeypatch.setattr(
        tool_module,
        "build_v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight",
        lambda **_kwargs: dict(model),
    )
    output_dir = tmp_path / "output"
    assert main(
        [
            "--next-step-plan-json",
            str(plan_path),
            "--authorization-packet-json",
            str(packet_path),
            "--output-dir",
            str(output_dir),
            "--fail-on-review-required",
        ]
    ) == 0
    payload = json.loads(
        (
            output_dir
            / "v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["next_step_execution_allowed"] is False

    rejected = tmp_path / "rejected"
    with pytest.raises(SystemExit) as exc:
        main(
            [
                "--next-step-plan-json",
                str(plan_path),
                "--authorization-packet-json",
                str(packet_path),
                "--output-dir",
                str(rejected),
                "--execute",
            ]
        )
    assert exc.value.code == 2
    assert not rejected.exists()

    entry = classify_v1_5_entrypoint(
        ROOT
        / "src/gas_calibrator/tools/export_v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight.py",
        root=ROOT,
    )
    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False


def test_formal_flow_rejects_authorization_preflight_as_canonical_step(
    tmp_path: Path,
) -> None:
    config = _write(tmp_path / "config.json", {})
    plan = build_full_flow_plan(
        config_path=config,
        output_dir=tmp_path / "flow",
        run_id="next-step-authorization-contract",
    )
    steps = list(plan.steps)
    index = next(
        index
        for index, step in enumerate(steps)
        if step.step_id == "temperature_channel_fast_review"
    )
    module = (
        "gas_calibrator.tools."
        "export_v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight"
    )
    steps[index] = replace(
        steps[index],
        step_id="offline_next_step_authorization_preflight",
        tool_module=module,
        command=("python", "-m", module),
    )
    report = validate_v1_5_formal_flow_contract(
        replace(plan, steps=tuple(steps)),
        inventory_entries=discover_current_v1_5_inventory(
            anchor_paths=(Path.cwd(),)
        ),
    )
    assert report.status == "blocked"
    assert any(
        issue.code == "offline_state_advance_evidence_must_remain_out_of_band"
        for issue in report.issues
    )
