import hashlib
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

import gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor as executor_module
import gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_next_step_execution_authorization as authorization_module
import gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_next_step_execution_preflight as preflight_module
from gas_calibrator.tools.run_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor import (
    main as executor_main,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor import (
    CONFIRMATION_TEXT,
    EXECUTED_STATUS,
    HOLD_STATUS,
    LOCKED_STATUS,
    run_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor,
    write_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design import (
    FUTURE_AUTHORIZATION_SCHEMA,
    READY_STATUS as DESIGN_READY_STATUS,
    SCHEMA as DESIGN_SCHEMA,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_next_step_execution_authorization import (
    AUTHORIZATION_FILENAME,
    AUTHORIZATION_OPERATION,
    CONFIRMATION_TEMPLATE,
    READY_STATUS as AUTH_READY_STATUS,
    REVIEW_STATUS as AUTH_REVIEW_STATUS,
    build_v1_5_authoritative_resume_offline_state_advance_next_step_execution_authorization,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_next_step_execution_preflight import (
    READY_STATUS as PREFLIGHT_READY_STATUS,
    REVIEW_STATUS as PREFLIGHT_REVIEW_STATUS,
    build_v1_5_authoritative_resume_offline_state_advance_next_step_execution_preflight,
)
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_formal_flow_contract import (
    AUTHORITATIVE_RESUME_OFFLINE_STATE_ADVANCE_NEXT_STEP_CONTROLLED_EXECUTOR_MODULE,
    AUTHORITATIVE_RESUME_OFFLINE_STATE_ADVANCE_NEXT_STEP_EXECUTION_AUTHORIZATION_MODULE,
    AUTHORITATIVE_RESUME_OFFLINE_STATE_ADVANCE_NEXT_STEP_EXECUTION_PREFLIGHT_MODULE,
    OUT_OF_BAND_OFFLINE_STATE_ADVANCE_MODULES,
)

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 7, 12, 12, 0, tzinfo=UTC)
MODULE = "gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue"


def _write(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return path


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _command_sha(command: list[str]) -> str:
    normalized = json.dumps(command, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict:
    root = tmp_path / "run"
    output = root / "co2" / "queue_manifest.csv"
    command = [
        "python",
        "-m",
        MODULE,
        "--config",
        str(
            root
            / "coefficient_epoch_0_getco_snapshot"
            / "runtime_identity_bound_config.json"
        ),
        "--output-root",
        str(root / "co2"),
    ]
    state_path = _write(root / "state.json", {"schema": "state", "run_id": "run-116"})
    consumer_path = _write(root / "consumer.json", {"schema": "consumer"})
    full_flow_path = _write(
        root / "v1_5_full_flow_plan.json",
        {
            "schema": "v1_5_full_calibration_flow_plan_v0",
            "run_id": "run-116",
            "steps": [
                {
                    "step_id": "co2_open_flow_sampling",
                    "tool_module": MODULE,
                    "command": command,
                    "expected_outputs": [str(output.relative_to(root))],
                    "execution_mode": "real_com_route_requires_authorization",
                    "uses_validated_v1_5_entry": True,
                    "opens_com_ports": True,
                    "controls_pressure": False,
                    "controls_gas_route": True,
                    "controls_water_route": False,
                    "writes_device_id": False,
                    "writes_coefficients": False,
                }
            ],
        },
    )
    plan_path = _write(
        root
        / "plan"
        / "v1_5_authoritative_resume_offline_state_advance_next_step_plan.json",
        {
            "schema": "v1_5_authoritative_resume_offline_state_advance_next_step_plan_v1",
            "overall_status": "ready_for_offline_advanced_resume_next_step_plan_review",
            "consumer_readiness_json": str(consumer_path.absolute()),
            "consumer_readiness_sha256": _sha(consumer_path),
            "full_flow_plan_json": str(full_flow_path.absolute()),
            "full_flow_plan_sha256": _sha(full_flow_path),
            "authoritative_state_json": str(state_path.absolute()),
            "authoritative_state_sha256": _sha(state_path),
            "run_id": "run-116",
            "attempt_id": "attempt-116",
            "verified_step_id": "temperature_channel_fast_review",
            "next_step_id": "co2_open_flow_sampling",
            "next_step_tool_module": MODULE,
            "next_step_command": command,
            "requires_real_com_authorization": True,
            "requires_pressure_authorization": False,
            "requires_route_authorization": True,
            "requires_write_authorization": False,
        },
    )
    review_path = _write(
        root
        / "review"
        / "v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight.json",
        {"schema": "review", "next_step_plan_json": str(plan_path.absolute())},
    )
    blocked_path = _write(
        root
        / "blocked"
        / "v1_5_authoritative_resume_offline_state_advance_next_step_blocked_executor.json",
        {
            "schema": "blocked",
            "next_step_authorization_preflight_json": str(review_path.absolute()),
            "next_step_plan_json": str(plan_path.absolute()),
        },
    )
    design = {
        "schema": DESIGN_SCHEMA,
        "generated_at": "2026-07-12T11:59:00Z",
        "overall_status": DESIGN_READY_STATUS,
        "controlled_next_step_executor_design_ready": True,
        "review_required_count": 0,
        "review_reasons": [],
        "next_step_blocked_executor_json": str(blocked_path.absolute()),
        "next_step_blocked_executor_sha256": _sha(blocked_path),
        "next_step_plan_json": str(plan_path.absolute()),
        "next_step_plan_sha256": _sha(plan_path),
        "next_step_command_sha256_recorded_only": _command_sha(command),
        "execution_supported": False,
        "next_step_execution_allowed": False,
        "resume_execution_allowed": False,
        "execute_flag_allowed": False,
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
    design_path = _write(
        root
        / "design"
        / "v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design.json",
        design,
    )
    packet = {
        "schema": FUTURE_AUTHORIZATION_SCHEMA,
        "requested_operation": AUTHORIZATION_OPERATION,
        "confirmation_template": CONFIRMATION_TEMPLATE,
        "authorization_id": "execute-once-116",
        "issued_at": "2026-07-12T11:55:00Z",
        "expires_at": "2026-07-12T12:15:00Z",
        "operator": "operator-a",
        "reviewer": "reviewer-b",
        "approver": "approver-c",
        "controlled_executor_design_json": str(design_path.absolute()),
        "controlled_executor_design_sha256": _sha(design_path),
        "blocked_executor_json": str(blocked_path.absolute()),
        "blocked_executor_sha256": _sha(blocked_path),
        "review_authorization_preflight_json": str(review_path.absolute()),
        "review_authorization_preflight_sha256": _sha(review_path),
        "next_step_plan_json": str(plan_path.absolute()),
        "next_step_plan_sha256": _sha(plan_path),
        "consumer_readiness_json": str(consumer_path.absolute()),
        "consumer_readiness_sha256": _sha(consumer_path),
        "full_flow_plan_json": str(full_flow_path.absolute()),
        "full_flow_plan_sha256": _sha(full_flow_path),
        "authoritative_state_json": str(state_path.absolute()),
        "authoritative_state_sha256": _sha(state_path),
        "run_id": "run-116",
        "attempt_id": "attempt-116",
        "verified_step_id": "temperature_channel_fast_review",
        "next_step_id": "co2_open_flow_sampling",
        "next_step_tool_module": MODULE,
        "next_step_command_sha256": _command_sha(command),
        "structured_confirmation": {
            "exact_one_step_only": True,
            "no_substitute_entry": True,
            "no_shell": True,
            "no_executor_retry": True,
            "no_fallback": True,
            "no_automatic_state_advance": True,
            "mature_runner_owns_physics_and_qc": True,
            "failure_holds": True,
            "no_postgresql_or_release": True,
        },
        "allow_real_com": True,
        "allow_pressure_control": False,
        "allow_route_control": True,
        "allow_device_or_coefficient_write": False,
        "allow_postgresql_import": False,
    }
    packet_path = _write(root / "authorization" / AUTHORIZATION_FILENAME, packet)
    monkeypatch.setattr(
        authorization_module,
        "build_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design",
        lambda **_kwargs: {"manifest": dict(design)},
    )
    return {
        "root": root,
        "output": output,
        "command": command,
        "design": design,
        "design_path": design_path,
        "packet": packet,
        "packet_path": packet_path,
    }


def _authorization(fixture: dict) -> dict:
    return build_v1_5_authoritative_resume_offline_state_advance_next_step_execution_authorization(
        controlled_executor_design_json=fixture["design_path"],
        execution_authorization_json=fixture["packet_path"],
        now=NOW,
    )


def _preflight(
    tmp_path: Path, fixture: dict, monkeypatch: pytest.MonkeyPatch
) -> tuple[Path, dict]:
    authorization = _authorization(fixture)
    validation_path = _write(
        tmp_path
        / "authorization-validation"
        / "v1_5_authoritative_resume_offline_state_advance_next_step_execution_authorization_validation.json",
        authorization,
    )
    monkeypatch.setattr(
        preflight_module,
        "build_v1_5_authoritative_resume_offline_state_advance_next_step_execution_authorization",
        lambda **_kwargs: dict(authorization),
    )
    model = build_v1_5_authoritative_resume_offline_state_advance_next_step_execution_preflight(
        execution_authorization_validation_json=validation_path,
        now=NOW,
    )
    path = _write(
        tmp_path
        / "execution-preflight"
        / "v1_5_authoritative_resume_offline_state_advance_next_step_execution_preflight.json",
        model,
    )
    monkeypatch.setattr(
        executor_module,
        "build_v1_5_authoritative_resume_offline_state_advance_next_step_execution_preflight",
        lambda **_kwargs: dict(model),
    )
    return path, model


def test_execution_authorization_binds_exact_chain_and_minimum_capabilities(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path, monkeypatch)
    model = _authorization(fixture)
    assert model["overall_status"] == AUTH_READY_STATUS
    assert model["execution_authorization_validated"] is True
    assert model["authorized_capabilities"] == {
        "allow_real_com": True,
        "allow_pressure_control": False,
        "allow_route_control": True,
        "allow_device_or_coefficient_write": False,
        "allow_postgresql_import": False,
    }
    assert model["next_step_execution_allowed"] is False


@pytest.mark.parametrize(
    ("mutator", "reason"),
    [
        (
            lambda packet: packet.update(approver="reviewer-b"),
            "execution_authorization_identities_must_be_distinct",
        ),
        (
            lambda packet: packet.update(expires_at="2026-07-12T11:59:00Z"),
            "execution_authorization_expired",
        ),
        (
            lambda packet: packet.update(allow_pressure_control=True),
            "execution_authorization_capability_mismatch:allow_pressure_control",
        ),
        (
            lambda packet: packet.update(next_step_command_sha256="bad"),
            "execution_authorization_binding_mismatch:next_step_command_sha256",
        ),
    ],
)
def test_execution_authorization_rejects_identity_expiry_overgrant_or_command_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mutator, reason: str
) -> None:
    fixture = _fixture(tmp_path, monkeypatch)
    mutator(fixture["packet"])
    _write(fixture["packet_path"], fixture["packet"])
    model = _authorization(fixture)
    assert model["overall_status"] == AUTH_REVIEW_STATUS
    assert reason in model["review_reasons"]
    assert model["next_step_execution_allowed"] is False


def test_immediate_preflight_allows_only_exact_mature_command(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path, monkeypatch)
    _path, model = _preflight(tmp_path, fixture, monkeypatch)
    assert model["overall_status"] == PREFLIGHT_READY_STATUS
    assert model["controlled_next_step_execution_preflight_ready"] is True
    assert model["next_step_execution_allowed"] is True
    assert model["next_step_command"] == fixture["command"]
    assert model["single_process_launch_max"] == 1
    assert model["automatic_retry_allowed"] is False
    assert model["fallback_entry_allowed"] is False


def test_immediate_preflight_rejects_plan_or_authorization_tamper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path, monkeypatch)
    authorization = _authorization(fixture)
    validation_path = _write(
        tmp_path
        / "validation"
        / "v1_5_authoritative_resume_offline_state_advance_next_step_execution_authorization_validation.json",
        authorization,
    )
    fixture["packet"]["operator"] = "changed-after-validation"
    _write(fixture["packet_path"], fixture["packet"])
    monkeypatch.setattr(
        preflight_module,
        "build_v1_5_authoritative_resume_offline_state_advance_next_step_execution_authorization",
        lambda **_kwargs: _authorization(fixture),
    )
    model = build_v1_5_authoritative_resume_offline_state_advance_next_step_execution_preflight(
        execution_authorization_validation_json=validation_path,
        now=NOW,
    )
    assert model["overall_status"] == PREFLIGHT_REVIEW_STATUS
    assert "execution_authorization_sha256_drift" in model["hold_reasons"]
    assert model["next_step_execution_allowed"] is False


def test_controlled_executor_defaults_locked_without_subprocess(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path, monkeypatch)
    preflight_path, _model = _preflight(tmp_path, fixture, monkeypatch)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("subprocess must remain locked")

    result = run_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor(
        next_step_execution_preflight_json=preflight_path,
        now=NOW,
        subprocess_runner=forbidden,
    )
    assert result["overall_status"] == LOCKED_STATUS
    assert result["execution_attempted"] is False
    assert result["process_launch_count"] == 0
    assert result["authoritative_state_advanced"] is False


def test_controlled_executor_runs_one_shell_free_command_with_fake_runner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path, monkeypatch)
    preflight_path, _model = _preflight(tmp_path, fixture, monkeypatch)
    calls = []

    def fake_runner(command, **kwargs):
        calls.append((command, kwargs))
        fixture["output"].parent.mkdir(parents=True, exist_ok=True)
        fixture["output"].write_text("fresh", encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    result = run_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor(
        next_step_execution_preflight_json=preflight_path,
        execute_next_step=True,
        expected_attempt_id="attempt-116",
        operator_confirmation_text=CONFIRMATION_TEXT,
        now=NOW,
        subprocess_runner=fake_runner,
    )
    assert result["overall_status"] == EXECUTED_STATUS
    assert result["next_step_process_completed"] is True
    assert len(calls) == 1
    assert calls[0][0] == fixture["command"]
    assert calls[0][1]["shell"] is False
    assert result["executor_retry_count"] == 0
    assert result["fallback_entry_used"] is False
    assert result["authoritative_state_advanced"] is False
    assert result["connects_postgresql"] is False


def test_controlled_executor_holds_without_retry_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path, monkeypatch)
    preflight_path, _model = _preflight(tmp_path, fixture, monkeypatch)
    calls = 0

    def fake_runner(command, **_kwargs):
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess(command, 7, stdout="", stderr="failed")

    result = run_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor(
        next_step_execution_preflight_json=preflight_path,
        execute_next_step=True,
        expected_attempt_id="attempt-116",
        operator_confirmation_text=CONFIRMATION_TEXT,
        now=NOW,
        subprocess_runner=fake_runner,
    )
    assert result["overall_status"] == HOLD_STATUS
    assert calls == 1
    assert result["next_step_process_completed"] is False
    assert "next_step_child_process_return_code:7" in result["hold_reasons"]
    assert result["authoritative_state_advanced"] is False


def test_controlled_executor_rejects_wrong_confirmation_before_process(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path, monkeypatch)
    preflight_path, _model = _preflight(tmp_path, fixture, monkeypatch)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("subprocess must not start")

    result = run_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor(
        next_step_execution_preflight_json=preflight_path,
        execute_next_step=True,
        expected_attempt_id="attempt-116",
        operator_confirmation_text="wrong",
        now=NOW,
        subprocess_runner=forbidden,
    )
    assert result["overall_status"] == HOLD_STATUS
    assert result["execution_attempted"] is False
    assert "next_step_execution_operator_confirmation_invalid" in result["hold_reasons"]


def test_executor_writer_emits_complete_evidence_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path, monkeypatch)
    preflight_path, _model = _preflight(tmp_path, fixture, monkeypatch)
    result = run_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor(
        next_step_execution_preflight_json=preflight_path,
        now=NOW,
    )
    outputs = write_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor(
        result, tmp_path / "evidence"
    )
    assert set(outputs) == {
        "invocation",
        "pre_execution_revalidation",
        "command_attempts",
        "child_process_result",
        "hold_events",
        "post_execution_evidence_index",
    }
    assert all(path.is_file() for path in outputs.values())
    index = json.loads(
        outputs["post_execution_evidence_index"].read_text(encoding="utf-8")
    )
    assert index["authoritative_state_advanced"] is False
    assert index["not_real_acceptance_evidence"] is True


def test_executor_cli_default_is_locked_and_classified_manual_authorized(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path, monkeypatch)
    preflight_path, _model = _preflight(tmp_path, fixture, monkeypatch)
    monkeypatch.setattr(executor_module, "_now", lambda: NOW)
    output = tmp_path / "cli-output"
    assert (
        executor_main(
            [
                "--next-step-execution-preflight-json",
                str(preflight_path),
                "--output-dir",
                str(output),
            ]
        )
        == 0
    )
    invocation = json.loads(
        (output / "executor_invocation.json").read_text(encoding="utf-8")
    )
    assert invocation["overall_status"] == LOCKED_STATUS
    assert invocation["execution_attempted"] is False

    entry = classify_v1_5_entrypoint(
        ROOT
        / "src/gas_calibrator/tools/run_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor.py",
        root=ROOT,
    )
    assert entry.category == "full_flow_orchestration"
    assert entry.formal_status == "manual_authorized_single_step_resume_only"
    assert entry.risk_level == "real_com_or_route_or_write_risk"


def test_execution_authorization_and_preflight_tools_remain_offline() -> None:
    for name in (
        "export_v1_5_authoritative_resume_offline_state_advance_next_step_execution_authorization.py",
        "export_v1_5_authoritative_resume_offline_state_advance_next_step_execution_preflight.py",
    ):
        entry = classify_v1_5_entrypoint(
            ROOT / "src/gas_calibrator/tools" / name, root=ROOT
        )
        assert entry.category == "formal_review_evidence"
        assert entry.formal_status == "formal_support"
        assert entry.risk_level == "offline"
        assert entry.opens_com_ports is False


def test_all_execution_components_remain_out_of_band_from_canonical_full_flow() -> None:
    assert {
        AUTHORITATIVE_RESUME_OFFLINE_STATE_ADVANCE_NEXT_STEP_EXECUTION_AUTHORIZATION_MODULE,
        AUTHORITATIVE_RESUME_OFFLINE_STATE_ADVANCE_NEXT_STEP_EXECUTION_PREFLIGHT_MODULE,
        AUTHORITATIVE_RESUME_OFFLINE_STATE_ADVANCE_NEXT_STEP_CONTROLLED_EXECUTOR_MODULE,
    }.issubset(OUT_OF_BAND_OFFLINE_STATE_ADVANCE_MODULES)
