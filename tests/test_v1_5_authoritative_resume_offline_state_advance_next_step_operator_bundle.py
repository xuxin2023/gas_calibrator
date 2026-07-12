import hashlib
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

import gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_next_step_execution_authorization as authorization_module
import gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_next_step_operator_bundle as bundle_module
from gas_calibrator.tools.run_v1_5_authoritative_resume_offline_state_advance_next_step_operator_bundle import (
    main,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor import (
    CONFIRMATION_TEXT,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design import (
    READY_STATUS as DESIGN_READY_STATUS,
    SCHEMA as DESIGN_SCHEMA,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_next_step_operator_bundle import (
    EXECUTED_STATUS,
    HOLD_STATUS,
    PREPARED_STATUS,
    build_v1_5_next_step_execution_authorization_packet,
    run_v1_5_authoritative_resume_offline_state_advance_next_step_operator_bundle,
)
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_formal_flow_contract import (
    AUTHORITATIVE_RESUME_OFFLINE_STATE_ADVANCE_NEXT_STEP_OPERATOR_BUNDLE_MODULE,
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
    expected_output = root / "co2" / "queue_manifest.csv"
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
    state_path = _write(root / "state.json", {"schema": "state", "run_id": "run-117"})
    consumer_path = _write(root / "consumer.json", {"schema": "consumer"})
    full_flow_path = _write(
        root / "v1_5_full_flow_plan.json",
        {
            "schema": "v1_5_full_calibration_flow_plan_v0",
            "run_id": "run-117",
            "steps": [
                {
                    "step_id": "co2_open_flow_sampling",
                    "tool_module": MODULE,
                    "command": command,
                    "expected_outputs": [str(expected_output.relative_to(root))],
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
            "run_id": "run-117",
            "attempt_id": "attempt-117",
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
    monkeypatch.setattr(
        authorization_module,
        "build_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design",
        lambda **_kwargs: {"manifest": dict(design)},
    )
    return {
        "root": root,
        "design": design,
        "design_path": design_path,
        "plan_path": plan_path,
        "full_flow_path": full_flow_path,
        "state_path": state_path,
        "consumer_path": consumer_path,
        "blocked_path": blocked_path,
        "review_path": review_path,
        "command": command,
        "expected_output": expected_output,
    }


def _run(tmp_path: Path, fixture: dict, **kwargs) -> dict:
    return (
        run_v1_5_authoritative_resume_offline_state_advance_next_step_operator_bundle(
            controlled_executor_design_json=fixture["design_path"],
            authorization_id="operator-bundle-117",
            operator="operator-a",
            reviewer="reviewer-b",
            approver="approver-c",
            output_dir=tmp_path / "bundle",
            now=NOW,
            **kwargs,
        )
    )


def test_packet_builder_derives_exact_hashes_ttl_and_capabilities(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path, monkeypatch)
    packet = build_v1_5_next_step_execution_authorization_packet(
        controlled_executor_design_json=fixture["design_path"],
        authorization_id="operator-bundle-117",
        operator="operator-a",
        reviewer="reviewer-b",
        approver="approver-c",
        ttl_s=900,
        now=NOW,
    )
    assert packet["issued_at"] == "2026-07-12T12:00:00Z"
    assert packet["expires_at"] == "2026-07-12T12:15:00Z"
    assert packet["next_step_plan_sha256"] == _sha(fixture["plan_path"])
    assert packet["next_step_command_sha256"] == _command_sha(fixture["command"])
    assert packet["allow_real_com"] is True
    assert packet["allow_route_control"] is True
    assert packet["allow_pressure_control"] is False
    assert packet["allow_device_or_coefficient_write"] is False
    assert packet["allow_postgresql_import"] is False
    assert packet["capabilities_derived_not_operator_selected"] is True


def test_operator_bundle_defaults_to_fresh_locked_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path, monkeypatch)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("default operator bundle must not start a process")

    model = _run(tmp_path, fixture, subprocess_runner=forbidden)
    assert model["overall_status"] == PREPARED_STATUS
    assert model["operator_bundle_prepared"] is True
    assert model["execution_requested"] is False
    assert model["execution_attempted"] is False
    assert model["next_step_process_completed"] is False
    assert model["authoritative_state_advanced"] is False
    assert Path(model["manifest_json"]).is_file()
    assert Path(model["execution_authorization_json"]).is_file()
    assert Path(model["execution_preflight_json"]).is_file()
    assert Path(model["executor_evidence_index_json"]).is_file()


def test_operator_bundle_executes_one_fake_process_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path, monkeypatch)
    calls = []

    def fake_runner(command, **kwargs):
        calls.append((command, kwargs))
        fixture["expected_output"].parent.mkdir(parents=True, exist_ok=True)
        fixture["expected_output"].write_text("fresh", encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    model = _run(
        tmp_path,
        fixture,
        execute_next_step=True,
        expected_attempt_id="attempt-117",
        operator_confirmation_text=CONFIRMATION_TEXT,
        subprocess_runner=fake_runner,
    )
    assert model["overall_status"] == EXECUTED_STATUS
    assert model["next_step_process_completed"] is True
    assert len(calls) == 1
    assert calls[0][1]["shell"] is False
    assert model["authoritative_state_advanced"] is False
    assert model["connects_postgresql"] is False


@pytest.mark.parametrize(
    ("kwargs", "reason"),
    [
        ({"ttl_s": 1801}, "execution_authorization_ttl_out_of_range"),
        (
            {"reviewer": "approver-c"},
            "execution_authorization_identities_must_be_distinct",
        ),
    ],
)
def test_operator_bundle_holds_invalid_ttl_or_identity_before_process(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kwargs: dict,
    reason: str,
) -> None:
    fixture = _fixture(tmp_path, monkeypatch)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("invalid authorization must not start a process")

    call = {
        "controlled_executor_design_json": fixture["design_path"],
        "authorization_id": "operator-bundle-117",
        "operator": "operator-a",
        "reviewer": "reviewer-b",
        "approver": "approver-c",
        "output_dir": tmp_path / "bundle",
        "execute_next_step": True,
        "expected_attempt_id": "attempt-117",
        "operator_confirmation_text": CONFIRMATION_TEXT,
        "now": NOW,
        "subprocess_runner": forbidden,
    }
    call.update(kwargs)
    model = (
        run_v1_5_authoritative_resume_offline_state_advance_next_step_operator_bundle(
            **call
        )
    )
    assert model["overall_status"] == HOLD_STATUS
    assert model["execution_attempted"] is False
    assert reason in model["hold_reasons"]
    assert model["authoritative_state_advanced"] is False


def test_operator_bundle_refuses_nonempty_output_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path, monkeypatch)
    output = tmp_path / "bundle"
    _write(output / "old.json", {"stale": True})
    with pytest.raises(ValueError, match="must be absent or empty"):
        run_v1_5_authoritative_resume_offline_state_advance_next_step_operator_bundle(
            controlled_executor_design_json=fixture["design_path"],
            authorization_id="operator-bundle-117",
            operator="operator-a",
            reviewer="reviewer-b",
            approver="approver-c",
            output_dir=output,
            now=NOW,
        )


def test_operator_bundle_refuses_reparse_output_before_writing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path, monkeypatch)
    output = tmp_path / "bundle"
    monkeypatch.setattr(
        bundle_module, "_contains_reparse", lambda path: path == output.absolute()
    )
    with pytest.raises(ValueError, match="must not contain a reparse point"):
        run_v1_5_authoritative_resume_offline_state_advance_next_step_operator_bundle(
            controlled_executor_design_json=fixture["design_path"],
            authorization_id="operator-bundle-117",
            operator="operator-a",
            reviewer="reviewer-b",
            approver="approver-c",
            output_dir=output,
            now=NOW,
        )
    assert not output.exists()


def test_operator_bundle_cli_rejects_generic_execute_before_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path, monkeypatch)
    output = tmp_path / "cli"
    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "--controlled-executor-design-json",
                str(fixture["design_path"]),
                "--authorization-id",
                "operator-bundle-117",
                "--operator",
                "operator-a",
                "--reviewer",
                "reviewer-b",
                "--approver",
                "approver-c",
                "--output-dir",
                str(output),
                "--execute",
            ]
        )
    assert exc_info.value.code == 2
    assert not output.exists()


def test_operator_bundle_is_manual_authorized_and_out_of_band() -> None:
    path = (
        ROOT
        / "src/gas_calibrator/tools/run_v1_5_authoritative_resume_offline_state_advance_next_step_operator_bundle.py"
    )
    entry = classify_v1_5_entrypoint(path, root=ROOT)
    assert entry.category == "full_flow_orchestration"
    assert entry.formal_status == "manual_authorized_single_step_resume_only"
    assert entry.risk_level == "real_com_or_route_or_write_risk"
    assert entry.opens_com_ports is True
    assert entry.controls_routes is True
    assert (
        AUTHORITATIVE_RESUME_OFFLINE_STATE_ADVANCE_NEXT_STEP_OPERATOR_BUNDLE_MODULE
        in OUT_OF_BAND_OFFLINE_STATE_ADVANCE_MODULES
    )
