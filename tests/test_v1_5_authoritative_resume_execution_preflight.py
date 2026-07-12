import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from gas_calibrator.tools.export_v1_5_authoritative_resume_execution_preflight import main
from gas_calibrator.v1_5.orchestration.full_flow import (
    build_full_flow_plan,
    build_full_flow_state,
    write_full_flow_plan,
)
from gas_calibrator.validation.v1_5_authoritative_resume_execution_preflight import (
    READY_STATUS,
    REVIEW_STATUS,
    build_v1_5_authoritative_resume_execution_preflight,
)
from gas_calibrator.validation.v1_5_authoritative_resume_executor_authorization_validator import (
    build_v1_5_authoritative_resume_executor_authorization_validator,
    write_v1_5_authoritative_resume_executor_authorization_validator,
)
from gas_calibrator.validation.v1_5_authoritative_resume_executor_blocked import (
    build_v1_5_authoritative_resume_executor_blocked,
    write_v1_5_authoritative_resume_executor_blocked,
)
from gas_calibrator.validation.v1_5_authoritative_resume_executor_controlled_design import (
    FUTURE_AUTHORIZATION_SCHEMA,
    build_v1_5_authoritative_resume_executor_controlled_design,
    write_v1_5_authoritative_resume_executor_controlled_design,
)
from gas_calibrator.validation.v1_5_authoritative_resume_executor_plan_preview import (
    build_v1_5_authoritative_resume_executor_plan_preview,
    write_v1_5_authoritative_resume_executor_plan_preview,
)
from gas_calibrator.validation.v1_5_authoritative_resume_state_consumer_contract import (
    build_v1_5_authoritative_resume_state_consumer_contract,
    write_v1_5_authoritative_resume_state_consumer_contract,
)
from gas_calibrator.validation.v1_5_authoritative_resume_state_post_write_verification import (
    READY_STATUS as VERIFY_READY,
    SCHEMA as VERIFY_SCHEMA,
)
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 7, 12, 12, 0, tzinfo=UTC)


def _write(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return path


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _bundle(tmp_path: Path) -> dict[str, Path]:
    config = _write(tmp_path / "config.json", {})
    root = tmp_path / "flow"
    plan_model = build_full_flow_plan(config_path=config, output_dir=root, run_id="resume-102")
    plan = write_full_flow_plan(plan_model, root)["json"]
    completed = [step.step_id for step in plan_model.steps[:4]]
    state_model = build_full_flow_state(plan_model, completed_steps=completed)
    state = _write(root / "v1_5_full_flow_state.json", state_model.to_json())
    verification = _write(
        root / "verify.json",
        {
            "schema": VERIFY_SCHEMA,
            "overall_status": VERIFY_READY,
            "post_write_verification_ready": True,
            "authoritative_state_json": str(state.resolve()),
            "authoritative_state_sha256": _sha(state),
        },
    )
    contract_model = build_v1_5_authoritative_resume_state_consumer_contract(
        full_flow_plan_json=plan,
        post_write_verification_json=verification,
    )
    contract = write_v1_5_authoritative_resume_state_consumer_contract(
        contract_model, root / "contract"
    )
    preview_model = build_v1_5_authoritative_resume_executor_plan_preview(
        consumer_contract_json=contract
    )
    preview = write_v1_5_authoritative_resume_executor_plan_preview(
        preview_model, root / "preview"
    )
    blocked_model = build_v1_5_authoritative_resume_executor_blocked(
        resume_executor_plan_preview_json=preview
    )
    blocked = write_v1_5_authoritative_resume_executor_blocked(
        blocked_model, root / "blocked"
    )["json"]
    design_model = build_v1_5_authoritative_resume_executor_controlled_design(
        authoritative_resume_executor_blocked_json=blocked
    )
    design = write_v1_5_authoritative_resume_executor_controlled_design(
        design_model, root / "design"
    )["manifest"]
    requirements = blocked_model["authorization_requirements_recorded_only"]
    authorization = _write(
        root / "authorization.json",
        {
            "schema": FUTURE_AUTHORIZATION_SCHEMA,
            "authorization_id": "resume-auth-102",
            "operator": "operator-a",
            "reviewer": "reviewer-b",
            "approver": "approver-c",
            "issued_at": "2026-07-12T11:55:00Z",
            "expires_at": "2026-07-12T12:25:00Z",
            "controlled_design_json": str(design.resolve()),
            "controlled_design_sha256": _sha(design),
            "blocked_executor_json": str(blocked.resolve()),
            "blocked_executor_sha256": _sha(blocked),
            "consumer_contract_json": str(contract.resolve()),
            "consumer_contract_sha256": _sha(contract),
            "full_flow_plan_json": str(plan.resolve()),
            "full_flow_plan_sha256": _sha(plan),
            "authoritative_state_json": str(state.resolve()),
            "authoritative_state_sha256": _sha(state),
            "run_id": "resume-102",
            "next_step_id": blocked_model["next_step_id_recorded_only"],
            "next_step_command_sha256": design_model["manifest"][
                "next_step_command_sha256_recorded_only"
            ],
            "structured_confirmation": {
                "resume_only": True,
                "no_implicit_writes": True,
                "no_database_import": True,
                "no_unrelated_permissions": True,
            },
            "allow_real_com": bool(requirements["real_com"]),
            "allow_pressure_control": bool(requirements["pressure"]),
            "allow_route_control": bool(requirements["route"]),
            "allow_device_or_coefficient_write": bool(requirements["write"]),
            "allow_postgresql_import": False,
        },
    )
    validation_model = build_v1_5_authoritative_resume_executor_authorization_validator(
        controlled_design_json=design,
        authorization_packet_json=authorization,
        now=NOW,
    )
    validation = write_v1_5_authoritative_resume_executor_authorization_validator(
        validation_model, root / "validation"
    )["json"]
    return {"validation": validation, "state": state, "authorization": authorization}


def test_preflight_revalidates_current_chain_and_records_attempt_without_execution(
    tmp_path: Path,
) -> None:
    bundle = _bundle(tmp_path)
    model = build_v1_5_authoritative_resume_execution_preflight(
        authorization_validation_json=bundle["validation"],
        now=NOW + timedelta(minutes=1),
    )
    assert model["overall_status"] == READY_STATUS
    assert model["resume_execution_preflight_ready"] is True
    assert model["attempt_id"].startswith("resume-attempt-")
    assert model["authorization_seconds_remaining"] == 24 * 60
    assert model["next_step_command_recorded_only"]
    assert model["execution_supported"] is False
    assert model["resume_execution_allowed"] is False
    assert model["would_execute"] is False
    assert model["opens_com_ports"] is False
    assert model["controls_pressure"] is False
    assert model["controls_water_or_gas_routes"] is False
    assert model["writes_coefficients"] is False
    assert model["connects_postgresql"] is False


def test_preflight_rejects_state_changed_after_authorization_validation(
    tmp_path: Path,
) -> None:
    bundle = _bundle(tmp_path)
    state_payload = json.loads(bundle["state"].read_text(encoding="utf-8"))
    state_payload["completed_step_ids"].append("tampered-step")
    _write(bundle["state"], state_payload)
    model = build_v1_5_authoritative_resume_execution_preflight(
        authorization_validation_json=bundle["validation"],
        now=NOW + timedelta(minutes=1),
    )
    assert model["overall_status"] == REVIEW_STATUS
    assert any(
        reason.startswith("authorization_revalidation:")
        for reason in model["review_reasons"]
    )


def test_preflight_rejects_authorization_expired_after_original_validation(
    tmp_path: Path,
) -> None:
    bundle = _bundle(tmp_path)
    model = build_v1_5_authoritative_resume_execution_preflight(
        authorization_validation_json=bundle["validation"],
        now=NOW + timedelta(minutes=30),
    )
    assert model["overall_status"] == REVIEW_STATUS
    assert "authorization_revalidation:authorization_expired" in model["review_reasons"]


def test_preflight_rejects_tampered_validation_next_step(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    payload = json.loads(bundle["validation"].read_text(encoding="utf-8"))
    payload["next_step_id"] = "co2_open_flow_sampling"
    _write(bundle["validation"], payload)
    model = build_v1_5_authoritative_resume_execution_preflight(
        authorization_validation_json=bundle["validation"],
        now=NOW + timedelta(minutes=1),
    )
    assert model["overall_status"] == REVIEW_STATUS
    assert "authorization_validation_recompute_mismatch:next_step_id" in model[
        "review_reasons"
    ]


def test_preflight_cli_and_entrypoint_remain_offline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _bundle(tmp_path)
    monkeypatch.setattr(
        "gas_calibrator.validation.v1_5_authoritative_resume_execution_preflight._now",
        lambda: NOW + timedelta(minutes=1),
    )
    output = tmp_path / "preflight"
    assert (
        main(
            [
                "--authorization-validation-json",
                str(bundle["validation"]),
                "--output-dir",
                str(output),
                "--fail-on-review-required",
            ]
        )
        == 0
    )
    assert (output / "v1_5_authoritative_resume_execution_preflight.json").is_file()
    entry = classify_v1_5_entrypoint(
        ROOT
        / "src/gas_calibrator/tools/export_v1_5_authoritative_resume_execution_preflight.py",
        root=ROOT,
    )
    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False


@pytest.mark.parametrize("flag", ["--execute", "--resume", "--allow-real-com"])
def test_preflight_cli_rejects_runtime_unlocks_before_output(
    tmp_path: Path, flag: str
) -> None:
    bundle = _bundle(tmp_path)
    output = tmp_path / flag.removeprefix("--")
    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "--authorization-validation-json",
                str(bundle["validation"]),
                "--output-dir",
                str(output),
                flag,
            ]
        )
    assert exc_info.value.code == 2
    assert not output.exists()
