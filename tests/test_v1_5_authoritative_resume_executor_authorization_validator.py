import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from gas_calibrator.tools.export_v1_5_authoritative_resume_executor_authorization_validator import (
    main,
)
from gas_calibrator.v1_5.orchestration.full_flow import (
    build_full_flow_plan,
    build_full_flow_state,
    write_full_flow_plan,
)
from gas_calibrator.validation.v1_5_authoritative_resume_executor_authorization_validator import (
    READY_STATUS,
    REVIEW_STATUS,
    build_v1_5_authoritative_resume_executor_authorization_validator,
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


def _bundle(tmp_path: Path) -> dict[str, Path | dict]:
    config = _write(tmp_path / "config.json", {})
    root = tmp_path / "flow"
    plan_model = build_full_flow_plan(config_path=config, output_dir=root, run_id="resume-101")
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
    authorization = {
        "schema": FUTURE_AUTHORIZATION_SCHEMA,
        "authorization_id": "resume-auth-101",
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
        "run_id": "resume-101",
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
    }
    authorization_path = _write(root / "authorization.json", authorization)
    return {
        "design": design,
        "authorization": authorization_path,
        "authorization_payload": authorization,
    }


def test_authorization_validator_accepts_exact_short_lived_least_privilege_packet(
    tmp_path: Path,
) -> None:
    bundle = _bundle(tmp_path)
    model = build_v1_5_authoritative_resume_executor_authorization_validator(
        controlled_design_json=bundle["design"],
        authorization_packet_json=bundle["authorization"],
        now=NOW,
    )
    assert model["overall_status"] == READY_STATUS
    assert model["resume_executor_authorization_validated_offline"] is True
    assert model["execution_supported"] is False
    assert model["resume_execution_allowed"] is False
    assert model["would_execute"] is False
    assert model["opens_com_ports"] is False
    assert model["controls_pressure"] is False
    assert model["controls_water_or_gas_routes"] is False
    assert model["writes_coefficients"] is False
    assert model["connects_postgresql"] is False


def test_authorization_validator_rejects_expired_packet(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    model = build_v1_5_authoritative_resume_executor_authorization_validator(
        controlled_design_json=bundle["design"],
        authorization_packet_json=bundle["authorization"],
        now=datetime(2026, 7, 12, 12, 30, tzinfo=UTC),
    )
    assert model["overall_status"] == REVIEW_STATUS
    assert "authorization_expired" in model["review_reasons"]


def test_authorization_validator_rejects_tampered_state_binding(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    payload = dict(bundle["authorization_payload"])
    payload["authoritative_state_sha256"] = "0" * 64
    _write(bundle["authorization"], payload)
    model = build_v1_5_authoritative_resume_executor_authorization_validator(
        controlled_design_json=bundle["design"],
        authorization_packet_json=bundle["authorization"],
        now=NOW,
    )
    assert model["overall_status"] == REVIEW_STATUS
    assert (
        "authorization_sha256_mismatch:authoritative_state_sha256"
        in model["review_reasons"]
    )


def test_authorization_validator_rejects_unneeded_capability(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    payload = dict(bundle["authorization_payload"])
    payload["allow_postgresql_import"] = True
    _write(bundle["authorization"], payload)
    model = build_v1_5_authoritative_resume_executor_authorization_validator(
        controlled_design_json=bundle["design"],
        authorization_packet_json=bundle["authorization"],
        now=NOW,
    )
    assert model["overall_status"] == REVIEW_STATUS
    assert (
        "authorization_capability_mismatch:allow_postgresql_import"
        in model["review_reasons"]
    )


def test_authorization_validator_cli_and_entrypoint_remain_offline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _bundle(tmp_path)
    monkeypatch.setattr(
        "gas_calibrator.validation.v1_5_authoritative_resume_executor_authorization_validator._now",
        lambda: NOW,
    )
    output = tmp_path / "validation"
    assert (
        main(
            [
                "--controlled-design-json",
                str(bundle["design"]),
                "--authorization-packet-json",
                str(bundle["authorization"]),
                "--output-dir",
                str(output),
                "--fail-on-review-required",
            ]
        )
        == 0
    )
    assert (output / "v1_5_resume_executor_authorization_validation.json").is_file()
    entry = classify_v1_5_entrypoint(
        ROOT
        / "src/gas_calibrator/tools/export_v1_5_authoritative_resume_executor_authorization_validator.py",
        root=ROOT,
    )
    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False


@pytest.mark.parametrize("flag", ["--execute", "--resume", "--allow-real-com"])
def test_authorization_validator_cli_rejects_runtime_unlocks_before_output(
    tmp_path: Path, flag: str
) -> None:
    bundle = _bundle(tmp_path)
    output = tmp_path / flag.removeprefix("--")
    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "--controlled-design-json",
                str(bundle["design"]),
                "--authorization-packet-json",
                str(bundle["authorization"]),
                "--output-dir",
                str(output),
                flag,
            ]
        )
    assert exc_info.value.code == 2
    assert not output.exists()
