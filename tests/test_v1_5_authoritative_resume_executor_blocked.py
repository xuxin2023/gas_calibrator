import hashlib
import json
from pathlib import Path

import pytest

from gas_calibrator.tools.run_v1_5_authoritative_resume_executor_blocked import main
from gas_calibrator.v1_5.orchestration.full_flow import (
    build_full_flow_plan,
    build_full_flow_state,
    write_full_flow_plan,
)
from gas_calibrator.validation.v1_5_authoritative_resume_executor_blocked import (
    BLOCKED_STATUS,
    REVIEW_STATUS,
    build_v1_5_authoritative_resume_executor_blocked,
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


def _write(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return path


def _preview_bundle(tmp_path: Path) -> Path:
    config = _write(tmp_path / "config.json", {})
    root = tmp_path / "flow"
    plan = build_full_flow_plan(config_path=config, output_dir=root, run_id="resume-99")
    plan_path = write_full_flow_plan(plan, root)["json"]
    completed = [step.step_id for step in plan.steps[:4]]
    state = build_full_flow_state(plan, completed_steps=completed)
    state_path = _write(root / "v1_5_full_flow_state.json", state.to_json())
    verification = _write(
        root / "verify.json",
        {
            "schema": VERIFY_SCHEMA,
            "overall_status": VERIFY_READY,
            "post_write_verification_ready": True,
            "authoritative_state_json": str(state_path.resolve()),
            "authoritative_state_sha256": hashlib.sha256(state_path.read_bytes()).hexdigest(),
        },
    )
    contract_model = build_v1_5_authoritative_resume_state_consumer_contract(
        full_flow_plan_json=plan_path,
        post_write_verification_json=verification,
    )
    contract = write_v1_5_authoritative_resume_state_consumer_contract(
        contract_model, root / "contract"
    )
    preview_model = build_v1_5_authoritative_resume_executor_plan_preview(
        consumer_contract_json=contract
    )
    return write_v1_5_authoritative_resume_executor_plan_preview(
        preview_model, root / "preview"
    )


def test_blocked_executor_recomputes_preview_and_keeps_all_actions_locked(
    tmp_path: Path,
) -> None:
    preview = _preview_bundle(tmp_path)
    model = build_v1_5_authoritative_resume_executor_blocked(
        resume_executor_plan_preview_json=preview
    )
    assert model["overall_status"] == BLOCKED_STATUS
    assert model["blocked_executor_ready"] is True
    assert model["execution_supported"] is False
    assert model["resume_execution_allowed"] is False
    assert model["does_not_execute_commands"] is True
    assert model["opens_com_ports"] is False
    assert model["controls_pressure"] is False
    assert model["controls_water_or_gas_routes"] is False
    assert model["writes_coefficients"] is False
    assert model["connects_postgresql"] is False


def test_blocked_executor_rejects_tampered_plan_preview(tmp_path: Path) -> None:
    preview = _preview_bundle(tmp_path)
    payload = json.loads(preview.read_text(encoding="utf-8"))
    payload["next_step_id"] = "co2_open_flow_sampling"
    _write(preview, payload)
    model = build_v1_5_authoritative_resume_executor_blocked(
        resume_executor_plan_preview_json=preview
    )
    assert model["overall_status"] == REVIEW_STATUS
    assert model["blocked_executor_ready"] is False
    assert "resume_executor_plan_preview_recompute_mismatch:next_step_id" in model[
        "review_reasons"
    ]


def test_blocked_executor_cli_writes_lock_evidence_and_is_offline(tmp_path: Path) -> None:
    preview = _preview_bundle(tmp_path)
    output = tmp_path / "blocked"
    assert (
        main(
            [
                "--resume-executor-plan-preview-json",
                str(preview),
                "--output-dir",
                str(output),
                "--fail-on-review-required",
            ]
        )
        == 0
    )
    assert (output / "v1_5_authoritative_resume_executor_blocked.json").is_file()
    entry = classify_v1_5_entrypoint(
        ROOT
        / "src/gas_calibrator/tools/run_v1_5_authoritative_resume_executor_blocked.py",
        root=ROOT,
    )
    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False


@pytest.mark.parametrize(
    "flag",
    [
        "--execute",
        "--resume",
        "--execute-read-only-real-com",
        "--allow-real-com",
        "--allow-pressure-control",
        "--allow-route-control",
        "--allow-writes",
        "--allow-database-import",
    ],
)
def test_blocked_executor_cli_rejects_execution_unlocks_before_writing(
    tmp_path: Path, flag: str
) -> None:
    preview = _preview_bundle(tmp_path)
    output = tmp_path / f"rejected-{flag.removeprefix('--')}"
    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "--resume-executor-plan-preview-json",
                str(preview),
                "--output-dir",
                str(output),
                flag,
            ]
        )
    assert exc_info.value.code == 2
    assert not output.exists()
