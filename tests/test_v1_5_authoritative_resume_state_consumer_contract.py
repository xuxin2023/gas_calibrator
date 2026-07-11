import hashlib
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_authoritative_resume_state_consumer_contract import main
from gas_calibrator.v1_5.orchestration.full_flow import (
    build_full_flow_plan,
    build_full_flow_state,
    write_full_flow_plan,
)
from gas_calibrator.validation.v1_5_authoritative_resume_state_consumer_contract import (
    BLOCKED_STATUS,
    READY_STATUS,
    build_v1_5_authoritative_resume_state_consumer_contract,
)
from gas_calibrator.validation.v1_5_authoritative_resume_state_post_write_verification import (
    READY_STATUS as VERIFICATION_READY_STATUS,
    SCHEMA as VERIFICATION_SCHEMA,
)
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint

ROOT = Path(__file__).resolve().parents[1]


def _write(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _bundle(tmp_path: Path) -> tuple[Path, Path, Path, list[str]]:
    config = _write(tmp_path / "config.json", {})
    root = tmp_path / "flow"
    plan = build_full_flow_plan(config_path=config, output_dir=root, run_id="resume-97")
    plan_path = write_full_flow_plan(plan, root)["json"]
    step_ids = [step.step_id for step in plan.steps]
    completed = step_ids[:4]
    state = build_full_flow_state(plan, completed_steps=completed)
    state_path = _write(root / "v1_5_full_flow_state.json", state.to_json())
    verification = _write(
        root / "post_write" / "v1_5_resume_state_post_write_verification.json",
        {
            "schema": VERIFICATION_SCHEMA,
            "overall_status": VERIFICATION_READY_STATUS,
            "post_write_verification_ready": True,
            "authoritative_state_json": str(state_path.resolve()),
            "authoritative_state_sha256": _sha(state_path),
        },
    )
    return plan_path, state_path, verification, step_ids


def test_consumer_contract_accepts_exact_locked_contiguous_state(tmp_path: Path) -> None:
    plan, _state, verification, steps = _bundle(tmp_path)
    model = build_v1_5_authoritative_resume_state_consumer_contract(
        full_flow_plan_json=plan, post_write_verification_json=verification
    )
    assert model["overall_status"] == READY_STATUS
    assert model["next_step_id"] == steps[4]
    assert model["execution_supported"] is False
    assert model["resume_execution_allowed"] is False
    assert model["would_execute"] is False


def test_consumer_contract_blocks_noncontiguous_completed_prefix(tmp_path: Path) -> None:
    plan, state, verification, steps = _bundle(tmp_path)
    payload = json.loads(state.read_text(encoding="utf-8"))
    payload["completed_step_ids"] = [steps[0], steps[2]]
    payload["current_step_id"] = steps[3]
    _write(state, payload)
    verification_payload = json.loads(verification.read_text(encoding="utf-8"))
    verification_payload["authoritative_state_sha256"] = _sha(state)
    _write(verification, verification_payload)
    model = build_v1_5_authoritative_resume_state_consumer_contract(
        full_flow_plan_json=plan, post_write_verification_json=verification
    )
    assert model["overall_status"] == BLOCKED_STATUS
    assert "completed_steps_not_exact_contiguous_prefix" in model["blocker_reasons"]


def test_consumer_contract_blocks_unlocked_state(tmp_path: Path) -> None:
    plan, state, verification, _steps = _bundle(tmp_path)
    payload = json.loads(state.read_text(encoding="utf-8"))
    payload["allow_real_com"] = True
    _write(state, payload)
    verification_payload = json.loads(verification.read_text(encoding="utf-8"))
    verification_payload["authoritative_state_sha256"] = _sha(state)
    _write(verification, verification_payload)
    model = build_v1_5_authoritative_resume_state_consumer_contract(
        full_flow_plan_json=plan, post_write_verification_json=verification
    )
    assert "authoritative_state_allow_real_com_not_false" in model["blocker_reasons"]


def test_consumer_contract_cli_exports_only_offline_contract(tmp_path: Path) -> None:
    plan, _state, verification, _steps = _bundle(tmp_path)
    out = tmp_path / "out"
    assert main(["--full-flow-plan-json", str(plan), "--post-write-verification-json", str(verification), "--output-dir", str(out), "--fail-on-blocker"]) == 0
    payload = json.loads((out / "v1_5_resume_state_consumer_contract.json").read_text(encoding="utf-8"))
    assert payload["would_execute"] is False


def test_consumer_contract_entrypoint_is_offline_support() -> None:
    entry = classify_v1_5_entrypoint(
        ROOT / "src/gas_calibrator/tools/export_v1_5_authoritative_resume_state_consumer_contract.py",
        root=ROOT,
    )
    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
