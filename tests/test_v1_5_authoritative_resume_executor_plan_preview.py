import hashlib
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_authoritative_resume_executor_plan_preview import main
from gas_calibrator.v1_5.orchestration.full_flow import build_full_flow_plan, build_full_flow_state, write_full_flow_plan
from gas_calibrator.validation.v1_5_authoritative_resume_executor_plan_preview import BLOCKED_STATUS, READY_STATUS, build_v1_5_authoritative_resume_executor_plan_preview
from gas_calibrator.validation.v1_5_authoritative_resume_state_consumer_contract import build_v1_5_authoritative_resume_state_consumer_contract, write_v1_5_authoritative_resume_state_consumer_contract
from gas_calibrator.validation.v1_5_authoritative_resume_state_post_write_verification import READY_STATUS as VERIFY_READY, SCHEMA as VERIFY_SCHEMA
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint

ROOT = Path(__file__).resolve().parents[1]


def _write(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def _bundle(tmp_path: Path) -> tuple[Path, str]:
    config = _write(tmp_path / "config.json", {})
    root = tmp_path / "flow"
    plan = build_full_flow_plan(config_path=config, output_dir=root, run_id="resume-98")
    plan_path = write_full_flow_plan(plan, root)["json"]
    completed = [step.step_id for step in plan.steps[:4]]
    state = build_full_flow_state(plan, completed_steps=completed)
    state_path = _write(root / "v1_5_full_flow_state.json", state.to_json())
    state_sha = hashlib.sha256(state_path.read_bytes()).hexdigest()
    verification = _write(root / "verify.json", {"schema": VERIFY_SCHEMA, "overall_status": VERIFY_READY, "post_write_verification_ready": True, "authoritative_state_json": str(state_path.resolve()), "authoritative_state_sha256": state_sha})
    contract_model = build_v1_5_authoritative_resume_state_consumer_contract(full_flow_plan_json=plan_path, post_write_verification_json=verification)
    contract = write_v1_5_authoritative_resume_state_consumer_contract(contract_model, root / "contract")
    return contract, plan.steps[4].step_id


def test_plan_preview_exposes_next_step_without_execution(tmp_path: Path) -> None:
    contract, next_step = _bundle(tmp_path)
    model = build_v1_5_authoritative_resume_executor_plan_preview(consumer_contract_json=contract)
    assert model["overall_status"] == READY_STATUS
    assert model["next_step_id"] == next_step
    assert model["execution_supported"] is False
    assert model["resume_execution_allowed"] is False
    assert model["would_execute"] is False


def test_plan_preview_blocks_tampered_consumer_contract(tmp_path: Path) -> None:
    contract, _next_step = _bundle(tmp_path)
    payload = json.loads(contract.read_text(encoding="utf-8"))
    payload["next_step_id"] = "co2_open_flow_sampling"
    _write(contract, payload)
    model = build_v1_5_authoritative_resume_executor_plan_preview(consumer_contract_json=contract)
    assert model["overall_status"] == BLOCKED_STATUS
    assert any(reason.startswith("consumer_contract_recompute_mismatch") for reason in model["blocker_reasons"])


def test_plan_preview_cli_and_entrypoint_remain_offline(tmp_path: Path) -> None:
    contract, _next_step = _bundle(tmp_path)
    out = tmp_path / "out"
    assert main(["--consumer-contract-json", str(contract), "--output-dir", str(out), "--fail-on-blocker"]) == 0
    entry = classify_v1_5_entrypoint(ROOT / "src/gas_calibrator/tools/export_v1_5_authoritative_resume_executor_plan_preview.py", root=ROOT)
    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.opens_com_ports is False
