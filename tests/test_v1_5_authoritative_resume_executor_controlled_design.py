import hashlib
import json
from pathlib import Path

import pytest

from gas_calibrator.tools.export_v1_5_authoritative_resume_executor_controlled_design import (
    main,
)
from gas_calibrator.v1_5.orchestration.full_flow import (
    build_full_flow_plan,
    build_full_flow_state,
    write_full_flow_plan,
)
from gas_calibrator.validation.v1_5_authoritative_resume_executor_blocked import (
    build_v1_5_authoritative_resume_executor_blocked,
    write_v1_5_authoritative_resume_executor_blocked,
)
from gas_calibrator.validation.v1_5_authoritative_resume_executor_controlled_design import (
    FUTURE_AUTHORIZATION_SCHEMA,
    READY_STATUS,
    REVIEW_STATUS,
    build_v1_5_authoritative_resume_executor_controlled_design,
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


def _blocked_bundle(tmp_path: Path) -> Path:
    config = _write(tmp_path / "config.json", {})
    root = tmp_path / "flow"
    plan = build_full_flow_plan(config_path=config, output_dir=root, run_id="resume-100")
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
    preview = write_v1_5_authoritative_resume_executor_plan_preview(
        preview_model, root / "preview"
    )
    blocked_model = build_v1_5_authoritative_resume_executor_blocked(
        resume_executor_plan_preview_json=preview
    )
    return write_v1_5_authoritative_resume_executor_blocked(
        blocked_model, root / "blocked"
    )["json"]


def test_controlled_design_binds_exact_blocked_evidence_and_remains_non_executable(
    tmp_path: Path,
) -> None:
    blocked = _blocked_bundle(tmp_path)
    model = build_v1_5_authoritative_resume_executor_controlled_design(
        authoritative_resume_executor_blocked_json=blocked
    )
    manifest = model["manifest"]
    assert manifest["overall_status"] == READY_STATUS
    assert manifest["controlled_resume_executor_design_ready"] is True
    assert manifest["future_authorization_schema"] == FUTURE_AUTHORIZATION_SCHEMA
    assert manifest["execution_supported"] is False
    assert manifest["resume_execution_allowed"] is False
    assert manifest["execute_flag_allowed"] is False
    assert manifest["opens_com_ports"] is False
    assert manifest["controls_pressure"] is False
    assert manifest["controls_water_or_gas_routes"] is False
    assert manifest["writes_coefficients"] is False
    assert manifest["connects_postgresql"] is False
    assert manifest["database_import_allowed"] is False
    assert {row["field"] for row in model["authorization_contract"]} >= {
        "operator_reviewer_approver",
        "issued_at_expires_at",
        "run_plan_state_binding",
        "next_step_binding",
    }


def test_controlled_design_rejects_tampered_blocked_evidence(tmp_path: Path) -> None:
    blocked = _blocked_bundle(tmp_path)
    payload = json.loads(blocked.read_text(encoding="utf-8"))
    payload["next_step_id_recorded_only"] = "co2_open_flow_sampling"
    _write(blocked, payload)
    model = build_v1_5_authoritative_resume_executor_controlled_design(
        authoritative_resume_executor_blocked_json=blocked
    )
    manifest = model["manifest"]
    assert manifest["overall_status"] == REVIEW_STATUS
    assert manifest["controlled_resume_executor_design_ready"] is False
    assert "blocked_executor_recompute_mismatch:next_step_id_recorded_only" in manifest[
        "review_reasons"
    ]


def test_capability_contract_is_least_privilege_and_database_stays_separate(
    tmp_path: Path,
) -> None:
    blocked = _blocked_bundle(tmp_path)
    model = build_v1_5_authoritative_resume_executor_controlled_design(
        authoritative_resume_executor_blocked_json=blocked
    )
    by_capability = {row["capability"]: row for row in model["capability_contract"]}
    assert by_capability["postgresql_import"]["required_by_canonical_next_step"] is False
    assert by_capability["postgresql_import"]["default"] is False
    assert all(row["default"] is False for row in model["capability_contract"])
    assert all("only when" in row["rule"] or row["capability"] == "postgresql_import" for row in model["capability_contract"])


def test_controlled_design_cli_and_entrypoint_remain_offline(tmp_path: Path) -> None:
    blocked = _blocked_bundle(tmp_path)
    output = tmp_path / "design"
    assert (
        main(
            [
                "--authoritative-resume-executor-blocked-json",
                str(blocked),
                "--output-dir",
                str(output),
                "--fail-on-review-required",
            ]
        )
        == 0
    )
    assert (output / "v1_5_authoritative_resume_executor_controlled_design.json").is_file()
    entry = classify_v1_5_entrypoint(
        ROOT
        / "src/gas_calibrator/tools/export_v1_5_authoritative_resume_executor_controlled_design.py",
        root=ROOT,
    )
    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False


@pytest.mark.parametrize(
    "flag",
    ["--execute", "--resume", "--allow-real-com", "--allow-pressure-control"],
)
def test_controlled_design_cli_rejects_runtime_unlocks_before_output(
    tmp_path: Path, flag: str
) -> None:
    blocked = _blocked_bundle(tmp_path)
    output = tmp_path / flag.removeprefix("--")
    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "--authoritative-resume-executor-blocked-json",
                str(blocked),
                "--output-dir",
                str(output),
                flag,
            ]
        )
    assert exc_info.value.code == 2
    assert not output.exists()
