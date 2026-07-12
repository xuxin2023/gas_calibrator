import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

import gas_calibrator.tools.export_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design as tool_module
import gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design as design_module
from gas_calibrator.v1_5.orchestration.full_flow import build_full_flow_plan
from gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_next_step_blocked_executor import (
    BLOCKED_READY_STATUS,
    SCHEMA as BLOCKED_SCHEMA,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design import (
    FUTURE_AUTHORIZATION_SCHEMA,
    READY_STATUS,
    REVIEW_STATUS,
    build_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design,
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
BLOCKED_NAME = (
    "v1_5_authoritative_resume_offline_state_advance_"
    "next_step_blocked_executor.json"
)


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
    module = "gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue"
    command = [
        "python",
        "-m",
        module,
        "--config",
        str(
            tmp_path
            / "coefficient_epoch_0_getco_snapshot"
            / "runtime_identity_bound_config.json"
        ),
        "--temperature-order",
        "40,30,20,10,0,-10,-20",
    ]
    plan = {
        "schema": PLAN_SCHEMA,
        "overall_status": PLAN_READY_STATUS,
        "next_step_plan_review_ready": True,
        "next_step_id": "co2_open_flow_sampling",
        "next_step_tool_module": module,
        "next_step_command": command,
        "requires_real_com_authorization": True,
        "requires_pressure_authorization": False,
        "requires_route_authorization": True,
        "requires_write_authorization": False,
    }
    plan_path = _write(tmp_path / "plan.json", plan)
    auth_preflight = _write(tmp_path / "authorization-preflight.json", {"ready": True})
    blocked = {
        "schema": BLOCKED_SCHEMA,
        "generated_at": "2026-07-12T11:59:00Z",
        "overall_status": BLOCKED_READY_STATUS,
        "blocked_executor_ready": True,
        "review_required_count": 0,
        "review_reasons": [],
        "next_step_authorization_preflight_json": str(auth_preflight.absolute()),
        "next_step_authorization_preflight_sha256": _sha(auth_preflight),
        "authorization_packet_json": "authorization.json",
        "authorization_packet_sha256": "a" * 64,
        "authorization_id": "authorization-115",
        "authorization_expires_at": "2026-07-12T12:15:00Z",
        "next_step_plan_json": str(plan_path.absolute()),
        "next_step_plan_sha256": _sha(plan_path),
        "run_id": "run-115",
        "attempt_id": "attempt-115",
        "verified_step_id": "temperature_channel_fast_review",
        "next_step_id": "co2_open_flow_sampling",
        "next_step_tool_module": module,
        "future_executor_must_recompute_authorization": True,
        "plan_review_allowed": True,
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
        "checks": [],
        "next_action": "Keep blocked.",
    }
    frozen = dict(blocked)
    blocked_path = _write(tmp_path / "blocked" / BLOCKED_NAME, blocked)
    monkeypatch.setattr(
        design_module,
        "build_v1_5_authoritative_resume_offline_state_advance_next_step_blocked_executor",
        lambda **_kwargs: dict(frozen),
    )
    return blocked_path, blocked, plan_path, plan


def test_controlled_design_freezes_exact_mature_command_and_holds(
    tmp_path: Path, monkeypatch
) -> None:
    blocked_path, _blocked, _plan_path, _plan = _fixture(tmp_path, monkeypatch)
    model = build_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design(
        next_step_blocked_executor_json=blocked_path,
        now=NOW,
    )
    manifest = model["manifest"]
    assert manifest["overall_status"] == READY_STATUS
    assert manifest["controlled_next_step_executor_design_ready"] is True
    assert manifest["future_authorization_schema"] == FUTURE_AUTHORIZATION_SCHEMA
    assert manifest["next_step_id_recorded_only"] == "co2_open_flow_sampling"
    assert manifest["single_exact_command_only"] is True
    assert manifest["shell_execution_allowed"] is False
    assert manifest["automatic_retry_allowed"] is False
    assert manifest["fallback_entry_allowed"] is False
    assert manifest["automatic_state_advance_allowed"] is False
    assert manifest["execution_supported"] is False
    assert manifest["next_step_execution_allowed"] is False
    triggers = {row["trigger"] for row in model["hold_contract"]}
    assert "pace_vent_pressure_dewpoint_ratio_qc_or_device_gate_failure" in triggers
    assert "partial_side_effect_or_operator_abort" in triggers
    assert {row["artifact"] for row in model["evidence_contract"]} >= {
        "executor_invocation.json",
        "pre_execution_revalidation.json",
        "hold_events.csv",
        "post_execution_evidence_index.json",
    }


def test_controlled_design_rejects_blocked_or_plan_drift(
    tmp_path: Path, monkeypatch
) -> None:
    blocked_path, blocked, plan_path, plan = _fixture(tmp_path, monkeypatch)
    blocked["next_step_id"] = "h2o_open_flow_sampling"
    _write(blocked_path, blocked)
    plan["next_step_tool_module"] = "gas_calibrator.tools.run_v1_migrated_co2_queue"
    _write(plan_path, plan)
    model = build_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design(
        next_step_blocked_executor_json=blocked_path,
        now=NOW,
    )
    manifest = model["manifest"]
    assert manifest["overall_status"] == REVIEW_STATUS
    assert "next_step_blocked_executor_recompute_mismatch" in manifest[
        "review_reasons"
    ]
    assert "next_step_plan_sha256_mismatch" in manifest["review_reasons"]
    assert manifest["next_step_execution_allowed"] is False


def test_capability_contract_is_exact_and_database_is_separate(
    tmp_path: Path, monkeypatch
) -> None:
    blocked_path, _blocked, _plan_path, _plan = _fixture(tmp_path, monkeypatch)
    model = build_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design(
        next_step_blocked_executor_json=blocked_path,
        now=NOW,
    )
    by_name = {row["capability"]: row for row in model["capability_contract"]}
    assert by_name["real_com"]["required_by_exact_next_step"] is True
    assert by_name["route_control"]["required_by_exact_next_step"] is True
    assert by_name["pressure_control"]["required_by_exact_next_step"] is False
    assert by_name["device_or_coefficient_write"]["required_by_exact_next_step"] is False
    assert by_name["postgresql_import"]["required_by_exact_next_step"] is False
    assert all(row["default"] is False for row in model["capability_contract"])


def test_cli_inventory_and_formal_flow_remain_offline(
    tmp_path: Path, monkeypatch
) -> None:
    blocked_path, _blocked, _plan_path, _plan = _fixture(tmp_path, monkeypatch)
    model = build_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design(
        next_step_blocked_executor_json=blocked_path,
        now=NOW,
    )
    monkeypatch.setattr(
        tool_module,
        "build_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design",
        lambda **_kwargs: model,
    )
    output = tmp_path / "output"
    assert tool_module.main(
        [
            "--next-step-blocked-executor-json",
            str(blocked_path),
            "--output-dir",
            str(output),
            "--fail-on-review-required",
        ]
    ) == 0
    payload = json.loads(
        (
            output
            / "v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["next_step_execution_allowed"] is False
    for flag in ("--execute", "--allow-real-com", "--allow-route-control"):
        rejected = tmp_path / flag.removeprefix("--")
        with pytest.raises(SystemExit) as exc:
            tool_module.main(
                [
                    "--next-step-blocked-executor-json",
                    str(blocked_path),
                    "--output-dir",
                    str(rejected),
                    flag,
                ]
            )
        assert exc.value.code == 2
        assert not rejected.exists()

    tool_path = (
        ROOT
        / "src/gas_calibrator/tools/export_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design.py"
    )
    entry = classify_v1_5_entrypoint(tool_path, root=ROOT)
    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"

    config = _write(tmp_path / "config.json", {})
    plan = build_full_flow_plan(
        config_path=config,
        output_dir=tmp_path / "flow",
        run_id="next-step-controlled-design",
    )
    steps = list(plan.steps)
    index = next(
        index
        for index, step in enumerate(steps)
        if step.step_id == "temperature_channel_fast_review"
    )
    module = (
        "gas_calibrator.tools."
        "export_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design"
    )
    steps[index] = replace(
        steps[index],
        step_id="offline_next_step_controlled_design",
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
