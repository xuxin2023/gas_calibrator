import hashlib
import json
from dataclasses import replace
from pathlib import Path

import gas_calibrator.tools.export_v1_5_authoritative_resume_offline_state_advance_next_step_plan as tool_module
import gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_next_step_plan as plan_module
from gas_calibrator.tools.export_v1_5_authoritative_resume_offline_state_advance_next_step_plan import (
    main,
)
from gas_calibrator.v1_5.orchestration.full_flow import (
    build_full_flow_plan,
    build_full_flow_state,
    write_full_flow_plan,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_consumer_readiness import (
    READY_STATUS as CONSUMER_READY_STATUS,
    SCHEMA as CONSUMER_SCHEMA,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_next_step_plan import (
    BLOCKED_STATUS,
    READY_STATUS,
    build_v1_5_authoritative_resume_offline_state_advance_next_step_plan,
)
from gas_calibrator.validation.v1_5_entrypoint_inventory import (
    classify_v1_5_entrypoint,
)
from gas_calibrator.validation.v1_5_formal_flow_contract import (
    discover_current_v1_5_inventory,
    validate_v1_5_formal_flow_contract,
)

ROOT = Path(__file__).resolve().parents[1]
CONSUMER_NAME = (
    "v1_5_authoritative_resume_offline_state_advance_consumer_readiness.json"
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


def _bundle(
    tmp_path: Path,
    monkeypatch,
    *,
    route_module: str | None = None,
    extra_route_flags: tuple[str, ...] = (),
):
    root = tmp_path / "flow"
    config = _write(root / "config.json", {})
    plan = build_full_flow_plan(
        config_path=config,
        output_dir=root,
        run_id="next-step-plan-111",
    )
    if route_module or extra_route_flags:
        steps = list(plan.steps)
        index = next(
            index
            for index, step in enumerate(steps)
            if step.step_id == "co2_open_flow_sampling"
        )
        selected_module = route_module or str(steps[index].tool_module)
        command = tuple(
            selected_module if value == steps[index].tool_module else value
            for value in steps[index].command
        ) + extra_route_flags
        steps[index] = replace(
            steps[index],
            tool_module=selected_module,
            command=command,
        )
        plan = replace(plan, steps=tuple(steps))
    plan_path = write_full_flow_plan(plan, root)["json"]
    step_ids = [step.step_id for step in plan.steps]
    temperature_index = step_ids.index("temperature_channel_fast_review")
    completed = step_ids[: temperature_index + 1]
    state = build_full_flow_state(plan, completed_steps=completed).to_json()
    state_path = _write(root / "v1_5_full_flow_state.json", state)
    verification_path = _write(
        root
        / "post-write"
        / "v1_5_authoritative_resume_offline_state_advance_post_write_verification.json",
        {"schema": "test-verification"},
    )
    consumer = {
        "schema": CONSUMER_SCHEMA,
        "overall_status": CONSUMER_READY_STATUS,
        "resume_state_consumer_readiness_ready": True,
        "blocker_count": 0,
        "blocker_reasons": [],
        "post_write_verification_json": str(verification_path.absolute()),
        "post_write_verification_sha256": _sha(verification_path),
        "atomic_write_json": str((root / "writer.json").absolute()),
        "atomic_write_sha256": "a" * 64,
        "full_flow_plan_json": str(plan_path.absolute()),
        "full_flow_plan_sha256": _sha(plan_path),
        "authoritative_state_json": str(state_path.absolute()),
        "authoritative_state_sha256": _sha(state_path),
        "run_id": plan.run_id,
        "attempt_id": "attempt-111",
        "verified_step_id": "temperature_channel_fast_review",
        "completed_step_ids": completed,
        "next_step_id": "co2_open_flow_sampling",
        "state_consumption_allowed": True,
        "execution_supported": False,
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
    consumer_path = _write(root / "consumer" / CONSUMER_NAME, consumer)
    recomputed_consumer = dict(consumer)
    monkeypatch.setattr(
        plan_module,
        "build_v1_5_authoritative_resume_offline_state_advance_consumer_readiness",
        lambda **_kwargs: dict(recomputed_consumer),
    )
    return plan, consumer, consumer_path


def test_next_step_plan_previews_mature_co2_without_execution(
    tmp_path: Path, monkeypatch
) -> None:
    _plan, _consumer, consumer_path = _bundle(tmp_path, monkeypatch)
    model = build_v1_5_authoritative_resume_offline_state_advance_next_step_plan(
        consumer_readiness_json=consumer_path
    )
    assert model["overall_status"] == READY_STATUS
    assert model["next_step_plan_review_ready"] is True
    assert model["next_step_id"] == "co2_open_flow_sampling"
    assert model["next_step_tool_module"] == (
        "gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue"
    )
    assert model["mature_route_module_verified"] is True
    assert model["requires_real_com_authorization"] is True
    assert model["requires_route_authorization"] is True
    assert model["plan_consumption_allowed"] is True
    assert model["next_step_execution_allowed"] is False
    assert model["resume_execution_allowed"] is False
    assert model["would_execute"] is False
    assert model["opens_com_ports"] is False
    assert model["controls_water_or_gas_routes"] is False


def test_next_step_plan_blocks_execution_capable_consumer(
    tmp_path: Path, monkeypatch
) -> None:
    _plan, consumer, consumer_path = _bundle(tmp_path, monkeypatch)
    consumer["resume_execution_allowed"] = True
    _write(consumer_path, consumer)
    model = build_v1_5_authoritative_resume_offline_state_advance_next_step_plan(
        consumer_readiness_json=consumer_path
    )
    assert model["overall_status"] == BLOCKED_STATUS
    assert model["next_step_plan_review_ready"] is False
    assert "consumer_readiness_resume_execution_allowed_not_false" in model[
        "blocker_reasons"
    ]
    assert "consumer_readiness_recompute_mismatch" in model["blocker_reasons"]
    assert model["next_step_execution_allowed"] is False


def test_next_step_plan_blocks_non_mature_co2_module(
    tmp_path: Path, monkeypatch
) -> None:
    _plan, _consumer, consumer_path = _bundle(
        tmp_path,
        monkeypatch,
        route_module="gas_calibrator.tools.run_v1_migrated_co2_queue",
    )
    model = build_v1_5_authoritative_resume_offline_state_advance_next_step_plan(
        consumer_readiness_json=consumer_path
    )
    assert model["overall_status"] == BLOCKED_STATUS
    assert "mature_route_tool_module_mismatch:co2_open_flow_sampling" in model[
        "blocker_reasons"
    ]
    assert model["mature_route_module_verified"] is False


def test_next_step_plan_blocks_mature_module_with_forbidden_route_flag(
    tmp_path: Path, monkeypatch
) -> None:
    _plan, _consumer, consumer_path = _bundle(
        tmp_path,
        monkeypatch,
        extra_route_flags=("--skip-stability-gate",),
    )
    model = build_v1_5_authoritative_resume_offline_state_advance_next_step_plan(
        consumer_readiness_json=consumer_path
    )
    assert model["overall_status"] == BLOCKED_STATUS
    assert "mature_route_forbidden_flag:--skip-stability-gate" in model[
        "blocker_reasons"
    ]
    assert model["next_step_execution_allowed"] is False


def test_next_step_plan_cli_and_entrypoint_stay_offline(
    tmp_path: Path, monkeypatch
) -> None:
    _plan, _consumer, consumer_path = _bundle(tmp_path, monkeypatch)
    model = build_v1_5_authoritative_resume_offline_state_advance_next_step_plan(
        consumer_readiness_json=consumer_path
    )
    monkeypatch.setattr(
        tool_module,
        "build_v1_5_authoritative_resume_offline_state_advance_next_step_plan",
        lambda **_kwargs: dict(model),
    )
    output_dir = tmp_path / "output"
    assert (
        main(
            [
                "--consumer-readiness-json",
                str(consumer_path),
                "--output-dir",
                str(output_dir),
                "--fail-on-blocker",
            ]
        )
        == 0
    )
    payload = json.loads(
        (
            output_dir
            / "v1_5_authoritative_resume_offline_state_advance_next_step_plan.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["next_step_execution_allowed"] is False
    entry = classify_v1_5_entrypoint(
        ROOT
        / "src/gas_calibrator/tools/export_v1_5_authoritative_resume_offline_state_advance_next_step_plan.py",
        root=ROOT,
    )
    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False


def test_formal_flow_rejects_next_step_preview_as_canonical_step(
    tmp_path: Path,
) -> None:
    config = _write(tmp_path / "config.json", {})
    plan = build_full_flow_plan(
        config_path=config,
        output_dir=tmp_path / "flow",
        run_id="next-step-plan-contract",
    )
    steps = list(plan.steps)
    index = next(
        index
        for index, step in enumerate(steps)
        if step.step_id == "temperature_channel_fast_review"
    )
    module = (
        "gas_calibrator.tools."
        "export_v1_5_authoritative_resume_offline_state_advance_next_step_plan"
    )
    steps[index] = replace(
        steps[index],
        step_id="offline_state_advance_next_step_plan",
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
