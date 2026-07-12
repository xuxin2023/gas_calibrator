import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

import gas_calibrator.tools.run_v1_5_authoritative_resume_offline_state_advance_next_step_blocked_executor as tool_module
import gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_next_step_blocked_executor as blocked_module
from gas_calibrator.v1_5.orchestration.full_flow import build_full_flow_plan
from gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight import (
    READY_STATUS as AUTHORIZATION_READY_STATUS,
    SCHEMA as AUTHORIZATION_SCHEMA,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_next_step_blocked_executor import (
    BLOCKED_READY_STATUS,
    REVIEW_STATUS,
    build_v1_5_authoritative_resume_offline_state_advance_next_step_blocked_executor,
)
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_formal_flow_contract import (
    discover_current_v1_5_inventory,
    validate_v1_5_formal_flow_contract,
)

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 7, 12, 12, 0, tzinfo=UTC)
PREFLIGHT_NAME = (
    "v1_5_authoritative_resume_offline_state_advance_"
    "next_step_authorization_preflight.json"
)


def _write(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return path


def _preflight_payload(tmp_path: Path) -> dict:
    packet = _write(tmp_path / "authorization-packet.json", {"ready": True})
    plan = _write(tmp_path / "next-step-plan.json", {"ready": True})
    return {
        "schema": AUTHORIZATION_SCHEMA,
        "generated_at": "2026-07-12T11:59:00Z",
        "overall_status": AUTHORIZATION_READY_STATUS,
        "next_step_authorization_preflight_ready": True,
        "authorization_packet_validated_offline": True,
        "review_required_count": 0,
        "review_reasons": [],
        "next_step_plan_json": str(plan.absolute()),
        "next_step_plan_sha256": "a" * 64,
        "authorization_packet_json": str(packet.absolute()),
        "authorization_packet_sha256": "b" * 64,
        "authorization_id": "authorization-114",
        "authorization_expires_at": "2026-07-12T12:15:00Z",
        "consumer_readiness_json": "consumer.json",
        "consumer_readiness_sha256": "c" * 64,
        "run_id": "run-114",
        "attempt_id": "attempt-114",
        "verified_step_id": "temperature_channel_fast_review",
        "next_step_id": "co2_open_flow_sampling",
        "next_step_tool_module": "gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue",
        "plan_review_allowed": True,
        "execution_supported": False,
        "next_step_execution_allowed": False,
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
        "checks": [],
        "next_action": "Review only.",
    }


def _fixture(tmp_path: Path, monkeypatch) -> tuple[Path, dict]:
    preflight = _preflight_payload(tmp_path)
    frozen = dict(preflight)
    path = _write(tmp_path / "preflight" / PREFLIGHT_NAME, preflight)
    monkeypatch.setattr(
        blocked_module,
        "build_v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight",
        lambda **_kwargs: dict(frozen),
    )
    return path, preflight


def test_blocked_executor_accepts_review_but_never_unlocks_execution(
    tmp_path: Path, monkeypatch
) -> None:
    path, _preflight = _fixture(tmp_path, monkeypatch)
    model = build_v1_5_authoritative_resume_offline_state_advance_next_step_blocked_executor(
        next_step_authorization_preflight_json=path,
        now=NOW,
    )
    assert model["overall_status"] == BLOCKED_READY_STATUS
    assert model["blocked_executor_ready"] is True
    assert model["plan_review_allowed"] is True
    assert model["execution_supported"] is False
    assert model["next_step_execution_allowed"] is False
    assert model["execute_flag_allowed"] is False
    assert model["opens_com_ports"] is False
    assert model["controls_water_or_gas_routes"] is False
    assert model["writes_coefficients"] is False
    assert model["connects_postgresql"] is False


def test_blocked_executor_rejects_preflight_drift(
    tmp_path: Path, monkeypatch
) -> None:
    path, preflight = _fixture(tmp_path, monkeypatch)
    preflight["next_step_id"] = "h2o_open_flow_sampling"
    _write(path, preflight)
    model = build_v1_5_authoritative_resume_offline_state_advance_next_step_blocked_executor(
        next_step_authorization_preflight_json=path,
        now=NOW,
    )
    assert model["overall_status"] == REVIEW_STATUS
    assert "next_step_authorization_preflight_recompute_mismatch" in model[
        "review_reasons"
    ]
    assert model["next_step_execution_allowed"] is False


def test_blocked_executor_rejects_reparse_preflight_path(
    tmp_path: Path, monkeypatch
) -> None:
    path, _preflight = _fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(blocked_module, "_contains_reparse", lambda _path: True)
    model = build_v1_5_authoritative_resume_offline_state_advance_next_step_blocked_executor(
        next_step_authorization_preflight_json=path,
        now=NOW,
    )
    assert model["overall_status"] == REVIEW_STATUS
    assert "next_step_authorization_preflight_path_contains_reparse_point" in model[
        "review_reasons"
    ]
    assert model["next_step_execution_allowed"] is False


@pytest.mark.parametrize(
    "field",
    (
        "next_step_execution_allowed",
        "resume_execution_allowed",
        "opens_com_ports",
        "controls_pressure",
        "controls_water_or_gas_routes",
        "writes_coefficients",
        "connects_postgresql",
    ),
)
def test_blocked_executor_rejects_any_unlocked_boundary(
    tmp_path: Path, monkeypatch, field: str
) -> None:
    path, preflight = _fixture(tmp_path, monkeypatch)
    preflight[field] = True
    _write(path, preflight)
    model = build_v1_5_authoritative_resume_offline_state_advance_next_step_blocked_executor(
        next_step_authorization_preflight_json=path,
        now=NOW,
    )
    assert model["overall_status"] == REVIEW_STATUS
    assert f"next_step_authorization_boundary_invalid:{field}" in model[
        "review_reasons"
    ]
    assert model["next_step_execution_allowed"] is False


def test_cli_rejects_all_execution_unlock_arguments(
    tmp_path: Path, monkeypatch
) -> None:
    path, _preflight = _fixture(tmp_path, monkeypatch)
    model = build_v1_5_authoritative_resume_offline_state_advance_next_step_blocked_executor(
        next_step_authorization_preflight_json=path,
        now=NOW,
    )
    monkeypatch.setattr(
        tool_module,
        "build_v1_5_authoritative_resume_offline_state_advance_next_step_blocked_executor",
        lambda **_kwargs: dict(model),
    )
    output = tmp_path / "output"
    assert tool_module.main(
        [
            "--next-step-authorization-preflight-json",
            str(path),
            "--output-dir",
            str(output),
            "--fail-on-review-required",
        ]
    ) == 0
    payload = json.loads(
        (
            output
            / "v1_5_authoritative_resume_offline_state_advance_next_step_blocked_executor.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["next_step_execution_allowed"] is False

    for flag in (
        "--execute",
        "--execute-next-step",
        "--allow-real-com",
        "--allow-pressure-control",
        "--allow-route-control",
        "--allow-writes",
        "--allow-database-import",
    ):
        rejected = tmp_path / f"rejected-{flag.removeprefix('--')}"
        with pytest.raises(SystemExit) as exc:
            tool_module.main(
                [
                    "--next-step-authorization-preflight-json",
                    str(path),
                    "--output-dir",
                    str(rejected),
                    flag,
                ]
            )
        assert exc.value.code == 2
        assert not rejected.exists()


def test_blocked_executor_is_offline_and_forbidden_as_canonical_step(
    tmp_path: Path,
) -> None:
    tool_path = (
        ROOT
        / "src/gas_calibrator/tools/run_v1_5_authoritative_resume_offline_state_advance_next_step_blocked_executor.py"
    )
    entry = classify_v1_5_entrypoint(tool_path, root=ROOT)
    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False

    config = _write(tmp_path / "config.json", {})
    plan = build_full_flow_plan(
        config_path=config,
        output_dir=tmp_path / "flow",
        run_id="next-step-blocked-executor",
    )
    steps = list(plan.steps)
    index = next(
        index
        for index, step in enumerate(steps)
        if step.step_id == "temperature_channel_fast_review"
    )
    module = (
        "gas_calibrator.tools."
        "run_v1_5_authoritative_resume_offline_state_advance_next_step_blocked_executor"
    )
    steps[index] = replace(
        steps[index],
        step_id="offline_next_step_blocked_executor",
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
