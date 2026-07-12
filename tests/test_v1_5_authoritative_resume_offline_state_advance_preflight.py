import hashlib
import json
from pathlib import Path

import pytest

from gas_calibrator.tools import (
    export_v1_5_authoritative_resume_offline_state_advance_preflight as tool,
)
from gas_calibrator.v1_5.orchestration.full_flow import (
    build_full_flow_plan,
    build_full_flow_state,
    write_full_flow_plan,
)
from gas_calibrator.validation import (
    v1_5_authoritative_resume_offline_state_advance_preflight as module,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_post_execution_verifier import (
    READY_STATUS as VERIFIER_READY_STATUS,
    SCHEMA as VERIFIER_SCHEMA,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_preflight import (
    BLOCKED_STATUS,
    READY_STATUS,
    build_v1_5_authoritative_resume_offline_state_advance_preflight,
)
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint

ROOT = Path(__file__).resolve().parents[1]


def _write(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return path


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fixture(
    tmp_path: Path,
) -> tuple[Path, dict, Path, Path, list[str], int]:
    root = tmp_path / "flow"
    config = _write(tmp_path / "config.json", {})
    plan = build_full_flow_plan(
        config_path=config,
        output_dir=root,
        run_id="resume-state-advance-106",
    )
    plan_path = write_full_flow_plan(plan, root)["json"]
    step_ids = [step.step_id for step in plan.steps]
    executed_index = next(
        index
        for index, step in enumerate(plan.steps)
        if index > 0
        and step.execution_mode in {"offline", "offline_sidecar"}
        and not step.opens_com_ports
        and not step.controls_pressure
        and not step.controls_gas_route
        and not step.controls_water_route
        and not step.writes_coefficients
        and not step.writes_device_id
    )
    completed = step_ids[:executed_index]
    state = build_full_flow_state(plan, completed_steps=completed)
    state_path = _write(root / "v1_5_full_flow_state.json", state.to_json())
    verified_output = root / "offline-output" / "result.json"
    verified_output.parent.mkdir(parents=True, exist_ok=True)
    verified_output.write_text('{"status": "fresh"}\n', encoding="utf-8")
    executor_path = _write(
        root / "executor.json",
        {
            "evidence": "executor-104",
            "finished_at": "2026-07-12T11:59:59Z",
        },
    )
    verifier = {
        "schema": VERIFIER_SCHEMA,
        "generated_at": "2026-07-12T12:00:00Z",
        "overall_status": VERIFIER_READY_STATUS,
        "offline_post_execution_verification_ready": True,
        "review_required_count": 0,
        "review_reasons": [],
        "offline_executor_json": str(executor_path.resolve()),
        "offline_executor_sha256": _sha(executor_path),
        "attempt_id": "resume-attempt-106",
        "run_id": plan.run_id,
        "next_step_id": step_ids[executed_index],
        "full_flow_plan_json": str(plan_path.resolve()),
        "full_flow_plan_sha256": _sha(plan_path),
        "authoritative_state_json": str(state_path.resolve()),
        "authoritative_state_sha256_expected": _sha(state_path),
        "authoritative_state_sha256_current": _sha(state_path),
        "verified_outputs": [
            {
                "path": str(verified_output.resolve()),
                "recorded_before_sha256": "",
                "recorded_after_sha256": _sha(verified_output),
                "current_sha256": _sha(verified_output),
                "status": "ready",
            }
        ],
        "authoritative_state_advance_allowed": False,
        "execution_supported": False,
        "would_execute": False,
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "connects_postgresql": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
    }
    verifier_path = _write(root / "post-execution-verifier.json", verifier)
    return verifier_path, verifier, state_path, verified_output, step_ids, executed_index


def _install_verifier(monkeypatch: pytest.MonkeyPatch, verifier: dict) -> None:
    monkeypatch.setattr(
        module,
        "build_v1_5_authoritative_resume_offline_post_execution_verifier",
        lambda **_kwargs: dict(verifier),
    )


def test_state_advance_preflight_builds_one_locked_contiguous_candidate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    verifier_path, verifier, state_path, _output, step_ids, index = _fixture(tmp_path)
    original_state = state_path.read_bytes()
    _install_verifier(monkeypatch, verifier)
    model = build_v1_5_authoritative_resume_offline_state_advance_preflight(
        offline_post_execution_verifier_json=verifier_path
    )
    assert model["overall_status"] == READY_STATUS
    assert model["offline_state_advance_preflight_ready"] is True
    assert model["expected_current_state_sha256"] == _sha(state_path)
    assert model["candidate_state"]["completed_step_ids"] == step_ids[: index + 1]
    assert model["candidate_state"]["current_step_id"] == step_ids[index + 1]
    assert model["candidate_state"]["failed_step_ids"] == []
    assert model["candidate_state"]["created_at"] == "2026-07-12T11:59:59Z"
    assert model["compare_and_swap_required"] is True
    assert model["authoritative_state_write_allowed"] is False
    assert model["writes_authoritative_state"] is False
    assert state_path.read_bytes() == original_state


def test_state_advance_preflight_blocks_stale_current_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    verifier_path, verifier, state_path, _output, _step_ids, _index = _fixture(tmp_path)
    _install_verifier(monkeypatch, verifier)
    state_path.write_text('{"changed": true}\n', encoding="utf-8")
    model = build_v1_5_authoritative_resume_offline_state_advance_preflight(
        offline_post_execution_verifier_json=verifier_path
    )
    assert model["overall_status"] == BLOCKED_STATUS
    assert "authoritative_state_compare_and_swap_sha256_mismatch" in model[
        "blocker_reasons"
    ]
    assert model["authoritative_state_write_allowed"] is False
    assert model["candidate_state"] == {}
    assert model["candidate_state_sha256"] == ""


def test_state_advance_preflight_blocks_reparse_state_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    verifier_path, verifier, state_path, _output, _step_ids, _index = _fixture(tmp_path)
    _install_verifier(monkeypatch, verifier)
    monkeypatch.setattr(
        module,
        "_has_reparse_point",
        lambda path: Path(path).absolute() == state_path.absolute(),
    )
    model = build_v1_5_authoritative_resume_offline_state_advance_preflight(
        offline_post_execution_verifier_json=verifier_path
    )
    assert model["overall_status"] == BLOCKED_STATUS
    assert "authoritative_state_target_or_parent_is_reparse_point" in model[
        "blocker_reasons"
    ]
    assert model["candidate_state"] == {}
    assert model["candidate_state_sha256"] == ""


def test_state_advance_preflight_blocks_changed_verified_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    verifier_path, verifier, _state, output, _step_ids, _index = _fixture(tmp_path)
    _install_verifier(monkeypatch, verifier)
    output.write_text('{"status": "changed"}\n', encoding="utf-8")
    model = build_v1_5_authoritative_resume_offline_state_advance_preflight(
        offline_post_execution_verifier_json=verifier_path
    )
    assert model["overall_status"] == BLOCKED_STATUS
    assert any(
        reason.startswith("verified_output_sha256_changed")
        for reason in model["blocker_reasons"]
    )
    assert model["candidate_state"] == {}


def test_state_advance_preflight_blocks_noncontiguous_or_repeated_step(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    verifier_path, verifier, state_path, _output, step_ids, index = _fixture(tmp_path)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["completed_step_ids"] = [step_ids[index]]
    state["current_step_id"] = step_ids[index]
    _write(state_path, state)
    verifier["authoritative_state_sha256_expected"] = _sha(state_path)
    verifier["authoritative_state_sha256_current"] = _sha(state_path)
    _write(verifier_path, verifier)
    _install_verifier(monkeypatch, verifier)
    model = build_v1_5_authoritative_resume_offline_state_advance_preflight(
        offline_post_execution_verifier_json=verifier_path
    )
    assert model["overall_status"] == BLOCKED_STATUS
    assert "completed_steps_not_exact_contiguous_prefix" in model["blocker_reasons"]
    assert "verified_step_already_completed" in model["blocker_reasons"]
    assert model["candidate_state"] == {}


def test_state_advance_preflight_blocks_recomputed_verifier_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    verifier_path, verifier, _state, _output, _step_ids, _index = _fixture(tmp_path)
    recomputed = dict(verifier)
    recomputed["attempt_id"] = "different-attempt"
    _install_verifier(monkeypatch, recomputed)
    model = build_v1_5_authoritative_resume_offline_state_advance_preflight(
        offline_post_execution_verifier_json=verifier_path
    )
    assert model["overall_status"] == BLOCKED_STATUS
    assert "post_execution_verifier_recompute_mismatch:attempt_id" in model[
        "blocker_reasons"
    ]
    assert model["candidate_state"] == {}


def test_state_advance_preflight_cli_writes_preview_but_not_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    verifier_path, verifier, state_path, _output, _step_ids, _index = _fixture(tmp_path)
    original_state = state_path.read_bytes()
    _install_verifier(monkeypatch, verifier)
    monkeypatch.setattr(
        tool,
        "build_v1_5_authoritative_resume_offline_state_advance_preflight",
        lambda **_kwargs: build_v1_5_authoritative_resume_offline_state_advance_preflight(
            offline_post_execution_verifier_json=verifier_path
        ),
    )
    output_dir = tmp_path / "preflight"
    assert tool.main(
        [
            "--offline-post-execution-verifier-json",
            str(verifier_path),
            "--output-dir",
            str(output_dir),
            "--fail-on-blocker",
        ]
    ) == 0
    payload = json.loads(
        (
            output_dir
            / "v1_5_authoritative_resume_offline_state_advance_preflight.json"
        ).read_text(encoding="utf-8")
    )
    preview = output_dir / "v1_5_authoritative_resume_offline_state_candidate.json"
    assert payload["candidate_state_preview_sha256"] == _sha(preview)
    assert payload["candidate_state_sha256"] == _sha(preview)
    assert payload["authoritative_state_write_allowed"] is False
    assert state_path.read_bytes() == original_state

    entry = classify_v1_5_entrypoint(
        ROOT
        / "src/gas_calibrator/tools/export_v1_5_authoritative_resume_offline_state_advance_preflight.py",
        root=ROOT,
    )
    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"


def test_state_advance_preflight_cli_rejects_execute_before_output(
    tmp_path: Path,
) -> None:
    verifier_path, _verifier, _state, _output, _step_ids, _index = _fixture(tmp_path)
    output_dir = tmp_path / "rejected"
    with pytest.raises(SystemExit) as exc_info:
        tool.main(
            [
                "--offline-post-execution-verifier-json",
                str(verifier_path),
                "--output-dir",
                str(output_dir),
                "--execute",
            ]
        )
    assert exc_info.value.code == 2
    assert not output_dir.exists()
