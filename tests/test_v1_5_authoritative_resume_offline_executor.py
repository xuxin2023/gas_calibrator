import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from gas_calibrator.tools.run_v1_5_authoritative_resume_offline_executor import main
from gas_calibrator.validation import v1_5_authoritative_resume_offline_executor as module
from gas_calibrator.validation.v1_5_authoritative_resume_offline_candidate_gate import (
    READY_STATUS as GATE_READY_STATUS,
    SCHEMA as GATE_SCHEMA,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_executor import (
    CONFIRMATION_TEXT,
    EXECUTED_STATUS,
    HOLD_STATUS,
    LOCKED_STATUS,
    run_v1_5_authoritative_resume_offline_executor,
)
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 7, 12, 12, 0, tzinfo=UTC)
MODULE = "gas_calibrator.tools.export_v1_5_authoritative_resume_executor_controlled_design"


def _write(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return path


def _fixture(tmp_path: Path) -> tuple[Path, dict, list[Path]]:
    root = tmp_path / "run"
    output_dir = root / "step-output"
    expected = [output_dir / "result.json", output_dir / "report.md"]
    command = [
        "python",
        "-m",
        MODULE,
        "--authoritative-resume-executor-blocked-json",
        str(root / "blocked.json"),
        "--output-dir",
        str(output_dir),
    ]
    step = {
        "step_id": "offline-step",
        "execution_mode": "offline_sidecar",
        "tool_module": MODULE,
        "command": command,
        "expected_outputs": [str(path.relative_to(root)) for path in expected],
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_gas_route": False,
        "controls_water_route": False,
        "writes_device_id": False,
        "writes_coefficients": False,
    }
    plan = _write(root / "v1_5_full_flow_plan.json", {"run_id": "run-104", "steps": [step]})
    gate = {
        "schema": GATE_SCHEMA,
        "generated_at": "2026-07-12T12:00:00Z",
        "overall_status": GATE_READY_STATUS,
        "offline_resume_candidate_ready": True,
        "review_required_count": 0,
        "review_reasons": [],
        "execution_preflight_json": str(root / "preflight.json"),
        "execution_preflight_sha256": "preflight-sha",
        "execution_preflight_age_s": 10.0,
        "attempt_id": "resume-attempt-104",
        "run_id": "run-104",
        "full_flow_plan_json": str(plan.resolve()),
        "full_flow_plan_sha256": "plan-sha",
        "next_step_id": "offline-step",
        "next_step_execution_mode": "offline_sidecar",
        "next_step_tool_module": MODULE,
        "next_step_command_recorded_only": command,
        "physical_or_write_step_must_use_dedicated_executor": False,
        "execution_supported": False,
        "resume_execution_allowed": False,
        "would_execute": False,
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "connects_postgresql": False,
        "database_written": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
    }
    gate_path = _write(root / "gate.json", gate)
    return gate_path, gate, expected


def _install_recompute(monkeypatch: pytest.MonkeyPatch, gate: dict) -> None:
    monkeypatch.setattr(
        module,
        "build_v1_5_authoritative_resume_offline_candidate_gate",
        lambda **_kwargs: dict(gate),
    )


def test_executor_defaults_to_locked_and_never_calls_subprocess(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_path, gate, _expected = _fixture(tmp_path)
    _install_recompute(monkeypatch, gate)

    def forbidden_runner(*_args, **_kwargs):
        raise AssertionError("subprocess must remain locked")

    model = run_v1_5_authoritative_resume_offline_executor(
        offline_candidate_gate_json=gate_path,
        now=NOW,
        subprocess_runner=forbidden_runner,
    )
    assert model["overall_status"] == LOCKED_STATUS
    assert model["process_attempted"] is False
    assert model["offline_step_executed"] is False
    assert model["authoritative_state_advanced"] is False


def test_executor_runs_exact_python_module_and_requires_fresh_outputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_path, gate, expected = _fixture(tmp_path)
    _install_recompute(monkeypatch, gate)

    def fake_runner(command, **kwargs):
        assert command[0] == module.sys.executable
        assert command[1:3] == ["-m", MODULE]
        assert kwargs["shell"] is False
        for index, path in enumerate(expected):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f"fresh-{index}", encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    model = run_v1_5_authoritative_resume_offline_executor(
        offline_candidate_gate_json=gate_path,
        execute_offline_step=True,
        expected_attempt_id="resume-attempt-104",
        operator_confirmation_text=CONFIRMATION_TEXT,
        now=NOW,
        subprocess_runner=fake_runner,
    )
    assert model["overall_status"] == EXECUTED_STATUS
    assert model["offline_step_executed"] is True
    assert model["process_return_code"] == 0
    assert model["expected_outputs_fresh"] is True
    assert all(model["expected_output_sha256_after"].values())
    assert set(model["expected_output_sha256_after"]) == {
        str(path.resolve()) for path in expected
    }
    assert model["authoritative_state_advanced"] is False
    assert model["opens_com_ports"] is False
    assert model["connects_postgresql"] is False


def test_executor_holds_when_process_returns_zero_without_fresh_outputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_path, gate, _expected = _fixture(tmp_path)
    _install_recompute(monkeypatch, gate)

    def fake_runner(command, **_kwargs):
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    model = run_v1_5_authoritative_resume_offline_executor(
        offline_candidate_gate_json=gate_path,
        execute_offline_step=True,
        expected_attempt_id="resume-attempt-104",
        operator_confirmation_text=CONFIRMATION_TEXT,
        now=NOW,
        subprocess_runner=fake_runner,
    )
    assert model["overall_status"] == HOLD_STATUS
    assert model["process_attempted"] is True
    assert model["offline_step_executed"] is False
    assert any(reason.startswith("expected_output_missing") for reason in model["hold_reasons"])
    assert model["authoritative_state_advanced"] is False


@pytest.mark.parametrize(
    ("attempt_id", "confirmation", "reason"),
    [
        ("wrong-attempt", CONFIRMATION_TEXT, "offline_execution_attempt_id_mismatch"),
        ("resume-attempt-104", "wrong", "offline_execution_confirmation_invalid"),
    ],
)
def test_executor_rejects_wrong_invocation_binding_before_subprocess(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    attempt_id: str,
    confirmation: str,
    reason: str,
) -> None:
    gate_path, gate, _expected = _fixture(tmp_path)
    _install_recompute(monkeypatch, gate)

    def forbidden_runner(*_args, **_kwargs):
        raise AssertionError("subprocess must not start")

    model = run_v1_5_authoritative_resume_offline_executor(
        offline_candidate_gate_json=gate_path,
        execute_offline_step=True,
        expected_attempt_id=attempt_id,
        operator_confirmation_text=confirmation,
        now=NOW,
        subprocess_runner=forbidden_runner,
    )
    assert model["overall_status"] == HOLD_STATUS
    assert model["process_attempted"] is False
    assert reason in model["hold_reasons"]


def test_executor_rejects_nonready_candidate_before_subprocess(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_path, gate, _expected = _fixture(tmp_path)
    gate["overall_status"] = "review_required"
    gate["offline_resume_candidate_ready"] = False
    _write(gate_path, gate)
    _install_recompute(monkeypatch, gate)

    def forbidden_runner(*_args, **_kwargs):
        raise AssertionError("subprocess must not start")

    model = run_v1_5_authoritative_resume_offline_executor(
        offline_candidate_gate_json=gate_path,
        execute_offline_step=True,
        expected_attempt_id="resume-attempt-104",
        operator_confirmation_text=CONFIRMATION_TEXT,
        now=NOW,
        subprocess_runner=forbidden_runner,
    )
    assert model["overall_status"] == HOLD_STATUS
    assert model["process_attempted"] is False
    assert "offline_candidate_gate_not_ready" in model["hold_reasons"]


def test_executor_rejects_expected_output_outside_plan_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_path, gate, _expected = _fixture(tmp_path)
    plan_path = Path(gate["full_flow_plan_json"])
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    plan["steps"][0]["expected_outputs"] = ["../outside.json"]
    _write(plan_path, plan)
    _install_recompute(monkeypatch, gate)

    def forbidden_runner(*_args, **_kwargs):
        raise AssertionError("subprocess must not start")

    model = run_v1_5_authoritative_resume_offline_executor(
        offline_candidate_gate_json=gate_path,
        execute_offline_step=True,
        expected_attempt_id="resume-attempt-104",
        operator_confirmation_text=CONFIRMATION_TEXT,
        now=NOW,
        subprocess_runner=forbidden_runner,
    )
    assert model["overall_status"] == HOLD_STATUS
    assert model["process_attempted"] is False
    assert any(
        reason.startswith("offline_candidate_expected_output_outside_plan_root")
        for reason in model["hold_reasons"]
    )


def test_executor_cli_default_and_entrypoint_classification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate_path, gate, _expected = _fixture(tmp_path)
    _install_recompute(monkeypatch, gate)
    monkeypatch.setattr(module, "_now", lambda: NOW)
    output = tmp_path / "executor-evidence"
    assert main(["--offline-candidate-gate-json", str(gate_path), "--output-dir", str(output)]) == 0
    payload = json.loads(
        (output / "v1_5_authoritative_resume_offline_executor.json").read_text(
            encoding="utf-8"
        )
    )
    assert payload["overall_status"] == LOCKED_STATUS
    entry = classify_v1_5_entrypoint(
        ROOT / "src/gas_calibrator/tools/run_v1_5_authoritative_resume_offline_executor.py",
        root=ROOT,
    )
    assert entry.category == "full_flow_orchestration"
    assert entry.formal_status == "manual_authorized_offline_resume_only"
    assert entry.risk_level == "offline_subprocess_risk"
    assert entry.opens_com_ports is False


def test_executor_cli_rejects_generic_execute_before_output(tmp_path: Path) -> None:
    gate_path, _gate, _expected = _fixture(tmp_path)
    output = tmp_path / "rejected"
    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "--offline-candidate-gate-json",
                str(gate_path),
                "--output-dir",
                str(output),
                "--execute",
            ]
        )
    assert exc_info.value.code == 2
    assert not output.exists()
