import hashlib
import json
import sys
from pathlib import Path

import pytest

from gas_calibrator.tools import (
    export_v1_5_authoritative_resume_offline_post_execution_verifier as tool,
)
from gas_calibrator.validation import (
    v1_5_authoritative_resume_offline_post_execution_verifier as module,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_candidate_gate import (
    READY_STATUS as GATE_READY_STATUS,
    SCHEMA as GATE_SCHEMA,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_executor import (
    EXECUTED_STATUS,
    SCHEMA as EXECUTOR_SCHEMA,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_post_execution_verifier import (
    READY_STATUS,
    REVIEW_STATUS,
    build_v1_5_authoritative_resume_offline_post_execution_verifier,
)
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint

ROOT = Path(__file__).resolve().parents[1]
MODULE = "gas_calibrator.tools.export_v1_5_authoritative_resume_executor_controlled_design"


def _write(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return path


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fixture(tmp_path: Path) -> tuple[Path, dict, Path, list[Path]]:
    root = tmp_path / "run"
    output_dir = root / "step-output"
    outputs = [output_dir / "result.json", output_dir / "report.md"]
    for index, path in enumerate(outputs):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"fresh-{index}\n", encoding="utf-8")
    state_path = _write(
        root / "v1_5_authoritative_resume_state.json",
        {"run_id": "run-105", "completed_step_ids": ["previous-step"]},
    )
    authorization_path = _write(
        root / "authorization.json",
        {
            "authoritative_state_json": str(state_path.resolve()),
            "authoritative_state_sha256": _sha(state_path),
        },
    )
    validation_path = _write(
        root / "validation.json",
        {
            "authorization_packet_json": str(authorization_path.resolve()),
            "authorization_packet_sha256": _sha(authorization_path),
        },
    )
    preflight_path = _write(
        root / "preflight.json",
        {"authorization_validation_json": str(validation_path.resolve())},
    )
    command = [
        "python",
        "-m",
        MODULE,
        "--output-dir",
        str(output_dir.resolve()),
    ]
    plan_path = _write(
        root / "v1_5_full_flow_plan.json",
        {
            "run_id": "run-105",
            "steps": [
                {
                    "step_id": "offline-step",
                    "execution_mode": "offline_sidecar",
                    "tool_module": MODULE,
                    "command": command,
                    "expected_outputs": [
                        str(path.relative_to(root)) for path in outputs
                    ],
                }
            ],
        },
    )
    gate = {
        "schema": GATE_SCHEMA,
        "overall_status": GATE_READY_STATUS,
        "offline_resume_candidate_ready": True,
        "execution_preflight_json": str(preflight_path.resolve()),
        "attempt_id": "resume-attempt-105",
        "run_id": "run-105",
        "full_flow_plan_json": str(plan_path.resolve()),
        "full_flow_plan_sha256": _sha(plan_path),
        "next_step_id": "offline-step",
    }
    gate_path = _write(root / "gate.json", gate)
    output_hashes = {str(path.resolve()): _sha(path) for path in outputs}
    executor = {
        "schema": EXECUTOR_SCHEMA,
        "overall_status": EXECUTED_STATUS,
        "offline_execution_requested": True,
        "offline_step_executed": True,
        "hold_count": 0,
        "hold_reasons": [],
        "offline_candidate_gate_json": str(gate_path.resolve()),
        "offline_candidate_gate_sha256": _sha(gate_path),
        "attempt_id": "resume-attempt-105",
        "run_id": "run-105",
        "next_step_id": "offline-step",
        "executed_command": [sys.executable, *command[1:]],
        "process_attempted": True,
        "process_return_code": 0,
        "started_at": "2026-07-12T12:00:00Z",
        "finished_at": "2026-07-12T12:00:01Z",
        "expected_output_paths": list(output_hashes),
        "expected_output_sha256_before": {key: "" for key in output_hashes},
        "expected_output_sha256_after": output_hashes,
        "expected_outputs_fresh": True,
        "authoritative_state_advanced": False,
        "execution_supported": True,
        "offline_execution_only": True,
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
    executor_path = _write(root / "executor.json", executor)
    return executor_path, gate, state_path, outputs


def _install_gate(monkeypatch: pytest.MonkeyPatch, gate: dict) -> None:
    monkeypatch.setattr(
        module,
        "build_v1_5_authoritative_resume_offline_candidate_gate",
        lambda **_kwargs: dict(gate),
    )


def test_post_execution_verifier_accepts_bound_fresh_outputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executor_path, gate, _state, outputs = _fixture(tmp_path)
    _install_gate(monkeypatch, gate)
    model = build_v1_5_authoritative_resume_offline_post_execution_verifier(
        offline_executor_json=executor_path
    )
    assert model["overall_status"] == READY_STATUS
    assert model["offline_post_execution_verification_ready"] is True
    assert model["authoritative_state_advance_allowed"] is False
    assert {row["path"] for row in model["verified_outputs"]} == {
        str(path.resolve()) for path in outputs
    }
    assert all(row["status"] == "ready" for row in model["verified_outputs"])


def test_post_execution_verifier_holds_when_output_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executor_path, gate, _state, outputs = _fixture(tmp_path)
    _install_gate(monkeypatch, gate)
    outputs[0].write_text("changed-after-execution\n", encoding="utf-8")
    model = build_v1_5_authoritative_resume_offline_post_execution_verifier(
        offline_executor_json=executor_path
    )
    assert model["overall_status"] == REVIEW_STATUS
    assert any(
        reason.startswith("offline_executor_output_sha256_mismatch")
        for reason in model["review_reasons"]
    )


def test_post_execution_verifier_holds_when_state_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executor_path, gate, state_path, _outputs = _fixture(tmp_path)
    _install_gate(monkeypatch, gate)
    state_path.write_text('{"advanced": true}\n', encoding="utf-8")
    model = build_v1_5_authoritative_resume_offline_post_execution_verifier(
        offline_executor_json=executor_path
    )
    assert model["overall_status"] == REVIEW_STATUS
    assert "authoritative_state_changed_during_offline_execution" in model["review_reasons"]
    assert model["authoritative_state_advance_allowed"] is False


def test_post_execution_verifier_holds_when_gate_is_replaced(tmp_path: Path) -> None:
    executor_path, gate, _state, _outputs = _fixture(tmp_path)
    executor = json.loads(executor_path.read_text(encoding="utf-8"))
    gate_path = Path(executor["offline_candidate_gate_json"])
    gate_path.write_text('{"schema": "tampered"}\n', encoding="utf-8")
    model = build_v1_5_authoritative_resume_offline_post_execution_verifier(
        offline_executor_json=executor_path
    )
    assert model["overall_status"] == REVIEW_STATUS
    assert "offline_candidate_gate_schema_invalid" in model["review_reasons"]
    assert "offline_executor_gate_sha256_mismatch" in model["review_reasons"]
    assert "offline_executor_gate_recompute_failed" in model["review_reasons"]


def test_post_execution_verifier_holds_when_recomputed_gate_contract_differs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executor_path, gate, _state, _outputs = _fixture(tmp_path)
    recomputed = dict(gate)
    recomputed["run_id"] = "different-run"
    _install_gate(monkeypatch, recomputed)
    model = build_v1_5_authoritative_resume_offline_post_execution_verifier(
        offline_executor_json=executor_path
    )
    assert model["overall_status"] == REVIEW_STATUS
    assert "offline_executor_gate_recompute_mismatch:run_id" in model["review_reasons"]


def test_post_execution_verifier_holds_on_executor_boundary_tamper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executor_path, gate, _state, _outputs = _fixture(tmp_path)
    _install_gate(monkeypatch, gate)
    executor = json.loads(executor_path.read_text(encoding="utf-8"))
    executor["writes_coefficients"] = True
    _write(executor_path, executor)
    model = build_v1_5_authoritative_resume_offline_post_execution_verifier(
        offline_executor_json=executor_path
    )
    assert model["overall_status"] == REVIEW_STATUS
    assert "offline_executor_boundary_invalid:writes_coefficients" in model["review_reasons"]


def test_post_execution_verifier_cli_and_entrypoint_classification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executor_path, gate, _state, _outputs = _fixture(tmp_path)
    monkeypatch.setattr(
        tool,
        "build_v1_5_authoritative_resume_offline_post_execution_verifier",
        lambda **_kwargs: build_v1_5_authoritative_resume_offline_post_execution_verifier(
            offline_executor_json=executor_path
        ),
    )
    _install_gate(monkeypatch, gate)
    output = tmp_path / "verification"
    assert tool.main(["--offline-executor-json", str(executor_path), "--output-dir", str(output)]) == 0
    payload = json.loads(
        (output / "v1_5_authoritative_resume_offline_post_execution_verifier.json").read_text(
            encoding="utf-8"
        )
    )
    assert payload["overall_status"] == READY_STATUS
    entry = classify_v1_5_entrypoint(
        ROOT
        / "src/gas_calibrator/tools/export_v1_5_authoritative_resume_offline_post_execution_verifier.py",
        root=ROOT,
    )
    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"


def test_post_execution_verifier_cli_rejects_execute_before_output(
    tmp_path: Path,
) -> None:
    executor_path, _gate, _state, _outputs = _fixture(tmp_path)
    output = tmp_path / "rejected"
    with pytest.raises(SystemExit) as exc_info:
        tool.main(
            [
                "--offline-executor-json",
                str(executor_path),
                "--output-dir",
                str(output),
                "--execute",
            ]
        )
    assert exc_info.value.code == 2
    assert not output.exists()
