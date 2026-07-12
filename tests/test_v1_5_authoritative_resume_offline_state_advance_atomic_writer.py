import hashlib
import json
from pathlib import Path

import pytest

from gas_calibrator.tools import (
    run_v1_5_authoritative_resume_offline_state_advance_atomic_writer as tool,
)
from gas_calibrator.validation import (
    v1_5_authoritative_resume_offline_state_advance_atomic_writer as writer_module,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_atomic_writer import (
    BLOCKED_STATUS,
    COMMITTED_STATUS,
    CONFIRMATION_TEMPLATE,
    ROLLED_BACK_STATUS,
    execute_v1_5_authoritative_resume_offline_state_advance_atomic_write,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_authorization import (
    READY_STATUS as AUTHORIZATION_READY_STATUS,
    SCHEMA as AUTHORIZATION_SCHEMA,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_blocked_executor import (
    AUTHORIZATION_COMPARE_KEYS,
)
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint


ROOT = Path(__file__).resolve().parents[1]
AUTHORIZATION_ID = "state-advance-writer-108"


def _write(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(
        (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    )
    return path


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fixture(tmp_path: Path) -> tuple[Path, dict, Path, Path, bytes, bytes]:
    root = tmp_path / "flow"
    original = b'{"current_step_id":"step-b","run_id":"run-108"}\n'
    candidate = b'{"current_step_id":"step-c","run_id":"run-108"}\n'
    state_path = root / "v1_5_full_flow_state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_bytes(original)
    candidate_path = root / "state-advance" / "v1_5_authoritative_resume_offline_state_candidate.json"
    candidate_path.parent.mkdir(parents=True, exist_ok=True)
    candidate_path.write_bytes(candidate)
    preflight_path = _write(
        candidate_path.parent
        / "v1_5_authoritative_resume_offline_state_advance_preflight.json",
        {"schema": "preflight", "ready": True},
    )
    packet_path = _write(root / "authorization-packet.json", {"ready": True})
    values = {key: None for key in AUTHORIZATION_COMPARE_KEYS}
    values.update(
        {
            "overall_status": AUTHORIZATION_READY_STATUS,
            "offline_state_advance_authorization_validated": True,
            "review_required_count": 0,
            "review_reasons": [],
            "offline_state_advance_preflight_json": str(preflight_path.resolve()),
            "offline_state_advance_preflight_sha256": _sha(preflight_path),
            "authorization_packet_json": str(packet_path.resolve()),
            "authorization_packet_sha256": _sha(packet_path),
            "authorization_id": AUTHORIZATION_ID,
            "authorization_expires_at": "2099-07-12T12:15:00Z",
            "run_id": "run-108",
            "attempt_id": "attempt-108",
            "verified_step_id": "step-b",
            "next_step_id_after_advance": "step-c",
            "authoritative_state_json": str(state_path.resolve()),
            "expected_current_state_sha256": _sha(state_path),
            "candidate_state_preview_json": str(candidate_path.resolve()),
            "candidate_state_sha256": _sha(candidate_path),
            "compare_and_swap_required": True,
            "execution_supported": False,
            "state_write_execution_allowed": False,
            "would_execute": False,
            "writes_authoritative_state": False,
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
            "checks": [{"check": "ready", "status": "ready", "reasons": []}],
        }
    )
    validation = {
        "schema": AUTHORIZATION_SCHEMA,
        "generated_at": "2026-07-12T12:00:00Z",
        **values,
    }
    validation_path = _write(
        root
        / "authorization-review"
        / "v1_5_authoritative_resume_offline_state_advance_authorization.json",
        validation,
    )
    return validation_path, validation, state_path, candidate_path, original, candidate


def _install_ready_authorization(
    monkeypatch: pytest.MonkeyPatch, validation: dict
) -> None:
    monkeypatch.setattr(
        writer_module,
        "build_v1_5_authoritative_resume_offline_state_advance_authorization",
        lambda **_kwargs: dict(validation),
    )


def _execute(
    tmp_path: Path,
    validation_path: Path,
    **kwargs,
) -> dict:
    return execute_v1_5_authoritative_resume_offline_state_advance_atomic_write(
        state_advance_authorization_json=validation_path,
        authorization_id=AUTHORIZATION_ID,
        confirmation_template=CONFIRMATION_TEMPLATE,
        output_dir=tmp_path / "writer-output",
        **kwargs,
    )


def test_atomic_writer_commits_exact_one_step_candidate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    validation_path, validation, state, _preview, original, candidate = _fixture(
        tmp_path
    )
    _install_ready_authorization(monkeypatch, validation)
    result = _execute(tmp_path, validation_path)
    assert result["overall_status"] == COMMITTED_STATUS
    assert result["authorization_recomputed_ready"] is True
    assert result["authorization_recomputed_under_lock"] is True
    assert result["current_state_sha256_rechecked"] is True
    assert result["candidate_sha256_rechecked"] is True
    assert result["authoritative_state_write_committed"] is True
    assert result["writes_authoritative_state"] is True
    assert result["final_state_matches_candidate"] is True
    assert result["run_id"] == "run-108"
    assert result["attempt_id"] == "attempt-108"
    assert result["verified_step_id"] == "step-b"
    assert result["next_step_id_after_advance"] == "step-c"
    assert result["opens_com_ports"] is False
    assert result["controls_pressure"] is False
    assert result["controls_water_or_gas_routes"] is False
    assert result["writes_coefficients"] is False
    assert result["connects_postgresql"] is False
    assert state.read_bytes() == candidate
    snapshot = Path(result["rollback_snapshot_path"])
    assert snapshot.read_bytes() == original
    assert not Path(result["lock_path"]).exists()
    assert Path(result["invocation_json"]).is_file()
    assert (
        tmp_path
        / "writer-output"
        / "v1_5_authoritative_resume_offline_state_advance_atomic_writer.json"
    ).is_file()


def test_atomic_writer_rejects_wrong_authorization_id_without_touching_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    validation_path, validation, state, _preview, original, _candidate = _fixture(
        tmp_path
    )
    _install_ready_authorization(monkeypatch, validation)
    result = execute_v1_5_authoritative_resume_offline_state_advance_atomic_write(
        state_advance_authorization_json=validation_path,
        authorization_id="different-authorization",
        confirmation_template=CONFIRMATION_TEMPLATE,
        output_dir=tmp_path / "wrong-id",
    )
    assert result["overall_status"] == BLOCKED_STATUS
    assert "authorization_id_mismatch" in result["failure_reasons"]
    assert result["write_attempted"] is False
    assert state.read_bytes() == original


def test_atomic_writer_rejects_recomputed_authorization_schema_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    validation_path, validation, state, _preview, original, _candidate = _fixture(
        tmp_path
    )
    changed = dict(validation)
    changed["schema"] = "unexpected_authorization_schema"
    monkeypatch.setattr(
        writer_module,
        "build_v1_5_authoritative_resume_offline_state_advance_authorization",
        lambda **_kwargs: changed,
    )
    result = _execute(tmp_path, validation_path)
    assert result["overall_status"] == BLOCKED_STATUS
    assert "state_advance_authorization_recomputed_schema_invalid" in result[
        "failure_reasons"
    ]
    assert result["write_attempted"] is False
    assert state.read_bytes() == original


def test_atomic_writer_rejects_reparse_point_in_authorization_ancestor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    validation_path, validation, state, _preview, original, _candidate = _fixture(
        tmp_path
    )
    _install_ready_authorization(monkeypatch, validation)
    reparse_ancestor = validation_path.parent.parent
    original_check = writer_module._has_reparse_point
    monkeypatch.setattr(
        writer_module,
        "_has_reparse_point",
        lambda path: path == reparse_ancestor or original_check(path),
    )
    result = _execute(tmp_path, validation_path)
    assert result["overall_status"] == BLOCKED_STATUS
    assert "state_advance_authorization_path_contains_reparse_point" in result[
        "failure_reasons"
    ]
    assert result["single_writer_lock_acquired"] is False
    assert result["write_attempted"] is False
    assert state.read_bytes() == original


def test_atomic_writer_blocks_when_authorization_drifts_under_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    validation_path, validation, state, _preview, original, _candidate = _fixture(
        tmp_path
    )
    calls = 0

    def rebuild(**_kwargs):
        nonlocal calls
        calls += 1
        payload = dict(validation)
        if calls >= 2:
            payload["overall_status"] = "review_required"
            payload["offline_state_advance_authorization_validated"] = False
        return payload

    monkeypatch.setattr(
        writer_module,
        "build_v1_5_authoritative_resume_offline_state_advance_authorization",
        rebuild,
    )
    result = _execute(tmp_path, validation_path)
    assert result["overall_status"] == BLOCKED_STATUS
    assert any(
        reason.startswith("authorization_revalidation_under_lock_failed")
        for reason in result["failure_reasons"]
    )
    assert result["write_attempted"] is False
    assert state.read_bytes() == original
    assert not Path(result["lock_path"]).exists()


def test_atomic_writer_rechecks_state_after_acquiring_shared_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    validation_path, validation, state, _preview, _original, _candidate = _fixture(
        tmp_path
    )
    _install_ready_authorization(monkeypatch, validation)
    original_acquire = writer_module._acquire_lock

    def acquire_then_change(lock_path: Path, payload: dict) -> int:
        fd = original_acquire(lock_path, payload)
        state.write_bytes(b'{"external":"change"}\n')
        return fd

    monkeypatch.setattr(writer_module, "_acquire_lock", acquire_then_change)
    result = _execute(tmp_path, validation_path)
    assert result["overall_status"] == BLOCKED_STATUS
    assert any(
        "authoritative_state_compare_and_swap_sha256_changed" in reason
        for reason in result["failure_reasons"]
    )
    assert result["write_attempted"] is False
    assert state.read_bytes() == b'{"external":"change"}\n'


def test_atomic_writer_rejects_validation_file_change_after_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    validation_path, validation, state, _preview, original, _candidate = _fixture(
        tmp_path
    )
    _install_ready_authorization(monkeypatch, validation)
    original_acquire = writer_module._acquire_lock

    def acquire_then_touch_validation(lock_path: Path, payload: dict) -> int:
        fd = original_acquire(lock_path, payload)
        validation_path.write_bytes(validation_path.read_bytes() + b" \n")
        return fd

    monkeypatch.setattr(
        writer_module, "_acquire_lock", acquire_then_touch_validation
    )
    result = _execute(tmp_path, validation_path)
    assert result["overall_status"] == BLOCKED_STATUS
    assert "state_advance_authorization_changed_before_write" in result[
        "failure_reasons"
    ]
    assert result["write_attempted"] is False
    assert state.read_bytes() == original


def test_atomic_writer_rechecks_candidate_after_acquiring_shared_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    validation_path, validation, state, preview, original, _candidate = _fixture(
        tmp_path
    )
    _install_ready_authorization(monkeypatch, validation)
    original_acquire = writer_module._acquire_lock

    def acquire_then_change(lock_path: Path, payload: dict) -> int:
        fd = original_acquire(lock_path, payload)
        preview.write_bytes(b'{"forged":true}\n')
        return fd

    monkeypatch.setattr(writer_module, "_acquire_lock", acquire_then_change)
    result = _execute(tmp_path, validation_path)
    assert result["overall_status"] == BLOCKED_STATUS
    assert any(
        "candidate_state_preview_sha256_changed" in reason
        for reason in result["failure_reasons"]
    )
    assert result["write_attempted"] is False
    assert state.read_bytes() == original


def test_atomic_writer_respects_existing_writer_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    validation_path, validation, state, _preview, original, _candidate = _fixture(
        tmp_path
    )
    _install_ready_authorization(monkeypatch, validation)
    lock = state.with_name(f"{state.name}.lock")
    lock.write_text("another-writer", encoding="utf-8")
    result = _execute(tmp_path, validation_path)
    assert result["overall_status"] == BLOCKED_STATUS
    assert "single_writer_lock_or_snapshot_exists" in result["failure_reasons"]
    assert state.read_bytes() == original
    assert lock.read_text(encoding="utf-8") == "another-writer"


def test_atomic_writer_rolls_back_on_post_replace_readback_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    validation_path, validation, state, _preview, original, _candidate = _fixture(
        tmp_path
    )
    _install_ready_authorization(monkeypatch, validation)

    def corrupt(path: Path) -> None:
        path.write_bytes(b'{"corrupt":true}\n')

    result = _execute(tmp_path, validation_path, after_replace_hook=corrupt)
    assert result["overall_status"] == ROLLED_BACK_STATUS
    assert result["write_attempted"] is True
    assert result["rollback_attempted"] is True
    assert result["rollback_confirmed"] is True
    assert result["final_state_matches_original"] is True
    assert result["authoritative_state_write_committed"] is False
    assert result["writes_authoritative_state"] is False
    assert state.read_bytes() == original


def test_atomic_writer_cli_requires_explicit_flag_and_exact_confirmation(
    tmp_path: Path, capsys
) -> None:
    validation_path, _validation, _state, _preview, _original, _candidate = _fixture(
        tmp_path
    )
    output = tmp_path / "cli-output"
    base = [
        "--state-advance-authorization-json",
        str(validation_path),
        "--authorization-id",
        AUTHORIZATION_ID,
        "--confirmation-template",
        CONFIRMATION_TEMPLATE,
        "--output-dir",
        str(output),
    ]
    assert tool.main(base) == 2
    assert "is locked" in capsys.readouterr().err
    assert not output.exists()

    with pytest.raises(SystemExit) as exc_info:
        tool.main([*base, "--execute"])
    assert exc_info.value.code == 2
    assert not output.exists()
    wrong = list(base)
    wrong[wrong.index(CONFIRMATION_TEMPLATE)] = "wrong-template"
    assert tool.main([*wrong, "--execute-controlled-state-advance"]) == 2
    assert "confirmation mismatch" in capsys.readouterr().err
    assert not output.exists()


def test_atomic_writer_entrypoint_is_manual_state_write_only() -> None:
    entry = classify_v1_5_entrypoint(
        ROOT
        / "src/gas_calibrator/tools/run_v1_5_authoritative_resume_offline_state_advance_atomic_writer.py",
        root=ROOT,
    )
    assert entry.category == "controlled_state_writer"
    assert entry.formal_status == "manual_authorized_only"
    assert entry.risk_level == "state_file_write_risk"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
