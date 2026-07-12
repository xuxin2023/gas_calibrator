import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from gas_calibrator.tools import (
    export_v1_5_authoritative_resume_offline_state_advance_authorization as auth_tool,
)
from gas_calibrator.tools import (
    run_v1_5_authoritative_resume_offline_state_advance_blocked_executor as blocked_tool,
)
from gas_calibrator.validation import (
    v1_5_authoritative_resume_offline_state_advance_authorization as auth_module,
)
from gas_calibrator.validation import (
    v1_5_authoritative_resume_offline_state_advance_blocked_executor as blocked_module,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_authorization import (
    AUTHORIZATION_OPERATION,
    AUTHORIZATION_SCHEMA,
    CONFIRMATION_TEMPLATE,
    READY_STATUS as AUTH_READY_STATUS,
    REVIEW_STATUS as AUTH_REVIEW_STATUS,
    build_v1_5_authoritative_resume_offline_state_advance_authorization,
    write_v1_5_authoritative_resume_offline_state_advance_authorization,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_blocked_executor import (
    BLOCKED_READY_STATUS,
    REVIEW_STATUS as BLOCKED_REVIEW_STATUS,
    build_v1_5_authoritative_resume_offline_state_advance_blocked_executor,
)
from gas_calibrator.validation.v1_5_authoritative_resume_offline_state_advance_preflight import (
    READY_STATUS as PREFLIGHT_READY_STATUS,
    SCHEMA as PREFLIGHT_SCHEMA,
)
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 7, 12, 12, 0, tzinfo=UTC)


def _write(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(
        (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    )
    return path


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fixture(tmp_path: Path) -> tuple[Path, dict, Path, Path, dict]:
    root = tmp_path / "flow"
    state_path = _write(
        root / "v1_5_full_flow_state.json",
        {
            "schema": "v1_5_full_calibration_flow_state_v0",
            "run_id": "run-107",
            "completed_step_ids": ["step-a"],
            "current_step_id": "step-b",
        },
    )
    candidate = {
        "schema": "v1_5_full_calibration_flow_state_v0",
        "run_id": "run-107",
        "completed_step_ids": ["step-a", "step-b"],
        "current_step_id": "step-c",
        "failed_step_ids": [],
        "allow_real_com": False,
        "allow_pressure_control": False,
        "allow_route_control": False,
        "allow_writes": False,
    }
    candidate_path = _write(
        root
        / "state-advance"
        / "v1_5_authoritative_resume_offline_state_candidate.json",
        candidate,
    )
    verifier_path = _write(root / "post-execution-verifier.json", {"ready": True})
    preflight = {
        "schema": PREFLIGHT_SCHEMA,
        "overall_status": PREFLIGHT_READY_STATUS,
        "offline_state_advance_preflight_ready": True,
        "blocker_count": 0,
        "blocker_reasons": [],
        "production_state": "offline_compare_and_swap_preflight_only",
        "offline_post_execution_verifier_json": str(verifier_path.resolve()),
        "offline_post_execution_verifier_sha256": _sha(verifier_path),
        "attempt_id": "attempt-107",
        "run_id": "run-107",
        "verified_step_id": "step-b",
        "verified_step_finished_at": "2026-07-12T11:59:30Z",
        "next_step_id_after_advance": "step-c",
        "full_flow_plan_json": str((root / "plan.json").resolve()),
        "full_flow_plan_sha256": "plan-sha",
        "authoritative_state_json": str(state_path.resolve()),
        "expected_current_state_sha256": _sha(state_path),
        "observed_current_state_sha256": _sha(state_path),
        "compare_and_swap_required": True,
        "candidate_state": candidate,
        "candidate_state_sha256": _sha(candidate_path),
        "candidate_state_preview_json": str(candidate_path.resolve()),
        "candidate_state_preview_sha256": _sha(candidate_path),
        "verified_outputs": [{"path": "result.json", "status": "ready"}],
        "execution_supported": False,
        "would_execute": False,
        "authoritative_state_write_allowed": False,
        "writes_authoritative_state": False,
        "state_file_created": False,
        "state_file_replaced": False,
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
    preflight_path = _write(
        candidate_path.parent
        / "v1_5_authoritative_resume_offline_state_advance_preflight.json",
        preflight,
    )
    packet = {
        "schema": AUTHORIZATION_SCHEMA,
        "requested_operation": AUTHORIZATION_OPERATION,
        "confirmation_template": CONFIRMATION_TEMPLATE,
        "authorization_id": "state-advance-107",
        "issued_at": "2026-07-12T11:55:00Z",
        "expires_at": "2026-07-12T12:15:00Z",
        "operator": "operator-a",
        "reviewer": "reviewer-b",
        "approver": "approver-c",
        "offline_state_advance_preflight_json": str(preflight_path.resolve()),
        "offline_state_advance_preflight_sha256": _sha(preflight_path),
        "authoritative_state_json": str(state_path.resolve()),
        "expected_current_state_sha256": _sha(state_path),
        "candidate_state_preview_json": str(candidate_path.resolve()),
        "candidate_state_sha256": _sha(candidate_path),
        "run_id": "run-107",
        "attempt_id": "attempt-107",
        "verified_step_id": "step-b",
        "next_step_id_after_advance": "step-c",
        "compare_and_swap_required": True,
        "structured_confirmation": {
            "exact_preflight_only": True,
            "one_verified_offline_step_only": True,
            "compare_and_swap_before_write": True,
            "atomic_replace_and_readback_required": True,
            "rollback_required": True,
            "no_com": True,
            "no_pressure_or_route": True,
            "no_device_or_coefficient_write": True,
            "no_postgresql_or_release": True,
        },
        "allow_authoritative_state_write": True,
        "allow_real_com": False,
        "allow_pressure_control": False,
        "allow_route_control": False,
        "allow_device_or_coefficient_write": False,
        "allow_postgresql_import": False,
    }
    packet_path = _write(root / "authorization.json", packet)
    return preflight_path, preflight, packet_path, state_path, packet


def _install_preflight(monkeypatch: pytest.MonkeyPatch, preflight: dict) -> None:
    monkeypatch.setattr(
        auth_module,
        "build_v1_5_authoritative_resume_offline_state_advance_preflight",
        lambda **_kwargs: dict(preflight),
    )


def _ready_authorization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[dict, Path, dict, Path, Path]:
    preflight_path, preflight, packet_path, state_path, packet = _fixture(tmp_path)
    _install_preflight(monkeypatch, preflight)
    model = build_v1_5_authoritative_resume_offline_state_advance_authorization(
        offline_state_advance_preflight_json=preflight_path,
        authorization_packet_json=packet_path,
        now=NOW,
    )
    return model, preflight_path, preflight, packet_path, state_path


def test_authorization_binds_exact_preflight_state_candidate_and_identities(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    model, _preflight_path, _preflight, _packet_path, _state = _ready_authorization(
        tmp_path, monkeypatch
    )
    assert model["overall_status"] == AUTH_READY_STATUS
    assert model["offline_state_advance_authorization_validated"] is True
    assert model["state_write_execution_allowed"] is False
    assert model["writes_authoritative_state"] is False
    assert model["opens_com_ports"] is False
    assert model["connects_postgresql"] is False


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("expires_at", "2026-07-12T11:59:00Z", "authorization_expired"),
        ("approver", "reviewer-b", "authorization_identities_must_be_distinct"),
        ("candidate_state_sha256", "0" * 64, "authorization_binding_mismatch:candidate_state_sha256"),
        ("allow_real_com", True, "authorization_capability_mismatch:allow_real_com"),
    ],
)
def test_authorization_rejects_expiry_identity_binding_or_capability_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: object,
    reason: str,
) -> None:
    preflight_path, preflight, packet_path, _state, packet = _fixture(tmp_path)
    packet[field] = value
    _write(packet_path, packet)
    _install_preflight(monkeypatch, preflight)
    model = build_v1_5_authoritative_resume_offline_state_advance_authorization(
        offline_state_advance_preflight_json=preflight_path,
        authorization_packet_json=packet_path,
        now=NOW,
    )
    assert model["overall_status"] == AUTH_REVIEW_STATUS
    assert reason in model["review_reasons"]
    assert model["state_write_execution_allowed"] is False


def test_authorization_rejects_state_or_candidate_changed_after_preflight(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    preflight_path, preflight, packet_path, state_path, _packet = _fixture(tmp_path)
    _install_preflight(monkeypatch, preflight)
    state_path.write_text('{"changed": true}\n', encoding="utf-8")
    candidate_path = Path(preflight["candidate_state_preview_json"])
    candidate_path.write_text('{"changed": true}\n', encoding="utf-8")
    model = build_v1_5_authoritative_resume_offline_state_advance_authorization(
        offline_state_advance_preflight_json=preflight_path,
        authorization_packet_json=packet_path,
        now=NOW,
    )
    assert model["overall_status"] == AUTH_REVIEW_STATUS
    assert "authoritative_state_compare_and_swap_sha256_changed" in model[
        "review_reasons"
    ]
    assert "candidate_state_preview_sha256_mismatch" in model["review_reasons"]


def test_authorization_rejects_reparse_authorization_parent_before_future_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    preflight_path, preflight, packet_path, _state, _packet = _fixture(tmp_path)
    _install_preflight(monkeypatch, preflight)
    original = auth_module._has_reparse_point
    monkeypatch.setattr(
        auth_module,
        "_has_reparse_point",
        lambda path: path == packet_path.parent or original(path),
    )
    model = build_v1_5_authoritative_resume_offline_state_advance_authorization(
        offline_state_advance_preflight_json=preflight_path,
        authorization_packet_json=packet_path,
        now=NOW,
    )
    assert model["overall_status"] == AUTH_REVIEW_STATUS
    assert "authorization_packet_or_parent_is_reparse_point" in model[
        "review_reasons"
    ]
    assert model["state_write_execution_allowed"] is False


def test_blocked_executor_accepts_ready_review_but_never_unlocks_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    auth_model, _preflight_path, _preflight, _packet_path, _state = (
        _ready_authorization(tmp_path, monkeypatch)
    )
    validation_path = write_v1_5_authoritative_resume_offline_state_advance_authorization(
        auth_model, tmp_path / "validation"
    )["json"]
    monkeypatch.setattr(
        blocked_module,
        "build_v1_5_authoritative_resume_offline_state_advance_authorization",
        lambda **_kwargs: dict(auth_model),
    )
    model = build_v1_5_authoritative_resume_offline_state_advance_blocked_executor(
        state_advance_authorization_json=validation_path,
        now=NOW,
    )
    assert model["overall_status"] == BLOCKED_READY_STATUS
    assert model["blocked_executor_ready"] is True
    assert model["execution_supported"] is False
    assert model["state_write_execution_allowed"] is False
    assert model["execute_flag_allowed"] is False
    assert model["writes_authoritative_state"] is False


def test_blocked_executor_rejects_recomputed_authorization_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    auth_model, _preflight_path, _preflight, _packet_path, _state = (
        _ready_authorization(tmp_path, monkeypatch)
    )
    validation_path = write_v1_5_authoritative_resume_offline_state_advance_authorization(
        auth_model, tmp_path / "validation"
    )["json"]
    changed = dict(auth_model)
    changed["candidate_state_sha256"] = "f" * 64
    monkeypatch.setattr(
        blocked_module,
        "build_v1_5_authoritative_resume_offline_state_advance_authorization",
        lambda **_kwargs: changed,
    )
    model = build_v1_5_authoritative_resume_offline_state_advance_blocked_executor(
        state_advance_authorization_json=validation_path,
        now=NOW,
    )
    assert model["overall_status"] == BLOCKED_REVIEW_STATUS
    assert "state_advance_authorization_recompute_mismatch:candidate_state_sha256" in model[
        "review_reasons"
    ]


def test_authorization_and_blocked_executor_clis_are_offline_and_reject_execute(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    auth_model, preflight_path, preflight, packet_path, _state = _ready_authorization(
        tmp_path, monkeypatch
    )
    monkeypatch.setattr(
        auth_tool,
        "build_v1_5_authoritative_resume_offline_state_advance_authorization",
        lambda **_kwargs: dict(auth_model),
    )
    auth_output = tmp_path / "auth-output"
    assert auth_tool.main(
        [
            "--offline-state-advance-preflight-json",
            str(preflight_path),
            "--authorization-packet-json",
            str(packet_path),
            "--output-dir",
            str(auth_output),
            "--fail-on-review-required",
        ]
    ) == 0
    validation_path = (
        auth_output / "v1_5_authoritative_resume_offline_state_advance_authorization.json"
    )
    monkeypatch.setattr(
        blocked_module,
        "build_v1_5_authoritative_resume_offline_state_advance_authorization",
        lambda **_kwargs: dict(auth_model),
    )
    blocked_model = build_v1_5_authoritative_resume_offline_state_advance_blocked_executor(
        state_advance_authorization_json=validation_path,
        now=NOW,
    )
    monkeypatch.setattr(
        blocked_tool,
        "build_v1_5_authoritative_resume_offline_state_advance_blocked_executor",
        lambda **_kwargs: dict(blocked_model),
    )
    blocked_output = tmp_path / "blocked-output"
    assert blocked_tool.main(
        [
            "--state-advance-authorization-json",
            str(validation_path),
            "--output-dir",
            str(blocked_output),
            "--fail-on-review-required",
        ]
    ) == 0
    assert json.loads(
        (
            blocked_output
            / "v1_5_authoritative_resume_offline_state_advance_blocked_executor.json"
        ).read_text(encoding="utf-8")
    )["state_write_execution_allowed"] is False

    for cli, args, output in (
        (
            auth_tool.main,
            [
                "--offline-state-advance-preflight-json",
                str(preflight_path),
                "--authorization-packet-json",
                str(packet_path),
            ],
            tmp_path / "auth-rejected",
        ),
        (
            blocked_tool.main,
            ["--state-advance-authorization-json", str(validation_path)],
            tmp_path / "blocked-rejected",
        ),
    ):
        with pytest.raises(SystemExit) as exc_info:
            cli([*args, "--output-dir", str(output), "--execute"])
        assert exc_info.value.code == 2
        assert not output.exists()

    for tool_path in (
        ROOT
        / "src/gas_calibrator/tools/export_v1_5_authoritative_resume_offline_state_advance_authorization.py",
        ROOT
        / "src/gas_calibrator/tools/run_v1_5_authoritative_resume_offline_state_advance_blocked_executor.py",
    ):
        entry = classify_v1_5_entrypoint(tool_path, root=ROOT)
        assert entry.category == "formal_review_evidence"
        assert entry.formal_status == "formal_support"
        assert entry.risk_level == "offline"
        assert entry.opens_com_ports is False

    assert preflight["offline_state_advance_preflight_ready"] is True
