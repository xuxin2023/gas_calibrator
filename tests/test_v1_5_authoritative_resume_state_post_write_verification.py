import hashlib
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_authoritative_resume_state_post_write_verification import main
from gas_calibrator.validation.v1_5_authoritative_resume_state_atomic_writer import (
    COMMITTED_STATUS,
    CONFIRMATION_TEMPLATE,
    NOOP_STATUS,
    SCHEMA as WRITER_SCHEMA,
    WRITER_AUTHORIZATION_OPERATION,
    WRITER_AUTHORIZATION_SCHEMA,
)
from gas_calibrator.validation.v1_5_authoritative_resume_state_controlled_write_preflight import (
    READY_STATUS as PREFLIGHT_READY_STATUS,
    SCHEMA as PREFLIGHT_SCHEMA,
)
from gas_calibrator.validation.v1_5_authoritative_resume_state_post_write_verification import (
    BLOCKED_STATUS,
    READY_STATUS,
    build_v1_5_authoritative_resume_state_post_write_verification,
)
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint


ROOT = Path(__file__).resolve().parents[1]


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _bundle(
    tmp_path: Path, *, status: str = COMMITTED_STATUS, replaced: bool = False
) -> tuple[Path, Path, Path, Path]:
    root = tmp_path / "flow"
    candidate = _write_json(
        root / "authoritative_resume_state_controlled_write_preflight" / "v1_5_resume_state_candidate_preview.json",
        {"schema": "v1_5_full_calibration_flow_state_v0", "run_id": "run-95", "current_step_id": "temperature_channel_fast_review"},
    )
    target = root / "v1_5_full_flow_state.json"
    target.write_bytes(candidate.read_bytes())
    candidate_sha = _sha(candidate)
    snapshot = root / "v1_5_full_flow_state.json.rollback.write-95.snapshot"
    if replaced:
        snapshot.write_text("previous-state\n", encoding="utf-8")
    preflight = _write_json(
        candidate.parent / "v1_5_resume_state_write_preflight.json",
        {
            "schema": PREFLIGHT_SCHEMA,
            "overall_status": PREFLIGHT_READY_STATUS,
            "controlled_write_preflight_ready": True,
            "candidate_state_sha256": candidate_sha,
            "candidate_state_preview_json": str(candidate.resolve()),
            "authoritative_state_json_read_only": str(target.resolve()),
        },
    )
    authorization = _write_json(
        root / "authoritative_resume_state_atomic_write_authorization" / "v1_5_resume_state_atomic_write_authorization.json",
        {
            "schema": WRITER_AUTHORIZATION_SCHEMA,
            "requested_operation": WRITER_AUTHORIZATION_OPERATION,
            "authorization_id": "write-95",
            "confirmation_template": CONFIRMATION_TEMPLATE,
            "operator": "operator-a",
            "reviewer": "reviewer-b",
            "approver": "approver-c",
            "authoritative_state_write_allowed": True,
            "preflight_json": str(preflight.resolve()),
            "preflight_sha256": _sha(preflight),
            "authoritative_state_json": str(target.resolve()),
            "candidate_state_sha256": candidate_sha,
            "opens_com_ports": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "connects_postgresql": False,
            "database_import_allowed": False,
            "formal_release_allowed": False,
            "not_real_acceptance_evidence": True,
        },
    )
    writer = _write_json(
        root / "atomic_writer" / "v1_5_resume_state_atomic_write.json",
        {
            "schema": WRITER_SCHEMA,
            "overall_status": status,
            "preflight_recomputed_ready": True,
            "authorization_validated": True,
            "current_state_sha256_rechecked": True,
            "single_writer_lock_acquired": True,
            "authoritative_state_write_committed": status in {COMMITTED_STATUS, NOOP_STATUS},
            "failure_reasons": [],
            "preflight_json": str(preflight.resolve()),
            "preflight_sha256": _sha(preflight),
            "writer_authorization_json": str(authorization.resolve()),
            "writer_authorization_sha256": _sha(authorization),
            "authorization_id": "write-95",
            "authoritative_state_json": str(target.resolve()),
            "candidate_state_preview_json": str(candidate.resolve()),
            "candidate_state_sha256": candidate_sha,
            "post_write_readback_sha256": candidate_sha,
            "lock_path": str(target.with_name(target.name + ".lock")),
            "state_file_created": status == COMMITTED_STATUS and not replaced,
            "state_file_replaced": status == COMMITTED_STATUS and replaced,
            "state_snapshot_created": replaced,
            "rollback_snapshot_path": str(snapshot.resolve()) if replaced else "",
            "rollback_snapshot_sha256": _sha(snapshot) if replaced else "",
            "rollback_attempted": False,
            "rollback_confirmed": False,
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
        },
    )
    return writer, target, candidate, authorization


def test_post_write_verification_accepts_exact_committed_state(tmp_path: Path) -> None:
    writer, target, candidate, _authorization = _bundle(tmp_path)
    model = build_v1_5_authoritative_resume_state_post_write_verification(atomic_write_json=writer)
    assert model["overall_status"] == READY_STATUS
    assert model["post_write_verification_ready"] is True
    assert model["authoritative_state_sha256"] == _sha(target) == _sha(candidate)
    assert model["writes_authoritative_state"] is False


def test_post_write_verification_accepts_exact_noop_state(tmp_path: Path) -> None:
    writer, _target, _candidate, _authorization = _bundle(tmp_path, status=NOOP_STATUS)
    model = build_v1_5_authoritative_resume_state_post_write_verification(atomic_write_json=writer)
    assert model["overall_status"] == READY_STATUS
    assert model["post_write_verification_ready"] is True


def test_post_write_verification_requires_intact_replacement_snapshot(
    tmp_path: Path,
) -> None:
    writer, _target, _candidate, _authorization = _bundle(tmp_path, replaced=True)
    payload = json.loads(writer.read_text(encoding="utf-8"))
    snapshot = Path(payload["rollback_snapshot_path"])
    assert build_v1_5_authoritative_resume_state_post_write_verification(
        atomic_write_json=writer
    )["overall_status"] == READY_STATUS
    snapshot.write_text("tampered-snapshot\n", encoding="utf-8")
    model = build_v1_5_authoritative_resume_state_post_write_verification(
        atomic_write_json=writer
    )
    assert "replacement_snapshot_sha256_mismatch" in model["blocker_reasons"]


def test_post_write_verification_blocks_tampered_state(tmp_path: Path) -> None:
    writer, target, _candidate, _authorization = _bundle(tmp_path)
    target.write_text("tampered\n", encoding="utf-8")
    model = build_v1_5_authoritative_resume_state_post_write_verification(atomic_write_json=writer)
    assert model["overall_status"] == BLOCKED_STATUS
    assert "authoritative_state_sha256_mismatch" in model["blocker_reasons"]


def test_post_write_verification_blocks_tampered_candidate(tmp_path: Path) -> None:
    writer, _target, candidate, _authorization = _bundle(tmp_path)
    candidate.write_text("tampered\n", encoding="utf-8")
    model = build_v1_5_authoritative_resume_state_post_write_verification(atomic_write_json=writer)
    assert "candidate_preview_sha256_mismatch" in model["blocker_reasons"]


def test_post_write_verification_blocks_changed_authorization(tmp_path: Path) -> None:
    writer, _target, _candidate, authorization = _bundle(tmp_path)
    payload = json.loads(authorization.read_text(encoding="utf-8"))
    payload["authoritative_state_write_allowed"] = False
    _write_json(authorization, payload)
    model = build_v1_5_authoritative_resume_state_post_write_verification(atomic_write_json=writer)
    assert "writer_authorization_sha256_mismatch" in model["blocker_reasons"]
    assert "writer_authorization_write_not_allowed" in model["blocker_reasons"]


def test_post_write_verification_independently_rejects_unsafe_authorization(
    tmp_path: Path,
) -> None:
    writer, _target, _candidate, authorization = _bundle(tmp_path)
    authorization_payload = json.loads(authorization.read_text(encoding="utf-8"))
    authorization_payload["opens_com_ports"] = True
    _write_json(authorization, authorization_payload)
    writer_payload = json.loads(writer.read_text(encoding="utf-8"))
    writer_payload["writer_authorization_sha256"] = _sha(authorization)
    _write_json(writer, writer_payload)
    model = build_v1_5_authoritative_resume_state_post_write_verification(
        atomic_write_json=writer
    )
    assert "writer_authorization_boundary_opens_com_ports_not_false" in model[
        "blocker_reasons"
    ]


def test_post_write_verification_rejects_rolled_back_writer(tmp_path: Path) -> None:
    writer, _target, _candidate, _authorization = _bundle(tmp_path, status="authoritative_resume_state_write_rolled_back")
    model = build_v1_5_authoritative_resume_state_post_write_verification(atomic_write_json=writer)
    assert model["overall_status"] == BLOCKED_STATUS
    assert any(reason.startswith("atomic_write_status=") for reason in model["blocker_reasons"])


def test_post_write_verification_blocks_present_lock(tmp_path: Path) -> None:
    writer, target, _candidate, _authorization = _bundle(tmp_path)
    target.with_name(target.name + ".lock").write_text("locked", encoding="utf-8")
    model = build_v1_5_authoritative_resume_state_post_write_verification(atomic_write_json=writer)
    assert "writer_lock_still_present" in model["blocker_reasons"]


def test_post_write_verification_cli_writes_offline_outputs(tmp_path: Path) -> None:
    writer, _target, _candidate, _authorization = _bundle(tmp_path)
    out = tmp_path / "review"
    assert main(["--atomic-write-json", str(writer), "--output-dir", str(out), "--fail-on-blocker"]) == 0
    assert (out / "v1_5_resume_state_post_write_verification.json").is_file()
    assert (out / "v1_5_resume_state_post_write_verification_summary.csv").is_file()


def test_post_write_verification_entrypoint_is_offline_support() -> None:
    entry = classify_v1_5_entrypoint(
        ROOT / "src/gas_calibrator/tools/export_v1_5_authoritative_resume_state_post_write_verification.py",
        root=ROOT,
    )
    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
