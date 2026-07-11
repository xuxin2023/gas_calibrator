import hashlib
import json
from pathlib import Path

import gas_calibrator.validation.v1_5_authoritative_resume_state_atomic_writer as atomic_writer_module
from gas_calibrator.tools.run_v1_5_authoritative_resume_state_atomic_writer import (
    main as writer_main,
)
from gas_calibrator.v1_5.orchestration.full_flow import build_full_flow_plan, write_full_flow_plan
from gas_calibrator.validation.v1_5_authoritative_resume_state_atomic_writer import (
    BLOCKED_STATUS,
    COMMITTED_STATUS,
    CONFIRMATION_TEMPLATE,
    ROLLED_BACK_STATUS,
    WRITER_AUTHORIZATION_OPERATION,
    WRITER_AUTHORIZATION_SCHEMA,
    execute_v1_5_authoritative_resume_state_atomic_write,
)
from gas_calibrator.validation.v1_5_authoritative_resume_state_controlled_write_preflight import (
    AUTHORIZATION_OPERATION,
    AUTHORIZATION_SCHEMA,
    CONFIRMATION_TEMPLATE as PREFLIGHT_CONFIRMATION_TEMPLATE,
    build_v1_5_authoritative_resume_state_controlled_write_preflight,
    write_v1_5_authoritative_resume_state_controlled_write_preflight_outputs,
)
from gas_calibrator.validation.v1_5_authoritative_resume_state_writer_blocked_executor import (
    build_v1_5_authoritative_resume_state_writer_blocked_executor,
    write_v1_5_authoritative_resume_state_writer_blocked_executor_outputs,
)
from gas_calibrator.validation.v1_5_authoritative_resume_state_writer_design import (
    write_v1_5_authoritative_resume_state_writer_design,
)
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_post_closeout_resume_gate import (
    write_v1_5_post_closeout_resume_gate,
)
from gas_calibrator.validation.v1_5_resume_prefix_application_review import (
    write_v1_5_resume_prefix_application_review,
)


ROOT = Path(__file__).resolve().parents[1]
AUTHORIZATION_ID = "resume-state-write-001"


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _ready_batch_payload() -> dict:
    return {
        "schema": "v1_5_batch_initialization_closeout_index_v1",
        "overall_status": "ready_for_mature_open_flow_from_initialization_index",
        "batch_initialization_closeout_ready": True,
        "ready_for_mature_open_flow_from_initialization_index": True,
        "device_count": 6,
        "device_ready_count": 6,
        "mature_route_baseline": "0620/0621 clean worktree mature physical route",
        "mature_fitting_baseline": "0613 V1.5 fitting path",
        "opens_com_ports": False,
        "read_only_real_com_execution_allowed": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "connects_postgresql": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "database_written": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
        "review_reasons": [],
    }


def _ready_preflight(
    tmp_path: Path,
    *,
    existing_state: dict | None = None,
) -> tuple[Path, Path, Path, bytes | None, Path]:
    config = _write_json(tmp_path / "config.json", {})
    root = tmp_path / "flow"
    plan = build_full_flow_plan(config_path=config, output_dir=root, run_id="atomic-writer-demo")
    plan_path = write_full_flow_plan(plan, root)["json"]
    batch_path = _write_json(
        root
        / "batch_initialization_closeout_index"
        / "v1_5_batch_initialization_closeout_index.json",
        _ready_batch_payload(),
    )
    gate_path = write_v1_5_post_closeout_resume_gate(
        output_dir=root / "post_closeout_resume_gate",
        full_flow_plan_json=plan_path,
        batch_initialization_closeout_json=batch_path,
    )["manifest"]
    application_path = write_v1_5_resume_prefix_application_review(
        output_dir=root / "resume_prefix_application_review",
        full_flow_plan_json=plan_path,
        post_closeout_resume_gate_json=gate_path,
    )["manifest"]
    design_path = write_v1_5_authoritative_resume_state_writer_design(
        output_dir=root / "authoritative_resume_state_writer_design",
        full_flow_plan_json=plan_path,
        resume_prefix_application_review_json=application_path,
    )["manifest"]
    blocked_model = build_v1_5_authoritative_resume_state_writer_blocked_executor(
        full_flow_plan_json=plan_path,
        resume_prefix_application_review_json=application_path,
        authoritative_resume_state_writer_design_json=design_path,
    )
    blocked_path = write_v1_5_authoritative_resume_state_writer_blocked_executor_outputs(
        blocked_model,
        root / "authoritative_resume_state_writer_blocked_executor",
    )["json"]
    target = root / "v1_5_full_flow_state.json"
    old_bytes: bytes | None = None
    if existing_state is not None:
        existing_state = dict(existing_state)
        existing_state.setdefault("schema", "v1_5_full_calibration_flow_state_v0")
        existing_state.setdefault("run_id", "atomic-writer-demo")
        target.write_text(
            json.dumps(existing_state, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        old_bytes = target.read_bytes()
    authorization_path = (
        root
        / "authoritative_resume_state_write_authorization"
        / "v1_5_authoritative_resume_state_write_authorization.json"
    )
    authorization = {
        "schema": AUTHORIZATION_SCHEMA,
        "requested_operation": AUTHORIZATION_OPERATION,
        "confirmation_template": PREFLIGHT_CONFIRMATION_TEMPLATE,
        "authorization_id": AUTHORIZATION_ID,
        "authorized_at": "2026-07-11T13:00:00Z",
        "operator": "operator-a",
        "reviewer": "reviewer-b",
        "approver": "approver-c",
        "preflight_only": True,
        "full_flow_plan_json": str(plan_path.resolve()),
        "full_flow_plan_sha256": _sha256(plan_path),
        "resume_prefix_application_review_json": str(application_path.resolve()),
        "resume_prefix_application_review_sha256": _sha256(application_path),
        "authoritative_resume_state_writer_design_json": str(design_path.resolve()),
        "authoritative_resume_state_writer_design_sha256": _sha256(design_path),
        "authoritative_resume_state_writer_blocked_executor_json": str(
            blocked_path.resolve()
        ),
        "authoritative_resume_state_writer_blocked_executor_sha256": _sha256(
            blocked_path
        ),
        "authoritative_state_json": str(target.resolve()),
        "expected_existing_state_sha256": _sha256(target) if target.exists() else "absent",
        "expected_candidate_state_sha256": "",
        "authoritative_state_write_allowed": False,
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "connects_postgresql": False,
        "database_import_allowed": False,
        "formal_release_allowed": False,
    }
    _write_json(authorization_path, authorization)
    first = build_v1_5_authoritative_resume_state_controlled_write_preflight(
        full_flow_plan_json=plan_path,
        resume_prefix_application_review_json=application_path,
        authoritative_resume_state_writer_design_json=design_path,
        authoritative_resume_state_writer_blocked_executor_json=blocked_path,
        authorization_packet_json=authorization_path,
    )
    authorization["expected_candidate_state_sha256"] = first["candidate_state_sha256"]
    _write_json(authorization_path, authorization)
    preflight = build_v1_5_authoritative_resume_state_controlled_write_preflight(
        full_flow_plan_json=plan_path,
        resume_prefix_application_review_json=application_path,
        authoritative_resume_state_writer_design_json=design_path,
        authoritative_resume_state_writer_blocked_executor_json=blocked_path,
        authorization_packet_json=authorization_path,
    )
    outputs = write_v1_5_authoritative_resume_state_controlled_write_preflight_outputs(
        preflight,
        root / "authoritative_resume_state_controlled_write_preflight",
    )
    assert preflight["controlled_write_preflight_ready"] is True
    writer_authorization_path = (
        root
        / "authoritative_resume_state_atomic_write_authorization"
        / "v1_5_resume_state_atomic_write_authorization.json"
    )
    _write_json(
        writer_authorization_path,
        {
            "schema": WRITER_AUTHORIZATION_SCHEMA,
            "requested_operation": WRITER_AUTHORIZATION_OPERATION,
            "confirmation_template": CONFIRMATION_TEMPLATE,
            "authorization_id": AUTHORIZATION_ID,
            "authorized_at": "2026-07-11T13:05:00Z",
            "operator": "operator-write-a",
            "reviewer": "reviewer-write-b",
            "approver": "approver-write-c",
            "authoritative_state_write_allowed": True,
            "preflight_json": str(outputs["json"].resolve()),
            "preflight_sha256": _sha256(outputs["json"]),
            "authoritative_state_json": str(target.resolve()),
            "expected_existing_state_sha256": preflight[
                "expected_existing_state_sha256"
            ],
            "candidate_state_sha256": preflight["candidate_state_sha256"],
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
    return (
        outputs["json"],
        target,
        outputs["candidate_preview"],
        old_bytes,
        writer_authorization_path,
    )


def test_atomic_writer_cli_is_locked_without_explicit_execute(tmp_path: Path, capsys) -> None:
    preflight, target, _candidate, _old, writer_auth = _ready_preflight(tmp_path)
    out = tmp_path / "writer"

    rc = writer_main(
        [
            "--preflight-json",
            str(preflight),
            "--writer-authorization-json",
            str(writer_auth),
            "--authorization-id",
            AUTHORIZATION_ID,
            "--confirmation-template",
            CONFIRMATION_TEMPLATE,
            "--output-dir",
            str(out),
        ]
    )

    assert rc == 2
    assert "writing is locked" in capsys.readouterr().err
    assert not out.exists()
    assert not target.exists()


def test_atomic_writer_cli_rejects_wrong_confirmation_before_artifact(
    tmp_path: Path, capsys
) -> None:
    preflight, target, _candidate, _old, writer_auth = _ready_preflight(tmp_path)
    out = tmp_path / "writer"
    rc = writer_main(
        [
            "--preflight-json",
            str(preflight),
            "--writer-authorization-json",
            str(writer_auth),
            "--authorization-id",
            AUTHORIZATION_ID,
            "--confirmation-template",
            "wrong",
            "--output-dir",
            str(out),
            "--execute-controlled-state-write",
        ]
    )
    assert rc == 2
    assert "confirmation template mismatch" in capsys.readouterr().err
    assert not out.exists()
    assert not target.exists()


def test_atomic_writer_creates_absent_state_from_exact_candidate(tmp_path: Path) -> None:
    preflight, target, candidate, _old, writer_auth = _ready_preflight(tmp_path)
    result = execute_v1_5_authoritative_resume_state_atomic_write(
        preflight_json=preflight,
        writer_authorization_json=writer_auth,
        authorization_id=AUTHORIZATION_ID,
        confirmation_template=CONFIRMATION_TEMPLATE,
        output_dir=tmp_path / "writer",
    )

    assert result["overall_status"] == COMMITTED_STATUS
    assert result["preflight_recomputed_ready"] is True
    assert result["current_state_sha256_rechecked"] is True
    assert result["single_writer_lock_acquired"] is True
    assert result["state_file_created"] is True
    assert result["state_file_replaced"] is False
    assert result["authoritative_state_write_committed"] is True
    assert target.read_bytes() == candidate.read_bytes()
    assert not Path(result["lock_path"]).exists()
    assert result["opens_com_ports"] is False
    assert result["connects_postgresql"] is False
    assert (tmp_path / "writer" / "v1_5_resume_state_atomic_write.json").exists()


def test_atomic_writer_replaces_existing_state_and_keeps_snapshot(tmp_path: Path) -> None:
    preflight, target, candidate, old_bytes, writer_auth = _ready_preflight(
        tmp_path,
        existing_state={"current_step_id": "old-step"},
    )
    result = execute_v1_5_authoritative_resume_state_atomic_write(
        preflight_json=preflight,
        writer_authorization_json=writer_auth,
        authorization_id=AUTHORIZATION_ID,
        confirmation_template=CONFIRMATION_TEMPLATE,
        output_dir=tmp_path / "writer",
    )

    assert result["overall_status"] == COMMITTED_STATUS
    assert result["state_file_created"] is False
    assert result["state_file_replaced"] is True
    assert result["state_snapshot_created"] is True
    snapshot = Path(result["rollback_snapshot_path"])
    assert snapshot.read_bytes() == old_bytes
    assert target.read_bytes() == candidate.read_bytes()


def test_atomic_writer_blocks_when_state_changes_after_preflight(tmp_path: Path) -> None:
    preflight, target, _candidate, _old, writer_auth = _ready_preflight(tmp_path)
    target.write_text(
        json.dumps(
            {
                "schema": "v1_5_full_calibration_flow_state_v0",
                "run_id": "atomic-writer-demo",
                "current_step_id": "external-change",
            }
        ),
        encoding="utf-8",
    )
    before = target.read_bytes()

    result = execute_v1_5_authoritative_resume_state_atomic_write(
        preflight_json=preflight,
        writer_authorization_json=writer_auth,
        authorization_id=AUTHORIZATION_ID,
        confirmation_template=CONFIRMATION_TEMPLATE,
        output_dir=tmp_path / "writer",
    )

    assert result["overall_status"] == BLOCKED_STATUS
    assert "current_state_sha256_changed_after_preflight" in result["failure_reasons"]
    assert result["write_attempted"] is False
    assert target.read_bytes() == before


def test_atomic_writer_respects_existing_single_writer_lock(tmp_path: Path) -> None:
    preflight, target, _candidate, _old, writer_auth = _ready_preflight(tmp_path)
    lock = target.with_name(f"{target.name}.lock")
    lock.write_text("owned-by-another-writer", encoding="utf-8")

    result = execute_v1_5_authoritative_resume_state_atomic_write(
        preflight_json=preflight,
        writer_authorization_json=writer_auth,
        authorization_id=AUTHORIZATION_ID,
        confirmation_template=CONFIRMATION_TEMPLATE,
        output_dir=tmp_path / "writer",
    )

    assert result["overall_status"] == BLOCKED_STATUS
    assert "single_writer_lock_or_snapshot_exists" in result["failure_reasons"]
    assert result["write_attempted"] is False
    assert lock.read_text(encoding="utf-8") == "owned-by-another-writer"
    assert not target.exists()


def test_atomic_writer_rolls_back_existing_state_on_readback_mismatch(tmp_path: Path) -> None:
    preflight, target, _candidate, old_bytes, writer_auth = _ready_preflight(
        tmp_path,
        existing_state={"current_step_id": "old-step"},
    )

    def corrupt(path: Path) -> None:
        path.write_text("corrupted-after-replace", encoding="utf-8")

    result = execute_v1_5_authoritative_resume_state_atomic_write(
        preflight_json=preflight,
        writer_authorization_json=writer_auth,
        authorization_id=AUTHORIZATION_ID,
        confirmation_template=CONFIRMATION_TEMPLATE,
        output_dir=tmp_path / "writer",
        after_replace_hook=corrupt,
    )

    assert result["overall_status"] == ROLLED_BACK_STATUS
    assert result["rollback_attempted"] is True
    assert result["rollback_confirmed"] is True
    assert result["authoritative_state_write_committed"] is False
    assert target.read_bytes() == old_bytes


def test_atomic_writer_rolls_back_absent_state_by_deleting_target(tmp_path: Path) -> None:
    preflight, target, _candidate, _old, writer_auth = _ready_preflight(tmp_path)

    def corrupt(path: Path) -> None:
        path.write_text("corrupted-after-create", encoding="utf-8")

    result = execute_v1_5_authoritative_resume_state_atomic_write(
        preflight_json=preflight,
        writer_authorization_json=writer_auth,
        authorization_id=AUTHORIZATION_ID,
        confirmation_template=CONFIRMATION_TEMPLATE,
        output_dir=tmp_path / "writer",
        after_replace_hook=corrupt,
    )

    assert result["overall_status"] == ROLLED_BACK_STATUS
    assert result["rollback_attempted"] is True
    assert result["rollback_confirmed"] is True
    assert not target.exists()


def test_atomic_writer_rejects_forged_preflight_and_candidate(tmp_path: Path) -> None:
    preflight, target, candidate, _old, writer_auth = _ready_preflight(tmp_path)
    payload = json.loads(preflight.read_text(encoding="utf-8"))
    payload["candidate_state_sha256"] = "0" * 64
    _write_json(preflight, payload)
    candidate.write_text("forged", encoding="utf-8")

    result = execute_v1_5_authoritative_resume_state_atomic_write(
        preflight_json=preflight,
        writer_authorization_json=writer_auth,
        authorization_id=AUTHORIZATION_ID,
        confirmation_template=CONFIRMATION_TEMPLATE,
        output_dir=tmp_path / "writer",
    )

    assert result["overall_status"] == BLOCKED_STATUS
    assert result["write_attempted"] is False
    assert not target.exists()
    assert any("candidate_preview_sha256_mismatch" in reason for reason in result["failure_reasons"])


def test_atomic_writer_rejects_preflight_only_authorization_as_write_authorization(
    tmp_path: Path,
) -> None:
    preflight, target, _candidate, _old, _writer_auth = _ready_preflight(tmp_path)
    preflight_payload = json.loads(preflight.read_text(encoding="utf-8"))

    result = execute_v1_5_authoritative_resume_state_atomic_write(
        preflight_json=preflight,
        writer_authorization_json=preflight_payload["authorization_packet_json"],
        authorization_id=AUTHORIZATION_ID,
        confirmation_template=CONFIRMATION_TEMPLATE,
        output_dir=tmp_path / "writer",
    )

    assert result["overall_status"] == BLOCKED_STATUS
    assert result["authorization_validated"] is False
    assert result["write_attempted"] is False
    assert not target.exists()
    assert any(
        reason.startswith("writer_authorization_schema=")
        for reason in result["failure_reasons"]
    )


def test_atomic_writer_rejects_non_distinct_write_authorization_reviewers(
    tmp_path: Path,
) -> None:
    preflight, target, _candidate, _old, writer_auth = _ready_preflight(tmp_path)
    payload = json.loads(writer_auth.read_text(encoding="utf-8"))
    payload["approver"] = payload["reviewer"]
    _write_json(writer_auth, payload)

    result = execute_v1_5_authoritative_resume_state_atomic_write(
        preflight_json=preflight,
        writer_authorization_json=writer_auth,
        authorization_id=AUTHORIZATION_ID,
        confirmation_template=CONFIRMATION_TEMPLATE,
        output_dir=tmp_path / "writer",
    )

    assert result["overall_status"] == BLOCKED_STATUS
    assert "writer_operator_reviewer_approver_must_be_distinct" in result[
        "failure_reasons"
    ]
    assert result["write_attempted"] is False
    assert not target.exists()


def test_atomic_writer_rechecks_state_after_acquiring_lock(
    tmp_path: Path, monkeypatch
) -> None:
    preflight, target, _candidate, _old, writer_auth = _ready_preflight(tmp_path)
    original_acquire_lock = atomic_writer_module._acquire_lock

    def acquire_then_change_state(lock_path: Path, payload: dict) -> int:
        fd = original_acquire_lock(lock_path, payload)
        target.write_text("external-change-after-lock\n", encoding="utf-8")
        return fd

    monkeypatch.setattr(
        atomic_writer_module,
        "_acquire_lock",
        acquire_then_change_state,
    )

    result = execute_v1_5_authoritative_resume_state_atomic_write(
        preflight_json=preflight,
        writer_authorization_json=writer_auth,
        authorization_id=AUTHORIZATION_ID,
        confirmation_template=CONFIRMATION_TEMPLATE,
        output_dir=tmp_path / "writer",
    )

    assert result["overall_status"] == BLOCKED_STATUS
    assert result["single_writer_lock_acquired"] is True
    assert "current_state_sha256_changed_after_preflight" in result["failure_reasons"]
    assert result["write_attempted"] is False
    assert target.read_text(encoding="utf-8") == "external-change-after-lock\n"
    assert not Path(result["lock_path"]).exists()


def test_atomic_writer_entrypoint_is_manual_state_write_only() -> None:
    entry = classify_v1_5_entrypoint(
        ROOT / "src/gas_calibrator/tools/run_v1_5_authoritative_resume_state_atomic_writer.py",
        root=ROOT,
    )
    assert entry.category == "controlled_state_writer"
    assert entry.formal_status == "manual_authorized_only"
    assert entry.risk_level == "state_file_write_risk"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
    assert any("atomic authoritative resume-state writer" in note for note in entry.notes)
