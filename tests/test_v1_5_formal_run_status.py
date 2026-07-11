import csv
import hashlib
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_formal_run_status import main as export_status_main
from gas_calibrator.validation.v1_5_authoritative_resume_state_writer_design import (
    write_v1_5_authoritative_resume_state_writer_design,
)
from gas_calibrator.validation.v1_5_authoritative_resume_state_writer_blocked_executor import (
    build_v1_5_authoritative_resume_state_writer_blocked_executor,
    write_v1_5_authoritative_resume_state_writer_blocked_executor_outputs,
)
from gas_calibrator.validation.v1_5_authoritative_resume_state_controlled_write_preflight import (
    AUTHORIZATION_OPERATION,
    AUTHORIZATION_SCHEMA,
    CONFIRMATION_TEMPLATE,
    build_v1_5_authoritative_resume_state_controlled_write_preflight,
    write_v1_5_authoritative_resume_state_controlled_write_preflight_outputs,
)
from gas_calibrator.validation.v1_5_formal_run_status import (
    build_v1_5_formal_run_status,
    render_v1_5_formal_run_status_markdown,
    write_v1_5_formal_run_status_outputs,
)
from gas_calibrator.validation.v1_5_senco_artifact_authorization import (
    WRITER_SCOPES,
    write_senco_artifact_authorization,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _seed_senco_artifact_authorization(root: Path) -> Path:
    output_dir = root / "main_senco_write_precheck"
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest = _write_json(
        output_dir / "main_senco_artifact_hash_manifest.json",
        {"schema": "v1_5_final_senco_artifact_hash_manifest_v1", "artifacts": []},
    )
    return write_senco_artifact_authorization(
        output_dir / "main_senco_artifact_authorization.json",
        manifest_path=manifest,
        reviewer="reviewer-a",
        approver="approver-b",
        authorization_id="AUTH-READY-001",
        authorized_writer_scopes=WRITER_SCOPES,
        authorized_device_ids=("001", "002", "003", "004", "005", "006"),
    )


def _seed_ready_run(
    root: Path,
    *,
    include_mature_route_continuity_gate: bool = True,
    include_senco_artifact_authorization: bool = True,
) -> None:
    _write_json(
        root / "initialization" / "v1_5_initialization_readiness.json",
        {
            "schema": "v1_5_initialization_readiness_v1",
            "readiness_status": "ready_for_open_flow_main_calibration",
        },
    )
    _write_json(
        root / "identity" / "v1_5_getco_identity_readiness.json",
        {
            "schema": "v1_5_getco_identity_readiness_v1",
            "overall_status": "identity_getco_ready_for_auxiliary_neutralization",
            "traceability_review_required": False,
        },
    )
    _write_json(
        root / "pre_gas" / "v1_5_pre_gas_readiness.json",
        {
            "schema": "v1_5_pre_gas_readiness_v1",
            "overall_status": "ready_for_open_flow_from_sidecar_evidence",
        },
    )
    _write_json(
        root / "run_evidence" / "v1_5_run_evidence_status.json",
        {
            "schema": "v1_5_run_evidence_status_v1",
            "overall_status": "ready_for_reviewer",
            "stage_statuses": [
                {"stage_id": "pressure_quick_check", "status": "pass"},
                {"stage_id": "co2_open_flow", "status": "pass"},
                {"stage_id": "h2o_open_flow", "status": "pass"},
                {"stage_id": "candidate_review", "status": "pass"},
                {"stage_id": "post_run_coefficient_executor", "status": "pass"},
                {"stage_id": "post_write_reverification", "status": "pass"},
            ],
        },
    )
    _write_json(
        root / "closure" / "v1_5_full_flow_closure_readiness.json",
        {
            "schema": "v1_5_full_flow_closure_readiness_v1",
            "overall_status": "ready_for_controlled_write_review",
            "release_status": "ready_for_formal_release",
            "gaps": [],
        },
    )
    _write_json(
        root / "archive" / "v1_5_formal_archive_closure_index.json",
        {
            "schema": "v1_5_formal_archive_closure_v1",
            "overall_status": "ready",
            "package_status": "ready",
            "database": {"mode": "import", "database_imported": True},
            "identity_getco_traceability": {
                "status": "ready",
                "ready_for_archive_release": True,
                "traceability_review_required": False,
            },
            "senco_authorization_write_traceability": {
                "overall_status": "not_applicable_no_main_senco_write_evidence",
                "ready_for_archive_release": True,
                "write_evidence_present": False,
            },
        },
    )
    if include_mature_route_continuity_gate:
        _seed_mature_route_continuity_gate(root)
    if include_senco_artifact_authorization:
        _seed_senco_artifact_authorization(root)


def _seed_pressure_s9_readiness_index(root: Path, *, ready: bool = True) -> Path:
    status = "ready_for_mature_open_flow_pressure_s9_index" if ready else "review_required"
    return _write_json(
        root / "pressure_s9_readiness_index" / "v1_5_pressure_s9_readiness_index.json",
        {
            "schema": "v1_5_pressure_s9_readiness_index_v1",
            "overall_status": status,
            "ready_for_mature_open_flow_pressure_s9_index": ready,
            "device_count": 6,
            "device_ready_count": 6 if ready else 5,
            "review_reasons": [] if ready else ["one_or_more_devices_missing_post_write_pressure_reverify"],
            "opens_com_ports": False,
            "read_only_real_com_execution_allowed": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "connects_postgresql": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "writes_senco9": False,
            "formal_release_allowed": False,
            "database_import_allowed": False,
            "database_written": False,
            "not_real_acceptance_evidence": True,
            "device_rows": [
                {
                    "ga_label": f"GA{idx:02d}",
                    "protocol_device_id": f"{idx:03d}",
                    "sn_code": f"012607{idx:02d}",
                    "readiness_status": "pass" if ready else ("review_required" if idx == 4 else "pass"),
                    "can_enter_open_flow_main_calibration": ready or idx != 4,
                    "s9_model": "linear_s9_controlled_exception" if idx == 4 else "offset_only",
                }
                for idx in range(1, 7)
            ],
        },
    )


def _seed_batch_initialization_closeout(root: Path, *, ready: bool = True) -> Path:
    status = "ready_for_mature_open_flow_from_initialization_index" if ready else "review_required"
    return _write_json(
        root
        / "batch_initialization_closeout_index"
        / "v1_5_batch_initialization_closeout_index.json",
        {
            "schema": "v1_5_batch_initialization_closeout_index_v1",
            "overall_status": status,
            "batch_initialization_closeout_ready": ready,
            "ready_for_mature_open_flow_from_initialization_index": ready,
            "device_count": 6,
            "device_ready_count": 6 if ready else 5,
            "review_reasons": [] if ready else ["one_or_more_devices_not_ready_for_pre_gas_index"],
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
        },
    )


def _seed_post_closeout_resume_gate(root: Path, *, ready: bool = True) -> Path:
    plan_path = _write_json(
        root / "v1_5_full_flow_plan.json",
        {
            "schema": "v1_5_full_calibration_flow_plan_v0",
            "contract": "pressure_first_temperature_review_then_open_flow_components",
            "run_id": "status-test",
            "created_at": "2026-07-11T12:00:00Z",
            "config_path": str((root / "config.json").resolve()),
            "output_dir": str(root.resolve()),
            "dry_run_only": True,
            "safety_contract": {},
            "coefficient_epoch_contract": {},
            "physical_order": [],
            "steps": [
                {"step_id": "batch_initialization_closeout_index"},
                {"step_id": "post_closeout_resume_gate_snapshot"},
                {
                    "step_id": "post_closeout_resume_prefix_application_review",
                    "tool_module": (
                        "gas_calibrator.tools."
                        "export_v1_5_resume_prefix_application_review"
                    ),
                    "command": [
                        "python",
                        "-m",
                        "gas_calibrator.tools.export_v1_5_resume_prefix_application_review",
                        "--full-flow-plan-json",
                        str((root / "v1_5_full_flow_plan.json").resolve()),
                        "--post-closeout-resume-gate-json",
                        str(
                            (
                                root
                                / "post_closeout_resume_gate"
                                / "v1_5_post_closeout_resume_gate.json"
                            ).resolve()
                        ),
                        "--output-dir",
                        str((root / "resume_prefix_application_review").resolve()),
                        "--fail-on-blocked",
                    ],
                    "execution_mode": "offline_sidecar",
                },
                {
                    "step_id": "authoritative_resume_state_writer_design",
                    "tool_module": (
                        "gas_calibrator.tools."
                        "export_v1_5_authoritative_resume_state_writer_design"
                    ),
                    "command": [
                        "python",
                        "-m",
                        "gas_calibrator.tools.export_v1_5_authoritative_resume_state_writer_design",
                        "--full-flow-plan-json",
                        str((root / "v1_5_full_flow_plan.json").resolve()),
                        "--resume-prefix-application-review-json",
                        str(
                            (
                                root
                                / "resume_prefix_application_review"
                                / "v1_5_resume_prefix_application_review.json"
                            ).resolve()
                        ),
                        "--output-dir",
                        str((root / "authoritative_resume_state_writer_design").resolve()),
                        "--fail-on-blocked",
                    ],
                    "execution_mode": "offline_sidecar",
                },
                {
                    "step_id": "authoritative_resume_state_writer_blocked_executor",
                    "tool_module": (
                        "gas_calibrator.tools."
                        "run_v1_5_authoritative_resume_state_writer_blocked_executor"
                    ),
                    "command": [
                        "python",
                        "-m",
                        "gas_calibrator.tools.run_v1_5_authoritative_resume_state_writer_blocked_executor",
                        "--full-flow-plan-json",
                        str((root / "v1_5_full_flow_plan.json").resolve()),
                        "--resume-prefix-application-review-json",
                        str(
                            (
                                root
                                / "resume_prefix_application_review"
                                / "v1_5_resume_prefix_application_review.json"
                            ).resolve()
                        ),
                        "--authoritative-resume-state-writer-design-json",
                        str(
                            (
                                root
                                / "authoritative_resume_state_writer_design"
                                / "v1_5_authoritative_resume_state_writer_design.json"
                            ).resolve()
                        ),
                        "--output-dir",
                        str(
                            (
                                root
                                / "authoritative_resume_state_writer_blocked_executor"
                            ).resolve()
                        ),
                        "--fail-on-blocked",
                    ],
                    "execution_mode": "offline_blocked_stub",
                },
                {
                    "step_id": "authoritative_resume_state_controlled_write_preflight",
                    "tool_module": (
                        "gas_calibrator.tools."
                        "export_v1_5_authoritative_resume_state_controlled_write_preflight"
                    ),
                    "command": [
                        "python",
                        "-m",
                        "gas_calibrator.tools.export_v1_5_authoritative_resume_state_controlled_write_preflight",
                        "--full-flow-plan-json",
                        str((root / "v1_5_full_flow_plan.json").resolve()),
                        "--resume-prefix-application-review-json",
                        str(
                            (
                                root
                                / "resume_prefix_application_review"
                                / "v1_5_resume_prefix_application_review.json"
                            ).resolve()
                        ),
                        "--authoritative-resume-state-writer-design-json",
                        str(
                            (
                                root
                                / "authoritative_resume_state_writer_design"
                                / "v1_5_authoritative_resume_state_writer_design.json"
                            ).resolve()
                        ),
                        "--authoritative-resume-state-writer-blocked-executor-json",
                        str(
                            (
                                root
                                / "authoritative_resume_state_writer_blocked_executor"
                                / "v1_5_authoritative_resume_state_writer_blocked_executor.json"
                            ).resolve()
                        ),
                        "--authorization-packet-json",
                        str(
                            (
                                root
                                / "authoritative_resume_state_write_authorization"
                                / "v1_5_authoritative_resume_state_write_authorization.json"
                            ).resolve()
                        ),
                        "--output-dir",
                        str(
                            (
                                root
                                / "authoritative_resume_state_controlled_write_preflight"
                            ).resolve()
                        ),
                        "--fail-on-blocker",
                        "--fail-on-review-required",
                    ],
                    "execution_mode": "offline_sidecar",
                },
                {"step_id": "temperature_channel_fast_review"},
                {"step_id": "co2_open_flow_sampling"},
                {"step_id": "h2o_open_flow_sampling"},
            ],
        },
    )
    batch_path = root / "batch_initialization_closeout_index" / "v1_5_batch_initialization_closeout_index.json"
    if not batch_path.exists():
        _seed_batch_initialization_closeout(root, ready=ready)
    resume_path = _write_json(
        root / "post_closeout_resume_gate" / "v1_5_post_closeout_resume_gate.json",
        {
            "schema": "v1_5_post_closeout_resume_gate_v1",
            "overall_status": "ready_for_post_closeout_resume_review" if ready else "blocked",
            "resume_gate_ready": ready,
            "ready_for_resume_state_application_review": ready,
            "next_step_id": "temperature_channel_fast_review" if ready else "",
            "run_id": "status-test",
            "resume_completed_step_ids": (
                ["batch_initialization_closeout_index", "post_closeout_resume_gate_snapshot"]
                if ready
                else []
            ),
            "resume_cli_arguments": (
                [
                    "--completed-step",
                    "batch_initialization_closeout_index",
                    "--completed-step",
                    "post_closeout_resume_gate_snapshot",
                ]
                if ready
                else []
            ),
            "review_reasons": [] if ready else ["full_flow_plan_hash_mismatch"],
            "full_flow_plan_json": str(plan_path.resolve()),
            "full_flow_plan_sha256": hashlib.sha256(plan_path.read_bytes()).hexdigest(),
            "batch_initialization_closeout_json": str(batch_path.resolve()),
            "batch_initialization_closeout_sha256": hashlib.sha256(batch_path.read_bytes()).hexdigest(),
            "does_not_execute_commands": True,
            "applies_completed_steps": False,
            "live_resume_execution_allowed": False,
            "route_authorization_still_required": True,
            "opens_com_ports": False,
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
        },
    )
    application_path = _seed_resume_prefix_application_review(
        root, resume_path=resume_path, ready=ready
    )
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
    authorization_path = (
        root
        / "authoritative_resume_state_write_authorization"
        / "v1_5_authoritative_resume_state_write_authorization.json"
    )
    authorization = {
        "schema": AUTHORIZATION_SCHEMA,
        "requested_operation": AUTHORIZATION_OPERATION,
        "confirmation_template": CONFIRMATION_TEMPLATE,
        "authorization_id": "status-resume-state-001",
        "authorized_at": "2026-07-11T12:00:00Z",
        "operator": "operator-a",
        "reviewer": "reviewer-b",
        "approver": "approver-c",
        "preflight_only": True,
        "full_flow_plan_json": str(plan_path.resolve()),
        "full_flow_plan_sha256": hashlib.sha256(plan_path.read_bytes()).hexdigest(),
        "resume_prefix_application_review_json": str(application_path.resolve()),
        "resume_prefix_application_review_sha256": hashlib.sha256(
            application_path.read_bytes()
        ).hexdigest(),
        "authoritative_resume_state_writer_design_json": str(design_path.resolve()),
        "authoritative_resume_state_writer_design_sha256": hashlib.sha256(
            design_path.read_bytes()
        ).hexdigest(),
        "authoritative_resume_state_writer_blocked_executor_json": str(
            blocked_path.resolve()
        ),
        "authoritative_resume_state_writer_blocked_executor_sha256": hashlib.sha256(
            blocked_path.read_bytes()
        ).hexdigest(),
        "authoritative_state_json": str(
            (root / "v1_5_full_flow_state.json").resolve()
        ),
        "expected_existing_state_sha256": "absent",
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
    preflight = build_v1_5_authoritative_resume_state_controlled_write_preflight(
        full_flow_plan_json=plan_path,
        resume_prefix_application_review_json=application_path,
        authoritative_resume_state_writer_design_json=design_path,
        authoritative_resume_state_writer_blocked_executor_json=blocked_path,
        authorization_packet_json=authorization_path,
    )
    authorization["expected_candidate_state_sha256"] = preflight[
        "candidate_state_sha256"
    ]
    _write_json(authorization_path, authorization)
    preflight = build_v1_5_authoritative_resume_state_controlled_write_preflight(
        full_flow_plan_json=plan_path,
        resume_prefix_application_review_json=application_path,
        authoritative_resume_state_writer_design_json=design_path,
        authoritative_resume_state_writer_blocked_executor_json=blocked_path,
        authorization_packet_json=authorization_path,
    )
    write_v1_5_authoritative_resume_state_controlled_write_preflight_outputs(
        preflight,
        root / "authoritative_resume_state_controlled_write_preflight",
    )
    return resume_path


def _seed_resume_prefix_application_review(
    root: Path,
    *,
    resume_path: Path,
    ready: bool = True,
) -> Path:
    resume = json.loads(resume_path.read_text(encoding="utf-8"))
    plan_path = Path(resume["full_flow_plan_json"])
    return _write_json(
        root / "resume_prefix_application_review" / "v1_5_resume_prefix_application_review.json",
        {
            "schema": "v1_5_resume_prefix_application_review_v1",
            "overall_status": "ready_for_resume_prefix_state_application_review" if ready else "blocked",
            "resume_prefix_application_review_ready": ready,
            "resume_prefix_consumed_for_review": ready,
            "state_preview_current_step_id": (
                "authoritative_resume_state_writer_design" if ready else ""
            ),
            "state_preview_current_status": (
                "ready_for_offline_review" if ready else "blocked"
            ),
            "downstream_route_step_ids": [
                "co2_open_flow_sampling",
                "h2o_open_flow_sampling",
            ],
            "run_id": "status-test",
            "reviewed_resume_completed_step_ids": (
                ["batch_initialization_closeout_index", "post_closeout_resume_gate_snapshot"]
                if ready
                else []
            ),
            "reviewed_completed_step_ids_after_application": (
                [
                    "batch_initialization_closeout_index",
                    "post_closeout_resume_gate_snapshot",
                    "post_closeout_resume_prefix_application_review",
                ]
                if ready
                else []
            ),
            "reviewed_resume_cli_arguments": (
                [
                    "--completed-step",
                    "batch_initialization_closeout_index",
                    "--completed-step",
                    "post_closeout_resume_gate_snapshot",
                ]
                if ready
                else []
            ),
            "reviewed_state_application_cli_arguments": (
                [
                    "--completed-step",
                    "batch_initialization_closeout_index",
                    "--completed-step",
                    "post_closeout_resume_gate_snapshot",
                    "--completed-step",
                    "post_closeout_resume_prefix_application_review",
                ]
                if ready
                else []
            ),
            "review_reasons": [] if ready else ["resume_gate_status_not_ready"],
            "full_flow_plan_json": str(plan_path.resolve()),
            "full_flow_plan_sha256": hashlib.sha256(plan_path.read_bytes()).hexdigest(),
            "post_closeout_resume_gate_json": str(resume_path.resolve()),
            "post_closeout_resume_gate_sha256": hashlib.sha256(resume_path.read_bytes()).hexdigest(),
            "batch_initialization_closeout_json": str(resume["batch_initialization_closeout_json"]),
            "batch_initialization_closeout_sha256": str(
                resume["batch_initialization_closeout_sha256"]
            ),
            "does_not_execute_commands": True,
            "applies_completed_steps": False,
            "writes_authoritative_state": False,
            "would_execute": False,
            "live_resume_execution_allowed": False,
            "route_authorization_still_required": True,
            "opens_com_ports": False,
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
        },
    )


def _seed_algorithm_profile_runner_dry_run(root: Path, *, blocker_count: int = 0) -> Path:
    status = "ready_for_profile_driven_runner_dry_run_review" if blocker_count == 0 else "blocked"
    return _write_json(
        root / "algorithm_profile_runner_dry_run" / "v1_5_algorithm_profile_runner_dry_run.json",
        {
            "schema": "v1_5_algorithm_profile_runner_dry_run_v1",
            "overall_status": status,
            "blocker_count": blocker_count,
            "profile_id": "absorption_ratio_shadow",
            "co2_runlist_count": 47,
            "h2o_runlist_count": 14,
            "opens_com_ports": False,
            "connects_postgresql": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "writes_device_id": False,
            "does_not_execute_commands": True,
            "does_not_modify_runners": True,
            "not_real_acceptance_evidence": True,
        },
    )


def _seed_full_flow_automation_closure(
    root: Path,
    *,
    blocker_count: int = 0,
    remaining_gap_count: int = 9,
    boundary_clean: bool = True,
) -> Path:
    status = "review_ready" if blocker_count == 0 else "blocked"
    return _write_json(
        root / "full_flow_automation_closure" / "v1_5_full_flow_automation_closure.json",
        {
            "schema": "v1_5_full_flow_automation_closure_v1",
            "overall_status": status,
            "automation_closure_status": "structure_closed_live_full_auto_still_gated",
            "mature_fitting_baseline": "0613 V1.5 fitting path",
            "mature_route_baseline": "0620/0621 clean-worktree mature physical route path",
            "legacy_point_counts": {"co2": 45, "h2o": 13},
            "new_algorithm_profile_point_counts": {"co2": 47, "h2o": 14},
            "blocker_count": blocker_count,
            "remaining_full_auto_gap_count": remaining_gap_count,
            "full_production_auto_allowed": False,
            "formal_release_allowed": False,
            "database_import_allowed": False,
            "opens_com_ports": not boundary_clean,
            "connects_postgresql": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
            "not_real_acceptance_evidence": True,
        },
    )


def _seed_route_physical_recovery_readiness(
    root: Path,
    *,
    status: str = "pass",
    blocker_count: int = 0,
    review_required_count: int = 0,
    next_continuous_run_allowed: bool = True,
) -> Path:
    return _write_json(
        root / "route_physical_recovery_readiness" / "v1_5_route_physical_recovery_readiness.json",
        {
            "schema": "v1_5_route_physical_recovery_readiness_v1",
            "manifest": {
                "status": status,
                "blocker_count": blocker_count,
                "review_required_count": review_required_count,
                "next_continuous_run_allowed": next_continuous_run_allowed,
                "segmented_evidence_fit_use_allowed": False,
                "opens_com_ports": False,
                "connects_postgresql": False,
                "controls_pressure": False,
                "controls_water_or_gas_routes": False,
                "writes_coefficients": False,
                "writes_sn_or_device_code": False,
                "formal_release_allowed": False,
                "database_import_allowed": False,
                "not_real_acceptance_evidence": True,
            },
        },
    )


def _seed_mature_route_continuity_gate(
    root: Path,
    *,
    status: str = "pass",
    blocker_count: int = 0,
    review_required_count: int = 0,
    fit_eligible: bool = True,
    boundary_clean: bool = True,
) -> Path:
    return _write_json(
        root / "mature_route_continuity_gate" / "v1_5_mature_route_continuity_gate.json",
        {
            "schema": "v1_5_mature_route_continuity_gate_v1",
            "manifest": {
                "status": status,
                "route_kind": "co2",
                "expected_point_count": 45,
                "observed_point_count": 45,
                "blocker_count": blocker_count,
                "review_required_count": review_required_count,
                "continuous_route_run_fit_eligible": fit_eligible,
                "mature_physical_baseline": "0613 fitting + 0620/0621 clean-worktree route path",
                "opens_com_ports": not boundary_clean,
                "controls_pressure": False,
                "controls_water_or_gas_routes": False,
                "connects_postgresql": False,
                "writes_coefficients": False,
                "writes_sn_or_device_code": False,
                "formal_release_allowed": False,
                "database_import_allowed": False,
                "not_real_acceptance_evidence": True,
            },
            "findings": [],
        },
    )


def _seed_formal_database_dry_run(root: Path, *, blocker_count: int = 0) -> Path:
    status = "ready_for_postgresql18_schema_dry_run_review" if blocker_count == 0 else "blocked"
    return _write_json(
        root / "formal_database_dry_run" / "v1_5_formal_database_dry_run.json",
        {
            "schema": "v1_5_formal_database_dry_run_contract_v1",
            "overall_status": status,
            "blocker_count": blocker_count,
            "production_backend": "postgresql",
            "production_postgresql_major": 18,
            "primary_identity": "sn_code/device_code",
            "opens_com_ports": False,
            "connects_postgresql": False,
            "controls_water_or_gas_routes": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "database_written": False,
            "database_import_allowed": False,
            "formal_release_allowed": False,
            "not_real_acceptance_evidence": True,
        },
    )


def _seed_formal_database_import_preflight(
    root: Path,
    *,
    blocker_count: int = 0,
    review_required_count: int = 0,
    dsn_configured: bool = True,
) -> Path:
    if blocker_count:
        status = "blocked"
    elif review_required_count:
        status = "review_required"
    else:
        status = "ready_for_authorized_postgresql18_import_review"
    return _write_json(
        root / "formal_database_import_preflight" / "v1_5_formal_database_import_preflight.json",
        {
            "schema": "v1_5_formal_database_import_preflight_v1",
            "overall_status": status,
            "blocker_count": blocker_count,
            "review_required_count": review_required_count,
            "production_backend": "postgresql",
            "production_postgresql_major": 18,
            "dry_run_contract_ready": blocker_count == 0,
            "dsn_configured": dsn_configured,
            "connects_postgresql": False,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "applies_migrations": False,
            "database_import_attempted": False,
            "database_written": False,
            "database_import_allowed": False,
            "formal_release_allowed": False,
            "not_real_acceptance_evidence": True,
        },
    )


def _seed_formal_database_import_authorization(
    root: Path,
    *,
    blocker_count: int = 0,
    review_required_count: int = 0,
    preflight_ready: bool = True,
    archive_release_ready: bool = True,
    manual_authorization_ready: bool = True,
) -> Path:
    if blocker_count:
        status = "blocked"
    elif review_required_count:
        status = "review_required"
    else:
        status = "ready_for_manual_postgresql18_import_authorization"
    database_import_allowed = (
        blocker_count == 0
        and review_required_count == 0
        and preflight_ready
        and archive_release_ready
        and manual_authorization_ready
    )
    return _write_json(
        root / "formal_database_import_authorization" / "v1_5_formal_database_import_authorization.json",
        {
            "schema": "v1_5_formal_database_import_authorization_v1",
            "overall_status": status,
            "blocker_count": blocker_count,
            "review_required_count": review_required_count,
            "production_backend": "postgresql",
            "production_postgresql_major": 18,
            "preflight_ready": preflight_ready,
            "database_import_preflight_binding_ready": preflight_ready,
            "archive_release_ready": archive_release_ready,
            "archive_closure_index_binding_ready": archive_release_ready,
            "senco_authorization_archive_binding_ready": archive_release_ready,
            "manual_authorization_ready": manual_authorization_ready,
            "connects_postgresql": False,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "applies_migrations": False,
            "database_import_attempted": False,
            "database_written": False,
            "database_import_allowed": database_import_allowed,
            "formal_release_allowed": archive_release_ready,
            "not_real_acceptance_evidence": True,
        },
    )


def _seed_formal_database_import_command_contract(
    root: Path,
    *,
    blocker_count: int = 0,
    review_required_count: int = 0,
    authorization_ready: bool = True,
    preflight_ready: bool = True,
    archive_release_ready: bool = True,
    evidence_bundle_ready: bool = True,
    command_contract_ready: bool = True,
) -> Path:
    if blocker_count:
        status = "blocked"
    elif review_required_count:
        status = "review_required"
    else:
        status = "ready_for_controlled_postgresql18_import_command_review"
    return _write_json(
        root / "formal_database_import_command_contract" / "v1_5_formal_database_import_command_contract.json",
        {
            "schema": "v1_5_formal_database_import_command_contract_v1",
            "overall_status": status,
            "blocker_count": blocker_count,
            "review_required_count": review_required_count,
            "production_backend": "postgresql",
            "production_postgresql_major": 18,
            "authorization_ready": authorization_ready,
            "database_import_authorization_binding_ready": authorization_ready,
            "preflight_ready": preflight_ready,
            "database_import_preflight_binding_ready": preflight_ready,
            "archive_release_ready": archive_release_ready,
            "archive_closure_index_binding_ready": archive_release_ready,
            "senco_authorization_archive_binding_ready": archive_release_ready,
            "evidence_bundle_ready": evidence_bundle_ready,
            "evidence_bundle_schema_ready": evidence_bundle_ready,
            "evidence_bundle_binding_ready": evidence_bundle_ready,
            "command_contract_ready": command_contract_ready,
            "connects_postgresql": False,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "applies_migrations": False,
            "database_import_attempted": False,
            "database_written": False,
            "database_import_allowed": False,
            "real_import_execution_allowed": False,
            "not_real_acceptance_evidence": True,
        },
    )


def _seed_formal_database_import_blocked_executor(
    root: Path,
    *,
    review_required_count: int = 0,
    blocked_executor_ready: bool = True,
    side_effect_lock_clean: bool = True,
) -> Path:
    status = (
        "blocked_pending_controlled_executor_implementation"
        if blocked_executor_ready
        else "review_required"
    )
    database_written = not side_effect_lock_clean
    return _write_json(
        root / "formal_database_import_blocked_executor" / "v1_5_formal_database_import_blocked_executor.json",
        {
            "schema": "v1_5_formal_database_import_blocked_executor_v1",
            "overall_status": status,
            "blocker_count": 0,
            "review_required_count": review_required_count,
            "blocked_executor_ready": blocked_executor_ready,
            "execution_supported": False,
            "database_import_authorization_binding_ready": (
                blocked_executor_ready and side_effect_lock_clean
            ),
            "database_import_preflight_binding_ready": (
                blocked_executor_ready and side_effect_lock_clean
            ),
            "evidence_bundle_binding_ready": blocked_executor_ready and side_effect_lock_clean,
            "evidence_bundle_schema_ready": blocked_executor_ready and side_effect_lock_clean,
            "archive_closure_index_binding_ready": (
                blocked_executor_ready and side_effect_lock_clean
            ),
            "senco_authorization_archive_binding_ready": (
                blocked_executor_ready and side_effect_lock_clean
            ),
            "real_import_execution_allowed": False,
            "production_backend": "postgresql",
            "production_postgresql_major": 18,
            "connects_postgresql": False,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "applies_migrations": False,
            "database_import_attempted": False,
            "database_written": database_written,
            "database_import_allowed": False,
            "not_real_acceptance_evidence": True,
        },
    )


def _seed_formal_database_import_controlled_executor_design(
    root: Path,
    *,
    review_required_count: int = 0,
    side_effect_lock_clean: bool = True,
) -> Path:
    status = (
        "ready_for_controlled_import_executor_design_review"
        if review_required_count == 0
        else "review_required"
    )
    database_written = not side_effect_lock_clean
    return _write_json(
        root
        / "formal_database_import_controlled_executor_design"
        / "v1_5_formal_database_import_controlled_executor_design.json",
        {
            "schema": "v1_5_formal_database_import_controlled_executor_design_v1",
            "overall_status": status,
            "blocker_count": 0,
            "review_required_count": review_required_count,
            "production_state": "blocked_design_only",
            "execution_supported": False,
            "database_import_authorization_binding_ready": side_effect_lock_clean,
            "database_import_preflight_binding_ready": side_effect_lock_clean,
            "evidence_bundle_binding_ready": side_effect_lock_clean,
            "evidence_bundle_schema_ready": side_effect_lock_clean,
            "archive_closure_index_binding_ready": side_effect_lock_clean,
            "senco_authorization_archive_binding_ready": side_effect_lock_clean,
            "real_import_execution_allowed": False,
            "production_backend": "postgresql",
            "production_postgresql_major": 18,
            "connects_postgresql": False,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "applies_migrations": False,
            "database_import_attempted": False,
            "database_written": database_written,
            "database_import_allowed": False,
            "not_real_acceptance_evidence": True,
        },
    )


def _seed_formal_initialization_controlled_executor_design(
    root: Path,
    *,
    review_required_count: int = 0,
    side_effect_lock_clean: bool = True,
) -> Path:
    status = (
        "ready_for_controlled_initialization_executor_design_review"
        if review_required_count == 0
        else "review_required"
    )
    opens_com_ports = not side_effect_lock_clean
    return _write_json(
        root
        / "formal_initialization_controlled_executor_design"
        / "v1_5_formal_initialization_controlled_executor_design.json",
        {
            "schema": "v1_5_formal_initialization_controlled_executor_design_v1",
            "overall_status": status,
            "blocker_count": 0,
            "review_required_count": review_required_count,
            "production_state": "blocked_design_only",
            "execution_supported": False,
            "live_execution_allowed": False,
            "read_only_real_com_execution_allowed": False,
            "controlled_write_execution_allowed": False,
            "real_com_execution_allowed": False,
            "execute_flag_allowed": False,
            "opens_com_ports": opens_com_ports,
            "connects_postgresql": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "database_written": False,
            "formal_release_allowed": False,
            "database_import_allowed": False,
            "not_real_acceptance_evidence": True,
        },
    )


def _seed_formal_initialization_readonly_com_preflight_design(
    root: Path,
    *,
    review_required_count: int = 0,
    side_effect_lock_clean: bool = True,
) -> Path:
    status = (
        "ready_for_readonly_real_com_preflight_design_review"
        if review_required_count == 0
        else "review_required"
    )
    opens_com_ports = not side_effect_lock_clean
    return _write_json(
        root
        / "formal_initialization_readonly_com_preflight_design"
        / "v1_5_formal_initialization_readonly_com_preflight_design.json",
        {
            "schema": "v1_5_formal_initialization_readonly_com_preflight_design_v1",
            "overall_status": status,
            "blocker_count": 0,
            "review_required_count": review_required_count,
            "production_state": "blocked_design_only",
            "execution_supported": False,
            "live_execution_allowed": False,
            "read_only_real_com_execution_allowed": False,
            "controlled_write_execution_allowed": False,
            "real_com_execution_allowed": False,
            "execute_flag_allowed": False,
            "opens_com_ports": opens_com_ports,
            "connects_postgresql": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "database_written": False,
            "formal_release_allowed": False,
            "database_import_allowed": False,
            "not_real_acceptance_evidence": True,
        },
    )


def _seed_formal_initialization_readonly_com_preflight_blocked_executor(
    root: Path,
    *,
    review_required_count: int = 0,
    side_effect_lock_clean: bool = True,
) -> Path:
    status = (
        "blocked_pending_readonly_real_com_preflight_implementation"
        if review_required_count == 0
        else "review_required"
    )
    opens_com_ports = not side_effect_lock_clean
    return _write_json(
        root
        / "ro_com_stub"
        / "v1_5_formal_initialization_readonly_com_preflight_blocked_executor.json",
        {
            "schema": "v1_5_formal_initialization_readonly_com_preflight_blocked_executor_v1",
            "overall_status": status,
            "blocker_count": 0,
            "review_required_count": review_required_count,
            "blocked_executor_ready": review_required_count == 0,
            "execution_supported": False,
            "execution_requested": False,
            "live_execution_allowed": False,
            "read_only_real_com_execution_allowed": False,
            "controlled_write_execution_allowed": False,
            "real_com_execution_allowed": False,
            "execute_flag_allowed": False,
            "opens_com_ports": opens_com_ports,
            "connects_postgresql": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "database_written": False,
            "formal_release_allowed": False,
            "database_import_allowed": False,
            "not_real_acceptance_evidence": True,
        },
    )


def _seed_formal_initialization_readonly_com_preflight_controlled_executor_design(
    root: Path,
    *,
    review_required_count: int = 0,
    side_effect_lock_clean: bool = True,
) -> Path:
    status = (
        "ready_for_readonly_com_preflight_controlled_executor_design_review"
        if review_required_count == 0
        else "review_required"
    )
    opens_com_ports = not side_effect_lock_clean
    return _write_json(
        root
        / "ro_com_controlled_design"
        / "v1_5_formal_initialization_readonly_com_preflight_controlled_executor_design.json",
        {
            "schema": "v1_5_formal_initialization_readonly_com_preflight_controlled_executor_design_v1",
            "overall_status": status,
            "blocker_count": 0,
            "review_required_count": review_required_count,
            "production_state": "blocked_design_only",
            "execution_supported": False,
            "live_execution_allowed": False,
            "read_only_real_com_execution_allowed": False,
            "controlled_write_execution_allowed": False,
            "real_com_execution_allowed": False,
            "execute_flag_allowed": False,
            "opens_com_ports": opens_com_ports,
            "connects_postgresql": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "database_written": False,
            "minimum_serial_command_gap_s": 1.0,
        },
    )


def _seed_formal_initialization_readonly_com_preflight_controlled_blocked_executor(
    root: Path,
    *,
    review_required_count: int = 0,
    side_effect_lock_clean: bool = True,
) -> Path:
    status = (
        "blocked_pending_controlled_readonly_com_preflight_executor_implementation"
        if review_required_count == 0
        else "review_required"
    )
    opens_com_ports = not side_effect_lock_clean
    path = (
        root
        / "ro_blk"
        / "v1_5_formal_initialization_readonly_com_preflight_controlled_blocked_executor.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    return _write_json(
        path,
        {
            "schema": "v1_5_formal_initialization_readonly_com_preflight_controlled_blocked_executor_v1",
            "overall_status": status,
            "blocker_count": 0,
            "review_required_count": review_required_count,
            "blocked_executor_ready": review_required_count == 0,
            "contract_ready_for_future_controlled_readonly_com_review": review_required_count == 0,
            "production_state": "blocked_executor_only",
            "execution_supported": False,
            "execution_requested": False,
            "dry_run_only": True,
            "live_execution_allowed": False,
            "read_only_real_com_execution_allowed": False,
            "controlled_write_execution_allowed": False,
            "real_com_execution_allowed": False,
            "execute_flag_allowed": False,
            "opens_com_ports": opens_com_ports,
            "connects_postgresql": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "database_written": False,
            "formal_release_allowed": False,
            "database_import_allowed": False,
            "not_real_acceptance_evidence": True,
            "minimum_serial_command_gap_s": 1.0,
        },
    )


def _seed_formal_readonly_com_execution_contract(
    root: Path,
    *,
    review_required_count: int = 0,
    side_effect_lock_clean: bool = True,
) -> Path:
    status = "ready_for_readonly_com_execution_contract_review" if review_required_count == 0 else "review_required"
    opens_com_ports = not side_effect_lock_clean
    path = root / "ro_contract" / "v1_5_formal_readonly_com_execution_contract.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return _write_json(
        path,
        {
            "schema": "v1_5_formal_readonly_com_execution_contract_v1",
            "overall_status": status,
            "blocker_count": 0,
            "review_required_count": review_required_count,
            "contract_ready": review_required_count == 0,
            "production_state": "contract_only",
            "execution_supported": False,
            "live_execution_allowed": False,
            "read_only_real_com_execution_allowed": False,
            "controlled_write_execution_allowed": False,
            "real_com_execution_allowed": False,
            "execute_flag_allowed": False,
            "opens_com_ports": opens_com_ports,
            "connects_postgresql": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "database_written": False,
            "formal_release_allowed": False,
            "database_import_allowed": False,
            "not_real_acceptance_evidence": True,
            "minimum_serial_command_gap_s": 1.0,
        },
    )


def _seed_formal_readonly_com_execution_blocked_executor(
    root: Path,
    *,
    review_required_count: int = 0,
    side_effect_lock_clean: bool = True,
) -> Path:
    status = (
        "blocked_pending_readonly_com_real_executor_implementation"
        if review_required_count == 0
        else "review_required"
    )
    opens_com_ports = not side_effect_lock_clean
    path = root / "ro_executor_blocked" / "v1_5_formal_readonly_com_execution_blocked_executor.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return _write_json(
        path,
        {
            "schema": "v1_5_formal_readonly_com_execution_blocked_executor_v1",
            "overall_status": status,
            "blocker_count": 0,
            "review_required_count": review_required_count,
            "blocked_executor_ready": review_required_count == 0,
            "contract_ready_for_future_readonly_com_executor_review": review_required_count == 0,
            "production_state": "blocked_executor_only",
            "execution_supported": False,
            "execution_requested": False,
            "dry_run_only": True,
            "live_execution_allowed": False,
            "read_only_real_com_execution_allowed": False,
            "controlled_write_execution_allowed": False,
            "real_com_execution_allowed": False,
            "execute_flag_allowed": False,
            "opens_com_ports": opens_com_ports,
            "connects_postgresql": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "database_written": False,
            "formal_release_allowed": False,
            "database_import_allowed": False,
            "not_real_acceptance_evidence": True,
            "minimum_serial_command_gap_s": 1.0,
            "supports_old_algorithm_check_skip": True,
        },
    )


def _seed_formal_readonly_com_execution_packet_validator(
    root: Path,
    *,
    review_required_count: int = 0,
    side_effect_lock_clean: bool = True,
    packet_status: str = "blocked_pending_readonly_com_execution_authorization_packet",
) -> Path:
    status = packet_status if review_required_count == 0 else "review_required"
    opens_com_ports = not side_effect_lock_clean
    path = root / "ro_packet_validator" / "v1_5_formal_readonly_com_execution_packet_validator.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return _write_json(
        path,
        {
            "schema": "v1_5_formal_readonly_com_execution_packet_validator_v1",
            "overall_status": status,
            "blocker_count": 0,
            "review_required_count": review_required_count,
            "packet_validator_ready": review_required_count == 0,
            "packet_validated_offline": packet_status == "ready_for_readonly_com_execution_packet_review",
            "execution_supported": False,
            "live_execution_allowed": False,
            "read_only_real_com_execution_allowed": False,
            "controlled_write_execution_allowed": False,
            "real_com_execution_allowed": False,
            "execute_flag_allowed": False,
            "opens_com_ports": opens_com_ports,
            "connects_postgresql": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "database_written": False,
            "formal_release_allowed": False,
            "database_import_allowed": False,
            "not_real_acceptance_evidence": True,
            "minimum_serial_command_gap_s": 1.0,
            "supports_old_algorithm_check_skip": True,
        },
    )


def _seed_formal_readonly_com_execution_plan_preview(
    root: Path,
    *,
    review_required_count: int = 0,
    side_effect_lock_clean: bool = True,
    status: str = "ready_for_readonly_com_execution_plan_preview_review",
) -> Path:
    source_status = status if review_required_count == 0 else "review_required"
    opens_com_ports = not side_effect_lock_clean
    path = root / "ro_plan_preview" / "v1_5_formal_readonly_com_execution_plan_preview.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return _write_json(
        path,
        {
            "schema": "v1_5_formal_readonly_com_execution_plan_preview_v1",
            "overall_status": source_status,
            "blocker_count": 0,
            "review_required_count": review_required_count,
            "plan_preview_ready": source_status == "ready_for_readonly_com_execution_plan_preview_review",
            "packet_validated_offline": source_status == "ready_for_readonly_com_execution_plan_preview_review",
            "execution_supported": False,
            "live_execution_allowed": False,
            "read_only_real_com_execution_allowed": False,
            "controlled_write_execution_allowed": False,
            "real_com_execution_allowed": False,
            "execute_flag_allowed": False,
            "opens_com_ports": opens_com_ports,
            "connects_postgresql": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "database_written": False,
            "formal_release_allowed": False,
            "database_import_allowed": False,
            "not_real_acceptance_evidence": True,
            "does_not_execute_commands": True,
            "minimum_serial_command_gap_s": 1.0,
            "future_check_command_count": 1,
            "old_algorithm_check_skip_count": 1,
        },
    )


def _seed_formal_readonly_com_minimal_executor_review(
    root: Path,
    *,
    review_required_count: int = 0,
    side_effect_lock_clean: bool = True,
) -> Path:
    status = (
        "blocked_pending_minimal_readonly_com_executor_implementation"
        if review_required_count == 0
        else "review_required"
    )
    opens_com_ports = not side_effect_lock_clean
    return _write_json(
        root
        / "ro_minimal_executor_review"
        / "v1_5_formal_readonly_com_minimal_executor_review.json",
        {
            "schema": "v1_5_formal_readonly_com_minimal_executor_review_v1",
            "overall_status": status,
            "blocker_count": 0,
            "review_required_count": review_required_count,
            "minimal_executor_review_ready": review_required_count == 0,
            "execution_supported": False,
            "live_execution_allowed": False,
            "read_only_real_com_execution_allowed": False,
            "controlled_write_execution_allowed": False,
            "real_com_execution_allowed": False,
            "execute_flag_allowed": False,
            "opens_com_ports": opens_com_ports,
            "connects_postgresql": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "database_written": False,
            "formal_release_allowed": False,
            "database_import_allowed": False,
            "not_real_acceptance_evidence": True,
            "minimum_serial_command_gap_s": 1.0,
        },
    )


def _seed_formal_readonly_com_minimal_executor_stub(
    root: Path,
    *,
    review_required_count: int = 0,
    side_effect_lock_clean: bool = True,
) -> Path:
    status = (
        "blocked_plan_only_minimal_readonly_com_executor_stub"
        if review_required_count == 0
        else "review_required"
    )
    opens_com_ports = not side_effect_lock_clean
    return _write_json(
        root
        / "ro_minimal_executor_stub"
        / "v1_5_formal_readonly_com_minimal_executor_stub.json",
        {
            "schema": "v1_5_formal_readonly_com_minimal_executor_stub_v1",
            "overall_status": status,
            "blocker_count": 0,
            "review_required_count": review_required_count,
            "minimal_executor_stub_ready": review_required_count == 0,
            "would_execute_artifact_ready": review_required_count == 0,
            "execution_supported": False,
            "live_execution_allowed": False,
            "read_only_real_com_execution_allowed": False,
            "controlled_write_execution_allowed": False,
            "real_com_execution_allowed": False,
            "execute_flag_allowed": False,
            "opens_com_ports": opens_com_ports,
            "connects_postgresql": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "database_written": False,
            "formal_release_allowed": False,
            "database_import_allowed": False,
            "not_real_acceptance_evidence": True,
            "minimum_serial_command_gap_s": 1.0,
            "authorization_context_consumed_as_unlock": False,
        },
    )


def test_formal_run_status_reports_ready_release_without_touching_devices(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_run"
    _seed_ready_run(run_dir)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gate_statuses = {row["gate_id"]: row["status"] for row in model["gates"]}

    assert model["schema"] == "v1_5_formal_run_status_v1"
    assert model["overall_status"] == "formal_release_ready"
    assert model["current_stage"] == "complete"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert model["gaps"] == []
    assert gate_statuses["pressure_senco9_pre_open_flow"] == "ready"
    assert gate_statuses["co2_open_flow_mature_queue"] == "ready"
    assert gate_statuses["h2o_open_flow_mature_queue"] == "ready"
    assert gate_statuses["senco_artifact_authorization"] == "ready"
    assert model["senco_artifact_authorization"]["controlled_write_authorization_ready"] is True
    assert model["senco_artifact_authorization"]["authorized_device_ids"] == [
        "001",
        "002",
        "003",
        "004",
        "005",
        "006",
    ]
    assert "algorithm_profile_runner_dry_run" not in gate_statuses
    assert "formal_database_dry_run" not in gate_statuses
    assert "formal_database_import_preflight" not in gate_statuses
    assert "formal_database_import_authorization" not in gate_statuses
    assert model["physical_boundaries"] == {
        "offline_status_only": True,
        "opens_com_ports": False,
        "connects_postgresql": False,
        "real_import_execution_allowed": False,
        "database_written": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_device_id": False,
        "not_real_acceptance_evidence": True,
    }


def test_formal_run_status_blocks_release_but_not_sampling_when_senco_authorization_is_missing(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "missing_senco_authorization"
    _seed_ready_run(run_dir, include_senco_artifact_authorization=False)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}

    assert model["overall_status"] == "in_progress"
    assert model["current_stage"] == "senco_artifact_authorization"
    assert model["formal_release_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert model["senco_artifact_authorization"]["controlled_write_authorization_ready"] is False
    assert gates["senco_artifact_authorization"]["status"] == "missing"
    assert gates["senco_artifact_authorization"]["blocks_release"] is True
    assert gates["senco_artifact_authorization"]["blocks_physical_flow"] is False


def test_formal_run_status_blocks_tampered_senco_authorization_before_release(tmp_path: Path) -> None:
    run_dir = tmp_path / "tampered_senco_authorization"
    _seed_ready_run(run_dir)
    manifest = run_dir / "main_senco_write_precheck" / "main_senco_artifact_hash_manifest.json"
    manifest.write_text("tampered", encoding="utf-8")

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gate = next(row for row in model["gates"] if row["gate_id"] == "senco_artifact_authorization")

    assert model["overall_status"] == "blocked"
    assert model["formal_release_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert gate["status"] == "blocked"
    assert "manifest_sha256_mismatch" in gate["reason"]


def test_formal_run_status_accepts_pressure_s9_readiness_index(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_run_with_pressure_s9_index"
    _seed_ready_run(run_dir)
    _write_json(
        run_dir / "run_evidence" / "v1_5_run_evidence_status.json",
        {
            "schema": "v1_5_run_evidence_status_v1",
            "overall_status": "ready_for_reviewer",
            "stage_statuses": [
                {"stage_id": "pressure_quick_check", "status": "not_attempted"},
                {"stage_id": "co2_open_flow", "status": "pass"},
                {"stage_id": "h2o_open_flow", "status": "pass"},
                {"stage_id": "candidate_review", "status": "pass"},
                {"stage_id": "post_run_coefficient_executor", "status": "pass"},
                {"stage_id": "post_write_reverification", "status": "pass"},
            ],
        },
    )
    pressure_index = _seed_pressure_s9_readiness_index(run_dir, ready=True)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gate = next(row for row in model["gates"] if row["gate_id"] == "pressure_senco9_pre_open_flow")

    assert gate["status"] == "ready"
    assert gate["source_path"] == str(pressure_index.resolve())
    assert gate["source_status"] == "ready_for_mature_open_flow_pressure_s9_index"
    assert model["linked_inputs"]["pressure_s9_readiness_index_json"] == str(pressure_index.resolve())
    assert model["can_continue_physical_flow"] is True
    assert model["formal_release_allowed"] is True


def test_formal_run_status_pressure_s9_index_overrides_legacy_stage_pass(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_run_with_pressure_s9_review"
    _seed_ready_run(run_dir)
    pressure_index = _seed_pressure_s9_readiness_index(run_dir, ready=False)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gate = next(row for row in model["gates"] if row["gate_id"] == "pressure_senco9_pre_open_flow")

    assert gate["status"] == "review_required"
    assert gate["source_path"] == str(pressure_index.resolve())
    assert "post_write_pressure_reverify" in gate["reason"]
    assert model["current_stage"] == "pressure_senco9_pre_open_flow"
    assert model["formal_release_allowed"] is False
    assert model["can_continue_physical_flow"] is False


def test_formal_run_status_accepts_ready_batch_initialization_closeout(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_run_with_batch_closeout"
    _seed_ready_run(run_dir)
    closeout_path = _seed_batch_initialization_closeout(run_dir, ready=True)
    resume_path = _seed_post_closeout_resume_gate(run_dir, ready=True)

    model = build_v1_5_formal_run_status(
        run_dir=run_dir,
        batch_initialization_closeout_json=closeout_path,
        post_closeout_resume_gate_json=resume_path,
    )
    gate = next(row for row in model["gates"] if row["gate_id"] == "batch_initialization_closeout")

    assert gate["status"] == "ready"
    assert gate["source_path"] == str(closeout_path.resolve())
    assert "6 active device" in gate["reason"]
    assert gate["blocks_physical_flow"] is False
    assert model["can_continue_physical_flow"] is True
    assert model["linked_inputs"]["batch_initialization_closeout_json"] == str(closeout_path.resolve())
    resume_gate = next(row for row in model["gates"] if row["gate_id"] == "post_closeout_resume_gate")
    assert resume_gate["status"] == "ready"
    assert resume_gate["blocks_physical_flow"] is False
    assert model["linked_inputs"]["post_closeout_resume_gate_json"] == str(resume_path.resolve())
    application_gate = next(
        row for row in model["gates"] if row["gate_id"] == "resume_prefix_application_review"
    )
    assert application_gate["status"] == "ready"
    assert application_gate["blocks_physical_flow"] is False
    assert model["linked_inputs"]["resume_prefix_application_review_json"].endswith(
        "v1_5_resume_prefix_application_review.json"
    )
    writer_design_gate = next(
        row for row in model["gates"] if row["gate_id"] == "authoritative_resume_state_writer_design"
    )
    assert writer_design_gate["status"] == "ready"
    assert writer_design_gate["blocks_physical_flow"] is False
    assert model["linked_inputs"]["authoritative_resume_state_writer_design_json"].endswith(
        "v1_5_authoritative_resume_state_writer_design.json"
    )
    blocked_executor_gate = next(
        row
        for row in model["gates"]
        if row["gate_id"] == "authoritative_resume_state_writer_blocked_executor"
    )
    assert blocked_executor_gate["status"] == "ready"
    assert blocked_executor_gate["blocks_physical_flow"] is False
    assert model["linked_inputs"][
        "authoritative_resume_state_writer_blocked_executor_json"
    ].endswith("v1_5_authoritative_resume_state_writer_blocked_executor.json")
    controlled_preflight_gate = next(
        row
        for row in model["gates"]
        if row["gate_id"] == "authoritative_resume_state_controlled_write_preflight"
    )
    assert controlled_preflight_gate["status"] == "ready"
    assert controlled_preflight_gate["blocks_physical_flow"] is False
    assert model["linked_inputs"][
        "authoritative_resume_state_controlled_write_preflight_json"
    ].endswith("v1_5_resume_state_write_preflight.json")


def test_formal_run_status_recomputes_controlled_write_preflight(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_run_with_forged_controlled_write_preflight"
    _seed_ready_run(run_dir)
    closeout_path = _seed_batch_initialization_closeout(run_dir, ready=True)
    resume_path = _seed_post_closeout_resume_gate(run_dir, ready=True)
    preflight_path = (
        run_dir
        / "authoritative_resume_state_controlled_write_preflight"
        / "v1_5_resume_state_write_preflight.json"
    )
    preflight = json.loads(preflight_path.read_text(encoding="utf-8"))
    preflight["candidate_state"]["completed_step_ids"].append("database_import")
    _write_json(preflight_path, preflight)

    model = build_v1_5_formal_run_status(
        run_dir=run_dir,
        batch_initialization_closeout_json=closeout_path,
        post_closeout_resume_gate_json=resume_path,
        authoritative_resume_state_controlled_write_preflight_json=preflight_path,
    )
    gate = next(
        row
        for row in model["gates"]
        if row["gate_id"] == "authoritative_resume_state_controlled_write_preflight"
    )

    assert gate["status"] == "blocked"
    assert "independently recomputed" in gate["reason"]
    assert gate["blocks_physical_flow"] is True
    assert model["can_continue_physical_flow"] is False
    assert model["formal_release_allowed"] is False


def test_formal_run_status_recomputes_blocked_executor_lock_evidence(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_run_with_forged_blocked_executor"
    _seed_ready_run(run_dir)
    closeout_path = _seed_batch_initialization_closeout(run_dir, ready=True)
    resume_path = _seed_post_closeout_resume_gate(run_dir, ready=True)
    blocked_path = (
        run_dir
        / "authoritative_resume_state_writer_blocked_executor"
        / "v1_5_authoritative_resume_state_writer_blocked_executor.json"
    )
    blocked = json.loads(blocked_path.read_text(encoding="utf-8"))
    blocked["state_file_created"] = True
    blocked["writes_authoritative_state"] = True
    _write_json(blocked_path, blocked)

    model = build_v1_5_formal_run_status(
        run_dir=run_dir,
        batch_initialization_closeout_json=closeout_path,
        post_closeout_resume_gate_json=resume_path,
        authoritative_resume_state_writer_blocked_executor_json=blocked_path,
    )
    gate = next(
        row
        for row in model["gates"]
        if row["gate_id"] == "authoritative_resume_state_writer_blocked_executor"
    )

    assert gate["status"] == "blocked"
    assert "boundary" in gate["reason"]
    assert gate["blocks_physical_flow"] is True
    assert model["can_continue_physical_flow"] is False
    assert model["formal_release_allowed"] is False


def test_formal_run_status_recomputes_authoritative_writer_design(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_run_with_forged_writer_design"
    _seed_ready_run(run_dir)
    closeout_path = _seed_batch_initialization_closeout(run_dir, ready=True)
    resume_path = _seed_post_closeout_resume_gate(run_dir, ready=True)
    design_path = (
        run_dir
        / "authoritative_resume_state_writer_design"
        / "v1_5_authoritative_resume_state_writer_design.json"
    )
    design = json.loads(design_path.read_text(encoding="utf-8"))
    design["proposed_completed_step_ids"].insert(-1, "database_import")
    design["proposed_completed_step_cli_arguments"].extend(
        ["--completed-step", "database_import"]
    )
    _write_json(design_path, design)

    model = build_v1_5_formal_run_status(
        run_dir=run_dir,
        batch_initialization_closeout_json=closeout_path,
        post_closeout_resume_gate_json=resume_path,
        authoritative_resume_state_writer_design_json=design_path,
    )
    gate = next(
        row for row in model["gates"] if row["gate_id"] == "authoritative_resume_state_writer_design"
    )

    assert gate["status"] == "blocked"
    assert "independently recomputed" in gate["reason"]
    assert gate["blocks_physical_flow"] is True
    assert model["can_continue_physical_flow"] is False
    assert model["formal_release_allowed"] is False


def test_formal_run_status_blocks_stale_resume_prefix_application_review(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_run_with_stale_resume_application_review"
    _seed_ready_run(run_dir)
    closeout_path = _seed_batch_initialization_closeout(run_dir, ready=True)
    resume_path = _seed_post_closeout_resume_gate(run_dir, ready=True)
    resume_payload = json.loads(resume_path.read_text(encoding="utf-8"))
    resume_payload["review_note_after_application_review"] = "changed"
    _write_json(resume_path, resume_payload)

    model = build_v1_5_formal_run_status(
        run_dir=run_dir,
        batch_initialization_closeout_json=closeout_path,
        post_closeout_resume_gate_json=resume_path,
    )
    gate = next(
        row for row in model["gates"] if row["gate_id"] == "resume_prefix_application_review"
    )

    assert gate["status"] == "blocked"
    assert "hash" in gate["reason"]
    assert gate["blocks_physical_flow"] is True
    assert model["current_stage"] == "resume_prefix_application_review"
    assert model["can_continue_physical_flow"] is False
    assert model["formal_release_allowed"] is False


def test_formal_run_status_recomputes_exact_resume_prefix_instead_of_trusting_ready_sidecar(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "ready_run_with_forged_resume_application_prefix"
    _seed_ready_run(run_dir)
    closeout_path = _seed_batch_initialization_closeout(run_dir, ready=True)
    resume_path = _seed_post_closeout_resume_gate(run_dir, ready=True)
    application_path = (
        run_dir
        / "resume_prefix_application_review"
        / "v1_5_resume_prefix_application_review.json"
    )
    application = json.loads(application_path.read_text(encoding="utf-8"))
    application["reviewed_completed_step_ids_after_application"] = [
        "batch_initialization_closeout_index",
        "post_closeout_resume_gate_snapshot",
        "database_import",
        "post_closeout_resume_prefix_application_review",
    ]
    application["reviewed_state_application_cli_arguments"] = [
        "--completed-step",
        "batch_initialization_closeout_index",
        "--completed-step",
        "post_closeout_resume_gate_snapshot",
        "--completed-step",
        "database_import",
        "--completed-step",
        "post_closeout_resume_prefix_application_review",
    ]
    _write_json(application_path, application)

    model = build_v1_5_formal_run_status(
        run_dir=run_dir,
        batch_initialization_closeout_json=closeout_path,
        post_closeout_resume_gate_json=resume_path,
        resume_prefix_application_review_json=application_path,
    )
    gate = next(
        row for row in model["gates"] if row["gate_id"] == "resume_prefix_application_review"
    )

    assert gate["status"] == "blocked"
    assert "exact-prefix" in gate["reason"]
    assert gate["blocks_physical_flow"] is True
    assert model["can_continue_physical_flow"] is False
    assert model["formal_release_allowed"] is False


def test_formal_run_status_rejects_same_hash_batch_copy_not_bound_by_resume_gate(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "ready_run_with_cross_path_batch_copy"
    _seed_ready_run(run_dir)
    closeout_path = _seed_batch_initialization_closeout(run_dir, ready=True)
    resume_path = _seed_post_closeout_resume_gate(run_dir, ready=True)
    application_path = (
        run_dir
        / "resume_prefix_application_review"
        / "v1_5_resume_prefix_application_review.json"
    )
    copied_closeout_path = _write_json(
        run_dir / "copied_batch" / "v1_5_batch_initialization_closeout_index.json",
        json.loads(closeout_path.read_text(encoding="utf-8")),
    )
    application = json.loads(application_path.read_text(encoding="utf-8"))
    application["batch_initialization_closeout_json"] = str(copied_closeout_path.resolve())
    _write_json(application_path, application)

    model = build_v1_5_formal_run_status(
        run_dir=run_dir,
        batch_initialization_closeout_json=closeout_path,
        post_closeout_resume_gate_json=resume_path,
        resume_prefix_application_review_json=application_path,
    )
    gate = next(
        row for row in model["gates"] if row["gate_id"] == "resume_prefix_application_review"
    )

    assert gate["status"] == "blocked"
    assert "source binding" in gate["reason"]
    assert gate["blocks_physical_flow"] is True
    assert model["can_continue_physical_flow"] is False


def test_formal_run_status_blocks_physical_flow_on_blocked_post_closeout_resume_gate(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_run_with_blocked_resume_gate"
    _seed_ready_run(run_dir)
    closeout_path = _seed_batch_initialization_closeout(run_dir, ready=True)
    resume_path = _seed_post_closeout_resume_gate(run_dir, ready=False)

    model = build_v1_5_formal_run_status(
        run_dir=run_dir,
        batch_initialization_closeout_json=closeout_path,
        post_closeout_resume_gate_json=resume_path,
    )
    gate = next(row for row in model["gates"] if row["gate_id"] == "post_closeout_resume_gate")

    assert gate["status"] == "blocked"
    assert gate["blocks_physical_flow"] is True
    assert model["current_stage"] == "post_closeout_resume_gate"
    assert model["can_continue_physical_flow"] is False
    assert model["formal_release_allowed"] is False


def test_formal_run_status_blocks_tampered_post_closeout_resume_source(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_run_with_tampered_resume_source"
    _seed_ready_run(run_dir)
    closeout_path = _seed_batch_initialization_closeout(run_dir, ready=True)
    resume_path = _seed_post_closeout_resume_gate(run_dir, ready=True)
    closeout_payload = json.loads(closeout_path.read_text(encoding="utf-8"))
    closeout_payload["tampered_after_resume_gate"] = True
    _write_json(closeout_path, closeout_payload)

    model = build_v1_5_formal_run_status(
        run_dir=run_dir,
        batch_initialization_closeout_json=closeout_path,
        post_closeout_resume_gate_json=resume_path,
    )
    gate = next(row for row in model["gates"] if row["gate_id"] == "post_closeout_resume_gate")

    assert gate["status"] == "blocked"
    assert "missing or mismatched" in gate["reason"]
    assert gate["blocks_physical_flow"] is True
    assert model["can_continue_physical_flow"] is False


def test_formal_run_status_blocks_resume_gate_bound_to_different_batch_path(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_run_with_cross_batch_resume_gate"
    _seed_ready_run(run_dir)
    original_closeout = _seed_batch_initialization_closeout(run_dir, ready=True)
    resume_path = _seed_post_closeout_resume_gate(run_dir, ready=True)
    current_closeout = _write_json(
        run_dir / "current_batch" / "v1_5_batch_initialization_closeout_index.json",
        json.loads(original_closeout.read_text(encoding="utf-8")),
    )

    model = build_v1_5_formal_run_status(
        run_dir=run_dir,
        batch_initialization_closeout_json=current_closeout,
        post_closeout_resume_gate_json=resume_path,
    )
    gate = next(row for row in model["gates"] if row["gate_id"] == "post_closeout_resume_gate")

    assert gate["status"] == "blocked"
    assert "batch-closeout path" in gate["reason"]
    assert gate["blocks_physical_flow"] is True
    assert model["can_continue_physical_flow"] is False


def test_formal_run_status_blocks_physical_flow_on_incomplete_batch_closeout(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_run_with_incomplete_batch_closeout"
    _seed_ready_run(run_dir)
    closeout_path = _seed_batch_initialization_closeout(run_dir, ready=False)

    model = build_v1_5_formal_run_status(
        run_dir=run_dir,
        batch_initialization_closeout_json=closeout_path,
    )
    gate = next(row for row in model["gates"] if row["gate_id"] == "batch_initialization_closeout")

    assert gate["status"] == "review_required"
    assert gate["blocks_physical_flow"] is True
    assert "one_or_more_devices_not_ready" in gate["reason"]
    assert model["current_stage"] == "batch_initialization_closeout"
    assert model["can_continue_physical_flow"] is False
    assert model["formal_release_allowed"] is False


def test_formal_run_status_fails_closed_on_malformed_batch_device_count(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_run_with_malformed_batch_closeout"
    _seed_ready_run(run_dir)
    closeout_path = _seed_batch_initialization_closeout(run_dir, ready=True)
    payload = json.loads(closeout_path.read_text(encoding="utf-8"))
    payload["device_count"] = "six"
    _write_json(closeout_path, payload)

    model = build_v1_5_formal_run_status(
        run_dir=run_dir,
        batch_initialization_closeout_json=closeout_path,
    )
    gate = next(row for row in model["gates"] if row["gate_id"] == "batch_initialization_closeout")

    assert gate["status"] == "review_required"
    assert gate["blocks_physical_flow"] is True
    assert model["current_stage"] == "batch_initialization_closeout"
    assert model["can_continue_physical_flow"] is False


def test_formal_run_status_blocks_physical_flow_on_route_recovery_blockers(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_run_with_unrecovered_route_physics"
    _seed_ready_run(run_dir)
    recovery_path = _seed_route_physical_recovery_readiness(
        run_dir,
        status="blocked",
        blocker_count=4,
        review_required_count=1,
        next_continuous_run_allowed=False,
    )

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gate = next(row for row in model["gates"] if row["gate_id"] == "route_physical_recovery_readiness")

    assert model["overall_status"] == "blocked"
    assert model["current_stage"] == "route_physical_recovery_readiness"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is False
    assert model["linked_inputs"]["route_physical_recovery_readiness_json"] == str(recovery_path.resolve())
    assert gate["status"] == "blocked"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is True


def test_formal_run_status_accepts_route_recovery_without_unlocking_import(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_run_with_recovered_route_physics"
    _seed_ready_run(run_dir)
    recovery_path = _seed_route_physical_recovery_readiness(run_dir)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gate = next(row for row in model["gates"] if row["gate_id"] == "route_physical_recovery_readiness")

    assert model["overall_status"] == "formal_release_ready"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert model["linked_inputs"]["route_physical_recovery_readiness_json"] == str(recovery_path.resolve())
    assert gate["status"] == "ready"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert (
        export_status_main(
            [
                "--run-dir",
                str(run_dir),
                "--route-physical-recovery-readiness-json",
                str(recovery_path),
                "--output-dir",
                str(tmp_path / "status_cli"),
            ]
        )
        == 0
    )


def test_formal_run_status_requires_mature_route_continuity_gate_before_release(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_run_missing_route_continuity"
    _seed_ready_run(run_dir, include_mature_route_continuity_gate=False)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gate = next(row for row in model["gates"] if row["gate_id"] == "mature_route_continuity_gate")

    assert model["overall_status"] == "review_required"
    assert model["current_stage"] == "mature_route_continuity_gate"
    assert model["formal_release_allowed"] is False
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert model["linked_inputs"]["mature_route_continuity_gate_json"] == ""
    assert gate["status"] == "review_required"
    assert gate["release_gate"] is True
    assert gate["blocks_release"] is True
    assert gate["blocks_physical_flow"] is False


def test_formal_run_status_blocks_release_on_segmented_mature_route_continuity_gate(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "ready_run_with_segmented_route_manifest"
    _seed_ready_run(run_dir)
    continuity_path = _seed_mature_route_continuity_gate(
        run_dir,
        status="blocked",
        blocker_count=2,
        fit_eligible=False,
    )

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gate = next(row for row in model["gates"] if row["gate_id"] == "mature_route_continuity_gate")

    assert model["overall_status"] == "blocked"
    assert model["current_stage"] == "mature_route_continuity_gate"
    assert model["formal_release_allowed"] is False
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert model["linked_inputs"]["mature_route_continuity_gate_json"] == str(continuity_path.resolve())
    assert gate["status"] == "blocked"
    assert gate["release_gate"] is True
    assert gate["blocks_release"] is True
    assert gate["blocks_physical_flow"] is False


def test_formal_run_status_surfaces_initialization_controlled_executor_design_without_unlocking_live(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "ready_run_with_initialization_design"
    _seed_ready_run(run_dir)
    design_path = _seed_formal_initialization_controlled_executor_design(run_dir)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["formal_initialization_controlled_executor_design"]

    assert model["overall_status"] == "formal_release_ready"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert model["linked_inputs"]["formal_initialization_controlled_executor_design_json"] == str(
        design_path.resolve()
    )
    assert gate["status"] == "ready"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "live initialization remains blocked" in gate["reason"]


def test_formal_run_status_blocks_dirty_initialization_controlled_executor_design_side_effects(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "dirty_initialization_design"
    _seed_ready_run(run_dir)
    _seed_formal_initialization_controlled_executor_design(run_dir, side_effect_lock_clean=False)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["formal_initialization_controlled_executor_design"]

    assert model["overall_status"] == "blocked"
    assert model["current_stage"] == "formal_initialization_controlled_executor_design"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert gate["status"] == "blocked"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "no-COM/no-write locks are not preserved" in gate["reason"]


def test_formal_run_status_surfaces_readonly_com_preflight_design_without_unlocking_live(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "ready_run_with_readonly_com_design"
    _seed_ready_run(run_dir)
    design_path = _seed_formal_initialization_readonly_com_preflight_design(run_dir)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["formal_initialization_readonly_com_preflight_design"]

    assert model["overall_status"] == "formal_release_ready"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert model["linked_inputs"]["formal_initialization_readonly_com_preflight_design_json"] == str(
        design_path.resolve()
    )
    assert gate["status"] == "ready"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "real COM remains locked" in gate["reason"]


def test_formal_run_status_blocks_dirty_readonly_com_preflight_design_side_effects(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "dirty_readonly_com_design"
    _seed_ready_run(run_dir)
    _seed_formal_initialization_readonly_com_preflight_design(run_dir, side_effect_lock_clean=False)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["formal_initialization_readonly_com_preflight_design"]

    assert model["overall_status"] == "blocked"
    assert model["current_stage"] == "formal_initialization_readonly_com_preflight_design"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert gate["status"] == "blocked"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "no-COM/no-write locks are not preserved" in gate["reason"]


def test_formal_run_status_surfaces_readonly_com_preflight_blocked_executor_without_unlocking_live(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "ready_run_with_readonly_com_blocked_executor"
    _seed_ready_run(run_dir)
    blocked_executor_path = _seed_formal_initialization_readonly_com_preflight_blocked_executor(run_dir)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["formal_initialization_readonly_com_preflight_blocked_executor"]

    assert model["overall_status"] == "review_required"
    assert model["current_stage"] == "formal_initialization_readonly_com_preflight_blocked_executor"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert model["linked_inputs"]["formal_initialization_readonly_com_preflight_blocked_executor_json"] == str(
        blocked_executor_path.resolve()
    )
    assert gate["status"] == "review_required"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "correctly refused analyzer contact" in gate["reason"]


def test_formal_run_status_blocks_dirty_readonly_com_preflight_blocked_executor_side_effects(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "dirty_readonly_com_blocked_executor"
    _seed_ready_run(run_dir)
    _seed_formal_initialization_readonly_com_preflight_blocked_executor(
        run_dir,
        side_effect_lock_clean=False,
    )

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["formal_initialization_readonly_com_preflight_blocked_executor"]

    assert model["overall_status"] == "blocked"
    assert model["current_stage"] == "formal_initialization_readonly_com_preflight_blocked_executor"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert gate["status"] == "blocked"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "side effects are not locked off" in gate["reason"]


def test_formal_run_status_surfaces_readonly_com_preflight_controlled_executor_design_without_unlocking_live(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "ready_run_with_readonly_com_controlled_design"
    _seed_ready_run(run_dir)
    design_path = _seed_formal_initialization_readonly_com_preflight_controlled_executor_design(run_dir)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["formal_initialization_readonly_com_preflight_controlled_executor_design"]

    assert model["overall_status"] == "formal_release_ready"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert model["linked_inputs"][
        "formal_initialization_readonly_com_preflight_controlled_executor_design_json"
    ] == str(design_path.resolve())
    assert gate["status"] == "ready"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False


def test_formal_run_status_blocks_dirty_readonly_com_preflight_controlled_executor_design_side_effects(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "dirty_readonly_com_controlled_design"
    _seed_ready_run(run_dir)
    _seed_formal_initialization_readonly_com_preflight_controlled_executor_design(
        run_dir,
        side_effect_lock_clean=False,
    )

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["formal_initialization_readonly_com_preflight_controlled_executor_design"]

    assert model["overall_status"] == "blocked"
    assert model["current_stage"] == "formal_initialization_readonly_com_preflight_controlled_executor_design"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert gate["status"] == "blocked"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False


def test_formal_run_status_surfaces_readonly_com_preflight_controlled_blocked_executor_without_unlocking_live(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "ready_run_with_readonly_com_controlled_blocked_executor"
    _seed_ready_run(run_dir)
    blocked_executor_path = _seed_formal_initialization_readonly_com_preflight_controlled_blocked_executor(run_dir)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["formal_initialization_readonly_com_preflight_controlled_blocked_executor"]

    assert model["overall_status"] == "formal_release_ready"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert model["linked_inputs"][
        "formal_initialization_readonly_com_preflight_controlled_blocked_executor_json"
    ] == str(blocked_executor_path.resolve())
    assert gate["status"] == "ready"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "real COM remains disabled" in gate["reason"]


def test_formal_run_status_blocks_dirty_readonly_com_preflight_controlled_blocked_executor_side_effects(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "dirty_readonly_com_controlled_blocked_executor"
    _seed_ready_run(run_dir)
    _seed_formal_initialization_readonly_com_preflight_controlled_blocked_executor(
        run_dir,
        side_effect_lock_clean=False,
    )

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["formal_initialization_readonly_com_preflight_controlled_blocked_executor"]

    assert model["overall_status"] == "blocked"
    assert model["current_stage"] == "formal_initialization_readonly_com_preflight_controlled_blocked_executor"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert gate["status"] == "blocked"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "locks are not preserved" in gate["reason"]


def test_formal_run_status_surfaces_readonly_com_execution_contract_without_unlocking_live(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "ready_run_with_readonly_com_execution_contract"
    _seed_ready_run(run_dir)
    contract_path = _seed_formal_readonly_com_execution_contract(run_dir)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["formal_readonly_com_execution_contract"]

    assert model["overall_status"] == "formal_release_ready"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert model["linked_inputs"]["formal_readonly_com_execution_contract_json"] == str(
        contract_path.resolve()
    )
    assert gate["status"] == "ready"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "live COM remains disabled" in gate["reason"]


def test_formal_run_status_blocks_dirty_readonly_com_execution_contract_side_effects(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "dirty_readonly_com_execution_contract"
    _seed_ready_run(run_dir)
    _seed_formal_readonly_com_execution_contract(run_dir, side_effect_lock_clean=False)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["formal_readonly_com_execution_contract"]

    assert model["overall_status"] == "blocked"
    assert model["current_stage"] == "formal_readonly_com_execution_contract"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert gate["status"] == "blocked"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "boundary locks are not preserved" in gate["reason"]


def test_formal_run_status_surfaces_readonly_com_execution_blocked_executor_without_unlocking_live(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "ready_run_with_readonly_com_execution_blocked_executor"
    _seed_ready_run(run_dir)
    blocked_executor_path = _seed_formal_readonly_com_execution_blocked_executor(run_dir)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["formal_readonly_com_execution_blocked_executor"]

    assert model["overall_status"] == "formal_release_ready"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert model["linked_inputs"]["formal_readonly_com_execution_blocked_executor_json"] == str(
        blocked_executor_path.resolve()
    )
    assert gate["status"] == "ready"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "live COM remains disabled" in gate["reason"]


def test_formal_run_status_blocks_dirty_readonly_com_execution_blocked_executor_side_effects(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "dirty_readonly_com_execution_blocked_executor"
    _seed_ready_run(run_dir)
    _seed_formal_readonly_com_execution_blocked_executor(run_dir, side_effect_lock_clean=False)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["formal_readonly_com_execution_blocked_executor"]

    assert model["overall_status"] == "blocked"
    assert model["current_stage"] == "formal_readonly_com_execution_blocked_executor"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert gate["status"] == "blocked"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "boundary locks are not preserved" in gate["reason"]


def test_formal_run_status_surfaces_readonly_com_execution_packet_validator_without_unlocking_live(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "ready_run_with_readonly_com_execution_packet_validator"
    _seed_ready_run(run_dir)
    packet_path = _seed_formal_readonly_com_execution_packet_validator(
        run_dir,
        packet_status="ready_for_readonly_com_execution_packet_review",
    )

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["formal_readonly_com_execution_packet_validator"]

    assert model["overall_status"] == "formal_release_ready"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert model["linked_inputs"]["formal_readonly_com_execution_packet_validator_json"] == str(
        packet_path.resolve()
    )
    assert gate["status"] == "ready"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "live COM remains disabled" in gate["reason"]
    assert "old-algorithm CHECK skip" in gate["physical_meaning"]


def test_formal_run_status_blocks_dirty_readonly_com_execution_packet_validator_side_effects(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "dirty_readonly_com_execution_packet_validator"
    _seed_ready_run(run_dir)
    _seed_formal_readonly_com_execution_packet_validator(run_dir, side_effect_lock_clean=False)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["formal_readonly_com_execution_packet_validator"]

    assert model["overall_status"] == "blocked"
    assert model["current_stage"] == "formal_readonly_com_execution_packet_validator"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert gate["status"] == "blocked"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "boundary locks are not preserved" in gate["reason"]


def test_formal_run_status_surfaces_readonly_com_execution_plan_preview_without_unlocking_live(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "ready_run_with_readonly_com_execution_plan_preview"
    _seed_ready_run(run_dir)
    plan_path = _seed_formal_readonly_com_execution_plan_preview(run_dir)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["formal_readonly_com_execution_plan_preview"]

    assert model["overall_status"] == "formal_release_ready"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert model["linked_inputs"]["formal_readonly_com_execution_plan_preview_json"] == str(
        plan_path.resolve()
    )
    assert gate["status"] == "ready"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "live COM remains disabled" in gate["reason"]
    assert "GETCO1-9" in gate["physical_meaning"]


def test_formal_run_status_blocks_dirty_readonly_com_execution_plan_preview_side_effects(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "dirty_readonly_com_execution_plan_preview"
    _seed_ready_run(run_dir)
    _seed_formal_readonly_com_execution_plan_preview(run_dir, side_effect_lock_clean=False)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["formal_readonly_com_execution_plan_preview"]

    assert model["overall_status"] == "blocked"
    assert model["current_stage"] == "formal_readonly_com_execution_plan_preview"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert gate["status"] == "blocked"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "boundary locks are not preserved" in gate["reason"]


def test_formal_run_status_surfaces_readonly_com_minimal_executor_stub_without_unlocking_live(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "ready_run_with_readonly_com_minimal_executor_stub"
    _seed_ready_run(run_dir)
    review_path = _seed_formal_readonly_com_minimal_executor_review(run_dir)
    stub_path = _seed_formal_readonly_com_minimal_executor_stub(run_dir)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    review_gate = gates["formal_readonly_com_minimal_executor_review"]
    stub_gate = gates["formal_readonly_com_minimal_executor_stub"]

    assert model["overall_status"] == "formal_release_ready"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert model["linked_inputs"]["formal_readonly_com_minimal_executor_review_json"] == str(
        review_path.resolve()
    )
    assert model["linked_inputs"]["formal_readonly_com_minimal_executor_stub_json"] == str(
        stub_path.resolve()
    )
    assert review_gate["status"] == "ready"
    assert stub_gate["status"] == "ready"
    assert stub_gate["release_gate"] is False
    assert stub_gate["blocks_release"] is False
    assert stub_gate["blocks_physical_flow"] is False
    assert "COM remains locked" in stub_gate["reason"]


def test_formal_run_status_blocks_dirty_readonly_com_minimal_executor_stub_side_effects(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "dirty_readonly_com_minimal_executor_stub"
    _seed_ready_run(run_dir)
    _seed_formal_readonly_com_minimal_executor_review(run_dir)
    _seed_formal_readonly_com_minimal_executor_stub(run_dir, side_effect_lock_clean=False)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["formal_readonly_com_minimal_executor_stub"]

    assert model["overall_status"] == "blocked"
    assert model["current_stage"] == "formal_readonly_com_minimal_executor_stub"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert gate["status"] == "blocked"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "boundary locks are not preserved" in gate["reason"]


def test_formal_run_status_surfaces_optional_algorithm_profile_runner_bundle(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_run_with_algorithm_profile"
    _seed_ready_run(run_dir)
    bundle_path = _seed_algorithm_profile_runner_dry_run(run_dir)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["algorithm_profile_runner_dry_run"]

    assert model["overall_status"] == "formal_release_ready"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert model["linked_inputs"]["algorithm_profile_runner_dry_run_json"] == str(bundle_path.resolve())
    assert gate["status"] == "ready"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "CO2/H2O=47/14" in gate["reason"]
    assert "without executing queues" in gate["physical_meaning"]


def test_formal_run_status_surfaces_full_flow_automation_closure_without_unlocking_full_auto(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "ready_run_with_automation_closure"
    _seed_ready_run(run_dir)
    closure_path = _seed_full_flow_automation_closure(run_dir)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["full_flow_automation_closure"]

    assert model["overall_status"] == "formal_release_ready"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["full_production_auto_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert model["linked_inputs"]["full_flow_automation_closure_json"] == str(closure_path.resolve())
    assert gate["status"] == "ready"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "full production automation still has 9 gated handoff" in gate["reason"]
    assert "0613 fitting and 0620/0621 mature physical routes" in gate["physical_meaning"]


def test_formal_run_status_marks_dirty_full_flow_automation_closure_review_only(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_run_with_dirty_automation_closure"
    _seed_ready_run(run_dir)
    _seed_full_flow_automation_closure(run_dir, boundary_clean=False)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["full_flow_automation_closure"]

    assert model["overall_status"] == "review_required"
    assert model["current_stage"] == "full_flow_automation_closure"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["full_production_auto_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert gate["status"] == "review_required"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "offline_boundary_not_clean" in gate["reason"]


def test_formal_run_status_surfaces_database_dry_run_without_authorizing_import(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_run_with_database_dry_run"
    _seed_ready_run(run_dir)
    database_path = _seed_formal_database_dry_run(run_dir)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["formal_database_dry_run"]

    assert model["overall_status"] == "formal_release_ready"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert model["linked_inputs"]["formal_database_dry_run_json"] == str(database_path.resolve())
    assert gate["status"] == "ready"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "PostgreSQL 18" in gate["reason"]
    assert "without connecting" in gate["physical_meaning"]


def test_formal_run_status_surfaces_database_import_preflight_without_authorizing_import(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_run_with_database_import_preflight"
    _seed_ready_run(run_dir)
    database_path = _seed_formal_database_dry_run(run_dir)
    import_preflight_path = _seed_formal_database_import_preflight(run_dir)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["formal_database_import_preflight"]

    assert model["overall_status"] == "formal_release_ready"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert model["linked_inputs"]["formal_database_dry_run_json"] == str(database_path.resolve())
    assert model["linked_inputs"]["formal_database_import_preflight_json"] == str(import_preflight_path.resolve())
    assert gate["status"] == "ready"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "real import remains separately unauthorized" in gate["reason"]
    assert "without opening PostgreSQL" in gate["physical_meaning"]


def test_formal_run_status_marks_database_import_preflight_review_only(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_run_with_database_import_preflight_review"
    _seed_ready_run(run_dir)
    _seed_formal_database_dry_run(run_dir)
    _seed_formal_database_import_preflight(
        run_dir,
        review_required_count=1,
        dsn_configured=False,
    )

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["formal_database_import_preflight"]

    assert model["overall_status"] == "review_required"
    assert model["current_stage"] == "formal_database_import_preflight"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert gate["status"] == "review_required"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "dsn_configured=False" in gate["reason"]


def test_formal_run_status_surfaces_database_import_authorization_without_authorizing_import(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_run_with_database_import_authorization"
    _seed_ready_run(run_dir)
    _seed_formal_database_dry_run(run_dir)
    _seed_formal_database_import_preflight(run_dir)
    authorization_path = _seed_formal_database_import_authorization(run_dir)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["formal_database_import_authorization"]

    assert model["overall_status"] == "formal_release_ready"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert model["linked_inputs"]["formal_database_import_authorization_json"] == str(authorization_path.resolve())
    assert gate["status"] == "ready"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "real import remains a separate command" in gate["reason"]
    assert "no-connect/no-import" in gate["physical_meaning"]


def test_formal_run_status_surfaces_database_import_command_contract_without_authorizing_import(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_run_with_database_import_command_contract"
    _seed_ready_run(run_dir)
    _seed_formal_database_dry_run(run_dir)
    _seed_formal_database_import_preflight(run_dir)
    _seed_formal_database_import_authorization(run_dir)
    command_contract_path = _seed_formal_database_import_command_contract(run_dir)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["formal_database_import_command_contract"]

    assert model["overall_status"] == "formal_release_ready"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert model["linked_inputs"]["formal_database_import_command_contract_json"] == str(
        command_contract_path.resolve()
    )
    assert "formal_database_import_controlled_executor_design" not in gates
    assert gate["status"] == "ready"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "does not execute import" in gate["reason"]
    assert "locked off" in gate["physical_meaning"]


def test_formal_run_status_keeps_import_locked_when_blocked_executor_exists(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_run_with_blocked_import_executor"
    _seed_ready_run(run_dir)
    _seed_formal_database_dry_run(run_dir)
    _seed_formal_database_import_preflight(run_dir)
    _seed_formal_database_import_authorization(run_dir)
    _seed_formal_database_import_command_contract(run_dir)
    blocked_executor_path = _seed_formal_database_import_blocked_executor(run_dir)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["formal_database_import_blocked_executor"]

    assert model["overall_status"] == "review_required"
    assert model["current_stage"] == "formal_database_import_blocked_executor"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert model["linked_inputs"]["formal_database_import_blocked_executor_json"] == str(
        blocked_executor_path.resolve()
    )
    assert gate["status"] == "review_required"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "correctly refused real import" in gate["reason"]


def test_formal_run_status_accepts_controlled_executor_design_without_unlocking_import(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_run_with_controlled_import_design"
    _seed_ready_run(run_dir)
    _seed_formal_database_dry_run(run_dir)
    _seed_formal_database_import_preflight(run_dir)
    _seed_formal_database_import_authorization(run_dir)
    _seed_formal_database_import_command_contract(run_dir)
    _seed_formal_database_import_blocked_executor(run_dir)
    design_path = _seed_formal_database_import_controlled_executor_design(run_dir)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["formal_database_import_controlled_executor_design"]

    assert model["overall_status"] == "review_required"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert model["linked_inputs"]["formal_database_import_controlled_executor_design_json"] == str(
        design_path.resolve()
    )
    assert gate["status"] == "ready"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "execution remains blocked" in gate["reason"]


def test_formal_run_status_blocks_database_import_when_controlled_executor_design_requires_review(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "ready_run_with_controlled_import_design_review"
    _seed_ready_run(run_dir)
    _seed_formal_database_dry_run(run_dir)
    _seed_formal_database_import_preflight(run_dir)
    _seed_formal_database_import_authorization(run_dir)
    _seed_formal_database_import_command_contract(run_dir)
    design_path = _seed_formal_database_import_controlled_executor_design(
        run_dir,
        review_required_count=1,
    )

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["formal_database_import_controlled_executor_design"]

    assert model["overall_status"] == "review_required"
    assert model["current_stage"] == "formal_database_import_controlled_executor_design"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert model["linked_inputs"]["formal_database_import_controlled_executor_design_json"] == str(
        design_path.resolve()
    )
    assert gate["status"] == "review_required"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "review_required_count=1" in gate["reason"]


def test_formal_run_status_blocks_dirty_import_executor_side_effects(tmp_path: Path) -> None:
    run_dir = tmp_path / "dirty_blocked_import_executor"
    _seed_ready_run(run_dir)
    _seed_formal_database_dry_run(run_dir)
    _seed_formal_database_import_preflight(run_dir)
    _seed_formal_database_import_authorization(run_dir)
    _seed_formal_database_import_command_contract(run_dir)
    _seed_formal_database_import_blocked_executor(run_dir, side_effect_lock_clean=False)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["formal_database_import_blocked_executor"]

    assert model["overall_status"] == "blocked"
    assert model["database_import_allowed"] is False
    assert gate["status"] == "blocked"
    assert "boundary is not clean" in gate["reason"]


def test_formal_run_status_marks_database_import_command_contract_review_only(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_run_with_database_import_command_contract_review"
    _seed_ready_run(run_dir)
    _seed_formal_database_dry_run(run_dir)
    _seed_formal_database_import_preflight(run_dir)
    _seed_formal_database_import_authorization(run_dir)
    _seed_formal_database_import_command_contract(
        run_dir,
        review_required_count=2,
        evidence_bundle_ready=False,
        command_contract_ready=False,
    )

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["formal_database_import_command_contract"]

    assert model["overall_status"] == "review_required"
    assert model["current_stage"] == "formal_database_import_command_contract"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert gate["status"] == "review_required"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "evidence_bundle_ready=False" in gate["reason"]
    assert "command_contract_ready=False" in gate["reason"]


def test_formal_run_status_marks_database_import_authorization_review_only(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_run_with_database_import_authorization_review"
    _seed_ready_run(run_dir)
    _seed_formal_database_dry_run(run_dir)
    _seed_formal_database_import_preflight(run_dir)
    _seed_formal_database_import_authorization(
        run_dir,
        review_required_count=2,
        archive_release_ready=False,
        manual_authorization_ready=False,
    )

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["formal_database_import_authorization"]

    assert model["overall_status"] == "review_required"
    assert model["current_stage"] == "formal_database_import_authorization"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert gate["status"] == "review_required"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "archive_release_ready=False" in gate["reason"]
    assert "manual_authorization_ready=False" in gate["reason"]


def test_formal_run_status_marks_database_dry_run_review_only(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_run_with_blocked_database_dry_run"
    _seed_ready_run(run_dir)
    _seed_formal_database_dry_run(run_dir, blocker_count=1)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["formal_database_dry_run"]

    assert model["overall_status"] == "review_required"
    assert model["current_stage"] == "formal_database_dry_run"
    assert model["formal_release_allowed"] is True
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert gate["status"] == "review_required"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "blocker_count=1" in gate["reason"]


def test_formal_run_status_marks_algorithm_profile_runner_bundle_review_only(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_run_with_blocked_algorithm_profile"
    _seed_ready_run(run_dir)
    _seed_algorithm_profile_runner_dry_run(run_dir, blocker_count=1)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    gate = gates["algorithm_profile_runner_dry_run"]

    assert model["overall_status"] == "review_required"
    assert model["current_stage"] == "algorithm_profile_runner_dry_run"
    assert model["formal_release_allowed"] is True
    assert model["can_continue_physical_flow"] is True
    assert gate["status"] == "review_required"
    assert gate["release_gate"] is False
    assert gate["blocks_release"] is False
    assert gate["blocks_physical_flow"] is False
    assert "blocker_count=1" in gate["reason"]


def test_formal_run_status_sn_review_blocks_release_not_physical_flow(tmp_path: Path) -> None:
    run_dir = tmp_path / "sn_review"
    _seed_ready_run(run_dir)
    _write_json(
        run_dir / "identity" / "v1_5_getco_identity_readiness.json",
        {
            "schema": "v1_5_getco_identity_readiness_v1",
            "overall_status": "identity_getco_ready_for_auxiliary_neutralization",
            "traceability_review_required": True,
        },
    )

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}

    assert model["overall_status"] == "review_required"
    assert model["current_stage"] == "identity_getco_sn_traceability"
    assert model["formal_release_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert gates["identity_getco_sn_traceability"]["status"] == "review_required"
    assert gates["identity_getco_sn_traceability"]["blocks_release"] is True
    assert gates["identity_getco_sn_traceability"]["blocks_physical_flow"] is False


def test_formal_run_status_requires_archive_index_even_when_closure_ready(tmp_path: Path) -> None:
    run_dir = tmp_path / "closure_ready_archive_missing"
    _seed_ready_run(run_dir)
    (run_dir / "archive" / "v1_5_formal_archive_closure_index.json").unlink()

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    gates = {row["gate_id"]: row for row in model["gates"]}
    release_gate = gates["formal_archive_database_release"]

    assert model["overall_status"] == "in_progress"
    assert model["current_stage"] == "formal_archive_database_release"
    assert model["formal_release_allowed"] is False
    assert model["database_import_allowed"] is False
    assert model["can_continue_physical_flow"] is True
    assert release_gate["status"] == "missing"
    assert release_gate["blocks_release"] is True
    assert release_gate["blocks_physical_flow"] is False
    assert "archive closure index is missing" in release_gate["reason"]


def test_formal_run_status_empty_run_dir_is_offline_todo_only(tmp_path: Path) -> None:
    run_dir = tmp_path / "empty"
    run_dir.mkdir()

    model = build_v1_5_formal_run_status(run_dir=run_dir)

    assert model["overall_status"] == "in_progress"
    assert model["current_stage"] == "initialization_readiness"
    assert model["formal_release_allowed"] is False
    assert model["can_continue_physical_flow"] is False
    assert model["physical_boundaries"]["opens_com_ports"] is False
    assert model["physical_boundaries"]["writes_coefficients"] is False
    assert model["physical_boundaries"]["connects_postgresql"] is False
    assert model["physical_boundaries"]["real_import_execution_allowed"] is False
    assert model["physical_boundaries"]["database_written"] is False


def test_formal_run_status_writes_json_markdown_and_csv(tmp_path: Path) -> None:
    run_dir = tmp_path / "ready_for_export"
    output_dir = tmp_path / "out"
    _seed_ready_run(run_dir)

    model = build_v1_5_formal_run_status(run_dir=run_dir)
    outputs = write_v1_5_formal_run_status_outputs(model, output_dir)
    markdown = render_v1_5_formal_run_status_markdown(model)

    assert "formal_release_allowed: `True`" in markdown
    assert Path(outputs["json_path"]).exists()
    assert Path(outputs["markdown_path"]).exists()
    assert Path(outputs["gates_csv_path"]).exists()
    assert Path(outputs["gaps_csv_path"]).exists()
    exported = json.loads(Path(outputs["json_path"]).read_text(encoding="utf-8"))
    assert exported["overall_status"] == "formal_release_ready"
    with Path(outputs["gates_csv_path"]).open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert {row["gate_id"] for row in rows} >= {
        "initialization_readiness",
        "formal_archive_database_release",
    }


def test_formal_run_status_cli_exports_rollup(tmp_path: Path, capsys) -> None:
    run_dir = tmp_path / "ready_cli"
    output_dir = tmp_path / "cli_out"
    _seed_ready_run(run_dir)
    readonly_com_blocked_executor_path = _seed_formal_initialization_readonly_com_preflight_blocked_executor(run_dir)
    readonly_com_controlled_executor_design_path = (
        _seed_formal_initialization_readonly_com_preflight_controlled_executor_design(run_dir)
    )
    bundle_path = _seed_algorithm_profile_runner_dry_run(run_dir)
    automation_closure_path = _seed_full_flow_automation_closure(run_dir)
    database_path = _seed_formal_database_dry_run(run_dir)
    import_preflight_path = _seed_formal_database_import_preflight(run_dir)
    import_authorization_path = _seed_formal_database_import_authorization(run_dir)
    import_command_contract_path = _seed_formal_database_import_command_contract(run_dir)
    import_blocked_executor_path = _seed_formal_database_import_blocked_executor(run_dir)
    import_controlled_executor_design_path = _seed_formal_database_import_controlled_executor_design(run_dir)
    minimal_executor_stub_path = _seed_formal_readonly_com_minimal_executor_stub(run_dir)
    pressure_s9_index_path = _seed_pressure_s9_readiness_index(run_dir)
    batch_closeout_path = _seed_batch_initialization_closeout(run_dir)
    resume_gate_path = _seed_post_closeout_resume_gate(run_dir)

    rc = export_status_main(
        [
            "--run-dir",
            str(run_dir),
            "--output-dir",
            str(output_dir),
            "--formal-initialization-readonly-com-preflight-blocked-executor-json",
            str(readonly_com_blocked_executor_path),
            "--formal-initialization-readonly-com-preflight-controlled-executor-design-json",
            str(readonly_com_controlled_executor_design_path),
            "--algorithm-profile-runner-dry-run-json",
            str(bundle_path),
            "--full-flow-automation-closure-json",
            str(automation_closure_path),
            "--formal-database-dry-run-json",
            str(database_path),
            "--formal-database-import-preflight-json",
            str(import_preflight_path),
            "--formal-database-import-authorization-json",
            str(import_authorization_path),
            "--formal-database-import-command-contract-json",
            str(import_command_contract_path),
            "--formal-database-import-blocked-executor-json",
            str(import_blocked_executor_path),
            "--formal-database-import-controlled-executor-design-json",
            str(import_controlled_executor_design_path),
            "--formal-readonly-com-minimal-executor-stub-json",
            str(minimal_executor_stub_path),
            "--pressure-s9-readiness-index-json",
            str(pressure_s9_index_path),
            "--batch-initialization-closeout-json",
            str(batch_closeout_path),
            "--post-closeout-resume-gate-json",
            str(resume_gate_path),
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert rc == 0
    assert payload["overall_status"] == "review_required"
    assert payload["physical_boundaries"]["opens_com_ports"] is False
    assert (output_dir / "v1_5_formal_run_status.json").exists()
    assert (output_dir / "v1_5_formal_run_status_gates.csv").exists()
    exported = json.loads((output_dir / "v1_5_formal_run_status.json").read_text(encoding="utf-8"))
    gates = {row["gate_id"]: row for row in exported["gates"]}
    assert gates["formal_initialization_readonly_com_preflight_blocked_executor"]["status"] == "review_required"
    assert (
        gates["formal_initialization_readonly_com_preflight_controlled_executor_design"]["status"]
        == "ready"
    )
    assert gates["algorithm_profile_runner_dry_run"]["status"] == "ready"
    assert gates["full_flow_automation_closure"]["status"] == "ready"
    assert gates["formal_database_dry_run"]["status"] == "ready"
    assert gates["formal_database_import_preflight"]["status"] == "ready"
    assert gates["formal_database_import_authorization"]["status"] == "ready"
    assert gates["formal_database_import_command_contract"]["status"] == "ready"
    assert gates["formal_database_import_blocked_executor"]["status"] == "review_required"
    assert gates["formal_database_import_controlled_executor_design"]["status"] == "ready"
    assert gates["formal_readonly_com_minimal_executor_stub"]["status"] == "ready"
    assert gates["batch_initialization_closeout"]["status"] == "ready"
    assert gates["post_closeout_resume_gate"]["status"] == "ready"
    assert gates["pressure_senco9_pre_open_flow"]["status"] == "ready"
    assert exported["linked_inputs"]["formal_readonly_com_minimal_executor_stub_json"] == str(
        minimal_executor_stub_path.resolve()
    )
    assert exported["linked_inputs"]["pressure_s9_readiness_index_json"] == str(
        pressure_s9_index_path.resolve()
    )
    assert exported["linked_inputs"]["full_flow_automation_closure_json"] == str(
        automation_closure_path.resolve()
    )
    assert exported["database_import_allowed"] is False
    assert exported["full_production_auto_allowed"] is False
