import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_formal_run_status import main as export_status_main
from gas_calibrator.validation.v1_5_formal_run_status import (
    build_v1_5_formal_run_status,
    render_v1_5_formal_run_status_markdown,
    write_v1_5_formal_run_status_outputs,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _seed_ready_run(root: Path) -> None:
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
            "archive_release_ready": archive_release_ready,
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
            "preflight_ready": preflight_ready,
            "archive_release_ready": archive_release_ready,
            "evidence_bundle_ready": evidence_bundle_ready,
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
    database_path = _seed_formal_database_dry_run(run_dir)
    import_preflight_path = _seed_formal_database_import_preflight(run_dir)
    import_authorization_path = _seed_formal_database_import_authorization(run_dir)
    import_command_contract_path = _seed_formal_database_import_command_contract(run_dir)
    import_blocked_executor_path = _seed_formal_database_import_blocked_executor(run_dir)
    import_controlled_executor_design_path = _seed_formal_database_import_controlled_executor_design(run_dir)

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
    assert gates["formal_database_dry_run"]["status"] == "ready"
    assert gates["formal_database_import_preflight"]["status"] == "ready"
    assert gates["formal_database_import_authorization"]["status"] == "ready"
    assert gates["formal_database_import_command_contract"]["status"] == "ready"
    assert gates["formal_database_import_blocked_executor"]["status"] == "review_required"
    assert gates["formal_database_import_controlled_executor_design"]["status"] == "ready"
    assert exported["database_import_allowed"] is False
