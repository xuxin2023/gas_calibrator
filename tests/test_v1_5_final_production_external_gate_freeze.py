import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_final_production_external_gate_freeze import main as cli_main
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_final_production_external_gate_freeze import (
    PROGRAM_CAPABILITIES,
    READY_STATUS,
    REMAINING_EXTERNAL_GATES,
    SCHEMA,
    build_v1_5_final_production_external_gate_freeze,
    write_v1_5_final_production_external_gate_freeze,
)


ROOT = Path(__file__).resolve().parents[1]
SOURCE_MAIN = "dbbed56689f2d48bd79339fa9af8bea58775fed4"


def _write_confirmed_migration(path: Path, *, authorization_validated: bool = True) -> Path:
    payload = {
        "schema": "v1_5_formal_database_migration_production_controlled_executor_v1",
        "overall_status": "production_migration_002_idempotent_noop",
        "authorization_validation_requested": True,
        "authorization_validated": authorization_validated,
        "execution_attempted": True,
        "connects_postgresql": True,
        "applies_migrations": True,
        "transaction_committed": True,
        "commit_uncertain": False,
        "migration_execution_confirmed": True,
        "production_import_execution_allowed": False,
        "database_import_allowed": False,
        "formal_release_allowed": False,
        "opens_com_ports": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "not_real_acceptance_evidence": True,
        "evidence_source": "postgresql18_migration_002_controlled_execution",
        "production_target": {
            "backend": "postgresql",
            "postgresql_major": 18,
            "database_name": "gas_calibrator",
            "core_schema": "public",
            "evidence_schema": "v1_5_evidence",
            "dsn_env": "V1_5_POSTGRES_DSN",
        },
        "migration_versions": [
            "001_v1_5_evidence_registry",
            "002_v1_5_production_import_ledger",
        ],
        "postcheck_reasons": [],
        "authorization_record": {
            "authorization_id": "auth-migration-002",
            "operator": "operator",
            "reviewer": "reviewer",
            "approver": "approver",
            "three_distinct_actors": True,
            "confirmation_matched": True,
        },
        "postcheck_state": {
            "database_name": "gas_calibrator",
            "postgresql_server_version_num": 180003,
            "postgresql_system_identifier": "7619229688891748052",
            "migration_001_checksum": "a" * 64,
            "migration_002_checksum": "b" * 64,
            "expected_migration_001_checksum": "a" * 64,
            "expected_migration_002_checksum": "b" * 64,
            "ledger_table_present": True,
            "ledger_columns": [
                "run_db_id",
                "run_id",
                "evidence_bundle_sha256",
                "transaction_plan_sha256",
                "promotion_preflight_sha256",
                "execution_authorization_sha256",
                "authorization_id",
                "operator_name",
                "reviewer_name",
                "approver_name",
                "table_counts",
                "committed_at",
            ],
        },
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_external_gate_freeze_separates_program_closure_from_real_production() -> None:
    model = build_v1_5_final_production_external_gate_freeze(
        repository_root=ROOT,
        source_origin_main_commit=SOURCE_MAIN,
    )

    assert model["schema"] == SCHEMA
    assert model["overall_status"] == READY_STATUS
    assert model["scope_frozen"] is True
    assert model["program_structure_and_offline_automation_complete"] is True
    assert model["live_production_automation_complete"] is False
    assert model["real_production_acceptance_complete"] is False
    assert model["program_capability_count"] == len(PROGRAM_CAPABILITIES) == 7
    assert model["remaining_external_gate_count"] == len(REMAINING_EXTERNAL_GATES) == 6
    assert model["recommended_next_gate_id"] == (
        "production_postgresql18_migration_002_authorization_and_execution"
    )
    assert model["review_reasons"] == []
    assert all(row["status"] == "bound" for row in model["source_evidence"])


def test_external_gate_freeze_keeps_mature_routes_and_algorithm_counts() -> None:
    model = build_v1_5_final_production_external_gate_freeze(
        repository_root=ROOT,
        source_origin_main_commit=SOURCE_MAIN,
    )

    assert model["mature_fitting_baseline"] == "0613 V1.5 fitting path"
    assert model["mature_route_baseline"] == "0620/0621 clean-worktree mature physical route path"
    assert model["legacy_point_counts"] == {"co2": 45, "h2o": 13}
    assert model["new_algorithm_point_counts"] == {"co2": 47, "h2o": 14}
    capability_ids = {row["capability_id"] for row in model["program_capabilities"]}
    assert "new_algorithm_47_14_mature_queue_handoff" in capability_ids
    assert "postgresql18_staging_migration_import_chain" in capability_ids


def test_external_gate_freeze_lists_only_real_external_gates() -> None:
    model = build_v1_5_final_production_external_gate_freeze(
        repository_root=ROOT,
        source_origin_main_commit=SOURCE_MAIN,
    )
    gate_ids = [row["gate_id"] for row in model["remaining_external_gates"]]

    assert [row["priority"] for row in model["remaining_external_gates"]] == list(range(1, 7))
    assert "legacy_full_flow_orchestrator_offline_replay" not in gate_ids
    assert "final_offline_acceptance_suite" not in gate_ids
    assert "current_batch_continuous_mature_route_evidence" in gate_ids
    assert "device_controlled_write_readback_short_reverify" in gate_ids
    assert "production_evidence_import_archive_and_release" in gate_ids


def test_external_gate_freeze_keeps_every_real_action_locked() -> None:
    model = build_v1_5_final_production_external_gate_freeze(
        repository_root=ROOT,
        source_origin_main_commit=SOURCE_MAIN,
    )

    assert model["postgresql18_real_staging_integration_verified"] is True
    for key in (
        "full_production_auto_allowed",
        "live_queue_execution_allowed",
        "formal_release_allowed",
        "database_import_allowed",
        "production_database_migrated",
        "production_database_written",
        "opens_com_ports",
        "controls_pressure",
        "controls_water_or_gas_routes",
        "writes_coefficients",
        "writes_sn_or_device_code",
        "connects_postgresql",
    ):
        assert model[key] is False
    assert model["not_real_acceptance_evidence"] is True


def test_confirmed_migration_advances_only_the_migration_external_gate(
    tmp_path: Path,
) -> None:
    migration = _write_confirmed_migration(tmp_path / "migration.json")

    model = build_v1_5_final_production_external_gate_freeze(
        repository_root=ROOT,
        source_origin_main_commit=SOURCE_MAIN,
        production_migration_execution_json=migration,
    )

    assert model["overall_status"] == READY_STATUS
    assert model["completed_external_gate_count"] == 1
    assert model["remaining_external_gate_count"] == 5
    assert model["production_database_migrated"] is True
    assert model["production_database_written"] is False
    assert model["recommended_next_gate_id"] == (
        "current_batch_continuous_mature_route_evidence"
    )
    assert model["completed_external_gates"][0]["gate_id"] == (
        "production_postgresql18_migration_002_authorization_and_execution"
    )
    assert model["remaining_external_gates"][0]["original_priority"] == 2
    assert model["remaining_external_gates"][0]["priority"] == 1
    assert model["database_import_allowed"] is False
    assert model["formal_release_allowed"] is False
    assert model["connects_postgresql"] is False


def test_invalid_migration_artifact_does_not_advance_the_gate(tmp_path: Path) -> None:
    migration = _write_confirmed_migration(
        tmp_path / "migration.json", authorization_validated=False
    )

    model = build_v1_5_final_production_external_gate_freeze(
        repository_root=ROOT,
        source_origin_main_commit=SOURCE_MAIN,
        production_migration_execution_json=migration,
    )

    assert model["overall_status"] != READY_STATUS
    assert model["production_database_migrated"] is False
    assert model["completed_external_gate_count"] == 0
    assert model["remaining_external_gate_count"] == 6
    assert model["recommended_next_gate_id"] == (
        "production_postgresql18_migration_002_authorization_and_execution"
    )
    assert (
        "production_migration_execution_flag_not_true:authorization_validated"
        in model["review_reasons"]
    )


def test_migration_artifact_with_unlocked_scope_does_not_advance_the_gate(
    tmp_path: Path,
) -> None:
    migration = _write_confirmed_migration(tmp_path / "migration.json")
    payload = json.loads(migration.read_text(encoding="utf-8"))
    payload["database_import_allowed"] = True
    migration.write_text(json.dumps(payload), encoding="utf-8")

    model = build_v1_5_final_production_external_gate_freeze(
        repository_root=ROOT,
        source_origin_main_commit=SOURCE_MAIN,
        production_migration_execution_json=migration,
    )

    assert model["production_database_migrated"] is False
    assert model["completed_external_gate_count"] == 0
    assert (
        "production_migration_execution_scope_lock_not_false:database_import_allowed"
        in model["review_reasons"]
    )


def test_external_gate_freeze_reviews_invalid_commit_or_missing_evidence(tmp_path: Path) -> None:
    model = build_v1_5_final_production_external_gate_freeze(
        repository_root=tmp_path,
        source_origin_main_commit="not-a-commit",
    )

    assert model["overall_status"] != READY_STATUS
    assert model["scope_frozen"] is False
    assert model["program_structure_and_offline_automation_complete"] is False
    assert "source_origin_main_commit_invalid" in model["review_reasons"]
    assert any(reason.startswith("source_evidence_missing:") for reason in model["review_reasons"])
    assert model["opens_com_ports"] is False


def test_external_gate_freeze_writer_cli_and_entrypoint(tmp_path: Path) -> None:
    output_dir = tmp_path / "freeze"
    paths = write_v1_5_final_production_external_gate_freeze(
        output_dir=output_dir,
        repository_root=ROOT,
        source_origin_main_commit=SOURCE_MAIN,
    )
    assert all(path.is_file() for path in paths.values())
    manifest = json.loads(paths["manifest"].read_text(encoding="utf-8"))
    markdown = paths["markdown"].read_text(encoding="utf-8")
    assert manifest["overall_status"] == READY_STATUS
    assert "已完成的程序能力" in markdown
    assert "剩余真实生产外部门禁" in markdown

    cli_dir = tmp_path / "cli"
    assert cli_main(
        [
            "--repository-root",
            str(ROOT),
            "--source-origin-main-commit",
            SOURCE_MAIN,
            "--output-dir",
            str(cli_dir),
        ]
    ) == 0
    assert (cli_dir / "v1_5_final_production_external_gate_freeze.json").is_file()

    migrated_cli_dir = tmp_path / "migrated_cli"
    migration = _write_confirmed_migration(tmp_path / "migration.json")
    assert cli_main(
        [
            "--repository-root",
            str(ROOT),
            "--source-origin-main-commit",
            SOURCE_MAIN,
            "--production-migration-execution-json",
            str(migration),
            "--output-dir",
            str(migrated_cli_dir),
        ]
    ) == 0
    migrated = json.loads(
        (migrated_cli_dir / "v1_5_final_production_external_gate_freeze.json").read_text(
            encoding="utf-8"
        )
    )
    assert migrated["production_database_migrated"] is True
    assert migrated["recommended_next_gate_id"] == (
        "current_batch_continuous_mature_route_evidence"
    )

    entry = classify_v1_5_entrypoint(
        ROOT / "src/gas_calibrator/tools/export_v1_5_final_production_external_gate_freeze.py",
        root=ROOT,
    )
    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
    assert any("external-gate freeze" in note for note in entry.notes)
