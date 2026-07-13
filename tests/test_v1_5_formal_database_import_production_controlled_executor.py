from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from gas_calibrator.storage.v1_5_evidence.bundle import TABLE_NAMES
from gas_calibrator.storage.v1_5_evidence.production_import import (
    PRODUCTION_DATABASE_NAME,
    ProductionImportError,
    validate_production_dsn,
)
from gas_calibrator.storage.v1_5_evidence.production_migration import (
    EXPECTED_CONSTRAINTS,
    LEDGER_COLUMNS,
    LEDGER_INDEX,
)
from gas_calibrator.storage.v1_5_evidence.schema import load_migrations
from gas_calibrator.tools import (
    run_v1_5_formal_database_import_production_controlled_executor as cli,
)
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_formal_database_import_production_controlled_executor import (
    AUTHORIZATION_SCHEMA,
    CONFIRMATION_TEMPLATE,
    EXECUTE_FLAG,
    build_production_import_preview,
    execute_reviewed_production_import,
    validate_execution_authorization,
)
from gas_calibrator.validation.v1_5_formal_database_import_production_promotion_preflight import (
    build_v1_5_formal_database_import_production_promotion_preflight,
)


def _write(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path.resolve()


def _sha256(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()


def _make_package(tmp_path: Path, *, count: int = 2) -> dict[str, Path]:
    run_id = "v1_5_production_executor_test_run"
    run_db_id = "22222222-2222-4222-8222-222222222222"
    devices = [
        {
            "slot": f"GA{index:02d}",
            "sn_code": f"012608{index:02d}",
            "device_code": f"012608{index:02d}",
            "protocol_device_id": f"{index:03d}",
            "port": f"COM{40 + index}",
        }
        for index in range(1, count + 1)
    ]
    tables = {name: [] for name in TABLE_NAMES}
    tables["runs"] = [{"id": run_db_id, "run_id": run_id}]
    tables["devices"] = [
        {
            "id": f"device-{row['slot']}",
            "serial_number": row["protocol_device_id"],
            "metadata": {"protocol_device_id": row["protocol_device_id"]},
        }
        for row in devices
    ]
    tables["run_devices"] = [
        {
            "id": f"run-device-{row['slot']}",
            "run_db_id": run_db_id,
            "device_id": f"device-{row['slot']}",
            "role": "device_under_test",
        }
        for row in devices
    ]
    bundle = _write(
        tmp_path / "evidence_bundle.json",
        {
            "schema": "v1_5_evidence_registry",
            "run_id": run_id,
            "run_db_id": run_db_id,
            "tables": tables,
        },
    )

    bound_payloads = {
        "controlled_executor_design": {
            "schema": "v1_5_formal_database_import_controlled_executor_design_v1",
            "overall_status": "ready_for_controlled_import_executor_design_review",
            "execution_supported": False,
            "real_import_execution_allowed": False,
            "database_import_allowed": False,
            "connects_postgresql": False,
            "database_written": False,
        },
        "command_contract": {
            "schema": "v1_5_formal_database_import_command_contract_v1",
            "overall_status": "ready_for_controlled_postgresql18_import_command_review",
            "command_contract_ready": True,
            "database_import_authorization_binding_ready": True,
            "database_import_preflight_binding_ready": True,
            "archive_release_ready": True,
            "evidence_bundle_ready": True,
            "connects_postgresql": False,
            "database_written": False,
            "database_import_allowed": False,
        },
        "formal_database_import_authorization": {
            "schema": "v1_5_formal_database_import_authorization_v1",
            "overall_status": "ready_for_manual_postgresql18_import_authorization",
            "manual_authorization_ready": True,
            "archive_release_ready": True,
            "database_import_allowed": True,
            "formal_release_allowed": True,
            "authorization_id": "package-auth-001",
            "operator": "package-operator",
            "reviewer": "package-reviewer",
            "approver": "package-approver",
        },
        "formal_database_import_preflight": {
            "schema": "v1_5_formal_database_import_preflight_v1",
            "overall_status": "ready_for_authorized_postgresql18_import_review",
            "production_backend": "postgresql",
            "production_postgresql_major": 18,
            "dsn_configured": True,
            "dry_run_contract_ready": True,
            "connects_postgresql": False,
            "database_written": False,
            "database_import_allowed": False,
            "formal_release_allowed": False,
        },
        "archive_closure": {
            "schema": "v1_5_formal_archive_closure_v1",
            "overall_status": "ready",
            "package_status": "ready",
            "identity_getco_traceability": {
                "ready_for_archive_release": True,
                "traceability_review_required": False,
            },
        },
    }
    bound_paths = {
        role: _write(tmp_path / f"{role}.json", payload)
        for role, payload in bound_payloads.items()
    }
    bound_paths["evidence_bundle"] = bundle
    plan = _write(
        tmp_path / "transaction_plan.json",
        {
            "schema": "v1_5_formal_database_import_transaction_plan_v1",
            "overall_status": "ready_for_postgresql18_transaction_plan_review",
            "transaction_plan_contract_ready": True,
            "production_transaction_package_ready": True,
            "production_blocking_reasons": [],
            "production_backend": "postgresql",
            "production_postgresql_major": 18,
            "connects_postgresql": False,
            "database_import_attempted": False,
            "database_written": False,
            "database_import_allowed": False,
            "real_import_execution_allowed": False,
            "execution_supported": False,
            "formal_release_allowed": False,
            "planned_devices": devices,
            "source_bindings": [
                {"role": role, "path": str(path), "sha256": _sha256(path)}
                for role, path in bound_paths.items()
            ],
        },
    )
    staging = _write(
        tmp_path / "staging_import.json",
        {
            "schema": "v1_5_formal_database_import_staging_executor_v1",
            "overall_status": "staging_import_committed",
            "blocker_count": 0,
            "transaction_committed": True,
            "idempotent": False,
            "staging_database_written": True,
            "postgresql_server_version_num": 180003,
            "staging_core_schema": "v1_5_core_staging_production_test",
            "staging_evidence_schema": "v1_5_evidence_staging_production_test",
            "production_database_written": False,
            "database_written": False,
            "database_import_allowed": False,
            "real_import_execution_allowed": False,
            "formal_release_allowed": False,
            "not_real_acceptance_evidence": True,
            "execution_attempted": True,
            "connects_postgresql": True,
            "evidence_source": "postgresql18_staging_transaction",
            "authorization_record": {
                "authorization_id": "staging-auth-001",
                "operator": "staging-operator",
                "reviewer": "staging-reviewer",
                "approver": "staging-approver",
                "reviewer_approver_distinct": True,
                "confirmation_matched": True,
            },
            "run_id": run_id,
            "run_db_id": run_db_id,
            "table_counts": {name: len(rows) for name, rows in tables.items()},
            "identity_readback": [
                {
                    **row,
                    "sensor_found": True,
                    "stored_sn_code": row["sn_code"],
                    "stored_device_code": row["device_code"],
                    "protocol_alias_count": 1,
                }
                for row in devices
            ],
            "source_bindings": [
                {
                    "role": "formal_database_import_transaction_plan",
                    "path": str(plan),
                    "sha256": _sha256(plan),
                },
                {
                    "role": "evidence_bundle",
                    "path": str(bundle),
                    "sha256": _sha256(bundle),
                },
            ],
        },
    )
    promotion_model = build_v1_5_formal_database_import_production_promotion_preflight(
        staging_import_json=staging,
        transaction_plan_json=plan,
        evidence_bundle_json=bundle,
    )
    promotion = _write(tmp_path / "promotion_preflight.json", promotion_model)
    migration_checksums = {row.version: row.checksum for row in load_migrations()}
    migration = _write(
        tmp_path / "migration_execution.json",
        {
            "schema": "v1_5_formal_database_migration_production_controlled_executor_v1",
            "overall_status": "production_migration_002_committed",
            "execution_attempted": True,
            "connects_postgresql": True,
            "applies_migrations": True,
            "transaction_started": True,
            "transaction_committed": True,
            "commit_uncertain": False,
            "idempotent": False,
            "migration_execution_confirmed": True,
            "database_written": True,
            "database_write_state": "committed",
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
            "execution_authorization_sha256": "e" * 64,
            "production_target": {
                "backend": "postgresql",
                "postgresql_major": 18,
                "database_name": "gas_calibrator",
                "core_schema": "public",
                "evidence_schema": "v1_5_evidence",
                "dsn_env": "V1_5_POSTGRES_DSN",
            },
            "postcheck_state": {
                "database_name": "gas_calibrator",
                "postgresql_server_version_num": 180003,
                "migration_001_checksum": migration_checksums[
                    "001_v1_5_evidence_registry"
                ],
                "migration_002_checksum": migration_checksums[
                    "002_v1_5_production_import_ledger"
                ],
                "ledger_table_present": True,
                "ledger_columns": list(LEDGER_COLUMNS),
                "ledger_constraints": sorted(EXPECTED_CONSTRAINTS),
                "ledger_indexes": [LEDGER_INDEX],
            },
            "source_bindings": [
                {"role": "dba_readiness", "path": "dba.json", "sha256": "a" * 64},
                {"role": "precheck_sql", "path": "pre.sql", "sha256": "b" * 64},
                {"role": "apply_sql", "path": "apply.sql", "sha256": "c" * 64},
                {"role": "postcheck_sql", "path": "post.sql", "sha256": "d" * 64},
            ],
            "authorization_record": {
                "authorization_id": "migration-auth-001",
                "operator": "migration-operator",
                "reviewer": "migration-reviewer",
                "approver": "migration-approver",
                "confirmation_matched": True,
                "authorization_sha256": "e" * 64,
                "source_sha256": {
                    "dba_readiness": "a" * 64,
                    "precheck_sql": "b" * 64,
                    "apply_sql": "c" * 64,
                    "postcheck_sql": "d" * 64,
                },
            },
        },
    )
    return {
        "promotion": promotion,
        "plan": plan,
        "bundle": bundle,
        "staging": staging,
        "migration": migration,
    }


def _authorization(paths: dict[str, Path], *, now: datetime | None = None) -> dict:
    current = now or datetime.now(timezone.utc)
    return {
        "schema": AUTHORIZATION_SCHEMA,
        "authorization_id": "production-execution-auth-001",
        "issued_at": (current - timedelta(minutes=1)).isoformat(),
        "expires_at": (current + timedelta(hours=1)).isoformat(),
        "operator": "operator-a",
        "reviewer": "reviewer-b",
        "approver": "approver-c",
        "requested_flag": EXECUTE_FLAG,
        "confirmation_template": CONFIRMATION_TEMPLATE,
        "operator_confirmed": True,
        "production_target": {
            "backend": "postgresql",
            "postgresql_major": 18,
            "dsn_env": "V1_5_POSTGRES_DSN",
            "database_name": "gas_calibrator",
            "core_schema": "public",
            "evidence_schema": "v1_5_evidence",
        },
        "boundaries": {
            "production_database_import": True,
            "applies_migrations": False,
            "opens_com_ports": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "grants_formal_release": False,
        },
        "source_bindings": [
            {"role": role, "path": str(paths[key]), "sha256": _sha256(paths[key])}
            for role, key in (
                ("promotion_preflight", "promotion"),
                ("transaction_plan", "plan"),
                ("evidence_bundle", "bundle"),
                ("migration_execution", "migration"),
            )
        ],
    }


@pytest.mark.parametrize("count", [1, 6])
def test_preview_revalidates_promotion_and_remains_no_connect(
    tmp_path: Path, count: int
) -> None:
    paths = _make_package(tmp_path, count=count)
    model = build_production_import_preview(
        promotion_preflight_json=paths["promotion"],
        transaction_plan_json=paths["plan"],
        evidence_bundle_json=paths["bundle"],
        migration_execution_json=paths["migration"],
    )

    assert model["production_import_package_ready"] is True
    assert model["migration_execution_confirmed"] is True
    assert model["planned_device_count"] == count
    assert model["dsn_value_read"] is False
    assert model["connects_postgresql"] is False
    assert model["production_database_written"] is False
    assert model["production_import_execution_allowed"] is False
    assert model["formal_release_allowed"] is False
    assert model["applies_migrations"] is False


@pytest.mark.parametrize(
    ("mutation", "expected_reason"),
    [
        (
            lambda payload: payload.update(migration_execution_confirmed=False),
            "migration_execution_confirmed_not_true",
        ),
        (
            lambda payload: payload.update(commit_uncertain=True),
            "migration_execution_boundary_commit_uncertain_invalid",
        ),
        (
            lambda payload: payload["postcheck_state"].update(
                migration_002_checksum="0" * 64
            ),
            "migration_execution_postcheck_migration_002_invalid",
        ),
        (
            lambda payload: payload["postcheck_state"].update(ledger_columns=[]),
            "migration_execution_postcheck_columns_invalid",
        ),
        (
            lambda payload: payload["authorization_record"]["source_sha256"].update(
                apply_sql="0" * 64
            ),
            "migration_execution_authorization_sources_mismatch",
        ),
    ],
)
def test_preview_blocks_unconfirmed_or_malformed_migration_evidence(
    tmp_path: Path, mutation, expected_reason: str
) -> None:
    paths = _make_package(tmp_path)
    payload = json.loads(paths["migration"].read_text(encoding="utf-8"))
    mutation(payload)
    _write(paths["migration"], payload)

    model = build_production_import_preview(
        promotion_preflight_json=paths["promotion"],
        transaction_plan_json=paths["plan"],
        evidence_bundle_json=paths["bundle"],
        migration_execution_json=paths["migration"],
    )

    assert model["production_import_package_ready"] is False
    assert model["migration_execution_confirmed"] is False
    assert expected_reason in model["reasons"]


def test_preview_accepts_confirmed_idempotent_migration_execution(
    tmp_path: Path,
) -> None:
    paths = _make_package(tmp_path)
    payload = json.loads(paths["migration"].read_text(encoding="utf-8"))
    payload.update(
        overall_status="production_migration_002_idempotent_noop",
        idempotent=True,
        database_written=False,
        database_write_state="idempotent_noop",
    )
    _write(paths["migration"], payload)

    model = build_production_import_preview(
        promotion_preflight_json=paths["promotion"],
        transaction_plan_json=paths["plan"],
        evidence_bundle_json=paths["bundle"],
        migration_execution_json=paths["migration"],
    )

    assert model["production_import_package_ready"] is True
    assert model["migration_execution_confirmed"] is True


def test_preview_blocks_plan_drift_after_promotion(tmp_path: Path) -> None:
    paths = _make_package(tmp_path)
    plan = json.loads(paths["plan"].read_text(encoding="utf-8"))
    plan["planned_devices"][0]["port"] = "COM99"
    _write(paths["plan"], plan)

    model = build_production_import_preview(
        promotion_preflight_json=paths["promotion"],
        transaction_plan_json=paths["plan"],
        evidence_bundle_json=paths["bundle"],
        migration_execution_json=paths["migration"],
    )

    assert model["production_import_package_ready"] is False
    assert "promotion_transaction_plan_sha256_mismatch" in model["reasons"]


def test_execution_authorization_binds_exact_files_and_three_distinct_actors(
    tmp_path: Path,
) -> None:
    paths = _make_package(tmp_path)
    preview = build_production_import_preview(
        promotion_preflight_json=paths["promotion"],
        transaction_plan_json=paths["plan"],
        evidence_bundle_json=paths["bundle"],
        migration_execution_json=paths["migration"],
    )
    authorization = _write(tmp_path / "execution_authorization.json", _authorization(paths))

    record = validate_execution_authorization(
        execution_authorization_json=authorization,
        preview=preview,
        promotion_preflight_json=paths["promotion"],
        transaction_plan_json=paths["plan"],
        evidence_bundle_json=paths["bundle"],
        migration_execution_json=paths["migration"],
    )

    assert record["authorization_id"] == "production-execution-auth-001"
    assert record["three_distinct_actors"] is True
    assert "migration_execution" in record["source_sha256"]

    payload = json.loads(authorization.read_text(encoding="utf-8"))
    payload["approver"] = payload["reviewer"]
    _write(authorization, payload)
    with pytest.raises(ProductionImportError, match="three_distinct_actors"):
        validate_execution_authorization(
            execution_authorization_json=authorization,
            preview=preview,
            promotion_preflight_json=paths["promotion"],
            transaction_plan_json=paths["plan"],
            evidence_bundle_json=paths["bundle"],
            migration_execution_json=paths["migration"],
        )


def test_expired_or_rebound_authorization_is_blocked(tmp_path: Path) -> None:
    current = datetime.now(timezone.utc)
    paths = _make_package(tmp_path)
    preview = build_production_import_preview(
        promotion_preflight_json=paths["promotion"],
        transaction_plan_json=paths["plan"],
        evidence_bundle_json=paths["bundle"],
        migration_execution_json=paths["migration"],
    )
    payload = _authorization(paths, now=current - timedelta(days=2))
    authorization = _write(tmp_path / "expired.json", payload)
    with pytest.raises(ProductionImportError, match="not_current"):
        validate_execution_authorization(
            execution_authorization_json=authorization,
            preview=preview,
            promotion_preflight_json=paths["promotion"],
            transaction_plan_json=paths["plan"],
            evidence_bundle_json=paths["bundle"],
            migration_execution_json=paths["migration"],
            now=current,
        )

    payload = _authorization(paths, now=current)
    payload["source_bindings"][0]["path"] = str(tmp_path / "other.json")
    authorization = _write(tmp_path / "rebound.json", payload)
    with pytest.raises(ProductionImportError, match="source_path_mismatch"):
        validate_execution_authorization(
            execution_authorization_json=authorization,
            preview=preview,
            promotion_preflight_json=paths["promotion"],
            transaction_plan_json=paths["plan"],
            evidence_bundle_json=paths["bundle"],
            migration_execution_json=paths["migration"],
            now=current,
        )


def test_reviewed_execution_passes_exact_hashes_to_atomic_kernel(tmp_path: Path) -> None:
    paths = _make_package(tmp_path)
    authorization = _write(tmp_path / "execution_authorization.json", _authorization(paths))
    captured: dict = {}

    def fake_runner(**kwargs):
        captured.update(kwargs)
        return {
            "status": "production_import_committed",
            "transaction_committed": True,
            "idempotent": False,
            "production_database_written": True,
            "identity_readback": [],
            "table_counts": {},
        }

    model = execute_reviewed_production_import(
        promotion_preflight_json=paths["promotion"],
        transaction_plan_json=paths["plan"],
        evidence_bundle_json=paths["bundle"],
        migration_execution_json=paths["migration"],
        execution_authorization_json=authorization,
        dsn="postgresql://ignored-for-injected-runner/gas_calibrator",
        transaction_runner=fake_runner,
    )

    assert captured["promotion_preflight_sha256"] == _sha256(paths["promotion"])
    assert captured["transaction_plan_sha256"] == _sha256(paths["plan"])
    assert captured["evidence_bundle_sha256"] == _sha256(paths["bundle"])
    assert captured["operator"] == "operator-a"
    assert model["overall_status"] == "production_import_committed"
    assert model["production_import_execution_authorized"] is True
    assert model["production_import_execution_allowed"] is True
    assert model["database_written"] is True
    assert model["database_import_allowed"] is True
    assert model["formal_release_allowed"] is False


def test_failed_transaction_keeps_import_and_release_closed(tmp_path: Path) -> None:
    paths = _make_package(tmp_path)
    authorization = _write(tmp_path / "execution_authorization.json", _authorization(paths))

    def rolled_back_runner(**_kwargs):
        return {
            "status": "production_import_rolled_back",
            "transaction_committed": False,
            "production_database_written": False,
            "failure_reason": "injected_failure",
            "rollback_attempted": True,
            "rollback_confirmed": True,
        }

    model = execute_reviewed_production_import(
        promotion_preflight_json=paths["promotion"],
        transaction_plan_json=paths["plan"],
        evidence_bundle_json=paths["bundle"],
        migration_execution_json=paths["migration"],
        execution_authorization_json=authorization,
        dsn="postgresql://ignored-for-injected-runner/gas_calibrator",
        transaction_runner=rolled_back_runner,
    )

    assert model["overall_status"] == "production_import_rolled_back"
    assert model["production_import_execution_authorized"] is True
    assert model["production_import_execution_allowed"] is False
    assert model["database_written"] is False
    assert model["database_import_allowed"] is False
    assert model["formal_release_allowed"] is False


def test_source_change_after_authorization_is_blocked_before_transaction(
    tmp_path: Path, monkeypatch
) -> None:
    paths = _make_package(tmp_path)
    authorization = _write(tmp_path / "execution_authorization.json", _authorization(paths))
    original_validate = validate_execution_authorization
    runner_called = False

    def validate_then_mutate(**kwargs):
        record = original_validate(**kwargs)
        plan = json.loads(paths["plan"].read_text(encoding="utf-8"))
        plan["planned_devices"][0]["port"] = "COM99"
        _write(paths["plan"], plan)
        return record

    def forbidden_runner(**_kwargs):
        nonlocal runner_called
        runner_called = True
        raise AssertionError("transaction runner must stay closed")

    module = __import__(
        "gas_calibrator.validation.v1_5_formal_database_import_production_controlled_executor",
        fromlist=["validate_execution_authorization"],
    )
    monkeypatch.setattr(module, "validate_execution_authorization", validate_then_mutate)

    with pytest.raises(ProductionImportError, match="source_changed_after_validation"):
        execute_reviewed_production_import(
            promotion_preflight_json=paths["promotion"],
            transaction_plan_json=paths["plan"],
            evidence_bundle_json=paths["bundle"],
            migration_execution_json=paths["migration"],
            execution_authorization_json=authorization,
            dsn="postgresql://must-not-connect/gas_calibrator",
            transaction_runner=forbidden_runner,
        )
    assert runner_called is False


def test_migration_evidence_change_after_authorization_is_blocked(
    tmp_path: Path, monkeypatch
) -> None:
    paths = _make_package(tmp_path)
    authorization = _write(tmp_path / "execution_authorization.json", _authorization(paths))
    original_validate = validate_execution_authorization
    runner_called = False

    def validate_then_mutate(**kwargs):
        record = original_validate(**kwargs)
        migration = json.loads(paths["migration"].read_text(encoding="utf-8"))
        migration["postcheck_state"]["ledger_columns"] = []
        _write(paths["migration"], migration)
        return record

    def forbidden_runner(**_kwargs):
        nonlocal runner_called
        runner_called = True
        raise AssertionError("transaction runner must stay closed")

    module = __import__(
        "gas_calibrator.validation.v1_5_formal_database_import_production_controlled_executor",
        fromlist=["validate_execution_authorization"],
    )
    monkeypatch.setattr(module, "validate_execution_authorization", validate_then_mutate)

    with pytest.raises(
        ProductionImportError,
        match="source_changed_after_validation:migration_execution",
    ):
        execute_reviewed_production_import(
            promotion_preflight_json=paths["promotion"],
            transaction_plan_json=paths["plan"],
            evidence_bundle_json=paths["bundle"],
            migration_execution_json=paths["migration"],
            execution_authorization_json=authorization,
            dsn="postgresql://must-not-connect/gas_calibrator",
            transaction_runner=forbidden_runner,
        )
    assert runner_called is False


def test_commit_uncertain_keeps_database_write_state_unknown(tmp_path: Path) -> None:
    paths = _make_package(tmp_path)
    authorization = _write(tmp_path / "execution_authorization.json", _authorization(paths))

    def uncertain_runner(**_kwargs):
        return {
            "status": "production_import_commit_uncertain_hold",
            "transaction_committed": False,
            "production_database_written": None,
            "production_database_write_state": "unknown_commit_uncertain",
            "failure_reason": "connection_lost_during_commit",
            "commit_attempted": True,
            "commit_uncertain": True,
        }

    model = execute_reviewed_production_import(
        promotion_preflight_json=paths["promotion"],
        transaction_plan_json=paths["plan"],
        evidence_bundle_json=paths["bundle"],
        migration_execution_json=paths["migration"],
        execution_authorization_json=authorization,
        dsn="postgresql://ignored-for-injected-runner/gas_calibrator",
        transaction_runner=uncertain_runner,
    )

    assert model["overall_status"] == "production_import_commit_uncertain_hold"
    assert model["production_database_written"] is None
    assert model["database_written"] is None
    assert model["database_import_allowed"] is False
    assert model["formal_release_allowed"] is False


def test_cli_preview_and_invalid_authorization_never_read_dsn(
    tmp_path: Path, monkeypatch
) -> None:
    paths = _make_package(tmp_path)

    def forbidden_read():
        raise AssertionError("DSN must not be read")

    monkeypatch.setattr(cli, "_read_production_dsn", forbidden_read)
    assert (
        cli.main(
            [
                "--promotion-preflight-json",
                str(paths["promotion"]),
                "--transaction-plan-json",
                str(paths["plan"]),
                "--evidence-bundle-json",
                str(paths["bundle"]),
                "--migration-execution-json",
                str(paths["migration"]),
                "--output-dir",
                str(tmp_path / "preview"),
            ]
        )
        == 0
    )

    invalid_authorization = _write(
        tmp_path / "invalid_authorization.json", {"schema": "wrong"}
    )
    assert (
        cli.main(
            [
                "--promotion-preflight-json",
                str(paths["promotion"]),
                "--transaction-plan-json",
                str(paths["plan"]),
                "--evidence-bundle-json",
                str(paths["bundle"]),
                "--migration-execution-json",
                str(paths["migration"]),
                "--execution-authorization-json",
                str(invalid_authorization),
                "--output-dir",
                str(tmp_path / "blocked"),
                "--execute-production-import",
            ]
        )
        == 2
    )
    blocked = json.loads(
        (
            tmp_path
            / "blocked/v1_5_formal_database_import_production_controlled_executor.json"
        ).read_text(encoding="utf-8-sig")
    )
    assert blocked["dsn_value_read"] is False
    assert blocked["connects_postgresql"] is False


def test_cli_unconfirmed_migration_never_reads_dsn(
    tmp_path: Path, monkeypatch
) -> None:
    paths = _make_package(tmp_path)
    migration = json.loads(paths["migration"].read_text(encoding="utf-8"))
    migration["migration_execution_confirmed"] = False
    _write(paths["migration"], migration)
    authorization = _write(
        tmp_path / "authorization.json", _authorization(paths)
    )

    def forbidden_read():
        raise AssertionError("DSN must not be read")

    monkeypatch.setattr(cli, "_read_production_dsn", forbidden_read)
    result = cli.main(
        [
            "--promotion-preflight-json",
            str(paths["promotion"]),
            "--transaction-plan-json",
            str(paths["plan"]),
            "--evidence-bundle-json",
            str(paths["bundle"]),
            "--migration-execution-json",
            str(paths["migration"]),
            "--execution-authorization-json",
            str(authorization),
            "--output-dir",
            str(tmp_path / "migration-blocked"),
            "--execute-production-import",
        ]
    )
    assert result == 2
    blocked = json.loads(
        (
            tmp_path
            / "migration-blocked/v1_5_formal_database_import_production_controlled_executor.json"
        ).read_text(encoding="utf-8-sig")
    )
    assert blocked["migration_execution_confirmed"] is False
    assert blocked["dsn_value_read"] is False
    assert blocked["connects_postgresql"] is False


def test_cli_rejects_target_overrides_before_inputs_are_loaded(tmp_path: Path) -> None:
    assert (
        cli.main(
            [
                "--promotion-preflight-json",
                str(tmp_path / "missing-promotion.json"),
                "--transaction-plan-json",
                str(tmp_path / "missing-plan.json"),
                "--evidence-bundle-json",
                str(tmp_path / "missing-bundle.json"),
                "--migration-execution-json",
                str(tmp_path / "missing-migration.json"),
                "--output-dir",
                str(tmp_path / "output"),
                "--database-name",
                "other_database",
            ]
        )
        == 2
    )


def test_production_dsn_and_ledger_migration_are_fixed() -> None:
    assert PRODUCTION_DATABASE_NAME == "gas_calibrator"
    assert (
        validate_production_dsn("postgresql://user:secret@localhost/gas_calibrator")
        == "postgresql+psycopg://user:secret@localhost/gas_calibrator"
    )
    with pytest.raises(ProductionImportError, match="database_name"):
        validate_production_dsn("postgresql://user:secret@localhost/staging_db")
    migrations = {row.version: row.sql for row in load_migrations()}
    assert "002_v1_5_production_import_ledger" in migrations
    assert "production_import_ledger" in migrations["002_v1_5_production_import_ledger"]
    assert "execution_authorization_sha256" in migrations[
        "002_v1_5_production_import_ledger"
    ]


def test_production_executor_has_no_schema_creation_or_migration_path() -> None:
    source = (
        Path("src/gas_calibrator/storage/v1_5_evidence/production_import.py")
        .read_text(encoding="utf-8")
    )
    assert "CreateSchema" not in source
    assert "create_all(" not in source
    assert "_create_staging_schemas" not in source
    assert "apply_migrations(" not in source
    assert "CREATE SCHEMA" not in source


def test_production_executor_entrypoint_is_manual_database_only(tmp_path: Path) -> None:
    path = (
        tmp_path
        / "src/gas_calibrator/tools/run_v1_5_formal_database_import_production_controlled_executor.py"
    )
    path.parent.mkdir(parents=True)
    path.write_text("", encoding="utf-8")
    entry = classify_v1_5_entrypoint(path, root=tmp_path)

    assert entry.category == "evidence_database"
    assert entry.formal_status == "manual_authorized_production_database_only"
    assert entry.risk_level == "production_database_write_risk"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
