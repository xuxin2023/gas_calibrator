from __future__ import annotations

import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text

from gas_calibrator.v2.storage.database import stable_uuid
from gas_calibrator.storage.v1_5_evidence.bundle import TABLE_NAMES
from gas_calibrator.storage.v1_5_evidence.staging_import import (
    StagingImportError,
    execute_staging_import,
    query_staging_identity,
    validate_staging_package,
    validate_staging_schemas,
)
from gas_calibrator.tools.query_v1_5_formal_database_import_staging import main as query_cli_main
from gas_calibrator.tools.run_v1_5_formal_database_import_staging_executor import main as cli_main
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_formal_database_import_staging_executor import (
    CONFIRMATION_TEXT,
    build_staging_import_preview,
    execute_reviewed_staging_import,
)


def _planned_devices(count: int = 2) -> list[dict[str, str]]:
    return [
        {
            "slot": f"GA{index:02d}",
            "sn_code": f"012607{index:02d}",
            "device_code": f"012607{index:02d}",
            "protocol_device_id": f"{index:03d}",
            "port": f"COM{34 + index}",
        }
        for index in range(1, count + 1)
    ]


def _transaction_plan(count: int = 2) -> dict:
    return {
        "schema": "v1_5_formal_database_import_transaction_plan_v1",
        "transaction_plan_contract_ready": True,
        "production_backend": "postgresql",
        "production_postgresql_major": 18,
        "connects_postgresql": False,
        "database_import_attempted": False,
        "database_written": False,
        "database_import_allowed": False,
        "real_import_execution_allowed": False,
        "execution_supported": False,
        "formal_release_allowed": False,
        "planned_devices": _planned_devices(count),
    }


def _bundle(*, run_id: str = "v1_5_staging_test_run", count: int = 2) -> dict:
    run_db_id = f"db_{run_id}"
    devices = []
    run_devices = []
    for row in _planned_devices(count):
        device_id = f"dut_{row['protocol_device_id']}"
        devices.append(
            {
                "id": device_id,
                "device_type": "gas_analyzer",
                "device_role": "device_under_test",
                "display_name": row["protocol_device_id"],
                "serial_number": row["protocol_device_id"],
                "metadata": {
                    "sn_code": row["sn_code"],
                    "device_code": row["device_code"],
                    "protocol_device_id": row["protocol_device_id"],
                    "analyzer_prefix": row["slot"].lower(),
                },
            }
        )
        run_devices.append(
            {
                "id": f"run_{device_id}",
                "run_db_id": run_db_id,
                "device_id": device_id,
                "role": "device_under_test",
                "metadata": {"slot": row["slot"]},
            }
        )
    tables = {name: [] for name in TABLE_NAMES}
    tables["runs"] = [
        {
            "id": run_db_id,
            "run_id": run_id,
            "run_dir": f"D:/staging/{run_id}",
            "plan_id": "legacy_ratio_production",
            "plan_version": "1.5",
            "analyzer_id": "all",
            "operator_name": "operator-a",
            "config_hash": "config-hash",
            "package_status": "review_required",
            "package_blockers": ["not_real_acceptance_evidence"],
            "evidence_status": "staging_only",
            "metadata": {"not_real_acceptance_evidence": True},
        }
    ]
    tables["devices"] = devices
    tables["run_devices"] = run_devices
    tables["audit_events"] = [
        {
            "id": f"audit_{run_id}",
            "run_db_id": run_db_id,
            "event_type": "staging_test_import",
            "actor": "pytest",
            "event_at": "2026-07-13T00:00:00+00:00",
            "payload": {"not_real_acceptance_evidence": True},
        }
    ]
    return {
        "schema": "v1_5_evidence_registry",
        "schema_version": "001",
        "run_id": run_id,
        "run_db_id": run_db_id,
        "tables": tables,
    }


def _write(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def test_staging_preview_is_no_connect_and_keeps_production_locked(tmp_path: Path) -> None:
    plan = _write(tmp_path / "plan.json", _transaction_plan())
    bundle = _write(tmp_path / "bundle.json", _bundle())
    model = build_staging_import_preview(
        transaction_plan_json=plan,
        evidence_bundle_json=bundle,
    )
    assert model["overall_status"] == "ready_for_postgresql18_staging_import_review"
    assert model["export_status"] == "ok"
    assert model["planned_device_count"] == 2
    assert model["dsn_value_read"] is False
    assert model["connects_postgresql"] is False
    assert model["staging_database_written"] is False
    assert model["production_database_written"] is False
    assert model["database_import_allowed"] is False
    assert model["formal_release_allowed"] is False
    assert model["opens_com_ports"] is False
    assert set(model["artifact_roles"]) == {
        "execution_rows",
        "execution_summary",
        "diagnostic_analysis",
        "formal_analysis",
    }


@pytest.mark.parametrize("count", [1, 6])
def test_staging_package_supports_one_to_six_devices(count: int) -> None:
    rows = validate_staging_package(_transaction_plan(count), _bundle(count=count))
    assert len(rows) == count


def test_staging_package_rejects_duplicate_sn() -> None:
    plan = _transaction_plan()
    plan["planned_devices"][1]["sn_code"] = plan["planned_devices"][0]["sn_code"]
    plan["planned_devices"][1]["device_code"] = plan["planned_devices"][0]["device_code"]
    with pytest.raises(StagingImportError, match="duplicate_sn_code"):
        validate_staging_package(plan, _bundle())


def test_staging_package_rejects_more_than_six_devices() -> None:
    with pytest.raises(StagingImportError, match="planned_device_count_must_be_1_to_6"):
        validate_staging_package(_transaction_plan(7), _bundle(count=7))


def test_staging_package_rejects_protocol_mismatch() -> None:
    bundle = _bundle()
    bundle["tables"]["devices"][1]["serial_number"] = "099"
    bundle["tables"]["devices"][1]["metadata"]["protocol_device_id"] = "099"
    with pytest.raises(StagingImportError, match="planned_protocol_ids_do_not_match"):
        validate_staging_package(_transaction_plan(), bundle)


def test_staging_schema_rejects_production_names() -> None:
    with pytest.raises(StagingImportError, match="prefix_required|production_schema_forbidden"):
        validate_staging_schemas("public", "v1_5_evidence")


def test_authorization_failure_never_attempts_connection(tmp_path: Path) -> None:
    plan = _write(tmp_path / "plan.json", _transaction_plan())
    bundle = _write(tmp_path / "bundle.json", _bundle())
    preview = build_staging_import_preview(
        transaction_plan_json=plan,
        evidence_bundle_json=bundle,
    )
    model = execute_reviewed_staging_import(
        preview=preview,
        transaction_plan_json=plan,
        evidence_bundle_json=bundle,
        dsn="postgresql://invalid/also_invalid",
        authorization_id="auth-1",
        operator="operator",
        reviewer="same",
        approver="same",
        operator_confirmation_text="wrong",
        initialize_staging_schemas=False,
    )
    assert model["overall_status"] == "staging_import_authorization_blocked"
    assert model["execution_attempted"] is False
    assert model["connects_postgresql"] is False


def test_cli_preview_writes_artifacts_without_dsn(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("V1_5_POSTGRES_STAGING_DSN", raising=False)
    plan = _write(tmp_path / "plan.json", _transaction_plan())
    bundle = _write(tmp_path / "bundle.json", _bundle())
    output = tmp_path / "output"
    assert (
        cli_main(
            [
                "--transaction-plan-json",
                str(plan),
                "--evidence-bundle-json",
                str(bundle),
                "--output-dir",
                str(output),
            ]
        )
        == 0
    )
    payload = json.loads(
        (output / "v1_5_formal_database_import_staging_executor.json").read_text(
            encoding="utf-8-sig"
        )
    )
    assert payload["connects_postgresql"] is False
    assert payload["production_database_written"] is False


def test_cli_rejects_production_unlock_options(tmp_path: Path) -> None:
    assert (
        cli_main(
            [
                "--transaction-plan-json",
                str(tmp_path / "missing-plan.json"),
                "--evidence-bundle-json",
                str(tmp_path / "missing-bundle.json"),
                "--output-dir",
                str(tmp_path / "output"),
                "--allow-production-import",
            ]
        )
        == 2
    )


def test_staging_executor_entrypoint_is_not_com_or_production_route(tmp_path: Path) -> None:
    path = (
        tmp_path
        / "src/gas_calibrator/tools/run_v1_5_formal_database_import_staging_executor.py"
    )
    path.parent.mkdir(parents=True)
    path.write_text("", encoding="utf-8")
    entry = classify_v1_5_entrypoint(path, root=tmp_path)
    assert entry.category == "evidence_database"
    assert entry.formal_status == "manual_authorized_staging_database_only"
    assert entry.risk_level == "staging_database_write_risk"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
    assert "staging-only transaction executor" in entry.notes[0]

    query_path = (
        tmp_path
        / "src/gas_calibrator/tools/query_v1_5_formal_database_import_staging.py"
    )
    query_path.write_text("", encoding="utf-8")
    query_entry = classify_v1_5_entrypoint(query_path, root=tmp_path)
    assert query_entry.category == "evidence_database"
    assert query_entry.formal_status == "explicit_staging_database_query_only"
    assert query_entry.risk_level == "staging_database_read_risk"
    assert query_entry.opens_com_ports is False


def test_staging_query_cli_is_locked_by_default(tmp_path: Path) -> None:
    assert (
        query_cli_main(
            [
                "--query-kind",
                "sn_code",
                "--query-value",
                "01260701",
                "--output-json",
                str(tmp_path / "query.json"),
            ]
        )
        == 2
    )
    assert not (tmp_path / "query.json").exists()


@pytest.mark.postgresql18_staging
def test_postgresql18_staging_atomic_idempotent_and_queryable() -> None:
    dsn = os.environ.get("V1_5_POSTGRES_STAGING_DSN_TEST", "")
    if not dsn:
        pytest.skip("V1_5_POSTGRES_STAGING_DSN_TEST is not configured")
    suffix = uuid4().hex[:8]
    core_schema = f"v1_5_core_staging_{suffix}"
    evidence_schema = f"v1_5_evidence_staging_{suffix}"
    engine_dsn = dsn.replace("postgresql://", "postgresql+psycopg://", 1)
    engine = create_engine(engine_dsn, future=True)
    plan = _transaction_plan()
    bundle = _bundle(run_id=f"atomic_{suffix}")
    try:
        first = execute_staging_import(
            dsn=dsn,
            transaction_plan=plan,
            evidence_bundle=bundle,
            transaction_plan_sha256="a" * 64,
            evidence_bundle_sha256="b" * 64,
            core_schema=core_schema,
            evidence_schema=evidence_schema,
            authorization_id=f"auth-{suffix}",
            operator="operator-a",
            reviewer="reviewer-a",
            approver="approver-b",
            initialize_schemas=True,
        )
        assert first["status"] == "staging_import_committed", first
        assert first["staging_database_written"] is True
        assert first["table_counts"]["runs"] == 1
        assert len(first["identity_readback"]) == 2

        second = execute_staging_import(
            dsn=dsn,
            transaction_plan=plan,
            evidence_bundle=bundle,
            transaction_plan_sha256="a" * 64,
            evidence_bundle_sha256="b" * 64,
            core_schema=core_schema,
            evidence_schema=evidence_schema,
            authorization_id=f"auth-{suffix}",
            operator="operator-a",
            reviewer="reviewer-a",
            approver="approver-b",
            initialize_schemas=False,
        )
        assert second["status"] == "staging_import_idempotent_noop", second
        assert second["staging_database_written"] is False

        by_sn = query_staging_identity(
            dsn=dsn,
            core_schema=core_schema,
            evidence_schema=evidence_schema,
            query_kind="sn_code",
            query_value="01260701",
        )
        by_protocol = query_staging_identity(
            dsn=dsn,
            core_schema=core_schema,
            evidence_schema=evidence_schema,
            query_kind="protocol_device_id",
            query_value="001",
        )
        by_run = query_staging_identity(
            dsn=dsn,
            core_schema=core_schema,
            evidence_schema=evidence_schema,
            query_kind="run_id",
            query_value=bundle["run_id"],
        )
        assert len(by_sn["rows"]) == 1
        assert len(by_protocol["rows"]) == 1
        assert len(by_run["rows"]) == 1

        changed = json.loads(json.dumps(bundle))
        changed["tables"]["runs"][0]["operator_name"] = "changed"
        conflict = execute_staging_import(
            dsn=dsn,
            transaction_plan=plan,
            evidence_bundle=changed,
            transaction_plan_sha256="a" * 64,
            evidence_bundle_sha256="c" * 64,
            core_schema=core_schema,
            evidence_schema=evidence_schema,
            authorization_id=f"auth-{suffix}",
            operator="operator-a",
            reviewer="reviewer-a",
            approver="approver-b",
            initialize_schemas=False,
        )
        assert conflict["status"] == "staging_import_rolled_back", conflict
        assert "idempotency_conflict" in conflict["failure_reason"]

        rollback_bundle = _bundle(run_id=f"rollback_{suffix}")

        def fail_after_core(stage: str) -> None:
            if stage == "after_runs":
                raise RuntimeError("injected_failure")

        rollback = execute_staging_import(
            dsn=dsn,
            transaction_plan=plan,
            evidence_bundle=rollback_bundle,
            transaction_plan_sha256="d" * 64,
            evidence_bundle_sha256="e" * 64,
            core_schema=core_schema,
            evidence_schema=evidence_schema,
            authorization_id=f"auth-rollback-{suffix}",
            operator="operator-a",
            reviewer="reviewer-a",
            approver="approver-b",
            initialize_schemas=False,
            failure_injector=fail_after_core,
        )
        assert rollback["status"] == "staging_import_rolled_back", rollback
        assert rollback["commit_attempted"] is False
        assert rollback["commit_uncertain"] is False
        assert rollback["rollback_confirmed"] is True
        rollback_run_uuid = str(stable_uuid("run", rollback_bundle["run_id"]))
        with engine.connect() as connection:
            core_run_count = int(
                connection.execute(
                    text(
                        f'SELECT COUNT(*) FROM "{core_schema}".runs '
                        "WHERE id=CAST(:run_uuid AS UUID)"
                    ),
                    {"run_uuid": rollback_run_uuid},
                ).scalar_one()
            )
        assert core_run_count == 0
        rolled_back_query = query_staging_identity(
            dsn=dsn,
            core_schema=core_schema,
            evidence_schema=evidence_schema,
            query_kind="run_id",
            query_value=rollback_bundle["run_id"],
        )
        assert rolled_back_query["rows"] == []

        cli_bundle = _bundle(run_id=f"cli_{suffix}")
        with TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            cli_plan_path = _write(temp / "plan.json", plan)
            cli_bundle_path = _write(temp / "bundle.json", cli_bundle)
            cli_output = temp / "import_output"
            assert (
                cli_main(
                    [
                        "--transaction-plan-json",
                        str(cli_plan_path),
                        "--evidence-bundle-json",
                        str(cli_bundle_path),
                        "--output-dir",
                        str(cli_output),
                        "--core-schema",
                        core_schema,
                        "--evidence-schema",
                        evidence_schema,
                        "--dsn-env",
                        "V1_5_POSTGRES_STAGING_DSN_TEST",
                        "--execute-staging-import",
                        "--authorization-id",
                        f"auth-cli-{suffix}",
                        "--operator",
                        "operator-a",
                        "--reviewer",
                        "reviewer-a",
                        "--approver",
                        "approver-b",
                        "--operator-confirmation-text",
                        CONFIRMATION_TEXT,
                    ]
                )
                == 0
            )
            cli_model = json.loads(
                (cli_output / "v1_5_formal_database_import_staging_executor.json").read_text(
                    encoding="utf-8-sig"
                )
            )
            assert cli_model["overall_status"] == "staging_import_committed"
            query_output = temp / "query.json"
            assert (
                query_cli_main(
                    [
                        "--query-kind",
                        "device_code",
                        "--query-value",
                        "01260702",
                        "--output-json",
                        str(query_output),
                        "--core-schema",
                        core_schema,
                        "--evidence-schema",
                        evidence_schema,
                        "--dsn-env",
                        "V1_5_POSTGRES_STAGING_DSN_TEST",
                        "--execute-staging-query",
                    ]
                )
                == 0
            )
            query_model = json.loads(query_output.read_text(encoding="utf-8-sig"))
            assert query_model["query_only"] is True
            assert query_model["database_written"] is False
            assert query_model["rows"]
    finally:
        with engine.begin() as connection:
            connection.execute(text(f'DROP SCHEMA IF EXISTS "{evidence_schema}" CASCADE'))
            connection.execute(text(f'DROP SCHEMA IF EXISTS "{core_schema}" CASCADE'))
        engine.dispose()
