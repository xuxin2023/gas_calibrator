from __future__ import annotations

import json
from pathlib import Path

import pytest

from gas_calibrator.storage.v1_5_evidence.bundle import TABLE_NAMES
from gas_calibrator.tools.export_v1_5_formal_database_import_production_promotion_preflight import (
    main as promotion_cli_main,
)
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_formal_database_import_production_promotion_preflight import (
    READY_STATUS,
    build_v1_5_formal_database_import_production_promotion_preflight,
    write_v1_5_formal_database_import_production_promotion_preflight_outputs,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path.resolve()


def _sha256(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()


def _make_package(tmp_path: Path, *, count: int = 2, idempotent: bool = False) -> dict[str, Path]:
    run_id = "v1_5_promotion_test_run"
    run_db_id = "11111111-1111-4111-8111-111111111111"
    devices = [
        {
            "slot": f"GA{index:02d}",
            "sn_code": f"012607{index:02d}",
            "device_code": f"012607{index:02d}",
            "protocol_device_id": f"{index:03d}",
            "port": f"COM{34 + index}",
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
    bundle_path = _write_json(
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
            "authorization_id": "production-auth-001",
            "operator": "operator-a",
            "reviewer": "reviewer-a",
            "approver": "approver-b",
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
        role: _write_json(tmp_path / f"{role}.json", payload)
        for role, payload in bound_payloads.items()
    }
    bound_paths["evidence_bundle"] = bundle_path
    source_bindings = [
        {"role": role, "path": str(path), "sha256": _sha256(path)}
        for role, path in bound_paths.items()
    ]
    plan_path = _write_json(
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
            "source_bindings": source_bindings,
        },
    )
    identity_readback = [
        {
            **row,
            "sensor_found": True,
            "stored_sn_code": row["sn_code"],
            "stored_device_code": row["device_code"],
            "protocol_alias_count": 1,
        }
        for row in devices
    ]
    staging_path = _write_json(
        tmp_path / "staging_import.json",
        {
            "schema": "v1_5_formal_database_import_staging_executor_v1",
            "overall_status": (
                "staging_import_idempotent_noop" if idempotent else "staging_import_committed"
            ),
            "blocker_count": 0,
            "transaction_committed": True,
            "idempotent": idempotent,
            "staging_database_written": not idempotent,
            "postgresql_server_version_num": 180003,
            "staging_core_schema": "v1_5_core_staging_promotion",
            "staging_evidence_schema": "v1_5_evidence_staging_promotion",
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
                "operator": "operator-a",
                "reviewer": "reviewer-a",
                "approver": "approver-b",
                "reviewer_approver_distinct": True,
                "confirmation_matched": True,
            },
            "run_id": run_id,
            "run_db_id": run_db_id,
            "table_counts": {name: len(rows) for name, rows in tables.items()},
            "identity_readback": identity_readback,
            "source_bindings": [
                {
                    "role": "formal_database_import_transaction_plan",
                    "path": str(plan_path),
                    "sha256": _sha256(plan_path),
                },
                {
                    "role": "evidence_bundle",
                    "path": str(bundle_path),
                    "sha256": _sha256(bundle_path),
                },
            ],
        },
    )
    return {
        "staging": staging_path,
        "plan": plan_path,
        "bundle": bundle_path,
        "authorization": bound_paths["formal_database_import_authorization"],
    }


def _build(paths: dict[str, Path]) -> dict:
    return build_v1_5_formal_database_import_production_promotion_preflight(
        staging_import_json=paths["staging"],
        transaction_plan_json=paths["plan"],
        evidence_bundle_json=paths["bundle"],
    )


@pytest.mark.parametrize(("count", "idempotent"), [(1, False), (6, False), (6, True)])
def test_promotion_preflight_ready_for_committed_or_idempotent_staging(
    tmp_path: Path, count: int, idempotent: bool
) -> None:
    model = _build(_make_package(tmp_path, count=count, idempotent=idempotent))

    assert model["overall_status"] == READY_STATUS
    assert model["promotion_preflight_ready"] is True
    assert model["production_import_executor_review_allowed"] is True
    assert model["planned_device_count"] == count
    assert all(row["status"] == "ready" for row in model["checks"])
    assert model["production_import_execution_allowed"] is False
    assert model["connects_postgresql"] is False
    assert model["production_database_written"] is False
    assert model["database_import_allowed"] is False
    assert model["not_real_acceptance_evidence"] is True

    outputs = write_v1_5_formal_database_import_production_promotion_preflight_outputs(
        model, tmp_path / "outputs"
    )
    assert all(path.is_file() for path in outputs.values())


@pytest.mark.parametrize(
    ("target", "mutate", "reason"),
    [
        ("staging", lambda row: row.update(transaction_committed=False), "staging_transaction_not_committed"),
        (
            "staging",
            lambda row: row["identity_readback"][0].update(stored_sn_code="99999999"),
            "staging_stored_sn_mismatch",
        ),
        (
            "plan",
            lambda row: row.update(production_transaction_package_ready=False),
            "production_transaction_package_not_ready",
        ),
        (
            "staging",
            lambda row: row.update(postgresql_server_version_num="invalid"),
            "staging_postgresql_server_version_num",
        ),
    ],
)
def test_promotion_preflight_blocks_invalid_state(
    tmp_path: Path, target: str, mutate, reason: str
) -> None:
    paths = _make_package(tmp_path)
    payload = json.loads(paths[target].read_text(encoding="utf-8"))
    mutate(payload)
    _write_json(paths[target], payload)

    model = _build(paths)

    assert model["overall_status"] == "blocked"
    assert model["promotion_preflight_ready"] is False
    assert model["production_import_executor_review_allowed"] is False
    assert any(reason in item for row in model["checks"] for item in row["reasons"])


def test_promotion_preflight_blocks_bound_authorization_drift(tmp_path: Path) -> None:
    paths = _make_package(tmp_path)
    authorization = json.loads(paths["authorization"].read_text(encoding="utf-8"))
    authorization["authorization_id"] = "changed-after-staging"
    _write_json(paths["authorization"], authorization)

    model = _build(paths)

    assert model["overall_status"] == "blocked"
    assert any(
        "transaction_plan_source_sha256_mismatch:formal_database_import_authorization" in reason
        for row in model["checks"]
        for reason in row["reasons"]
    )


def test_promotion_preflight_rechecks_semantics_even_when_hashes_are_rebound(
    tmp_path: Path,
) -> None:
    paths = _make_package(tmp_path)
    authorization = json.loads(paths["authorization"].read_text(encoding="utf-8"))
    authorization["approver"] = authorization["reviewer"]
    _write_json(paths["authorization"], authorization)

    plan = json.loads(paths["plan"].read_text(encoding="utf-8"))
    for binding in plan["source_bindings"]:
        if binding["role"] == "formal_database_import_authorization":
            binding["sha256"] = _sha256(paths["authorization"])
    _write_json(paths["plan"], plan)

    staging = json.loads(paths["staging"].read_text(encoding="utf-8"))
    for binding in staging["source_bindings"]:
        if binding["role"] == "formal_database_import_transaction_plan":
            binding["sha256"] = _sha256(paths["plan"])
    _write_json(paths["staging"], staging)

    model = _build(paths)

    assert model["overall_status"] == "blocked"
    assert any(
        reason == "production_authorization_reviewer_approver_not_distinct"
        for row in model["checks"]
        for reason in row["reasons"]
    )


def test_promotion_preflight_blocks_noncanonical_production_dsn_env(tmp_path: Path) -> None:
    paths = _make_package(tmp_path)
    model = build_v1_5_formal_database_import_production_promotion_preflight(
        staging_import_json=paths["staging"],
        transaction_plan_json=paths["plan"],
        evidence_bundle_json=paths["bundle"],
        production_dsn_env="V1_5_POSTGRES_STAGING_DSN",
    )

    assert model["overall_status"] == "blocked"
    assert model["dsn_value_read"] is False
    assert model["connects_postgresql"] is False


def test_promotion_cli_rejects_production_execution_options(tmp_path: Path) -> None:
    paths = _make_package(tmp_path)
    output_dir = tmp_path / "forbidden"
    code = promotion_cli_main(
        [
            "--staging-import-json",
            str(paths["staging"]),
            "--transaction-plan-json",
            str(paths["plan"]),
            "--evidence-bundle-json",
            str(paths["bundle"]),
            "--output-dir",
            str(output_dir),
            "--execute-production-import",
        ]
    )

    assert code == 2
    assert not output_dir.exists()


def test_promotion_entrypoint_is_offline_formal_support(tmp_path: Path) -> None:
    root = tmp_path
    path = (
        root
        / "src/gas_calibrator/tools"
        / "export_v1_5_formal_database_import_production_promotion_preflight.py"
    )
    path.parent.mkdir(parents=True)
    path.write_text("", encoding="utf-8")

    entry = classify_v1_5_entrypoint(path, root=root)

    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
