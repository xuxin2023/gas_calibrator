from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

from gas_calibrator.v1_5.orchestration.full_flow import build_full_flow_plan
from gas_calibrator.validation.v1_5_entrypoint_inventory import classify_v1_5_entrypoint
from gas_calibrator.validation.v1_5_formal_flow_contract import (
    discover_current_v1_5_inventory,
    validate_v1_5_formal_flow_contract,
)
from gas_calibrator.validation.v1_5_formal_run_status import build_v1_5_formal_run_status


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _config(tmp_path: Path) -> Path:
    path = tmp_path / "config.json"
    path.write_text("{}", encoding="utf-8")
    return path


def _flag_value(command: tuple[str, ...], flag: str) -> str:
    values = [str(value) for value in command]
    return values[values.index(flag) + 1]


def test_full_flow_places_transaction_plan_and_blocker_before_database_import(tmp_path: Path) -> None:
    plan = build_full_flow_plan(
        config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo"
    )
    ids = [step.step_id for step in plan.steps]
    design_id = "formal_database_import_controlled_executor_design_snapshot"
    plan_id = "formal_database_import_transaction_plan_snapshot"
    blocker_id = "formal_database_import_transaction_blocked_executor_snapshot"
    assert ids.index(design_id) < ids.index(plan_id) < ids.index(blocker_id) < ids.index("database_import")

    transaction_step = next(step for step in plan.steps if step.step_id == plan_id)
    assert transaction_step.tool_module == (
        "gas_calibrator.tools.export_v1_5_formal_database_import_transaction_plan"
    )
    assert transaction_step.execution_mode == "offline_sidecar"
    assert transaction_step.opens_com_ports is False
    assert transaction_step.controls_gas_route is False
    assert transaction_step.controls_water_route is False
    assert transaction_step.writes_coefficients is False
    assert _flag_value(transaction_step.command, "--formal-database-dry-run-json").endswith(
        "formal_database_dry_run\\v1_5_formal_database_dry_run.json"
    )
    assert "--fail-on-blocker" in transaction_step.command

    blocked_step = next(step for step in plan.steps if step.step_id == blocker_id)
    assert blocked_step.tool_module == (
        "gas_calibrator.tools.run_v1_5_formal_database_import_transaction_blocked_executor"
    )
    assert blocked_step.execution_mode == "offline_sidecar"
    assert blocked_step.opens_com_ports is False
    assert blocked_step.writes_coefficients is False
    assert _flag_value(blocked_step.command, "--transaction-plan-json").endswith(
        "formal_database_import_transaction_plan\\v1_5_formal_database_import_transaction_plan.json"
    )
    assert "--fail-on-blocked" in blocked_step.command

    status_step = next(step for step in plan.steps if step.step_id == "formal_run_status_snapshot")
    assert _flag_value(status_step.command, "--formal-database-import-transaction-plan-json").endswith(
        "formal_database_import_transaction_plan\\v1_5_formal_database_import_transaction_plan.json"
    )
    assert _flag_value(
        status_step.command, "--formal-database-import-transaction-blocked-executor-json"
    ).endswith(
        "formal_database_import_transaction_blocked_executor\\v1_5_formal_database_import_transaction_blocked_executor.json"
    )


def test_formal_flow_contract_blocks_unlocked_transaction_surfaces(tmp_path: Path) -> None:
    plan = build_full_flow_plan(
        config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo"
    )
    report = validate_v1_5_formal_flow_contract(
        plan, inventory_entries=discover_current_v1_5_inventory(anchor_paths=(Path.cwd(),))
    )
    assert report.status == "pass"

    steps = list(plan.steps)
    index = [step.step_id for step in steps].index(
        "formal_database_import_transaction_plan_snapshot"
    )
    steps[index] = replace(
        steps[index],
        tool_module="gas_calibrator.tools.import_v1_5_evidence_package",
        execution_mode="offline_database_requires_configured_dsn",
        opens_com_ports=True,
        writes_coefficients=True,
        command=("python", "-m", "wrong"),
    )
    report = validate_v1_5_formal_flow_contract(
        replace(plan, steps=tuple(steps)),
        inventory_entries=discover_current_v1_5_inventory(anchor_paths=(Path.cwd(),)),
    )
    codes = {issue.code for issue in report.issues}
    assert report.status == "blocked"
    assert "formal_database_import_transaction_plan_wrong_tool" in codes
    assert "formal_database_import_transaction_plan_must_be_offline_no_write" in codes
    assert "formal_database_import_transaction_plan_missing_required_flag" in codes


def test_formal_status_tracks_both_transaction_gates_without_unlocking_import(tmp_path: Path) -> None:
    transaction = _write_json(
        tmp_path / "transaction.json",
        {
            "schema": "v1_5_formal_database_import_transaction_plan_v1",
            "overall_status": "ready_for_postgresql18_transaction_plan_review",
            "transaction_plan_contract_ready": True,
            "production_backend": "postgresql",
            "production_postgresql_major": 18,
            "dsn_value_read": False,
            "emits_executable_sql": False,
            "connects_postgresql": False,
            "applies_migrations": False,
            "database_import_attempted": False,
            "database_written": False,
            "database_import_allowed": False,
            "real_import_execution_allowed": False,
            "execution_supported": False,
        },
    )
    blocked = _write_json(
        tmp_path / "blocked.json",
        {
            "schema": "v1_5_formal_database_import_transaction_blocked_executor_v1",
            "overall_status": "blocked_pending_controlled_transaction_executor",
            "blocked_executor_ready": True,
            "emits_executable_sql": False,
            "connects_postgresql": False,
            "applies_migrations": False,
            "database_import_attempted": False,
            "database_written": False,
            "database_import_allowed": False,
            "real_import_execution_allowed": False,
            "execution_supported": False,
            "would_execute": False,
        },
    )
    model = build_v1_5_formal_run_status(
        run_dir=tmp_path,
        formal_database_import_transaction_plan_json=transaction,
        formal_database_import_transaction_blocked_executor_json=blocked,
    )
    gates = {row["gate_id"]: row for row in model["gates"]}
    assert gates["formal_database_import_transaction_plan"]["status"] == "ready"
    assert gates["formal_database_import_transaction_blocked_executor"]["status"] == "ready"
    assert model["database_import_transaction_package_ready"] is False
    assert model["database_import_allowed"] is False
    assert model["linked_inputs"]["formal_database_import_transaction_plan_json"] == str(
        transaction.resolve()
    )
    assert model["linked_inputs"][
        "formal_database_import_transaction_blocked_executor_json"
    ] == str(blocked.resolve())


def test_formal_status_blocks_transaction_plan_that_claims_connection(tmp_path: Path) -> None:
    transaction = _write_json(
        tmp_path / "transaction.json",
        {
            "schema": "v1_5_formal_database_import_transaction_plan_v1",
            "overall_status": "ready_for_postgresql18_transaction_plan_review",
            "transaction_plan_contract_ready": True,
            "production_backend": "postgresql",
            "production_postgresql_major": 18,
            "dsn_value_read": False,
            "emits_executable_sql": False,
            "connects_postgresql": True,
            "applies_migrations": False,
            "database_import_attempted": False,
            "database_written": False,
            "database_import_allowed": False,
            "real_import_execution_allowed": False,
            "execution_supported": False,
        },
    )
    model = build_v1_5_formal_run_status(
        run_dir=tmp_path,
        formal_database_import_transaction_plan_json=transaction,
    )
    gates = {row["gate_id"]: row for row in model["gates"]}
    assert gates["formal_database_import_transaction_plan"]["status"] == "blocked"
    assert model["database_import_allowed"] is False


def test_transaction_tools_are_offline_formal_support(tmp_path: Path) -> None:
    root = tmp_path
    paths = [
        root / "src/gas_calibrator/tools/export_v1_5_formal_database_import_transaction_plan.py",
        root
        / "src/gas_calibrator/tools/run_v1_5_formal_database_import_transaction_blocked_executor.py",
    ]
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")
        entry = classify_v1_5_entrypoint(path, root=root)
        assert entry.category == "formal_review_evidence"
        assert entry.formal_status == "formal_support"
        assert entry.risk_level == "offline"
        assert entry.opens_com_ports is False
        assert entry.controls_routes is False
        assert entry.writes_coefficients is False
