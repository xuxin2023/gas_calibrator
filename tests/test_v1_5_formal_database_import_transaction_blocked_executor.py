from __future__ import annotations

import json
from pathlib import Path

import pytest

from gas_calibrator.tools.run_v1_5_formal_database_import_transaction_blocked_executor import (
    FORBIDDEN_FLAGS,
    main as cli_main,
)
from gas_calibrator.validation.v1_5_formal_database_import_transaction_blocked_executor import (
    build_v1_5_formal_database_import_transaction_blocked_executor,
)


def _plan(path: Path, *, ready: bool = True) -> Path:
    payload = {
        "schema": "v1_5_formal_database_import_transaction_plan_v1",
        "transaction_plan_contract_ready": ready,
        "production_transaction_package_ready": False,
        "connects_postgresql": False,
        "database_written": False,
        "database_import_attempted": False,
        "database_import_allowed": False,
        "real_import_execution_allowed": False,
        "execution_supported": False,
        "emits_executable_sql": False,
        "transaction_operations": [{"order": 1, "would_execute": False}],
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def test_blocked_executor_accepts_plan_but_never_executes(tmp_path: Path) -> None:
    model = build_v1_5_formal_database_import_transaction_blocked_executor(
        transaction_plan_json=_plan(tmp_path / "plan.json")
    )
    assert model["overall_status"] == "blocked_pending_controlled_transaction_executor"
    assert model["blocked_executor_ready"] is True
    assert model["execution_supported"] is False
    assert model["would_execute"] is False
    assert model["connects_postgresql"] is False
    assert model["database_written"] is False
    assert model["database_import_allowed"] is False


def test_blocked_executor_holds_invalid_or_unlocked_plan(tmp_path: Path) -> None:
    path = _plan(tmp_path / "plan.json")
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["connects_postgresql"] = True
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    model = build_v1_5_formal_database_import_transaction_blocked_executor(
        transaction_plan_json=path
    )
    assert model["overall_status"] == "review_required"
    assert model["blocked_executor_ready"] is False
    assert "transaction_plan_connects_postgresql_not_false" in model["reasons"]


@pytest.mark.parametrize("flag", sorted(FORBIDDEN_FLAGS))
def test_blocked_executor_cli_rejects_all_live_unlock_inputs_without_artifact(
    tmp_path: Path, flag: str
) -> None:
    output_dir = tmp_path / flag.removeprefix("--")
    value = "true" if flag in {"--execute", "--execute-controlled-import", "--apply-migrations"} else "value"
    rc = cli_main(
        [
            "--transaction-plan-json",
            str(_plan(tmp_path / "plan.json")),
            "--output-dir",
            str(output_dir),
            flag,
            value,
        ]
    )
    assert rc == 2
    assert not output_dir.exists()


def test_blocked_executor_cli_writes_only_blocked_evidence(tmp_path: Path, capsys) -> None:
    output_dir = tmp_path / "out"
    rc = cli_main(
        [
            "--transaction-plan-json",
            str(_plan(tmp_path / "plan.json")),
            "--output-dir",
            str(output_dir),
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert payload["blocked_executor_ready"] is True
    assert payload["connects_postgresql"] is False
    assert payload["database_written"] is False
    assert (output_dir / "v1_5_formal_database_import_transaction_blocked_executor.json").exists()

    rc = cli_main(
        [
            "--transaction-plan-json",
            str(tmp_path / "plan.json"),
            "--output-dir",
            str(tmp_path / "blocked"),
            "--fail-on-blocked",
        ]
    )
    assert rc == 2
