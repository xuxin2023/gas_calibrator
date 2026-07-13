"""Blocked executor evidence for the V1.5 PostgreSQL transaction plan."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping


SCHEMA = "v1_5_formal_database_import_transaction_blocked_executor_v1"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _display_path(path: Path) -> str:
    try:
        return path.relative_to(Path.cwd().resolve()).as_posix()
    except ValueError:
        return str(path)


def build_v1_5_formal_database_import_transaction_blocked_executor(
    *, transaction_plan_json: str | Path
) -> dict[str, Any]:
    path = Path(transaction_plan_json).resolve()
    payload: dict[str, Any] = {}
    if path.exists() and path.is_file():
        loaded = json.loads(path.read_text(encoding="utf-8-sig"))
        if isinstance(loaded, dict):
            payload = loaded
    reasons: list[str] = []
    if payload.get("schema") != "v1_5_formal_database_import_transaction_plan_v1":
        reasons.append("transaction_plan_schema_invalid_or_missing")
    if payload.get("transaction_plan_contract_ready") is not True:
        reasons.append("transaction_plan_contract_not_ready")
    for key in (
        "connects_postgresql",
        "database_written",
        "database_import_attempted",
        "database_import_allowed",
        "real_import_execution_allowed",
        "execution_supported",
        "emits_executable_sql",
    ):
        if payload.get(key) is not False:
            reasons.append(f"transaction_plan_{key}_not_false")
    operations = payload.get("transaction_operations") or []
    if (
        not isinstance(operations, list)
        or not operations
        or any(not isinstance(row, Mapping) or row.get("would_execute") is not False for row in operations)
    ):
        reasons.append("transaction_operations_not_locked")
    ready = not reasons
    return {
        "schema": SCHEMA,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "overall_status": (
            "blocked_pending_controlled_transaction_executor" if ready else "review_required"
        ),
        "blocker_count": 0,
        "review_required_count": len(reasons),
        "blocked_executor_ready": ready,
        "transaction_plan_json": _display_path(path),
        "transaction_plan_sha256": _sha256(path) if path.exists() and path.is_file() else "",
        "transaction_plan_contract_ready": bool(payload.get("transaction_plan_contract_ready")),
        "production_transaction_package_ready": bool(payload.get("production_transaction_package_ready")),
        "reasons": reasons,
        "execution_supported": False,
        "would_execute": False,
        "emits_executable_sql": False,
        "connects_postgresql": False,
        "applies_migrations": False,
        "database_import_attempted": False,
        "database_written": False,
        "database_import_allowed": False,
        "real_import_execution_allowed": False,
        "formal_release_allowed": False,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "writes_sn": False,
        "writes_device_id": False,
        "writes_coefficients": False,
        "not_real_acceptance_evidence": True,
        "next_action": "Keep import locked until a separate reviewed controlled executor exists.",
    }


def write_v1_5_formal_database_import_transaction_blocked_executor_outputs(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    paths = {
        "json": out / "v1_5_formal_database_import_transaction_blocked_executor.json",
        "summary_csv": out / "v1_5_formal_database_import_transaction_blocked_executor_summary.csv",
        "markdown": out / "V1_5_FORMAL_DATABASE_IMPORT_TRANSACTION_BLOCKED_EXECUTOR.md",
    }
    out.mkdir(parents=True, exist_ok=True)
    paths["json"].write_text(
        json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8-sig"
    )
    with paths["summary_csv"].open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=("overall_status", "blocked_executor_ready", "connects_postgresql", "database_written", "reasons"),
        )
        writer.writeheader()
        writer.writerow(
            {
                "overall_status": model.get("overall_status"),
                "blocked_executor_ready": model.get("blocked_executor_ready"),
                "connects_postgresql": model.get("connects_postgresql"),
                "database_written": model.get("database_written"),
                "reasons": ";".join(model.get("reasons") or []),
            }
        )
    paths["markdown"].write_text(
        "\n".join(
            [
                "# V1.5 PostgreSQL 18 transaction blocked executor",
                "",
                "This executable surface is intentionally blocked. It does not connect PostgreSQL or import rows.",
                "",
                f"- overall_status: `{model.get('overall_status')}`",
                f"- blocked_executor_ready: `{model.get('blocked_executor_ready')}`",
                f"- connects_postgresql: `{model.get('connects_postgresql')}`",
                f"- database_written: `{model.get('database_written')}`",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return paths
