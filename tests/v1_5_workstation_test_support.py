from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from gas_calibrator.validation.v1_5_algorithm_route_profiles import (
    build_v1_5_profile_queue_rows,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = ROOT / "configs" / "default_config.json"
PROFILE_PATH = ROOT / "configs" / "v1_5_algorithm_route_profiles.json"


def write_csv_rows(path: Path, rows: list[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return path


def write_legacy_profile_queues(tmp_path: Path) -> tuple[Path, Path]:
    queues = build_v1_5_profile_queue_rows(
        PROFILE_PATH,
        profile_id="legacy_ratio_production",
    )
    return (
        write_csv_rows(tmp_path / "co2_runner_queue.csv", queues["co2_rows"]),
        write_csv_rows(tmp_path / "h2o_runner_queue.csv", queues["h2o_rows"]),
    )


def write_decision_authority_archive(
    tmp_path: Path,
    *,
    formal_overrides: Mapping[str, Any] | None = None,
    report_overrides: Mapping[str, Any] | None = None,
    bundle_overrides: Mapping[str, Any] | None = None,
    run_id: str = "formal_batch_001",
    device_ids: tuple[str, ...] = ("001", "002"),
    config_sha256: str | None = None,
) -> tuple[Path, Path, Path]:
    """Create one internally consistent synthetic authority archive for tests."""

    run_dir = (tmp_path / "formal_batch_run").resolve()
    closure_dir = run_dir / "formal_archive_closure"
    closure_dir.mkdir(parents=True, exist_ok=True)
    run_db_id = "run-db-formal-batch-001"
    active_config_sha = config_sha256 or hashlib.sha256(
        DEFAULT_CONFIG_PATH.read_bytes()
    ).hexdigest()
    formal_status = {
        "schema": "v1_5_formal_run_status_v1",
        "run_dir": str(run_dir),
        "overall_status": "formal_release_ready",
        "current_stage": "complete",
        "can_continue_physical_flow": True,
        "formal_release_allowed": True,
        "senco_artifact_authorization": {
            "controlled_write_authorization_ready": True,
            "authorized_device_ids": list(device_ids),
        },
    }
    formal_status.update(formal_overrides or {})
    report_model = {
        "run_id": run_id,
        "run_db_id": run_db_id,
        "formal_run_status": {
            "available": True,
            "overall_status": formal_status["overall_status"],
            "current_stage": formal_status["current_stage"],
            "formal_release_allowed": formal_status["formal_release_allowed"],
            "can_continue_physical_flow": formal_status[
                "can_continue_physical_flow"
            ],
        },
        "report_release_decision": {
            "formal_issue_allowed": True,
            "release_status": "formal_release_ready",
            "reasons": [],
        },
        "per_device_certificate_readiness": [
            {"analyzer_device_id": device_id, "status": "ready"}
            for device_id in device_ids
        ],
    }
    report_model.update(report_overrides or {})
    evidence_bundle = {
        "schema": "v1_5_evidence_registry",
        "schema_version": "001",
        "run_id": run_id,
        "run_db_id": run_db_id,
        "tables": {
            "runs": [
                {
                    "id": run_db_id,
                    "run_id": run_id,
                    "run_dir": str(run_dir),
                    "config_hash": active_config_sha,
                    "metadata": {"analyzer_device_ids": list(device_ids)},
                }
            ]
        },
    }
    evidence_bundle.update(bundle_overrides or {})

    formal_path = closure_dir / "v1_5_formal_run_status.json"
    report_path = closure_dir / "report_model.json"
    evidence_path = closure_dir / "evidence_bundle.json"
    for path, payload in (
        (formal_path, formal_status),
        (report_path, report_model),
        (evidence_path, evidence_bundle),
    ):
        path.write_text(
            json.dumps(payload, ensure_ascii=False),
            encoding="utf-8",
        )

    archive = {
        "schema": "v1_5_formal_archive_closure_v1",
        "run_id": run_id,
        "run_db_id": run_db_id,
        "run_dir": str(run_dir),
        "identity_getco_traceability": {
            "analyzer_device_ids": list(device_ids),
        },
        "senco_authorization_write_traceability": {
            "device_ids": list(device_ids),
        },
        "formal_run_status": {"json_path": str(formal_path.resolve())},
        "reports": {"report_model": str(report_path.resolve())},
        "artifacts": [
            {
                "role": role,
                "path": str(path.resolve()),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "size_bytes": path.stat().st_size,
            }
            for role, path in (
                ("formal_run_status_json", formal_path),
                ("report_report_model", report_path),
                ("evidence_bundle", evidence_path),
            )
        ],
    }
    archive_path = closure_dir / "v1_5_formal_archive_closure_index.json"
    archive_path.write_text(
        json.dumps(archive, ensure_ascii=False),
        encoding="utf-8",
    )
    return archive_path, formal_path, report_path
