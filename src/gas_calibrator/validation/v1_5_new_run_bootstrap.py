"""Atomically bootstrap one new, zero-authority V1.5 production run."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import shutil
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from gas_calibrator.v1_5.orchestration.full_flow import (
    PLAN_SCHEMA,
    build_full_flow_plan,
    build_full_flow_state,
    write_full_flow_plan,
    write_full_flow_state,
)

from .v1_5_authoritative_resume_offline_state_advance_post_write_verification import (
    _contains_reparse,
)

SCHEMA = "v1_5_new_run_bootstrap_v1"
READY_STATUS = "new_v1_5_run_bootstrapped_execution_locked"
RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{5,95}$")
STATE_SCHEMA = "v1_5_full_calibration_flow_state_v0"


def _now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _iso(value: datetime) -> str:
    return (
        value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    )


def _sha(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else ""
    except OSError:
        return ""


def _load_json_object_bytes(path: Path) -> tuple[dict[str, Any], bytes]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8-sig"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("bootstrap config must be a readable JSON object") from exc
    if not isinstance(value, Mapping):
        raise ValueError("bootstrap config must be a readable JSON object")
    return dict(value), raw


def _write_artifact_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=("role", "path", "sha256"))
        writer.writeheader()
        writer.writerows(
            {
                "role": row.get("role"),
                "path": row.get("path"),
                "sha256": row.get("sha256"),
            }
            for row in rows
        )


def bootstrap_v1_5_new_run(
    *,
    config_path: str | Path,
    runs_root: str | Path,
    run_id: str,
    operator: str,
    reviewer: str,
    approver: str,
    analyzer_id: str = "multi_device",
    now: datetime | None = None,
) -> dict[str, Any]:
    created_at = (now or _now()).astimezone(UTC).replace(microsecond=0)
    run_id = str(run_id).strip()
    if not RUN_ID_RE.fullmatch(run_id):
        raise ValueError("run_id must match the production bootstrap naming contract")
    identities = [str(value).strip() for value in (operator, reviewer, approver)]
    if any(not value for value in identities) or len(set(identities)) != 3:
        raise ValueError(
            "operator, reviewer, and approver must be present and distinct"
        )

    config_source = Path(config_path).absolute()
    root = Path(runs_root).absolute()
    target = root / run_id
    if _contains_reparse(config_source):
        raise ValueError("bootstrap config path must not contain a reparse point")
    _config, config_bytes = _load_json_object_bytes(config_source)
    config_source_sha = hashlib.sha256(config_bytes).hexdigest()
    if _contains_reparse(root) or _contains_reparse(target):
        raise ValueError("bootstrap run path must not contain a reparse point")
    root.mkdir(parents=True, exist_ok=True)
    if target.exists():
        raise FileExistsError(f"V1.5 run already exists: {target}")
    lock_path = root / f".{run_id}.bootstrap.lock"
    try:
        lock_fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise FileExistsError(f"V1.5 run bootstrap already active: {run_id}") from exc
    temp: Path | None = None
    try:
        os.write(
            lock_fd,
            f"run_id={run_id}\ncreated_at={_iso(created_at)}\n".encode("utf-8"),
        )
        temp = Path(tempfile.mkdtemp(prefix=f".{run_id}.bootstrap.", dir=root))
        snapshot_relative = Path("bootstrap_input") / "config_snapshot.json"
        snapshot_temp = temp / snapshot_relative
        snapshot_final = target / snapshot_relative
        snapshot_temp.parent.mkdir(parents=True, exist_ok=True)
        snapshot_temp.write_bytes(config_bytes)

        plan = build_full_flow_plan(
            config_path=snapshot_final,
            output_dir=target,
            run_id=run_id,
            operator=identities[0],
            analyzer_id=str(analyzer_id).strip() or "multi_device",
            reviewer=identities[1],
            approver=identities[2],
        )
        if plan.schema != PLAN_SCHEMA or plan.run_id != run_id:
            raise RuntimeError("generated full-flow plan identity mismatch")
        plan_outputs = write_full_flow_plan(plan, temp)
        state = build_full_flow_state(
            plan,
            completed_steps=(),
            failed_steps=(),
            allow_real_com=False,
            allow_pressure_control=False,
            allow_route_control=False,
            allow_writes=False,
        )
        state_outputs = write_full_flow_state(state, temp)
        first_step_id = plan.steps[0].step_id if plan.steps else ""
        if state.schema != STATE_SCHEMA or state.run_id != run_id:
            raise RuntimeError("generated initial state identity mismatch")
        if state.current_step_id != first_step_id:
            raise RuntimeError(
                "generated initial state does not start at the first step"
            )
        if state.completed_step_ids or state.failed_step_ids:
            raise RuntimeError("generated initial state contains an advanced prefix")
        if any(
            (
                state.allow_real_com,
                state.allow_pressure_control,
                state.allow_route_control,
                state.allow_writes,
            )
        ):
            raise RuntimeError("generated initial state contains physical authority")

        output_paths = {**plan_outputs, **state_outputs}
        artifacts = [
            {
                "role": "config_snapshot",
                "path": str(snapshot_final),
                "sha256": _sha(snapshot_temp),
            }
        ]
        for role, path in output_paths.items():
            relative = Path(path).resolve().relative_to(temp.resolve())
            artifacts.append(
                {
                    "role": str(role),
                    "path": str(target / relative),
                    "sha256": _sha(Path(path)),
                }
            )
        artifact_csv = temp / "v1_5_new_run_bootstrap_artifacts.csv"
        _write_artifact_csv(artifact_csv, artifacts)
        manifest = {
            "schema": SCHEMA,
            "generated_at": _iso(created_at),
            "overall_status": READY_STATUS,
            "new_run_bootstrap_ready": True,
            "run_id": run_id,
            "operator": identities[0],
            "reviewer": identities[1],
            "approver": identities[2],
            "analyzer_id": str(analyzer_id).strip() or "multi_device",
            "run_root": str(target),
            "config_source_path_recorded_only": str(config_source),
            "config_source_sha256": config_source_sha,
            "config_snapshot_json": str(snapshot_final),
            "config_snapshot_sha256": _sha(snapshot_temp),
            "full_flow_plan_json": str(target / "v1_5_full_flow_plan.json"),
            "full_flow_plan_sha256": _sha(temp / "v1_5_full_flow_plan.json"),
            "authoritative_state_json": str(target / "v1_5_full_flow_state.json"),
            "authoritative_state_sha256": _sha(temp / "v1_5_full_flow_state.json"),
            "current_step_id": state.current_step_id,
            "completed_step_ids": [],
            "failed_step_ids": [],
            "physical_capabilities": {
                "allow_real_com": False,
                "allow_pressure_control": False,
                "allow_route_control": False,
                "allow_writes": False,
            },
            "bootstrap_is_atomic_directory_publish": True,
            "execution_supported": False,
            "would_execute": False,
            "opens_com_ports": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "writes_sn": False,
            "writes_device_id": False,
            "writes_coefficients": False,
            "connects_postgresql": False,
            "database_written": False,
            "formal_release_allowed": False,
            "database_import_allowed": False,
            "not_real_acceptance_evidence": True,
            "artifacts": artifacts,
            "next_action": "Run initialization evidence steps from the first canonical stage; do not mark steps complete manually.",
        }
        manifest_temp = temp / "v1_5_new_run_bootstrap.json"
        manifest_temp.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        if _sha(config_source) != config_source_sha:
            raise RuntimeError("bootstrap config changed before atomic publish")
        os.replace(temp, target)
        return {
            **manifest,
            "manifest_json": str(target / manifest_temp.name),
            "manifest_sha256": _sha(target / manifest_temp.name),
            "artifact_csv": str(target / artifact_csv.name),
            "artifact_csv_sha256": _sha(target / artifact_csv.name),
        }
    except BaseException:
        if temp is not None and temp.exists():
            shutil.rmtree(temp, ignore_errors=True)
        raise
    finally:
        os.close(lock_fd)
        lock_path.unlink(missing_ok=True)


__all__ = [
    "READY_STATUS",
    "RUN_ID_RE",
    "SCHEMA",
    "STATE_SCHEMA",
    "bootstrap_v1_5_new_run",
]
