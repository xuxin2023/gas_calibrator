from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any, Mapping, Sequence

from .database import DatabaseManager, StorageSettings, load_storage_config_file, resolve_run_uuid, stable_uuid
from .models import DeviceEventRecord, RunRecord


EVENT_TYPE = "v1_5_post_initialization_pre_route_readiness"


def _load_json(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object JSON: {path}")
    return payload


def _infer_backend_from_dsn(dsn: str) -> str | None:
    lowered = str(dsn or "").strip().lower()
    if lowered.startswith("sqlite"):
        return "sqlite"
    if lowered.startswith("postgresql") or lowered.startswith("postgres"):
        return "postgresql"
    return None


def _build_settings(args: argparse.Namespace) -> StorageSettings:
    settings = load_storage_config_file(args.config) if args.config else StorageSettings()
    if args.dsn:
        settings.dsn = str(args.dsn)
        if not args.backend:
            inferred = _infer_backend_from_dsn(args.dsn)
            if inferred:
                settings.backend = inferred
    if args.backend:
        settings.backend = str(args.backend)
    if args.database:
        settings.database = str(args.database)
        if not args.backend and not args.dsn and settings.normalized_backend not in {"sqlite", "postgresql"}:
            settings.backend = "sqlite"
    if args.host:
        settings.host = str(args.host)
    if args.port is not None:
        settings.port = int(args.port)
    if args.user:
        settings.user = str(args.user)
    if args.password is not None:
        settings.password = str(args.password)
    if args.pool_size is not None:
        settings.pool_size = int(args.pool_size)
    if args.echo:
        settings.echo = True
    if not settings.is_enabled:
        raise ValueError("storage is not configured; pass --dsn, --config, or --backend sqlite --database <path>")
    return settings


def _readiness_gates(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    gates_container = payload.get("gates") if isinstance(payload.get("gates"), Mapping) else {}
    gates = gates_container.get("gates") if isinstance(gates_container, Mapping) else None
    if not isinstance(gates, list):
        raise ValueError("readiness payload is missing gates.gates")
    return [dict(gate) for gate in gates if isinstance(gate, Mapping)]


def _device_scope(payload: Mapping[str, Any], gates: Sequence[Mapping[str, Any]]) -> list[str]:
    devices: list[str] = []
    seen: set[str] = set()
    for gate in gates:
        for key in ("expected_device_scope", "device_scope", "devices"):
            values = gate.get(key)
            if not isinstance(values, list):
                continue
            for value in values:
                token = str(value or "").strip()
                if token and token not in seen:
                    seen.add(token)
                    devices.append(token)
    if devices:
        return devices
    configured = payload.get("device_scope")
    if isinstance(configured, list):
        for value in configured:
            token = str(value or "").strip()
            if token and token not in seen:
                seen.add(token)
                devices.append(token)
    if not devices:
        raise ValueError("readiness payload does not identify any device scope")
    return devices


def _gate_statuses(gates: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    return {str(gate.get("name") or ""): str(gate.get("status") or "") for gate in gates if gate.get("name")}


def build_v1_5_readiness_event_preview(
    *,
    readiness_payload: Mapping[str, Any],
    source_run_id: str,
) -> dict[str, Any]:
    gates = _readiness_gates(readiness_payload)
    devices = _device_scope(readiness_payload, gates)
    gate_statuses = _gate_statuses(gates)
    return {
        "schema_version": "v1_5_readiness_event_import_preview_v0",
        "source_run_id": source_run_id,
        "source_run_uuid": str(resolve_run_uuid(source_run_id)),
        "readiness_run_id": str(readiness_payload.get("run_id") or ""),
        "readiness_status": str(readiness_payload.get("status") or ""),
        "device_scope": devices,
        "event_type": EVENT_TYPE,
        "event_count": len(devices),
        "gate_statuses": gate_statuses,
        "database_written": False,
        "boundary": {
            "opens_com": False,
            "writes_device": False,
            "controls_pressure": False,
            "controls_temperature": False,
            "controls_gas_route": False,
            "controls_water_route": False,
            "runs_sampling": False,
            "runs_fitting": False,
            "not_calibration_acceptance_result": True,
        },
    }


def import_v1_5_readiness_events(
    database: DatabaseManager,
    *,
    readiness_payload: Mapping[str, Any],
    source_run_id: str,
    dry_run: bool = True,
    allow_write: bool = False,
    operator: str | None = None,
) -> dict[str, Any]:
    preview = build_v1_5_readiness_event_preview(readiness_payload=readiness_payload, source_run_id=source_run_id)
    if dry_run:
        return preview
    if not allow_write:
        raise PermissionError("formal database readiness event write requires allow_write=True")

    run_uuid = resolve_run_uuid(source_run_id)
    readiness_run_id = str(readiness_payload.get("run_id") or "v1_5_post_initialization_readiness")
    generated_at = str(readiness_payload.get("generated_at") or "")
    timestamp = datetime.now(timezone.utc)
    gates_payload = readiness_payload.get("gates") if isinstance(readiness_payload.get("gates"), Mapping) else {}
    event_payload = {
        "readiness_schema_version": readiness_payload.get("schema_version"),
        "readiness_run_id": readiness_run_id,
        "readiness_status": readiness_payload.get("status"),
        "generated_at": generated_at,
        "gate_statuses": preview["gate_statuses"],
        "gates": gates_payload,
        "source_paths": dict(readiness_payload.get("source_paths") or {}),
        "evidence_paths": dict(readiness_payload.get("evidence_paths") or {}),
        "operator": operator,
        "meaning": "V1.5 post-initialization pressure/temperature readiness and pre-route gate state.",
        "boundary": {
            **dict(readiness_payload.get("boundary") or {}),
            "opens_com": False,
            "writes_device": False,
            "controls_gas_route": False,
            "controls_water_route": False,
            "runs_sampling": False,
            "runs_fitting": False,
            "not_calibration_acceptance_result": True,
        },
    }

    with database.session_scope() as session:
        if session.get(RunRecord, run_uuid) is None:
            raise ValueError(f"source run is missing from storage: {source_run_id}")
        for device in preview["device_scope"]:
            event_id = stable_uuid("device_event", run_uuid, device, EVENT_TYPE, readiness_run_id)
            session.merge(
                DeviceEventRecord(
                    id=event_id,
                    run_id=run_uuid,
                    device_name=device,
                    event_type=EVENT_TYPE,
                    event_data={**event_payload, "device_name": device},
                    timestamp=timestamp,
                )
            )
    return {
        **preview,
        "dry_run": False,
        "database_written": True,
        "events_written": len(preview["device_scope"]),
    }


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Preview or import V1.5 post-initialization readiness gates as storage device events.")
    parser.add_argument("--readiness", required=True, help="V1.5 post-initialization readiness backfill result JSON.")
    parser.add_argument("--source-run-id", required=True, help="Existing storage run id to attach readiness events to.")
    parser.add_argument("--config", help="Optional JSON config file containing a storage section.")
    parser.add_argument("--dsn", help="SQLAlchemy DSN, e.g. sqlite:///D:/tmp/storage.sqlite")
    parser.add_argument("--backend", help="Storage backend, e.g. sqlite or postgresql.")
    parser.add_argument("--database", help="Database name or SQLite file path.")
    parser.add_argument("--host", help="Database host override.")
    parser.add_argument("--port", type=int, help="Database port override.")
    parser.add_argument("--user", help="Database user override.")
    parser.add_argument("--password", help="Database password override.")
    parser.add_argument("--pool-size", type=int, help="Connection pool size override.")
    parser.add_argument("--echo", action="store_true", help="Enable SQLAlchemy echo logging.")
    parser.add_argument("--operator", help="Optional operator name for imported readiness metadata.")
    parser.add_argument("--apply", action="store_true", help="Write readiness events to configured database.")
    parser.add_argument(
        "--acknowledge-formal-db-write",
        action="store_true",
        help="Required with --apply to confirm this is an intentional formal DB write.",
    )
    return parser.parse_args(argv)


def run_import(
    *,
    readiness: str | Path,
    source_run_id: str,
    settings: StorageSettings | None = None,
    apply: bool = False,
    acknowledge_formal_db_write: bool = False,
    operator: str | None = None,
) -> dict[str, Any]:
    payload = _load_json(readiness)
    if not apply:
        return import_v1_5_readiness_events(
            DatabaseManager(settings or StorageSettings(backend="file")),
            readiness_payload=payload,
            source_run_id=source_run_id,
            dry_run=True,
            operator=operator,
        )
    if not acknowledge_formal_db_write:
        raise PermissionError("--apply requires --acknowledge-formal-db-write")
    if settings is None:
        raise ValueError("settings are required when --apply is used")
    database = DatabaseManager(settings)
    try:
        return import_v1_5_readiness_events(
            database,
            readiness_payload=payload,
            source_run_id=source_run_id,
            dry_run=False,
            allow_write=True,
            operator=operator,
        )
    finally:
        database.dispose()


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        settings = _build_settings(args) if args.apply else None
        result = run_import(
            readiness=args.readiness,
            source_run_id=args.source_run_id,
            settings=settings,
            apply=bool(args.apply),
            acknowledge_formal_db_write=bool(args.acknowledge_formal_db_write),
            operator=args.operator,
        )
    except Exception as exc:  # pragma: no cover - CLI failure path
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
