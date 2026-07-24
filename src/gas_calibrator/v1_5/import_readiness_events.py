from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any, Sequence

from ..storage.database import DatabaseManager, StorageSettings, load_storage_config_file
from .readiness_event_database import import_v1_5_readiness_events


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
