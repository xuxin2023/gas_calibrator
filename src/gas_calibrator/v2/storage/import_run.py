from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .database import DatabaseManager, StorageSettings, load_storage_config_file
from .importer import ArtifactImporter


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import an offline V2 run directory into database storage")
    parser.add_argument("--run-dir", required=True, help="Path to the exported V2 run directory")
    parser.add_argument("--config", help="Optional JSON config file containing a storage section")
    parser.add_argument("--stage", choices=("raw", "enrich", "all"), default="all", help="Import stage to execute")
    parser.add_argument("--dsn", help="SQLAlchemy DSN, e.g. sqlite:///D:/tmp/storage.sqlite")
    parser.add_argument("--backend", help="Storage backend, e.g. sqlite or postgresql")
    parser.add_argument("--database", help="Database name or SQLite file path")
    parser.add_argument("--host", help="Database host override")
    parser.add_argument("--port", type=int, help="Database port override")
    parser.add_argument("--user", help="Database user override")
    parser.add_argument("--password", help="Database password override")
    parser.add_argument("--pool-size", type=int, help="Connection pool size override")
    parser.add_argument("--echo", action="store_true", help="Enable SQLAlchemy echo logging")
    parser.add_argument("--operator", help="Optional operator name for imported run metadata")
    parser.add_argument("--batch-size", type=int, default=500, help="Batch size for sample import")
    parser.add_argument("--init-schema", action="store_true", help="Create database schema before import")
    return parser.parse_args(argv)


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
            inferred_backend = _infer_backend_from_dsn(args.dsn)
            if inferred_backend:
                settings.backend = inferred_backend

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


def run_import(
    *,
    run_dir: str | Path,
    settings: StorageSettings,
    stage: str = "all",
    init_schema: bool = False,
    operator: str | None = None,
    batch_size: int = 500,
    artifact_dir: str | Path | None = None,
) -> dict[str, object]:
    database = DatabaseManager(settings)
    try:
        if init_schema:
            database.initialize()
        importer = ArtifactImporter(database)
        normalized_stage = str(stage or "all").strip().lower()
        if normalized_stage == "raw":
            result = importer.import_raw_run_directory(run_dir, operator=operator, batch_size=batch_size)
        elif normalized_stage == "enrich":
            result = importer.import_enrich_run_directory(run_dir, artifact_dir=artifact_dir, operator=operator)
        else:
            result = importer.import_run_directory(
                run_dir,
                operator=operator,
                batch_size=batch_size,
                artifact_dir=artifact_dir,
            )
        return {
            "run_dir": str(Path(run_dir).resolve()),
            "artifact_dir": str(Path(artifact_dir).resolve()) if artifact_dir is not None else None,
            "backend": settings.normalized_backend,
            "stage": normalized_stage,
            "schema_initialized": bool(init_schema),
            "imported": result,
        }
    finally:
        database.dispose()


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        settings = _build_settings(args)
        payload = run_import(
            run_dir=args.run_dir,
            settings=settings,
            stage=args.stage,
            init_schema=bool(args.init_schema),
            operator=args.operator,
            batch_size=int(args.batch_size),
        )
    except Exception as exc:  # pragma: no cover - exercised by CLI usage, not success-path unit tests
        print(str(exc), file=sys.stderr)
        return 1

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
