from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from ..v2.storage.database import DatabaseManager, StorageSettings, load_storage_config_file
from .initialization_database import (
    build_v1_5_initialization_storage_preview,
    build_v1_5_runtime_setup_storage_preview,
    import_v1_5_initialization_bundle,
    import_v1_5_initialization_payload,
    import_v1_5_runtime_setup_result,
    load_v1_5_initialization_bundle,
    load_v1_5_runtime_setup_result,
    subset_v1_5_initialization_bundle,
)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Preview or import a V1.5 formal initialization identity bundle into the calibration storage database"
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--bundle", help="Path to V1.5 formal DB dry-run JSON bundle")
    source.add_argument("--runtime-setup-result", help="Path to V1.5 analyzer runtime setup result JSON")
    parser.add_argument(
        "--include-sn-code",
        action="append",
        default=[],
        help="Limit a formal initialization bundle import to one or more 8-digit SN codes; may be repeated or comma-separated",
    )
    parser.add_argument("--derived-run-id", help="Run ID to use when importing an SN subset from a source bundle")
    parser.add_argument("--config", help="Optional JSON config file containing a storage section")
    parser.add_argument("--dsn", help="SQLAlchemy DSN, e.g. sqlite:///D:/tmp/storage.sqlite")
    parser.add_argument("--backend", help="Storage backend, e.g. sqlite or postgresql")
    parser.add_argument("--database", help="Database name or SQLite file path")
    parser.add_argument("--host", help="Database host override")
    parser.add_argument("--port", type=int, help="Database port override")
    parser.add_argument("--user", help="Database user override")
    parser.add_argument("--password", help="Database password override")
    parser.add_argument("--pool-size", type=int, help="Connection pool size override")
    parser.add_argument("--echo", action="store_true", help="Enable SQLAlchemy echo logging")
    parser.add_argument("--operator", help="Optional operator name for imported initialization metadata")
    parser.add_argument("--init-schema", action="store_true", help="Create database schema before import")
    parser.add_argument("--apply", action="store_true", help="Write to configured database")
    parser.add_argument(
        "--acknowledge-formal-db-write",
        action="store_true",
        help="Required with --apply to confirm this is an intentional formal DB write",
    )
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
    bundle: str | Path | None = None,
    runtime_setup_result: str | Path | None = None,
    include_sn_codes: Sequence[str] | None = None,
    derived_run_id: str | None = None,
    settings: StorageSettings | None = None,
    init_schema: bool = False,
    apply: bool = False,
    acknowledge_formal_db_write: bool = False,
    operator: str | None = None,
) -> dict[str, object]:
    if bool(bundle) == bool(runtime_setup_result):
        raise ValueError("pass exactly one of bundle or runtime_setup_result")
    if include_sn_codes and runtime_setup_result:
        raise ValueError("include_sn_codes is only supported for formal initialization bundles")
    if not apply:
        if runtime_setup_result:
            payload = load_v1_5_runtime_setup_result(runtime_setup_result)
            preview = build_v1_5_runtime_setup_storage_preview(payload, result_path=runtime_setup_result)
            return {
                "runtime_setup_result": str(Path(runtime_setup_result).resolve()),
                "import_type": "v1_5_analyzer_runtime_setup",
                "dry_run": True,
                "database_written": False,
                "imported": preview,
            }
        payload = load_v1_5_initialization_bundle(bundle)
        if include_sn_codes:
            payload = subset_v1_5_initialization_bundle(
                payload,
                include_sn_codes=list(include_sn_codes),
                run_id=derived_run_id,
                source_note="derived current-device initialization subset for formal DB import",
            )
        preview = build_v1_5_initialization_storage_preview(payload)
        return {
            "bundle": str(Path(bundle).resolve()),
            "import_type": "v1_5_formal_initialization",
            "include_sn_codes": list(include_sn_codes or []),
            "derived_run_id": preview["run_id"] if include_sn_codes else None,
            "dry_run": True,
            "database_written": False,
            "imported": preview,
        }

    if not acknowledge_formal_db_write:
        raise PermissionError("--apply requires --acknowledge-formal-db-write")
    if settings is None:
        raise ValueError("settings are required when --apply is used")

    database = DatabaseManager(settings)
    try:
        if init_schema:
            database.initialize()
        if runtime_setup_result:
            result = import_v1_5_runtime_setup_result(
                database,
                runtime_setup_result,
                dry_run=False,
                allow_write=True,
                operator=operator,
            )
            source_key = "runtime_setup_result"
            source_path = str(Path(runtime_setup_result).resolve())
            import_type = "v1_5_analyzer_runtime_setup"
        else:
            if include_sn_codes:
                payload = load_v1_5_initialization_bundle(bundle)
                payload = subset_v1_5_initialization_bundle(
                    payload,
                    include_sn_codes=list(include_sn_codes),
                    run_id=derived_run_id,
                    source_note="derived current-device initialization subset for formal DB import",
                )
                result = import_v1_5_initialization_payload(
                    database,
                    payload,
                    dry_run=False,
                    allow_write=True,
                    operator=operator,
                )
            else:
                result = import_v1_5_initialization_bundle(
                    database,
                    bundle,
                    dry_run=False,
                    allow_write=True,
                    operator=operator,
                )
            source_key = "bundle"
            source_path = str(Path(bundle).resolve())
            import_type = "v1_5_formal_initialization"
        return {
            source_key: source_path,
            "import_type": import_type,
            "backend": settings.normalized_backend,
            "schema_initialized": bool(init_schema),
            "dry_run": False,
            "database_written": True,
            "include_sn_codes": list(include_sn_codes or []),
            "derived_run_id": result.get("run_id") if include_sn_codes else None,
            "imported": result,
        }
    finally:
        database.dispose()


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        settings = _build_settings(args) if args.apply else None
        payload = run_import(
            bundle=args.bundle,
            runtime_setup_result=args.runtime_setup_result,
            include_sn_codes=args.include_sn_code,
            derived_run_id=args.derived_run_id,
            settings=settings,
            init_schema=bool(args.init_schema),
            apply=bool(args.apply),
            acknowledge_formal_db_write=bool(args.acknowledge_formal_db_write),
            operator=args.operator,
        )
    except Exception as exc:  # pragma: no cover - CLI failure path
        print(str(exc), file=sys.stderr)
        return 1

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
