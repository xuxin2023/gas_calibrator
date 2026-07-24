"""CLI for read-only V1.5 initialization database preflight."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from ..v1_5.initialization_db_preflight import (
    build_v1_5_initialization_db_preflight,
    write_v1_5_initialization_db_preflight_report,
)
from ..storage.database import DatabaseManager, StorageSettings, load_storage_config_file


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Check V1.5 initialization readiness from the database only. "
            "This does not open COM, write analyzer state, control routes, sample, or fit."
        )
    )
    parser.add_argument(
        "--expected-device",
        action="append",
        default=[],
        help="Expected analyzer identity, e.g. 01260604=004. May be repeated.",
    )
    parser.add_argument(
        "--expected-sn-code",
        action="append",
        default=[],
        help="Expected 8-digit SN when protocol ID is not supplied. May be repeated.",
    )
    parser.add_argument("--config", help="Optional JSON config file containing a storage section.")
    parser.add_argument("--dsn", help="SQLAlchemy DSN. For production use PostgreSQL 18.")
    parser.add_argument("--backend", help="Storage backend, e.g. postgresql or sqlite.")
    parser.add_argument("--database", help="Database name or SQLite file path.")
    parser.add_argument("--host", help="Database host override.")
    parser.add_argument("--port", type=int, help="Database port override.")
    parser.add_argument("--user", help="Database user override.")
    parser.add_argument("--password", help="Database password override.")
    parser.add_argument("--pool-size", type=int, help="Connection pool size override.")
    parser.add_argument("--echo", action="store_true", help="Enable SQLAlchemy SQL echo logging.")
    parser.add_argument(
        "--require-postgresql-18",
        action="store_true",
        help="Require the connected database backend to be PostgreSQL major version 18.",
    )
    parser.add_argument("--output-json", help="Optional path for the JSON preflight report.")
    parser.add_argument("--output-md", help="Optional path for the Markdown preflight report.")
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
        raise ValueError("storage is not configured; pass --config, --dsn, or backend/database options")
    return settings


def run_preflight(
    *,
    expected_devices: Sequence[str],
    settings: StorageSettings,
    require_postgresql_18: bool = False,
    output_json: str | Path | None = None,
    output_md: str | Path | None = None,
) -> dict[str, object]:
    database = DatabaseManager(settings)
    try:
        report = build_v1_5_initialization_db_preflight(
            database,
            expected_devices=expected_devices,
            require_postgresql_major=18 if require_postgresql_18 else None,
        )
    finally:
        database.dispose()
    outputs = write_v1_5_initialization_db_preflight_report(
        report,
        output_json=output_json,
        output_md=output_md,
    )
    return {
        "report": report,
        "outputs": {key: str(Path(value).resolve()) for key, value in outputs.items()},
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        expected_devices = list(args.expected_device or []) + list(args.expected_sn_code or [])
        settings = _build_settings(args)
        payload = run_preflight(
            expected_devices=expected_devices,
            settings=settings,
            require_postgresql_18=bool(args.require_postgresql_18),
            output_json=args.output_json,
            output_md=args.output_md,
        )
    except Exception as exc:  # pragma: no cover - CLI failure path
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
