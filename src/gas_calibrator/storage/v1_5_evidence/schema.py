"""PostgreSQL schema helpers for the V1.5 evidence registry.

The registry is a sidecar index for already-created V1.5 calibration evidence.
It never opens COM ports, controls routes, or writes analyzer coefficients.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import List


SCHEMA_NAME = "v1_5_evidence"
MIGRATION_VERSION = "001_v1_5_evidence_registry"


@dataclass(frozen=True)
class Migration:
    version: str
    path: Path
    sql: str
    checksum: str


def migrations_dir() -> Path:
    return Path(__file__).resolve().parent / "migrations"


def load_migrations() -> List[Migration]:
    migrations: List[Migration] = []
    for path in sorted(migrations_dir().glob("*.sql")):
        sql = path.read_text(encoding="utf-8")
        migrations.append(
            Migration(
                version=path.stem,
                path=path,
                sql=sql,
                checksum=hashlib.sha256(sql.encode("utf-8")).hexdigest(),
            )
        )
    return migrations


def load_latest_migration_sql() -> str:
    migrations = load_migrations()
    if not migrations:
        raise FileNotFoundError(f"No V1.5 evidence migrations under {migrations_dir()}")
    return migrations[-1].sql
