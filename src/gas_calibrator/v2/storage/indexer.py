"""SQLite3 indexer for run artifacts (stdlib only, zero external deps).
Walks a completed run output directory, reads summary.json,
and indexes all artifacts into run_index / artifact_index / coefficient_version.
Idempotent: skips artifacts already stored with matching hash.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

SCHEMA_VERSION = 2

DDL = """
CREATE TABLE IF NOT EXISTS run_index (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL UNIQUE,
    timestamp TEXT NOT NULL,
    branch TEXT DEFAULT '',
    head TEXT DEFAULT '',
    final_decision TEXT DEFAULT '',
    pressure_points_completed INTEGER DEFAULT 0,
    sample_count_total INTEGER DEFAULT 0,
    attempted_write_count INTEGER DEFAULT 0,
    analyzer_sn TEXT DEFAULT '',
    config_path TEXT DEFAULT '',
    output_dir TEXT DEFAULT '',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS artifact_index (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    artifact_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    artifact_type TEXT NOT NULL,
    file_path TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(run_id, artifact_type, file_hash)
);

CREATE TABLE IF NOT EXISTS coefficient_version (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    analyzer_id TEXT NOT NULL,
    analyzer_sn TEXT DEFAULT '',
    coefficient_value REAL,
    written_to_device INTEGER DEFAULT 0,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS analyzer_registry (
    analyzer_sn TEXT PRIMARY KEY,
    first_seen_run_id TEXT NOT NULL,
    first_seen_time TEXT NOT NULL,
    last_seen_time TEXT NOT NULL,
    model TEXT DEFAULT '',
    notes TEXT DEFAULT ''
);

CREATE INDEX IF NOT EXISTS ix_artifact_run_id ON artifact_index(run_id);
CREATE INDEX IF NOT EXISTS ix_coefficient_run_id ON coefficient_version(run_id);
CREATE INDEX IF NOT EXISTS ix_coefficient_analyzer_sn ON coefficient_version(analyzer_sn);
"""


def _utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)
    conn.commit()


def _read_json(path: Path) -> Optional[dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return None


def _collect_artifacts(output_dir: str) -> list[dict[str, str]]:
    base = Path(output_dir).resolve()
    artifacts: list[dict[str, str]] = []

    type_map = {
        "summary.json": "summary",
        "manifest.json": "manifest",
        "route_trace.jsonl": "route_trace",
        "samples.csv": "samples_csv",
        "calibration_report.json": "calibration_report",
        "diagnostic_analysis.json": "diagnostic_analysis",
        "formal_analysis.json": "formal_analysis",
        "execution_rows.csv": "execution_rows",
        "execution_summary.json": "execution_summary",
        "verification_digest.json": "verification_digest",
        "verification_rollup.json": "verification_rollup",
    }

    for fpath in sorted(base.rglob("*")):
        if fpath.is_dir():
            continue
        relative = str(fpath.relative_to(base))
        fname = fpath.name.lower()
        try:
            file_hash = _sha256_hex(fpath.read_bytes())
        except Exception:
            file_hash = ""
        artifact_type = type_map.get(fname, f"file:{fpath.suffix.lstrip('.')}")
        artifacts.append({
            "artifact_type": artifact_type,
            "file_path": relative,
            "file_hash": file_hash,
        })
    return artifacts


def _discover_analyzer_sns(output_dir: str) -> list[str]:
    base = Path(output_dir)
    csv_path = base / "samples_runtime.csv"
    if not csv_path.exists():
        csv_path = base / "samples.csv"
    if not csv_path.exists():
        return []

    sns: list[str] = []
    try:
        with open(csv_path, "r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            if reader.fieldnames:
                for col in reader.fieldnames:
                    if col.endswith("_serial") and col.lower().startswith(("ga", "analyzer_")):
                        prefix = col.rsplit("_serial", 1)[0]
                        try:
                            first_row = next(reader)
                        except StopIteration:
                            break
                        sn_val = (first_row.get(col) or "").strip()
                        if sn_val:
                            sns.append(sn_val)
    except Exception:
        pass
    return sns


def _index_analyzer_registry(
    conn: sqlite3.Connection,
    run_id: str,
    timestamp: str,
    analyzer_sns: list[str],
) -> int:
    updated = 0
    for sn in analyzer_sns:
        if not sn:
            continue
        existing = conn.execute(
            "SELECT analyzer_sn FROM analyzer_registry WHERE analyzer_sn=?",
            (sn,),
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE analyzer_registry SET last_seen_time=? WHERE analyzer_sn=?",
                (timestamp, sn),
            )
        else:
            conn.execute(
                "INSERT INTO analyzer_registry "
                "(analyzer_sn, first_seen_run_id, first_seen_time, last_seen_time, model, notes) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (sn, run_id, timestamp, timestamp, "", ""),
            )
        updated += 1
    return updated


def index_run(output_dir: str, *, db_path: str = "gas_calibrator_index.db") -> dict[str, Any]:
    if not output_dir or not Path(output_dir).is_dir():
        return {"ok": False, "error": f"output_dir not found: {output_dir}"}

    summary = _read_json(Path(output_dir) / "summary.json")
    if summary is None:
        return {"ok": False, "error": "summary.json not found in output_dir"}

    conn = sqlite3.connect(db_path)
    _ensure_schema(conn)

    run_id = str(summary.get("run_id") or Path(output_dir).name)
    timestamp = str(summary.get("started_at") or _utc_now_text())
    stats = summary.get("stats", {})
    status = summary.get("status", {})
    final_decision = str(status.get("phase") or "")

    points_completed = int(stats.get("completed_points", stats.get("points_completed", 0)))
    sample_count = int(stats.get("sample_count", 0))
    write_count = 0
    config_path = str(summary.get("config_path", ""))
    created_at = _utc_now_text()

    analyzer_sns = _discover_analyzer_sns(output_dir)
    analyzer_sn_text = ";".join(analyzer_sns)
    registry_updated = _index_analyzer_registry(conn, run_id, timestamp, analyzer_sns)

    conn.execute(
        """INSERT OR REPLACE INTO run_index
           (run_id, timestamp, branch, head, final_decision,
            pressure_points_completed, sample_count_total, attempted_write_count,
            analyzer_sn, config_path, output_dir, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (run_id, timestamp, "", "", final_decision,
         points_completed, sample_count, write_count,
         analyzer_sn_text, config_path, str(Path(output_dir).resolve()), created_at),
    )

    artifacts = _collect_artifacts(output_dir)
    inserted = 0
    skipped = 0
    for art in artifacts:
        existing = conn.execute(
            "SELECT id FROM artifact_index WHERE run_id=? AND artifact_type=? AND file_hash=?",
            (run_id, art["artifact_type"], art["file_hash"]),
        ).fetchone()
        if existing:
            skipped += 1
            continue
        conn.execute(
            """INSERT INTO artifact_index
               (artifact_id, run_id, artifact_type, file_path, file_hash, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (f"{run_id}:{art['artifact_type']}", run_id,
             art["artifact_type"], art["file_path"], art["file_hash"], created_at),
        )
        inserted += 1

    conn.commit()
    conn.close()

    return {
        "ok": True,
        "run_id": run_id,
        "analyzer_sns": analyzer_sns,
        "registry_updated": registry_updated,
        "artifacts_inserted": inserted,
        "artifacts_skipped": skipped,
        "artifacts_total": len(artifacts),
        "db_path": str(Path(db_path).resolve()),
    }
