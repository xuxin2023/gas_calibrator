"""SQLAlchemy-based indexer for run artifacts.
Walks a completed run output directory, reads summary.json,
and indexes all artifacts into run_index / artifact_index / coefficient_version.
Idempotent: skips artifacts already stored with matching hash.
"""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import select

SCHEMA_VERSION = 2


def _utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


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


def _ensure_schema_via_engine(manager: Any) -> None:
    from .models import Base
    Base.metadata.create_all(manager.engine, checkfirst=True)


def _index_analyzer_registry(
    session: Any,
    run_id: str,
    timestamp: str,
    analyzer_sns: list[str],
) -> int:
    from .models import RunAnalyzerRegistry

    updated = 0
    for sn in analyzer_sns:
        if not sn:
            continue
        existing = session.get(RunAnalyzerRegistry, sn)
        if existing is not None:
            existing.last_seen_time = timestamp
        else:
            entry = RunAnalyzerRegistry(
                analyzer_sn=sn,
                first_seen_run_id=run_id,
                first_seen_time=timestamp,
                last_seen_time=timestamp,
                model="",
                notes="",
            )
            session.add(entry)
        updated += 1
    return updated


def index_run(output_dir: str, *, db_path: str = "") -> dict[str, Any]:
    if not output_dir or not Path(output_dir).is_dir():
        return {"ok": False, "error": f"output_dir not found: {output_dir}"}

    summary = _read_json(Path(output_dir) / "summary.json")
    if summary is None:
        return {"ok": False, "error": "summary.json not found in output_dir"}

    from .database import get_engine_manager
    from .models import RunIndexRecord, ArtifactIndexRecord

    manager = get_engine_manager()
    _ensure_schema_via_engine(manager)

    with manager.session_scope() as session:

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
        registry_updated = _index_analyzer_registry(session, run_id, timestamp, analyzer_sns)

        stmt = select(RunIndexRecord).where(RunIndexRecord.run_id == run_id)
        existing_run = session.execute(stmt).scalar_one_or_none()

        if existing_run is not None:
            existing_run.timestamp = timestamp
            existing_run.final_decision = final_decision
            existing_run.pressure_points_completed = points_completed
            existing_run.sample_count_total = sample_count
            existing_run.attempted_write_count = write_count
            existing_run.analyzer_sn = analyzer_sn_text
            existing_run.config_path = config_path
            existing_run.output_dir = str(Path(output_dir).resolve())
            existing_run.created_at = created_at
        else:
            run_entry = RunIndexRecord(
                run_id=run_id,
                timestamp=timestamp,
                branch="",
                head="",
                final_decision=final_decision,
                pressure_points_completed=points_completed,
                sample_count_total=sample_count,
                attempted_write_count=write_count,
                analyzer_sn=analyzer_sn_text,
                config_path=config_path,
                output_dir=str(Path(output_dir).resolve()),
                created_at=created_at,
            )
            session.add(run_entry)

        session.flush()

        artifacts = _collect_artifacts(output_dir)
        inserted = 0
        skipped = 0
        for art in artifacts:
            stmt = (
                select(ArtifactIndexRecord)
                .where(
                    ArtifactIndexRecord.run_id == run_id,
                    ArtifactIndexRecord.artifact_type == art["artifact_type"],
                    ArtifactIndexRecord.file_hash == art["file_hash"],
                )
            )
            existing = session.execute(stmt).scalar_one_or_none()
            if existing is not None:
                skipped += 1
                continue
            entry = ArtifactIndexRecord(
                artifact_id=f"{run_id}:{art['artifact_type']}",
                run_id=run_id,
                artifact_type=art["artifact_type"],
                file_path=art["file_path"],
                file_hash=art["file_hash"],
                created_at=created_at,
            )
            session.add(entry)
            inserted += 1

    return {
        "ok": True,
        "run_id": run_id,
        "analyzer_sns": analyzer_sns,
        "registry_updated": registry_updated,
        "artifacts_inserted": inserted,
        "artifacts_skipped": skipped,
        "artifacts_total": len(artifacts),
    }
