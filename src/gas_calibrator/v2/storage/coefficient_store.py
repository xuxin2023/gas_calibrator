from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import and_, func, or_, select, update

from .database import DatabaseManager
from .models import CoefficientVersionRecord, SensorRecord


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class CoefficientVersionStore:
    def __init__(self, database: DatabaseManager):
        self.database = database

    @staticmethod
    def _load_sensor(session, sensor_id: str | UUID) -> SensorRecord:
        record = session.execute(
            select(SensorRecord).where(SensorRecord.sensor_id == sensor_id)
        ).scalars().first()
        if record is None:
            raise ValueError(f"sensor not found: {sensor_id}")
        return record

    def _resolve_identity(
        self,
        session,
        *,
        sensor_id: str | UUID | None,
        analyzer_id: str | None,
        analyzer_serial: str | None,
    ) -> tuple[UUID | None, str, str | None]:
        if sensor_id is None:
            if not analyzer_id:
                raise ValueError("analyzer_id is required when sensor_id is not provided")
            return None, str(analyzer_id), analyzer_serial

        sensor = self._load_sensor(session, sensor_id)
        resolved_analyzer_id = str(analyzer_id or sensor.analyzer_id or "").strip()
        if not resolved_analyzer_id:
            raise ValueError(f"sensor {sensor_id} does not provide analyzer_id")
        resolved_analyzer_serial = analyzer_serial
        if resolved_analyzer_serial in (None, ""):
            resolved_analyzer_serial = sensor.analyzer_serial
        return sensor.sensor_id, resolved_analyzer_id, resolved_analyzer_serial

    def _lookup_clause(
        self,
        session,
        *,
        sensor_id: str | UUID | None,
        analyzer_id: str | None,
        analyzer_serial: str | None,
    ):
        resolved_sensor_id, resolved_analyzer_id, resolved_analyzer_serial = self._resolve_identity(
            session,
            sensor_id=sensor_id,
            analyzer_id=analyzer_id,
            analyzer_serial=analyzer_serial,
        )
        if resolved_sensor_id is not None:
            direct = CoefficientVersionRecord.sensor_id == resolved_sensor_id
            legacy = and_(
                CoefficientVersionRecord.sensor_id.is_(None),
                CoefficientVersionRecord.analyzer_id == resolved_analyzer_id,
            )
            if resolved_analyzer_serial not in (None, ""):
                legacy = and_(legacy, CoefficientVersionRecord.analyzer_serial == resolved_analyzer_serial)
            return (
                or_(direct, legacy),
                resolved_sensor_id,
                resolved_analyzer_id,
                resolved_analyzer_serial,
            )
        return (
            and_(
                CoefficientVersionRecord.analyzer_id == resolved_analyzer_id,
                CoefficientVersionRecord.analyzer_serial == resolved_analyzer_serial,
            ),
            None,
            resolved_analyzer_id,
            resolved_analyzer_serial,
        )

    def save_new_version(
        self,
        *,
        sensor_id: str | UUID | None = None,
        analyzer_id: str | None = None,
        analyzer_serial: str | None = None,
        coefficients: dict[str, Any],
        created_by: str | None = None,
        notes: str | None = None,
        approved: bool = False,
    ) -> CoefficientVersionRecord:
        with self.database.session_scope() as session:
            clause, resolved_sensor_id, resolved_analyzer_id, resolved_analyzer_serial = self._lookup_clause(
                session,
                sensor_id=sensor_id,
                analyzer_id=analyzer_id,
                analyzer_serial=analyzer_serial,
            )
            current_version = session.execute(
                select(func.max(CoefficientVersionRecord.version)).where(clause)
            ).scalar()
            record = CoefficientVersionRecord(
                id=uuid4(),
                sensor_id=resolved_sensor_id,
                analyzer_id=resolved_analyzer_id,
                analyzer_serial=resolved_analyzer_serial,
                version=int(current_version or 0) + 1,
                coefficients=dict(coefficients),
                created_at=_utc_now(),
                created_by=created_by,
                approved=approved,
                approved_by=created_by if approved else None,
                approved_at=_utc_now() if approved else None,
                deployed=False,
                deployed_at=None,
                notes=notes,
            )
            session.add(record)
            session.flush()
            return record

    def get_current_version(
        self,
        *,
        sensor_id: str | UUID | None = None,
        analyzer_id: str | None = None,
        analyzer_serial: str | None = None,
        approved_only: bool = False,
        deployed_only: bool = False,
    ) -> CoefficientVersionRecord | None:
        with self.database.session_scope() as session:
            clause, _, _, _ = self._lookup_clause(
                session,
                sensor_id=sensor_id,
                analyzer_id=analyzer_id,
                analyzer_serial=analyzer_serial,
            )
            stmt = (
                select(CoefficientVersionRecord)
                .where(clause)
                .order_by(CoefficientVersionRecord.version.desc())
            )
            if approved_only:
                stmt = stmt.where(CoefficientVersionRecord.approved.is_(True))
            if deployed_only:
                stmt = stmt.where(CoefficientVersionRecord.deployed.is_(True))
            return session.execute(stmt).scalars().first()

    def list_versions(
        self,
        *,
        sensor_id: str | UUID | None = None,
        analyzer_id: str | None = None,
        analyzer_serial: str | None = None,
        limit: int = 100,
    ) -> list[CoefficientVersionRecord]:
        with self.database.session_scope() as session:
            clause, _, _, _ = self._lookup_clause(
                session,
                sensor_id=sensor_id,
                analyzer_id=analyzer_id,
                analyzer_serial=analyzer_serial,
            )
            stmt = (
                select(CoefficientVersionRecord)
                .where(clause)
                .order_by(CoefficientVersionRecord.version.desc())
                .limit(limit)
            )
            return session.execute(stmt).scalars().all()

    def approve_version(self, version_id: str | UUID, *, approved_by: str) -> CoefficientVersionRecord:
        with self.database.session_scope() as session:
            record = session.get(CoefficientVersionRecord, version_id)
            if record is None:
                raise ValueError(f"coefficient version not found: {version_id}")
            record.approved = True
            record.approved_by = approved_by
            record.approved_at = _utc_now()
            session.flush()
            return record

    def deploy_version(self, version_id: str | UUID) -> CoefficientVersionRecord:
        with self.database.session_scope() as session:
            record = session.get(CoefficientVersionRecord, version_id)
            if record is None:
                raise ValueError(f"coefficient version not found: {version_id}")
            clause, _, _, _ = self._lookup_clause(
                session,
                sensor_id=record.sensor_id,
                analyzer_id=record.analyzer_id,
                analyzer_serial=record.analyzer_serial,
            )
            session.execute(
                update(CoefficientVersionRecord)
                .where(clause)
                .values(deployed=False, deployed_at=None)
            )
            record.deployed = True
            record.deployed_at = _utc_now()
            session.flush()
            return record

    def rollback_to_version(
        self,
        *,
        sensor_id: str | UUID | None = None,
        analyzer_id: str | None = None,
        analyzer_serial: str | None = None,
        version: int,
        created_by: str | None = None,
        notes: str | None = None,
    ) -> CoefficientVersionRecord:
        with self.database.session_scope() as session:
            clause, resolved_sensor_id, resolved_analyzer_id, resolved_analyzer_serial = self._lookup_clause(
                session,
                sensor_id=sensor_id,
                analyzer_id=analyzer_id,
                analyzer_serial=analyzer_serial,
            )
            source = session.execute(
                select(CoefficientVersionRecord).where(
                    clause,
                    CoefficientVersionRecord.version == version,
                )
            ).scalars().first()
            if source is None:
                raise ValueError(f"coefficient version not found: {version}")
            coefficients = dict(source.coefficients)

        rollback_notes = f"rollback_to={version}"
        if notes:
            rollback_notes = f"{rollback_notes}; {notes}"
        return self.save_new_version(
            sensor_id=resolved_sensor_id,
            analyzer_id=resolved_analyzer_id,
            analyzer_serial=resolved_analyzer_serial,
            coefficients=coefficients,
            created_by=created_by,
            notes=rollback_notes,
            approved=False,
        )
