from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import event

from gas_calibrator.storage.coefficient_store import CoefficientVersionStore
from gas_calibrator.storage.database import DatabaseManager, StorageSettings
from gas_calibrator.storage.models import SensorRecord


def _database(tmp_path: Path) -> DatabaseManager:
    database = DatabaseManager(
        StorageSettings(
            backend="sqlite",
            database=str(tmp_path / "coefficient_store.sqlite"),
        )
    )
    database.initialize()
    return database


def _sensor_id(database: DatabaseManager) -> str:
    with database.session_scope() as session:
        sensor = SensorRecord(
            device_key="ga01-main",
            analyzer_id="ga01",
            analyzer_serial="SN01",
            software_version="v5_plus",
            model="GA",
            channel_type="co2_h2o_dual",
            metadata_json={},
        )
        session.add(sensor)
        session.flush()
        return str(sensor.sensor_id)


def test_shared_store_preserves_version_approval_deployment_and_rollback(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    try:
        sensor_id = _sensor_id(database)
        store = CoefficientVersionStore(database)

        baseline = store.save_new_version(
            sensor_id=sensor_id,
            coefficients={"slope": 1.0},
            created_by="alice",
            approved=True,
        )
        candidate = store.save_new_version(
            sensor_id=sensor_id,
            coefficients={"slope": 1.2},
            created_by="bob",
        )
        store.approve_version(candidate.id, approved_by="lead")
        store.deploy_version(candidate.id)
        rollback = store.rollback_to_version(
            sensor_id=sensor_id,
            version=baseline.version,
            created_by="ops",
            notes="restore baseline",
        )

        current = store.get_current_version(sensor_id=sensor_id, deployed_only=True)
        history = store.list_versions(sensor_id=sensor_id)

        assert current is not None and current.id == candidate.id
        assert [record.version for record in history] == [3, 2, 1]
        assert rollback.approved is False
        assert rollback.deployed is False
        assert rollback.coefficients == {"slope": 1.0}
        assert rollback.notes == "rollback_to=1; restore baseline"
    finally:
        database.dispose()


def test_deployment_marker_update_rolls_back_as_one_transaction(tmp_path: Path) -> None:
    database = _database(tmp_path)
    try:
        sensor_id = _sensor_id(database)
        store = CoefficientVersionStore(database)
        baseline = store.save_new_version(
            sensor_id=sensor_id,
            coefficients={"slope": 1.0},
            created_by="alice",
            approved=True,
        )
        candidate = store.save_new_version(
            sensor_id=sensor_id,
            coefficients={"slope": 1.2},
            created_by="bob",
            approved=True,
        )
        store.deploy_version(baseline.id)

        def fail_coefficient_update(
            _connection,
            _cursor,
            statement,
            _parameters,
            _context,
            _executemany,
        ) -> None:
            if statement.lstrip().upper().startswith("UPDATE COEFFICIENT_VERSIONS"):
                raise RuntimeError("forced deployment metadata failure")

        event.listen(database.engine, "before_cursor_execute", fail_coefficient_update)
        try:
            with pytest.raises(RuntimeError, match="forced deployment metadata failure"):
                store.deploy_version(candidate.id)
        finally:
            event.remove(database.engine, "before_cursor_execute", fail_coefficient_update)

        current = store.get_current_version(sensor_id=sensor_id, deployed_only=True)
        candidate_after = store.get_current_version(sensor_id=sensor_id)

        assert current is not None and current.id == baseline.id
        assert candidate_after is not None and candidate_after.id == candidate.id
        assert candidate_after.deployed is False
    finally:
        database.dispose()
