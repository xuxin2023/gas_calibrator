from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from types import SimpleNamespace
import threading

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.device_manager import DeviceManager
from gas_calibrator.v2.core.event_bus import EventBus
from gas_calibrator.v2.core.models import CalibrationPoint, SamplingResult
from gas_calibrator.v2.core.orchestration_context import OrchestrationContext
from gas_calibrator.v2.core.result_store import ResultStore
from gas_calibrator.v2.core.run_logger import RunLogger
from gas_calibrator.v2.core.run_state import RunState
from gas_calibrator.v2.core.services import ArtifactService
from gas_calibrator.v2.core.session import RunSession
from gas_calibrator.v2.core.stability_checker import StabilityChecker
from gas_calibrator.v2.core.state_manager import StateManager


def _build_service(tmp_path: Path, *, with_storage: bool = False) -> tuple[ArtifactService, OrchestrationContext, RunState, SimpleNamespace, RunSession]:
    payload: dict[str, object] = {"paths": {"output_dir": str(tmp_path)}}
    if with_storage:
        payload["storage"] = {"backend": "sqlite", "database": str(tmp_path / "storage.sqlite")}
    config = AppConfig.from_dict(payload)
    session = RunSession(config)
    event_bus = EventBus()
    state_manager = StateManager(event_bus)
    result_store = ResultStore(tmp_path, session.run_id)
    run_logger = RunLogger(str(tmp_path), session.run_id)
    device_manager = DeviceManager(config.devices)
    stability_checker = StabilityChecker(config.workflow.stability)
    stop_event = threading.Event()
    pause_event = threading.Event()
    pause_event.set()
    context = OrchestrationContext(
        config=config,
        session=session,
        state_manager=state_manager,
        event_bus=event_bus,
        result_store=result_store,
        run_logger=run_logger,
        device_manager=device_manager,
        stability_checker=stability_checker,
        stop_event=stop_event,
        pause_event=pause_event,
    )
    run_state = RunState()
    remembered: list[str] = []
    logs: list[str] = []
    calls: list[str] = []
    service_host = SimpleNamespace(
        _points_path=tmp_path / "points.json",
        _sync_results_to_storage_impl=None,
    )
    host = SimpleNamespace(
        get_results=result_store.get_samples,
        _remember_output_file=remembered.append,
        _export_coefficient_report=lambda: calls.append("coeff"),
        _export_qc_report=lambda: calls.append("qc"),
        _export_temperature_snapshots=lambda: calls.append("temperature"),
        _log=logs.append,
        _startup_pressure_precheck_payload=lambda: {
            "passed": True,
            "route": "co2",
            "warning_count": 0,
            "error_count": 0,
            "details": {"samples": 3},
        },
        remembered=remembered,
        logs=logs,
        calls=calls,
        service=service_host,
    )
    return ArtifactService(context, run_state, host=host), context, run_state, host, session


def test_artifact_service_exports_summary_manifest_and_run_artifacts(tmp_path: Path) -> None:
    service, context, _, host, session = _build_service(tmp_path)
    point = CalibrationPoint(index=1, temperature_c=25.0, co2_ppm=400.0, pressure_hpa=1000.0, route="co2")
    context.result_store.save_sample(
        SamplingResult(
            point=point,
            analyzer_id="ga01",
            timestamp=datetime.now(),
            co2_ppm=401.0,
            h2o_mmol=9.0,
            pressure_gauge_hpa=998.0,
            thermometer_temp_c=25.2,
            frame_has_data=True,
            frame_usable=True,
            frame_status="ok",
            point_phase="co2",
            point_tag="co2_1",
            sample_index=1,
        )
    )
    context.result_store.save_point_summary(
        point,
        {
            "point_phase": "co2",
            "point_tag": "co2_1",
            "usable_sample_count": 1,
            "raw_sample_count": 1,
            "cleaned_sample_count": 1,
            "removed_sample_count": 0,
            "valid": True,
            "recommendation": "use",
            "reason": "passed",
        },
    )
    session.start()
    session.end("")

    manifest_path = service.export_manifest(session, startup_pressure_precheck=host._startup_pressure_precheck_payload())
    service.export_all_artifacts()

    summary_path = context.result_store.data_writer.summary_path
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_path.exists()
    assert summary_path.exists()
    assert (context.result_store.run_dir / "samples.csv").exists()
    assert (context.result_store.run_dir / "samples.xlsx").exists()
    assert (context.result_store.run_dir / "results.json").exists()
    assert (context.result_store.run_dir / "points_readable.csv").exists()
    assert (context.result_store.run_dir / "acceptance_plan.json").exists()
    assert (context.result_store.run_dir / "analytics_summary.json").exists()
    assert (context.result_store.run_dir / "trend_registry.json").exists()
    assert (context.result_store.run_dir / "lineage_summary.json").exists()
    assert (context.result_store.run_dir / "evidence_registry.json").exists()
    assert (context.result_store.run_dir / "coefficient_registry.json").exists()
    assert summary["startup_pressure_precheck"]["passed"] is True
    assert summary["stats"]["startup_pressure_precheck"]["route"] == "co2"
    assert summary["reporting"]["include_fleet_stats"] is False
    assert summary["stats"]["artifact_exports"]["samples_csv"]["status"] == "ok"
    assert summary["stats"]["artifact_exports"]["points_readable"]["status"] == "ok"
    assert summary["stats"]["artifact_exports"]["points_readable"]["role"] == "execution_summary"
    assert summary["stats"]["artifact_exports"]["run_summary"]["status"] == "ok"
    assert summary["stats"]["artifact_exports"]["acceptance_plan"]["status"] == "ok"
    assert summary["stats"]["artifact_exports"]["analytics_summary"]["status"] == "ok"
    assert summary["stats"]["acceptance_readiness_summary"]["ready_for_promotion"] is False
    assert summary["stats"]["acceptance_plan"]["promotion_state"] == "dry_run_only"
    assert summary["stats"]["analytics_summary_digest"]["summary"]
    assert summary["stats"]["lineage_summary"]["config_version"].startswith("cfg-")
    assert summary["stats"]["artifact_role_summary"]["execution_summary"]["count"] >= 2
    assert manifest["startup_pressure_precheck"]["details"]["samples"] == 3
    assert manifest["report_policy"]["include_fleet_stats"] is False
    assert "points_readable" in manifest["artifacts"]["role_catalog"]["execution_summary"]
    assert manifest["evidence_governance"]["promotion_state"] == "dry_run_only"
    assert manifest["versions"]["config_version"].startswith("cfg-")
    assert host.calls == ["coeff", "qc", "temperature"]
    assert str(manifest_path) in host.remembered
    assert str(summary_path) in host.remembered
    assert context.run_logger._samples_file.closed is True


def test_artifact_service_storage_sync_warns_without_raising(tmp_path: Path) -> None:
    service, _, _, host, _ = _build_service(tmp_path, with_storage=True)

    def fail_sync() -> None:
        raise RuntimeError("database unavailable")

    host.service._sync_results_to_storage_impl = fail_sync

    service.sync_results_to_storage()

    assert any("Storage warning: database unavailable" in message for message in host.logs)


def test_artifact_service_skips_formal_calibration_report_when_not_auto_mode(tmp_path: Path) -> None:
    service, context, _, host, session = _build_service(tmp_path)
    context.config.workflow.run_mode = "co2_measurement"
    session.config.workflow.run_mode = "co2_measurement"
    point = CalibrationPoint(index=1, temperature_c=25.0, co2_ppm=400.0, pressure_hpa=1000.0, route="co2")
    context.result_store.save_sample(
        SamplingResult(
            point=point,
            analyzer_id="ga01",
            timestamp=datetime.now(),
            co2_ppm=401.0,
            h2o_mmol=9.0,
        )
    )

    manifest_path = service.export_manifest(session)
    service.export_all_artifacts()

    assert "coeff" not in host.calls
    assert host.calls == ["qc", "temperature"]
    assert any("Formal calibration report skipped: run_mode=co2_measurement" in message for message in host.logs)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["run_mode"] == "co2_measurement"
    assert manifest["report_policy"]["formal_calibration_report"] is False
    summary = json.loads(context.result_store.data_writer.summary_path.read_text(encoding="utf-8"))
    assert summary["stats"]["artifact_exports"]["coefficient_report"]["status"] == "skipped"


def test_artifact_service_host_export_supports_explicit_missing_status(tmp_path: Path) -> None:
    service, _, run_state, _, _ = _build_service(tmp_path)

    service._run_host_export(
        "future_report",
        role="formal_analysis",
        callback=lambda: {"status": "missing", "path": "", "error": "not generated"},
    )

    assert run_state.artifacts.export_statuses["future_report"]["status"] == "missing"
    assert run_state.artifacts.export_statuses["future_report"]["error"] == "not generated"
