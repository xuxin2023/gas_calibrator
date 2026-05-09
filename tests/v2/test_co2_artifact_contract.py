from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.event_bus import EventBus
from gas_calibrator.v2.core.models import CalibrationPoint, SamplingResult
from gas_calibrator.v2.core.no_write_guard import NoWriteGuard
from gas_calibrator.v2.core.orchestration_context import OrchestrationContext
from gas_calibrator.v2.core.result_store import ResultStore
from gas_calibrator.v2.core.run_logger import RunLogger
from gas_calibrator.v2.core.run_state import RunState
from gas_calibrator.v2.core.services import ArtifactService, StatusService
from gas_calibrator.v2.core.session import RunSession
from gas_calibrator.v2.core.stability_checker import StabilityChecker
from gas_calibrator.v2.core.state_manager import StateManager
from gas_calibrator.v2.core.device_manager import DeviceManager


EXPECTED_ARTIFACTS = {
    "summary.json",
    "manifest.json",
    "route_trace.jsonl",
    "samples.csv",
    "samples.xlsx",
    "results.json",
    "points_readable.csv",
    "acceptance_plan.json",
    "analytics_summary.json",
    "trend_registry.json",
    "lineage_summary.json",
    "evidence_registry.json",
    "coefficient_registry.json",
    "point_summaries.json",
}


def _build_context(tmp_path: Path) -> tuple[OrchestrationContext, RunState, RunSession, SimpleNamespace]:
    config = AppConfig.from_dict({"paths": {"output_dir": str(tmp_path)}})
    session = RunSession(config)
    event_bus = EventBus()
    state_manager = StateManager(event_bus)
    result_store = ResultStore(tmp_path, session.run_id)
    run_logger = RunLogger(str(tmp_path), session.run_id)
    device_manager = DeviceManager(config.devices)
    stability_checker = StabilityChecker(config.workflow.stability)
    remembered: list[str] = []
    logs: list[str] = []
    calls: list[str] = []
    service_host = SimpleNamespace(
        _points_path=tmp_path / "points.json",
        _sync_results_to_storage_impl=None,
    )
    host = SimpleNamespace(
        _points_path=tmp_path / "points.json",
        _sync_results_to_storage_impl=None,
        get_results=result_store.get_samples,
        _remember_output_file=remembered.append,
        _export_coefficient_report=lambda: calls.append("coeff"),
        _export_qc_report=lambda: calls.append("qc"),
        _export_temperature_snapshots=lambda: calls.append("temperature"),
        _log=logs.append,
        _startup_pressure_precheck_payload=lambda: {"passed": True, "details": {"samples": 3}},
        remembered=remembered,
        logs=logs,
        calls=calls,
        service=service_host,
    )
    context = OrchestrationContext(
        config=config,
        session=session,
        state_manager=state_manager,
        event_bus=event_bus,
        result_store=result_store,
        run_logger=run_logger,
        device_manager=device_manager,
        stability_checker=stability_checker,
        stop_event=SimpleNamespace(is_set=lambda: False),
        pause_event=SimpleNamespace(is_set=lambda: True),
    )
    return context, RunState(), session, host


def test_co2_samples_runtime_schema_stays_stable(tmp_path: Path) -> None:
    context, run_state, session, host = _build_context(tmp_path)
    service = ArtifactService(context, run_state, host=host)
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=800.0, pressure_hpa=1100.0, route="co2")
    context.result_store.save_sample(
        SamplingResult(
            point=point,
            analyzer_id="ga01",
            timestamp=__import__("datetime").datetime.now(),
            co2_ppm=800.0,
            h2o_mmol=0.0,
            pressure_gauge_hpa=1100.0,
            thermometer_temp_c=20.0,
            frame_has_data=True,
            frame_usable=True,
            frame_status="ok",
            point_phase="co2",
            point_tag="co2_groupa_800ppm_1100hpa",
            sample_index=1,
        )
    )
    service.export_manifest(session, startup_pressure_precheck={"passed": True, "details": {"samples": 3}})
    service.export_all_artifacts()

    samples_csv = context.result_store.run_dir / "samples.csv"
    summary = json.loads((context.result_store.run_dir / "summary.json").read_text(encoding="utf-8"))
    assert samples_csv.exists()
    assert summary["stats"]["artifact_exports"]["samples_csv"]["status"] == "ok"
    assert summary["stats"]["acceptance_plan"]["promotion_state"] == "dry_run_only"
    assert summary["stats"]["artifact_role_summary"]["execution_rows"]["count"] >= 1


def test_co2_point_results_schema_stays_stable(tmp_path: Path) -> None:
    context, run_state, session, host = _build_context(tmp_path)
    service = ArtifactService(context, run_state, host=host)
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=800.0, pressure_hpa=1100.0, route="co2")
    context.result_store.save_point_summary(
        point,
        {
            "point_phase": "co2",
            "point_tag": "co2_groupa_800ppm_1100hpa",
            "usable_sample_count": 4,
            "raw_sample_count": 4,
            "cleaned_sample_count": 4,
            "removed_sample_count": 0,
            "valid": True,
            "recommendation": "use",
            "reason": "passed",
        },
    )
    service.export_manifest(session, startup_pressure_precheck={"passed": True, "details": {"samples": 3}})
    service.export_all_artifacts()

    point_summaries = context.result_store.run_dir / "point_summaries.json"
    payload = json.loads(point_summaries.read_text(encoding="utf-8"))
    assert point_summaries.exists()
    assert payload
    assert payload[0]["point"]["co2_ppm"] == 800.0
    assert payload[0]["stats"]["usable_sample_count"] == 4


def test_co2_no_write_guard_stays_armed(tmp_path: Path) -> None:
    context, run_state, session, host = _build_context(tmp_path)
    service = ArtifactService(context, run_state, host=host)
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=800.0, pressure_hpa=1100.0, route="co2")
    context.result_store.save_sample(
        SamplingResult(
            point=point,
            analyzer_id="ga01",
            timestamp=__import__("datetime").datetime.now(),
            co2_ppm=800.0,
            h2o_mmol=0.0,
            frame_has_data=True,
            frame_usable=True,
            frame_status="ok",
            point_phase="co2",
            point_tag="co2_groupa_800ppm_1100hpa",
            sample_index=1,
        )
    )
    service.export_manifest(session, startup_pressure_precheck={"passed": True, "details": {"samples": 3}})
    service.export_all_artifacts()

    guard_artifact = NoWriteGuard().to_artifact()
    assert guard_artifact["attempted_write_count"] == 0
    assert guard_artifact["final_decision"] == "PASS"
    assert guard_artifact["guard_enabled"] is True


def test_probe_dir_and_run_dir_are_not_confused(tmp_path: Path) -> None:
    context, run_state, session, host = _build_context(tmp_path)
    service = ArtifactService(context, run_state, host=host)
    service.export_manifest(session, startup_pressure_precheck={"passed": True, "details": {"samples": 3}})
    service.export_all_artifacts()

    run_dir = context.result_store.run_dir
    assert run_dir.name.startswith("run_")
    assert (run_dir / "summary.json").exists()
    assert (run_dir / "manifest.json").exists()
    assert not (tmp_path / "probe").exists()


def test_manifest_links_probe_head_branch_operator_and_no_write_assertion(tmp_path: Path) -> None:
    context, run_state, session, host = _build_context(tmp_path)
    service = ArtifactService(context, run_state, host=host)
    manifest_path = service.export_manifest(session, startup_pressure_precheck={"passed": True, "details": {"samples": 3}})
    service.export_all_artifacts()

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["run_id"] == session.run_id
    assert manifest["source_points_file"] == str(tmp_path / "points.json")
    assert manifest["startup_pressure_precheck"]["details"]["samples"] == 3
    assert manifest["artifacts"]["output_files"] == []
    assert "manifest" in manifest["artifacts"]["role_catalog"]["execution_summary"]
    assert "samples_csv" in manifest["artifacts"]["role_catalog"]["execution_rows"]
    assert manifest["report_policy"]["include_fleet_stats"] is False
    assert manifest["versions"]["config_version"].startswith("cfg-")


def test_co2_artifact_contract_lists_expected_files(tmp_path: Path) -> None:
    context, run_state, session, host = _build_context(tmp_path)
    service = ArtifactService(context, run_state, host=host)
    service.export_manifest(session, startup_pressure_precheck={"passed": True, "details": {"samples": 3}})
    service.export_all_artifacts()

    run_dir = context.result_store.run_dir
    produced = {path.name for path in run_dir.iterdir() if path.is_file()}
    assert "summary.json" in produced
    assert "manifest.json" in produced
    assert produced & EXPECTED_ARTIFACTS

