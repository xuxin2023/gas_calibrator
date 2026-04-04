from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Optional

from ..config import AppConfig
from ..core.csv_resilience import load_csv_rows
from ..core.data_writer import DataWriter
from ..core.device_factory import DeviceFactory
from ..core.device_manager import DeviceManager
from ..core.event_bus import EventBus
from ..core.models import CalibrationPoint, SamplingResult
from ..core.orchestration_context import OrchestrationContext
from ..core.result_store import ResultStore
from ..core.run_logger import RunLogger
from ..core.run_state import RunState
from ..core.services import ArtifactService
from ..core.session import RunSession
from ..core.stability_checker import StabilityChecker
from ..core.state_manager import StateManager
from ..ui_v2.i18n import display_acceptance_value, display_compare_status, display_evidence_source, display_risk_level, t


def build_export_resilience_report(*, report_root: Path, run_name: Optional[str] = None) -> dict[str, Any]:
    report_dir = Path(report_root) / str(run_name or "export_resilience")
    report_dir.mkdir(parents=True, exist_ok=True)
    cases = [
        _dynamic_header_case(report_dir / "dynamic_header_expansion"),
        _points_readable_case(report_dir / "points_readable_execution_summary"),
        _artifact_failure_isolation_case(report_dir / "export_failure_isolation"),
    ]
    status = "MATCH" if all(case["status"] == "MATCH" for case in cases) else "MISMATCH"
    report = {
        "tool": "export_resilience",
        "status": status,
        "evidence_source": "diagnostic",
        "evidence_state": "collected",
        "acceptance_level": "offline_regression",
        "not_real_acceptance_evidence": True,
        "risk_level": "low" if status == "MATCH" else "medium",
        "cases": cases,
    }
    json_path = report_dir / "export_resilience_report.json"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_path = report_dir / "export_resilience_report.md"
    lines = [
        f"# {t('resilience.title')}",
        "",
        f"- {t('resilience.status')}: {display_compare_status(status, default=str(status))}",
        f"- {t('resilience.evidence_source')}: {display_evidence_source(report['evidence_source'], default=str(report['evidence_source']))}",
        f"- {t('resilience.acceptance_level')}: {display_acceptance_value(report['acceptance_level'], default=str(report['acceptance_level']))}",
        f"- {t('resilience.risk_level')}: {display_risk_level(report['risk_level'], default=str(report['risk_level']))}",
        "",
    ]
    for case in cases:
        lines.append(f"## {case['name']}")
        lines.append(
            f"- {t('resilience.status')}: {display_compare_status(case['status'], default=str(case['status']))}"
        )
        lines.append(f"- {t('resilience.details')}: {json.dumps(case.get('details', {}), ensure_ascii=False)}")
        lines.append("")
    markdown_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    return {
        "status": status,
        "report_dir": str(report_dir),
        "report_json": str(json_path),
        "report_markdown": str(markdown_path),
        "report": report,
    }


def _dynamic_header_case(case_dir: Path) -> dict[str, Any]:
    logger = RunLogger(str(case_dir), "run_logger_case")
    try:
        logger.log_sample({"timestamp": "2026-03-26T10:00:00", "point_index": 1, "route": "co2"})
        logger.log_sample(
            {
                "timestamp": "2026-03-26T10:00:01",
                "point_index": 1,
                "route": "co2",
                "pressure_gauge_hpa": 998.0,
                "thermometer_temp_c": 25.0,
                "frame_has_data": True,
                "frame_usable": True,
                "frame_status": "ok",
                "sample_index": 2,
            }
        )
        point = CalibrationPoint(index=1, temperature_c=25.0, co2_ppm=400.0, pressure_hpa=1000.0, route="co2")
        logger.log_point(point, "done", point_tag="co2_1", extra_fields={"points_readable": True, "analyzer_summary": "ready"})
    finally:
        logger.finalize()

    header, rows = load_csv_rows(logger.samples_path)
    point_header, point_rows = load_csv_rows(logger.points_path)
    writer = DataWriter(str(case_dir), "writer_case")
    writer.write_samples(
        [
            SamplingResult(
                point=point,
                analyzer_id="ga01",
                timestamp=datetime(2026, 3, 26, 10, 0, 2, tzinfo=timezone.utc),
                co2_ppm=401.0,
                pressure_hpa=1000.0,
                pressure_gauge_hpa=999.0,
                thermometer_temp_c=25.1,
                frame_has_data=True,
                frame_usable=True,
                frame_status="ok",
                sample_index=1,
            )
        ]
    )
    written_header, written_rows = load_csv_rows(writer.samples_csv_path)
    ok = (
        "pressure_gauge_hpa" in header
        and "thermometer_temp_c" in header
        and "frame_status" in header
        and len(rows) == 2
        and "analyzer_summary" in point_header
        and len(point_rows) == 1
        and "pressure_gauge_hpa" in written_header
        and len(written_rows) == 1
    )
    return {
        "name": "dynamic_header_expansion",
        "status": "MATCH" if ok else "MISMATCH",
        "details": {
            "samples_header": header,
            "point_header": point_header,
            "writer_header": written_header,
            "samples_row_count": len(rows),
            "point_row_count": len(point_rows),
            "writer_row_count": len(written_rows),
        },
    }


def _artifact_failure_isolation_case(case_dir: Path) -> dict[str, Any]:
    service, context, run_state, host, session = _build_artifact_service(case_dir)
    point = CalibrationPoint(index=1, temperature_c=25.0, co2_ppm=400.0, pressure_hpa=1000.0, route="co2")
    context.result_store.save_sample(
        SamplingResult(
            point=point,
            analyzer_id="ga01",
            timestamp=datetime.now(timezone.utc),
            co2_ppm=401.0,
            h2o_mmol=9.0,
        )
    )
    session.start()
    session.end("")
    service.export_all_artifacts()
    summary_path = context.result_store.data_writer.summary_path
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    export_statuses = dict(summary.get("stats", {}).get("artifact_exports", {}) or {})
    ok = (
        summary_path.exists()
        and export_statuses.get("qc_report", {}).get("status") == "error"
        and export_statuses.get("samples_csv", {}).get("status") == "ok"
        and export_statuses.get("run_summary", {}).get("status") == "ok"
        and any("qc_report export failed" in message for message in host.logs)
    )
    return {
        "name": "export_failure_isolation",
        "status": "MATCH" if ok else "MISMATCH",
        "details": {
            "summary_path": str(summary_path),
            "artifact_exports": export_statuses,
            "remembered_files": list(host.remembered),
            "logs": list(host.logs),
        },
    }


def _points_readable_case(case_dir: Path) -> dict[str, Any]:
    service, context, run_state, host, session = _build_artifact_service(case_dir)
    point = CalibrationPoint(index=1, temperature_c=25.0, co2_ppm=400.0, pressure_hpa=1000.0, route="co2")
    sample = SamplingResult(
        point=point,
        analyzer_id="GA01",
        timestamp=datetime.now(timezone.utc),
        co2_ppm=401.0,
        h2o_mmol=9.0,
        pressure_hpa=1000.0,
        pressure_gauge_hpa=998.5,
        thermometer_temp_c=25.2,
        frame_has_data=True,
        frame_usable=True,
        frame_status="ok",
        point_phase="co2",
        point_tag="co2_1",
        sample_index=1,
    )
    context.result_store.save_sample(sample)
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
    service.export_all_artifacts()
    points_readable_path = context.result_store.points_readable_path
    header, rows = load_csv_rows(points_readable_path)
    summary = json.loads(context.result_store.data_writer.summary_path.read_text(encoding="utf-8"))
    export_statuses = dict(summary.get("stats", {}).get("artifact_exports", {}) or {})
    ok = (
        points_readable_path.exists()
        and "thermometer_temp_c_mean" in header
        and len(rows) == 1
        and export_statuses.get("points_readable", {}).get("role") == "execution_summary"
        and export_statuses.get("points_readable", {}).get("status") == "ok"
    )
    return {
        "name": "points_readable_execution_summary",
        "status": "MATCH" if ok else "MISMATCH",
        "details": {
            "points_readable_path": str(points_readable_path),
            "points_readable_header": header,
            "points_readable_rows": rows,
            "artifact_exports": export_statuses,
        },
    }


def _build_artifact_service(
    tmp_path: Path,
) -> tuple[ArtifactService, OrchestrationContext, RunState, SimpleNamespace, RunSession]:
    payload: dict[str, object] = {
        "paths": {"output_dir": str(tmp_path)},
        "features": {"simulation_mode": True},
        "workflow": {"reporting": {"include_fleet_stats": True}},
    }
    config = AppConfig.from_dict(payload)
    session = RunSession(config)
    event_bus = EventBus()
    state_manager = StateManager(event_bus)
    result_store = ResultStore(tmp_path, session.run_id)
    run_logger = RunLogger(str(tmp_path), session.run_id)
    device_manager = DeviceManager(
        config.devices,
        device_factory=DeviceFactory(simulation_mode=True),
    )
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
    service_host = SimpleNamespace(_points_path=tmp_path / "points.json", _sync_results_to_storage_impl=None)
    host = SimpleNamespace(
        get_results=result_store.get_samples,
        get_output_files=lambda: remembered,
        _remember_output_file=remembered.append,
        _export_coefficient_report=lambda: calls.append("coeff"),
        _export_qc_report=lambda: (_ for _ in ()).throw(RuntimeError("qc export exploded")),
        _export_temperature_snapshots=lambda: calls.append("temperature"),
        _log=logs.append,
        _startup_pressure_precheck_payload=lambda: None,
        remembered=remembered,
        logs=logs,
        calls=calls,
        service=service_host,
    )
    return ArtifactService(context, run_state, host=host), context, run_state, host, session
