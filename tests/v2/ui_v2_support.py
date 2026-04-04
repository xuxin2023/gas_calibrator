from __future__ import annotations

from datetime import datetime
import json
import tkinter as tk
from pathlib import Path
from types import SimpleNamespace

import pytest

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.event_bus import EventBus
from gas_calibrator.v2.core.models import CalibrationPhase, CalibrationPoint, SamplingResult
from gas_calibrator.v2.core.result_store import ResultStore
from gas_calibrator.v2.core.route_context import RouteContext
from gas_calibrator.v2.core.run_state import RunState
from gas_calibrator.v2.core.session import RunSession
from gas_calibrator.v2.core.state_manager import StateManager
from gas_calibrator.v2.qc.point_validator import PointValidationResult
from gas_calibrator.v2.qc.qc_report import QCReport
from gas_calibrator.v2.qc.quality_scorer import RunQualityScore
from gas_calibrator.v2.ui_v2.controllers.app_facade import AppFacade
from gas_calibrator.v2.ui_v2.utils.preferences_store import PreferencesStore
from gas_calibrator.v2.ui_v2.utils.recent_runs_store import RecentRunsStore
from gas_calibrator.v2.ui_v2.utils.runtime_paths import RuntimePaths


def make_root() -> tk.Tk:
    try:
        root = tk.Tk()
    except tk.TclError as exc:  # pragma: no cover - environment guard
        pytest.skip(f"Tk unavailable: {exc}")
    root.withdraw()
    return root


class FakeService:
    def __init__(self, tmp_path: Path) -> None:
        points_path = tmp_path / "points.json"
        points_path.write_text(
            json.dumps(
                {
                    "points": [
                        {"index": 1, "temperature_c": 25.0, "humidity_pct": 30.0, "pressure_hpa": 1000.0, "route": "h2o"},
                        {"index": 2, "temperature_c": 25.0, "co2_ppm": 400.0, "pressure_hpa": 1000.0, "route": "co2"},
                    ]
                }
            ),
            encoding="utf-8",
        )
        self.config = AppConfig.from_dict(
            {
                "devices": {
                    "temperature_chamber": {"port": "COM1", "enabled": True},
                    "gas_analyzers": [{"port": "COM2", "enabled": True}],
                },
                "paths": {
                    "output_dir": str(tmp_path),
                    "points_excel": str(points_path),
                },
                "features": {"use_v2": True, "simulation_mode": True},
                "coefficients": {"enabled": True, "auto_fit": True, "model": "ratio_poly_rt_p"},
                "algorithm": {"default_algorithm": "amt", "candidates": ["linear", "polynomial", "amt"], "auto_select": True},
            }
        )
        self.event_bus = EventBus()
        self.state_manager = StateManager(self.event_bus)
        self.session = RunSession(self.config)
        self.session.start()
        self.result_store = ResultStore(tmp_path, self.session.run_id)
        device_info_map = {
            "temperature_chamber": SimpleNamespace(status=SimpleNamespace(value="online"), port="COM1"),
            "gas_analyzer_0": SimpleNamespace(status=SimpleNamespace(value="online"), port="COM2"),
            "dewpoint_meter": SimpleNamespace(status=SimpleNamespace(value="disabled"), port="COM25"),
            "humidity_generator": SimpleNamespace(status=SimpleNamespace(value="disabled"), port="COM24"),
        }
        self.device_manager = SimpleNamespace(
            get_info=lambda name: device_info_map.get(
                name,
                SimpleNamespace(status=SimpleNamespace(value="unknown"), port=f"{name}.port"),
            ),
            list_device_info=lambda: dict(device_info_map),
        )
        self.orchestrator = SimpleNamespace(route_context=RouteContext(), run_state=RunState())
        self._log_callback = None
        self.is_running = False
        self.start_calls: list[str | None] = []
        self.stop_calls = 0
        self.pause_calls = 0
        self.resume_calls = 0

    def set_log_callback(self, callback) -> None:
        self._log_callback = callback

    def start(self, points_path: str | None = None) -> None:
        self.start_calls.append(points_path)
        self.is_running = True
        self.state_manager.update_status(
            phase=CalibrationPhase.INITIALIZING,
            message="已请求启动",
        )
        self._emit_log(f"start {points_path or ''}".strip())

    def stop(self, wait: bool = False) -> None:
        self.stop_calls += 1
        self.is_running = False
        self.state_manager.stop("Stopped from UI")
        self._emit_log("stop")

    def pause(self) -> None:
        self.pause_calls += 1
        self.state_manager.pause()
        self._emit_log("pause")

    def resume(self) -> None:
        self.resume_calls += 1
        self.state_manager.resume()
        self._emit_log("resume")

    def get_status(self):
        return self.state_manager.status

    def get_results(self):
        return self.result_store.get_samples()

    def get_cleaned_results(self, point_index=None):
        cleaned = self.orchestrator.run_state.qc.cleaned_point_samples
        if point_index is None:
            rows = []
            for items in cleaned.values():
                rows.extend(items)
            return rows
        return list(cleaned.get(point_index, []))

    def get_output_files(self):
        return list(self.orchestrator.run_state.artifacts.output_files)

    def _emit_log(self, message: str) -> None:
        if self._log_callback is not None:
            self._log_callback(message)


def build_fake_service(tmp_path: Path) -> FakeService:
    service = FakeService(tmp_path)
    point_ok = CalibrationPoint(index=1, temperature_c=25.0, co2_ppm=400.0, pressure_hpa=1000.0, route="co2")
    point_bad = CalibrationPoint(index=2, temperature_c=25.0, co2_ppm=0.0, pressure_hpa=1000.0, route="co2")
    sample = SamplingResult(
        point=point_ok,
        analyzer_id="gas_analyzer_0",
        timestamp=datetime.now(),
        co2_ppm=401.2,
        h2o_mmol=9.8,
        co2_signal=1001.0,
        h2o_signal=501.0,
        co2_ratio_f=1.01,
        h2o_ratio_f=0.25,
        ref_signal=2002.0,
        temperature_c=25.0,
        pressure_hpa=1000.0,
        dew_point_c=5.2,
        point_phase="co2",
        point_tag="co2-400ppm",
        stability_time_s=12.0,
        total_time_s=34.0,
    )
    service.result_store.save_sample(sample)
    service.result_store.export_json()

    service.state_manager.prepare_run(2)
    service.state_manager.start()
    service.state_manager.update_status(
        phase=CalibrationPhase.CO2_ROUTE,
        current_point=point_ok,
        message="正在采样点位 1",
    )
    service.session.current_point = point_ok
    service.session.phase = CalibrationPhase.CO2_ROUTE
    service.session.add_warning("质控告警")

    service.orchestrator.route_context.enter(
        current_route="co2",
        current_phase=CalibrationPhase.CO2_ROUTE,
        current_point=point_ok,
        source_point=point_ok,
        active_point=point_ok,
        point_tag="co2-400ppm",
        retry=1,
        route_state={"soak": "已完成", "pressure_ready": True},
    )

    qc_report = QCReport(
        run_id=service.session.run_id,
        timestamp=datetime.now(),
        total_points=2,
        valid_points=1,
        invalid_points=1,
        overall_score=0.81,
        grade="B",
        point_details=[
            {
                "point_index": 1,
                "route": "co2",
                "temperature_c": 25.0,
                "co2_ppm": 400.0,
                "quality_score": 0.92,
                "valid": True,
                "recommendation": "use",
                "reason": "passed",
            },
            {
                "point_index": 2,
                "route": "co2",
                "temperature_c": 25.0,
                "co2_ppm": 0.0,
                "quality_score": 0.58,
                "valid": False,
                "recommendation": "exclude",
                "reason": "outlier_ratio_too_high",
            },
        ],
        recommendations=["拟合前请复核无效点。"],
    )
    service.orchestrator.run_state.qc.qc_report = qc_report
    service.orchestrator.run_state.qc.point_validations = [
        PointValidationResult(
            valid=True,
            point_index=1,
            usable_sample_count=8,
            outlier_ratio=0.0,
            quality_score=0.92,
            recommendation="use",
            reason="passed",
        ),
        PointValidationResult(
            valid=False,
            point_index=2,
            usable_sample_count=4,
            outlier_ratio=0.4,
            quality_score=0.58,
            recommendation="exclude",
            reason="outlier_ratio_too_high",
        ),
    ]
    service.orchestrator.run_state.qc.run_quality_score = RunQualityScore(
        overall_score=0.81,
        point_scores={1: 0.92, 2: 0.58},
        phase_scores={"overall": 0.81},
        grade="B",
        summary="1/2 points valid",
        recommendations=["拟合前请复核无效点。"],
    )
    service.orchestrator.run_state.qc.cleaned_point_samples[1] = [sample]
    service.result_store.save_point_summary(
        point_ok,
        {
            "quality_score": 0.92,
            "valid": True,
            "recommendation": "use",
            "reason": "passed",
        },
    )
    service.result_store.save_point_summary(
        point_bad,
        {
            "quality_score": 0.58,
            "valid": False,
            "recommendation": "exclude",
            "reason": "outlier_ratio_too_high",
            "failed_checks": [{"rule_name": "signal_span", "message": "wide"}],
        },
    )

    service.orchestrator.run_state.artifacts.export_statuses.update(
        {
            "run_summary": {"role": "execution_summary", "status": "ok", "path": str(service.result_store.run_dir / "summary.json"), "error": ""},
            "points_readable": {"role": "execution_summary", "status": "ok", "path": str(service.result_store.run_dir / "points_readable.csv"), "error": ""},
            "samples_csv": {"role": "execution_rows", "status": "ok", "path": str(service.result_store.run_dir / "samples.csv"), "error": ""},
        }
    )
    service.result_store.save_run_summary(
        service.session,
        service.state_manager.status,
        export_statuses=service.orchestrator.run_state.artifacts.export_statuses,
    )
    service.result_store.save_run_manifest(service.session, source_points_file=service.config.paths.points_excel)
    offline_payload = service.result_store.export_offline_artifacts(
        service.session,
        source_points_file=service.config.paths.points_excel,
        output_files=list(service.orchestrator.run_state.artifacts.output_files),
        export_statuses=service.orchestrator.run_state.artifacts.export_statuses,
    )
    service.orchestrator.run_state.artifacts.export_statuses.update(dict(offline_payload.get("artifact_statuses") or {}))
    service.result_store.save_run_manifest(
        service.session,
        source_points_file=service.config.paths.points_excel,
        extra_sections=dict(offline_payload.get("manifest_sections") or {}),
    )
    service.result_store.save_run_summary(
        service.session,
        service.state_manager.status,
        export_statuses=service.orchestrator.run_state.artifacts.export_statuses,
        extra_stats=dict(offline_payload.get("summary_stats") or {}),
    )
    (service.result_store.run_dir / "ai_run_summary.md").write_text(
        "# AI 运行摘要\n\n运行状态稳定。\n",
        encoding="utf-8",
    )
    (service.result_store.run_dir / "calibration_coefficients.xlsx").write_text("", encoding="utf-8")
    (service.result_store.run_dir / "qc_report.json").write_text("{}", encoding="utf-8")
    (service.result_store.run_dir / "qc_report.csv").write_text("point,score\n1,0.92\n", encoding="utf-8")
    (service.result_store.run_dir / "points_readable.csv").write_text(
        "point_index,execution_status,AnalyzerCoverage\n1,usable,1/1\n",
        encoding="utf-8",
    )
    service.orchestrator.run_state.artifacts.output_files.extend(
        [
            str(service.result_store.run_dir / "summary.json"),
            str(service.result_store.run_dir / "manifest.json"),
            str(service.result_store.run_dir / "results.json"),
            str(service.result_store.run_dir / "points_readable.csv"),
            str(service.result_store.run_dir / "qc_report.json"),
            str(service.result_store.run_dir / "qc_report.csv"),
            str(service.result_store.run_dir / "ai_run_summary.md"),
            str(service.result_store.run_dir / "calibration_coefficients.xlsx"),
            str(service.result_store.run_dir / "acceptance_plan.json"),
            str(service.result_store.run_dir / "analytics_summary.json"),
            str(service.result_store.run_dir / "trend_registry.json"),
            str(service.result_store.run_dir / "lineage_summary.json"),
            str(service.result_store.run_dir / "evidence_registry.json"),
            str(service.result_store.run_dir / "coefficient_registry.json"),
        ]
    )
    return service


def build_fake_facade(tmp_path: Path) -> AppFacade:
    runtime_paths = RuntimePaths.from_base_dir(tmp_path / "ui_v2_state").ensure_dirs()
    return AppFacade(
        service=build_fake_service(tmp_path),
        runtime_paths=runtime_paths,
        preferences_store=PreferencesStore(runtime_paths.preferences_path),
        recent_runs_store=RecentRunsStore(runtime_paths.recent_runs_path),
    )
