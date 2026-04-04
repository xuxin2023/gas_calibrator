from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import threading

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.event_bus import EventBus, EventType
from gas_calibrator.v2.core.models import CalibrationPhase
from gas_calibrator.v2.core.runners.finalization_runner import FinalizationRunner
from gas_calibrator.v2.core.session import RunSession
from gas_calibrator.v2.core.state_manager import StateManager


def _build_service(
    tmp_path: Path,
    *,
    fail_finalization: bool = False,
    fail_summary: bool = False,
    restore_baseline_on_finish: bool = True,
):
    config = AppConfig.from_dict({"paths": {"output_dir": str(tmp_path)}})
    session = RunSession(config)
    session.start()
    event_bus = EventBus()
    state_manager = StateManager(event_bus)
    state_manager.prepare_run(1)
    state_manager.start()
    done_event = threading.Event()
    logs: list[str] = []
    calls: list[str] = []
    events: list[dict] = []
    event_bus.subscribe(EventType.WORKFLOW_COMPLETED, lambda event: events.append(dict(event.data or {})))

    def run_finalization() -> None:
        calls.append("finalization")
        if fail_finalization:
            raise RuntimeError("boom")

    def export_summary(session_arg, *, current_status=None):
        calls.append("summary")
        assert session_arg is session
        assert current_status is not None
        if fail_summary:
            raise RuntimeError("summary boom")

    def export_manifest(session_arg, *, source_points_file=None):
        calls.append("manifest")
        assert session_arg is session

    def cfg_get(path: str, default=None):
        if path == "workflow.restore_baseline_on_finish":
            return restore_baseline_on_finish
        return default

    service = SimpleNamespace(
        run_id=session.run_id,
        session=session,
        state_manager=state_manager,
        event_bus=event_bus,
        orchestrator=SimpleNamespace(
            _log=logs.append,
            _cfg_get=cfg_get,
            pressure_control_service=SimpleNamespace(
                safe_stop_after_run=lambda reason="": calls.append(f"pressure_safe:{reason}") or {"vent_on": True}
            ),
            valve_routing_service=SimpleNamespace(
                restore_baseline_after_run=lambda reason="": calls.append(f"restore_baseline:{reason}") or {"relay_state": {}},
                safe_stop_after_run=lambda baseline_already_restored=False, reason="": calls.append(
                    f"route_safe:{baseline_already_restored}:{reason}"
                ) or {"relay_state": {}},
            ),
        ),
        _done_event=done_event,
        _points_path=Path(tmp_path) / "points.json",
        _run_finalization=run_finalization,
        _export_summary=export_summary,
        _export_manifest=export_manifest,
        _generate_ai_outputs=lambda: calls.append("ai"),
        _sync_results_to_storage=lambda: calls.append("storage"),
    )
    return service, calls, logs, events


def test_finalization_runner_completes_and_publishes_completion(tmp_path: Path) -> None:
    service, calls, logs, events = _build_service(tmp_path)

    FinalizationRunner(service).run(
        final_phase=CalibrationPhase.COMPLETED,
        final_message="Calibration completed",
        final_error=None,
    )

    assert calls == [
        "restore_baseline:restore baseline on finish",
        "pressure_safe:final safe stop",
        "route_safe:True:final safe stop",
        "finalization",
        "summary",
        "manifest",
        "ai",
        "storage",
    ]
    assert service.state_manager.status.phase is CalibrationPhase.COMPLETED
    assert service.session.ended_at is not None
    assert service._done_event.is_set() is True
    assert events and events[0]["phase"] == "completed"


def test_finalization_runner_escalates_finalization_failure_but_still_exits(tmp_path: Path) -> None:
    service, calls, logs, events = _build_service(tmp_path, fail_finalization=True)

    FinalizationRunner(service).run(
        final_phase=CalibrationPhase.COMPLETED,
        final_message="Calibration completed",
        final_error=None,
    )

    assert calls[:3] == [
        "restore_baseline:restore baseline on finish",
        "pressure_safe:final safe stop",
        "route_safe:True:final safe stop",
    ]
    assert calls[3:] == ["finalization", "summary", "manifest", "ai", "storage"]
    assert service.state_manager.status.phase is CalibrationPhase.ERROR
    assert service._done_event.is_set() is True
    assert any("Finalization failed: boom" in message for message in logs)
    assert "boom" in service.session.errors
    assert events and events[0]["phase"] == "error"


def test_finalization_runner_runs_safe_stop_before_export_failure(tmp_path: Path) -> None:
    service, calls, logs, events = _build_service(tmp_path, fail_summary=True)

    FinalizationRunner(service).run(
        final_phase=CalibrationPhase.COMPLETED,
        final_message="Calibration completed",
        final_error=None,
    )

    assert calls[:3] == [
        "restore_baseline:restore baseline on finish",
        "pressure_safe:final safe stop",
        "route_safe:True:final safe stop",
    ]
    assert "summary" in calls
    assert service.state_manager.status.phase is CalibrationPhase.ERROR
    assert any("Finalization failed: summary boom" in message for message in logs)
    assert "summary boom" in service.session.errors
    assert events and events[0]["phase"] == "error"
