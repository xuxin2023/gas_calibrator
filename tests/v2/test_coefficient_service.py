from __future__ import annotations

from datetime import datetime
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
from gas_calibrator.v2.core.services import CoefficientService
from gas_calibrator.v2.core.session import RunSession
from gas_calibrator.v2.core.stability_checker import StabilityChecker
from gas_calibrator.v2.core.state_manager import StateManager


def _config(tmp_path: Path) -> AppConfig:
    return AppConfig.from_dict(
        {
            "paths": {"output_dir": str(tmp_path)},
            "coefficients": {"enabled": True, "auto_fit": True, "model": "ratio_poly_rt_p"},
        }
    )


def test_coefficient_service_exports_ratio_poly_report(tmp_path: Path, monkeypatch) -> None:
    config = _config(tmp_path)
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
    point = CalibrationPoint(index=1, temperature_c=25.0, co2_ppm=400.0, pressure_hpa=1000.0, route="co2")
    results = [SamplingResult(point=point, analyzer_id="ga01", timestamp=datetime.now(), co2_ppm=400.0)]
    remembered: list[str] = []
    logs: list[str] = []
    host = SimpleNamespace(
        get_results=lambda: list(results),
        _remember_output_file=remembered.append,
        _log=logs.append,
    )

    output_path = result_store.run_dir / "calibration_coefficients.xlsx"

    def fake_export(
        samples,
        *,
        out_dir,
        coeff_cfg,
        expected_analyzers=None,
        reference_on_aligned_rows=True,
    ):
        assert samples == results
        assert out_dir == result_store.run_dir
        assert expected_analyzers == []
        assert reference_on_aligned_rows is True
        output_path.write_text("ok", encoding="utf-8")
        return output_path

    monkeypatch.setattr(
        "gas_calibrator.v2.core.services.coefficient_service.export_ratio_poly_report",
        fake_export,
    )

    service = CoefficientService(context, run_state, host=host)
    service.export_coefficient_report()

    assert remembered == [str(output_path)]
    assert any("Coefficient report saved" in message for message in logs)
    assert output_path.exists()

    context.run_logger.finalize()
