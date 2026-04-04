from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
import threading
import time

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.device_manager import DeviceManager
from gas_calibrator.v2.core.event_bus import EventBus
from gas_calibrator.v2.core.orchestration_context import OrchestrationContext
from gas_calibrator.v2.core.result_store import ResultStore
from gas_calibrator.v2.core.run_logger import RunLogger
from gas_calibrator.v2.core.run_state import RunState
from gas_calibrator.v2.core.services import AnalyzerFleetService
from gas_calibrator.v2.core.session import RunSession
from gas_calibrator.v2.core.stability_checker import StabilityChecker
from gas_calibrator.v2.core.services.status_service import StatusService
from gas_calibrator.v2.core.state_manager import StateManager
from gas_calibrator.v2.exceptions import WorkflowValidationError


class FakeAnalyzer:
    def __init__(
        self,
        snapshot: dict | None = None,
        snapshots: list[dict] | None = None,
        read_snapshots: list[dict] | None = None,
        raw_lines: list[str] | None = None,
        passive_lines: list[str] | None = None,
    ) -> None:
        self.snapshot = dict(snapshot or {})
        self.snapshots = list(snapshots or [])
        self.read_snapshots = list(read_snapshots or [])
        self.raw_lines = list(raw_lines or [])
        self.passive_lines = list(passive_lines or [])
        self.fetch_calls = 0
        self.calls: list[tuple[str, tuple, dict]] = []
        self.device_id = ""

    def set_mode_with_ack(self, value, require_ack=False):
        self.calls.append(("set_mode_with_ack", (value,), {"require_ack": require_ack}))

    def set_comm_way_with_ack(self, value, require_ack=False):
        self.calls.append(("set_comm_way_with_ack", (value,), {"require_ack": require_ack}))

    def set_active_freq_with_ack(self, value, require_ack=False):
        self.calls.append(("set_active_freq_with_ack", (value,), {"require_ack": require_ack}))

    def set_average_filter_with_ack(self, value, require_ack=False):
        self.calls.append(("set_average_filter_with_ack", (value,), {"require_ack": require_ack}))

    def set_average_filter_channel_with_ack(self, channel, value, require_ack=False):
        self.calls.append(
            (
                "set_average_filter_channel_with_ack",
                (channel, value),
                {"require_ack": require_ack},
            )
        )

    def set_average_with_ack(self, *, co2_n, h2o_n, require_ack=False):
        self.calls.append(
            ("set_average_with_ack", tuple(), {"co2_n": co2_n, "h2o_n": h2o_n, "require_ack": require_ack})
        )

    def set_warning_phase(self, value: str) -> None:
        self.calls.append(("set_warning_phase", (value,), {}))

    def set_device_id_with_ack(self, value, require_ack=False) -> None:
        self.device_id = str(value)
        self.calls.append(("set_device_id_with_ack", (value,), {"require_ack": require_ack}))

    def read_device_id(self) -> str:
        self.calls.append(("read_device_id", tuple(), {}))
        return self.device_id

    def fetch_all(self) -> dict:
        self.fetch_calls += 1
        if self.snapshots:
            next_item = self.snapshots.pop(0)
            if isinstance(next_item, Exception):
                raise next_item
            self.snapshot = dict(next_item)
        return dict(self.snapshot)

    def read(self) -> dict:
        self.calls.append(("read", tuple(), {}))
        if self.read_snapshots:
            next_item = self.read_snapshots.pop(0)
            if isinstance(next_item, Exception):
                raise next_item
            self.snapshot = dict(next_item)
            return dict(self.snapshot)
        return self.fetch_all()

    def status(self) -> dict:
        self.calls.append(("status", tuple(), {}))
        snapshot = self.read()
        payload = dict(snapshot)
        payload["ok"] = bool(snapshot)
        return payload

    def read_latest_data(self, prefer_stream=None, allow_passive_fallback=False) -> str:
        self.calls.append(
            (
                "read_latest_data",
                tuple(),
                {"prefer_stream": prefer_stream, "allow_passive_fallback": allow_passive_fallback},
            )
        )
        if self.raw_lines:
            next_item = self.raw_lines.pop(0)
            if isinstance(next_item, Exception):
                raise next_item
            return str(next_item)
        if allow_passive_fallback:
            return self.read_data_passive()
        return ""

    def read_data_passive(self) -> str:
        self.calls.append(("read_data_passive", tuple(), {}))
        if self.passive_lines:
            next_item = self.passive_lines.pop(0)
            if isinstance(next_item, Exception):
                raise next_item
            return str(next_item)
        return ""

    def parse_line_mode2(self, line: str) -> dict | None:
        self.calls.append(("parse_line_mode2", (line,), {}))
        text = str(line or "").strip()
        parts = [part.strip() for part in text.split(",")]
        if len(parts) < 4 or parts[0].upper() != "YGAS":
            return None
        try:
            return {
                "mode": 2,
                "co2_ppm": float(parts[2]),
                "h2o_mmol": float(parts[3]),
                "raw": text,
            }
        except Exception:
            return None

    def parse_line(self, line: str) -> dict | None:
        self.calls.append(("parse_line", (line,), {}))
        return self.parse_line_mode2(line)

    def _is_success_ack(self, line: str) -> bool:
        return str(line or "").strip().upper() == "ACK"


def _config(tmp_path: Path) -> AppConfig:
    return AppConfig.from_dict({"paths": {"output_dir": str(tmp_path)}})


def _build_service(tmp_path: Path) -> tuple[AnalyzerFleetService, OrchestrationContext, RunState, SimpleNamespace]:
    config = _config(tmp_path)
    config.devices.gas_analyzers = [
        SimpleNamespace(
            name="GA01",
            port="COM1",
            enabled=True,
            mode=3,
            active_send=False,
            ftd_hz=5,
            average_co2=2,
            average_h2o=4,
            average_filter=21,
        ),
        SimpleNamespace(
            name="GA02",
            port="COM2",
            enabled=True,
            mode=2,
            active_send=True,
            ftd_hz=10,
            average_co2=1,
            average_h2o=1,
            average_filter=49,
        ),
    ]
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
    ga01 = FakeAnalyzer({"co2_ppm": 401.2})
    ga02 = FakeAnalyzer({"co2_ppm": 402.0})
    device_manager.register_device("gas_analyzer_0", ga01, device_type="gas_analyzer")
    device_manager.register_device("gas_analyzer_1", ga02, device_type="gas_analyzer")
    run_state = RunState()
    logs: list[str] = []

    class Host(SimpleNamespace):
        def _cfg_get(self, path: str, default=None):
            if path == "workflow.analyzer_reprobe.cooldown_s":
                return 300.0
            return default

        def _as_int(self, value):
            if value is None:
                return None
            return int(value)

        def _first_method(self, device, names):
            for name in names:
                method = getattr(device, name, None)
                if callable(method):
                    return method
            return None

        def _normalize_snapshot(self, snapshot):
            payload = dict(snapshot or {})
            data = payload.get("data")
            if isinstance(data, dict):
                normalized = dict(data)
                normalized.update(payload)
                return normalized
            return payload

        def _read_device_snapshot(self, analyzer):
            return analyzer.fetch_all()

        def _pick_numeric(self, snapshot, *keys):
            for key in keys:
                value = snapshot.get(key)
                if value is not None:
                    return float(value)
            return None

        def _log(self, message: str):
            logs.append(message)

    host = Host(logs=logs)
    host.status_service = StatusService(context, run_state, host=host)
    return AnalyzerFleetService(context, run_state, host=host), context, run_state, host


def test_analyzer_fleet_service_filters_disables_and_configures(tmp_path: Path) -> None:
    service, context, run_state, host = _build_service(tmp_path)

    analyzers = service.all_gas_analyzers()
    assert [label for label, _, _ in analyzers] == ["GA01", "GA02"]

    service.disable_analyzers(["GA02"], "manual drop")
    active = service.active_gas_analyzers()

    assert [label for label, _, _ in active] == ["GA01"]
    assert run_state.analyzers.disabled == {"GA02"}
    assert run_state.analyzers.disabled_reasons["GA02"] == "manual drop"
    assert any("GA02" in message for message in host.logs)

    label, analyzer, cfg = analyzers[0]
    service.configure_gas_analyzer(analyzer, label=label, cfg=cfg)

    assert [name for name, _, _ in analyzer.calls] == [
        "set_mode_with_ack",
        "set_comm_way_with_ack",
        "set_active_freq_with_ack",
        "set_average_filter_with_ack",
        "set_average_with_ack",
    ]
    assert analyzer.calls[0][1] == (3,)
    assert analyzer.calls[1][1] == (False,)
    assert analyzer.calls[2][1] == (5,)
    assert analyzer.calls[3][1] == (21,)
    assert analyzer.calls[4][2]["co2_n"] == 2
    assert analyzer.calls[4][2]["h2o_n"] == 4

    context.run_logger.finalize()


def test_analyzer_fleet_service_reenables_after_cooldown(tmp_path: Path) -> None:
    service, context, run_state, host = _build_service(tmp_path)

    service.disable_analyzers(["GA02"], "missing frame")
    run_state.analyzers.disabled_last_reprobe_ts["GA02"] = time.time()
    service.attempt_reenable_disabled_analyzers()
    assert run_state.analyzers.disabled == {"GA02"}

    run_state.analyzers.disabled_last_reprobe_ts["GA02"] = time.time() - 3600.0
    service.attempt_reenable_disabled_analyzers()

    assert run_state.analyzers.disabled == set()
    assert "GA02" not in run_state.analyzers.disabled_reasons
    assert "GA02" not in run_state.analyzers.disabled_last_reprobe_ts
    assert any("restored to active set" in message for message in host.logs)

    context.run_logger.finalize()


def test_analyzer_fleet_service_refresh_live_snapshots_honors_interval(tmp_path: Path, monkeypatch) -> None:
    service, context, run_state, host = _build_service(tmp_path)
    host._cfg_get = lambda path, default=None: {
        "workflow.analyzer_reprobe.cooldown_s": 300.0,
        "workflow.analyzer_live_snapshot.interval_s": 2.0,
    }.get(path, default)
    monotonic_values = iter([10.0, 10.5, 12.6, 12.7])
    monkeypatch.setattr(
        "gas_calibrator.v2.core.services.analyzer_fleet_service.time.monotonic",
        lambda: next(monotonic_values),
    )

    first = service.refresh_live_snapshots(reason="temperature_wait")
    second = service.refresh_live_snapshots(reason="temperature_wait")
    third = service.refresh_live_snapshots(reason="temperature_wait")

    ga01 = context.device_manager.get_device("gas_analyzer_0")
    ga02 = context.device_manager.get_device("gas_analyzer_1")
    assert first is True
    assert second is False
    assert third is True
    assert ga01.fetch_calls == 2
    assert ga02.fetch_calls == 2
    assert run_state.analyzers.last_live_snapshot_ts == 12.6

    context.run_logger.finalize()


def test_analyzer_fleet_service_sensor_precheck_applies_overrides_and_passes(tmp_path: Path) -> None:
    service, context, run_state, host = _build_service(tmp_path)
    context.config.workflow.sensor_precheck = {
        "enabled": True,
        "active_send": True,
        "ftd_hz": 7,
        "average_filter": 33,
        "average_co2": 6,
        "average_h2o": 8,
        "duration_s": 0.5,
        "poll_s": 0.0,
        "min_valid_frames": 2,
        "strict": True,
    }
    context.config.workflow.sensor_read_retry = {"retries": 1, "delay_s": 0.0}
    analyzer = context.device_manager.get_device("gas_analyzer_0")
    analyzer.snapshots = [
        RuntimeError("transient analyzer read failure"),
        {"data": {"co2_ratio_f": 1.01}},
        {"data": {"co2_ratio_f": 1.02}},
    ]

    service.run_sensor_precheck()

    assert [name for name, _, _ in analyzer.calls[:5]] == [
        "set_mode_with_ack",
        "set_comm_way_with_ack",
        "set_active_freq_with_ack",
        "set_average_filter_with_ack",
        "set_average_with_ack",
    ]
    assert analyzer.calls[0][1] == (2,)
    assert analyzer.calls[1][1] == (True,)
    assert analyzer.calls[2][1] == (7,)
    assert analyzer.calls[3][1] == (33,)
    assert analyzer.calls[4][2]["co2_n"] == 6
    assert analyzer.calls[4][2]["h2o_n"] == 8
    assert any("Sensor precheck passed (GA01): profile=snapshot valid_frames=2" in message for message in host.logs)

    context.run_logger.finalize()


def test_analyzer_fleet_service_mode2_like_precheck_parses_raw_read_frames(tmp_path: Path) -> None:
    service, context, _run_state, host = _build_service(tmp_path)
    context.config.workflow.sensor_precheck = {
        "enabled": True,
        "profile": "mode2_like",
        "duration_s": 0.2,
        "poll_s": 0.0,
        "min_valid_frames": 1,
        "strict": True,
    }
    context.config.workflow.sensor_read_retry = {"retries": 0, "delay_s": 0.0}

    class RawMode2Analyzer(FakeAnalyzer):
        def read(self):
            self.calls.append(("read", tuple(), {}))
            return "YGAS,2,401.5,9.4"

    analyzer = RawMode2Analyzer()
    context.device_manager.register_device("gas_analyzer_0", analyzer, device_type="gas_analyzer")

    service.run_sensor_precheck()

    assert any("profile=mode2_like" in message for message in host.logs)
    assert any(name == "parse_line_mode2" for name, _, _ in analyzer.calls)

    context.run_logger.finalize()


def test_analyzer_fleet_service_configure_gas_analyzer_uses_mode2_init_controls(tmp_path: Path) -> None:
    service, context, run_state, host = _build_service(tmp_path)
    context.config.workflow.analyzer_mode2_init = {
        "enabled": True,
        "reapply_attempts": 2,
        "stream_attempts": 2,
        "passive_attempts": 1,
        "retry_delay_s": 0.0,
        "reapply_delay_s": 0.0,
        "command_gap_s": 0.0,
        "post_enable_stream_wait_s": 0.0,
        "post_enable_stream_ack_wait_s": 0.0,
    }
    analyzer = context.device_manager.get_device("gas_analyzer_0")
    analyzer.raw_lines = ["noise frame", "YGAS,01,400.0,10.0"]
    label, analyzer, cfg = service.all_gas_analyzers()[0]

    service.configure_gas_analyzer(analyzer, label=label, cfg=cfg)

    call_names = [name for name, _, _ in analyzer.calls]
    assert "set_warning_phase" in call_names
    assert ("set_average_filter_channel_with_ack", (1, 21), {"require_ack": False}) in analyzer.calls
    assert ("set_average_filter_channel_with_ack", (2, 21), {"require_ack": False}) in analyzer.calls
    assert call_names.count("read_latest_data") >= 2
    assert analyzer.calls[-1] == ("set_average_with_ack", tuple(), {"co2_n": 2, "h2o_n": 4, "require_ack": False})

    context.run_logger.finalize()


def test_analyzer_fleet_service_configure_gas_analyzer_pre_v5_skips_mode2_init(tmp_path: Path) -> None:
    service, context, _run_state, host = _build_service(tmp_path)
    context.config.workflow.analyzer_mode2_init = {
        "enabled": True,
        "reapply_attempts": 2,
        "stream_attempts": 2,
        "passive_attempts": 1,
        "retry_delay_s": 0.0,
        "reapply_delay_s": 0.0,
        "command_gap_s": 0.0,
        "post_enable_stream_wait_s": 0.0,
        "post_enable_stream_ack_wait_s": 0.0,
    }
    context.config.workflow.analyzer_setup = {
        "software_version": "pre_v5",
    }
    label, analyzer, cfg = service.all_gas_analyzers()[0]
    analyzer.raw_lines = ["YGAS,01,400.0,10.0"]

    service.configure_gas_analyzer(analyzer, label=label, cfg=cfg)

    call_names = [name for name, _, _ in analyzer.calls]
    assert "set_warning_phase" not in call_names
    assert "read_latest_data" not in call_names
    assert call_names[:5] == [
        "set_mode_with_ack",
        "set_comm_way_with_ack",
        "set_active_freq_with_ack",
        "set_average_filter_with_ack",
        "set_average_with_ack",
    ]
    assert any("MODE2 init skipped" in message for message in host.logs)

    context.run_logger.finalize()


def test_analyzer_fleet_service_apply_analyzer_setup_assigns_device_ids_and_records_trace(tmp_path: Path) -> None:
    service, context, _run_state, host = _build_service(tmp_path)
    context.config.workflow.analyzer_setup = {
        "software_version": "v5_plus",
        "device_id_assignment_mode": "manual",
        "start_device_id": "021",
        "manual_device_ids": ["021", "022"],
    }

    service.apply_analyzer_setup()

    first = context.device_manager.get_device("gas_analyzer_0")
    second = context.device_manager.get_device("gas_analyzer_1")
    assert first.device_id == "021"
    assert second.device_id == "022"
    assert ("set_device_id_with_ack", ("021",), {"require_ack": False}) in first.calls
    assert ("set_device_id_with_ack", ("022",), {"require_ack": False}) in second.calls
    assert any("Analyzer setup software_version=v5_plus" in message for message in host.logs)

    trace_path = context.result_store.run_dir / "route_trace.jsonl"
    entries = [json.loads(line) for line in trace_path.read_text(encoding="utf-8").splitlines()]
    assert any(
        entry["action"] == "analyzer_setup_profile"
        and entry["target"].get("device_id_assignment_mode") == "manual"
        and entry["actual"].get("planned_device_ids") == ["021", "022"]
        for entry in entries
    )
    assert any(
        entry["action"] == "analyzer_device_id_assignment"
        and entry["target"].get("analyzer") == "GA01"
        and entry["target"].get("device_id") == "021"
        and entry["result"] == "ok"
        for entry in entries
    )

    context.run_logger.finalize()


def test_analyzer_fleet_service_sensor_precheck_raises_when_strict(tmp_path: Path) -> None:
    service, context, run_state, host = _build_service(tmp_path)
    context.config.workflow.sensor_precheck = {
        "enabled": True,
        "duration_s": 0.5,
        "poll_s": 0.0,
        "min_valid_frames": 2,
        "strict": True,
    }
    analyzer = context.device_manager.get_device("gas_analyzer_0")
    analyzer.snapshots = [
        {"data": {"temperature_c": 25.0}},
        {"data": {"pressure_hpa": 1000.0}},
        {"data": {"dewpoint_c": 5.0}},
    ]

    try:
        service.run_sensor_precheck()
    except WorkflowValidationError as exc:
        assert exc.context["analyzer"] == "GA01"
        assert exc.context["valid_frames"] == 0
        assert exc.context["min_valid_frames"] == 2
    else:
        raise AssertionError("Expected WorkflowValidationError")

    context.run_logger.finalize()


def test_analyzer_fleet_service_sensor_precheck_logs_when_non_strict(tmp_path: Path) -> None:
    service, context, run_state, host = _build_service(tmp_path)
    context.config.workflow.sensor_precheck = {
        "enabled": True,
        "duration_s": 0.5,
        "poll_s": 0.0,
        "min_valid_frames": 3,
        "strict": False,
    }
    analyzer = context.device_manager.get_device("gas_analyzer_0")
    analyzer.snapshots = [
        {"data": {"temperature_c": 25.0}},
        {"data": {"pressure_hpa": 1000.0}},
    ]

    service.run_sensor_precheck()

    assert any("Sensor precheck failed (GA01): valid_frames=0/3" in message for message in host.logs)

    context.run_logger.finalize()


def test_analyzer_fleet_service_sensor_precheck_v1_compatible_checks_only_first_analyzer(tmp_path: Path) -> None:
    service, context, run_state, host = _build_service(tmp_path)
    context.config.workflow.sensor_precheck = {
        "enabled": True,
        "mode": "v1_compatible",
        "duration_s": 0.5,
        "poll_s": 0.0,
        "min_valid_frames": 1,
        "strict": True,
    }
    first = context.device_manager.get_device("gas_analyzer_0")
    second = context.device_manager.get_device("gas_analyzer_1")
    first.read_snapshots = [{"mode": 2, "co2_ppm": 401.0, "h2o_mmol": 12.5, "raw": "YGAS,..."}]
    second.read_snapshots = [RuntimeError("GA02 should not be checked in v1-compatible mode")]

    service.run_sensor_precheck()

    assert any(
        "Sensor precheck profile=mode2_like scope=first_analyzer_only validation_mode=v1_mode2_like analyzers=1"
        in message
        for message in host.logs
    )
    assert any("Sensor precheck passed (GA01): profile=mode2_like valid_frames=1" in message for message in host.logs)
    assert second.calls == []
    trace_path = context.result_store.run_dir / "route_trace.jsonl"
    entries = [json.loads(line) for line in trace_path.read_text(encoding="utf-8").splitlines()]
    assert any(
        entry["action"] == "sensor_precheck_profile"
        and entry["target"].get("profile") == "mode2_like"
        and entry["target"].get("validation_mode") == "v1_mode2_like"
        and entry["target"].get("scope") == "first_analyzer_only"
        for entry in entries
    )
    assert any(
        entry["action"] == "sensor_precheck_analyzer"
        and entry["result"] == "ok"
        and entry["target"].get("analyzer") == "GA01"
        for entry in entries
    )

    context.run_logger.finalize()


def test_analyzer_fleet_service_sensor_precheck_v1_mode2_like_requires_co2_and_h2o(tmp_path: Path) -> None:
    service, context, run_state, host = _build_service(tmp_path)
    context.config.workflow.sensor_precheck = {
        "enabled": True,
        "validation_mode": "v1_mode2_like",
        "duration_s": 0.5,
        "poll_s": 0.0,
        "min_valid_frames": 1,
        "strict": True,
    }
    analyzer = context.device_manager.get_device("gas_analyzer_0")
    analyzer.read_snapshots = [
        {"mode": 2, "co2_ppm": 401.0, "raw": "YGAS,..."},
        {"mode": 2, "co2_ratio_f": 1.01, "raw": "YGAS,..."},
    ]

    try:
        service.run_sensor_precheck()
    except WorkflowValidationError as exc:
        assert exc.context["analyzer"] == "GA01"
        assert exc.context["valid_frames"] == 0
        assert exc.context["min_valid_frames"] == 1
    else:
        raise AssertionError("Expected WorkflowValidationError")

    assert any(
        "Sensor precheck profile=mode2_like scope=first_analyzer_only validation_mode=v1_mode2_like analyzers=1"
        in message
        for message in host.logs
    )
    trace_path = context.result_store.run_dir / "route_trace.jsonl"
    entries = [json.loads(line) for line in trace_path.read_text(encoding="utf-8").splitlines()]
    assert any(
        entry["action"] == "sensor_precheck_analyzer"
        and entry["result"] == "fail"
        and entry["target"].get("validation_mode") == "v1_mode2_like"
        for entry in entries
    )

    context.run_logger.finalize()


def test_analyzer_fleet_service_sensor_precheck_all_analyzers_scope_checks_second_analyzer(tmp_path: Path) -> None:
    service, context, run_state, host = _build_service(tmp_path)
    context.config.workflow.sensor_precheck = {
        "enabled": True,
        "scope": "all_analyzers",
        "duration_s": 0.5,
        "poll_s": 0.0,
        "min_valid_frames": 2,
        "strict": True,
    }
    first = context.device_manager.get_device("gas_analyzer_0")
    second = context.device_manager.get_device("gas_analyzer_1")
    first.snapshots = [
        {"data": {"co2_ratio_f": 1.01}},
        {"data": {"co2_ratio_f": 1.02}},
    ]
    second.snapshots = [
        {"data": {"temperature_c": 25.0}},
        {"data": {"pressure_hpa": 1000.0}},
    ]

    try:
        service.run_sensor_precheck()
    except WorkflowValidationError as exc:
        assert exc.context["analyzer"] == "GA02"
        assert exc.context["valid_frames"] == 0
        assert exc.context["min_valid_frames"] == 2
    else:
        raise AssertionError("Expected WorkflowValidationError")

    assert any(
        "Sensor precheck profile=snapshot scope=all_analyzers validation_mode=snapshot analyzers=2" in message
        for message in host.logs
    )
    assert [name for name, _, _ in second.calls[:5]] == [
        "set_mode_with_ack",
        "set_comm_way_with_ack",
        "set_active_freq_with_ack",
        "set_average_filter_with_ack",
        "set_average_with_ack",
    ]

    context.run_logger.finalize()


def test_analyzer_fleet_service_sensor_precheck_validation_mode_v1_mode2_like(tmp_path: Path) -> None:
    """Test that explicit validation_mode=v1_mode2_like is logged and passes with V1-like fields."""
    service, context, run_state, host = _build_service(tmp_path)
    context.config.workflow.sensor_precheck = {
        "enabled": True,
        "validation_mode": "v1_mode2_like",
        "duration_s": 0.5,
        "poll_s": 0.0,
        "min_valid_frames": 1,
        "strict": True,
    }
    first = context.device_manager.get_device("gas_analyzer_0")
    first.read_snapshots = [{"mode": 2, "co2_ppm": 400.0, "h2o_mmol": 10.0, "raw": "YGAS,..."}]

    service.run_sensor_precheck()

    assert any("profile=mode2_like" in message for message in host.logs)
    assert any("validation_mode=v1_mode2_like" in message for message in host.logs)
    assert any("Sensor precheck passed (GA01): profile=mode2_like valid_frames=1" in message for message in host.logs)

    context.run_logger.finalize()


def test_analyzer_fleet_service_sensor_precheck_validation_mode_snapshot(tmp_path: Path) -> None:
    """Test that validation_mode=snapshot uses full frame validation."""
    service, context, run_state, host = _build_service(tmp_path)
    context.config.workflow.sensor_precheck = {
        "enabled": True,
        "scope": "first_analyzer_only",
        "validation_mode": "snapshot",
        "duration_s": 0.5,
        "poll_s": 0.0,
        "min_valid_frames": 1,
        "strict": True,
    }
    first = context.device_manager.get_device("gas_analyzer_0")
    first.snapshots = [{"data": {"co2_ratio_f": 1.01, "h2o_ratio_f": 0.02}}]

    service.run_sensor_precheck()

    assert any("validation_mode=snapshot" in message for message in host.logs)
    assert any("Sensor precheck passed (GA01): profile=snapshot valid_frames=1" in message for message in host.logs)

    context.run_logger.finalize()


def test_analyzer_fleet_service_sensor_precheck_v1_frame_like_prefers_raw_frame_parse(tmp_path: Path) -> None:
    service, context, run_state, host = _build_service(tmp_path)
    context.config.workflow.sensor_precheck = {
        "enabled": True,
        "validation_mode": "v1_frame_like",
        "duration_s": 0.5,
        "poll_s": 0.0,
        "min_valid_frames": 1,
        "strict": True,
    }
    first = context.device_manager.get_device("gas_analyzer_0")
    second = context.device_manager.get_device("gas_analyzer_1")
    first.raw_lines = ["YGAS,01,400.0,10.0"]
    second.raw_lines = [RuntimeError("GA02 should not be checked in v1-frame mode")]

    service.run_sensor_precheck()

    assert any(
        "Sensor precheck profile=raw_frame_first scope=first_analyzer_only validation_mode=v1_frame_like analyzers=1"
        in message
        for message in host.logs
    )
    assert any(
        "Sensor precheck passed (GA01): profile=raw_frame_first valid_frames=1 source=read_latest_data"
        in message
        for message in host.logs
    )
    assert any(name == "read_latest_data" for name, _, _ in first.calls)
    assert second.calls == []
    trace_path = context.result_store.run_dir / "route_trace.jsonl"
    entries = [json.loads(line) for line in trace_path.read_text(encoding="utf-8").splitlines()]
    assert any(
        entry["action"] == "sensor_precheck_analyzer"
        and entry["result"] == "ok"
        and entry["actual"].get("source") == "read_latest_data"
        and entry["target"].get("validation_mode") == "v1_frame_like"
        for entry in entries
    )

    context.run_logger.finalize()


def test_analyzer_fleet_service_sensor_precheck_v1_frame_like_falls_back_to_snapshot(tmp_path: Path) -> None:
    service, context, run_state, host = _build_service(tmp_path)
    context.config.workflow.sensor_precheck = {
        "enabled": True,
        "validation_mode": "v1_frame_like",
        "duration_s": 0.5,
        "poll_s": 0.0,
        "min_valid_frames": 1,
        "strict": True,
    }
    first = context.device_manager.get_device("gas_analyzer_0")
    first.raw_lines = ["noise frame"]
    first.read_snapshots = [{"mode": 2, "co2_ppm": 401.0, "h2o_mmol": 12.0, "raw": "YGAS,01,401.0,12.0"}]

    service.run_sensor_precheck()

    assert any("validation_mode=v1_frame_like" in message for message in host.logs)
    assert any("source=snapshot_fallback:read" in message for message in host.logs)
    assert any("fallback_reason=invalid raw frame via=" in message for message in host.logs)

    trace_path = context.result_store.run_dir / "route_trace.jsonl"
    entries = [json.loads(line) for line in trace_path.read_text(encoding="utf-8").splitlines()]
    assert any(
        entry["action"] == "sensor_precheck_analyzer"
        and entry["result"] == "ok"
        and entry["actual"].get("source") == "snapshot_fallback:read"
        and "invalid raw frame via=" in str(entry["actual"].get("fallback_reason", ""))
        for entry in entries
    )

    context.run_logger.finalize()


def test_analyzer_fleet_service_sensor_precheck_v1_frame_like_falls_back_when_raw_api_missing(tmp_path: Path) -> None:
    service, context, run_state, host = _build_service(tmp_path)
    context.config.workflow.sensor_precheck = {
        "enabled": True,
        "validation_mode": "v1_frame_like",
        "duration_s": 0.5,
        "poll_s": 0.0,
        "min_valid_frames": 1,
        "strict": True,
    }
    first = context.device_manager.get_device("gas_analyzer_0")
    first.read_latest_data = None
    first.read_data_passive = None
    first.read_snapshots = [{"mode": 2, "co2_ppm": 401.0, "h2o_mmol": 12.0, "raw": "YGAS,01,401.0,12.0"}]

    service.run_sensor_precheck()

    assert any("validation_mode=v1_frame_like" in message for message in host.logs)
    assert any("source=snapshot_fallback:read" in message for message in host.logs)
    assert any("fallback_reason=no supported raw sensor read method" in message for message in host.logs)

    context.run_logger.finalize()


def test_analyzer_fleet_service_sensor_precheck_explicit_raw_frame_first_profile(tmp_path: Path) -> None:
    service, context, run_state, host = _build_service(tmp_path)
    context.config.workflow.sensor_precheck = {
        "enabled": True,
        "profile": "raw_frame_first",
        "duration_s": 0.5,
        "poll_s": 0.0,
        "min_valid_frames": 1,
        "strict": True,
    }
    first = context.device_manager.get_device("gas_analyzer_0")
    first.raw_lines = ["YGAS,01,400.0,10.0"]

    service.run_sensor_precheck()

    assert any(
        "Sensor precheck profile=raw_frame_first scope=first_analyzer_only validation_mode=v1_frame_like analyzers=1"
        in message
        for message in host.logs
    )

    context.run_logger.finalize()
