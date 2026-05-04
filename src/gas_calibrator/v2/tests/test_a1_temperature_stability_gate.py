from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from gas_calibrator.v2.core.no_write_guard import NoWriteGuard
from gas_calibrator.v2.core.run001_a1_dry_run import (
    build_run001_a1_evidence_payload,
    write_run001_a1_artifacts,
)
from gas_calibrator.v2.core.run_state import RunState
from gas_calibrator.v2.core.services import temperature_control_service as temperature_module
from gas_calibrator.v2.core.services.temperature_control_service import TemperatureControlService


CONFIG_PATH = (
    Path(__file__).resolve().parents[1]
    / "configs"
    / "validation"
    / "run001_a1_co2_only_skip0_no_write_real_machine_dry_run.json"
)


class _Analyzer:
    def __init__(self, values: list[float]) -> None:
        self._values = list(values)
        self._last = values[-1]

    def fetch_all(self) -> dict[str, float | str]:
        if self._values:
            self._last = self._values.pop(0)
        return {
            "chamber_temp_c": self._last,
            "stable_device_id": "001",
        }


class _Host:
    def __init__(self, analyzer: _Analyzer, cfg: dict[str, float | bool]) -> None:
        self._analyzer = analyzer
        self._analyzer_cfg = SimpleNamespace(port="COM35", device_id="001")
        self.service = SimpleNamespace(no_write_guard=NoWriteGuard(scope="run001_a1", enabled=True))
        self.logs: list[str] = []

        self._cfg = {
            "workflow.sensor_read_retry.retries": 0,
            "workflow.sensor_read_retry.delay_s": 0.0,
            "workflow.stability.temperature.analyzer_chamber_temp_enabled": True,
            "workflow.stability.temperature.analyzer_chamber_temp_window_s": 60.0,
            "workflow.stability.temperature.analyzer_chamber_temp_span_c": 0.08,
            "workflow.stability.temperature.analyzer_chamber_temp_timeout_s": 3600.0,
            "workflow.stability.temperature.analyzer_chamber_temp_first_valid_timeout_s": 120.0,
            "workflow.stability.temperature.analyzer_chamber_temp_poll_s": 1.0,
            **cfg,
        }

    def _cfg_get(self, key: str, default=None):
        return self._cfg.get(key, default)

    def _active_gas_analyzers(self):
        return [("ga01", self._analyzer, self._analyzer_cfg)]

    def _all_gas_analyzers(self):
        return [("ga01", self._analyzer, self._analyzer_cfg)]

    def _check_stop(self) -> None:
        return None

    def _refresh_live_analyzer_snapshots(self, *, force: bool = False, reason: str = "") -> bool:
        return True

    def _normalize_snapshot(self, snapshot):
        return dict(snapshot or {})

    @staticmethod
    def _pick_numeric(snapshot, *keys):
        for key in keys:
            try:
                return float(snapshot[key])
            except Exception:
                continue
        return None

    @staticmethod
    def _first_method(target, names):
        for name in names:
            method = getattr(target, name, None)
            if callable(method):
                return method
        return None

    def _log(self, message: str) -> None:
        self.logs.append(str(message))


def _install_fake_clock(monkeypatch):
    clock = {"now": 0.0}
    monkeypatch.setattr(temperature_module.time, "time", lambda: clock["now"])
    monkeypatch.setattr(
        temperature_module.time,
        "sleep",
        lambda seconds: clock.__setitem__("now", clock["now"] + float(seconds)),
    )
    return clock


def _temperature_service(values: list[float], cfg: dict[str, float | bool]) -> tuple[TemperatureControlService, RunState]:
    run_state = RunState()
    host = _Host(_Analyzer(values), cfg)
    service = TemperatureControlService(SimpleNamespace(), run_state, host=host)
    return service, run_state


def test_a1_config_uses_008_analyzer_chamber_temperature_tolerance() -> None:
    raw_cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    temperature = raw_cfg["workflow"]["stability"]["temperature"]

    assert temperature["analyzer_chamber_temp_span_c"] == 0.08


def test_span_within_008_passes_and_records_evidence(monkeypatch) -> None:
    _install_fake_clock(monkeypatch)
    service, run_state = _temperature_service(
        [20.00, 20.03, 20.06, 20.07],
        {
            "workflow.stability.temperature.analyzer_chamber_temp_window_s": 3.0,
            "workflow.stability.temperature.analyzer_chamber_temp_timeout_s": 10.0,
        },
    )

    assert service._wait_analyzer_chamber_temp_stable(20.0) is True

    evidence = run_state.temperature.analyzer_chamber_temp_stability_evidence
    assert evidence["decision"] == "PASS"
    assert evidence["tolerance_c"] == 0.08
    assert evidence["observed_span_c"] <= 0.08
    assert evidence["samples"][-1]["route_opened"] is False
    assert evidence["samples"][-1]["no_write_guard_active"] is True


def test_span_gt_008_times_out_without_route(monkeypatch) -> None:
    _install_fake_clock(monkeypatch)
    service, run_state = _temperature_service(
        [20.00, 20.20, 20.00, 20.20, 20.00, 20.20],
        {
            "workflow.stability.temperature.analyzer_chamber_temp_window_s": 2.0,
            "workflow.stability.temperature.analyzer_chamber_temp_timeout_s": 5.0,
        },
    )

    assert service._wait_analyzer_chamber_temp_stable(20.0) is False

    evidence = run_state.temperature.analyzer_chamber_temp_stability_evidence
    assert evidence["decision"] == "FAIL"
    assert evidence["failure_stage"] == "analyzer_chamber_temperature_stability"
    assert "tolerance=0.0800C" in evidence["failure_reason"]
    assert evidence["route_opened"] is False
    assert evidence["observed_span_c"] > 0.08


def test_temperature_timeout_writes_hard_fail_artifacts(tmp_path) -> None:
    raw_cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    evidence = {
        "schema_version": "run001_a1.temperature_stability.1",
        "artifact_type": "temperature_stability_evidence",
        "stage": "analyzer_chamber_temperature_stability",
        "enabled": True,
        "temperature_source": "active_send_snapshot",
        "rolling_window_s": 60.0,
        "tolerance_c": 0.08,
        "timeout_s": 3600.0,
        "sampling_interval_s": 1.0,
        "decision": "FAIL",
        "failure_stage": "analyzer_chamber_temperature_stability",
        "failure_reason": "span=0.1200C > tolerance=0.0800C window=60s timeout=3600s",
        "observed_min_c": 20.0,
        "observed_max_c": 20.12,
        "observed_span_c": 0.12,
        "route_opened": False,
        "no_write_guard_active": True,
        "samples": [
            {
                "logical_analyzer_name": "ga01",
                "port": "COM35",
                "device_id": "001",
                "temperature_source": "active_send_snapshot",
                "timestamp": "2026-04-25T00:00:00+00:00",
                "chamber_temperature_c": 20.12,
                "rolling_window_s": 60.0,
                "rolling_min_c": 20.0,
                "rolling_max_c": 20.12,
                "rolling_span_c": 0.12,
                "tolerance_c": 0.08,
                "timeout_s": 3600.0,
                "decision": "FAIL",
                "failure_reason": "span=0.1200C > tolerance=0.0800C",
                "stale_frame_status": "not_checked_no_frame_timestamp",
                "data_gap_status": "ok",
                "route_opened": False,
                "no_write_guard_active": True,
            }
        ],
    }
    guard = NoWriteGuard(scope="run001_a1", enabled=True)
    payload = build_run001_a1_evidence_payload(
        raw_cfg,
        config_path=CONFIG_PATH,
        run_dir=tmp_path,
        guard=guard,
        require_runtime_artifacts=True,
        service_summary={"points_completed": 0, "stats": {"sample_count": 0}},
        service_status={"phase": "failed", "error": evidence["failure_reason"]},
        temperature_stability_evidence=evidence,
    )

    paths = write_run001_a1_artifacts(tmp_path, payload)
    summary = json.loads(Path(paths["summary"]).read_text(encoding="utf-8"))
    guard_payload = json.loads(Path(paths["no_write_guard"]).read_text(encoding="utf-8"))
    manifest = json.loads(Path(paths["manifest"]).read_text(encoding="utf-8"))
    temperature_payload = json.loads(Path(paths["temperature_stability_evidence"]).read_text(encoding="utf-8"))

    assert Path(paths["summary"]).exists()
    assert Path(paths["no_write_guard"]).exists()
    assert Path(paths["manifest"]).exists()
    assert Path(paths["report"]).exists()
    assert Path(paths["temperature_stability_samples"]).read_text(encoding="utf-8").count("\n") >= 2
    assert summary["final_decision"] == "FAIL"
    assert summary["a1_final_decision"] == "FAIL"
    assert summary["failure_stage"] == "analyzer_chamber_temperature_stability"
    assert summary["points_completed"] == 0
    assert summary["sample_count"] == 0
    assert summary["attempted_write_count"] == 0
    assert summary["identity_write_command_sent"] is False
    assert summary["persistent_write_command_sent"] is False
    assert guard_payload["attempted_write_count"] == 0
    assert guard_payload["identity_write_command_sent"] is False
    assert guard_payload["persistent_write_command_sent"] is False
    assert manifest["temperature_stability_tolerance_c"] == 0.08
    assert temperature_payload["tolerance_c"] == 0.08
    assert temperature_payload["route_opened"] is False
