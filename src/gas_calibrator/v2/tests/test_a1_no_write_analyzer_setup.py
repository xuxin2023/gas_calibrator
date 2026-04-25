from __future__ import annotations

import json
from types import SimpleNamespace

from gas_calibrator.v2.core.device_factory import DeviceType
from gas_calibrator.v2.core.no_write_guard import NoWriteGuard
from gas_calibrator.v2.core.run001_a1_dry_run import (
    build_effective_analyzer_fleet_payload,
    build_run001_a1_evidence_payload,
    evaluate_run001_a1_readiness,
    write_run001_a1_artifacts,
)
from gas_calibrator.v2.core.services.analyzer_fleet_service import AnalyzerFleetService


class _FakeDeviceManager:
    def __init__(self, analyzer):
        self.analyzer = analyzer

    def get_devices_by_type(self, device_type):
        assert DeviceType.from_value(device_type) is DeviceType.GAS_ANALYZER
        return {"gas_analyzer_0": self.analyzer}


class _Analyzer:
    def __init__(self) -> None:
        self.device_id_calls: list[tuple] = []

    def set_device_id_with_ack(self, *args, **kwargs) -> None:
        self.device_id_calls.append((args, kwargs))
        raise AssertionError("A1 no-write must not attempt device-id writes")


class _TraceRecorder:
    def __init__(self) -> None:
        self.events: list[dict] = []

    def record_route_trace(self, **kwargs) -> None:
        self.events.append(dict(kwargs))


class _Host:
    def __init__(self, *, no_write_guard: NoWriteGuard | None) -> None:
        self.service = SimpleNamespace(no_write_guard=no_write_guard)
        self.status_service = _TraceRecorder()
        self.logs: list[str] = []

    def _as_int(self, value):
        try:
            return int(value)
        except Exception:
            return None

    def _log(self, message: str) -> None:
        self.logs.append(str(message))

    @staticmethod
    def _first_method(target, names):
        for name in names:
            method = getattr(target, name, None)
            if callable(method):
                return method
        return None


def _service_for(analyzer: _Analyzer, host: _Host) -> AnalyzerFleetService:
    context = SimpleNamespace(
        device_manager=_FakeDeviceManager(analyzer),
        config=SimpleNamespace(
            devices=SimpleNamespace(gas_analyzers=[SimpleNamespace(name="ga01")]),
            workflow=SimpleNamespace(
                analyzer_setup={
                    "device_id_assignment_mode": "automatic",
                    "start_device_id": "001",
                    "apply_device_id": True,
                }
            ),
        ),
    )
    return AnalyzerFleetService(context, SimpleNamespace(analyzers=SimpleNamespace(disabled=set())), host=host)


def test_a1_no_write_guard_forces_analyzer_device_id_keep() -> None:
    analyzer = _Analyzer()
    host = _Host(no_write_guard=NoWriteGuard(scope="run001_a1", enabled=True))

    _service_for(analyzer, host).apply_analyzer_setup()

    assert analyzer.device_id_calls == []
    assert any("no_write_guard_active=True" in line for line in host.logs)
    profile_event = host.status_service.events[0]
    assert profile_event["action"] == "analyzer_setup_profile"
    assert profile_event["actual"]["configured_apply_device_id"] is True
    assert profile_event["actual"]["effective_apply_device_id"] is False
    assert profile_event["actual"]["no_write_guard_active"] is True
    keep_event = host.status_service.events[1]
    assert keep_event["action"] == "analyzer_device_id_keep"
    assert "no-write guard" in keep_event["actual"]["detail"]


def _a1_raw_config(analyzers: list[dict], truth_path: str = "truth.json") -> dict:
    return {
        "run001_a1": {
            "mode": "real_machine_dry_run",
            "no_write": True,
            "co2_only": True,
            "single_route": True,
            "single_temperature_group": True,
            "allow_real_route": True,
            "allow_real_pressure": True,
            "allow_real_wait": True,
            "allow_real_sample": True,
            "allow_artifact": True,
            "allow_write_coefficients": False,
            "allow_write_zero": False,
            "allow_write_span": False,
            "allow_write_calibration_parameters": False,
            "default_cutover_to_v2": False,
            "disable_v1": False,
            "full_h2o_co2_group": False,
            "enabled_analyzer_list_source": "test_truth_audit",
            "mode2_truth_audit_path": truth_path,
        },
        "devices": {"gas_analyzers": analyzers},
        "workflow": {
            "route_mode": "co2_only",
            "skip_co2_ppm": [0],
            "selected_temps_c": [20.0],
        },
    }


def _truth_row(port: str, device_id: str, *, bytes_received: int = 100, mode2_frames: int = 4) -> dict:
    return {
        "port": port,
        "configured_port": port,
        "read_only": True,
        "commands_sent": [],
        "port_open": True,
        "bytes_received": bytes_received,
        "raw_frame_count": mode2_frames,
        "mode1_frame_count": 0,
        "mode2_frame_count": mode2_frames,
        "active_send_detected": bytes_received > 0,
        "stable_device_id": device_id,
    }


def test_passive_mode2_active_send_counts_ready_without_readdata(tmp_path) -> None:
    truth = {
        "read_only": True,
        "passive_listen_only": True,
        "commands_sent": [],
        "analyzers": [_truth_row("COM35", "001")],
    }
    (tmp_path / "truth.json").write_text(json.dumps(truth), encoding="utf-8")
    raw_cfg = _a1_raw_config(
        [{"name": "ga01", "enabled": True, "port": "COM35", "device_id": "001"}],
    )

    payload = build_effective_analyzer_fleet_payload(raw_cfg, config_path=tmp_path / "config.json")

    assert payload["mapping_status"] == "match"
    assert payload["intended_effective_match"] is True
    assert payload["all_enabled_mode2_ready"] is True
    analyzer = payload["analyzers"][0]
    assert analyzer["mode2_ready"] is True
    assert analyzer["readdata_response_status"] == "not_required_passive_mode2_active_send"
    assert analyzer["identity_write_command_sent"] is False
    assert analyzer["persistent_write_command_sent"] is False


def test_effective_analyzer_truth_mismatch_fails_closed(tmp_path) -> None:
    truth = {
        "read_only": True,
        "passive_listen_only": True,
        "commands_sent": [],
        "analyzers": [_truth_row("COM35", "001"), _truth_row("COM37", "029")],
    }
    (tmp_path / "truth.json").write_text(json.dumps(truth), encoding="utf-8")
    raw_cfg = _a1_raw_config(
        [
            {"name": "ga01", "enabled": True, "port": "COM35", "device_id": "001"},
            {"name": "ga02", "enabled": True, "port": "COM36", "device_id": "002"},
        ],
    )

    readiness = evaluate_run001_a1_readiness(raw_cfg, config_path=tmp_path / "config.json")

    assert readiness["effective_analyzer_fleet_summary"]["mapping_status"] == "mismatch"
    assert "effective_analyzer_list_mismatch" in readiness["hard_stop_reasons"]
    assert "enabled_analyzer_not_mode2_ready" in readiness["hard_stop_reasons"]


def test_effective_analyzer_fleet_artifact_is_written_and_referenced(tmp_path) -> None:
    truth = {
        "read_only": True,
        "passive_listen_only": True,
        "commands_sent": [],
        "analyzers": [_truth_row("COM35", "001")],
    }
    (tmp_path / "truth.json").write_text(json.dumps(truth), encoding="utf-8")
    raw_cfg = _a1_raw_config(
        [{"name": "ga01", "enabled": True, "port": "COM35", "device_id": "001"}],
    )
    payload = build_run001_a1_evidence_payload(raw_cfg, config_path=tmp_path / "config.json")

    paths = write_run001_a1_artifacts(tmp_path / "artifacts", payload)

    assert "effective_analyzer_fleet" in paths
    fleet_text = (tmp_path / "artifacts" / "effective_analyzer_fleet.json").read_text(encoding="utf-8")
    assert '"mapping_source": "test_truth_audit"' in fleet_text
    manifest_text = (tmp_path / "artifacts" / "run_manifest.json").read_text(encoding="utf-8")
    assert "effective_analyzer_fleet.json" in manifest_text
