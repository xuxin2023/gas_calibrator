from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Mapping

import pytest

from gas_calibrator.v2.core.run001_a1_analyzer_diagnostics import (
    build_analyzer_precheck_diagnostics,
    run001_a1_analyzer_configs,
)
from gas_calibrator.v2.core.run001_a1_analyzer_mode2_setup import (
    MODE2_SETUP_ALLOWED_COMMANDS,
    Mode2SetupSafetyError,
    build_analyzer_mode2_setup_payload,
    command_contains_forbidden_mode2_setup_token,
    run_analyzer_mode2_setup,
    validate_mode2_setup_command_plan,
    write_analyzer_mode2_setup_artifacts,
)


CONFIG_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "gas_calibrator"
    / "v2"
    / "configs"
    / "validation"
    / "run001_a1_co2_only_skip0_no_write_real_machine_dry_run.json"
)


def _raw_config() -> dict[str, Any]:
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def _mode2_line(device_id: str) -> str:
    return f"YGAS,{device_id},100.0,0.0,1,2,3,4,5,6,7,8,9,10,11,12,OK"


class FakeMode2Analyzer:
    def __init__(self, cfg: Mapping[str, Any], *, mode1: bool = False) -> None:
        self.cfg = dict(cfg)
        self.mode1 = mode1
        self.ser = self
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
        self.active_send = False

    def open(self) -> None:
        self.calls.append(("open", tuple(), {}))

    def close(self) -> None:
        self.calls.append(("close", tuple(), {}))

    def set_mode_with_ack(self, value: int, *, require_ack: bool = True) -> bool:
        self.calls.append(("set_mode_with_ack", (value,), {"require_ack": require_ack}))
        return True

    def set_comm_way_with_ack(self, value: bool, *, require_ack: bool = True) -> bool:
        self.calls.append(("set_comm_way_with_ack", (value,), {"require_ack": require_ack}))
        return True

    def read_latest_data(self, **_kwargs: Any) -> str:
        device_id = str(self.cfg.get("configured_device_id") or "001")
        if self.mode1:
            return f"YGAS,{device_id},100.0,0.0,1.0,2.0"
        return _mode2_line(device_id)

    def parse_line_mode2(self, line: str) -> dict[str, Any] | None:
        parts = [part.strip() for part in str(line or "").split(",")]
        if len(parts) >= 16 and parts[0] == "YGAS":
            return {"raw": line, "id": parts[1], "mode": 2, "co2_ppm": 100.0, "h2o_mmol": 0.0}
        return None

    def parse_line(self, line: str) -> dict[str, Any] | None:
        mode2 = self.parse_line_mode2(line)
        if mode2:
            return mode2
        parts = [part.strip() for part in str(line or "").split(",")]
        if len(parts) >= 6 and parts[0] == "YGAS":
            return {"raw": line, "id": parts[1], "mode": 1, "co2_ppm": 100.0, "h2o_mmol": 0.0}
        return None


def _factory(*, mode1: bool = False):
    created: list[FakeMode2Analyzer] = []

    def _make(cfg: Mapping[str, Any]) -> FakeMode2Analyzer:
        analyzer = FakeMode2Analyzer(cfg, mode1=mode1)
        created.append(analyzer)
        return analyzer

    _make.created = created  # type: ignore[attr-defined]
    return _make


def test_mode2_setup_defaults_to_dry_run_and_sends_no_commands() -> None:
    factory = _factory()
    payload = build_analyzer_mode2_setup_payload(
        _raw_config(),
        analyzers=["gas_analyzer_0", "gas_analyzer_1", "gas_analyzer_2", "gas_analyzer_3"],
        analyzer_factory=factory,
    )

    assert payload["dry_run"] is True
    assert payload["commands_sent"] == []
    assert payload["mode_setup_command_sent"] is False
    assert payload["persistent_write_command_sent"] is False
    assert payload["calibration_write_command_sent"] is False
    assert factory.created == []  # type: ignore[attr-defined]


def test_mode2_setup_requires_explicit_confirmation_before_sending() -> None:
    factory = _factory()
    payload = build_analyzer_mode2_setup_payload(
        _raw_config(),
        analyzers=["gas_analyzer_0"],
        dry_run=False,
        set_mode2_active_send=True,
        confirm_mode2_communication_setup=False,
        analyzer_factory=factory,
    )

    assert payload["setup_request_status"] == "missing_confirm_mode2_communication_setup"
    assert payload["commands_sent"] == []
    assert payload["summary"]["a1_no_write_rerun_allowed"] is False
    assert factory.created == []  # type: ignore[attr-defined]


def test_mode2_setup_allows_only_whitelisted_mode2_active_send_commands() -> None:
    validate_mode2_setup_command_plan(list(MODE2_SETUP_ALLOWED_COMMANDS))

    with pytest.raises(Mode2SetupSafetyError):
        validate_mode2_setup_command_plan(["FTD,YGAS,FFF,10"])


@pytest.mark.parametrize(
    "token",
    ["SENCO", "COEFF", "ZERO", "SPAN", "CALIBRATION", "SAVE", "COMMIT", "WRITEBACK", "EEPROM", "FLASH", "NVM", "PARAM"],
)
def test_mode2_setup_rejects_forbidden_write_tokens(token: str) -> None:
    command = f"{token},YGAS,FFF,1"

    assert command_contains_forbidden_mode2_setup_token(command) is True
    with pytest.raises(Mode2SetupSafetyError):
        validate_mode2_setup_command_plan([command])


def test_mode2_setup_records_commands_and_never_marks_calibration_or_persistent_writes() -> None:
    payload = build_analyzer_mode2_setup_payload(
        _raw_config(),
        analyzers=["gas_analyzer_0", "gas_analyzer_1", "gas_analyzer_2", "gas_analyzer_3"],
        dry_run=False,
        set_mode2_active_send=True,
        confirm_mode2_communication_setup=True,
        analyzer_factory=_factory(),
    )

    assert payload["commands_sent"] == list(MODE2_SETUP_ALLOWED_COMMANDS) * 4
    assert payload["mode_setup_command_sent"] is True
    assert payload["persistent_write_command_sent"] is False
    assert payload["calibration_write_command_sent"] is False
    assert all(row["commands_sent"] == list(MODE2_SETUP_ALLOWED_COMMANDS) for row in payload["analyzers"])


def test_run001_a1_scope_uses_only_four_enabled_connected_analyzers_not_eight_placeholders() -> None:
    raw = _raw_config()
    expanded = copy.deepcopy(raw["devices"]["gas_analyzers"])
    for index in range(4, 8):
        expanded.append(
            {
                "name": f"ga{index + 1:02d}",
                "enabled": False,
                "connected": False,
                "port": f"COM{40 + index}",
                "baud": 115200,
                "device_id": f"{index + 1:03d}",
                "mode": 2,
                "active_send": True,
            }
        )
    raw["devices"]["gas_analyzers"] = expanded

    configs = run001_a1_analyzer_configs(raw)
    payload = build_analyzer_mode2_setup_payload(raw)

    assert [item["logical_id"] for item in configs] == [
        "gas_analyzer_0",
        "gas_analyzer_1",
        "gas_analyzer_2",
        "gas_analyzer_3",
    ]
    assert payload["summary"]["total"] == 4


def test_mode1_frame_cannot_pass_run001_a1_analyzer_precheck() -> None:
    payload = build_analyzer_precheck_diagnostics(
        _raw_config(),
        analyzers=["gas_analyzer_0"],
        read_only=True,
        analyzer_factory=_factory(mode1=True),
    )
    result = payload["analyzers"][0]

    assert result["frame_parse"] is True
    assert result["mode2_detected"] is False
    assert result["error_type"] == "mode_mismatch"
    assert payload["summary"]["a1_no_write_rerun_allowed"] is False


def test_four_mode2_active_send_analyzers_with_correct_device_ids_allow_next_a1_rerun() -> None:
    payload = build_analyzer_mode2_setup_payload(
        _raw_config(),
        analyzers=["gas_analyzer_0", "gas_analyzer_1", "gas_analyzer_2", "gas_analyzer_3"],
        dry_run=False,
        set_mode2_active_send=True,
        confirm_mode2_communication_setup=True,
        analyzer_factory=_factory(),
    )

    assert payload["summary"]["ready"] == 4
    assert payload["summary"]["a1_no_write_rerun_allowed"] is True
    for row in payload["analyzers"]:
        assert row["after_mode2_detected"] is True
        assert row["after_active_send_detected"] is True
        assert row["after_device_id"] == row["expected_device_id"]
        assert row["final_status"] == "ready"


def test_mode2_setup_artifacts_record_boundaries_without_touching_v1(tmp_path: Path) -> None:
    payload = build_analyzer_mode2_setup_payload(
        _raw_config(),
        analyzers=["gas_analyzer_0"],
        analyzer_factory=_factory(),
    )
    written = write_analyzer_mode2_setup_artifacts(tmp_path, payload)

    assert Path(written["analyzer_mode2_setup_json"]).exists()
    assert Path(written["analyzer_mode2_setup_report"]).exists()
    assert Path(written["analyzer_precheck_diagnostics_json"]).exists()
    assert Path(written["analyzer_precheck_diagnostics_report"]).exists()
    assert payload["run_app_touched"] is False
    assert payload["v1_production_flow_touched"] is False
    assert payload["protected_paths_touched"] == []
    assert payload["a1_execute_invoked"] is False
    assert payload["a2_invoked"] is False
    assert payload["h2o_invoked"] is False
    assert payload["full_group_invoked"] is False


def test_actual_mode2_setup_creates_artifact_before_first_command(tmp_path: Path) -> None:
    observed = {"initial_artifact_seen": False}

    class InspectingAnalyzer(FakeMode2Analyzer):
        def set_mode_with_ack(self, value: int, *, require_ack: bool = True) -> bool:
            artifact = tmp_path / "analyzer_mode2_setup.json"
            assert artifact.exists()
            snapshot = json.loads(artifact.read_text(encoding="utf-8"))
            assert snapshot["status"] == "running"
            assert snapshot["started_at"]
            assert snapshot["dry_run"] is False
            observed["initial_artifact_seen"] = True
            return super().set_mode_with_ack(value, require_ack=require_ack)

    def _make(cfg: Mapping[str, Any]) -> InspectingAnalyzer:
        return InspectingAnalyzer(cfg)

    payload, written = run_analyzer_mode2_setup(
        _raw_config(),
        output_dir=tmp_path,
        analyzers=["gas_analyzer_0"],
        dry_run=False,
        set_mode2_active_send=True,
        confirm_mode2_communication_setup=True,
        command_timeout_s=1.0,
        device_timeout_s=5.0,
        analyzer_factory=_make,
        config_path=str(CONFIG_PATH),
    )

    assert observed["initial_artifact_seen"] is True
    assert Path(written["analyzer_mode2_setup_json"]).exists()
    assert payload["commands_sent"] == list(MODE2_SETUP_ALLOWED_COMMANDS)
    assert payload["persistent_write_command_sent"] is False
    assert payload["calibration_write_command_sent"] is False


def test_mode2_setup_command_timeout_preserves_partial_artifact_and_send_attempt(tmp_path: Path) -> None:
    import time

    class SlowAnalyzer(FakeMode2Analyzer):
        def set_mode_with_ack(self, value: int, *, require_ack: bool = True) -> bool:
            time.sleep(0.2)
            return super().set_mode_with_ack(value, require_ack=require_ack)

    def _make(cfg: Mapping[str, Any]) -> SlowAnalyzer:
        return SlowAnalyzer(cfg)

    payload, written = run_analyzer_mode2_setup(
        _raw_config(),
        output_dir=tmp_path,
        analyzers=["gas_analyzer_0"],
        dry_run=False,
        set_mode2_active_send=True,
        confirm_mode2_communication_setup=True,
        command_timeout_s=0.01,
        device_timeout_s=1.0,
        total_timeout_s=2.0,
        analyzer_factory=_make,
        config_path=str(CONFIG_PATH),
    )
    saved = json.loads(Path(written["analyzer_mode2_setup_json"]).read_text(encoding="utf-8"))
    row = saved["analyzers"][0]
    first_command = row["command_result"][0]

    assert payload["status"] == "timeout"
    assert saved["status"] == "timeout"
    assert row["status"] == "timeout"
    assert row["error_type"] == "command_timeout"
    assert first_command["send_attempted_at"]
    assert first_command["timeout"] is True
    assert saved["partial_results"][0]["logical_id"] == "gas_analyzer_0"
    assert saved["commands_sent"] == []
