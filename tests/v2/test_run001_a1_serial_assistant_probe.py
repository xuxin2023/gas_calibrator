from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from gas_calibrator.v2.core.run001_a1_serial_assistant_probe import (
    build_serial_assistant_baseline_payload,
    build_serial_assistant_equivalent_probe_payload,
    write_serial_assistant_artifacts,
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


def _mode1_line(device_id: str) -> str:
    return f"YGAS,{device_id},100.0,0.0,1.0,2.0"


def _mode2_line(device_id: str) -> str:
    return f"YGAS,{device_id},100.0,0.0,1,2,3,4,5,6,7,8,9,10,11,12,OK"


class ProbeFakeAnalyzer:
    def __init__(
        self,
        cfg: Mapping[str, Any],
        *,
        lines: list[str],
        query_line: str = "",
        command_ack_lines: list[str] | None = None,
    ) -> None:
        self.cfg = dict(cfg)
        self.lines = list(lines)
        self.query_line = query_line
        self.command_ack_lines = list(command_ack_lines or [])
        self.active_send = False
        self.ser = self
        self.flushed = False
        self.writes: list[str] = []

    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    def flush_input(self) -> None:
        self.flushed = True

    def _drain_stream_lines(self, **_kwargs: Any) -> list[str]:
        return list(self.lines)

    def drain_input_nonblock(self, **_kwargs: Any) -> list[str]:
        return list(self.command_ack_lines)

    def write(self, payload: str) -> None:
        self.writes.append(payload)

    def read_data_passive(self) -> str:
        self.writes.append("READDATA,YGAS,FFF\r\n")
        return self.query_line

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


def test_serial_assistant_baseline_records_site_success_and_v1_serial_defaults() -> None:
    payload = build_serial_assistant_baseline_payload(
        _raw_config(),
        ports=["COM35", "COM37", "COM41", "COM42"],
    )

    assert payload["tested_by_site_operator"] is True
    assert payload["serial_assistant_can_read_write"] is True
    assert payload["known_detected_ids"] == {"COM35": "091", "COM37": "003", "COM41": "023", "COM42": "012"}
    assert payload["command_target_id"] == "FFF"
    assert payload["serial_parameters"]["COM35"]["baudrate"] == 115200
    assert payload["serial_parameters"]["COM35"]["data_bits"] == 8
    assert payload["serial_parameters"]["COM35"]["parity"] == "N"
    assert payload["serial_parameters"]["COM35"]["stop_bits"] == 1
    assert payload["serial_parameters"]["COM35"]["line_ending"] == "CRLF"
    assert payload["serial_parameters"]["COM35"]["line_ending_bytes"] == "\\r\\n"
    assert payload["missing_serial_assistant_details"]


def test_probe_scans_ack_candidates_without_treating_active_data_as_ack() -> None:
    def _factory(cfg: Mapping[str, Any]) -> ProbeFakeAnalyzer:
        return ProbeFakeAnalyzer(
            cfg,
            lines=[
                _mode1_line("091"),
                _mode1_line("091"),
                "prefix<YGAS,091,T>suffix",
            ],
        )

    payload = build_serial_assistant_equivalent_probe_payload(
        _raw_config(),
        ports=["COM35"],
        read_only=True,
        analyzer_factory=_factory,
    )
    result = payload["analyzers"][0]

    assert result["port_open"] is True
    assert result["flush_before_listen"] is True
    assert result["frame_parse"] is True
    assert result["detected_mode"] == "MODE1"
    assert result["active_send"] is True
    assert result["observed_device_id"] == "091"
    assert result["ack_candidates"] == ["YGAS,091,T"]
    assert result["ignored_active_frame_count"] == 2
    assert payload["summary"]["serial_assistant_success_reproduced_by_v2"] is True


def test_probe_read_query_uses_readdata_without_calibration_or_persistent_write() -> None:
    created: list[ProbeFakeAnalyzer] = []

    def _factory(cfg: Mapping[str, Any]) -> ProbeFakeAnalyzer:
        analyzer = ProbeFakeAnalyzer(cfg, lines=[], query_line=_mode2_line("003"))
        created.append(analyzer)
        return analyzer

    payload = build_serial_assistant_equivalent_probe_payload(
        _raw_config(),
        ports=["COM37"],
        read_only=True,
        allow_read_query=True,
        analyzer_factory=_factory,
    )
    result = payload["analyzers"][0]

    assert result["read_query_sent"] is True
    assert result["commands_sent"] == ["READDATA,YGAS,FFF"]
    assert created[0].writes == ["READDATA,YGAS,FFF\r\n"]
    assert payload["persistent_write_command_sent"] is False
    assert payload["calibration_write_command_sent"] is False
    assert result["detected_mode"] == "MODE2"


def test_probe_optional_mode_setup_uses_fff_and_records_active_stream_ack_noise() -> None:
    created: list[ProbeFakeAnalyzer] = []

    def _factory(cfg: Mapping[str, Any]) -> ProbeFakeAnalyzer:
        analyzer = ProbeFakeAnalyzer(
            cfg,
            lines=[_mode1_line("023")],
            command_ack_lines=[
                _mode1_line("023"),
                "noise<YGAS,023,T>",
                _mode1_line("023"),
            ],
        )
        created.append(analyzer)
        return analyzer

    payload = build_serial_assistant_equivalent_probe_payload(
        _raw_config(),
        ports=["COM41"],
        read_only=False,
        send_mode2_active_send=True,
        confirm_communication_setup=True,
        analyzer_factory=_factory,
    )
    result = payload["analyzers"][0]

    assert result["mode_setup_command_sent"] is True
    assert result["commands_sent"] == ["MODE,YGAS,FFF,2", "SETCOMWAY,YGAS,FFF,1"]
    assert created[0].writes == ["MODE,YGAS,FFF,2\r\n", "SETCOMWAY,YGAS,FFF,1\r\n"]
    assert result["ack_candidates"] == ["YGAS,023,T", "YGAS,023,T"]
    assert result["ignored_active_frame_count"] == 5
    assert payload["command_target_id"] == "FFF"
    assert payload["persistent_write_command_sent"] is False
    assert payload["calibration_write_command_sent"] is False


def test_probe_artifacts_record_boundaries_without_touching_v1_or_run_app(tmp_path: Path) -> None:
    baseline = build_serial_assistant_baseline_payload(_raw_config(), ports=["COM35"])
    probe = build_serial_assistant_equivalent_probe_payload(
        _raw_config(),
        ports=["COM35"],
        read_only=True,
        analyzer_factory=lambda cfg: ProbeFakeAnalyzer(cfg, lines=[_mode1_line("091")]),
    )
    written = write_serial_assistant_artifacts(tmp_path, baseline, probe)

    assert Path(written["serial_assistant_baseline_json"]).exists()
    assert Path(written["serial_assistant_baseline_md"]).exists()
    assert Path(written["serial_assistant_equivalent_probe_json"]).exists()
    assert Path(written["serial_assistant_equivalent_probe_md"]).exists()
    assert probe["v1_production_flow_touched"] is False
    assert probe["run_app_touched"] is False
    assert probe["a1_execute_invoked"] is False
    assert probe["a2_invoked"] is False
    assert probe["h2o_invoked"] is False
    assert probe["full_group_invoked"] is False
    assert "SENCO" in probe["forbidden_tokens"]
