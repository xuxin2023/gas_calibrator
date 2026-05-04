from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from gas_calibrator.v2.core.run001_a1_analyzer_id_truth import (
    TRUTH_AUDIT_POLICY,
    build_analyzer_id_truth_audit_payload,
    write_analyzer_id_truth_audit_artifacts,
)


def _mode1_line(device_id: str) -> str:
    return f"YGAS,{device_id},100.0,0.0,1.0,2.0"


def _mode2_line(device_id: str) -> str:
    return f"YGAS,{device_id},0627.154,04.582,1055.908,03.156,1.1974,1.1969,0.7195,0.7188,03293,03943,02365,027.59,027.76,095.67"


def _raw_config() -> dict[str, Any]:
    return {
        "devices": {
            "gas_analyzers": [
                {"name": "analyzer_0", "enabled": True, "port": "COM35", "baud": 115200, "device_id": "091"},
                {"name": "analyzer_1", "enabled": True, "port": "COM37", "baud": 115200, "device_id": "003"},
                {"name": "analyzer_2", "enabled": True, "port": "COM41", "baud": 115200, "device_id": "023"},
                {"name": "analyzer_3", "enabled": True, "port": "COM42", "baud": 115200, "device_id": "012"},
            ]
        }
    }


class TruthFakeAnalyzer:
    def __init__(self, cfg: Mapping[str, Any], *, lines: list[str]) -> None:
        self.cfg = dict(cfg)
        self.lines = list(lines)
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

    def write(self, payload: str) -> None:
        self.writes.append(payload)

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


def test_truth_audit_saves_raw_samples_for_each_com(tmp_path: Path) -> None:
    lines_by_port = {
        "COM35": [_mode2_line("001")] * 20,
        "COM37": [_mode2_line("003")] * 20,
        "COM41": [_mode2_line("003")] * 20,
        "COM42": [_mode2_line("004")] * 20,
    }

    def _factory(cfg: Mapping[str, Any]) -> TruthFakeAnalyzer:
        return TruthFakeAnalyzer(cfg, lines=lines_by_port[str(cfg["configured_port"])])

    payload = build_analyzer_id_truth_audit_payload(
        _raw_config(),
        ports=["COM35", "COM37", "COM41", "COM42"],
        analyzer_factory=_factory,
        timeout_s=30,
    )
    written = write_analyzer_id_truth_audit_artifacts(tmp_path, payload)

    assert Path(written["analyzer_id_truth_audit_json"]).exists()
    assert Path(written["analyzer_id_truth_audit_md"]).exists()
    for port in ("COM35", "COM37", "COM41", "COM42"):
        sample_path = Path(written[f"raw_frame_samples_{port}"])
        assert sample_path.exists()
        assert len([line for line in sample_path.read_text(encoding="utf-8").splitlines() if line.strip()]) == 20
    saved = json.loads(Path(written["analyzer_id_truth_audit_json"]).read_text(encoding="utf-8"))
    assert saved["commands_sent"] == []
    assert saved["read_only"] is True
    assert saved["duplicate_device_id_detected"] is True
    assert saved["duplicate_device_id_status"] == "blocked"


def test_mode1_discovery_id_does_not_override_mode2_truth() -> None:
    def _factory(cfg: Mapping[str, Any]) -> TruthFakeAnalyzer:
        return TruthFakeAnalyzer(
            cfg,
            lines=[_mode1_line("091"), _mode1_line("091"), _mode2_line("001"), _mode2_line("001")],
        )

    payload = build_analyzer_id_truth_audit_payload(
        _raw_config(),
        ports=["COM35"],
        analyzer_factory=_factory,
    )
    result = payload["analyzers"][0]

    assert result["previous_mode1_discovery_id"] == "091"
    assert result["mode1_observed_device_id_set"] == ["091"]
    assert result["observed_device_id_set"] == ["001"]
    assert result["stable_device_id"] == "001"
    assert result["mode1_parser_id_matches_mode2_truth"] is False
    assert result["mode1_parser_id_trust_for_a1"] == "not_used_for_a1_expected_id"


def test_duplicate_mode2_device_ids_are_detected_and_blocking_policy_recorded() -> None:
    lines_by_port = {
        "COM37": [_mode2_line("003")] * 3,
        "COM41": [_mode2_line("003")] * 3,
    }

    def _factory(cfg: Mapping[str, Any]) -> TruthFakeAnalyzer:
        return TruthFakeAnalyzer(cfg, lines=lines_by_port[str(cfg["configured_port"])])

    payload = build_analyzer_id_truth_audit_payload(
        _raw_config(),
        ports=["COM37", "COM41"],
        analyzer_factory=_factory,
    )

    assert payload["duplicate_device_id_detected"] is True
    assert payload["duplicate_device_id_value"] == "003"
    assert payload["duplicate_device_id_ports"] == ["COM37", "COM41"]
    assert payload["duplicate_device_id_policy"] == TRUTH_AUDIT_POLICY
    assert payload["duplicate_device_id_status"] == "blocked"
