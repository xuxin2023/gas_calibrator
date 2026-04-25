from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

import pytest

from gas_calibrator.v2.core.run001_a1_analyzer_diagnostics import (
    FORBIDDEN_PERSISTENT_COMMAND_TOKENS,
    build_analyzer_precheck_diagnostics,
    contains_persistent_write_token,
    run001_a1_analyzer_configs,
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


class FakeAnalyzer:
    def __init__(self, behavior: str) -> None:
        self.behavior = behavior
        self.active_send = False
        self.commands_sent: list[str] = []
        self.ser = self

    def open(self) -> None:
        if self.behavior == "port_open_fail":
            raise OSError("port busy")

    def close(self) -> None:
        return None

    def read_latest_data(self, **_kwargs) -> str:
        if self.behavior == "no_data":
            return ""
        if self.behavior == "parse_fail":
            return "not-a-ygas-frame"
        if self.behavior == "mode_mismatch":
            return "YGAS,002,100.0,0.0,1.0,2.0"
        return "YGAS,002,100.0,0.0,1,2,3,4,5,6,7,8,9,10,11,12,OK"

    def read_data_passive(self) -> str:
        self.commands_sent.append("READDATA")
        return "YGAS,002,100.0,0.0,1,2,3,4,5,6,7,8,9,10,11,12,OK"

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


def _factory_for(behavior_by_id: Mapping[str, str]):
    created: dict[str, FakeAnalyzer] = {}

    def _factory(cfg: Mapping[str, Any]) -> FakeAnalyzer:
        behavior = behavior_by_id.get(str(cfg.get("logical_id")), "ok")
        analyzer = FakeAnalyzer(behavior)
        created[str(cfg.get("logical_id"))] = analyzer
        return analyzer

    _factory.created = created  # type: ignore[attr-defined]
    return _factory


def test_run001_a1_config_maps_failed_logical_ids_to_ga02_ga04() -> None:
    configs = run001_a1_analyzer_configs(_raw_config())
    selected = {cfg["logical_id"]: cfg for cfg in configs}

    assert selected["gas_analyzer_1"]["physical_label"] == "GA02"
    assert selected["gas_analyzer_1"]["configured_port"] == "COM36"
    assert selected["gas_analyzer_2"]["physical_label"] == "GA03"
    assert selected["gas_analyzer_2"]["configured_port"] == "COM37"
    assert selected["gas_analyzer_3"]["physical_label"] == "GA04"
    assert selected["gas_analyzer_3"]["configured_port"] == "COM38"
    assert selected["gas_analyzer_1"]["baudrate"] == 115200
    assert selected["gas_analyzer_1"]["expected_mode"] == 2
    assert selected["gas_analyzer_1"]["active_send_expected"] is True


def test_read_only_diagnostics_do_not_send_persistent_write_tokens() -> None:
    factory = _factory_for({"gas_analyzer_1": "no_data"})
    payload = build_analyzer_precheck_diagnostics(
        _raw_config(),
        only_failed=["gas_analyzer_1"],
        read_only=True,
        allow_read_query=True,
        analyzer_factory=factory,
    )
    analyzer = factory.created["gas_analyzer_1"]  # type: ignore[attr-defined]

    assert analyzer.commands_sent == ["READDATA"]
    assert payload["summary"]["persistent_write_command_sent"] is False
    assert contains_persistent_write_token("READDATA") is False
    assert all(token not in "READDATA" for token in FORBIDDEN_PERSISTENT_COMMAND_TOKENS)


@pytest.mark.parametrize(
    ("behavior", "expected_error"),
    [
        ("port_open_fail", "port_open_fail"),
        ("no_data", "no_data"),
        ("parse_fail", "parse_fail"),
        ("mode_mismatch", "mode_mismatch"),
    ],
)
def test_read_only_diagnostics_report_common_failure_modes(behavior: str, expected_error: str) -> None:
    factory = _factory_for({"gas_analyzer_1": behavior})
    payload = build_analyzer_precheck_diagnostics(
        _raw_config(),
        only_failed=["gas_analyzer_1"],
        read_only=True,
        allow_read_query=False,
        analyzer_factory=factory,
    )
    result = payload["analyzers"][0]

    assert result["error_type"] == expected_error
    assert result["logical_id"] == "gas_analyzer_1"
    assert result["suggested_onsite_check"]


def test_read_only_diagnostics_default_to_active_send_listen_without_query() -> None:
    factory = _factory_for({"gas_analyzer_1": "no_data"})
    payload = build_analyzer_precheck_diagnostics(
        _raw_config(),
        only_failed=["gas_analyzer_1"],
        read_only=True,
        allow_read_query=False,
        analyzer_factory=factory,
    )
    result = payload["analyzers"][0]

    assert payload["default_mode"] == "active_send_listen"
    assert result["read_query_command_used"] is False
    assert result["commands_sent"] == []
    assert result["error_type"] == "no_data"


def test_read_only_diagnostics_reject_non_read_only_mode() -> None:
    with pytest.raises(ValueError):
        build_analyzer_precheck_diagnostics(
            _raw_config(),
            only_failed=["gas_analyzer_1"],
            read_only=False,
            analyzer_factory=_factory_for({}),
        )
