from __future__ import annotations

import json
from pathlib import Path

from gas_calibrator.devices.serial_base import ReplaySerial
from gas_calibrator.tools import run_v1_readback_state_matrix as matrix_module
from gas_calibrator.tools.run_v1_safe_readback_session import run_safe_readback_session


class _MatrixSerialFactory:
    def __init__(self, *, enable_explicit_combo: bool = True) -> None:
        self.enable_explicit_combo = bool(enable_explicit_combo)

    def __call__(self, **kwargs):
        serial = ReplaySerial(**kwargs, on_write=self._on_write)
        serial.session_mode = 1
        serial.active_send = True
        serial.setcommway_zero_count = 0
        serial.historical_prime_ready = False
        serial.queue_line("YGAS,079,0794.987,00.000,0.99,0.99,031.94,104.20,0001,2781")
        return serial

    def _on_write(self, payload: bytes, serial: ReplaySerial) -> None:
        text = payload.decode("ascii", errors="ignore").strip()
        if text.startswith("MODE,YGAS,FFF,2"):
            serial.session_mode = 2
            serial.historical_prime_ready = True
            serial.queue_line("YGAS,079,T")
            return
        if text.startswith("MODE,YGAS,FFF,1"):
            serial.session_mode = 1
            serial.queue_line("YGAS,079,T")
            return
        if text.startswith("FTD,YGAS,FFF,10"):
            serial.queue_line("YGAS,079,T")
            return
        if text.startswith("AVERAGE1,YGAS,FFF,49") or text.startswith("AVERAGE2,YGAS,FFF,49"):
            serial.queue_line("YGAS,079,T")
            return
        if text.startswith("SETCOMWAY,YGAS,FFF,1"):
            serial.active_send = True
            serial.queue_line("YGAS,079,T")
            serial.queue_line("YGAS,079,0795.043,00.000,0.99,0.99,031.96,104.20,0001,2767")
            return
        if text.startswith("SETCOMWAY,YGAS,FFF,0"):
            serial.active_send = False
            serial.setcommway_zero_count += 1
            serial.queue_line("YGAS,079,T")
            return
        if text.startswith("READDATA,YGAS,FFF"):
            serial.queue_line("YGAS,079,0794.901,00.000,0.99,0.99,031.94,104.19,0001,2780")
            return
        if text.startswith("GETCO,YGAS,079,") or text.startswith("GETCO1,YGAS,079") or text.startswith("GETCO3,YGAS,079") or text.startswith("GETCO7,YGAS,079") or text.startswith("GETCO8,YGAS,079"):
            if text.startswith("GETCO,YGAS,079,"):
                group = int(text.split(",")[-1])
                parameterized_actual = True
            else:
                group = int(text.split(",")[0].replace("GETCO", ""))
                parameterized_actual = False
            explicit_ready = bool(
                self.enable_explicit_combo
                and serial.historical_prime_ready
                and serial.setcommway_zero_count >= 2
                and parameterized_actual
            )
            if explicit_ready and group == 1:
                serial.queue_line("C0:19846.2,C1:-38766.1,C2:22273.1,C3:-3565.31,C4:0,C5:0")
                return
            if explicit_ready and group == 7:
                serial.queue_line("C0:-1.50402,C1:0.975407,C2:0.00190803,C3:-4.78878e-05")
                return
            if explicit_ready and group == 3:
                serial.queue_line(
                    "YGAS,079,0793.000,00.000,0.99,0.99,031.94,104.20,0001,2781 "
                    "C0:24.8028,C1:0.000504508,C2:-16.6009,C3:0,C4:0,C5:0"
                )
                return
            serial.queue_line("YGAS,079,0792.490,00.000,0.99,0.99,031.95,104.19,0001,2777")
            return
        if text.startswith("GETCO,YGAS,FFF,") or text.startswith("GETCO1,YGAS,FFF") or text.startswith("GETCO3,YGAS,FFF") or text.startswith("GETCO7,YGAS,FFF") or text.startswith("GETCO8,YGAS,FFF"):
            serial.queue_line("YGAS,079,0792.490,00.000,0.99,0.99,031.95,104.19,0001,2777")


def _fast_timing_profiles():
    return [
        {
            "name": "default",
            "read_timeout_s": 0.01,
            "read_retries": 0,
            "command_delay_s": 0.0,
            "inter_command_gap_s": 0.0,
            "drain_window_s": 0.0,
            "quiet_window_s": 0.0,
            "active_settle_s": 0.0,
        },
        {
            "name": "relaxed",
            "read_timeout_s": 0.01,
            "read_retries": 0,
            "command_delay_s": 0.0,
            "inter_command_gap_s": 0.0,
            "drain_window_s": 0.0,
            "quiet_window_s": 0.0,
            "active_settle_s": 0.0,
        },
    ]


def _minimal_success_combos(device_id: str):
    timing = dict(_fast_timing_profiles()[0])
    return [
        {
            "base_session": "historical_prime",
            "local_action": "driver_prepare",
            "command_style": "parameterized",
            "target_variant": {"name": "actual_device_id", "target_id": device_id},
            "timing": dict(timing),
        }
    ]


def _minimal_failure_combos(device_id: str):
    timing = dict(_fast_timing_profiles()[0])
    return [
        {
            "base_session": "baseline",
            "local_action": "direct",
            "command_style": "parameterized",
            "target_variant": {"name": "actual_device_id", "target_id": device_id},
            "timing": dict(timing),
        }
    ]


def test_readback_state_matrix_outputs_explicit_ambiguous_and_no_valid(tmp_path: Path, monkeypatch) -> None:
    out_dir = tmp_path / "matrix"
    monkeypatch.setattr(matrix_module, "_timing_profiles", _fast_timing_profiles)
    monkeypatch.setattr(matrix_module, "_search_combos", _minimal_success_combos)
    monkeypatch.setattr(matrix_module.GasAnalyzer, "COEFFICIENT_COMM_QUIET_DELAY_S", 0.0)

    result = matrix_module.run_readback_state_matrix(
        port="COM39",
        device_id="079",
        output_dir=out_dir,
        confirmation_limit=1,
        confirmation_passes=2,
        serial_factory=_MatrixSerialFactory(enable_explicit_combo=True),
    )

    summary = result["summary"]
    csv_text = (out_dir / "readback_state_matrix.csv").read_text(encoding="utf-8-sig")

    assert summary["explicit_c0_found"] is True
    assert "parsed_from_explicit_c0_line" in csv_text
    assert "parsed_from_ambiguous_line" in csv_text
    assert "no_valid_coefficient_line" in csv_text


def test_safe_readback_session_can_reuse_matrix_success_combo(tmp_path: Path, monkeypatch) -> None:
    matrix_dir = tmp_path / "matrix_success"
    safe_dir = tmp_path / "safe_success"
    monkeypatch.setattr(matrix_module, "_timing_profiles", _fast_timing_profiles)
    monkeypatch.setattr(matrix_module, "_search_combos", _minimal_success_combos)
    monkeypatch.setattr(matrix_module.GasAnalyzer, "COEFFICIENT_COMM_QUIET_DELAY_S", 0.0)

    matrix_result = matrix_module.run_readback_state_matrix(
        port="COM39",
        device_id="079",
        output_dir=matrix_dir,
        confirmation_limit=1,
        confirmation_passes=2,
        serial_factory=_MatrixSerialFactory(enable_explicit_combo=True),
    )
    safe_result = run_safe_readback_session(
        port="COM39",
        device_id="079",
        output_dir=safe_dir,
        repeat_passes=2,
        strategy_names=["historical_prime"],
        inter_command_gap_s=0.0,
        active_settle_s=0.0,
        quiet_window_s=0.0,
        read_timeout_s=0.05,
        read_retries=0,
        serial_factory=_MatrixSerialFactory(enable_explicit_combo=True),
    )

    assert matrix_result["summary"]["explicit_c0_found"] is True
    assert safe_result["summary"]["best_strategy"]["minimum_success"] is True
    assert safe_result["summary"]["best_strategy"]["name"] == "historical_prime"


def test_readback_state_matrix_restores_stream_when_no_combo_succeeds(tmp_path: Path, monkeypatch) -> None:
    out_dir = tmp_path / "matrix_restore"
    monkeypatch.setattr(matrix_module, "_timing_profiles", _fast_timing_profiles)
    monkeypatch.setattr(matrix_module, "_search_combos", _minimal_failure_combos)
    monkeypatch.setattr(matrix_module.GasAnalyzer, "COEFFICIENT_COMM_QUIET_DELAY_S", 0.0)

    result = matrix_module.run_readback_state_matrix(
        port="COM39",
        device_id="079",
        output_dir=out_dir,
        confirmation_limit=1,
        confirmation_passes=2,
        serial_factory=_MatrixSerialFactory(enable_explicit_combo=False),
    )

    summary = json.loads((out_dir / "readback_state_matrix_summary.json").read_text(encoding="utf-8"))
    assert result["summary"]["explicit_c0_found"] is False
    assert summary["best_result"]["post_restore_stream_ok"] is True
