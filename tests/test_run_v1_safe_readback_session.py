from __future__ import annotations

import json
from pathlib import Path

from gas_calibrator.devices.serial_base import ReplaySerial
from gas_calibrator.tools.run_v1_safe_readback_session import run_safe_readback_session


class _SafeSessionFactory:
    def __init__(
        self,
        *,
        getco_success_groups: set[int],
        ambiguous_success_groups: set[int] | None = None,
        fail_restore: bool = False,
    ) -> None:
        self.getco_success_groups = set(getco_success_groups)
        self.ambiguous_success_groups = {int(group) for group in set(ambiguous_success_groups or set())}
        self.fail_restore = bool(fail_restore)

    def __call__(self, **kwargs):
        serial = ReplaySerial(**kwargs, on_write=self._on_write)
        serial.session_mode = 1
        serial.active_send = True
        serial.restore_fail = self.fail_restore
        serial.queue_line("YGAS,079,0794.987,00.000,0.99,0.99,031.94,104.20,0001,2781")
        return serial

    def _on_write(self, payload: bytes, serial: ReplaySerial) -> None:
        text = payload.decode("ascii", errors="ignore").strip()
        if text.startswith("MODE,YGAS,FFF,2"):
            serial.session_mode = 2
            serial.queue_line("YGAS,079,T")
            return
        if text.startswith("MODE,YGAS,FFF,1"):
            serial.session_mode = 1
            if not serial.restore_fail:
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
            if not serial.restore_fail:
                serial.queue_line("YGAS,079,T")
            serial.queue_line("YGAS,079,0795.043,00.000,0.99,0.99,031.96,104.20,0001,2767")
            return
        if text.startswith("SETCOMWAY,YGAS,FFF,0"):
            serial.active_send = False
            serial.queue_line("YGAS,079,T")
            return
        if text.startswith("GETCO,YGAS,"):
            group = int(text.split(",")[-1])
            if group in self.ambiguous_success_groups:
                serial.queue_line(
                    "YGAS,079,0793.000,00.000,0.99,0.99,031.94,104.20,0001,2781 "
                    "C0:19846.2,C1:-38766.1,C2:22273.1,C3:-3565.31,C4:0,C5:0"
                )
                return
            if group in self.getco_success_groups:
                if group == 1:
                    serial.queue_line(
                        "YGAS,079,0793.000,00.000,0.99,0.99,031.94,104.20,0001,2781\r\n"
                        "C0:19846.2,C1:-38766.1,C2:22273.1,C3:-3565.31,C4:0,C5:0"
                    )
                elif group == 3:
                    serial.queue_line("C0:24.8028,C1:0.000504508,C2:-16.6009,C3:0,C4:0,C5:0")
                elif group == 7:
                    serial.queue_line("C0:-1.50402,C1:0.975407,C2:0.00190803,C3:-4.78878e-05")
                elif group == 8:
                    serial.queue_line("C0:-1.48652,C1:0.955634,C2:0.00263834,C3:-5.58578e-05")
                return
            serial.queue_line("YGAS,079,0792.490,00.000,0.99,0.99,031.95,104.19,0001,2777")


def test_safe_readback_session_handles_mixed_stream_and_c0_lines(tmp_path: Path) -> None:
    out_dir = tmp_path / "safe_readback"

    result = run_safe_readback_session(
        port="COM39",
        device_id="079",
        output_dir=out_dir,
        repeat_passes=2,
        strategy_names=["quiet_only"],
        serial_factory=_SafeSessionFactory(getco_success_groups={1, 3, 7, 8}),
    )

    best = result["summary"]["best_strategy"]
    assert best["name"] == "quiet_only"
    assert best["backup_ready"] is True

    groups_payload = json.loads((out_dir / "safe_readback_summary.json").read_text(encoding="utf-8"))
    assert groups_payload["live_backup_ready"] is True
    assert "1" in groups_payload["best_strategy"]["stable_group_names"]
    assert "7" in groups_payload["best_strategy"]["stable_group_names"]


def test_safe_readback_session_restores_when_getco_never_succeeds(tmp_path: Path) -> None:
    out_dir = tmp_path / "safe_readback_restore"

    result = run_safe_readback_session(
        port="COM39",
        device_id="079",
        output_dir=out_dir,
        repeat_passes=2,
        strategy_names=["historical_prime"],
        serial_factory=_SafeSessionFactory(getco_success_groups=set()),
    )

    assert result["summary"]["live_backup_ready"] is False
    restore_payload = json.loads((out_dir / "safe_readback_restore_summary.json").read_text(encoding="utf-8"))
    assert restore_payload["rows"][0]["post_restore_stream_ok"] is True
    raw_log = (out_dir / "safe_readback_raw.log").read_text(encoding="utf-8")
    assert "restore_comm_way" in raw_log


def test_safe_readback_session_summary_marks_group_success(tmp_path: Path) -> None:
    out_dir = tmp_path / "safe_readback_success"

    run_safe_readback_session(
        port="COM39",
        device_id="079",
        output_dir=out_dir,
        repeat_passes=2,
        strategy_names=["quiet_only"],
        serial_factory=_SafeSessionFactory(getco_success_groups={1, 7}),
    )

    groups_csv = (out_dir / "safe_readback_groups.csv").read_text(encoding="utf-8-sig")
    summary_payload = json.loads((out_dir / "safe_readback_summary.json").read_text(encoding="utf-8"))

    assert "quiet_only,1,True" in groups_csv
    assert "quiet_only,7,True" in groups_csv
    assert summary_payload["best_strategy"]["minimum_success"] is True
    assert summary_payload["best_strategy"]["backup_ready"] is False


def test_safe_readback_session_requires_explicit_c0_source(tmp_path: Path) -> None:
    out_dir = tmp_path / "safe_readback_ambiguous"

    run_safe_readback_session(
        port="COM39",
        device_id="079",
        output_dir=out_dir,
        repeat_passes=2,
        strategy_names=["quiet_only"],
        serial_factory=_SafeSessionFactory(getco_success_groups=set(), ambiguous_success_groups={1, 7}),
    )

    summary_payload = json.loads((out_dir / "safe_readback_summary.json").read_text(encoding="utf-8"))
    groups_payload = summary_payload["strategy_results"][0]["groups"]

    assert summary_payload["live_backup_ready"] is False
    assert groups_payload["1"]["stable"] is False
    assert groups_payload["7"]["stable"] is False
