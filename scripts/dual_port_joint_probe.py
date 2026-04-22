from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from gas_calibrator.config import load_config
from gas_calibrator.devices import Pace5000, ParoscientificGauge


ROW_FIELDS = [
    "timestamp",
    "group",
    "step",
    "ports",
    "open_ok",
    "query_ok",
    "raw_result",
    "exception",
    "elapsed_s",
    "step_duration_s",
    "pace_open",
    "gauge_open",
    "delay_s",
]


def _timestamp() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _normalize_text(value: Any) -> str:
    text = str(value or "").strip()
    return text


def _bool_or_none(value: Any) -> Optional[bool]:
    if value is None:
        return None
    return bool(value)


class JointProbeRecorder:
    def __init__(self) -> None:
        self.run_start = time.monotonic()
        self.rows: List[Dict[str, Any]] = []

    def record(
        self,
        *,
        group: str,
        step: str,
        ports: str,
        open_ok: Optional[bool],
        query_ok: Optional[bool],
        raw_result: Any = "",
        exception: Any = "",
        pace_open: bool = False,
        gauge_open: bool = False,
        delay_s: Optional[float] = None,
        step_started: Optional[float] = None,
    ) -> Dict[str, Any]:
        finished = time.monotonic()
        row = {
            "timestamp": _timestamp(),
            "group": group,
            "step": step,
            "ports": ports,
            "open_ok": _bool_or_none(open_ok),
            "query_ok": _bool_or_none(query_ok),
            "raw_result": "" if raw_result is None else str(raw_result),
            "exception": "" if exception is None else str(exception),
            "elapsed_s": round(finished - self.run_start, 6),
            "step_duration_s": round(finished - step_started, 6) if step_started is not None else None,
            "pace_open": bool(pace_open),
            "gauge_open": bool(gauge_open),
            "delay_s": None if delay_s is None else float(delay_s),
        }
        self.rows.append(row)
        print(json.dumps(row, ensure_ascii=False))
        return row


def _load_settings(config_path: Path) -> Dict[str, Any]:
    cfg = load_config(config_path)
    devices = cfg.get("devices", {}) if isinstance(cfg, dict) else {}
    pace_cfg = devices.get("pressure_controller", {}) if isinstance(devices, dict) else {}
    gauge_cfg = devices.get("pressure_gauge", {}) if isinstance(devices, dict) else {}
    return {
        "pace_port": str(pace_cfg.get("port") or "COM31").strip(),
        "pace_baudrate": int(pace_cfg.get("baud", 9600) or 9600),
        "pace_timeout": float(pace_cfg.get("timeout", 1.0) or 1.0),
        "pace_line_ending": pace_cfg.get("line_ending"),
        "pace_query_line_endings": pace_cfg.get("query_line_endings"),
        "pace_pressure_queries": pace_cfg.get("pressure_queries"),
        "gauge_port": str(gauge_cfg.get("port") or "COM30").strip(),
        "gauge_baudrate": int(gauge_cfg.get("baud", 9600) or 9600),
        "gauge_timeout": float(gauge_cfg.get("timeout", 1.0) or 1.0),
        "gauge_dest_id": str(gauge_cfg.get("dest_id") or "01"),
        "gauge_response_timeout_s": float(gauge_cfg.get("response_timeout_s", 2.2) or 2.2),
    }


def _make_pace(settings: Dict[str, Any]) -> Pace5000:
    return Pace5000(
        settings["pace_port"],
        baudrate=settings["pace_baudrate"],
        timeout=settings["pace_timeout"],
        line_ending=settings.get("pace_line_ending"),
        query_line_endings=settings.get("pace_query_line_endings"),
        pressure_queries=settings.get("pace_pressure_queries"),
    )


def _make_gauge(settings: Dict[str, Any]) -> ParoscientificGauge:
    return ParoscientificGauge(
        settings["gauge_port"],
        baudrate=settings["gauge_baudrate"],
        timeout=settings["gauge_timeout"],
        dest_id=settings["gauge_dest_id"],
        response_timeout_s=settings["gauge_response_timeout_s"],
    )


def _write_csv(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=ROW_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in ROW_FIELDS})


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _pace_text_query(dev: Pace5000, command: str) -> str:
    text = _normalize_text(dev.query(command))
    if not text:
        raise RuntimeError("EMPTY_RESPONSE")
    return text


def _record_open(
    recorder: JointProbeRecorder,
    *,
    group: str,
    step: str,
    ports: str,
    opener: Callable[[], None],
    pace_open: bool,
    gauge_open: bool,
    delay_s: Optional[float] = None,
) -> bool:
    started = time.monotonic()
    try:
        opener()
    except Exception as exc:
        failed_pace_open = bool(pace_open)
        failed_gauge_open = bool(gauge_open)
        if step == "open_pace":
            failed_pace_open = False
            failed_gauge_open = False
        elif step == "open_gauge":
            failed_gauge_open = False
        recorder.record(
            group=group,
            step=step,
            ports=ports,
            open_ok=False,
            query_ok=None,
            exception=exc,
            pace_open=failed_pace_open,
            gauge_open=failed_gauge_open,
            delay_s=delay_s,
            step_started=started,
        )
        return False
    recorder.record(
        group=group,
        step=step,
        ports=ports,
        open_ok=True,
        query_ok=None,
        raw_result="OPEN_OK",
        pace_open=pace_open,
        gauge_open=gauge_open,
        delay_s=delay_s,
        step_started=started,
    )
    return True


def _record_close(
    recorder: JointProbeRecorder,
    *,
    group: str,
    step: str,
    ports: str,
    closer: Callable[[], None],
    pace_open: bool,
    gauge_open: bool,
    delay_s: Optional[float] = None,
) -> None:
    started = time.monotonic()
    try:
        closer()
    except Exception as exc:
        recorder.record(
            group=group,
            step=step,
            ports=ports,
            open_ok=False,
            query_ok=None,
            exception=exc,
            pace_open=pace_open,
            gauge_open=gauge_open,
            delay_s=delay_s,
            step_started=started,
        )
        return
    recorder.record(
        group=group,
        step=step,
        ports=ports,
        open_ok=False,
        query_ok=None,
        raw_result="CLOSE_OK",
        pace_open=pace_open,
        gauge_open=gauge_open,
        delay_s=delay_s,
        step_started=started,
    )


def _record_query(
    recorder: JointProbeRecorder,
    *,
    group: str,
    step: str,
    ports: str,
    runner: Callable[[], Any],
    pace_open: bool,
    gauge_open: bool,
    delay_s: Optional[float] = None,
) -> bool:
    started = time.monotonic()
    try:
        result = runner()
        raw_text = _normalize_text(result) if isinstance(result, str) else result
        query_ok = bool(raw_text if isinstance(raw_text, str) else raw_text is not None)
        if not query_ok:
            raise RuntimeError("EMPTY_RESPONSE")
    except Exception as exc:
        recorder.record(
            group=group,
            step=step,
            ports=ports,
            open_ok=pace_open or gauge_open,
            query_ok=False,
            exception=exc,
            pace_open=pace_open,
            gauge_open=gauge_open,
            delay_s=delay_s,
            step_started=started,
        )
        return False
    recorder.record(
        group=group,
        step=step,
        ports=ports,
        open_ok=pace_open or gauge_open,
        query_ok=True,
        raw_result=raw_text,
        pace_open=pace_open,
        gauge_open=gauge_open,
        delay_s=delay_s,
        step_started=started,
    )
    return True


def _sleep_between_steps(delay_s: float) -> None:
    if delay_s > 0:
        time.sleep(float(delay_s))


def _run_group1(recorder: JointProbeRecorder, settings: Dict[str, Any]) -> bool:
    group = "group1"
    pace = _make_pace(settings)
    ok = _record_open(
        recorder,
        group=group,
        step="open_pace",
        ports=settings["pace_port"],
        opener=pace.open,
        pace_open=True,
        gauge_open=False,
    )
    if ok:
        ok = _record_query(
            recorder,
            group=group,
            step="pace_*IDN?",
            ports=settings["pace_port"],
            runner=lambda: _pace_text_query(pace, "*IDN?"),
            pace_open=True,
            gauge_open=False,
        )
    _record_close(
        recorder,
        group=group,
        step="close_pace",
        ports=settings["pace_port"],
        closer=pace.close,
        pace_open=False,
        gauge_open=False,
    )
    return ok


def _run_group2(recorder: JointProbeRecorder, settings: Dict[str, Any]) -> bool:
    group = "group2"
    gauge = _make_gauge(settings)
    ok = _record_open(
        recorder,
        group=group,
        step="open_gauge",
        ports=settings["gauge_port"],
        opener=gauge.open,
        pace_open=False,
        gauge_open=True,
    )
    if ok:
        ok = _record_query(
            recorder,
            group=group,
            step="gauge_read_pressure_fast",
            ports=settings["gauge_port"],
            runner=gauge.read_pressure_fast,
            pace_open=False,
            gauge_open=True,
        )
    _record_close(
        recorder,
        group=group,
        step="close_gauge",
        ports=settings["gauge_port"],
        closer=gauge.close,
        pace_open=False,
        gauge_open=False,
    )
    return ok


def _run_group3(recorder: JointProbeRecorder, settings: Dict[str, Any]) -> bool:
    group = "group3"
    ports = f"{settings['pace_port']}+{settings['gauge_port']}"
    pace = _make_pace(settings)
    gauge = _make_gauge(settings)
    pace_open = _record_open(
        recorder,
        group=group,
        step="open_pace",
        ports=settings["pace_port"],
        opener=pace.open,
        pace_open=True,
        gauge_open=False,
    )
    gauge_open = False
    ok = pace_open
    if pace_open:
        gauge_open = _record_open(
            recorder,
            group=group,
            step="open_gauge",
            ports=ports,
            opener=gauge.open,
            pace_open=True,
            gauge_open=True,
        )
        ok = ok and gauge_open
    if pace_open and gauge_open:
        ok = _record_query(
            recorder,
            group=group,
            step="pace_*IDN?_with_gauge_open",
            ports=ports,
            runner=lambda: _pace_text_query(pace, "*IDN?"),
            pace_open=True,
            gauge_open=True,
        ) and ok
    if gauge_open:
        _record_close(
            recorder,
            group=group,
            step="close_gauge",
            ports=ports,
            closer=gauge.close,
            pace_open=True,
            gauge_open=False,
        )
    _record_close(
        recorder,
        group=group,
        step="close_pace",
        ports=settings["pace_port"],
        closer=pace.close,
        pace_open=False,
        gauge_open=False,
    )
    return ok


def _run_group4_once(
    recorder: JointProbeRecorder,
    settings: Dict[str, Any],
    *,
    delay_s: float,
) -> bool:
    group = f"group4_delay_{delay_s:.1f}s"
    ports = f"{settings['pace_port']}+{settings['gauge_port']}"
    pace = _make_pace(settings)
    gauge = _make_gauge(settings)
    pace_open = _record_open(
        recorder,
        group=group,
        step="open_pace",
        ports=settings["pace_port"],
        opener=pace.open,
        pace_open=True,
        gauge_open=False,
        delay_s=delay_s,
    )
    gauge_open = False
    ok = pace_open
    if pace_open:
        _sleep_between_steps(delay_s)
        gauge_open = _record_open(
            recorder,
            group=group,
            step="open_gauge",
            ports=ports,
            opener=gauge.open,
            pace_open=True,
            gauge_open=True,
            delay_s=delay_s,
        )
        ok = ok and gauge_open

    steps: List[tuple[str, str, Callable[[], Any]]] = [
        ("pace_VENT?", settings["pace_port"], lambda: _pace_text_query(pace, ":SOUR:PRES:LEV:IMM:AMPL:VENT?")),
        ("pace_OUTP:STAT?", settings["pace_port"], lambda: _pace_text_query(pace, ":OUTP:STAT?")),
        ("pace_OUTP:ISOL:STAT?", settings["pace_port"], lambda: _pace_text_query(pace, ":OUTP:ISOL:STAT?")),
        ("pace_read_pressure", settings["pace_port"], pace.read_pressure),
        ("gauge_read_pressure_fast", settings["gauge_port"], gauge.read_pressure_fast),
        ("pace_SYST:ERR?", settings["pace_port"], lambda: _pace_text_query(pace, ":SYST:ERR?")),
    ]

    if pace_open and gauge_open:
        for index, (step_name, step_ports, runner) in enumerate(steps):
            _sleep_between_steps(delay_s)
            step_ok = _record_query(
                recorder,
                group=group,
                step=step_name,
                ports=ports,
                runner=runner,
                pace_open=True,
                gauge_open=True,
                delay_s=delay_s,
            )
            ok = ok and step_ok
            if not step_ok:
                break
            if index == len(steps) - 1:
                continue

    if gauge_open:
        _record_close(
            recorder,
            group=group,
            step="close_gauge",
            ports=ports,
            closer=gauge.close,
            pace_open=True,
            gauge_open=False,
            delay_s=delay_s,
        )
    _record_close(
        recorder,
        group=group,
        step="close_pace",
        ports=settings["pace_port"],
        closer=pace.close,
        pace_open=False,
        gauge_open=False,
        delay_s=delay_s,
    )
    return ok


def _find_first_failure(rows: Sequence[Dict[str, Any]]) -> Optional[str]:
    for row in rows:
        if row.get("query_ok") is False or (row.get("step", "").startswith("open_") and row.get("open_ok") is False):
            return f"{row.get('group')}::{row.get('step')}"
    return None


def _build_likely_trigger(
    *,
    group3_ok: bool,
    group4_ok_delay_0_5s: bool,
    group4_ok_delay_2_0s: Optional[bool],
) -> str:
    if not group3_ok:
        return "只要双开两个串口，联合会话就会失败。"
    if group3_ok and not group4_ok_delay_0_5s and group4_ok_delay_2_0s is True:
        return "双设备联合路径存在明显时序或设备就绪延迟；0.5s 不够，2.0s 可以恢复。"
    if group3_ok and not group4_ok_delay_0_5s and group4_ok_delay_2_0s is False:
        return "不是端口本身，也不只是双开；是 pre-probe 具体查询顺序下的更深层会话或驱动问题。"
    if group3_ok and group4_ok_delay_0_5s:
        return "本次最小联合复现实验未重现故障；原问题更像脚本外部上下文、残留状态或当次进程生命周期问题。"
    return "联合路径触发条件仍需进一步缩小。"


def _build_recommended_next_step(
    *,
    group3_ok: bool,
    group4_ok_delay_0_5s: bool,
    group4_ok_delay_2_0s: Optional[bool],
    first_failing_step: Optional[str],
) -> str:
    if not group3_ok:
        return "先在原 pre-probe 路径外做双开后的逐步 I/O tracing，确认第二端口打开瞬间是否影响另一个端口，不建议直接重跑 B 组。"
    if group3_ok and not group4_ok_delay_0_5s and group4_ok_delay_2_0s is True:
        return "先在联合 pre-probe 中验证双开后增加 2.0s readiness delay 的只读探针，不建议直接重跑 B 组。"
    if group3_ok and not group4_ok_delay_0_5s and group4_ok_delay_2_0s is False:
        return f"先围绕 {first_failing_step or '首个失败查询'} 增加联合 I/O tracing 或更细粒度拆步复现，再决定是否动 B 组。"
    return "先对原脚本增加同路径联合 I/O tracing，对比本次最小复现实验与原 pre-probe 的差异。"


def run_joint_probe(args: argparse.Namespace) -> Dict[str, Any]:
    settings = _load_settings(Path(args.config).resolve())
    output_root = Path(args.output_root).resolve()
    run_dir = output_root / datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    recorder = JointProbeRecorder()
    group1_ok = _run_group1(recorder, settings)
    group2_ok = _run_group2(recorder, settings)
    group3_ok = _run_group3(recorder, settings)
    group4_ok_delay_0_5s = _run_group4_once(recorder, settings, delay_s=float(args.group4_delay_default_s))
    group4_ok_delay_2_0s: Optional[bool] = None
    if not group4_ok_delay_0_5s:
        group4_ok_delay_2_0s = _run_group4_once(recorder, settings, delay_s=float(args.group4_delay_retry_s))

    rows_csv = run_dir / "joint_probe_rows.csv"
    rows_json = run_dir / "joint_probe_rows.json"
    summary_json = run_dir / "joint_probe_summary.json"

    _write_csv(rows_csv, recorder.rows)
    _write_json(rows_json, recorder.rows)

    first_failing_step = _find_first_failure(recorder.rows)
    summary = {
        "config_path": str(Path(args.config).resolve()),
        "pace_port": settings["pace_port"],
        "gauge_port": settings["gauge_port"],
        "group1_ok": bool(group1_ok),
        "group2_ok": bool(group2_ok),
        "group3_ok": bool(group3_ok),
        "group4_ok_delay_0_5s": bool(group4_ok_delay_0_5s),
        "group4_ok_delay_2_0s": group4_ok_delay_2_0s,
        "first_failing_step": first_failing_step,
        "likely_trigger": _build_likely_trigger(
            group3_ok=bool(group3_ok),
            group4_ok_delay_0_5s=bool(group4_ok_delay_0_5s),
            group4_ok_delay_2_0s=group4_ok_delay_2_0s,
        ),
        "recommended_next_step": _build_recommended_next_step(
            group3_ok=bool(group3_ok),
            group4_ok_delay_0_5s=bool(group4_ok_delay_0_5s),
            group4_ok_delay_2_0s=group4_ok_delay_2_0s,
            first_failing_step=first_failing_step,
        ),
        "rows_csv_path": str(rows_csv),
        "rows_json_path": str(rows_json),
        "summary_json_path": str(summary_json),
    }
    _write_json(summary_json, summary)
    print(f"joint_probe_rows_csv={rows_csv}")
    print(f"joint_probe_rows_json={rows_json}")
    print(f"joint_probe_summary_json={summary_json}")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return summary


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dual-port minimal joint probe for PACE and pressure gauge.")
    parser.add_argument("--config", default=str(ROOT / "configs" / "default_config.json"))
    parser.add_argument("--output-root", default=str(ROOT / "results" / "dual_port_joint_probe"))
    parser.add_argument("--group4-delay-default-s", type=float, default=0.5)
    parser.add_argument("--group4-delay-retry-s", type=float, default=2.0)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Sequence[str]] = None) -> int:
    run_joint_probe(parse_args(argv))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
