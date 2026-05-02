from __future__ import annotations

import argparse
import csv
import json
import re
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


PROBE_ROW_FIELDS = [
    "timestamp",
    "sample_index",
    "elapsed_s",
    "probe_pass",
    "probe_fail_due_to_comm",
    "probe_warning_only",
    "first_failing_step",
    "vent_status",
    "outp_state",
    "isol_state",
    "pace_pressure_hpa",
    "gauge_pressure_hpa",
    "device_idn",
    "syst_err",
    "warnings",
    "errors",
]

TRACE_ROW_FIELDS = [
    "timestamp",
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

WARNING_ONLY_SYST_ERR_CODES = {-102}
DEFAULT_OUTPUT_ROOT = ROOT / "results" / "vent_refresh_ab" / "pre_probe"


def _timestamp() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _parse_first_float(text: Any) -> Optional[float]:
    raw = _normalize_text(text)
    if not raw:
        return None
    match = re.search(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", raw)
    if not match:
        return None
    try:
        return float(match.group(0))
    except Exception:
        return None


def _parse_first_int(text: Any) -> Optional[int]:
    value = _parse_first_float(text)
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _parse_syst_err_code(text: Any) -> Optional[int]:
    raw = _normalize_text(text)
    if not raw:
        return None
    match = re.search(r"(?<!\d)-?\d+", raw)
    if not match:
        return None
    try:
        return int(match.group(0))
    except Exception:
        return None


def _write_csv(path: Path, rows: Sequence[Dict[str, Any]], fieldnames: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


class TraceRecorder:
    def __init__(self) -> None:
        self.start_mono = time.monotonic()
        self.rows: List[Dict[str, Any]] = []

    def record(
        self,
        *,
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
            "step": step,
            "ports": ports,
            "open_ok": None if open_ok is None else bool(open_ok),
            "query_ok": None if query_ok is None else bool(query_ok),
            "raw_result": "" if raw_result is None else str(raw_result),
            "exception": "" if exception is None else str(exception),
            "elapsed_s": round(finished - self.start_mono, 6),
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


def _record_open(
    recorder: TraceRecorder,
    *,
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


def _record_delay(
    recorder: TraceRecorder,
    *,
    step: str,
    ports: str,
    delay_s: float,
    pace_open: bool,
    gauge_open: bool,
) -> None:
    started = time.monotonic()
    if delay_s > 0:
        time.sleep(float(delay_s))
    recorder.record(
        step=step,
        ports=ports,
        open_ok=pace_open or gauge_open,
        query_ok=None,
        raw_result=f"SLEPT_{float(delay_s):.1f}s",
        pace_open=pace_open,
        gauge_open=gauge_open,
        delay_s=delay_s,
        step_started=started,
    )


def _record_query(
    recorder: TraceRecorder,
    *,
    step: str,
    ports: str,
    runner: Callable[[], Any],
    pace_open: bool,
    gauge_open: bool,
    delay_s: Optional[float] = None,
) -> tuple[bool, Any]:
    started = time.monotonic()
    try:
        result = runner()
    except Exception as exc:
        recorder.record(
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
        return False, None

    raw_text = result if not isinstance(result, str) else _normalize_text(result)
    ok = bool(raw_text if isinstance(raw_text, str) else raw_text is not None)
    if not ok:
        recorder.record(
            step=step,
            ports=ports,
            open_ok=pace_open or gauge_open,
            query_ok=False,
            exception="EMPTY_RESPONSE",
            pace_open=pace_open,
            gauge_open=gauge_open,
            delay_s=delay_s,
            step_started=started,
        )
        return False, None

    recorder.record(
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
    return True, raw_text


def _record_close(
    recorder: TraceRecorder,
    *,
    step: str,
    ports: str,
    closer: Callable[[], None],
    pace_open: bool,
    gauge_open: bool,
) -> None:
    started = time.monotonic()
    try:
        closer()
    except Exception as exc:
        recorder.record(
            step=step,
            ports=ports,
            open_ok=False,
            query_ok=None,
            exception=exc,
            pace_open=pace_open,
            gauge_open=gauge_open,
            step_started=started,
        )
        return
    recorder.record(
        step=step,
        ports=ports,
        open_ok=False,
        query_ok=None,
        raw_result="CLOSE_OK",
        pace_open=pace_open,
        gauge_open=gauge_open,
        step_started=started,
    )


def _system_error_to_warning(text: Any) -> str:
    raw = _normalize_text(text)
    if not raw:
        return ""
    code = _parse_syst_err_code(raw)
    if code in WARNING_ONLY_SYST_ERR_CODES:
        return f"syst_err_warning:{raw}"
    if code is None:
        return f"syst_err_warning:{raw}"
    if code == 0 or "no error" in raw.lower():
        return ""
    return f"syst_err_warning:{raw}"


def run_preprobe(
    *,
    config_path: Path,
    output_root: Path,
    delay_s: float = 0.5,
    pace_factory: Optional[Callable[[Dict[str, Any]], Pace5000]] = None,
    gauge_factory: Optional[Callable[[Dict[str, Any]], ParoscientificGauge]] = None,
) -> Dict[str, Any]:
    settings = _load_settings(config_path)
    run_dir = output_root / datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    recorder = TraceRecorder()
    warnings: List[str] = []
    errors: List[str] = []
    probe_pass = False
    probe_fail_due_to_comm = False
    probe_warning_only = False
    first_failing_step = ""
    row = {
        "timestamp": _timestamp(),
        "sample_index": 0,
        "elapsed_s": 0.0,
        "probe_pass": False,
        "probe_fail_due_to_comm": False,
        "probe_warning_only": False,
        "first_failing_step": "",
        "vent_status": None,
        "outp_state": None,
        "isol_state": None,
        "pace_pressure_hpa": None,
        "gauge_pressure_hpa": None,
        "device_idn": "",
        "syst_err": "",
        "warnings": [],
        "errors": [],
    }

    pace = None
    gauge = None
    pace_open = False
    gauge_open = False
    ports = f"{settings['pace_port']}+{settings['gauge_port']}"

    try:
        pace = (pace_factory or _make_pace)(settings)
        pace_open = _record_open(
            recorder,
            step="open_pace",
            ports=settings["pace_port"],
            opener=pace.open,
            pace_open=True,
            gauge_open=False,
        )
        if not pace_open:
            first_failing_step = "open_pace"
            errors.append("pace_open_failed")
            probe_fail_due_to_comm = True
        else:
            _record_delay(
                recorder,
                step="delay_after_open_pace",
                ports=settings["pace_port"],
                delay_s=delay_s,
                pace_open=True,
                gauge_open=False,
            )

            gauge = (gauge_factory or _make_gauge)(settings)
            gauge_open = _record_open(
                recorder,
                step="open_gauge",
                ports=ports,
                opener=gauge.open,
                pace_open=True,
                gauge_open=True,
            )
            if not gauge_open:
                first_failing_step = "open_gauge"
                errors.append("gauge_open_failed")
                probe_fail_due_to_comm = True
            else:
                _record_delay(
                    recorder,
                    step="delay_after_open_gauge",
                    ports=ports,
                    delay_s=delay_s,
                    pace_open=True,
                    gauge_open=True,
                )

                hard_steps: List[tuple[str, Callable[[], Any], Callable[[Any], Any]]] = [
                    ("pace_vent_status", pace.get_vent_status, _parse_first_int),
                    ("pace_outp_state", pace.get_output_state, _parse_first_int),
                    ("pace_isol_state", pace.get_isolation_state, _parse_first_int),
                    ("pace_read_pressure", pace.read_pressure, _parse_first_float),
                    ("gauge_read_pressure_fast", gauge.read_pressure_fast, _parse_first_float),
                ]
                field_map = {
                    "pace_vent_status": "vent_status",
                    "pace_outp_state": "outp_state",
                    "pace_isol_state": "isol_state",
                    "pace_read_pressure": "pace_pressure_hpa",
                    "gauge_read_pressure_fast": "gauge_pressure_hpa",
                }

                hard_fail = False
                for step_name, runner, parser in hard_steps:
                    ok, raw_result = _record_query(
                        recorder,
                        step=step_name,
                        ports=ports,
                        runner=runner,
                        pace_open=True,
                        gauge_open=True,
                        delay_s=delay_s,
                    )
                    if not ok:
                        first_failing_step = step_name
                        errors.append(f"{step_name}_failed")
                        probe_fail_due_to_comm = True
                        hard_fail = True
                        break
                    row[field_map[step_name]] = parser(raw_result)

                if not hard_fail:
                    ok, device_idn = _record_query(
                        recorder,
                        step="pace_device_idn",
                        ports=ports,
                        runner=lambda: pace.query("*IDN?"),
                        pace_open=True,
                        gauge_open=True,
                        delay_s=delay_s,
                    )
                    if ok:
                        row["device_idn"] = _normalize_text(device_idn)
                    else:
                        warnings.append("device_idn_warning")

                    ok, syst_err = _record_query(
                        recorder,
                        step="pace_syst_err",
                        ports=ports,
                        runner=lambda: pace.query(":SYST:ERR?"),
                        pace_open=True,
                        gauge_open=True,
                        delay_s=delay_s,
                    )
                    if ok:
                        row["syst_err"] = _normalize_text(syst_err)
                        warning = _system_error_to_warning(syst_err)
                        if warning:
                            warnings.append(warning)
                    else:
                        warnings.append("syst_err_query_warning")

                    probe_pass = True
                    probe_warning_only = bool(warnings)
    finally:
        if gauge is not None and gauge_open:
            _record_close(
                recorder,
                step="close_gauge",
                ports=ports,
                closer=gauge.close,
                pace_open=True,
                gauge_open=False,
            )
        if pace is not None and pace_open:
            _record_close(
                recorder,
                step="close_pace",
                ports=settings["pace_port"],
                closer=pace.close,
                pace_open=False,
                gauge_open=False,
            )

    return _finalize_preprobe(
        recorder=recorder,
        run_dir=run_dir,
        row=row,
        settings=settings,
        warnings=warnings,
        errors=errors,
        probe_pass=probe_pass,
        probe_fail_due_to_comm=probe_fail_due_to_comm,
        probe_warning_only=probe_warning_only,
        first_failing_step=first_failing_step,
    )


def _finalize_preprobe(
    *,
    recorder: TraceRecorder,
    run_dir: Path,
    row: Dict[str, Any],
    settings: Dict[str, Any],
    warnings: Sequence[str],
    errors: Sequence[str],
    probe_pass: bool,
    probe_fail_due_to_comm: bool,
    probe_warning_only: bool,
    first_failing_step: str,
) -> Dict[str, Any]:
    row["probe_pass"] = bool(probe_pass)
    row["probe_fail_due_to_comm"] = bool(probe_fail_due_to_comm)
    row["probe_warning_only"] = bool(probe_warning_only)
    row["first_failing_step"] = str(first_failing_step or "")
    row["warnings"] = list(warnings)
    row["errors"] = list(errors)
    if recorder.rows:
        row["timestamp"] = recorder.rows[-1]["timestamp"]
        row["elapsed_s"] = recorder.rows[-1]["elapsed_s"]

    probe_rows_path = run_dir / "probe_rows.csv"
    probe_rows_json_path = run_dir / "probe_rows.json"
    trace_rows_path = run_dir / "probe_trace_rows.csv"
    trace_rows_json_path = run_dir / "probe_trace_rows.json"
    summary_path = run_dir / "probe_summary.json"

    probe_rows = [row]
    _write_csv(probe_rows_path, probe_rows, PROBE_ROW_FIELDS)
    _write_json(probe_rows_json_path, probe_rows)
    _write_csv(trace_rows_path, recorder.rows, TRACE_ROW_FIELDS)
    _write_json(trace_rows_json_path, recorder.rows)

    summary = {
        "pace_port": settings["pace_port"],
        "gauge_port": settings["gauge_port"],
        "probe_pass": bool(probe_pass),
        "probe_fail_due_to_comm": bool(probe_fail_due_to_comm),
        "probe_warning_only": bool(probe_warning_only),
        "first_failing_step": str(first_failing_step or ""),
        "warning_count": len(list(warnings)),
        "warnings": list(warnings),
        "error_count": len(list(errors)),
        "errors": list(errors),
        "probe_rows_csv": str(probe_rows_path),
        "probe_rows_json": str(probe_rows_json_path),
        "probe_trace_rows_csv": str(trace_rows_path),
        "probe_trace_rows_json": str(trace_rows_json_path),
        "device_idn": row.get("device_idn") or "",
        "vent_status": row.get("vent_status"),
        "outp_state": row.get("outp_state"),
        "isol_state": row.get("isol_state"),
        "pace_pressure_hpa": row.get("pace_pressure_hpa"),
        "gauge_pressure_hpa": row.get("gauge_pressure_hpa"),
        "syst_err": row.get("syst_err") or "",
        "summary_json": str(summary_path),
    }
    _write_json(summary_path, summary)
    print(f"probe_rows_csv={probe_rows_path}")
    print(f"probe_rows_json={probe_rows_json_path}")
    print(f"probe_trace_rows_csv={trace_rows_path}")
    print(f"probe_trace_rows_json={trace_rows_json_path}")
    print(f"probe_summary_json={summary_path}")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return summary


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reliable pre-probe for vent refresh A/B before any live rerun.")
    parser.add_argument("--config", default=str(ROOT / "configs" / "default_config.json"))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--delay-s", type=float, default=0.5)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    run_preprobe(
        config_path=Path(args.config).resolve(),
        output_root=Path(args.output_root).resolve(),
        delay_s=float(args.delay_s),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
