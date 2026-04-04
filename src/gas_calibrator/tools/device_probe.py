"""On-bench single-device probe helper."""

from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from ..config import load_config
from ..devices import Pace5000, ParoscientificGauge, Thermometer
from ..logging_utils import RunLogger


def _make_logger(cfg: Dict[str, Any]) -> RunLogger:
    out_dir = Path(str(cfg.get("paths", {}).get("output_dir", "logs")))
    return RunLogger(out_dir)


def _probe_pressure_controller(dev_cfg: Dict[str, Any], io_logger: Any) -> Dict[str, Any]:
    dev = Pace5000(
        dev_cfg["port"],
        int(dev_cfg.get("baud", 9600)),
        timeout=float(dev_cfg.get("timeout", 1.0)),
        line_ending=dev_cfg.get("line_ending"),
        query_line_endings=dev_cfg.get("query_line_endings"),
        pressure_queries=dev_cfg.get("pressure_queries"),
        io_logger=io_logger,
    )
    try:
        dev.open()
        value = dev.read_pressure()
        return {"ok": True, "pressure_hpa": value}
    except Exception as exc:
        return {"ok": False, "err": str(exc)}
    finally:
        try:
            dev.close()
        except Exception:
            pass


def _probe_pressure_gauge(dev_cfg: Dict[str, Any], io_logger: Any) -> Dict[str, Any]:
    dev = ParoscientificGauge(
        dev_cfg["port"],
        int(dev_cfg.get("baud", 9600)),
        timeout=float(dev_cfg.get("timeout", 1.0)),
        dest_id=str(dev_cfg.get("dest_id", "01")),
        response_timeout_s=dev_cfg.get("response_timeout_s"),
        io_logger=io_logger,
    )
    try:
        dev.open()
        value = dev.read_pressure()
        return {"ok": True, "pressure_hpa": value, "dest_id": str(dev_cfg.get("dest_id", "01"))}
    except Exception as exc:
        return {"ok": False, "err": str(exc), "dest_id": str(dev_cfg.get("dest_id", "01"))}
    finally:
        try:
            dev.close()
        except Exception:
            pass


def _scan_pressure_gauge_ids(
    dev_cfg: Dict[str, Any],
    io_logger: Any,
    *,
    max_id: int = 31,
    wait_s: float = 3.0,
) -> List[Dict[str, Any]]:
    gauge = ParoscientificGauge(
        dev_cfg["port"],
        int(dev_cfg.get("baud", 9600)),
        timeout=float(dev_cfg.get("timeout", 1.0)),
        dest_id="00",
        response_timeout_s=dev_cfg.get("response_timeout_s"),
        io_logger=io_logger,
    )
    hits: List[Dict[str, Any]] = []
    try:
        gauge.open()
        for i in range(max(0, int(max_id)) + 1):
            did = f"{i:02d}"
            gauge.dest_id = did
            cmd = gauge._cmd("P3")
            echo = cmd.strip().upper()

            lines: List[str] = []
            try:
                gauge.ser.write(cmd)
                deadline = time.time() + max(0.2, float(wait_s))
                while time.time() < deadline:
                    raw = gauge.ser.readline()
                    text = (raw or "").strip()
                    if not text:
                        continue
                    lines.append(text)
                    if text.upper() == echo:
                        continue
                    value = gauge._parse_pressure_value(text)
                    if value is not None:
                        hit = {
                            "dest_id": did,
                            "pressure_hpa": value,
                            "line": text,
                            "lines": list(lines),
                        }
                        hits.append(hit)
                        break
            except Exception:
                continue
    finally:
        try:
            gauge.close()
        except Exception:
            pass
    return hits


def _candidate_thermo_settings(dev_cfg: Dict[str, Any], try_all: bool) -> List[Tuple[int, str, int, float, float]]:
    base = (
        int(dev_cfg.get("baud", 2400)),
        str(dev_cfg.get("parity", "N")).upper(),
        int(dev_cfg.get("bytesize", 8)),
        float(dev_cfg.get("stopbits", 1)),
        float(dev_cfg.get("timeout", 1.2)),
    )
    settings = [base]
    if try_all:
        settings.extend(
            [
                (2400, "N", 8, 1.0, 1.2),
                (2400, "E", 7, 1.0, 1.2),
                (2400, "E", 8, 1.0, 1.2),
                (1200, "N", 8, 1.0, 1.2),
                (9600, "N", 8, 1.0, 1.2),
            ]
        )

    out: List[Tuple[int, str, int, float, float]] = []
    seen = set()
    for one in settings:
        if one in seen:
            continue
        seen.add(one)
        out.append(one)
    return out


def _probe_thermometer(
    dev_cfg: Dict[str, Any],
    io_logger: Any,
    *,
    duration_s: float,
    try_all: bool,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    settings = _candidate_thermo_settings(dev_cfg, try_all)
    port = str(dev_cfg["port"])

    for baud, parity, bytesize, stopbits, timeout_s in settings:
        dev = Thermometer(
            port,
            baudrate=baud,
            timeout=timeout_s,
            parity=parity,
            stopbits=stopbits,
            bytesize=bytesize,
            io_logger=io_logger,
        )
        sample_count = 0
        ok_count = 0
        nonempty_count = 0
        last_raw = ""
        last_temp = None
        err = None
        try:
            dev.open()
            dev.flush_input()
            deadline = time.time() + max(1.0, float(duration_s))
            while time.time() < deadline:
                row = dev.read_current()
                sample_count += 1
                raw = str(row.get("raw", "") or "")
                if raw:
                    nonempty_count += 1
                    last_raw = raw
                if row.get("ok"):
                    ok_count += 1
                    last_temp = row.get("temp_c")
                time.sleep(0.15)
        except Exception as exc:
            err = str(exc)
        finally:
            try:
                dev.close()
            except Exception:
                pass

        out.append(
            {
                "port": port,
                "baud": baud,
                "parity": parity,
                "bytesize": bytesize,
                "stopbits": stopbits,
                "timeout": timeout_s,
                "samples": sample_count,
                "ok_frames": ok_count,
                "nonempty_frames": nonempty_count,
                "last_temp_c": last_temp,
                "last_raw": last_raw,
                "err": err,
            }
        )
    return out


def _print_header(title: str) -> None:
    print("")
    print("=" * 72)
    print(title)
    print("=" * 72)


def _run_pressure_mode(cfg: Dict[str, Any], logger: RunLogger, scan_ids: bool, scan_max_id: int, scan_wait_s: float) -> int:
    dcfg = cfg.get("devices", {})
    pcfg = dcfg.get("pressure_controller", {})
    gcfg = dcfg.get("pressure_gauge", {})

    _print_header("Pressure Controller Probe")
    pc = _probe_pressure_controller(pcfg, logger)
    print(pc)

    _print_header("Digital Pressure Gauge Probe")
    pg = _probe_pressure_gauge(gcfg, logger)
    print(pg)

    if scan_ids:
        _print_header(f"Digital Pressure Gauge ID Scan (0..{scan_max_id})")
        hits = _scan_pressure_gauge_ids(gcfg, logger, max_id=scan_max_id, wait_s=scan_wait_s)
        if hits:
            print("ID hits:")
            for item in hits:
                print(
                    f"  dest_id={item['dest_id']} pressure={item['pressure_hpa']} line={item['line']}"
                )
        else:
            print("No ID hit.")
    return 0


def _run_thermometer_mode(cfg: Dict[str, Any], logger: RunLogger, duration_s: float, try_all: bool) -> int:
    dcfg = cfg.get("devices", {})
    tcfg = dcfg.get("thermometer", {})

    _print_header("Thermometer Probe")
    rows = _probe_thermometer(tcfg, logger, duration_s=duration_s, try_all=try_all)
    for row in rows:
        print(row)
    return 0


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="On-bench probe tool")
    parser.add_argument("--config", default="configs/default_config.json")

    sub = parser.add_subparsers(dest="mode", required=True)

    p_pressure = sub.add_parser("pressure", help="Probe pressure controller and pressure gauge")
    p_pressure.add_argument("--scan-ids", action="store_true", help="Scan gauge ID 00..N")
    p_pressure.add_argument("--scan-max-id", type=int, default=31)
    p_pressure.add_argument("--scan-wait-s", type=float, default=3.0)

    p_thermo = sub.add_parser("thermometer", help="Probe thermometer")
    p_thermo.add_argument("--duration-s", type=float, default=6.0)
    p_thermo.add_argument("--try-all", action="store_true", help="Try extra serial settings")

    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    ns = parse_args(argv)
    cfg = load_config(ns.config)
    logger = _make_logger(cfg)
    print(f"run_dir={logger.run_dir}")
    try:
        if ns.mode == "pressure":
            return _run_pressure_mode(
                cfg,
                logger,
                scan_ids=bool(ns.scan_ids),
                scan_max_id=int(ns.scan_max_id),
                scan_wait_s=float(ns.scan_wait_s),
            )
        if ns.mode == "thermometer":
            return _run_thermometer_mode(
                cfg,
                logger,
                duration_s=float(ns.duration_s),
                try_all=bool(ns.try_all),
            )
        print(f"Unknown mode: {ns.mode}")
        return 2
    finally:
        logger.close()


if __name__ == "__main__":
    raise SystemExit(main())

