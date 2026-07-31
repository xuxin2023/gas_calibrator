"""On-bench single-device probe helper."""

from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Tuple

from ..config import load_config
from ..devices import GasAnalyzer, Pace5000, ParoscientificGauge, Thermometer
from ..logging_utils import RunLogger
from .run_v1_5_analyzer_runtime_setup import _read_sn


def _make_logger(cfg: Dict[str, Any], output_dir: str | None = None) -> RunLogger:
    configured = cfg.get("paths", {}).get("output_dir", "logs")
    out_dir = Path(str(output_dir or configured))
    return RunLogger(out_dir)


def _probe_pressure_controller(
    dev_cfg: Dict[str, Any],
    io_logger: Any,
    *,
    exhaustive: bool = False,
) -> Dict[str, Any]:
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
        if exhaustive:
            value = dev.read_pressure()
            return {"ok": True, "pressure_hpa": value, "probe_mode": "exhaustive"}

        # Keep bench discovery bounded when a stale COM binding points at an
        # open-but-silent serial card channel. The production read path
        # intentionally tries a large compatibility matrix; the probe uses a
        # small query-only whitelist and fails fast so binding can be corrected.
        attempts: List[Dict[str, str]] = []
        for command in (":SENS:PRES?", ":MEAS:PRES?"):
            response = str(dev.query(command) or "").strip()
            attempts.append({"command": command, "response": response})
            value = dev._parse_first_float(response)
            if value is not None:
                return {
                    "ok": True,
                    "pressure_hpa": float(value),
                    "probe_mode": "bounded_query_only",
                    "matched_command": command,
                }
        return {
            "ok": False,
            "err": "NO_RESPONSE_OR_PARSE",
            "probe_mode": "bounded_query_only",
            "attempts": attempts,
        }
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


def _normalized_analyzer_id(value: Any) -> str:
    text = str(value or "").strip()
    if text.isdigit():
        return f"{int(text):03d}"
    return text.upper()


def _serial_port_metadata() -> Dict[str, Dict[str, str]]:
    """Return stable USB/FTDI metadata without opening any serial port."""

    try:
        from serial.tools import list_ports

        ports = list_ports.comports()
    except Exception:
        return {}
    return {
        str(item.device).upper(): {
            "description": str(item.description or ""),
            "hardware_id": str(item.hwid or ""),
            "usb_serial_number": str(item.serial_number or ""),
        }
        for item in ports
        if str(getattr(item, "device", "") or "").strip()
    }


def _enabled_analyzer_configs(cfg: Mapping[str, Any]) -> List[Dict[str, Any]]:
    devices = cfg.get("devices", {}) if isinstance(cfg, Mapping) else {}
    analyzers = devices.get("gas_analyzers", []) if isinstance(devices, Mapping) else []
    if isinstance(analyzers, list) and analyzers:
        return [
            dict(item)
            for item in analyzers
            if isinstance(item, Mapping) and bool(item.get("enabled", True))
        ]
    single = devices.get("gas_analyzer", {}) if isinstance(devices, Mapping) else {}
    if isinstance(single, Mapping) and bool(single.get("enabled", False)):
        return [dict(single)]
    return []


def _probe_analyzers_receive_only(
    cfg: Mapping[str, Any],
    io_logger: Any,
    *,
    duration_per_port_s: float,
) -> Dict[str, Any]:
    """Inventory analyzer streams by COM port without transmitting a byte."""

    port_metadata = _serial_port_metadata()
    rows: List[Dict[str, Any]] = []
    observed_id_ports: Dict[str, List[str]] = {}
    configured_id_ports: Dict[str, List[str]] = {}

    for index, analyzer_cfg in enumerate(_enabled_analyzer_configs(cfg), start=1):
        port = str(analyzer_cfg.get("port") or "").strip()
        configured_id = _normalized_analyzer_id(analyzer_cfg.get("device_id"))
        configured_id_ports.setdefault(configured_id, []).append(port)
        dev = GasAnalyzer(
            port,
            int(analyzer_cfg.get("baud", analyzer_cfg.get("baudrate", 115200)) or 115200),
            timeout=float(analyzer_cfg.get("timeout", 1.0) or 1.0),
            device_id=configured_id,
            io_logger=io_logger,
        )
        lines: List[str] = []
        parsed_frames: List[Dict[str, Any]] = []
        error = ""
        opened = False
        try:
            dev.open()
            opened = True
            lines = list(
                dev.ser.drain_input_nonblock(
                    drain_s=max(0.05, float(duration_per_port_s)),
                    read_timeout_s=0.05,
                )
            )
            for line in lines:
                parsed = dev.parse_line(line)
                if isinstance(parsed, Mapping):
                    parsed_frames.append(dict(parsed))
        except Exception as exc:
            error = str(exc)
        finally:
            try:
                dev.close()
            except Exception:
                pass

        observed_ids = sorted(
            {
                _normalized_analyzer_id(frame.get("id"))
                for frame in parsed_frames
                if _normalized_analyzer_id(frame.get("id"))
            }
        )
        for observed_id in observed_ids:
            observed_id_ports.setdefault(observed_id, []).append(port)
        observed_modes = sorted(
            {int(frame["mode"]) for frame in parsed_frames if frame.get("mode") is not None}
        )
        last = parsed_frames[-1] if parsed_frames else {}
        if not observed_ids:
            identity_matches: bool | None = None
        else:
            identity_matches = observed_ids == [configured_id]
        rows.append(
            {
                "analyzer_name": str(analyzer_cfg.get("name") or f"ga{index:02d}"),
                "port": port,
                "baud": int(analyzer_cfg.get("baud", analyzer_cfg.get("baudrate", 115200)) or 115200),
                "configured_device_id": configured_id,
                "open_ok": opened,
                "streaming": bool(parsed_frames),
                "received_line_count": len(lines),
                "parsed_frame_count": len(parsed_frames),
                "observed_device_ids": observed_ids,
                "observed_modes": observed_modes,
                "configured_identity_matches": identity_matches,
                "last_measurement": {
                    key: last.get(key)
                    for key in (
                        "co2_ppm",
                        "h2o_mmol",
                        "chamber_temp_c",
                        "case_temp_c",
                        "temp_c",
                        "pressure_kpa",
                        "status",
                    )
                    if key in last
                },
                "serial_metadata": port_metadata.get(port.upper(), {}),
                "error": error,
                "tx_bytes": 0,
            }
        )

    duplicate_observed_ids = {
        device_id: sorted(set(ports))
        for device_id, ports in observed_id_ports.items()
        if device_id and len(set(ports)) > 1
    }
    duplicate_configured_ids = {
        device_id: sorted(set(ports))
        for device_id, ports in configured_id_ports.items()
        if device_id and len(set(ports)) > 1
    }
    silent_ports = [row["port"] for row in rows if row["open_ok"] and not row["streaming"]]
    open_error_ports = [row["port"] for row in rows if not row["open_ok"]]
    mismatch_ports = [row["port"] for row in rows if row["configured_identity_matches"] is False]
    if duplicate_observed_ids or duplicate_configured_ids:
        status = "blocked_duplicate_identity"
    elif mismatch_ports:
        status = "review_required_identity_mismatch"
    elif silent_ports or open_error_ports:
        status = "review_required_silent_or_unavailable_ports"
    else:
        status = "ready"

    return {
        "schema_version": "v1_5_analyzer_receive_only_snapshot_v1",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "status": status,
        "engineering_probe_only": True,
        "promotion_state": "blocked",
        "not_real_acceptance_evidence": True,
        "evidence_source": "real_com_receive_only",
        "duration_per_port_s": float(duration_per_port_s),
        "analyzers": rows,
        "summary": {
            "configured_port_count": len(rows),
            "streaming_port_count": sum(1 for row in rows if row["streaming"]),
            "silent_ports": silent_ports,
            "open_error_ports": open_error_ports,
            "identity_mismatch_ports": mismatch_ports,
            "duplicate_observed_ids": duplicate_observed_ids,
            "duplicate_configured_ids": duplicate_configured_ids,
        },
        "safety": {
            "receive_only": True,
            "sends_device_commands": False,
            "tx_bytes": 0,
            "writes_device_id": False,
            "writes_senco": False,
            "controls_water_or_gas_routes": False,
            "opens_dewpoint_meter": False,
        },
    }


def _probe_analyzer_sn_query(
    cfg: Mapping[str, Any],
    io_logger: Any,
    *,
    timeout_s: float,
) -> Dict[str, Any]:
    """Query analyzer SN values without changing any persistent or route state."""

    port_metadata = _serial_port_metadata()
    rows: List[Dict[str, Any]] = []
    sn_ports: Dict[str, List[str]] = {}
    for index, analyzer_cfg in enumerate(_enabled_analyzer_configs(cfg), start=1):
        port = str(analyzer_cfg.get("port") or "").strip()
        configured_id = _normalized_analyzer_id(analyzer_cfg.get("device_id"))
        dev = GasAnalyzer(
            port,
            int(analyzer_cfg.get("baud", analyzer_cfg.get("baudrate", 115200)) or 115200),
            timeout=float(analyzer_cfg.get("timeout", 1.0) or 1.0),
            device_id=configured_id,
            io_logger=io_logger,
        )
        opened = False
        sn_code = ""
        raw = ""
        error = ""
        try:
            dev.open()
            opened = True
            value, raw = _read_sn(dev, timeout_s=max(0.05, float(timeout_s)), attempts=1)
            sn_code = str(value or "").strip()
        except Exception as exc:
            error = str(exc)
        finally:
            try:
                dev.close()
            except Exception:
                pass
        if sn_code:
            sn_ports.setdefault(sn_code, []).append(port)
        sn_bound_valid = bool(re.fullmatch(r"\d{8}", sn_code)) and sn_code != "00000000"
        rows.append(
            {
                "analyzer_name": str(analyzer_cfg.get("name") or f"ga{index:02d}"),
                "port": port,
                "configured_device_id": configured_id,
                "usb_serial_number": port_metadata.get(port.upper(), {}).get(
                    "usb_serial_number", ""
                ),
                "open_ok": opened,
                "sn_code": sn_code,
                "sn_read_ok": bool(sn_code),
                "sn_bound_valid": sn_bound_valid,
                "raw_summary": str(raw or "")[:500],
                "error": error,
                "query_command": "SN,YGAS,FFF",
                "state_change": False,
            }
        )

    duplicate_sn_codes = {
        sn_code: sorted(set(ports))
        for sn_code, ports in sn_ports.items()
        if len(set(ports)) > 1
    }
    missing_sn_ports = [row["port"] for row in rows if not row["sn_read_ok"]]
    invalid_or_uninitialized_sn_ports = [
        row["port"]
        for row in rows
        if row["sn_read_ok"] and not row["sn_bound_valid"]
    ]
    if duplicate_sn_codes:
        status = "blocked_duplicate_sn"
    elif missing_sn_ports:
        status = "blocked_missing_sn"
    elif invalid_or_uninitialized_sn_ports:
        status = "blocked_invalid_or_uninitialized_sn"
    else:
        status = "ready"
    return {
        "schema_version": "v1_5_analyzer_sn_query_snapshot_v1",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "status": status,
        "engineering_probe_only": True,
        "promotion_state": "blocked",
        "not_real_acceptance_evidence": True,
        "evidence_source": "real_com_query_only",
        "analyzers": rows,
        "summary": {
            "configured_port_count": len(rows),
            "sn_read_count": sum(1 for row in rows if row["sn_read_ok"]),
            "valid_bound_sn_count": sum(1 for row in rows if row["sn_bound_valid"]),
            "missing_sn_ports": missing_sn_ports,
            "invalid_or_uninitialized_sn_ports": invalid_or_uninitialized_sn_ports,
            "duplicate_sn_codes": duplicate_sn_codes,
        },
        "safety": {
            "query_only": True,
            "receive_only": False,
            "sends_device_commands": True,
            "allowed_command": "SN,YGAS,FFF",
            "writes_sn": False,
            "writes_device_id": False,
            "writes_senco": False,
            "controls_water_or_gas_routes": False,
            "opens_dewpoint_meter": False,
        },
    }


def _print_header(title: str) -> None:
    print("")
    print("=" * 72)
    print(title)
    print("=" * 72)


def _run_pressure_mode(
    cfg: Dict[str, Any],
    logger: RunLogger,
    scan_ids: bool,
    scan_max_id: int,
    scan_wait_s: float,
    exhaustive_controller: bool = False,
) -> int:
    dcfg = cfg.get("devices", {})
    pcfg = dcfg.get("pressure_controller", {})
    gcfg = dcfg.get("pressure_gauge", {})

    _print_header("Pressure Controller Probe")
    pc = _probe_pressure_controller(pcfg, logger, exhaustive=exhaustive_controller)
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


def _run_analyzers_mode(
    cfg: Dict[str, Any],
    logger: RunLogger,
    duration_per_port_s: float,
    *,
    query_sn: bool,
    sn_timeout_s: float,
) -> int:
    _print_header("Gas Analyzer Receive-only Identity Inventory")
    snapshot = _probe_analyzers_receive_only(
        cfg,
        logger,
        duration_per_port_s=duration_per_port_s,
    )
    path = logger.run_dir / "analyzer_receive_only_snapshot.json"
    path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(snapshot["summary"], ensure_ascii=False, indent=2))
    print(f"status={snapshot['status']} snapshot={path}")
    if query_sn:
        sn_snapshot = _probe_analyzer_sn_query(cfg, logger, timeout_s=sn_timeout_s)
        sn_path = logger.run_dir / "analyzer_sn_query_snapshot.json"
        sn_path.write_text(
            json.dumps(sn_snapshot, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(json.dumps(sn_snapshot["summary"], ensure_ascii=False, indent=2))
        print(f"sn_status={sn_snapshot['status']} sn_snapshot={sn_path}")
    return 0


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="On-bench probe tool")
    parser.add_argument("--config", default="configs/default_config.json")
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional isolated probe-artifact directory; defaults to the config output path.",
    )

    sub = parser.add_subparsers(dest="mode", required=True)

    p_pressure = sub.add_parser("pressure", help="Probe pressure controller and pressure gauge")
    p_pressure.add_argument("--scan-ids", action="store_true", help="Scan gauge ID 00..N")
    p_pressure.add_argument("--scan-max-id", type=int, default=31)
    p_pressure.add_argument("--scan-wait-s", type=float, default=3.0)
    p_pressure.add_argument(
        "--exhaustive-controller",
        action="store_true",
        help="Use the production compatibility query matrix instead of the bounded bench probe.",
    )

    p_thermo = sub.add_parser("thermometer", help="Probe thermometer")
    p_thermo.add_argument("--duration-s", type=float, default=6.0)
    p_thermo.add_argument("--try-all", action="store_true", help="Try extra serial settings")

    p_analyzers = sub.add_parser(
        "analyzers",
        help="Receive-only analyzer stream inventory; sends no serial commands",
    )
    p_analyzers.add_argument("--duration-per-port-s", type=float, default=1.5)
    p_analyzers.add_argument(
        "--query-sn",
        action="store_true",
        help="Also send one query-only SN command per port; disabled by default",
    )
    p_analyzers.add_argument("--sn-timeout-s", type=float, default=1.2)

    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    ns = parse_args(argv)
    cfg = load_config(ns.config)
    logger = _make_logger(cfg, output_dir=ns.output_dir)
    print(f"run_dir={logger.run_dir}")
    try:
        if ns.mode == "pressure":
            return _run_pressure_mode(
                cfg,
                logger,
                scan_ids=bool(ns.scan_ids),
                scan_max_id=int(ns.scan_max_id),
                scan_wait_s=float(ns.scan_wait_s),
                exhaustive_controller=bool(ns.exhaustive_controller),
            )
        if ns.mode == "thermometer":
            return _run_thermometer_mode(
                cfg,
                logger,
                duration_s=float(ns.duration_s),
                try_all=bool(ns.try_all),
            )
        if ns.mode == "analyzers":
            return _run_analyzers_mode(
                cfg,
                logger,
                duration_per_port_s=float(ns.duration_per_port_s),
                query_sn=bool(ns.query_sn),
                sn_timeout_s=float(ns.sn_timeout_s),
            )
        print(f"Unknown mode: {ns.mode}")
        return 2
    finally:
        logger.close()


if __name__ == "__main__":
    raise SystemExit(main())

