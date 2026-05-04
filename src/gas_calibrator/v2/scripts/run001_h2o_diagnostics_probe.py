from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


def _bootstrap_src_path() -> None:
    src_root = Path(__file__).resolve().parents[3]
    src_root_text = str(src_root)
    if src_root_text not in sys.path:
        sys.path.insert(0, src_root_text)


_bootstrap_src_path()

from gas_calibrator.devices.dewpoint_meter import DewpointMeter
from gas_calibrator.devices.humidity_generator import HumidityGenerator
from gas_calibrator.devices.relay import RelayController


H2O_DIAG_SCHEMA = "v2.run001.h2o_diagnostics_probe.1"
H2O_DIAG_EVIDENCE = {
    "probe_identity": "Run-001 H2O diagnostics probe (three-pathway query-only)",
    "probe_version": "H2O-DIAG.1",
    "evidence_source": "real_probe_h2o_diagnostics",
    "acceptance_level": "engineering_probe_only",
    "not_real_acceptance_evidence": True,
    "promotion_state": "blocked",
    "real_primary_latest_refresh": False,
    "no_write": True,
    "query_only": True,
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _load_config(config_path: str) -> Dict[str, Any]:
    p = Path(config_path).expanduser().resolve()
    if not p.exists():
        raise FileNotFoundError(f"Config not found: {p}")
    with open(p, "r", encoding="utf-8-sig") as fh:
        return json.load(fh)


def _path_value(cfg: Dict[str, Any], *keys: str) -> Any:
    node: Any = cfg
    for k in keys:
        if isinstance(node, dict):
            node = node.get(k)
        else:
            return None
    return node


def _get_str(cfg: Dict[str, Any], path: str, default: str = "") -> str:
    parts = path.split(".")
    val = _path_value(cfg, *parts)
    if val is None:
        return default
    return str(val)


def _get_port(cfg: Dict[str, Any], device_key: str) -> str:
    return _get_str(cfg, f"devices.{device_key}.port")


def _get_baud(cfg: Dict[str, Any], device_key: str) -> int:
    val = _path_value(cfg, "devices", device_key, "baud")
    try:
        return int(val)
    except (TypeError, ValueError):
        return 9600


def _get_station(cfg: Dict[str, Any], device_key: str) -> str:
    return _get_str(cfg, f"devices.{device_key}.station", "001")


def diagnose_humidity_generator(cfg: Dict[str, Any]) -> Dict[str, Any]:
    port = _get_port(cfg, "humidity_generator")
    baud = _get_baud(cfg, "humidity_generator")
    result: Dict[str, Any] = {
        "pathway": "humidity_generator_control",
        "port": port,
        "baud": baud,
        "timestamp": _now(),
    }
    if not port:
        result["ok"] = False
        result["error"] = "no_port_configured"
        return result

    dev: Optional[HumidityGenerator] = None
    try:
        dev = HumidityGenerator(port=port, baudrate=baud, timeout=2.0)
        dev.open()
        time.sleep(0.3)
        snap = dev.fetch_all()
        if snap and snap.get("raw"):
            data = snap.get("data", {})
            result["ok"] = True
            result["raw_length"] = len(str(snap.get("raw", "")))
            result["flow_lpm"] = data.get("Fl")
            result["dewpoint_c"] = data.get("Td")
            result["temp_c"] = data.get("Tc")
            result["summary"] = (
                f"湿度发生器 {port} 响应正常, 流量={data.get('Fl')}, "
                f"露点={data.get('Td')}C, 温度={data.get('Tc')}C"
            )
        else:
            result["ok"] = False
            result["error"] = "no_response"
            result["summary"] = f"湿度发生器 {port} 无响应"
    except Exception as exc:
        result["ok"] = False
        result["error"] = str(exc)
        result["summary"] = f"湿度发生器 {port} 异常: {exc}"
    finally:
        if dev is not None:
            try:
                dev.close()
            except Exception:
                pass
    return result


def diagnose_dewpoint_meter(cfg: Dict[str, Any]) -> Dict[str, Any]:
    port = _get_port(cfg, "dewpoint_meter")
    baud = _get_baud(cfg, "dewpoint_meter")
    station = _get_station(cfg, "dewpoint_meter")
    result: Dict[str, Any] = {
        "pathway": "dewpoint_meter_reading",
        "port": port,
        "baud": baud,
        "station": station,
        "timestamp": _now(),
    }
    if not port:
        result["ok"] = False
        result["error"] = "no_port_configured"
        return result

    dev: Optional[DewpointMeter] = None
    try:
        dev = DewpointMeter(port=port, baudrate=baud, timeout=2.0, station=station)
        dev.open()
        time.sleep(0.3)
        status = dev.status()
        if status.get("ok"):
            result["ok"] = True
            result["dewpoint_c"] = status.get("dewpoint_c")
            result["temp_c"] = status.get("temp_c")
            result["rh_pct"] = status.get("rh_pct")
            result["summary"] = (
                f"露点仪 {port} 响应正常, 露点={status.get('dewpoint_c')}C, "
                f"温度={status.get('temp_c')}C, RH={status.get('rh_pct')}%"
            )
        else:
            result["ok"] = False
            result["error"] = "no_valid_reading"
            result["raw"] = status.get("raw")
            result["summary"] = f"露点仪 {port} 无有效读数"
    except Exception as exc:
        result["ok"] = False
        result["error"] = str(exc)
        result["summary"] = f"露点仪 {port} 异常: {exc}"
    finally:
        if dev is not None:
            try:
                dev.close()
            except Exception:
                pass
    return result


def diagnose_valve_routing(cfg: Dict[str, Any]) -> Dict[str, Any]:
    relay_port = _get_port(cfg, "relay")
    relay8_port = _get_port(cfg, "relay_8")
    relay_baud = _get_baud(cfg, "relay")
    relay8_baud = _get_baud(cfg, "relay_8")

    valves = _path_value(cfg, "valves")
    h2o_path_ch = None
    hold_ch = None
    flow_switch_ch = None
    relay_map = {}

    if isinstance(valves, dict):
        h2o_path_ch = valves.get("h2o_path")
        hold_ch = valves.get("hold")
        flow_switch_ch = valves.get("flow_switch")
        relay_map = valves.get("relay_map", {})

    result: Dict[str, Any] = {
        "pathway": "valve_routing_h2o",
        "relay_port": relay_port,
        "relay8_port": relay8_port,
        "h2o_path_channel": h2o_path_ch,
        "hold_channel": hold_ch,
        "flow_switch_channel": flow_switch_ch,
        "timestamp": _now(),
        "relay_devices": [],
    }

    def _probe_relay(label: str, port: str, baud: int, addr: int = 1) -> Dict[str, Any]:
        sub: Dict[str, Any] = {"label": label, "port": port, "baud": baud, "addr": addr, "ok": False}
        dev = None
        try:
            dev = RelayController(port=port, baudrate=baud, addr=addr)
            dev.open()
            time.sleep(0.2)
            coils = dev.read_coils(0, 8)
            if coils is not None:
                sub["ok"] = True
                sub["coils"] = list(coils) if hasattr(coils, "__iter__") else str(coils)
                sub["summary"] = f"继电器 {label} ({port}) 响应正常, coils[0:8]={sub['coils']}"
            else:
                sub["error"] = "no_response"
                sub["summary"] = f"继电器 {label} ({port}) 无响应"
        except Exception as exc:
            sub["error"] = str(exc)
            sub["summary"] = f"继电器 {label} ({port}) 异常: {exc}"
        finally:
            if dev is not None:
                try:
                    dev.close()
                except Exception:
                    pass
        return sub

    if relay_port:
        r = _probe_relay("relay", relay_port, relay_baud)
        result["relay_devices"].append(r)

    if relay8_port:
        r = _probe_relay("relay_8", relay8_port, relay8_baud)
        result["relay_devices"].append(r)

    all_ok = all(d.get("ok") for d in result["relay_devices"]) if result["relay_devices"] else False
    result["ok"] = all_ok
    if all_ok:
        h2o_desc = f"阀门{h2o_path_ch}" if h2o_path_ch is not None else "?"
        hold_desc = f"阀门{hold_ch}" if hold_ch is not None else "?"
        fs_desc = f"阀门{flow_switch_ch}" if flow_switch_ch is not None else "?"
        result["summary"] = (
            f"H₂O 路由阀门可连通: h2o_path={h2o_desc}, hold={hold_desc}, flow_switch={fs_desc}"
        )
    else:
        result["summary"] = "H₂O 路由阀门探针未全部通过"

    return result


def build_h2o_diagnostics_report(
    cfg: Dict[str, Any],
    *,
    config_path: str = "",
    branch: str = "",
    head: str = "",
) -> Dict[str, Any]:
    results = []

    hgen = diagnose_humidity_generator(cfg)
    results.append(hgen)

    dew = diagnose_dewpoint_meter(cfg)
    results.append(dew)

    valves = diagnose_valve_routing(cfg)
    results.append(valves)

    all_ok = all(r.get("ok") for r in results)
    failed = [r["pathway"] for r in results if not r.get("ok")]

    return {
        "schema_version": H2O_DIAG_SCHEMA,
        "record_type": "h2o_diagnostics_report",
        "created_at": _now(),
        "config_path": config_path,
        "branch": branch,
        "head": head,
        **H2O_DIAG_EVIDENCE,
        "pathways_tested": 3,
        "pathways_passed": sum(1 for r in results if r.get("ok")),
        "pathways_failed": len(failed),
        "failed_pathways": failed,
        "overall_pass": all_ok,
        "diagnosis": (
            "H₂O 三条独立通路全部通过 —— 湿度发生器、露点仪、阀门路由均可正常通信"
            if all_ok
            else f"H₂O 诊断未全部通过，失败通路: {', '.join(failed)}"
        ),
        "results": results,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Run-001 H2O diagnostics probe — query-only, no-write, three-pathway verification"
    )
    parser.add_argument(
        "--config",
        required=True,
        help="H2O real-machine config JSON",
    )
    parser.add_argument("--branch", default="", help="Current git branch")
    parser.add_argument("--head", default="", help="Current git HEAD")
    parser.add_argument(
        "--output",
        default="",
        help="Output JSON path (prints to stdout if not provided)",
    )
    args = parser.parse_args(argv)

    config_path = str(Path(args.config).expanduser().resolve())
    cfg = _load_config(config_path)

    report = build_h2o_diagnostics_report(
        cfg,
        config_path=config_path,
        branch=args.branch,
        head=args.head,
    )

    output = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        out_path = Path(args.output).expanduser().resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(output, encoding="utf-8")
        print(f"Report written to {out_path}", flush=True)

    print(output, flush=True)
    return 0 if report.get("overall_pass") else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
