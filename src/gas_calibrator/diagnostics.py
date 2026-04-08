"""Device self-test helpers."""

from __future__ import annotations

import math
import time
from typing import Any, Callable, Dict, Iterable, Optional, Set

from .config import load_config
from .devices import (
    DewpointMeter,
    GasAnalyzer,
    HumidityGenerator,
    Pace5000,
    ParoscientificGauge,
    RelayController,
    TemperatureChamber,
    Thermometer,
)


def _log(log_fn: Optional[Callable[[str], None]], msg: str) -> None:
    if log_fn:
        log_fn(msg)


def _normalize_only_devices(only_devices: Optional[Iterable[str]]) -> Optional[Set[str]]:
    if only_devices is None:
        return None
    return {str(name) for name in only_devices}


def _want_device(name: str, only_set: Optional[Set[str]]) -> bool:
    return only_set is None or name in only_set


def _retry(
    fn: Callable[[], Any],
    *,
    tries: int = 3,
    delay_s: float = 0.15,
    accept: Optional[Callable[[Any], bool]] = None,
) -> Any:
    last_exc: Optional[Exception] = None
    for idx in range(max(1, tries)):
        try:
            value = fn()
            if accept is None or accept(value):
                return value
            last_exc = RuntimeError("INVALID_RESPONSE")
        except Exception as exc:
            last_exc = exc
        if idx < tries - 1 and delay_s > 0:
            time.sleep(delay_s)
    if last_exc:
        raise last_exc
    raise RuntimeError("RETRY_FAILED")


def _gas_analyzer_probe(
    dev: GasAnalyzer,
    *,
    active_send: bool,
    ftd_hz: int,
) -> tuple[str, Optional[Dict[str, Any]]]:
    last_raw = ""
    if active_send:
        try:
            last_raw = dev.read_data_active(drain_s=max(0.2, 2.0 / max(1, int(ftd_hz))))
        except Exception:
            last_raw = ""
        if last_raw:
            parsed = dev.parse_line(last_raw)
            if parsed:
                return last_raw, parsed

    last_raw = dev.read_data_passive()
    return last_raw, dev.parse_line(last_raw)


def _coerce_float(value: Any) -> float | None:
    try:
        numeric = float(value)
    except Exception:
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _matches_sentinel(value: float | None, sentinels: list[float], tolerance: float) -> bool:
    if value is None:
        return False
    return any(abs(value - sentinel) <= tolerance for sentinel in sentinels)


def _has_usable_ratio_value(parsed: Dict[str, Any], qcfg: Dict[str, Any]) -> bool:
    tolerance = abs(float(qcfg.get("invalid_sentinel_tolerance", 0.001) or 0.001))
    sentinels: list[float] = []
    for item in qcfg.get("invalid_sentinel_values", [-1001.0, -9999.0, 999999.0]) or []:
        numeric = _coerce_float(item)
        if numeric is not None:
            sentinels.append(numeric)

    for key in ("co2_ratio_f", "h2o_ratio_f"):
        numeric = _coerce_float(parsed.get(key))
        if numeric is None or numeric <= 0:
            continue
        if _matches_sentinel(numeric, sentinels, tolerance):
            continue
        return True
    return False


def _assess_gas_analyzer_frame(
    parsed: Optional[Dict[str, Any]],
    cfg: Dict[str, Any],
) -> tuple[bool, str]:
    if not isinstance(parsed, dict) or not parsed:
        return False, "NO_FRAME"

    qcfg = dict(cfg.get("workflow", {}).get("analyzer_frame_quality", {}) or {})
    suspicious_co2_ppm_min = float(qcfg.get("suspicious_co2_ppm_min", 2999.0) or 2999.0)
    suspicious_h2o_mmol_min = float(qcfg.get("suspicious_h2o_mmol_min", 70.0) or 70.0)

    co2_ppm = _coerce_float(parsed.get("co2_ppm"))
    h2o_mmol = _coerce_float(parsed.get("h2o_mmol"))

    if (
        co2_ppm is not None
        and h2o_mmol is not None
        and co2_ppm >= suspicious_co2_ppm_min
        and h2o_mmol >= suspicious_h2o_mmol_min
    ):
        if _has_usable_ratio_value(parsed, qcfg):
            return True, "极值已标记"
        return False, "异常极值"

    return True, "OK"


def run_self_test(
    cfg: Dict[str, Any],
    log_fn: Optional[Callable[[str], None]] = None,
    io_logger: Optional[Any] = None,
    only_devices: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    results: Dict[str, Any] = {}
    dcfg = cfg.get("devices", {})
    only_set = _normalize_only_devices(only_devices)

    if only_set:
        _log(log_fn, f"Self-test started (subset): {', '.join(sorted(only_set))}")
    else:
        _log(log_fn, "Self-test started")

    if _want_device("pressure_controller", only_set):
        pcfg = dcfg.get("pressure_controller", {})
        if pcfg.get("enabled"):
            dev = Pace5000(
                pcfg["port"],
                pcfg["baud"],
                timeout=float(pcfg.get("timeout", 1.0)),
                line_ending=pcfg.get("line_ending"),
                query_line_endings=pcfg.get("query_line_endings"),
                pressure_queries=pcfg.get("pressure_queries"),
                io_logger=io_logger,
            )
            try:
                dev.open()
                p = _retry(dev.read_pressure, tries=3, delay_s=0.2)
                results["pressure_controller"] = {"ok": True, "pressure_hpa": p}
                _log(log_fn, f"[pressure_controller] OK pressure={p}")
            except Exception as exc:
                results["pressure_controller"] = {"ok": False, "err": str(exc)}
                _log(log_fn, f"[pressure_controller] FAIL {exc}")
            finally:
                try:
                    dev.close()
                except Exception:
                    pass
        else:
            results["pressure_controller"] = {"ok": False, "err": "DISABLED"}
            _log(log_fn, "[pressure_controller] DISABLED")

    if _want_device("pressure_gauge", only_set):
        pgcfg = dcfg.get("pressure_gauge", {})
        if pgcfg.get("enabled"):
            dev = ParoscientificGauge(
                pgcfg["port"],
                pgcfg["baud"],
                timeout=float(pgcfg.get("timeout", 1.0)),
                dest_id=pgcfg["dest_id"],
                response_timeout_s=pgcfg.get("response_timeout_s"),
                io_logger=io_logger,
            )
            try:
                dev.open()
                p = _retry(dev.read_pressure, tries=3, delay_s=0.2)
                results["pressure_gauge"] = {"ok": True, "pressure_hpa": p}
                _log(log_fn, f"[pressure_gauge] OK pressure={p}")
            except Exception as exc:
                results["pressure_gauge"] = {"ok": False, "err": str(exc)}
                _log(log_fn, f"[pressure_gauge] FAIL {exc}")
            finally:
                try:
                    dev.close()
                except Exception:
                    pass
        else:
            results["pressure_gauge"] = {"ok": False, "err": "DISABLED"}
            _log(log_fn, "[pressure_gauge] DISABLED")

    if _want_device("dewpoint_meter", only_set):
        dpcfg = dcfg.get("dewpoint_meter", {})
        if dpcfg.get("enabled"):
            dev = DewpointMeter(
                dpcfg["port"],
                dpcfg["baud"],
                station=dpcfg["station"],
                io_logger=io_logger,
            )
            try:
                dev.open()
                data = dev.get_current()
                results["dewpoint_meter"] = {"ok": True, "data": data}
                _log(log_fn, f"[dewpoint_meter] OK dewpoint={data.get('dewpoint_c')}")
            except Exception as exc:
                results["dewpoint_meter"] = {"ok": False, "err": str(exc)}
                _log(log_fn, f"[dewpoint_meter] FAIL {exc}")
            finally:
                try:
                    dev.close()
                except Exception:
                    pass
        else:
            results["dewpoint_meter"] = {"ok": False, "err": "DISABLED"}
            _log(log_fn, "[dewpoint_meter] DISABLED")

    if _want_device("humidity_generator", only_set):
        hcfg = dcfg.get("humidity_generator", {})
        if hcfg.get("enabled"):
            dev = HumidityGenerator(hcfg["port"], hcfg["baud"], io_logger=io_logger)
            try:
                dev.open()
                snap = dev.fetch_all()
                if not snap.get("raw"):
                    raise RuntimeError("NO_RESPONSE")
                results["humidity_generator"] = {
                    "ok": True,
                    "raw": snap.get("raw"),
                    "data": snap.get("data"),
                }
                _log(log_fn, f"[humidity_generator] OK flow={snap.get('data', {}).get('Fl')}")
            except Exception as exc:
                results["humidity_generator"] = {"ok": False, "err": str(exc)}
                _log(log_fn, f"[humidity_generator] FAIL {exc}")
            finally:
                try:
                    dev.close()
                except Exception:
                    pass
        else:
            results["humidity_generator"] = {"ok": False, "err": "DISABLED"}
            _log(log_fn, "[humidity_generator] DISABLED")

    if _want_device("gas_analyzer", only_set):
        gas_list_cfg = dcfg.get("gas_analyzers", [])
        enabled_multi = [
            item
            for item in gas_list_cfg
            if isinstance(item, dict) and item.get("enabled", True)
        ] if isinstance(gas_list_cfg, list) else []

        if enabled_multi:
            item_results = []
            all_ok = True
            for idx, gcfg in enumerate(enabled_multi, start=1):
                name = str(gcfg.get("name") or f"ga{idx:02d}")
                dev = GasAnalyzer(
                    gcfg["port"],
                    gcfg.get("baud", 115200),
                    device_id=gcfg.get("device_id", f"{idx:03d}"),
                    io_logger=io_logger,
                )
                try:
                    dev.open()
                    active_send = bool(gcfg.get("active_send", dcfg.get("gas_analyzer", {}).get("active_send", False)))
                    ftd_hz = int(gcfg.get("ftd_hz", dcfg.get("gas_analyzer", {}).get("ftd_hz", 1)))
                    last_line = {"raw": ""}

                    def _read_and_parse():
                        text, parsed = _gas_analyzer_probe(dev, active_send=active_send, ftd_hz=ftd_hz)
                        last_line["raw"] = text
                        return parsed

                    parsed = _retry(
                        _read_and_parse,
                        tries=4,
                        delay_s=0.1,
                        accept=lambda x: bool(x),
                    )
                    if not parsed:
                        raise RuntimeError("PARSE_FAILED")
                    usable, status = _assess_gas_analyzer_frame(parsed, cfg)
                    if not usable:
                        raise RuntimeError(status)
                    item_results.append(
                        {
                            "name": name,
                            "ok": True,
                            "co2_ppm": parsed.get("co2_ppm"),
                            "frame_status": status,
                        }
                    )
                    suffix = "" if status == "OK" else f" status={status}"
                    _log(log_fn, f"[gas_analyzer:{name}] OK co2={parsed.get('co2_ppm')}{suffix}")
                except Exception as exc:
                    all_ok = False
                    item_results.append({"name": name, "ok": False, "err": str(exc)})
                    _log(log_fn, f"[gas_analyzer:{name}] FAIL {exc}")
                finally:
                    try:
                        dev.close()
                    except Exception:
                        pass

            results["gas_analyzer"] = {"ok": all_ok, "items": item_results}
        else:
            gacfg = dcfg.get("gas_analyzer", {})
            if gacfg.get("enabled"):
                dev = GasAnalyzer(
                    gacfg["port"],
                    gacfg["baud"],
                    device_id=gacfg["device_id"],
                    io_logger=io_logger,
                )
                try:
                    dev.open()
                    last_line = {"raw": ""}
                    active_send = bool(gacfg.get("active_send", False))
                    ftd_hz = int(gacfg.get("ftd_hz", 1))

                    def _read_and_parse():
                        text, parsed = _gas_analyzer_probe(dev, active_send=active_send, ftd_hz=ftd_hz)
                        last_line["raw"] = text
                        return parsed

                    parsed = _retry(
                        _read_and_parse,
                        tries=4,
                        delay_s=0.1,
                        accept=lambda x: bool(x),
                    )
                    if not parsed:
                        raise RuntimeError("PARSE_FAILED")
                    usable, status = _assess_gas_analyzer_frame(parsed, cfg)
                    if not usable:
                        raise RuntimeError(status)
                    results["gas_analyzer"] = {
                        "ok": True,
                        "raw": last_line["raw"],
                        "parsed": parsed,
                        "frame_status": status,
                    }
                    suffix = "" if status == "OK" else f" status={status}"
                    _log(log_fn, f"[gas_analyzer] OK co2={parsed.get('co2_ppm')}{suffix}")
                except Exception as exc:
                    results["gas_analyzer"] = {"ok": False, "err": str(exc)}
                    _log(log_fn, f"[gas_analyzer] FAIL {exc}")
                finally:
                    try:
                        dev.close()
                    except Exception:
                        pass
            else:
                results["gas_analyzer"] = {"ok": False, "err": "DISABLED"}
                _log(log_fn, "[gas_analyzer] DISABLED")

    if _want_device("temperature_chamber", only_set):
        tcfg = dcfg.get("temperature_chamber", {})
        if tcfg.get("enabled"):
            dev = TemperatureChamber(
                tcfg["port"],
                tcfg["baud"],
                addr=tcfg["addr"],
                io_logger=io_logger,
            )
            try:
                dev.open()
                temp = _retry(dev.read_temp_c, tries=3, delay_s=0.15)
                rh = _retry(dev.read_rh_pct, tries=3, delay_s=0.15)
                results["temperature_chamber"] = {"ok": True, "temp_c": temp, "rh_pct": rh}
                _log(log_fn, f"[temperature_chamber] OK temp={temp} rh={rh}")
            except Exception as exc:
                results["temperature_chamber"] = {"ok": False, "err": str(exc)}
                _log(log_fn, f"[temperature_chamber] FAIL {exc}")
            finally:
                try:
                    dev.close()
                except Exception:
                    pass
        else:
            results["temperature_chamber"] = {"ok": False, "err": "DISABLED"}
            _log(log_fn, "[temperature_chamber] DISABLED")

    if _want_device("thermometer", only_set):
        thcfg = dcfg.get("thermometer", {})
        if thcfg.get("enabled"):
            dev = Thermometer(
                thcfg["port"],
                thcfg["baud"],
                timeout=thcfg.get("timeout", 1.2),
                parity=thcfg.get("parity", "N"),
                stopbits=thcfg.get("stopbits", 1),
                bytesize=thcfg.get("bytesize", 8),
                io_logger=io_logger,
            )
            try:
                dev.open()
                data = _retry(
                    dev.read_current,
                    tries=5,
                    delay_s=0.1,
                    accept=lambda x: bool(isinstance(x, dict) and x.get("ok")),
                )
                temp = data.get("temp_c")
                if data.get("ok"):
                    results["thermometer"] = {"ok": True, "temp_c": temp, "raw": data.get("raw")}
                    _log(log_fn, f"[thermometer] OK temp={temp} raw={data.get('raw')!r}")
                else:
                    results["thermometer"] = {"ok": False, "err": "NO_VALID_FRAME", "raw": data.get("raw")}
                    _log(log_fn, f"[thermometer] FAIL NO_VALID_FRAME raw={data.get('raw')!r}")
            except Exception as exc:
                results["thermometer"] = {"ok": False, "err": str(exc)}
                _log(log_fn, f"[thermometer] FAIL {exc}")
            finally:
                try:
                    dev.close()
                except Exception:
                    pass
        else:
            results["thermometer"] = {"ok": False, "err": "DISABLED"}
            _log(log_fn, "[thermometer] DISABLED")

    if _want_device("relay", only_set):
        rcfg = dcfg.get("relay", {})
        if rcfg.get("enabled"):
            dev = RelayController(rcfg["port"], rcfg["baud"], addr=rcfg["addr"], io_logger=io_logger)
            try:
                dev.open()
                coils = dev.read_coils(0, 1)
                results["relay"] = {"ok": True, "coils": coils}
                _log(log_fn, f"[relay] OK coil0={coils[0] if coils else None}")
            except Exception as exc:
                results["relay"] = {"ok": False, "err": str(exc)}
                _log(log_fn, f"[relay] FAIL {exc}")
            finally:
                try:
                    dev.close()
                except Exception:
                    pass
        else:
            results["relay"] = {"ok": False, "err": "DISABLED"}
            _log(log_fn, "[relay] DISABLED")

    has_relay8_cfg = "relay_8" in dcfg
    relay8_requested = bool(only_set and "relay_8" in only_set)
    if _want_device("relay_8", only_set) and (has_relay8_cfg or relay8_requested):
        rcfg = dcfg.get("relay_8", {})
        if rcfg.get("enabled"):
            dev = RelayController(rcfg["port"], rcfg["baud"], addr=rcfg["addr"], io_logger=io_logger)
            try:
                dev.open()
                coils = dev.read_coils(0, 1)
                results["relay_8"] = {"ok": True, "coils": coils}
                _log(log_fn, f"[relay_8] OK coil0={coils[0] if coils else None}")
            except Exception as exc:
                results["relay_8"] = {"ok": False, "err": str(exc)}
                _log(log_fn, f"[relay_8] FAIL {exc}")
            finally:
                try:
                    dev.close()
                except Exception:
                    pass
        else:
            results["relay_8"] = {"ok": False, "err": "DISABLED"}
            _log(log_fn, "[relay_8] DISABLED")

    _log(log_fn, "Self-test finished")
    return results


def main() -> None:
    cfg = load_config(str("configs/default_config.json"))
    run_self_test(cfg, log_fn=print)


if __name__ == "__main__":
    main()
