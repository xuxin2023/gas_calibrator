"""Safe-stop helper for bench hardware."""

from __future__ import annotations

import argparse
import copy
from datetime import datetime
from pathlib import Path
import time
from typing import Any, Dict, Iterable, Optional

from ..config import load_config
from ..logging_utils import RunLogger
from .run_headless import _build_devices, _close_devices


def _log(msg: str) -> None:
    print(msg, flush=True)


def _as_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except Exception:
        return None


def _as_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        return None


def _safe_stop_cfg(cfg: Dict[str, Any]) -> Dict[str, Any]:
    reduced = copy.deepcopy(cfg)
    devices = reduced.get("devices", {})
    keep_enabled = {
        "pressure_controller",
        "pressure_gauge",
        "humidity_generator",
        "temperature_chamber",
        "relay",
        "relay_8",
    }
    for name, dev_cfg in devices.items():
        if name == "gas_analyzers" and isinstance(dev_cfg, list):
            for item in dev_cfg:
                if isinstance(item, dict):
                    item["enabled"] = False
            continue
        if name in keep_enabled or not isinstance(dev_cfg, dict) or "enabled" not in dev_cfg:
            continue
        dev_cfg["enabled"] = False
    return reduced


def _set_relay_states(relay: Any, desired: Iterable[bool], label: str, log_fn) -> None:
    setter = getattr(relay, "set_valve", None)
    if not callable(setter):
        log_fn(f"{label} reset skipped: set_valve unavailable")
        return
    for idx, state in enumerate(desired, start=1):
        try:
            setter(idx, bool(state))
        except Exception as exc:
            log_fn(f"{label} ch{idx} -> {bool(state)} failed: {exc}")


def _baseline_relay_states_from_cfg(cfg: Optional[Dict[str, Any]]) -> Optional[Dict[str, list[bool]]]:
    if not isinstance(cfg, dict):
        return None
    valves_cfg = cfg.get("valves", {})
    if not isinstance(valves_cfg, dict):
        return None

    relay_map = valves_cfg.get("relay_map", {})
    relay_states = {"relay": [False] * 16, "relay_8": [False] * 8}

    managed: set[int] = set()
    for key in ("co2_path", "co2_path_group2", "gas_main", "h2o_path", "hold", "flow_switch"):
        iv = _as_int(valves_cfg.get(key))
        if iv is not None:
            managed.add(iv)
    for key in ("co2_map", "co2_map_group2"):
        one_map = valves_cfg.get(key, {})
        if isinstance(one_map, dict):
            for val in one_map.values():
                iv = _as_int(val)
                if iv is not None:
                    managed.add(iv)

    open_set: set[int] = set()

    for logical_valve in managed:
        desired = logical_valve in open_set

        relay_name = "relay"
        channel = logical_valve
        entry = relay_map.get(str(logical_valve)) if isinstance(relay_map, dict) else None
        if isinstance(entry, dict):
            relay_name = str(entry.get("device", "relay") or "relay")
            mapped = _as_int(entry.get("channel"))
            if mapped is not None:
                channel = mapped
        if relay_name not in relay_states:
            continue
        if 1 <= channel <= len(relay_states[relay_name]):
            relay_states[relay_name][channel - 1] = bool(desired)

    return relay_states


def _verify_chamber(chamber: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    reader = getattr(chamber, "read_temp_c", None)
    if callable(reader):
        try:
            out["temp_c"] = reader()
        except Exception as exc:
            out["temp_err"] = str(exc)
    reader = getattr(chamber, "read_rh_pct", None)
    if callable(reader):
        try:
            out["rh_pct"] = reader()
        except Exception as exc:
            out["rh_err"] = str(exc)
    reader = getattr(chamber, "read_run_state", None)
    if callable(reader):
        try:
            out["run_state"] = reader()
        except Exception as exc:
            out["run_err"] = str(exc)
    return out


def _normalize_bool_sequence(values: Any) -> Optional[list[bool]]:
    if values is None:
        return None
    try:
        return [bool(item) for item in list(values)]
    except Exception:
        return None


def _tail_token(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return text.split()[-1].strip().upper()


def validate_safe_stop_result(result: Dict[str, Any], *, cfg: Optional[Dict[str, Any]] = None) -> list[str]:
    issues: list[str] = []
    relay_baseline = _baseline_relay_states_from_cfg(cfg)
    if relay_baseline:
        relay_actual = _normalize_bool_sequence(result.get("relay_states"))
        if relay_actual is not None:
            expected = relay_baseline["relay"][: len(relay_actual)]
            if relay_actual[: len(expected)] != expected:
                issues.append("relay state mismatch")
        relay8_actual = _normalize_bool_sequence(result.get("relay8_states"))
        if relay8_actual is not None:
            expected = relay_baseline["relay_8"][: len(relay8_actual)]
            if relay8_actual[: len(expected)] != expected:
                issues.append("relay_8 state mismatch")

    chamber = result.get("chamber")
    if isinstance(chamber, dict):
        run_state = chamber.get("run_state")
        if run_state not in (None, "", 0, "0", False):
            issues.append(f"chamber run_state not stopped: {run_state}")

    hcfg = (cfg or {}).get("workflow", {}).get("humidity_generator", {}) if isinstance(cfg, dict) else {}
    enforce_hgen_stop_check = bool(hcfg.get("safe_stop_enforce_flow_check", True))
    max_flow_lpm = _as_float(hcfg.get("safe_stop_max_flow_lpm"))
    if max_flow_lpm is None:
        max_flow_lpm = 0.05
    hgen_safe_stop = result.get("hgen_safe_stop")
    if isinstance(hgen_safe_stop, dict):
        for key in ("flow_off", "ctrl_off", "cool_off", "heat_off"):
            state = str(hgen_safe_stop.get(key) or "")
            if state == "failed":
                issues.append(f"humidity generator {key} failed")
    hgen_stop_check = result.get("hgen_stop_check")
    has_hgen_evidence = any(key in result for key in ("hgen_safe_stop", "hgen_stop_check", "hgen_current"))
    if enforce_hgen_stop_check and has_hgen_evidence:
        if isinstance(hgen_stop_check, dict):
            if hgen_stop_check.get("ok") is False:
                issues.append("humidity generator stop check failed")
        else:
            hgen_current = result.get("hgen_current")
            data = hgen_current.get("data", {}) if isinstance(hgen_current, dict) else {}
            flow_lpm = _as_float(data.get("Fl", data.get("Flux")))
            if flow_lpm is not None and flow_lpm > max_flow_lpm:
                issues.append(f"humidity generator flow still high: {flow_lpm}")
            else:
                issues.append("humidity generator stop check missing")

    pace_outp = _tail_token(result.get("pace_outp"))
    if pace_outp and pace_outp not in {"0", "OFF", "FALSE"}:
        issues.append(f"pace output not off: {pace_outp}")

    pace_isol = _tail_token(result.get("pace_isol"))
    if pace_isol and pace_isol not in {"1", "ON", "TRUE"}:
        issues.append(f"pace isolation not open: {pace_isol}")

    return issues


def perform_safe_stop(devices: Dict[str, Any], log_fn=_log, cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    relay_baseline = _baseline_relay_states_from_cfg(cfg)

    pace = devices.get("pace")
    if pace:
        try:
            enter = getattr(pace, "enter_atmosphere_mode", None)
            if callable(enter):
                enter()
                log_fn("pace enter_atmosphere_mode ok")
            else:
                pace.set_output(False)
                iso = getattr(pace, "set_isolation_open", None)
                if callable(iso):
                    iso(True)
                pace.vent(True)
                log_fn("pace fallback atmosphere sequence ok")
        except Exception as exc:
            log_fn(f"pace atmosphere sequence failed: {exc}")
        try:
            result["pace_pressure_hpa"] = pace.read_pressure()
            log_fn(f"pace pressure={result['pace_pressure_hpa']}")
        except Exception as exc:
            log_fn(f"pace read failed: {exc}")
        for key, cmd in (
            ("pace_vent", ":SOUR:PRES:LEV:IMM:AMPL:VENT?"),
            ("pace_outp", ":OUTP:STAT?"),
            ("pace_isol", ":OUTP:ISOL:STAT?"),
        ):
            try:
                result[key] = pace.query(cmd).strip()
                log_fn(f"{key}={result[key]}")
            except Exception as exc:
                log_fn(f"{key} query failed: {exc}")

    relay = devices.get("relay")
    if relay:
        desired = relay_baseline["relay"] if relay_baseline else [False] * 16
        _set_relay_states(relay, desired, "relay", log_fn)
        try:
            result["relay_states"] = relay.read_coils(0, 16)
            log_fn(f"relay states={result['relay_states']}")
        except Exception as exc:
            log_fn(f"relay verify failed: {exc}")

    relay8 = devices.get("relay_8")
    if relay8:
        desired = relay_baseline["relay_8"] if relay_baseline else [False] * 8
        _set_relay_states(relay8, desired, "relay8", log_fn)
        try:
            result["relay8_states"] = relay8.read_coils(0, 8)
            log_fn(f"relay8 states={result['relay8_states']}")
        except Exception as exc:
            log_fn(f"relay8 verify failed: {exc}")

    chamber = devices.get("temp_chamber")
    if chamber:
        try:
            chamber.stop()
            log_fn("chamber stop ok")
        except Exception as exc:
            log_fn(f"chamber stop failed: {exc}")
        chamber_state = _verify_chamber(chamber)
        if chamber_state:
            result["chamber"] = chamber_state
            log_fn(f"chamber state={chamber_state}")

    hgen = devices.get("humidity_gen")
    if hgen:
        try:
            stop_result = hgen.safe_stop()
            if isinstance(stop_result, dict):
                result["hgen_safe_stop"] = dict(stop_result)
                for key in ("flow_off", "ctrl_off", "cool_off", "heat_off"):
                    state = str(stop_result.get(key) or "")
                    error = stop_result.get(f"{key}_error")
                    if state == "ok":
                        log_fn(f"hgen {key} ok")
                    elif state == "failed":
                        log_fn(f"hgen {key} failed: {error}")
                log_fn(
                    "hgen safe_stop summary: "
                    f"flow_off={stop_result.get('flow_off')} "
                    f"ctrl_off={stop_result.get('ctrl_off')} "
                    f"cool_off={stop_result.get('cool_off')} "
                    f"heat_off={stop_result.get('heat_off')}"
                )
            else:
                log_fn("hgen safe_stop ok")
        except Exception as exc:
            log_fn(f"hgen safe_stop failed: {exc}")
        hcfg = (cfg or {}).get("workflow", {}).get("humidity_generator", {}) if isinstance(cfg, dict) else {}
        verify_flow = bool(hcfg.get("safe_stop_verify_flow", True))
        waiter = getattr(hgen, "wait_stopped", None)
        if verify_flow and callable(waiter):
            try:
                stop_check = waiter(
                    max_flow_lpm=_as_float(hcfg.get("safe_stop_max_flow_lpm")) or 0.05,
                    timeout_s=_as_float(hcfg.get("safe_stop_timeout_s")) or 5.0,
                    poll_s=_as_float(hcfg.get("safe_stop_poll_s")) or 0.5,
                )
                result["hgen_stop_check"] = stop_check
                log_fn(f"hgen stop check={stop_check}")
            except Exception as exc:
                log_fn(f"hgen stop verify failed: {exc}")
        elif callable(waiter):
            result["hgen_stop_check"] = {"skipped": True, "reason": "flow verification disabled"}
            log_fn("hgen stop flow verification skipped")
        try:
            result["hgen_current"] = hgen.fetch_all()
            log_fn(f"hgen current={result['hgen_current']}")
        except Exception as exc:
            log_fn(f"hgen verify failed: {exc}")

    gauge = devices.get("pressure_gauge")
    if gauge:
        try:
            result["gauge_pressure_hpa"] = gauge.read_pressure()
            log_fn(f"gauge pressure={result['gauge_pressure_hpa']}")
        except Exception as exc:
            log_fn(f"gauge read failed: {exc}")

    return result


def perform_safe_stop_with_retries(
    devices: Dict[str, Any],
    *,
    log_fn=_log,
    cfg: Optional[Dict[str, Any]] = None,
    attempts: int = 3,
    retry_delay_s: float = 1.5,
) -> Dict[str, Any]:
    max_attempts = max(1, int(attempts))
    delay_s = max(0.0, float(retry_delay_s))
    last_result: Dict[str, Any] = {}
    for attempt in range(1, max_attempts + 1):
        if attempt > 1:
            log_fn(f"safe-stop retry {attempt}/{max_attempts}")
        result = perform_safe_stop(devices, log_fn=log_fn, cfg=cfg)
        issues = validate_safe_stop_result(result, cfg=cfg)
        result["safe_stop_attempt"] = attempt
        result["safe_stop_verified"] = not issues
        result["safe_stop_issues"] = list(issues)
        last_result = result
        if not issues:
            return result
        log_fn(f"safe-stop verification failed: {', '.join(issues)}")
        if attempt < max_attempts and delay_s > 0:
            time.sleep(delay_s)
    return last_result


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Safe-stop all configured devices")
    parser.add_argument("--config", default="configs/default_config.json")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    ns = parse_args(argv)
    cfg = load_config(ns.config)
    safe_cfg = _safe_stop_cfg(cfg)
    out_dir = Path(str(cfg.get("paths", {}).get("output_dir", "logs")))
    run_id = "safe_stop_" + datetime.now().strftime("%Y%m%d_%H%M%S")
    logger = RunLogger(out_dir, run_id=run_id)
    _log(f"run_dir={logger.run_dir}")
    devices: Dict[str, Any] = {}
    try:
        devices = _build_devices(safe_cfg, io_logger=logger)
        perform_safe_stop(devices, log_fn=_log, cfg=cfg)
        return 0
    finally:
        _close_devices(devices)
        logger.close()


if __name__ == "__main__":
    raise SystemExit(main())
