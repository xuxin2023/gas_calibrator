"""V1.5 formal route readiness checks.

This module owns the pre-open-flow route readiness contract used by formal
initialization. It is intentionally separate from CO2/H2O point runners so a
missing valve map, offline relay, offline dewpoint meter, or unusable N2
pre-purge valve blocks before any temperature-chamber soak.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence


SCHEMA = "v1_5_formal_route_readiness_v1"
EVIDENCE_FILE = "formal_route_readiness.json"


@dataclass(frozen=True)
class RelayTarget:
    logical_valve: str
    relay_name: str
    channel: int
    route_role: str

    def to_json(self) -> dict[str, Any]:
        return {
            "logical_valve": self.logical_valve,
            "relay_name": self.relay_name,
            "channel": self.channel,
            "route_role": self.route_role,
        }


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on", "enabled"}


def _explicit_false(value: Any) -> bool:
    if isinstance(value, bool):
        return value is False
    return str(value or "").strip().lower() in {"0", "false", "no", "n", "off", "disabled"}


def _as_optional_float(value: Any) -> float | None:
    if value in (None, "", "None", "null"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _nested_mapping(mapping: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = mapping.get(key)
    return value if isinstance(value, Mapping) else {}


def _add_issue(
    issues: list[dict[str, Any]],
    code: str,
    message: str,
    *,
    severity: str = "fail",
    details: Mapping[str, Any] | None = None,
) -> None:
    issues.append(
        {
            "code": code,
            "severity": severity,
            "message": message,
            "details": dict(details or {}),
        }
    )


def _normal_logical_valve(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.endswith(".0") and text[:-2].isdigit():
        return text[:-2]
    return text


def infer_n2_prepurge_seconds(cfg: Mapping[str, Any]) -> float:
    workflow = _nested_mapping(cfg, "workflow")
    nitrogen = _nested_mapping(workflow, "nitrogen_purge")
    for key in ("co2_prepurge_s", "prepurge_s", "purge_s"):
        numeric = _as_optional_float(nitrogen.get(key))
        if numeric is not None:
            return max(0.0, numeric)
    return 0.0


def infer_n2_source_valve(cfg: Mapping[str, Any], override: Any = None) -> str:
    if override not in (None, ""):
        return _normal_logical_valve(override)
    valves = _nested_mapping(cfg, "valves")
    workflow = _nested_mapping(cfg, "workflow")
    nitrogen = _nested_mapping(workflow, "nitrogen_purge")
    for source in (nitrogen, valves):
        for key in (
            "source_valve",
            "logical_valve",
            "nitrogen_purge_source",
            "n2_purge_source",
            "nitrogen_source",
            "n2_source",
        ):
            valve = _normal_logical_valve(source.get(key))
            if valve:
                return valve
    return ""


def collect_formal_route_logical_valves(
    cfg: Mapping[str, Any],
    *,
    n2_prepurge_s: float | None = None,
    n2_source_valve: Any = None,
) -> dict[str, str]:
    """Return logical valves required by formal N2/CO2/H2O route readiness."""

    valves = _nested_mapping(cfg, "valves")
    out: dict[str, str] = {}

    def add(value: Any, role: str) -> None:
        logical = _normal_logical_valve(value)
        if logical:
            out.setdefault(logical, role)

    # CO2 route: source valves plus total/open-flow path valves used by V1.5.
    for key in ("h2o_path", "gas_main", "co2_path", "co2_path_group2"):
        add(valves.get(key), f"co2_route.{key}")
    for map_key in ("co2_map", "co2_map_group2"):
        source_map = valves.get(map_key)
        if isinstance(source_map, Mapping):
            for label, logical in source_map.items():
                add(logical, f"co2_source.{map_key}.{label}")

    # H2O route: the open-flow humidity route valves.
    for key in ("h2o_path", "hold", "flow_switch"):
        add(valves.get(key), f"h2o_route.{key}")

    prepurge_s = infer_n2_prepurge_seconds(cfg) if n2_prepurge_s is None else max(0.0, float(n2_prepurge_s))
    if prepurge_s > 0:
        add(infer_n2_source_valve(cfg, n2_source_valve), "n2_prepurge.source")

    return out


def _parse_relay_target(
    relay_map: Mapping[str, Any],
    logical_valve: str,
    route_role: str,
) -> tuple[RelayTarget | None, str]:
    if logical_valve not in relay_map:
        return None, "missing"
    raw = relay_map.get(logical_valve)
    relay_name = "relay"
    channel: Any = None
    if isinstance(raw, Mapping):
        relay_name = str(raw.get("relay") or raw.get("device") or raw.get("relay_name") or "relay").strip()
        channel = raw.get("channel", raw.get("coil", raw.get("index")))
    elif isinstance(raw, (list, tuple)) and len(raw) >= 2:
        relay_name = str(raw[0] or "relay").strip()
        channel = raw[1]
    elif isinstance(raw, str):
        text = raw.strip()
        if ":" in text:
            left, right = text.split(":", 1)
            relay_name = left.strip() or "relay"
            channel = right.strip()
        else:
            channel = text
    else:
        channel = raw
    try:
        channel_i = int(float(str(channel).strip()))
    except Exception:
        return None, "bad_channel"
    relay_name = relay_name or "relay"
    if relay_name not in {"relay", "relay_8"}:
        return None, "bad_relay"
    if channel_i <= 0:
        return None, "bad_channel"
    return RelayTarget(logical_valve, relay_name, channel_i, route_role), ""


def resolve_formal_route_relay_targets(cfg: Mapping[str, Any]) -> tuple[list[RelayTarget], list[dict[str, Any]]]:
    issues: list[dict[str, Any]] = []
    valves = _nested_mapping(cfg, "valves")
    relay_map = valves.get("relay_map")
    if not isinstance(relay_map, Mapping):
        return [], [
            {
                "code": "RELAY_MAP_MISSING",
                "severity": "fail",
                "message": "valves.relay_map is missing or invalid.",
                "details": {},
            }
        ]
    logical = collect_formal_route_logical_valves(cfg)
    targets: list[RelayTarget] = []
    for valve, role in sorted(logical.items()):
        target, reason = _parse_relay_target(relay_map, valve, role)
        if target is None:
            _add_issue(
                issues,
                "RELAY_MAP_ENTRY_INVALID" if reason != "missing" else "RELAY_MAP_ENTRY_MISSING",
                f"Logical valve {valve!r} for {role} is not a valid relay_map entry.",
                details={"logical_valve": valve, "route_role": role, "reason": reason},
            )
            continue
        targets.append(target)
    return targets, issues


def _relay_bits(relay: Any, count: int) -> list[bool]:
    bits = relay.read_coils(0, count)
    if hasattr(bits, "bits"):
        bits = bits.bits
    return [bool(item) for item in list(bits or [])[:count]]


def _relay_channel_count(relay_name: str) -> int:
    return 8 if relay_name == "relay_8" else 16


def _device_enabled(cfg: Mapping[str, Any], key: str) -> bool:
    devices = _nested_mapping(cfg, "devices")
    item = devices.get(key)
    if not isinstance(item, Mapping):
        return False
    if "enabled" in item and _explicit_false(item.get("enabled")):
        return False
    return bool(item.get("port"))


def _build_route_devices(cfg: Mapping[str, Any]) -> dict[str, Any]:
    from ..devices.dewpoint_meter import DewpointMeter
    from ..devices.relay import RelayController

    devices_cfg = _nested_mapping(cfg, "devices")
    built: dict[str, Any] = {}
    try:
        for name in ("relay", "relay_8"):
            item = devices_cfg.get(name)
            if not isinstance(item, Mapping) or not _device_enabled(cfg, name):
                continue
            relay = RelayController(
                str(item["port"]),
                int(item.get("baud", 9600)),
                addr=int(item.get("addr", 1)),
            )
            relay.open()
            built[name] = relay
        item = devices_cfg.get("dewpoint_meter")
        if isinstance(item, Mapping) and _device_enabled(cfg, "dewpoint_meter"):
            dewpoint = DewpointMeter(
                str(item["port"]),
                int(item.get("baud", 9600)),
                station=str(item.get("station", "A")),
            )
            dewpoint.open()
            built["dewpoint"] = dewpoint
    except Exception:
        _close_route_devices(built)
        raise
    return built


def _close_route_devices(devices: Mapping[str, Any]) -> None:
    for dev in devices.values():
        close = getattr(dev, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                pass


def build_formal_route_readiness_model(
    cfg: Mapping[str, Any],
    *,
    output_dir: str | Path,
    n2_prepurge_s: float | None = None,
    n2_source_valve: Any = None,
    build_devices: Callable[[Mapping[str, Any]], Mapping[str, Any]] | None = None,
    close_devices: Callable[[Mapping[str, Any]], None] | None = None,
) -> dict[str, Any]:
    output = Path(output_dir).resolve()
    prepurge_s = infer_n2_prepurge_seconds(cfg) if n2_prepurge_s is None else max(0.0, float(n2_prepurge_s))
    source_valve = infer_n2_source_valve(cfg, n2_source_valve) if prepurge_s > 0 else ""
    targets, issues = resolve_formal_route_relay_targets(cfg)
    checks: list[dict[str, Any]] = []
    checks.append(
        {
            "check": "formal_route_relay_map",
            "status": "pass" if not issues else "fail",
            "details": {
                "logical_valves": collect_formal_route_logical_valves(
                    cfg,
                    n2_prepurge_s=prepurge_s,
                    n2_source_valve=source_valve,
                ),
                "relay_targets": [target.to_json() for target in targets],
            },
        }
    )

    devices_cfg = _nested_mapping(cfg, "devices")
    for name in ("relay", "relay_8", "dewpoint_meter"):
        if not _device_enabled(cfg, name):
            _add_issue(
                issues,
                "DEVICE_CONFIG_MISSING_OR_DISABLED",
                f"{name} is not enabled with a usable port in runtime config.",
                details={"device": name, "config": dict(devices_cfg.get(name) or {})},
            )

    devices: Mapping[str, Any] = {}
    builder = build_devices or _build_route_devices
    closer = close_devices or _close_route_devices
    if not any(issue["severity"] == "fail" for issue in issues):
        try:
            devices = builder(cfg)
        except Exception as exc:
            _add_issue(issues, "DEVICE_OPEN_FAILED", f"Route readiness device open failed: {exc}")
            devices = {}

    try:
        relay_check_details: dict[str, Any] = {}
        if devices:
            for name in ("relay", "relay_8"):
                relay = devices.get(name)
                if relay is None:
                    _add_issue(issues, "RELAY_DEVICE_NOT_BUILT", f"{name} was not opened by route readiness.")
                    continue
                count = _relay_channel_count(name)
                try:
                    before = _relay_bits(relay, count)
                    if len(before) < count:
                        _add_issue(
                            issues,
                            "RELAY_READBACK_SHORT",
                            f"{name} returned {len(before)} coil states, expected {count}.",
                            details={"relay": name, "returned": len(before), "expected": count},
                        )
                        continue
                    # Same-state writes prove the write path without changing route state.
                    for target in [item for item in targets if item.relay_name == name]:
                        if target.channel > count:
                            _add_issue(
                                issues,
                                "RELAY_CHANNEL_OUT_OF_RANGE",
                                f"{target.logical_valve} maps to {name} channel {target.channel}, outside 1..{count}.",
                                details=target.to_json(),
                            )
                            continue
                        relay.set_valve(target.channel, before[target.channel - 1])
                    after = _relay_bits(relay, count)
                    relay_check_details[name] = {"before": before, "after_same_state_write": after}
                except Exception as exc:
                    _add_issue(issues, "RELAY_READ_WRITE_FAILED", f"{name} read/write readiness failed: {exc}")

            dewpoint = devices.get("dewpoint")
            if dewpoint is None:
                _add_issue(issues, "DEWPOINT_DEVICE_NOT_BUILT", "Dewpoint meter was not opened by route readiness.")
            else:
                try:
                    status = dewpoint.status()
                except Exception as exc:
                    _add_issue(issues, "DEWPOINT_STATUS_FAILED", f"Dewpoint status read failed: {exc}")
                    status = {}
                checks.append(
                    {
                        "check": "dewpoint_online",
                        "status": "pass" if status.get("ok") else "fail",
                        "details": dict(status or {}),
                    }
                )
                if not status.get("ok"):
                    _add_issue(issues, "DEWPOINT_OFFLINE", "Dewpoint meter did not return a valid status frame.")

            if prepurge_s > 0:
                n2_target = next((item for item in targets if item.logical_valve == source_valve), None)
                if n2_target is None:
                    _add_issue(
                        issues,
                        "N2_SOURCE_TARGET_MISSING",
                        "N2 prepurge is enabled but the N2 source relay target is unavailable.",
                        details={"n2_source_valve": source_valve, "n2_prepurge_s": prepurge_s},
                    )
                else:
                    relay = devices.get(n2_target.relay_name)
                    if relay is None:
                        _add_issue(issues, "N2_RELAY_NOT_BUILT", "N2 relay device was not opened.")
                    else:
                        count = _relay_channel_count(n2_target.relay_name)
                        try:
                            relay.set_valve(n2_target.channel, True)
                            open_bits = _relay_bits(relay, count)
                            open_ok = bool(open_bits[n2_target.channel - 1]) if len(open_bits) >= n2_target.channel else False
                            relay.set_valve(n2_target.channel, False)
                            close_bits = _relay_bits(relay, count)
                            close_ok = not bool(close_bits[n2_target.channel - 1]) if len(close_bits) >= n2_target.channel else False
                            checks.append(
                                {
                                    "check": "n2_prepurge_valve_open_close",
                                    "status": "pass" if open_ok and close_ok else "fail",
                                    "details": {
                                        **n2_target.to_json(),
                                        "open_readback_ok": open_ok,
                                        "close_readback_ok": close_ok,
                                    },
                                }
                            )
                            if not (open_ok and close_ok):
                                _add_issue(
                                    issues,
                                    "N2_VALVE_READBACK_FAILED",
                                    "N2 source valve did not read back open and closed during readiness.",
                                    details=n2_target.to_json(),
                                )
                        except Exception as exc:
                            try:
                                relay.set_valve(n2_target.channel, False)
                            except Exception:
                                pass
                            _add_issue(issues, "N2_VALVE_OPEN_CLOSE_FAILED", f"N2 source valve open/close failed: {exc}")

        if relay_check_details:
            checks.append(
                {
                    "check": "relay_ports_readable_writable",
                    "status": "pass",
                    "details": relay_check_details,
                }
            )
    finally:
        if devices:
            closer(devices)

    ok = not any(issue["severity"] == "fail" for issue in issues)
    return {
        "schema": SCHEMA,
        "created_at": _now(),
        "status": "pass" if ok else "fail",
        "ok": ok,
        "output_dir": str(output),
        "n2_prepurge_enabled": prepurge_s > 0,
        "n2_prepurge_s": prepurge_s,
        "n2_source_valve": source_valve,
        "opens_com_ports": True,
        "controls_n2_prepurge_valve": prepurge_s > 0,
        "controls_co2_route": False,
        "controls_h2o_route": False,
        "writes_coefficients": False,
        "writes_device_id": False,
        "checks": checks,
        "issues": issues,
        "physical_meaning": (
            "Formal route readiness proves the N2/CO2/H2O logical valves are mapped, relay and relay_8 "
            "ports are readable/writable, the dewpoint meter is online, and the N2 pre-purge valve can "
            "actually open before any temperature soak or open-flow sampling starts."
        ),
    }


def write_formal_route_readiness_model(model: Mapping[str, Any], output_dir: str | Path) -> Path:
    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    path = root / EVIDENCE_FILE
    path.write_text(json.dumps(dict(model), ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def read_formal_route_readiness_model(path: str | Path) -> dict[str, Any] | None:
    source = Path(path)
    if not source.exists():
        return None
    try:
        payload = json.loads(source.read_text(encoding="utf-8-sig"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None
