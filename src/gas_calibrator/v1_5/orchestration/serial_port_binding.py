"""V1.5 runtime serial-port binding helpers.

COM ports are transport paths, not calibration identities.  This module only
handles the known industrial-PC reference-device bank shift between COM24-COM31
and COM16-COM23.  Gas analyzer identity binding remains MODE2-ID based and is
handled separately by the GETCO snapshot stage.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any


REFERENCE_DEVICE_KEYS = {
    "pressure_controller",
    "pressure_gauge",
    "dewpoint_meter",
    "humidity_generator",
    "temperature_chamber",
    "thermometer",
    "relay",
    "relay_8",
}

PROTECTED_ANALYZER_KEYS = {"gas_analyzer", "gas_analyzers"}
REFERENCE_SOURCE_BANK = tuple(f"COM{idx}" for idx in range(24, 32))
REFERENCE_TARGET_BANK = tuple(f"COM{idx}" for idx in range(16, 24))
GAS_ANALYZER_PROTECTED_BANK = tuple(f"COM{idx}" for idx in range(35, 43))

REFERENCE_ROLE_ALIASES = {
    "pressure_controller": {
        "pressure_controller",
        "pressure_control",
        "pace",
        "pace_controller",
        "k0472",
    },
    "pressure_gauge": {
        "pressure_gauge",
        "pressure_meter",
        "digital_pressure_gauge",
        "com22",
        "barometer",
    },
    "dewpoint_meter": {
        "dewpoint_meter",
        "dew_point_meter",
        "dewpoint",
        "precision_dewpoint_meter",
    },
    "humidity_generator": {
        "humidity_generator",
        "wet_generator",
        "humidity_source",
        "dewpoint_generator",
    },
    "temperature_chamber": {
        "temperature_chamber",
        "temp_chamber",
        "environment_chamber",
        "thermal_chamber",
    },
    "thermometer": {
        "thermometer",
        "digital_thermometer",
        "temperature_meter",
        "temperature_reference",
        "pt100",
    },
    "relay": {"relay", "valve_relay"},
    "relay_8": {"relay_8", "relay", "valve_relay"},
}


@dataclass(frozen=True)
class RuntimeSerialPortBindingResult:
    """Result of resolving a V1.5 runtime serial-port binding policy."""

    status: str
    reason: str
    config: dict[str, Any]
    evidence_rows: tuple[dict[str, Any], ...]
    changed_count: int
    blocked_count: int
    enabled: bool

    def summary(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "reason": self.reason,
            "enabled": self.enabled,
            "changed_count": self.changed_count,
            "blocked_count": self.blocked_count,
            "created_at": datetime.now().isoformat(timespec="seconds"),
        }


def normalize_com_port(value: Any) -> str:
    text = str(value or "").strip().upper()
    match = re.fullmatch(r"COM\s*([0-9]+)", text)
    if not match:
        return text
    return f"COM{int(match.group(1))}"


def _copy_config(cfg: Mapping[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(cfg, ensure_ascii=False, default=str))


def _parse_ports(values: Iterable[Any] | str | None) -> set[str] | None:
    if values is None:
        return None
    if isinstance(values, str):
        parts: Iterable[Any] = values.replace(";", ",").split(",")
    else:
        parts = values
    out = {normalize_com_port(item) for item in parts if str(item or "").strip()}
    return out


def _parse_device_keys(values: Iterable[Any] | str | None) -> set[str] | None:
    if values is None:
        return None
    if isinstance(values, str):
        parts: Iterable[Any] = values.replace(";", ",").split(",")
    else:
        parts = values
    return {_normalize_role(item) for item in parts if str(item or "").strip()}


def _iter_inventory_ports(values: Any) -> Iterable[str]:
    if values is None:
        return ()
    if isinstance(values, str):
        return _parse_ports(values) or ()
    if isinstance(values, Mapping):
        if "ports" in values:
            return _iter_inventory_ports(values.get("ports"))
        return (normalize_com_port(key) for key in values)
    ports: list[str] = []
    for item in values:
        if isinstance(item, Mapping):
            port = item.get("port") or item.get("runtime_port") or item.get("configured_port") or item.get("com")
            ports.append(normalize_com_port(port))
        else:
            ports.append(normalize_com_port(item))
    return (port for port in ports if port)


def _normalize_role(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def expected_reference_role_matches(device_key: str, observed_role: Any) -> bool:
    """Return whether a protocol/inventory role is plausible for a device key."""

    expected = _normalize_role(device_key)
    observed = _normalize_role(observed_role)
    if not observed:
        return False
    aliases = REFERENCE_ROLE_ALIASES.get(expected, {expected})
    return observed in aliases or observed == expected


def _coerce_protocol_entry(port: str, value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        entry = dict(value)
    else:
        entry = {"observed_role": value}
    entry.setdefault("port", normalize_com_port(port))
    return entry


def _normalize_protocol_inventory(value: Any) -> dict[str, dict[str, Any]] | None:
    if value is None:
        return None
    if isinstance(value, Mapping):
        if "ports" in value:
            return _normalize_protocol_inventory(value.get("ports"))
        out: dict[str, dict[str, Any]] = {}
        for key, item in value.items():
            if isinstance(item, Mapping) and any(
                field in item for field in ("port", "runtime_port", "configured_port", "com")
            ):
                port = normalize_com_port(
                    item.get("port") or item.get("runtime_port") or item.get("configured_port") or item.get("com")
                )
            else:
                port = normalize_com_port(key)
            if port:
                out[port] = _coerce_protocol_entry(port, item)
        return out
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        out = {}
        for item in value:
            if not isinstance(item, Mapping):
                continue
            port = normalize_com_port(
                item.get("port") or item.get("runtime_port") or item.get("configured_port") or item.get("com")
            )
            if port:
                out[port] = dict(item)
        return out
    raise TypeError("protocol_inventory must be a mapping, sequence of mappings, or None")


def _entry_observed_role(entry: Mapping[str, Any] | None) -> str:
    if not entry:
        return ""
    for key in (
        "observed_role",
        "role",
        "device_role",
        "protocol_role",
        "device_key",
        "expected_role",
        "kind",
        "type",
    ):
        value = entry.get(key)
        if value:
            return str(value)
    return ""


def _apply_protocol_guard(
    row: dict[str, Any],
    *,
    device_key: str,
    runtime_port: str,
    protocol_inventory: dict[str, dict[str, Any]] | None,
    require_protocol_match: bool,
) -> bool:
    row["protocol_status"] = "not_checked"
    row["observed_runtime_role"] = ""
    row["protocol_match"] = ""

    if protocol_inventory is None:
        if require_protocol_match:
            row["protocol_status"] = "blocked_missing_protocol_inventory"
            row["status"] = "blocked_missing_protocol_inventory"
            row["blocked"] = True
            return True
        return False

    entry = protocol_inventory.get(normalize_com_port(runtime_port))
    if entry is None:
        row["protocol_status"] = "missing_runtime_port_protocol_identity"
        if require_protocol_match:
            row["status"] = "blocked_missing_runtime_port_protocol_identity"
            row["blocked"] = True
            return True
        return False

    observed_role = _entry_observed_role(entry)
    match = expected_reference_role_matches(device_key, observed_role)
    row["observed_runtime_role"] = observed_role
    row["protocol_match"] = match
    row["protocol_status"] = "matched" if match else "mismatch"
    if require_protocol_match and not match:
        row["status"] = "blocked_protocol_role_mismatch"
        row["blocked"] = True
        return True
    return False


def _bank_ports(start: int, count: int = 8) -> tuple[str, ...]:
    return tuple(f"COM{idx}" for idx in range(int(start), int(start) + int(count)))


def allowed_bank_shift_map() -> dict[str, str]:
    """Return the only V1.5 reference-device port shifts allowed by policy."""

    low = REFERENCE_TARGET_BANK
    high = REFERENCE_SOURCE_BANK
    mapping: dict[str, str] = {}
    for source, target in zip(high, low):
        mapping[source] = target
        mapping[target] = source
    return mapping


def classify_v1_5_serial_port(port: Any) -> str:
    """Classify a COM port for V1.5 runtime-binding safety evidence."""

    normalized = normalize_com_port(port)
    if normalized in REFERENCE_SOURCE_BANK:
        return "reference_source_bank_com24_31"
    if normalized in REFERENCE_TARGET_BANK:
        return "reference_target_bank_com16_23"
    if normalized in GAS_ANALYZER_PROTECTED_BANK:
        return "gas_analyzer_protected_bank_com35_42"
    return "outside_v1_5_known_banks"


def build_v1_5_serial_port_inventory(
    ports: Iterable[Any] | str | Mapping[str, Any],
    *,
    protocol_inventory: Any = None,
    source: str = "provided",
) -> dict[str, Any]:
    """Build a no-open-COM serial-port inventory artifact.

    This produces evidence that a UI or operator can feed into the optional
    reference-device bank-shift resolver. It does not identify gas analyzers and
    never sends device commands.
    """

    normalized_ports = sorted({port for port in _iter_inventory_ports(ports) if port})
    protocol_ports = _normalize_protocol_inventory(protocol_inventory)
    mapping = allowed_bank_shift_map()
    rows: list[dict[str, Any]] = []
    for port in normalized_ports:
        protocol_entry = protocol_ports.get(port) if protocol_ports else None
        rows.append(
            {
                "port": port,
                "bank_role": classify_v1_5_serial_port(port),
                "allowed_reference_shift_peer": mapping.get(port, ""),
                "allowed_for_reference_bank_shift": port in mapping,
                "gas_analyzer_identity_must_use_mode2_id": port in GAS_ANALYZER_PROTECTED_BANK,
                "observed_role": _entry_observed_role(protocol_entry),
                "source": source,
                "opens_com_ports": False,
                "sends_device_commands": False,
            }
        )

    source_ports = {row["port"] for row in rows if row["bank_role"] == "reference_source_bank_com24_31"}
    target_ports = {row["port"] for row in rows if row["bank_role"] == "reference_target_bank_com16_23"}
    if source_ports and target_ports:
        reference_bank_state = "ambiguous_both_reference_banks_present"
    elif source_ports:
        reference_bank_state = "source_bank_present_com24_31"
    elif target_ports:
        reference_bank_state = "target_bank_present_com16_23"
    else:
        reference_bank_state = "no_reference_bank_ports_present"

    return {
        "schema": "v1_5_runtime_serial_port_inventory_v1",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "source": source,
        "opens_com_ports": False,
        "sends_device_commands": False,
        "gas_analyzer_identity_policy": "COM35-COM42_are_paths_only_MODE2_device_id_required",
        "reference_bank_shift_policy": "optional_default_off_COM24_31_between_COM16_23_only",
        "reference_bank_state": reference_bank_state,
        "port_count": len(rows),
        "reference_source_bank_count": len(source_ports),
        "reference_target_bank_count": len(target_ports),
        "gas_analyzer_protected_bank_count": sum(
            1 for row in rows if row["bank_role"] == "gas_analyzer_protected_bank_com35_42"
        ),
        "ports": rows,
    }


def _iter_device_entries(devices: Mapping[str, Any]) -> Iterable[tuple[str, dict[str, Any]]]:
    for key, value in devices.items():
        if key in PROTECTED_ANALYZER_KEYS:
            continue
        if key in REFERENCE_DEVICE_KEYS and isinstance(value, dict):
            yield key, value


def resolve_reference_port_bank_shift(
    cfg: Mapping[str, Any],
    *,
    enabled: bool = False,
    available_ports: Iterable[Any] | str | None = None,
    protocol_inventory: Any = None,
    require_protocol_match: bool = False,
    device_keys: Iterable[Any] | str | None = None,
) -> RuntimeSerialPortBindingResult:
    """Resolve the optional COM24-COM31 <-> COM16-COM23 bank shift.

    The resolver never probes hardware and never changes gas analyzer ports.
    A caller that wants automatic remap must explicitly enable it and provide
    an already-collected available-port set.  Optional protocol inventory is
    treated as externally supplied evidence; this resolver never opens COM.
    """

    original_payload = _copy_config(cfg)
    payload = _copy_config(cfg)
    devices = payload.setdefault("devices", {})
    if not isinstance(devices, dict):
        return RuntimeSerialPortBindingResult(
            status="blocked",
            reason="devices_section_missing_or_invalid",
            config=payload,
            evidence_rows=(),
            changed_count=0,
            blocked_count=1,
            enabled=bool(enabled),
        )

    selected_device_keys = _parse_device_keys(device_keys)
    unknown_device_keys = (
        selected_device_keys - REFERENCE_DEVICE_KEYS if selected_device_keys is not None else set()
    )
    if unknown_device_keys:
        return RuntimeSerialPortBindingResult(
            status="blocked",
            reason="unknown_reference_device_keys",
            config=payload,
            evidence_rows=tuple(
                {
                    "device_key": key,
                    "status": "blocked_unknown_reference_device_key",
                    "changed": False,
                    "blocked": True,
                }
                for key in sorted(unknown_device_keys)
            ),
            changed_count=0,
            blocked_count=len(unknown_device_keys),
            enabled=bool(enabled),
        )

    rows: list[dict[str, Any]] = []
    available = _parse_ports(available_ports)
    protocol_ports = _normalize_protocol_inventory(protocol_inventory)
    mapping = allowed_bank_shift_map()

    for analyzer_key in PROTECTED_ANALYZER_KEYS:
        analyzer_payload = devices.get(analyzer_key)
        if isinstance(analyzer_payload, list):
            for item in analyzer_payload:
                if isinstance(item, dict):
                    rows.append(
                        {
                            "device_key": analyzer_key,
                            "device_name": item.get("name", ""),
                            "configured_port": normalize_com_port(item.get("port")),
                            "runtime_port": normalize_com_port(item.get("port")),
                            "status": "protected_gas_analyzer_identity_uses_mode2_id",
                            "changed": False,
                            "blocked": False,
                        }
                    )
        elif isinstance(analyzer_payload, dict):
            rows.append(
                {
                    "device_key": analyzer_key,
                    "device_name": analyzer_payload.get("name", ""),
                    "configured_port": normalize_com_port(analyzer_payload.get("port")),
                    "runtime_port": normalize_com_port(analyzer_payload.get("port")),
                    "status": "protected_gas_analyzer_identity_uses_mode2_id",
                    "changed": False,
                    "blocked": False,
                }
            )

    if not enabled:
        for key, item in _iter_device_entries(devices):
            rows.append(
                {
                    "device_key": key,
                    "device_name": item.get("name", key),
                    "configured_port": normalize_com_port(item.get("port")),
                    "runtime_port": normalize_com_port(item.get("port")),
                    "status": "disabled",
                    "changed": False,
                    "blocked": False,
                }
            )
        return RuntimeSerialPortBindingResult(
            status="disabled",
            reason="reference_port_bank_shift_disabled",
            config=payload,
            evidence_rows=tuple(rows),
            changed_count=0,
            blocked_count=0,
            enabled=False,
        )

    changed_count = 0
    blocked_count = 0
    if available is None:
        blocked_count = 1
        for key, item in _iter_device_entries(devices):
            if selected_device_keys is not None and key not in selected_device_keys:
                rows.append(
                    {
                        "device_key": key,
                        "device_name": item.get("name", key),
                        "configured_port": normalize_com_port(item.get("port")),
                        "runtime_port": normalize_com_port(item.get("port")),
                        "status": "not_selected_unchanged",
                        "changed": False,
                        "blocked": False,
                    }
                )
                continue
            rows.append(
                {
                    "device_key": key,
                    "device_name": item.get("name", key),
                    "configured_port": normalize_com_port(item.get("port")),
                    "runtime_port": normalize_com_port(item.get("port")),
                    "status": "blocked_missing_available_ports",
                    "changed": False,
                    "blocked": True,
                }
            )
        return RuntimeSerialPortBindingResult(
            status="blocked",
            reason="enabled_requires_available_port_inventory",
            config=payload,
            evidence_rows=tuple(rows),
            changed_count=0,
            blocked_count=blocked_count,
            enabled=True,
        )

    for key, item in _iter_device_entries(devices):
        configured = normalize_com_port(item.get("port"))
        candidate = mapping.get(configured, "")
        row = {
            "device_key": key,
            "device_name": item.get("name", key),
            "configured_port": configured,
            "runtime_port": configured,
            "candidate_runtime_port": candidate,
            "status": "unchanged_not_in_reference_bank",
            "changed": False,
            "blocked": False,
        }
        if selected_device_keys is not None and key not in selected_device_keys:
            row["status"] = "not_selected_unchanged"
            rows.append(row)
            continue
        if configured in mapping:
            configured_present = configured in available
            candidate_present = candidate in available
            if configured_present and candidate_present:
                configured_entry = protocol_ports.get(configured) if protocol_ports else None
                candidate_entry = protocol_ports.get(candidate) if protocol_ports else None
                configured_role = _entry_observed_role(configured_entry)
                candidate_role = _entry_observed_role(candidate_entry)
                configured_match = expected_reference_role_matches(key, configured_role)
                candidate_match = expected_reference_role_matches(key, candidate_role)
                row.update(
                    {
                        "configured_observed_role": configured_role,
                        "candidate_observed_role": candidate_role,
                        "configured_protocol_match": configured_match,
                        "candidate_protocol_match": candidate_match,
                    }
                )
                if configured_match == candidate_match:
                    row["status"] = (
                        "blocked_both_bank_protocol_match"
                        if configured_match
                        else "blocked_both_bank_ports_present"
                    )
                    row["protocol_status"] = "ambiguous" if configured_match else "no_unique_match"
                    row["blocked"] = True
                    blocked_count += 1
                elif configured_match:
                    row["status"] = "unchanged_configured_port_protocol_match"
                    row["protocol_status"] = "matched"
                    row["observed_runtime_role"] = configured_role
                    row["protocol_match"] = True
                else:
                    row["runtime_port"] = candidate
                    row["status"] = "mapped_by_unique_protocol_match"
                    row["protocol_status"] = "matched"
                    row["observed_runtime_role"] = candidate_role
                    row["protocol_match"] = True
                    row["changed"] = True
                    item["configured_port"] = configured
                    item["port"] = candidate
                    item["runtime_port"] = candidate
                    item["runtime_port_binding_source"] = "v1_5_reference_bank_shift_protocol_identity"
                    item["runtime_port_binding_frozen"] = True
                    changed_count += 1
            elif candidate_present and not configured_present:
                row["runtime_port"] = candidate
                if _apply_protocol_guard(
                    row,
                    device_key=key,
                    runtime_port=candidate,
                    protocol_inventory=protocol_ports,
                    require_protocol_match=require_protocol_match,
                ):
                    blocked_count += 1
                else:
                    item["configured_port"] = configured
                    item["port"] = candidate
                    item["runtime_port"] = candidate
                    item["runtime_port_binding_source"] = "v1_5_reference_bank_shift"
                    item["runtime_port_binding_frozen"] = True
                    row["status"] = "mapped_by_reference_bank_shift"
                    row["changed"] = True
                    changed_count += 1
            elif configured_present:
                row["status"] = "unchanged_configured_port_present"
                if _apply_protocol_guard(
                    row,
                    device_key=key,
                    runtime_port=configured,
                    protocol_inventory=protocol_ports,
                    require_protocol_match=require_protocol_match,
                ):
                    blocked_count += 1
            else:
                row["status"] = "blocked_neither_configured_nor_candidate_present"
                row["blocked"] = True
                blocked_count += 1
        elif row["runtime_port"]:
            if _apply_protocol_guard(
                row,
                device_key=key,
                runtime_port=row["runtime_port"],
                protocol_inventory=protocol_ports,
                require_protocol_match=require_protocol_match,
            ):
                blocked_count += 1
        rows.append(row)

    binding_metadata = {
        "enabled": True,
        "policy": "reference_devices_only_com24_31_between_com16_23",
        "available_ports": sorted(available),
        "changed_count": changed_count,
        "blocked_count": blocked_count,
        "gas_analyzer_ports_protected": True,
        "protocol_inventory_checked": protocol_ports is not None,
        "require_protocol_match": bool(require_protocol_match),
        "selected_device_keys": (
            sorted(selected_device_keys) if selected_device_keys is not None else "all_reference_devices"
        ),
        "created_at": datetime.now().isoformat(timespec="seconds"),
    }
    status = "blocked" if blocked_count else "pass"
    reason = "ambiguous_or_missing_reference_ports" if blocked_count else "reference_ports_bound"
    result_config = payload if not blocked_count else original_payload
    result_config["v1_5_serial_port_binding"] = binding_metadata
    return RuntimeSerialPortBindingResult(
        status=status,
        reason=reason,
        config=result_config,
        evidence_rows=tuple(rows),
        changed_count=changed_count,
        blocked_count=blocked_count,
        enabled=True,
    )
