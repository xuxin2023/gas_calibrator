from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, Optional


class NoWriteViolation(RuntimeError):
    """Raised when a no-write dry-run attempts a calibration write."""


class NoWriteConfigurationError(RuntimeError):
    """Raised when a real-machine dry-run is not fail-closed for writes."""


WRITE_VERBS = ("set", "write", "apply", "commit", "save", "store", "update")
CALIBRATION_TERMS = (
    "coeff",
    "senco",
    "zero",
    "span",
    "calibration",
    "calibrate",
    "eeprom",
    "flash",
    "nvm",
    "parameter_store",
    "parameterstore",
    "writeback",
)
RAW_WRITE_METHODS = ("write", "query", "_send_config", "_send_config_with_retries")
ANALYZER_RAW_DEVICE_TYPES = ("gas_analyzer", "analyzer", "gas_analyzer_serial")
RAW_CALIBRATION_COMMAND_TOKENS = (
    "SENCO",
    "COEFF",
    "WRITECOEFF",
    "WRITE_COEFF",
    "SETCOEFF",
    "SET_COEFF",
    "COEFFWRITE",
    "COEFF_WRITE",
    "WRITEZERO",
    "WRITE_ZERO",
    "SETZERO",
    "SET_ZERO",
    "WRITESPAN",
    "WRITE_SPAN",
    "SETSPAN",
    "SET_SPAN",
    "APPLYCAL",
    "APPLY_CAL",
    "CALIBRATION",
    "APPLY_CALIBRATION",
    "COMMITCAL",
    "COMMIT_CAL",
    "COMMIT_CALIBRATION",
    "SAVECAL",
    "SAVE_CAL",
    "SAVE_PARAMETERS",
    "SAVEPARAMETERS",
    "STORE_PARAMETERS",
    "STOREPARAMETERS",
    "WRITEBACK",
    "EEPROM",
    "FLASH",
    "NVM",
    "PARAMETER_STORE",
    "PARAMETERSTORE",
    "PARAM",
    "SETID",
    "SET_ID",
    "WRITEID",
    "WRITE_ID",
    "DEVICEID",
    "DEVICE_ID",
)
RAW_IDENTITY_COMMAND_PREFIXES = (
    "ID,YGAS,",
)
EXACT_BLOCKED_METHODS = {
    "set_senco",
    "set_coefficients",
    "write_coefficients",
    "set_coefficient",
    "write_coefficient",
    "write_zero",
    "set_zero",
    "zero_calibration",
    "write_span",
    "set_span",
    "span_calibration",
    "apply_calibration",
    "commit_calibration",
    "save_calibration",
    "save_parameters",
    "write_parameters",
    "write_calibration_parameters",
    "set_calibration_parameters",
    "writeback",
    "write_to_eeprom",
    "write_eeprom",
    "write_flash",
    "write_nvm",
    "commit_to_nvm",
    "store_parameters",
    "parameter_store_write",
    "set_device_id_with_ack",
    "set_device_id",
    "write_device_id",
    "assign_device_id",
    "set_id",
}
IDENTITY_WRITE_METHODS = {
    "set_device_id_with_ack",
    "set_device_id",
    "write_device_id",
    "assign_device_id",
    "set_id",
}


def _normalize_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _run001_policy(raw_cfg: Optional[Mapping[str, Any]]) -> dict[str, Any]:
    if not isinstance(raw_cfg, Mapping):
        return {}
    candidate = raw_cfg.get("run001_a2")
    if isinstance(candidate, Mapping):
        return dict(candidate)
    candidate = raw_cfg.get("run001_a1")
    if isinstance(candidate, Mapping):
        return dict(candidate)
    candidate = raw_cfg.get("run001_h2o_1_point")
    if isinstance(candidate, Mapping):
        return dict(candidate)
    candidate = raw_cfg.get("run001")
    if isinstance(candidate, Mapping):
        return dict(candidate)
    return {}


def is_run001_real_machine_dry_run(raw_cfg: Optional[Mapping[str, Any]]) -> bool:
    policy = _run001_policy(raw_cfg)
    mode = str(policy.get("mode", raw_cfg.get("mode") if isinstance(raw_cfg, Mapping) else "") or "").strip().lower()
    return mode == "real_machine_dry_run"


def is_blocked_write_method(method_name: str, *, device_type: str = "") -> bool:
    name = str(method_name or "").strip().lower()
    if not name:
        return False
    if name in EXACT_BLOCKED_METHODS:
        return True
    has_verb = (
        name.startswith(WRITE_VERBS)
        or any(f"_{verb}_" in name or name.endswith(f"_{verb}") for verb in WRITE_VERBS)
    )
    has_calibration_term = any(term in name for term in CALIBRATION_TERMS)
    return bool(has_verb and has_calibration_term)


def _raw_write_method_needs_payload_check(method_name: str, *, device_type: str = "") -> bool:
    name = str(method_name or "").strip()
    normalized_type = str(device_type or "").strip().lower()
    return name in RAW_WRITE_METHODS and normalized_type in ANALYZER_RAW_DEVICE_TYPES


def _raw_payload_preview(args: tuple[Any, ...], kwargs: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for value in args[:4]:
        parts.append(str(value or ""))
    for key in ("data", "cmd", "command", "payload"):
        if key in kwargs:
            parts.append(str(kwargs.get(key) or ""))
    return "\n".join(parts)


def _normalized_raw_payload_text(args: tuple[Any, ...], kwargs: Mapping[str, Any]) -> str:
    text = _raw_payload_preview(args, kwargs).upper().replace("-", "_").replace(" ", "")
    return text


def is_blocked_raw_identity_write_payload(args: tuple[Any, ...], kwargs: Mapping[str, Any]) -> bool:
    text = _normalized_raw_payload_text(args, kwargs)
    if not text:
        return False
    return any(text.startswith(prefix) or f"\n{prefix}" in text for prefix in RAW_IDENTITY_COMMAND_PREFIXES)


def is_blocked_raw_write_payload(args: tuple[Any, ...], kwargs: Mapping[str, Any]) -> bool:
    text = _normalized_raw_payload_text(args, kwargs)
    if not text:
        return False
    if is_blocked_raw_identity_write_payload(args, kwargs):
        return True
    return any(token in text for token in RAW_CALIBRATION_COMMAND_TOKENS)


@dataclass
class NoWriteGuard:
    scope: str = "run001_a1"
    enabled: bool = True
    blocked_events: list[dict[str, Any]] = field(default_factory=list)

    @property
    def attempted_write_count(self) -> int:
        return len(self.blocked_events)

    def guard_device(self, device: Any, *, device_name: str = "", device_type: str = "") -> Any:
        if not self.enabled or device is None:
            return device
        if isinstance(device, NoWriteDeviceProxy):
            return device
        return NoWriteDeviceProxy(
            device,
            guard=self,
            device_name=str(device_name or ""),
            device_type=str(device_type or ""),
        )

    def record_blocked_write(
        self,
        *,
        device_name: str,
        device_type: str,
        method_name: str,
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
        write_category: str = "",
    ) -> NoWriteViolation:
        category = str(write_category or "").strip().lower()
        if not category:
            method_text = str(method_name or "").strip().lower()
            category = (
                "persistent_identity_write"
                if method_text in IDENTITY_WRITE_METHODS or is_blocked_raw_identity_write_payload(args, kwargs)
                else "calibration_or_parameter_write"
            )
        event = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "scope": self.scope,
            "device_name": str(device_name or ""),
            "device_type": str(device_type or ""),
            "method_name": str(method_name or ""),
            "args_preview": [self._preview(value) for value in args[:4]],
            "kwargs_keys": sorted(str(key) for key in dict(kwargs).keys()),
            "write_category": category,
            "identity_write_command_sent": category == "persistent_identity_write",
            "persistent_write_command_sent": category == "persistent_identity_write",
            "reason": "blocked_by_run001_a1_no_write_guard",
        }
        self.blocked_events.append(event)
        return NoWriteViolation(
            "Run-001/A1 no-write guard blocked write "
            f"{event['device_name'] or event['device_type']}.{event['method_name']}"
        )

    def to_artifact(self) -> dict[str, Any]:
        identity_write = any(bool(event.get("identity_write_command_sent")) for event in self.blocked_events)
        persistent_write = any(bool(event.get("persistent_write_command_sent")) for event in self.blocked_events)
        return {
            "guard_enabled": bool(self.enabled),
            "scope": self.scope,
            "attempted_write_count": self.attempted_write_count,
            "blocked_write_events": list(self.blocked_events),
            "identity_write_command_sent": identity_write,
            "persistent_write_command_sent": persistent_write,
            "blocked_method_policy": {
                "exact": sorted(EXACT_BLOCKED_METHODS),
                "identity_methods": sorted(IDENTITY_WRITE_METHODS),
                "terms": list(CALIBRATION_TERMS),
                "verbs": list(WRITE_VERBS),
                "raw_write_methods": list(RAW_WRITE_METHODS),
                "raw_calibration_command_tokens": list(RAW_CALIBRATION_COMMAND_TOKENS),
                "raw_identity_command_prefixes": list(RAW_IDENTITY_COMMAND_PREFIXES),
                "gas_analyzer_raw_write_blocked": "calibration_and_identity_payloads",
            },
            "final_decision": "FAIL" if self.attempted_write_count > 0 else "PASS",
        }

    @staticmethod
    def _preview(value: Any) -> str:
        text = repr(value)
        if len(text) > 120:
            text = text[:117] + "..."
        return text


class NoWriteDeviceProxy:
    def __init__(self, device: Any, *, guard: NoWriteGuard, device_name: str, device_type: str) -> None:
        object.__setattr__(self, "_device", device)
        object.__setattr__(self, "_guard", guard)
        object.__setattr__(self, "_device_name", device_name)
        object.__setattr__(self, "_device_type", device_type)

    def __getattr__(self, name: str) -> Any:
        value = getattr(object.__getattribute__(self, "_device"), name)
        guard = object.__getattribute__(self, "_guard")
        device_name = object.__getattribute__(self, "_device_name")
        device_type = object.__getattribute__(self, "_device_type")
        if callable(value) and is_blocked_write_method(name, device_type=device_type):
            def _blocked(*args: Any, **kwargs: Any) -> Any:
                raise guard.record_blocked_write(
                    device_name=device_name,
                    device_type=device_type,
                    method_name=name,
                    args=args,
                    kwargs=kwargs,
                )

            return _blocked
        if callable(value) and _raw_write_method_needs_payload_check(name, device_type=device_type):
            def _checked_raw_write(*args: Any, **kwargs: Any) -> Any:
                if is_blocked_raw_write_payload(args, kwargs):
                    category = (
                        "persistent_identity_write"
                        if is_blocked_raw_identity_write_payload(args, kwargs)
                        else "calibration_or_parameter_write"
                    )
                    raise guard.record_blocked_write(
                        device_name=device_name,
                        device_type=device_type,
                        method_name=name,
                        args=args,
                        kwargs=kwargs,
                        write_category=category,
                    )
                return value(*args, **kwargs)

            return _checked_raw_write
        if name == "ser" and str(device_type).strip().lower() in {"gas_analyzer", "analyzer"}:
            return guard.guard_device(value, device_name=f"{device_name}.ser", device_type="gas_analyzer_serial")
        return value

    def __setattr__(self, name: str, value: Any) -> None:
        setattr(object.__getattribute__(self, "_device"), name, value)


class NoWriteDeviceFactory:
    def __init__(self, wrapped_factory: Any, guard: NoWriteGuard) -> None:
        self._wrapped_factory = wrapped_factory
        self.guard = guard

    def __getattr__(self, name: str) -> Any:
        return getattr(self._wrapped_factory, name)

    def create(self, device_type: Any, config: Any) -> Any:
        device = self._wrapped_factory.create(device_type, config)
        device_type_text = str(getattr(device_type, "value", device_type) or "")
        device_name = str(getattr(config, "name", "") or device_type_text)
        return self.guard.guard_device(device, device_name=device_name, device_type=device_type_text)


def build_no_write_guard_from_raw_config(raw_cfg: Optional[Mapping[str, Any]]) -> Optional[NoWriteGuard]:
    if not is_run001_real_machine_dry_run(raw_cfg):
        return None
    policy = _run001_policy(raw_cfg)
    no_write = policy.get("no_write")
    if no_write is None and isinstance(raw_cfg, Mapping):
        no_write = raw_cfg.get("no_write")
    if not _normalize_bool(no_write):
        raise NoWriteConfigurationError("Run-001/A1 real_machine_dry_run requires no_write=true")
    scope = str(policy.get("guard_scope") or policy.get("scope") or "run001_a1").strip() or "run001_a1"
    return NoWriteGuard(scope=scope, enabled=True)
