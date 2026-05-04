from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
from typing import Any, Mapping, Optional


REAL_COM_PROBE_ENV_VAR = "GAS_CAL_V2_CONDITIONING_ONLY_REAL_COM"
REAL_COM_PROBE_ENV_VALUE = "1"
REAL_COM_PROBE_CLI_FLAG = "--allow-v2-conditioning-only-real-com"
REAL_COM_PROBE_EVIDENCE_MARKERS = {
    "evidence_source": "real_probe_conditioning_only",
    "not_real_acceptance_evidence": True,
    "acceptance_level": "engineering_probe_only",
    "promotion_state": "blocked",
    "real_primary_latest_refresh": False,
}
REQUIRED_OPERATOR_ACKS = (
    "conditioning_only",
    "no_write",
    "no_seal",
    "no_vent_off",
    "no_high_pressure",
    "no_sample",
    "not_real_acceptance",
    "v1_fallback_required",
)
REQUIRED_OPERATOR_FIELDS = (
    "operator_name",
    "timestamp",
    "branch",
    "HEAD",
    "config_path",
    "port_manifest",
    "explicit_acknowledgement",
)


@dataclass(frozen=True)
class RealComProbeAdmission:
    approved: bool
    reasons: tuple[str, ...]
    evidence: dict[str, Any]
    operator_confirmation: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "approved": self.approved,
            "reasons": list(self.reasons),
            "evidence": dict(self.evidence),
            "operator_confirmation": dict(self.operator_confirmation),
        }


def load_json_mapping(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError(f"JSON payload must be an object: {path}")
    return dict(payload)


def _as_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if value is None or value == "":
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return None


def _section(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    candidate = value.get(name)
    return dict(candidate) if isinstance(candidate, Mapping) else {}


def _path_value(raw_cfg: Mapping[str, Any], dotted_path: str) -> Any:
    current: Any = raw_cfg
    for part in dotted_path.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current.get(part)
    return current


def _first_value(raw_cfg: Mapping[str, Any], paths: tuple[str, ...]) -> Any:
    for path in paths:
        value = _path_value(raw_cfg, path)
        if value is not None:
            return value
    return None


def _truthy(raw_cfg: Mapping[str, Any], paths: tuple[str, ...]) -> bool:
    return _as_bool(_first_value(raw_cfg, paths)) is True


def _explicit_false(raw_cfg: Mapping[str, Any], paths: tuple[str, ...]) -> bool:
    return _as_bool(_first_value(raw_cfg, paths)) is False


def _scope(raw_cfg: Mapping[str, Any]) -> str:
    return str(
        _first_value(
            raw_cfg,
            (
                "scope",
                "config.scope",
                "real_com_probe.scope",
                "run001_conditioning_only.scope",
            ),
        )
        or ""
    ).strip().lower()


def _skip0_only(raw_cfg: Mapping[str, Any]) -> bool:
    if _truthy(raw_cfg, ("skip0", "real_com_probe.skip0")):
        return True
    value = _first_value(raw_cfg, ("skip_co2_ppm", "workflow.skip_co2_ppm", "run001_conditioning_only.skip_co2_ppm"))
    if isinstance(value, list):
        return [int(item) for item in value if str(item).strip() != ""] == [0]
    return str(value).strip() in {"0", "[0]", "0.0"}


def _single_temperature(raw_cfg: Mapping[str, Any]) -> bool:
    if _truthy(raw_cfg, ("single_temperature", "single_temperature_group", "real_com_probe.single_temperature")):
        return True
    value = _first_value(raw_cfg, ("selected_temps_c", "workflow.selected_temps_c"))
    return isinstance(value, list) and len(value) == 1


def _h2o_disabled(raw_cfg: Mapping[str, Any]) -> bool:
    if _explicit_false(raw_cfg, ("h2o_enabled", "real_com_probe.h2o_enabled")):
        return True
    dewpoint = _as_bool(_path_value(raw_cfg, "devices.dewpoint_meter.enabled"))
    humidity = _as_bool(_path_value(raw_cfg, "devices.humidity_generator.enabled"))
    route_mode = str(_path_value(raw_cfg, "workflow.route_mode") or "").strip().lower()
    return dewpoint is False and humidity is False and route_mode == "co2_only"


def _real_primary_latest_refresh_disabled(raw_cfg: Mapping[str, Any]) -> bool:
    return _explicit_false(
        raw_cfg,
        (
            "real_primary_latest_refresh",
            "real_primary_latest_refresh_enabled",
            "real_primary_latest.refresh",
            "real_com_probe.real_primary_latest_refresh",
            "governance.real_primary_latest_refresh",
        ),
    )


def _validate_operator_confirmation(
    path: Optional[str | Path],
    *,
    expected_branch: str = "",
    expected_head: str = "",
    expected_config_path: str = "",
) -> tuple[dict[str, Any], list[str]]:
    if not path:
        return {}, ["missing_operator_confirmation_json"]
    confirmation_path = Path(path)
    if not confirmation_path.exists():
        return {}, ["missing_operator_confirmation_json"]
    try:
        payload = load_json_mapping(confirmation_path)
    except Exception:
        return {}, ["invalid_operator_confirmation_json"]
    reasons: list[str] = []
    for field in REQUIRED_OPERATOR_FIELDS:
        if field not in payload or payload.get(field) in ("", None):
            reasons.append(f"operator_confirmation_missing_{field}")
    acks = payload.get("explicit_acknowledgement")
    if not isinstance(acks, Mapping):
        reasons.append("operator_confirmation_missing_explicit_acknowledgement")
        acks = {}
    for ack in REQUIRED_OPERATOR_ACKS:
        if _as_bool(acks.get(ack)) is not True:
            reasons.append(f"operator_ack_missing_{ack}")
    if expected_branch and str(payload.get("branch") or "") != expected_branch:
        reasons.append("operator_confirmation_branch_mismatch")
    if expected_head and str(payload.get("HEAD") or "") != expected_head:
        reasons.append("operator_confirmation_head_mismatch")
    if expected_config_path and Path(str(payload.get("config_path") or "")).resolve() != Path(expected_config_path).resolve():
        reasons.append("operator_confirmation_config_path_mismatch")
    return payload, reasons


def evaluate_conditioning_only_real_com_gate(
    raw_cfg: Mapping[str, Any],
    *,
    cli_allow: bool = False,
    env: Optional[Mapping[str, str]] = None,
    operator_confirmation_path: Optional[str | Path] = None,
    branch: str = "",
    head: str = "",
    config_path: str = "",
) -> RealComProbeAdmission:
    env_map = os.environ if env is None else env
    reasons: list[str] = []
    if not cli_allow:
        reasons.append("missing_cli_flag_allow_v2_conditioning_only_real_com")
    if str(env_map.get(REAL_COM_PROBE_ENV_VAR, "")).strip() != REAL_COM_PROBE_ENV_VALUE:
        reasons.append("missing_env_gas_cal_v2_conditioning_only_real_com")

    confirmation, confirmation_reasons = _validate_operator_confirmation(
        operator_confirmation_path,
        expected_branch=branch,
        expected_head=head,
        expected_config_path=config_path,
    )
    reasons.extend(confirmation_reasons)

    if _scope(raw_cfg) != "conditioning_only":
        reasons.append("config_scope_not_conditioning_only")
    if not _truthy(raw_cfg, ("co2_only", "real_com_probe.co2_only", "run001_conditioning_only.co2_only")):
        reasons.append("config_not_co2_only")
    if not _skip0_only(raw_cfg):
        reasons.append("config_not_skip0")
    if not _truthy(raw_cfg, ("single_route", "real_com_probe.single_route", "run001_conditioning_only.single_route")):
        reasons.append("config_not_single_route")
    if not _single_temperature(raw_cfg):
        reasons.append("config_not_single_temperature")
    if not _truthy(raw_cfg, ("no_write", "real_com_probe.no_write", "run001_conditioning_only.no_write")):
        reasons.append("config_no_write_not_true")
    if not _h2o_disabled(raw_cfg):
        reasons.append("config_h2o_not_disabled")
    if not _explicit_false(raw_cfg, ("full_group_enabled", "real_com_probe.full_group_enabled")):
        reasons.append("config_full_group_not_disabled")
    if not _explicit_false(raw_cfg, ("a2_enabled", "real_com_probe.a2_enabled")):
        reasons.append("config_a2_not_disabled")
    if not _explicit_false(raw_cfg, ("a3_enabled", "real_com_probe.a3_enabled")):
        reasons.append("config_a3_not_disabled")
    for name in ("vent_off_enabled", "seal_enabled", "high_pressure_enabled", "sample_enabled"):
        if not _explicit_false(raw_cfg, (name, f"conditioning_only_controls.{name}", f"real_com_probe.{name}")):
            reasons.append(f"config_{name}_not_disabled")
    if not _real_primary_latest_refresh_disabled(raw_cfg):
        reasons.append("config_real_primary_latest_refresh_not_disabled")

    approved = not reasons
    evidence = {
        **REAL_COM_PROBE_EVIDENCE_MARKERS,
        "admission_approved": approved,
        "gate_only": True,
        "real_com_opened": False,
        "real_probe_executed": False,
        "operator_confirmation_recorded": bool(confirmation),
        "attempted_write_count": 0,
        "identity_write_command_sent": False,
        "calibration_write_command_sent": False,
        "senco_write_command_sent": False,
        "blocked_capabilities": {
            "vent_off": True,
            "seal": True,
            "high_pressure": True,
            "sample": True,
            "h2o": True,
            "full_group": True,
            "a2": True,
            "a3": True,
            "real_primary_latest_refresh": True,
        },
        "rejection_reasons": list(reasons),
    }
    return RealComProbeAdmission(
        approved=approved,
        reasons=tuple(dict.fromkeys(reasons)),
        evidence=evidence,
        operator_confirmation=confirmation,
    )
