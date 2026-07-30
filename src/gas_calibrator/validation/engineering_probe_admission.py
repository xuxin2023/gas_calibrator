"""Pure admission contract for tightly bounded engineering probes.

This module only evaluates configuration and operator evidence.  It never opens
ports, controls equipment, writes coefficients, or executes a probe.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
from typing import Any, Mapping


ENGINEERING_PROBE_ENV_VAR = "GAS_CAL_ENGINEERING_PROBE"
ENGINEERING_PROBE_ENV_VALUE = "1"
ENGINEERING_PROBE_EVIDENCE_MARKERS = {
    "evidence_source": "engineering_probe_admission_only",
    "acceptance_level": "engineering_probe_only",
    "not_real_acceptance_evidence": True,
    "promotion_state": "blocked",
    "real_primary_latest_refresh": False,
}
REQUIRED_OPERATOR_FIELDS = (
    "operator_name",
    "timestamp",
    "branch",
    "HEAD",
    "config_path",
    "port_manifest",
    "explicit_acknowledgement",
)
REQUIRED_TRUE_ACKS = (
    "no_write",
    "no_id_write",
    "no_senco_write",
    "no_calibration_write",
    "not_real_acceptance",
    "engineering_probe_only",
    "v1_fallback_required",
    "do_not_refresh_real_primary_latest",
)
REQUIRED_FALSE_ACKS = ("real_primary_latest_refresh",)
FORBIDDEN_CAPABILITY_FLAGS = (
    "h2o_enabled",
    "full_group_enabled",
    "multi_temperature_enabled",
    "analyzer_id_write_enabled",
    "senco_write_enabled",
    "calibration_write_enabled",
    "allow_write_coefficients",
    "allow_write_zero",
    "allow_write_span",
    "allow_write_calibration_parameters",
    "real_acceptance_enabled",
    "default_entry_switch_enabled",
    "disable_v1",
)


@dataclass(frozen=True)
class EngineeringProbeAdmission:
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
    payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if not isinstance(payload, Mapping):
        raise ValueError(f"JSON payload must be an object: {path}")
    return dict(payload)


def _as_bool(value: Any) -> bool | None:
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


def _path_value(raw_cfg: Mapping[str, Any], dotted_path: str) -> Any:
    current: Any = raw_cfg
    for part in dotted_path.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current.get(part)
    return current


def _first_value(raw_cfg: Mapping[str, Any], *paths: str) -> Any:
    for path in paths:
        value = _path_value(raw_cfg, path)
        if value is not None:
            return value
    return None


def _scope(raw_cfg: Mapping[str, Any]) -> str:
    return str(_first_value(raw_cfg, "scope", "engineering_probe.scope") or "").strip()


def _load_operator_confirmation(path: str | Path | None) -> tuple[dict[str, Any], list[str]]:
    if not path or not Path(path).is_file():
        return {}, ["missing_operator_confirmation_json"]
    try:
        return load_json_mapping(path), []
    except (OSError, ValueError, json.JSONDecodeError):
        return {}, ["invalid_operator_confirmation_json"]


def _operator_reasons(
    confirmation: Mapping[str, Any],
    *,
    expected_branch: str,
    expected_head: str,
    expected_config_path: str,
) -> list[str]:
    reasons: list[str] = []
    for field in REQUIRED_OPERATOR_FIELDS:
        if confirmation.get(field) in ("", None):
            reasons.append(f"operator_confirmation_missing_{field}")
    if not isinstance(confirmation.get("port_manifest"), Mapping):
        reasons.append("operator_confirmation_port_manifest_not_mapping")

    acknowledgements = confirmation.get("explicit_acknowledgement")
    if not isinstance(acknowledgements, Mapping):
        reasons.append("operator_confirmation_missing_explicit_acknowledgement")
        acknowledgements = {}
    for name in REQUIRED_TRUE_ACKS:
        if _as_bool(acknowledgements.get(name)) is not True:
            reasons.append(f"operator_ack_missing_{name}")
    for name in REQUIRED_FALSE_ACKS:
        if _as_bool(acknowledgements.get(name)) is not False:
            reasons.append(f"operator_ack_not_false_{name}")

    if expected_branch and str(confirmation.get("branch") or "") != expected_branch:
        reasons.append("operator_confirmation_branch_mismatch")
    if expected_head and str(confirmation.get("HEAD") or "") != expected_head:
        reasons.append("operator_confirmation_head_mismatch")
    if expected_config_path:
        recorded = str(confirmation.get("config_path") or "")
        if not recorded or Path(recorded).resolve() != Path(expected_config_path).resolve():
            reasons.append("operator_confirmation_config_path_mismatch")
    return reasons


def evaluate_engineering_probe_admission(
    raw_cfg: Mapping[str, Any],
    *,
    expected_scope: str,
    cli_allow: bool = False,
    env: Mapping[str, str] | None = None,
    operator_confirmation_path: str | Path | None = None,
    branch: str = "",
    head: str = "",
    config_path: str = "",
) -> EngineeringProbeAdmission:
    """Evaluate admission without performing any device or filesystem mutation."""

    reasons: list[str] = []
    env_map = os.environ if env is None else env
    if not cli_allow:
        reasons.append("missing_cli_engineering_probe_unlock")
    if str(env_map.get(ENGINEERING_PROBE_ENV_VAR, "")).strip() != ENGINEERING_PROBE_ENV_VALUE:
        reasons.append("missing_env_engineering_probe_unlock")

    confirmation, load_reasons = _load_operator_confirmation(operator_confirmation_path)
    reasons.extend(load_reasons)
    if confirmation:
        reasons.extend(
            _operator_reasons(
                confirmation,
                expected_branch=branch,
                expected_head=head,
                expected_config_path=config_path,
            )
        )

    if not expected_scope or _scope(raw_cfg) != expected_scope:
        reasons.append("config_scope_mismatch")
    if _as_bool(_first_value(raw_cfg, "no_write", "engineering_probe.no_write")) is not True:
        reasons.append("config_no_write_not_true")
    if (
        _as_bool(
            _first_value(
                raw_cfg,
                "real_primary_latest_refresh",
                "engineering_probe.real_primary_latest_refresh",
            )
        )
        is not False
    ):
        reasons.append("config_real_primary_latest_refresh_not_false")
    if (
        _as_bool(
            _first_value(
                raw_cfg,
                "v1_fallback_required",
                "engineering_probe.v1_fallback_required",
            )
        )
        is not True
    ):
        reasons.append("config_v1_fallback_not_required")
    for name in FORBIDDEN_CAPABILITY_FLAGS:
        value = _first_value(raw_cfg, name, f"engineering_probe.{name}")
        if _as_bool(value) is True:
            reasons.append(f"config_forbidden_capability_enabled_{name}")

    unique_reasons = tuple(dict.fromkeys(reasons))
    approved = not unique_reasons
    evidence = {
        **ENGINEERING_PROBE_EVIDENCE_MARKERS,
        "admission_approved": approved,
        "gate_only": True,
        "real_com_opened": False,
        "real_probe_executed": False,
        "operator_confirmation_recorded": bool(confirmation),
        "dual_unlock": {
            "cli": bool(cli_allow),
            "environment": (
                str(env_map.get(ENGINEERING_PROBE_ENV_VAR, "")).strip()
                == ENGINEERING_PROBE_ENV_VALUE
            ),
        },
        "attempted_write_count": 0,
        "identity_write_command_sent": False,
        "senco_write_command_sent": False,
        "calibration_write_command_sent": False,
        "blocked_capabilities": list(FORBIDDEN_CAPABILITY_FLAGS),
        "rejection_reasons": list(unique_reasons),
    }
    return EngineeringProbeAdmission(
        approved=approved,
        reasons=unique_reasons,
        evidence=evidence,
        operator_confirmation=confirmation,
    )
