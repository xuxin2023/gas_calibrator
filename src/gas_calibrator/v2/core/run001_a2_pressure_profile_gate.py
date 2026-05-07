from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Mapping, Sequence, Union

_A2_SEVEN_PRESSURE_FORMAL_HPA = (1100.0, 1000.0, 900.0, 800.0, 700.0, 600.0, 500.0)
_A2_ENGINEERING_SMOKE_SCOPE = "run001_a2_co2_no_write_pressure_profile"
_A2_SEVEN_PRESSURE_SCOPE = "run001_a2_co2_no_write_pressure_sweep"

_ProfileItem = Union[float, str]


def normalize_pressure_profile(values: Any) -> list[_ProfileItem]:
    raw = values if isinstance(values, list) else [values]
    result: list[_ProfileItem] = []
    for item in raw:
        if isinstance(item, (int, float)):
            result.append(float(item))
        elif isinstance(item, str) and item.strip().lower() == "ambient_open":
            result.append("ambient_open")
        else:
            try:
                result.append(float(item))
            except (ValueError, TypeError):
                pass
    return result


def normalize_pressure_profile_numeric_only(values: Any) -> list[float]:
    raw = values if isinstance(values, list) else [values]
    result: list[float] = []
    for item in raw:
        if isinstance(item, (int, float)):
            result.append(float(item))
        elif isinstance(item, str) and item.strip().lower() == "ambient_open":
            continue
        else:
            try:
                result.append(float(item))
            except (ValueError, TypeError):
                pass
    return result


def resolve_acceptance_mode(raw_cfg: Mapping[str, Any]) -> str:
    policy = _a2_policy(raw_cfg)
    if _truthy(policy.get("engineering_smoke_only")):
        return "engineering_smoke"
    return "seven_pressure_formal"


def scope_for_mode(mode: str) -> str:
    if mode == "engineering_smoke":
        return _A2_ENGINEERING_SMOKE_SCOPE
    return _A2_SEVEN_PRESSURE_SCOPE


def planned_pressure_profile(raw_cfg: Mapping[str, Any]) -> list[_ProfileItem]:
    policy = _a2_policy(raw_cfg)
    profile = policy.get("authorized_pressure_points_hpa")
    if profile is None:
        probe = raw_cfg.get("a2_co2_7_pressure_no_write_probe", {})
        if isinstance(probe, Mapping):
            profile = probe.get("pressure_points_hpa") or probe.get("authorized_pressure_points_hpa")
    return normalize_pressure_profile(profile)


def validate_configured_profile(raw_cfg: Mapping[str, Any]) -> tuple[bool, list[str]]:
    mode = resolve_acceptance_mode(raw_cfg)
    planned = planned_pressure_profile(raw_cfg)
    if not planned:
        return False, ["planned_pressure_profile_empty"]
    if mode == "engineering_smoke":
        return _validate_engineering_smoke_planned(planned)
    else:
        return _validate_seven_pressure_planned(planned)


def validate_completed_profile(
    planned: Sequence[_ProfileItem],
    completed: Sequence[_ProfileItem],
    mode: str,
) -> tuple[bool, list[str]]:
    if not planned:
        return False, ["planned_pressure_profile_empty"]
    if mode == "engineering_smoke":
        planned_set = {_profile_key(p) for p in planned}
        completed_set = {_profile_key(c) for c in completed}
        if planned_set != completed_set:
            missing = planned_set - completed_set
            extra = completed_set - planned_set
            reasons = []
            if missing:
                reasons.append("planned_pressure_points_not_completed")
            if extra:
                reasons.append("unexpected_pressure_points_completed")
            return False, reasons
        return True, []
    else:
        planned_numeric = [float(p) for p in planned if isinstance(p, (int, float))]
        completed_numeric = [float(c) for c in completed if isinstance(c, (int, float))]
        if planned_numeric != list(_A2_SEVEN_PRESSURE_FORMAL_HPA):
            return False, ["planned_pressure_points_not_7_point_formal"]
        if len(completed_numeric) != len(_A2_SEVEN_PRESSURE_FORMAL_HPA):
            return False, ["points_completed_not_7"]
        if not all(abs(float(a) - float(b)) <= 1e-6 for a, b in zip(completed_numeric, _A2_SEVEN_PRESSURE_FORMAL_HPA)):
            return False, ["points_completed_not_7"]
        return True, []


def validate_operator_confirmation_profile(
    config_profile: Sequence[_ProfileItem],
    operator_profile: Any,
    mode: str,
) -> tuple[bool, list[str]]:
    op_normalized = normalize_pressure_profile(operator_profile)
    config_set = {_profile_key(p) for p in config_profile}
    op_set = {_profile_key(p) for p in op_normalized}
    reasons: list[str] = []
    if config_set != op_set:
        reasons.append("operator_confirmation_pressure_points_mismatch")
    return not reasons, reasons


def operator_required_true_acks(mode: str) -> tuple[str, ...]:
    base = (
        "co2_only",
        "skip0",
        "single_route",
        "single_temperature",
        "skip_temperature_stabilization_wait",
        "no_write",
        "no_id_write",
        "no_senco_write",
        "no_calibration_write",
        "no_chamber_sv_write",
        "no_chamber_set_temperature",
        "no_chamber_start",
        "no_chamber_stop",
        "no_mode_switch",
        "not_real_acceptance",
        "engineering_probe_only",
        "v1_fallback_required",
        "authorized_pressure_control_scope_acknowledged",
        "do_not_refresh_real_primary_latest",
    )
    if mode == "engineering_smoke":
        return base + ("pressure_profile_acknowledged", "only_a2_co2_pressure_profile_no_write")
    else:
        return base + ("seven_pressure_points", "only_a2_co2_7_pressure_no_write")


def planned_pressure_point_count(planned: Sequence[_ProfileItem]) -> int:
    return len(planned)


def _validate_engineering_smoke_planned(planned: list[_ProfileItem]) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    numeric_points = [p for p in planned if isinstance(p, (int, float))]
    if not numeric_points:
        reasons.append("engineering_smoke_no_numeric_pressure_points")
    return not reasons, reasons


def _validate_seven_pressure_planned(planned: list[_ProfileItem]) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    numeric_points = [p for p in planned if isinstance(p, (int, float))]
    if numeric_points != list(_A2_SEVEN_PRESSURE_FORMAL_HPA):
        reasons.append("a2_authorized_pressure_points_mismatch")
    return not reasons, reasons


def _profile_key(value: _ProfileItem) -> str:
    if isinstance(value, str):
        return "ambient_open"
    return f"{float(value):.1f}"


def completed_profile_from_trace(run_dir: str | Path | None) -> list[_ProfileItem]:
    if run_dir is None:
        return []
    trace_path = Path(run_dir) / "route_trace.jsonl"
    if not trace_path.exists():
        return []
    out: list[_ProfileItem] = []
    seen: set[str] = set()
    numeric_pattern = re.compile(r"_(\d+(?:\.\d+)?)hpa\b", re.IGNORECASE)
    for line in trace_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except Exception:
            continue
        if str(item.get("action") or "") != "sample_end":
            continue
        if str(item.get("result") or "").lower() != "ok":
            continue
        tag = str(item.get("point_tag") or "")
        target = item.get("target", {})
        target_pressure = None
        if isinstance(target, Mapping):
            target_pressure = target.get("pressure_hpa")
        if "ambient" in tag.lower() and tag.lower().endswith("ambient"):
            key = "ambient_open"
            if key not in seen:
                seen.add(key)
                out.append("ambient_open")
            continue
        match = numeric_pattern.search(tag)
        if match:
            pressure = float(match.group(1))
            pkey = f"{pressure:.1f}"
            if pkey not in seen:
                seen.add(pkey)
                out.append(pressure)
            continue
        if target_pressure is not None:
            pressure = float(target_pressure)
            pkey = f"{pressure:.1f}"
            if pkey not in seen:
                seen.add(pkey)
                out.append(pressure)
    return out


def _a2_policy(raw_cfg: Mapping[str, Any]) -> dict[str, Any]:
    candidate = raw_cfg.get("run001_a2")
    return dict(candidate) if isinstance(candidate, Mapping) else {}


def _truthy(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}
