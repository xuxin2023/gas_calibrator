"""Offline simulation configuration contracts.

These configuration objects support validation-owned stability, sampling,
preflight, pressure-tolerance, and quality checks, optional diagnostic AI
helpers, offline input/output path selection, and offline ratio-poly candidate
reports. They do not authorize device I/O, route control, coefficient writes,
or real acceptance.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ...h2o_summary_selection import (
    DEFAULT_H2O_CO2_ZERO_PPM_ANCHOR_MAX_PPM_H2O_DEW_BY_TEMP_C,
    DEFAULT_H2O_CO2_ZERO_PPM_ANCHOR_MAX_PPM_H2O_DEW_DEFAULT,
    DEFAULT_H2O_CO2_ZERO_PPM_ANCHOR_QUALITY_GATE_ENABLED,
    DEFAULT_H2O_CO2_ZERO_PPM_ANCHOR_REQUIRE_H2O_DEW,
    DEFAULT_H2O_CO2_ZERO_PPM_TARGET,
    DEFAULT_H2O_CO2_ZERO_PPM_TOLERANCE,
    DEFAULT_H2O_INCLUDE_CO2_TEMP_GROUPS_C,
    DEFAULT_H2O_INCLUDE_CO2_ZERO_PPM_ROWS,
    DEFAULT_H2O_INCLUDE_CO2_ZERO_PPM_TEMP_GROUPS_C,
    DEFAULT_H2O_TEMPERATURE_BUCKETS_C,
    DEFAULT_H2O_TEMPERATURE_BUCKET_TOLERANCE_C,
    DEFAULT_H2O_TEMP_TOLERANCE_C,
    normalize_h2o_summary_selection,
)


STEP2_ENGINEERING_ONLY_PRESSURE_FLAG_SPECS: tuple[tuple[str, str, str], ...] = (
    (
        "capture_then_hold_enabled",
        "workflow.pressure.capture_then_hold_enabled",
        "capture_then_hold",
    ),
    (
        "adaptive_pressure_sampling_enabled",
        "workflow.pressure.adaptive_pressure_sampling_enabled",
        "adaptive_pressure_sampling",
    ),
    (
        "soft_control_enabled",
        "workflow.pressure.soft_control_enabled",
        "soft_control",
    ),
)


def port_requires_real_device_review(port: str) -> bool:
    normalized = str(port or "").strip().upper()
    if not normalized:
        return False
    if normalized.startswith("SIM-") or normalized in {"SIM", "SIMULATED", "REPLAY"}:
        return False
    if normalized.startswith("COM"):
        return True
    if normalized.startswith("/DEV/") or normalized.startswith("TTY") or "TTYUSB" in normalized or "TTYACM" in normalized:
        return True
    return False


def iter_config_device_ports(config: Any) -> list[tuple[str, str]]:
    devices = getattr(config, "devices", None)
    if devices is None:
        return []
    rows: list[tuple[str, str]] = []
    for name in (
        "pressure_controller",
        "pressure_gauge",
        "pressure_meter",
        "dewpoint_meter",
        "humidity_generator",
        "temperature_chamber",
        "thermometer",
        "relay",
        "relay_a",
        "relay_b",
        "relay_8",
        "gas_analyzer",
    ):
        payload = getattr(devices, name, None)
        if payload is None or not bool(getattr(payload, "enabled", True)):
            continue
        rows.append((name, str(getattr(payload, "port", "") or "").strip()))
    for index, payload in enumerate(list(getattr(devices, "gas_analyzers", []) or [])):
        if payload is None or not bool(getattr(payload, "enabled", True)):
            continue
        device_name = str(getattr(payload, "name", "") or "").strip() or f"gas_analyzer_{index}"
        rows.append((device_name, str(getattr(payload, "port", "") or "").strip()))
    return rows


def enabled_engineering_only_flags(config: Any) -> list[dict[str, Any]]:
    workflow = getattr(config, "workflow", None)
    pressure = dict(getattr(workflow, "pressure", {}) or {})
    enabled_flags: list[dict[str, Any]] = []
    for flag_name, config_path, label in STEP2_ENGINEERING_ONLY_PRESSURE_FLAG_SPECS:
        if not bool(pressure.get(flag_name, False)):
            continue
        enabled_flags.append(
            {
                "flag": flag_name,
                "config_path": config_path,
                "label": label,
                "category": "engineering_only",
                "default_enabled": False,
            }
        )
    return enabled_flags


def _normalize_run_mode(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"", "auto", "auto_calibration", "automatic_calibration", "calibration"}:
        return "auto_calibration"
    if normalized in {"co2", "co2_measurement", "co2_measure", "co2_test"}:
        return "co2_measurement"
    if normalized in {"h2o", "h2o_measurement", "water", "water_measurement", "humidity_measurement"}:
        return "h2o_measurement"
    if normalized in {"experiment", "experiment_measurement", "lab"}:
        return "experiment_measurement"
    return "auto_calibration"


def _normalize_sensor_precheck_config(d: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    payload = dict(d or {})
    raw_profile = payload.get("profile")
    raw_scope = payload.get("scope")
    raw_mode = payload.get("mode")
    raw_value = raw_scope if raw_scope not in (None, "") else raw_mode
    normalized_scope: Optional[str] = None
    if raw_value in (None, ""):
        pass
    else:
        normalized = str(raw_value).strip().lower()
        if normalized in {"v1_compatible", "first_analyzer_only"}:
            normalized_scope = "first_analyzer_only"
        elif normalized in {"full", "all_analyzers"}:
            normalized_scope = "all_analyzers"
        if normalized_scope is not None:
            payload["scope"] = normalized_scope

    if raw_profile is not None:
        normalized_profile = str(raw_profile).strip().lower()
        if normalized_profile in {"snapshot", "full", "strict"}:
            payload["profile"] = "snapshot"
        elif normalized_profile in {"mode2_like", "v1_mode2_like", "v1_compatible"}:
            payload["profile"] = "mode2_like"
        elif normalized_profile in {"raw_frame_first", "v1_frame_like", "v1_raw_frame_like", "raw_frame", "v1_like", "mode2_like_raw_first"}:
            payload["profile"] = "raw_frame_first"

    raw_validation_mode = payload.get("validation_mode")
    if raw_validation_mode is not None:
        normalized_vm = str(raw_validation_mode).strip().lower()
        if normalized_vm in {"v1_frame_like", "v1_raw_frame_like", "raw_frame", "v1_like"}:
            payload["validation_mode"] = "v1_frame_like"
        elif normalized_vm in {"v1_mode2_like", "mode2_like", "v1_compatible"}:
            payload["validation_mode"] = "v1_mode2_like"
        elif normalized_vm in {"snapshot", "full", "strict"}:
            payload["validation_mode"] = "snapshot"
        else:
            pass

    normalized_profile = payload.get("profile")
    if normalized_profile == "raw_frame_first" and payload.get("validation_mode") in (None, ""):
        payload["validation_mode"] = "v1_frame_like"
    elif normalized_profile == "mode2_like" and payload.get("validation_mode") in (None, ""):
        payload["validation_mode"] = "v1_mode2_like"
    elif normalized_profile == "snapshot" and payload.get("validation_mode") in (None, ""):
        payload["validation_mode"] = "snapshot"

    if str(raw_mode or "").strip().lower() == "v1_compatible" and payload.get("validation_mode") in (None, ""):
        payload["validation_mode"] = "v1_mode2_like"

    if payload.get("validation_mode") == "v1_frame_like" and payload.get("profile") in (None, ""):
        payload["profile"] = "raw_frame_first"
    elif payload.get("validation_mode") == "v1_mode2_like" and payload.get("profile") in (None, ""):
        payload["profile"] = "mode2_like"
    elif payload.get("validation_mode") == "snapshot" and payload.get("profile") in (None, ""):
        payload["profile"] = "snapshot"

    if payload.get("validation_mode") in {"v1_mode2_like", "v1_frame_like"} and raw_scope in (None, "") and normalized_scope is None:
        payload["scope"] = "first_analyzer_only"
    elif payload.get("profile") in {"mode2_like", "raw_frame_first"} and raw_scope in (None, "") and normalized_scope is None:
        payload["scope"] = "first_analyzer_only"

    return payload


def _step2_config_safety_classification(
    *,
    simulation_only: bool,
    real_ports: list[dict[str, Any]],
    engineering_flags: list[dict[str, Any]],
) -> str:
    has_real_ports = bool(real_ports)
    has_engineering_flags = bool(engineering_flags)
    if simulation_only and not has_real_ports and not has_engineering_flags:
        return "operator_safe_simulation_only"
    if simulation_only and has_real_ports and has_engineering_flags:
        return "simulation_mixed_inventory_risk"
    if simulation_only and has_real_ports:
        return "simulation_real_port_inventory_risk"
    if simulation_only and has_engineering_flags:
        return "simulation_engineering_only_risk"
    if has_real_ports and has_engineering_flags:
        return "non_simulation_mixed_risk"
    if has_real_ports:
        return "non_simulation_real_port_risk"
    if has_engineering_flags:
        return "non_simulation_engineering_only_risk"
    return "non_simulation_boundary_risk"


def _step2_config_safety_classification_display(classification: str) -> str:
    mapping = {
        "operator_safe_simulation_only": "默认安全仿真库存",
        "simulation_mixed_inventory_risk": "仿真库存混合风险",
        "simulation_real_port_inventory_risk": "仿真库存含 real-COM 风险",
        "simulation_engineering_only_risk": "仿真库存含 engineering-only 风险",
        "non_simulation_mixed_risk": "非仿真混合风险库存",
        "non_simulation_real_port_risk": "非仿真 real-COM 风险库存",
        "non_simulation_engineering_only_risk": "非仿真 engineering-only 风险库存",
        "non_simulation_boundary_risk": "非仿真边界风险",
    }
    return mapping.get(str(classification or "").strip(), "Step 2 配置风险库存")


def _step2_config_safety_badge_spec(badge_id: str) -> dict[str, str]:
    mapping = {
        "simulation_only": {"label": "仿真边界", "tone": "info"},
        "simulation_disabled": {"label": "关闭仿真", "tone": "warn"},
        "operator_safe": {"label": "默认安全", "tone": "ok"},
        "real_com_risk": {"label": "real-COM 风险", "tone": "warn"},
        "engineering_only": {"label": "工程实验开关", "tone": "warn"},
        "requires_dual_unlock": {"label": "需双重解锁", "tone": "warn"},
        "step2_blocked": {"label": "Step 2 默认拦截", "tone": "error"},
        "step2_override": {"label": "工程隔离解锁", "tone": "info"},
    }
    spec = mapping.get(str(badge_id or "").strip(), {"label": str(badge_id or "--"), "tone": "info"})
    return {
        "id": str(badge_id or "").strip(),
        "label": str(spec.get("label") or badge_id or "--"),
        "tone": str(spec.get("tone") or "info"),
    }


def _build_step2_config_safety_badges(
    *,
    simulation_only: bool,
    real_ports: list[dict[str, Any]],
    engineering_flags: list[dict[str, Any]],
    requires_explicit_unlock: bool,
    step2_default_workflow_allowed: bool,
) -> list[dict[str, str]]:
    badge_ids: list[str] = []
    badge_ids.append("simulation_only" if simulation_only else "simulation_disabled")
    if not real_ports and not engineering_flags and simulation_only:
        badge_ids.append("operator_safe")
    if real_ports:
        badge_ids.append("real_com_risk")
    if engineering_flags:
        badge_ids.append("engineering_only")
    if requires_explicit_unlock:
        badge_ids.append("requires_dual_unlock")
        badge_ids.append("step2_override" if step2_default_workflow_allowed else "step2_blocked")
    unique_ids = list(dict.fromkeys(badge_ids))
    return [_step2_config_safety_badge_spec(item) for item in unique_ids]


def _build_shared_pressure_flag_inventory(engineering_flags: list[dict[str, Any]]) -> list[dict[str, Any]]:
    enabled_by_path = {
        str(item.get("config_path") or "").strip(): dict(item)
        for item in list(engineering_flags or [])
        if isinstance(item, dict) and str(item.get("config_path") or "").strip()
    }
    shared_flags: list[dict[str, Any]] = []
    for flag_name, config_path, label in STEP2_ENGINEERING_ONLY_PRESSURE_FLAG_SPECS:
        enabled_payload = enabled_by_path.get(config_path, {})
        enabled = bool(enabled_payload)
        shared_flags.append(
            {
                "flag": flag_name,
                "config_path": config_path,
                "label": str(enabled_payload.get("label") or label),
                "category": "engineering_only",
                "default_enabled": False,
                "enabled": enabled,
                "status": "engineering_only_enabled" if enabled else "default_safe",
            }
        )
    return shared_flags


def _build_step2_config_safety_inventory(
    device_ports: list[dict[str, Any]],
    engineering_flags: list[dict[str, Any]],
) -> dict[str, Any]:
    enabled_device_count = len(device_ports)
    real_port_device_count = sum(1 for item in device_ports if bool(item.get("requires_real_device_review", False)))
    simulated_device_count = max(0, enabled_device_count - real_port_device_count)
    shared_pressure_flags = _build_shared_pressure_flag_inventory(engineering_flags)
    summary = (
        f"库存治理：已启用设备 {enabled_device_count} 台；"
        f"SIM {simulated_device_count} 台；"
        f"real-COM {real_port_device_count} 台；"
        f"engineering-only 开关 {len(engineering_flags)} 个。"
    )
    return {
        "enabled_device_count": enabled_device_count,
        "simulated_device_count": simulated_device_count,
        "real_port_device_count": real_port_device_count,
        "engineering_only_flag_count": len(engineering_flags),
        "device_ports": device_ports,
        "engineering_only_flags": engineering_flags,
        "shared_pressure_flag_count": len(shared_pressure_flags),
        "shared_pressure_flags_enabled_count": sum(1 for item in shared_pressure_flags if bool(item.get("enabled", False))),
        "shared_pressure_flags": shared_pressure_flags,
        "summary": summary,
    }


def _build_step2_blocked_reason_details(
    *,
    risk_markers: list[str],
    simulation_only: bool,
    real_ports: list[dict[str, Any]],
    engineering_flags: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    for marker in list(risk_markers or []):
        if marker == "simulation_mode_disabled":
            details.append(
                {
                    "code": marker,
                    "title": "simulation_mode 已关闭",
                    "severity": "warn",
                    "category": "simulation_boundary",
                    "summary": "当前配置未启用 simulation_mode；Step 2 仅允许 simulation/offline/headless 验证。",
                }
            )
        elif marker == "real_ports_detected":
            details.append(
                {
                    "code": marker,
                    "title": "检测到 real-COM / 非仿真端口",
                    "severity": "warn",
                    "category": "real_com_inventory",
                    "summary": (
                        "检测到非仿真设备端口："
                        + ", ".join(f"{item['device']}={item['port']}" for item in real_ports)
                        + "。Step 2 默认工作流不接受 real-COM 配置。"
                    ),
                }
            )
        elif marker == "engineering_only_flags_enabled":
            details.append(
                {
                    "code": marker,
                    "title": "启用 engineering-only 实验开关",
                    "severity": "warn",
                    "category": "engineering_only",
                    "summary": (
                        "检测到 engineering-only 实验开关已启用："
                        + ", ".join(str(item.get("config_path") or "--") for item in engineering_flags)
                        + "。Step 2 默认工作流要求这些能力保持 non-default 且默认关闭。"
                    ),
                }
            )
    if not details and not simulation_only:
        details.append(
            {
                "code": "simulation_mode_disabled",
                "title": "simulation_mode 已关闭",
                "severity": "warn",
                "category": "simulation_boundary",
                "summary": "当前配置未启用 simulation_mode；Step 2 仅允许 simulation/offline/headless 验证。",
            }
        )
    return details


def hydrate_step2_config_safety_summary(summary: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(summary or {})
    simulation_only = bool(payload.get("simulation_only", False))
    real_ports = [
        {
            "device": str(item.get("device") or "--"),
            "port": str(item.get("port") or "").strip(),
        }
        for item in list(payload.get("devices_with_real_ports") or [])
        if isinstance(item, dict)
    ]
    engineering_flags = [
        {
            "flag": str(item.get("flag") or "").strip(),
            "config_path": str(item.get("config_path") or "").strip(),
            "label": str(item.get("label") or "").strip(),
            "category": str(item.get("category") or "engineering_only"),
            "default_enabled": bool(item.get("default_enabled", False)),
        }
        for item in list(payload.get("enabled_engineering_flags") or [])
        if isinstance(item, dict)
    ]
    risk_markers = [str(item).strip() for item in list(payload.get("risk_markers") or []) if str(item).strip()]
    warnings = [str(item).strip() for item in list(payload.get("warnings") or []) if str(item).strip()]
    execution_gate = dict(payload.get("execution_gate") or {})
    requires_explicit_unlock = bool(payload.get("requires_explicit_unlock", bool(risk_markers)))
    dual_unlock_ready = bool(
        execution_gate.get("allow_unsafe_step2_config_flag") and execution_gate.get("allow_unsafe_step2_config_env")
    )
    step2_default_workflow_allowed = bool(
        payload.get("step2_default_workflow_allowed", (not requires_explicit_unlock) or dual_unlock_ready)
    )
    device_ports = [
        {
            "device": str(item.get("device") or "--"),
            "port": str(item.get("port") or "").strip(),
            "requires_real_device_review": bool(item.get("requires_real_device_review", False)),
            "classification": str(item.get("classification") or "simulated_port"),
        }
        for item in list(dict(payload.get("inventory") or {}).get("device_ports") or [])
        if isinstance(item, dict)
    ]
    if not device_ports:
        simulated_port_count = max(0, int(dict(payload.get("inventory") or {}).get("simulated_device_count", 0) or 0))
        device_ports = [
            {
                "device": item["device"],
                "port": item["port"],
                "requires_real_device_review": True,
                "classification": "real_port",
            }
            for item in real_ports
        ]
        for index in range(simulated_port_count):
            device_ports.append(
                {
                    "device": f"simulated_device_{index + 1}",
                    "port": "SIM-*",
                    "requires_real_device_review": False,
                    "classification": "simulated_port",
                }
            )
    if not warnings:
        if not simulation_only:
            warnings.append("??????? simulation_mode?Step 2 ??? simulation/offline/headless ???")
        if real_ports:
            warnings.append(
                "???????????"
                + ", ".join(f"{item['device']}={item['port']}" for item in real_ports)
                + "?Step 2 ???????? real-COM ???"
            )
        if engineering_flags:
            warnings.append(
                "??? engineering-only ????????"
                + ", ".join(str(item["config_path"]) for item in engineering_flags)
                + "?Step 2 ????????????? non-default ??????"
            )
    if not risk_markers:
        if not simulation_only:
            risk_markers.append("simulation_mode_disabled")
        if real_ports:
            risk_markers.append("real_ports_detected")
        if engineering_flags:
            risk_markers.append("engineering_only_flags_enabled")
    classification = str(
        payload.get("classification")
        or _step2_config_safety_classification(
            simulation_only=simulation_only,
            real_ports=real_ports,
            engineering_flags=engineering_flags,
        )
    )
    classification_display = str(
        payload.get("classification_display") or _step2_config_safety_classification_display(classification)
    )
    normalized_inventory = _build_step2_config_safety_inventory(device_ports, engineering_flags)
    inventory = dict(payload.get("inventory") or {})
    if not inventory:
        inventory = dict(normalized_inventory)
    inventory.setdefault("summary", str(normalized_inventory.get("summary") or "--"))
    if not list(inventory.get("shared_pressure_flags") or []):
        inventory["shared_pressure_flags"] = list(normalized_inventory.get("shared_pressure_flags") or [])
    inventory.setdefault("shared_pressure_flag_count", int(normalized_inventory.get("shared_pressure_flag_count", 0) or 0))
    inventory.setdefault(
        "shared_pressure_flags_enabled_count",
        int(normalized_inventory.get("shared_pressure_flags_enabled_count", 0) or 0),
    )
    blocked_reason_details = [dict(item) for item in list(payload.get("blocked_reason_details") or []) if isinstance(item, dict)]
    if not blocked_reason_details:
        blocked_reason_details = _build_step2_blocked_reason_details(
            risk_markers=risk_markers,
            simulation_only=simulation_only,
            real_ports=real_ports,
            engineering_flags=engineering_flags,
        )
    badges = list(payload.get("badges") or [])
    if badges:
        badge_ids = [str(item.get("id") or "").strip() for item in badges if isinstance(item, dict)]
        badge_ids = [item for item in badge_ids if item]
        badges = [_step2_config_safety_badge_spec(item) for item in badge_ids]
    else:
        badges = _build_step2_config_safety_badges(
            simulation_only=simulation_only,
            real_ports=real_ports,
            engineering_flags=engineering_flags,
            requires_explicit_unlock=requires_explicit_unlock,
            step2_default_workflow_allowed=step2_default_workflow_allowed,
        )
        badge_ids = [str(item.get("id") or "").strip() for item in badges if str(item.get("id") or "").strip()]
    execution_gate_status = str(
        execution_gate.get("status")
        or (
            "blocked"
            if requires_explicit_unlock and not dual_unlock_ready
            else "unlocked_override"
            if requires_explicit_unlock
            else "open"
        )
    )
    execution_gate_summary = str(
        execution_gate.get("summary")
        or (
            "Step 2 ???????????????????????? non-default ????????? "
            f"{str(execution_gate.get('unlock_cli_flag') or '--allow-unsafe-step2-config')} ? "
            f"{str(execution_gate.get('unlock_env_var') or 'GAS_CALIBRATOR_V2_ALLOW_UNSAFE_CONFIG')}=1?"
            if requires_explicit_unlock and not dual_unlock_ready
            else (
                "?????? non-default Step 2 ???????? CLI flag + ???????????"
                "????????????????????????? real acceptance?"
                if requires_explicit_unlock
                else "?????? Step 2 simulation-only ??????????????"
            )
        )
    )
    execution_gate = {
        "status": execution_gate_status,
        "summary": execution_gate_summary,
        "requires_dual_unlock": requires_explicit_unlock,
        "allow_unsafe_step2_config_flag": bool(execution_gate.get("allow_unsafe_step2_config_flag", False)),
        "allow_unsafe_step2_config_env": bool(execution_gate.get("allow_unsafe_step2_config_env", False)),
        "unlock_cli_flag": str(execution_gate.get("unlock_cli_flag") or "--allow-unsafe-step2-config"),
        "unlock_env_var": str(execution_gate.get("unlock_env_var") or "GAS_CALIBRATOR_V2_ALLOW_UNSAFE_CONFIG"),
        "blocked_reasons": [
            str(item).strip()
            for item in list(execution_gate.get("blocked_reasons") or risk_markers)
            if str(item).strip()
        ],
        "blocked_reason_details": blocked_reason_details,
    }
    status = str(payload.get("status") or ("warn" if warnings else "ok"))
    summary_text = str(
        payload.get("summary")
        or (
            f"?????? {len(warnings)} ??{warnings[0]}"
            if warnings
            else "????? simulation-only???????????"
        )
    )
    review_lines = [str(item).strip() for item in list(payload.get("review_lines") or []) if str(item).strip()]
    if not review_lines:
        review_lines = [
            summary_text,
            execution_gate_summary,
            f"?????{classification_display}",
            f"?????{str(inventory.get('summary') or '--')}",
            "simulation_mode=" + ("enabled" if simulation_only else "disabled") + "???????? simulation/offline/headless?",
        ]
        shared_flag_lines = [
            f"{str(item.get('label') or item.get('flag') or '--')}="
            f"{'on' if bool(item.get('enabled', False)) else 'off(default)'}"
            for item in list(inventory.get("shared_pressure_flags") or [])
            if isinstance(item, dict)
        ]
        if shared_flag_lines:
            review_lines.append("shared runner ?????" + " / ".join(shared_flag_lines))
        if real_ports:
            review_lines.append("real-COM ?????" + ", ".join(f"{item['device']}={item['port']}" for item in real_ports))
        if engineering_flags:
            review_lines.append("engineering-only ???" + ", ".join(str(item["config_path"]) for item in engineering_flags))
        if badges:
            review_lines.append("?????" + " / ".join(str(item.get("label") or "--") for item in badges))
        if not real_ports and not engineering_flags and simulation_only:
            review_lines.append("?????? Step 2 simulation-only operator-safe ???")
    return {
        **payload,
        "status": status,
        "summary": summary_text,
        "review_lines": review_lines,
        "simulation_only": simulation_only,
        "operator_safe": bool(payload.get("operator_safe", not warnings)),
        "risk_markers": risk_markers,
        "real_port_device_count": len(real_ports),
        "devices_with_real_ports": real_ports,
        "engineering_only_flag_count": len(engineering_flags),
        "enabled_engineering_flags": engineering_flags,
        "warnings": warnings,
        "requires_explicit_unlock": requires_explicit_unlock,
        "step2_default_workflow_allowed": step2_default_workflow_allowed,
        "classification": classification,
        "classification_display": classification_display,
        "badge_ids": badge_ids,
        "badges": badges,
        "inventory": inventory,
        "inventory_summary": str(inventory.get("summary") or "--"),
        "blocked_reason_details": blocked_reason_details,
        "execution_gate": execution_gate,
    }


def build_step2_config_safety_review(summary: dict[str, Any] | None) -> dict[str, Any]:
    hydrated = hydrate_step2_config_safety_summary(summary)
    execution_gate = dict(hydrated.get("execution_gate") or {})
    inventory = dict(hydrated.get("inventory") or {})
    return {
        "status": str(execution_gate.get("status") or hydrated.get("status") or "ok"),
        "summary": str(
            execution_gate.get("summary")
            or hydrated.get("summary")
            or "当前配置为 simulation-only，未发现真实串口风险。"
        ),
        "review_lines": list(hydrated.get("review_lines") or []),
        "classification": str(hydrated.get("classification") or ""),
        "classification_display": str(hydrated.get("classification_display") or ""),
        "badge_ids": list(hydrated.get("badge_ids") or []),
        "badges": [dict(item) for item in list(hydrated.get("badges") or [])],
        "inventory_summary": str(inventory.get("summary") or hydrated.get("inventory_summary") or "--"),
        "inventory": inventory,
        "blocked_reasons": list(execution_gate.get("blocked_reasons") or hydrated.get("risk_markers") or []),
        "blocked_reason_details": [
            dict(item) for item in list(hydrated.get("blocked_reason_details") or execution_gate.get("blocked_reason_details") or [])
        ],
        "operator_safe": bool(hydrated.get("operator_safe", False)),
        "simulation_only": bool(hydrated.get("simulation_only", False)),
        "risk_markers": [str(item).strip() for item in list(hydrated.get("risk_markers") or []) if str(item).strip()],
        "warnings": [str(item).strip() for item in list(hydrated.get("warnings") or []) if str(item).strip()],
        "real_port_device_count": int(hydrated.get("real_port_device_count", 0) or 0),
        "devices_with_real_ports": [
            dict(item) for item in list(hydrated.get("devices_with_real_ports") or []) if isinstance(item, dict)
        ],
        "engineering_only_flag_count": int(hydrated.get("engineering_only_flag_count", 0) or 0),
        "enabled_engineering_flags": [
            dict(item) for item in list(hydrated.get("enabled_engineering_flags") or []) if isinstance(item, dict)
        ],
        "execution_gate": execution_gate,
        "requires_explicit_unlock": bool(hydrated.get("requires_explicit_unlock", False)),
        "step2_default_workflow_allowed": bool(hydrated.get("step2_default_workflow_allowed", True)),
    }


def build_step2_config_governance_handoff(summary: dict[str, Any] | None) -> dict[str, Any]:
    review = build_step2_config_safety_review(summary)
    return {
        "status": str(review.get("status") or "ok"),
        "summary": str(review.get("summary") or "--"),
        "review_lines": [str(item).strip() for item in list(review.get("review_lines") or []) if str(item).strip()],
        "classification": str(review.get("classification") or ""),
        "classification_display": str(review.get("classification_display") or ""),
        "badge_ids": [str(item).strip() for item in list(review.get("badge_ids") or []) if str(item).strip()],
        "badges": [dict(item) for item in list(review.get("badges") or []) if isinstance(item, dict)],
        "inventory_summary": str(review.get("inventory_summary") or "--"),
        "inventory": dict(review.get("inventory") or {}),
        "blocked_reasons": [str(item).strip() for item in list(review.get("blocked_reasons") or []) if str(item).strip()],
        "blocked_reason_details": [
            dict(item) for item in list(review.get("blocked_reason_details") or []) if isinstance(item, dict)
        ],
        "warnings": [str(item).strip() for item in list(review.get("warnings") or []) if str(item).strip()],
        "operator_safe": bool(review.get("operator_safe", False)),
        "simulation_only": bool(review.get("simulation_only", False)),
        "risk_markers": [str(item).strip() for item in list(review.get("risk_markers") or []) if str(item).strip()],
        "real_port_device_count": int(review.get("real_port_device_count", 0) or 0),
        "devices_with_real_ports": [
            dict(item) for item in list(review.get("devices_with_real_ports") or []) if isinstance(item, dict)
        ],
        "engineering_only_flag_count": int(review.get("engineering_only_flag_count", 0) or 0),
        "enabled_engineering_flags": [
            dict(item) for item in list(review.get("enabled_engineering_flags") or []) if isinstance(item, dict)
        ],
        "execution_gate": dict(review.get("execution_gate") or {}),
        "requires_explicit_unlock": bool(review.get("requires_explicit_unlock", False)),
        "step2_default_workflow_allowed": bool(review.get("step2_default_workflow_allowed", True)),
    }


@dataclass
class TemperatureStabilityConfig:
    """温度稳定性检测配置"""
    tol: float = 0.2                    # 温度容差 (°C)
    window_s: float = 40.0              # 检测窗口 (秒)
    soak_after_reach_s: float = 1800.0  # 达到稳定后浸泡时间 (秒)
    timeout_s: float = 1800.0           # 超时时间 (秒)

    wait_after_reach_s: float = 0.0
    wait_for_target_before_continue: bool = True
    restart_on_target_change: bool = False
    reuse_running_in_tol_without_soak: bool = True
    precondition_next_group_enabled: bool = False
    transition_check_window_s: float = 120.0
    transition_min_delta_c: float = 0.3
    command_offset_c: float = 0.0
    analyzer_chamber_temp_enabled: bool = True
    analyzer_chamber_temp_window_s: float = 60.0
    analyzer_chamber_temp_span_c: float = 0.03
    analyzer_chamber_temp_target_tol_c: Optional[float] = None
    analyzer_chamber_temp_timeout_s: float = 3600.0
    analyzer_chamber_temp_first_valid_timeout_s: float = 120.0
    analyzer_chamber_temp_poll_s: float = 1.0

    @classmethod
    def from_dict(cls, d: Optional[Dict[str, Any]]) -> "TemperatureStabilityConfig":
        if not d:
            return cls()
        soak_after_reach_s = d.get("soak_after_reach_s")
        if soak_after_reach_s is None:
            soak_after_reach_s = d.get("wait_after_reach_s", 1800.0)
        return cls(
            tol=float(d.get("tol", 0.2)),
            window_s=float(d.get("window_s", 40.0)),
            soak_after_reach_s=float(soak_after_reach_s),
            wait_after_reach_s=float(d.get("wait_after_reach_s", 0.0)),
            timeout_s=float(d.get("timeout_s", 1800.0)),
            wait_for_target_before_continue=bool(d.get("wait_for_target_before_continue", True)),
            restart_on_target_change=bool(d.get("restart_on_target_change", False)),
            reuse_running_in_tol_without_soak=bool(d.get("reuse_running_in_tol_without_soak", True)),
            precondition_next_group_enabled=bool(d.get("precondition_next_group_enabled", False)),
            transition_check_window_s=float(d.get("transition_check_window_s", 120.0)),
            transition_min_delta_c=float(d.get("transition_min_delta_c", 0.3)),
            command_offset_c=float(d.get("command_offset_c", 0.0)),
            analyzer_chamber_temp_enabled=bool(d.get("analyzer_chamber_temp_enabled", True)),
            analyzer_chamber_temp_window_s=float(d.get("analyzer_chamber_temp_window_s", 60.0)),
            analyzer_chamber_temp_span_c=float(d.get("analyzer_chamber_temp_span_c", 0.03)),
            analyzer_chamber_temp_target_tol_c=(
                float(d["analyzer_chamber_temp_target_tol_c"])
                if d.get("analyzer_chamber_temp_target_tol_c") is not None
                else None
            ),
            analyzer_chamber_temp_timeout_s=float(d.get("analyzer_chamber_temp_timeout_s", 3600.0)),
            analyzer_chamber_temp_first_valid_timeout_s=float(d.get("analyzer_chamber_temp_first_valid_timeout_s", 120.0)),
            analyzer_chamber_temp_poll_s=float(d.get("analyzer_chamber_temp_poll_s", 1.0)),
        )


@dataclass
class HumidityStabilityConfig:
    """湿度稳定性检测配置"""
    tol_dp: float = 0.3                 # 露点容差 (°C)
    window_s: float = 40.0              # 检测窗口 (秒)
    soak_after_reach_s: float = 600.0   # 达到稳定后浸泡时间 (秒)
    timeout_s: float = 1800.0           # 超时时间 (秒)

    precondition_next_group_enabled: bool = True

    @classmethod
    def from_dict(cls, d: Optional[Dict[str, Any]]) -> "HumidityStabilityConfig":
        if not d:
            return cls()
        return cls(
            tol_dp=d.get("tol_dp", 0.3),
            window_s=d.get("window_s", 40.0),
            soak_after_reach_s=d.get("soak_after_reach_s", 600.0),
            timeout_s=d.get("timeout_s", 1800.0),
            precondition_next_group_enabled=bool(d.get("precondition_next_group_enabled", True)),
        )


@dataclass
class PressureStabilityConfig:
    """压力稳定性检测配置"""
    tol_hpa: float = 0.5                # 压力容差 (hPa)
    window_s: float = 10.0              # 检测窗口 (秒)
    soak_after_reach_s: float = 30.0    # 达到稳定后浸泡时间 (秒)
    timeout_s: float = 300.0            # 超时时间 (秒)

    @classmethod
    def from_dict(cls, d: Optional[Dict[str, Any]]) -> "PressureStabilityConfig":
        if not d:
            return cls()
        return cls(
            tol_hpa=d.get("tol_hpa", 0.5),
            window_s=d.get("window_s", 10.0),
            soak_after_reach_s=d.get("soak_after_reach_s", 30.0),
            timeout_s=d.get("timeout_s", 300.0),
        )


@dataclass
class SignalStabilityConfig:
    """信号稳定性检测配置"""
    tol_pct: float = 0.1                # 信号容差 (%)
    window_s: float = 30.0              # 检测窗口 (秒)
    timeout_s: float = 600.0            # 超时时间 (秒)

    @classmethod
    def from_dict(cls, d: Optional[Dict[str, Any]]) -> "SignalStabilityConfig":
        if not d:
            return cls()
        return cls(
            tol_pct=d.get("tol_pct", 0.1),
            window_s=d.get("window_s", 30.0),
            timeout_s=d.get("timeout_s", 600.0),
        )


@dataclass
class StabilityConfig:
    """稳定性检测配置集合"""
    temperature: TemperatureStabilityConfig = field(default_factory=TemperatureStabilityConfig)
    humidity: HumidityStabilityConfig = field(default_factory=HumidityStabilityConfig)
    pressure: PressureStabilityConfig = field(default_factory=PressureStabilityConfig)
    signal: SignalStabilityConfig = field(default_factory=SignalStabilityConfig)
    humidity_generator: Dict[str, Any] = field(default_factory=dict)
    dewpoint: Dict[str, Any] = field(default_factory=dict)
    h2o_route: Dict[str, Any] = field(default_factory=dict)
    co2_route: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: Optional[Dict[str, Any]]) -> "StabilityConfig":
        if not d:
            return cls()
        return cls(
            temperature=TemperatureStabilityConfig.from_dict(d.get("temperature")),
            humidity=HumidityStabilityConfig.from_dict(d.get("humidity") or d.get("humidity_generator")),
            pressure=PressureStabilityConfig.from_dict(d.get("pressure")),
            signal=SignalStabilityConfig.from_dict(d.get("signal")),
            humidity_generator=dict(d.get("humidity_generator", {})),
            dewpoint=dict(d.get("dewpoint", {})),
            h2o_route=dict(d.get("h2o_route", {})),
            co2_route=dict(d.get("co2_route", {})),
        )


@dataclass
class SamplingConfig:
    """采样配置"""
    interval_s: float = 1.0             # 采样间隔 (秒)
    count: int = 10                     # 采样次数
    discard_first_n: int = 0            # 丢弃前N个样本

    @classmethod
    def from_dict(cls, d: Optional[Dict[str, Any]]) -> "SamplingConfig":
        if not d:
            return cls()
        return cls(
            interval_s=d.get("interval_s", 1.0),
            count=d.get("count", 10),
            discard_first_n=d.get("discard_first_n", 0),
        )


@dataclass
class PrecheckConfig:
    """预检配置"""
    enabled: bool = True                # 是否启用预检
    pressure_leak_test: bool = True     # 压力泄漏测试
    sensor_check: bool = True           # 传感器检查
    device_connection: bool = True      # 设备连接检查

    @classmethod
    def from_dict(cls, d: Optional[Dict[str, Any]]) -> "PrecheckConfig":
        if not d:
            return cls()
        return cls(
            enabled=d.get("enabled", True),
            pressure_leak_test=d.get("pressure_leak_test", True),
            sensor_check=d.get("sensor_check", True),
            device_connection=d.get("device_connection", True),
        )


@dataclass
class PressureControlConfig:
    """压力控制配置"""
    setpoint_tolerance_hpa: float = 0.5 # 设定点容差 (hPa)

    @classmethod
    def from_dict(cls, d: Optional[Dict[str, Any]]) -> "PressureControlConfig":
        if not d:
            return cls()
        return cls(
            setpoint_tolerance_hpa=d.get("setpoint_tolerance_hpa", 0.5),
        )


@dataclass
class ValveConfig:
    """Snapshot of raw valve-route keys that the V2 routing service consumes."""

    co2_path: Optional[int] = None
    co2_path_group2: Optional[int] = None
    gas_main: Optional[int] = None
    h2o_path: Optional[int] = None
    flow_switch: Optional[int] = None
    hold: Optional[int] = None
    relay_map: Dict[str, Any] = field(default_factory=dict)
    co2_map: Dict[str, Any] = field(default_factory=dict)
    co2_map_group2: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: Optional[Dict[str, Any]]) -> "ValveConfig":
        if not d:
            return cls()
        config = cls()
        config.co2_path = None if d.get("co2_path") is None else int(d.get("co2_path"))
        config.co2_path_group2 = None if d.get("co2_path_group2") is None else int(d.get("co2_path_group2"))
        config.gas_main = None if d.get("gas_main") is None else int(d.get("gas_main"))
        config.h2o_path = None if d.get("h2o_path") is None else int(d.get("h2o_path"))
        config.flow_switch = None if d.get("flow_switch") is None else int(d.get("flow_switch"))
        config.hold = None if d.get("hold") is None else int(d.get("hold"))
        config.relay_map = dict(d.get("relay_map", {}))
        config.co2_map = dict(d.get("co2_map", {}))
        config.co2_map_group2 = dict(d.get("co2_map_group2", {}))
        return config


@dataclass
class CoefficientSummaryColumnConfig:
    """Summary-column mapping for one gas fit."""

    target: str
    ratio: str
    pressure_scale: float = 1.0

    @classmethod
    def from_dict(
        cls,
        d: Optional[Dict[str, Any]],
        *,
        default_target: str,
        default_ratio: str,
    ) -> "CoefficientSummaryColumnConfig":
        payload = d or {}
        return cls(
            target=str(payload.get("target", default_target)),
            ratio=str(payload.get("ratio", default_ratio)),
            pressure_scale=float(payload.get("pressure_scale", 1.0)),
        )


@dataclass
class H2OSummarySelectionConfig:
    """Corrected H2O point-selection rule for ratio-poly fitting."""

    include_h2o_phase: bool = True
    temperature_buckets_c: List[float] = field(default_factory=lambda: [float(value) for value in DEFAULT_H2O_TEMPERATURE_BUCKETS_C])
    temperature_bucket_tolerance_c: float = DEFAULT_H2O_TEMPERATURE_BUCKET_TOLERANCE_C
    include_co2_temp_groups_c: List[float] = field(default_factory=lambda: [float(value) for value in DEFAULT_H2O_INCLUDE_CO2_TEMP_GROUPS_C])
    include_co2_zero_ppm_rows: bool = DEFAULT_H2O_INCLUDE_CO2_ZERO_PPM_ROWS
    co2_zero_ppm_target: float = DEFAULT_H2O_CO2_ZERO_PPM_TARGET
    co2_zero_ppm_tolerance: float = DEFAULT_H2O_CO2_ZERO_PPM_TOLERANCE
    include_co2_zero_ppm_temp_groups_c: List[float] = field(default_factory=lambda: [float(value) for value in DEFAULT_H2O_INCLUDE_CO2_ZERO_PPM_TEMP_GROUPS_C])
    temp_tolerance_c: float = DEFAULT_H2O_TEMP_TOLERANCE_C
    co2_zero_ppm_anchor_quality_gate_enabled: bool = DEFAULT_H2O_CO2_ZERO_PPM_ANCHOR_QUALITY_GATE_ENABLED
    co2_zero_ppm_anchor_require_h2o_dew: bool = DEFAULT_H2O_CO2_ZERO_PPM_ANCHOR_REQUIRE_H2O_DEW
    co2_zero_ppm_anchor_max_ppm_h2o_dew_default: float = DEFAULT_H2O_CO2_ZERO_PPM_ANCHOR_MAX_PPM_H2O_DEW_DEFAULT
    co2_zero_ppm_anchor_max_ppm_h2o_dew_by_temp_c: Dict[str, float] = field(
        default_factory=lambda: {
            str(key): float(value)
            for key, value in DEFAULT_H2O_CO2_ZERO_PPM_ANCHOR_MAX_PPM_H2O_DEW_BY_TEMP_C.items()
        }
    )

    @classmethod
    def from_dict(cls, d: Optional[Dict[str, Any]]) -> "H2OSummarySelectionConfig":
        payload = normalize_h2o_summary_selection(d)
        return cls(
            include_h2o_phase=bool(payload["include_h2o_phase"]),
            temperature_buckets_c=[float(value) for value in payload["temperature_buckets_c"]],
            temperature_bucket_tolerance_c=float(payload["temperature_bucket_tolerance_c"]),
            include_co2_temp_groups_c=[float(value) for value in payload["include_co2_temp_groups_c"]],
            include_co2_zero_ppm_rows=bool(payload["include_co2_zero_ppm_rows"]),
            co2_zero_ppm_target=float(payload["co2_zero_ppm_target"]),
            co2_zero_ppm_tolerance=float(payload["co2_zero_ppm_tolerance"]),
            include_co2_zero_ppm_temp_groups_c=[float(value) for value in payload["include_co2_zero_ppm_temp_groups_c"]],
            temp_tolerance_c=float(payload["temp_tolerance_c"]),
            co2_zero_ppm_anchor_quality_gate_enabled=bool(payload["co2_zero_ppm_anchor_quality_gate_enabled"]),
            co2_zero_ppm_anchor_require_h2o_dew=bool(payload["co2_zero_ppm_anchor_require_h2o_dew"]),
            co2_zero_ppm_anchor_max_ppm_h2o_dew_default=float(payload["co2_zero_ppm_anchor_max_ppm_h2o_dew_default"]),
            co2_zero_ppm_anchor_max_ppm_h2o_dew_by_temp_c={
                str(key): float(value)
                for key, value in dict(payload["co2_zero_ppm_anchor_max_ppm_h2o_dew_by_temp_c"]).items()
            },
        )


@dataclass
class CoefficientsConfig:
    """系数拟合配置"""

    enabled: bool = False
    auto_fit: bool = False
    model: str = "amt"                  # 拟合模型
    ratio_degree: int = 3
    temperature_offset_c: float = 273.15
    add_intercept: bool = True
    simplify_coefficients: bool = True
    simplification_method: str = "column_norm"
    target_digits: int = 6
    report_temperature_key: str = "Temp"
    report_pressure_key: str = "P_fit"
    report_output_name: str = "calibration_coefficients.xlsx"
    summary_columns: Dict[str, CoefficientSummaryColumnConfig] = field(
        default_factory=lambda: {
            "co2": CoefficientSummaryColumnConfig(
                target="ppm_CO2_Tank",
                ratio="R_CO2",
                pressure_scale=1.0,
            ),
            "h2o": CoefficientSummaryColumnConfig(
                target="ppm_H2O_Dew",
                ratio="R_H2O",
                pressure_scale=1.0,
            ),
        }
    )
    h2o_summary_selection: H2OSummarySelectionConfig = field(default_factory=H2OSummarySelectionConfig)

    @classmethod
    def from_dict(cls, d: Optional[Dict[str, Any]]) -> "CoefficientsConfig":
        if not d:
            return cls()
        summary_columns = d.get("summary_columns") or {}
        return cls(
            enabled=d.get("enabled", False),
            auto_fit=d.get("auto_fit", False),
            model=d.get("model", "amt"),
            ratio_degree=d.get("ratio_degree", 3),
            temperature_offset_c=d.get("temperature_offset_c", 273.15),
            add_intercept=d.get("add_intercept", True),
            simplify_coefficients=d.get("simplify_coefficients", True),
            simplification_method=d.get("simplification_method", "column_norm"),
            target_digits=d.get("target_digits", 6),
            report_temperature_key=d.get("report_temperature_key", "Temp"),
            report_pressure_key=d.get("report_pressure_key", "P_fit"),
            report_output_name=d.get("report_output_name", "calibration_coefficients.xlsx"),
            summary_columns={
                "co2": CoefficientSummaryColumnConfig.from_dict(
                    summary_columns.get("co2"),
                    default_target="ppm_CO2_Tank",
                    default_ratio="R_CO2",
                ),
                "h2o": CoefficientSummaryColumnConfig.from_dict(
                    summary_columns.get("h2o"),
                    default_target="ppm_H2O_Dew",
                    default_ratio="R_H2O",
                ),
            },
            h2o_summary_selection=H2OSummarySelectionConfig.from_dict(d.get("h2o_summary_selection")),
        )


@dataclass
class AIFeaturesConfig:
    """Feature flags for optional AI copilot integrations."""

    run_summary: bool = True
    qc_explanation: bool = True
    anomaly_diagnosis: bool = True
    algorithm_recommendation: bool = True

    @classmethod
    def from_dict(cls, d: Optional[Dict[str, Any]]) -> "AIFeaturesConfig":
        if not d:
            return cls()
        return cls(
            run_summary=bool(d.get("run_summary", True)),
            qc_explanation=bool(d.get("qc_explanation", True)),
            anomaly_diagnosis=bool(d.get("anomaly_diagnosis", True)),
            algorithm_recommendation=bool(d.get("algorithm_recommendation", True)),
        )


@dataclass
class AIConfig:
    """Optional AI copilot configuration."""

    enabled: bool = False
    provider: str = "mock"
    model: str = "gpt-4o-mini"
    api_key: str = ""
    base_url: str = ""
    timeout_s: float = 30.0
    max_retries: int = 3
    max_tokens: int = 1200
    temperature: float = 0.2
    fallback_to_mock: bool = True
    features: AIFeaturesConfig = field(default_factory=AIFeaturesConfig)

    @classmethod
    def from_dict(cls, d: Optional[Dict[str, Any]]) -> "AIConfig":
        if not d:
            return cls()
        payload = d.get("ai") if isinstance(d.get("ai"), dict) else d
        return cls(
            enabled=bool(payload.get("enabled", False)),
            provider=str(payload.get("provider", "mock")),
            model=str(payload.get("model", "gpt-4o-mini")),
            api_key=str(payload.get("api_key", "")),
            base_url=str(payload.get("base_url", "")),
            timeout_s=float(payload.get("timeout_s", 30.0)),
            max_retries=int(payload.get("max_retries", 3)),
            max_tokens=int(payload.get("max_tokens", 1200)),
            temperature=float(payload.get("temperature", 0.2)),
            fallback_to_mock=bool(payload.get("fallback_to_mock", True)),
            features=AIFeaturesConfig.from_dict(payload.get("features")),
        )

    def feature_enabled(self, name: str) -> bool:
        return bool(self.enabled and getattr(self.features, name, False))


@dataclass
class PathsConfig:
    """路径配置"""
    points_excel: str = "points.xlsx"   # 校准点文件
    output_dir: str = "output"          # 输出目录
    logs_dir: str = "logs"              # 日志目录

    @classmethod
    def from_dict(cls, d: Optional[Dict[str, Any]]) -> "PathsConfig":
        if not d:
            return cls()
        return cls(
            points_excel=d.get("points_excel", "points.xlsx"),
            output_dir=d.get("output_dir", "output"),
            logs_dir=d.get("logs_dir", "logs"),
        )


@dataclass
class FeaturesConfig:
    """特性开关配置"""

    simulation_mode: bool = False
    debug_mode: bool = False

    @classmethod
    def from_dict(cls, d: Optional[Dict[str, Any]]) -> "FeaturesConfig":
        if not d:
            return cls()
        return cls(
            simulation_mode=d.get("simulation_mode", False),
            debug_mode=d.get("debug_mode", False),
        )


@dataclass
class QCConfig:
    """Quality-control configuration."""

    min_sample_count: int = 5
    max_outlier_ratio: float = 0.2
    spike_threshold: float = 3.0
    drift_threshold: float = 0.1
    quality_threshold: float = 0.7
    rule_config: "QCRuleConfig" = field(default_factory=lambda: QCRuleConfig())

    @classmethod
    def from_dict(cls, d: Optional[Dict[str, Any]]) -> "QCConfig":
        if not d:
            return cls()
        return cls(
            min_sample_count=d.get("min_sample_count", 5),
            max_outlier_ratio=d.get("max_outlier_ratio", 0.2),
            spike_threshold=d.get("spike_threshold", 3.0),
            drift_threshold=d.get("drift_threshold", 0.1),
            quality_threshold=d.get("quality_threshold", 0.7),
            rule_config=QCRuleConfig.from_dict(d.get("rule_config")),
        )


@dataclass
class QCRuleConfig:
    """QC rule mapping configuration."""

    default_rule: str = "default"
    route_rules: Dict[str, str] = field(default_factory=lambda: {
        "co2": "co2_strict",
        "h2o": "h2o_strict",
    })
    mode_rules: Dict[str, str] = field(default_factory=lambda: {
        "fast": "fast_mode",
        "verify": "verify_mode",
        "subzero": "subzero",
    })
    custom_rules: List[Dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: Optional[Dict[str, Any]]) -> "QCRuleConfig":
        if not d:
            return cls()
        return cls(
            default_rule=d.get("default_rule", "default"),
            route_rules=dict(d.get("route_rules", {"co2": "co2_strict", "h2o": "h2o_strict"})),
            mode_rules=dict(d.get("mode_rules", {"fast": "fast_mode", "verify": "verify_mode", "subzero": "subzero"})),
            custom_rules=list(d.get("custom_rules", [])),
        )
