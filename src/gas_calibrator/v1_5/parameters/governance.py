"""V1.5 parameter classification and audit rules.

This module does not write instruments. It classifies parameters, evaluates
whether a proposed change is allowed in the current role/run state, and builds
audit records for reviewed configuration changes.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional


RUN_STATES_LOCKING_CRITICAL_PARAMETERS = {
    "precheck",
    "pressure_channel_quick_check",
    "open_flow_purge",
    "stability_gate",
    "sample_window",
    "qc_classification",
    "candidate_review",
}

HIGH_RISK_NAMES = {
    "SENCO1",
    "SENCO2",
    "SENCO3",
    "SENCO4",
    "SENCO5",
    "SENCO6",
    "SENCO7",
    "SENCO8",
    "SENCO9",
    "CLEARSENCO1",
    "CLEARSENCO2",
    "CLEARSENCO3",
    "CLEARSENCO4",
    "CLEARSENCO5",
    "CLEARSENCO6",
    "CLEARSENCO7",
    "CLEARSENCO8",
    "CLEARSENCO9",
    "SETPOW",
    "SETILLUM",
    "SETCO2",
    "SETCOM",
    "ID",
    "RESET",
}


@dataclass(frozen=True)
class ParameterDefinition:
    name: str
    level: str
    label: str
    writable_roles: tuple[str, ...]
    requires_approval: bool = False
    device_write: bool = False
    hidden_by_default: bool = False
    readback_required: bool = False
    rollback_required: bool = False
    report_snapshot_required: bool = True


@dataclass(frozen=True)
class ParameterChangeRequest:
    name: str
    old_value: Any
    new_value: Any
    actor: str
    role: str
    reason: str
    run_state: str = "planning"
    approved_by: str = ""
    readback_value: Any = None
    rollback_plan: str = ""


@dataclass(frozen=True)
class ParameterDecision:
    status: str
    reasons: List[str]
    level: str
    audit_required: bool
    device_write: bool
    high_risk: bool


PARAMETERS: Dict[str, ParameterDefinition] = {
    # A: run parameters, no direct device write.
    "calibration_plan": ParameterDefinition("calibration_plan", "A", "校准计划", ("operator", "engineer", "admin")),
    "sample_window_s": ParameterDefinition("sample_window_s", "A", "采样窗口", ("operator", "engineer", "admin")),
    "purge_min_s": ParameterDefinition("purge_min_s", "A", "最短吹扫时间", ("operator", "engineer", "admin")),
    "allow_candidate_coefficients": ParameterDefinition("allow_candidate_coefficients", "A", "是否生成候选系数", ("operator", "engineer", "admin")),
    # B: QC parameters.
    "co2_stability_slope_max": ParameterDefinition("co2_stability_slope_max", "B", "CO2 稳定阈值", ("engineer", "admin"), requires_approval=True),
    "h2o_stability_slope_max": ParameterDefinition("h2o_stability_slope_max", "B", "H2O 稳定阈值", ("engineer", "admin"), requires_approval=True),
    "dewpoint_span_c_max": ParameterDefinition("dewpoint_span_c_max", "B", "露点稳定阈值", ("engineer", "admin"), requires_approval=True),
    "pressure_delta_hpa_max": ParameterDefinition("pressure_delta_hpa_max", "B", "压力一致性阈值", ("engineer", "admin"), requires_approval=True),
    "factory_signal_span_max": ParameterDefinition("factory_signal_span_max", "B", "工厂信号稳定阈值", ("engineer", "admin"), requires_approval=True),
    # C: traceability data.
    "standard_gas_certificate_value": ParameterDefinition("standard_gas_certificate_value", "C", "标准气证书值", ("engineer", "admin"), requires_approval=True),
    "standard_gas_certificate_hash": ParameterDefinition("standard_gas_certificate_hash", "C", "标准气证书 hash", ("engineer", "admin"), requires_approval=True),
    "com22_certificate_hash": ParameterDefinition("com22_certificate_hash", "C", "COM22 证书 hash", ("engineer", "admin"), requires_approval=True),
    "dewpoint_reference_certificate_hash": ParameterDefinition("dewpoint_reference_certificate_hash", "C", "露点仪证书 hash", ("engineer", "admin"), requires_approval=True),
    # D: controlled device working parameters.
    "MODE": ParameterDefinition("MODE", "D", "分析仪模式", ("engineer", "admin"), requires_approval=True, device_write=True, readback_required=True, rollback_required=True),
    "FTD": ParameterDefinition("FTD", "D", "主动发送频率", ("engineer", "admin"), requires_approval=True, device_write=True, readback_required=True, rollback_required=True),
    "AVERAGE1": ParameterDefinition("AVERAGE1", "D", "H2O 平均/滤波参数", ("engineer", "admin"), requires_approval=True, device_write=True, readback_required=True, rollback_required=True),
    "AVERAGE2": ParameterDefinition("AVERAGE2", "D", "CO2 平均/滤波参数", ("engineer", "admin"), requires_approval=True, device_write=True, readback_required=True, rollback_required=True),
    "SENTEMP1": ParameterDefinition("SENTEMP1", "D", "校准温度点 1", ("engineer", "admin"), requires_approval=True, device_write=True, readback_required=True, rollback_required=True),
    "SENTEMP2": ParameterDefinition("SENTEMP2", "D", "校准温度点 2", ("engineer", "admin"), requires_approval=True, device_write=True, readback_required=True, rollback_required=True),
    "TIMEOUT": ParameterDefinition("TIMEOUT", "D", "通信超时", ("engineer", "admin"), requires_approval=True, device_write=True, readback_required=True, rollback_required=True),
}

for _name in HIGH_RISK_NAMES:
    PARAMETERS[_name] = ParameterDefinition(
        _name,
        "E",
        f"高风险设备参数 {_name}",
        ("admin",),
        requires_approval=True,
        device_write=True,
        hidden_by_default=True,
        readback_required=True,
        rollback_required=True,
    )


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def snapshot_hash(payload: Mapping[str, Any]) -> str:
    rendered = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(rendered.encode("utf-8")).hexdigest()


def classify_parameter(name: str) -> ParameterDefinition:
    normalized = str(name or "").strip()
    upper = normalized.upper()
    if upper in PARAMETERS:
        return PARAMETERS[upper]
    if normalized in PARAMETERS:
        return PARAMETERS[normalized]
    return ParameterDefinition(normalized, "A", normalized, ("operator", "engineer", "admin"))


def validate_parameter_change(request: ParameterChangeRequest) -> ParameterDecision:
    definition = classify_parameter(request.name)
    reasons: List[str] = []
    role = str(request.role or "").lower()
    run_state = str(request.run_state or "planning").lower()

    if role not in definition.writable_roles:
        reasons.append("role_not_authorized")
    if not request.actor:
        reasons.append("actor_required")
    if not str(request.reason or "").strip():
        reasons.append("change_reason_required")
    if definition.level in {"B", "C", "D", "E"} and run_state in RUN_STATES_LOCKING_CRITICAL_PARAMETERS:
        reasons.append("critical_parameter_locked_during_run")
    if definition.requires_approval and not request.approved_by:
        reasons.append("approval_required")
    if definition.readback_required and request.readback_value != request.new_value:
        reasons.append("readback_mismatch_or_missing")
    if definition.rollback_required and not str(request.rollback_plan or "").strip():
        reasons.append("rollback_plan_required")
    if definition.level == "E":
        reasons.append("high_risk_parameter_hidden_by_default")
    if definition.device_write:
        reasons.append("device_write_not_enabled_in_v1_5_parameter_ui_v0")

    return ParameterDecision(
        status="pass" if not reasons else "fail",
        reasons=reasons,
        level=definition.level,
        audit_required=True,
        device_write=definition.device_write,
        high_risk=definition.level == "E",
    )


def build_parameter_audit_event(request: ParameterChangeRequest, decision: ParameterDecision) -> Dict[str, Any]:
    event = {
        "event_type": "v1_5_parameter_change_request",
        "timestamp": now_iso(),
        "parameter_name": request.name,
        "level": decision.level,
        "old_value": request.old_value,
        "new_value": request.new_value,
        "actor": request.actor,
        "role": request.role,
        "reason": request.reason,
        "run_state": request.run_state,
        "approved_by": request.approved_by,
        "readback_value": request.readback_value,
        "rollback_plan": request.rollback_plan,
        "decision_status": decision.status,
        "decision_reasons": list(decision.reasons),
        "device_write": decision.device_write,
        "high_risk": decision.high_risk,
    }
    event["audit_hash"] = snapshot_hash(event)
    return event


def build_parameter_surface(*, include_hidden: bool = False) -> Dict[str, Any]:
    visible = []
    for definition in sorted(PARAMETERS.values(), key=lambda item: (item.level, item.name)):
        if definition.hidden_by_default and not include_hidden:
            continue
        visible.append(asdict(definition))
    return {
        "schema_version": "v1_5_parameter_surface_v0",
        "sidecar_only": True,
        "device_write_enabled": False,
        "high_risk_parameters_hidden_by_default": True,
        "levels": {
            "A": "运行参数，默认仅写本次 run/config snapshot",
            "B": "QC 参数，工程师修改并需要审核",
            "C": "标准气/参考设备证书，变更必须可追溯",
            "D": "受控设备工作参数，v0 只读展示，不执行写入",
            "E": "高风险设备参数，默认隐藏且 v0 禁止写入",
        },
        "parameters": visible,
    }
