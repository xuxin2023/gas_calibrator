from __future__ import annotations

from typing import Any


def build_step2_cli_safety_lines(config_safety: dict[str, Any] | None) -> list[str]:
    payload = dict(config_safety or {})
    if not payload:
        return []
    execution_gate = dict(payload.get("execution_gate") or {})
    classification = str(payload.get("classification") or "--")
    simulation_only = bool(payload.get("simulation_only", False))
    real_port_devices = int(payload.get("real_port_device_count", 0) or 0)
    engineering_flags = int(payload.get("engineering_only_flag_count", 0) or 0)
    lines = [
        "[Step2 safety] classification={classification} simulation_only={simulation_only} real_port_devices={real_port_devices} engineering_flags={engineering_flags}".format(
            classification=classification,
            simulation_only=str(simulation_only).lower(),
            real_port_devices=real_port_devices,
            engineering_flags=engineering_flags,
        )
    ]
    if execution_gate:
        lines.append(
            "[Step2 gate] status={status} requires_dual_unlock={requires_dual_unlock} allow_unsafe_cli={allow_cli} allow_unsafe_env={allow_env}".format(
                status=str(execution_gate.get("status") or "--"),
                requires_dual_unlock=str(bool(execution_gate.get("requires_dual_unlock", False))).lower(),
                allow_cli=str(bool(execution_gate.get("allow_unsafe_step2_config_flag", False))).lower(),
                allow_env=str(bool(execution_gate.get("allow_unsafe_step2_config_env", False))).lower(),
            )
        )
        lines.append(
            "[Step2 boundary] workflow_allowed={workflow_allowed} real_com={real_com} engineering_isolation_only={engineering_only}".format(
                workflow_allowed=str(bool(payload.get("step2_default_workflow_allowed", False))).lower(),
                real_com=real_port_devices,
                engineering_only=str(
                    bool(execution_gate.get("allow_unsafe_step2_config_flag", False))
                    and bool(execution_gate.get("allow_unsafe_step2_config_env", False))
                ).lower(),
            )
        )
        summary = str(execution_gate.get("summary") or "").strip()
        if summary:
            lines.append(f"[Step2 summary] {summary}")
    blocked_details = [
        str(item).strip()
        for item in list(payload.get("blocked_reason_details") or [])
        if str(item).strip()
    ]
    if blocked_details:
        lines.append(f"[Step2 reasons] {' | '.join(blocked_details[:2])}")
    return lines
