from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .acceptance_model import build_user_visible_evidence_boundary


STEP2_READINESS_SUMMARY_FILENAME = "step2_readiness_summary.json"
REAL_BENCH_UNLOCK_FLAG = "--allow-real-bench"
REAL_BENCH_UNLOCK_ENV = "GAS_CALIBRATOR_V2_ALLOW_REAL_BENCH"
HEADLESS_SMOKE_COMMAND = (
    "PYTHONPATH=src python -m gas_calibrator.v2.scripts.run_v2 "
    "--config src/gas_calibrator/v2/configs/smoke_v2_minimal.json --simulation --headless"
)
_OFFLINE_ONLY_ADAPTER_IDS = [
    "room_temp_pressure_diagnostic",
    "analyzer_chain_isolation",
]
_REVIEWER_HYDRATION_CONSUMERS = [
    "review_scope_manifest",
    "review_scope_export_index",
    "artifact_scope_view",
]
_GATE_LABELS = {
    "simulation_only_boundary": "仿真边界",
    "real_bench_locked_by_default": "real bench 锁门",
    "shared_experiment_flags_default_off": "共享实验开关",
    "offline_only_adapters_not_in_default_path": "离线适配器边界",
    "reviewer_surface_hydration_chain_ready": "reviewer handoff 主链",
    "headless_smoke_path_available": "headless smoke 路径",
    "readiness_evidence_complete": "治理证据完整性",
    "step2_gate_status": "阶段结论",
}


def build_step2_readiness_summary(
    *,
    run_id: str,
    simulation_mode: bool,
    config_governance_handoff: dict[str, Any] | None = None,
    smoke_config_path: str | Path | None = None,
    smoke_points_path: str | Path | None = None,
) -> dict[str, Any]:
    governance = dict(config_governance_handoff or {})
    smoke_config = Path(smoke_config_path) if smoke_config_path is not None else _default_smoke_config_path()
    smoke_points = Path(smoke_points_path) if smoke_points_path is not None else _default_smoke_points_path()
    boundary = build_user_visible_evidence_boundary(simulation_mode=simulation_mode)

    simulation_only = bool(governance.get("simulation_only", simulation_mode))
    operator_safe = bool(governance.get("operator_safe", simulation_only))
    real_port_device_count = int(governance.get("real_port_device_count", 0) or 0)
    engineering_only_flag_count = int(governance.get("engineering_only_flag_count", 0) or 0)
    enabled_engineering_flags = _normalize_string_list(governance.get("enabled_engineering_flags"))
    risk_markers = _normalize_string_list(governance.get("risk_markers"))
    execution_gate = dict(governance.get("execution_gate") or {})
    evidence_completeness = _build_evidence_completeness(governance)
    evidence_complete = bool(evidence_completeness.get("complete", False))
    step2_default_workflow_allowed = bool(
        governance.get(
            "step2_default_workflow_allowed",
            simulation_only and real_port_device_count == 0 and engineering_only_flag_count == 0,
        )
    )
    execution_gate_status = str(
        execution_gate.get("status") or ("open" if step2_default_workflow_allowed else "blocked")
    ).strip() or ("open" if step2_default_workflow_allowed else "blocked")
    requires_explicit_unlock = bool(
        governance.get(
            "requires_explicit_unlock",
            execution_gate_status in {"blocked", "unlocked_override"} and not step2_default_workflow_allowed,
        )
    )

    simulation_boundary_ok = simulation_mode and simulation_only and real_port_device_count == 0
    experiment_flags_default_off = engineering_only_flag_count == 0 and not enabled_engineering_flags
    real_bench_locked = True
    offline_adapters_isolated = True
    reviewer_hydration_ready = True
    headless_smoke_ready = smoke_config.exists() and smoke_points.exists()

    gates = [
        _gate(
            "simulation_only_boundary",
            "pass" if simulation_boundary_ok else "blocked",
            "simulation_boundary_confirmed" if simulation_boundary_ok else "simulation_boundary_broken",
            {
                "simulation_mode": bool(simulation_mode),
                "simulation_only": bool(simulation_only),
                "operator_safe": bool(operator_safe),
                "real_port_device_count": real_port_device_count,
                "devices_with_real_ports": list(governance.get("devices_with_real_ports") or []),
            },
        ),
        _gate(
            "real_bench_locked_by_default",
            "pass" if real_bench_locked else "blocked",
            "real_bench_dual_unlock_required",
            {
                "locked_by_default": True,
                "unlock_flag": REAL_BENCH_UNLOCK_FLAG,
                "unlock_env": REAL_BENCH_UNLOCK_ENV,
                "default_path_unchanged": True,
            },
        ),
        _gate(
            "shared_experiment_flags_default_off",
            "pass" if experiment_flags_default_off else "blocked",
            "shared_experiment_flags_default_off"
            if experiment_flags_default_off
            else "shared_experiment_flags_enabled",
            {
                "engineering_only_flag_count": engineering_only_flag_count,
                "enabled_engineering_flags": enabled_engineering_flags,
                "default_off_flags": [
                    "capture_then_hold_enabled",
                    "adaptive_pressure_sampling_enabled",
                    "soft_control_enabled",
                ],
            },
        ),
        _gate(
            "offline_only_adapters_not_in_default_path",
            "pass" if offline_adapters_isolated else "blocked",
            "offline_only_adapters_isolated" if offline_adapters_isolated else "offline_only_adapters_leaked",
            {
                "adapter_ids": list(_OFFLINE_ONLY_ADAPTER_IDS),
                "default_path_integration": False,
            },
        ),
        _gate(
            "reviewer_surface_hydration_chain_ready",
            "pass" if reviewer_hydration_ready else "blocked",
            "reviewer_surface_hydration_ready"
            if reviewer_hydration_ready
            else "reviewer_surface_hydration_missing",
            {
                "reviewer_display_available": True,
                "hydration_consumers": list(_REVIEWER_HYDRATION_CONSUMERS),
            },
        ),
        _gate(
            "headless_smoke_path_available",
            "pass" if headless_smoke_ready else "blocked",
            "headless_smoke_path_available" if headless_smoke_ready else "headless_smoke_path_missing",
            {
                "smoke_config_path": str(smoke_config),
                "smoke_config_exists": smoke_config.exists(),
                "smoke_points_path": str(smoke_points),
                "smoke_points_exists": smoke_points.exists(),
                "headless_command": HEADLESS_SMOKE_COMMAND,
            },
        ),
        _gate(
            "readiness_evidence_complete",
            "pass" if evidence_complete else "blocked",
            "config_governance_handoff_complete"
            if evidence_complete
            else "config_governance_handoff_incomplete",
            evidence_completeness,
        ),
    ]

    overall_ready = (
        simulation_boundary_ok
        and real_bench_locked
        and experiment_flags_default_off
        and offline_adapters_isolated
        and reviewer_hydration_ready
        and headless_smoke_ready
        and evidence_complete
        and step2_default_workflow_allowed
        and execution_gate_status == "open"
    )
    overall_status = "ready_for_engineering_isolation" if overall_ready else "not_ready"
    gates.append(
        _gate(
            "step2_gate_status",
            overall_status,
            f"execution_gate_{execution_gate_status}",
            {
                "execution_gate_status": execution_gate_status,
                "step2_default_workflow_allowed": step2_default_workflow_allowed,
                "requires_explicit_unlock": requires_explicit_unlock,
                "simulation_only": bool(simulation_only),
            },
        )
    )
    gate_status_counts = dict(Counter(str(gate.get("status") or "unknown") for gate in gates))

    blocking_items = [
        str(gate.get("gate_id") or "")
        for gate in gates
        if str(gate.get("status") or "") in {"blocked", "not_ready"}
    ]
    warning_items = list(
        dict.fromkeys(
            [
                *risk_markers,
                *(["requires_explicit_unlock"] if requires_explicit_unlock else []),
                *(["config_governance_handoff_incomplete"] if not evidence_complete else []),
                "simulation_offline_headless_only",
                "not_real_acceptance_evidence",
            ]
        )
    )
    notes = [
        "step2_readiness_artifact",
        "phase2_closeout_step3_engineering_isolation_bridge",
        "simulation_offline_headless_only",
        "not_real_acceptance_evidence",
        "default_path_unchanged",
    ]
    reviewer_display = _build_reviewer_display(
        overall_status=overall_status,
        gates=gates,
        blocking_items=blocking_items,
        warning_items=warning_items,
    )

    return {
        "schema_version": "1.0",
        "artifact_type": "step2_readiness_summary",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": str(run_id or ""),
        "phase": "step2_readiness_bridge",
        "mode": "simulation_only",
        "overall_status": overall_status,
        "ready_for_engineering_isolation": overall_status == "ready_for_engineering_isolation",
        "real_acceptance_ready": False,
        "evidence_mode": "simulation_offline_headless",
        "evidence_source": boundary.get("evidence_source"),
        "not_real_acceptance_evidence": True,
        "acceptance_level": boundary.get("acceptance_level"),
        "promotion_state": boundary.get("promotion_state"),
        "gates": gates,
        "gate_status_counts": gate_status_counts,
        "blocking_items": blocking_items,
        "warning_items": warning_items,
        "notes": notes,
        "reviewer_display": reviewer_display,
    }


def _default_smoke_config_path() -> Path:
    return Path(__file__).resolve().parents[1] / "configs" / "smoke_v2_minimal.json"


def _default_smoke_points_path() -> Path:
    return Path(__file__).resolve().parents[1] / "configs" / "smoke_points_minimal.json"


def _normalize_string_list(values: Any) -> list[str]:
    rows: list[str] = []
    for value in list(values or []):
        text = str(value or "").strip()
        if text and text not in rows:
            rows.append(text)
    return rows


def _gate(gate_id: str, status: str, reason_code: str, details: dict[str, Any]) -> dict[str, Any]:
    return {
        "gate_id": str(gate_id or ""),
        "status": str(status or ""),
        "reason_code": str(reason_code or ""),
        "details": dict(details or {}),
    }


def _build_evidence_completeness(governance: dict[str, Any]) -> dict[str, Any]:
    required_fields = [
        "simulation_only",
        "operator_safe",
        "execution_gate",
        "real_port_device_count",
        "engineering_only_flag_count",
        "step2_default_workflow_allowed",
        "requires_explicit_unlock",
    ]
    present_fields = [
        field
        for field in required_fields
        if field in governance and governance.get(field) not in (None, "")
    ]
    missing_fields = [field for field in required_fields if field not in present_fields]
    return {
        "governance_handoff_present": bool(governance),
        "required_fields": required_fields,
        "present_fields": present_fields,
        "missing_fields": missing_fields,
        "complete": bool(governance) and not missing_fields,
    }


def _build_reviewer_display(
    *,
    overall_status: str,
    gates: list[dict[str, Any]],
    blocking_items: list[str],
    warning_items: list[str],
) -> dict[str, Any]:
    status_line = (
        "阶段状态：已具备 engineering-isolation 准入准备"
        if overall_status == "ready_for_engineering_isolation"
        else "阶段状态：当前仍未达到 engineering-isolation 准入准备"
    )
    summary_text = (
        "Step 2 readiness：当前第二阶段的 simulation/offline/headless 边界已收口，"
        "可作为第三阶段 engineering-isolation 准入准备；不是 real acceptance 结论。"
        if overall_status == "ready_for_engineering_isolation"
        else "Step 2 readiness：当前仍存在阻塞项，只能继续作为第二阶段 simulation/offline/headless 治理证据；"
        "不是 real acceptance 结论。"
    )
    blocking_text = (
        "阻塞项：无。"
        if not blocking_items
        else "阻塞项：" + "、".join(_GATE_LABELS.get(item, item) for item in blocking_items) + "。"
    )
    warning_text = (
        "提示：本工件仅用于 simulation/offline/headless 阶段治理与准入准备，不代表 real acceptance evidence。"
        if not warning_items
        else "提示：本工件仅用于 simulation/offline/headless 阶段治理与准入准备；当前 warning code 包括 "
        + ", ".join(warning_items)
        + "。"
    )
    gate_lines = [_build_reviewer_gate_line(dict(gate)) for gate in list(gates or [])]
    return {
        "summary_text": summary_text,
        "status_line": status_line,
        "blocking_text": blocking_text,
        "warning_text": warning_text,
        "gate_lines": gate_lines,
    }


def _build_reviewer_gate_line(gate: dict[str, Any]) -> str:
    gate_id = str(gate.get("gate_id") or "")
    status = str(gate.get("status") or "")
    details = dict(gate.get("details") or {})
    label = _GATE_LABELS.get(gate_id, gate_id or "--")
    if gate_id == "simulation_only_boundary":
        return (
            f"{label}：通过，当前仍为 simulation/offline/headless；real-COM 设备 {int(details.get('real_port_device_count', 0) or 0)} 台。"
            if status == "pass"
            else f"{label}：阻塞，当前边界已偏离 simulation-only。"
        )
    if gate_id == "real_bench_locked_by_default":
        return (
            f"{label}：通过，默认仍需 {details.get('unlock_flag', REAL_BENCH_UNLOCK_FLAG)} "
            f"+ {details.get('unlock_env', REAL_BENCH_UNLOCK_ENV)}=1 才能进入工程隔离路径。"
        )
    if gate_id == "shared_experiment_flags_default_off":
        if status == "pass":
            return f"{label}：通过，capture_then_hold / adaptive_pressure_sampling / soft_control 默认关闭。"
        flags = ", ".join(list(details.get("enabled_engineering_flags") or [])) or "--"
        return f"{label}：阻塞，检测到 engineering-only 开关已开启：{flags}。"
    if gate_id == "offline_only_adapters_not_in_default_path":
        return "离线适配器边界：通过，room-temp / analyzer-chain 仍保持 offline-only，不在 default path。"
    if gate_id == "reviewer_surface_hydration_chain_ready":
        return "reviewer handoff 主链：通过，reviewer_display / hydration 主链已可用于 offline handoff。"
    if gate_id == "headless_smoke_path_available":
        return (
            f"{label}：通过，headless smoke 配置已就绪：{details.get('smoke_config_path', '--')}。"
            if status == "pass"
            else f"{label}：阻塞，headless smoke 配置或 points 路径缺失。"
        )
    if gate_id == "readiness_evidence_complete":
        if status == "pass":
            return "治理证据完整性：通过，config_governance_handoff 已具备 Step 2 readiness 所需关键字段。"
        missing_fields = ", ".join(list(details.get("missing_fields") or [])) or "--"
        return f"治理证据完整性：阻塞，config_governance_handoff 缺少关键字段：{missing_fields}。"
    if gate_id == "step2_gate_status":
        return (
            "阶段结论：已具备 engineering-isolation 准入准备；仍不是 real acceptance。"
            if status == "ready_for_engineering_isolation"
            else "阶段结论：当前仍未达到 engineering-isolation 准入准备；仍不是 real acceptance。"
        )
    return f"{label}：{status or '--'}"
