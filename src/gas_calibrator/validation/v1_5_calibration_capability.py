"""Assess whether the V1.5 program is currently calibratable.

This is an offline reviewer aid. It reads existing evidence-status and
verification artifacts only; it never opens COM ports, controls routes or
valves, or writes analyzer coefficients.
"""

from __future__ import annotations

import csv
import json
import math
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence


SCHEMA = "v1_5_calibration_capability_v1"


@dataclass(frozen=True)
class CapabilityIssue:
    severity: str
    code: str
    message: str
    physical_meaning: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class VerificationRollup:
    available: bool
    status: str
    row_count: int = 0
    device_ids: tuple[str, ...] = ()
    components: tuple[str, ...] = ()
    max_abs_error_pct: Optional[float] = None
    max_abs_error_ppm: Optional[float] = None
    max_ratio_range: Optional[float] = None
    min_dewpoint_c: Optional[float] = None
    max_dewpoint_c: Optional[float] = None
    source_paths: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CandidateRollup:
    available: bool
    status: str
    row_count: int = 0
    device_ids: tuple[str, ...] = ()
    max_payload_error_pct: Optional[float] = None
    max_payload_error_ppm: Optional[float] = None
    blocked_devices: tuple[str, ...] = ()
    source_paths: tuple[str, ...] = ()

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _load_json(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return {}
    source = Path(path)
    if not source.exists():
        return {}
    payload = json.loads(source.read_text(encoding="utf-8"))
    return dict(payload) if isinstance(payload, Mapping) else {}


def _load_csv(path: str | Path) -> list[dict[str, Any]]:
    source = Path(path)
    with source.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except Exception:
        return None
    if not math.isfinite(number):
        return None
    return number


def _unique(values: Iterable[Any]) -> tuple[str, ...]:
    cleaned = {str(value).strip() for value in values if str(value or "").strip()}
    return tuple(sorted(cleaned))


def _stage_map(status: Mapping[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for row in status.get("stage_statuses") or []:
        if not isinstance(row, Mapping):
            continue
        stage_id = str(row.get("stage_id") or "").strip()
        if stage_id:
            out[stage_id] = str(row.get("status") or "").strip()
    return out


def _first_present(row: Mapping[str, Any], keys: Sequence[str]) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _component_from_row(row: Mapping[str, Any]) -> str:
    component = str(row.get("component") or "").strip().lower()
    if component:
        return component
    point = str(row.get("point") or row.get("point_id") or "").lower()
    if "h2o" in point or "water" in point:
        return "h2o"
    return "co2"


def summarize_verification_csvs(
    paths: Sequence[str | Path],
    *,
    co2_limit_pct: float = 1.5,
    h2o_limit_pct: float = 2.0,
) -> VerificationRollup:
    rows: list[dict[str, Any]] = []
    source_paths: list[str] = []
    for path in paths:
        source = Path(path)
        if not source.exists():
            continue
        source_paths.append(str(source.resolve()))
        rows.extend(_load_csv(source))
    if not rows:
        return VerificationRollup(available=False, status="not_available")

    error_pcts: list[float] = []
    error_ppms: list[float] = []
    ratio_ranges: list[float] = []
    dewpoints: list[float] = []
    notes: list[str] = []
    row_failures = 0
    components: list[str] = []
    for row in rows:
        component = _component_from_row(row)
        components.append(component)
        limit = h2o_limit_pct if component == "h2o" else co2_limit_pct
        error_pct = _safe_float(_first_present(row, ("error_pct", "max_abs_error_pct", "payload_max_abs_error_pct")))
        if error_pct is not None:
            error_pcts.append(abs(error_pct))
            if abs(error_pct) > limit:
                row_failures += 1
        row_status = str(row.get("status") or row.get("qc_status") or "").strip().lower()
        if row_status in {"fail", "blocked"}:
            row_failures += 1
        error_ppm = _safe_float(_first_present(row, ("error_ppm", "max_abs_error_ppm", "payload_max_abs_error_ppm")))
        if error_ppm is not None:
            error_ppms.append(abs(error_ppm))
        ratio_range = _safe_float(_first_present(row, ("ratio_f_range", "ratio_range", "co2_ratio_f_range", "h2o_ratio_f_range")))
        if ratio_range is not None:
            ratio_ranges.append(abs(ratio_range))
        dewpoint = _safe_float(_first_present(row, ("dewpoint_mean_c", "dewpoint_c", "dewpoint_avg_c")))
        if dewpoint is not None:
            dewpoints.append(dewpoint)

    if error_pcts:
        notes.append(f"max_abs_error_pct={max(error_pcts):.6g}")
    if ratio_ranges:
        notes.append(f"max_ratio_range={max(ratio_ranges):.6g}")
    status = "pass" if row_failures == 0 and error_pcts else ("review_required" if row_failures == 0 else "fail")
    return VerificationRollup(
        available=True,
        status=status,
        row_count=len(rows),
        device_ids=_unique(row.get("device_id") or row.get("analyzer_id") for row in rows),
        components=_unique(components),
        max_abs_error_pct=max(error_pcts) if error_pcts else None,
        max_abs_error_ppm=max(error_ppms) if error_ppms else None,
        max_ratio_range=max(ratio_ranges) if ratio_ranges else None,
        min_dewpoint_c=min(dewpoints) if dewpoints else None,
        max_dewpoint_c=max(dewpoints) if dewpoints else None,
        source_paths=tuple(source_paths),
        notes=tuple(notes),
    )


def summarize_candidate_csvs(paths: Sequence[str | Path]) -> CandidateRollup:
    rows: list[dict[str, Any]] = []
    source_paths: list[str] = []
    for path in paths:
        source = Path(path)
        if not source.exists():
            continue
        source_paths.append(str(source.resolve()))
        rows.extend(_load_csv(source))
    if not rows:
        return CandidateRollup(available=False, status="not_available")

    payload_error_pcts: list[float] = []
    payload_error_ppms: list[float] = []
    blocked_devices: list[str] = []
    for row in rows:
        status = str(row.get("candidate_status") or row.get("status") or "").strip().lower()
        blocked = str(row.get("blocked_reasons") or "").strip()
        if blocked or status in {"blocked", "fail", "not_ready"}:
            blocked_devices.append(str(row.get("device_id") or row.get("analyzer_id") or "unknown"))
        error_pct = _safe_float(_first_present(row, ("payload_max_abs_error_pct", "max_abs_error_pct")))
        if error_pct is not None:
            payload_error_pcts.append(abs(error_pct))
        error_ppm = _safe_float(_first_present(row, ("payload_max_abs_error_ppm", "max_abs_error_ppm")))
        if error_ppm is not None:
            payload_error_ppms.append(abs(error_ppm))

    status = "blocked" if blocked_devices else "review_ready"
    return CandidateRollup(
        available=True,
        status=status,
        row_count=len(rows),
        device_ids=_unique(row.get("device_id") or row.get("analyzer_id") for row in rows),
        max_payload_error_pct=max(payload_error_pcts) if payload_error_pcts else None,
        max_payload_error_ppm=max(payload_error_ppms) if payload_error_ppms else None,
        blocked_devices=tuple(sorted(set(blocked_devices))),
        source_paths=tuple(source_paths),
    )


def _status_is_pass(stage_statuses: Mapping[str, str], stage_id: str) -> bool:
    return stage_statuses.get(stage_id) == "pass"


def _required_method_stages(component: str) -> tuple[str, ...]:
    stages = ["full_flow_contract_gate", "plan_traceability", "pressure_quick_check", "candidate_review", "evidence_bundle"]
    component_lower = component.lower()
    if component_lower in {"co2", "both"}:
        stages.append("co2_open_flow")
    if component_lower in {"h2o", "both"}:
        stages.append("h2o_open_flow")
    return tuple(stages)


def build_v1_5_calibration_capability(
    *,
    run_status: Mapping[str, Any] | None = None,
    run_status_json: str | Path | None = None,
    verification_csvs: Sequence[str | Path] = (),
    candidate_csvs: Sequence[str | Path] = (),
    component: str = "both",
    co2_limit_pct: float = 1.5,
    h2o_limit_pct: float = 2.0,
) -> dict[str, Any]:
    """Return an offline V1.5 calibratability decision from existing evidence."""

    status_payload = dict(run_status or _load_json(run_status_json))
    component_lower = str(component or status_payload.get("component") or "both").strip().lower()
    stage_statuses = _stage_map(status_payload)
    verification = summarize_verification_csvs(
        verification_csvs,
        co2_limit_pct=co2_limit_pct,
        h2o_limit_pct=h2o_limit_pct,
    )
    candidates = summarize_candidate_csvs(candidate_csvs)

    issues: list[CapabilityIssue] = []
    if status_payload.get("contract_status") == "blocked" or status_payload.get("overall_status") == "blocked":
        issues.append(
            CapabilityIssue(
                "P0",
                "contract_blocked",
                "V1.5 正式流程合同被阻断。",
                "方法边界本身不安全或不完整，因此后续校准证据不能作为可信正式结果。",
            )
        )

    method_missing = [
        stage for stage in _required_method_stages(component_lower) if not _status_is_pass(stage_statuses, stage)
    ]
    for stage in method_missing:
        issues.append(
            CapabilityIssue(
                "P0",
                f"method_stage_not_passed:{stage}",
                f"必需方法阶段未通过: {stage}。",
                "压力先验证、开放流通采样、候选系数评审这一条物理测量链尚未完整证明。",
            )
        )

    traceability_checks = status_payload.get("traceability_checks")
    checks = traceability_checks if isinstance(traceability_checks, Mapping) else {}
    if component_lower in {"h2o", "both"}:
        if checks.get("has_water_route_traceability") is False:
            issues.append(
                CapabilityIssue(
                    "P1",
                    "h2o_traceability_incomplete",
                    "被检查证据中的 H2O 水路溯源不完整。",
                    "水路校准需要露点/参考支撑的水汽证据；CO2 零气不能替代 H2O 干气低水锚点。",
                )
            )
        if checks.get("has_h2o_raw_signal_fields") is False:
            issues.append(
                CapabilityIssue(
                    "P1",
                    "h2o_raw_signal_fields_missing",
                    "被检查证据中的 H2O 原始信号/比值字段不完整。",
                    "没有工厂模式 H2O signal/ratio，就难以把最终 H2O 数值与信号、压力、湿度状态影响区分开。",
                )
            )

    release_missing = [
        stage
        for stage in ("identity_getco_epoch0", "controlled_write_events", "post_write_reverification", "database_import")
        if stage_statuses.get(stage) not in {"pass", "write_attempted"}
    ]
    for stage in release_missing:
        issues.append(
            CapabilityIssue(
                "P1",
                f"release_stage_not_closed:{stage}",
                f"正式签发阶段未闭环: {stage}。",
                "正式签发需要设备身份、系数 epoch、写入/回读、写后复验和数据库审计闭环。",
            )
        )

    if candidates.available and candidates.status == "blocked":
        issues.append(
            CapabilityIssue(
                "P0",
                "candidate_review_blocked",
                f"候选系数评审存在阻断设备: {','.join(candidates.blocked_devices)}。",
                "被阻断的候选系数不能写入仪器。",
            )
        )
    if verification.available and verification.status == "fail":
        issues.append(
            CapabilityIssue(
                "P0",
                "verification_failed",
                "修复后或写入后复验存在失败点。",
                "更新后的测量模型尚未复现独立开放流通复验点。",
            )
        )

    p0_count = sum(1 for issue in issues if issue.severity == "P0")
    method_ready = not method_missing and p0_count == 0
    release_ready = method_ready and not release_missing and (
        not verification.available or verification.status == "pass"
    )

    if p0_count:
        capability_status = "not_calibratable_until_p0_resolved"
    elif method_ready and verification.available and verification.status == "pass":
        capability_status = "demonstrated_calibratable_for_verified_scope"
    elif method_ready:
        capability_status = "conditionally_calibratable_needs_release_closure"
    else:
        capability_status = "not_yet_calibratable_evidence_incomplete"

    physical_conclusion = (
        "V1.5 方法骨架已经具备支撑校准的能力，前提是先完成压力通道验证，组分采样期间保持开放流通直到采样窗口结束，"
        "保留工厂模式 ratio/signal 证据，并且每台分析仪都独立生成候选系数、写入后再做复验。"
        if method_ready
        else "被检查证据尚未完整证明压力先验证、开放流通采样的物理测量链。"
    )
    if verification.available and verification.status == "pass":
        physical_conclusion += (
            " 当前提供的复验证据说明，在已记录的气体状态和设备范围内，至少被验证设备可以被校准到一致状态。"
        )

    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "component": component_lower,
        "capability_status": capability_status,
        "method_backbone_ready": method_ready,
        "formal_release_ready": release_ready,
        "formal_release_blockers": release_missing,
        "physical_boundaries": {
            "offline_assessment_only": True,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "controls_valves_or_pace": False,
            "writes_coefficients": False,
            "not_real_acceptance_evidence": True,
        },
        "stage_statuses": dict(stage_statuses),
        "verification_rollup": verification.to_json(),
        "candidate_rollup": candidates.to_json(),
        "issues": [issue.to_json() for issue in issues],
        "physical_conclusion": physical_conclusion,
        "interpretation": {
            "program_can_calibrate": method_ready,
            "can_issue_formal_certificate_now": release_ready,
            "why_not_release_ready": release_missing,
            "verified_scope_device_ids": verification.device_ids,
            "verified_scope_components": verification.components,
        },
    }


def render_v1_5_calibration_capability_markdown(assessment: Mapping[str, Any]) -> str:
    """Render a Chinese reviewer-facing capability report."""

    lines: list[str] = [
        "# V1.5 校准能力离线评估",
        "",
        f"- 评估时间: `{assessment.get('generated_at')}`",
        f"- 组件范围: `{assessment.get('component')}`",
        f"- 能力状态: `{assessment.get('capability_status')}`",
        f"- 方法骨架可校准: `{assessment.get('method_backbone_ready')}`",
        f"- 可直接正式签发: `{assessment.get('formal_release_ready')}`",
        "",
        "## 物理结论",
        "",
        str(assessment.get("physical_conclusion") or ""),
        "",
        "## 复验证据摘要",
        "",
    ]
    verification = assessment.get("verification_rollup") if isinstance(assessment.get("verification_rollup"), Mapping) else {}
    lines.extend(
        [
            f"- 是否有复验证据: `{verification.get('available')}`",
            f"- 复验状态: `{verification.get('status')}`",
            f"- 设备 ID: `{', '.join(verification.get('device_ids') or [])}`",
            f"- 组件: `{', '.join(verification.get('components') or [])}`",
            f"- 最大相对误差: `{verification.get('max_abs_error_pct')}` %",
            f"- 最大绝对误差: `{verification.get('max_abs_error_ppm')}` ppm",
            f"- 最大滤波后比值波动: `{verification.get('max_ratio_range')}`",
            f"- 露点范围: `{verification.get('min_dewpoint_c')}` 到 `{verification.get('max_dewpoint_c')}` degC",
            "",
            "## 候选系数摘要",
            "",
        ]
    )
    candidate = assessment.get("candidate_rollup") if isinstance(assessment.get("candidate_rollup"), Mapping) else {}
    lines.extend(
        [
            f"- 是否有候选系数: `{candidate.get('available')}`",
            f"- 候选状态: `{candidate.get('status')}`",
            f"- 设备 ID: `{', '.join(candidate.get('device_ids') or [])}`",
            f"- 载荷层最大相对误差: `{candidate.get('max_payload_error_pct')}` %",
            f"- 载荷层最大绝对误差: `{candidate.get('max_payload_error_ppm')}` ppm",
            "",
            "## 阻塞项",
            "",
        ]
    )
    issues = assessment.get("issues") or []
    if not issues:
        lines.append("- 无 P0/P1 阻塞项。")
    for issue in issues:
        if not isinstance(issue, Mapping):
            continue
        lines.append(
            f"- `{issue.get('severity')}` `{issue.get('code')}`: {issue.get('message')} 物理意义: {issue.get('physical_meaning')}"
        )
    lines.extend(
        [
            "",
            "## 评估边界",
            "",
            "- 本报告只读取既有证据文件，不打开串口、不控制气路/水路/阀/压力控制器、不写 SENCO。",
            "- `方法骨架可校准=True` 表示程序流程具备校准能力；`可直接正式签发=True` 还要求身份、写入、回读、复验、数据库和报告闭环。",
        ]
    )
    return "\n".join(lines) + "\n"
