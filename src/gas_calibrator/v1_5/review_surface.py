"""Unified V1.5 formal calibration review surface.

This module merges already-recorded V1.5 evidence, the operation-console model,
parameter governance, and advanced QC outputs into a single static review
surface. It does not control devices, open COM ports, switch routes, control
PACE/valves, or write coefficients.
"""

from __future__ import annotations

import html
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

from .parameters.governance import build_parameter_surface
from .ui.operation_console import build_operation_console_model


SAFETY_FLAGS = (
    "opens_com_ports",
    "controls_water_or_gas_routes",
    "controls_valves_or_pace",
    "writes_coefficients",
)


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def load_json_object(path: str | Path | None) -> Dict[str, Any]:
    if not path:
        return {}
    source = Path(path)
    if not source.exists():
        return {}
    payload = json.loads(source.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError(f"JSON file must contain an object: {source}")
    return dict(payload)


def _cards_by_key(model: Mapping[str, Any]) -> Dict[str, Mapping[str, Any]]:
    return {
        str(card.get("key") or ""): card
        for card in model.get("cards", [])
        if isinstance(card, Mapping)
    }


def _split_reasons(value: Any) -> List[str]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    return [item for item in str(value).split(";") if item]


def _append_unique(target: List[str], values: Sequence[Any]) -> None:
    for value in values:
        text = str(value or "").strip()
        if text and text not in target:
            target.append(text)


def _safety_summary(*models: Mapping[str, Any]) -> Dict[str, Any]:
    violations: List[str] = []
    for flag in SAFETY_FLAGS:
        if any(bool(model.get(flag)) for model in models if model):
            violations.append(f"{flag}_true")
    return {
        "status": "pass" if not violations else "fail",
        "violations": violations,
        "sidecar_only": True,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "controls_valves_or_pace": False,
        "writes_coefficients": False,
    }


def _parameter_summary(surface: Mapping[str, Any]) -> Dict[str, Any]:
    rows = [row for row in surface.get("parameters", []) if isinstance(row, Mapping)]
    levels: Dict[str, int] = {}
    for row in rows:
        level = str(row.get("level") or "unknown")
        levels[level] = levels.get(level, 0) + 1
    blockers: List[str] = []
    if bool(surface.get("device_write_enabled")):
        blockers.append("parameter_surface_device_write_enabled")
    if not bool(surface.get("high_risk_parameters_hidden_by_default", True)):
        blockers.append("high_risk_parameters_not_hidden")
    return {
        "status": "pass" if not blockers else "fail",
        "blockers": blockers,
        "device_write_enabled": bool(surface.get("device_write_enabled")),
        "high_risk_parameters_hidden_by_default": bool(surface.get("high_risk_parameters_hidden_by_default", True)),
        "visible_parameter_count": len(rows),
        "level_counts": levels,
    }


def _advanced_qc_summary(advanced_qc: Mapping[str, Any]) -> Dict[str, Any]:
    if not advanced_qc:
        return {
            "status": "pending",
            "root_cause_status": "pending",
            "root_cause_codes": [],
            "summary": "高级 QC 结果尚未接入。",
            "blockers": [],
        }
    root = advanced_qc.get("root_cause") if isinstance(advanced_qc.get("root_cause"), Mapping) else advanced_qc
    status = str(root.get("status") or advanced_qc.get("status") or "pending")
    blockers: List[str] = []
    if status in {"block_formal", "reject_point", "fail"}:
        _append_unique(blockers, root.get("root_cause_codes") or root.get("reasons") or [])
    return {
        "status": status,
        "root_cause_status": status,
        "root_cause_codes": list(root.get("root_cause_codes") or []),
        "summary": str(root.get("summary") or advanced_qc.get("summary") or ""),
        "blockers": blockers,
    }


def _evidence_summary(formal_workbench: Mapping[str, Any]) -> Dict[str, Any]:
    cards = _cards_by_key(formal_workbench)
    report = formal_workbench.get("report_summary") if isinstance(formal_workbench.get("report_summary"), Mapping) else {}
    package_status = str(formal_workbench.get("package_status") or "pending")
    preflight_status = str(formal_workbench.get("preflight_status") or "pending")
    release_status = str(report.get("release_status") or (cards.get("report_release") or {}).get("status") or "pending")
    blockers: List[str] = []
    for card in cards.values():
        _append_unique(blockers, card.get("blockers") or [])
    _append_unique(blockers, report.get("reasons") or [])
    _append_unique(blockers, report.get("missing_uncertainty") or [])
    return {
        "status": package_status if package_status != "ready_for_reviewer" else release_status,
        "preflight_status": preflight_status,
        "package_status": package_status,
        "report_release_status": release_status,
        "formal_issue_allowed": bool(report.get("formal_issue_allowed")),
        "blockers": blockers,
        "a_grade_metric": str((cards.get("open_flow_samples") or {}).get("metric") or ""),
    }


def _operation_summary(operation_console: Mapping[str, Any]) -> Dict[str, Any]:
    pages = [page for page in operation_console.get("pages", []) if isinstance(page, Mapping)]
    blockers: List[str] = []
    for page in pages:
        _append_unique(blockers, page.get("blockers") or [])
    return {
        "status": str((operation_console.get("source_workbench") or {}).get("package_status") or "pending"),
        "page_count": len(pages),
        "blocked_pages": [str(page.get("key")) for page in pages if page.get("blockers")],
        "blockers": blockers,
    }


def _section(key: str, title: str, status: str, summary: str, blockers: Sequence[Any]) -> Dict[str, Any]:
    clean_blockers: List[str] = []
    _append_unique(clean_blockers, blockers)
    return {
        "key": key,
        "title": title,
        "status": status,
        "summary": summary,
        "blockers": clean_blockers,
    }


def _overall_status(
    *,
    safety: Mapping[str, Any],
    evidence: Mapping[str, Any],
    parameters: Mapping[str, Any],
    advanced_qc: Mapping[str, Any],
) -> str:
    if safety.get("status") != "pass":
        return "not_releasable"
    if parameters.get("status") != "pass":
        return "blocked"
    if advanced_qc.get("status") in {"block_formal", "reject_point", "fail"}:
        return "blocked"
    if evidence.get("package_status") == "blocked" or evidence.get("preflight_status") == "fail":
        return "blocked"
    release_status = str(evidence.get("report_release_status") or "")
    if release_status in {"formal_release_ready", "review_ready", "draft_only", "not_releasable"}:
        return release_status
    if evidence.get("package_status") == "ready_for_reviewer":
        return "ready_for_reviewer"
    return "pending"


def _next_actions(status: str, sections: Sequence[Mapping[str, Any]]) -> List[str]:
    actions: List[str] = []
    all_blockers: List[str] = []
    for section in sections:
        _append_unique(all_blockers, section.get("blockers") or [])

    if status == "not_releasable":
        actions.append("停止发布，先处理 safety boundary 或高风险写入异常。")
    if "pressure_quick_check_artifact_missing" in all_blockers:
        actions.append("补做或导入压力通道快速验证工件。")
    if any("uncertainty" in blocker for blocker in all_blockers) or status == "draft_only":
        actions.append("补齐并审核 released_uncertainty_inputs，报告才能脱离 draft_only。")
    if status == "review_ready":
        actions.append("提交 reviewer/approver 签字后再进入正式发布。")
    if any("real_moisture_release" in blocker for blocker in all_blockers):
        actions.append("检查气路反湿或样气真实变湿，相关点位不得进入正式拟合。")
    if not actions and status in {"ready_for_reviewer", "formal_release_ready"}:
        actions.append("进入人工评审，确认 A 级样本、报告 hash 和归档证据。")
    if not actions:
        actions.append("补齐缺失证据后重新生成 review surface。")
    return actions


def build_review_surface_model(
    *,
    formal_workbench: Mapping[str, Any] | None = None,
    operation_console: Mapping[str, Any] | None = None,
    parameter_surface: Mapping[str, Any] | None = None,
    advanced_qc: Mapping[str, Any] | None = None,
    role: str = "operator",
) -> Dict[str, Any]:
    """Merge V1.5 review inputs into one static review-surface model."""

    formal = dict(formal_workbench or {})
    operation = dict(operation_console or {})
    if not operation:
        operation = build_operation_console_model(workbench_model=formal, role=role)
    parameters = dict(parameter_surface or build_parameter_surface())
    qc = dict(advanced_qc or {})

    safety = _safety_summary(formal, operation, parameters, qc)
    evidence = _evidence_summary(formal)
    operation_summary = _operation_summary(operation)
    parameter_summary = _parameter_summary(parameters)
    qc_summary = _advanced_qc_summary(qc)

    sections = [
        _section(
            "operation_console",
            "操作流程",
            operation_summary["status"],
            f"{operation_summary['page_count']} 个页面，阻塞页面 {len(operation_summary['blocked_pages'])} 个。",
            operation_summary["blockers"],
        ),
        _section(
            "evidence_chain",
            "证据链",
            evidence["status"],
            (
                f"preflight={evidence['preflight_status']}，package={evidence['package_status']}，"
                f"report={evidence['report_release_status']}，{evidence['a_grade_metric']}"
            ),
            evidence["blockers"],
        ),
        _section(
            "parameters",
            "参数治理",
            parameter_summary["status"],
            (
                f"可见参数 {parameter_summary['visible_parameter_count']} 个，"
                f"高风险默认隐藏={parameter_summary['high_risk_parameters_hidden_by_default']}。"
            ),
            parameter_summary["blockers"],
        ),
        _section(
            "advanced_qc",
            "高级 QC",
            qc_summary["status"],
            qc_summary["summary"],
            qc_summary["blockers"],
        ),
    ]
    overall = _overall_status(
        safety=safety,
        evidence=evidence,
        parameters=parameter_summary,
        advanced_qc=qc_summary,
    )
    return {
        "schema_version": "v1_5_review_surface_v0",
        "generated_at": _now(),
        "role": role,
        "overall_status": overall,
        "sidecar_only": True,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "controls_valves_or_pace": False,
        "writes_coefficients": False,
        "safety_summary": safety,
        "evidence_summary": evidence,
        "operation_summary": operation_summary,
        "parameter_summary": parameter_summary,
        "advanced_qc_summary": qc_summary,
        "sections": sections,
        "next_actions": _next_actions(overall, sections),
        "formal_scope": {
            "main_calibration": "open_flow_current_atmosphere_co2_h2o",
            "pressure_channel": "independent_validation",
            "pressure_compensation": "optional_downstream_validation",
            "sealed_pressure_points": "diagnostic_only_by_default",
        },
    }


def _esc(value: Any) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def _badge(status: Any) -> str:
    text = str(status or "pending")
    tone = "ok" if text in {"pass", "ready_for_reviewer", "formal_release_ready"} else "warn"
    if text in {"blocked", "fail", "not_releasable", "reject_point", "block_formal"}:
        tone = "bad"
    return f'<span class="badge {tone}">{_esc(text)}</span>'


def render_review_surface_html(model: Mapping[str, Any]) -> str:
    sections = "\n".join(
        f"""
        <section class="card">
          <div class="card-head"><h2>{_esc(section.get('title'))}</h2>{_badge(section.get('status'))}</div>
          <p>{_esc(section.get('summary'))}</p>
          <div class="blockers">{_esc('; '.join(section.get('blockers') or []) or '无')}</div>
        </section>
        """
        for section in model.get("sections", [])
    )
    actions = "\n".join(f"<li>{_esc(item)}</li>" for item in model.get("next_actions", []))
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <link rel="icon" href="data:," />
  <title>V1.5 正式校准 Review Surface</title>
  <style>
    body {{ margin: 0; font: 14px/1.5 "Microsoft YaHei", "Segoe UI", Arial, sans-serif; background: #f6f8fb; color: #172033; }}
    main {{ max-width: 1400px; margin: 0 auto; padding: 24px; }}
    header {{ display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 16px; border-bottom: 1px solid #d9e0ea; padding-bottom: 18px; }}
    h1 {{ margin: 0 0 8px; font-size: 28px; letter-spacing: 0; }}
    h2 {{ margin: 0; font-size: 16px; letter-spacing: 0; }}
    .sub {{ color: #667085; margin: 0; }}
    .boundary {{ background: #fff; border: 1px solid #d9e0ea; border-radius: 8px; padding: 12px; min-width: 360px; }}
    .flags {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 6px; color: #667085; }}
    .status {{ font-size: 18px; font-weight: 700; }}
    .grid {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin-top: 20px; }}
    .card {{ background: #fff; border: 1px solid #d9e0ea; border-radius: 8px; padding: 14px; min-height: 150px; }}
    .card-head {{ display: flex; justify-content: space-between; gap: 8px; align-items: start; }}
    .badge {{ display: inline-flex; padding: 2px 8px; border-radius: 999px; background: #eef2f6; color: #475467; white-space: nowrap; }}
    .badge.ok {{ color: #0f7a48; background: #eaf7f0; }}
    .badge.warn {{ color: #a15c00; background: #fff5df; }}
    .badge.bad {{ color: #b42318; background: #fff0ee; }}
    .blockers {{ color: #b42318; font-size: 12px; word-break: break-word; }}
    .actions {{ background: #fff; border: 1px solid #d9e0ea; border-radius: 8px; padding: 14px; margin-top: 20px; }}
    @media (max-width: 1100px) {{ header {{ grid-template-columns: 1fr; }} .grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }} .boundary {{ min-width: 0; }} }}
    @media (max-width: 680px) {{ main {{ padding: 14px; }} .grid {{ grid-template-columns: 1fr; }} }}
  </style>
</head>
<body>
<main>
  <header>
    <div>
      <h1>V1.5 正式校准 Review Surface</h1>
      <p class="sub">统一汇总操作流程、证据链、参数治理、高级 QC 与报告发布门禁。</p>
      <p class="status">总体状态：{_esc(model.get('overall_status'))}</p>
    </div>
    <aside class="boundary">
      <strong>安全边界</strong>
      <div class="flags">
        <span>COM: {_esc(model.get('opens_com_ports'))}</span>
        <span>水路/气路: {_esc(model.get('controls_water_or_gas_routes'))}</span>
        <span>阀/PACE: {_esc(model.get('controls_valves_or_pace'))}</span>
        <span>SENCO: {_esc(model.get('writes_coefficients'))}</span>
      </div>
    </aside>
  </header>
  <div class="grid">{sections}</div>
  <section class="actions">
    <h2>下一步建议</h2>
    <ol>{actions}</ol>
  </section>
</main>
</body>
</html>
"""


def render_review_surface_markdown(model: Mapping[str, Any]) -> str:
    lines = [
        "# V1.5 正式校准 Review Surface",
        "",
        f"- overall_status: {model.get('overall_status')}",
        f"- opens_com_ports: {model.get('opens_com_ports')}",
        f"- controls_water_or_gas_routes: {model.get('controls_water_or_gas_routes')}",
        f"- controls_valves_or_pace: {model.get('controls_valves_or_pace')}",
        f"- writes_coefficients: {model.get('writes_coefficients')}",
        "",
        "## Sections",
    ]
    for section in model.get("sections", []):
        lines.append(
            f"- {section.get('title')}: {section.get('status')} | {section.get('summary')} | "
            f"{';'.join(section.get('blockers') or []) or 'no blockers'}"
        )
    lines.extend(["", "## Next Actions"])
    for action in model.get("next_actions", []):
        lines.append(f"- {action}")
    return "\n".join(lines) + "\n"


def write_review_surface(
    *,
    output_dir: str | Path,
    formal_workbench: Mapping[str, Any] | None = None,
    operation_console: Mapping[str, Any] | None = None,
    parameter_surface: Mapping[str, Any] | None = None,
    advanced_qc: Mapping[str, Any] | None = None,
    role: str = "operator",
) -> Dict[str, Path]:
    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    model = build_review_surface_model(
        formal_workbench=formal_workbench,
        operation_console=operation_console,
        parameter_surface=parameter_surface,
        advanced_qc=advanced_qc,
        role=role,
    )
    json_path = root / "v1_5_review_surface.json"
    html_path = root / "v1_5_review_surface.html"
    md_path = root / "v1_5_review_surface.md"
    json_path.write_text(json.dumps(model, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    html_path.write_text(render_review_surface_html(model), encoding="utf-8")
    md_path.write_text(render_review_surface_markdown(model), encoding="utf-8")
    return {"model": json_path, "html": html_path, "markdown": md_path}
