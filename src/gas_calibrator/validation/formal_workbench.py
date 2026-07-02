"""Offline V1.5 formal calibration evidence workbench.

The workbench is a static reviewer/operator surface. It only reads files and
builds HTML/JSON/Markdown summaries; it does not open COM ports, control
water/gas routes, control valves/PACE, or write analyzer coefficients.
"""

from __future__ import annotations

import html
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

from .formal_calibration_package import build_formal_calibration_package_tables
from .formal_contracts import (
    validate_formal_plan_contract,
    validate_pressure_reference_contract,
)
from .formal_open_flow_artifacts import load_plan_snapshot, load_pressure_reference_snapshot
from .formal_preflight import build_formal_preflight_tables
from .formal_reports import build_report_model_from_bundle


TEXT = {
    "title": "V1.5 正式校准证据工作台",
    "subtitle": "离线 sidecar / no-write / 不控制水路气路",
    "boundary": "安全边界",
    "workflow": "正式运行顺序",
    "cards": "证据状态",
    "blockers": "阻塞原因",
    "artifacts": "关键工件",
    "preflight": "预检明细",
    "candidate": "候选系数评审",
    "report": "报告发布门禁",
    "sidecar_note": "本工作台只读已有文件，不打开 COM，不控制 PACE/阀，不切换水路或气路，不写 SENCO。",
    "open_flow_scope": "正式 CO2/H2O 主校准范围为当前大气压附近开放流通样气。",
    "diagnostic_exclusion": "封路压力点、动态控压、PACE continuous sink、VENT-hold 默认仅为工程诊断。",
}

STATUS_LABELS = {
    "pass": "通过",
    "ready_for_reviewer": "可进入评审",
    "formal_release_ready": "可正式发布",
    "review_ready": "待签字",
    "draft_only": "草稿",
    "pending": "待补齐",
    "missing": "缺失",
    "blocked": "阻塞",
    "fail": "失败",
    "not_releasable": "不可发布",
    "not_run": "未运行",
    "skipped": "跳过",
}

STATUS_TONE = {
    "pass": "ok",
    "ready_for_reviewer": "ok",
    "formal_release_ready": "ok",
    "review_ready": "warn",
    "draft_only": "warn",
    "pending": "warn",
    "missing": "bad",
    "blocked": "bad",
    "fail": "bad",
    "not_releasable": "bad",
    "not_run": "muted",
    "skipped": "muted",
}

FORMAL_ORDER = [
    ("PRECHECK", "设备预检"),
    ("PRESSURE_CHANNEL_QUICK_CHECK", "压力通道快速验证"),
    ("OPEN_FLOW_PURGE", "开放流通吹扫"),
    ("STABILITY_GATE", "稳定性门禁"),
    ("SAMPLE_WINDOW", "正式采样窗口"),
    ("QC_AND_REPORT", "QC 与报告"),
    ("CANDIDATE_REVIEW", "候选系数评审"),
]


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _load_json(path: str | Path | None) -> Optional[Dict[str, Any]]:
    if not path:
        return None
    source = Path(path)
    if not source.exists():
        return None
    payload = json.loads(source.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {"payload": payload}


def _safe_first(rows: Sequence[Mapping[str, Any]] | None) -> Dict[str, Any]:
    if rows:
        return dict(rows[0])
    return {}


def _split_reasons(value: Any) -> List[str]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    return [item for item in str(value).split(";") if item]


def _status_label(status: Any) -> str:
    text = str(status or "pending")
    return STATUS_LABELS.get(text, text)


def _tone(status: Any) -> str:
    return STATUS_TONE.get(str(status or "pending"), "muted")


def _path_exists(path: str | Path | None) -> bool:
    return bool(path) and Path(path).exists()


def _check_template_file(package_dir: Optional[Path], filename: str) -> Dict[str, Any]:
    path = package_dir / filename if package_dir else None
    exists = _path_exists(path)
    return {
        "filename": filename,
        "path": str(path) if path else "",
        "exists": exists,
        "status": "pass" if exists else "missing",
    }


def _card(
    key: str,
    title: str,
    status: str,
    detail: str = "",
    *,
    blockers: Optional[Sequence[str]] = None,
    path: str = "",
    metric: str = "",
) -> Dict[str, Any]:
    return {
        "key": key,
        "title": title,
        "status": status,
        "status_label": _status_label(status),
        "tone": _tone(status),
        "detail": detail,
        "blockers": list(blockers or []),
        "path": path,
        "metric": metric,
    }


def _preflight_check_map(tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> Dict[str, Dict[str, Any]]:
    return {
        str(row.get("check") or ""): dict(row)
        for row in tables.get("preflight_checks", [])
        if isinstance(row, Mapping)
    }


def _artifact_rows(
    *,
    package_dir: Optional[Path],
    run_dir: Optional[Path],
    plan_path: str | Path | None,
    pressure_reference_path: str | Path | None,
    config_path: str | Path | None,
    evidence_bundle_path: str | Path | None,
    report_model_path: str | Path | None,
    sidecar_summary_path: str | Path | None,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for label, path in (
        ("运行目录", run_dir),
        ("正式计划快照", plan_path),
        ("COM22 压力参考", pressure_reference_path),
        ("运行配置", config_path),
        ("证据包", evidence_bundle_path),
        ("报告模型", report_model_path),
        ("sidecar 摘要", sidecar_summary_path),
    ):
        rows.append(
            {
                "label": label,
                "path": str(path) if path else "",
                "status": "pass" if _path_exists(path) else "missing",
            }
        )
    for filename in (
        "formal_plan_snapshot_template.json",
        "standard_gases_template.json",
        "com22_pressure_reference_template.json",
        "released_uncertainty_inputs_template.json",
        "v1_5_formal_no_write_runbook.md",
    ):
        rows.append(_check_template_file(package_dir, filename) | {"label": filename})
    return rows


def _build_report_summary(
    *,
    evidence_bundle_path: str | Path | None,
    report_model_path: str | Path | None,
    uncertainty_json: str | Path | None,
    reviewer: str,
    approver: str,
    analyzer_prefix: str,
) -> Dict[str, Any]:
    if report_model_path and Path(report_model_path).exists():
        model = _load_json(report_model_path) or {}
    elif evidence_bundle_path and Path(evidence_bundle_path).exists():
        bundle = _load_json(evidence_bundle_path) or {}
        uncertainty_payload = _load_json(uncertainty_json) or {}
        model = build_report_model_from_bundle(
            bundle,
            reviewer=reviewer,
            approver=approver,
            analyzer_prefix=analyzer_prefix,
            uncertainty_payload=uncertainty_payload,
        )
    else:
        return {
            "status": "pending",
            "release_status": "pending",
            "formal_issue_allowed": False,
            "reasons": ["evidence_bundle_or_report_model_missing"],
            "result_rows": [],
            "uncertainty_status": "pending",
        }

    release = dict(model.get("report_release_decision") or {})
    uncertainty = dict(model.get("uncertainty_summary") or {})
    return {
        "status": str(release.get("release_status") or "pending"),
        "release_status": str(release.get("release_status") or "pending"),
        "formal_issue_allowed": bool(release.get("formal_issue_allowed")),
        "reasons": list(release.get("reasons") or []),
        "result_rows": list(model.get("result_rows") or []),
        "uncertainty_status": str(uncertainty.get("status") or "pending"),
        "missing_uncertainty": list(uncertainty.get("missing_required") or []),
        "report_no": str(model.get("report_no") or ""),
    }


def build_formal_workbench_model(
    *,
    output_dir: str | Path | None = None,
    run_dir: str | Path | None = None,
    plan_path: str | Path | None = None,
    pressure_reference_path: str | Path | None = None,
    config_path: str | Path | None = None,
    evidence_bundle_path: str | Path | None = None,
    report_model_path: str | Path | None = None,
    uncertainty_json: str | Path | None = None,
    sidecar_summary_path: str | Path | None = None,
    package_dir: str | Path | None = None,
    component: str = "both",
    analyzer_prefix: str = "ga01",
    reviewer: str = "",
    approver: str = "",
    today: Any = None,
) -> Dict[str, Any]:
    """Build the static workbench model from existing evidence files."""

    root = Path(run_dir).resolve() if run_dir else None
    package_root = Path(package_dir).resolve() if package_dir else None
    destination = Path(output_dir).resolve() if output_dir else None

    preflight_tables: Dict[str, List[Dict[str, Any]]] = {}
    preflight_context: Dict[str, Any] = {}
    preflight_error = ""
    if root and plan_path and pressure_reference_path:
        try:
            preflight_tables, preflight_context = build_formal_preflight_tables(
                run_dir=root,
                plan_path=plan_path,
                pressure_reference_path=pressure_reference_path,
                config_path=config_path,
                component=component,
                analyzer_prefix=analyzer_prefix,
                today=today,
            )
        except Exception as exc:
            preflight_error = str(exc)

    package_tables: Dict[str, List[Dict[str, Any]]] = {}
    package_error = ""
    if root and plan_path and pressure_reference_path:
        try:
            plan = load_plan_snapshot(plan_path)
            pressure_reference = load_pressure_reference_snapshot(pressure_reference_path)
            package_tables, _ = build_formal_calibration_package_tables(
                run_dir=root,
                plan=plan,
                pressure_reference=pressure_reference,
                component=component,
                analyzer_prefix=analyzer_prefix,
                today=today,
            )
        except Exception as exc:
            package_error = str(exc)

    plan_check = None
    plan = _load_json(plan_path)
    if plan is not None:
        result = validate_formal_plan_contract(plan, today=today)
        plan_check = {"status": result.status, "reasons": result.reasons}

    pressure_reference_check = None
    reference = _load_json(pressure_reference_path)
    if reference is not None:
        result = validate_pressure_reference_contract(reference, today=today)
        pressure_reference_check = {"status": result.status, "reasons": result.reasons}

    checks = _preflight_check_map(preflight_tables)
    preflight_summary = _safe_first(preflight_tables.get("preflight_summary"))
    package_summary = _safe_first(package_tables.get("package_summary"))
    open_summary = package_tables.get("open_flow_run_summary", [])
    pressure_summary = _safe_first(package_tables.get("pressure_validation_summary"))
    candidate_rows = [dict(row) for row in package_tables.get("candidate_coefficient_review", [])]
    sidecar_summary = _load_json(sidecar_summary_path) or {}
    report_summary = _build_report_summary(
        evidence_bundle_path=evidence_bundle_path,
        report_model_path=report_model_path,
        uncertainty_json=uncertainty_json,
        reviewer=reviewer,
        approver=approver,
        analyzer_prefix=analyzer_prefix,
    )

    pressure_check = checks.get("pressure_quick_check_contract", {})
    samples_check = checks.get("samples_artifact", {})
    no_write_check = checks.get("no_write_config", {})
    package_status = str(package_summary.get("package_status") or ("not_run" if not package_error else "fail"))
    package_blockers = _split_reasons(package_summary.get("package_blockers")) + ([package_error] if package_error else [])
    preflight_status = str(preflight_summary.get("preflight_status") or ("not_run" if not preflight_error else "fail"))

    a_grade_count = sum(int(row.get("a_grade_count") or 0) for row in open_summary)
    rejected_count = sum(int(row.get("rejected_count") or 0) for row in open_summary)
    candidate_blockers = [
        blocker
        for row in candidate_rows
        for blocker in _split_reasons(row.get("blockers"))
    ]

    cards = [
        _card(
            "sidecar_boundary",
            "sidecar / no-write 边界",
            "pass",
            TEXT["sidecar_note"],
        ),
        _card(
            "formal_plan",
            "正式计划与标准气",
            str((plan_check or {}).get("status") or ("pass" if _path_exists(plan_path) else "missing")),
            "标准气证书、计划版本、配置 hash 与 allow_device_write=false。",
            blockers=list((plan_check or {}).get("reasons") or []),
            path=str(plan_path or ""),
        ),
        _card(
            "pressure_reference",
            "COM22 压力参考溯源",
            str((pressure_reference_check or {}).get("status") or ("pass" if _path_exists(pressure_reference_path) else "missing")),
            "压力通道验证的主参考，证书有效性决定是否可作为正式证据。",
            blockers=list((pressure_reference_check or {}).get("reasons") or []),
            path=str(pressure_reference_path or ""),
        ),
        _card(
            "no_write_config",
            "no-write 配置",
            str(no_write_check.get("status") or ("pending" if config_path else "missing")),
            "确认不启用系数写入、设备写入或静态 SENCO。",
            blockers=_split_reasons(no_write_check.get("reasons")),
            path=str(config_path or ""),
        ),
        _card(
            "pressure_quick_check",
            "压力通道快速验证",
            str(pressure_check.get("status") or ("pending" if root else "missing")),
            f"验证对象为分析仪内部 P；状态: {pressure_summary.get('status', 'pending')}",
            blockers=_split_reasons(pressure_check.get("reasons")),
            path=str(pressure_check.get("path") or ""),
        ),
        _card(
            "open_flow_samples",
            "开放流通 CO2/H2O 样本",
            str(samples_check.get("status") or ("pending" if root else "missing")),
            TEXT["open_flow_scope"],
            path=str(samples_check.get("path") or ""),
            metric=f"A 级 {a_grade_count} / 拒绝 {rejected_count}",
        ),
        _card(
            "qc_package",
            "QC 与正式证据包",
            "ready_for_reviewer" if package_status == "ready_for_reviewer" else package_status,
            "只允许 A 级开放流通样本进入候选系数评审。",
            blockers=package_blockers,
            metric=f"候选点 {len(candidate_rows)}",
        ),
        _card(
            "candidate_review",
            "候选系数评审",
            "ready_for_reviewer" if candidate_rows and not candidate_blockers else ("blocked" if candidate_blockers else "pending"),
            "候选系数只供评审，不自动写入设备。",
            blockers=candidate_blockers,
        ),
        _card(
            "report_release",
            "校准报告发布",
            str(report_summary.get("release_status") or "pending"),
            f"不确定度状态: {report_summary.get('uncertainty_status', 'pending')}",
            blockers=list(report_summary.get("reasons") or []) + list(report_summary.get("missing_uncertainty") or [])[:5],
            metric=f"结果行 {len(report_summary.get('result_rows') or [])}",
        ),
        _card(
            "database_import",
            "数据库索引",
            "pass" if sidecar_summary.get("database_imported") else "pending",
            "数据库只索引证据链，不是 real acceptance。",
            blockers=[] if sidecar_summary.get("database_imported") else ["database_import_not_run"],
        ),
    ]

    workflow_steps = []
    status_by_step = {
        "PRECHECK": preflight_status,
        "PRESSURE_CHANNEL_QUICK_CHECK": str(pressure_check.get("status") or "pending"),
        "OPEN_FLOW_PURGE": str(samples_check.get("status") or "pending"),
        "STABILITY_GATE": "pass" if a_grade_count else ("pending" if root else "missing"),
        "SAMPLE_WINDOW": "pass" if a_grade_count else ("pending" if root else "missing"),
        "QC_AND_REPORT": "ready_for_reviewer" if package_status == "ready_for_reviewer" else package_status,
        "CANDIDATE_REVIEW": "ready_for_reviewer" if candidate_rows and not candidate_blockers else ("blocked" if candidate_blockers else "pending"),
    }
    for key, label in FORMAL_ORDER:
        status = status_by_step.get(key, "pending")
        workflow_steps.append(
            {
                "key": key,
                "label": label,
                "status": status,
                "status_label": _status_label(status),
                "tone": _tone(status),
            }
        )

    model = {
        "schema_version": "v1_5_formal_workbench_v0",
        "generated_at": _now(),
        "title": TEXT["title"],
        "subtitle": TEXT["subtitle"],
        "run_dir": str(root) if root else "",
        "output_dir": str(destination) if destination else "",
        "component": component,
        "analyzer_prefix": analyzer_prefix,
        "sidecar_only": True,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "controls_valves_or_pace": False,
        "writes_coefficients": False,
        "formal_scope": {
            "open_flow_main_calibration": True,
            "sealed_pressure_points_formal_fit_default": False,
            "dynamic_pressure_control_formal_fit_default": False,
            "diagnostic_note": TEXT["diagnostic_exclusion"],
        },
        "workflow_steps": workflow_steps,
        "cards": cards,
        "preflight_status": preflight_status,
        "preflight_error": preflight_error,
        "preflight_checks": list(preflight_tables.get("preflight_checks", [])),
        "package_status": package_status,
        "package_error": package_error,
        "candidate_rows": candidate_rows,
        "open_flow_summary": [dict(row) for row in open_summary],
        "pressure_summary": pressure_summary,
        "report_summary": report_summary,
        "artifact_rows": _artifact_rows(
            package_dir=package_root,
            run_dir=root,
            plan_path=plan_path,
            pressure_reference_path=pressure_reference_path,
            config_path=config_path,
            evidence_bundle_path=evidence_bundle_path,
            report_model_path=report_model_path,
            sidecar_summary_path=sidecar_summary_path,
        ),
    }
    return model


def _html_escape(value: Any) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def _render_badges(items: Sequence[str]) -> str:
    if not items:
        return '<span class="muted">无</span>'
    return "".join(f"<span class=\"reason\">{_html_escape(item)}</span>" for item in items[:12])


def render_formal_workbench_html(model: Mapping[str, Any]) -> str:
    cards = "".join(
        f"""
        <section class="card { _html_escape(card.get('tone')) }">
          <div class="card-head">
            <h3>{_html_escape(card.get('title'))}</h3>
            <span class="badge { _html_escape(card.get('tone')) }">{_html_escape(card.get('status_label'))}</span>
          </div>
          <p>{_html_escape(card.get('detail'))}</p>
          <div class="metric">{_html_escape(card.get('metric'))}</div>
          <div class="reasons">{_render_badges(list(card.get('blockers') or []))}</div>
          <div class="path">{_html_escape(card.get('path'))}</div>
        </section>
        """
        for card in model.get("cards", [])
    )
    steps = "".join(
        f"""
        <li class="{_html_escape(step.get('tone'))}">
          <span class="step-key">{_html_escape(step.get('key'))}</span>
          <span>{_html_escape(step.get('label'))}</span>
          <strong>{_html_escape(step.get('status_label'))}</strong>
        </li>
        """
        for step in model.get("workflow_steps", [])
    )
    artifacts = "".join(
        f"""
        <tr>
          <td>{_html_escape(row.get('label') or row.get('filename'))}</td>
          <td><span class="badge {_html_escape(_tone(row.get('status')))}">{_html_escape(_status_label(row.get('status')))}</span></td>
          <td>{_html_escape(row.get('path'))}</td>
        </tr>
        """
        for row in model.get("artifact_rows", [])
    )
    candidates = "".join(
        f"""
        <tr>
          <td>{_html_escape(row.get('component'))}</td>
          <td>{_html_escape(row.get('candidate_review_status'))}</td>
          <td>{_html_escape(row.get('a_grade_count'))}</td>
          <td>{_html_escape(row.get('b_grade_count'))}</td>
          <td>{_html_escape(row.get('rejected_count'))}</td>
          <td>{_html_escape(row.get('blockers'))}</td>
        </tr>
        """
        for row in model.get("candidate_rows", [])
    )
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <link rel="icon" href="data:," />
  <title>{_html_escape(model.get('title'))}</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f6f8fb;
      --panel: #ffffff;
      --text: #172033;
      --muted: #667085;
      --line: #d9e0ea;
      --ok: #0f7a48;
      --warn: #a15c00;
      --bad: #b42318;
      --neutral: #475467;
      --ok-bg: #eaf7f0;
      --warn-bg: #fff5df;
      --bad-bg: #fff0ee;
      --neutral-bg: #eef2f6;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font: 14px/1.5 "Microsoft YaHei", "Segoe UI", Arial, sans-serif;
      color: var(--text);
      background: var(--bg);
    }}
    .page {{ max-width: 1480px; margin: 0 auto; padding: 24px; }}
    header {{
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 16px;
      align-items: start;
      padding: 20px 0 18px;
      border-bottom: 1px solid var(--line);
    }}
    h1 {{ font-size: 28px; margin: 0 0 6px; letter-spacing: 0; }}
    h2 {{ font-size: 18px; margin: 24px 0 12px; letter-spacing: 0; }}
    h3 {{ font-size: 15px; margin: 0; letter-spacing: 0; }}
    .subtitle {{ color: var(--muted); margin: 0; }}
    .boundary {{
      min-width: 320px;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
    }}
    .boundary b {{ display: block; margin-bottom: 6px; }}
    .flags {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 6px;
      color: var(--muted);
    }}
    .workflow {{
      display: grid;
      grid-template-columns: repeat(7, minmax(120px, 1fr));
      gap: 8px;
      list-style: none;
      padding: 0;
      margin: 0;
    }}
    .workflow li {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px;
      min-height: 88px;
    }}
    .workflow li.ok {{ border-color: #9dd7b7; }}
    .workflow li.warn {{ border-color: #e8c878; }}
    .workflow li.bad {{ border-color: #f3b2aa; }}
    .step-key {{ display: block; color: var(--muted); font-size: 11px; word-break: break-word; }}
    .workflow strong {{ display: block; margin-top: 6px; }}
    .cards {{
      display: grid;
      grid-template-columns: repeat(5, minmax(190px, 1fr));
      gap: 12px;
    }}
    .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 14px;
      min-height: 178px;
      overflow: hidden;
    }}
    .card.ok {{ border-color: #9dd7b7; }}
    .card.warn {{ border-color: #e8c878; }}
    .card.bad {{ border-color: #f3b2aa; }}
    .card-head {{ display: flex; justify-content: space-between; gap: 8px; align-items: start; }}
    .badge {{
      display: inline-flex;
      align-items: center;
      min-height: 24px;
      padding: 2px 8px;
      border-radius: 999px;
      white-space: nowrap;
      font-size: 12px;
      color: var(--neutral);
      background: var(--neutral-bg);
    }}
    .badge.ok {{ color: var(--ok); background: var(--ok-bg); }}
    .badge.warn {{ color: var(--warn); background: var(--warn-bg); }}
    .badge.bad {{ color: var(--bad); background: var(--bad-bg); }}
    .card p {{ color: var(--muted); min-height: 42px; margin: 10px 0; }}
    .metric {{ font-weight: 600; min-height: 22px; }}
    .reasons {{ display: flex; flex-wrap: wrap; gap: 4px; min-height: 28px; margin-top: 8px; }}
    .reason {{ font-size: 12px; padding: 2px 6px; border-radius: 6px; background: var(--bad-bg); color: var(--bad); }}
    .muted {{ color: var(--muted); }}
    .path {{ color: var(--muted); font-size: 12px; word-break: break-all; margin-top: 8px; }}
    table {{ width: 100%; border-collapse: collapse; background: var(--panel); border: 1px solid var(--line); border-radius: 8px; overflow: hidden; }}
    th, td {{ text-align: left; border-bottom: 1px solid var(--line); padding: 8px 10px; vertical-align: top; word-break: break-word; }}
    th {{ background: #eef2f6; font-weight: 700; }}
    tr:last-child td {{ border-bottom: 0; }}
    .note {{ margin: 18px 0; color: var(--muted); }}
    @media (max-width: 1180px) {{
      header {{ grid-template-columns: 1fr; }}
      .cards {{ grid-template-columns: repeat(2, minmax(220px, 1fr)); }}
      .workflow {{ grid-template-columns: repeat(2, minmax(160px, 1fr)); }}
    }}
    @media (max-width: 720px) {{
      .page {{ padding: 14px; }}
      .cards, .workflow {{ grid-template-columns: 1fr; }}
      .boundary {{ min-width: 0; }}
    }}
  </style>
</head>
<body>
  <main class="page">
    <header>
      <div>
        <h1>{_html_escape(model.get('title'))}</h1>
        <p class="subtitle">{_html_escape(model.get('subtitle'))}</p>
        <p class="note">{_html_escape(TEXT['open_flow_scope'])} {_html_escape(TEXT['diagnostic_exclusion'])}</p>
      </div>
      <aside class="boundary">
        <b>{_html_escape(TEXT['boundary'])}</b>
        <div class="flags">
          <span>COM: {_html_escape(model.get('opens_com_ports'))}</span>
          <span>水路/气路: {_html_escape(model.get('controls_water_or_gas_routes'))}</span>
          <span>阀/PACE: {_html_escape(model.get('controls_valves_or_pace'))}</span>
          <span>SENCO: {_html_escape(model.get('writes_coefficients'))}</span>
        </div>
      </aside>
    </header>
    <h2>{_html_escape(TEXT['workflow'])}</h2>
    <ol class="workflow">{steps}</ol>
    <h2>{_html_escape(TEXT['cards'])}</h2>
    <div class="cards">{cards}</div>
    <h2>{_html_escape(TEXT['candidate'])}</h2>
    <table>
      <thead><tr><th>组分</th><th>状态</th><th>A 级</th><th>B 级</th><th>拒绝</th><th>阻塞原因</th></tr></thead>
      <tbody>{candidates or '<tr><td colspan="6" class="muted">暂无候选系数评审数据</td></tr>'}</tbody>
    </table>
    <h2>{_html_escape(TEXT['artifacts'])}</h2>
    <table>
      <thead><tr><th>工件</th><th>状态</th><th>路径</th></tr></thead>
      <tbody>{artifacts}</tbody>
    </table>
  </main>
</body>
</html>
"""


def render_formal_workbench_markdown(model: Mapping[str, Any]) -> str:
    lines = [
        f"# {model.get('title')}",
        "",
        str(model.get("subtitle") or ""),
        "",
        f"- sidecar_only: {model.get('sidecar_only')}",
        f"- opens_com_ports: {model.get('opens_com_ports')}",
        f"- controls_water_or_gas_routes: {model.get('controls_water_or_gas_routes')}",
        f"- controls_valves_or_pace: {model.get('controls_valves_or_pace')}",
        f"- writes_coefficients: {model.get('writes_coefficients')}",
        "",
        "## 流程状态",
    ]
    for step in model.get("workflow_steps", []):
        lines.append(f"- {step.get('label')} ({step.get('key')}): {step.get('status_label')}")
    lines.extend(["", "## 证据状态"])
    for card in model.get("cards", []):
        blockers = ";".join(card.get("blockers") or []) or "无"
        lines.append(f"- {card.get('title')}: {card.get('status_label')} | {card.get('detail')} | {blockers}")
    return "\n".join(lines) + "\n"


def write_formal_workbench(
    *,
    output_dir: str | Path,
    run_dir: str | Path | None = None,
    plan_path: str | Path | None = None,
    pressure_reference_path: str | Path | None = None,
    config_path: str | Path | None = None,
    evidence_bundle_path: str | Path | None = None,
    report_model_path: str | Path | None = None,
    uncertainty_json: str | Path | None = None,
    sidecar_summary_path: str | Path | None = None,
    package_dir: str | Path | None = None,
    component: str = "both",
    analyzer_prefix: str = "ga01",
    reviewer: str = "",
    approver: str = "",
    today: Any = None,
) -> Dict[str, Path]:
    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    model = build_formal_workbench_model(
        output_dir=root,
        run_dir=run_dir,
        plan_path=plan_path,
        pressure_reference_path=pressure_reference_path,
        config_path=config_path,
        evidence_bundle_path=evidence_bundle_path,
        report_model_path=report_model_path,
        uncertainty_json=uncertainty_json,
        sidecar_summary_path=sidecar_summary_path,
        package_dir=package_dir,
        component=component,
        analyzer_prefix=analyzer_prefix,
        reviewer=reviewer,
        approver=approver,
        today=today,
    )
    model_path = root / "v1_5_formal_workbench.json"
    html_path = root / "v1_5_formal_workbench.html"
    markdown_path = root / "v1_5_formal_workbench.md"
    model_path.write_text(json.dumps(model, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    html_path.write_text(render_formal_workbench_html(model), encoding="utf-8")
    markdown_path.write_text(render_formal_workbench_markdown(model), encoding="utf-8")
    return {
        "model": model_path,
        "html": html_path,
        "markdown": markdown_path,
    }
