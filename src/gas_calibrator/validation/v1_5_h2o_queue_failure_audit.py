"""Offline audit helpers for V1.5 formal H2O queue point failures.

The audit is intentionally log-based so an interrupted or already-finished
queue can be diagnosed without reopening real COM ports, valves, PACE, or the
humidity generator.
"""

from __future__ import annotations

import csv
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence


_FLOAT_PATTERN = r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?"
_KEY_VALUE_FLOAT = re.compile(
    rf"(?P<key>dewpoint|time_to_gate|tail_span_60s|tail_slope_60s)="
    rf"(?P<value>{_FLOAT_PATTERN}|None)"
)


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "none":
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _short_reason(text: str, *, limit: int = 500) -> str:
    text = " ".join(str(text or "").split())
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def classify_point_failure_from_log(point_log_path: Path) -> Dict[str, str]:
    """Classify one H2O point failure from its point log only."""

    result = {
        "failure_category": "subprocess_failed",
        "failure_reason": "",
    }
    try:
        text = Path(point_log_path).read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        result["failure_category"] = "log_unavailable"
        result["failure_reason"] = f"log_unavailable:{exc}"
        return result

    lower = text.lower()
    reason_candidates: List[str] = []
    for line in text.splitlines():
        if "reason=" in line:
            reason_candidates.append(line.split("reason=", 1)[1].strip())
        elif "reasons=" in line:
            reason_candidates.append(line.split("reasons=", 1)[1].strip())
    if reason_candidates:
        result["failure_reason"] = _short_reason(reason_candidates[-1])

    if (
        "humidity_generator_gate_failed" in lower
        or "humidity-generator gate failed" in lower
        or "h2o open-flow humidity-generator gate failed" in lower
        or "humidity generator prewarm failed" in lower
    ):
        result["failure_category"] = "hgen_not_ready"
    elif "dewpoint_meter_ready_check_failed" in lower or "dewpoint meter ready check failed" in lower:
        result["failure_category"] = "dewpoint_reference_unavailable"
    elif "severe_humidity_reference_contradiction" in lower:
        result["failure_category"] = "humidity_reference_contradiction"
    elif "dewpoint_alignment_gate_failed" in lower or "strict alignment gate failed" in lower:
        result["failure_category"] = "hgen_dewpoint_alignment"
    elif "dewpoint_rebound_detected" in lower:
        result["failure_category"] = "dewpoint_rebound"
    elif "dewpoint_tail_reference_missing" in lower or "dewpoint_tail_reference_not_dry_enough" in lower:
        result["failure_category"] = "dewpoint_reference_mismatch"
    elif (
        "dewpoint_tail_span_too_large" in lower
        or "dewpoint_tail_slope_too_large" in lower
        or "dewpoint gate timeout" in lower
        or "route dewpoint gate failed" in lower
    ):
        result["failure_category"] = "dewpoint_unstable"
    elif "analyzer_h2o_signal_gate_failed" in lower or "h2o signal gate failed" in lower:
        result["failure_category"] = "h2o_signal_gate"
    elif (
        "h2o precondition analyzer stability gate failed" in lower
        or ("h2o_ratio" in lower and "result=fail" in lower)
        or "h2o ratio" in lower and "unstable" in lower
    ):
        result["failure_category"] = "h2o_ratio_unstable"
    elif "analyzer_pressure_presample_gate_failed" in lower or "analyzer internal pressure gate failed" in lower:
        result["failure_category"] = "pressure_diagnostic_unstable"
    elif "mode2 not ready" in lower or "startup_mode2_verify_failed" in lower:
        result["failure_category"] = "analyzer_startup_mode2"
    elif "relay" in lower and "unavailable" in lower:
        result["failure_category"] = "route_readiness"

    if not result["failure_reason"]:
        tail_lines = [line.strip() for line in text.splitlines()[-8:] if line.strip()]
        result["failure_reason"] = _short_reason(" | ".join(tail_lines))
    return result


def _parse_dewpoint_wait_line(line: str) -> Dict[str, Optional[float]]:
    parsed: Dict[str, Optional[float]] = {
        "dewpoint_c": None,
        "time_to_gate_s": None,
        "tail_span_60s_c": None,
        "tail_slope_60s_c_per_s": None,
    }
    for match in _KEY_VALUE_FLOAT.finditer(line):
        key = match.group("key")
        value = _safe_float(match.group("value"))
        if key == "dewpoint":
            parsed["dewpoint_c"] = value
        elif key == "time_to_gate":
            parsed["time_to_gate_s"] = value
        elif key == "tail_span_60s":
            parsed["tail_span_60s_c"] = value
        elif key == "tail_slope_60s":
            parsed["tail_slope_60s_c_per_s"] = value
    return parsed


def analyze_point_log(point_log_path: Path) -> Dict[str, Any]:
    """Return physical diagnostics extracted from one H2O point log."""

    base: Dict[str, Any] = {
        "point_log": str(point_log_path),
        "wait_sample_count": 0,
        "first_dewpoint_c": None,
        "last_dewpoint_c": None,
        "min_dewpoint_c": None,
        "max_dewpoint_c": None,
        "last_time_to_gate_s": None,
        "last_tail_span_60s_c": None,
        "last_tail_slope_60s_c_per_min": None,
    }
    base.update(classify_point_failure_from_log(Path(point_log_path)))
    try:
        text = Path(point_log_path).read_text(encoding="utf-8", errors="replace")
    except OSError:
        base["physical_interpretation_zh"] = "无法读取点位日志，只能判为日志缺失。"
        return base

    dewpoints: List[float] = []
    last_wait: Dict[str, Optional[float]] = {}
    for line in text.splitlines():
        if "H2O route precondition dewpoint gate waiting" not in line:
            continue
        parsed = _parse_dewpoint_wait_line(line)
        dewpoint_c = parsed.get("dewpoint_c")
        if dewpoint_c is not None:
            dewpoints.append(float(dewpoint_c))
        last_wait = parsed

    if dewpoints:
        base["wait_sample_count"] = len(dewpoints)
        base["first_dewpoint_c"] = dewpoints[0]
        base["last_dewpoint_c"] = dewpoints[-1]
        base["min_dewpoint_c"] = min(dewpoints)
        base["max_dewpoint_c"] = max(dewpoints)
        base["last_time_to_gate_s"] = last_wait.get("time_to_gate_s")
        base["last_tail_span_60s_c"] = last_wait.get("tail_span_60s_c")
        slope = last_wait.get("tail_slope_60s_c_per_s")
        base["last_tail_slope_60s_c_per_min"] = None if slope is None else float(slope) * 60.0

    category = str(base.get("failure_category") or "")
    interpretations = {
        "hgen_not_ready": "湿度发生器目标或状态证据未满足，当前水汽输入量不可信，不能进入 H2O 拟合。",
        "dewpoint_reference_unavailable": "露点仪不可读或未准备好，缺少 H2O 主校准的外部湿度参考。",
        "humidity_reference_contradiction": "露点仪与湿度发生器状态矛盾，说明湿度参考链路需要先诊断。",
        "hgen_dewpoint_alignment": "湿度发生器目标与露点仪参考未对齐，标准湿气状态尚不能代表设定点。",
        "dewpoint_rebound": "露点曾达到要求后回升，说明管路、探头或死体积仍在释放水汽。",
        "dewpoint_reference_mismatch": "露点尾窗与目标或参考不一致，当前样气水汽摩尔分数不可直接用于拟合。",
        "dewpoint_unstable": "露点尾窗漂移或超时，说明水汽状态仍在变化，采样均值缺少计量代表性。",
        "h2o_signal_gate": "分析仪 H2O 信号门禁失败，需要先区分光学信号异常、湿气未稳或设备状态异常。",
        "h2o_ratio_unstable": "分析仪 H2O 底层比值未稳定，不能只按时间窗口采样进入正式拟合。",
        "pressure_diagnostic_unstable": "开放流通下压力波动作为诊断项保留，应结合露点和 H2O ratio 判断，不能静默进入拟合。",
        "analyzer_startup_mode2": "至少一台分析仪 MODE2 初始化失败，属于串口或固件响应问题，应按设备独立降级。",
        "route_readiness": "继电器或水路路径不可用，需要先核对 relay_map 与物理阀路。",
        "log_unavailable": "点位日志不可读，无法复核物理过程，不能作为正式拟合证据。",
        "subprocess_failed": "点位进程失败但未命中已知 H2O 物理分类，需要人工查看完整点位日志。",
    }
    base["physical_interpretation_zh"] = interpretations.get(
        category,
        "未能从日志中归类为已知 H2O 门禁原因，需要人工查看完整点位日志。",
    )
    return base


def _read_manifest(manifest_path: Path) -> List[Dict[str, Any]]:
    with Path(manifest_path).open(encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def audit_queue_manifest(manifest_path: Path) -> Dict[str, Any]:
    rows = _read_manifest(Path(manifest_path))
    audited_rows: List[Dict[str, Any]] = []
    for row in rows:
        audited = dict(row)
        point_log = str(row.get("point_log") or "").strip()
        if point_log and str(row.get("status") or "").strip().lower() == "failed":
            audited.update(analyze_point_log(Path(point_log)))
        else:
            audited.setdefault("failure_category", "")
            audited.setdefault("failure_reason", "")
            audited.setdefault("physical_interpretation_zh", "")
        audited_rows.append(audited)

    counts = Counter(str(row.get("status") or "") for row in audited_rows)
    failure_counts = Counter(str(row.get("failure_category") or "") for row in audited_rows if row.get("failure_category"))
    return {
        "schema_version": "v1_5_h2o_queue_failure_audit_v1",
        "manifest_path": str(manifest_path),
        "total_points": len(audited_rows),
        "status_counts": dict(counts),
        "failure_category_counts": dict(failure_counts),
        "rows": audited_rows,
    }


def write_audit_outputs(audit: Mapping[str, Any], output_dir: Path) -> Dict[str, str]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    rows: Sequence[Mapping[str, Any]] = list(audit.get("rows") or [])
    csv_path = output_dir / "h2o_queue_failure_audit.csv"
    json_path = output_dir / "h2o_queue_failure_audit.json"
    md_path = output_dir / "h2o_queue_failure_audit_zh.md"
    alias_csv_path = output_dir / "queue_failure_audit.csv"
    alias_json_path = output_dir / "queue_failure_audit.json"
    alias_md_path = output_dir / "queue_failure_audit_zh.md"

    fieldnames: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(str(key))
    if fieldnames:
        with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow({key: row.get(key, "") for key in fieldnames})

    json_path.write_text(json.dumps(audit, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")

    lines = [
        "# V1.5 H2O 队列失败离线审计",
        "",
        f"- manifest: `{audit.get('manifest_path')}`",
        f"- total_points: {audit.get('total_points')}",
        f"- status_counts: `{audit.get('status_counts')}`",
        f"- failure_category_counts: `{audit.get('failure_category_counts')}`",
        "",
        "| 点位 | 状态 | 失败分类 | 最后露点(°C) | 最低露点(°C) | 物理解释 |",
        "| --- | --- | --- | ---: | ---: | --- |",
    ]
    for row in rows:
        lines.append(
            "| {point} | {status} | {category} | {last_dp} | {min_dp} | {meaning} |".format(
                point=row.get("point_run_id") or "",
                status=row.get("status") or "",
                category=row.get("failure_category") or "",
                last_dp=row.get("last_dewpoint_c") if row.get("last_dewpoint_c") is not None else "",
                min_dp=row.get("min_dewpoint_c") if row.get("min_dewpoint_c") is not None else "",
                meaning=str(row.get("physical_interpretation_zh") or "").replace("|", "/"),
            )
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    if csv_path.exists():
        alias_csv_path.write_bytes(csv_path.read_bytes())
    alias_json_path.write_text(json_path.read_text(encoding="utf-8"), encoding="utf-8")
    alias_md_path.write_text(md_path.read_text(encoding="utf-8"), encoding="utf-8")

    return {
        "csv": str(csv_path),
        "json": str(json_path),
        "markdown": str(md_path),
        "queue_failure_csv": str(alias_csv_path),
        "queue_failure_json": str(alias_json_path),
        "queue_failure_markdown": str(alias_md_path),
    }


def audit_and_write(manifest_path: Path, output_dir: Path) -> Dict[str, Any]:
    audit = audit_queue_manifest(Path(manifest_path))
    audit["outputs"] = write_audit_outputs(audit, Path(output_dir))
    return audit
