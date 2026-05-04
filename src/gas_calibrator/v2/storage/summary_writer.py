from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


def _utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _read_json(path: Path) -> Optional[dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return None


def _safe_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def generate_run_summary(output_dir: str) -> dict[str, Any]:
    base = Path(output_dir).resolve()
    summary = _read_json(base / "summary.json") or {}
    manifest = _read_json(base / "manifest.json") or {}

    run_id = _safe_str(summary.get("run_id") or base.name)
    status = summary.get("status") or {}
    stats = summary.get("stats") or {}

    phase = _safe_str(status.get("phase"), "unknown")
    message = _safe_str(status.get("message"))
    error = _safe_str(status.get("error"))
    started_at = _safe_str(summary.get("started_at"))
    config_path = _safe_str(summary.get("config_path"))
    operator = _safe_str(summary.get("operator") or summary.get("reviewer"))
    final_decision = _safe_str(
        summary.get("final_decision") or phase
    )
    not_real_evidence = summary.get("not_real_acceptance_evidence", True)
    evidence_source = _safe_str(summary.get("evidence_source"), "unknown")

    points_completed = _safe_int(stats.get("completed_points") or status.get("completed_points"))
    points_total = _safe_int(stats.get("total_points") or status.get("total_points"))
    sample_count = _safe_int(stats.get("sample_count"))
    warning_count = _safe_int(stats.get("warning_count"))
    error_count = _safe_int(stats.get("error_count"))
    software_version = _safe_str(stats.get("software_version") or summary.get("software_version"))
    run_mode = _safe_str(stats.get("run_mode") or manifest.get("run_mode"))
    route_mode = _safe_str(stats.get("route_mode") or manifest.get("route_mode"))
    elapsed_s = status.get("elapsed_s")

    manifest_schema = _safe_str(manifest.get("schema_version"))
    source_points_file = _safe_str(manifest.get("source_points_file"))
    manifest_devices = manifest.get("device_snapshot") or {}
    device_count = len(manifest_devices) if isinstance(manifest_devices, dict) else 0

    analyzer_sns: list[str] = []
    for csv_name in ("samples_runtime.csv", "samples.csv"):
        csv_path = base / csv_name
        if csv_path.exists():
            try:
                import csv as csv_mod
                with open(csv_path, "r", encoding="utf-8", newline="") as fh:
                    reader = csv_mod.DictReader(fh)
                    if reader.fieldnames:
                        for col in reader.fieldnames:
                            if col.endswith("_serial") and col.lower().startswith(("ga", "analyzer_")):
                                try:
                                    first_row = next(reader)
                                except StopIteration:
                                    break
                                sn_val = (first_row.get(col) or "").strip()
                                if sn_val:
                                    analyzer_sns.append(sn_val)
            except Exception:
                pass
            break

    lines: list[str] = []
    lines.append("# 气体分析仪校准运行证据摘要")
    lines.append("")
    lines.append(f"**运行 ID**: `{run_id}`")
    lines.append(f"**生成时间**: {_utc_now_text()}")
    lines.append("")
    lines.append("## 运行概览")
    lines.append("")
    lines.append(f"| 字段 | 值 |")
    lines.append(f"|------|-----|")
    lines.append(f"| 最终状态 | {phase} |")
    if final_decision and final_decision != phase:
        lines.append(f"| 最终决策 | {final_decision} |")
    if started_at:
        lines.append(f"| 启动时间 | {started_at} |")
    if operator:
        lines.append(f"| 操作者 | {operator} |")
    if software_version:
        lines.append(f"| 软件版本 | {software_version} |")
    if run_mode:
        lines.append(f"| 运行模式 | {run_mode} |")
    if route_mode:
        lines.append(f"| 路线模式 | {route_mode} |")
    if config_path:
        lines.append(f"| 配置文件 | {config_path} |")
    if elapsed_s is not None:
        lines.append(f"| 耗时 (秒) | {elapsed_s:.1f} |")
    lines.append("")
    lines.append("## 点位统计")
    lines.append("")
    lines.append(f"| 指标 | 值 |")
    lines.append(f"|------|-----|")
    lines.append(f"| 完成的点位 | {points_completed} / {points_total} |")
    lines.append(f"| 采集样本数 | {sample_count} |")
    lines.append(f"| 警告数 | {warning_count} |")
    lines.append(f"| 错误数 | {error_count} |")
    lines.append("")

    if analyzer_sns:
        lines.append("## 分析仪清单")
        lines.append("")
        for sn in analyzer_sns:
            lines.append(f"- {sn}")
        lines.append("")

    if manifest:
        lines.append("## 清单摘要")
        lines.append("")
        if manifest_schema:
            lines.append(f"- **清单 Schema 版本**: {manifest_schema}")
        if source_points_file:
            lines.append(f"- **点位文件**: {source_points_file}")
        if device_count:
            lines.append(f"- **设备数量**: {device_count}")
        lines.append("")

    if error:
        lines.append("## 错误信息")
        lines.append("")
        lines.append(f"```")
        lines.append(error)
        lines.append(f"```")
        lines.append("")

    if message:
        lines.append("## 状态消息")
        lines.append("")
        lines.append(f"> {message}")
        lines.append("")

    lines.append("## 证据属性")
    lines.append("")
    lines.append(f"- **证据来源**: {evidence_source}")
    lines.append(f"- **非真实验收证据**: {'是' if not_real_evidence else '否'}")
    lines.append("")

    markdown_text = "\n".join(lines)
    output_path = base / "run_evidence_summary.md"
    output_path.write_text(markdown_text, encoding="utf-8")

    return {
        "ok": True,
        "run_id": run_id,
        "output_path": str(output_path),
        "phase": phase,
        "points_completed": points_completed,
        "points_total": points_total,
        "sample_count": sample_count,
        "analyzer_sns": analyzer_sns,
    }
