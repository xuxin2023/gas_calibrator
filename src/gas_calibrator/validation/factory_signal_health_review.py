"""V1.5 factory-mode optical signal health review.

This module is intentionally offline-only.  It turns MODE2 factory fields
(`ref_signal`, `co2_signal`, `h2o_signal`) into reviewer evidence before a
candidate coefficient write.  The review must not open COM ports or modify the
gas/water route.
"""

from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Mapping


def _read_csv(path: str | Path | None) -> list[dict[str, str]]:
    if not path:
        return []
    csv_path = Path(path)
    if not csv_path.exists():
        return []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    materialized = [dict(row) for row in rows]
    fieldnames: list[str] = []
    for row in materialized:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(materialized)


def _as_float(value: Any) -> float | None:
    try:
        number = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _as_key(*parts: Any) -> tuple[str, ...]:
    return tuple(str(part or "").strip() for part in parts)


def _normalized_device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _pct(part: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round(part * 100.0 / total, 6)


def _median(values: Iterable[float | None]) -> float | None:
    clean = [value for value in values if value is not None]
    if not clean:
        return None
    return float(median(clean))


def _range(values: Iterable[float | None]) -> float | None:
    clean = [value for value in values if value is not None]
    if not clean:
        return None
    return float(max(clean) - min(clean))


@dataclass(frozen=True)
class FactorySignalHealthConfig:
    """Thresholds for an offline reviewer gate.

    `ref_full_scale_hint` is a reviewer hint derived from the manual commands
    SETCO2/SETILLUM, not an ADC range assertion.  MODE2 `ref_signal` itself is
    documented with a wider numeric range, so this threshold should be reported
    as "near configured reference full-scale hint" rather than "saturated ADC".
    """

    target_device_ids: tuple[str, ...] = ()
    ref_full_scale_hint: float = 4000.0
    high_ref_fraction_warn: float = 0.20
    relative_error_pct_warn: float = 5.0
    absolute_error_warn: float = 25.0
    ratio_span_warn: float = 0.001
    min_point_count_for_pass: int = 5
    require_residual_evidence_for_block: bool = True


def load_factory_signal_health_summary(path: str | Path | None) -> dict[str, dict[str, Any]]:
    """Load a factory-signal health summary keyed by analyzer device ID.

    The summary is evidence produced by this module, not live device state.  A
    caller that explicitly supplies the CSV should treat a missing device row as
    an unresolved pre-write review item rather than assuming the optical chain
    is healthy.
    """

    rows = _read_csv(path)
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        device_id = _normalized_device_id(row.get("device_id") or row.get("analyzer_device_id"))
        if not device_id:
            continue
        result[device_id] = dict(row)
    return result


def factory_signal_health_block_reason(
    row: Mapping[str, Any] | None,
    *,
    summary_was_required: bool = False,
) -> str:
    if row is None:
        return "factory_signal_health_missing_device" if summary_was_required else ""
    gate = str(row.get("candidate_gate") or "").strip()
    if not gate or gate == "pass_factory_signal_health":
        return ""
    if gate.startswith("block"):
        return f"factory_signal_health_block:{gate}"
    return f"factory_signal_health_review:{gate}"


def _residual_lookup(rows: list[dict[str, str]]) -> dict[tuple[str, str, str], dict[str, str]]:
    result: dict[tuple[str, str, str], dict[str, str]] = {}
    for row in rows:
        key = _as_key(row.get("component"), row.get("device_id") or row.get("analyzer_device_id"), row.get("source_run_id"))
        result[key] = row
    return result


def _point_flags(
    *,
    row: Mapping[str, Any],
    residual: Mapping[str, Any] | None,
    cfg: FactorySignalHealthConfig,
) -> tuple[str, str, bool]:
    flags: list[str] = []
    severity = "pass"
    blocks_candidate = False

    ref_signal = _as_float(row.get("ref_signal_median"))
    co2_signal = _as_float(row.get("co2_signal_median"))
    h2o_signal = _as_float(row.get("h2o_signal_median"))
    ratio_span = _as_float(row.get("ratio_span"))
    rel_error = _as_float((residual or {}).get("relative_error_pct"))
    abs_error = _as_float((residual or {}).get("error"))

    if ref_signal is None:
        flags.append("ref_signal_missing")
        severity = "review"
    elif ref_signal >= cfg.ref_full_scale_hint:
        flags.append("ref_signal_near_configured_full_scale_hint")
        severity = "review"

    if co2_signal is None and str(row.get("component") or "").lower() == "co2":
        flags.append("co2_signal_missing")
        severity = "review"
    if h2o_signal is None and str(row.get("component") or "").lower() == "h2o":
        flags.append("h2o_signal_missing")
        severity = "review"

    if ratio_span is not None and ratio_span > cfg.ratio_span_warn:
        flags.append("ratio_window_not_stable_enough")
        severity = "review"

    high_error = False
    if abs_error is not None and abs(abs_error) > cfg.absolute_error_warn:
        flags.append("large_absolute_model_residual")
        high_error = True
    if rel_error is not None and abs(rel_error) > cfg.relative_error_pct_warn:
        flags.append("large_relative_model_residual")
        high_error = True

    if high_error and "ref_signal_near_configured_full_scale_hint" in flags:
        flags.append("stable_ratio_but_reference_chain_unhealthy")
        severity = "block"
        blocks_candidate = True
    elif high_error:
        severity = "review" if severity == "pass" else severity

    return ";".join(flags) if flags else "none", severity, blocks_candidate


def build_factory_signal_health_tables(
    *,
    point_means_csv: str | Path,
    residuals_csv: str | Path | None = None,
    cfg: FactorySignalHealthConfig | None = None,
) -> dict[str, list[dict[str, Any]]]:
    config = cfg or FactorySignalHealthConfig()
    point_rows = _read_csv(point_means_csv)
    residuals = _residual_lookup(_read_csv(residuals_csv))
    target_ids = {_normalized_device_id(item) for item in config.target_device_ids}

    point_flags: list[dict[str, Any]] = []
    by_device: dict[str, list[dict[str, Any]]] = {}
    for row in point_rows:
        device_id = _normalized_device_id(row.get("device_id") or row.get("analyzer_device_id"))
        if target_ids and device_id not in target_ids:
            continue
        component = str(row.get("component") or "").strip().lower()
        residual = residuals.get(_as_key(component, device_id, row.get("source_run_id")))
        flags, severity, blocks = _point_flags(row=row, residual=residual, cfg=config)
        item: dict[str, Any] = {
            "component": component,
            "device_id": device_id,
            "analyzer_prefix": row.get("analyzer_prefix", ""),
            "source_run_id": row.get("source_run_id", ""),
            "point_tag": row.get("point_tag", ""),
            "target": row.get("target", ""),
            "temp_set_c": row.get("temp_set_c", ""),
            "ratio_median": row.get("ratio_median", ""),
            "ratio_span": row.get("ratio_span", ""),
            "ref_signal_median": row.get("ref_signal_median", ""),
            "co2_signal_median": row.get("co2_signal_median", ""),
            "h2o_signal_median": row.get("h2o_signal_median", ""),
            "dewpoint_c_median": row.get("dewpoint_c_median", ""),
            "model_error": (residual or {}).get("error", ""),
            "relative_error_pct": (residual or {}).get("relative_error_pct", ""),
            "signal_health_flags": flags,
            "signal_health_severity": severity,
            "blocks_candidate_write": "true" if blocks else "false",
        }
        point_flags.append(item)
        by_device.setdefault(device_id, []).append(item)

    summary: list[dict[str, Any]] = []
    for device_id, rows in sorted(by_device.items()):
        total = len(rows)
        high_ref = sum("ref_signal_near_configured_full_scale_hint" in str(row["signal_health_flags"]) for row in rows)
        blocking = [row for row in rows if row["blocks_candidate_write"] == "true"]
        review = [row for row in rows if row["signal_health_severity"] in {"review", "block"}]
        max_abs_error = max((abs(_as_float(row.get("model_error")) or 0.0) for row in rows), default=0.0)
        rel_values = [abs(value) for value in (_as_float(row.get("relative_error_pct")) for row in rows) if value is not None]
        max_rel_error = max(rel_values, default=0.0)
        ref_values = [_as_float(row.get("ref_signal_median")) for row in rows]
        ratio_spans = [_as_float(row.get("ratio_span")) for row in rows]
        if blocking:
            gate = "block_optical_reference_health_review"
        elif total < config.min_point_count_for_pass:
            gate = "review_insufficient_factory_signal_coverage"
        elif review:
            gate = "review_factory_signal_health"
        else:
            gate = "pass_factory_signal_health"
        if config.require_residual_evidence_for_block and not residuals and gate == "block_optical_reference_health_review":
            gate = "review_factory_signal_health"

        summary.append(
            {
                "device_id": device_id,
                "point_count": total,
                "review_point_count": len(review),
                "blocking_point_count": len(blocking),
                "high_ref_point_count": high_ref,
                "high_ref_point_pct": _pct(high_ref, total),
                "ref_signal_median": _median(ref_values),
                "ref_signal_range": _range(ref_values),
                "max_ratio_span": max((value for value in ratio_spans if value is not None), default=0.0),
                "max_abs_model_error": round(max_abs_error, 6),
                "max_relative_model_error_pct": round(max_rel_error, 6),
                "candidate_gate": gate,
                "physical_interpretation": _summary_interpretation(gate),
            }
        )

    metadata = [
        {
            "manual_basis": (
                "MODE2 field 11 is reference signal value; fields 12/13 are CO2/H2O signal values. "
                "SETCO2/SETILLUM define reference full-value behavior, so ref>=4000 is a configured-full-scale hint, not an ADC saturation assertion."
            ),
            "no_write": "true",
            "route_control": "not_used",
            "fit_policy": "Do not absorb optical reference-chain faults into SENCO1/2/3/4 candidate coefficients.",
        }
    ]
    return {"summary": summary, "point_flags": point_flags, "metadata": metadata}


def _summary_interpretation(gate: str) -> str:
    if gate == "block_optical_reference_health_review":
        return "参考光学信号和模型残差同时异常；先查 SETCO2/SETPOW/光路/状态寄存器，不建议直接写组分系数。"
    if gate == "review_factory_signal_health":
        return "存在工厂信号或残差疑点；进入候选系数评审前需要人工确认点位可用性。"
    if gate == "review_insufficient_factory_signal_coverage":
        return "工厂模式点位覆盖不足，不能据此放行候选系数评审；需回看上游 QC 阻断原因。"
    return "工厂模式参考信号未发现阻断级异常，可继续进入候选系数评审。"


def write_factory_signal_health_report(
    *,
    point_means_csv: str | Path,
    output_dir: str | Path,
    residuals_csv: str | Path | None = None,
    cfg: FactorySignalHealthConfig | None = None,
) -> dict[str, Path]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    tables = build_factory_signal_health_tables(
        point_means_csv=point_means_csv,
        residuals_csv=residuals_csv,
        cfg=cfg,
    )

    summary_path = output / "factory_signal_health_summary.csv"
    point_flags_path = output / "factory_signal_health_point_flags.csv"
    meta_path = output / "factory_signal_health_meta.json"
    report_path = output / "factory_signal_health_report_zh.md"

    _write_csv(summary_path, tables["summary"])
    _write_csv(point_flags_path, tables["point_flags"])
    meta_path.write_text(json.dumps(tables["metadata"][0], ensure_ascii=False, indent=2), encoding="utf-8-sig")
    report_path.write_text(_render_markdown(tables), encoding="utf-8-sig")
    return {
        "summary": summary_path,
        "point_flags": point_flags_path,
        "metadata": meta_path,
        "markdown": report_path,
    }


def _render_markdown(tables: Mapping[str, list[dict[str, Any]]]) -> str:
    lines = [
        "# V1.5 工厂模式参考信号健康评审",
        "",
        "## 物理依据",
        "",
        "- 手册 MODE2 第 11 项是参考信号值，第 12/13 项是 CO2/H2O 信号值。",
        "- 参考信号不是浓度、温度、压力或露点，而是光学测量链路的参考量。",
        "- `ref_signal >= 4000` 在这里只作为 SETCO2/SETILLUM 参考满值附近的评审提示，不等同于 ADC 饱和结论。",
        "- 如果 ratio 很稳定但参考信号/CO2 信号/H2O 信号处于异常工作区，说明可能是“稳定的错误光学状态”，不能用 S1/S3 或 S2/S4 去吸收。",
        "",
        "## 设备结论",
        "",
        "| 设备ID | 点数 | 高参考信号点 | 最大绝对残差 | 最大相对残差% | 门禁 | 物理解释 |",
        "| --- | ---: | ---: | ---: | ---: | --- | --- |",
    ]
    for row in tables.get("summary", []):
        lines.append(
            "| {device_id} | {point_count} | {high_ref_point_count} | {max_abs_model_error} | {max_relative_model_error_pct} | {candidate_gate} | {physical_interpretation} |".format(
                **row
            )
        )
    lines.extend(
        [
            "",
            "## 阻断或需复核点位",
            "",
            "| 设备ID | 组分 | 点位 | 目标 | 温度 | ratio | ref | CO2信号 | H2O信号 | 残差 | 相对残差% | 标记 |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    flagged = [row for row in tables.get("point_flags", []) if row.get("signal_health_flags") != "none"]
    for row in flagged[:80]:
        lines.append(
            "| {device_id} | {component} | {source_run_id} | {target} | {temp_set_c} | {ratio_median} | {ref_signal_median} | {co2_signal_median} | {h2o_signal_median} | {model_error} | {relative_error_pct} | {signal_health_flags} |".format(
                **row
            )
        )
    if len(flagged) > 80:
        lines.append(f"| ... | ... | 仅显示前 80 条，完整见 CSV |  |  |  |  |  |  |  |  | total={len(flagged)} |")
    lines.extend(
        [
            "",
            "## 后续动作",
            "",
            "- 对阻断设备，优先只读核对 `SETCO2`、`SETPOW`、状态寄存器以及 MODE2 原始信号，不直接写组分系数。",
            "- 对通过设备，才进入 S1/S3 或 S2/S4 候选系数评审。",
            "- 该报告只使用离线证据，不打开 COM，不控制气路/水路，不写 SENCO。",
            "",
        ]
    )
    return "\n".join(lines)
