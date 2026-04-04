"""CO2 ratio gas-route leak-check analysis helpers.

This module is intentionally sidecar-only. It evaluates whether the analyzer2
CO2 raw ratio response stays monotonic, linear, and stable across a fixed CO2
gas ladder without writing any calibration parameters.
"""

from __future__ import annotations

import csv
import json
import math
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from statistics import mean, median, pstdev
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


DEFAULT_GAS_PPM_SEQUENCE: tuple[int, ...] = (0, 200, 400, 600, 800, 1000)
DEFAULT_POINT_DURATION_S = 120.0
DEFAULT_STABLE_WINDOW_S = 30.0
DEFAULT_TAIL_WINDOW_S = 10.0


@dataclass(frozen=True)
class LeakCheckThresholds:
    linear_r2_pass_min: float = 0.995
    linear_r2_warn_min: float = 0.990
    normalized_residual_pass_max: float = 0.03
    normalized_residual_warn_max: float = 0.05
    normalized_tail_delta_pass_max: float = 0.01
    normalized_tail_delta_warn_max: float = 0.02
    normalized_stable_std_pass_max: float = 0.01
    normalized_stable_std_warn_max: float = 0.02
    endpoint_compression_warn_max: float = 0.97
    endpoint_compression_fail_max: float = 0.90
    endpoint_middle_residual_norm_max: float = 0.03


DEFAULT_THRESHOLDS = LeakCheckThresholds()


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _safe_mean(values: Sequence[float]) -> Optional[float]:
    return mean(values) if values else None


def _safe_median(values: Sequence[float]) -> Optional[float]:
    return median(values) if values else None


def _safe_std(values: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    if len(values) == 1:
        return 0.0
    return pstdev(values)


def _relative_seconds(rows: Sequence[Mapping[str, Any]]) -> List[float]:
    elapsed_values = [_safe_float(row.get("elapsed_s")) for row in rows]
    if elapsed_values and all(value is not None for value in elapsed_values):
        return [float(value) for value in elapsed_values if value is not None]

    timestamps: List[Optional[datetime]] = []
    for row in rows:
        raw = str(row.get("timestamp") or "").strip()
        if not raw:
            timestamps = []
            break
        normalized = raw.replace("Z", "+00:00")
        try:
            timestamps.append(datetime.fromisoformat(normalized))
        except Exception:
            timestamps = []
            break
    if timestamps:
        start = timestamps[0]
        return [(value - start).total_seconds() for value in timestamps]

    return [float(index) for index in range(len(rows))]


def _linear_fit(xs: Sequence[float], ys: Sequence[float]) -> Dict[str, Any]:
    if len(xs) != len(ys):
        raise ValueError("xs and ys must have the same length")
    if len(xs) < 2:
        return {
            "slope": None,
            "intercept": None,
            "r2": None,
            "fitted": [None for _ in xs],
            "residuals": [None for _ in ys],
        }

    x_mean = mean(xs)
    y_mean = mean(ys)
    sxx = sum((x - x_mean) ** 2 for x in xs)
    if sxx <= 0.0:
        slope = 0.0
    else:
        sxy = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))
        slope = sxy / sxx
    intercept = y_mean - slope * x_mean
    fitted = [intercept + slope * x for x in xs]
    residuals = [y - y_hat for y, y_hat in zip(ys, fitted)]
    sst = sum((y - y_mean) ** 2 for y in ys)
    ssr = sum((y - y_hat) ** 2 for y, y_hat in zip(ys, fitted))
    if sst <= 0.0:
        r2 = 1.0 if ssr <= 1e-12 else 0.0
    else:
        r2 = max(0.0, 1.0 - ssr / sst)
    return {
        "slope": slope,
        "intercept": intercept,
        "r2": r2,
        "fitted": fitted,
        "residuals": residuals,
    }


def _normalize_by_span(value: Optional[float], ratio_span: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    if ratio_span is None or ratio_span <= 0.0:
        return None
    return float(value) / float(ratio_span)


def _status_low_good(value: Optional[float], *, pass_max: float, warn_max: float) -> str:
    if value is None:
        return "fail"
    numeric = abs(float(value))
    if numeric <= pass_max:
        return "pass"
    if numeric <= warn_max:
        return "warn"
    return "fail"


def _status_high_good(value: Optional[float], *, pass_min: float, warn_min: float) -> str:
    if value is None:
        return "fail"
    numeric = float(value)
    if numeric >= pass_min:
        return "pass"
    if numeric >= warn_min:
        return "warn"
    return "fail"


def _worst_status(statuses: Iterable[str]) -> str:
    rank = {"pass": 0, "warn": 1, "fail": 2}
    worst = "pass"
    for status in statuses:
        if rank.get(status, 2) > rank[worst]:
            worst = status
    return worst


def summarize_point_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    gas_ppm: int,
    stable_window_s: float = DEFAULT_STABLE_WINDOW_S,
    tail_window_s: float = DEFAULT_TAIL_WINDOW_S,
) -> Dict[str, Any]:
    point_rows = [dict(row) for row in rows]
    elapsed_s = _relative_seconds(point_rows)
    total_samples = len(point_rows)
    total_elapsed_s = elapsed_s[-1] if elapsed_s else 0.0

    ratio_series: List[tuple[float, float]] = []
    for row, point_elapsed_s in zip(point_rows, elapsed_s):
        ratio = _safe_float(row.get("co2_ratio_raw"))
        if ratio is not None:
            ratio_series.append((point_elapsed_s, ratio))

    whole_values = [ratio for _ts, ratio in ratio_series]
    stable_begin_s = max(0.0, total_elapsed_s - max(0.0, float(stable_window_s)))
    stable_series = [(ts, ratio) for ts, ratio in ratio_series if ts >= stable_begin_s]
    stable_values = [ratio for _ts, ratio in stable_series]

    prev_begin_s = max(0.0, total_elapsed_s - max(0.0, float(tail_window_s)) * 2.0)
    prev_end_s = max(0.0, total_elapsed_s - max(0.0, float(tail_window_s)))
    prev_values = [
        ratio for ts, ratio in ratio_series if prev_begin_s <= ts < prev_end_s
    ]
    last_values = [
        ratio for ts, ratio in ratio_series if ts >= max(0.0, total_elapsed_s - max(0.0, float(tail_window_s)))
    ]

    whole_fit = _linear_fit(
        [ts for ts, _ratio in ratio_series],
        [ratio for _ts, ratio in ratio_series],
    )
    stable_fit = _linear_fit(
        [ts - stable_series[0][0] for ts, _ratio in stable_series] if stable_series else [],
        [ratio for _ts, ratio in stable_series],
    )

    return {
        "gas_ppm": int(gas_ppm),
        "start_time": point_rows[0].get("timestamp") if point_rows else None,
        "end_time": point_rows[-1].get("timestamp") if point_rows else None,
        "total_samples": total_samples,
        "ratio_valid_samples": len(ratio_series),
        "raw_ratio_first": whole_values[0] if whole_values else None,
        "raw_ratio_last": whole_values[-1] if whole_values else None,
        "whole_120s_mean": _safe_mean(whole_values),
        "whole_120s_std": _safe_std(whole_values),
        "stable_window_seconds": float(stable_window_s),
        "stable_samples": len(stable_series),
        "stable_mean_ratio": _safe_mean(stable_values),
        "stable_median_ratio": _safe_median(stable_values),
        "stable_std_ratio": _safe_std(stable_values),
        "stable_min_ratio": min(stable_values) if stable_values else None,
        "stable_max_ratio": max(stable_values) if stable_values else None,
        "prev10s_mean_ratio": _safe_mean(prev_values),
        "last10s_mean_ratio": _safe_mean(last_values),
        "tail_delta_ratio": (
            _safe_mean(last_values) - _safe_mean(prev_values)
            if _safe_mean(last_values) is not None and _safe_mean(prev_values) is not None
            else None
        ),
        "whole_window_slope_ratio_per_s": whole_fit["slope"],
        "stable_window_slope_ratio_per_s": stable_fit["slope"],
    }


def analyze_point_summaries(
    point_summaries: Sequence[Mapping[str, Any]],
    *,
    thresholds: LeakCheckThresholds = DEFAULT_THRESHOLDS,
) -> Dict[str, Any]:
    ordered_points = sorted(
        [dict(row) for row in point_summaries],
        key=lambda row: int(row.get("gas_ppm", 0)),
    )
    xs: List[float] = []
    ys: List[float] = []
    usable_points: List[Dict[str, Any]] = []
    for row in ordered_points:
        gas_ppm = _safe_float(row.get("gas_ppm"))
        stable_mean = _safe_float(row.get("stable_mean_ratio"))
        if gas_ppm is None or stable_mean is None:
            continue
        xs.append(gas_ppm)
        ys.append(stable_mean)
        usable_points.append(row)

    ratio_span = (max(ys) - min(ys)) if ys else None
    adjacent_deltas = [ys[index + 1] - ys[index] for index in range(len(ys) - 1)]
    overall_delta = (ys[-1] - ys[0]) if len(ys) >= 2 else None
    eps = max(1e-12, abs(ratio_span or 0.0) * 1e-9)

    monotonic_direction = "flat"
    if overall_delta is not None:
        if overall_delta > eps:
            monotonic_direction = "increasing"
        elif overall_delta < -eps:
            monotonic_direction = "decreasing"
        elif sum(adjacent_deltas) > eps:
            monotonic_direction = "increasing"
        elif sum(adjacent_deltas) < -eps:
            monotonic_direction = "decreasing"

    reversal_indices: List[int] = []
    monotonic_ok = bool(adjacent_deltas)
    if monotonic_direction == "increasing":
        for index, delta in enumerate(adjacent_deltas):
            if delta < -eps:
                reversal_indices.append(index)
        monotonic_ok = not reversal_indices
    elif monotonic_direction == "decreasing":
        for index, delta in enumerate(adjacent_deltas):
            if delta > eps:
                reversal_indices.append(index)
        monotonic_ok = not reversal_indices
    else:
        monotonic_ok = False

    fit = _linear_fit(xs, ys)
    fitted = fit["fitted"]
    residuals = fit["residuals"]
    normalized_residuals = [
        _normalize_by_span(residual, ratio_span) for residual in residuals
    ]
    max_abs_normalized_residual = (
        max(abs(value) for value in normalized_residuals if value is not None)
        if normalized_residuals
        else None
    )

    point_rows: List[Dict[str, Any]] = []
    normalized_tail_deltas: List[float] = []
    normalized_stable_stds: List[float] = []
    for row, fitted_ratio, residual_ratio, normalized_residual in zip(
        usable_points,
        fitted,
        residuals,
        normalized_residuals,
    ):
        tail_delta_ratio = _safe_float(row.get("tail_delta_ratio"))
        stable_std_ratio = _safe_float(row.get("stable_std_ratio"))
        tail_delta_norm = _normalize_by_span(tail_delta_ratio, ratio_span)
        stable_std_norm = _normalize_by_span(stable_std_ratio, ratio_span)
        if tail_delta_norm is not None:
            normalized_tail_deltas.append(abs(tail_delta_norm))
        if stable_std_norm is not None:
            normalized_stable_stds.append(abs(stable_std_norm))
        point_rows.append(
            {
                "gas_ppm": int(_safe_float(row.get("gas_ppm")) or 0),
                "stable_mean_ratio": _safe_float(row.get("stable_mean_ratio")),
                "stable_std_ratio": stable_std_ratio,
                "tail_delta_ratio": tail_delta_ratio,
                "fitted_ratio": fitted_ratio,
                "residual_ratio": residual_ratio,
                "residual_ratio_normalized_by_span": normalized_residual,
                "tail_delta_ratio_normalized_by_span": tail_delta_norm,
                "stable_std_ratio_normalized_by_span": stable_std_norm,
            }
        )

    max_abs_tail_delta_ratio_by_span = max(normalized_tail_deltas) if normalized_tail_deltas else None
    max_stable_std_ratio_by_span = max(normalized_stable_stds) if normalized_stable_stds else None

    if xs:
        min_ppm = min(xs)
        max_ppm = max(xs)
    else:
        min_ppm = 0.0
        max_ppm = 1000.0
    expected_end_span = None
    observed_end_span = None
    span_compression_ratio = None
    endpoint_inward = False
    endpoint_middle_max_abs_residual_norm = None
    endpoint_compression_detected = False
    if len(xs) >= 2 and fit["slope"] is not None and fit["intercept"] is not None:
        fitted_low = fit["intercept"] + fit["slope"] * min_ppm
        fitted_high = fit["intercept"] + fit["slope"] * max_ppm
        expected_end_span = abs(fitted_high - fitted_low)
        observed_end_span = abs(ys[-1] - ys[0])
        if expected_end_span > 0.0:
            span_compression_ratio = observed_end_span / expected_end_span
        low_residual = residuals[0]
        high_residual = residuals[-1]
        if monotonic_direction == "increasing":
            endpoint_inward = bool(low_residual is not None and high_residual is not None and low_residual > eps and high_residual < -eps)
        elif monotonic_direction == "decreasing":
            endpoint_inward = bool(low_residual is not None and high_residual is not None and low_residual < -eps and high_residual > eps)
        middle_norms = [
            abs(value)
            for value in normalized_residuals[1:-1]
            if value is not None
        ]
        if middle_norms:
            endpoint_middle_max_abs_residual_norm = max(middle_norms)
        if (
            span_compression_ratio is not None
            and endpoint_inward
            and endpoint_middle_max_abs_residual_norm is not None
            and endpoint_middle_max_abs_residual_norm <= thresholds.endpoint_middle_residual_norm_max
            and span_compression_ratio < thresholds.endpoint_compression_warn_max
        ):
            endpoint_compression_detected = True

    metrics: List[Dict[str, Any]] = []
    monotonic_status = "pass" if monotonic_ok else "fail"
    metrics.append(
        {
            "name": "monotonic",
            "status": monotonic_status,
            "value": monotonic_ok,
            "detail": monotonic_direction,
        }
    )
    metrics.append(
        {
            "name": "linear_r2",
            "status": _status_high_good(
                fit["r2"],
                pass_min=thresholds.linear_r2_pass_min,
                warn_min=thresholds.linear_r2_warn_min,
            ),
            "value": fit["r2"],
        }
    )
    metrics.append(
        {
            "name": "max_abs_normalized_residual",
            "status": _status_low_good(
                max_abs_normalized_residual,
                pass_max=thresholds.normalized_residual_pass_max,
                warn_max=thresholds.normalized_residual_warn_max,
            ),
            "value": max_abs_normalized_residual,
        }
    )
    metrics.append(
        {
            "name": "max_abs_tail_delta_ratio_by_span",
            "status": _status_low_good(
                max_abs_tail_delta_ratio_by_span,
                pass_max=thresholds.normalized_tail_delta_pass_max,
                warn_max=thresholds.normalized_tail_delta_warn_max,
            ),
            "value": max_abs_tail_delta_ratio_by_span,
        }
    )
    metrics.append(
        {
            "name": "max_stable_std_ratio_by_span",
            "status": _status_low_good(
                max_stable_std_ratio_by_span,
                pass_max=thresholds.normalized_stable_std_pass_max,
                warn_max=thresholds.normalized_stable_std_warn_max,
            ),
            "value": max_stable_std_ratio_by_span,
        }
    )

    endpoint_status = "pass"
    if endpoint_compression_detected:
        if span_compression_ratio is not None and span_compression_ratio <= thresholds.endpoint_compression_fail_max:
            endpoint_status = "fail"
        else:
            endpoint_status = "warn"
    metrics.append(
        {
            "name": "endpoint_compression",
            "status": endpoint_status,
            "value": span_compression_ratio,
        }
    )

    summary_messages: List[str] = []
    if not monotonic_ok:
        summary_messages.append("相邻气点方向出现反转，单调性不成立")
    if endpoint_status in {"warn", "fail"}:
        summary_messages.append("疑似漏气/混气：端点压缩")
    if ratio_span is None or ratio_span <= 0.0:
        summary_messages.append("stable_mean_ratio 的整体 span 过小，无法进行可靠归一化判断")

    classification = _worst_status(metric["status"] for metric in metrics)
    return {
        "diagnostic_name": "gas_route_ratio_leak_check",
        "diagnostic_only": True,
        "not_real_acceptance_evidence": True,
        "point_count": len(ordered_points),
        "usable_point_count": len(usable_points),
        "gas_points_ppm": [int(value) for value in xs],
        "monotonic_direction": monotonic_direction,
        "monotonic_ok": monotonic_ok,
        "reversal_indices": reversal_indices,
        "adjacent_deltas": adjacent_deltas,
        "linear_slope": fit["slope"],
        "linear_intercept": fit["intercept"],
        "linear_r2": fit["r2"],
        "ratio_span": ratio_span,
        "expected_end_span": expected_end_span,
        "observed_end_span": observed_end_span,
        "span_compression_ratio": span_compression_ratio,
        "endpoint_inward": endpoint_inward,
        "endpoint_middle_max_abs_residual_norm": endpoint_middle_max_abs_residual_norm,
        "endpoint_compression_detected": endpoint_compression_detected,
        "max_abs_normalized_residual": max_abs_normalized_residual,
        "max_abs_tail_delta_ratio_by_span": max_abs_tail_delta_ratio_by_span,
        "max_stable_std_ratio_by_span": max_stable_std_ratio_by_span,
        "classification": classification,
        "thresholds": asdict(thresholds),
        "summary_messages": summary_messages,
        "metrics": metrics,
        "points": point_rows,
    }


def build_readable_report(
    point_summaries: Sequence[Mapping[str, Any]],
    fit_summary: Mapping[str, Any],
    *,
    plot_files: Optional[Mapping[str, Any]] = None,
) -> str:
    lines: List[str] = []
    lines.append("气路线性查漏诊断报告")
    lines.append(f"生成时间: {datetime.now().isoformat(timespec='seconds')}")
    lines.append(f"结论: {str(fit_summary.get('classification', 'unknown')).upper()}")
    lines.append(
        "关键指标: "
        f"monotonic={fit_summary.get('monotonic_ok')} "
        f"r2={fit_summary.get('linear_r2')} "
        f"max_residual_norm={fit_summary.get('max_abs_normalized_residual')} "
        f"span_compression={fit_summary.get('span_compression_ratio')}"
    )
    for message in fit_summary.get("summary_messages", []) or []:
        lines.append(f"提示: {message}")

    lines.append("")
    lines.append("气点摘要:")
    for row in sorted(point_summaries, key=lambda item: int(item.get("gas_ppm", 0))):
        lines.append(
            f"- {int(row.get('gas_ppm', 0))} ppm | "
            f"stable_mean={row.get('stable_mean_ratio')} "
            f"stable_std={row.get('stable_std_ratio')} "
            f"tail_delta={row.get('tail_delta_ratio')} "
            f"slope={row.get('stable_window_slope_ratio_per_s')}"
        )

    lines.append("")
    lines.append("拟合点详情:")
    for row in fit_summary.get("points", []) or []:
        lines.append(
            f"- {int(row.get('gas_ppm', 0))} ppm | "
            f"mean={row.get('stable_mean_ratio')} "
            f"fit={row.get('fitted_ratio')} "
            f"residual={row.get('residual_ratio')} "
            f"residual_norm={row.get('residual_ratio_normalized_by_span')}"
        )
    if plot_files:
        lines.append("")
        lines.append("plot_files:")
        for key, value in plot_files.items():
            if isinstance(value, Mapping):
                for inner_key, inner_value in value.items():
                    lines.append(f"- {key}.{inner_key}: {inner_value}")
            else:
                lines.append(f"- {key}: {value}")
    return "\n".join(lines) + "\n"


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    normalized = [dict(row) for row in rows]
    header: List[str] = []
    for row in normalized:
        for key in row.keys():
            if key not in header:
                header.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        for row in normalized:
            writer.writerow(row)


def export_leak_check_results(
    output_dir: str | Path,
    *,
    raw_rows: Sequence[Mapping[str, Any]],
    point_summaries: Sequence[Mapping[str, Any]],
    fit_summary: Mapping[str, Any],
) -> Dict[str, Path]:
    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)

    raw_path = root / "raw_timeseries.csv"
    point_path = root / "point_summary.csv"
    fit_path = root / "fit_summary.json"
    report_path = root / "readable_report.txt"

    _write_csv(raw_path, raw_rows)
    _write_csv(point_path, point_summaries)
    from .gas_route_ratio_leak_check_plots import generate_leak_check_plots

    plot_outputs = generate_leak_check_plots(
        root,
        raw_rows=raw_rows,
        point_summaries=point_summaries,
        fit_summary=fit_summary,
    )
    fit_payload = dict(fit_summary)
    fit_payload["plot_files"] = {
        key: (
            {inner_key: str(inner_value) for inner_key, inner_value in value.items()}
            if isinstance(value, Mapping)
            else str(value)
        )
        for key, value in plot_outputs.items()
    }
    fit_path.write_text(
        json.dumps(fit_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    report_path.write_text(
        build_readable_report(point_summaries, fit_payload, plot_files=fit_payload["plot_files"]),
        encoding="utf-8",
    )
    outputs = {
        "output_dir": root,
        "raw_timeseries": raw_path,
        "point_summary": point_path,
        "fit_summary": fit_path,
        "readable_report": report_path,
    }
    outputs.update(plot_outputs)
    return outputs
