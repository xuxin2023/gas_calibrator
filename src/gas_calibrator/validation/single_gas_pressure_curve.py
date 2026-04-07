"""Point-mean export helpers for a single-gas room-temperature pressure sweep."""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _safe_int(value: Any) -> Optional[int]:
    numeric = _safe_float(value)
    if numeric is None:
        return None
    try:
        return int(round(numeric))
    except Exception:
        return None


def _parse_ts(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None


def _ordered(rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    return sorted((dict(row) for row in rows), key=lambda row: _parse_ts(row.get("timestamp")) or datetime.min)


def _mean(rows: Sequence[Mapping[str, Any]], key: str) -> Optional[float]:
    values = [_safe_float(row.get(key)) for row in rows]
    valid = [value for value in values if value is not None]
    if not valid:
        return None
    return sum(valid) / len(valid)


def _tail_window_rows(rows: Sequence[Mapping[str, Any]], window_s: float) -> List[Dict[str, Any]]:
    ordered = _ordered(rows)
    if not ordered:
        return []
    last_ts = _parse_ts(ordered[-1].get("timestamp"))
    if last_ts is None:
        return ordered
    cutoff = last_ts - timedelta(seconds=max(0.0, float(window_s)))
    return [row for row in ordered if (_parse_ts(row.get("timestamp")) or datetime.min) >= cutoff]


def _filter_rows(
    raw_rows: Sequence[Mapping[str, Any]],
    *,
    process_variant: Optional[str],
    gas_ppm: Optional[int],
    repeat_index: Optional[int],
) -> List[Dict[str, Any]]:
    filtered: List[Dict[str, Any]] = []
    for raw_row in raw_rows:
        row = dict(raw_row)
        if process_variant is not None and str(row.get("process_variant") or "").strip().upper() != str(process_variant).strip().upper():
            continue
        if gas_ppm is not None and _safe_int(row.get("gas_ppm")) != int(gas_ppm):
            continue
        if repeat_index is not None and _safe_int(row.get("repeat_index")) != int(repeat_index):
            continue
        filtered.append(row)
    return filtered


def build_pressure_curve_point_means(
    raw_rows: Sequence[Mapping[str, Any]],
    *,
    process_variant: Optional[str] = None,
    gas_ppm: Optional[int] = None,
    repeat_index: Optional[int] = None,
    ambient_tail_window_s: float = 60.0,
) -> List[Dict[str, Any]]:
    rows = _filter_rows(
        raw_rows,
        process_variant=process_variant,
        gas_ppm=gas_ppm,
        repeat_index=repeat_index,
    )
    if not rows:
        return []

    point_rows: List[Dict[str, Any]] = []

    ambient_rows = [
        row
        for row in rows
        if str(row.get("phase") or "").strip().lower() == "gas_flush_vent_on"
    ]
    ambient_tail = _tail_window_rows(ambient_rows, ambient_tail_window_s)
    if ambient_tail:
        ambient_pressure = _mean(ambient_tail, "gauge_pressure_hpa")
        if ambient_pressure is None:
            ambient_pressure = _mean(ambient_tail, "controller_pressure_hpa")
        point_rows.append(
            {
                "point_label": "ambient",
                "point_kind": "ambient",
                "process_variant": ambient_tail[-1].get("process_variant"),
                "gas_ppm": _safe_int(ambient_tail[-1].get("gas_ppm")),
                "repeat_index": _safe_int(ambient_tail[-1].get("repeat_index")),
                "pressure_target_hpa": None,
                "pressure_hpa_mean": ambient_pressure,
                "co2_ratio_raw_mean": _mean(ambient_tail, "co2_ratio_raw"),
                "co2_ratio_f_mean": _mean(ambient_tail, "co2_ratio_f"),
                "co2_density_mean": _mean(ambient_tail, "co2_density"),
                "co2_ppm_mean": _mean(ambient_tail, "co2_ppm"),
                "chamber_temp_mean": _mean(ambient_tail, "chamber_temp_c"),
                "shell_temp_mean": _mean(ambient_tail, "shell_temp_c"),
                "sample_count": len(ambient_tail),
                "window_s": float(ambient_tail_window_s),
                "source_phase": "gas_flush_vent_on",
            }
        )

    grouped: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if str(row.get("phase") or "").strip().lower() != "stable_sampling":
            continue
        pressure_target = _safe_int(row.get("pressure_target_hpa"))
        if pressure_target is None:
            continue
        grouped[pressure_target].append(row)

    for pressure_target in sorted(grouped.keys(), reverse=True):
        group_rows = _ordered(grouped[pressure_target])
        pressure_mean = _mean(group_rows, "gauge_pressure_hpa")
        if pressure_mean is None:
            pressure_mean = _mean(group_rows, "controller_pressure_hpa")
        point_rows.append(
            {
                "point_label": f"{pressure_target}hPa",
                "point_kind": "sealed",
                "process_variant": group_rows[-1].get("process_variant"),
                "gas_ppm": _safe_int(group_rows[-1].get("gas_ppm")),
                "repeat_index": _safe_int(group_rows[-1].get("repeat_index")),
                "pressure_target_hpa": pressure_target,
                "pressure_hpa_mean": pressure_mean,
                "co2_ratio_raw_mean": _mean(group_rows, "co2_ratio_raw"),
                "co2_ratio_f_mean": _mean(group_rows, "co2_ratio_f"),
                "co2_density_mean": _mean(group_rows, "co2_density"),
                "co2_ppm_mean": _mean(group_rows, "co2_ppm"),
                "chamber_temp_mean": _mean(group_rows, "chamber_temp_c"),
                "shell_temp_mean": _mean(group_rows, "shell_temp_c"),
                "sample_count": len(group_rows),
                "window_s": None,
                "source_phase": "stable_sampling",
            }
        )

    for row in point_rows:
        pressure = _safe_float(row.get("pressure_hpa_mean"))
        density = _safe_float(row.get("co2_density_mean"))
        row["density_over_pressure_mean"] = None if pressure in (None, 0.0) or density is None else density / pressure
    return point_rows


def write_pressure_curve_point_means_csv(path: str | Path, rows: Sequence[Mapping[str, Any]]) -> Path:
    output_path = Path(path).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "point_label",
        "point_kind",
        "process_variant",
        "gas_ppm",
        "repeat_index",
        "pressure_target_hpa",
        "pressure_hpa_mean",
        "co2_ratio_raw_mean",
        "co2_ratio_f_mean",
        "co2_density_mean",
        "co2_ppm_mean",
        "density_over_pressure_mean",
        "chamber_temp_mean",
        "shell_temp_mean",
        "sample_count",
        "window_s",
        "source_phase",
    ]
    with output_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})
    return output_path


def summarize_pressure_curve_relationships(point_rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    ordered = sorted(
        (dict(row) for row in point_rows),
        key=lambda row: (_safe_float(row.get("pressure_hpa_mean")) or 0.0),
        reverse=True,
    )
    ratio_f_values = [_safe_float(row.get("co2_ratio_f_mean")) for row in ordered]
    ppm_values = [_safe_float(row.get("co2_ppm_mean")) for row in ordered]
    density_values = [_safe_float(row.get("co2_density_mean")) for row in ordered]
    pressure_values = [_safe_float(row.get("pressure_hpa_mean")) for row in ordered]

    def _rel_span(values: Sequence[Optional[float]]) -> Optional[float]:
        valid = [value for value in values if value is not None]
        if len(valid) < 2:
            return None
        base = sum(valid) / len(valid)
        if base == 0:
            return None
        return (max(valid) - min(valid)) / abs(base)

    return {
        "point_count": len(ordered),
        "pressure_span_hpa": None
        if len([value for value in pressure_values if value is not None]) < 2
        else max(value for value in pressure_values if value is not None)
        - min(value for value in pressure_values if value is not None),
        "ratio_f_relative_span": _rel_span(ratio_f_values),
        "ppm_relative_span": _rel_span(ppm_values),
        "density_relative_span": _rel_span(density_values),
        "judgement_hint": (
            "ratio_nearly_flat_but_ppm_runs_with_pressure"
            if (_rel_span(ratio_f_values) or 0.0) < 0.01 and (_rel_span(ppm_values) or 0.0) > 0.03
            else "ratio_itself_moves_with_pressure"
            if (_rel_span(ratio_f_values) or 0.0) >= 0.01
            else "insufficient_evidence"
        ),
    }


def write_pressure_curve_summary_json(path: str | Path, summary: Mapping[str, Any]) -> Path:
    output_path = Path(path).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(dict(summary), ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path


def generate_pressure_curve_plots(output_dir: str | Path, *, point_rows: Sequence[Mapping[str, Any]]) -> Dict[str, Path]:
    import matplotlib

    matplotlib.use("Agg", force=True)
    import matplotlib.pyplot as plt

    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    plt.style.use("seaborn-v0_8-whitegrid")

    ordered = sorted(
        (dict(row) for row in point_rows),
        key=lambda row: (_safe_float(row.get("pressure_hpa_mean")) or 0.0),
        reverse=True,
    )
    pressures = [_safe_float(row.get("pressure_hpa_mean")) for row in ordered]
    ratio_raw = [_safe_float(row.get("co2_ratio_raw_mean")) for row in ordered]
    ratio_f = [_safe_float(row.get("co2_ratio_f_mean")) for row in ordered]
    density = [_safe_float(row.get("co2_density_mean")) for row in ordered]
    ppm = [_safe_float(row.get("co2_ppm_mean")) for row in ordered]
    density_over_pressure = [_safe_float(row.get("density_over_pressure_mean")) for row in ordered]

    outputs: Dict[str, Path] = {}

    fig, ax = plt.subplots(figsize=(7.5, 5.2), dpi=150)
    valid_raw = [(x, y) for x, y in zip(pressures, ratio_raw) if x is not None and y is not None]
    valid_f = [(x, y) for x, y in zip(pressures, ratio_f) if x is not None and y is not None]
    if valid_raw:
        ax.plot([x for x, _ in valid_raw], [y for _, y in valid_raw], "o-", label="co2_ratio_raw")
    if valid_f:
        ax.plot([x for x, _ in valid_f], [y for _, y in valid_f], "s--", label="co2_ratio_f")
    ax.set_xlabel("Pressure (hPa)")
    ax.set_ylabel("Ratio mean")
    ax.set_title("Ratio vs Pressure")
    ax.legend()
    fig.tight_layout()
    path = root / "ratio_vs_pressure.png"
    fig.savefig(path)
    plt.close(fig)
    outputs["ratio_vs_pressure"] = path

    fig, ax = plt.subplots(figsize=(7.5, 5.2), dpi=150)
    valid_density = [(x, y) for x, y in zip(pressures, density) if x is not None and y is not None]
    if valid_density:
        ax.plot([x for x, _ in valid_density], [y for _, y in valid_density], "o-", color="#d62728")
    ax.set_xlabel("Pressure (hPa)")
    ax.set_ylabel("CO2 density mean")
    ax.set_title("Density vs Pressure")
    fig.tight_layout()
    path = root / "density_vs_pressure.png"
    fig.savefig(path)
    plt.close(fig)
    outputs["density_vs_pressure"] = path

    fig, ax = plt.subplots(figsize=(7.5, 5.2), dpi=150)
    valid_ppm = [(x, y) for x, y in zip(pressures, ppm) if x is not None and y is not None]
    if valid_ppm:
        ax.plot([x for x, _ in valid_ppm], [y for _, y in valid_ppm], "o-", color="#2ca02c")
    ax.set_xlabel("Pressure (hPa)")
    ax.set_ylabel("CO2 ppm mean")
    ax.set_title("PPM vs Pressure")
    fig.tight_layout()
    path = root / "ppm_vs_pressure.png"
    fig.savefig(path)
    plt.close(fig)
    outputs["ppm_vs_pressure"] = path

    fig, ax = plt.subplots(figsize=(7.5, 5.2), dpi=150)
    valid_density_pressure = [
        (x, y)
        for x, y in zip(density_over_pressure, ppm)
        if x is not None and y is not None
    ]
    if valid_density_pressure:
        ax.plot([x for x, _ in valid_density_pressure], [y for _, y in valid_density_pressure], "o-", color="#9467bd")
    ax.set_xlabel("CO2 density / pressure")
    ax.set_ylabel("CO2 ppm mean")
    ax.set_title("PPM vs Density/Pressure")
    fig.tight_layout()
    path = root / "ppm_vs_density_over_pressure.png"
    fig.savefig(path)
    plt.close(fig)
    outputs["ppm_vs_density_over_pressure"] = path

    return outputs
