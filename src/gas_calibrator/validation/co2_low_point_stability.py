"""Offline CO2 low-point stability diagnostics for V1.5.

The diagnostic consumes already-recorded open-flow sample CSV files. It never
opens COM ports, controls routes, or writes coefficients. Its purpose is to
decide whether a low CO2 point is a trustworthy fit anchor or a moving physical
state that should stay out of SENCO1 fitting.
"""

from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


@dataclass(frozen=True)
class Co2LowPointStabilityConfig:
    target_device_ids: tuple[str, ...] = ("030", "022", "033", "051")
    low_point_max_ppm: float = 150.0
    acceptance_pct: float = 1.0
    full_window_ratio_span_limit: float = 0.0015
    tail_ratio_span_limit: float = 0.0005
    full_window_co2_span_ppm_limit: float = 3.0
    tail_co2_span_ppm_limit: float = 1.0
    tail_fraction: float = 0.25
    min_samples: int = 20


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "None", "null"):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    return numeric if math.isfinite(numeric) else None


def _device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _read_csv(path: str | Path) -> List[Dict[str, Any]]:
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields: List[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows([dict(row) for row in rows])


def _mean(values: Sequence[float]) -> Optional[float]:
    return sum(values) / len(values) if values else None


def _span(values: Sequence[float]) -> Optional[float]:
    return max(values) - min(values) if values else None


def _linear_slope(values: Sequence[float]) -> Optional[float]:
    if len(values) < 2:
        return None
    n = len(values)
    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / n
    denom = sum((idx - x_mean) ** 2 for idx in range(n))
    if denom == 0:
        return None
    return sum((idx - x_mean) * (value - y_mean) for idx, value in enumerate(values)) / denom


def _target_ppm(rows: Sequence[Mapping[str, Any]]) -> Optional[float]:
    for row in rows:
        for key in ("co2_ppm_target", "target_ppm", "目标值"):
            value = _safe_float(row.get(key))
            if value is not None:
                return value
    return None


def _run_label(path: Path, rows: Sequence[Mapping[str, Any]]) -> str:
    if rows:
        point = str(rows[0].get("point_title") or rows[0].get("point") or "").strip()
        if point:
            return point
    return path.parent.name


def _device_prefixes(rows: Sequence[Mapping[str, Any]], cfg: Co2LowPointStabilityConfig) -> Dict[str, str]:
    wanted = {_device_id(value) for value in cfg.target_device_ids}
    prefixes: Dict[str, str] = {}
    if not rows:
        return prefixes
    fields = set(rows[0].keys())
    for field in fields:
        if not field.endswith("_analyzer_device_id"):
            continue
        prefix = field[: -len("_analyzer_device_id")]
        for row in rows:
            dev = _device_id(row.get(field))
            if dev in wanted:
                prefixes[dev] = prefix
                break
    return prefixes


def _values(rows: Sequence[Mapping[str, Any]], key: str) -> List[float]:
    out: List[float] = []
    for row in rows:
        value = _safe_float(row.get(key))
        if value is not None:
            out.append(float(value))
    return out


def _tail(values: Sequence[float], fraction: float) -> List[float]:
    if not values:
        return []
    count = max(1, int(math.ceil(len(values) * max(0.0, min(1.0, fraction)))))
    return list(values[-count:])


def _classify(
    *,
    target: float,
    mean_co2: Optional[float],
    full_ratio_span: Optional[float],
    tail_ratio_span: Optional[float],
    full_co2_span: Optional[float],
    tail_co2_span: Optional[float],
    n: int,
    cfg: Co2LowPointStabilityConfig,
) -> tuple[str, str]:
    if n < cfg.min_samples:
        return "reject", "insufficient_low_point_samples"
    if target <= 0 or mean_co2 is None:
        return "reject", "missing_target_or_co2_mean"

    rel_error = abs(mean_co2 - target) / target * 100.0
    full_moving = (
        (full_ratio_span is not None and full_ratio_span > cfg.full_window_ratio_span_limit)
        or (full_co2_span is not None and full_co2_span > cfg.full_window_co2_span_ppm_limit)
    )
    tail_stable = (
        (tail_ratio_span is None or tail_ratio_span <= cfg.tail_ratio_span_limit)
        and (tail_co2_span is None or tail_co2_span <= cfg.tail_co2_span_ppm_limit)
    )
    if full_moving and tail_stable:
        return "diagnostic_only", "low_point_history_moves_even_if_tail_is_stable"
    if full_moving:
        return "reject", "low_point_still_moving"
    if rel_error > cfg.acceptance_pct:
        return "diagnostic_only", "stable_low_point_bias_review_source_or_model"
    return "fit_eligible", "low_point_stable_and_within_acceptance"


def build_co2_low_point_stability_tables(
    sample_csv_paths: Iterable[str | Path],
    cfg: Co2LowPointStabilityConfig = Co2LowPointStabilityConfig(),
) -> Dict[str, List[Dict[str, Any]]]:
    point_rows: List[Dict[str, Any]] = []
    run_rows: List[Dict[str, Any]] = []
    for raw_path in sample_csv_paths:
        path = Path(raw_path)
        rows = _read_csv(path)
        target = _target_ppm(rows)
        if target is None or target > cfg.low_point_max_ppm:
            continue
        prefixes = _device_prefixes(rows, cfg)
        run_label = _run_label(path, rows)
        run_statuses: List[str] = []
        for dev, prefix in sorted(prefixes.items()):
            co2 = _values(rows, f"{prefix}_co2_ppm")
            ratio = _values(rows, f"{prefix}_co2_ratio_f")
            h2o = _values(rows, f"{prefix}_h2o_mmol")
            chamber = _values(rows, f"{prefix}_chamber_temp_c")
            pressure = _values(rows, f"{prefix}_pressure_kpa")
            dew = _values(rows, "dewpoint_c")
            tail_co2 = _tail(co2, cfg.tail_fraction)
            tail_ratio = _tail(ratio, cfg.tail_fraction)
            mean_co2 = _mean(co2)
            rel_error = (
                abs(float(mean_co2) - float(target)) / float(target) * 100.0
                if mean_co2 is not None and target
                else None
            )
            full_ratio_span = _span(ratio)
            tail_ratio_span = _span(tail_ratio)
            full_co2_span = _span(co2)
            tail_co2_span = _span(tail_co2)
            fit_role, reason = _classify(
                target=float(target),
                mean_co2=mean_co2,
                full_ratio_span=full_ratio_span,
                tail_ratio_span=tail_ratio_span,
                full_co2_span=full_co2_span,
                tail_co2_span=tail_co2_span,
                n=len(co2),
                cfg=cfg,
            )
            run_statuses.append(fit_role)
            point_rows.append(
                {
                    "sample_csv": str(path),
                    "run_label": run_label,
                    "device_id": dev,
                    "target_ppm": float(target),
                    "sample_count": len(co2),
                    "co2_mean_ppm": mean_co2,
                    "co2_first_ppm": co2[0] if co2 else "",
                    "co2_last_ppm": co2[-1] if co2 else "",
                    "co2_full_span_ppm": full_co2_span,
                    "co2_tail_span_ppm": tail_co2_span,
                    "co2_slope_ppm_per_sample": _linear_slope(co2),
                    "relative_error_pct": rel_error,
                    "ratio_mean": _mean(ratio),
                    "ratio_first": ratio[0] if ratio else "",
                    "ratio_last": ratio[-1] if ratio else "",
                    "ratio_full_span": full_ratio_span,
                    "ratio_tail_span": tail_ratio_span,
                    "ratio_slope_per_sample": _linear_slope(ratio),
                    "h2o_mmol_mean": _mean(h2o),
                    "dewpoint_c_mean": _mean(dew),
                    "dewpoint_c_first": dew[0] if dew else "",
                    "dewpoint_c_last": dew[-1] if dew else "",
                    "chamber_temp_c_mean": _mean(chamber),
                    "pressure_kpa_mean": _mean(pressure),
                    "fit_role_recommendation": fit_role,
                    "reason": reason,
                }
            )
        if prefixes:
            status = (
                "all_fit_eligible"
                if all(value == "fit_eligible" for value in run_statuses)
                else "contains_unfit_low_point_evidence"
            )
            run_rows.append(
                {
                    "sample_csv": str(path),
                    "run_label": run_label,
                    "target_ppm": float(target),
                    "device_count": len(prefixes),
                    "run_low_point_status": status,
                    "fit_eligible_count": sum(value == "fit_eligible" for value in run_statuses),
                    "diagnostic_only_count": sum(value == "diagnostic_only" for value in run_statuses),
                    "reject_count": sum(value == "reject" for value in run_statuses),
                }
            )
    return {"run_summary": run_rows, "device_low_point_diagnostics": point_rows}


def write_co2_low_point_stability_report(
    *,
    sample_csv_paths: Iterable[str | Path],
    output_dir: str | Path,
    cfg: Co2LowPointStabilityConfig = Co2LowPointStabilityConfig(),
) -> Dict[str, Path]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    tables = build_co2_low_point_stability_tables(sample_csv_paths, cfg=cfg)
    paths = {
        "run_summary": output / "co2_low_point_stability_run_summary.csv",
        "device_low_point_diagnostics": output / "co2_low_point_stability_device_diagnostics.csv",
        "metadata": output / "co2_low_point_stability_meta.json",
        "markdown": output / "co2_low_point_stability_review.md",
    }
    _write_csv(paths["run_summary"], tables["run_summary"])
    _write_csv(paths["device_low_point_diagnostics"], tables["device_low_point_diagnostics"])
    paths["metadata"].write_text(
        json.dumps(
            {
                "tool": "co2_low_point_stability",
                "created_at": _now(),
                "config": cfg.__dict__,
                "opens_com_ports": False,
                "controls_water_or_gas_routes": False,
                "writes_coefficients": False,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    _write_markdown(paths["markdown"], tables)
    return paths


def _fmt(value: Any) -> str:
    numeric = _safe_float(value)
    return "" if numeric is None else f"{numeric:.6g}"


def _write_markdown(path: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> None:
    lines = [
        "# V1.5 CO2 Low-Point Stability Review",
        "",
        "Offline/no-write diagnostic. It does not open COM ports, control routes, or write SENCO.",
        "",
        "## Device Diagnostics",
        "",
        "| Run | Device | Target ppm | Mean ppm | Error % | CO2 Span | Tail CO2 Span | R Span | Tail R Span | Role | Reason |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |",
    ]
    for row in tables.get("device_low_point_diagnostics", []):
        lines.append(
            "| {run} | {device} | {target} | {mean} | {err} | {span} | {tail_span} | {rspan} | {rtail} | {role} | {reason} |".format(
                run=row.get("run_label", ""),
                device=row.get("device_id", ""),
                target=_fmt(row.get("target_ppm")),
                mean=_fmt(row.get("co2_mean_ppm")),
                err=_fmt(row.get("relative_error_pct")),
                span=_fmt(row.get("co2_full_span_ppm")),
                tail_span=_fmt(row.get("co2_tail_span_ppm")),
                rspan=_fmt(row.get("ratio_full_span")),
                rtail=_fmt(row.get("ratio_tail_span")),
                role=row.get("fit_role_recommendation", ""),
                reason=row.get("reason", ""),
            )
        )
    lines.extend(
        [
            "",
            "## Physical Meaning",
            "",
            "- A low CO2 point can enter SENCO1 fitting only after the full purge/sample history and the tail window both support a stable physical state.",
            "- If the full history moves but the tail is stable, the point is diagnostic-only until the flow sequence proves it is repeatable.",
            "- If a stable low point remains biased, review source gas, route mixing, leak/dilution, or the lower-chain model before using a final-output affine trim.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
