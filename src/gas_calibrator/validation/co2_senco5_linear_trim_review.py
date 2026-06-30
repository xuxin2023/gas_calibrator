"""No-write CO2 SENCO5 final output-layer candidate review.

SENCO5 is treated as the firmware final CO2 concentration affine layer:

    corrected_co2_ppm = measured_co2_ppm * C1 + C0

This module consumes already-recorded open-flow verification summaries. It does
not open COM ports, control gas/water routes, or write coefficients.
"""

from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import numpy as np


@dataclass(frozen=True)
class Co2Senco5LinearTrimConfig:
    acceptance_pct: float = 1.0
    min_points: int = 4
    target_device_ids: Tuple[str, ...] = ("022", "030", "033", "051")
    exclude_device_ids: Tuple[str, ...] = ()
    command_c0_decimals: int = 1
    command_c1_decimals: int = 1
    max_abs_c0_ppm: float = 100.0
    max_abs_c1_delta: float = 0.10
    command_c1_min: float = 0.0
    command_c1_max: float = 2.0


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "None", "null"):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _read_rows(path: str | Path) -> List[Dict[str, Any]]:
    source = Path(path)
    with source.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    keys: List[str] = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def _fit_linear_trim(rows: Sequence[Mapping[str, Any]]) -> Tuple[float, float]:
    measured = np.asarray([float(row["_measured"]) for row in rows], dtype=float)
    target = np.asarray([float(row["_target"]) for row in rows], dtype=float)
    matrix = np.column_stack([np.ones_like(measured), measured])
    c0, c1 = np.linalg.lstsq(matrix, target, rcond=None)[0]
    return float(c0), float(c1)


def _command_number(value: float, decimals: int) -> str:
    return f"{float(value):.{int(decimals)}f}"


def _quantize(value: float, decimals: int) -> float:
    return float(_command_number(float(value), int(decimals)))


def _fit_quantized_command_trim(
    rows: Sequence[Mapping[str, Any]],
    *,
    c0_decimals: int,
    c1_decimals: int,
    c1_min: float,
    c1_max: float,
) -> Tuple[float, float, float, float, float]:
    """Find the best writable C0/C1 under fixed decimal precision.

    The firmware command is the final affine layer, not a raw-ratio fit. When
    C1 is constrained to a fixed decimal precision, directly rounding the continuous least
    squares solution can discard the multiplier correction. For each writable
    C1, the best C0 is a one-dimensional weighted Chebyshev problem. We solve
    that continuous problem first, then test only nearby writable C0 values
    plus a few absolute-error anchors. This keeps three-decimal,
    full-temperature reviews fast without changing the replayed error
    objective.
    """

    step = 10 ** -max(0, int(c1_decimals))
    start = int(math.ceil(float(c1_min) / step))
    stop = int(math.floor(float(c1_max) / step))
    best: Tuple[float, float, float, float, float] | None = None
    for index in range(start, stop + 1):
        c1 = _quantize(index * step, c1_decimals)
        for c0 in _candidate_c0_values(rows, c1=c1, c0_decimals=c0_decimals):
            max_abs_pct, max_abs_ppm, rmse_ppm = _residual_metrics(rows, c0=c0, c1=c1)
            score = (max_abs_pct, max_abs_ppm, rmse_ppm, abs(c1 - 1.0), abs(c0))
            if best is None or score < (best[2], best[3], best[4], abs(best[1] - 1.0), abs(best[0])):
                best = (c0, c1, max_abs_pct, max_abs_ppm, rmse_ppm)
    if best is None:
        raise ValueError("no writable SENCO5 C0/C1 candidates were generated")
    return best


def _candidate_c0_values(
    rows: Sequence[Mapping[str, Any]],
    *,
    c1: float,
    c0_decimals: int,
) -> Tuple[float, ...]:
    c0_step = 10 ** -max(0, int(c0_decimals))
    desired_offsets = [float(row["_target"]) - float(row["_measured"]) * float(c1) for row in rows]
    raw_candidates = set(desired_offsets)
    if desired_offsets:
        raw_candidates.add(sum(desired_offsets) / len(desired_offsets))
        raw_candidates.add((min(desired_offsets) + max(desired_offsets)) / 2.0)
    chebyshev = _continuous_percent_chebyshev_c0(rows, c1=c1)
    if chebyshev is not None:
        raw_candidates.add(chebyshev)

    quantized = set()
    for value in raw_candidates:
        anchor = math.floor(float(value) / c0_step)
        for delta in range(-3, 4):
            quantized.add(_quantize((anchor + delta) * c0_step, c0_decimals))
    return tuple(sorted(quantized))


def _continuous_percent_chebyshev_c0(
    rows: Sequence[Mapping[str, Any]],
    *,
    c1: float,
) -> float | None:
    constraints: List[Tuple[float, float]] = []
    for row in rows:
        target = abs(float(row["_target"]))
        if target <= 0.0:
            continue
        desired = float(row["_target"]) - float(row["_measured"]) * float(c1)
        constraints.append((desired, target))
    if not constraints:
        return None

    def feasible(error_pct: float) -> Tuple[bool, float, float]:
        lower = -math.inf
        upper = math.inf
        for desired, target in constraints:
            width = target * float(error_pct) / 100.0
            lower = max(lower, desired - width)
            upper = min(upper, desired + width)
        return lower <= upper, lower, upper

    high = 1.0
    ok, lower, upper = feasible(high)
    while not ok and high < 1.0e9:
        high *= 2.0
        ok, lower, upper = feasible(high)
    if not ok:
        return None
    low = 0.0
    for _ in range(48):
        mid = (low + high) / 2.0
        ok, lower, upper = feasible(mid)
        if ok:
            high = mid
        else:
            low = mid
    _, lower, upper = feasible(high)
    return (lower + upper) / 2.0


def _residual_metrics(
    rows: Sequence[Mapping[str, Any]],
    *,
    c0: float,
    c1: float,
) -> Tuple[float, float, float]:
    errors_ppm: List[float] = []
    errors_pct: List[float] = []
    for row in rows:
        target = float(row["_target"])
        corrected = float(row["_measured"]) * float(c1) + float(c0)
        error_ppm = corrected - target
        error_pct = error_ppm / target * 100.0 if target else 0.0
        errors_ppm.append(error_ppm)
        errors_pct.append(error_pct)
    max_abs_pct = max(abs(value) for value in errors_pct) if errors_pct else 0.0
    max_abs_ppm = max(abs(value) for value in errors_ppm) if errors_ppm else 0.0
    rmse_ppm = float(math.sqrt(sum(value * value for value in errors_ppm) / len(errors_ppm))) if errors_ppm else 0.0
    return max_abs_pct, max_abs_ppm, rmse_ppm


def build_co2_senco5_linear_trim_review(
    *,
    verification_summary_csv: str | Path,
    cfg: Co2Senco5LinearTrimConfig = Co2Senco5LinearTrimConfig(),
) -> Dict[str, List[Dict[str, Any]]]:
    """Build no-write SENCO5 review tables from CO2 candidate evidence."""

    raw_rows = _read_rows(verification_summary_csv)
    target_devices = {_device_id(value) for value in cfg.target_device_ids}
    excluded_devices = {_device_id(value) for value in cfg.exclude_device_ids}
    by_device: Dict[str, List[Dict[str, Any]]] = {}
    rejected: List[Dict[str, Any]] = []

    for row in raw_rows:
        dev = _device_id(row.get("device_id") or row.get("analyzer_device_id") or row.get("device"))
        target = _safe_float(
            row.get("certificate_co2_ppm")
            or row.get("standard_gas_certificate_value_ppm")
            or row.get("standard_value")
            or row.get("target_co2_ppm")
            or row.get("target_ppm")
        )
        measured = _safe_float(
            row.get("measured_co2_ppm")
            or row.get("measured_value")
            or row.get("co2_ppm")
            or row.get("co2_mean_ppm")
        )
        status = str(row.get("point_status") or "").strip().lower()
        reasons: List[str] = []
        if dev in excluded_devices:
            reasons.append("device_excluded")
        if target_devices and dev not in target_devices:
            reasons.append("device_not_in_target_set")
        if status not in {"", "ok", "pass"}:
            reasons.append(f"point_status_{status}")
        if target is None:
            reasons.append("target_missing")
        if measured is None:
            reasons.append("measured_co2_missing")
        if reasons:
            rejected.append(
                {
                    "device_id": dev,
                    "point_run_id": row.get("point_run_id") or row.get("point") or "",
                    "source_nominal_ppm": row.get("source_nominal_ppm") or row.get("point") or "",
                    "reject_reasons": ";".join(reasons),
                }
            )
            continue
        item = dict(row)
        item["_device_id"] = dev
        item["_target"] = float(target)
        item["_measured"] = float(measured)
        by_device.setdefault(dev, []).append(item)

    summary_rows: List[Dict[str, Any]] = []
    candidate_rows: List[Dict[str, Any]] = []
    residual_rows: List[Dict[str, Any]] = []
    acceptance_pct = float(cfg.acceptance_pct)

    for dev in sorted(target_devices):
        rows = by_device.get(dev, [])
        blockers: List[str] = []
        if len(rows) < int(cfg.min_points):
            blockers.append(f"points<{int(cfg.min_points)}")
        c0 = c1 = None
        payload_c0 = payload_c1 = None
        max_abs_pct = ""
        max_abs_ppm = ""
        rmse_ppm = ""
        payload_max_abs_pct = ""
        payload_max_abs_ppm = ""
        payload_rmse_ppm = ""
        if not blockers:
            c0, c1 = _fit_linear_trim(rows)
            errors_ppm: List[float] = []
            errors_pct: List[float] = []
            for row in sorted(rows, key=lambda r: float(r["_target"])):
                corrected = float(row["_measured"]) * float(c1) + float(c0)
                error_ppm = corrected - float(row["_target"])
                error_pct = error_ppm / float(row["_target"]) * 100.0 if float(row["_target"]) else 0.0
                errors_ppm.append(error_ppm)
                errors_pct.append(error_pct)
            max_abs_pct = max(abs(value) for value in errors_pct) if errors_pct else ""
            max_abs_ppm = max(abs(value) for value in errors_ppm) if errors_ppm else ""
            rmse_ppm = float(math.sqrt(sum(value * value for value in errors_ppm) / len(errors_ppm))) if errors_ppm else ""
            payload_c0, payload_c1, payload_max_abs_pct, payload_max_abs_ppm, payload_rmse_ppm = _fit_quantized_command_trim(
                rows,
                c0_decimals=int(cfg.command_c0_decimals),
                c1_decimals=int(cfg.command_c1_decimals),
                c1_min=float(cfg.command_c1_min),
                c1_max=float(cfg.command_c1_max),
            )
            if payload_max_abs_pct != "" and float(payload_max_abs_pct) > acceptance_pct:
                blockers.append(f"max_abs_error_pct>{acceptance_pct:g}")
            if abs(float(payload_c0)) > float(cfg.max_abs_c0_ppm):
                blockers.append("senco5_c0_exceeds_final_trim_scope")
            if abs(float(payload_c1) - 1.0) > float(cfg.max_abs_c1_delta):
                blockers.append("senco5_c1_exceeds_final_trim_scope")
            for row in sorted(rows, key=lambda r: float(r["_target"])):
                target = float(row["_target"])
                measured = float(row["_measured"])
                continuous_corrected = measured * float(c1) + float(c0)
                continuous_error_ppm = continuous_corrected - target
                continuous_error_pct = continuous_error_ppm / target * 100.0 if target else 0.0
                payload_corrected = measured * float(payload_c1) + float(payload_c0)
                payload_error_ppm = payload_corrected - target
                payload_error_pct = payload_error_ppm / target * 100.0 if target else 0.0
                residual_rows.append(
                    {
                        "device_id": dev,
                        "point_run_id": row.get("point_run_id") or row.get("point") or "",
                        "source_nominal_ppm": row.get("source_nominal_ppm") or row.get("point") or "",
                        "certificate_co2_ppm": target,
                        "measured_co2_ppm": measured,
                        "candidate_C0": c0,
                        "candidate_C1": c1,
                        "continuous_C0": c0,
                        "continuous_C1": c1,
                        "continuous_corrected_co2_ppm": continuous_corrected,
                        "continuous_error_ppm": continuous_error_ppm,
                        "continuous_error_pct": continuous_error_pct,
                        "payload_C0": payload_c0,
                        "payload_C1": payload_c1,
                        "corrected_co2_ppm": payload_corrected,
                        "error_ppm": payload_error_ppm,
                        "error_pct": payload_error_pct,
                        "status": "pass" if abs(payload_error_pct) <= acceptance_pct else "fail",
                    }
                )

        status = "review_ready" if not blockers else "blocked"
        summary_rows.append(
            {
                "device_id": dev,
                "candidate_status": status,
                "point_count": len(rows),
                "candidate_C0": "" if c0 is None else c0,
                "candidate_C1": "" if c1 is None else c1,
                "max_abs_error_pct": max_abs_pct,
                "max_abs_error_ppm": max_abs_ppm,
                "rmse_ppm": rmse_ppm,
                "payload_C0": "" if payload_c0 is None else payload_c0,
                "payload_C1": "" if payload_c1 is None else payload_c1,
                "payload_max_abs_error_pct": payload_max_abs_pct,
                "payload_max_abs_error_ppm": payload_max_abs_ppm,
                "payload_rmse_ppm": payload_rmse_ppm,
                "one_decimal_C0_C1_max_abs_error_pct": payload_max_abs_pct,
                "one_decimal_C0_C1_max_abs_error_ppm": payload_max_abs_ppm,
                "acceptance_pct": acceptance_pct,
                "blocked_reasons": ";".join(blockers),
                "auto_write_allowed": False,
                "requires_controlled_write_review": True,
                "fit_contract_stage": "integrated_firmware_output_candidate",
                "candidate_package_role": "senco5_final_output_layer_with_senco13",
                "not_ad_hoc_post_acceptance_repair": True,
                "physical_scope": "CO2 final output concentration affine layer",
            }
        )
        if c0 is not None and c1 is not None:
            write_c0 = float(payload_c0 if payload_c0 is not None else _quantize(c0, cfg.command_c0_decimals))
            write_c1 = float(payload_c1 if payload_c1 is not None else _quantize(c1, cfg.command_c1_decimals))
            c0_text = _command_number(write_c0, cfg.command_c0_decimals)
            c1_text = _command_number(write_c1, cfg.command_c1_decimals)
            candidate_rows.append(
                {
                    "device_id": dev,
                    "senco_group": "SENCO5",
                    "C0": write_c0,
                    "C1": write_c1,
                    "continuous_C0": c0,
                    "continuous_C1": c1,
                    "command_preview": f"SENCO5,YGAS,FFF,{c0_text},{c1_text}",
                    "one_decimal_C0_C1_command_preview": f"SENCO5,YGAS,FFF,{write_c0:.1f},{write_c1:.1f}",
                    "one_decimal_C0_C1_max_abs_error_pct": payload_max_abs_pct,
                    "payload_max_abs_error_ppm": payload_max_abs_ppm,
                    "decimal_write_contract": "C0/C1 decimal values; no scientific notation; payload is optimized under the writable decimal precision",
                    "write_target_note": "Command preview is per verified serial port after binding the analyzer device_id; do not broadcast one shared C0/C1 set across multiple analyzers.",
                    "fit_contract_stage": "integrated_firmware_output_candidate",
                    "candidate_package_role": "senco5_final_output_layer_with_senco13",
                    "not_ad_hoc_post_acceptance_repair": True,
                    "candidate_status": status,
                    "auto_write_allowed": False,
                }
            )

    run_status = "pass" if summary_rows and all(row["candidate_status"] == "review_ready" for row in summary_rows) else "blocked"
    run_summary = [
        {
            "created_at": _now(),
            "source_csv": str(Path(verification_summary_csv).resolve()),
            "target_device_ids": ";".join(sorted(target_devices)),
            "excluded_device_ids": ";".join(sorted(excluded_devices)),
            "acceptance_pct": acceptance_pct,
            "run_status": run_status,
            "fit_contract_stage": "integrated_firmware_output_candidate",
            "candidate_package_role": "senco5_final_output_layer_with_senco13",
            "not_ad_hoc_post_acceptance_repair": True,
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
        }
    ]

    return {
        "run_summary": run_summary,
        "candidate_summary": summary_rows,
        "candidate_coefficients": candidate_rows,
        "candidate_residuals": residual_rows,
        "rejected_rows": rejected,
    }


def write_co2_senco5_linear_trim_review(
    *,
    verification_summary_csv: str | Path,
    output_dir: str | Path,
    cfg: Co2Senco5LinearTrimConfig = Co2Senco5LinearTrimConfig(),
) -> Dict[str, Path]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    tables = build_co2_senco5_linear_trim_review(verification_summary_csv=verification_summary_csv, cfg=cfg)
    paths = {
        "run_summary": output / "co2_senco5_linear_trim_run_summary.csv",
        "candidate_summary": output / "co2_senco5_linear_trim_candidate_summary.csv",
        "candidate_coefficients": output / "co2_senco5_linear_trim_candidate_coefficients.csv",
        "candidate_residuals": output / "co2_senco5_linear_trim_candidate_residuals.csv",
        "rejected_rows": output / "co2_senco5_linear_trim_rejected_rows.csv",
        "metadata": output / "co2_senco5_linear_trim_meta.json",
        "markdown": output / "co2_senco5_linear_trim_review.md",
    }
    for key in ("run_summary", "candidate_summary", "candidate_coefficients", "candidate_residuals", "rejected_rows"):
        _write_csv(paths[key], tables[key])
    paths["metadata"].write_text(
        json.dumps(
            {
                "tool": "co2_senco5_linear_trim_review",
                "created_at": _now(),
                "source_csv": str(Path(verification_summary_csv).resolve()),
                "output_dir": str(output.resolve()),
                "config": {
                    "acceptance_pct": cfg.acceptance_pct,
                    "min_points": cfg.min_points,
                    "target_device_ids": list(cfg.target_device_ids),
                    "exclude_device_ids": list(cfg.exclude_device_ids),
                    "command_c0_decimals": cfg.command_c0_decimals,
                    "command_c1_decimals": cfg.command_c1_decimals,
                    "command_c1_min": cfg.command_c1_min,
                    "command_c1_max": cfg.command_c1_max,
                    "max_abs_c0_ppm": cfg.max_abs_c0_ppm,
                    "max_abs_c1_delta": cfg.max_abs_c1_delta,
                    "fit_contract_stage": "integrated_firmware_output_candidate",
                    "candidate_package_role": "senco5_final_output_layer_with_senco13",
                    "not_ad_hoc_post_acceptance_repair": True,
                },
                "opens_com_ports": False,
                "controls_water_or_gas_routes": False,
                "writes_coefficients": False,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    _write_markdown(paths["markdown"], tables)
    return paths


def _write_markdown(path: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> None:
    lines = [
        "# V1.5 CO2 SENCO5 Final Output-Layer Review",
        "",
        "Offline/no-write review of the final CO2 concentration output layer: `corrected = measured*C1 + C0`.",
        "This artifact does not open COM ports, control routes, or write SENCO.",
        "SENCO5 is treated as part of the same CO2 candidate coefficient package, not as an ad-hoc repair after acceptance.",
        "",
        "## Device Summary",
        "",
        "| Device | Status | Points | Payload C0 | Payload C1 | Payload Max Error % | Payload Max Error ppm | Blockers |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in tables.get("candidate_summary", []):
        lines.append(
            "| {device} | {status} | {points} | {c0} | {c1} | {pct} | {ppm} | {blockers} |".format(
                device=row.get("device_id", ""),
                status=row.get("candidate_status", ""),
                points=row.get("point_count", ""),
                c0=_fmt(row.get("payload_C0")),
                c1=_fmt(row.get("payload_C1")),
                pct=_fmt(row.get("payload_max_abs_error_pct")),
                ppm=_fmt(row.get("payload_max_abs_error_ppm")),
                blockers=row.get("blocked_reasons", ""),
            )
        )
    lines.extend(
        [
            "",
            "## Physical Meaning",
            "",
            "- SENCO5 is not a raw optical-ratio coefficient. It is the final concentration affine layer.",
            "- It may only correct residual output concentration bias inside a controlled SENCO1/SENCO3/SENCO5 candidate package.",
            "- It must not be used to hide unstable gas, humidity, pressure, temperature, or route evidence.",
            "- The firmware command is evaluated at the configured writable decimal precision, so this review optimizes the actual C0/C1 payload instead of merely rounding a continuous fit.",
            "- Command previews assume one analyzer per verified serial port; different devices require their own C0/C1 values.",
            "- Any real write still requires old GETCO5 backup, controlled write review, readback, rollback plan, and independent post-write verification.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _fmt(value: Any) -> str:
    numeric = _safe_float(value)
    if numeric is None:
        return ""
    return f"{numeric:.6g}"
