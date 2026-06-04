"""No-write H2O SENCO6 final output-layer candidate review.

SENCO6 is treated as the firmware final H2O concentration affine layer:

    corrected_h2o_mmol_mol = measured_h2o_mmol_mol * C1 + C0

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
from typing import Any, Dict, List, Mapping, Sequence, Tuple

import numpy as np


@dataclass(frozen=True)
class H2oSenco6LinearTrimConfig:
    acceptance_pct: float = 2.0
    min_points: int = 2
    target_device_ids: Tuple[str, ...] = ("022", "030", "033", "051")
    exclude_device_ids: Tuple[str, ...] = ("100",)
    command_c0_decimals: int = 1
    command_c1_decimals: int = 1
    max_abs_c0_mmol: float = 2.0
    max_abs_c1_delta: float = 0.15
    command_c1_min: float = 0.0
    command_c1_max: float = 2.0


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _safe_float(value: Any) -> float | None:
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
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
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


def _metrics(rows: Sequence[Mapping[str, Any]], *, c0: float, c1: float) -> Tuple[float, float, float]:
    errors_mmol: List[float] = []
    errors_pct: List[float] = []
    for row in rows:
        target = float(row["_target"])
        corrected = float(row["_measured"]) * float(c1) + float(c0)
        error = corrected - target
        errors_mmol.append(error)
        if target:
            errors_pct.append(error / target * 100.0)
    max_abs_pct = max(abs(value) for value in errors_pct) if errors_pct else 0.0
    max_abs_mmol = max(abs(value) for value in errors_mmol) if errors_mmol else 0.0
    rmse_mmol = math.sqrt(sum(value * value for value in errors_mmol) / len(errors_mmol)) if errors_mmol else 0.0
    return float(max_abs_pct), float(max_abs_mmol), float(rmse_mmol)


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

    For each writable C1, the best C0 is a one-dimensional weighted Chebyshev
    problem. We solve that continuous problem first, then test only nearby
    writable C0 values plus a few absolute-error anchors. This keeps
    three-decimal, full-temperature reviews fast while preserving the replayed
    error objective.
    """

    step = 10 ** -max(0, int(c1_decimals))
    start = int(math.ceil(float(c1_min) / step))
    stop = int(math.floor(float(c1_max) / step))
    best: Tuple[float, float, float, float, float] | None = None
    for index in range(start, stop + 1):
        c1 = _quantize(index * step, c1_decimals)
        for c0 in _candidate_c0_values(rows, c1=c1, c0_decimals=c0_decimals):
            max_abs_pct, max_abs_mmol, rmse_mmol = _metrics(rows, c0=c0, c1=c1)
            score = (max_abs_pct, max_abs_mmol, rmse_mmol, abs(c1 - 1.0), abs(c0))
            if best is None or score < (best[2], best[3], best[4], abs(best[1] - 1.0), abs(best[0])):
                best = (c0, c1, max_abs_pct, max_abs_mmol, rmse_mmol)
    if best is None:
        raise ValueError("no writable SENCO6 C0/C1 candidates were generated")
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


def build_h2o_senco6_linear_trim_review(
    *,
    verification_summary_csv: str | Path,
    cfg: H2oSenco6LinearTrimConfig = H2oSenco6LinearTrimConfig(),
) -> Dict[str, List[Dict[str, Any]]]:
    raw_rows = _read_rows(verification_summary_csv)
    target_devices = {_device_id(value) for value in cfg.target_device_ids}
    excluded_devices = {_device_id(value) for value in cfg.exclude_device_ids}
    by_device: Dict[str, List[Dict[str, Any]]] = {}
    rejected: List[Dict[str, Any]] = []

    for row in raw_rows:
        dev = _device_id(row.get("device_id") or row.get("analyzer_device_id") or row.get("device"))
        target = _safe_float(
            row.get("reference_h2o_mmol")
            or row.get("target_h2o_mmol")
            or row.get("h2o_mmol_target")
            or row.get("ppm_H2O_Dew")
        )
        measured = _safe_float(row.get("measured_h2o_mmol") or row.get("h2o_mmol") or row.get("ppm_H2O"))
        reasons: List[str] = []
        if not dev:
            reasons.append("missing_device_id")
        if dev in excluded_devices:
            reasons.append("excluded_device")
        if target is None:
            reasons.append("missing_reference_h2o_mmol")
        if measured is None:
            reasons.append("missing_measured_h2o_mmol")
        if reasons:
            rejected.append({**row, "device_id": dev, "reject_reasons": ";".join(reasons)})
            continue
        if target_devices and dev not in target_devices:
            rejected.append({**row, "device_id": dev, "reject_reasons": "device_not_selected"})
            continue
        item = dict(row)
        item["device_id"] = dev
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
        c0 = c1 = None
        payload_c0 = payload_c1 = None
        max_abs_pct = max_abs_mmol = rmse_mmol = ""
        payload_max_abs_pct = payload_max_abs_mmol = payload_rmse_mmol = ""
        if len(rows) < int(cfg.min_points):
            blockers.append("insufficient_points")
        else:
            c0, c1 = _fit_linear_trim(rows)
            max_abs_pct, max_abs_mmol, rmse_mmol = _metrics(rows, c0=c0, c1=c1)
            payload_c0, payload_c1, payload_max_abs_pct, payload_max_abs_mmol, payload_rmse_mmol = _fit_quantized_command_trim(
                rows,
                c0_decimals=int(cfg.command_c0_decimals),
                c1_decimals=int(cfg.command_c1_decimals),
                c1_min=float(cfg.command_c1_min),
                c1_max=float(cfg.command_c1_max),
            )
            if payload_max_abs_pct > acceptance_pct:
                blockers.append("post_trim_error_exceeds_acceptance")
            if abs(float(payload_c0)) > float(cfg.max_abs_c0_mmol):
                blockers.append("senco6_c0_exceeds_final_trim_scope")
            if abs(float(payload_c1) - 1.0) > float(cfg.max_abs_c1_delta):
                blockers.append("senco6_c1_exceeds_final_trim_scope")
            for row in rows:
                corrected = float(row["_measured"]) * float(payload_c1) + float(payload_c0)
                target = float(row["_target"])
                error = corrected - target
                residual_rows.append(
                    {
                        "device_id": dev,
                        "point_run_id": row.get("point_run_id", ""),
                        "reference_h2o_mmol": target,
                        "measured_h2o_mmol": float(row["_measured"]),
                        "corrected_h2o_mmol": corrected,
                        "error_mmol": error,
                        "error_pct": error / target * 100.0 if target else "",
                        "candidate_C0": c0,
                        "candidate_C1": c1,
                        "payload_C0": payload_c0,
                        "payload_C1": payload_c1,
                        "reference_source": row.get("reference_source", "dewpoint_pressure_reference"),
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
                "max_abs_error_mmol": max_abs_mmol,
                "rmse_mmol": rmse_mmol,
                "payload_C0": "" if payload_c0 is None else payload_c0,
                "payload_C1": "" if payload_c1 is None else payload_c1,
                "payload_max_abs_error_pct": payload_max_abs_pct,
                "payload_max_abs_error_mmol": payload_max_abs_mmol,
                "payload_rmse_mmol": payload_rmse_mmol,
                "acceptance_pct": acceptance_pct,
                "blocked_reasons": ";".join(blockers),
                "auto_write_allowed": False,
                "requires_controlled_write_review": True,
                "fit_contract_stage": "integrated_firmware_output_candidate",
                "candidate_package_role": "senco6_final_output_layer_with_senco24",
                "physical_scope": "H2O final output concentration affine layer",
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
                    "senco_group": "SENCO6",
                    "C0": write_c0,
                    "C1": write_c1,
                    "continuous_C0": c0,
                    "continuous_C1": c1,
                    "command_preview": f"SENCO6,YGAS,FFF,{c0_text},{c1_text}",
                    "payload_max_abs_error_pct": payload_max_abs_pct,
                    "payload_max_abs_error_mmol": payload_max_abs_mmol,
                    "decimal_write_contract": "C0/C1 decimal values; no scientific notation; payload is optimized under the writable decimal precision",
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
            "candidate_package_role": "senco6_final_output_layer_with_senco24",
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


def write_h2o_senco6_linear_trim_review(
    *,
    verification_summary_csv: str | Path,
    output_dir: str | Path,
    cfg: H2oSenco6LinearTrimConfig = H2oSenco6LinearTrimConfig(),
) -> Dict[str, Path]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    tables = build_h2o_senco6_linear_trim_review(verification_summary_csv=verification_summary_csv, cfg=cfg)
    paths = {
        "run_summary": output / "h2o_senco6_linear_trim_run_summary.csv",
        "candidate_summary": output / "h2o_senco6_linear_trim_candidate_summary.csv",
        "candidate_coefficients": output / "h2o_senco6_linear_trim_candidate_coefficients.csv",
        "candidate_residuals": output / "h2o_senco6_linear_trim_candidate_residuals.csv",
        "rejected_rows": output / "h2o_senco6_linear_trim_rejected_rows.csv",
        "metadata": output / "h2o_senco6_linear_trim_meta.json",
        "markdown": output / "h2o_senco6_linear_trim_review.md",
    }
    for key in ("run_summary", "candidate_summary", "candidate_coefficients", "candidate_residuals", "rejected_rows"):
        _write_csv(paths[key], tables[key])
    paths["metadata"].write_text(
        json.dumps(
            {
                "tool": "h2o_senco6_linear_trim_review",
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
                    "max_abs_c0_mmol": cfg.max_abs_c0_mmol,
                    "max_abs_c1_delta": cfg.max_abs_c1_delta,
                    "candidate_package_role": "senco6_final_output_layer_with_senco24",
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
    _write_markdown_payload(paths["markdown"], tables)
    return paths


def _fmt(value: Any) -> str:
    numeric = _safe_float(value)
    return "" if numeric is None else f"{numeric:.6g}"


def _write_markdown(path: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> None:
    summary = list(tables.get("run_summary") or [{}])[0]
    lines = [
        "# V1.5 H2O SENCO6 Linear Trim Review",
        "",
        f"- Run status: `{summary.get('run_status')}`",
        f"- Acceptance: `±{summary.get('acceptance_pct')}%`",
        f"- Opens COM ports: `{summary.get('opens_com_ports')}`",
        f"- Writes coefficients: `{summary.get('writes_coefficients')}`",
        "",
        "| Device | Status | C0 | C1 | Max abs % | Blockers |",
        "| --- | --- | ---: | ---: | ---: | --- |",
    ]
    for row in tables.get("candidate_summary") or []:
        lines.append(
            "| {dev} | {status} | {c0} | {c1} | {err} | {blockers} |".format(
                dev=row.get("device_id", ""),
                status=row.get("candidate_status", ""),
                c0=_fmt(row.get("candidate_C0")),
                c1=_fmt(row.get("candidate_C1")),
                err=_fmt(row.get("max_abs_error_pct")),
                blockers=row.get("blocked_reasons", ""),
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_markdown_payload(path: Path, tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> None:
    summary = list(tables.get("run_summary") or [{}])[0]
    lines = [
        "# V1.5 H2O SENCO6 Linear Trim Review",
        "",
        f"- Run status: `{summary.get('run_status')}`",
        f"- Acceptance: `+/-{summary.get('acceptance_pct')}%`",
        f"- Opens COM ports: `{summary.get('opens_com_ports')}`",
        f"- Writes coefficients: `{summary.get('writes_coefficients')}`",
        "",
        "| Device | Status | Payload C0 | Payload C1 | Payload Max abs % | Blockers |",
        "| --- | --- | ---: | ---: | ---: | --- |",
    ]
    for row in tables.get("candidate_summary") or []:
        lines.append(
            "| {dev} | {status} | {c0} | {c1} | {err} | {blockers} |".format(
                dev=row.get("device_id", ""),
                status=row.get("candidate_status", ""),
                c0=_fmt(row.get("payload_C0")),
                c1=_fmt(row.get("payload_C1")),
                err=_fmt(row.get("payload_max_abs_error_pct")),
                blockers=row.get("blocked_reasons", ""),
            )
        )
    lines.extend(
        [
            "",
            "## Physical Meaning",
            "",
            "- SENCO6 is the final H2O concentration affine layer: `corrected = measured*C1 + C0`.",
            "- It must be evaluated after SENCO2/SENCO4, and must not be mixed back into the raw H2O ratio fit.",
            "- The firmware command is evaluated at the configured writable decimal precision, so this review optimizes the actual C0/C1 payload.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
