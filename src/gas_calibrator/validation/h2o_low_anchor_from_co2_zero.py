"""Build H2O low-end anchor evidence from CO2 zero-gas route points.

This module is offline-only. It consumes completed V1.5 CO2 0 ppm point
artifacts and extracts the H2O ratio, dewpoint-derived residual H2O target,
pressure, and analyzer chamber temperature needed by the new absorption-ratio
H2O fit. It never opens COM ports, controls routes, or writes coefficients.
"""

from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

from .h2o_senco24_candidate_review import (
    _dry_anchor_rows,
    _normal_device_id,
    _resolve_dry_anchor_root,
    _safe_float,
)


@dataclass(frozen=True)
class H2OLowAnchorFromCO2ZeroConfig:
    """Policy for low-end H2O anchor extraction from gas-route 0 ppm points."""

    max_residual_h2o_mmol: float = 0.5
    max_dewpoint_c: float = -30.0
    min_distinct_temperatures: int = 3
    preferred_distinct_temperatures: int = 5


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers: List[str] = []
    for row in rows:
        for key in row:
            if key not in headers:
                headers.append(str(key))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows([dict(row) for row in rows])


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8-sig")


def _pressure_kpa(row: Mapping[str, Any]) -> Optional[float]:
    analyzer_pressure = _safe_float(row.get("analyzer_pressure_kpa"))
    if analyzer_pressure is not None:
        return float(analyzer_pressure)
    pressure_hpa = _safe_float(row.get("reference_pressure_hpa"))
    if pressure_hpa is not None:
        return float(pressure_hpa) / 10.0
    sample_pressure = _safe_float(row.get("sample_analyzer_pressure_kpa"))
    if sample_pressure is not None:
        return float(sample_pressure)
    return None


def _anchor_status(row: Mapping[str, Any], cfg: H2OLowAnchorFromCO2ZeroConfig) -> tuple[str, str]:
    blockers: List[str] = []
    warnings: List[str] = []
    if _safe_float(row.get("h2o_ratio_f")) is None:
        blockers.append("missing_h2o_ratio_f")
    if _safe_float(row.get("reference_h2o_mmol")) is None:
        blockers.append("missing_dewpoint_pressure_residual_h2o")
    if _safe_float(row.get("reference_dewpoint_c")) is None:
        blockers.append("missing_dewpoint_c")
    if _pressure_kpa(row) is None:
        blockers.append("missing_pressure_kpa")
    if _safe_float(row.get("chamber_temp_c")) is None:
        blockers.append("missing_analyzer_chamber_temperature")

    residual = _safe_float(row.get("reference_h2o_mmol"))
    if residual is not None and residual > float(cfg.max_residual_h2o_mmol):
        warnings.append("residual_h2o_above_low_anchor_limit")
    dewpoint = _safe_float(row.get("reference_dewpoint_c"))
    if dewpoint is not None and dewpoint > float(cfg.max_dewpoint_c):
        warnings.append("dewpoint_above_low_anchor_limit")

    if blockers:
        return "blocked_missing_required_evidence", ";".join(blockers + warnings)
    if warnings:
        return "qc_only_low_anchor_limit_warning", ";".join(warnings)
    return "fit_ready_low_anchor", ""


def build_h2o_low_anchor_from_co2_zero_tables(
    *,
    co2_zero_run_dirs: Sequence[str | Path],
    cfg: H2OLowAnchorFromCO2ZeroConfig = H2OLowAnchorFromCO2ZeroConfig(),
) -> Dict[str, Any]:
    """Return machine-readable H2O low-end anchor tables from CO2 zero-gas points."""

    raw_rows: List[Dict[str, Any]] = []
    resolved_roots = [str(_resolve_dry_anchor_root(path)) for path in co2_zero_run_dirs]
    for root in resolved_roots:
        raw_rows.extend(_dry_anchor_rows(Path(root)))

    anchor_rows: List[Dict[str, Any]] = []
    for row in raw_rows:
        ratio = _safe_float(row.get("h2o_ratio_f"))
        pressure_kpa = _pressure_kpa(row)
        chamber_temp = _safe_float(row.get("chamber_temp_c"))
        status, reason = _anchor_status(row, cfg)
        pressure_norm = pressure_kpa / 100.0 if pressure_kpa is not None else None
        ln_ratio = math.log(float(ratio)) if ratio is not None and ratio > 0 else None
        anchor_rows.append(
            {
                "component": "h2o",
                "anchor_role": "h2o_low_anchor_from_co2_zero_gas",
                "anchor_status": status,
                "anchor_status_reason": reason,
                "analyzer_prefix": row.get("analyzer_prefix", ""),
                "analyzer_device_id": _normal_device_id(row.get("analyzer_device_id")),
                "co2_zero_point_run_id": row.get("point_run_id", ""),
                "co2_zero_temp_set_c": row.get("temp_set_c", ""),
                "h2o_ratio_f": ratio if ratio is not None else "",
                "ln_h2o_ratio_f": ln_ratio if ln_ratio is not None else "",
                "residual_h2o_mmol_from_dewpoint_pressure": row.get("reference_h2o_mmol", ""),
                "dewpoint_c": row.get("reference_dewpoint_c", ""),
                "pressure_kpa": pressure_kpa if pressure_kpa is not None else "",
                "pressure_norm_p_over_100kpa": pressure_norm if pressure_norm is not None else "",
                "chamber_temp_c": chamber_temp if chamber_temp is not None else "",
                "chamber_temp_k": chamber_temp + 273.15 if chamber_temp is not None else "",
                "not_forced_to_zero": True,
                "r0_equation_basis": (
                    "ln(R_H2O)=ln(R0_H2O(T))-k(T)*H2O_residual*(P_kPa/100)"
                ),
                "r0_low_water_proxy": ratio if ratio is not None else "",
                "source_root": row.get("dry_anchor_source_root", ""),
                "source_summary_file": row.get("summary_file", ""),
                "sample_alignment_status": row.get("sample_alignment_status", ""),
            }
        )

    summary_rows: List[Dict[str, Any]] = []
    device_ids = sorted({_normal_device_id(row.get("analyzer_device_id")) for row in anchor_rows})
    for device_id in device_ids:
        if not device_id:
            continue
        device_rows = [row for row in anchor_rows if row.get("analyzer_device_id") == device_id]
        ready_rows = [row for row in device_rows if row.get("anchor_status") == "fit_ready_low_anchor"]
        ready_temps = sorted(
            {
                float(row["co2_zero_temp_set_c"])
                for row in ready_rows
                if _safe_float(row.get("co2_zero_temp_set_c")) is not None
            }
        )
        if len(ready_temps) >= int(cfg.preferred_distinct_temperatures):
            recommendation = "ready_for_new_algorithm_r0_h2o_fit"
        elif len(ready_temps) >= int(cfg.min_distinct_temperatures):
            recommendation = "minimum_ready_collect_more_temperature_span"
        else:
            recommendation = "collect_more_co2_zero_low_anchor_evidence"
        summary_rows.append(
            {
                "component": "h2o",
                "analyzer_device_id": device_id,
                "anchor_count": len(device_rows),
                "fit_ready_anchor_count": len(ready_rows),
                "fit_ready_distinct_temperature_count": len(ready_temps),
                "fit_ready_temperatures_c": ";".join(f"{temp:g}" for temp in ready_temps),
                "recommendation": recommendation,
            }
        )

    manifest = {
        "tool_name": "export_v1_5_h2o_low_anchor_from_co2_zero",
        "created_at": _now(),
        "no_write": True,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "source_contract": (
            "Use CO2 0 ppm gas-route points as H2O low-end anchors only when "
            "R_H2O, dewpoint-derived residual H2O, pressure, and analyzer chamber "
            "temperature are all present. Residual water is never forced to zero."
        ),
        "co2_zero_run_dirs": resolved_roots,
        "max_residual_h2o_mmol": cfg.max_residual_h2o_mmol,
        "max_dewpoint_c": cfg.max_dewpoint_c,
        "min_distinct_temperatures": cfg.min_distinct_temperatures,
        "preferred_distinct_temperatures": cfg.preferred_distinct_temperatures,
    }
    return {
        "manifest": manifest,
        "h2o_low_anchor_inputs": anchor_rows,
        "h2o_low_anchor_device_summary": summary_rows,
    }


def write_h2o_low_anchor_from_co2_zero_review(
    *,
    co2_zero_run_dirs: Sequence[str | Path],
    output_dir: str | Path,
    cfg: H2OLowAnchorFromCO2ZeroConfig = H2OLowAnchorFromCO2ZeroConfig(),
) -> Dict[str, str]:
    """Write H2O low-end anchor evidence extracted from CO2 zero-gas points."""

    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    tables = build_h2o_low_anchor_from_co2_zero_tables(
        co2_zero_run_dirs=co2_zero_run_dirs,
        cfg=cfg,
    )
    paths = {
        "manifest": output / "h2o_low_anchor_from_co2_zero_manifest.json",
        "h2o_low_anchor_inputs": output / "h2o_low_anchor_inputs.csv",
        "h2o_low_anchor_device_summary": output / "h2o_low_anchor_device_summary.csv",
    }
    _write_json(paths["manifest"], tables["manifest"])
    _write_csv(paths["h2o_low_anchor_inputs"], tables["h2o_low_anchor_inputs"])
    _write_csv(paths["h2o_low_anchor_device_summary"], tables["h2o_low_anchor_device_summary"])
    return {key: str(path) for key, path in paths.items()}
