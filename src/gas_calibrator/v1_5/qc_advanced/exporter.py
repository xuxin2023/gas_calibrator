"""Offline exporter for V1.5 advanced QC summaries.

The exporter reads existing CSV artifacts and writes advanced QC JSON/Markdown.
It does not open COM ports, control water/gas routes, control valves/PACE, or
write analyzer coefficients.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

from ...validation.artifact_rows import load_latest_sample_rows, normalize_sample_row
from ...validation.common import latest_artifact, load_csv_rows
from ._math import safe_float
from .control_charts import build_control_chart
from .factory_signal_health import evaluate_factory_signal_health
from .humidity_diagnostics import classify_humidity_behavior
from .pressure_trend import evaluate_pressure_trend
from .root_cause_classifier import classify_root_cause
from .steady_state_selector import select_steady_state_window


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _first_value(row: Mapping[str, Any], keys: Sequence[str]) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _float_value(row: Mapping[str, Any], keys: Sequence[str]) -> Optional[float]:
    return safe_float(_first_value(row, keys))


def _component(row: Mapping[str, Any]) -> str:
    text = " ".join(
        str(row.get(key) or "")
        for key in ("component", "point_phase", "route", "gas", "sample_role", "point_tag")
    ).lower()
    if "h2o" in text or "water" in text:
        return "h2o"
    if "co2" in text:
        return "co2"
    return "unknown"


def _is_open_flow(row: Mapping[str, Any]) -> bool:
    mode = str(row.get("pressure_mode") or row.get("PressureMode") or "").strip().lower()
    if not mode:
        return True
    return mode in {"ambient_open", "open_flow", "open_route", "atmosphere", "ambient"}


def normalize_advanced_qc_row(row: Mapping[str, Any], *, analyzer_prefix: str = "ga01") -> Dict[str, Any]:
    prefix = str(analyzer_prefix or "ga01")
    analyzer_kpa = _float_value(row, [f"{prefix}_pressure_kpa", "analyzer_pressure_kpa", "pressure_kpa"])
    analyzer_hpa = _float_value(row, [f"{prefix}_pressure_hpa", "analyzer_pressure_hpa"])
    if analyzer_hpa is None and analyzer_kpa is not None:
        analyzer_hpa = analyzer_kpa * 10.0
    com22_hpa = _float_value(
        row,
        [
            "com22_pressure_hpa",
            "pressure_gauge_hpa",
            "digital_pressure_hpa",
            "pressure_hpa",
            "reference_pressure_hpa",
        ],
    )
    pace_hpa = _float_value(row, ["pace_pressure_hpa", "controller_pressure", "controller_pressure_hpa"])
    h2o_mmol = _float_value(row, [f"{prefix}_h2o_mmol", "h2o_mmol"])
    h2o_dry_ppmv = _float_value(row, ["h2o_dry_ppmv", f"{prefix}_h2o_dry_ppmv"])
    h2o_wet_ppmv = _float_value(row, ["h2o_wet_ppmv", f"{prefix}_h2o_wet_ppmv"])
    if h2o_dry_ppmv is None and h2o_mmol is not None:
        h2o_dry_ppmv = h2o_mmol * 1000.0
    if h2o_wet_ppmv is None and h2o_mmol is not None:
        h2o_wet_ppmv = h2o_mmol * 1000.0
    analyzer_minus_com22 = _float_value(row, ["analyzer_minus_com22_hpa"])
    if analyzer_minus_com22 is None and analyzer_hpa is not None and com22_hpa is not None:
        analyzer_minus_com22 = analyzer_hpa - com22_hpa
    pace_minus_com22 = _float_value(row, ["pace_minus_com22_hpa"])
    if pace_minus_com22 is None and pace_hpa is not None and com22_hpa is not None:
        pace_minus_com22 = pace_hpa - com22_hpa

    out = dict(row)
    out.update(
        {
            "component": _component(row),
            "is_open_flow_formal_candidate": _is_open_flow(row),
            "co2_ppm": _float_value(row, [f"{prefix}_co2_ppm", "co2_ppm"]),
            "h2o_mmol": h2o_mmol,
            "dewpoint_c": _float_value(row, ["dewpoint_c", "dew_point_c"]),
            "h2o_dry_ppmv": h2o_dry_ppmv,
            "h2o_wet_ppmv": h2o_wet_ppmv,
            "com22_pressure_hpa": com22_hpa,
            "pace_pressure_hpa": pace_hpa,
            "analyzer_pressure_hpa": analyzer_hpa,
            "analyzer_minus_com22_hpa": analyzer_minus_com22,
            "pace_minus_com22_hpa": pace_minus_com22,
            "co2_ratio": _float_value(row, [f"{prefix}_co2_ratio_f", f"{prefix}_co2_ratio", "co2_ratio"]),
            "h2o_ratio": _float_value(row, [f"{prefix}_h2o_ratio_f", f"{prefix}_h2o_ratio", "h2o_ratio"]),
            "ref_signal": _float_value(row, [f"{prefix}_ref_signal", "ref_signal"]),
            "co2_signal": _float_value(row, [f"{prefix}_co2_signal", "co2_signal"]),
            "h2o_signal": _float_value(row, [f"{prefix}_h2o_signal", "h2o_signal"]),
            "chamber_temp_c": _float_value(row, [f"{prefix}_chamber_temp_c", "chamber_temp_c"]),
            "case_temp_c": _float_value(row, [f"{prefix}_case_temp_c", "case_temp_c"]),
        }
    )
    status_text = str(
        _first_value(row, [f"{prefix}_status_register_qc", "status_register_qc", f"{prefix}_frame_usable"])
        or ""
    ).lower()
    if status_text in {"false", "fail", "bad"}:
        out["status_register_qc"] = "fail"
    return out


def _load_pressure_rows(
    run_dir: Path,
    *,
    pressure_quick_check_path: str | Path | None,
    fallback_rows: Sequence[Mapping[str, Any]],
    analyzer_prefix: str,
) -> tuple[str, List[Dict[str, Any]], str]:
    path = Path(pressure_quick_check_path).resolve() if pressure_quick_check_path else latest_artifact(
        run_dir,
        "pressure_channel_quick_check*.csv",
    )
    if path and path.exists():
        rows = [normalize_advanced_qc_row(normalize_sample_row(row), analyzer_prefix=analyzer_prefix) for row in load_csv_rows(path)]
        return "pressure_quick_check_artifact", rows, str(path)
    return "sample_rows_fallback", [dict(row) for row in fallback_rows], ""


def _component_rows(rows: Sequence[Mapping[str, Any]], component: str) -> List[Dict[str, Any]]:
    return [
        dict(row)
        for row in rows
        if str(row.get("component") or "") == component and bool(row.get("is_open_flow_formal_candidate"))
    ]


def _excluded_rows(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for index, row in enumerate(rows):
        if bool(row.get("is_open_flow_formal_candidate")):
            continue
        out.append(
            {
                "row_index": index,
                "component": row.get("component", ""),
                "pressure_mode": row.get("pressure_mode", ""),
                "reason": "non_open_flow_pressure_mode",
            }
        )
    return out


def _analyze_component(
    component: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    window_size: int,
    pressure_trend: Mapping[str, Any],
) -> Dict[str, Any]:
    if not rows:
        return {
            "component": component,
            "status": "missing",
            "sample_count": 0,
            "root_cause": {
                "status": "review",
                "root_cause_codes": ["component_rows_missing"],
                "summary": f"{component.upper()} open-flow rows missing.",
            },
        }
    effective_window = min(int(window_size), len(rows))
    steady = select_steady_state_window(rows, window_size=effective_window)
    humidity = classify_humidity_behavior(rows)
    factory = evaluate_factory_signal_health(rows)
    root = classify_root_cause(
        humidity=humidity,
        factory_signal=factory,
        pressure_trend=pressure_trend,
    )
    statuses = {steady.get("status"), humidity.get("status"), factory.get("status"), root.get("status")}
    status = "pass"
    if any(item in statuses for item in {"block_formal", "reject_point", "fail"}):
        status = "fail"
    elif any(item in statuses for item in {"review"}):
        status = "review"
    return {
        "component": component,
        "status": status,
        "sample_count": len(rows),
        "steady_state": steady,
        "humidity": humidity,
        "factory_signal": factory,
        "root_cause": root,
    }


def build_advanced_qc_summary(
    *,
    run_dir: str | Path,
    pressure_quick_check_path: str | Path | None = None,
    analyzer_prefix: str = "ga01",
    window_size: int = 10,
) -> Dict[str, Any]:
    root = Path(run_dir).resolve()
    samples_path, raw_rows = load_latest_sample_rows(root)
    normalized_rows = [normalize_advanced_qc_row(row, analyzer_prefix=analyzer_prefix) for row in raw_rows]
    pressure_source, pressure_rows, pressure_path = _load_pressure_rows(
        root,
        pressure_quick_check_path=pressure_quick_check_path,
        fallback_rows=normalized_rows,
        analyzer_prefix=analyzer_prefix,
    )
    pressure = evaluate_pressure_trend(pressure_rows)
    pressure_chart = build_control_chart(
        [row.get("analyzer_minus_com22_hpa") for row in pressure_rows if row.get("analyzer_minus_com22_hpa") not in (None, "")]
    )
    components = {
        component: _analyze_component(
            component,
            _component_rows(normalized_rows, component),
            window_size=window_size,
            pressure_trend=pressure,
        )
        for component in ("co2", "h2o")
    }
    root_cause = classify_root_cause(
        humidity=components.get("h2o", {}).get("humidity", {}),
        factory_signal={
            "findings": [
                finding
                for item in components.values()
                for finding in (item.get("factory_signal", {}) or {}).get("findings", [])
            ]
        },
        pressure_trend=pressure,
    )
    excluded = _excluded_rows(normalized_rows)
    overall_status = "pass"
    if excluded:
        overall_status = "review"
    if any(item.get("status") == "fail" for item in components.values()) or root_cause.get("status") in {
        "reject_point",
        "block_formal",
    }:
        overall_status = "fail"
    elif any(item.get("status") == "review" for item in components.values()) or pressure.get("status") == "review":
        overall_status = "review"
    return {
        "schema_version": "v1_5_advanced_qc_summary_v0",
        "generated_at": _now(),
        "run_dir": str(Path(run_dir).resolve()),
        "samples_path": str(samples_path),
        "sidecar_only": True,
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "controls_valves_or_pace": False,
        "writes_coefficients": False,
        "formal_fit_boundary": {
            "open_flow_rows_only": True,
            "sealed_or_dynamic_pressure_rows_excluded": True,
            "excluded_row_count": len(excluded),
        },
        "status": overall_status,
        "pressure_source": pressure_source,
        "pressure_path": pressure_path,
        "pressure_trend": pressure,
        "pressure_control_chart": pressure_chart,
        "components": components,
        "excluded_diagnostic_rows": excluded[:200],
        "root_cause": root_cause,
    }


def render_advanced_qc_markdown(summary: Mapping[str, Any]) -> str:
    lines = [
        "# V1.5 advanced QC summary",
        "",
        f"- status: {summary.get('status')}",
        f"- run_dir: {summary.get('run_dir')}",
        f"- samples_path: {summary.get('samples_path')}",
        f"- pressure_source: {summary.get('pressure_source')}",
        f"- excluded_diagnostic_rows: {(summary.get('formal_fit_boundary') or {}).get('excluded_row_count')}",
        f"- root_cause: {(summary.get('root_cause') or {}).get('summary')}",
        "",
        "## Components",
    ]
    for component, item in (summary.get("components") or {}).items():
        root = item.get("root_cause") or {}
        lines.append(
            f"- {component.upper()}: status={item.get('status')} samples={item.get('sample_count')} "
            f"root={root.get('summary')}"
        )
    lines.extend(["", "## Boundary"])
    for key in ("opens_com_ports", "controls_water_or_gas_routes", "controls_valves_or_pace", "writes_coefficients"):
        lines.append(f"- {key}: {summary.get(key)}")
    return "\n".join(lines) + "\n"


def write_advanced_qc_summary(
    *,
    run_dir: str | Path,
    output_dir: str | Path | None = None,
    pressure_quick_check_path: str | Path | None = None,
    analyzer_prefix: str = "ga01",
    window_size: int = 10,
) -> Dict[str, Path]:
    root = Path(output_dir).resolve() if output_dir else Path(run_dir).resolve() / "advanced_qc"
    root.mkdir(parents=True, exist_ok=True)
    summary = build_advanced_qc_summary(
        run_dir=run_dir,
        pressure_quick_check_path=pressure_quick_check_path,
        analyzer_prefix=analyzer_prefix,
        window_size=window_size,
    )
    json_path = root / "advanced_qc_summary.json"
    markdown_path = root / "advanced_qc_summary.md"
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    markdown_path.write_text(render_advanced_qc_markdown(summary), encoding="utf-8")
    return {"summary_json": json_path, "summary_markdown": markdown_path}
