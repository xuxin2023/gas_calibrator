from __future__ import annotations

import argparse
import ast
import csv
import json
import math
import statistics
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from ..coefficients.write_readiness import build_write_readiness_decision


def _safe_float(value: Any) -> float | None:
    if value in (None, "", "null", "None"):
        return None
    try:
        number = float(value)
    except Exception:
        return None
    if not math.isfinite(number):
        return None
    return number


def _safe_bool(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on", "ok"}


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _normalize_analyzer(value: Any) -> str:
    text = str(value or "").strip().upper()
    return text


def _read_csv_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    row_list = [dict(row) for row in rows]
    headers: list[str] = []
    for row in row_list:
        for key in row:
            if key not in headers:
                headers.append(str(key))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in row_list:
            writer.writerow(row)


def _coefficient_group_values(text: Any) -> list[float]:
    raw = str(text or "").strip()
    if not raw:
        return []
    try:
        parsed = ast.literal_eval(raw)
    except Exception:
        return []
    if not isinstance(parsed, (list, tuple)):
        return []
    values: list[float] = []
    for item in parsed:
        numeric = _safe_float(item)
        if numeric is not None:
            values.append(numeric)
    return values


def _poly_temp(coeffs: Mapping[str, Any], temp_c: float) -> float:
    return float(
        float(coeffs.get("A", 0.0))
        + float(coeffs.get("B", 0.0)) * float(temp_c)
        + float(coeffs.get("C", 0.0)) * float(temp_c) ** 2
        + float(coeffs.get("D", 0.0)) * float(temp_c) ** 3
    )


def _evaluate_ratio_poly(coefficients: Mapping[str, Any], ratio: float, temp_c: float, pressure_hpa: float) -> float:
    temp_k = float(temp_c) + 273.15
    return float(
        float(coefficients.get("a0", 0.0))
        + float(coefficients.get("a1", 0.0)) * float(ratio)
        + float(coefficients.get("a2", 0.0)) * float(ratio) ** 2
        + float(coefficients.get("a3", 0.0)) * float(ratio) ** 3
        + float(coefficients.get("a4", 0.0)) * temp_k
        + float(coefficients.get("a5", 0.0)) * temp_k**2
        + float(coefficients.get("a6", 0.0)) * float(ratio) * temp_k
        + float(coefficients.get("a7", 0.0)) * float(pressure_hpa)
        + float(coefficients.get("a8", 0.0)) * float(ratio) * temp_k * float(pressure_hpa)
    )


def _compute_stats(values: list[float | None]) -> dict[str, Any] | None:
    usable = [value for value in values if value is not None and math.isfinite(value)]
    if not usable:
        return None
    return {
        "count": len(usable),
        "mean": sum(usable) / len(usable),
        "median": statistics.median(usable),
        "min": min(usable),
        "max": max(usable),
        "span": max(usable) - min(usable),
        "std": statistics.pstdev(usable) if len(usable) > 1 else 0.0,
    }


def _pearson(xs: list[float | None], ys: list[float | None]) -> float | None:
    pairs = [(x, y) for x, y in zip(xs, ys) if x is not None and y is not None]
    if len(pairs) < 3:
        return None
    x_values = [pair[0] for pair in pairs]
    y_values = [pair[1] for pair in pairs]
    x_mean = sum(x_values) / len(x_values)
    y_mean = sum(y_values) / len(y_values)
    numerator = sum((x - x_mean) * (y - y_mean) for x, y in pairs)
    denominator = math.sqrt(sum((x - x_mean) ** 2 for x in x_values) * sum((y - y_mean) ** 2 for y in y_values))
    if denominator == 0:
        return None
    return numerator / denominator


def _compute_candidate_metrics(actual: list[float | None], predicted: list[float | None]) -> dict[str, Any] | None:
    pairs = [(a, p) for a, p in zip(actual, predicted) if a is not None and p is not None]
    if not pairs:
        return None
    errors = [p - a for a, p in pairs]
    abs_errors = [abs(value) for value in errors]
    squared = [value * value for value in errors]
    return {
        "count": len(pairs),
        "rmse": math.sqrt(sum(squared) / len(squared)),
        "mae": sum(abs_errors) / len(abs_errors),
        "bias": sum(errors) / len(errors),
        "mean_signed_error": sum(errors) / len(errors),
        "max_abs_error": max(abs_errors),
        "residual_std": statistics.pstdev(errors) if len(errors) > 1 else 0.0,
        "_residuals": errors,
    }


def _gas_keys(gas: str) -> dict[str, str]:
    gas_norm = str(gas or "CO2").strip().upper()
    if gas_norm == "H2O":
        return {
            "target": "h2o_mmol",
            "ratio_f": "h2o_ratio_f",
            "ratio_raw": "h2o_ratio_raw",
            "signal": "h2o_signal",
            "legacy_signal": "h2o_sig",
        }
    return {
        "target": "co2_ppm",
        "ratio_f": "co2_ratio_f",
        "ratio_raw": "co2_ratio_raw",
        "signal": "co2_signal",
        "legacy_signal": "co2_sig",
    }


def _resolve_latest_artifact(run_dir: Path, pattern: str) -> Path | None:
    matches = [path for path in run_dir.glob(pattern) if path.is_file()]
    if not matches:
        return None
    return max(matches, key=lambda item: item.stat().st_mtime)


def _resolve_capture_path(run_dir: Path | None, baseline_capture_path: str | None) -> Path:
    if baseline_capture_path:
        return Path(baseline_capture_path).resolve()
    if run_dir is None:
        raise FileNotFoundError("baseline capture path is required when run_dir is not provided")
    direct = run_dir / "baseline_stream_079.csv"
    if direct.exists():
        return direct
    latest = _resolve_latest_artifact(run_dir, "baseline_parity_audit_*\\baseline_stream_*.csv")
    if latest is not None:
        return latest
    raise FileNotFoundError(f"could not locate baseline capture under {run_dir}")


def _load_main_coefficients(
    *,
    download_plan_path: Path,
    actual_device_id: str,
    analyzer: str,
    gas: str,
    writeback_detail_path: Path | None,
) -> tuple[dict[str, float], str]:
    main: dict[str, float] | None = None
    gas_norm = str(gas or "CO2").strip().upper()
    for row in _read_csv_rows(download_plan_path):
        row_device_id = _normalize_device_id(row.get("ActualDeviceId") or row.get("device_id") or row.get("DeviceId"))
        row_analyzer = _normalize_analyzer(row.get("Analyzer"))
        row_gas = str(row.get("Gas") or row.get("gas_type") or "").strip().upper()
        if row_device_id == actual_device_id and row_gas == gas_norm and (not analyzer or row_analyzer == analyzer):
            main = {f"a{i}": float(_safe_float(row.get(f"a{i}")) or 0.0) for i in range(9)}
            break
    if main is None:
        raise ValueError(f"could not find {gas_norm} coefficients for {analyzer or actual_device_id} in {download_plan_path}")

    source = f"download_plan:{download_plan_path}"
    if writeback_detail_path is not None and writeback_detail_path.exists():
        detail_rows = _read_csv_rows(writeback_detail_path)
        readback_map: dict[str, list[float]] = {}
        for row in detail_rows:
            if _normalize_device_id(row.get("TargetDeviceId")) != actual_device_id:
                continue
            if analyzer and _normalize_analyzer(row.get("Analyzer")) != analyzer:
                continue
            if not _safe_bool(row.get("ReadbackOk")):
                continue
            group = str(row.get("Group") or "").strip()
            if group not in {"1", "2", "3", "4"}:
                continue
            readback_map[group] = _coefficient_group_values(row.get("Readback"))
        if gas_norm == "CO2" and readback_map.get("1") and readback_map.get("3"):
            group1 = readback_map["1"]
            group3 = readback_map["3"]
            main = {
                "a0": float(group1[0]),
                "a1": float(group1[1]),
                "a2": float(group1[2]),
                "a3": float(group1[3]),
                "a4": float(group3[0]),
                "a5": float(group3[1]),
                "a6": float(group3[2]),
                "a7": 0.0,
                "a8": 0.0,
            }
            source = f"writeback_readback:{writeback_detail_path}"
        if gas_norm == "H2O" and readback_map.get("2") and readback_map.get("4"):
            group2 = readback_map["2"]
            group4 = readback_map["4"]
            main = {
                "a0": float(group2[0]),
                "a1": float(group2[1]),
                "a2": float(group2[2]),
                "a3": float(group2[3]),
                "a4": float(group4[0]),
                "a5": float(group4[1]),
                "a6": float(group4[2]),
                "a7": 0.0,
                "a8": 0.0,
            }
            source = f"writeback_readback:{writeback_detail_path}"
    return main, source


def _load_temperature_coefficients(
    *,
    temperature_coefficients_path: Path,
    analyzer: str,
    actual_device_id: str,
    writeback_detail_path: Path | None,
) -> tuple[dict[str, dict[str, float]], str]:
    coeffs: dict[str, dict[str, float]] = {}
    for row in _read_csv_rows(temperature_coefficients_path):
        row_analyzer = _normalize_analyzer(row.get("analyzer_id") or row.get("Analyzer"))
        channel = str(row.get("senco_channel") or row.get("Channel") or "").strip().upper()
        if analyzer and row_analyzer != analyzer:
            continue
        if channel not in {"SENCO7", "SENCO8"}:
            continue
        coeffs[channel] = {name: float(_safe_float(row.get(name)) or 0.0) for name in ("A", "B", "C", "D")}
    if not coeffs:
        return {}, f"temperature_coefficients_missing:{temperature_coefficients_path}"

    source = f"temperature_target:{temperature_coefficients_path}"
    if writeback_detail_path is not None and writeback_detail_path.exists():
        detail_rows = _read_csv_rows(writeback_detail_path)
        readback_map: dict[str, list[float]] = {}
        for row in detail_rows:
            if _normalize_device_id(row.get("TargetDeviceId")) != actual_device_id:
                continue
            if analyzer and _normalize_analyzer(row.get("Analyzer")) != analyzer:
                continue
            if not _safe_bool(row.get("ReadbackOk")):
                continue
            group = str(row.get("Group") or "").strip()
            if group in {"7", "8"}:
                readback_map[group] = _coefficient_group_values(row.get("Readback"))
        if readback_map.get("7"):
            values = readback_map["7"]
            coeffs["SENCO7"] = {"A": float(values[0]), "B": float(values[1]), "C": float(values[2]), "D": float(values[3])}
            source = f"writeback_readback:{writeback_detail_path}"
        if readback_map.get("8"):
            values = readback_map["8"]
            coeffs["SENCO8"] = {"A": float(values[0]), "B": float(values[1]), "C": float(values[2]), "D": float(values[3])}
            source = f"writeback_readback:{writeback_detail_path}"
    return coeffs, source


def _visible_inputs(rows: list[dict[str, Any]], gas: str, temperature_coeffs: Mapping[str, Any]) -> dict[str, Any]:
    keys = _gas_keys(gas)

    def _has_value(name: str) -> bool:
        return any(_safe_float(row.get(name)) is not None for row in rows)

    def _has_positive(name: str) -> bool:
        return any((_safe_float(row.get(name)) or 0.0) > 0 for row in rows)

    stream_formats = sorted({str(row.get("stream_format") or "").strip() for row in rows if str(row.get("stream_format") or "").strip()})
    legacy_only = bool(rows) and stream_formats == ["legacy"]
    ratio_f_available = _has_positive(keys["ratio_f"])
    ratio_raw_available = _has_positive(keys["ratio_raw"])
    signal_available = _has_positive(keys["signal"])
    ref_signal_available = _has_positive("ref_signal")
    signal_over_ref_available = signal_available and ref_signal_available
    legacy_signal_available = _has_positive(keys["legacy_signal"])
    temperature_available = _has_value("temp_c")
    chamber_temp_available = _has_value("chamber_temp_c")
    case_temp_available = _has_value("case_temp_c")
    direct_ratio_available = ratio_f_available or ratio_raw_available
    temp_comp_required = bool(temperature_coeffs)
    temp_comp_visible = (not temp_comp_required) or (chamber_temp_available and case_temp_available)
    enough_for_final = bool(rows) and direct_ratio_available and not legacy_only and temp_comp_visible

    return {
        "stream_formats_seen": stream_formats,
        "legacy_stream_only": legacy_only,
        "target_available": _has_value(keys["target"]),
        "ratio_f_available": ratio_f_available,
        "ratio_raw_available": ratio_raw_available,
        "signal_available": signal_available,
        "ref_signal_available": ref_signal_available,
        "signal_over_ref_available": signal_over_ref_available,
        "legacy_signal_available": legacy_signal_available,
        "temperature_available": temperature_available,
        "chamber_temp_available": chamber_temp_available,
        "case_temp_available": case_temp_available,
        "direct_ratio_available": direct_ratio_available,
        "temperature_compensation_coefficients_available": bool(temperature_coeffs),
        "temperature_compensation_visible": temp_comp_visible,
        "enough_for_final_parity": enough_for_final,
    }


def _candidate_pass(metrics: Mapping[str, Any], gas: str) -> bool:
    gas_norm = str(gas or "CO2").strip().upper()
    if gas_norm == "H2O":
        thresholds = {"rmse": 1.0, "mae": 0.8, "bias": 0.6, "max_abs_error": 2.0}
    else:
        thresholds = {"rmse": 40.0, "mae": 35.0, "bias": 30.0, "max_abs_error": 75.0}
    return (
        float(metrics.get("rmse") or math.inf) <= thresholds["rmse"]
        and float(metrics.get("mae") or math.inf) <= thresholds["mae"]
        and abs(float(metrics.get("bias") or math.inf)) <= thresholds["bias"]
        and float(metrics.get("max_abs_error") or math.inf) <= thresholds["max_abs_error"]
    )


def run_runtime_parity_audit(
    *,
    run_dir: str | None,
    analyzer: str | None,
    actual_device_id: str,
    gas: str = "CO2",
    baseline_capture_path: str | None = None,
    port: str | None = None,
    download_plan_path: str | None = None,
    temperature_coefficients_path: str | None = None,
    writeback_detail_path: str | None = None,
    output_dir: str | None = None,
) -> dict[str, Any]:
    run_dir_path = Path(run_dir).resolve() if run_dir else None
    analyzer_norm = _normalize_analyzer(analyzer)
    device_id_norm = _normalize_device_id(actual_device_id)
    gas_norm = str(gas or "CO2").strip().upper()

    if run_dir_path is None and (download_plan_path is None or temperature_coefficients_path is None):
        raise ValueError("run_dir is required unless coefficient artifact paths are provided explicitly")

    capture_path = _resolve_capture_path(run_dir_path, baseline_capture_path)
    download_plan = Path(download_plan_path).resolve() if download_plan_path else (run_dir_path / "download_plan_no_500.csv")
    temperature_path = (
        Path(temperature_coefficients_path).resolve()
        if temperature_coefficients_path
        else (run_dir_path / "temperature_coefficients_target.csv")
    )
    detail_path = Path(writeback_detail_path).resolve() if writeback_detail_path else None
    if detail_path is None and run_dir_path is not None:
        candidate = _resolve_latest_artifact(run_dir_path, "live_write_*\\detail.csv")
        if candidate is not None:
            detail_path = candidate

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir).resolve() if output_dir else ((run_dir_path or capture_path.parent) / f"runtime_parity_audit_{device_id_norm}_{timestamp}")
    out_dir.mkdir(parents=True, exist_ok=True)

    points_rows = _read_csv_rows(capture_path)
    filtered_rows = []
    for row in points_rows:
        row_device_id = _normalize_device_id(row.get("device_id") or row.get("id") or row.get("ActualDeviceId"))
        if row_device_id and row_device_id != device_id_norm:
            continue
        filtered_rows.append({key: row.get(key) for key in row})

    main_coefficients, main_source = _load_main_coefficients(
        download_plan_path=download_plan,
        actual_device_id=device_id_norm,
        analyzer=analyzer_norm,
        gas=gas_norm,
        writeback_detail_path=detail_path,
    )
    temperature_coeffs, temperature_source = _load_temperature_coefficients(
        temperature_coefficients_path=temperature_path,
        analyzer=analyzer_norm,
        actual_device_id=device_id_norm,
        writeback_detail_path=detail_path,
    )

    keys = _gas_keys(gas_norm)
    visible = _visible_inputs(filtered_rows, gas_norm, temperature_coeffs)
    actual_values: list[float | None] = [_safe_float(row.get(keys["target"])) for row in filtered_rows]

    enriched_points: list[dict[str, Any]] = []
    for row in filtered_rows:
        enriched = dict(row)
        for key in (
            keys["target"],
            keys["ratio_f"],
            keys["ratio_raw"],
            keys["signal"],
            keys["legacy_signal"],
            "ref_signal",
            "temp_c",
            "chamber_temp_c",
            "case_temp_c",
            "pressure_kpa",
        ):
            enriched[key] = _safe_float(enriched.get(key))
        enriched_points.append(enriched)

    candidate_specs: list[dict[str, Any]] = []

    def _add_candidate(
        *,
        name: str,
        family: str,
        required: list[str],
        builder,
        note: str = "",
    ) -> None:
        missing = [item for item in required if not visible.get(item, False)]
        if missing:
            candidate_specs.append(
                {
                    "candidate_name": name,
                    "signal_family": family,
                    "candidate_status": "insufficient_inputs",
                    "required_inputs": ",".join(required),
                    "missing_inputs": ",".join(missing),
                    "note": note,
                }
            )
            return

        predicted = []
        for row in enriched_points:
            try:
                predicted.append(builder(row))
            except Exception:
                predicted.append(None)
        metrics = _compute_candidate_metrics(actual_values, predicted)
        if metrics is None:
            candidate_specs.append(
                {
                    "candidate_name": name,
                    "signal_family": family,
                    "candidate_status": "skipped",
                    "required_inputs": ",".join(required),
                    "missing_inputs": "",
                    "note": "candidate produced no usable points",
                }
            )
            return
        residuals = metrics.pop("_residuals")
        candidate_specs.append(
            {
                "candidate_name": name,
                "signal_family": family,
                "candidate_status": "tested",
                "required_inputs": ",".join(required),
                "missing_inputs": "",
                "points_tested": metrics["count"],
                "rmse": metrics["rmse"],
                "mae": metrics["mae"],
                "bias": metrics["bias"],
                "mean_signed_error": metrics["mean_signed_error"],
                "max_abs_error": metrics["max_abs_error"],
                "residual_std": metrics["residual_std"],
                "corr_residual_vs_target": _pearson(
                    actual_values[: len(residuals)],
                    residuals,
                ),
                "corr_residual_vs_temp_c": _pearson(
                    [_safe_float(row.get("temp_c")) for row in enriched_points[: len(residuals)]],
                    residuals,
                ),
                "corr_residual_vs_ratio_f": _pearson(
                    [_safe_float(row.get(keys["ratio_f"])) for row in enriched_points[: len(residuals)]],
                    residuals,
                ),
                "note": note,
                "_predicted": predicted,
            }
        )

    _add_candidate(
        name="ratio_f_plus_temperature",
        family="ratio_f",
        required=["ratio_f_available", "temperature_available"],
        builder=lambda row: _evaluate_ratio_poly(
            main_coefficients,
            float(row[keys["ratio_f"]]),
            float(row["temp_c"]),
            float((_safe_float(row.get("pressure_kpa")) or 0.0) * 10.0),
        ),
    )
    _add_candidate(
        name="ratio_raw_plus_temperature",
        family="ratio_raw",
        required=["ratio_raw_available", "temperature_available"],
        builder=lambda row: _evaluate_ratio_poly(
            main_coefficients,
            float(row[keys["ratio_raw"]]),
            float(row["temp_c"]),
            float((_safe_float(row.get("pressure_kpa")) or 0.0) * 10.0),
        ),
    )
    _add_candidate(
        name="signal_over_ref_plus_temperature",
        family="signal_over_ref",
        required=["signal_over_ref_available", "temperature_available"],
        builder=lambda row: _evaluate_ratio_poly(
            main_coefficients,
            float(row[keys["signal"]]) / float(row["ref_signal"]),
            float(row["temp_c"]),
            float((_safe_float(row.get("pressure_kpa")) or 0.0) * 10.0),
        ),
    )
    _add_candidate(
        name="ratio_f_plus_senco7_chamber",
        family="ratio_f",
        required=["ratio_f_available", "chamber_temp_available", "temperature_compensation_coefficients_available"],
        builder=lambda row: _evaluate_ratio_poly(
            main_coefficients,
            float(row[keys["ratio_f"]]),
            _poly_temp(temperature_coeffs["SENCO7"], float(row["chamber_temp_c"])),
            float((_safe_float(row.get("pressure_kpa")) or 0.0) * 10.0),
        ),
        note="uses visible chamber_temp_c with SENCO7 compensation",
    )
    _add_candidate(
        name="ratio_f_plus_senco8_case",
        family="ratio_f",
        required=["ratio_f_available", "case_temp_available", "temperature_compensation_coefficients_available"],
        builder=lambda row: _evaluate_ratio_poly(
            main_coefficients,
            float(row[keys["ratio_f"]]),
            _poly_temp(temperature_coeffs["SENCO8"], float(row["case_temp_c"])),
            float((_safe_float(row.get("pressure_kpa")) or 0.0) * 10.0),
        ),
        note="uses visible case_temp_c with SENCO8 compensation",
    )
    _add_candidate(
        name="ratio_f_plus_avg_senco7_senco8",
        family="ratio_f",
        required=[
            "ratio_f_available",
            "chamber_temp_available",
            "case_temp_available",
            "temperature_compensation_coefficients_available",
        ],
        builder=lambda row: _evaluate_ratio_poly(
            main_coefficients,
            float(row[keys["ratio_f"]]),
            (
                _poly_temp(temperature_coeffs["SENCO7"], float(row["chamber_temp_c"]))
                + _poly_temp(temperature_coeffs["SENCO8"], float(row["case_temp_c"]))
            )
            / 2.0,
            float((_safe_float(row.get("pressure_kpa")) or 0.0) * 10.0),
        ),
        note="uses average of visible SENCO7/SENCO8 compensated temperatures",
    )
    _add_candidate(
        name="visible_signal_proxy_plus_temperature",
        family="visible_fallback",
        required=["legacy_signal_available", "temperature_available"],
        builder=lambda row: _evaluate_ratio_poly(
            main_coefficients,
            float(row[keys["legacy_signal"]]),
            float(row["temp_c"]),
            float((_safe_float(row.get("pressure_kpa")) or 0.0) * 10.0),
        ),
        note="pure visible fallback; not eligible to prove final runtime parity on its own",
    )

    candidate_rows = []
    tested_candidates = []
    prediction_map: dict[str, list[float | None]] = {}
    for row in candidate_specs:
        candidate = {key: value for key, value in row.items() if not key.startswith("_")}
        candidate_rows.append(candidate)
        if row.get("candidate_status") == "tested":
            tested_candidates.append(row)
            prediction_map[str(row["candidate_name"])] = list(row.get("_predicted") or [])

    best_candidate = None
    if tested_candidates:
        best_candidate = min(tested_candidates, key=lambda item: float(item.get("rmse") or math.inf))

    if not filtered_rows:
        verdict = "parity_inconclusive_missing_live_stream"
    elif visible["legacy_stream_only"]:
        verdict = "parity_inconclusive_missing_runtime_inputs"
    elif not visible["direct_ratio_available"]:
        if visible["signal_over_ref_available"] or visible["legacy_signal_available"]:
            verdict = "parity_inconclusive_mixed_signal_semantics"
        else:
            verdict = "parity_inconclusive_missing_runtime_inputs"
    elif not visible["temperature_compensation_visible"]:
        verdict = "parity_inconclusive_missing_runtime_inputs"
    elif not tested_candidates:
        verdict = "parity_inconclusive_missing_runtime_inputs"
    else:
        ranked = sorted(tested_candidates, key=lambda item: float(item.get("rmse") or math.inf))
        if len(ranked) >= 2:
            first = ranked[0]
            second = ranked[1]
            rmse_gap = abs(float(second.get("rmse") or 0.0) - float(first.get("rmse") or 0.0))
            if rmse_gap <= max(5.0, float(first.get("rmse") or 0.0) * 0.1) and str(first.get("signal_family")) != str(second.get("signal_family")):
                verdict = "parity_inconclusive_mixed_signal_semantics"
            elif _candidate_pass(first, gas_norm):
                verdict = "parity_pass"
            else:
                verdict = "parity_fail"
        elif _candidate_pass(ranked[0], gas_norm):
            verdict = "parity_pass"
        else:
            verdict = "parity_fail"

    readiness = build_write_readiness_decision(
        fit_quality="unknown",
        delivery_recommendation="unknown",
        coefficient_source=main_source,
        writeback_status="unknown",
        runtime_parity_verdict=verdict,
        legacy_stream_only=visible["legacy_stream_only"],
    )

    point_rows_for_csv: list[dict[str, Any]] = []
    for idx, row in enumerate(enriched_points):
        payload = dict(row)
        for candidate_name, predictions in prediction_map.items():
            prediction = predictions[idx] if idx < len(predictions) else None
            actual = actual_values[idx] if idx < len(actual_values) else None
            payload[f"{candidate_name}_predicted"] = prediction
            payload[f"{candidate_name}_residual"] = prediction - actual if prediction is not None and actual is not None else None
        point_rows_for_csv.append(payload)

    visible_missing = [key for key, value in visible.items() if key.endswith("_available") and not value]
    visible_available = [key for key, value in visible.items() if key.endswith("_available") and value]
    summary = {
        "run_dir": str(run_dir_path) if run_dir_path is not None else "",
        "analyzer": analyzer_norm,
        "actual_device_id": device_id_norm,
        "gas": gas_norm,
        "port": str(port or ""),
        "baseline_capture_path": str(capture_path),
        "download_plan_path": str(download_plan),
        "temperature_coefficients_path": str(temperature_path),
        "writeback_detail_path": str(detail_path) if detail_path is not None else "",
        "runtime_parity_points_path": str(out_dir / "runtime_parity_points.csv"),
        "runtime_parity_candidates_path": str(out_dir / "runtime_parity_candidates.csv"),
        "runtime_parity_report_path": str(out_dir / "runtime_parity_report.md"),
        "main_coefficients_source": main_source,
        "temperature_coefficients_source": temperature_source,
        "main_coefficients": main_coefficients,
        "temperature_coefficients": temperature_coeffs,
        "stream_row_count": len(filtered_rows),
        "visible_runtime_inputs_available": visible_available,
        "visible_runtime_inputs_missing": visible_missing,
        "visible_runtime_inputs": visible,
        "target_stats": _compute_stats(actual_values),
        "parity_candidates_actually_testable": [
            row["candidate_name"] for row in candidate_rows if row.get("candidate_status") == "tested"
        ],
        "parity_candidates_missing_inputs": [
            row["candidate_name"] for row in candidate_rows if row.get("candidate_status") == "insufficient_inputs"
        ],
        "candidate_rows": candidate_rows,
        "best_candidate": {key: value for key, value in best_candidate.items() if not key.startswith("_")} if best_candidate else None,
        "legacy_stream_only": visible["legacy_stream_only"],
        "parity_verdict": verdict,
        "runtime_parity_quality": "pass" if verdict == "parity_pass" else verdict,
        "final_write_ready": readiness["final_write_ready"],
        "readiness_code": readiness["readiness_code"],
        "readiness_reason": readiness["readiness_reason"],
        "readiness_summary": readiness["readiness_summary"],
    }

    _write_csv(out_dir / "runtime_parity_points.csv", point_rows_for_csv)
    _write_csv(out_dir / "runtime_parity_candidates.csv", candidate_rows)
    (out_dir / "runtime_parity_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )

    report_lines = [
        "# V1 Runtime Parity Audit",
        "",
        f"- analyzer: {analyzer_norm or '--'}",
        f"- actual_device_id: {device_id_norm}",
        f"- gas: {gas_norm}",
        f"- port: {port or '--'}",
        f"- baseline_capture: {capture_path}",
        f"- main_coefficients_source: {main_source}",
        f"- temperature_coefficients_source: {temperature_source}",
        f"- stream_row_count: {len(filtered_rows)}",
        f"- parity_verdict: {verdict}",
        f"- legacy_stream_only: {visible['legacy_stream_only']}",
        f"- final_write_ready: {readiness['final_write_ready']}",
        f"- readiness_code: {readiness['readiness_code']}",
        f"- readiness_reason: {readiness['readiness_reason']}",
        "",
        "## Visible Runtime Inputs",
        f"- available: {', '.join(visible_available) or 'none'}",
        f"- missing: {', '.join(visible_missing) or 'none'}",
        "",
        "## Candidates",
    ]
    if candidate_rows:
        for row in candidate_rows:
            if row.get("candidate_status") == "tested":
                report_lines.append(
                    f"- {row['candidate_name']}: status=tested rmse={row.get('rmse')} mae={row.get('mae')} bias={row.get('bias')} max_abs_error={row.get('max_abs_error')}"
                )
            else:
                report_lines.append(
                    f"- {row['candidate_name']}: status={row.get('candidate_status')} missing={row.get('missing_inputs', '')} note={row.get('note', '')}"
                )
    else:
        report_lines.append("- none")
    (out_dir / "runtime_parity_report.md").write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    return summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit whether current V1 runtime can be explained by visible main-chain inputs.")
    parser.add_argument("--run-dir", type=str, default="", help="Run directory containing download plan and temperature coefficients.")
    parser.add_argument("--analyzer", type=str, default="", help="Analyzer id, e.g. GA03.")
    parser.add_argument("--actual-device-id", type=str, required=True, help="Actual device id, e.g. 079.")
    parser.add_argument("--gas", type=str, default="CO2", help="Gas to audit, e.g. CO2 or H2O.")
    parser.add_argument("--baseline-capture", type=str, default="", help="Path to baseline capture csv.")
    parser.add_argument("--port", type=str, default="", help="Port label for reporting only.")
    parser.add_argument("--download-plan", type=str, default="", help="Optional explicit download plan path.")
    parser.add_argument("--temperature-coefficients", type=str, default="", help="Optional explicit temperature coefficients path.")
    parser.add_argument("--writeback-detail", type=str, default="", help="Optional detail.csv path for readback-confirmed coefficients.")
    parser.add_argument("--output-dir", type=str, default="", help="Optional output directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    summary = run_runtime_parity_audit(
        run_dir=args.run_dir or None,
        analyzer=args.analyzer or None,
        actual_device_id=args.actual_device_id,
        gas=args.gas,
        baseline_capture_path=args.baseline_capture or None,
        port=args.port or None,
        download_plan_path=args.download_plan or None,
        temperature_coefficients_path=args.temperature_coefficients or None,
        writeback_detail_path=args.writeback_detail or None,
        output_dir=args.output_dir or None,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
