"""Offline diagnostics for post-H2O CO2 verification failures.

The diagnostic explains why a previously acceptable CO2 point can fail after a
water-route run when the written coefficients are unchanged. It only reads
recorded artifacts; it never opens COM ports, controls routes, or writes SENCO
coefficients.
"""

from __future__ import annotations

import csv
import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


DEFAULT_TARGET_DEVICES = ("022", "030", "033", "051")


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


def _read_csv(path: str | Path | None) -> List[Dict[str, Any]]:
    if path in (None, ""):
        return []
    source = Path(path)
    if not source.exists():
        return []
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


def _device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _nominal_ppm_from_text(value: Any) -> Optional[int]:
    text = str(value or "")
    match = re.search(r"open_flow_(-?\d+(?:\.\d+)?)ppm", text)
    if not match:
        return None
    numeric = _safe_float(match.group(1))
    if numeric is None:
        return None
    return int(round(numeric))


def _nominal_ppm(row: Mapping[str, Any]) -> Optional[int]:
    for key in ("source_nominal_ppm", "nominal_ppm", "target_nominal_ppm"):
        numeric = _safe_float(row.get(key))
        if numeric is not None:
            return int(round(numeric))
    for key in ("sample_index", "point_identity", "point_run_id"):
        parsed = _nominal_ppm_from_text(row.get(key))
        if parsed is not None:
            return parsed
    numeric = _safe_float(row.get("certificate_co2_ppm") or row.get("target_value"))
    return None if numeric is None else int(round(numeric))


def _h2o_dry_correction_pct(h2o_mmol_mol: Optional[float]) -> Optional[float]:
    if h2o_mmol_mol is None:
        return None
    denominator = 1.0 - float(h2o_mmol_mol) / 1000.0
    if denominator <= 0.0:
        return None
    return (1.0 / denominator - 1.0) * 100.0


def _fit_lookup(rows: Sequence[Mapping[str, Any]]) -> Dict[Tuple[str, int], Mapping[str, Any]]:
    out: Dict[Tuple[str, int], Mapping[str, Any]] = {}
    for row in rows:
        device = _device_id(row.get("analyzer_device_id") or row.get("device_id"))
        if not device:
            continue
        sample_index = str(row.get("sample_index") or row.get("point_identity") or "")
        if "_T20_" not in sample_index:
            continue
        nominal = _nominal_ppm(row)
        if nominal is None:
            continue
        out[(device, nominal)] = row
    return out


def _firmware_replay_summary(rows: Sequence[Mapping[str, Any]], *, max_delta_ppm: float = 0.2) -> Dict[str, Any]:
    today_rows = [
        row
        for row in rows
        if str(row.get("source") or "").strip().lower().startswith("today")
    ]
    deltas = [
        abs(float(value))
        for value in (_safe_float(row.get("replay_minus_measured_ppm")) for row in today_rows)
        if value is not None
    ]
    max_delta = max(deltas) if deltas else None
    return {
        "row_count": len(today_rows),
        "max_abs_replay_minus_measured_ppm": max_delta,
        "replay_consistent": bool(deltas) and max_delta is not None and max_delta <= float(max_delta_ppm),
        "limit_ppm": float(max_delta_ppm),
    }


def _comparison_rows(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "device_id": _device_id(row.get("device_id")),
                "yesterday_900_co2_ppm": _safe_float(row.get("yesterday_900_co2_ppm")),
                "today_900_co2_ppm": _safe_float(row.get("today_900_co2_ppm")),
                "delta_error_pct": _safe_float(row.get("delta_error_pct")),
                "yesterday_R_CO2": _safe_float(row.get("yesterday_R_CO2")),
                "today_R_CO2": _safe_float(row.get("today_R_CO2")),
                "delta_R_CO2": _safe_float(row.get("delta_R_CO2")),
                "yesterday_T1": _safe_float(row.get("yesterday_T1")),
                "today_T1": _safe_float(row.get("today_T1")),
                "delta_T1": _safe_float(row.get("delta_T1")),
            }
        )
    return out


def _row_driver(
    *,
    status: str,
    error_pct: Optional[float],
    h2o_effect_pct: Optional[float],
    delta_ratio: Optional[float],
    delta_temp_c: Optional[float],
    replay_consistent: bool,
    acceptance_pct: float,
) -> str:
    if status == "pass" or error_pct is None or abs(float(error_pct)) <= float(acceptance_pct):
        return "within_acceptance"
    drivers: List[str] = []
    if h2o_effect_pct is not None and abs(float(h2o_effect_pct)) < 0.2:
        drivers.append("h2o_final_dry_correction_too_small_to_explain_error")
    elif h2o_effect_pct is not None:
        drivers.append("h2o_final_dry_correction_needs_review")
    if replay_consistent:
        drivers.append("firmware_replay_matches_displayed_output")
    if delta_ratio is not None and abs(float(delta_ratio)) >= 0.002:
        drivers.append("co2_ratio_shift_vs_original_T20_fit_sample")
    if delta_temp_c is not None and abs(float(delta_temp_c)) >= 0.5:
        drivers.append("temperature_state_shift_vs_original_T20_fit_sample")
    if not drivers:
        drivers.append("insufficient_evidence_needs_same_condition_repeat")
    return ";".join(drivers)


def build_co2_post_h2o_diagnostic(
    *,
    verification_summary_csv: str | Path,
    fit_residuals_csv: str | Path | None = None,
    firmware_replay_csv: str | Path | None = None,
    yesterday_today_csv: str | Path | None = None,
    target_device_ids: Iterable[str] = DEFAULT_TARGET_DEVICES,
    acceptance_pct: float = 1.0,
) -> Dict[str, Any]:
    """Build diagnostic tables from already-recorded V1.5 artifacts."""

    target_set = {_device_id(item) for item in target_device_ids if str(item).strip()}
    current_rows = _read_csv(verification_summary_csv)
    fit_by_key = _fit_lookup(_read_csv(fit_residuals_csv))
    replay = _firmware_replay_summary(_read_csv(firmware_replay_csv))
    replay_consistent = bool(replay.get("replay_consistent"))
    yesterday_today = _comparison_rows(_read_csv(yesterday_today_csv))
    point_rows: List[Dict[str, Any]] = []
    by_device: Dict[str, List[Dict[str, Any]]] = {}

    for row in current_rows:
        device = _device_id(row.get("device_id") or row.get("analyzer_device_id"))
        if target_set and device not in target_set:
            continue
        nominal = _nominal_ppm(row)
        fit = fit_by_key.get((device, nominal)) if nominal is not None else None
        measured = _safe_float(row.get("measured_co2_ppm") or row.get("co2_ppm"))
        target = _safe_float(row.get("certificate_co2_ppm") or row.get("target_co2_ppm"))
        error_ppm = _safe_float(row.get("error_ppm") or row.get("co2_error_ppm"))
        if error_ppm is None and measured is not None and target is not None:
            error_ppm = measured - target
        error_pct = _safe_float(row.get("error_pct") or row.get("co2_error_pct"))
        h2o = _safe_float(row.get("h2o_mmol_mol") or row.get("h2o_mmol_mol_mean"))
        current_ratio = _safe_float(row.get("co2_ratio_f") or row.get("co2_ratio_filtered_mean"))
        current_temp = _safe_float(row.get("chamber_temp_c") or row.get("chamber_temp_c_mean"))
        fit_ratio = _safe_float(fit.get("ratio")) if fit else None
        fit_temp = _safe_float(fit.get("temperature_c")) if fit else None
        fit_prediction = _safe_float(fit.get("prediction")) if fit else None
        delta_ratio = None if current_ratio is None or fit_ratio is None else current_ratio - fit_ratio
        delta_temp = None if current_temp is None or fit_temp is None else current_temp - fit_temp
        status = str(row.get("status") or "").strip().lower()
        h2o_effect = _h2o_dry_correction_pct(h2o)
        item = {
            "device_id": device,
            "point_run_id": row.get("point_run_id", ""),
            "source_nominal_ppm": nominal,
            "certificate_co2_ppm": target,
            "measured_co2_ppm": measured,
            "error_ppm": error_ppm,
            "error_pct": error_pct,
            "acceptance_pct": acceptance_pct,
            "status": status,
            "h2o_mmol_mol": h2o,
            "h2o_dry_correction_pct": h2o_effect,
            "current_co2_ratio_f": current_ratio,
            "fit_T20_co2_ratio_f": fit_ratio,
            "delta_ratio_vs_fit_T20": delta_ratio,
            "current_chamber_temp_c": current_temp,
            "fit_T20_chamber_temp_c": fit_temp,
            "delta_chamber_temp_vs_fit_T20_c": delta_temp,
            "fit_T20_prediction_ppm": fit_prediction,
            "diagnostic_driver": _row_driver(
                status=status,
                error_pct=error_pct,
                h2o_effect_pct=h2o_effect,
                delta_ratio=delta_ratio,
                delta_temp_c=delta_temp,
                replay_consistent=replay_consistent,
                acceptance_pct=acceptance_pct,
            ),
        }
        point_rows.append(item)
        by_device.setdefault(device, []).append(item)

    device_rows: List[Dict[str, Any]] = []
    for device in sorted(target_set):
        rows = by_device.get(device, [])
        errors = [abs(float(row["error_pct"])) for row in rows if row.get("error_pct") is not None]
        signed_errors_ppm = [float(row["error_ppm"]) for row in rows if row.get("error_ppm") is not None]
        h2o_effects = [
            abs(float(row["h2o_dry_correction_pct"]))
            for row in rows
            if row.get("h2o_dry_correction_pct") is not None
        ]
        ratio_deltas = [
            abs(float(row["delta_ratio_vs_fit_T20"]))
            for row in rows
            if row.get("delta_ratio_vs_fit_T20") is not None
        ]
        temp_deltas = [
            abs(float(row["delta_chamber_temp_vs_fit_T20_c"]))
            for row in rows
            if row.get("delta_chamber_temp_vs_fit_T20_c") is not None
        ]
        failed = [row for row in rows if str(row.get("status")) == "fail"]
        max_error = max(errors) if errors else None
        mean_error_ppm = sum(signed_errors_ppm) / len(signed_errors_ppm) if signed_errors_ppm else None
        error_ppm_span = (
            max(signed_errors_ppm) - min(signed_errors_ppm)
            if len(signed_errors_ppm) >= 2
            else 0.0
            if signed_errors_ppm
            else None
        )
        additive_offset_like = (
            mean_error_ppm is not None
            and error_ppm_span is not None
            and abs(float(mean_error_ppm)) >= 5.0
            and float(error_ppm_span) <= 15.0
        )
        max_h2o = max(h2o_effects) if h2o_effects else None
        max_ratio_delta = max(ratio_deltas) if ratio_deltas else None
        max_temp_delta = max(temp_deltas) if temp_deltas else None
        conclusion = "pass" if rows and not failed else "blocked"
        if failed and max_h2o is not None and max_h2o < 0.2 and replay_consistent:
            conclusion = "blocked_ratio_temperature_or_physical_state_shift"
        device_rows.append(
            {
                "device_id": device,
                "point_count": len(rows),
                "failed_point_count": len(failed),
                "max_abs_error_pct": max_error,
                "mean_error_ppm": mean_error_ppm,
                "error_ppm_span": error_ppm_span,
                "additive_offset_like": additive_offset_like,
                "max_h2o_dry_correction_pct": max_h2o,
                "max_abs_ratio_delta_vs_fit_T20": max_ratio_delta,
                "max_abs_chamber_temp_delta_vs_fit_T20_c": max_temp_delta,
                "firmware_replay_consistent": replay_consistent,
                "conclusion": conclusion,
            }
        )

    target_point_rows = [row for row in point_rows if row.get("status") in {"pass", "fail"}]
    failed_count = sum(1 for row in target_point_rows if row.get("status") == "fail")
    h2o_can_explain = any(
        row.get("h2o_dry_correction_pct") is not None
        and row.get("error_pct") is not None
        and abs(float(row["h2o_dry_correction_pct"])) >= abs(float(row["error_pct"])) * 0.5
        for row in target_point_rows
    )
    run_summary = [
        {
            "created_at": _now(),
            "verification_summary_csv": str(Path(verification_summary_csv).resolve()),
            "fit_residuals_csv": "" if fit_residuals_csv in (None, "") else str(Path(fit_residuals_csv).resolve()),
            "firmware_replay_csv": "" if firmware_replay_csv in (None, "") else str(Path(firmware_replay_csv).resolve()),
            "yesterday_today_csv": "" if yesterday_today_csv in (None, "") else str(Path(yesterday_today_csv).resolve()),
            "target_device_ids": ";".join(sorted(target_set)),
            "acceptance_pct": acceptance_pct,
            "target_pair_count": len(target_point_rows),
            "failed_pair_count": failed_count,
            "overall_pass": bool(target_point_rows) and failed_count == 0,
            "firmware_replay_consistent": replay_consistent,
            "firmware_replay_max_abs_delta_ppm": replay.get("max_abs_replay_minus_measured_ppm"),
            "h2o_can_explain_current_co2_error": h2o_can_explain,
            "recommended_next_step": (
                "no_write_same_condition_repeat_before_any_CO2_or_H2O_write"
                if failed_count
                else "eligible_for_formal_report_generation"
            ),
            "opens_com_ports": False,
            "controls_water_or_gas_routes": False,
            "writes_coefficients": False,
        }
    ]
    return {
        "run_summary": run_summary,
        "device_summary": device_rows,
        "point_diagnostics": point_rows,
        "yesterday_today_900": yesterday_today,
    }


def write_co2_post_h2o_diagnostic(
    *,
    verification_summary_csv: str | Path,
    output_dir: str | Path,
    fit_residuals_csv: str | Path | None = None,
    firmware_replay_csv: str | Path | None = None,
    yesterday_today_csv: str | Path | None = None,
    target_device_ids: Iterable[str] = DEFAULT_TARGET_DEVICES,
    acceptance_pct: float = 1.0,
) -> Dict[str, Path]:
    payload = build_co2_post_h2o_diagnostic(
        verification_summary_csv=verification_summary_csv,
        fit_residuals_csv=fit_residuals_csv,
        firmware_replay_csv=firmware_replay_csv,
        yesterday_today_csv=yesterday_today_csv,
        target_device_ids=target_device_ids,
        acceptance_pct=acceptance_pct,
    )
    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    summary_csv = root / "co2_post_h2o_failure_diagnostic_summary.csv"
    device_csv = root / "co2_post_h2o_failure_diagnostic_by_device.csv"
    points_csv = root / "co2_post_h2o_failure_diagnostic_points.csv"
    compare_csv = root / "co2_post_h2o_yesterday_today_900.csv"
    json_path = root / "co2_post_h2o_failure_diagnostic.json"
    md_path = root / "co2_post_h2o_failure_diagnostic.md"
    _write_csv(summary_csv, payload["run_summary"])
    _write_csv(device_csv, payload["device_summary"])
    _write_csv(points_csv, payload["point_diagnostics"])
    _write_csv(compare_csv, payload["yesterday_today_900"])
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")

    summary = payload["run_summary"][0]
    lines = [
        "# V1.5 CO2 Post-H2O Failure Diagnostic",
        "",
        f"- Overall pass: `{summary.get('overall_pass')}`",
        f"- Failed target pairs: `{summary.get('failed_pair_count')}` / `{summary.get('target_pair_count')}`",
        f"- Firmware replay consistent: `{summary.get('firmware_replay_consistent')}` "
        f"(max delta `{summary.get('firmware_replay_max_abs_delta_ppm')}` ppm)",
        f"- H2O can explain current CO2 error: `{summary.get('h2o_can_explain_current_co2_error')}`",
        f"- Recommended next step: `{summary.get('recommended_next_step')}`",
        "",
        "## Device Summary",
        "",
        "| Device | Failed points | Mean error ppm | Error span ppm | Additive-like | Max error % | Max H2O dry effect % | Max ratio delta vs T20 fit | Conclusion |",
        "| --- | ---: | ---: | ---: | --- | ---: | ---: | ---: | --- |",
    ]
    for row in payload["device_summary"]:
        lines.append(
            "| {device} | {failed} | {mean_ppm} | {span_ppm} | {additive} | {err} | {h2o} | {ratio} | {conclusion} |".format(
                device=row.get("device_id", ""),
                failed=row.get("failed_point_count", ""),
                mean_ppm="" if row.get("mean_error_ppm") is None else f"{float(row['mean_error_ppm']):.3f}",
                span_ppm="" if row.get("error_ppm_span") is None else f"{float(row['error_ppm_span']):.3f}",
                additive=row.get("additive_offset_like", ""),
                err="" if row.get("max_abs_error_pct") is None else f"{float(row['max_abs_error_pct']):.3f}",
                h2o="" if row.get("max_h2o_dry_correction_pct") is None else f"{float(row['max_h2o_dry_correction_pct']):.4f}",
                ratio="" if row.get("max_abs_ratio_delta_vs_fit_T20") is None else f"{float(row['max_abs_ratio_delta_vs_fit_T20']):.6f}",
                conclusion=row.get("conclusion", ""),
            )
        )
    lines.extend(
        [
            "",
            "## Physical Meaning",
            "",
            "- The displayed firmware ppm is reproducible from the current SENCO1/SENCO3 formula and the current R/T/H2O inputs.",
            "- The H2O dry-basis correction during this dry CO2 check is too small to explain multi-percent CO2 errors.",
            "- Current T20 points show CO2 ratio shifts versus the original T20 fit evidence, so the failure should be treated as a ratio/temperature/physical-state problem before any new write.",
            "- The error shape is mostly additive in ppm on each analyzer, which is consistent with baseline/route-conditioning/optical-state shift and explains why the low point looks much worse in percent than 900-1000 ppm.",
        ]
    )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {
        "summary_csv": summary_csv,
        "device_csv": device_csv,
        "points_csv": points_csv,
        "compare_csv": compare_csv,
        "json": json_path,
        "markdown": md_path,
    }
