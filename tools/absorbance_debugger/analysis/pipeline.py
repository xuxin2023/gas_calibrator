"""End-to-end offline absorbance reconstruction pipeline."""

from __future__ import annotations

import json
import math
import shutil
from itertools import product
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from .absorbance_models import evaluate_absorbance_models
from .comparison import build_comparison_outputs
from ..io.run_bundle import RunBundle, discover_run_artifacts
from ..models.config import DebuggerConfig
from ..parsers.schema import (
    ANALYZER_FIELDS,
    POINT_COLUMNS,
    SAMPLE_COLUMNS,
    analyzer_prefix,
    build_analyzer_column,
    normalize_analyzer_label,
)
from ..plots.charts import (
    plot_absorbance_compare,
    plot_absorbance_model_fit_by_temp,
    plot_absorbance_model_fit_overall,
    plot_absorbance_model_old_vs_new,
    plot_absorbance_model_residual_hist,
    plot_absorbance_model_residual_vs_target,
    plot_absorbance_model_residual_vs_temp,
    plot_error_boxplot,
    plot_error_hist,
    plot_error_vs_target_ppm,
    plot_error_vs_temp,
    plot_per_temp_compare,
    plot_pressure_compare,
    plot_ratio_series,
    plot_r0_fit,
    plot_temperature_fit,
    plot_timeseries_base_final,
    plot_zero_compare,
    plot_zero_drift,
)
from ..reports.renderers import render_report_html, render_report_markdown, write_workbook
from .fits import clamp_positive, fit_polynomial, rolling_lowpass


def _ensure_output_dir(path: Path, overwrite: bool) -> None:
    if path.exists() and overwrite:
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _safe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except Exception:
        return None
    if not math.isfinite(number):
        return None
    return number


def _safe_bool(value: Any) -> bool | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y", "\u662f"}:
        return True
    if text in {"false", "0", "no", "n", "\u5426"}:
        return False
    return None


def _frame_to_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, encoding="utf-8-sig")


def _series_to_numeric(frame: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    out = frame.copy()
    for column in columns:
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce")
    return out


def _join_reasons(row: pd.Series) -> str:
    reasons = [str(item) for item in row if str(item)]
    return "|".join(reasons)


def _configured_analyzers(runtime_cfg: dict[str, Any], sample_columns: Iterable[str]) -> list[dict[str, Any]]:
    sample_columns = list(sample_columns)
    devices_cfg = runtime_cfg.get("devices", {}) if isinstance(runtime_cfg, dict) else {}
    configured = devices_cfg.get("gas_analyzers", []) if isinstance(devices_cfg, dict) else []
    out: list[dict[str, Any]] = []
    for slot in range(1, 17):
        prefix = analyzer_prefix(slot)
        if not any(column.startswith(prefix) for column in sample_columns):
            continue
        cfg = configured[slot - 1] if slot - 1 < len(configured) else {}
        label = normalize_analyzer_label(cfg.get("name") or f"GA{slot:02d}")
        out.append(
            {
                "slot": slot,
                "label": label,
                "runtime_name": cfg.get("name") or label,
                "runtime_device_id": cfg.get("device_id"),
            }
        )
    return out


def _build_inventory(bundle: RunBundle, artifacts: dict[str, str | None]) -> pd.DataFrame:
    rows = []
    for key, value in artifacts.items():
        rows.append(
            {
                "artifact_key": key,
                "relative_path": value,
                "detected": bool(value),
            }
        )
    rows.append(
        {
            "artifact_key": "source_path",
            "relative_path": str(bundle.source_path),
            "detected": True,
        }
    )
    return pd.DataFrame(rows)


def _normalize_points(points_raw: pd.DataFrame) -> pd.DataFrame:
    data = {}
    for key, column_name in POINT_COLUMNS.items():
        data[key] = points_raw[column_name] if column_name in points_raw.columns else pd.Series(np.nan, index=points_raw.index)
    points = pd.DataFrame(data)
    numeric_columns = [
        "point_row",
        "temp_set_c",
        "target_co2_ppm",
        "target_pressure_hpa",
        "analyzers_expected",
        "analyzers_with_frames",
        "analyzers_usable",
        "pressure_ctrl_hpa_mean",
        "pressure_gauge_hpa_mean",
        "temp_std_c_mean",
        "ratio_co2_filt_mean",
        "ratio_co2_raw_mean",
        "ref_signal_mean",
        "co2_signal_mean",
        "temp_shell_c_mean",
        "pressure_dev_kpa_mean",
    ]
    points = _series_to_numeric(points, numeric_columns)
    for column in (
        "point_title",
        "stage",
        "point_tag",
        "pressure_mode",
        "point_quality",
        "point_quality_reason",
        "analyzer_integrity",
        "missing_analyzers",
    ):
        if column in points.columns:
            points[column] = points[column].fillna("").astype(str)
    return points


def _normalize_samples(samples_raw: pd.DataFrame, points: pd.DataFrame, analyzers: list[dict[str, Any]]) -> pd.DataFrame:
    base_data = {}
    for key, column_name in SAMPLE_COLUMNS.items():
        base_data[key] = samples_raw[column_name] if column_name in samples_raw.columns else pd.Series(np.nan, index=samples_raw.index)
    base = pd.DataFrame(base_data)
    base = _series_to_numeric(
        base,
        [
            "sample_index",
            "sample_lag_ms",
            "point_row",
            "pressure_gauge_hpa",
            "temp_std_c",
            "target_co2_ppm",
            "target_pressure_hpa",
            "temp_set_c",
            "sampling_duration_ms",
        ],
    )
    for column in ("point_title", "stage", "point_tag", "stage_tag", "route", "pressure_mode", "pressure_target_label"):
        if column in base.columns:
            base[column] = base[column].fillna("").astype(str)

    point_summary = points[
        [
            "point_title",
            "point_row",
            "point_quality",
            "point_quality_reason",
            "point_quality_flag",
            "point_quality_blocking",
            "analyzer_integrity",
            "missing_analyzers",
            "abnormal_analyzers",
            "analyzers_expected",
            "analyzers_usable",
            "analyzers_with_frames",
            "pressure_ctrl_hpa_mean",
            "pressure_gauge_hpa_mean",
            "temp_std_c_mean",
        ]
    ].copy()

    long_frames: list[pd.DataFrame] = []
    for analyzer in analyzers:
        frame = base.copy()
        frame["analyzer"] = analyzer["label"]
        frame["analyzer_slot"] = analyzer["slot"]
        frame["device_id_runtime"] = analyzer["runtime_device_id"]
        frame["runtime_name"] = analyzer["runtime_name"]
        for field_name in ANALYZER_FIELDS:
            csv_column = build_analyzer_column(analyzer["slot"], field_name)
            frame[field_name] = samples_raw[csv_column] if csv_column in samples_raw.columns else pd.Series(np.nan, index=samples_raw.index)
        frame = frame.rename(columns={"device_id": "device_id_sample"})
        frame = _series_to_numeric(
            frame,
            [
                "mode",
                "mode2_field_count",
                "co2_ppm",
                "h2o_mmol",
                "co2_density",
                "h2o_density",
                "ratio_co2_filt",
                "ratio_co2_raw",
                "ratio_h2o_filt",
                "ratio_h2o_raw",
                "ref_signal",
                "co2_signal",
                "h2o_signal",
                "temp_cavity_c",
                "temp_shell_c",
                "pressure_dev_kpa",
                "frame_age_ms",
                "selected_frame_seq",
                "frame_offset_ms",
            ],
        )
        for bool_column in ("frame_is_stale", "frame_is_realtime", "has_frame", "usable_frame"):
            if bool_column in frame.columns:
                frame[bool_column] = frame[bool_column].map(_safe_bool)
        frame = frame.merge(point_summary, on=["point_title", "point_row"], how="left")
        frame["temp_std_c"] = frame["temp_std_c"].fillna(frame["temp_std_c_mean"])
        frame["pressure_gauge_hpa"] = frame["pressure_gauge_hpa"].fillna(frame["pressure_gauge_hpa_mean"])
        frame["pressure_std_hpa"] = frame["pressure_gauge_hpa"]
        frame["pressure_dev_raw_hpa"] = frame["pressure_dev_kpa"] * 10.0
        required_fields = [
            "ratio_co2_raw",
            "ratio_co2_filt",
            "ref_signal",
            "co2_signal",
            "temp_cavity_c",
            "temp_shell_c",
            "pressure_dev_kpa",
        ]
        frame["analyzer_data_complete"] = frame[required_fields].notna().all(axis=1)
        if "usable_frame" in frame.columns:
            frame["analyzer_data_complete"] = frame["analyzer_data_complete"] & frame["usable_frame"].fillna(True)
        long_frames.append(frame)
    if not long_frames:
        raise ValueError("No analyzer data columns were detected in the samples CSV")
    samples = pd.concat(long_frames, ignore_index=True)
    samples["route"] = samples["route"].fillna("").astype(str).str.lower()
    samples["point_quality"] = samples["point_quality"].fillna("").astype(str)
    samples["missing_analyzers"] = samples["missing_analyzers"].fillna("").astype(str)
    return samples


def _filter_samples(samples: pd.DataFrame, config: DebuggerConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    reasons = pd.DataFrame(index=samples.index)
    reasons["route_not_co2"] = np.where(samples["route"] != "co2", "route_not_co2", "")
    reasons["point_quality_fail"] = np.where(samples["point_quality"] != "pass", "point_quality_fail", "")
    reasons["warning_only_analyzer"] = np.where(
        samples["analyzer"].isin(config.warning_only_analyzers),
        "warning_only_analyzer",
        "",
    )
    reasons["analyzer_not_whitelisted"] = np.where(
        ~samples["analyzer"].isin(config.analyzer_whitelist)
        & ~samples["analyzer"].isin(config.warning_only_analyzers),
        "analyzer_not_whitelisted",
        "",
    )
    reasons["analyzer_data_incomplete"] = np.where(
        ~samples["analyzer_data_complete"],
        "analyzer_data_incomplete",
        "",
    )
    samples = samples.copy()
    samples["exclude_reason"] = reasons.apply(_join_reasons, axis=1)
    filtered = samples[samples["exclude_reason"] == ""].copy()
    excluded = samples[samples["exclude_reason"] != ""].copy()
    return filtered, excluded


def _fit_temperature(filtered: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    coeff_rows: list[dict[str, Any]] = []
    residual_rows: list[dict[str, Any]] = []
    lookup: dict[str, Any] = {}
    fit_input = filtered.dropna(subset=["temp_cavity_c", "temp_std_c"])
    for analyzer, subset in fit_input.groupby("analyzer"):
        x = subset["temp_cavity_c"].to_numpy(dtype=float)
        y = subset["temp_std_c"].to_numpy(dtype=float)
        for model_name, degree in (("linear", 1), ("quadratic", 2)):
            fit = fit_polynomial(x, y, degree=degree, model_name=model_name)
            lookup[(analyzer, model_name)] = fit
            predicted = fit.evaluate(x)
            coeff_rows.append(
                {
                    "analyzer": analyzer,
                    "model_name": model_name,
                    "degree": degree,
                    "sample_count": fit.sample_count,
                    "rmse_c": fit.rmse,
                    "mae_c": fit.mae,
                    "r2": fit.r2,
                    "coefficients_desc": json.dumps(fit.coefficients_desc),
                    "formula": fit.formula(variable="temp_cavity_c"),
                    "selected_for_temp_corr": model_name == "quadratic",
                }
            )
            for point_title, sample_index, x_val, y_val, y_hat in zip(
                subset["point_title"],
                subset["sample_index"],
                x,
                y,
                predicted,
                strict=False,
            ):
                residual_rows.append(
                    {
                        "analyzer": analyzer,
                        "model_name": model_name,
                        "point_title": point_title,
                        "sample_index": sample_index,
                        "temp_cavity_c": x_val,
                        "temp_std_c": y_val,
                        "temp_std_pred_c": float(y_hat),
                        "residual_c": float(y_hat - y_val),
                    }
                )
    return pd.DataFrame(coeff_rows), pd.DataFrame(residual_rows), lookup


def _pressure_offsets(filtered: pd.DataFrame, summary: pd.DataFrame | None) -> tuple[pd.DataFrame, dict[str, float]]:
    coeff_rows: list[dict[str, Any]] = []
    offsets: dict[str, float] = {}
    summary = summary.copy() if summary is not None else pd.DataFrame()
    for analyzer, subset in filtered.groupby("analyzer"):
        slot = int(subset["analyzer_slot"].iloc[0])
        raw_diff = subset["pressure_std_hpa"] - subset["pressure_dev_raw_hpa"]
        source = "estimated_from_run"
        offset_hpa = float(raw_diff.mean())
        if not summary.empty:
            match = summary[summary["analyzer_index"] == slot]
            if not match.empty and "offset_mean_kpa" in match.columns:
                offset_hpa = float(match.iloc[0]["offset_mean_kpa"]) * 10.0
                source = "pressure_offset_summary.csv"
        offsets[analyzer] = offset_hpa
        corrected = subset["pressure_dev_raw_hpa"] + offset_hpa
        coeff_rows.append(
            {
                "analyzer": analyzer,
                "analyzer_slot": slot,
                "offset_hpa": offset_hpa,
                "offset_kpa": offset_hpa / 10.0,
                "source": source,
                "sample_count": int(len(subset)),
                "raw_mean_diff_hpa": float(raw_diff.mean()),
                "raw_rmse_hpa": float(np.sqrt(np.mean(np.square(raw_diff)))),
                "corr_mean_diff_hpa": float((subset["pressure_std_hpa"] - corrected).mean()),
                "corr_rmse_hpa": float(np.sqrt(np.mean(np.square(subset["pressure_std_hpa"] - corrected)))),
                "formula": "pressure_corr_hpa = pressure_dev_raw_hpa + offset_hpa",
            }
        )
    return pd.DataFrame(coeff_rows), offsets


def _fit_r0(filtered: pd.DataFrame, config: DebuggerConfig) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[tuple[str, str, str, str], Any]]:
    zero = filtered[filtered["target_co2_ppm"] == 0].copy()
    obs_rows: list[dict[str, Any]] = []
    coeff_rows: list[dict[str, Any]] = []
    residual_rows: list[dict[str, Any]] = []
    lookup: dict[tuple[str, str, str, str], Any] = {}
    for temp_source, ratio_source in product(config.temp_sources, config.ratio_sources):
        subset = zero.dropna(subset=[temp_source, ratio_source]).copy()
        for _, row in subset.iterrows():
            obs_rows.append(
                {
                    "analyzer": row["analyzer"],
                    "point_title": row["point_title"],
                    "point_row": row["point_row"],
                    "sample_index": row["sample_index"],
                    "temp_source": temp_source,
                    "ratio_source": ratio_source,
                    "temp_use_c": row[temp_source],
                    "ratio_value": row[ratio_source],
                }
            )
        for analyzer, analyzer_subset in subset.groupby("analyzer"):
            x = analyzer_subset[temp_source].to_numpy(dtype=float)
            y = analyzer_subset[ratio_source].to_numpy(dtype=float)
            for model_name, degree in (("linear", 1), ("quadratic", 2), ("cubic", 3)):
                if len(analyzer_subset) <= degree:
                    continue
                fit = fit_polynomial(x, y, degree=degree, model_name=model_name)
                lookup[(analyzer, ratio_source, temp_source, model_name)] = fit
                predicted = fit.evaluate(x)
                coeff_rows.append(
                    {
                        "analyzer": analyzer,
                        "ratio_source": ratio_source,
                        "temp_source": temp_source,
                        "model_name": model_name,
                        "degree": degree,
                        "sample_count": fit.sample_count,
                        "rmse_ratio": fit.rmse,
                        "mae_ratio": fit.mae,
                        "r2": fit.r2,
                        "coefficients_desc": json.dumps(fit.coefficients_desc),
                        "formula": fit.formula(variable=temp_source),
                        "selected_for_absorbance": model_name == config.default_r0_model,
                    }
                )
                for point_title, sample_index, x_val, y_val, y_hat in zip(
                    analyzer_subset["point_title"],
                    analyzer_subset["sample_index"],
                    x,
                    y,
                    predicted,
                    strict=False,
                ):
                    residual_rows.append(
                        {
                            "analyzer": analyzer,
                            "ratio_source": ratio_source,
                            "temp_source": temp_source,
                            "model_name": model_name,
                            "point_title": point_title,
                            "sample_index": sample_index,
                            "temp_use_c": x_val,
                            "ratio_observed": y_val,
                            "ratio_pred": float(y_hat),
                            "residual_ratio": float(y_hat - y_val),
                        }
                    )
    return pd.DataFrame(obs_rows), pd.DataFrame(coeff_rows), pd.DataFrame(residual_rows), lookup


def _compute_absorbance(filtered: pd.DataFrame, config: DebuggerConfig, r0_lookup: dict[tuple[str, str, str, str], Any]) -> tuple[pd.DataFrame, pd.DataFrame]:
    sample_frames: list[pd.DataFrame] = []
    for ratio_source, temp_source, pressure_source in product(
        config.ratio_sources,
        config.temp_sources,
        config.pressure_sources,
    ):
        for analyzer, subset in filtered.groupby("analyzer"):
            fit_key = (analyzer, ratio_source, temp_source, config.default_r0_model)
            if fit_key not in r0_lookup:
                continue
            fit = r0_lookup[fit_key]
            work = subset.copy()
            temp_values = work[temp_source].to_numpy(dtype=float)
            ratio_values = work[ratio_source].to_numpy(dtype=float)
            pressure_values = work[pressure_source].to_numpy(dtype=float)
            r0_values = fit.evaluate(temp_values)
            ratio_clamped = clamp_positive(ratio_values, config.eps)
            r0_clamped = clamp_positive(r0_values, config.eps)
            pressure_clamped = clamp_positive(pressure_values, config.p_min_hpa)
            log_term = -np.log(ratio_clamped / r0_clamped)
            work["ratio_source"] = ratio_source
            work["temp_source"] = temp_source
            work["pressure_source"] = pressure_source
            work["r0_model"] = config.default_r0_model
            work["branch_id"] = "__".join((ratio_source, temp_source, pressure_source, config.default_r0_model))
            work["is_default_branch"] = work["branch_id"] == config.default_branch_id()
            work["ratio_in"] = ratio_values
            work["temp_use_c"] = temp_values
            work["pressure_use_hpa"] = pressure_values
            work["R0_T"] = r0_values
            work["ratio_clamped"] = ratio_clamped
            work["R0_T_clamped"] = r0_clamped
            work["pressure_clamped_hpa"] = pressure_clamped
            work["pressure_scale_factor"] = config.p_ref_hpa / pressure_clamped
            work["A_raw"] = log_term * work["pressure_scale_factor"]
            work["A_alt"] = log_term / pressure_clamped
            work["absorbance_formula"] = (
                "A_raw = -ln(clamp(R_in, eps) / clamp(R0(T_use), eps)) * "
                "(P_ref / clamp(P_use, P_min))"
            )
            sample_frames.append(work)
    if not sample_frames:
        raise ValueError("No absorbance branches could be computed")

    absorbance_samples = pd.concat(sample_frames, ignore_index=True)
    group_cols = [
        "analyzer",
        "analyzer_slot",
        "point_title",
        "point_row",
        "point_tag",
        "temp_set_c",
        "target_co2_ppm",
        "ratio_source",
        "temp_source",
        "pressure_source",
        "r0_model",
        "branch_id",
        "is_default_branch",
    ]
    absorbance_points = (
        absorbance_samples.groupby(group_cols, dropna=False)
        .agg(
            sample_count=("sample_index", "count"),
            ratio_in_mean=("ratio_in", "mean"),
            temp_use_mean_c=("temp_use_c", "mean"),
            pressure_use_mean_hpa=("pressure_use_hpa", "mean"),
            R0_T_mean=("R0_T", "mean"),
            A_mean=("A_raw", "mean"),
            A_std=("A_raw", "std"),
            A_min=("A_raw", "min"),
            A_max=("A_raw", "max"),
            A_alt_mean=("A_alt", "mean"),
        )
        .reset_index()
    )
    absorbance_points["A_std"] = absorbance_points["A_std"].fillna(0.0)
    return absorbance_samples, absorbance_points


def _base_final(absorbance_samples: pd.DataFrame, config: DebuggerConfig) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    timeseries = absorbance_samples[absorbance_samples["branch_id"] == config.default_branch_id()].copy()
    timeseries = timeseries.sort_values(["analyzer", "point_row", "sample_index"]).reset_index(drop=True)
    base_final_source = "per-point sample order from samples_*.csv"
    timeseries["A_base"] = np.nan
    timeseries["A_turb"] = np.nan
    timeseries["A_final"] = np.nan
    if config.enable_base_final:
        for (_, _), idx in timeseries.groupby(["analyzer", "point_title"]).groups.items():
            values = timeseries.loc[idx, "A_raw"].to_numpy(dtype=float)
            if values.size == 0:
                continue
            base = rolling_lowpass(values, window=min(3, values.size))
            turb = values - base
            final = base + rolling_lowpass(turb, window=min(3, values.size))
            timeseries.loc[idx, "A_base"] = base
            timeseries.loc[idx, "A_turb"] = turb
            timeseries.loc[idx, "A_final"] = final

    point_summary = (
        timeseries.groupby(
            ["analyzer", "point_title", "point_row", "temp_set_c", "target_co2_ppm"],
            dropna=False,
        )
        .agg(
            sample_count=("sample_index", "count"),
            A_raw_mean=("A_raw", "mean"),
            A_base_mean=("A_base", "mean"),
            A_final_mean=("A_final", "mean"),
        )
        .reset_index()
    )
    point_summary["base_final_enabled"] = config.enable_base_final
    return timeseries, point_summary, base_final_source


def _fit_absorbance_models(
    absorbance_points: pd.DataFrame,
    config: DebuggerConfig,
    output_dir: Path,
) -> dict[str, pd.DataFrame]:
    default_points = absorbance_points[absorbance_points["branch_id"] == config.default_branch_id()].copy()
    model_results = evaluate_absorbance_models(default_points, config)
    _frame_to_csv(output_dir / "step_06_absorbance_model_candidates.csv", model_results["candidates"])
    _frame_to_csv(output_dir / "step_06_absorbance_model_scores.csv", model_results["scores"])
    _frame_to_csv(output_dir / "step_06_absorbance_model_selection.csv", model_results["selection"])
    _frame_to_csv(output_dir / "step_06_absorbance_model_coefficients.csv", model_results["coefficients"])
    _frame_to_csv(output_dir / "step_06_absorbance_model_residuals.csv", model_results["residuals"])

    plot_dir = output_dir / "step_06_absorbance_model_plots"
    plot_dir.mkdir(parents=True, exist_ok=True)
    best_predictions = model_results["best_predictions"]
    plot_absorbance_model_fit_overall(best_predictions, plot_dir / "absorbance_model_fit_overall.png")
    plot_absorbance_model_fit_by_temp(best_predictions, plot_dir / "absorbance_model_fit_by_temp.png")
    plot_absorbance_model_residual_hist(best_predictions, plot_dir / "absorbance_model_residual_hist.png")
    plot_absorbance_model_residual_vs_temp(best_predictions, plot_dir / "absorbance_model_residual_vs_temp.png")
    plot_absorbance_model_residual_vs_target(best_predictions, plot_dir / "absorbance_model_residual_vs_target.png")
    return model_results


def _load_old_ratio_residuals(bundle: RunBundle) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for relative_path in bundle.list_files():
        name = Path(relative_path).name
        if not (name.startswith("co2_GA") and name.endswith("_residuals.csv")):
            continue
        frame = bundle.read_csv(relative_path)
        if "Analyzer" not in frame.columns:
            continue
        frame = frame.rename(
            columns={
                "Analyzer": "analyzer",
                "PointRow": "point_row",
                "PointTitle": "point_title",
                "R": "old_ratio_value",
                "P": "old_pressure_hpa",
                "T_c": "old_temp_c",
                "prediction_simplified": "old_prediction_ppm",
                "error_simplified": "old_residual_ppm",
            }
        )
        rows.append(frame)
    if not rows:
        return pd.DataFrame(
            columns=["analyzer", "point_row", "point_title", "old_prediction_ppm", "old_residual_ppm", "old_ratio_value"]
        )
    combined = pd.concat(rows, ignore_index=True)
    combined["analyzer"] = combined["analyzer"].fillna("").astype(str).str.upper()
    combined["point_row"] = pd.to_numeric(combined["point_row"], errors="coerce")
    return combined


def _comparison_tables(
    bundle: RunBundle,
    filtered: pd.DataFrame,
    absorbance_samples: pd.DataFrame,
    absorbance_points: pd.DataFrame,
    model_results: dict[str, pd.DataFrame],
    config: DebuggerConfig,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame | list[str]]]:
    point_raw = (
        filtered.groupby(["analyzer", "point_title", "point_row", "point_tag", "temp_set_c", "target_co2_ppm"], dropna=False)
        .agg(
            ratio_co2_raw_mean=("ratio_co2_raw", "mean"),
            ratio_co2_filt_mean=("ratio_co2_filt", "mean"),
            pressure_std_hpa_mean=("pressure_std_hpa", "mean"),
            pressure_corr_hpa_mean=("pressure_corr_hpa", "mean"),
            temp_std_c_mean=("temp_std_c", "mean"),
            temp_corr_c_mean=("temp_corr_c", "mean"),
        )
        .reset_index()
    )
    default_points = absorbance_points[absorbance_points["branch_id"] == config.default_branch_id()].copy()
    default_samples = absorbance_samples[absorbance_samples["branch_id"] == config.default_branch_id()].copy()
    default_sample_points = (
        default_samples.groupby(["analyzer", "point_title", "point_row"], dropna=False)
        .agg(
            A_raw=("A_raw", "mean"),
            pressure_source=("pressure_source", "first"),
            temperature_source=("temp_source", "first"),
            ratio_source_selected=("ratio_source", "first"),
        )
        .reset_index()
    )
    compare = point_raw.merge(
        default_points[
            [
                "analyzer",
                "point_title",
                "point_row",
                "A_mean",
                "A_std",
                "A_alt_mean",
                "R0_T_mean",
            ]
        ],
        on=["analyzer", "point_title", "point_row"],
        how="left",
    )
    compare = compare.merge(
        default_sample_points,
        on=["analyzer", "point_title", "point_row"],
        how="left",
    )
    old = _load_old_ratio_residuals(bundle)
    compare = compare.merge(
        old[["analyzer", "point_row", "point_title", "old_prediction_ppm", "old_residual_ppm", "old_ratio_value"]],
        on=["analyzer", "point_row", "point_title"],
        how="left",
    )
    best_predictions = model_results["best_predictions"].copy()
    if not best_predictions.empty:
        compare = compare.merge(
            best_predictions[
                [
                    "analyzer_id",
                    "point_title",
                    "point_row",
                    "best_absorbance_model",
                    "best_absorbance_model_label",
                    "selection_reason",
                    "selected_prediction_scope",
                    "best_overall_fit_pred_ppm",
                    "best_overall_fit_error_ppm",
                    "best_validation_pred_ppm",
                    "best_validation_error_ppm",
                    "selected_pred_ppm",
                    "selected_error_ppm",
                ]
            ].rename(columns={"analyzer_id": "analyzer"}),
            on=["analyzer", "point_title", "point_row"],
            how="left",
        )
    else:
        compare["best_absorbance_model"] = np.nan
        compare["best_absorbance_model_label"] = np.nan
        compare["selection_reason"] = np.nan
        compare["selected_prediction_scope"] = np.nan
        compare["best_overall_fit_pred_ppm"] = np.nan
        compare["best_overall_fit_error_ppm"] = np.nan
        compare["best_validation_pred_ppm"] = np.nan
        compare["best_validation_error_ppm"] = np.nan
        compare["selected_pred_ppm"] = np.nan
        compare["selected_error_ppm"] = np.nan

    compare["new_pred_ppm"] = compare["selected_pred_ppm"]
    compare["old_pred_ppm"] = compare["old_prediction_ppm"]
    compare["old_error"] = compare["old_pred_ppm"] - compare["target_co2_ppm"]
    compare["new_error"] = compare["selected_error_ppm"].combine_first(compare["new_pred_ppm"] - compare["target_co2_ppm"])
    compare["winner_for_point"] = compare.apply(
        lambda row: (
            "old_chain"
            if abs(row["old_error"]) < abs(row["new_error"])
            else "new_chain"
            if abs(row["new_error"]) < abs(row["old_error"])
            else "tie"
        )
        if pd.notna(row["old_error"]) and pd.notna(row["new_error"])
        else "new_chain"
        if pd.isna(row["old_error"]) and pd.notna(row["new_error"])
        else "old_chain"
        if pd.notna(row["old_error"]) and pd.isna(row["new_error"])
        else "tie",
        axis=1,
    )
    compare["pressure_source"] = compare["pressure_source"].map(
        {
            "pressure_std_hpa": "P_std",
            "pressure_corr_hpa": "P_corr",
        }
    ).fillna(config.default_pressure_label())
    compare["temperature_source"] = compare["temperature_source"].map(
        {
            "temp_std_c": "T_std",
            "temp_corr_c": "T_corr",
        }
    ).fillna(config.default_temperature_label())
    compare["ratio_source_selected"] = compare["ratio_source_selected"].map(
        {
            "ratio_co2_raw": "raw",
            "ratio_co2_filt": "filt",
        }
    ).fillna(config.default_ratio_label())

    point_reconciliation = compare.rename(
        columns={
            "analyzer": "analyzer_id",
            "temp_set_c": "temp_c",
            "target_co2_ppm": "target_ppm",
        }
    )[
        [
            "analyzer_id",
            "temp_c",
            "target_ppm",
            "old_pred_ppm",
            "new_pred_ppm",
            "old_error",
            "new_error",
            "winner_for_point",
            "ratio_co2_raw_mean",
            "ratio_co2_filt_mean",
            "A_raw",
            "A_mean",
            "pressure_source",
            "temperature_source",
            "ratio_source_selected",
            "best_absorbance_model",
            "best_absorbance_model_label",
            "selection_reason",
            "selected_prediction_scope",
            "best_overall_fit_pred_ppm",
            "best_validation_pred_ppm",
            "point_title",
            "point_row",
            "point_tag",
            "A_std",
            "A_alt_mean",
            "R0_T_mean",
        ]
    ].copy()
    comparison_outputs = build_comparison_outputs(point_reconciliation, model_results["selection"])
    return point_reconciliation, comparison_outputs


def _report_tables(
    config: DebuggerConfig,
    bundle: RunBundle,
    output_dir: Path,
    points: pd.DataFrame,
    filtered: pd.DataFrame,
    temp_coeffs: pd.DataFrame,
    pressure_coeffs: pd.DataFrame,
    r0_coeffs: pd.DataFrame,
    model_results: dict[str, pd.DataFrame],
    comparison_outputs: dict[str, pd.DataFrame | list[str]],
    validation_table: pd.DataFrame,
    base_final_enabled: bool,
    base_final_source: str,
) -> dict[str, Any]:
    co2_points = points[points["stage"].astype(str).str.lower() == "co2"].copy()
    zero_temps = sorted(co2_points[co2_points["target_co2_ppm"] == 0]["temp_set_c"].dropna().unique().tolist())
    co2_temps = sorted(co2_points["temp_set_c"].dropna().unique().tolist())
    missing_zero_temps = [temp for temp in co2_temps if temp not in zero_temps]
    main_r0 = r0_coeffs[
        (r0_coeffs["ratio_source"] == config.default_ratio_source)
        & (r0_coeffs["temp_source"] == config.default_temp_source)
        & (r0_coeffs["model_name"] == config.default_r0_model)
    ].copy()
    overview_summary = comparison_outputs["overview_summary"]
    by_temperature = comparison_outputs["by_temperature"]
    by_concentration = comparison_outputs["by_concentration_range"]
    zero_special = comparison_outputs["zero_special"]
    regression_overall = comparison_outputs["regression_overall"]
    auto_conclusions = comparison_outputs["auto_conclusions"]
    conclusion_lines = comparison_outputs["conclusion_lines"]
    absorbance_model_scores = model_results["scores"]
    absorbance_model_selection = model_results["selection"]
    return {
        "run_name": bundle.source_path.stem,
        "input_path": str(bundle.source_path),
        "output_dir": str(output_dir),
        "point_count": int(len(points)),
        "co2_point_count": int(len(co2_points)),
        "h2o_point_count": int(len(points) - len(co2_points)),
        "main_analyzers": list(config.analyzer_whitelist),
        "warning_only_analyzers": list(config.warning_only_analyzers),
        "detected_analyzers": sorted(filtered["analyzer"].dropna().unique().tolist()),
        "co2_temperatures": co2_temps,
        "zero_temperatures": zero_temps,
        "missing_zero_temperatures": missing_zero_temps,
        "formulas": {
            "Temperature correction (selected)": "temp_corr_c = q0 + q1*temp_cavity_c + q2*temp_cavity_c^2",
            "Pressure correction": "pressure_corr_hpa = pressure_dev_raw_hpa + offset_hpa",
            "R0(T) default": "R0(T) = c0 + c1*T + c2*T^2",
            "Absorbance main": "A_raw = -ln(clamp(R_in, eps) / clamp(R0(T_use), eps)) * (P_ref / clamp(P_use, P_min))",
            "Absorbance alt": "A_alt = -ln(R_in / R0(T_use)) / P_use",
            "Absorbance model A": "ppm = b0 + b1*A",
            "Absorbance model B": "ppm = b0 + b1*A + b2*A^2",
            "Absorbance model C": "ppm = b0 + b1*A + b2*A^2 + b3*A^3",
            "Absorbance model D": "ppm = b0 + b1*A + b2*A^2 + b3*T + b4*T^2",
            "Absorbance model E": "ppm = b0 + b1*A + b2*A^2 + b3*T + b4*A*T",
        },
        "validation_table": validation_table,
        "temperature_coefficients": temp_coeffs,
        "pressure_coefficients": pressure_coeffs,
        "r0_coefficients": main_r0,
        "absorbance_model_scores": absorbance_model_scores,
        "absorbance_model_selection": absorbance_model_selection,
        "overview_summary": overview_summary,
        "by_temperature": by_temperature,
        "by_concentration_range": by_concentration,
        "zero_special": zero_special,
        "regression_overall": regression_overall,
        "auto_conclusions": auto_conclusions,
        "comparison_conclusions": conclusion_lines,
        "base_final_enabled": base_final_enabled,
        "base_final_source": base_final_source,
        "limitations": [
            "40 C has no 0 ppm CO2 point in this run, so it is excluded from R0(T) fitting and used only as an extrapolation check.",
            "Pressure handling is offset-only in V1 of this debugger; it does not fit a full pressure polynomial yet.",
            "GA04 is detected but excluded from the main fit path by default because the run marks it missing for most points.",
            "Base/final is sample-order based and stays disabled by default for static calibration review.",
            "The new-chain comparison now uses grouped validation of bounded absorbance candidates, but it is still limited by this run's sparse temperature-zero coverage and single-pressure emphasis.",
        ],
        "next_steps": [
            "Add a 40 C / 0 ppm CO2 point so R0(T) can be anchored instead of extrapolated there.",
            "Collect an explicit multi-pressure CO2 sweep before enabling a full pressure polynomial.",
            "Repair or re-enable GA04 data availability before adding it to the main absorbance fit set.",
            "If the absorbance model improves zero drift but still loses overall, add an external validation run before considering any production migration.",
        ],
    }


def execute_pipeline(config: DebuggerConfig) -> dict[str, Any]:
    """Run the offline absorbance debugger and emit all requested artifacts."""

    bundle = RunBundle(config.input_path)
    artifacts = discover_run_artifacts(bundle)
    _ensure_output_dir(config.output_dir, overwrite=config.overwrite_output)

    runtime_cfg = bundle.read_json(artifacts.files["runtime_config"])
    points_raw = bundle.read_csv(artifacts.files["points_readable"])
    points_machine_raw = bundle.read_csv(artifacts.files["points"])
    samples_raw = bundle.read_csv(artifacts.files["samples"])
    pressure_summary = (
        bundle.read_csv(artifacts.files["pressure_offset_summary"])
        if artifacts.files.get("pressure_offset_summary")
        else None
    )

    inventory = _build_inventory(bundle, dict(artifacts.files))
    points = _normalize_points(points_raw)
    analyzers = _configured_analyzers(runtime_cfg, samples_raw.columns)
    samples_core = _normalize_samples(samples_raw, points, analyzers)

    run_summary = {
        "run_name": artifacts.run_name,
        "input_path": str(config.input_path),
        "output_dir": str(config.output_dir),
        "point_count": int(len(points)),
        "points_machine_row_count": int(len(points_machine_raw)),
        "sample_row_count": int(len(samples_raw)),
        "analyzers_detected": [item["label"] for item in analyzers],
    }

    _frame_to_csv(config.output_dir / "step_00_run_inventory.csv", inventory)
    (config.output_dir / "step_00_run_summary.json").write_text(
        json.dumps(run_summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    _frame_to_csv(config.output_dir / "step_01_samples_core.csv", samples_core)

    filtered, excluded = _filter_samples(samples_core, config)
    _frame_to_csv(config.output_dir / "step_02_samples_filtered.csv", filtered)
    _frame_to_csv(config.output_dir / "step_02_excluded_rows.csv", excluded)

    temp_coeffs, temp_residuals, temp_lookup = _fit_temperature(filtered)
    filtered = filtered.copy()
    filtered["temp_corr_c"] = filtered.apply(
        lambda row: temp_lookup[(row["analyzer"], "quadratic")].evaluate([row["temp_cavity_c"]])[0],
        axis=1,
    )
    _frame_to_csv(config.output_dir / "step_03_temperature_fit_coefficients.csv", temp_coeffs)
    _frame_to_csv(config.output_dir / "step_03_temperature_fit_residuals.csv", temp_residuals)
    plot_temperature_fit(
        filtered,
        temp_coeffs.assign(coefficients_desc=temp_coeffs["coefficients_desc"].map(json.loads)),
        config.output_dir / "step_03_temperature_fit_plot.png",
    )

    pressure_coeffs, offsets = _pressure_offsets(filtered, pressure_summary)
    filtered["offset_hpa"] = filtered["analyzer"].map(offsets)
    filtered["pressure_corr_hpa"] = filtered["pressure_dev_raw_hpa"] + filtered["offset_hpa"]
    pressure_compare = filtered[
        [
            "analyzer",
            "point_title",
            "point_row",
            "sample_index",
            "pressure_std_hpa",
            "pressure_dev_raw_hpa",
            "pressure_corr_hpa",
        ]
    ].copy()
    pressure_compare["diff_raw_hpa"] = pressure_compare["pressure_dev_raw_hpa"] - pressure_compare["pressure_std_hpa"]
    pressure_compare["diff_corr_hpa"] = pressure_compare["pressure_corr_hpa"] - pressure_compare["pressure_std_hpa"]
    _frame_to_csv(config.output_dir / "step_04_pressure_fit_coefficients.csv", pressure_coeffs)
    _frame_to_csv(config.output_dir / "step_04_pressure_compare_samples.csv", pressure_compare)
    plot_pressure_compare(pressure_compare, config.output_dir / "step_04_pressure_compare_plot.png")

    r0_obs, r0_coeffs, r0_residuals, r0_lookup = _fit_r0(filtered, config)
    _frame_to_csv(config.output_dir / "step_05_r0_observations.csv", r0_obs)
    _frame_to_csv(config.output_dir / "step_05_r0_fit_coefficients.csv", r0_coeffs)
    _frame_to_csv(config.output_dir / "step_05_r0_fit_residuals.csv", r0_residuals)
    plot_r0_fit(
        r0_obs,
        r0_coeffs.assign(coefficients_desc=r0_coeffs["coefficients_desc"].map(json.loads)),
        config.output_dir / "step_05_r0_fit_plot.png",
    )

    absorbance_samples, absorbance_points = _compute_absorbance(filtered, config, r0_lookup)
    _frame_to_csv(config.output_dir / "step_06_absorbance_samples.csv", absorbance_samples)
    _frame_to_csv(config.output_dir / "step_06_absorbance_points.csv", absorbance_points)
    model_results = _fit_absorbance_models(absorbance_points, config, config.output_dir)

    timeseries, base_final_points, base_final_source = _base_final(absorbance_samples, config)
    _frame_to_csv(config.output_dir / "step_07_absorbance_timeseries.csv", timeseries)
    _frame_to_csv(config.output_dir / "step_07_absorbance_base_final.csv", base_final_points)
    plot_timeseries_base_final(
        timeseries,
        config.output_dir / "step_07_absorbance_base_final_plot.png",
        config.enable_base_final,
    )

    point_reconciliation, comparison_outputs = _comparison_tables(
        bundle,
        filtered,
        absorbance_samples,
        absorbance_points,
        model_results,
        config,
    )
    plot_absorbance_model_old_vs_new(
        point_reconciliation,
        config.output_dir / "step_06_absorbance_model_plots" / "absorbance_model_old_vs_new.png",
    )
    compare_plots_dir = config.output_dir / "step_08_compare_plots"
    compare_plots_dir.mkdir(parents=True, exist_ok=True)
    default_sample_compare = absorbance_samples[absorbance_samples["branch_id"] == config.default_branch_id()].copy()
    plot_absorbance_compare(point_reconciliation.rename(columns={"analyzer_id": "analyzer", "temp_c": "temp_set_c", "target_ppm": "target_co2_ppm"}), "ratio_co2_raw_mean", "ratio_co2_raw_mean", compare_plots_dir / "target_vs_ratio_raw.png")
    plot_absorbance_compare(default_sample_compare, "A_raw", "A_raw", compare_plots_dir / "target_vs_A_raw.png")
    plot_absorbance_compare(point_reconciliation.rename(columns={"analyzer_id": "analyzer", "temp_c": "temp_set_c", "target_ppm": "target_co2_ppm"}), "A_mean", "A_mean", compare_plots_dir / "target_vs_A_mean.png")
    plot_zero_drift(
        point_reconciliation[point_reconciliation["target_ppm"] == 0].rename(columns={"analyzer_id": "analyzer", "temp_c": "temp_set_c"}),
        "ratio_co2_raw_mean",
        "ratio_co2_raw_mean",
        compare_plots_dir / "zero_drift_ratio_raw.png",
    )
    plot_zero_drift(
        point_reconciliation[point_reconciliation["target_ppm"] == 0].rename(columns={"analyzer_id": "analyzer", "temp_c": "temp_set_c"}),
        "A_mean",
        "A_mean",
        compare_plots_dir / "zero_drift_A_mean.png",
    )
    plot_ratio_series(point_reconciliation.rename(columns={"analyzer_id": "analyzer", "temp_c": "temp_set_c", "target_ppm": "target_co2_ppm"}), compare_plots_dir / "ratio_filt_abs_compare.png")
    plot_error_hist(point_reconciliation, compare_plots_dir / "old_vs_new_error_hist.png")
    plot_error_boxplot(point_reconciliation, compare_plots_dir / "old_vs_new_error_boxplot.png")
    plot_error_vs_temp(point_reconciliation, compare_plots_dir / "error_vs_temp.png")
    plot_error_vs_target_ppm(point_reconciliation, compare_plots_dir / "error_vs_target_ppm.png")
    plot_per_temp_compare(comparison_outputs["by_temperature"], compare_plots_dir / "per_temp_compare.png")
    plot_zero_compare(comparison_outputs["zero_special"], compare_plots_dir / "zero_compare.png")

    _frame_to_csv(config.output_dir / "step_08_overview_summary.csv", comparison_outputs["overview_summary"])
    _frame_to_csv(config.output_dir / "step_08_by_temperature.csv", comparison_outputs["by_temperature"])
    _frame_to_csv(config.output_dir / "step_08_by_concentration_range.csv", comparison_outputs["by_concentration_range"])
    _frame_to_csv(config.output_dir / "step_08_zero_special.csv", comparison_outputs["zero_special"])
    _frame_to_csv(config.output_dir / "step_08_regression_overall.csv", comparison_outputs["regression_overall"])
    _frame_to_csv(config.output_dir / "step_08_regression_by_temperature.csv", comparison_outputs["regression_by_temperature"])
    _frame_to_csv(config.output_dir / "step_08_point_reconciliation.csv", point_reconciliation)
    _frame_to_csv(config.output_dir / "step_08_auto_conclusions.csv", comparison_outputs["auto_conclusions"])
    _frame_to_csv(config.output_dir / "step_08_residual_summary.csv", comparison_outputs["overview_summary"])
    write_workbook(
        config.output_dir / "step_08_old_vs_new_compare.xlsx",
        {
            "abs_model_selection": model_results["selection"],
            "abs_model_scores": model_results["scores"],
            "overview_summary": comparison_outputs["overview_summary"],
            "by_temperature": comparison_outputs["by_temperature"],
            "by_concentration": comparison_outputs["by_concentration_range"],
            "zero_special": comparison_outputs["zero_special"],
            "reg_overall": comparison_outputs["regression_overall"],
            "reg_by_temp": comparison_outputs["regression_by_temperature"],
            "point_reconciliation": point_reconciliation,
            "auto_conclusions": comparison_outputs["auto_conclusions"],
        },
    )

    co2_points = points[points["stage"].astype(str).str.lower() == "co2"].copy()
    zero_points = co2_points[co2_points["target_co2_ppm"] == 0]
    validation_table = pd.DataFrame(
        [
            {
                "check_name": "identified_48_points",
                "expected": 48,
                "observed": int(len(points)),
                "passed": int(len(points)) == 48,
            },
            {
                "check_name": "identified_zero_ppm_temperatures",
                "expected": "[-20,-10,0,10,20,30]",
                "observed": json.dumps(sorted(zero_points["temp_set_c"].dropna().unique().tolist())),
                "passed": sorted(zero_points["temp_set_c"].dropna().unique().tolist()) == [-20.0, -10.0, 0.0, 10.0, 20.0, 30.0],
            },
            {
                "check_name": "identified_missing_40c_zero_ppm",
                "expected": True,
                "observed": 40.0 not in sorted(zero_points["temp_set_c"].dropna().unique().tolist()),
                "passed": 40.0 not in sorted(zero_points["temp_set_c"].dropna().unique().tolist()),
            },
            {
                "check_name": "main_results_only_ga01_to_ga03",
                "expected": "GA01,GA02,GA03",
                "observed": ",".join(sorted(filtered["analyzer"].dropna().unique().tolist())),
                "passed": sorted(filtered["analyzer"].dropna().unique().tolist()) == ["GA01", "GA02", "GA03"],
            },
        ]
    )

    report = _report_tables(
        config=config,
        bundle=bundle,
        output_dir=config.output_dir,
        points=points,
        filtered=filtered,
        temp_coeffs=temp_coeffs,
        pressure_coeffs=pressure_coeffs,
        r0_coeffs=r0_coeffs,
        model_results=model_results,
        comparison_outputs=comparison_outputs,
        validation_table=validation_table,
        base_final_enabled=config.enable_base_final,
        base_final_source=base_final_source,
    )
    report_md = render_report_markdown(report)
    report_html = render_report_html(report)
    (config.output_dir / "report.md").write_text(report_md, encoding="utf-8")
    (config.output_dir / "report.html").write_text(report_html, encoding="utf-8")
    write_workbook(
        config.output_dir / "report.xlsx",
        {
            "validation": validation_table,
            "temperature_fit": temp_coeffs,
            "pressure_fit": pressure_coeffs,
            "r0_fit": r0_coeffs,
            "abs_model_scores": model_results["scores"],
            "abs_model_selection": model_results["selection"],
            "abs_model_coeffs": model_results["coefficients"],
            "overview_summary": comparison_outputs["overview_summary"],
            "by_temperature": comparison_outputs["by_temperature"],
            "by_concentration": comparison_outputs["by_concentration_range"],
            "zero_special": comparison_outputs["zero_special"],
            "regression": comparison_outputs["regression_overall"],
            "point_compare": point_reconciliation,
            "auto_conclusions": comparison_outputs["auto_conclusions"],
        },
    )

    return {
        "output_dir": config.output_dir,
        "inventory": inventory,
        "points": points,
        "samples_core": samples_core,
        "filtered": filtered,
        "excluded": excluded,
        "validation_table": validation_table,
        "model_results": model_results,
        "comparison_outputs": comparison_outputs,
        "point_reconciliation": point_reconciliation,
    }
