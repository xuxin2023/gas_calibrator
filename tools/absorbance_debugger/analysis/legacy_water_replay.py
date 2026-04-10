"""Diagnostic-only replay of legacy water-lineage inheritance inside the debugger."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .absorbance_models import _fit_one_candidate, active_model_specs
from .diagnostics import build_point_raw_summary

WATER_LINEAGE_MODES: tuple[str, ...] = (
    "none",
    "simplified_subzero_anchor",
    "legacy_h2o_summary_selection",
    "legacy_h2o_summary_selection_plus_zero_ppm_rows",
)

LOW_RANGE_NAME = "0~200 ppm"
MAIN_RANGE_NAME = "200~1000 ppm"


@dataclass(frozen=True)
class LegacyWaterReplayRules:
    """Repo-backed defaults mirrored from the legacy water summary selection."""

    repo_root: Path
    config_path: Path
    corrected_water_report_path: Path
    include_h2o_phase: bool = True
    include_co2_temp_groups_c: tuple[float, ...] = (-20.0, -10.0, 0.0)
    include_co2_zero_ppm_rows: bool = True
    include_co2_zero_ppm_temp_groups_c: tuple[float, ...] = (10.0,)
    co2_zero_ppm_target: float = 0.0
    co2_zero_ppm_tolerance: float = 0.5
    temp_tolerance_c: float = 0.6
    water_first_all_temps: bool = True
    water_first_temp_gte: float = 10.0


@dataclass(frozen=True)
class ReplayCorrectionFit:
    """One diagnostic-only absorbance replay correction surface."""

    water_lineage_mode: str
    active_terms: tuple[str, ...]
    coefficients: tuple[float, ...]
    sample_count: int
    rmse_zero_absorbance: float
    fit_status: str

    def evaluate(self, frame: pd.DataFrame) -> pd.Series:
        valid_mask = pd.Series(True, index=frame.index)
        for term in self.active_terms:
            if term == "intercept":
                continue
            valid_mask &= pd.to_numeric(frame[_TERM_COLUMN_MAP[term]], errors="coerce").notna()
        values = pd.Series(0.0, index=frame.index, dtype=float)
        if not valid_mask.any():
            return values
        matrix = _design_matrix(frame.loc[valid_mask], self.active_terms)
        values.loc[valid_mask] = matrix @ np.asarray(self.coefficients, dtype=float)
        return values


_TERM_COLUMN_MAP: dict[str, str] = {
    "delta_sub": "delta_h2o_ratio_vs_subzero_anchor",
    "delta_zero": "delta_h2o_ratio_vs_zeroC_anchor",
    "delta_legacy_summary": "delta_h2o_ratio_vs_legacy_summary_anchor",
    "delta_legacy_zero_ppm": "delta_h2o_ratio_vs_legacy_zero_ppm_anchor",
    "temp_use_c": "temp_use_mean_c",
}

_MODE_TERM_PRIORITY: dict[str, tuple[str, ...]] = {
    "simplified_subzero_anchor": ("delta_sub", "delta_zero", "temp_use_c"),
    "legacy_h2o_summary_selection": ("delta_sub", "delta_zero", "delta_legacy_summary", "temp_use_c"),
    "legacy_h2o_summary_selection_plus_zero_ppm_rows": (
        "delta_sub",
        "delta_zero",
        "delta_legacy_summary",
        "delta_legacy_zero_ppm",
        "temp_use_c",
    ),
}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _safe_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _safe_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""


def _safe_float(value: Any) -> float | None:
    try:
        numeric = float(value)
    except Exception:
        return None
    return numeric if math.isfinite(numeric) else None


def load_legacy_water_replay_rules(repo_root: Path | None = None) -> LegacyWaterReplayRules:
    """Load legacy water-selection defaults from repo config, with safe fallbacks."""

    root = repo_root or _repo_root()
    config_path = root / "configs" / "default_config.json"
    corrected_water_report_path = root / "src" / "gas_calibrator" / "export" / "corrected_water_points_report.py"
    payload = _safe_json(config_path)
    coeff_cfg = payload.get("coefficients", {}) if isinstance(payload, dict) else {}
    workflow_cfg = payload.get("workflow", {}) if isinstance(payload, dict) else {}
    h2o_cfg = coeff_cfg.get("h2o_summary_selection", {}) if isinstance(coeff_cfg, dict) else {}

    report_text = _safe_text(corrected_water_report_path)
    has_select_logic = "select_corrected_fit_rows" in report_text and "include_co2_zero_ppm_rows" in report_text

    return LegacyWaterReplayRules(
        repo_root=root,
        config_path=config_path,
        corrected_water_report_path=corrected_water_report_path,
        include_h2o_phase=bool(h2o_cfg.get("include_h2o_phase", True)),
        include_co2_temp_groups_c=tuple(float(item) for item in h2o_cfg.get("include_co2_temp_groups_c", (-20.0, -10.0, 0.0))),
        include_co2_zero_ppm_rows=bool(h2o_cfg.get("include_co2_zero_ppm_rows", has_select_logic)),
        include_co2_zero_ppm_temp_groups_c=tuple(
            float(item) for item in h2o_cfg.get("include_co2_zero_ppm_temp_groups_c", (10.0,))
        ),
        co2_zero_ppm_target=float(h2o_cfg.get("co2_zero_ppm_target", 0.0)),
        co2_zero_ppm_tolerance=float(h2o_cfg.get("co2_zero_ppm_tolerance", 0.5)),
        temp_tolerance_c=float(h2o_cfg.get("temp_tolerance_c", 0.6)),
        water_first_all_temps=bool(workflow_cfg.get("water_first_all_temps", True)),
        water_first_temp_gte=float(workflow_cfg.get("water_first_temp_gte", 10.0)),
    )


def _metrics(errors: pd.Series | np.ndarray) -> dict[str, float]:
    clean = pd.to_numeric(pd.Series(errors), errors="coerce").dropna()
    if clean.empty:
        return {
            "rmse": math.nan,
            "mae": math.nan,
            "max_abs_error": math.nan,
            "bias": math.nan,
            "std": math.nan,
        }
    values = clean.to_numpy(dtype=float)
    return {
        "rmse": float(np.sqrt(np.mean(np.square(values)))),
        "mae": float(np.mean(np.abs(values))),
        "max_abs_error": float(np.max(np.abs(values))),
        "bias": float(np.mean(values)),
        "std": float(np.std(values)),
    }


def _temp_bias_spread(frame: pd.DataFrame, column: str) -> float:
    grouped = (
        frame.dropna(subset=["temp_c", column])
        .groupby("temp_c", dropna=False)[column]
        .mean()
    )
    if grouped.empty:
        return math.nan
    return float(np.std(grouped.to_numpy(dtype=float)))


def _range_mask(series: pd.Series, lower: float | None, upper: float | None) -> pd.Series:
    if lower == 0.0 and upper == 0.0:
        return pd.to_numeric(series, errors="coerce") == 0.0
    mask = pd.Series(True, index=series.index)
    numeric = pd.to_numeric(series, errors="coerce")
    if lower is not None:
        mask &= numeric > lower
    if upper is not None:
        mask &= numeric <= upper
    return mask


def _route_key(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"h2o", "water", "water_route"}:
        return "h2o"
    if text in {"co2", "gas", "co2_route"}:
        return "co2"
    return text


def _nearest_anchor(temp_value: float | None, by_temp: pd.Series, fallback: float, tolerance_c: float) -> float:
    if temp_value is None or not math.isfinite(temp_value) or by_temp.empty:
        return fallback
    temp_map = pd.to_numeric(pd.Index(by_temp.index), errors="coerce")
    if temp_map.notna().any():
        diffs = np.abs(temp_map.to_numpy(dtype=float) - float(temp_value))
        nearest_idx = int(np.argmin(diffs))
        if diffs[nearest_idx] <= float(tolerance_c):
            return float(by_temp.iloc[nearest_idx])
    return fallback


def _matches_any_temp(series: pd.Series, targets: tuple[float, ...], tolerance_c: float) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    mask = pd.Series(False, index=series.index)
    for target in targets:
        mask |= (values - float(target)).abs().le(float(tolerance_c))
    return mask


def _selected_sample_points(absorbance_samples: pd.DataFrame, fixed_selection: pd.DataFrame, config: Any) -> pd.DataFrame:
    selected = fixed_selection.copy()
    if selected.empty:
        return pd.DataFrame(
            columns=[
                "analyzer",
                "point_title",
                "point_row",
                "A_raw",
                "pressure_source",
                "temperature_source",
                "ratio_source_selected",
            ]
        )
    selected = selected.rename(columns={"analyzer_id": "analyzer"})
    work = absorbance_samples.merge(
        selected[["analyzer", "selected_ratio_source"]],
        left_on=["analyzer", "ratio_source"],
        right_on=["analyzer", "selected_ratio_source"],
        how="inner",
    )
    work = work[
        (work["temp_source"] == config.default_temp_source)
        & (work["pressure_source"] == config.default_pressure_source)
        & (work["r0_model"] == config.default_r0_model)
    ].copy()
    return (
        work.groupby(["analyzer", "point_title", "point_row"], dropna=False)
        .agg(
            A_raw=("A_raw", "mean"),
            pressure_source=("pressure_source", "first"),
            temperature_source=("temp_source", "first"),
            ratio_source_selected=("ratio_source", "first"),
        )
        .reset_index()
    )


def _fixed_zero_variant_points(point_variants: pd.DataFrame, fixed_selection: pd.DataFrame) -> pd.DataFrame:
    if point_variants.empty or fixed_selection.empty:
        return pd.DataFrame()
    fixed = fixed_selection[
        [
            "analyzer_id",
            "selected_ratio_source",
            "selected_source_pair",
            "best_absorbance_model",
            "best_model_family",
            "zero_residual_mode",
            "selected_prediction_scope",
        ]
    ].copy()
    fixed = fixed.rename(
        columns={
            "analyzer_id": "analyzer",
            "selected_ratio_source": "fixed_ratio_source",
            "selected_source_pair": "fixed_selected_source_pair",
            "best_absorbance_model": "fixed_best_model",
            "best_model_family": "fixed_model_family",
            "zero_residual_mode": "fixed_zero_residual_mode",
            "selected_prediction_scope": "fixed_prediction_scope",
        }
    )
    merged = point_variants.merge(fixed, on="analyzer", how="inner")
    mask = (
        (merged["ratio_source"] == merged["fixed_ratio_source"])
        & (merged["zero_residual_mode"] == merged["fixed_zero_residual_mode"])
    )
    merged = merged[mask].copy()
    merged["analyzer_id"] = merged["analyzer"]
    return merged.sort_values(["analyzer", "point_row", "point_title"], ignore_index=True)


def _point_water_summary(samples: pd.DataFrame) -> pd.DataFrame:
    if samples.empty:
        return pd.DataFrame()
    grouped = (
        samples.groupby(
            ["analyzer", "point_title", "point_row", "temp_set_c", "target_co2_ppm"],
            dropna=False,
        )
        .agg(
            route=("route", "first"),
            stage=("stage", "first"),
            point_tag=("point_tag", "first"),
            h2o_ratio_raw_mean=("ratio_h2o_raw", "mean"),
            h2o_ratio_filt_mean=("ratio_h2o_filt", "mean"),
            h2o_signal_mean=("h2o_signal", "mean"),
            h2o_density_mean=("h2o_density", "mean"),
            h2o_sample_count=("sample_index", "count"),
        )
        .reset_index()
    )
    grouped["route"] = grouped["route"].map(_route_key)
    return grouped


def build_legacy_water_replay_features(
    fixed_point_variants: pd.DataFrame,
    water_lineage_samples: pd.DataFrame,
    rules: LegacyWaterReplayRules,
) -> pd.DataFrame:
    """Build point-level replay features for four legacy water-lineage modes."""

    if fixed_point_variants.empty:
        return pd.DataFrame()

    point_water = _point_water_summary(water_lineage_samples)
    merged = fixed_point_variants.merge(
        point_water,
        on=["analyzer", "point_title", "point_row", "temp_set_c", "target_co2_ppm"],
        how="left",
    ).copy()
    merged["A_zero_ready"] = pd.to_numeric(merged["A_mean"], errors="coerce")
    merged["temp_use_mean_c"] = pd.to_numeric(merged["temp_use_mean_c"], errors="coerce")

    rows: list[pd.DataFrame] = []
    group_columns = ["analyzer", "ratio_source", "fixed_zero_residual_mode"]
    for (_, ratio_source, _), subset in merged.groupby(group_columns, dropna=False):
        anchor_base = point_water[point_water["analyzer"] == subset["analyzer"].iloc[0]].copy()
        if anchor_base.empty:
            anchor_base = pd.DataFrame(columns=point_water.columns.tolist())
        ratio_column = "h2o_ratio_raw_mean" if ratio_source == "ratio_co2_raw" else "h2o_ratio_filt_mean"
        subset = subset.copy()
        anchor_base = anchor_base.copy()
        subset["water_ratio_mean"] = pd.to_numeric(subset[ratio_column], errors="coerce")
        anchor_base["water_ratio_mean"] = pd.to_numeric(anchor_base[ratio_column], errors="coerce")

        co2_zero_mask = (
            (anchor_base["route"] == "co2")
            & pd.to_numeric(anchor_base["target_co2_ppm"], errors="coerce")
            .sub(float(rules.co2_zero_ppm_target))
            .abs()
            .le(float(rules.co2_zero_ppm_tolerance))
        )
        subzero_mask = co2_zero_mask & pd.to_numeric(anchor_base["temp_set_c"], errors="coerce").lt(0.0)
        zero_c_mask = co2_zero_mask & pd.to_numeric(anchor_base["temp_set_c"], errors="coerce").eq(0.0)
        legacy_co2_temp_mask = (
            (anchor_base["route"] == "co2")
            & _matches_any_temp(anchor_base["temp_set_c"], rules.include_co2_temp_groups_c, rules.temp_tolerance_c)
        )
        legacy_zero_ppm_mask = (
            (anchor_base["route"] == "co2")
            & pd.to_numeric(anchor_base["target_co2_ppm"], errors="coerce")
            .sub(float(rules.co2_zero_ppm_target))
            .abs()
            .le(float(rules.co2_zero_ppm_tolerance))
            & _matches_any_temp(anchor_base["temp_set_c"], rules.include_co2_zero_ppm_temp_groups_c, rules.temp_tolerance_c)
        )
        legacy_h2o_mask = (anchor_base["route"] == "h2o") if rules.include_h2o_phase else pd.Series(False, index=anchor_base.index)

        subzero_anchor = pd.to_numeric(anchor_base.loc[subzero_mask, "water_ratio_mean"], errors="coerce").dropna()
        zero_c_anchor_rows = anchor_base.loc[zero_c_mask].sort_values(["point_row", "point_title"], ignore_index=True)
        zero_c_anchor = pd.to_numeric(zero_c_anchor_rows["water_ratio_mean"], errors="coerce").dropna()
        legacy_summary_rows = anchor_base[(legacy_h2o_mask | legacy_co2_temp_mask)].copy()
        legacy_summary_plus_zero_rows = anchor_base[(legacy_h2o_mask | legacy_co2_temp_mask | legacy_zero_ppm_mask)].copy()
        zero_ppm_rows = anchor_base[legacy_zero_ppm_mask].copy()

        subzero_anchor_value = float(subzero_anchor.mean()) if not subzero_anchor.empty else math.nan
        zero_c_anchor_value = float(zero_c_anchor.iloc[0]) if not zero_c_anchor.empty else math.nan
        legacy_summary_global = float(pd.to_numeric(legacy_summary_rows["water_ratio_mean"], errors="coerce").dropna().mean()) if not legacy_summary_rows.empty else math.nan
        legacy_summary_plus_zero_global = float(pd.to_numeric(legacy_summary_plus_zero_rows["water_ratio_mean"], errors="coerce").dropna().mean()) if not legacy_summary_plus_zero_rows.empty else math.nan
        zero_ppm_anchor_value = float(pd.to_numeric(zero_ppm_rows["water_ratio_mean"], errors="coerce").dropna().mean()) if not zero_ppm_rows.empty else math.nan
        legacy_summary_by_temp = (
            legacy_summary_rows.dropna(subset=["temp_set_c", "water_ratio_mean"])
            .groupby("temp_set_c", dropna=False)["water_ratio_mean"]
            .mean()
        )
        legacy_summary_plus_zero_by_temp = (
            legacy_summary_plus_zero_rows.dropna(subset=["temp_set_c", "water_ratio_mean"])
            .groupby("temp_set_c", dropna=False)["water_ratio_mean"]
            .mean()
        )

        subset["subzero_zero_water_ratio_anchor"] = subzero_anchor_value
        subset["zeroC_first_zero_water_ratio_anchor"] = zero_c_anchor_value
        subset["legacy_summary_water_ratio_anchor"] = subset["temp_set_c"].map(
            lambda temp: _nearest_anchor(_safe_float(temp), legacy_summary_by_temp, legacy_summary_global, rules.temp_tolerance_c)
        )
        subset["legacy_summary_plus_zero_water_ratio_anchor"] = subset["temp_set_c"].map(
            lambda temp: _nearest_anchor(
                _safe_float(temp),
                legacy_summary_plus_zero_by_temp,
                legacy_summary_plus_zero_global,
                rules.temp_tolerance_c,
            )
        )
        subset["legacy_zero_ppm_water_ratio_anchor"] = zero_ppm_anchor_value
        subset["delta_h2o_ratio_vs_subzero_anchor"] = subset["water_ratio_mean"] - subset["subzero_zero_water_ratio_anchor"]
        subset["delta_h2o_ratio_vs_zeroC_anchor"] = subset["water_ratio_mean"] - subset["zeroC_first_zero_water_ratio_anchor"]
        subset["delta_h2o_ratio_vs_legacy_summary_anchor"] = subset["water_ratio_mean"] - subset["legacy_summary_water_ratio_anchor"]
        subset["delta_h2o_ratio_vs_legacy_summary_plus_zero_anchor"] = (
            subset["water_ratio_mean"] - subset["legacy_summary_plus_zero_water_ratio_anchor"]
        )
        subset["delta_h2o_ratio_vs_legacy_zero_ppm_anchor"] = subset["water_ratio_mean"] - subset["legacy_zero_ppm_water_ratio_anchor"]
        subset["subzero_anchor_row_count"] = int(len(subzero_anchor))
        subset["zero_c_anchor_row_count"] = int(len(zero_c_anchor))
        subset["legacy_summary_anchor_row_count"] = int(
            pd.to_numeric(legacy_summary_rows["water_ratio_mean"], errors="coerce").dropna().shape[0]
        )
        subset["zero_ppm_anchor_row_count"] = int(
            pd.to_numeric(zero_ppm_rows["water_ratio_mean"], errors="coerce").dropna().shape[0]
        )
        subset["legacy_rules_source_config"] = str(rules.config_path)
        subset["legacy_rules_source_report"] = str(rules.corrected_water_report_path)
        subset["legacy_water_first_all_temps"] = bool(rules.water_first_all_temps)
        subset["legacy_water_first_temp_gte"] = float(rules.water_first_temp_gte)
        subset["legacy_rule_temp_groups"] = "|".join(f"{item:g}" for item in rules.include_co2_temp_groups_c)
        subset["legacy_rule_zero_ppm_temp_groups"] = "|".join(f"{item:g}" for item in rules.include_co2_zero_ppm_temp_groups_c)

        for mode in WATER_LINEAGE_MODES:
            work = subset.copy()
            work["water_lineage_mode"] = mode
            work["uses_co2_temp_groups"] = mode in {
                "legacy_h2o_summary_selection",
                "legacy_h2o_summary_selection_plus_zero_ppm_rows",
            }
            work["uses_co2_zero_ppm_rows"] = mode == "legacy_h2o_summary_selection_plus_zero_ppm_rows"
            work["uses_subzero_zero_water_anchor"] = mode != "none"
            work["legacy_summary_anchor_mode"] = (
                "none"
                if mode in {"none", "simplified_subzero_anchor"}
                else "legacy_summary_plus_zero" if mode == "legacy_h2o_summary_selection_plus_zero_ppm_rows" else "legacy_summary"
            )
            if mode == "legacy_h2o_summary_selection_plus_zero_ppm_rows":
                work["delta_h2o_ratio_vs_legacy_summary_anchor"] = work["delta_h2o_ratio_vs_legacy_summary_plus_zero_anchor"]
            rows.append(work)

    combined = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    if combined.empty:
        return combined
    combined["A_zero_obs"] = np.where(
        pd.to_numeric(combined["target_co2_ppm"], errors="coerce") == 0,
        pd.to_numeric(combined["A_zero_ready"], errors="coerce"),
        np.nan,
    )
    return combined.sort_values(
        ["analyzer", "water_lineage_mode", "point_row", "point_title"],
        ignore_index=True,
    )


def _design_matrix(frame: pd.DataFrame, terms: tuple[str, ...]) -> np.ndarray:
    columns: list[np.ndarray] = []
    for term in terms:
        if term == "intercept":
            columns.append(np.ones(len(frame), dtype=float))
        else:
            column = _TERM_COLUMN_MAP[term]
            columns.append(pd.to_numeric(frame[column], errors="coerce").to_numpy(dtype=float))
    return np.column_stack(columns)


def _fit_one_replay_mode(zero_subset: pd.DataFrame, mode: str) -> ReplayCorrectionFit:
    if mode == "none":
        return ReplayCorrectionFit(
            water_lineage_mode=mode,
            active_terms=tuple(),
            coefficients=tuple(),
            sample_count=int(len(zero_subset)),
            rmse_zero_absorbance=_metrics(zero_subset["A_zero_obs"])["rmse"],
            fit_status="identity",
        )
    requested = list(_MODE_TERM_PRIORITY.get(mode, tuple()))
    available_terms = [
        term
        for term in requested
        if pd.to_numeric(zero_subset[_TERM_COLUMN_MAP[term]], errors="coerce").notna().any()
    ]
    if not [term for term in available_terms if term != "temp_use_c"]:
        return ReplayCorrectionFit(mode, tuple(), tuple(), int(len(zero_subset)), math.nan, "missing_water_terms")

    active = ["intercept", *available_terms]
    while len(active) > 1:
        required = [_TERM_COLUMN_MAP[term] for term in active if term != "intercept"]
        working = zero_subset.dropna(subset=required + ["A_zero_obs"]).copy()
        if len(working) >= len(active):
            matrix = _design_matrix(working, tuple(active))
            coeffs, _, _, _ = np.linalg.lstsq(matrix, pd.to_numeric(working["A_zero_obs"], errors="coerce").to_numpy(dtype=float), rcond=None)
            predicted = matrix @ coeffs
            rmse = _metrics(predicted - pd.to_numeric(working["A_zero_obs"], errors="coerce").to_numpy(dtype=float))["rmse"]
            return ReplayCorrectionFit(
                water_lineage_mode=mode,
                active_terms=tuple(active),
                coefficients=tuple(float(item) for item in coeffs),
                sample_count=int(len(working)),
                rmse_zero_absorbance=rmse,
                fit_status="ok",
            )
        active.pop()
    return ReplayCorrectionFit(mode, tuple(), tuple(), int(len(zero_subset)), math.nan, "insufficient_zero_points")


def apply_legacy_water_replay(feature_frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fit and apply one deterministic replay surface per analyzer/mode."""

    if feature_frame.empty:
        return pd.DataFrame(), pd.DataFrame()

    rows: list[pd.DataFrame] = []
    fit_rows: list[dict[str, Any]] = []
    group_columns = ["analyzer", "ratio_source", "fixed_zero_residual_mode", "water_lineage_mode"]
    for (analyzer_id, ratio_source, zero_mode, mode), subset in feature_frame.groupby(group_columns, dropna=False):
        zero_subset = subset[pd.to_numeric(subset["target_co2_ppm"], errors="coerce") == 0].copy()
        fit = _fit_one_replay_mode(zero_subset, str(mode))
        fit_rows.append(
            {
                "analyzer_id": analyzer_id,
                "ratio_source": ratio_source,
                "fixed_zero_residual_mode": zero_mode,
                "water_lineage_mode": mode,
                "active_terms": "|".join(fit.active_terms),
                "coefficient_count": len(fit.coefficients),
                "coefficients_desc": json.dumps(list(fit.coefficients)),
                "sample_count": fit.sample_count,
                "rmse_zero_absorbance": fit.rmse_zero_absorbance,
                "fit_status": fit.fit_status,
            }
        )
        work = subset.copy()
        work["water_lineage_fit_status"] = fit.fit_status
        work["water_lineage_active_terms"] = "|".join(fit.active_terms)
        work["water_lineage_fit_sample_count"] = fit.sample_count
        work["water_lineage_zero_absorbance_rmse"] = fit.rmse_zero_absorbance
        work["A_before_water_lineage_replay"] = pd.to_numeric(work["A_zero_ready"], errors="coerce")
        if mode == "none" or not fit.active_terms:
            delta = pd.Series(0.0, index=work.index, dtype=float)
        else:
            delta = fit.evaluate(work)
        work["water_lineage_replay_delta"] = delta
        work["A_mean"] = pd.to_numeric(work["A_zero_ready"], errors="coerce") - delta
        if "A_from_mean" in work.columns:
            work["A_from_mean_before_water_lineage_replay"] = work["A_from_mean"]
            work["A_from_mean"] = pd.to_numeric(work["A_from_mean"], errors="coerce") - delta
        if "A_alt_mean" in work.columns:
            work["A_alt_mean_before_water_lineage_replay"] = work["A_alt_mean"]
            work["A_alt_mean"] = pd.to_numeric(work["A_alt_mean"], errors="coerce") - delta
        rows.append(work)

    variants = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    fits = pd.DataFrame(fit_rows).sort_values(
        ["analyzer_id", "water_lineage_mode"],
        ignore_index=True,
    )
    return variants, fits


def fit_fixed_absorbance_replay_models(
    replay_points: pd.DataFrame,
    fixed_selection: pd.DataFrame,
    config: Any,
    *,
    absorbance_column: str = "A_mean",
) -> dict[str, pd.DataFrame]:
    """Refit only the already-selected deployable ppm model for each analyzer/mode."""

    if replay_points.empty or fixed_selection.empty:
        return {
            "scores": pd.DataFrame(),
            "selection": pd.DataFrame(),
            "coefficients": pd.DataFrame(),
            "residuals": pd.DataFrame(),
            "best_predictions": pd.DataFrame(),
        }

    branch_points = replay_points.copy()
    branch_points["target_ppm"] = pd.to_numeric(branch_points["target_co2_ppm"], errors="coerce")
    branch_points["temp_c"] = pd.to_numeric(branch_points["temp_set_c"], errors="coerce")
    branch_points["temp_model_c"] = pd.to_numeric(branch_points["temp_use_mean_c"], errors="coerce")
    branch_points["group_key"] = (
        branch_points["temp_c"].map(lambda value: "nan" if pd.isna(value) else f"{float(value):g}")
        + "|"
        + branch_points["target_ppm"].map(lambda value: "nan" if pd.isna(value) else f"{float(value):g}")
        + "|"
        + branch_points["point_title"].fillna("").astype(str)
    )

    fixed = fixed_selection.set_index("analyzer_id")
    spec_map = {spec.model_id: spec for spec in active_model_specs(config)}
    weight_map = config.composite_weight_map()
    legacy_weight_map = config.legacy_composite_weight_map()

    score_rows: list[dict[str, Any]] = []
    selection_rows: list[dict[str, Any]] = []
    coefficient_rows: list[dict[str, Any]] = []
    residual_rows: list[dict[str, Any]] = []

    for (analyzer_id, mode), analyzer_df in branch_points.groupby(["analyzer", "water_lineage_mode"], dropna=False):
        analyzer_key = str(analyzer_id)
        if analyzer_key not in fixed.index:
            continue
        fixed_row = fixed.loc[analyzer_key]
        if isinstance(fixed_row, pd.DataFrame):
            fixed_row = fixed_row.iloc[0]
        model_id = str(fixed_row.get("best_absorbance_model") or "")
        spec = spec_map.get(model_id)
        if spec is None:
            continue
        try:
            score_row, coeffs, residuals = _fit_one_candidate(
                analyzer_df=analyzer_df,
                spec=spec,
                strategy=config.model_selection_strategy,
                score_weights=weight_map,
                legacy_score_weights=legacy_weight_map,
                enable_composite_score=config.enable_composite_score,
                absorbance_column=absorbance_column,
            )
        except Exception:
            continue
        score_row["water_lineage_mode"] = mode
        score_row["fixed_best_model"] = model_id
        score_row["fixed_model_family"] = str(fixed_row.get("best_model_family") or spec.model_family)
        score_row["fixed_prediction_scope"] = str(fixed_row.get("selected_prediction_scope") or score_row.get("score_source") or "overall_fit")
        score_row["fixed_zero_residual_mode"] = str(fixed_row.get("zero_residual_mode") or "none")
        score_row["fixed_selected_source_pair"] = str(fixed_row.get("selected_source_pair") or "")
        score_row["fixed_selected_ratio_source"] = str(fixed_row.get("selected_ratio_source") or "")
        score_rows.append(score_row)

        for row in coeffs:
            row["water_lineage_mode"] = mode
            row["fixed_best_model"] = model_id
            row["fixed_model_family"] = score_row["fixed_model_family"]
            coefficient_rows.append(row)
        for row in residuals:
            row["water_lineage_mode"] = mode
            row["fixed_best_model"] = model_id
            row["fixed_model_family"] = score_row["fixed_model_family"]
            row["fixed_prediction_scope"] = score_row["fixed_prediction_scope"]
            residual_rows.append(row)

        selection_rows.append(
            {
                "analyzer_id": analyzer_key,
                "water_lineage_mode": mode,
                "best_absorbance_model": model_id,
                "best_absorbance_model_label": str(fixed_row.get("best_absorbance_model_label") or spec.model_label),
                "best_model_family": score_row["fixed_model_family"],
                "fixed_best_model": model_id,
                "fixed_model_family": score_row["fixed_model_family"],
                "fixed_zero_residual_mode": score_row["fixed_zero_residual_mode"],
                "fixed_prediction_scope": score_row["fixed_prediction_scope"],
                "selected_source_pair": score_row["fixed_selected_source_pair"],
                "selected_ratio_source": score_row["fixed_selected_ratio_source"],
                "selected_prediction_scope": score_row["fixed_prediction_scope"],
                "selection_reason": (
                    f"Fixed replay keeps source pair {score_row['fixed_selected_source_pair']}, "
                    f"zero residual mode {score_row['fixed_zero_residual_mode']}, "
                    f"ppm model {model_id}, and prediction scope {score_row['fixed_prediction_scope']}."
                ),
                "composite_score": score_row.get("composite_score", math.nan),
                "score_source": score_row.get("score_source", ""),
                "overall_rmse": score_row.get("overall_rmse", math.nan),
                "validation_rmse": score_row.get("validation_rmse", math.nan),
                "zero_rmse": score_row.get("zero_rmse", math.nan),
                "temp_bias_spread": score_row.get("temp_bias_spread", math.nan),
            }
        )

    scores = pd.DataFrame(score_rows).sort_values(["analyzer_id", "water_lineage_mode"], ignore_index=True) if score_rows else pd.DataFrame()
    selection = pd.DataFrame(selection_rows).sort_values(["analyzer_id", "water_lineage_mode"], ignore_index=True) if selection_rows else pd.DataFrame()
    coefficients = pd.DataFrame(coefficient_rows).sort_values(["analyzer_id", "water_lineage_mode", "term_order"], ignore_index=True) if coefficient_rows else pd.DataFrame()
    residuals = pd.DataFrame(residual_rows).sort_values(["analyzer_id", "water_lineage_mode", "prediction_scope", "point_row"], ignore_index=True) if residual_rows else pd.DataFrame()

    best_predictions = pd.DataFrame()
    if not residuals.empty and not selection.empty:
        overall_rows = residuals[residuals["prediction_scope"] == "overall_fit"].copy()
        overall_rows = overall_rows.rename(
            columns={
                "predicted_ppm": "best_overall_fit_pred_ppm",
                "error_ppm": "best_overall_fit_error_ppm",
            }
        )
        validation_rows = residuals[residuals["prediction_scope"] == "validation_oof"].copy()
        if not validation_rows.empty:
            validation_rows = validation_rows.rename(
                columns={
                    "predicted_ppm": "best_validation_pred_ppm",
                    "error_ppm": "best_validation_error_ppm",
                }
            )[
                [
                    "analyzer_id",
                    "water_lineage_mode",
                    "point_title",
                    "point_row",
                    "best_validation_pred_ppm",
                    "best_validation_error_ppm",
                ]
            ].copy()
            best_predictions = overall_rows.merge(
                validation_rows,
                on=["analyzer_id", "water_lineage_mode", "point_title", "point_row"],
                how="left",
            )
        else:
            best_predictions = overall_rows.copy()
            best_predictions["best_validation_pred_ppm"] = np.nan
            best_predictions["best_validation_error_ppm"] = np.nan

        selection_columns = [
            "analyzer_id",
            "water_lineage_mode",
            "best_absorbance_model",
            "best_absorbance_model_label",
            "best_model_family",
            "fixed_best_model",
            "fixed_model_family",
            "fixed_zero_residual_mode",
            "fixed_prediction_scope",
            "selected_source_pair",
            "selected_ratio_source",
            "selection_reason",
        ]
        selection_lookup = selection[selection_columns].copy()
        duplicate_columns = [
            column
            for column in selection_lookup.columns
            if column not in {"analyzer_id", "water_lineage_mode"} and column in best_predictions.columns
        ]
        if duplicate_columns:
            selection_lookup = selection_lookup.drop(columns=duplicate_columns)

        best_predictions = best_predictions.merge(
            selection_lookup,
            on=["analyzer_id", "water_lineage_mode"],
            how="left",
        )
        best_predictions["selected_pred_ppm"] = np.where(
            best_predictions["fixed_prediction_scope"] == "validation_oof",
            best_predictions["best_validation_pred_ppm"],
            best_predictions["best_overall_fit_pred_ppm"],
        )
        best_predictions["selected_error_ppm"] = np.where(
            best_predictions["fixed_prediction_scope"] == "validation_oof",
            best_predictions["best_validation_error_ppm"],
            best_predictions["best_overall_fit_error_ppm"],
        )

        replay_point_columns = [
            "analyzer",
            "water_lineage_mode",
            "point_title",
            "point_row",
            "A_zero_ready",
            "A_before_water_lineage_replay",
            "water_lineage_replay_delta",
            "water_lineage_fit_status",
            "water_lineage_active_terms",
            "water_lineage_fit_sample_count",
            "water_lineage_zero_absorbance_rmse",
            "uses_co2_temp_groups",
            "uses_co2_zero_ppm_rows",
            "uses_subzero_zero_water_anchor",
            "subzero_anchor_row_count",
            "zero_ppm_anchor_row_count",
            "legacy_summary_anchor_row_count",
            "water_ratio_mean",
            "delta_h2o_ratio_vs_subzero_anchor",
            "delta_h2o_ratio_vs_zeroC_anchor",
            "delta_h2o_ratio_vs_legacy_summary_anchor",
            "delta_h2o_ratio_vs_legacy_zero_ppm_anchor",
        ]
        replay_point_columns = [column for column in replay_point_columns if column in replay_points.columns]
        replay_lookup = replay_points[replay_point_columns].rename(columns={"analyzer": "analyzer_id"}).drop_duplicates(
            ["analyzer_id", "water_lineage_mode", "point_title", "point_row"]
        )
        best_predictions = best_predictions.merge(
            replay_lookup,
            on=["analyzer_id", "water_lineage_mode", "point_title", "point_row"],
            how="left",
        )
    return {
        "scores": scores,
        "selection": selection,
        "coefficients": coefficients,
        "residuals": residuals,
        "best_predictions": best_predictions,
    }


def _build_compare_frame(
    point_raw: pd.DataFrame,
    selected_sample_points: pd.DataFrame,
    old_ratio_residuals: pd.DataFrame,
    best_predictions: pd.DataFrame,
    config: Any,
) -> pd.DataFrame:
    compare = point_raw.copy()
    compare = compare.merge(
        selected_sample_points,
        on=["analyzer", "point_title", "point_row"],
        how="left",
    )
    compare = compare.merge(
        old_ratio_residuals[["analyzer", "point_row", "point_title", "old_prediction_ppm", "old_residual_ppm", "old_ratio_value"]],
        on=["analyzer", "point_row", "point_title"],
        how="left",
    )
    selected = best_predictions.rename(columns={"analyzer_id": "analyzer"}).copy()
    compare = compare.merge(
        selected,
        on=["analyzer", "point_title", "point_row"],
        how="left",
    )
    compare["old_pred_ppm"] = compare["old_prediction_ppm"]
    compare["old_error"] = compare["old_pred_ppm"] - compare["target_co2_ppm"]
    compare["new_pred_ppm"] = compare["selected_pred_ppm"]
    compare["new_error"] = compare["selected_error_ppm"].combine_first(compare["new_pred_ppm"] - compare["target_co2_ppm"])
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
    compare["selected_source_pair"] = compare["selected_source_pair"].fillna(
        compare["selected_ratio_source"].map(
            {
                "ratio_co2_raw": "raw/raw",
                "ratio_co2_filt": "filt/filt",
            }
        )
    )
    compare["temp_c"] = pd.to_numeric(compare["temp_set_c"], errors="coerce")
    compare["target_ppm"] = pd.to_numeric(compare["target_co2_ppm"], errors="coerce")
    compare["winner_for_point"] = compare.apply(
        lambda row: (
            "old_chain"
            if abs(row["old_error"]) < abs(row["new_error"])
            else "replay_chain"
            if abs(row["new_error"]) < abs(row["old_error"])
            else "tie"
        )
        if pd.notna(row["old_error"]) and pd.notna(row["new_error"])
        else "replay_chain"
        if pd.isna(row["old_error"]) and pd.notna(row["new_error"])
        else "old_chain"
        if pd.notna(row["old_error"]) and pd.isna(row["new_error"])
        else "tie",
        axis=1,
    )
    return compare.rename(columns={"analyzer": "analyzer_id"})


def _detail_rows(compare: pd.DataFrame, fixed_selection: pd.DataFrame) -> pd.DataFrame:
    fixed = fixed_selection.set_index("analyzer_id") if not fixed_selection.empty else pd.DataFrame()
    rows: list[dict[str, Any]] = []

    for (analyzer_id, mode), subset in compare.groupby(["analyzer_id", "water_lineage_mode"], dropna=False):
        subset = subset.copy()
        zero_subset = subset[pd.to_numeric(subset["target_ppm"], errors="coerce") == 0].copy()
        low_subset = subset[_range_mask(subset["target_ppm"], 0.0, 200.0)].copy()
        main_subset = subset[_range_mask(subset["target_ppm"], 200.0, 1000.0)].copy()
        new_metrics = _metrics(subset["new_error"])
        zero_metrics = _metrics(zero_subset["new_error"])
        old_metrics = _metrics(subset["old_error"])
        old_zero_metrics = _metrics(zero_subset["old_error"])
        detail = {
            "analyzer_id": analyzer_id,
            "water_lineage_mode": mode,
            "mode2_semantic_profile": str(subset.get("mode2_semantic_profile", pd.Series(["mode2_semantics_unknown"])).fillna("mode2_semantics_unknown").iloc[0]),
            "mode2_legacy_raw_compare_safe": bool(subset.get("mode2_legacy_raw_compare_safe", pd.Series([False])).fillna(False).iloc[0]),
            "mode2_is_baseline_bearing_profile": bool(subset.get("mode2_is_baseline_bearing_profile", pd.Series([False])).fillna(False).iloc[0]),
            "overall_rmse": new_metrics["rmse"],
            "zero_rmse": zero_metrics["rmse"],
            "low_range_rmse": _metrics(low_subset["new_error"])["rmse"],
            "main_range_rmse": _metrics(main_subset["new_error"])["rmse"],
            "temp_bias_spread": _temp_bias_spread(subset, "new_error"),
            "old_chain_overall_rmse": old_metrics["rmse"],
            "old_chain_zero_rmse": old_zero_metrics["rmse"],
            "gap_to_old_overall": new_metrics["rmse"] - old_metrics["rmse"] if pd.notna(new_metrics["rmse"]) and pd.notna(old_metrics["rmse"]) else math.nan,
            "gap_to_old_zero": zero_metrics["rmse"] - old_zero_metrics["rmse"] if pd.notna(zero_metrics["rmse"]) and pd.notna(old_zero_metrics["rmse"]) else math.nan,
            "uses_co2_temp_groups": bool(subset["uses_co2_temp_groups"].fillna(False).iloc[0]) if "uses_co2_temp_groups" in subset.columns else False,
            "uses_co2_zero_ppm_rows": bool(subset["uses_co2_zero_ppm_rows"].fillna(False).iloc[0]) if "uses_co2_zero_ppm_rows" in subset.columns else False,
            "uses_subzero_zero_water_anchor": bool(subset["uses_subzero_zero_water_anchor"].fillna(False).iloc[0]) if "uses_subzero_zero_water_anchor" in subset.columns else False,
            "subzero_anchor_row_count": int(pd.to_numeric(subset.get("subzero_anchor_row_count"), errors="coerce").fillna(0).max()) if "subzero_anchor_row_count" in subset.columns else 0,
            "zero_ppm_anchor_row_count": int(pd.to_numeric(subset.get("zero_ppm_anchor_row_count"), errors="coerce").fillna(0).max()) if "zero_ppm_anchor_row_count" in subset.columns else 0,
            "legacy_summary_anchor_row_count": int(pd.to_numeric(subset.get("legacy_summary_anchor_row_count"), errors="coerce").fillna(0).max()) if "legacy_summary_anchor_row_count" in subset.columns else 0,
            "A_mean_zero_rmse": _metrics(zero_subset["A_mean"])["rmse"],
            "A_zero_ready_zero_rmse": _metrics(zero_subset["A_zero_ready"])["rmse"],
            "baseline_like_stability_proxy": _temp_bias_spread(zero_subset.rename(columns={"A_mean": "absorbance_value"}), "absorbance_value"),
            "final_residual_spread": new_metrics["std"],
            "water_lineage_fit_status": str(subset.get("water_lineage_fit_status", pd.Series([""])).iloc[0]),
            "water_lineage_active_terms": str(subset.get("water_lineage_active_terms", pd.Series([""])).iloc[0]),
            "water_lineage_fit_sample_count": int(pd.to_numeric(subset.get("water_lineage_fit_sample_count"), errors="coerce").fillna(0).max()) if "water_lineage_fit_sample_count" in subset.columns else 0,
        }
        if isinstance(fixed, pd.DataFrame) and not fixed.empty and str(analyzer_id) in fixed.index:
            fixed_row = fixed.loc[str(analyzer_id)]
            if isinstance(fixed_row, pd.DataFrame):
                fixed_row = fixed_row.iloc[0]
            detail.update(
                {
                    "selected_source_pair": str(fixed_row.get("selected_source_pair") or ""),
                    "fixed_best_model": str(fixed_row.get("best_absorbance_model") or ""),
                    "fixed_model_family": str(fixed_row.get("best_model_family") or ""),
                    "fixed_zero_residual_mode": str(fixed_row.get("zero_residual_mode") or ""),
                    "fixed_prediction_scope": str(fixed_row.get("selected_prediction_scope") or ""),
                }
            )
        else:
            detail.update(
                {
                    "selected_source_pair": "",
                    "fixed_best_model": "",
                    "fixed_model_family": "",
                    "fixed_zero_residual_mode": "",
                    "fixed_prediction_scope": "",
                }
            )
        rows.append(detail)

    detail_df = pd.DataFrame(rows).sort_values(["analyzer_id", "water_lineage_mode"], ignore_index=True) if rows else pd.DataFrame()
    if detail_df.empty:
        return detail_df

    none_lookup = detail_df[detail_df["water_lineage_mode"] == "none"].set_index("analyzer_id") if (detail_df["water_lineage_mode"] == "none").any() else pd.DataFrame()
    gains = []
    for row in detail_df.itertuples(index=False):
        none_row = none_lookup.loc[row.analyzer_id] if isinstance(none_lookup, pd.DataFrame) and not none_lookup.empty and row.analyzer_id in none_lookup.index else None
        none_overall = float(none_row["overall_rmse"]) if none_row is not None else math.nan
        none_zero = float(none_row["zero_rmse"]) if none_row is not None else math.nan
        none_low = float(none_row["low_range_rmse"]) if none_row is not None else math.nan
        none_main = float(none_row["main_range_rmse"]) if none_row is not None else math.nan
        none_gap = float(none_row["gap_to_old_overall"]) if none_row is not None else math.nan
        current_gap = float(row.gap_to_old_overall) if pd.notna(row.gap_to_old_overall) else math.nan
        gap_closed_ratio_raw = (
            (none_gap - current_gap) / none_gap
            if pd.notna(none_gap) and abs(none_gap) > 1.0e-12 and pd.notna(current_gap)
            else math.nan
        )
        gap_closed_ratio_capped = float(np.clip(gap_closed_ratio_raw, 0.0, 1.0)) if pd.notna(gap_closed_ratio_raw) else math.nan
        is_laggard = bool(pd.notna(none_gap) and none_gap > 0.0)
        gains.append(
            {
                "analyzer_id": row.analyzer_id,
                "water_lineage_mode": row.water_lineage_mode,
                "delta_vs_none_overall": none_overall - float(row.overall_rmse) if pd.notna(none_overall) and pd.notna(row.overall_rmse) else math.nan,
                "delta_vs_none_zero": none_zero - float(row.zero_rmse) if pd.notna(none_zero) and pd.notna(row.zero_rmse) else math.nan,
                "delta_vs_none_low": none_low - float(row.low_range_rmse) if pd.notna(none_low) and pd.notna(row.low_range_rmse) else math.nan,
                "delta_vs_none_main": none_main - float(row.main_range_rmse) if pd.notna(none_main) and pd.notna(row.main_range_rmse) else math.nan,
                "gap_closed_ratio_vs_current_new_chain": gap_closed_ratio_raw,
                "gap_closed_ratio_raw": gap_closed_ratio_raw,
                "gap_closed_ratio_capped_0_100": gap_closed_ratio_capped,
                "baseline_none_gap_to_old_overall": none_gap,
                "crossed_old_chain_flag": bool(pd.notna(current_gap) and current_gap <= 0.0),
                "overclosure_ratio": max(float(gap_closed_ratio_raw) - 1.0, 0.0) if pd.notna(gap_closed_ratio_raw) else math.nan,
                "laggard_only_weighted_gap_closed_ratio_capped": gap_closed_ratio_capped if is_laggard else math.nan,
                "laggard_only_analyzer_count": 1 if is_laggard else 0,
            }
        )
    gain_df = pd.DataFrame(gains)
    return detail_df.merge(gain_df, on=["analyzer_id", "water_lineage_mode"], how="left")


def _stage_rows(compare: pd.DataFrame, detail_df: pd.DataFrame) -> pd.DataFrame:
    if compare.empty or detail_df.empty:
        return pd.DataFrame()
    detail_lookup = detail_df.set_index(["analyzer_id", "water_lineage_mode"])
    rows: list[dict[str, Any]] = []
    for (analyzer_id, mode), _subset in compare.groupby(["analyzer_id", "water_lineage_mode"], dropna=False):
        key = (str(analyzer_id), str(mode))
        detail = detail_lookup.loc[key] if key in detail_lookup.index else pd.Series(dtype=object)
        none_key = (str(analyzer_id), "none")
        none_detail = detail_lookup.loc[none_key] if none_key in detail_lookup.index else pd.Series(dtype=object)
        zero_gain = (
            float(none_detail.get("zero_rmse")) - float(detail.get("zero_rmse"))
            if not none_detail.empty and pd.notna(detail.get("zero_rmse")) and pd.notna(none_detail.get("zero_rmse"))
            else math.nan
        )
        absorbance_residual_gain = (
            float(none_detail.get("final_residual_spread")) - float(detail.get("final_residual_spread"))
            if not none_detail.empty and pd.notna(detail.get("final_residual_spread")) and pd.notna(none_detail.get("final_residual_spread"))
            else math.nan
        )
        residual_spread_before = (
            float(none_detail.get("final_residual_spread"))
            if not none_detail.empty and pd.notna(none_detail.get("final_residual_spread"))
            else math.nan
        )
        metrics_to_emit = [
            ("zero_temp", "zero_rmse", detail.get("zero_rmse"), math.nan),
            ("zero_temp", "temp_bias_spread", detail.get("temp_bias_spread"), math.nan),
            ("zero_temp", "zero_point_stability_gain_vs_none", zero_gain, zero_gain),
            ("absorbance", "A_mean_zero_rmse", detail.get("A_mean_zero_rmse"), math.nan),
            ("absorbance", "A_zero_ready_zero_rmse", detail.get("A_zero_ready_zero_rmse"), math.nan),
            ("absorbance", "baseline_like_stability_proxy", detail.get("baseline_like_stability_proxy"), math.nan),
            ("absorbance", "residual_spread_before_replay", residual_spread_before, math.nan),
            ("absorbance", "residual_spread_after_replay", detail.get("final_residual_spread"), math.nan),
            ("absorbance", "residual_spread_gain_vs_none", absorbance_residual_gain, absorbance_residual_gain),
            ("final_ppm", "overall_rmse", detail.get("overall_rmse"), detail.get("delta_vs_none_overall")),
            ("final_ppm", "low_range_rmse", detail.get("low_range_rmse"), detail.get("delta_vs_none_low")),
            ("final_ppm", "main_range_rmse", detail.get("main_range_rmse"), detail.get("delta_vs_none_main")),
            ("final_ppm", "gap_to_old_overall", detail.get("gap_to_old_overall"), math.nan),
            ("final_ppm", "gap_closed_ratio_vs_current_new_chain", detail.get("gap_closed_ratio_vs_current_new_chain"), detail.get("gap_closed_ratio_vs_current_new_chain")),
            ("final_ppm", "gap_closed_ratio_capped_0_100", detail.get("gap_closed_ratio_capped_0_100"), detail.get("gap_closed_ratio_capped_0_100")),
        ]
        for layer, metric_name, metric_value, gain_vs_none in metrics_to_emit:
            rows.append(
                {
                    "analyzer_id": analyzer_id,
                    "water_lineage_mode": mode,
                    "mode2_semantic_profile": detail.get("mode2_semantic_profile"),
                    "mode2_legacy_raw_compare_safe": detail.get("mode2_legacy_raw_compare_safe"),
                    "mode2_is_baseline_bearing_profile": detail.get("mode2_is_baseline_bearing_profile"),
                    "layer": layer,
                    "metric_name": metric_name,
                    "metric_value": metric_value,
                    "gain_vs_none": gain_vs_none,
                }
            )
    return pd.DataFrame(rows).sort_values(["analyzer_id", "water_lineage_mode", "layer", "metric_name"], ignore_index=True)


def _summary_rows(detail_df: pd.DataFrame) -> pd.DataFrame:
    if detail_df.empty:
        return pd.DataFrame()
    profile_cols = [
        "mode2_semantic_profile",
        "mode2_legacy_raw_compare_safe",
        "mode2_is_baseline_bearing_profile",
    ]
    rows: list[dict[str, Any]] = []
    legacy_modes = [mode for mode in WATER_LINEAGE_MODES if mode != "none"]
    for analyzer_id, subset in detail_df.groupby("analyzer_id", dropna=False):
        legacy_subset = subset[subset["water_lineage_mode"].isin(legacy_modes)].sort_values(
            ["gap_closed_ratio_capped_0_100", "gap_closed_ratio_raw", "delta_vs_none_overall", "delta_vs_none_zero"],
            ascending=[False, False, False, False],
            na_position="last",
            ignore_index=True,
        )
        best = legacy_subset.iloc[0] if not legacy_subset.empty else subset.iloc[0]
        rows.append(
            {
                "summary_scope": "per_analyzer_best_mode",
                "analyzer_id": analyzer_id,
                **{column: best[column] for column in profile_cols},
                "water_lineage_mode": best["water_lineage_mode"],
                "fixed_best_model": best["fixed_best_model"],
                "fixed_model_family": best["fixed_model_family"],
                "fixed_zero_residual_mode": best["fixed_zero_residual_mode"],
                "fixed_prediction_scope": best["fixed_prediction_scope"],
                "overall_rmse": best["overall_rmse"],
                "zero_rmse": best["zero_rmse"],
                "gap_to_old_overall": best["gap_to_old_overall"],
                "gap_closed_ratio_vs_current_new_chain": best["gap_closed_ratio_vs_current_new_chain"],
                "gap_closed_ratio_raw": best["gap_closed_ratio_raw"],
                "gap_closed_ratio_capped_0_100": best["gap_closed_ratio_capped_0_100"],
                "crossed_old_chain_flag": best["crossed_old_chain_flag"],
                "overclosure_ratio": best["overclosure_ratio"],
                "laggard_only_weighted_gap_closed_ratio_capped": best["laggard_only_weighted_gap_closed_ratio_capped"],
                "laggard_only_analyzer_count": best["laggard_only_analyzer_count"],
                "delta_vs_none_overall": best["delta_vs_none_overall"],
                "delta_vs_none_zero": best["delta_vs_none_zero"],
            }
        )

    aggregate_metric_columns = [
        "overall_rmse",
        "zero_rmse",
        "gap_to_old_overall",
        "gap_closed_ratio_vs_current_new_chain",
        "gap_closed_ratio_raw",
        "gap_closed_ratio_capped_0_100",
        "overclosure_ratio",
        "delta_vs_none_overall",
        "delta_vs_none_zero",
    ]
    for group_key, subset in detail_df.groupby(["water_lineage_mode", *profile_cols], dropna=False):
        mode, profile, legacy_safe, baseline_bearing = group_key
        laggards = subset[pd.to_numeric(subset["laggard_only_weighted_gap_closed_ratio_capped"], errors="coerce").notna()].copy()
        if not laggards.empty and "baseline_none_gap_to_old_overall" in laggards.columns:
            weights = pd.to_numeric(laggards["baseline_none_gap_to_old_overall"], errors="coerce")
            ratios = pd.to_numeric(laggards["gap_closed_ratio_capped_0_100"], errors="coerce")
            usable = weights.notna() & ratios.notna() & (weights > 0.0)
            laggard_weighted = float(np.average(ratios[usable].to_numpy(dtype=float), weights=weights[usable].to_numpy(dtype=float))) if usable.any() else math.nan
            laggard_count = int(laggards["analyzer_id"].nunique())
        else:
            laggard_weighted = math.nan
            laggard_count = 0
        rows.append(
            {
                "summary_scope": "aggregate_mode_mean",
                "analyzer_id": "ALL",
                "water_lineage_mode": mode,
                "mode2_semantic_profile": profile,
                "mode2_legacy_raw_compare_safe": legacy_safe,
                "mode2_is_baseline_bearing_profile": baseline_bearing,
                "laggard_only_weighted_gap_closed_ratio_capped": laggard_weighted,
                "laggard_only_analyzer_count": laggard_count,
                "crossed_old_chain_flag": bool(subset["crossed_old_chain_flag"].fillna(False).any()),
                **subset[aggregate_metric_columns].mean(numeric_only=True).to_dict(),
            }
        )

    aggregate_df = pd.DataFrame([row for row in rows if row["summary_scope"] == "aggregate_mode_mean"])
    if not aggregate_df.empty:
        for group_key, subset in aggregate_df.groupby(profile_cols, dropna=False):
            best = subset.sort_values(
                ["laggard_only_weighted_gap_closed_ratio_capped", "gap_closed_ratio_capped_0_100", "delta_vs_none_overall"],
                ascending=[False, False, False],
                na_position="last",
                ignore_index=True,
            ).iloc[0]
            row = best.to_dict()
            row["summary_scope"] = "aggregate_profile_best_mode"
            rows.append(row)
    return pd.DataFrame(rows).sort_values(["summary_scope", "analyzer_id", "water_lineage_mode"], ignore_index=True)


def _layer_focus(detail_df: pd.DataFrame, stage_df: pd.DataFrame) -> str:
    if (
        not detail_df.empty
        and {"mode2_semantic_profile", "mode2_legacy_raw_compare_safe", "mode2_is_baseline_bearing_profile"} <= set(detail_df.columns)
        and {"mode2_semantic_profile", "mode2_legacy_raw_compare_safe", "mode2_is_baseline_bearing_profile"} <= set(stage_df.columns)
    ):
        first = detail_df.iloc[0]
        stage_df = stage_df[
            (stage_df["mode2_semantic_profile"] == first["mode2_semantic_profile"])
            & (stage_df["mode2_legacy_raw_compare_safe"] == first["mode2_legacy_raw_compare_safe"])
            & (stage_df["mode2_is_baseline_bearing_profile"] == first["mode2_is_baseline_bearing_profile"])
        ].copy()
    legacy_stage = stage_df[
        stage_df["water_lineage_mode"].isin({"legacy_h2o_summary_selection", "legacy_h2o_summary_selection_plus_zero_ppm_rows"})
    ].copy()
    if legacy_stage.empty:
        return "insufficient_data"
    zero_score = pd.to_numeric(
        legacy_stage[legacy_stage["metric_name"].isin({"zero_point_stability_gain_vs_none", "temp_bias_spread"})]["gain_vs_none"],
        errors="coerce",
    ).dropna().mean()
    final_score = pd.to_numeric(
        legacy_stage[legacy_stage["metric_name"].isin({"overall_rmse", "gap_closed_ratio_vs_current_new_chain"})]["gain_vs_none"],
        errors="coerce",
    ).dropna().mean()
    if pd.notna(zero_score) and pd.notna(final_score):
        if zero_score > final_score * 1.10:
            return "zero_temp_layer"
        if final_score > zero_score * 1.10:
            return "final_ppm_layer"
    return "mixed_or_balanced"


def _conclusion_rows(detail_df: pd.DataFrame, summary_df: pd.DataFrame, stage_df: pd.DataFrame) -> pd.DataFrame:
    if detail_df.empty:
        return pd.DataFrame()
    legacy_modes = detail_df[detail_df["water_lineage_mode"].isin({"legacy_h2o_summary_selection", "legacy_h2o_summary_selection_plus_zero_ppm_rows"})].copy()
    best_legacy = legacy_modes.sort_values(
        ["gap_closed_ratio_vs_current_new_chain", "delta_vs_none_overall", "delta_vs_none_zero"],
        ascending=[False, False, False],
        na_position="last",
        ignore_index=True,
    ).head(1)
    best_row = best_legacy.iloc[0] if not best_legacy.empty else detail_df.iloc[0]
    best_mean = summary_df[
        (summary_df["summary_scope"] == "aggregate_mode_mean")
        & (summary_df["water_lineage_mode"] == best_row["water_lineage_mode"])
    ]
    mean_gap_closed = float(pd.to_numeric(best_mean.iloc[0]["gap_closed_ratio_vs_current_new_chain"], errors="coerce")) if not best_mean.empty else math.nan
    most_dependent = (
        legacy_modes.sort_values("gap_closed_ratio_vs_current_new_chain", ascending=False, na_position="last").iloc[0]["analyzer_id"]
        if not legacy_modes.empty
        else ""
    )
    layer_focus = _layer_focus(detail_df, stage_df)
    remaining_gap = float(pd.to_numeric(best_row.get("gap_to_old_overall"), errors="coerce")) if pd.notna(best_row.get("gap_to_old_overall")) else math.nan
    significant = pd.notna(mean_gap_closed) and mean_gap_closed >= 0.15
    support_statement = pd.notna(mean_gap_closed) and mean_gap_closed > 0.0 and pd.notna(remaining_gap) and remaining_gap > 0.0
    rows = [
        {
            "question_id": "legacy_gap_closure",
            "question": "legacy water lineage replay 是否显著缩小了与 old_chain 的差距",
            "answer": "yes" if significant else "partial_or_no",
            "evidence": f"best_mode={best_row['water_lineage_mode']}; mean_gap_closed_ratio={mean_gap_closed:.4f}" if pd.notna(mean_gap_closed) else "insufficient_data",
            "recommended_mode": best_row["water_lineage_mode"],
        },
        {
            "question_id": "dominant_layer",
            "question": "改善主要发生在 zero/temp layer 还是 final ppm layer",
            "answer": layer_focus,
            "evidence": "Stage metrics compare zero-point stability gains against final ppm RMSE/gap closure gains.",
            "recommended_mode": best_row["water_lineage_mode"],
        },
        {
            "question_id": "most_dependent_analyzer",
            "question": "GA01 / GA02 / GA03 哪个最依赖 legacy water replay",
            "answer": str(most_dependent),
            "evidence": "Chosen by the largest gap_closed_ratio_vs_current_new_chain among legacy modes.",
            "recommended_mode": best_row["water_lineage_mode"],
        },
        {
            "question_id": "statement_support",
            "question": "是否支持 “missing legacy water-lineage consumption is a real contributor, but not the sole cause”",
            "answer": "supported" if support_statement else "not_supported_or_inconclusive",
            "evidence": (
                f"mean_gap_closed_ratio={mean_gap_closed:.4f}; remaining_gap_to_old_overall={remaining_gap:.4f}"
                if pd.notna(mean_gap_closed) and pd.notna(remaining_gap)
                else "insufficient_data"
            ),
            "recommended_mode": best_row["water_lineage_mode"],
        },
        {
            "question_id": "remaining_main_causes",
            "question": "若仍有明显剩余 gap，剩余主因是什么",
            "answer": (
                "weak_absorbance_ppm_model|40℃ / 0 ppm 缺失，高温 R0(T) 未锚定"
                if pd.notna(remaining_gap) and remaining_gap > 0.0
                else "remaining_gap_not_obvious"
            ),
            "evidence": "Legacy replay is diagnostic-only and does not remove the baseline weak ppm model / missing 40C zero-anchor risks.",
            "recommended_mode": best_row["water_lineage_mode"],
        },
        {
            "question_id": "headline",
            "question": "headline",
            "answer": (
                f"{best_row['water_lineage_mode']} closed {mean_gap_closed:.2%} of the current-new-chain gap on average"
                if pd.notna(mean_gap_closed)
                else "insufficient_data"
            ),
            "evidence": f"best_mode={best_row['water_lineage_mode']}",
            "recommended_mode": best_row["water_lineage_mode"],
        },
    ]
    return pd.DataFrame(rows)


def _conclusion_rows_clean(detail_df: pd.DataFrame, summary_df: pd.DataFrame, stage_df: pd.DataFrame) -> pd.DataFrame:
    if detail_df.empty:
        return pd.DataFrame()
    legacy_modes = detail_df[
        detail_df["water_lineage_mode"].isin(
            {"legacy_h2o_summary_selection", "legacy_h2o_summary_selection_plus_zero_ppm_rows"}
        )
    ].copy()
    best_legacy = legacy_modes.sort_values(
        ["gap_closed_ratio_vs_current_new_chain", "delta_vs_none_overall", "delta_vs_none_zero"],
        ascending=[False, False, False],
        na_position="last",
        ignore_index=True,
    ).head(1)
    best_row = best_legacy.iloc[0] if not best_legacy.empty else detail_df.iloc[0]
    best_mean = summary_df[
        (summary_df["summary_scope"] == "aggregate_mode_mean")
        & (summary_df["water_lineage_mode"] == best_row["water_lineage_mode"])
    ]
    mean_gap_closed = (
        float(pd.to_numeric(best_mean.iloc[0]["gap_closed_ratio_vs_current_new_chain"], errors="coerce"))
        if not best_mean.empty
        else math.nan
    )
    most_dependent = (
        legacy_modes.sort_values("gap_closed_ratio_vs_current_new_chain", ascending=False, na_position="last").iloc[0]["analyzer_id"]
        if not legacy_modes.empty
        else ""
    )
    layer_focus = _layer_focus(detail_df, stage_df)
    remaining_gap = (
        float(pd.to_numeric(best_row.get("gap_to_old_overall"), errors="coerce"))
        if pd.notna(best_row.get("gap_to_old_overall"))
        else math.nan
    )
    significant = pd.notna(mean_gap_closed) and mean_gap_closed >= 0.15
    support_statement = pd.notna(mean_gap_closed) and mean_gap_closed > 0.0 and pd.notna(remaining_gap) and remaining_gap > 0.0
    rows = [
        {
            "question_id": "legacy_gap_closure",
            "question": "Did legacy water-lineage replay significantly shrink the gap to old_chain?",
            "answer": "yes" if significant else "partial_or_no",
            "evidence": (
                f"best_mode={best_row['water_lineage_mode']}; mean_gap_closed_ratio={mean_gap_closed:.4f}"
                if pd.notna(mean_gap_closed)
                else "insufficient_data"
            ),
            "recommended_mode": best_row["water_lineage_mode"],
        },
        {
            "question_id": "dominant_layer",
            "question": "Did the gains land more in the zero/temp layer or the final ppm layer?",
            "answer": layer_focus,
            "evidence": "Stage metrics compare zero-point stability gains against final ppm RMSE/gap closure gains.",
            "recommended_mode": best_row["water_lineage_mode"],
        },
        {
            "question_id": "most_dependent_analyzer",
            "question": "Which of GA01 / GA02 / GA03 depends most on legacy water replay?",
            "answer": str(most_dependent),
            "evidence": "Chosen by the largest gap_closed_ratio_vs_current_new_chain among legacy modes.",
            "recommended_mode": best_row["water_lineage_mode"],
        },
        {
            "question_id": "statement_support",
            "question": "Does the evidence support 'missing legacy water-lineage consumption is a real contributor, but not the sole cause'?",
            "answer": "supported" if support_statement else "not_supported_or_inconclusive",
            "evidence": (
                f"mean_gap_closed_ratio={mean_gap_closed:.4f}; remaining_gap_to_old_overall={remaining_gap:.4f}"
                if pd.notna(mean_gap_closed) and pd.notna(remaining_gap)
                else "insufficient_data"
            ),
            "recommended_mode": best_row["water_lineage_mode"],
        },
        {
            "question_id": "remaining_main_causes",
            "question": "If a material gap remains, what are the main residual causes?",
            "answer": (
                "weak_absorbance_ppm_model|40℃ / 0 ppm 缺失，高温 R0(T) 未锚定"
                if pd.notna(remaining_gap) and remaining_gap > 0.0
                else "remaining_gap_not_obvious"
            ),
            "evidence": "Legacy replay is diagnostic-only and does not remove the baseline weak ppm model / missing 40C zero-anchor risks.",
            "recommended_mode": best_row["water_lineage_mode"],
        },
        {
            "question_id": "headline",
            "question": "headline",
            "answer": (
                f"{best_row['water_lineage_mode']} closed {mean_gap_closed:.2%} of the current-new-chain gap on average"
                if pd.notna(mean_gap_closed)
                else "insufficient_data"
            ),
            "evidence": f"best_mode={best_row['water_lineage_mode']}",
            "recommended_mode": best_row["water_lineage_mode"],
        },
    ]
    return pd.DataFrame(rows)


def _conclusion_rows_profiled(detail_df: pd.DataFrame, summary_df: pd.DataFrame, stage_df: pd.DataFrame) -> pd.DataFrame:
    if detail_df.empty:
        return pd.DataFrame()
    profile_cols = [
        "mode2_semantic_profile",
        "mode2_legacy_raw_compare_safe",
        "mode2_is_baseline_bearing_profile",
    ]
    rows: list[dict[str, Any]] = []
    for group_key, profile_df in detail_df.groupby(profile_cols, dropna=False):
        profile_values = dict(zip(profile_cols, group_key if isinstance(group_key, tuple) else (group_key,), strict=False))
        legacy_modes = profile_df[
            profile_df["water_lineage_mode"].isin(
                {"legacy_h2o_summary_selection", "legacy_h2o_summary_selection_plus_zero_ppm_rows"}
            )
        ].copy()
        best_legacy = legacy_modes.sort_values(
            ["gap_closed_ratio_capped_0_100", "gap_closed_ratio_raw", "delta_vs_none_overall", "delta_vs_none_zero"],
            ascending=[False, False, False, False],
            na_position="last",
            ignore_index=True,
        ).head(1)
        best_row = best_legacy.iloc[0] if not best_legacy.empty else profile_df.iloc[0]
        best_mean = summary_df[
            (summary_df["summary_scope"] == "aggregate_mode_mean")
            & (summary_df["water_lineage_mode"] == best_row["water_lineage_mode"])
            & (summary_df["mode2_semantic_profile"] == profile_values["mode2_semantic_profile"])
            & (summary_df["mode2_legacy_raw_compare_safe"] == profile_values["mode2_legacy_raw_compare_safe"])
            & (summary_df["mode2_is_baseline_bearing_profile"] == profile_values["mode2_is_baseline_bearing_profile"])
        ]
        mean_gap_closed_capped = (
            float(pd.to_numeric(best_mean.iloc[0]["gap_closed_ratio_capped_0_100"], errors="coerce"))
            if not best_mean.empty
            else math.nan
        )
        laggard_weighted = (
            float(pd.to_numeric(best_mean.iloc[0]["laggard_only_weighted_gap_closed_ratio_capped"], errors="coerce"))
            if not best_mean.empty
            else math.nan
        )
        most_dependent = (
            legacy_modes.sort_values("gap_closed_ratio_capped_0_100", ascending=False, na_position="last").iloc[0]["analyzer_id"]
            if not legacy_modes.empty
            else ""
        )
        layer_focus = _layer_focus(profile_df, stage_df)
        remaining_gap = (
            float(pd.to_numeric(best_row.get("gap_to_old_overall"), errors="coerce"))
            if pd.notna(best_row.get("gap_to_old_overall"))
            else math.nan
        )
        crossed = bool(best_row.get("crossed_old_chain_flag", False))
        significant = pd.notna(mean_gap_closed_capped) and mean_gap_closed_capped >= 0.15
        support_statement = pd.notna(mean_gap_closed_capped) and mean_gap_closed_capped > 0.0 and pd.notna(remaining_gap) and remaining_gap > 0.0
        rows.extend(
            [
                {
                    **profile_values,
                    "question_id": "legacy_gap_closure",
                    "question": "Did legacy water-lineage replay significantly shrink the gap to old_chain?",
                    "answer": "yes" if significant else "partial_or_no",
                    "evidence": (
                        f"best_mode={best_row['water_lineage_mode']}; capped_gap_closed_ratio={mean_gap_closed_capped:.4f}; laggard_weighted_capped={laggard_weighted:.4f}"
                        if pd.notna(mean_gap_closed_capped)
                        else "insufficient_data"
                    ),
                    "recommended_mode": best_row["water_lineage_mode"],
                },
                {
                    **profile_values,
                    "question_id": "dominant_layer",
                    "question": "Did the gains land more in the zero/temp layer or the final ppm layer?",
                    "answer": layer_focus,
                    "evidence": "Stage metrics compare zero-point stability gains against final ppm RMSE and capped gap-closure gains.",
                    "recommended_mode": best_row["water_lineage_mode"],
                },
                {
                    **profile_values,
                    "question_id": "most_dependent_analyzer",
                    "question": "Which analyzer depends most on legacy water replay within this semantic profile?",
                    "answer": str(most_dependent),
                    "evidence": "Chosen by the largest capped gap-closure ratio among legacy modes.",
                    "recommended_mode": best_row["water_lineage_mode"],
                },
                {
                    **profile_values,
                    "question_id": "statement_support",
                    "question": "Does the evidence support 'missing legacy water-lineage consumption is a real contributor, but not the sole cause'?",
                    "answer": "supported" if support_statement else "not_supported_or_inconclusive",
                    "evidence": (
                        f"capped_gap_closed_ratio={mean_gap_closed_capped:.4f}; remaining_gap_to_old_overall={remaining_gap:.4f}"
                        if pd.notna(mean_gap_closed_capped) and pd.notna(remaining_gap)
                        else "insufficient_data"
                    ),
                    "recommended_mode": best_row["water_lineage_mode"],
                },
                {
                    **profile_values,
                    "question_id": "remaining_main_causes",
                    "question": "If a material gap remains, what are the main residual causes?",
                    "answer": (
                        "weak_absorbance_ppm_model|missing_40C_zero_anchor"
                        if pd.notna(remaining_gap) and remaining_gap > 0.0
                        else "remaining_gap_not_obvious"
                    ),
                    "evidence": "Legacy replay is diagnostic-only and does not remove the weak ppm model / missing 40C zero-anchor risks.",
                    "recommended_mode": best_row["water_lineage_mode"],
                },
                {
                    **profile_values,
                    "question_id": "headline",
                    "question": "headline",
                    "answer": (
                        f"{best_row['water_lineage_mode']} over-close/crossed_old_chain before capping; report uses capped {mean_gap_closed_capped:.2%}"
                        if crossed and pd.notna(mean_gap_closed_capped)
                        else f"{best_row['water_lineage_mode']} closed {mean_gap_closed_capped:.2%} of the current-new-chain gap (capped)"
                        if pd.notna(mean_gap_closed_capped)
                        else "insufficient_data"
                    ),
                    "evidence": f"best_mode={best_row['water_lineage_mode']}; crossed_old_chain={crossed}",
                    "recommended_mode": best_row["water_lineage_mode"],
                },
            ]
        )
    return pd.DataFrame(rows)


def run_legacy_water_replay_diagnostic(
    *,
    water_lineage_samples: pd.DataFrame,
    comparison_samples: pd.DataFrame,
    absorbance_samples: pd.DataFrame,
    zero_residual_point_variants: pd.DataFrame,
    fixed_selection: pd.DataFrame,
    old_ratio_residuals: pd.DataFrame,
    config: Any,
) -> dict[str, pd.DataFrame | LegacyWaterReplayRules]:
    """Execute the repo-aligned legacy-water inheritance replay diagnostic."""

    rules = load_legacy_water_replay_rules()
    fixed_points = _fixed_zero_variant_points(zero_residual_point_variants, fixed_selection)
    feature_frame = build_legacy_water_replay_features(fixed_points, water_lineage_samples, rules)
    replay_points, replay_fits = apply_legacy_water_replay(feature_frame)
    replay_model_results = fit_fixed_absorbance_replay_models(replay_points, fixed_selection, config)
    selected_sample_points = _selected_sample_points(absorbance_samples, fixed_selection, config)
    point_raw = build_point_raw_summary(comparison_samples)
    compare = _build_compare_frame(
        point_raw,
        selected_sample_points,
        old_ratio_residuals,
        replay_model_results["best_predictions"],
        config,
    )
    detail = _detail_rows(compare, fixed_selection)
    summary = _summary_rows(detail)
    stage_metrics = _stage_rows(compare, detail)
    conclusions = _conclusion_rows_profiled(detail, summary, stage_metrics)
    return {
        "rules": rules,
        "feature_frame": feature_frame,
        "replay_points": replay_points,
        "replay_fits": replay_fits,
        "model_results": replay_model_results,
        "compare": compare,
        "detail": detail,
        "summary": summary,
        "stage_metrics": stage_metrics,
        "conclusions": conclusions,
    }
