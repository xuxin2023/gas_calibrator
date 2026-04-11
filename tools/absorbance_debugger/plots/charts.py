"""Matplotlib chart helpers."""

from __future__ import annotations

import json
import math
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def _finalize(fig: plt.Figure, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def plot_temperature_fit(data: pd.DataFrame, fit_table: pd.DataFrame, output_path: Path) -> None:
    """Plot temp_cavity versus temp_std with fitted lines."""

    analyzers = sorted(data["analyzer"].dropna().unique().tolist())
    if not analyzers:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.text(0.5, 0.5, "No temperature-fit data available", ha="center", va="center")
        ax.axis("off")
        return _finalize(fig, output_path)

    fig, axes = plt.subplots(len(analyzers), 1, figsize=(8, 3.5 * len(analyzers)), squeeze=False)
    for row_idx, analyzer in enumerate(analyzers):
        ax = axes[row_idx, 0]
        subset = data[data["analyzer"] == analyzer]
        ax.scatter(subset["temp_cavity_c"], subset["temp_std_c"], s=16, alpha=0.6, label="samples")
        x_line = np.linspace(subset["temp_cavity_c"].min(), subset["temp_cavity_c"].max(), 200)
        for model_name in ("linear", "quadratic"):
            fit_row = fit_table[
                (fit_table["analyzer"] == analyzer)
                & (fit_table["model_name"] == model_name)
            ]
            if fit_row.empty:
                continue
            coeffs = fit_row.iloc[0]["coefficients_desc"]
            y_line = np.polyval(np.asarray(coeffs, dtype=float), x_line)
            ax.plot(x_line, y_line, label=model_name)
        ax.set_title(f"{analyzer} temperature correction")
        ax.set_xlabel("temp_cavity_c")
        ax.set_ylabel("temp_std_c")
        ax.legend()
        ax.grid(alpha=0.2)
    _finalize(fig, output_path)


def plot_pressure_compare(data: pd.DataFrame, output_path: Path) -> None:
    """Plot raw and corrected pressure against reference pressure."""

    analyzers = sorted(data["analyzer"].dropna().unique().tolist())
    if not analyzers:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.text(0.5, 0.5, "No pressure data available", ha="center", va="center")
        ax.axis("off")
        return _finalize(fig, output_path)

    fig, axes = plt.subplots(len(analyzers), 1, figsize=(8, 3.5 * len(analyzers)), squeeze=False)
    for row_idx, analyzer in enumerate(analyzers):
        ax = axes[row_idx, 0]
        subset = data[data["analyzer"] == analyzer].sort_values(["point_row", "sample_index"])
        x = np.arange(len(subset))
        ax.plot(x, subset["pressure_std_hpa"], label="pressure_std_hpa", linewidth=1.6)
        ax.plot(x, subset["pressure_dev_raw_hpa"], label="pressure_dev_raw_hpa", linewidth=1.2)
        ax.plot(x, subset["pressure_corr_hpa"], label="pressure_corr_hpa", linewidth=1.2)
        ax.set_title(f"{analyzer} pressure comparison")
        ax.set_xlabel("sample order")
        ax.set_ylabel("hPa")
        ax.grid(alpha=0.2)
        ax.legend()
    _finalize(fig, output_path)


def plot_r0_fit(observations: pd.DataFrame, fit_table: pd.DataFrame, output_path: Path) -> None:
    """Plot R0(T) fits for raw and filtered ratios."""

    analyzers = sorted(observations["analyzer"].dropna().unique().tolist())
    if not analyzers:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.text(0.5, 0.5, "No R0 observations available", ha="center", va="center")
        ax.axis("off")
        return _finalize(fig, output_path)

    fig, axes = plt.subplots(len(analyzers), 2, figsize=(12, 3.6 * len(analyzers)), squeeze=False)
    for row_idx, analyzer in enumerate(analyzers):
        subset = observations[observations["analyzer"] == analyzer]
        for col_idx, ratio_source in enumerate(("ratio_co2_raw", "ratio_co2_filt")):
            ax = axes[row_idx, col_idx]
            ratio_subset = subset[subset["ratio_source"] == ratio_source]
            ax.scatter(ratio_subset["temp_use_c"], ratio_subset["ratio_value"], s=20, alpha=0.7, label="zero-gas samples")
            if not ratio_subset.empty:
                x_line = np.linspace(ratio_subset["temp_use_c"].min(), ratio_subset["temp_use_c"].max(), 200)
                for model_name in ("linear", "quadratic", "cubic"):
                    fit_row = fit_table[
                        (fit_table["analyzer"] == analyzer)
                        & (fit_table["ratio_source"] == ratio_source)
                        & (fit_table["temp_source"] == "temp_corr_c")
                        & (fit_table["model_name"] == model_name)
                    ]
                    if fit_row.empty:
                        continue
                    coeffs = fit_row.iloc[0]["coefficients_desc"]
                    ax.plot(x_line, np.polyval(np.asarray(coeffs, dtype=float), x_line), label=model_name)
            ax.set_title(f"{analyzer} {ratio_source}")
            ax.set_xlabel("temperature (C)")
            ax.set_ylabel("R0(T)")
            ax.grid(alpha=0.2)
            ax.legend()
    _finalize(fig, output_path)


def plot_timeseries_base_final(data: pd.DataFrame, output_path: Path, enabled: bool) -> None:
    """Plot default-branch absorbance timeseries."""

    analyzers = sorted(data["analyzer"].dropna().unique().tolist())
    if not analyzers:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.text(0.5, 0.5, "No absorbance timeseries available", ha="center", va="center")
        ax.axis("off")
        return _finalize(fig, output_path)

    fig, axes = plt.subplots(len(analyzers), 1, figsize=(10, 3.4 * len(analyzers)), squeeze=False)
    for row_idx, analyzer in enumerate(analyzers):
        ax = axes[row_idx, 0]
        subset = data[data["analyzer"] == analyzer].sort_values(["point_row", "sample_index"])
        x = np.arange(len(subset))
        ax.plot(x, subset["A_raw"], label="A_raw", linewidth=1.3)
        if enabled and "A_base" in subset.columns:
            ax.plot(x, subset["A_base"], label="A_base", linewidth=1.2)
            ax.plot(x, subset["A_final"], label="A_final", linewidth=1.2)
        else:
            ax.text(
                0.98,
                0.92,
                "base/final disabled",
                transform=ax.transAxes,
                ha="right",
                va="top",
                fontsize=9,
                bbox={"facecolor": "white", "alpha": 0.7, "edgecolor": "none"},
            )
        ax.set_title(f"{analyzer} absorbance timeseries")
        ax.set_xlabel("sample order")
        ax.set_ylabel("absorbance")
        ax.grid(alpha=0.2)
        ax.legend()
    _finalize(fig, output_path)


def plot_absorbance_compare(data: pd.DataFrame, value_col: str, y_label: str, output_path: Path) -> None:
    """Scatter target ppm against one diagnostic value, colored by temperature."""

    analyzers = sorted(data["analyzer"].dropna().unique().tolist())
    if not analyzers:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.text(0.5, 0.5, "No comparison data available", ha="center", va="center")
        ax.axis("off")
        return _finalize(fig, output_path)

    fig, axes = plt.subplots(len(analyzers), 1, figsize=(8, 3.4 * len(analyzers)), squeeze=False)
    temps = sorted(data["temp_set_c"].dropna().unique().tolist())
    cmap = plt.get_cmap("viridis", max(len(temps), 1))
    for row_idx, analyzer in enumerate(analyzers):
        ax = axes[row_idx, 0]
        subset = data[data["analyzer"] == analyzer]
        for temp_idx, temp in enumerate(temps):
            one_temp = subset[subset["temp_set_c"] == temp]
            if one_temp.empty:
                continue
            ax.scatter(
                one_temp["target_co2_ppm"],
                one_temp[value_col],
                label=f"{temp:.0f} C",
                color=cmap(temp_idx),
                s=28,
                alpha=0.75,
            )
        ax.set_title(f"{analyzer} target ppm vs {value_col}")
        ax.set_xlabel("target_co2_ppm")
        ax.set_ylabel(y_label)
        ax.grid(alpha=0.2)
        ax.legend(ncol=4, fontsize=8)
    _finalize(fig, output_path)


def plot_zero_drift(data: pd.DataFrame, value_col: str, y_label: str, output_path: Path) -> None:
    """Plot zero-gas drift across temperatures."""

    analyzers = sorted(data["analyzer"].dropna().unique().tolist())
    if not analyzers:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.text(0.5, 0.5, "No zero-gas points available", ha="center", va="center")
        ax.axis("off")
        return _finalize(fig, output_path)

    fig, ax = plt.subplots(figsize=(8, 4.2))
    for analyzer in analyzers:
        subset = data[data["analyzer"] == analyzer].sort_values("temp_set_c")
        ax.plot(subset["temp_set_c"], subset[value_col], marker="o", linewidth=1.2, label=analyzer)
    ax.set_title(f"Zero-gas drift: {value_col}")
    ax.set_xlabel("temperature (C)")
    ax.set_ylabel(y_label)
    ax.grid(alpha=0.2)
    ax.legend()
    _finalize(fig, output_path)


def plot_ratio_series(data: pd.DataFrame, output_path: Path) -> None:
    """Plot point-order comparison between raw ratio, filtered ratio, and absorbance."""

    analyzers = sorted(data["analyzer"].dropna().unique().tolist())
    if not analyzers:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.text(0.5, 0.5, "No ratio series available", ha="center", va="center")
        ax.axis("off")
        return _finalize(fig, output_path)

    fig, axes = plt.subplots(len(analyzers), 1, figsize=(10, 3.2 * len(analyzers)), squeeze=False)
    for row_idx, analyzer in enumerate(analyzers):
        ax = axes[row_idx, 0]
        subset = data[data["analyzer"] == analyzer].sort_values(["temp_set_c", "target_co2_ppm"])
        x = np.arange(len(subset))
        ax.plot(x, subset["ratio_co2_raw_mean"], label="ratio_co2_raw_mean", linewidth=1.2)
        ax.plot(x, subset["ratio_co2_filt_mean"], label="ratio_co2_filt_mean", linewidth=1.2)
        ax.plot(x, subset["A_mean"], label="A_mean", linewidth=1.2)
        ax.set_title(f"{analyzer} raw ratio / filt ratio / A_mean")
        ax.set_xlabel("point order")
        ax.set_ylabel("diagnostic value")
        ax.grid(alpha=0.2)
        ax.legend()
    _finalize(fig, output_path)


def plot_error_hist(data: pd.DataFrame, output_path: Path) -> None:
    """Plot overlaid histograms for old/new chain errors."""

    analyzers = sorted(data["analyzer_id"].dropna().unique().tolist())
    fig, axes = plt.subplots(max(len(analyzers), 1), 1, figsize=(9, 3.4 * max(len(analyzers), 1)), squeeze=False)
    if not analyzers:
        axes[0, 0].text(0.5, 0.5, "No comparison errors available", ha="center", va="center")
        axes[0, 0].axis("off")
        return _finalize(fig, output_path)
    for row_idx, analyzer in enumerate(analyzers):
        ax = axes[row_idx, 0]
        subset = data[data["analyzer_id"] == analyzer]
        ax.hist(subset["old_error"].dropna(), bins=12, alpha=0.55, label="old_error")
        ax.hist(subset["new_error"].dropna(), bins=12, alpha=0.55, label="new_error")
        ax.set_title(f"{analyzer} error histogram")
        ax.set_xlabel("error (ppm)")
        ax.set_ylabel("count")
        ax.grid(alpha=0.2)
        ax.legend()
    _finalize(fig, output_path)


def plot_error_boxplot(data: pd.DataFrame, output_path: Path) -> None:
    """Plot old/new error boxplots per analyzer."""

    analyzers = sorted(data["analyzer_id"].dropna().unique().tolist())
    fig, ax = plt.subplots(figsize=(max(8, 2.5 * max(len(analyzers), 1)), 4.5))
    if not analyzers:
        ax.text(0.5, 0.5, "No comparison errors available", ha="center", va="center")
        ax.axis("off")
        return _finalize(fig, output_path)
    positions = []
    values = []
    labels = []
    pos = 1
    for analyzer in analyzers:
        subset = data[data["analyzer_id"] == analyzer]
        positions.extend([pos, pos + 1])
        values.extend([subset["old_error"].dropna().tolist(), subset["new_error"].dropna().tolist()])
        labels.extend([f"{analyzer}\nold", f"{analyzer}\nnew"])
        pos += 3
    ax.boxplot(values, positions=positions, widths=0.7, patch_artist=True)
    ax.set_xticks(positions)
    ax.set_xticklabels(labels)
    ax.set_title("Old vs new error boxplot")
    ax.set_ylabel("error (ppm)")
    ax.grid(alpha=0.2, axis="y")
    _finalize(fig, output_path)


def plot_error_vs_temp(data: pd.DataFrame, output_path: Path) -> None:
    """Plot error versus temperature."""

    analyzers = sorted(data["analyzer_id"].dropna().unique().tolist())
    fig, axes = plt.subplots(max(len(analyzers), 1), 1, figsize=(9, 3.4 * max(len(analyzers), 1)), squeeze=False)
    if not analyzers:
        axes[0, 0].text(0.5, 0.5, "No comparison data available", ha="center", va="center")
        axes[0, 0].axis("off")
        return _finalize(fig, output_path)
    for row_idx, analyzer in enumerate(analyzers):
        ax = axes[row_idx, 0]
        subset = data[data["analyzer_id"] == analyzer]
        ax.scatter(subset["temp_c"], subset["old_error"], alpha=0.65, s=24, label="old_error")
        ax.scatter(subset["temp_c"], subset["new_error"], alpha=0.65, s=24, label="new_error")
        ax.set_title(f"{analyzer} error vs temperature")
        ax.set_xlabel("temp_c")
        ax.set_ylabel("error (ppm)")
        ax.grid(alpha=0.2)
        ax.legend()
    _finalize(fig, output_path)


def plot_error_vs_target_ppm(data: pd.DataFrame, output_path: Path) -> None:
    """Plot error versus target ppm."""

    analyzers = sorted(data["analyzer_id"].dropna().unique().tolist())
    fig, axes = plt.subplots(max(len(analyzers), 1), 1, figsize=(9, 3.4 * max(len(analyzers), 1)), squeeze=False)
    if not analyzers:
        axes[0, 0].text(0.5, 0.5, "No comparison data available", ha="center", va="center")
        axes[0, 0].axis("off")
        return _finalize(fig, output_path)
    for row_idx, analyzer in enumerate(analyzers):
        ax = axes[row_idx, 0]
        subset = data[data["analyzer_id"] == analyzer]
        ax.scatter(subset["target_ppm"], subset["old_error"], alpha=0.65, s=24, label="old_error")
        ax.scatter(subset["target_ppm"], subset["new_error"], alpha=0.65, s=24, label="new_error")
        ax.set_title(f"{analyzer} error vs target ppm")
        ax.set_xlabel("target_ppm")
        ax.set_ylabel("error (ppm)")
        ax.grid(alpha=0.2)
        ax.legend()
    _finalize(fig, output_path)


def plot_per_temp_compare(data: pd.DataFrame, output_path: Path) -> None:
    """Plot per-temperature RMSE comparison."""

    analyzers = sorted(data["analyzer_id"].dropna().unique().tolist())
    fig, axes = plt.subplots(max(len(analyzers), 1), 1, figsize=(10, 3.6 * max(len(analyzers), 1)), squeeze=False)
    if not analyzers:
        axes[0, 0].text(0.5, 0.5, "No per-temperature comparison available", ha="center", va="center")
        axes[0, 0].axis("off")
        return _finalize(fig, output_path)
    for row_idx, analyzer in enumerate(analyzers):
        ax = axes[row_idx, 0]
        subset = data[data["analyzer_id"] == analyzer].sort_values("temp_c")
        x = np.arange(len(subset))
        ax.bar(x - 0.18, subset["old_rmse"], width=0.36, label="old_rmse")
        ax.bar(x + 0.18, subset["new_rmse"], width=0.36, label="new_rmse")
        ax.set_xticks(x)
        ax.set_xticklabels([f"{temp:g}" for temp in subset["temp_c"]])
        ax.set_title(f"{analyzer} per-temp RMSE compare")
        ax.set_xlabel("temp_c")
        ax.set_ylabel("RMSE (ppm)")
        ax.grid(alpha=0.2, axis="y")
        ax.legend()
    _finalize(fig, output_path)


def plot_zero_compare(data: pd.DataFrame, output_path: Path) -> None:
    """Plot zero-point mean error versus temperature."""

    analyzers = sorted(data["analyzer_id"].dropna().unique().tolist())
    fig, axes = plt.subplots(max(len(analyzers), 1), 1, figsize=(9, 3.4 * max(len(analyzers), 1)), squeeze=False)
    if not analyzers:
        axes[0, 0].text(0.5, 0.5, "No zero comparison available", ha="center", va="center")
        axes[0, 0].axis("off")
        return _finalize(fig, output_path)
    for row_idx, analyzer in enumerate(analyzers):
        ax = axes[row_idx, 0]
        subset = data[data["analyzer_id"] == analyzer].sort_values("temp_c")
        ax.plot(subset["temp_c"], subset["old_zero_mean_error"], marker="o", label="old_zero_mean_error")
        ax.plot(subset["temp_c"], subset["new_zero_mean_error"], marker="o", label="new_zero_mean_error")
        ax.set_title(f"{analyzer} zero compare")
        ax.set_xlabel("temp_c")
        ax.set_ylabel("mean zero error (ppm)")
        ax.grid(alpha=0.2)
        ax.legend()
    _finalize(fig, output_path)


def plot_absorbance_model_fit_overall(data: pd.DataFrame, output_path: Path) -> None:
    """Plot target ppm against best-model overall-fit prediction."""

    analyzers = sorted(data["analyzer_id"].dropna().unique().tolist())
    fig, axes = plt.subplots(max(len(analyzers), 1), 1, figsize=(9, 3.6 * max(len(analyzers), 1)), squeeze=False)
    if not analyzers:
        axes[0, 0].text(0.5, 0.5, "No absorbance model fit data available", ha="center", va="center")
        axes[0, 0].axis("off")
        return _finalize(fig, output_path)
    for row_idx, analyzer in enumerate(analyzers):
        ax = axes[row_idx, 0]
        subset = data[data["analyzer_id"] == analyzer]
        ax.scatter(subset["target_ppm"], subset["best_overall_fit_pred_ppm"], alpha=0.7, s=28, c=subset["temp_c"], cmap="viridis")
        if subset["target_ppm"].notna().any():
            lo = float(np.nanmin(subset["target_ppm"]))
            hi = float(np.nanmax(subset["target_ppm"]))
            ax.plot([lo, hi], [lo, hi], color="black", linestyle="--", linewidth=1.0)
        label = subset["best_absorbance_model_label"].dropna().iloc[0] if subset["best_absorbance_model_label"].notna().any() else "best model"
        ax.set_title(f"{analyzer} overall fit: {label}")
        ax.set_xlabel("target_ppm")
        ax.set_ylabel("best_overall_fit_pred_ppm")
        ax.grid(alpha=0.2)
    _finalize(fig, output_path)


def plot_absorbance_model_fit_by_temp(data: pd.DataFrame, output_path: Path) -> None:
    """Plot best-model prediction curves by temperature."""

    analyzers = sorted(data["analyzer_id"].dropna().unique().tolist())
    fig, axes = plt.subplots(max(len(analyzers), 1), 1, figsize=(10, 3.8 * max(len(analyzers), 1)), squeeze=False)
    if not analyzers:
        axes[0, 0].text(0.5, 0.5, "No absorbance model fit data available", ha="center", va="center")
        axes[0, 0].axis("off")
        return _finalize(fig, output_path)
    for row_idx, analyzer in enumerate(analyzers):
        ax = axes[row_idx, 0]
        subset = data[data["analyzer_id"] == analyzer]
        temps = sorted(subset["temp_c"].dropna().unique().tolist())
        cmap = plt.get_cmap("tab10", max(len(temps), 1))
        for temp_idx, temp_c in enumerate(temps):
            one_temp = subset[subset["temp_c"] == temp_c].sort_values("target_ppm")
            ax.plot(one_temp["target_ppm"], one_temp["best_overall_fit_pred_ppm"], marker="o", linewidth=1.2, color=cmap(temp_idx), label=f"{temp_c:g} C")
        ax.plot(subset["target_ppm"], subset["target_ppm"], color="black", linestyle="--", linewidth=1.0, label="ideal")
        ax.set_title(f"{analyzer} best-model fit by temperature")
        ax.set_xlabel("target_ppm")
        ax.set_ylabel("best_overall_fit_pred_ppm")
        ax.grid(alpha=0.2)
        ax.legend(ncol=4, fontsize=8)
    _finalize(fig, output_path)


def plot_absorbance_model_residual_hist(data: pd.DataFrame, output_path: Path) -> None:
    """Plot selected-model residual histogram using the selected prediction scope."""

    analyzers = sorted(data["analyzer_id"].dropna().unique().tolist())
    fig, axes = plt.subplots(max(len(analyzers), 1), 1, figsize=(9, 3.4 * max(len(analyzers), 1)), squeeze=False)
    if not analyzers:
        axes[0, 0].text(0.5, 0.5, "No absorbance model residuals available", ha="center", va="center")
        axes[0, 0].axis("off")
        return _finalize(fig, output_path)
    for row_idx, analyzer in enumerate(analyzers):
        ax = axes[row_idx, 0]
        subset = data[data["analyzer_id"] == analyzer]
        ax.hist(subset["selected_error_ppm"].dropna(), bins=12, alpha=0.7, label="selected_error_ppm")
        ax.set_title(f"{analyzer} selected-model residual histogram")
        ax.set_xlabel("error (ppm)")
        ax.set_ylabel("count")
        ax.grid(alpha=0.2)
        ax.legend()
    _finalize(fig, output_path)


def plot_absorbance_model_residual_vs_temp(data: pd.DataFrame, output_path: Path) -> None:
    """Plot selected-model residual versus temperature."""

    analyzers = sorted(data["analyzer_id"].dropna().unique().tolist())
    fig, axes = plt.subplots(max(len(analyzers), 1), 1, figsize=(9, 3.4 * max(len(analyzers), 1)), squeeze=False)
    if not analyzers:
        axes[0, 0].text(0.5, 0.5, "No absorbance model residuals available", ha="center", va="center")
        axes[0, 0].axis("off")
        return _finalize(fig, output_path)
    for row_idx, analyzer in enumerate(analyzers):
        ax = axes[row_idx, 0]
        subset = data[data["analyzer_id"] == analyzer]
        ax.scatter(subset["temp_c"], subset["selected_error_ppm"], alpha=0.7, s=28)
        ax.set_title(f"{analyzer} selected-model residual vs temperature")
        ax.set_xlabel("temp_c")
        ax.set_ylabel("selected_error_ppm")
        ax.grid(alpha=0.2)
    _finalize(fig, output_path)


def plot_absorbance_model_residual_vs_target(data: pd.DataFrame, output_path: Path) -> None:
    """Plot selected-model residual versus target ppm."""

    analyzers = sorted(data["analyzer_id"].dropna().unique().tolist())
    fig, axes = plt.subplots(max(len(analyzers), 1), 1, figsize=(9, 3.4 * max(len(analyzers), 1)), squeeze=False)
    if not analyzers:
        axes[0, 0].text(0.5, 0.5, "No absorbance model residuals available", ha="center", va="center")
        axes[0, 0].axis("off")
        return _finalize(fig, output_path)
    for row_idx, analyzer in enumerate(analyzers):
        ax = axes[row_idx, 0]
        subset = data[data["analyzer_id"] == analyzer]
        ax.scatter(subset["target_ppm"], subset["selected_error_ppm"], alpha=0.7, s=28)
        ax.set_title(f"{analyzer} selected-model residual vs target")
        ax.set_xlabel("target_ppm")
        ax.set_ylabel("selected_error_ppm")
        ax.grid(alpha=0.2)
    _finalize(fig, output_path)


def plot_absorbance_model_old_vs_new(data: pd.DataFrame, output_path: Path) -> None:
    """Plot old-chain predictions against selected absorbance-model predictions."""

    analyzers = sorted(data["analyzer_id"].dropna().unique().tolist())
    fig, axes = plt.subplots(max(len(analyzers), 1), 1, figsize=(9, 3.6 * max(len(analyzers), 1)), squeeze=False)
    if not analyzers:
        axes[0, 0].text(0.5, 0.5, "No old/new comparison available", ha="center", va="center")
        axes[0, 0].axis("off")
        return _finalize(fig, output_path)
    for row_idx, analyzer in enumerate(analyzers):
        ax = axes[row_idx, 0]
        subset = data[data["analyzer_id"] == analyzer]
        ax.scatter(subset["target_ppm"], subset["old_pred_ppm"], alpha=0.65, s=26, label="old_chain")
        ax.scatter(subset["target_ppm"], subset["new_pred_ppm"], alpha=0.65, s=26, label="best_absorbance_model")
        if subset["target_ppm"].notna().any():
            lo = float(np.nanmin(subset["target_ppm"]))
            hi = float(np.nanmax(subset["target_ppm"]))
            ax.plot([lo, hi], [lo, hi], color="black", linestyle="--", linewidth=1.0)
        ax.set_title(f"{analyzer} old vs best absorbance model")
        ax.set_xlabel("target_ppm")
        ax.set_ylabel("predicted_ppm")
        ax.grid(alpha=0.2)
        ax.legend()
    _finalize(fig, output_path)


def plot_branch_metric_compare(
    data: pd.DataFrame,
    category_column: str,
    output_path: Path,
    *,
    title: str,
) -> None:
    """Plot per-analyzer diagnostic branch comparisons for overall and zero RMSE."""

    if "analyzer_id" not in data.columns or category_column not in data.columns:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.text(0.5, 0.5, "No diagnostic comparison data available", ha="center", va="center")
        ax.axis("off")
        return _finalize(fig, output_path)
    analyzers = sorted(data["analyzer_id"].dropna().unique().tolist())
    fig, axes = plt.subplots(max(len(analyzers), 1), 2, figsize=(12, 3.6 * max(len(analyzers), 1)), squeeze=False)
    if not analyzers:
        axes[0, 0].text(0.5, 0.5, "No diagnostic comparison data available", ha="center", va="center")
        axes[0, 0].axis("off")
        axes[0, 1].axis("off")
        return _finalize(fig, output_path)
    for row_idx, analyzer in enumerate(analyzers):
        subset = data[data["analyzer_id"] == analyzer].copy()
        labels = subset[category_column].astype(str).tolist()
        x = np.arange(len(labels))
        overall_ax = axes[row_idx, 0]
        zero_ax = axes[row_idx, 1]
        overall_ax.bar(x, subset["new_chain_rmse"], color="#4472c4")
        if "old_chain_rmse" in subset.columns and subset["old_chain_rmse"].notna().any():
            overall_ax.axhline(float(subset["old_chain_rmse"].dropna().iloc[0]), color="#c0504d", linestyle="--", linewidth=1.2, label="old_chain_rmse")
        overall_ax.set_xticks(x)
        overall_ax.set_xticklabels(labels, rotation=20, ha="right")
        overall_ax.set_title(f"{analyzer} overall RMSE")
        overall_ax.set_ylabel("ppm")
        overall_ax.grid(alpha=0.2, axis="y")
        if row_idx == 0 and "old_chain_rmse" in subset.columns and subset["old_chain_rmse"].notna().any():
            overall_ax.legend()

        zero_ax.bar(x, subset["new_zero_rmse"], color="#70ad47")
        if "old_zero_rmse" in subset.columns and subset["old_zero_rmse"].notna().any():
            zero_ax.axhline(float(subset["old_zero_rmse"].dropna().iloc[0]), color="#c0504d", linestyle="--", linewidth=1.2, label="old_zero_rmse")
        zero_ax.set_xticks(x)
        zero_ax.set_xticklabels(labels, rotation=20, ha="right")
        zero_ax.set_title(f"{analyzer} zero RMSE")
        zero_ax.set_ylabel("ppm")
        zero_ax.grid(alpha=0.2, axis="y")
        if row_idx == 0 and "old_zero_rmse" in subset.columns and subset["old_zero_rmse"].notna().any():
            zero_ax.legend()
    fig.suptitle(title)
    _finalize(fig, output_path)


def plot_upper_bound_vs_deployable(data: pd.DataFrame, output_path: Path) -> None:
    """Plot old/new RMSE under physics upper bound and deployable contexts."""

    if "analyzer_id" not in data.columns:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.text(0.5, 0.5, "No upper-bound/deployable data available", ha="center", va="center")
        ax.axis("off")
        return _finalize(fig, output_path)
    analyzers = sorted(data["analyzer_id"].dropna().unique().tolist())
    fig, axes = plt.subplots(max(len(analyzers), 1), 1, figsize=(10, 3.8 * max(len(analyzers), 1)), squeeze=False)
    if not analyzers:
        axes[0, 0].text(0.5, 0.5, "No upper-bound/deployable data available", ha="center", va="center")
        axes[0, 0].axis("off")
        return _finalize(fig, output_path)
    for row_idx, analyzer in enumerate(analyzers):
        ax = axes[row_idx, 0]
        subset = data[data["analyzer_id"] == analyzer].copy()
        subset = subset.sort_values("chain_context")
        x = np.arange(len(subset))
        ax.bar(x - 0.18, subset["old_chain_rmse"], width=0.36, label="old_chain_rmse", color="#c0504d")
        ax.bar(x + 0.18, subset["new_chain_rmse"], width=0.36, label="new_chain_rmse", color="#4472c4")
        ax.set_xticks(x)
        ax.set_xticklabels(subset["chain_context"].astype(str).tolist())
        ax.set_title(f"{analyzer} upper bound vs deployable")
        ax.set_ylabel("RMSE (ppm)")
    ax.grid(alpha=0.2, axis="y")
    ax.legend()
    _finalize(fig, output_path)


def plot_invalid_pressure_points(data: pd.DataFrame, output_path: Path) -> None:
    """Plot invalid-pressure hits and their pressure evidence."""

    if data.empty:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.text(0.5, 0.5, "No invalid-pressure points were detected", ha="center", va="center")
        ax.axis("off")
        return _finalize(fig, output_path)

    analyzers = sorted(data["analyzer_id"].dropna().unique().tolist())
    fig, axes = plt.subplots(max(len(analyzers), 1), 1, figsize=(10, 3.2 * max(len(analyzers), 1)), squeeze=False)
    for row_idx, analyzer in enumerate(analyzers):
        ax = axes[row_idx, 0]
        subset = data[data["analyzer_id"] == analyzer].reset_index(drop=True)
        x = np.arange(len(subset))
        ax.scatter(x, subset["pressure_std_hpa_mean"], label="pressure_std_hpa_mean", s=36)
        ax.scatter(x, subset["pressure_corr_hpa_mean"], label="pressure_corr_hpa_mean", s=36)
        if "target_pressure_hpa_if_available" in subset.columns:
            ax.scatter(x, subset["target_pressure_hpa_if_available"], label="target_pressure_hpa", s=36, marker="x")
        ax.set_xticks(x)
        ax.set_xticklabels([str(value) for value in subset["point_title"]], rotation=20, ha="right")
        ax.set_title(f"{analyzer} invalid pressure evidence")
        ax.set_ylabel("hPa")
        ax.grid(alpha=0.2)
        ax.legend(fontsize=8)
    _finalize(fig, output_path)


def plot_default_chain_before_after(data: pd.DataFrame, output_path: Path) -> None:
    """Plot new-chain RMSE before/after rule freeze and valid-only exclusion."""

    analyzers = sorted(data["analyzer_id"].dropna().unique().tolist()) if not data.empty else []
    fig, ax = plt.subplots(figsize=(max(8, 2.8 * max(len(analyzers), 1)), 4.6))
    if not analyzers:
        ax.text(0.5, 0.5, "No before/after comparison data available", ha="center", va="center")
        ax.axis("off")
        return _finalize(fig, output_path)

    x = np.arange(len(analyzers), dtype=float)
    width = 0.24
    ordered = data.set_index("analyzer_id").reindex(analyzers)
    ax.bar(x - width, ordered["new_chain_rmse_before_default_full_data"], width=width, label="before default (full)")
    ax.bar(x, ordered["new_chain_rmse_after_default_full_data"], width=width, label="after default (full)")
    ax.bar(x + width, ordered["new_chain_rmse_after_default_valid_only"], width=width, label="after default (valid-only)")
    ax.plot(x, ordered["old_chain_rmse_valid_only"], color="#1b5e20", marker="o", linewidth=1.4, label="old_chain valid-only")
    ax.set_xticks(x)
    ax.set_xticklabels(analyzers)
    ax.set_title("Default-chain tightening before/after")
    ax.set_ylabel("RMSE (ppm)")
    ax.grid(alpha=0.2, axis="y")
    ax.legend(fontsize=8)
    _finalize(fig, output_path)


def _predict_zero_residual_curve(
    model_id: str,
    coefficients: np.ndarray,
    x_line: np.ndarray,
    breakpoint_temp_c: float | None,
) -> np.ndarray:
    if model_id == "linear":
        return coefficients[0] + coefficients[1] * x_line
    if model_id == "quadratic":
        return coefficients[0] + coefficients[1] * x_line + coefficients[2] * np.square(x_line)
    if model_id == "piecewise_linear":
        hinge = np.maximum(x_line - float(breakpoint_temp_c or 0.0), 0.0)
        return coefficients[0] + coefficients[1] * x_line + coefficients[2] * hinge
    return np.zeros(len(x_line), dtype=float)


def plot_zero_residual_models(observations: pd.DataFrame, models: pd.DataFrame, output_path: Path) -> None:
    """Plot zero-point absorbance observations and fitted residual models."""

    if observations.empty:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.text(0.5, 0.5, "No zero-residual observations available", ha="center", va="center")
        ax.axis("off")
        return _finalize(fig, output_path)

    analyzers = sorted(observations["analyzer_id"].dropna().unique().tolist())
    fig, axes = plt.subplots(max(len(analyzers), 1), 2, figsize=(12, 3.6 * max(len(analyzers), 1)), squeeze=False)
    for row_idx, analyzer in enumerate(analyzers):
        for col_idx, ratio_source in enumerate(("ratio_co2_raw", "ratio_co2_filt")):
            ax = axes[row_idx, col_idx]
            subset = observations[
                (observations["analyzer_id"] == analyzer)
                & (observations["ratio_source"] == ratio_source)
            ].sort_values("temp_use_c")
            ax.scatter(subset["temp_use_c"], subset["A_zero_obs"], s=28, alpha=0.75, label="0 ppm observations")
            if not subset.empty:
                x_line = np.linspace(float(subset["temp_use_c"].min()), float(subset["temp_use_c"].max()), 200)
                fit_subset = models[
                    (models["analyzer_id"] == analyzer)
                    & (models["ratio_source"] == ratio_source)
                    & (models["zero_residual_model"] != "none")
                ]
                for _, fit_row in fit_subset.iterrows():
                    coeffs = np.asarray(json.loads(fit_row["coefficients_desc"]), dtype=float)
                    y_line = _predict_zero_residual_curve(
                        str(fit_row["zero_residual_model"]),
                        coeffs,
                        x_line,
                        float(fit_row["breakpoint_temp_c"]) if pd.notna(fit_row["breakpoint_temp_c"]) else None,
                    )
                    ax.plot(x_line, y_line, linewidth=1.2, label=str(fit_row["zero_residual_model"]))
            ax.axhline(0.0, color="black", linestyle="--", linewidth=0.9)
            ax.set_title(f"{analyzer} {ratio_source}")
            ax.set_xlabel("temp_use_c")
            ax.set_ylabel("A_zero_obs")
            ax.grid(alpha=0.2)
            ax.legend(fontsize=8)
    _finalize(fig, output_path)


def plot_water_zero_anchor_models(observations: pd.DataFrame, models: pd.DataFrame, output_path: Path) -> None:
    """Plot water zero-anchor candidate RMSE by analyzer and source pair."""

    if observations.empty or models.empty:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.text(0.5, 0.5, "No water zero-anchor diagnostics available", ha="center", va="center")
        ax.axis("off")
        return _finalize(fig, output_path)

    analyzers = sorted(models["analyzer_id"].dropna().astype(str).unique().tolist())
    fig, axes = plt.subplots(max(len(analyzers), 1), 1, figsize=(12, 3.6 * max(len(analyzers), 1)), squeeze=False)
    for row_idx, analyzer in enumerate(analyzers):
        ax = axes[row_idx, 0]
        subset = models[models["analyzer_id"] == analyzer].copy()
        subset = subset.sort_values(["ratio_source", "zero_residual_mode", "water_zero_anchor_model"], ignore_index=True)
        labels = [
            f"{row['ratio_source'].replace('ratio_co2_', '')}\n{row['zero_residual_mode']}\n{row['water_zero_anchor_model']}"
            for row in subset.to_dict(orient="records")
        ]
        x = np.arange(len(subset), dtype=float)
        colors = [
            "#4472c4" if str(model_id) == "none" else "#70ad47" if str(model_id) == "linear" else "#ed7d31"
            for model_id in subset["water_zero_anchor_model"].astype(str)
        ]
        ax.bar(x, subset["rmse_zero_absorbance"], color=colors)
        for idx, row in enumerate(subset.itertuples(index=False), start=0):
            if bool(getattr(row, "is_selected_water_zero_anchor_model", False)):
                ax.text(idx, float(getattr(row, "rmse_zero_absorbance", 0.0)), " selected", rotation=90, va="bottom", ha="center", fontsize=8)
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=20, ha="right")
        ax.set_title(f"{analyzer} water zero-anchor RMSE")
        ax.set_ylabel("zero absorbance RMSE")
        ax.grid(alpha=0.2, axis="y")
    _finalize(fig, output_path)


def plot_water_anchor_compare(data: pd.DataFrame, output_path: Path) -> None:
    """Plot baseline vs water-anchor diagnostic metrics."""

    if data.empty:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.text(0.5, 0.5, "No water-anchor comparison data available", ha="center", va="center")
        ax.axis("off")
        return _finalize(fig, output_path)

    analyzers = sorted(data["analyzer_id"].dropna().astype(str).unique().tolist())
    ordered = data.set_index("analyzer_id").reindex(analyzers)
    x = np.arange(len(analyzers), dtype=float)
    width = 0.36
    fig, axes = plt.subplots(3, 2, figsize=(16, 10), squeeze=False)
    panels = (
        ("baseline_overall_rmse", "water_anchor_overall_rmse", "Overall RMSE"),
        ("baseline_zero_rmse", "water_anchor_zero_rmse", "Zero RMSE"),
        ("baseline_temp_bias_spread", "water_anchor_temp_bias_spread", "Temp Bias Spread"),
        ("baseline_max_abs_error", "water_anchor_max_abs_error", "Max Abs Error"),
        ("baseline_low_range_rmse", "water_anchor_low_range_rmse", "Low-range RMSE"),
        ("baseline_new_rmse_at_40c", "water_anchor_new_rmse_at_40c", "40C RMSE"),
    )
    for ax, (base_col, water_col, title) in zip(axes.flatten(), panels, strict=False):
        ax.bar(x - width / 2.0, ordered[base_col], width=width, label="baseline", color="#4472c4")
        ax.bar(x + width / 2.0, ordered[water_col], width=width, label="water_anchor", color="#70ad47")
        ax.set_xticks(x)
        ax.set_xticklabels(analyzers)
        ax.set_title(title)
        ax.grid(alpha=0.2, axis="y")
    axes[0, 0].legend(fontsize=8)
    _finalize(fig, output_path)


def plot_piecewise_model_compare(data: pd.DataFrame, output_path: Path) -> None:
    """Plot best single-range vs best piecewise metrics per analyzer."""

    if data.empty:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.text(0.5, 0.5, "No piecewise comparison data available", ha="center", va="center")
        ax.axis("off")
        return _finalize(fig, output_path)

    analyzers = sorted(data["analyzer_id"].dropna().unique().tolist())
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5), squeeze=False)
    x = np.arange(len(analyzers), dtype=float)
    ordered = data.set_index("analyzer_id").reindex(analyzers)
    axes[0, 0].bar(x - 0.18, ordered["single_range_overall_rmse"], width=0.36, label="single-range")
    axes[0, 0].bar(x + 0.18, ordered["piecewise_overall_rmse"], width=0.36, label="piecewise")
    axes[0, 0].set_xticks(x)
    axes[0, 0].set_xticklabels(analyzers)
    axes[0, 0].set_title("Overall RMSE")
    axes[0, 0].grid(alpha=0.2, axis="y")
    axes[0, 0].legend()
    axes[0, 1].bar(x - 0.18, ordered["single_range_zero_rmse"], width=0.36, label="single-range")
    axes[0, 1].bar(x + 0.18, ordered["piecewise_zero_rmse"], width=0.36, label="piecewise")
    axes[0, 1].set_xticks(x)
    axes[0, 1].set_xticklabels(analyzers)
    axes[0, 1].set_title("Zero RMSE")
    axes[0, 1].grid(alpha=0.2, axis="y")
    axes[0, 1].legend()
    _finalize(fig, output_path)


def plot_ga01_residual_profile(data: pd.DataFrame, output_path: Path) -> None:
    """Plot the GA01 residual profile summary."""

    if data.empty:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.text(0.5, 0.5, "No GA01 profile data available", ha="center", va="center")
        ax.axis("off")
        return _finalize(fig, output_path)

    fig, axes = plt.subplots(2, 3, figsize=(15, 8), squeeze=False)

    by_temp = data[data["profile_section"] == "by_temperature"].sort_values("temp_c")
    axes[0, 0].plot(by_temp["temp_c"], by_temp["new_rmse"], marker="o", label="new_rmse")
    axes[0, 0].plot(by_temp["temp_c"], by_temp["old_rmse"], marker="o", label="old_rmse")
    axes[0, 0].set_title("By temperature")
    axes[0, 0].grid(alpha=0.2)
    axes[0, 0].legend(fontsize=8)

    by_target = data[data["profile_section"] == "by_target_ppm"].sort_values("target_ppm")
    axes[0, 1].plot(by_target["target_ppm"], by_target["new_rmse"], marker="o", label="new_rmse")
    axes[0, 1].plot(by_target["target_ppm"], by_target["old_rmse"], marker="o", label="old_rmse")
    axes[0, 1].set_title("By target ppm")
    axes[0, 1].grid(alpha=0.2)
    axes[0, 1].legend(fontsize=8)

    source_cmp = data[data["profile_section"] == "source_pair_compare"].sort_values("new_chain_rmse")
    axes[0, 2].bar(source_cmp["variant_label"], source_cmp["new_chain_rmse"], label="new")
    axes[0, 2].axhline(float(source_cmp["old_chain_rmse"].dropna().iloc[0]) if source_cmp["old_chain_rmse"].notna().any() else np.nan, color="black", linestyle="--", linewidth=1.0, label="old")
    axes[0, 2].set_title("raw/raw vs filt/filt")
    axes[0, 2].tick_params(axis="x", rotation=20)
    axes[0, 2].grid(alpha=0.2, axis="y")
    axes[0, 2].legend(fontsize=8)

    zero_cmp = data[data["profile_section"] == "zero_residual_compare"].sort_values("new_chain_rmse")
    axes[1, 0].bar(zero_cmp["variant_label"], zero_cmp["new_chain_rmse"])
    axes[1, 0].set_title("with / without ΔA0(T)")
    axes[1, 0].tick_params(axis="x", rotation=20)
    axes[1, 0].grid(alpha=0.2, axis="y")

    family_cmp = data[data["profile_section"] == "model_family_compare"].sort_values("new_chain_rmse")
    axes[1, 1].bar(family_cmp["variant_label"], family_cmp["new_chain_rmse"])
    axes[1, 1].set_title("single vs piecewise")
    axes[1, 1].tick_params(axis="x", rotation=20)
    axes[1, 1].grid(alpha=0.2, axis="y")

    diag = data[data["profile_section"] == "diagnosis"].head(1)
    axes[1, 2].axis("off")
    if not diag.empty:
        axes[1, 2].text(
            0.02,
            0.98,
            str(diag.iloc[0]["diagnosis_note"]),
            ha="left",
            va="top",
            wrap=True,
        )
        axes[1, 2].set_title(str(diag.iloc[0]["primary_issue"]))
    _finalize(fig, output_path)


def plot_legacy_water_replay(data: pd.DataFrame, output_path: Path) -> None:
    """Plot gap-closure and zero-point gains for legacy water replay modes."""

    if data.empty:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.text(0.5, 0.5, "No legacy water replay data available", ha="center", va="center")
        ax.axis("off")
        return _finalize(fig, output_path)

    plot_data = data[data["water_lineage_mode"] != "none"].copy()
    analyzers = sorted(plot_data["analyzer_id"].dropna().unique().tolist())
    modes = [
        "simplified_subzero_anchor",
        "legacy_h2o_summary_selection",
        "legacy_h2o_summary_selection_plus_zero_ppm_rows",
    ]
    if not analyzers:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.text(0.5, 0.5, "No non-baseline replay modes available", ha="center", va="center")
        ax.axis("off")
        return _finalize(fig, output_path)

    fig, axes = plt.subplots(2, 1, figsize=(12, 8), squeeze=False)
    width = 0.22
    x = np.arange(len(analyzers), dtype=float)
    for idx, mode in enumerate(modes):
        subset = plot_data[plot_data["water_lineage_mode"] == mode].set_index("analyzer_id").reindex(analyzers)
        gap_column = "gap_closed_ratio_capped_0_100" if "gap_closed_ratio_capped_0_100" in subset.columns else "gap_closed_ratio_vs_current_new_chain"
        axes[0, 0].bar(
            x + (idx - 1) * width,
            pd.to_numeric(subset[gap_column], errors="coerce"),
            width=width,
            label=mode,
        )
        axes[1, 0].bar(
            x + (idx - 1) * width,
            pd.to_numeric(subset["delta_vs_none_zero"], errors="coerce"),
            width=width,
            label=mode,
        )
    axes[0, 0].axhline(0.0, color="black", linewidth=0.8)
    axes[0, 0].set_title("Gap Closed Ratio vs Current New Chain (capped)")
    axes[0, 0].set_ylabel("ratio")
    axes[0, 0].grid(alpha=0.2, axis="y")
    axes[0, 0].legend(fontsize=8)

    axes[1, 0].axhline(0.0, color="black", linewidth=0.8)
    axes[1, 0].set_title("Zero RMSE Gain vs None")
    axes[1, 0].set_ylabel("ppm gain")
    axes[1, 0].grid(alpha=0.2, axis="y")
    axes[1, 0].legend(fontsize=8)

    for ax in axes[:, 0]:
        ax.set_xticks(x)
        ax.set_xticklabels(analyzers)
    _finalize(fig, output_path)


def plot_ppm_family_challenge(data: pd.DataFrame, output_path: Path) -> None:
    """Plot fixed-chain ppm family challenge metrics."""

    if data.empty:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.text(0.5, 0.5, "No ppm family challenge data available", ha="center", va="center")
        ax.axis("off")
        return _finalize(fig, output_path)

    families = ["current_fixed_family", "v5_abs_k_minimal", "legacy_humidity_cross_D", "legacy_humidity_cross_E"]
    analyzers = sorted(data["analyzer_id"].dropna().astype(str).unique().tolist())
    fig, axes = plt.subplots(2, 2, figsize=(15, 9), squeeze=False)
    width = 0.18
    x = np.arange(len(analyzers), dtype=float)

    panels = (
        ("overall_rmse", "Overall RMSE"),
        ("zero_rmse", "Zero RMSE"),
        ("delta_vs_current_fixed_family_overall", "Delta vs current family (overall)"),
        ("gap_closed_ratio_vs_current_fixed_family_capped", "Gap closed vs current family (capped)"),
    )
    for ax, (column_name, title) in zip(axes.flatten(), panels, strict=False):
        for idx, family in enumerate(families):
            subset = data[data["ppm_family_mode"] == family].set_index("analyzer_id").reindex(analyzers)
            ax.bar(
                x + (idx - 1.5) * width,
                pd.to_numeric(subset[column_name], errors="coerce"),
                width=width,
                label=family,
            )
        ax.set_xticks(x)
        ax.set_xticklabels(analyzers)
        ax.set_title(title)
        ax.grid(alpha=0.2, axis="y")
    axes[0, 0].legend(fontsize=8)
    _finalize(fig, output_path)


def plot_old_vs_new_comparison(
    aggregate_segments: pd.DataFrame,
    detail: pd.DataFrame,
    output_path: Path,
) -> None:
    """Plot deployable current_deployable_new_chain vs old_chain summary panels."""

    if aggregate_segments.empty and detail.empty:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.text(0.5, 0.5, "No old-vs-new comparison data available", ha="center", va="center")
        ax.axis("off")
        return _finalize(fig, output_path)

    fig, axes = plt.subplots(3, 1, figsize=(12, 12), squeeze=False)

    segment_ax = axes[0, 0]
    if aggregate_segments.empty:
        segment_ax.text(0.5, 0.5, "No aggregate segment comparison available", ha="center", va="center")
        segment_ax.axis("off")
    else:
        ordered = aggregate_segments.set_index("segment_tag").reindex(["overall", "zero", "low", "main"])
        x = np.arange(len(ordered), dtype=float)
        segment_ax.bar(x - 0.18, ordered["old_chain_rmse"], width=0.36, label="old_chain", color="#7f8c8d")
        segment_ax.bar(x + 0.18, ordered["new_chain_rmse"], width=0.36, label="current_deployable_new_chain", color="#2c7fb8")
        segment_ax.set_xticks(x)
        segment_ax.set_xticklabels(["overall", "zero", "low", "main"])
        segment_ax.set_title("Deployable current_deployable_new_chain vs old_chain: overall / zero / low / main")
        segment_ax.set_ylabel("RMSE (ppm)")
        segment_ax.grid(alpha=0.2, axis="y")
        segment_ax.legend(fontsize=8)

    analyzer_ax = axes[1, 0]
    if detail.empty:
        analyzer_ax.text(0.5, 0.5, "No analyzer-level improvement summary available", ha="center", va="center")
        analyzer_ax.axis("off")
    else:
        ranked = detail.sort_values(["improvement_pct_overall", "analyzer_id"], ascending=[False, True], ignore_index=True)
        y = np.arange(len(ranked), dtype=float)
        colors = ["#2ca25f" if float(value) > 0.0 else "#d95f0e" for value in ranked["improvement_pct_overall"].fillna(0.0)]
        analyzer_ax.barh(y, ranked["improvement_pct_overall"], color=colors)
        analyzer_ax.axvline(0.0, color="black", linewidth=0.9)
        analyzer_ax.set_yticks(y)
        analyzer_ax.set_yticklabels(ranked["analyzer_id"].astype(str).tolist())
        analyzer_ax.invert_yaxis()
        analyzer_ax.set_title("Deployable current_deployable_new_chain vs old_chain: analyzer overall improvement_pct")
        analyzer_ax.set_xlabel("Improvement % vs old_chain")
        analyzer_ax.grid(alpha=0.2, axis="x")

    local_ax = axes[2, 0]
    if detail.empty:
        local_ax.text(0.5, 0.5, "No local win/loss counts available", ha="center", va="center")
        local_ax.axis("off")
    else:
        ordered = detail.sort_values(["analyzer_id"], ignore_index=True)
        x = np.arange(len(ordered), dtype=float)
        local_ax.bar(x - 0.18, ordered["pointwise_win_count"], width=0.36, label="local wins", color="#31a354")
        local_ax.bar(x + 0.18, ordered["pointwise_loss_count"], width=0.36, label="local losses", color="#de2d26")
        local_ax.set_xticks(x)
        local_ax.set_xticklabels(ordered["analyzer_id"].astype(str).tolist())
        local_ax.set_title("Deployable current_deployable_new_chain vs old_chain: local win / loss counts")
        local_ax.set_ylabel("Point count")
        local_ax.grid(alpha=0.2, axis="y")
        local_ax.legend(fontsize=8)

    _finalize(fig, output_path)


def plot_source_policy_challenge(summary: pd.DataFrame, detail: pd.DataFrame, output_path: Path) -> None:
    """Plot diagnostic-only source policy challenge summaries."""

    if summary.empty and detail.empty:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.text(0.5, 0.5, "No source policy challenge data available", ha="center", va="center")
        ax.axis("off")
        return _finalize(fig, output_path)

    fig, axes = plt.subplots(3, 1, figsize=(12, 12), squeeze=False)

    summary_ax = axes[0, 0]
    if summary.empty:
        summary_ax.text(0.5, 0.5, "No source policy summary available", ha="center", va="center")
        summary_ax.axis("off")
    else:
        ordered = summary.set_index("source_policy_mode").reindex(
            ["current_deployable_mixed", "raw_first_with_fallback", "raw_only_strict", "filt_only_strict"]
        )
        x = np.arange(len(ordered), dtype=float)
        summary_ax.bar(x, ordered["overall_rmse"], color=["#4d4d4d", "#1f78b4", "#33a02c", "#ff7f00"])
        summary_ax.set_xticks(x)
        summary_ax.set_xticklabels(["current_mixed", "raw_first", "raw_only", "filt_only"], rotation=15, ha="right")
        summary_ax.set_title("Fixed-chain source policy challenge: overall RMSE")
        summary_ax.set_ylabel("RMSE (ppm)")
        summary_ax.grid(alpha=0.2, axis="y")

    delta_ax = axes[1, 0]
    raw_first = detail[detail["source_policy_mode"] == "raw_first_with_fallback"].copy()
    if raw_first.empty:
        delta_ax.text(0.5, 0.5, "No raw-first per-analyzer comparison available", ha="center", va="center")
        delta_ax.axis("off")
    else:
        raw_first = raw_first.sort_values(["delta_vs_current_mixed_overall", "analyzer_id"], ascending=[False, True], ignore_index=True)
        y = np.arange(len(raw_first), dtype=float)
        colors = ["#33a02c" if float(value) > 0.0 else "#e31a1c" for value in raw_first["delta_vs_current_mixed_overall"].fillna(0.0)]
        delta_ax.barh(y, raw_first["delta_vs_current_mixed_overall"], color=colors)
        delta_ax.axvline(0.0, color="black", linewidth=0.9)
        delta_ax.set_yticks(y)
        delta_ax.set_yticklabels(raw_first["analyzer_id"].astype(str).tolist())
        delta_ax.invert_yaxis()
        delta_ax.set_title("raw_first_with_fallback vs current_deployable_mixed")
        delta_ax.set_xlabel("Delta overall RMSE vs current mixed (positive is better)")
        delta_ax.grid(alpha=0.2, axis="x")

    counts_ax = axes[2, 0]
    if summary.empty:
        counts_ax.text(0.5, 0.5, "No source policy counts available", ha="center", va="center")
        counts_ax.axis("off")
    else:
        ordered = summary.set_index("source_policy_mode").reindex(
            ["current_deployable_mixed", "raw_first_with_fallback", "raw_only_strict", "filt_only_strict"]
        )
        x = np.arange(len(ordered), dtype=float)
        counts_ax.bar(x - 0.18, ordered["analyzers_beating_current_mixed_count"], width=0.36, label="beat current mixed", color="#33a02c")
        counts_ax.bar(x + 0.18, ordered["analyzers_beating_old_count"], width=0.36, label="beat old_chain", color="#1f78b4")
        counts_ax.set_xticks(x)
        counts_ax.set_xticklabels(["current_mixed", "raw_first", "raw_only", "filt_only"], rotation=15, ha="right")
        counts_ax.set_title("Source policy challenge: analyzer win counts")
        counts_ax.set_ylabel("Analyzer count")
        counts_ax.grid(alpha=0.2, axis="y")
        counts_ax.legend(fontsize=8)

    _finalize(fig, output_path)


def plot_cross_run_summary(data: pd.DataFrame, output_path: Path) -> None:
    """Plot old-vs-new RMSE across runs for each analyzer."""

    if data.empty:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.text(0.5, 0.5, "No cross-run summary data available", ha="center", va="center")
        ax.axis("off")
        return _finalize(fig, output_path)

    analyzers = sorted(data["analyzer_id"].dropna().unique().tolist())
    fig, axes = plt.subplots(max(len(analyzers), 1), 1, figsize=(11, 3.5 * max(len(analyzers), 1)), squeeze=False)
    for row_idx, analyzer in enumerate(analyzers):
        ax = axes[row_idx, 0]
        run_key = "run_id" if "run_id" in data.columns else "run_name"
        old_key = "old_overall_rmse" if "old_overall_rmse" in data.columns else "old_chain_rmse"
        new_key = "new_overall_rmse" if "new_overall_rmse" in data.columns else "new_chain_rmse"
        subset = data[data["analyzer_id"] == analyzer].sort_values(run_key)
        x = np.arange(len(subset), dtype=float)
        ax.plot(x, subset[old_key], marker="o", label="old_chain_rmse")
        ax.plot(x, subset[new_key], marker="o", label="new_chain_rmse")
        ax.set_xticks(x)
        ax.set_xticklabels(subset[run_key], rotation=20, ha="right")
        ax.set_title(f"{analyzer} cross-run RMSE")
        ax.set_ylabel("RMSE (ppm)")
        ax.grid(alpha=0.2)
        ax.legend(fontsize=8)
    _finalize(fig, output_path)


def plot_merged_zero_anchor_compare(data: pd.DataFrame, output_path: Path) -> None:
    """Plot baseline vs merged-zero-anchor metrics."""

    if data.empty:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.text(0.5, 0.5, "No merged zero-anchor comparison available", ha="center", va="center")
        ax.axis("off")
        return _finalize(fig, output_path)

    analyzers = sorted(data["analyzer_id"].dropna().unique().tolist())
    ordered = data.set_index("analyzer_id").reindex(analyzers)
    x = np.arange(len(analyzers), dtype=float)
    fig, axes = plt.subplots(1, 3, figsize=(15, 4.8), squeeze=False)
    axes[0, 0].bar(x - 0.18, ordered["baseline_new_chain_rmse"], width=0.36, label="baseline")
    axes[0, 0].bar(x + 0.18, ordered["merged_anchor_new_chain_rmse"], width=0.36, label="merged")
    axes[0, 0].plot(x, ordered["old_chain_rmse"], color="black", marker="o", linewidth=1.2, label="old")
    axes[0, 0].set_title("Overall RMSE")
    axes[0, 1].bar(x - 0.18, ordered["baseline_zero_rmse"], width=0.36, label="baseline")
    axes[0, 1].bar(x + 0.18, ordered["merged_zero_rmse"], width=0.36, label="merged")
    axes[0, 1].set_title("Zero RMSE")
    axes[0, 2].bar(x - 0.18, ordered["baseline_high_temp_zero_rmse"], width=0.36, label="baseline")
    axes[0, 2].bar(x + 0.18, ordered["merged_high_temp_zero_rmse"], width=0.36, label="merged")
    axes[0, 2].set_title("40C / 0 ppm zero RMSE")
    for ax in axes[0]:
        ax.set_xticks(x)
        ax.set_xticklabels(analyzers)
        ax.grid(alpha=0.2, axis="y")
        ax.legend(fontsize=8)
    _finalize(fig, output_path)
