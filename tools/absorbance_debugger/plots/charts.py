"""Matplotlib chart helpers."""

from __future__ import annotations

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
