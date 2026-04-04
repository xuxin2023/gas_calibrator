"""Plot exporters for the independent CO2 raw-ratio gas-route leak check."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Mapping, MutableMapping, Sequence


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _safe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def _group_raw_rows(raw_rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    grouped: MutableMapping[int, List[Dict[str, Any]]] = defaultdict(list)
    order: List[int] = []
    for raw_row in raw_rows:
        row = dict(raw_row)
        gas_ppm = _safe_int(row.get("gas_ppm"))
        if gas_ppm is None:
            continue
        if gas_ppm not in grouped:
            order.append(gas_ppm)
        grouped[gas_ppm].append(row)
    return [
        {
            "gas_ppm": gas_ppm,
            "rows": grouped[gas_ppm],
        }
        for gas_ppm in order
    ]


def generate_leak_check_plots(
    output_dir: str | Path,
    *,
    raw_rows: Sequence[Mapping[str, Any]],
    point_summaries: Sequence[Mapping[str, Any]],
    fit_summary: Mapping[str, Any],
) -> Dict[str, Path]:
    import matplotlib

    matplotlib.use("Agg", force=True)
    import matplotlib.pyplot as plt

    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)

    grouped_points = _group_raw_rows(raw_rows)
    ordered_summaries = sorted(
        [dict(row) for row in point_summaries],
        key=lambda row: int(float(row.get("gas_ppm", 0) or 0)),
    )
    summary_by_ppm = {
        int(float(row.get("gas_ppm", 0) or 0)): row for row in ordered_summaries
    }

    plt.style.use("seaborn-v0_8-whitegrid")

    # Overview plot
    overview_path = root / "ratio_overview.png"
    fig, ax = plt.subplots(figsize=(14, 6), dpi=150)
    color_values = plt.cm.viridis(
        [0.1 + 0.8 * idx / max(1, len(grouped_points) - 1) for idx in range(len(grouped_points))]
    )
    cumulative_start = 0.0
    for color, item in zip(color_values, grouped_points):
        gas_ppm = item["gas_ppm"]
        rows = item["rows"]
        point_times: List[float] = []
        ratios: List[float] = []
        for row in rows:
            elapsed_s = _safe_float(row.get("elapsed_s"))
            ratio = _safe_float(row.get("co2_ratio_raw"))
            if elapsed_s is None or ratio is None:
                continue
            point_times.append(cumulative_start + elapsed_s)
            ratios.append(ratio)
        if not point_times:
            continue
        ax.plot(point_times, ratios, color=color, linewidth=1.2, label=f"{gas_ppm} ppm")
        ax.axvline(point_times[0], color=color, linestyle="--", linewidth=0.6, alpha=0.5)
        point_summary = summary_by_ppm.get(gas_ppm)
        stable_mean_ratio = _safe_float(point_summary.get("stable_mean_ratio")) if point_summary else None
        if stable_mean_ratio is not None:
            marker_x = point_times[-1] - 6.0
            ax.scatter([marker_x], [stable_mean_ratio], color="black", s=18, zorder=5)
            ax.text(marker_x, stable_mean_ratio + 0.004, f"{gas_ppm}", fontsize=8, ha="center")
        cumulative_start = point_times[-1] + 1.0
    ax.set_title("GA02 CO2 raw ratio overview")
    ax.set_xlabel("Elapsed time (s)")
    ax.set_ylabel("CO2 raw ratio")
    ax.legend(ncol=3, fontsize=8)
    fig.tight_layout()
    fig.savefig(overview_path)
    plt.close(fig)

    # Stable-mean fit plot
    fit_path = root / "stable_mean_fit.png"
    fig, ax = plt.subplots(figsize=(8, 5), dpi=150)
    xs: List[float] = []
    ys: List[float] = []
    for row in ordered_summaries:
        gas_ppm = _safe_float(row.get("gas_ppm"))
        stable_mean_ratio = _safe_float(row.get("stable_mean_ratio"))
        if gas_ppm is None or stable_mean_ratio is None:
            continue
        xs.append(gas_ppm)
        ys.append(stable_mean_ratio)
    ax.plot(xs, ys, "o-", color="#1f77b4", label="Stable mean ratio")
    slope = _safe_float(fit_summary.get("linear_slope"))
    intercept = _safe_float(fit_summary.get("linear_intercept"))
    linear_r2 = _safe_float(fit_summary.get("linear_r2"))
    if slope is not None and intercept is not None and xs:
        fitted = [intercept + slope * x for x in xs]
        label = "Linear fit"
        if linear_r2 is not None:
            label = f"Linear fit (R2={linear_r2:.4f})"
        ax.plot(xs, fitted, "--", color="#d62728", label=label)
    for x_value, y_value in zip(xs, ys):
        ax.text(x_value, y_value + 0.0035, f"{y_value:.4f}", fontsize=8, ha="center")
    ax.set_title("Stable mean ratio vs gas ppm")
    ax.set_xlabel("Gas ppm")
    ax.set_ylabel("Stable mean CO2 raw ratio")
    ax.legend()
    fig.tight_layout()
    fig.savefig(fit_path)
    plt.close(fig)

    # Transition plots
    combined_transition_path = root / "transition_windows.png"
    transition_detail_paths: Dict[str, Path] = {}
    pairs = [
        (ordered_summaries[index], ordered_summaries[index + 1])
        for index in range(len(ordered_summaries) - 1)
    ]
    if pairs:
        fig, axes = plt.subplots(len(pairs), 1, figsize=(10, 3 * len(pairs)), dpi=150)
        if len(pairs) == 1:
            axes = [axes]
        for axis, (prev_summary, next_summary) in zip(axes, pairs):
            prev_ppm = int(float(prev_summary.get("gas_ppm", 0) or 0))
            next_ppm = int(float(next_summary.get("gas_ppm", 0) or 0))
            prev_rows = next((item["rows"] for item in grouped_points if item["gas_ppm"] == prev_ppm), [])
            next_rows = next((item["rows"] for item in grouped_points if item["gas_ppm"] == next_ppm), [])

            prev_xs: List[float] = []
            prev_ys: List[float] = []
            prev_max_elapsed = 0.0
            for row in prev_rows:
                elapsed_s = _safe_float(row.get("elapsed_s"))
                ratio = _safe_float(row.get("co2_ratio_raw"))
                if elapsed_s is None or ratio is None:
                    continue
                prev_max_elapsed = max(prev_max_elapsed, elapsed_s)
            for row in prev_rows:
                elapsed_s = _safe_float(row.get("elapsed_s"))
                ratio = _safe_float(row.get("co2_ratio_raw"))
                if elapsed_s is None or ratio is None:
                    continue
                if elapsed_s < max(0.0, prev_max_elapsed - 20.0):
                    continue
                prev_xs.append(elapsed_s - prev_max_elapsed)
                prev_ys.append(ratio)

            next_xs: List[float] = []
            next_ys: List[float] = []
            for row in next_rows:
                elapsed_s = _safe_float(row.get("elapsed_s"))
                ratio = _safe_float(row.get("co2_ratio_raw"))
                if elapsed_s is None or ratio is None:
                    continue
                if elapsed_s > 40.0:
                    continue
                next_xs.append(elapsed_s)
                next_ys.append(ratio)

            axis.plot(prev_xs, prev_ys, color="#1f77b4", label=f"{prev_ppm} ppm")
            axis.plot(next_xs, next_ys, color="#d62728", label=f"{next_ppm} ppm")
            axis.axvline(0.0, color="black", linestyle="--", linewidth=0.8)
            axis.set_title(f"Transition {prev_ppm} -> {next_ppm} ppm")
            axis.set_xlabel("Time from switch (s)")
            axis.set_ylabel("CO2 raw ratio")
            axis.legend(fontsize=8, loc="best")

            detail_path = root / f"transition_{prev_ppm}_{next_ppm}.png"
            detail_fig, detail_axis = plt.subplots(figsize=(9, 4), dpi=150)
            detail_axis.plot(prev_xs, prev_ys, color="#1f77b4", label=f"{prev_ppm} ppm")
            detail_axis.plot(next_xs, next_ys, color="#d62728", label=f"{next_ppm} ppm")
            detail_axis.axvline(0.0, color="black", linestyle="--", linewidth=0.8)
            detail_axis.set_title(f"Transition {prev_ppm} -> {next_ppm} ppm")
            detail_axis.set_xlabel("Time from switch (s)")
            detail_axis.set_ylabel("CO2 raw ratio")
            detail_axis.legend(loc="best")
            detail_fig.tight_layout()
            detail_fig.savefig(detail_path)
            plt.close(detail_fig)
            transition_detail_paths[f"{prev_ppm}_{next_ppm}"] = detail_path
        fig.tight_layout()
        fig.savefig(combined_transition_path)
        plt.close(fig)

    return {
        "ratio_overview_plot": overview_path,
        "stable_mean_fit_plot": fit_path,
        "transition_windows_plot": combined_transition_path,
        "transition_detail_plots": transition_detail_paths,
    }
