"""PNG plot exporters for metrology-grade seal/pressure qualification diagnostic V2."""

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
        return int(round(float(value)))
    except Exception:
        return None


def _gas_tag(gas_ppm: int) -> str:
    return f"{int(gas_ppm):04d}ppm"


def _variant_tag(value: Any) -> str:
    return str(value or "").strip().upper() or "UNK"


def _repeat_tag(value: Any) -> str:
    numeric = _safe_int(value)
    return f"r{numeric if numeric is not None else 0}"


def _group_rows(rows: Sequence[Mapping[str, Any]], *keys: str) -> Dict[tuple[Any, ...], List[Dict[str, Any]]]:
    grouped: MutableMapping[tuple[Any, ...], List[Dict[str, Any]]] = defaultdict(list)
    for raw_row in rows:
        row = dict(raw_row)
        key = tuple(row.get(name) for name in keys)
        grouped[key].append(row)
    return {key: value for key, value in grouped.items()}


def _ordered(rows: Sequence[Mapping[str, Any]], key: str) -> List[Dict[str, Any]]:
    return sorted(rows, key=lambda row: _safe_float(row.get(key)) or 0.0)


def _collect_variant_pressure_rows(pressure_summaries: Sequence[Mapping[str, Any]], variant: str, gas_ppm: int) -> List[Dict[str, Any]]:
    rows = [
        dict(row)
        for row in pressure_summaries
        if _variant_tag(row.get("process_variant")) == _variant_tag(variant)
        and _safe_int(row.get("gas_ppm")) == int(gas_ppm)
    ]
    return sorted(rows, key=lambda row: -(_safe_float(row.get("pressure_target_hpa")) or 0.0))


def generate_room_temp_diagnostic_plots(
    output_dir: str | Path,
    *,
    raw_rows: Sequence[Mapping[str, Any]],
    flush_summaries: Sequence[Mapping[str, Any]],
    seal_hold_summaries: Sequence[Mapping[str, Any]],
    pressure_summaries: Sequence[Mapping[str, Any]],
    diagnostic_summary: Mapping[str, Any],
) -> Dict[str, Path | Dict[str, Path]]:
    import matplotlib

    matplotlib.use("Agg", force=True)
    import matplotlib.pyplot as plt

    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    plt.style.use("seaborn-v0_8-whitegrid")

    plot_files: Dict[str, Any] = {
        "flush_time_series": {},
        "flush_dewpoint_rh": {},
        "return_to_zero": {},
        "sealed_hold_time_series": {},
        "sealed_hold_drift_summary": {},
        "co2_ratio_vs_pressure": {},
        "co2_ratio_norm_vs_pressure": {},
        "analyzer_vs_gauge_pressure": {},
        "dewpoint_vs_pressure": {},
        "ratio_vs_gasppm_at_pressure": {},
        "endpoint_span_raw_comparison": {},
        "endpoint_span_norm_comparison": {},
        "pressure_error_heatmap": {},
        "variant_comparison_summary": None,
    }

    raw_groups = _group_rows(raw_rows, "process_variant", "phase", "gas_ppm", "repeat_index")
    for (variant, phase, gas_ppm, repeat_index), rows in raw_groups.items():
        variant_tag = _variant_tag(variant)
        gas_value = _safe_int(gas_ppm)
        repeat_tag = _repeat_tag(repeat_index)
        if gas_value is None:
            continue
        times = [_safe_float(row.get("phase_elapsed_s")) for row in rows]
        ratios = [_safe_float(row.get("analyzer2_co2_ratio")) for row in rows]
        gauge = [_safe_float(row.get("gauge_pressure_hpa")) for row in rows]
        dewpoint = [_safe_float(row.get("dewpoint_c")) for row in rows]
        rh = [_safe_float(row.get("dewpoint_rh_percent")) for row in rows]

        if phase == "gas_flush_vent_on":
            fig, ax1 = plt.subplots(figsize=(11, 5), dpi=150)
            valid_ratio = [(x, y) for x, y in zip(times, ratios) if x is not None and y is not None]
            if valid_ratio:
                ax1.plot([x for x, _ in valid_ratio], [y for _, y in valid_ratio], color="#1f77b4", label="CO2 ratio")
            ax1.set_xlabel("Time (s)")
            ax1.set_ylabel("CO2 ratio", color="#1f77b4")
            ax2 = ax1.twinx()
            valid_gauge = [(x, y) for x, y in zip(times, gauge) if x is not None and y is not None]
            if valid_gauge:
                ax2.plot([x for x, _ in valid_gauge], [y for _, y in valid_gauge], color="#d62728", label="Gauge pressure")
            ax2.set_ylabel("Pressure (hPa)")
            ax1.axvspan(0.0, 2.0, color="#bbbbbb", alpha=0.25, label="deadtime")
            ax1.axvline(0.0, color="black", linestyle="--", linewidth=0.8)
            if valid_ratio:
                ax1.axvline(max(item[0] for item in valid_ratio), color="#9467bd", linestyle="--", linewidth=0.8, label="flush end")
            handles, labels = [], []
            for axis in (ax1, ax2):
                one_handles, one_labels = axis.get_legend_handles_labels()
                handles.extend(one_handles)
                labels.extend(one_labels)
            ax1.legend(handles, labels, loc="best", fontsize=8)
            ax1.set_title(f"Layer 1 Flush {variant_tag} {gas_value} ppm {repeat_tag}")
            fig.tight_layout()
            path = root / f"flush_time_series_{variant_tag}_{_gas_tag(gas_value)}_{repeat_tag}.png"
            fig.savefig(path)
            plt.close(fig)
            plot_files["flush_time_series"][f"{variant_tag}_{gas_value}_{repeat_tag}"] = path

            fig, ax1 = plt.subplots(figsize=(11, 5), dpi=150)
            valid_dew = [(x, y) for x, y in zip(times, dewpoint) if x is not None and y is not None]
            if valid_dew:
                ax1.plot([x for x, _ in valid_dew], [y for _, y in valid_dew], color="#ff7f0e", label="Dewpoint")
            ax1.set_xlabel("Time (s)")
            ax1.set_ylabel("Dewpoint (C)", color="#ff7f0e")
            ax2 = ax1.twinx()
            valid_rh = [(x, y) for x, y in zip(times, rh) if x is not None and y is not None]
            if valid_rh:
                ax2.plot([x for x, _ in valid_rh], [y for _, y in valid_rh], color="#2ca02c", label="RH")
            ax2.set_ylabel("RH (%)")
            handles, labels = [], []
            for axis in (ax1, ax2):
                one_handles, one_labels = axis.get_legend_handles_labels()
                handles.extend(one_handles)
                labels.extend(one_labels)
            ax1.legend(handles, labels, loc="best", fontsize=8)
            ax1.set_title(f"Flush Dewpoint/RH {variant_tag} {gas_value} ppm {repeat_tag}")
            fig.tight_layout()
            path = root / f"flush_dewpoint_rh_{variant_tag}_{_gas_tag(gas_value)}_{repeat_tag}.png"
            fig.savefig(path)
            plt.close(fig)
            plot_files["flush_dewpoint_rh"][f"{variant_tag}_{gas_value}_{repeat_tag}"] = path

        if phase == "sealed_hold":
            fig, ax1 = plt.subplots(figsize=(11, 5), dpi=150)
            valid_ratio = [(x, y) for x, y in zip(times, ratios) if x is not None and y is not None]
            if valid_ratio:
                ax1.plot([x for x, _ in valid_ratio], [y for _, y in valid_ratio], color="#1f77b4", label="CO2 ratio")
            ax1.set_xlabel("Time (s)")
            ax1.set_ylabel("CO2 ratio", color="#1f77b4")
            ax2 = ax1.twinx()
            valid_gauge = [(x, y) for x, y in zip(times, gauge) if x is not None and y is not None]
            valid_dew = [(x, y) for x, y in zip(times, dewpoint) if x is not None and y is not None]
            if valid_gauge:
                ax2.plot([x for x, _ in valid_gauge], [y for _, y in valid_gauge], color="#d62728", label="Gauge pressure")
            if valid_dew:
                ax2.plot([x for x, _ in valid_dew], [y for _, y in valid_dew], color="#ff7f0e", linestyle="--", label="Dewpoint")
            ax2.set_ylabel("Pressure / Dewpoint")
            handles, labels = [], []
            for axis in (ax1, ax2):
                one_handles, one_labels = axis.get_legend_handles_labels()
                handles.extend(one_handles)
                labels.extend(one_labels)
            ax1.legend(handles, labels, loc="best", fontsize=8)
            ax1.set_title(f"Sealed Hold {variant_tag} {gas_value} ppm {repeat_tag}")
            fig.tight_layout()
            path = root / f"sealed_hold_time_series_{variant_tag}_{_gas_tag(gas_value)}_{repeat_tag}.png"
            fig.savefig(path)
            plt.close(fig)
            plot_files["sealed_hold_time_series"][f"{variant_tag}_{gas_value}_{repeat_tag}"] = path

    return_rows = []
    for variant_summary in diagnostic_summary.get("variant_summaries", []) or []:
        variant = _variant_tag(variant_summary.get("process_variant"))
        for row in variant_summary.get("return_to_zero", []) or []:
            return_rows.append((variant, dict(row)))
    grouped_return = _group_rows([row for _, row in return_rows], "process_variant")
    for (variant,), rows in grouped_return.items():
        variant_tag = _variant_tag(variant)
        fig, ax = plt.subplots(figsize=(7, 4.5), dpi=150)
        xs = [index + 1 for index in range(len(rows))]
        ys = [_safe_float(row.get("return_to_zero_delta")) for row in rows]
        valid = [(x, y) for x, y in zip(xs, ys) if y is not None]
        if valid:
            ax.plot([x for x, _ in valid], [y for _, y in valid], "o-", color="#9467bd")
        ax.axhline(0.0, color="black", linestyle="--", linewidth=0.8)
        ax.set_xlabel("Cycle")
        ax.set_ylabel("Return-to-zero delta")
        ax.set_title(f"Return to Zero {variant_tag}")
        fig.tight_layout()
        path = root / f"return_to_zero_{variant_tag}_cycle_summary.png"
        fig.savefig(path)
        plt.close(fig)
        plot_files["return_to_zero"][variant_tag] = path

    hold_groups = _group_rows(seal_hold_summaries, "process_variant")
    for (variant,), rows in hold_groups.items():
        variant_tag = _variant_tag(variant)
        fig, ax = plt.subplots(figsize=(7, 4.5), dpi=150)
        ordered_rows = sorted(rows, key=lambda row: (_safe_int(row.get("gas_ppm")) or 0, _safe_int(row.get("repeat_index")) or 0))
        labels = [f"{_safe_int(row.get('gas_ppm'))}ppm-r{_safe_int(row.get('repeat_index'))}" for row in ordered_rows]
        values = [_safe_float(row.get("hold_pressure_drift_hpa_per_min")) for row in ordered_rows]
        ax.bar(range(len(labels)), [value or 0.0 for value in values], color="#d62728")
        ax.set_xticks(range(len(labels)))
        ax.set_xticklabels(labels, rotation=45, ha="right")
        ax.set_ylabel("Pressure drift (hPa/min)")
        ax.set_title(f"Sealed Hold Drift Summary {variant_tag}")
        fig.tight_layout()
        path = root / f"sealed_hold_drift_summary_{variant_tag}.png"
        fig.savefig(path)
        plt.close(fig)
        plot_files["sealed_hold_drift_summary"][variant_tag] = path

    variants = sorted({_variant_tag(row.get("process_variant")) for row in pressure_summaries if _variant_tag(row.get("process_variant"))})
    for variant in variants:
        gas_values = sorted({_safe_int(row.get("gas_ppm")) for row in pressure_summaries if _variant_tag(row.get("process_variant")) == variant and _safe_int(row.get("gas_ppm")) is not None})
        for gas_ppm in gas_values:
            rows = _collect_variant_pressure_rows(pressure_summaries, variant, gas_ppm)
            if not rows:
                continue
            pressures = [_safe_float(row.get("pressure_target_hpa")) for row in rows]
            ratio_mean = [_safe_float(row.get("analyzer2_co2_ratio_mean")) for row in rows]
            ratio_std = [_safe_float(row.get("analyzer2_co2_ratio_std")) or 0.0 for row in rows]
            gauge_mean = [_safe_float(row.get("gauge_pressure_mean")) for row in rows]
            analyzer_mean = [_safe_float(row.get("analyzer2_pressure_mean")) for row in rows]
            dew_mean = [_safe_float(row.get("dewpoint_mean")) for row in rows]

            fig, ax = plt.subplots(figsize=(7, 5), dpi=150)
            valid = [(x, y, e) for x, y, e in zip(pressures, ratio_mean, ratio_std) if x is not None and y is not None]
            if valid:
                ax.errorbar([x for x, _, _ in valid], [y for _, y, _ in valid], yerr=[e for _, _, e in valid], fmt="o-", capsize=4, color="#1f77b4")
            ax.set_xlabel("Pressure (hPa)")
            ax.set_ylabel("CO2 ratio mean")
            ax.set_title(f"CO2 Ratio vs Pressure {variant} {gas_ppm} ppm")
            fig.tight_layout()
            path = root / f"co2_ratio_vs_pressure_{variant}_{_gas_tag(gas_ppm)}.png"
            fig.savefig(path)
            plt.close(fig)
            plot_files["co2_ratio_vs_pressure"][f"{variant}_{gas_ppm}"] = path

            fig, ax = plt.subplots(figsize=(7, 5), dpi=150)
            valid_norm = [(x, y / x, e / x if x not in (None, 0.0) else 0.0) for x, y, e in zip(pressures, ratio_mean, ratio_std) if x not in (None, 0.0) and y is not None]
            if valid_norm:
                ax.errorbar([x for x, _, _ in valid_norm], [y for _, y, _ in valid_norm], yerr=[e for _, _, e in valid_norm], fmt="o-", capsize=4, color="#ff7f0e")
            ax.set_xlabel("Pressure (hPa)")
            ax.set_ylabel("CO2 ratio / pressure")
            ax.set_title(f"Normalized CO2 Ratio vs Pressure {variant} {gas_ppm} ppm")
            fig.tight_layout()
            path = root / f"co2_ratio_norm_vs_pressure_{variant}_{_gas_tag(gas_ppm)}.png"
            fig.savefig(path)
            plt.close(fig)
            plot_files["co2_ratio_norm_vs_pressure"][f"{variant}_{gas_ppm}"] = path

            fig, ax = plt.subplots(figsize=(7, 5), dpi=150)
            valid_gauge = [(x, y) for x, y in zip(pressures, gauge_mean) if x is not None and y is not None]
            valid_an = [(x, y) for x, y in zip(pressures, analyzer_mean) if x is not None and y is not None]
            if valid_gauge:
                ax.plot([x for x, _ in valid_gauge], [y for _, y in valid_gauge], "o-", label="Gauge")
            if valid_an:
                ax.plot([x for x, _ in valid_an], [y for _, y in valid_an], "s--", label="Analyzer pressure")
            ax.set_xlabel("Pressure target (hPa)")
            ax.set_ylabel("Pressure (hPa)")
            ax.legend()
            ax.set_title(f"Analyzer vs Gauge {variant} {gas_ppm} ppm")
            fig.tight_layout()
            path = root / f"analyzer_vs_gauge_pressure_{variant}_{_gas_tag(gas_ppm)}.png"
            fig.savefig(path)
            plt.close(fig)
            plot_files["analyzer_vs_gauge_pressure"][f"{variant}_{gas_ppm}"] = path

            fig, ax = plt.subplots(figsize=(7, 5), dpi=150)
            valid_dew = [(x, y) for x, y in zip(pressures, dew_mean) if x is not None and y is not None]
            if valid_dew:
                ax.plot([x for x, _ in valid_dew], [y for _, y in valid_dew], "o-", color="#2ca02c")
            ax.set_xlabel("Pressure target (hPa)")
            ax.set_ylabel("Dewpoint (C)")
            ax.set_title(f"Dewpoint vs Pressure {variant} {gas_ppm} ppm")
            fig.tight_layout()
            path = root / f"dewpoint_vs_pressure_{variant}_{_gas_tag(gas_ppm)}.png"
            fig.savefig(path)
            plt.close(fig)
            plot_files["dewpoint_vs_pressure"][f"{variant}_{gas_ppm}"] = path

        pressures = sorted({_safe_int(row.get("pressure_target_hpa")) for row in pressure_summaries if _variant_tag(row.get("process_variant")) == variant and _safe_int(row.get("pressure_target_hpa")) is not None}, reverse=True)
        for pressure in pressures:
            rows = sorted(
                [
                    dict(row)
                    for row in pressure_summaries
                    if _variant_tag(row.get("process_variant")) == variant and _safe_int(row.get("pressure_target_hpa")) == pressure
                ],
                key=lambda row: _safe_int(row.get("gas_ppm")) or 0,
            )
            gases = [_safe_float(row.get("gas_ppm")) for row in rows]
            ratios = [_safe_float(row.get("analyzer2_co2_ratio_mean")) for row in rows]
            stds = [_safe_float(row.get("analyzer2_co2_ratio_std")) or 0.0 for row in rows]
            fig, ax = plt.subplots(figsize=(7, 5), dpi=150)
            valid = [(x, y, e) for x, y, e in zip(gases, ratios, stds) if x is not None and y is not None]
            if valid:
                ax.errorbar([x for x, _, _ in valid], [y for _, y, _ in valid], yerr=[e for _, _, e in valid], fmt="o-", capsize=4, color="#2ca02c")
            ax.set_xlabel("Gas ppm")
            ax.set_ylabel("CO2 ratio mean")
            ax.set_title(f"Ratio vs Gas ppm {variant} at {pressure} hPa")
            fig.tight_layout()
            path = root / f"ratio_vs_gasppm_at_{variant}_{int(pressure)}hpa.png"
            fig.savefig(path)
            plt.close(fig)
            plot_files["ratio_vs_gasppm_at_pressure"][f"{variant}_{pressure}"] = path

        endpoint_rows = []
        for variant_summary in diagnostic_summary.get("variant_summaries", []) or []:
            if _variant_tag(variant_summary.get("process_variant")) != variant:
                continue
            for metric in variant_summary.get("metrics", []) or []:
                if metric.get("name") == "normalized_endpoint_span_retention_check":
                    endpoint_rows = metric.get("value") or []
                    break
        if endpoint_rows:
            pressures = [_safe_float(row.get("pressure_target_hpa")) for row in endpoint_rows]
            raw_span = [_safe_float(row.get("endpoint_span_raw")) for row in endpoint_rows]
            norm_span = [_safe_float(row.get("endpoint_span_norm")) for row in endpoint_rows]

            fig, ax = plt.subplots(figsize=(7, 5), dpi=150)
            valid = [(x, y) for x, y in zip(pressures, raw_span) if x is not None and y is not None]
            if valid:
                ax.plot([x for x, _ in valid], [y for _, y in valid], "o-", color="#1f77b4")
            ax.set_xlabel("Pressure (hPa)")
            ax.set_ylabel("Endpoint span raw")
            ax.set_title(f"Endpoint Span Raw {variant}")
            fig.tight_layout()
            path = root / f"endpoint_span_raw_comparison_{variant}.png"
            fig.savefig(path)
            plt.close(fig)
            plot_files["endpoint_span_raw_comparison"][variant] = path

            fig, ax = plt.subplots(figsize=(7, 5), dpi=150)
            valid = [(x, y) for x, y in zip(pressures, norm_span) if x is not None and y is not None]
            if valid:
                ax.plot([x for x, _ in valid], [y for _, y in valid], "o-", color="#ff7f0e")
            ax.set_xlabel("Pressure (hPa)")
            ax.set_ylabel("Endpoint span norm")
            ax.set_title(f"Endpoint Span Norm {variant}")
            fig.tight_layout()
            path = root / f"endpoint_span_norm_comparison_{variant}.png"
            fig.savefig(path)
            plt.close(fig)
            plot_files["endpoint_span_norm_comparison"][variant] = path

        gas_values = sorted({_safe_int(row.get("gas_ppm")) for row in pressure_summaries if _variant_tag(row.get("process_variant")) == variant and _safe_int(row.get("gas_ppm")) is not None})
        pressure_values = sorted({_safe_int(row.get("pressure_target_hpa")) for row in pressure_summaries if _variant_tag(row.get("process_variant")) == variant and _safe_int(row.get("pressure_target_hpa")) is not None}, reverse=True)
        if gas_values and pressure_values:
            heatmap: List[List[float]] = []
            for gas_ppm in gas_values:
                row_values: List[float] = []
                lookup = {
                    (_safe_int(item.get("gas_ppm")), _safe_int(item.get("pressure_target_hpa"))): item
                    for item in pressure_summaries
                    if _variant_tag(item.get("process_variant")) == variant
                }
                for pressure in pressure_values:
                    value = _safe_float(lookup.get((gas_ppm, pressure), {}).get("pressure_tracking_error"))
                    row_values.append(value if value is not None else float("nan"))
                heatmap.append(row_values)
            fig, ax = plt.subplots(figsize=(8, 4.5), dpi=150)
            image = ax.imshow(heatmap, aspect="auto", cmap="coolwarm")
            ax.set_xticks(range(len(pressure_values)))
            ax.set_xticklabels([str(item) for item in pressure_values])
            ax.set_yticks(range(len(gas_values)))
            ax.set_yticklabels([str(item) for item in gas_values])
            ax.set_xlabel("Pressure target (hPa)")
            ax.set_ylabel("Gas ppm")
            ax.set_title(f"Pressure Error Heatmap {variant}")
            fig.colorbar(image, ax=ax, label="Tracking error (hPa)")
            fig.tight_layout()
            path = root / f"pressure_error_heatmap_{variant}.png"
            fig.savefig(path)
            plt.close(fig)
            plot_files["pressure_error_heatmap"][variant] = path

    variant_rows = diagnostic_summary.get("variant_summaries", []) or []
    if variant_rows:
        fig, ax = plt.subplots(figsize=(8, 4.5), dpi=150)
        status_rank = {"pass": 3, "warn": 2, "insufficient_evidence": 1, "fail": 0}
        names = [_variant_tag(row.get("process_variant")) for row in variant_rows]
        scores = [status_rank.get(str(row.get("classification") or ""), 0) for row in variant_rows]
        ax.bar(names, scores, color=["#2ca02c" if score == 3 else "#ff7f0e" if score == 2 else "#d62728" for score in scores])
        ax.set_ylim(-0.2, 3.5)
        ax.set_ylabel("Qualification score")
        ax.set_title("Variant Comparison Summary")
        fig.tight_layout()
        path = root / "variant_comparison_summary.png"
        fig.savefig(path)
        plt.close(fig)
        plot_files["variant_comparison_summary"] = path

    return plot_files


def generate_analyzer_chain_isolation_plots(
    output_dir: str | Path,
    *,
    raw_rows: Sequence[Mapping[str, Any]],
    flush_gate_trace_rows: Sequence[Mapping[str, Any]],
    isolation_summaries: Sequence[Mapping[str, Any]],
    comparison_summary: Mapping[str, Any],
) -> Dict[str, Path]:
    import matplotlib

    matplotlib.use("Agg", force=True)
    import matplotlib.pyplot as plt

    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    plt.style.use("seaborn-v0_8-whitegrid")

    def placeholder(path: Path, title: str, message: str) -> Path:
        fig, ax = plt.subplots(figsize=(8, 4.5), dpi=150)
        ax.axis("off")
        ax.text(0.5, 0.5, message, ha="center", va="center", wrap=True)
        ax.set_title(title)
        fig.tight_layout()
        fig.savefig(path)
        plt.close(fig)
        return path

    outputs: Dict[str, Path] = {}
    mode_rows = _group_rows(raw_rows, "chain_mode")
    trace_rows = _group_rows(flush_gate_trace_rows, "chain_mode")
    summary_map = {str(row.get("chain_mode")): dict(row) for row in isolation_summaries}

    for mode in ("analyzer_out_keep_rest", "analyzer_in_keep_rest"):
        rows = sorted(mode_rows.get((mode,), []), key=lambda row: _safe_float(row.get("phase_elapsed_s")) or 0.0)
        path = root / f"dewpoint_time_series_{mode}.png"
        if not rows:
            outputs[f"dewpoint_time_series_{mode}"] = placeholder(path, f"Dewpoint Time Series {mode}", f"No data for {mode}.")
            continue
        times = [_safe_float(row.get("phase_elapsed_s")) for row in rows]
        dewpoint = [_safe_float(row.get("dewpoint_c")) for row in rows]
        fig, ax = plt.subplots(figsize=(9, 4.8), dpi=150)
        valid = [(x, y) for x, y in zip(times, dewpoint) if x is not None and y is not None]
        if valid:
            ax.plot([x for x, _ in valid], [y for _, y in valid], color="#ff7f0e", linewidth=1.5)
        ax.set_xlabel("Time (s)")
        ax.set_ylabel("Dewpoint (C)")
        ax.set_title(f"Dewpoint Time Series {mode}")
        fig.tight_layout()
        fig.savefig(path)
        plt.close(fig)
        outputs[f"dewpoint_time_series_{mode}"] = path

    overlay_path = root / "flush_gate_trace_overlay.png"
    fig, ax = plt.subplots(figsize=(9, 4.8), dpi=150)
    colors = {
        "analyzer_out_keep_rest": "#1f77b4",
        "analyzer_in_keep_rest": "#d62728",
    }
    any_trace = False
    for mode in ("analyzer_out_keep_rest", "analyzer_in_keep_rest"):
        rows = sorted(trace_rows.get((mode,), []), key=lambda row: _safe_float(row.get("elapsed_s_real")) or 0.0)
        valid = [
            (_safe_float(row.get("elapsed_s_real")), _safe_float(row.get("dewpoint_span_window_c")))
            for row in rows
            if _safe_float(row.get("elapsed_s_real")) is not None and _safe_float(row.get("dewpoint_span_window_c")) is not None
        ]
        if valid:
            any_trace = True
            ax.plot([x for x, _ in valid], [y for _, y in valid], label=mode, color=colors[mode])
    ax.set_xlabel("Elapsed real (s)")
    ax.set_ylabel("Dewpoint span window (C)")
    ax.set_title("Flush Gate Trace Overlay")
    if any_trace:
        ax.legend()
    else:
        ax.text(0.5, 0.5, "No flush gate trace rows available.", ha="center", va="center", transform=ax.transAxes)
    fig.tight_layout()
    fig.savefig(overlay_path)
    plt.close(fig)
    outputs["flush_gate_trace_overlay"] = overlay_path

    rebound_path = root / "rebound_overlay.png"
    fig, ax = plt.subplots(figsize=(9, 4.8), dpi=150)
    any_rebound = False
    for mode in ("analyzer_out_keep_rest", "analyzer_in_keep_rest"):
        rows = sorted(mode_rows.get((mode,), []), key=lambda row: _safe_float(row.get("phase_elapsed_s")) or 0.0)
        valid = [
            (_safe_float(row.get("phase_elapsed_s")), _safe_float(row.get("dewpoint_c")))
            for row in rows
            if _safe_float(row.get("phase_elapsed_s")) is not None and _safe_float(row.get("dewpoint_c")) is not None
        ]
        if valid:
            any_rebound = True
            ax.plot([x for x, _ in valid], [y for _, y in valid], label=mode, color=colors[mode], alpha=0.85)
        summary = summary_map.get(mode, {})
        rebound_rise = _safe_float(summary.get("rebound_rise_c"))
        if rebound_rise is not None:
            ax.text(0.02, 0.95 if mode.endswith("out_keep_rest") else 0.88, f"{mode}: rebound={rebound_rise:.3f} C", transform=ax.transAxes, color=colors[mode])
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Dewpoint (C)")
    ax.set_title("Rebound Overlay")
    if any_rebound:
        ax.legend()
    else:
        ax.text(0.5, 0.5, "No dewpoint series available.", ha="center", va="center", transform=ax.transAxes)
    fig.tight_layout()
    fig.savefig(rebound_path)
    plt.close(fig)
    outputs["rebound_overlay"] = rebound_path

    gauge_path = root / "gauge_time_series_comparison.png"
    fig, ax = plt.subplots(figsize=(9, 4.8), dpi=150)
    any_gauge = False
    for mode in ("analyzer_out_keep_rest", "analyzer_in_keep_rest"):
        rows = sorted(mode_rows.get((mode,), []), key=lambda row: _safe_float(row.get("phase_elapsed_s")) or 0.0)
        valid = [
            (_safe_float(row.get("phase_elapsed_s")), _safe_float(row.get("gauge_pressure_hpa")))
            for row in rows
            if _safe_float(row.get("phase_elapsed_s")) is not None and _safe_float(row.get("gauge_pressure_hpa")) is not None
        ]
        if valid:
            any_gauge = True
            ax.plot([x for x, _ in valid], [y for _, y in valid], label=mode, color=colors[mode])
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Gauge pressure (hPa)")
    ax.set_title("Gauge Time Series Comparison")
    if any_gauge:
        ax.legend()
    else:
        ax.text(0.5, 0.5, "No gauge series available.", ha="center", va="center", transform=ax.transAxes)
    fig.tight_layout()
    fig.savefig(gauge_path)
    plt.close(fig)
    outputs["gauge_time_series_comparison"] = gauge_path

    comparison_path = root / "chain_mode_comparison_summary.png"
    fig, axes = plt.subplots(2, 2, figsize=(10, 7), dpi=150)
    metric_specs = [
        ("dewpoint_tail_span_60s", "Tail span (C)"),
        ("dewpoint_tail_slope_60s", "Tail slope (C/s)"),
        ("rebound_rise_c", "Rebound rise (C)"),
        ("dewpoint_time_to_gate", "Time to gate (s)"),
    ]
    for ax, (key, label) in zip(axes.flatten(), metric_specs):
        values = []
        labels = []
        for mode in ("analyzer_out_keep_rest", "analyzer_in_keep_rest"):
            labels.append(mode.replace("_keep_rest", ""))
            value = _safe_float(summary_map.get(mode, {}).get(key))
            values.append(0.0 if value is None else value)
        ax.bar(labels, values, color=[colors["analyzer_out_keep_rest"], colors["analyzer_in_keep_rest"]])
        ax.set_title(label)
    fig.suptitle(
        f"Chain Mode Comparison Summary\nconclusion={comparison_summary.get('dominant_isolation_conclusion')}",
        fontsize=11,
    )
    fig.tight_layout()
    fig.savefig(comparison_path)
    plt.close(fig)
    outputs["chain_mode_comparison_summary"] = comparison_path

    return outputs


def generate_compare_vs_8ch_time_to_gate_plot(
    output_dir: str | Path,
    *,
    compare_vs_8ch_rows: Sequence[Mapping[str, Any]],
) -> Optional[Path]:
    import matplotlib

    matplotlib.use("Agg", force=True)
    import matplotlib.pyplot as plt

    rows = [dict(row) for row in compare_vs_8ch_rows if str(row.get("case") or "").strip()]
    if not rows:
        return None

    cases: List[str] = []
    current_values: List[float] = []
    baseline_values: List[float] = []
    for row in rows:
        current_value = _safe_float(row.get("current_time_to_gate_s"))
        baseline_value = _safe_float(row.get("baseline_time_to_gate_s_8ch"))
        if current_value is None and baseline_value is None:
            continue
        cases.append(str(row.get("case")))
        current_values.append(0.0 if current_value is None else current_value)
        baseline_values.append(0.0 if baseline_value is None else baseline_value)

    if not cases:
        return None

    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(8.5, 4.8), dpi=150)
    indices = list(range(len(cases)))
    width = 0.36
    ax.bar([index - width / 2 for index in indices], current_values, width=width, label="4ch", color="#1f77b4")
    ax.bar([index + width / 2 for index in indices], baseline_values, width=width, label="8ch baseline", color="#d62728")
    ax.set_xticks(indices, cases)
    ax.set_ylabel("Time to gate / timeout duration (s)")
    ax.set_title("4ch vs 8ch Time to Gate")
    ax.legend()
    fig.tight_layout()
    path = root / "compare_vs_8ch_time_to_gate.png"
    fig.savefig(path)
    plt.close(fig)
    return path
