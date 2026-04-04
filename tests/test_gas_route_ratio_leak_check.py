from __future__ import annotations

from datetime import datetime, timedelta

from gas_calibrator.tools.run_gas_route_ratio_leak_check import (
    _intermediate_open_valves,
    _non_source_open_valves,
)
from gas_calibrator.validation.gas_route_ratio_leak_check import (
    DEFAULT_THRESHOLDS,
    analyze_point_summaries,
    export_leak_check_results,
    summarize_point_rows,
)


def _point_summary(gas_ppm: int, stable_mean_ratio: float, *, stable_std_ratio: float = 0.001, tail_delta_ratio: float = 0.0005):
    return {
        "gas_ppm": gas_ppm,
        "stable_mean_ratio": stable_mean_ratio,
        "stable_std_ratio": stable_std_ratio,
        "tail_delta_ratio": tail_delta_ratio,
    }


def test_summarize_point_rows_calculates_requested_statistics() -> None:
    start = datetime(2026, 4, 1, 8, 0, 0)
    rows = []
    for second in range(120):
        rows.append(
            {
                "timestamp": (start + timedelta(seconds=second)).isoformat(timespec="seconds"),
                "elapsed_s": float(second),
                "co2_ratio_raw": 1.0 + 0.01 * second,
            }
        )

    summary = summarize_point_rows(rows, gas_ppm=400, stable_window_s=30.0, tail_window_s=10.0)

    assert summary["gas_ppm"] == 400
    assert summary["total_samples"] == 120
    assert summary["raw_ratio_first"] == 1.0
    assert summary["raw_ratio_last"] == 1.0 + 0.01 * 119
    assert summary["stable_samples"] == 31
    assert summary["whole_window_slope_ratio_per_s"] == 0.01
    assert summary["stable_window_slope_ratio_per_s"] == 0.01
    assert summary["tail_delta_ratio"] > 0


def test_route_source_close_first_keeps_non_source_valves_open() -> None:
    previous_route = {
        "source_valve": 5,
        "open_logical_valves": [8, 11, 7, 5],
    }
    next_route = {
        "source_valve": 6,
        "open_logical_valves": [8, 11, 7, 6],
    }

    assert _non_source_open_valves(previous_route) == [8, 11, 7]
    assert _non_source_open_valves(next_route) == [8, 11, 7]
    assert _intermediate_open_valves(previous_route, next_route) == [8, 11, 7]


def test_analyze_point_summaries_passes_for_monotonic_linear_response() -> None:
    points = [
        _point_summary(0, 1.60),
        _point_summary(200, 1.48),
        _point_summary(400, 1.36),
        _point_summary(600, 1.24),
        _point_summary(800, 1.12),
        _point_summary(1000, 1.00),
    ]

    result = analyze_point_summaries(points)

    assert result["classification"] == "pass"
    assert result["monotonic_ok"] is True
    assert result["monotonic_direction"] == "decreasing"
    assert result["linear_r2"] >= DEFAULT_THRESHOLDS.linear_r2_pass_min


def test_analyze_point_summaries_fails_on_adjacent_reversal() -> None:
    points = [
        _point_summary(0, 1.60),
        _point_summary(200, 1.48),
        _point_summary(400, 1.36),
        _point_summary(600, 1.40),
        _point_summary(800, 1.12),
        _point_summary(1000, 1.00),
    ]

    result = analyze_point_summaries(points)

    assert result["classification"] == "fail"
    assert result["monotonic_ok"] is False
    assert result["reversal_indices"]


def test_analyze_point_summaries_marks_endpoint_compression() -> None:
    points = [
        _point_summary(0, 1.57),
        _point_summary(200, 1.48),
        _point_summary(400, 1.36),
        _point_summary(600, 1.24),
        _point_summary(800, 1.12),
        _point_summary(1000, 1.03),
    ]

    result = analyze_point_summaries(points)
    endpoint_metric = next(item for item in result["metrics"] if item["name"] == "endpoint_compression")

    assert result["endpoint_compression_detected"] is True
    assert endpoint_metric["status"] in {"warn", "fail"}
    assert "疑似漏气/混气：端点压缩" in result["summary_messages"]


def test_analyze_point_summaries_fails_when_platform_is_unstable() -> None:
    points = [
        _point_summary(0, 1.60, stable_std_ratio=0.001),
        _point_summary(200, 1.48, stable_std_ratio=0.001),
        _point_summary(400, 1.36, stable_std_ratio=0.001),
        _point_summary(600, 1.24, stable_std_ratio=0.050),
        _point_summary(800, 1.12, stable_std_ratio=0.001),
        _point_summary(1000, 1.00, stable_std_ratio=0.001),
    ]

    result = analyze_point_summaries(points)
    std_metric = next(item for item in result["metrics"] if item["name"] == "max_stable_std_ratio_by_span")

    assert result["classification"] == "fail"
    assert std_metric["status"] == "fail"


def test_export_leak_check_results_generates_plot_files(tmp_path) -> None:
    start = datetime(2026, 4, 1, 8, 0, 0)
    raw_rows = []
    point_summaries = []
    for point_index, gas_ppm in enumerate((0, 200, 400, 600, 800, 1000)):
        point_rows = []
        for sample_index in range(60):
            ratio = 1.40 - 0.00024 * gas_ppm - 0.00004 * sample_index
            point_rows.append(
                {
                    "timestamp": (start + timedelta(minutes=point_index * 3, seconds=sample_index)).isoformat(timespec="seconds"),
                    "elapsed_s": float(sample_index),
                    "sample_index": sample_index,
                    "gas_ppm": gas_ppm,
                    "route_group": "A",
                    "source_valve": point_index + 1,
                    "path_valve": 7,
                    "analyzer": "ga02",
                    "co2_ratio_raw": ratio,
                }
            )
        raw_rows.extend(point_rows)
        summary = summarize_point_rows(point_rows, gas_ppm=gas_ppm, stable_window_s=5.0, tail_window_s=10.0)
        summary["analyzer"] = "ga02"
        point_summaries.append(summary)

    fit_summary = analyze_point_summaries(point_summaries)
    outputs = export_leak_check_results(
        tmp_path,
        raw_rows=raw_rows,
        point_summaries=point_summaries,
        fit_summary=fit_summary,
    )

    assert outputs["ratio_overview_plot"].exists()
    assert outputs["stable_mean_fit_plot"].exists()
    assert outputs["transition_windows_plot"].exists()
    assert outputs["transition_detail_plots"]
