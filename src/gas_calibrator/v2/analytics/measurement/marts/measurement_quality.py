from __future__ import annotations

from statistics import mean
from typing import Any


def _average(values: list[float | int | None]) -> float | None:
    numbers = [float(item) for item in values if item is not None]
    if not numbers:
        return None
    return mean(numbers)


def _quality_score(usable_rate: float, has_data_rate: float, coverage_ratio: float | None) -> float:
    coverage = usable_rate if coverage_ratio is None else float(coverage_ratio)
    return round(100.0 * ((0.55 * usable_rate) + (0.25 * has_data_rate) + (0.20 * coverage)), 2)


def build_measurement_quality(features: dict[str, Any], **_: Any) -> dict[str, Any]:
    runs = list(features.get("run_features", []))
    analyzers = list(features.get("analyzer_features", []))
    frames = list(features.get("frame_features", []))

    frame_count = len(frames)
    usable_frame_count = sum(1 for frame in frames if frame.get("frame_usable"))
    has_data_frame_count = sum(1 for frame in frames if frame.get("frame_has_data"))
    usable_rate = 0.0 if frame_count == 0 else usable_frame_count / frame_count
    has_data_rate = 0.0 if frame_count == 0 else has_data_frame_count / frame_count
    overall_coverage_ratio = _average([item.get("mean_coverage_ratio") for item in analyzers])

    run_breakdown = []
    for item in runs:
        run_breakdown.append(
            {
                "run_id": item.get("run_id"),
                "status": item.get("status"),
                "frame_count": item.get("frame_count", 0),
                "usable_frame_count": item.get("usable_frame_count", 0),
                "frame_has_data_count": item.get("frame_has_data_count", 0),
                "frame_usable_rate": item.get("frame_usable_rate", 0.0),
                "frame_has_data_rate": item.get("frame_has_data_rate", 0.0),
                "mean_coverage_ratio": item.get("mean_coverage_ratio"),
                "quality_score": _quality_score(
                    float(item.get("frame_usable_rate") or 0.0),
                    float(item.get("frame_has_data_rate") or 0.0),
                    item.get("mean_coverage_ratio"),
                ),
            }
        )

    analyzer_breakdown = []
    for item in analyzers:
        analyzer_breakdown.append(
            {
                "analyzer_label": item.get("analyzer_label"),
                "run_count": item.get("run_count", 0),
                "frame_count": item.get("frame_count", 0),
                "usable_frame_count": item.get("usable_frame_count", 0),
                "missing_frame_count": item.get("missing_frame_count", 0),
                "usable_rate": item.get("usable_rate", 0.0),
                "has_data_rate": item.get("has_data_rate", 0.0),
                "mean_coverage_ratio": item.get("mean_coverage_ratio"),
                "quality_score": _quality_score(
                    float(item.get("usable_rate") or 0.0),
                    float(item.get("has_data_rate") or 0.0),
                    item.get("mean_coverage_ratio"),
                ),
            }
        )

    return {
        "run_count": len(runs),
        "analyzer_count": len(analyzers),
        "frame_count": frame_count,
        "usable_frame_count": usable_frame_count,
        "has_data_frame_count": has_data_frame_count,
        "usable_rate": usable_rate,
        "has_data_rate": has_data_rate,
        "overall_coverage_ratio": overall_coverage_ratio,
        "overall_quality_score": _quality_score(usable_rate, has_data_rate, overall_coverage_ratio),
        "run_breakdown": run_breakdown,
        "analyzer_breakdown": analyzer_breakdown,
    }
