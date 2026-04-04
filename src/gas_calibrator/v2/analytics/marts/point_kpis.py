from __future__ import annotations

from collections import defaultdict
from statistics import mean
from typing import Any


def _average(values: list[float | int | None]) -> float | None:
    numbers = [float(item) for item in values if item is not None]
    if not numbers:
        return None
    return mean(numbers)


def build_point_kpis(features: dict[str, Any], **_: Any) -> dict[str, Any]:
    points = list(features.get("points", []))
    completed = sum(1 for item in points if item.get("status") == "completed")
    route_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for point in points:
        route_groups[str(point.get("route_type") or "unknown")].append(point)
    route_breakdown = []
    for route, items in sorted(route_groups.items()):
        route_completed = sum(1 for item in items if item.get("status") == "completed")
        route_breakdown.append(
            {
                "route_type": route,
                "point_count": len(items),
                "completed_point_count": route_completed,
                "point_success_rate": 0.0 if not items else route_completed / len(items),
                "average_total_time_s": _average([item.get("total_time_s") for item in items]),
            }
        )
    return {
        "point_count": len(points),
        "completed_point_count": completed,
        "failed_point_count": sum(1 for item in points if item.get("status") == "failed"),
        "point_success_rate": 0.0 if not points else completed / len(points),
        "average_total_time_s": _average([item.get("total_time_s") for item in points]),
        "average_stability_time_s": _average([item.get("stability_time_s") for item in points]),
        "average_retry_count": _average([item.get("retry_count") for item in points]),
        "route_breakdown": route_breakdown,
    }
