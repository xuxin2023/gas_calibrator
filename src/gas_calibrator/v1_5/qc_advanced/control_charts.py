"""Control-chart helpers for historical V1.5 QC trends."""

from __future__ import annotations

from typing import Any, Dict, List, Sequence

from ._math import mean, safe_float, stddev


def build_control_chart(values: Sequence[Any]) -> Dict[str, Any]:
    data = [number for number in (safe_float(value) for value in values) if number is not None]
    avg = mean(data)
    sigma = stddev(data)
    if avg is None or sigma is None:
        return {"status": "fail", "reason": "no_numeric_history", "points": []}
    points: List[Dict[str, Any]] = []
    for index, value in enumerate(data):
        z = 0.0 if sigma == 0 else (value - avg) / sigma
        violation = ""
        if abs(z) > 3:
            violation = "beyond_3_sigma"
        elif abs(z) > 2:
            violation = "beyond_2_sigma"
        points.append({"index": index, "value": value, "z_score": z, "violation": violation})
    return {
        "status": "pass" if not any(point["violation"] == "beyond_3_sigma" for point in points) else "review",
        "mean": avg,
        "sigma": sigma,
        "ucl_2sigma": avg + 2 * sigma,
        "lcl_2sigma": avg - 2 * sigma,
        "ucl_3sigma": avg + 3 * sigma,
        "lcl_3sigma": avg - 3 * sigma,
        "points": points,
    }
