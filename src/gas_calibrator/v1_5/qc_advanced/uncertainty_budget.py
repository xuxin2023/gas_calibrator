"""Input-quantity uncertainty budget helper for V1.5 reports."""

from __future__ import annotations

import math
from typing import Any, Dict, List, Mapping, Sequence

from ._math import safe_float


def build_uncertainty_budget(
    inputs: Sequence[Mapping[str, Any]],
    *,
    coverage_factor: float = 2.0,
) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    missing: List[str] = []
    sum_sq = 0.0
    for item in inputs:
        name = str(item.get("input_quantity") or item.get("source") or "")
        standard = safe_float(item.get("standard_uncertainty"))
        sensitivity = safe_float(item.get("sensitivity_coefficient", 1.0))
        if standard is None or sensitivity is None:
            missing.append(name or "unnamed_input")
            contribution = None
        else:
            contribution = standard * sensitivity
            sum_sq += contribution * contribution
        rows.append(
            {
                "input_quantity": name,
                "distribution": str(item.get("distribution") or "normal"),
                "standard_uncertainty": standard,
                "sensitivity_coefficient": sensitivity,
                "contribution": contribution,
                "evidence_source": str(item.get("evidence_source") or ""),
            }
        )
    combined = math.sqrt(sum_sq) if rows and not missing else None
    return {
        "status": "released" if combined is not None else "not_released",
        "coverage_factor": coverage_factor,
        "combined_standard_uncertainty": combined,
        "expanded_uncertainty": None if combined is None else combined * coverage_factor,
        "missing_inputs": missing,
        "uncertainty_budget_table": rows,
    }
