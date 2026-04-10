"""Pressure-lineage assessment for the offline debugger."""

from __future__ import annotations

import math
from typing import Any

import pandas as pd


def _safe_float(value: Any) -> float:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return float(numeric) if pd.notna(numeric) else math.nan


def _median_abs(frame: pd.DataFrame, column: str) -> float:
    if column not in frame.columns or frame.empty:
        return math.nan
    values = pd.to_numeric(frame[column], errors="coerce").dropna().abs()
    if values.empty:
        return math.nan
    return float(values.median())


def _pressure_lineage_judgment(pressure_coeffs: pd.DataFrame) -> str:
    if pressure_coeffs.empty:
        return "3) unable to confirm from current data"
    raw_bias = _median_abs(pressure_coeffs, "raw_mean_diff_hpa")
    corr_bias = _median_abs(pressure_coeffs, "corr_mean_diff_hpa")
    raw_rmse = _median_abs(pressure_coeffs, "raw_rmse_hpa")
    corr_rmse = _median_abs(pressure_coeffs, "corr_rmse_hpa")
    if math.isfinite(raw_bias) and math.isfinite(corr_bias) and raw_bias >= corr_bias + 5.0:
        return "1) analyzer pressure field looks like raw or pre-calibration pressure"
    if math.isfinite(raw_rmse) and math.isfinite(corr_rmse) and raw_rmse > corr_rmse:
        return "2) analyzer pressure field may already contain partial calibration but still needs offline offset correction"
    return "3) unable to confirm from current data"


def _pressure_primary_failure(overview_summary: pd.DataFrame) -> bool:
    if overview_summary.empty or "winner_overall" not in overview_summary.columns:
        return False
    winners = set(overview_summary["winner_overall"].dropna().astype(str).tolist())
    if "new_chain" in winners:
        return False
    gap = pd.to_numeric(overview_summary.get("new_chain_rmse"), errors="coerce") - pd.to_numeric(
        overview_summary.get("old_chain_rmse"),
        errors="coerce",
    )
    return bool(gap.dropna().gt(5.0).all()) if not gap.dropna().empty else False


def build_pressure_data_assessment(
    pressure_coeffs: pd.DataFrame,
    overview_summary: pd.DataFrame,
) -> pd.DataFrame:
    """Summarize what the current pressure fields most likely represent."""

    overall_judgment = _pressure_lineage_judgment(pressure_coeffs)
    primary_failure = _pressure_primary_failure(overview_summary)
    rows: list[dict[str, Any]] = [
        {
            "assessment_scope": "overall",
            "analyzer_id": "",
            "pressure_field_lineage_judgment": overall_judgment,
            "pressure_corr_is_offset_only": True,
            "raw_mean_diff_hpa": _median_abs(pressure_coeffs, "raw_mean_diff_hpa"),
            "raw_rmse_hpa": _median_abs(pressure_coeffs, "raw_rmse_hpa"),
            "corr_mean_diff_hpa": _median_abs(pressure_coeffs, "corr_mean_diff_hpa"),
            "corr_rmse_hpa": _median_abs(pressure_coeffs, "corr_rmse_hpa"),
            "pressure_may_contribute_error": bool(
                math.isfinite(_median_abs(pressure_coeffs, "corr_rmse_hpa"))
                and _median_abs(pressure_coeffs, "corr_rmse_hpa") > 5.0
            ),
            "pressure_is_primary_failure": primary_failure,
            "assessment_note": (
                "pressure_corr_hpa is explicitly offset-only in the debugger. It improves agreement with pressure_std_hpa, "
                "but the remaining residual is still material. Current evidence suggests pressure contributes error but is not "
                "the main reason zero/temp stability still trails old_chain."
            ),
        }
    ]

    for row in pressure_coeffs.to_dict(orient="records"):
        raw_bias = abs(_safe_float(row.get("raw_mean_diff_hpa")))
        corr_bias = abs(_safe_float(row.get("corr_mean_diff_hpa")))
        lineage = "1) analyzer pressure field looks like raw or pre-calibration pressure"
        if math.isfinite(raw_bias) and math.isfinite(corr_bias) and raw_bias <= corr_bias + 3.0:
            lineage = "2) analyzer pressure field may already include partial correction"
        rows.append(
            {
                "assessment_scope": "analyzer",
                "analyzer_id": row.get("analyzer", ""),
                "pressure_field_lineage_judgment": lineage,
                "pressure_corr_is_offset_only": True,
                "raw_mean_diff_hpa": _safe_float(row.get("raw_mean_diff_hpa")),
                "raw_rmse_hpa": _safe_float(row.get("raw_rmse_hpa")),
                "corr_mean_diff_hpa": _safe_float(row.get("corr_mean_diff_hpa")),
                "corr_rmse_hpa": _safe_float(row.get("corr_rmse_hpa")),
                "pressure_may_contribute_error": bool(
                    math.isfinite(_safe_float(row.get("corr_rmse_hpa")))
                    and _safe_float(row.get("corr_rmse_hpa")) > 5.0
                ),
                "pressure_is_primary_failure": primary_failure,
                "assessment_note": (
                    "Offset correction clearly helps this analyzer, which is more consistent with a raw/pre-calibration "
                    "device pressure field than a final standard pressure field."
                    if lineage.startswith("1)")
                    else "The raw device pressure does not behave like a perfect standard pressure reference."
                ),
            }
        )

    return pd.DataFrame(rows)
