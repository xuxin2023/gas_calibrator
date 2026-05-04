from __future__ import annotations

from typing import Any, Mapping

import pandas as pd

DEFAULT_RATIO_UNIQUE_COUNT_FAIL_THRESHOLD = 3
DEFAULT_RATIO_SPAN_FAIL_THRESHOLD = 5e-3
DEFAULT_TARGET_GROUP_MEAN_SPAN_WARN_THRESHOLD = 5e-3


def evaluate_ratio_fit_input_quality(
    frame: pd.DataFrame,
    *,
    target_key: str,
    ratio_key: str,
    qc_cfg: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    cfg = dict(qc_cfg or {})
    unique_count_fail_threshold = int(
        cfg.get("fit_input_qc_ratio_unique_count_fail_threshold", DEFAULT_RATIO_UNIQUE_COUNT_FAIL_THRESHOLD) or 0
    )
    ratio_span_fail_threshold = float(
        cfg.get("fit_input_qc_ratio_span_fail_threshold", DEFAULT_RATIO_SPAN_FAIL_THRESHOLD) or 0.0
    )
    target_group_span_warn_threshold = float(
        cfg.get(
            "fit_input_qc_target_group_mean_span_warn_threshold",
            DEFAULT_TARGET_GROUP_MEAN_SPAN_WARN_THRESHOLD,
        )
        or 0.0
    )

    working = frame.copy()
    working["_target_num"] = pd.to_numeric(working.get(target_key), errors="coerce")
    working["_ratio_num"] = pd.to_numeric(working.get(ratio_key), errors="coerce")
    working = working.dropna(subset=["_target_num", "_ratio_num"]).copy()

    if working.empty:
        return {
            "status": "fail",
            "warnings": ["fit_input_empty"],
            "warning_text": "fit_input_empty",
            "ratio_span": 0.0,
            "ratio_unique_count": 0,
            "target_group_count": 0,
            "target_group_mean_span": 0.0,
            "valid_row_count": 0,
            "delivery_recommendation": "禁止下发",
            "delivery_recommendation_code": "forbid_download",
        }

    ratio = working["_ratio_num"]
    target = working["_target_num"]
    ratio_unique_count = int(ratio.round(12).nunique())
    ratio_span = float(ratio.max() - ratio.min())
    grouped_ratio = working.groupby(target.round(9))["_ratio_num"].mean()
    target_group_count = int(len(grouped_ratio))
    target_group_mean_span = float(grouped_ratio.max() - grouped_ratio.min()) if not grouped_ratio.empty else 0.0

    warnings: list[str] = []
    status = "ok"
    if ratio_unique_count <= unique_count_fail_threshold:
        warnings.append("ratio_unique_count_too_low")
        status = "fail"
    if ratio_span <= ratio_span_fail_threshold:
        warnings.append("ratio_span_too_small")
        status = "fail"
    if target_group_count >= 3 and target_group_mean_span <= target_group_span_warn_threshold:
        warnings.append("ratio_target_trend_too_flat")
        if status != "fail":
            status = "warn"

    if status == "fail":
        delivery_recommendation = "禁止下发"
        delivery_recommendation_code = "forbid_download"
    elif status == "warn":
        delivery_recommendation = "仅供诊断"
        delivery_recommendation_code = "diagnostic_only"
    else:
        delivery_recommendation = "可下发"
        delivery_recommendation_code = "ok"

    return {
        "status": status,
        "warnings": warnings,
        "warning_text": "; ".join(warnings),
        "ratio_span": ratio_span,
        "ratio_unique_count": ratio_unique_count,
        "target_group_count": target_group_count,
        "target_group_mean_span": target_group_mean_span,
        "valid_row_count": int(len(working)),
        "delivery_recommendation": delivery_recommendation,
        "delivery_recommendation_code": delivery_recommendation_code,
    }
