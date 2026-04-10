"""Single-run role assessment helpers for historical absorbance packages."""

from __future__ import annotations

from typing import Any

import pandas as pd


def build_run_role_assessment(
    *,
    run_name: str,
    points: pd.DataFrame,
    filtered_valid: pd.DataFrame,
    overview_summary: pd.DataFrame,
    invalid_pressure_summary: pd.DataFrame,
    analyzer_scope: pd.DataFrame,
) -> tuple[pd.DataFrame, str]:
    """Assess how one run should be used in the offline challenge workflow."""

    co2_points = points[points["stage"].astype(str).str.lower() == "co2"].copy()
    ambient_40c_zero = co2_points[
        (pd.to_numeric(co2_points["temp_set_c"], errors="coerce") == 40.0)
        & (pd.to_numeric(co2_points["target_co2_ppm"], errors="coerce") == 0.0)
        & (pd.to_numeric(co2_points["target_pressure_hpa"], errors="coerce").isna())
    ].copy()
    has_high_temp_zero_anchor_candidate = not ambient_40c_zero.empty

    overall_invalid = invalid_pressure_summary[invalid_pressure_summary["summary_scope"] == "overall"].copy()
    invalid_point_count = int(pd.to_numeric(overall_invalid.iloc[0]["invalid_point_count"], errors="coerce")) if not overall_invalid.empty else 0
    invalid_sample_count = int(pd.to_numeric(overall_invalid.iloc[0]["invalid_sample_count"], errors="coerce")) if not overall_invalid.empty else 0
    valid_only_sample_count = int(len(filtered_valid))
    valid_only_point_count = int(filtered_valid[["point_title", "point_row"]].drop_duplicates().shape[0]) if not filtered_valid.empty else 0
    valid_only_co2_point_count = int(
        filtered_valid[filtered_valid["route"].astype(str).str.lower() == "co2"][["point_title", "point_row"]]
        .drop_duplicates()
        .shape[0]
    ) if not filtered_valid.empty else 0

    overview = overview_summary.copy() if overview_summary is not None else pd.DataFrame()
    winners = overview["winner_overall"].astype(str).tolist() if not overview.empty else []
    old_win_count = winners.count("old_chain")
    new_win_count = winners.count("new_chain")

    promoted_warning = analyzer_scope[
        analyzer_scope["promoted_to_main"].fillna(False)
    ]["analyzer_id"].astype(str).tolist() if analyzer_scope is not None and not analyzer_scope.empty else []
    if has_high_temp_zero_anchor_candidate and (invalid_point_count > 0 or promoted_warning):
        recommended_role = "mixed role"
    elif has_high_temp_zero_anchor_candidate:
        recommended_role = "high-temperature R0(T) anchor support"
    elif invalid_point_count > 0:
        recommended_role = "external validation"
    else:
        recommended_role = "main training candidate"

    if old_win_count > new_win_count:
        overall_winner = "old_chain"
    elif new_win_count > old_win_count:
        overall_winner = "new_chain"
    else:
        overall_winner = "mixed"

    rows: list[dict[str, Any]] = [
        {
            "assessment_scope": "run_summary",
            "run_id": run_name,
            "recommended_role": recommended_role,
            "overall_winner": overall_winner,
            "invalid_pressure_excluded_point_count": invalid_point_count,
            "invalid_pressure_excluded_sample_count": invalid_sample_count,
            "valid_only_sample_count": valid_only_sample_count,
            "valid_only_point_count": valid_only_point_count,
            "valid_only_co2_point_count": valid_only_co2_point_count,
            "has_high_temp_zero_anchor_candidate": has_high_temp_zero_anchor_candidate,
            "high_temp_zero_anchor_note": (
                "this run provides a high-temperature zero anchor candidate"
                if has_high_temp_zero_anchor_candidate
                else ""
            ),
            "promoted_warning_analyzers": ",".join(promoted_warning),
        }
    ]

    for _, row in overview.iterrows():
        analyzer_id = str(row["analyzer_id"])
        analyzer_scope_row = (
            analyzer_scope[analyzer_scope["analyzer_id"] == analyzer_id].iloc[0]
            if analyzer_scope is not None and not analyzer_scope.empty and (analyzer_scope["analyzer_id"] == analyzer_id).any()
            else None
        )
        scope_after = str(analyzer_scope_row["scope_after"]) if analyzer_scope_row is not None else ""
        promoted = bool(analyzer_scope_row["promoted_to_main"]) if analyzer_scope_row is not None else False
        rows.append(
            {
                "assessment_scope": "analyzer",
                "run_id": run_name,
                "analyzer_id": analyzer_id,
                "overall_winner": row.get("winner_overall"),
                "winner_zero": row.get("winner_zero"),
                "winner_temp_stability": row.get("winner_temp_stability"),
                "old_overall_rmse": row.get("old_chain_rmse"),
                "new_overall_rmse": row.get("new_chain_rmse"),
                "old_zero_rmse": row.get("old_zero_rmse"),
                "new_zero_rmse": row.get("new_zero_rmse"),
                "scope_after": scope_after,
                "promoted_to_main": promoted,
                "recommendation": row.get("recommendation"),
            }
        )

    note = (
        f"{run_name}: {recommended_role}. "
        f"Invalid-pressure hard exclude removed {invalid_point_count} point(s) / {invalid_sample_count} sample(s). "
        + (
            "This run provides a high-temperature zero anchor candidate."
            if has_high_temp_zero_anchor_candidate
            else "No ambient 40C / 0 ppm anchor candidate was found."
        )
    )
    return pd.DataFrame(rows), note
