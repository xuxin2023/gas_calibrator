"""V1 CO2 bad-frame quarantine helpers.

These helpers are intentionally pure and V1-only so the live runner can
reuse them without pulling in V2 runtime paths.
"""

from __future__ import annotations

import math
from collections import Counter
from typing import Any, Dict, Mapping, Optional, Sequence


def _contains_token(text: str, tokens: Sequence[str]) -> Optional[str]:
    upper_text = str(text or "").strip().upper()
    if not upper_text:
        return None
    for token in tokens or []:
        cleaned = str(token or "").strip().upper()
        if cleaned and cleaned in upper_text:
            return cleaned
    return None


def _matches_numeric_tolerance(value: Any, target: float, tolerance: float) -> bool:
    try:
        numeric = float(value)
    except Exception:
        return False
    if not math.isfinite(numeric):
        return False
    return abs(numeric - float(target)) <= max(0.0, float(tolerance))


def _matches_any_numeric_tolerance(value: Any, targets: Sequence[float], tolerance: float) -> bool:
    return any(_matches_numeric_tolerance(value, target, tolerance) for target in targets or [])


def format_reason_summary(reason_counts: Mapping[str, int]) -> str:
    parts = []
    for reason, count in sorted(
        ((str(reason or "").strip(), int(count or 0)) for reason, count in dict(reason_counts or {}).items()),
        key=lambda item: (-item[1], item[0]),
    ):
        if not reason or count <= 0:
            continue
        parts.append(f"{reason}:{count}")
    return ",".join(parts)


def quarantine_co2_source_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    hard_bad_status_tokens: Sequence[str],
    soft_bad_status_tokens: Sequence[str],
    invalid_sentinel_values: Sequence[float],
    invalid_sentinel_tolerance: float,
    hard_bad_temp_values_c: Sequence[float],
    hard_bad_temp_value_tolerance_c: float,
    isolated_spike_delta_ppm: Optional[float],
    isolated_neighbor_match_delta_ppm: Optional[float],
) -> Dict[str, Any]:
    normalized_rows = []
    hard_reject_counts: Counter[str] = Counter()
    soft_warn_counts: Counter[str] = Counter()
    previous_ts: Optional[float] = None

    for raw_row in rows or []:
        sample_index = raw_row.get("sample_index")
        sample_ts = raw_row.get("sample_ts")
        value = raw_row.get("value")
        usable = raw_row.get("usable")
        frame_status = str(raw_row.get("frame_status") or "").strip()
        chamber_temp_c = raw_row.get("chamber_temp_c")
        case_temp_c = raw_row.get("case_temp_c")

        hard_reasons = []
        soft_reasons = []

        numeric_value: Optional[float]
        try:
            numeric_value = float(value)
        except Exception:
            numeric_value = None
        if numeric_value is None or not math.isfinite(numeric_value):
            hard_reasons.append("co2_value_missing_or_nonfinite")
        elif _matches_any_numeric_tolerance(
            numeric_value,
            invalid_sentinel_values,
            invalid_sentinel_tolerance,
        ):
            hard_reasons.append("co2_value_sentinel")

        if usable is False:
            hard_reasons.append("frame_usable_false")

        hard_status = _contains_token(frame_status, hard_bad_status_tokens)
        if hard_status:
            hard_reasons.append("frame_status_hard_bad")
        elif _contains_token(frame_status, soft_bad_status_tokens):
            soft_reasons.append("frame_status_soft_bad")

        if _matches_any_numeric_tolerance(
            chamber_temp_c,
            hard_bad_temp_values_c,
            hard_bad_temp_value_tolerance_c,
        ) or _matches_any_numeric_tolerance(
            case_temp_c,
            hard_bad_temp_values_c,
            hard_bad_temp_value_tolerance_c,
        ):
            hard_reasons.append("internal_temp_hard_bad")

        if sample_ts is not None:
            try:
                numeric_ts = float(sample_ts)
            except Exception:
                numeric_ts = None
            if numeric_ts is not None and math.isfinite(numeric_ts):
                if previous_ts is not None and numeric_ts < previous_ts:
                    hard_reasons.append("sample_ts_non_monotonic")
                else:
                    previous_ts = numeric_ts

        row_state = {
            "sample_index": sample_index,
            "sample_ts": sample_ts,
            "value": numeric_value,
            "hard_reject_reasons": list(dict.fromkeys(hard_reasons)),
            "soft_warn_reasons": list(dict.fromkeys(soft_reasons)),
        }
        normalized_rows.append(row_state)
        hard_reject_counts.update(row_state["hard_reject_reasons"])
        soft_warn_counts.update(row_state["soft_warn_reasons"])

    spike_delta = None if isolated_spike_delta_ppm is None else max(0.0, float(isolated_spike_delta_ppm))
    neighbor_delta = (
        None
        if isolated_neighbor_match_delta_ppm is None
        else max(0.0, float(isolated_neighbor_match_delta_ppm))
    )
    if spike_delta and neighbor_delta is not None:
        candidate_indexes = [
            idx
            for idx, row in enumerate(normalized_rows)
            if not row["hard_reject_reasons"] and row["value"] is not None
        ]
        for pos in range(1, len(candidate_indexes) - 1):
            prev_idx = candidate_indexes[pos - 1]
            curr_idx = candidate_indexes[pos]
            next_idx = candidate_indexes[pos + 1]
            prev_row = normalized_rows[prev_idx]
            curr_row = normalized_rows[curr_idx]
            next_row = normalized_rows[next_idx]
            prev_value = prev_row["value"]
            curr_value = curr_row["value"]
            next_value = next_row["value"]
            if prev_value is None or curr_value is None or next_value is None:
                continue
            if (
                abs(curr_value - prev_value) > spike_delta
                and abs(curr_value - next_value) > spike_delta
                and abs(prev_value - next_value) <= neighbor_delta
            ):
                curr_row["hard_reject_reasons"].append("isolated_spike")
                hard_reject_counts.update(["isolated_spike"])

    kept_series = []
    row_diagnostics: Dict[int, Dict[str, Any]] = {}
    hard_reject_count = 0
    soft_warn_count = 0
    for row in normalized_rows:
        sample_index = int(row.get("sample_index") or 0)
        hard_reasons = list(dict.fromkeys(row["hard_reject_reasons"]))
        soft_reasons = list(dict.fromkeys(row["soft_warn_reasons"]))
        is_hard = bool(hard_reasons)
        is_soft = bool(soft_reasons)
        if is_hard:
            hard_reject_count += 1
        if is_soft:
            soft_warn_count += 1
        row_diagnostics[sample_index] = {
            "co2_bad_frame": is_hard,
            "co2_bad_frame_reason": ";".join(hard_reasons),
            "co2_soft_warn": is_soft,
            "co2_soft_warn_reason": ";".join(soft_reasons),
        }
        if not is_hard and row.get("value") is not None:
            kept_series.append(
                {
                    "sample_index": sample_index,
                    "sample_ts": row.get("sample_ts"),
                    "value": row.get("value"),
                }
            )

    rows_before = len(normalized_rows)
    rows_after = len(kept_series)
    hard_ratio = round(hard_reject_count / rows_before, 6) if rows_before > 0 else 0.0
    soft_ratio = round(soft_warn_count / rows_before, 6) if rows_before > 0 else 0.0
    return {
        "series": kept_series,
        "rows_before": rows_before,
        "rows_after": rows_after,
        "hard_reject_count": hard_reject_count,
        "hard_reject_ratio": hard_ratio,
        "soft_warn_count": soft_warn_count,
        "soft_warn_ratio": soft_ratio,
        "hard_reject_reason_counts": dict(hard_reject_counts),
        "soft_warn_reason_counts": dict(soft_warn_counts),
        "hard_reject_reason_summary": format_reason_summary(hard_reject_counts),
        "soft_warn_reason_summary": format_reason_summary(soft_warn_counts),
        "row_diagnostics": row_diagnostics,
    }
