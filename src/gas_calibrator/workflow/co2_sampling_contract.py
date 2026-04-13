"""V1 CO2 phase-aware sampling contract helpers.

These helpers stay in the V1 workflow path and only prepare candidate rows
for the existing quarantine / source-trust / steady-state logic.
"""

from __future__ import annotations

import math
from collections import Counter
from typing import Any, Dict, Mapping, Optional, Sequence

from .co2_bad_frame_qc import format_reason_summary


def _safe_float(value: Any) -> Optional[float]:
    try:
        numeric = float(value)
    except Exception:
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def apply_co2_phase_sampling_contract(
    rows: Sequence[Mapping[str, Any]],
    *,
    min_rows_after_gate: int,
    head_exclusion_max_rows: int,
    head_step_max_delta_ppm: Optional[float],
) -> Dict[str, Any]:
    row_states = []
    reason_counts: Counter[str] = Counter()
    first_usable_pos: Optional[int] = None

    for pos, raw_row in enumerate(rows or []):
        sample_index = int(raw_row.get("sample_index") or (pos + 1))
        usable = raw_row.get("usable")
        value = _safe_float(raw_row.get("value"))
        phase_label = "acquisition_candidate"
        phase_reason = ""

        if first_usable_pos is None and (usable is False or value is None):
            phase_label = "presample"
            phase_reason = "presample_not_usable_or_missing_value"
            reason_counts.update([phase_reason])
        elif first_usable_pos is None:
            first_usable_pos = pos

        row_states.append(
            {
                "sample_index": sample_index,
                "sample_ts": raw_row.get("sample_ts"),
                "value": value,
                "usable": usable,
                "raw_row": dict(raw_row),
                "phase_label": phase_label,
                "phase_reason": phase_reason,
            }
        )

    start_pos = first_usable_pos
    head_limit = max(0, int(head_exclusion_max_rows or 0))
    max_delta = _safe_float(head_step_max_delta_ppm)
    head_excluded = 0

    if start_pos is not None and head_limit > 0 and max_delta is not None:
        cursor = start_pos
        while cursor is not None and cursor < len(row_states) - 1 and head_excluded < head_limit:
            current = row_states[cursor]
            next_pos: Optional[int] = None
            for candidate_pos in range(cursor + 1, len(row_states)):
                candidate = row_states[candidate_pos]
                if candidate["usable"] is False or candidate["value"] is None:
                    continue
                next_pos = candidate_pos
                break
            if next_pos is None:
                break
            next_row = row_states[next_pos]
            current_value = _safe_float(current.get("value"))
            next_value = _safe_float(next_row.get("value"))
            if current_value is None or next_value is None:
                break
            delta_ppm = abs(current_value - next_value)
            if delta_ppm <= max_delta:
                break
            current["phase_label"] = "transition_excluded"
            current["phase_reason"] = (
                f"transition_head_delta_ppm={delta_ppm:.3f}>max_delta_ppm={max_delta:.3f}"
            )
            reason_counts.update(["transition_head_delta_gt_threshold"])
            head_excluded += 1
            cursor = next_pos
            start_pos = next_pos

    eligible_rows = []
    row_diagnostics: Dict[int, Dict[str, Any]] = {}
    phase_excluded_count = 0
    for row_state in row_states:
        label = str(row_state.get("phase_label") or "")
        reason = str(row_state.get("phase_reason") or "")
        if label in {"presample", "transition_excluded"}:
            phase_excluded_count += 1
        if label == "acquisition_candidate":
            eligible_rows.append(dict(row_state.get("raw_row") or {}))
        row_diagnostics[int(row_state.get("sample_index") or 0)] = {
            "co2_phase_label": label,
            "co2_phase_reason": reason,
        }

    rows_before = len(row_states)
    rows_after = len(eligible_rows)
    contract_reasons = []
    if first_usable_pos is None:
        contract_reasons.append("no_usable_rows_before_phase_gate")
    if rows_after < max(0, int(min_rows_after_gate or 0)):
        contract_reasons.append(
            f"rows_after_phase_gate={rows_after}<min_rows_after_phase_gate={int(min_rows_after_gate or 0)}"
        )
    if head_excluded >= head_limit > 0:
        last_state = row_states[start_pos] if start_pos is not None and start_pos < len(row_states) else None
        if last_state and str(last_state.get("phase_label") or "") == "transition_excluded":
            contract_reasons.append("head_exclusion_cap_reached")

    phase_excluded_ratio = round(phase_excluded_count / rows_before, 6) if rows_before > 0 else 0.0
    return {
        "eligible_rows": eligible_rows,
        "rows_before_phase_gate": rows_before,
        "rows_after_phase_gate": rows_after,
        "phase_excluded_count": phase_excluded_count,
        "phase_excluded_ratio": phase_excluded_ratio,
        "phase_reason_summary": format_reason_summary(reason_counts),
        "phase_contract_reason": ";".join(contract_reasons),
        "row_diagnostics": row_diagnostics,
    }
