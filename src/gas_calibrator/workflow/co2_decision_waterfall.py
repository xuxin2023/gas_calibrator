"""V1 CO2 decision waterfall / lineage helpers.

These helpers do not introduce new gates. They only consolidate the existing
V1 hardened CO2 decisions into a single, readable lineage object that can be
reused by the live runner, exports, and tests.
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional


def _as_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def _as_text(value: Any) -> str:
    return str(value or "").strip()


def _join_reasons(*parts: Any) -> str:
    seen = []
    for part in parts:
        text = _as_text(part)
        if text and text not in seen:
            seen.append(text)
    return ";".join(seen)


def _worst_status(statuses: List[str]) -> str:
    order = {"pass": 0, "warn": 1, "degraded": 2, "fail": 3}
    normalized = [status if status in order else "warn" for status in statuses or []]
    return max(normalized, key=lambda item: order[item]) if normalized else "pass"


def _stage_status_from_contract(reason: str, *, input_rows: int, output_rows: int) -> str:
    text = _as_text(reason)
    if input_rows > 0 and output_rows <= 0:
        return "fail"
    if "<min_rows_" in text or text.startswith("no_"):
        return "fail"
    if input_rows > output_rows or text:
        return "warn"
    return "pass"


def _stage_summary(
    *,
    stage_name: str,
    input_rows: Any,
    output_rows: Any,
    hard_rejected_count: Any,
    soft_warned_count: Any,
    selected_source: Any,
    selected_segment: Any,
    reason_summary: Any,
    status: Any,
) -> Dict[str, Any]:
    return {
        "stage_name": str(stage_name or ""),
        "input_rows": _as_int(input_rows),
        "output_rows": _as_int(output_rows),
        "hard_rejected_count": _as_int(hard_rejected_count),
        "soft_warned_count": _as_int(soft_warned_count),
        "selected_source": _as_text(selected_source),
        "selected_segment": _as_text(selected_segment),
        "reason_summary": _as_text(reason_summary),
        "status": _as_text(status) or "pass",
    }


def build_co2_decision_waterfall(
    result: Mapping[str, Any],
    *,
    diagnostic_candidate: Optional[Mapping[str, Any]] = None,
    selected_candidate: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    result_map = dict(result or {})
    selected = dict(selected_candidate or {})
    diagnostic = dict(diagnostic_candidate or {})
    reference = selected or diagnostic

    selected_source = _as_text(result_map.get("co2_source_selected") or reference.get("source"))
    selected_segment = _as_text(
        result_map.get("co2_source_segment_selected")
        or result_map.get("co2_source_segment_id")
        or reference.get("segment_id")
    )

    phase_reason = _join_reasons(
        result_map.get("co2_phase_reason_summary"),
        result_map.get("co2_phase_contract_reason"),
    )
    segment_reason = _join_reasons(
        result_map.get("co2_source_segment_reason_summary"),
        result_map.get("co2_source_segment_contract_reason"),
    )
    temporal_reason = _join_reasons(
        result_map.get("co2_temporal_contract_reason"),
        result_map.get("co2_temporal_fallback_reason"),
        reference.get("co2_temporal_reason_summary"),
    )
    quarantine_reason = _join_reasons(result_map.get("co2_quarantine_reason_summary"))
    source_reason = _join_reasons(
        result_map.get("co2_source_switch_reason"),
        result_map.get("co2_source_trust_reason"),
    )
    steady_reason = _join_reasons(
        result_map.get("co2_steady_window_reason"),
        result_map.get("measured_value_source"),
    )

    phase_status = _stage_status_from_contract(
        phase_reason,
        input_rows=_as_int(result_map.get("co2_candidate_rows_before_phase_gate")),
        output_rows=_as_int(result_map.get("co2_candidate_rows_after_phase_gate")),
    )
    segment_status = _stage_status_from_contract(
        segment_reason,
        input_rows=_as_int(result_map.get("co2_candidate_rows_before_segment_gate")),
        output_rows=_as_int(result_map.get("co2_candidate_rows_after_segment_gate")),
    )
    temporal_raw_status = _as_text(result_map.get("co2_temporal_contract_status")).lower()
    if temporal_raw_status in {"", "pass", "skipped"}:
        temporal_status = "pass"
    elif temporal_raw_status in {"warn", "fallback_row_count"}:
        temporal_status = "warn"
    elif temporal_raw_status in {"degraded"}:
        temporal_status = "degraded"
    else:
        temporal_status = "fail"

    quarantine_input_rows = _as_int(reference.get("rows_before_quarantine") or result_map.get("co2_rows_before_quarantine"))
    quarantine_output_rows = _as_int(result_map.get("co2_rows_after_quarantine"))
    if quarantine_input_rows > 0 and quarantine_output_rows <= 0:
        quarantine_status = "fail"
    elif _as_int(result_map.get("co2_bad_frame_count")) > 0 or _as_int(result_map.get("co2_soft_warn_count")) > 0:
        quarantine_status = "warn"
    elif quarantine_reason:
        quarantine_status = "warn"
    else:
        quarantine_status = "pass"

    measured_value_source = _as_text(result_map.get("measured_value_source"))
    if measured_value_source == "co2_no_trusted_source":
        source_status = "fail"
    elif _as_text(result_map.get("co2_source_switch_reason")):
        source_status = "warn"
    elif selected_source:
        source_status = "pass"
    elif _as_text(result_map.get("co2_source_trust_reason")):
        source_status = "warn"
    else:
        source_status = "pass"

    steady_raw_status = _as_text(result_map.get("co2_steady_window_status")).lower()
    if steady_raw_status in {"", "pass", "skipped"}:
        steady_status = "pass"
    elif steady_raw_status in {"warn", "degraded"}:
        steady_status = "warn"
    else:
        steady_status = "fail"

    stages = [
        _stage_summary(
            stage_name="phase",
            input_rows=result_map.get("co2_candidate_rows_before_phase_gate"),
            output_rows=result_map.get("co2_candidate_rows_after_phase_gate"),
            hard_rejected_count=result_map.get("co2_phase_excluded_count"),
            soft_warned_count=0,
            selected_source=selected_source or reference.get("source"),
            selected_segment="",
            reason_summary=phase_reason,
            status=phase_status,
        ),
        _stage_summary(
            stage_name="source_segment",
            input_rows=result_map.get("co2_candidate_rows_before_segment_gate"),
            output_rows=result_map.get("co2_candidate_rows_after_segment_gate"),
            hard_rejected_count=result_map.get("co2_source_segment_settle_excluded_count"),
            soft_warned_count=0,
            selected_source=selected_source or reference.get("source"),
            selected_segment=selected_segment,
            reason_summary=segment_reason,
            status=segment_status,
        ),
        _stage_summary(
            stage_name="temporal",
            input_rows=result_map.get("co2_temporal_candidate_rows_before_gate"),
            output_rows=result_map.get("co2_temporal_candidate_rows_after_gate"),
            hard_rejected_count=max(
                0,
                _as_int(result_map.get("co2_temporal_candidate_rows_before_gate"))
                - _as_int(result_map.get("co2_temporal_candidate_rows_after_gate")),
            ),
            soft_warned_count=0,
            selected_source=selected_source or reference.get("source"),
            selected_segment=selected_segment,
            reason_summary=temporal_reason,
            status=temporal_status,
        ),
        _stage_summary(
            stage_name="quarantine",
            input_rows=quarantine_input_rows,
            output_rows=quarantine_output_rows,
            hard_rejected_count=result_map.get("co2_bad_frame_count"),
            soft_warned_count=result_map.get("co2_soft_warn_count"),
            selected_source=selected_source or reference.get("source"),
            selected_segment=selected_segment,
            reason_summary=quarantine_reason,
            status=quarantine_status,
        ),
        _stage_summary(
            stage_name="source_trust",
            input_rows=quarantine_output_rows,
            output_rows=quarantine_output_rows if selected_source else 0,
            hard_rejected_count=0,
            soft_warned_count=1 if source_status in {"warn", "degraded"} else 0,
            selected_source=selected_source,
            selected_segment=selected_segment,
            reason_summary=source_reason,
            status=source_status,
        ),
        _stage_summary(
            stage_name="steady_state",
            input_rows=quarantine_output_rows,
            output_rows=result_map.get("co2_steady_window_sample_count"),
            hard_rejected_count=0,
            soft_warned_count=1 if steady_status in {"warn", "degraded"} else 0,
            selected_source=selected_source,
            selected_segment=selected_segment,
            reason_summary=steady_reason,
            status=steady_status,
        ),
    ]

    waterfall_status = _worst_status([str(stage.get("status") or "pass") for stage in stages])
    waterfall_reason = ";".join(
        f"{stage['stage_name']}={stage['reason_summary']}"
        for stage in stages
        if stage.get("status") != "pass" and stage.get("reason_summary")
    )
    selected_stage_path = ">".join(
        f"{stage['stage_name']}:{stage['status']}" for stage in stages
    )
    stage_summary = ";".join(
        (
            f"{stage['stage_name']}[{stage['status']} "
            f"in={stage['input_rows']} out={stage['output_rows']}"
            + (
                f" reason={stage['reason_summary']}]"
                if stage.get("reason_summary")
                else "]"
            )
        )
        for stage in stages
    )

    lineage = {
        "selected_source": selected_source,
        "selected_segment": selected_segment,
        "measured_value_source": measured_value_source,
        "waterfall_status": waterfall_status,
        "waterfall_reason": waterfall_reason,
        "stages": stages,
    }
    return {
        "co2_decision_lineage": lineage,
        "co2_decision_waterfall_status": waterfall_status,
        "co2_decision_waterfall_reason": waterfall_reason,
        "co2_decision_stage_count": len(stages),
        "co2_decision_selected_stage_path": selected_stage_path,
        "co2_decision_stage_summary": stage_summary,
    }
