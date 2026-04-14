"""V1 CO2 source-segment settle contract helpers.

The goal is to keep the live runner simple while making source/segment
eligibility explicit and auditable before quarantine / trust / window
selection run.
"""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional, Sequence

from .co2_bad_frame_qc import format_reason_summary


def _as_float(value: Any) -> Optional[float]:
    try:
        numeric = float(value)
    except Exception:
        return None
    return numeric


def _build_segment(
    rows: Sequence[Mapping[str, Any]],
    *,
    source: str,
    segment_index: int,
    head_exclusion_rows: int,
    min_rows_after_settle: int,
    min_settle_seconds: float,
    started_after_transition: bool,
) -> Dict[str, Any]:
    normalized_rows = [dict(row or {}) for row in rows or []]
    segment_id = f"{source}#{segment_index}"
    settle_excluded_count = 0
    settle_reasons: Dict[str, int] = {}
    eligible_rows = list(normalized_rows)
    row_diagnostics: Dict[int, Dict[str, Any]] = {}

    if started_after_transition and normalized_rows:
        start_ts = _as_float(normalized_rows[0].get("sample_ts"))
        while settle_excluded_count < len(normalized_rows):
            candidate = normalized_rows[settle_excluded_count]
            elapsed_s = None
            candidate_ts = _as_float(candidate.get("sample_ts"))
            if start_ts is not None and candidate_ts is not None:
                elapsed_s = candidate_ts - start_ts
            enough_rows = settle_excluded_count >= max(0, int(head_exclusion_rows or 0))
            enough_time = elapsed_s is None or elapsed_s >= max(0.0, float(min_settle_seconds or 0.0))
            if enough_rows and enough_time:
                break
            settle_excluded_count += 1
        if settle_excluded_count > 0:
            settle_reasons["segment_settle_gate"] = settle_excluded_count
            eligible_rows = normalized_rows[settle_excluded_count:]

    for idx, row in enumerate(normalized_rows):
        sample_index = int(row.get("sample_index") or 0)
        row_diagnostics[sample_index] = {
            "co2_source_segment_id": segment_id,
            "co2_source_segment_reason": "",
            "co2_source_segment_settle_excluded": False,
        }
        if idx < settle_excluded_count:
            row_diagnostics[sample_index]["co2_source_segment_settle_excluded"] = True
            row_diagnostics[sample_index]["co2_source_segment_reason"] = (
                f"segment_settle_excluded:index={idx + 1}<head_rows={max(0, int(head_exclusion_rows or 0))}"
            )

    contract_reasons = []
    if started_after_transition:
        contract_reasons.append("segment_started_after_transition")
    if len(eligible_rows) < max(0, int(min_rows_after_settle or 0)):
        contract_reasons.append(
            f"rows_after_segment_gate={len(eligible_rows)}<min_rows_after_segment_gate={int(min_rows_after_settle or 0)}"
        )

    rows_before = len(normalized_rows)
    rows_after = len(eligible_rows)
    return {
        "source": source,
        "segment_index": segment_index,
        "segment_id": segment_id,
        "rows_before_segment_gate": rows_before,
        "rows_after_segment_gate": rows_after,
        "source_segment_rows": rows_before,
        "segment_start_sample_index": int(normalized_rows[0].get("sample_index") or 0) if normalized_rows else None,
        "segment_end_sample_index": int(normalized_rows[-1].get("sample_index") or 0) if normalized_rows else None,
        "segment_start_ts": normalized_rows[0].get("sample_ts") if normalized_rows else None,
        "segment_end_ts": normalized_rows[-1].get("sample_ts") if normalized_rows else None,
        "started_after_transition": bool(started_after_transition),
        "segment_settle_excluded_count": settle_excluded_count,
        "segment_settle_excluded_ratio": round(settle_excluded_count / rows_before, 6) if rows_before > 0 else 0.0,
        "segment_reason_summary": format_reason_summary(settle_reasons),
        "segment_contract_reason": ";".join(contract_reasons),
        "eligible_rows": eligible_rows,
        "row_diagnostics": row_diagnostics,
    }


def build_co2_source_segments(
    rows: Sequence[Mapping[str, Any]],
    *,
    source: str,
    head_exclusion_rows: int,
    min_rows_after_settle: int,
    min_settle_seconds: float,
    first_segment_started_after_transition: bool = False,
) -> Dict[str, Any]:
    normalized_rows = [dict(row or {}) for row in rows or []]
    segments_raw = []
    current = []
    previous_index: Optional[int] = None
    for row in normalized_rows:
        sample_index = int(row.get("sample_index") or 0)
        row_usable = row.get("usable") not in (False,) and row.get("value") is not None
        if not row_usable:
            if current:
                segments_raw.append(list(current))
                current = []
            previous_index = None
            continue
        if current and previous_index is not None and sample_index != previous_index + 1:
            segments_raw.append(list(current))
            current = []
        current.append(row)
        previous_index = sample_index
    if current:
        segments_raw.append(list(current))

    segments = []
    row_diagnostics: Dict[int, Dict[str, Any]] = {}
    total_settle_excluded = 0
    reason_counts: Dict[str, int] = {}
    for idx, segment_rows in enumerate(segments_raw, start=1):
        segment_start = int(segment_rows[0].get("sample_index") or 0) if segment_rows else 0
        started_after_transition = idx > 1 or (idx == 1 and bool(first_segment_started_after_transition))
        segment = _build_segment(
            segment_rows,
            source=source,
            segment_index=idx,
            head_exclusion_rows=head_exclusion_rows,
            min_rows_after_settle=min_rows_after_settle,
            min_settle_seconds=min_settle_seconds,
            started_after_transition=started_after_transition,
        )
        segments.append(segment)
        total_settle_excluded += int(segment.get("segment_settle_excluded_count") or 0)
        if segment.get("segment_reason_summary"):
            reason_counts[str(segment["segment_reason_summary"])] = (
                int(reason_counts.get(str(segment["segment_reason_summary"]), 0)) + 1
            )
        row_diagnostics.update(dict(segment.get("row_diagnostics") or {}))

    rows_before = len(normalized_rows)
    rows_after = sum(int(segment.get("rows_after_segment_gate") or 0) for segment in segments)
    summary_parts = []
    for reason, count in sorted(reason_counts.items()):
        if reason and count > 0:
            summary_parts.append(f"{reason}x{count}")
    return {
        "segments": segments,
        "rows_before_segment_gate": rows_before,
        "rows_after_segment_gate": rows_after,
        "segment_settle_excluded_count": total_settle_excluded,
        "segment_settle_excluded_ratio": round(total_settle_excluded / rows_before, 6) if rows_before > 0 else 0.0,
        "segment_reason_summary": ";".join(summary_parts),
        "row_diagnostics": row_diagnostics,
    }
