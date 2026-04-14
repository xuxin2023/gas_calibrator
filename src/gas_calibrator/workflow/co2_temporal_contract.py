"""V1 CO2 temporal cadence / dwell contract helpers.

These helpers stay in the V1 workflow path and refine candidate eligibility
after phase/segment gating but before quarantine / source trust / window
selection. They only use existing sample fields.
"""

from __future__ import annotations

import math
from statistics import median
from typing import Any, Dict, List, Mapping, Optional, Sequence

from .co2_bad_frame_qc import format_reason_summary


def _as_float(value: Any) -> Optional[float]:
    try:
        numeric = float(value)
    except Exception:
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _build_temporal_cluster(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    normalized = [dict(row or {}) for row in rows or []]
    row_count = len(normalized)
    if row_count <= 0:
        return {
            "rows": [],
            "effective_dwell_seconds": 0.0,
            "nominal_dt_seconds": None,
            "cadence_coverage_ratio": 0.0,
        }
    timestamps = [_as_float(row.get("sample_ts")) for row in normalized]
    usable_ts = [ts for ts in timestamps if ts is not None]
    positive_deltas: List[float] = []
    for prev_ts, curr_ts in zip(usable_ts, usable_ts[1:]):
        delta = curr_ts - prev_ts
        if delta > 0:
            positive_deltas.append(delta)
    nominal_dt = median(positive_deltas) if positive_deltas else None
    effective_dwell = 0.0
    if len(usable_ts) >= 2:
        effective_dwell = max(0.0, usable_ts[-1] - usable_ts[0])
    nominal_dwell = (
        float(nominal_dt) * max(0, row_count - 1)
        if nominal_dt is not None and row_count > 1
        else None
    )
    if nominal_dwell is None:
        coverage_ratio = 1.0 if row_count <= 1 else 0.0
    elif nominal_dwell <= 0:
        coverage_ratio = 1.0
    else:
        coverage_ratio = max(0.0, min(1.0, effective_dwell / nominal_dwell))
    return {
        "rows": normalized,
        "effective_dwell_seconds": round(effective_dwell, 6),
        "nominal_dt_seconds": round(float(nominal_dt), 6) if nominal_dt is not None else None,
        "cadence_coverage_ratio": round(float(coverage_ratio), 6),
    }


def apply_co2_temporal_sampling_contract(
    rows: Sequence[Mapping[str, Any]],
    *,
    min_rows_after_gate: int,
    max_gap_factor: float,
    min_effective_dwell_seconds: float,
    min_cadence_coverage_ratio: float,
    allow_row_fallback_without_timestamp: bool,
) -> Dict[str, Any]:
    normalized_rows = [dict(row or {}) for row in rows or []]
    rows_before = len(normalized_rows)
    row_diagnostics: Dict[int, Dict[str, Any]] = {}
    reason_counts: Dict[str, int] = {}

    for row in normalized_rows:
        sample_index = int(row.get("sample_index") or 0)
        row_diagnostics[sample_index] = {
            "co2_temporal_excluded": False,
            "co2_temporal_reason": "",
        }

    timestamps_present = [
        _as_float(row.get("sample_ts"))
        for row in normalized_rows
        if bool(row.get("sample_ts_present", True))
    ]
    all_have_timestamp = rows_before > 0 and len(timestamps_present) == rows_before and all(
        ts is not None for ts in timestamps_present
    )

    if rows_before <= 0:
        return {
            "eligible_rows": [],
            "rows_before_temporal_gate": 0,
            "rows_after_temporal_gate": 0,
            "timestamp_monotonic": None,
            "duplicate_timestamp_count": 0,
            "backward_timestamp_count": 0,
            "large_gap_count": 0,
            "effective_dwell_seconds": 0.0,
            "nominal_dt_seconds": None,
            "cadence_coverage_ratio": 0.0,
            "temporal_contract_status": "fail",
            "temporal_contract_reason": "no_rows_before_temporal_gate",
            "temporal_fallback_reason": "",
            "row_diagnostics": row_diagnostics,
        }

    if not all_have_timestamp:
        rows_after = len(normalized_rows)
        fallback_reason = "timestamp_missing_row_fallback" if allow_row_fallback_without_timestamp else "timestamp_missing_no_row_fallback"
        contract_reason = ""
        status = "fallback_row_count" if allow_row_fallback_without_timestamp else "fail"
        if rows_after < max(0, int(min_rows_after_gate or 0)):
            contract_reason = f"rows_after_temporal_gate={rows_after}<min_rows_after_temporal_gate={int(min_rows_after_gate or 0)}"
            if status != "fail":
                status = "warn"
        return {
            "eligible_rows": list(normalized_rows) if allow_row_fallback_without_timestamp else [],
            "rows_before_temporal_gate": rows_before,
            "rows_after_temporal_gate": rows_after if allow_row_fallback_without_timestamp else 0,
            "timestamp_monotonic": None,
            "duplicate_timestamp_count": 0,
            "backward_timestamp_count": 0,
            "large_gap_count": 0,
            "effective_dwell_seconds": None,
            "nominal_dt_seconds": None,
            "cadence_coverage_ratio": None,
            "temporal_contract_status": status,
            "temporal_contract_reason": contract_reason,
            "temporal_fallback_reason": fallback_reason,
            "row_diagnostics": row_diagnostics,
        }

    sanitized_rows: List[Dict[str, Any]] = []
    duplicate_timestamp_count = 0
    backward_timestamp_count = 0
    previous_ts: Optional[float] = None
    for row in normalized_rows:
        sample_index = int(row.get("sample_index") or 0)
        ts = _as_float(row.get("sample_ts"))
        if ts is None:
            row_diagnostics[sample_index]["co2_temporal_excluded"] = True
            row_diagnostics[sample_index]["co2_temporal_reason"] = "timestamp_invalid"
            reason_counts["timestamp_invalid"] = int(reason_counts.get("timestamp_invalid", 0)) + 1
            continue
        if previous_ts is not None and ts < previous_ts:
            backward_timestamp_count += 1
            row_diagnostics[sample_index]["co2_temporal_excluded"] = True
            row_diagnostics[sample_index]["co2_temporal_reason"] = "timestamp_rollback"
            reason_counts["timestamp_rollback"] = int(reason_counts.get("timestamp_rollback", 0)) + 1
            continue
        if previous_ts is not None and ts == previous_ts:
            duplicate_timestamp_count += 1
            row_diagnostics[sample_index]["co2_temporal_excluded"] = True
            row_diagnostics[sample_index]["co2_temporal_reason"] = "timestamp_duplicate"
            reason_counts["timestamp_duplicate"] = int(reason_counts.get("timestamp_duplicate", 0)) + 1
            continue
        sanitized_rows.append(dict(row))
        previous_ts = ts

    positive_deltas = []
    for prev_row, curr_row in zip(sanitized_rows, sanitized_rows[1:]):
        prev_ts = _as_float(prev_row.get("sample_ts"))
        curr_ts = _as_float(curr_row.get("sample_ts"))
        if prev_ts is None or curr_ts is None:
            continue
        delta = curr_ts - prev_ts
        if delta > 0:
            positive_deltas.append(delta)
    nominal_dt = median(positive_deltas) if positive_deltas else None
    max_gap = None
    if nominal_dt is not None and nominal_dt > 0:
        max_gap = float(nominal_dt) * max(1.0, float(max_gap_factor or 1.0))

    clusters: List[List[Dict[str, Any]]] = []
    current_cluster: List[Dict[str, Any]] = []
    large_gap_count = 0
    for row in sanitized_rows:
        if not current_cluster:
            current_cluster = [dict(row)]
            continue
        prev_ts = _as_float(current_cluster[-1].get("sample_ts"))
        curr_ts = _as_float(row.get("sample_ts"))
        gap = None if prev_ts is None or curr_ts is None else curr_ts - prev_ts
        if max_gap is not None and gap is not None and gap > max_gap:
            large_gap_count += 1
            reason_counts["large_gap_split"] = int(reason_counts.get("large_gap_split", 0)) + 1
            clusters.append(list(current_cluster))
            current_cluster = [dict(row)]
            continue
        current_cluster.append(dict(row))
    if current_cluster:
        clusters.append(list(current_cluster))

    cluster_metrics = [_build_temporal_cluster(cluster) for cluster in clusters]
    selected_cluster = (
        max(
            cluster_metrics,
            key=lambda item: (
                _as_float((item.get("rows") or [{}])[-1].get("sample_ts")) or 0.0,
                len(item.get("rows") or []),
                _as_float(item.get("effective_dwell_seconds")) or 0.0,
            ),
        )
        if cluster_metrics
        else _build_temporal_cluster([])
    )
    selected_indices = {
        int(row.get("sample_index") or 0)
        for row in list(selected_cluster.get("rows") or [])
    }
    for row in sanitized_rows:
        sample_index = int(row.get("sample_index") or 0)
        if sample_index not in selected_indices:
            row_diagnostics[sample_index]["co2_temporal_excluded"] = True
            row_diagnostics[sample_index]["co2_temporal_reason"] = "temporal_gap_cluster_excluded"

    rows_after = len(list(selected_cluster.get("rows") or []))
    effective_dwell = selected_cluster.get("effective_dwell_seconds")
    cadence_coverage_ratio = selected_cluster.get("cadence_coverage_ratio")
    contract_reasons: List[str] = []
    status = "pass"
    if backward_timestamp_count > 0 or duplicate_timestamp_count > 0 or large_gap_count > 0:
        status = "warn"
    if rows_after < max(0, int(min_rows_after_gate or 0)):
        status = "fail"
        contract_reasons.append(
            f"rows_after_temporal_gate={rows_after}<min_rows_after_temporal_gate={int(min_rows_after_gate or 0)}"
        )
    min_dwell = max(0.0, float(min_effective_dwell_seconds or 0.0))
    if effective_dwell is not None and float(effective_dwell) < min_dwell:
        status = "fail"
        contract_reasons.append(
            f"effective_dwell_seconds={float(effective_dwell):.3f}<min_effective_dwell_seconds={min_dwell:.3f}"
        )
    min_coverage = max(0.0, float(min_cadence_coverage_ratio or 0.0))
    if cadence_coverage_ratio is not None and float(cadence_coverage_ratio) < min_coverage:
        status = "fail"
        contract_reasons.append(
            f"cadence_coverage_ratio={float(cadence_coverage_ratio):.3f}<min_cadence_coverage_ratio={min_coverage:.3f}"
        )

    timestamp_monotonic = backward_timestamp_count <= 0
    return {
        "eligible_rows": list(selected_cluster.get("rows") or []),
        "rows_before_temporal_gate": rows_before,
        "rows_after_temporal_gate": rows_after,
        "timestamp_monotonic": timestamp_monotonic,
        "duplicate_timestamp_count": duplicate_timestamp_count,
        "backward_timestamp_count": backward_timestamp_count,
        "large_gap_count": large_gap_count,
        "effective_dwell_seconds": effective_dwell,
        "nominal_dt_seconds": selected_cluster.get("nominal_dt_seconds"),
        "cadence_coverage_ratio": cadence_coverage_ratio,
        "temporal_contract_status": status,
        "temporal_contract_reason": ";".join(contract_reasons),
        "temporal_fallback_reason": "",
        "temporal_reason_summary": format_reason_summary(reason_counts),
        "row_diagnostics": row_diagnostics,
    }
