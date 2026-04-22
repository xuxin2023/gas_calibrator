from __future__ import annotations

from typing import Any, Mapping, Sequence


def _normalize_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _normalize_point_verdict(value: Any) -> str:
    text = _normalize_text(value)
    if text in {"pass", "ok", "通过"}:
        return "pass"
    if text in {"review", "minor_deviation", "轻微偏差"}:
        return "review"
    if text in {"fail", "不通过"}:
        return "fail"
    return "not_requested"


def summarize_fit_quality(fit_quality_summary: Sequence[Mapping[str, Any]] | None) -> dict[str, str]:
    rows = list(fit_quality_summary or [])
    if not rows:
        return {
            "quality": "unknown",
            "delivery_recommendation": "unknown",
        }

    fit_statuses = {_normalize_text(row.get("FitInputQuality")) for row in rows if _normalize_text(row.get("FitInputQuality"))}
    delivery_codes = {
        _normalize_text(row.get("DeliveryRecommendationCode") or row.get("delivery_recommendation_code"))
        for row in rows
        if _normalize_text(row.get("DeliveryRecommendationCode") or row.get("delivery_recommendation_code"))
    }

    if "fail" in fit_statuses:
        quality = "fail"
    elif "warn" in fit_statuses:
        quality = "warn"
    elif "ok" in fit_statuses:
        quality = "pass"
    else:
        quality = "unknown"

    if "forbid_download" in delivery_codes:
        recommendation = "forbid_download"
    elif "diagnostic_only" in delivery_codes:
        recommendation = "diagnostic_only"
    elif "ok" in delivery_codes:
        recommendation = "ok"
    else:
        recommendation = "unknown"

    return {
        "quality": quality,
        "delivery_recommendation": recommendation,
    }


def summarize_device_write_verify(device_write_verify_summary: Sequence[Mapping[str, Any]] | None) -> str:
    rows = list(device_write_verify_summary or [])
    if not rows:
        return "not_requested"

    statuses = {_normalize_text(row.get("Status")) for row in rows if _normalize_text(row.get("Status"))}
    if not statuses:
        return "not_requested"
    if statuses == {"not_requested"}:
        return "not_requested"
    if statuses == {"ok"}:
        return "pass"
    if "partial" in statuses:
        return "partial"
    if "ok" in statuses and len(statuses) == 1:
        return "pass"
    return "fail"


def summarize_runtime_parity(parity_summary: Mapping[str, Any] | None) -> dict[str, Any]:
    payload = dict(parity_summary or {})
    verdict = str(payload.get("parity_verdict") or payload.get("verdict") or "not_audited").strip() or "not_audited"
    legacy_stream_only = bool(payload.get("legacy_stream_only", False))
    if verdict == "parity_pass":
        quality = "pass"
    elif verdict == "not_audited":
        quality = "not_audited"
    else:
        quality = verdict
    return {
        "quality": quality,
        "verdict": verdict,
        "legacy_stream_only": legacy_stream_only,
    }


def summarize_runtime_standard_validation(runtime_validation: Mapping[str, Any] | None) -> dict[str, Any]:
    payload = dict(runtime_validation or {})
    explicit_status = _normalize_text(
        payload.get("runtime_standard_validation_status") or payload.get("status")
    )
    trim_status = _normalize_text(
        payload.get("offset_trim_status")
        or payload.get("runtime_offset_trim_status")
        or payload.get("trim_status")
    )
    point_rows = list(payload.get("high_point_rows") or payload.get("high_points") or [])

    passed_targets: list[int] = []
    review_targets: list[int] = []
    failed_targets: list[int] = []
    for row in point_rows:
        verdict = _normalize_point_verdict(
            row.get("verdict") or row.get("high_point_verdict") or row.get("status")
        )
        target_raw = row.get("target_ppm")
        try:
            target_ppm = int(round(float(target_raw)))
        except Exception:
            target_ppm = 0
        if verdict == "pass":
            passed_targets.append(target_ppm)
        elif verdict == "review":
            review_targets.append(target_ppm)
        elif verdict == "fail":
            failed_targets.append(target_ppm)

    passed_targets = sorted({value for value in passed_targets if value > 0})
    review_targets = sorted({value for value in review_targets if value > 0})
    failed_targets = sorted({value for value in failed_targets if value > 0})
    high_point_checked = bool(passed_targets or review_targets or failed_targets)
    high_end_review_needed = bool(payload.get("high_end_review_needed")) or any(
        value >= 1000 for value in review_targets + failed_targets
    )

    status = explicit_status
    quality = "not_requested"
    note = ""
    if not status:
        if trim_status == "pass":
            if 400 in passed_targets and 600 in passed_targets:
                status = "runtime_standard_validation_pass_low_mid"
                quality = "pass"
            elif passed_targets:
                status = "pass"
                quality = "pass"
            else:
                status = "offset_trim_pass_single_point"
                quality = "pass"
        elif trim_status in {"review", "partial"}:
            status = "review"
            quality = "review"
        elif trim_status == "fail":
            status = "fail"
            quality = "fail"
        else:
            status = "not_requested"
            quality = "not_requested"

    if quality == "not_requested":
        if status in {"pass", "runtime_standard_validation_pass_low_mid", "offset_trim_pass_single_point"}:
            quality = "pass"
        elif status in {"review", "high_end_review_needed"}:
            quality = "review"
        elif status == "fail":
            quality = "fail"

    if status == "runtime_standard_validation_pass_low_mid":
        note = "offset trim passed and both 400 ppm / 600 ppm checks passed"
    elif status == "offset_trim_pass_single_point":
        note = "offset trim passed at the trim point, but no additional high-point check is recorded"
    elif status == "pass":
        note = "offset trim passed and at least one high-point check passed"
    elif status == "review":
        note = "runtime standard validation needs additional review"
    elif status == "fail":
        note = "runtime standard validation failed"
    else:
        note = "runtime standard validation not provided"

    if high_end_review_needed:
        note = (
            note + "; high-end review remains needed"
            if note
            else "high-end review remains needed"
        )

    return {
        "status": status,
        "quality": quality,
        "high_point_checked": high_point_checked,
        "high_end_review_needed": high_end_review_needed,
        "passed_targets_ppm": passed_targets,
        "review_targets_ppm": review_targets,
        "failed_targets_ppm": failed_targets,
        "note": note,
    }


def build_write_readiness_decision(
    *,
    fit_quality: Any,
    delivery_recommendation: Any,
    coefficient_source: Any,
    writeback_status: Any,
    runtime_parity_verdict: Any,
    legacy_stream_only: bool,
    runtime_standard_validation_status: Any = None,
    high_end_review_needed: bool = False,
) -> dict[str, Any]:
    fit_quality_norm = _normalize_text(fit_quality)
    delivery_recommendation_norm = _normalize_text(delivery_recommendation)
    coefficient_source_norm = str(coefficient_source or "").strip()
    writeback_status_norm = _normalize_text(writeback_status)
    runtime_parity_norm = _normalize_text(runtime_parity_verdict)
    runtime_standard_validation_norm = _normalize_text(runtime_standard_validation_status)

    readiness_code = "unknown"
    readiness_reason = "write readiness not established"
    final_write_ready = False

    if legacy_stream_only:
        readiness_code = "legacy_stream_insufficient_for_runtime_parity"
        readiness_reason = "legacy_stream_insufficient_for_runtime_parity"
    elif runtime_parity_norm in {"", "not_audited"}:
        readiness_code = "runtime_parity_not_audited"
        readiness_reason = "runtime parity has not been audited"
    elif runtime_parity_norm == "parity_inconclusive_missing_runtime_inputs":
        readiness_code = "runtime_parity_inconclusive_missing_runtime_inputs"
        readiness_reason = "runtime parity is inconclusive because visible runtime inputs are missing"
    elif runtime_parity_norm == "parity_inconclusive_missing_live_stream":
        readiness_code = "runtime_parity_inconclusive_missing_live_stream"
        readiness_reason = "runtime parity is inconclusive because no live stream was available"
    elif runtime_parity_norm == "parity_inconclusive_mixed_signal_semantics":
        readiness_code = "runtime_parity_inconclusive_mixed_signal_semantics"
        readiness_reason = "runtime parity is inconclusive because visible signal semantics are mixed"
    elif runtime_parity_norm == "parity_fail":
        readiness_code = "runtime_parity_fail"
        readiness_reason = "runtime parity audit failed"
    elif fit_quality_norm in {"", "unknown"}:
        readiness_code = "fit_quality_unknown"
        readiness_reason = "fit quality is unknown"
    elif fit_quality_norm == "fail":
        readiness_code = "fit_quality_fail"
        readiness_reason = "fit quality failed"
    elif delivery_recommendation_norm in {"", "unknown"}:
        readiness_code = "delivery_recommendation_unknown"
        readiness_reason = "delivery recommendation is unknown"
    elif delivery_recommendation_norm == "diagnostic_only":
        readiness_code = "delivery_recommendation_diagnostic_only"
        readiness_reason = "delivery recommendation is diagnostic-only"
    elif delivery_recommendation_norm == "forbid_download":
        readiness_code = "delivery_recommendation_forbid_download"
        readiness_reason = "delivery recommendation forbids download"
    elif writeback_status_norm in {"", "unknown"}:
        readiness_code = "device_write_verify_unknown"
        readiness_reason = "device write verification status is unknown"
    elif writeback_status_norm == "not_requested":
        readiness_code = "device_write_verify_not_requested"
        readiness_reason = "device write verification was not requested"
    elif writeback_status_norm == "partial":
        readiness_code = "device_write_verify_partial"
        readiness_reason = "device write verification is partial"
    elif writeback_status_norm != "pass":
        readiness_code = "device_write_verify_failed"
        readiness_reason = "device write verification failed"
    elif runtime_standard_validation_norm == "offset_trim_pass_single_point":
        readiness_code = "runtime_standard_validation_single_point_only"
        readiness_reason = "runtime standard validation only passed at a single trim point"
    elif runtime_standard_validation_norm in {"review", "high_end_review_needed"}:
        readiness_code = "runtime_standard_validation_review"
        readiness_reason = "runtime standard validation still needs review"
    elif runtime_standard_validation_norm == "fail":
        readiness_code = "runtime_standard_validation_fail"
        readiness_reason = "runtime standard validation failed"
    else:
        final_write_ready = True
        readiness_code = "all_gates_passed"
        readiness_reason = "fit quality, writeback quality, and runtime parity all passed"

    readiness_summary = (
        f"fit_quality={fit_quality_norm or 'unknown'}; "
        f"delivery_recommendation={delivery_recommendation_norm or 'unknown'}; "
        f"coefficient_source={coefficient_source_norm or 'unknown'}; "
        f"writeback_status={writeback_status_norm or 'unknown'}; "
        f"runtime_parity_verdict={runtime_parity_norm or 'not_audited'}; "
        f"legacy_stream_only={bool(legacy_stream_only)}; "
        f"runtime_standard_validation_status={runtime_standard_validation_norm or 'not_requested'}; "
        f"high_end_review_needed={bool(high_end_review_needed)}"
    )

    return {
        "final_write_ready": final_write_ready,
        "readiness_code": readiness_code,
        "readiness_reason": readiness_reason,
        "readiness_summary": readiness_summary,
    }


__all__ = [
    "build_write_readiness_decision",
    "summarize_device_write_verify",
    "summarize_fit_quality",
    "summarize_runtime_parity",
    "summarize_runtime_standard_validation",
]
