from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping


TRACE_GUARD_SCHEMA_VERSION = "v2.trace_size_guard.1"
MAX_TRACE_EVENT_JSON_BYTES = 64 * 1024
MAX_TRACE_FILE_INLINE_LOAD_BYTES = 128 * 1024 * 1024
MAX_TRACE_LINE_INLINE_LOAD_BYTES = 2 * 1024 * 1024
MAX_TRACE_MAPPING_ITEMS = 64
MAX_TRACE_LIST_ITEMS = 16
MAX_TRACE_CONTEXT_LIST_ITEMS = 2
MAX_TRACE_TEXT_CHARS = 1024
MAX_TRACE_DEPTH = 5

LARGE_CONTEXT_KEYS = {
    "route_state",
    "pressure_samples",
    "route_open_transient_pressure_samples",
    "vent_ticks",
    "diagnostic_deferred_events",
}

TRACE_GUARD_FLAGS = {
    "route_trace": "trace_guard_applied_to_route_trace",
    "pressure_trace": "trace_guard_applied_to_pressure_trace",
}

SUMMARY_PRESERVE_KEYS = (
    "ts",
    "run_id",
    "route",
    "point_index",
    "point_tag",
    "action",
    "result",
    "message",
    "event",
    "event_name",
    "event_type",
    "stage",
    "timestamp_local",
    "timestamp_monotonic_s",
    "target_pressure_hpa",
    "pressure_hpa",
    "pressure_point_index",
    "gate_decision",
    "control_ready_status",
    "pressure_source_selected",
    "selected_pressure_source",
    "selected_pressure_sample_age_s",
    "selected_pressure_sample_is_stale",
    "selected_pressure_parse_ok",
    "selected_pressure_freshness_ok",
    "pressure_freshness_decision_source",
    "selected_pressure_fail_closed_reason",
    "diagnostic_blocked_vent_scheduler_source",
    "max_vent_pulse_write_gap_ms_including_terminal_gap",
    "terminal_gap_source",
    "terminal_gap_operation",
    "route_conditioning_phase",
)


def json_payload_size_bytes(payload: Any) -> int:
    return len(
        json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    )


def _list_summary(value: Any, *, depth: int) -> dict[str, Any]:
    raw = list(value)
    summary: dict[str, Any] = {
        "_truncated": True,
        "_type": type(value).__name__,
        "_length": len(raw),
    }
    if raw:
        summary["first"] = _json_safe(raw[0], depth=depth + 1, key_hint="")
        if len(raw) > 1:
            summary["last"] = _json_safe(raw[-1], depth=depth + 1, key_hint="")
    return summary


def _mapping_summary(value: Mapping[str, Any], *, depth: int) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "_truncated": True,
        "_type": "mapping",
        "_length": len(value),
    }
    preserved = 0
    for key, item in value.items():
        if preserved >= MAX_TRACE_MAPPING_ITEMS:
            break
        text_key = str(key)
        if item is None or isinstance(item, (str, int, float, bool)):
            summary[text_key] = _json_safe(item, depth=depth + 1, key_hint=text_key)
            preserved += 1
        elif text_key in LARGE_CONTEXT_KEYS:
            if isinstance(item, Mapping):
                summary[text_key] = {"_type": "mapping", "_length": len(item), "_truncated": True}
            elif isinstance(item, (list, tuple, set)):
                summary[text_key] = _list_summary(item, depth=depth + 1)
            else:
                summary[text_key] = {"_type": type(item).__name__, "_truncated": True}
            preserved += 1
    if len(value) > preserved:
        summary["_omitted_items"] = max(0, len(value) - preserved)
    return summary


def _json_safe(value: Any, *, depth: int = 0, key_hint: str = "") -> Any:
    if value is None or isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, str):
        if len(value) <= MAX_TRACE_TEXT_CHARS:
            return value
        return {
            "_truncated": True,
            "_type": "str",
            "_length": len(value),
            "preview": value[:MAX_TRACE_TEXT_CHARS],
        }
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        if depth >= MAX_TRACE_DEPTH:
            return _mapping_summary(value, depth=depth)
        out: dict[str, Any] = {}
        for index, (key, item) in enumerate(value.items()):
            if index >= MAX_TRACE_MAPPING_ITEMS:
                out["_truncated_items"] = max(0, len(value) - MAX_TRACE_MAPPING_ITEMS)
                break
            text_key = str(key)
            out[text_key] = _json_safe(item, depth=depth + 1, key_hint=text_key)
        return out
    if isinstance(value, (list, tuple, set)):
        raw = list(value)
        if key_hint in LARGE_CONTEXT_KEYS:
            return _list_summary(raw, depth=depth)
        if depth >= MAX_TRACE_DEPTH:
            return _list_summary(raw, depth=depth)
        out = [_json_safe(item, depth=depth + 1, key_hint="") for item in raw[:MAX_TRACE_LIST_ITEMS]]
        if len(raw) > MAX_TRACE_LIST_ITEMS:
            out.append({"_truncated_items": len(raw) - MAX_TRACE_LIST_ITEMS})
        return out
    return str(value)


def _compact_trace_context(value: Any, *, source_bytes: int, reason: str) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "_truncated": True,
        "_reason": reason,
        "_event_json_bytes_before_compaction": int(source_bytes),
        "_event_json_byte_limit": MAX_TRACE_EVENT_JSON_BYTES,
    }
    if isinstance(value, Mapping):
        summary.update(_mapping_summary(value, depth=1))
    else:
        summary["_original_type"] = type(value).__name__
    return summary


def _apply_trace_name_flag(record: dict[str, Any], trace_name: str) -> None:
    flag = TRACE_GUARD_FLAGS.get(str(trace_name or "").strip())
    if flag:
        record[flag] = True


def _finalize_guard_metadata(
    record: dict[str, Any],
    *,
    original_size_bytes: int,
    trace_name: str,
    truncated: bool,
) -> dict[str, Any]:
    _apply_trace_name_flag(record, trace_name)
    record["trace_guard_schema_version"] = TRACE_GUARD_SCHEMA_VERSION
    if truncated:
        record["trace_event_truncated"] = True
        record["trace_event_original_size_bytes"] = int(original_size_bytes)
        record["trace_event_byte_limit"] = MAX_TRACE_EVENT_JSON_BYTES
    return record


def _minimal_trace_record(
    record: Mapping[str, Any],
    *,
    original_size_bytes: int,
    trace_name: str,
    reason: str,
) -> dict[str, Any]:
    minimal: dict[str, Any] = {}
    for key in SUMMARY_PRESERVE_KEYS:
        if key in record:
            minimal[key] = _json_safe(record.get(key), depth=1, key_hint=key)
    omitted = [str(key) for key in record.keys() if str(key) not in minimal]
    if omitted:
        minimal["_omitted_top_level_keys"] = omitted[:MAX_TRACE_LIST_ITEMS]
        if len(omitted) > MAX_TRACE_LIST_ITEMS:
            minimal["_omitted_top_level_key_count"] = len(omitted)
    minimal["trace_event_truncation_reason"] = reason
    return _finalize_guard_metadata(
        minimal,
        original_size_bytes=original_size_bytes,
        trace_name=trace_name,
        truncated=True,
    )


def guard_trace_event(row: Mapping[str, Any], *, trace_name: str = "") -> dict[str, Any]:
    original = dict(row)
    original_size = json_payload_size_bytes(original)
    record = _json_safe(original, depth=0, key_hint="")
    if not isinstance(record, Mapping):
        record = {"value": record}
    guarded = dict(record)
    safe_size = json_payload_size_bytes(guarded)
    truncated = safe_size < original_size or safe_size > MAX_TRACE_EVENT_JSON_BYTES

    if safe_size > MAX_TRACE_EVENT_JSON_BYTES:
        for key in ("actual", "route_state", "target", "relay_state"):
            value = guarded.get(key)
            if isinstance(value, Mapping):
                guarded[key] = _compact_trace_context(
                    value,
                    source_bytes=original_size,
                    reason="trace_event_exceeded_json_byte_limit",
                )
        safe_size = json_payload_size_bytes(guarded)
        truncated = True

    guarded = _finalize_guard_metadata(
        guarded,
        original_size_bytes=original_size,
        trace_name=trace_name,
        truncated=truncated,
    )
    guarded_size = json_payload_size_bytes(guarded)
    if guarded_size > MAX_TRACE_EVENT_JSON_BYTES:
        guarded = _minimal_trace_record(
            guarded,
            original_size_bytes=original_size,
            trace_name=trace_name,
            reason="trace_event_still_exceeded_json_byte_limit_after_context_compaction",
        )
        guarded_size = json_payload_size_bytes(guarded)

    if bool(guarded.get("trace_event_truncated")):
        guarded["trace_event_truncated_size_bytes"] = int(guarded_size)
    return guarded


def write_guarded_jsonl(
    path: str | Path,
    rows: list[Mapping[str, Any]],
    *,
    trace_name: str = "",
) -> dict[str, Any]:
    target = Path(path)
    stats: dict[str, Any] = {
        "trace_name": trace_name,
        "trace_rows_written": 0,
        "trace_event_truncated_count": 0,
        "trace_event_max_original_size_bytes": 0,
        "trace_event_max_truncated_size_bytes": 0,
        "trace_event_max_written_line_bytes": 0,
        "trace_large_line_warning_count": 0,
        "trace_file_size_guard_triggered": False,
        "trace_streaming_read_used": False,
        "trace_inline_load_blocked": False,
    }
    _apply_trace_name_flag(stats, trace_name)
    with target.open("w", encoding="utf-8") as handle:
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            guarded = guard_trace_event(row, trace_name=trace_name)
            line = json.dumps(guarded, ensure_ascii=False, separators=(",", ":"))
            line_bytes = len(line.encode("utf-8"))
            stats["trace_rows_written"] += 1
            stats["trace_event_max_written_line_bytes"] = max(
                int(stats["trace_event_max_written_line_bytes"]),
                line_bytes,
            )
            if guarded.get("trace_event_truncated"):
                stats["trace_event_truncated_count"] += 1
                stats["trace_event_max_original_size_bytes"] = max(
                    int(stats["trace_event_max_original_size_bytes"]),
                    int(guarded.get("trace_event_original_size_bytes") or 0),
                )
                stats["trace_event_max_truncated_size_bytes"] = max(
                    int(stats["trace_event_max_truncated_size_bytes"]),
                    int(guarded.get("trace_event_truncated_size_bytes") or line_bytes),
                )
            handle.write(line + "\n")
    return stats


def load_guarded_jsonl(path: str | Path, *, trace_name: str = "") -> list[dict[str, Any]]:
    target = Path(path)
    if not target.exists():
        return []
    try:
        size_bytes = target.stat().st_size
    except OSError:
        size_bytes = 0
    if size_bytes > MAX_TRACE_FILE_INLINE_LOAD_BYTES:
        row = {
            "event": "trace_inline_load_blocked",
            "path": str(target),
            "file_size_bytes": size_bytes,
            "max_inline_bytes": MAX_TRACE_FILE_INLINE_LOAD_BYTES,
            "reason": "trace_file_too_large_for_inline_load",
            "trace_file_size_guard_triggered": True,
            "trace_inline_load_blocked": True,
            "trace_streaming_read_used": True,
            "trace_large_line_warning_count": 0,
            "trace_guard_schema_version": TRACE_GUARD_SCHEMA_VERSION,
        }
        _apply_trace_name_flag(row, trace_name)
        return [row]

    rows: list[dict[str, Any]] = []
    warning_count = 0
    with target.open("rb") as handle:
        for raw_line in handle:
            if not raw_line.strip():
                continue
            if len(raw_line) > MAX_TRACE_LINE_INLINE_LOAD_BYTES:
                warning_count += 1
                row = {
                    "event": "trace_large_line_skipped",
                    "path": str(target),
                    "line_bytes": len(raw_line),
                    "max_line_bytes": MAX_TRACE_LINE_INLINE_LOAD_BYTES,
                    "reason": "trace_line_too_large_for_inline_load",
                    "trace_large_line_skipped": True,
                    "trace_large_line_warning_count": warning_count,
                    "trace_streaming_read_used": True,
                    "trace_inline_load_blocked": False,
                    "trace_file_size_guard_triggered": False,
                    "trace_guard_schema_version": TRACE_GUARD_SCHEMA_VERSION,
                }
                _apply_trace_name_flag(row, trace_name)
                rows.append(row)
                continue
            try:
                payload = json.loads(raw_line.decode("utf-8"))
            except Exception:
                continue
            if isinstance(payload, Mapping):
                guarded = guard_trace_event(payload, trace_name=trace_name)
                guarded["trace_streaming_read_used"] = True
                rows.append(guarded)
    return rows


def summarize_trace_guard_rows(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "trace_event_truncated_count": sum(1 for row in rows if row.get("trace_event_truncated") is True),
        "trace_large_line_warning_count": sum(1 for row in rows if row.get("trace_large_line_skipped") is True),
        "trace_file_size_guard_triggered": any(row.get("trace_file_size_guard_triggered") is True for row in rows),
        "trace_streaming_read_used": any(row.get("trace_streaming_read_used") is True for row in rows),
        "trace_inline_load_blocked": any(row.get("trace_inline_load_blocked") is True for row in rows),
        "trace_event_max_original_size_bytes": max(
            [int(row.get("trace_event_original_size_bytes") or 0) for row in rows] or [0]
        ),
        "trace_event_max_truncated_size_bytes": max(
            [int(row.get("trace_event_truncated_size_bytes") or 0) for row in rows] or [0]
        ),
    }
