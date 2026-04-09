from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
import json
from pathlib import Path
from typing import Any, Iterable, Optional

from ..config import (
    build_step2_config_governance_handoff,
    build_step2_config_safety_review,
    summarize_step2_config_safety,
)
from ..domain.pressure_selection import effective_pressure_mode, pressure_target_label
from ..domain.services import build_run_spectral_quality_summary
from ..qc.qc_report import build_qc_evidence_section, build_qc_review_payload
from .acceptance_model import (
    build_run_acceptance_plan,
    build_suite_acceptance_plan,
    build_user_visible_evidence_boundary,
    build_version_snapshot,
    gate_display_name,
    normalize_evidence_source,
    reference_quality_ok,
    relay_mismatch_present,
)
from .controlled_state_machine_profile import (
    STATE_TRANSITION_EVIDENCE_FILENAME,
    STATE_TRANSITION_EVIDENCE_MARKDOWN_FILENAME,
    build_state_transition_evidence,
)
from .multi_source_stability import (
    MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME,
    MULTI_SOURCE_STABILITY_EVIDENCE_MARKDOWN_FILENAME,
    SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME,
    build_multi_source_stability_evidence,
    build_simulation_evidence_sidecar_bundle,
)
from .measurement_phase_coverage import (
    MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME,
    MEASUREMENT_PHASE_COVERAGE_REPORT_MARKDOWN_FILENAME,
    build_measurement_phase_coverage_report,
)


OFFLINE_ARTIFACT_SCHEMA_VERSION = "1.0"
ACCEPTANCE_PLAN_FILENAME = "acceptance_plan.json"
ANALYTICS_SUMMARY_FILENAME = "analytics_summary.json"
SPECTRAL_QUALITY_SUMMARY_FILENAME = "spectral_quality_summary.json"
TREND_REGISTRY_FILENAME = "trend_registry.json"
LINEAGE_SUMMARY_FILENAME = "lineage_summary.json"
EVIDENCE_REGISTRY_FILENAME = "evidence_registry.json"
COEFFICIENT_REGISTRY_FILENAME = "coefficient_registry.json"
SUITE_ANALYTICS_SUMMARY_FILENAME = "suite_analytics_summary.json"
SUITE_ACCEPTANCE_PLAN_FILENAME = "suite_acceptance_plan.json"
SUITE_EVIDENCE_REGISTRY_FILENAME = "suite_evidence_registry.json"
ROOM_TEMP_DIAGNOSTIC_SUMMARY_FILENAME = "diagnostic_summary.json"
ANALYZER_CHAIN_ISOLATION_SUMMARY_FILENAME = "isolation_comparison_summary.json"


def write_json(path: str | Path, payload: dict[str, Any]) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return target


def _load_route_trace_events(run_dir: Path) -> list[dict[str, Any]]:
    path = Path(run_dir) / "route_trace.jsonl"
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            text = str(line or "").strip()
            if not text:
                continue
            payload = json.loads(text)
            if isinstance(payload, dict):
                rows.append(payload)
    except Exception:
        return []
    return rows


def _route_trace_text(event: dict[str, Any]) -> str:
    payload = dict(event or {})
    return " ".join(
        str(payload.get(key) or "")
        for key in ("route", "action", "message", "point_tag", "result")
    ).strip().lower()


def _augment_measurement_trace_events(
    route_trace_events: list[dict[str, Any]],
    *,
    trace_profile: str,
) -> list[dict[str, Any]]:
    rows = [dict(item) for item in list(route_trace_events or []) if isinstance(item, dict)]
    if str(trace_profile or "").strip() != "measurement_trace_rich_v1":
        return rows

    lower_rows = [_route_trace_text(item) for item in rows]
    supplemental_events: list[dict[str, Any]] = []
    if not any("ambient" in text and "diagnostic" in text for text in lower_rows):
        supplemental_events.append(
            {
                "route": "ambient",
                "point_index": 1,
                "point_tag": "ambient_diagnostic_trace",
                "action": "ambient_diagnostic",
                "result": "simulation_only_synthetic",
                "message": (
                    "Synthetic ambient diagnostic trace for richer measurement-core simulation coverage; "
                    "simulation/headless only and not real-device provenance."
                ),
            }
        )
    if not any("ambient" in text and "sample" in text for text in lower_rows):
        supplemental_events.append(
            {
                "route": "ambient",
                "point_index": 1,
                "point_tag": "ambient_sample_ready_trace",
                "action": "ambient_sample_start",
                "result": "simulation_only_synthetic",
                "message": (
                    "Synthetic ambient sample-ready trace for richer measurement-core simulation coverage; "
                    "simulation/headless only and not real-device provenance."
                ),
            }
        )
    if not any(any(token in text for token in ("retry", "recovery", "abort", "fault_capture")) for text in lower_rows):
        supplemental_events.append(
            {
                "route": "",
                "point_index": 0,
                "point_tag": "measurement_trace_recovery",
                "action": "retry_recovery",
                "result": "simulation_only_synthetic",
                "message": (
                    "Synthetic recovery trace for richer measurement-core simulation coverage; "
                    "simulation/headless only and not real-device provenance."
                ),
            }
        )
    return rows + supplemental_events


def summarize_offline_diagnostic_adapters(run_dir: Path) -> dict[str, Any]:
    root = Path(run_dir)
    if not root.exists():
        return {}

    room_temp_bundles = _discover_room_temp_diagnostic_bundles(root)
    analyzer_chain_bundles = _discover_analyzer_chain_isolation_bundles(root)
    bundles = sorted(
        [*room_temp_bundles, *analyzer_chain_bundles],
        key=lambda item: str(item.get("generated_at") or ""),
        reverse=True,
    )
    if not bundles:
        return {}

    artifact_paths = _unique_existing_paths(
        path
        for bundle in bundles
        for path in list(bundle.get("artifact_paths") or [])
    )
    plot_artifact_paths = _unique_existing_paths(
        path
        for bundle in bundles
        for path in list(bundle.get("plot_artifact_paths") or [])
    )
    primary_artifact_paths = _unique_existing_paths(
        bundle.get("primary_artifact_path")
        for bundle in bundles
    )
    artifact_count = len(artifact_paths)
    plot_count = len(plot_artifact_paths)
    primary_artifact_count = len(primary_artifact_paths)
    primary_artifact_keys = {str(path) for path in primary_artifact_paths}
    plot_artifact_keys = {str(path) for path in plot_artifact_paths}
    supporting_artifact_count = sum(
        1
        for path in artifact_paths
        if str(path) not in primary_artifact_keys and str(path) not in plot_artifact_keys
    )
    latest_bundle = dict(bundles[0] or {})
    latest_room_temp = dict(room_temp_bundles[0] or {}) if room_temp_bundles else {}
    latest_analyzer_chain = dict(analyzer_chain_bundles[0] or {}) if analyzer_chain_bundles else {}
    detail_items = _build_offline_diagnostic_detail_items(
        latest_room_temp=latest_room_temp,
        latest_analyzer_chain=latest_analyzer_chain,
    )
    detail_lines = [
        str(item.get("detail_line") or "").strip()
        for item in detail_items
        if str(item.get("detail_line") or "").strip()
    ]
    summary = (
        f"room-temp {len(room_temp_bundles)} | "
        f"analyzer-chain {len(analyzer_chain_bundles)} | "
        f"latest {str(latest_bundle.get('summary_text') or '--')}"
    )
    coverage_parts = [
        f"room-temp {len(room_temp_bundles)}",
        f"analyzer-chain {len(analyzer_chain_bundles)}",
        f"artifacts {artifact_count}",
    ]
    if plot_count:
        coverage_parts.append(f"plots {plot_count}")
    coverage_summary = " | ".join(coverage_parts)
    review_scope_parts = [
        f"primary {primary_artifact_count}",
        f"supporting {supporting_artifact_count}",
    ]
    if plot_count:
        review_scope_parts.append(f"plots {plot_count}")
    review_scope_summary = " | ".join(review_scope_parts)
    next_check_summary = " | ".join(
        _unique_review_lines(
            [
                detail_item.get("next_check") or detail_item.get("recommendation")
                for detail_item in detail_items
            ]
        )
    )
    review_lines = _unique_review_lines(
        [
            summary,
            f"coverage: {coverage_summary}" if coverage_summary else "",
            f"artifact scope: {review_scope_summary}" if review_scope_summary else "",
            f"next checks: {next_check_summary}" if next_check_summary else "",
            *detail_lines,
            *[
                str(bundle.get("summary_text") or "").strip()
                for bundle in bundles[:4]
            ],
            "证据边界: 仅限 simulation/offline/headless evidence，不代表 real acceptance evidence。",
        ]
    )
    boundary_line = next(
        (
            line
            for line in reversed(review_lines)
            if "real acceptance" in str(line or "").strip().lower()
        ),
        "",
    )
    review_highlight_lines = [
        line
        for line in detail_lines[:2]
        if str(line or "").strip()
    ]
    if str(boundary_line).strip() and boundary_line not in review_highlight_lines:
        review_highlight_lines.append(boundary_line)
    return {
        "found": True,
        "bundle_count": len(bundles),
        "room_temp_count": len(room_temp_bundles),
        "analyzer_chain_count": len(analyzer_chain_bundles),
        "summary": summary,
        "detail_lines": detail_lines,
        "detail_items": detail_items,
        "review_lines": review_lines,
        "review_highlight_lines": review_highlight_lines,
        "artifact_paths": artifact_paths,
        "plot_artifact_paths": plot_artifact_paths,
        "primary_artifact_paths": primary_artifact_paths,
        "artifact_count": artifact_count,
        "primary_artifact_count": primary_artifact_count,
        "supporting_artifact_count": supporting_artifact_count,
        "plot_count": plot_count,
        "coverage_summary": coverage_summary,
        "review_scope_summary": review_scope_summary,
        "next_check_summary": next_check_summary,
        "bundles": bundles,
        "latest_room_temp": latest_room_temp,
        "latest_analyzer_chain": latest_analyzer_chain,
        "evidence_source": "diagnostic",
        "evidence_state": "collected",
        "acceptance_level": "diagnostic",
        "promotion_state": "dry_run_only",
        "not_real_acceptance_evidence": True,
    }


def build_point_taxonomy_handoff(point_summaries: list[dict[str, Any]]) -> dict[str, Any]:
    pressure_counts = Counter()
    pressure_mode_counts = Counter()
    pressure_target_label_counts = Counter()
    flush_counts = Counter()
    preseal_point_count = 0
    postseal_timeout_blocked_count = 0
    late_rebound_count = 0
    stale_gauge_point_count = 0
    max_preseal_overshoot_hpa: float | None = None
    max_preseal_route_sealed_ms: float | None = None
    max_stale_ratio: float | None = None

    for item in list(point_summaries or []):
        point = dict(item.get("point") or {})
        stats = dict(item.get("stats") or {})

        pressure_mode = (
            effective_pressure_mode(
                pressure_hpa=point.get("pressure_hpa"),
                pressure_mode=point.get("pressure_mode"),
                pressure_selection_token=point.get("pressure_selection_token"),
            )
            or "--"
        )
        pressure_mode_counts[pressure_mode] += 1

        target_label = str(
            pressure_target_label(
                pressure_hpa=point.get("pressure_hpa"),
                pressure_mode=point.get("pressure_mode"),
                pressure_selection_token=point.get("pressure_selection_token"),
                explicit_label=point.get("pressure_target_label"),
            )
            or point.get("pressure_target_label")
            or point.get("pressure_selection_token")
            or point.get("pressure_hpa")
            or "--"
        ).strip() or "--"
        pressure_target_label_counts[target_label] += 1

        pressure_label = str(
            point.get("pressure_target_label")
            or point.get("pressure_selection_token")
            or point.get("pressure_mode")
            or point.get("pressure_hpa")
            or "--"
        ).strip() or "--"
        pressure_counts[pressure_label] += 1

        flush_status = str(stats.get("flush_gate_status") or stats.get("dewpoint_gate_result") or "").strip().lower()
        if flush_status:
            flush_counts[flush_status] += 1

        if any(
            stats.get(key) not in (None, "", [])
            for key in (
                "preseal_dewpoint_c",
                "preseal_temp_c",
                "preseal_rh_pct",
                "preseal_pressure_hpa",
                "preseal_trigger_overshoot_hpa",
                "preseal_vent_off_begin_to_route_sealed_ms",
            )
        ):
            preseal_point_count += 1

        overshoot = _coerce_float(stats.get("preseal_trigger_overshoot_hpa"))
        if overshoot is not None:
            max_preseal_overshoot_hpa = overshoot if max_preseal_overshoot_hpa is None else max(max_preseal_overshoot_hpa, overshoot)

        route_sealed_ms = _coerce_float(stats.get("preseal_vent_off_begin_to_route_sealed_ms"))
        if route_sealed_ms is not None:
            max_preseal_route_sealed_ms = (
                route_sealed_ms
                if max_preseal_route_sealed_ms is None
                else max(max_preseal_route_sealed_ms, route_sealed_ms)
            )

        if bool(stats.get("postseal_timeout_blocked")):
            postseal_timeout_blocked_count += 1

        if bool(stats.get("dewpoint_rebound_detected")) or str(stats.get("postsample_late_rebound_status") or "").strip().lower() in {
            "warn",
            "fail",
        }:
            late_rebound_count += 1

        stale_ratio = _coerce_float(stats.get("pressure_gauge_stale_ratio"))
        if stale_ratio is not None and stale_ratio > 0:
            stale_gauge_point_count += 1
            max_stale_ratio = stale_ratio if max_stale_ratio is None else max(max_stale_ratio, stale_ratio)

    pressure_summary = _counter_summary(pressure_counts)
    pressure_mode_summary = _counter_summary(pressure_mode_counts)
    pressure_target_label_summary = _counter_summary(pressure_target_label_counts)
    flush_parts = [f"{status} {count}" for status, count in flush_counts.items()]
    if late_rebound_count:
        flush_parts.append(f"rebound {late_rebound_count}")
    flush_gate_summary = " | ".join(flush_parts)

    preseal_parts: list[str] = []
    if preseal_point_count:
        preseal_parts.append(f"points {preseal_point_count}")
    if max_preseal_overshoot_hpa is not None:
        preseal_parts.append(f"max overshoot {max_preseal_overshoot_hpa:g} hPa")
    if max_preseal_route_sealed_ms is not None:
        preseal_parts.append(f"max sealed wait {max_preseal_route_sealed_ms:g} ms")
    preseal_summary = " | ".join(preseal_parts)

    postseal_parts: list[str] = []
    if postseal_timeout_blocked_count:
        postseal_parts.append(f"timeout blocked {postseal_timeout_blocked_count}")
    if late_rebound_count:
        postseal_parts.append(f"late rebound {late_rebound_count}")
    postseal_summary = " | ".join(postseal_parts)

    stale_parts: list[str] = []
    if stale_gauge_point_count:
        stale_parts.append(f"points {stale_gauge_point_count}")
    if max_stale_ratio is not None:
        stale_parts.append(f"worst {max_stale_ratio * 100:g}%")
    stale_gauge_summary = " | ".join(stale_parts)

    return {
        "pressure_summary": pressure_summary,
        "pressure_mode_summary": pressure_mode_summary,
        "pressure_target_label_summary": pressure_target_label_summary,
        "flush_gate_summary": flush_gate_summary,
        "preseal_summary": preseal_summary,
        "postseal_summary": postseal_summary,
        "stale_gauge_summary": stale_gauge_summary,
    }


def _counter_summary(counter: Counter[str]) -> str:
    return " | ".join(f"{label} {count}" for label, count in counter.items()) if counter else ""


def _load_json_dict(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _unique_existing_paths(values: Iterable[Any]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        try:
            candidate = Path(text).expanduser()
        except Exception:
            continue
        if not candidate.exists():
            continue
        try:
            key = str(candidate.resolve())
        except Exception:
            key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        normalized.append(key)
    return normalized


def _flatten_plot_files(values: Any) -> list[str]:
    flattened: list[str] = []

    def _collect(value: Any) -> None:
        if value in (None, ""):
            return
        if isinstance(value, dict):
            for item in value.values():
                _collect(item)
            return
        if isinstance(value, (list, tuple, set)):
            for item in value:
                _collect(item)
            return
        text = str(value).strip()
        if text:
            flattened.append(text)

    _collect(values)
    return flattened


def _generated_at_or_mtime(path: Path, payload: Optional[dict[str, Any]] = None) -> str:
    generated_at = str(dict(payload or {}).get("generated_at") or "").strip()
    if generated_at:
        return generated_at
    try:
        return datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds")
    except Exception:
        return ""


def _resolve_bundle_path(source_dir: Path, value: Any) -> Path:
    candidate = Path(str(value or "").strip()).expanduser()
    return candidate if candidate.is_absolute() else source_dir / candidate


def _room_temp_summary_text(payload: dict[str, Any]) -> str:
    explicit = str(payload.get("summary") or payload.get("summary_text") or "").strip()
    if explicit:
        return explicit
    classification = str(payload.get("classification") or payload.get("status") or "--").strip() or "--"
    recommended_variant = str(
        payload.get("recommended_variant")
        or payload.get("recommended_route")
        or payload.get("recommended_mode")
        or "--"
    ).strip() or "--"
    dominant_error = str(
        payload.get("dominant_error")
        or payload.get("dominant_error_code")
        or payload.get("dominant_issue")
        or "--"
    ).strip() or "--"
    return (
        f"Room-temp pressure diagnostic | classification {classification} | "
        f"variant {recommended_variant} | dominant {dominant_error}"
    )


def _analyzer_chain_summary_text(payload: dict[str, Any]) -> str:
    explicit = str(payload.get("summary") or payload.get("summary_text") or "").strip()
    if explicit:
        return explicit
    dominant_conclusion = str(
        payload.get("dominant_conclusion")
        or payload.get("dominant_issue")
        or payload.get("dominant_delta")
        or "--"
    ).strip() or "--"
    recommendation = str(
        payload.get("recommended_next_check")
        or payload.get("next_check")
        or payload.get("recommendation")
        or "--"
    ).strip() or "--"
    should_continue_s1 = payload.get("should_continue_s1")
    continue_text = "--" if should_continue_s1 is None else ("continue" if bool(should_continue_s1) else "hold")
    return (
        f"Analyzer-chain isolation | continue_s1 {continue_text} | "
        f"conclusion {dominant_conclusion} | next {recommendation}"
    )


def _discover_room_temp_diagnostic_bundles(root: Path) -> list[dict[str, Any]]:
    bundles: list[dict[str, Any]] = []
    try:
        summary_paths = sorted(
            root.rglob(ROOM_TEMP_DIAGNOSTIC_SUMMARY_FILENAME),
            key=lambda item: item.stat().st_mtime,
            reverse=True,
        )
    except Exception:
        summary_paths = []
    for summary_path in summary_paths:
        payload = _load_json_dict(summary_path)
        source_dir = summary_path.parent
        plot_artifact_paths = _unique_existing_paths(
            _resolve_bundle_path(source_dir, path)
            for path in _flatten_plot_files(payload.get("plot_files"))
        )
        artifact_paths = _unique_existing_paths(
            [
                summary_path,
                source_dir / "readable_report.md",
                source_dir / "diagnostic_workbook.xlsx",
                *plot_artifact_paths,
            ]
        )
        bundles.append(
            {
                "kind": "room_temp",
                "primary_artifact_path": str(summary_path.resolve()),
                "source_dir": str(source_dir.resolve()),
                "generated_at": _generated_at_or_mtime(summary_path, payload),
                "summary_text": _room_temp_summary_text(payload),
                "artifact_paths": artifact_paths,
                "plot_artifact_paths": plot_artifact_paths,
                "classification": str(payload.get("classification") or payload.get("status") or "").strip(),
                "recommended_variant": str(
                    payload.get("recommended_variant")
                    or payload.get("recommended_route")
                    or payload.get("recommended_mode")
                    or ""
                ).strip(),
                "dominant_error": str(
                    payload.get("dominant_error")
                    or payload.get("dominant_error_code")
                    or payload.get("dominant_issue")
                    or ""
                ).strip(),
                "next_check": str(
                    payload.get("recommended_next_check")
                    or payload.get("next_check")
                    or payload.get("recommendation")
                    or ""
                ).strip(),
            }
        )
    return bundles


def _discover_analyzer_chain_isolation_bundles(root: Path) -> list[dict[str, Any]]:
    bundles: list[dict[str, Any]] = []
    try:
        summary_paths = sorted(
            root.rglob(ANALYZER_CHAIN_ISOLATION_SUMMARY_FILENAME),
            key=lambda item: item.stat().st_mtime,
            reverse=True,
        )
    except Exception:
        summary_paths = []
    for summary_path in summary_paths:
        payload = _load_json_dict(summary_path)
        source_dir = summary_path.parent
        plot_artifact_paths = _unique_existing_paths(
            _resolve_bundle_path(source_dir, path)
            for path in _flatten_plot_files(payload.get("plot_files"))
        )
        artifact_paths = _unique_existing_paths(
            [
                summary_path,
                source_dir / "summary.json",
                source_dir / "readable_report.md",
                source_dir / "diagnostic_workbook.xlsx",
                source_dir / "operator_checklist.md",
                source_dir / "compare_vs_8ch.md",
                source_dir / "compare_vs_baseline.md",
                *plot_artifact_paths,
            ]
        )
        bundles.append(
            {
                "kind": "analyzer_chain",
                "primary_artifact_path": str(summary_path.resolve()),
                "source_dir": str(source_dir.resolve()),
                "generated_at": _generated_at_or_mtime(summary_path, payload),
                "summary_text": _analyzer_chain_summary_text(payload),
                "artifact_paths": artifact_paths,
                "plot_artifact_paths": plot_artifact_paths,
                "should_continue_s1": payload.get("should_continue_s1"),
                "dominant_conclusion": str(
                    payload.get("dominant_conclusion")
                    or payload.get("dominant_issue")
                    or payload.get("dominant_delta")
                    or ""
                ).strip(),
                "recommendation": str(
                    payload.get("recommended_next_check")
                    or payload.get("next_check")
                    or payload.get("recommendation")
                    or ""
                ).strip(),
            }
        )
    return bundles


def _normalize_review_evidence_source(value: Any, *, default: str = "--") -> str:
    text = str(value or "").strip()
    if not text:
        return default
    return normalize_evidence_source(text)


def _normalized_review_evidence_sources(values: list[Any]) -> list[str]:
    normalized: list[str] = []
    for value in list(values or []):
        source = _normalize_review_evidence_source(value)
        if source not in normalized:
            normalized.append(source)
    return normalized


def _unique_review_lines(lines: list[Any]) -> list[str]:
    normalized: list[str] = []
    for value in list(lines or []):
        text = str(value or "").strip()
        if text and text not in normalized:
            normalized.append(text)
    return normalized


def _build_offline_diagnostic_detail_items(
    *,
    latest_room_temp: dict[str, Any],
    latest_analyzer_chain: dict[str, Any],
) -> list[dict[str, Any]]:
    detail_items = [
        _build_room_temp_detail_item(latest_room_temp),
        _build_analyzer_chain_detail_item(latest_analyzer_chain),
    ]
    return [item for item in detail_items if item]


def _build_room_temp_detail_item(bundle: dict[str, Any]) -> dict[str, Any]:
    payload = dict(bundle or {})
    if not payload:
        return {}
    classification = str(payload.get("classification") or "--").strip() or "--"
    recommended_variant = str(payload.get("recommended_variant") or "--").strip() or "--"
    dominant_error = str(payload.get("dominant_error") or "--").strip() or "--"
    next_check = str(payload.get("next_check") or "--").strip() or "--"
    artifact_scope_summary = _offline_diagnostic_scope_summary(payload)
    return {
        "kind": "room_temp",
        "summary": str(payload.get("summary_text") or "").strip(),
        "detail_line": (
            "room-temp latest | "
            f"classification {classification} | "
            f"variant {recommended_variant} | "
            f"dominant {dominant_error} | "
            f"next {next_check}"
            + (f" | scope {artifact_scope_summary}" if artifact_scope_summary else "")
        ),
        "generated_at": str(payload.get("generated_at") or ""),
        "primary_artifact_path": str(payload.get("primary_artifact_path") or ""),
        "source_dir": str(payload.get("source_dir") or ""),
        "classification": classification,
        "recommended_variant": recommended_variant,
        "dominant_error": dominant_error,
        "next_check": next_check,
        "artifact_scope_summary": artifact_scope_summary,
        "artifact_count": len(list(payload.get("artifact_paths") or [])),
        "plot_count": len(list(payload.get("plot_artifact_paths") or [])),
    }


def _build_analyzer_chain_detail_item(bundle: dict[str, Any]) -> dict[str, Any]:
    payload = dict(bundle or {})
    if not payload:
        return {}
    should_continue_s1 = payload.get("should_continue_s1")
    continue_text = "--" if should_continue_s1 is None else ("continue" if bool(should_continue_s1) else "hold")
    dominant_conclusion = str(payload.get("dominant_conclusion") or "--").strip() or "--"
    recommendation = str(payload.get("recommendation") or "--").strip() or "--"
    artifact_scope_summary = _offline_diagnostic_scope_summary(payload)
    return {
        "kind": "analyzer_chain",
        "summary": str(payload.get("summary_text") or "").strip(),
        "detail_line": (
            "analyzer-chain latest | "
            f"continue_s1 {continue_text} | "
            f"conclusion {dominant_conclusion} | "
            f"next {recommendation}"
            + (f" | scope {artifact_scope_summary}" if artifact_scope_summary else "")
        ),
        "generated_at": str(payload.get("generated_at") or ""),
        "primary_artifact_path": str(payload.get("primary_artifact_path") or ""),
        "source_dir": str(payload.get("source_dir") or ""),
        "should_continue_s1": should_continue_s1,
        "continue_s1": continue_text,
        "dominant_conclusion": dominant_conclusion,
        "recommendation": recommendation,
        "artifact_scope_summary": artifact_scope_summary,
        "artifact_count": len(list(payload.get("artifact_paths") or [])),
        "plot_count": len(list(payload.get("plot_artifact_paths") or [])),
    }


def _offline_diagnostic_scope_summary(bundle: dict[str, Any]) -> str:
    payload = dict(bundle or {})
    artifact_count = len([item for item in list(payload.get("artifact_paths") or []) if str(item or "").strip()])
    plot_count = len([item for item in list(payload.get("plot_artifact_paths") or []) if str(item or "").strip()])
    parts = [f"artifacts {artifact_count}"]
    if plot_count > 0:
        parts.append(f"plots {plot_count}")
    return " | ".join(parts)


def export_run_offline_artifacts(
    *,
    run_dir: Path,
    output_dir: Path,
    run_id: str,
    session: Any,
    samples: list[Any],
    point_summaries: list[dict[str, Any]],
    output_files: list[str],
    export_statuses: dict[str, dict[str, Any]],
    source_points_file: Optional[str | Path],
    software_build_id: Optional[str],
    config_safety: Optional[dict[str, Any]] = None,
    config_safety_review: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    workflow = getattr(getattr(session, "config", None), "workflow", None)
    features = getattr(getattr(session, "config", None), "features", None)
    trace_profile = str(getattr(workflow, "profile_name", None) or "").strip()
    step2_config_safety = dict(config_safety or {})
    if not step2_config_safety and getattr(session, "config", None) is not None:
        step2_config_safety = summarize_step2_config_safety(getattr(session, "config"))
    step2_config_safety_review = dict(config_safety_review or {})
    if step2_config_safety and not step2_config_safety_review:
        step2_config_safety_review = build_step2_config_safety_review(step2_config_safety)
    versions = build_version_snapshot(
        config_snapshot=getattr(session, "config", None),
        source_points_file=source_points_file,
        profile_name=getattr(workflow, "profile_name", None) if workflow is not None else None,
        profile_version=getattr(workflow, "profile_version", None) if workflow is not None else None,
        software_build_id=software_build_id,
    )
    analytics_summary = build_run_analytics_summary(
        run_id=run_id,
        run_dir=run_dir,
        session=session,
        samples=samples,
        point_summaries=point_summaries,
        export_statuses=export_statuses,
        config_safety=step2_config_safety,
        config_safety_review=step2_config_safety_review,
    )
    acceptance_plan = build_run_acceptance_plan(
        run_id=run_id,
        simulation_mode=bool(getattr(features, "simulation_mode", False)),
        reference_quality_ok_flag=reference_quality_ok(analytics_summary.get("reference_quality_statistics")),
        export_error_count=sum(1 for payload in export_statuses.values() if str((payload or {}).get("status", "")) == "error"),
        parity_status=str(analytics_summary.get("summary_parity_status") or "missing"),
    )
    lineage_summary = build_lineage_summary(
        run_id=run_id,
        run_dir=run_dir,
        output_files=output_files,
        export_statuses=export_statuses,
        versions=versions,
    )
    coefficient_registry = build_coefficient_registry(
        run_id=run_id,
        run_dir=run_dir,
        samples=samples,
        export_statuses=export_statuses,
        versions=versions,
        acceptance_plan=acceptance_plan,
        analytics_summary=analytics_summary,
    )
    trend_registry = build_trend_registry(
        run_id=run_id,
        run_dir=run_dir,
        output_dir=output_dir,
        samples=samples,
        analytics_summary=analytics_summary,
    )
    evidence_registry = build_run_evidence_registry(
        run_id=run_id,
        run_dir=run_dir,
        export_statuses=export_statuses,
        versions=versions,
        acceptance_plan=acceptance_plan,
        analytics_summary=analytics_summary,
        coefficient_registry=coefficient_registry,
        trend_registry=trend_registry,
        config_safety=step2_config_safety,
        config_safety_review=step2_config_safety_review,
    )
    spectral_quality_summary: dict[str, Any] = {}
    spectral_quality_path: Optional[Path] = None
    if bool(getattr(features, "enable_spectral_quality_analysis", False)):
        spectral_min_samples = int(getattr(features, "spectral_min_samples", 64) or 64)
        spectral_min_duration_s = float(getattr(features, "spectral_min_duration_s", 30.0) or 30.0)
        spectral_low_freq_max_hz = float(getattr(features, "spectral_low_freq_max_hz", 0.01) or 0.01)
        try:
            spectral_quality_summary = build_run_spectral_quality_summary(
                run_id=run_id,
                samples=samples,
                simulation_mode=bool(getattr(features, "simulation_mode", False)),
                min_samples=spectral_min_samples,
                min_duration_s=spectral_min_duration_s,
                low_freq_max_hz=spectral_low_freq_max_hz,
            )
        except Exception as exc:
            spectral_quality_summary = _build_skipped_spectral_quality_summary(
                run_id=run_id,
                simulation_mode=bool(getattr(features, "simulation_mode", False)),
                min_samples=spectral_min_samples,
                min_duration_s=spectral_min_duration_s,
                low_freq_max_hz=spectral_low_freq_max_hz,
                error=str(exc),
            )
        spectral_quality_path = write_json(run_dir / SPECTRAL_QUALITY_SUMMARY_FILENAME, spectral_quality_summary)

    acceptance_path = write_json(run_dir / ACCEPTANCE_PLAN_FILENAME, acceptance_plan)
    analytics_path = write_json(run_dir / ANALYTICS_SUMMARY_FILENAME, analytics_summary)
    lineage_path = write_json(run_dir / LINEAGE_SUMMARY_FILENAME, lineage_summary)
    trend_path = write_json(run_dir / TREND_REGISTRY_FILENAME, trend_registry)
    evidence_path = write_json(run_dir / EVIDENCE_REGISTRY_FILENAME, evidence_registry)
    coefficient_path = write_json(run_dir / COEFFICIENT_REGISTRY_FILENAME, coefficient_registry)
    route_trace_events = _augment_measurement_trace_events(
        _load_route_trace_events(run_dir),
        trace_profile=trace_profile,
    )
    synthetic_trace_provenance = {
        "summary": (
            "simulation-generated trace only; richer non-default trace profiles remain simulation/headless and do not claim real-device provenance"
            if not trace_profile
            else (
                f"simulation-generated trace only; trace profile {trace_profile} adds synthetic phase coverage "
                "for reviewer evidence and does not claim real-device provenance"
            )
        ),
        "contains_synthetic_channel_injection": False,
        "trace_profile": trace_profile or "simulation_generated",
    }
    measurement_artifact_paths = {
        "multi_source_stability_evidence": str(run_dir / MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME),
        "multi_source_stability_evidence_markdown": str(run_dir / MULTI_SOURCE_STABILITY_EVIDENCE_MARKDOWN_FILENAME),
        "state_transition_evidence": str(run_dir / STATE_TRANSITION_EVIDENCE_FILENAME),
        "state_transition_evidence_markdown": str(run_dir / STATE_TRANSITION_EVIDENCE_MARKDOWN_FILENAME),
        "simulation_evidence_sidecar_bundle": str(run_dir / SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME),
        "measurement_phase_coverage_report": str(run_dir / MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME),
        "measurement_phase_coverage_report_markdown": str(run_dir / MEASUREMENT_PHASE_COVERAGE_REPORT_MARKDOWN_FILENAME),
    }
    multi_source_stability_evidence = build_multi_source_stability_evidence(
        run_id=run_id,
        samples=samples,
        point_summaries=point_summaries,
        route_trace_events=route_trace_events,
        artifact_paths=measurement_artifact_paths,
    )
    multi_source_stability_evidence_path = write_json(
        run_dir / MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME,
        dict(multi_source_stability_evidence.get("raw") or {}),
    )
    multi_source_stability_markdown_path = run_dir / MULTI_SOURCE_STABILITY_EVIDENCE_MARKDOWN_FILENAME
    multi_source_stability_markdown_path.write_text(
        str(multi_source_stability_evidence.get("markdown") or ""),
        encoding="utf-8",
    )
    state_transition_evidence = build_state_transition_evidence(
        run_id=run_id,
        samples=samples,
        point_summaries=point_summaries,
        route_trace_events=route_trace_events,
        artifact_paths=measurement_artifact_paths,
    )
    state_transition_evidence_path = write_json(
        run_dir / STATE_TRANSITION_EVIDENCE_FILENAME,
        dict(state_transition_evidence.get("raw") or {}),
    )
    state_transition_markdown_path = run_dir / STATE_TRANSITION_EVIDENCE_MARKDOWN_FILENAME
    state_transition_markdown_path.write_text(
        str(state_transition_evidence.get("markdown") or ""),
        encoding="utf-8",
    )
    measurement_phase_coverage_report = build_measurement_phase_coverage_report(
        run_id=run_id,
        samples=samples,
        point_summaries=point_summaries,
        route_trace_events=route_trace_events,
        multi_source_stability_evidence=multi_source_stability_evidence,
        state_transition_evidence=state_transition_evidence,
        artifact_paths=measurement_artifact_paths,
        synthetic_trace_provenance=synthetic_trace_provenance,
    )
    measurement_phase_coverage_path = write_json(
        run_dir / MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME,
        dict(measurement_phase_coverage_report.get("raw") or {}),
    )
    measurement_phase_coverage_markdown_path = run_dir / MEASUREMENT_PHASE_COVERAGE_REPORT_MARKDOWN_FILENAME
    measurement_phase_coverage_markdown_path.write_text(
        str(measurement_phase_coverage_report.get("markdown") or ""),
        encoding="utf-8",
    )
    simulation_evidence_sidecar_bundle = build_simulation_evidence_sidecar_bundle(
        run_id=run_id,
        multi_source_stability_evidence=multi_source_stability_evidence,
        state_transition_evidence=state_transition_evidence,
        measurement_phase_coverage_report=measurement_phase_coverage_report,
        artifact_paths=measurement_artifact_paths,
        synthetic_trace_provenance=synthetic_trace_provenance,
    )
    simulation_evidence_sidecar_bundle_path = write_json(
        run_dir / SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME,
        simulation_evidence_sidecar_bundle,
    )

    statuses = {
        "acceptance_plan": _artifact_status_payload("execution_summary", acceptance_path),
        "analytics_summary": _artifact_status_payload("diagnostic_analysis", analytics_path),
        "lineage_summary": _artifact_status_payload("execution_summary", lineage_path),
        "trend_registry": _artifact_status_payload("diagnostic_analysis", trend_path),
        "evidence_registry": _artifact_status_payload("execution_summary", evidence_path),
        "coefficient_registry": _artifact_status_payload("formal_analysis", coefficient_path),
        "multi_source_stability_evidence": _artifact_status_payload(
            "diagnostic_analysis",
            multi_source_stability_evidence_path,
        ),
        "multi_source_stability_evidence_markdown": _artifact_status_payload(
            "diagnostic_analysis",
            multi_source_stability_markdown_path,
        ),
        "state_transition_evidence": _artifact_status_payload(
            "diagnostic_analysis",
            state_transition_evidence_path,
        ),
        "state_transition_evidence_markdown": _artifact_status_payload(
            "diagnostic_analysis",
            state_transition_markdown_path,
        ),
        "simulation_evidence_sidecar_bundle": _artifact_status_payload(
            "execution_summary",
            simulation_evidence_sidecar_bundle_path,
        ),
        "measurement_phase_coverage_report": _artifact_status_payload(
            "diagnostic_analysis",
            measurement_phase_coverage_path,
        ),
        "measurement_phase_coverage_report_markdown": _artifact_status_payload(
            "diagnostic_analysis",
            measurement_phase_coverage_markdown_path,
        ),
    }
    if spectral_quality_path is not None:
        statuses["spectral_quality_summary"] = _artifact_status_payload("diagnostic_analysis", spectral_quality_path)
    summary_stats = {
        "acceptance_plan": acceptance_plan,
        "acceptance_readiness_summary": acceptance_plan.get("readiness_summary", {}),
        "analytics_summary": analytics_summary,
        "analytics_summary_digest": analytics_summary.get("digest", {}),
        "lineage_summary": lineage_summary,
        "evidence_governance": {
            "evidence_source": acceptance_plan.get("evidence_source"),
            "evidence_state": acceptance_plan.get("evidence_state"),
            "acceptance_level": acceptance_plan.get("acceptance_level"),
            "promotion_state": acceptance_plan.get("promotion_state"),
            "review_state": acceptance_plan.get("review_state"),
            "approval_state": acceptance_plan.get("approval_state"),
            "ready_for_promotion": acceptance_plan.get("ready_for_promotion"),
        },
        "role_views": acceptance_plan.get("role_views", {}),
        "versions": versions,
        "coefficient_registry": coefficient_registry,
        "trend_registry": trend_registry,
        "evidence_registry_path": str(evidence_path),
        "multi_source_stability_evidence": {
            "path": str(multi_source_stability_evidence_path),
            "markdown_path": str(multi_source_stability_markdown_path),
            "overall_status": str(dict(multi_source_stability_evidence.get("raw") or {}).get("overall_status") or ""),
            "coverage_status": str(dict(multi_source_stability_evidence.get("raw") or {}).get("coverage_status") or ""),
            "review_surface": dict(dict(multi_source_stability_evidence.get("raw") or {}).get("review_surface") or {}),
        },
        "multi_source_stability_evidence_digest": dict(multi_source_stability_evidence.get("digest") or {}),
        "state_transition_evidence": {
            "path": str(state_transition_evidence_path),
            "markdown_path": str(state_transition_markdown_path),
            "overall_status": str(dict(state_transition_evidence.get("raw") or {}).get("overall_status") or ""),
            "review_surface": dict(dict(state_transition_evidence.get("raw") or {}).get("review_surface") or {}),
            "illegal_transition_count": len(list(dict(state_transition_evidence.get("raw") or {}).get("illegal_transitions") or [])),
        },
        "state_transition_evidence_digest": dict(state_transition_evidence.get("digest") or {}),
        "simulation_evidence_sidecar_bundle": {
            "path": str(simulation_evidence_sidecar_bundle_path),
            "title_text": str(simulation_evidence_sidecar_bundle.get("title_text") or ""),
            "reviewer_note": str(simulation_evidence_sidecar_bundle.get("reviewer_note") or ""),
            "store_counts": {
                key: len(list(value or []))
                for key, value in dict(simulation_evidence_sidecar_bundle.get("stores") or {}).items()
            },
            "boundary_statements": list(simulation_evidence_sidecar_bundle.get("boundary_statements") or []),
        },
        "measurement_phase_coverage_report": {
            "path": str(measurement_phase_coverage_path),
            "markdown_path": str(measurement_phase_coverage_markdown_path),
            "overall_status": str(dict(measurement_phase_coverage_report.get("raw") or {}).get("overall_status") or ""),
            "review_surface": dict(dict(measurement_phase_coverage_report.get("raw") or {}).get("review_surface") or {}),
        },
        "measurement_phase_coverage_report_digest": dict(measurement_phase_coverage_report.get("digest") or {}),
    }
    if analytics_summary.get("point_taxonomy_summary"):
        summary_stats["point_taxonomy_summary"] = dict(analytics_summary.get("point_taxonomy_summary") or {})
    if analytics_summary.get("offline_diagnostic_adapter_summary"):
        summary_stats["offline_diagnostic_adapter_summary"] = dict(
            analytics_summary.get("offline_diagnostic_adapter_summary") or {}
        )
    if spectral_quality_summary:
        summary_stats["spectral_quality_summary"] = spectral_quality_summary
        summary_stats["spectral_quality_digest"] = _spectral_quality_digest(spectral_quality_summary)
    manifest_sections = {
        "versions": versions,
        "evidence_governance": summary_stats["evidence_governance"],
        "acceptance_plan_digest": acceptance_plan.get("readiness_summary", {}),
        "role_views": acceptance_plan.get("role_views", {}),
        "lineage_summary": {
            "parent_run_id": lineage_summary.get("parent_run_id"),
            "config_version": lineage_summary.get("config_version"),
            "points_version": lineage_summary.get("points_version"),
            "profile_version": lineage_summary.get("profile_version"),
            "software_build_id": lineage_summary.get("software_build_id"),
        },
        "multi_source_stability_evidence": {
            "path": str(multi_source_stability_evidence_path),
            "markdown_path": str(multi_source_stability_markdown_path),
            "summary": str(dict(multi_source_stability_evidence.get("digest") or {}).get("summary") or ""),
            "coverage_summary": str(dict(multi_source_stability_evidence.get("digest") or {}).get("coverage_summary") or ""),
            "decision_summary": str(dict(multi_source_stability_evidence.get("digest") or {}).get("decision_summary") or ""),
            "gap_summary": str(dict(multi_source_stability_evidence.get("digest") or {}).get("gap_summary") or ""),
            "boundary_summary": str(dict(multi_source_stability_evidence.get("digest") or {}).get("boundary_summary") or ""),
        },
        "state_transition_evidence": {
            "path": str(state_transition_evidence_path),
            "markdown_path": str(state_transition_markdown_path),
            "summary": str(dict(state_transition_evidence.get("digest") or {}).get("summary") or ""),
            "transition_summary": str(dict(state_transition_evidence.get("digest") or {}).get("transition_summary") or ""),
            "recovery_summary": str(dict(state_transition_evidence.get("digest") or {}).get("recovery_summary") or ""),
            "boundary_summary": str(dict(state_transition_evidence.get("digest") or {}).get("boundary_summary") or ""),
        },
        "simulation_evidence_sidecar_bundle": {
            "path": str(simulation_evidence_sidecar_bundle_path),
            "title_text": str(simulation_evidence_sidecar_bundle.get("title_text") or ""),
            "reviewer_note": str(simulation_evidence_sidecar_bundle.get("reviewer_note") or ""),
            "store_counts": {
                key: len(list(value or []))
                for key, value in dict(simulation_evidence_sidecar_bundle.get("stores") or {}).items()
            },
            "boundary_summary": " | ".join(list(simulation_evidence_sidecar_bundle.get("boundary_statements") or [])),
        },
        "measurement_phase_coverage_report": {
            "path": str(measurement_phase_coverage_path),
            "markdown_path": str(measurement_phase_coverage_markdown_path),
            "summary": str(dict(measurement_phase_coverage_report.get("digest") or {}).get("summary") or ""),
            "actual_phase_summary": str(dict(measurement_phase_coverage_report.get("digest") or {}).get("actual_phase_summary") or ""),
            "coverage_summary": str(dict(measurement_phase_coverage_report.get("digest") or {}).get("coverage_summary") or ""),
            "gap_summary": str(dict(measurement_phase_coverage_report.get("digest") or {}).get("gap_summary") or ""),
            "boundary_summary": str(dict(measurement_phase_coverage_report.get("digest") or {}).get("boundary_summary") or ""),
        },
    }
    if spectral_quality_summary:
        manifest_sections["spectral_quality"] = _spectral_quality_digest(spectral_quality_summary)
        manifest_sections["spectral_quality"]["not_real_acceptance_evidence"] = bool(
            spectral_quality_summary.get("not_real_acceptance_evidence", True)
        )
        manifest_sections["spectral_quality"]["evidence_source"] = spectral_quality_summary.get("evidence_source")
    return {
        "artifact_statuses": statuses,
        "summary_stats": summary_stats,
        "manifest_sections": manifest_sections,
        "remembered_files": [
            str(acceptance_path),
            str(analytics_path),
            str(lineage_path),
            str(trend_path),
            str(evidence_path),
            str(coefficient_path),
            str(multi_source_stability_evidence_path),
            str(multi_source_stability_markdown_path),
            str(state_transition_evidence_path),
            str(state_transition_markdown_path),
            str(simulation_evidence_sidecar_bundle_path),
            str(measurement_phase_coverage_path),
            str(measurement_phase_coverage_markdown_path),
            *([str(spectral_quality_path)] if spectral_quality_path is not None else []),
        ],
    }


def export_suite_offline_artifacts(*, suite_dir: Path, summary: dict[str, Any]) -> dict[str, Any]:
    analytics_summary = build_suite_analytics_summary(summary)
    acceptance_plan = build_suite_acceptance_plan(
        suite_name=str(summary.get("suite") or "--"),
        offline_green=bool(summary.get("all_passed", False)),
        parity_green=any(str(item.get("kind") or "") == "parity" and bool(item.get("ok", False)) for item in list(summary.get("cases") or [])),
        resilience_green=any(str(item.get("kind") or "") == "resilience" and bool(item.get("ok", False)) for item in list(summary.get("cases") or [])),
        evidence_sources_present=_normalized_review_evidence_sources(
            [item.get("evidence_source") for item in list(summary.get("cases") or [])]
        ),
    )
    evidence_registry = build_suite_evidence_registry(summary, analytics_summary=analytics_summary, acceptance_plan=acceptance_plan)
    analytics_path = write_json(suite_dir / SUITE_ANALYTICS_SUMMARY_FILENAME, analytics_summary)
    acceptance_path = write_json(suite_dir / SUITE_ACCEPTANCE_PLAN_FILENAME, acceptance_plan)
    evidence_path = write_json(suite_dir / SUITE_EVIDENCE_REGISTRY_FILENAME, evidence_registry)
    return {
        "suite_analytics_summary": analytics_summary,
        "suite_acceptance_plan": acceptance_plan,
        "suite_evidence_registry": evidence_registry,
        "remembered_files": [str(analytics_path), str(acceptance_path), str(evidence_path)],
    }


def build_run_analytics_summary(
    *,
    run_id: str,
    run_dir: Path,
    session: Any,
    samples: list[Any],
    point_summaries: list[dict[str, Any]],
    export_statuses: dict[str, dict[str, Any]],
    config_safety: Optional[dict[str, Any]] = None,
    config_safety_review: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    features = getattr(getattr(session, "config", None), "features", None)
    simulation_mode = bool(getattr(features, "simulation_mode", False))
    expected_analyzers = _expected_analyzers(session)
    present_analyzers = sorted(
        {
            str(getattr(sample, "analyzer_id", "") or "").strip().upper()
            for sample in samples
            if str(getattr(sample, "analyzer_id", "") or "").strip()
        }
    )
    usable_analyzers = sorted(
        {
            str(getattr(sample, "analyzer_id", "") or "").strip().upper()
            for sample in samples
            if str(getattr(sample, "analyzer_id", "") or "").strip() and bool(getattr(sample, "frame_usable", True))
        }
    )
    unusable_analyzers = sorted(set(present_analyzers) - set(usable_analyzers))
    frame_status_counts = Counter(
        str(getattr(sample, "frame_status", "") or "").strip() or "unspecified"
        for sample in samples
    )
    total_frames = len(samples)
    frames_with_data = sum(1 for sample in samples if bool(getattr(sample, "frame_has_data", False)))
    usable_frames = sum(1 for sample in samples if bool(getattr(sample, "frame_usable", True)))
    reference_quality = build_reference_quality_statistics(samples)
    export_status_counts = Counter(
        str((payload or {}).get("status", "unknown") or "unknown")
        for payload in export_statuses.values()
    )
    parity_status = _load_named_status(run_dir / "summary_parity_report.json", key="status", default="missing")
    routes = sorted(
        {
            str(getattr(getattr(sample, "point", None), "route", "") or "").strip().lower()
            for sample in samples
            if str(getattr(getattr(sample, "point", None), "route", "") or "").strip()
        }
    )
    point_failure_reasons = Counter(
        str(dict(item.get("stats") or {}).get("reason") or "").strip() or "unspecified"
        for item in point_summaries
        if item
    )
    boundary = build_user_visible_evidence_boundary(simulation_mode=simulation_mode)
    run_kpis = _build_run_kpis(
        samples=samples,
        point_summaries=point_summaries,
        expected_analyzers=expected_analyzers,
        present_analyzers=present_analyzers,
        usable_analyzers=usable_analyzers,
        total_frames=total_frames,
        usable_frames=usable_frames,
        export_status_counts=export_status_counts,
        parity_status=parity_status,
    )
    point_kpis = _build_point_kpis(point_summaries=point_summaries, samples=samples)
    qc_overview = _build_qc_overview(run_id=run_id, point_summaries=point_summaries)
    drift_summary = _build_drift_summary(samples)
    control_chart_summary = _build_control_chart_summary(samples=samples)
    analyzer_health_digest = _build_analyzer_health_digest(
        samples=samples,
        expected_analyzers=expected_analyzers,
    )
    fault_attribution_summary = _build_fault_attribution_summary(
        point_failure_reasons=point_failure_reasons,
        frame_status_counts=frame_status_counts,
        reference_quality=reference_quality,
    )
    measurement_analytics_summary = _build_measurement_analytics_summary(samples, point_summaries=point_summaries)
    offline_diagnostic_adapter_summary = summarize_offline_diagnostic_adapters(run_dir)
    if offline_diagnostic_adapter_summary:
        measurement_analytics_summary = dict(measurement_analytics_summary)
        measurement_summary = str(measurement_analytics_summary.get("summary") or "").strip()
        adapter_summary = str(offline_diagnostic_adapter_summary.get("summary") or "").strip()
        measurement_analytics_summary["offline_diagnostic_adapter_summary"] = dict(offline_diagnostic_adapter_summary)
        if adapter_summary and adapter_summary not in measurement_summary:
            measurement_analytics_summary["summary"] = (
                f"{measurement_summary} | {adapter_summary}" if measurement_summary else adapter_summary
            )
    unified_review_summary = _build_unified_review_summary(
        run_kpis=run_kpis,
        point_kpis=point_kpis,
        qc_overview=qc_overview,
        drift_summary=drift_summary,
        control_chart_summary=control_chart_summary,
        analyzer_health_digest=analyzer_health_digest,
        fault_attribution_summary=fault_attribution_summary,
        measurement_analytics_summary=measurement_analytics_summary,
        boundary=boundary,
    )
    qc_evidence_section = build_qc_evidence_section(
        reviewer_digest=dict(qc_overview.get("reviewer_digest") or {}),
        reviewer_card=dict(qc_overview.get("reviewer_card") or {}),
        run_gate=dict(qc_overview.get("run_gate") or {}),
        point_gate_summary=dict(qc_overview.get("point_gate_summary") or {}),
        decision_counts=dict(qc_overview.get("decision_counts") or {}),
        route_decision_breakdown=dict(qc_overview.get("route_decision_breakdown") or {}),
        reject_reason_taxonomy=list(qc_overview.get("reject_reason_taxonomy") or []),
        failed_check_taxonomy=list(qc_overview.get("failed_check_taxonomy") or []),
        review_sections=[dict(item) for item in list(qc_overview.get("review_sections") or []) if isinstance(item, dict)],
        summary_override=str(dict(unified_review_summary.get("qc_summary") or {}).get("summary") or "").strip() or None,
        lines_override=[
            str(item).strip()
            for item in list(dict(unified_review_summary.get("qc_summary") or {}).get("lines") or [])
            if str(item).strip()
        ],
        evidence_source=str(boundary.get("evidence_source") or "simulated_protocol"),
        evidence_state=str(boundary.get("evidence_state") or "collected"),
        not_real_acceptance_evidence=bool(boundary.get("not_real_acceptance_evidence", True)),
        acceptance_level=str(boundary.get("acceptance_level") or "offline_regression"),
        promotion_state=str(boundary.get("promotion_state") or "dry_run_only"),
    )
    config_governance_handoff = (
        build_step2_config_governance_handoff(config_safety_review or config_safety)
        if (config_safety_review or config_safety)
        else {}
    )
    point_taxonomy_summary = build_point_taxonomy_handoff(point_summaries)
    digest = {
        "summary": (
            f"coverage {len(usable_analyzers)}/{len(expected_analyzers) or len(present_analyzers)} | "
            f"reference {reference_quality['reference_quality']} | "
            f"exports {export_status_counts.get('error', 0)} error | parity {parity_status} | "
            f"质控 {str(dict(qc_overview.get('run_gate') or {}).get('status') or '--')} | "
            f"drift {drift_summary.get('overall_trend', '--')} | "
            f"faults {fault_attribution_summary.get('primary_fault', '--')}"
        ),
        "health": str(analyzer_health_digest.get("overall_status") or "attention"),
        "reviewer_summary": str(unified_review_summary.get("summary") or ""),
    }
    if offline_diagnostic_adapter_summary:
        adapter_digest = str(offline_diagnostic_adapter_summary.get("summary") or "").strip()
        if adapter_digest:
            digest["summary"] = f"{digest['summary']} | offline {adapter_digest}"
    payload = {
        "schema_version": OFFLINE_ARTIFACT_SCHEMA_VERSION,
        "artifact_type": "run_analytics_summary",
        "run_id": run_id,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        **boundary,
        "evidence_state": "collected",
        "config_safety": dict(config_safety or {}),
        "config_safety_review": dict(config_safety_review or {}),
        "config_governance_handoff": config_governance_handoff,
        "analyzer_coverage": {
            "expected_analyzers": expected_analyzers,
            "present_analyzers": present_analyzers,
            "usable_analyzers": usable_analyzers,
            "unusable_analyzers": unusable_analyzers,
            "coverage_text": f"{len(usable_analyzers)}/{len(expected_analyzers) or len(present_analyzers)}",
        },
        "frame_quality_statistics": {
            "total_frames": total_frames,
            "frames_with_data": frames_with_data,
            "usable_frames": usable_frames,
            "unusable_frames": max(total_frames - usable_frames, 0),
            "usable_frame_ratio": _safe_ratio(usable_frames, total_frames),
            "frame_status_counts": dict(frame_status_counts),
        },
        "reference_quality_statistics": reference_quality,
        "route_physical_mismatch_counts": {"total": 0, "mismatched": 0},
        "export_resilience_status": {
            "status_counts": dict(export_status_counts),
            "overall_status": "degraded" if export_status_counts.get("error", 0) else "ok",
            "failing_exports": sorted(
                name for name, payload in export_statuses.items() if str((payload or {}).get("status", "")) == "error"
            ),
        },
        "summary_parity_status": parity_status,
        "routes": routes,
        "point_failure_reasons": dict(point_failure_reasons),
        "run_kpis": run_kpis,
        "point_kpis": point_kpis,
        "qc_overview": qc_overview,
        "drift_summary": drift_summary,
        "control_chart_summary": control_chart_summary,
        "analyzer_health_digest": analyzer_health_digest,
        "fault_attribution_summary": fault_attribution_summary,
        "measurement_analytics_summary": measurement_analytics_summary,
        "unified_review_summary": unified_review_summary,
        "qc_reviewer_card": dict(qc_evidence_section.get("reviewer_card") or {}),
        "qc_evidence_section": qc_evidence_section,
        "qc_review_cards": [dict(item) for item in list(qc_evidence_section.get("cards") or []) if isinstance(item, dict)],
        "point_taxonomy_summary": point_taxonomy_summary,
        "digest": digest,
    }
    if offline_diagnostic_adapter_summary:
        payload["offline_diagnostic_adapter_summary"] = dict(offline_diagnostic_adapter_summary)
    return payload


def build_reference_quality_statistics(samples: list[Any]) -> dict[str, Any]:
    total = len(samples)
    thermometer_count = sum(1 for sample in samples if getattr(sample, "thermometer_temp_c", None) is not None)
    pressure_count = sum(1 for sample in samples if getattr(sample, "pressure_gauge_hpa", None) is not None)
    thermometer_status_counts = _status_counts(samples, "thermometer_reference_status")
    pressure_status_counts = _status_counts(samples, "pressure_reference_status")
    thermometer_status = _dominant_status(thermometer_status_counts)
    pressure_status = _dominant_status(pressure_status_counts)
    failed_statuses = {"no_response", "corrupted_ascii", "truncated_ascii", "display_interrupted", "unsupported_command"}
    degraded_statuses = {"stale", "drift", "warmup_unstable", "wrong_unit_configuration"}
    statuses = {item for item in (thermometer_status, pressure_status) if item}
    reference_quality = "missing"
    if total == 0:
        reference_quality = "missing"
    elif any(item in failed_statuses for item in statuses):
        reference_quality = "failed"
    elif any(item in degraded_statuses for item in statuses):
        reference_quality = "degraded"
    elif _safe_ratio(thermometer_count, total) >= 0.9 and _safe_ratio(pressure_count, total) >= 0.9:
        reference_quality = "healthy"
    elif thermometer_count > 0 or pressure_count > 0:
        reference_quality = "degraded"
    return {
        "reference_quality": reference_quality,
        "thermometer_reference_count": thermometer_count,
        "pressure_reference_count": pressure_count,
        "thermometer_reference_ratio": _safe_ratio(thermometer_count, total),
        "pressure_reference_ratio": _safe_ratio(pressure_count, total),
        "thermometer_reference_status": thermometer_status or ("healthy" if thermometer_count else "missing"),
        "pressure_reference_status": pressure_status or ("healthy" if pressure_count else "missing"),
        "thermometer_status_counts": thermometer_status_counts,
        "pressure_status_counts": pressure_status_counts,
        "reference_quality_trend": reference_quality,
    }


def _build_run_kpis(
    *,
    samples: list[Any],
    point_summaries: list[dict[str, Any]],
    expected_analyzers: list[str],
    present_analyzers: list[str],
    usable_analyzers: list[str],
    total_frames: int,
    usable_frames: int,
    export_status_counts: Counter[str],
    parity_status: str,
) -> dict[str, Any]:
    failure_count = sum(
        1
        for item in list(point_summaries or [])
        if str(dict(item.get("stats") or {}).get("reason") or "").strip().lower() not in {"", "passed"}
    )
    point_count = len(list(point_summaries or []))
    return {
        "sample_count": len(samples),
        "point_count": point_count,
        "completed_points": max(point_count - failure_count, 0),
        "flagged_points": failure_count,
        "expected_analyzer_count": len(expected_analyzers),
        "present_analyzer_count": len(present_analyzers),
        "usable_analyzer_count": len(usable_analyzers),
        "coverage_ratio": _safe_ratio(len(usable_analyzers), len(expected_analyzers) or len(present_analyzers) or 1),
        "usable_frame_ratio": _safe_ratio(usable_frames, total_frames),
        "export_error_count": int(export_status_counts.get("error", 0) or 0),
        "parity_status": str(parity_status or "missing"),
    }


def _build_point_kpis(*, point_summaries: list[dict[str, Any]], samples: list[Any]) -> dict[str, Any]:
    grouped: dict[str, dict[str, Any]] = {}
    for sample in list(samples or []):
        point = getattr(sample, "point", None)
        point_index = str(getattr(point, "index", "unknown"))
        row = grouped.setdefault(
            point_index,
            {
                "point_index": getattr(point, "index", None),
                "route": str(getattr(point, "route", "") or "--"),
                "temperature_c": getattr(point, "temperature_c", None),
                "co2_ppm": getattr(point, "co2_ppm", None),
                "sample_count": 0,
                "usable_sample_count": 0,
            },
        )
        row["sample_count"] += 1
        if bool(getattr(sample, "frame_usable", True)):
            row["usable_sample_count"] += 1
    status_counts = Counter()
    route_breakdown = Counter()
    rows: list[dict[str, Any]] = []
    summary_by_index: dict[int, dict[str, Any]] = {}
    for item in list(point_summaries or []):
        try:
            point_index = int(item.get("point_index"))
        except Exception:
            continue
        summary_by_index[point_index] = dict(item)
    for key in sorted(grouped, key=lambda value: (float(value) if str(value).isdigit() else float("inf"), value)):
        row = dict(grouped[key])
        point_index = row.get("point_index")
        stats = dict(summary_by_index.get(int(point_index)) or {}).get("stats", {}) if point_index is not None else {}
        reason = str(dict(stats or {}).get("reason") or "passed").strip() or "passed"
        row["reason"] = reason
        row["usable_ratio"] = _safe_ratio(int(row.get("usable_sample_count", 0) or 0), int(row.get("sample_count", 0) or 0))
        row["status"] = "flagged" if reason != "passed" else "ok"
        rows.append(row)
        status_counts[str(row["status"])] += 1
        route_breakdown[str(row.get("route") or "--")] += 1
    return {
        "rows": rows,
        "status_counts": dict(status_counts),
        "route_breakdown": dict(route_breakdown),
        "point_count": len(rows),
    }


def _split_guard_flags(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return [item for item in str(value or "").split(",") if item.strip()]


def _postseal_guard_status_review(point_summaries: list[dict[str, Any]]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    status_counts = Counter()
    active_points = 0
    rebound_veto_count = 0
    physical_flagged_count = 0
    late_rebound_flagged_count = 0
    stale_flagged_count = 0

    for item in list(point_summaries or []):
        point = dict(item.get("point") or {})
        stats = dict(item.get("stats") or {})
        guard_enabled = bool(stats.get("co2_postseal_quality_guards_enabled", False))
        guard_status = str(stats.get("postseal_guard_status") or "skipped").strip().lower() or "skipped"
        guard_flags = _split_guard_flags(stats.get("postseal_guard_flags"))
        if guard_enabled:
            active_points += 1
        if bool(stats.get("postseal_rebound_veto")):
            rebound_veto_count += 1
        if str(stats.get("postseal_physical_qc_status") or "").strip().lower() == "fail":
            physical_flagged_count += 1
        if str(stats.get("postsample_late_rebound_status") or "").strip().lower() in {"warn", "fail"}:
            late_rebound_flagged_count += 1
        if "pressure_gauge_stale_ratio" in guard_flags:
            stale_flagged_count += 1
        status_counts[guard_status] += 1
        rows.append(
            {
                "point_index": point.get("index"),
                "route": str(point.get("route") or "--"),
                "temperature_c": point.get("temperature_c"),
                "co2_ppm": point.get("co2_ppm"),
                "pressure_target_label": str(point.get("pressure_target_label") or point.get("pressure_hpa") or "--"),
                "guard_enabled": guard_enabled,
                "guard_status": guard_status,
                "guard_flags": guard_flags,
                "guard_reason": str(stats.get("postseal_guard_reason") or "").strip(),
                "postseal_rebound_veto": bool(stats.get("postseal_rebound_veto")),
                "postseal_physical_qc_status": str(stats.get("postseal_physical_qc_status") or "skipped"),
                "postsample_late_rebound_status": str(stats.get("postsample_late_rebound_status") or "skipped"),
                "pressure_gauge_stale_ratio": stats.get("pressure_gauge_stale_ratio"),
            }
        )

    if active_points == 0:
        summary = "CO2 post-seal 守卫未启用或当前运行未命中低压密封点"
        lines = [
            summary,
            "证据边界: 仅限 simulation/offline/headless evidence，不代表 real acceptance evidence。",
        ]
    else:
        summary = (
            f"CO2 post-seal 守卫 {active_points} 点 | rebound veto {rebound_veto_count} | "
            f"physical {physical_flagged_count} | late rebound {late_rebound_flagged_count} | stale gauge {stale_flagged_count}"
        )
        top_rows = [
            row
            for row in rows
            if row["guard_enabled"] and (row["guard_status"] != "pass" or row["guard_flags"])
        ][:3]
        lines = [summary]
        for row in top_rows:
            flags = ", ".join(row["guard_flags"]) or "--"
            lines.append(
                f"点 {row['point_index']} | 路由 {row['route']} | 压力 {row['pressure_target_label']} | "
                f"状态 {row['guard_status']} | flags {flags}"
            )
        lines.append("证据边界: 仅限 simulation/offline/headless evidence，不代表 real acceptance evidence。")
    return {
        "rows": rows,
        "status_counts": dict(status_counts),
        "active_point_count": active_points,
        "rebound_veto_count": rebound_veto_count,
        "physical_flagged_count": physical_flagged_count,
        "late_rebound_flagged_count": late_rebound_flagged_count,
        "stale_flagged_count": stale_flagged_count,
        "summary": summary,
        "lines": _unique_review_lines(lines),
        "section": {
            "id": "co2_postseal_quality",
            "title": "CO2 post-seal 守卫",
            "summary": summary,
            "lines": _unique_review_lines(lines),
        },
    }


def _build_qc_overview(*, run_id: str, point_summaries: list[dict[str, Any]]) -> dict[str, Any]:
    point_rows: list[dict[str, Any]] = []
    for item in list(point_summaries or []):
        point = dict(item.get("point") or {})
        stats = dict(item.get("stats") or {})
        point_rows.append(
            {
                "point_index": point.get("index"),
                "route": str(point.get("route") or "--"),
                "temperature_c": point.get("temperature_c"),
                "co2_ppm": point.get("co2_ppm"),
                "pressure_target_label": point.get("pressure_target_label"),
                "quality_score": stats.get("quality_score"),
                "valid": stats.get("valid"),
                "recommendation": stats.get("recommendation"),
                "reason": str(stats.get("reason") or "passed").strip() or "passed",
                "failed_checks": list(stats.get("failed_checks") or []),
                "postseal_guard_status": stats.get("postseal_guard_status"),
                "postseal_guard_flags": _split_guard_flags(stats.get("postseal_guard_flags")),
                "pressure_gauge_stale_ratio": stats.get("pressure_gauge_stale_ratio"),
            }
        )
    payload = build_qc_review_payload(point_rows=point_rows, run_id=run_id)
    guard_review = _postseal_guard_status_review(point_summaries)
    sections = [dict(item) for item in list(payload.get("review_sections") or []) if isinstance(item, dict)]
    sections.append(dict(guard_review.get("section") or {}))
    reviewer_card = dict(payload.get("reviewer_card") or {})
    reviewer_card["sections"] = sections
    reviewer_card["summary"] = str(reviewer_card.get("summary") or "").strip() or str(guard_review.get("summary") or "")
    reviewer_card_lines = _unique_review_lines(
        list(reviewer_card.get("lines") or []) + [str(guard_review.get("summary") or "")] + list(guard_review.get("lines") or [])
    )
    reviewer_card["lines"] = reviewer_card_lines
    payload["reviewer_card"] = reviewer_card
    payload["review_sections"] = sections
    payload["review_card_lines"] = reviewer_card_lines
    payload["postseal_guard_review"] = guard_review
    payload["evidence_section"] = build_qc_evidence_section(
        reviewer_digest=dict(payload.get("reviewer_digest") or {}),
        reviewer_card=reviewer_card,
        run_gate=dict(payload.get("run_gate") or {}),
        point_gate_summary=dict(payload.get("point_gate_summary") or {}),
        decision_counts=dict(payload.get("decision_counts") or {}),
        route_decision_breakdown=dict(payload.get("route_decision_breakdown") or {}),
        reject_reason_taxonomy=list(payload.get("reject_reason_taxonomy") or []),
        failed_check_taxonomy=list(payload.get("failed_check_taxonomy") or []),
        review_sections=sections,
        summary_override=str(reviewer_card.get("summary") or "").strip() or None,
        lines_override=reviewer_card_lines,
    )
    return payload


def _build_drift_summary(samples: list[Any]) -> dict[str, Any]:
    by_analyzer: dict[str, list[float]] = defaultdict(list)
    for sample in list(samples or []):
        analyzer = str(getattr(sample, "analyzer_id", "") or "").strip().upper() or "UNSPECIFIED"
        value = getattr(sample, "co2_ppm", None)
        if value is None:
            value = getattr(sample, "h2o_mmol", None)
        if value is None:
            value = getattr(sample, "pressure_hpa", None)
        if value is None:
            continue
        by_analyzer[analyzer].append(float(value))
    analyzer_rows = []
    deltas = []
    for analyzer, values in sorted(by_analyzer.items()):
        delta = 0.0 if len(values) < 2 else round(values[-1] - values[0], 6)
        deltas.append(delta)
        analyzer_rows.append(
            {
                "analyzer_id": analyzer,
                "start_value": values[0],
                "end_value": values[-1],
                "delta": delta,
                "trend": "stable" if abs(delta) < 1e-6 else ("increasing" if delta > 0 else "decreasing"),
            }
        )
    max_abs_delta = max((abs(value) for value in deltas), default=0.0)
    if max_abs_delta < 1e-6:
        overall_trend = "stable"
    elif sum(deltas) >= 0.0:
        overall_trend = "increasing"
    else:
        overall_trend = "decreasing"
    return {
        "overall_trend": overall_trend,
        "max_abs_delta": round(max_abs_delta, 6),
        "analyzers": analyzer_rows,
        "summary": f"drift {overall_trend} | max_delta {round(max_abs_delta, 3):g}",
    }


def _build_control_chart_summary(*, samples: list[Any]) -> dict[str, Any]:
    usable_series = [1.0 if bool(getattr(sample, "frame_usable", True)) else 0.0 for sample in list(samples or [])]
    metric = _spc_metric("usable_frame_ratio", usable_series)
    return {
        "metric": metric,
        "status": _control_limit_status(usable_series),
        "sample_count": len(usable_series),
        "summary": (
            f"control {str(_control_limit_status(usable_series) or '--')} | "
            f"latest {metric.get('latest') if metric.get('latest') is not None else '--'}"
        ),
    }


def _build_analyzer_health_digest(*, samples: list[Any], expected_analyzers: list[str]) -> dict[str, Any]:
    grouped: dict[str, dict[str, int]] = {}
    for sample in list(samples or []):
        analyzer = str(getattr(sample, "analyzer_id", "") or "").strip().upper() or "UNSPECIFIED"
        counters = grouped.setdefault(analyzer, {"total": 0, "usable": 0, "missing_data": 0})
        counters["total"] += 1
        if bool(getattr(sample, "frame_usable", True)):
            counters["usable"] += 1
        if not bool(getattr(sample, "frame_has_data", True)):
            counters["missing_data"] += 1
    rows = []
    statuses = []
    for analyzer in sorted(set(expected_analyzers) | set(grouped.keys())):
        counters = grouped.get(analyzer, {"total": 0, "usable": 0, "missing_data": 0})
        usable_ratio = _safe_ratio(counters["usable"], counters["total"] or 1)
        if counters["total"] == 0:
            status = "missing"
        elif usable_ratio >= 0.95 and counters["missing_data"] == 0:
            status = "healthy"
        elif usable_ratio >= 0.7:
            status = "attention"
        else:
            status = "failed"
        statuses.append(status)
        rows.append(
            {
                "analyzer_id": analyzer,
                "status": status,
                "frame_count": counters["total"],
                "usable_frame_ratio": usable_ratio,
                "missing_data_count": counters["missing_data"],
            }
        )
    if "failed" in statuses:
        overall_status = "failed"
    elif "attention" in statuses or "missing" in statuses:
        overall_status = "attention"
    else:
        overall_status = "healthy"
    return {
        "overall_status": overall_status,
        "rows": rows,
        "summary": f"analyzers {len(rows)} | overall {overall_status}",
    }


def _build_fault_attribution_summary(
    *,
    point_failure_reasons: Counter[str],
    frame_status_counts: Counter[str],
    reference_quality: dict[str, Any],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for code, count in point_failure_reasons.items():
        if str(code or "").strip().lower() == "passed":
            continue
        rows.append({"source": "point_qc", "code": str(code), "count": int(count), "impact": "point_quality"})
    for code, count in frame_status_counts.items():
        normalized = str(code or "").strip().lower()
        if normalized in {"", "ok", "unspecified"}:
            continue
        rows.append({"source": "frame_status", "code": str(code), "count": int(count), "impact": "frame_quality"})
    reference_state = str(reference_quality.get("reference_quality") or "").strip().lower()
    if reference_state and reference_state not in {"healthy", "missing"}:
        rows.append({"source": "reference_quality", "code": reference_state, "count": 1, "impact": "reference_quality"})
    rows.sort(key=lambda item: (-int(item["count"]), str(item["source"]), str(item["code"])))
    primary_fault = str(rows[0]["code"]) if rows else "none"
    return {
        "primary_fault": primary_fault,
        "rows": rows,
        "summary": f"faults {primary_fault} | categories {len(rows)}",
    }


def _build_measurement_analytics_summary(
    samples: list[Any],
    *,
    point_summaries: list[dict[str, Any]],
) -> dict[str, Any]:
    route_counts = Counter(
        str(getattr(getattr(sample, "point", None), "route", "") or "").strip().lower() or "--"
        for sample in list(samples or [])
    )
    usable_frames = sum(1 for sample in list(samples or []) if bool(getattr(sample, "frame_usable", True)))
    total_frames = len(list(samples or []))
    guard_review = _postseal_guard_status_review(point_summaries)
    return {
        "frame_count": total_frames,
        "usable_frame_count": usable_frames,
        "usable_frame_ratio": _safe_ratio(usable_frames, total_frames),
        "route_counts": dict(route_counts),
        "postseal_guard_review": guard_review,
        "summary": (
            f"frames {usable_frames}/{total_frames} usable | "
            f"routes {', '.join(sorted(route_counts)) or '--'} | "
            f"{str(guard_review.get('summary') or '--')}"
        ),
    }


def _build_unified_review_summary(
    *,
    run_kpis: dict[str, Any],
    point_kpis: dict[str, Any],
    qc_overview: dict[str, Any],
    drift_summary: dict[str, Any],
    control_chart_summary: dict[str, Any],
    analyzer_health_digest: dict[str, Any],
    fault_attribution_summary: dict[str, Any],
    measurement_analytics_summary: dict[str, Any],
    boundary: dict[str, Any],
) -> dict[str, Any]:
    qc_digest = dict(qc_overview.get("reviewer_digest") or {})
    qc_gate = dict(qc_overview.get("run_gate") or {})
    point_gate = dict(qc_overview.get("point_gate_summary") or {})
    decision_counts = dict(qc_overview.get("decision_counts") or {})
    reject_reason_taxonomy = list(qc_overview.get("reject_reason_taxonomy") or [])
    failed_check_taxonomy = list(qc_overview.get("failed_check_taxonomy") or [])
    flagged_routes = ", ".join(
        str(item) for item in list(point_gate.get("flagged_routes") or []) if str(item).strip()
    ) or "--"
    top_reject_reason = str((reject_reason_taxonomy[0] or {}).get("code") or "--") if reject_reason_taxonomy else "--"
    top_failed_check = str((failed_check_taxonomy[0] or {}).get("code") or "--") if failed_check_taxonomy else "--"
    qc_summary = {
        "summary": str(
            qc_digest.get("summary")
            or (
                f"质控门禁 {str(qc_gate.get('status') or '--')} | "
                f"点级门禁 {str(point_gate.get('status') or '--')} | "
                f"结果分级 通过 {int(decision_counts.get('pass', 0) or 0)} / "
                f"预警 {int(decision_counts.get('warn', 0) or 0)} / "
                f"拒绝 {int(decision_counts.get('reject', 0) or 0)} / "
                f"跳过 {int(decision_counts.get('skipped', 0) or 0)}"
            )
        ),
        "lines": _unique_review_lines(
            [
                qc_digest.get("summary"),
                f"运行门禁: {str(qc_gate.get('status') or '--')} | 原因: {str(qc_gate.get('reason') or '--')}",
                (
                    f"点级门禁: {str(point_gate.get('status') or '--')} | "
                    f"关注路由: {flagged_routes} | "
                    f"非通过点数: {int(point_gate.get('flagged_point_count', 0) or 0)}"
                ),
                (
                    f"结果分级: 通过 {int(decision_counts.get('pass', 0) or 0)} / "
                    f"预警 {int(decision_counts.get('warn', 0) or 0)} / "
                    f"拒绝 {int(decision_counts.get('reject', 0) or 0)} / "
                    f"跳过 {int(decision_counts.get('skipped', 0) or 0)}"
                ),
                f"主要拒绝原因: {top_reject_reason} | 失败检查: {top_failed_check}",
                *list(qc_digest.get("lines") or []),
            ]
        ),
        "reviewer_card": dict(qc_overview.get("reviewer_card") or {}),
        "reviewer_card_lines": list(dict(qc_overview.get("reviewer_card") or {}).get("lines") or []),
        "review_sections": [dict(item) for item in list(qc_overview.get("review_sections") or []) if isinstance(item, dict)],
    }
    analytics_summary = {
        "summary": (
            f"离线分析覆盖 {int(run_kpis.get('usable_analyzer_count', 0) or 0)}/"
            f"{int(run_kpis.get('expected_analyzer_count', 0) or 0)} | "
            f"点位 {int(point_kpis.get('point_count', 0) or 0)} | "
            f"漂移 {str(drift_summary.get('overall_trend') or '--')} | "
            f"控制图 {str(control_chart_summary.get('status') or '--')} | "
            f"健康 {str(analyzer_health_digest.get('overall_status') or '--')} | "
            f"主故障 {str(fault_attribution_summary.get('primary_fault') or '--')}"
        ),
        "lines": _unique_review_lines(
            [
                measurement_analytics_summary.get("summary"),
                (
                    f"离线分析覆盖: {int(run_kpis.get('usable_analyzer_count', 0) or 0)}/"
                    f"{int(run_kpis.get('expected_analyzer_count', 0) or 0)} | "
                    f"点位 {int(point_kpis.get('point_count', 0) or 0)}"
                ),
                f"漂移趋势: {str(drift_summary.get('overall_trend') or '--')}",
                f"控制图状态: {str(control_chart_summary.get('status') or '--')}",
                f"分析仪健康: {str(analyzer_health_digest.get('overall_status') or '--')}",
                f"主故障归因: {str(fault_attribution_summary.get('primary_fault') or '--')}",
            ]
        ),
    }
    boundary_note = (
        f"证据边界: evidence_source={str(boundary.get('evidence_source') or '--')} | "
        "仅限 simulation/offline/headless evidence，不代表 real acceptance evidence。"
    )
    summary = (
        f"离线分析摘要：点位 {int(point_kpis.get('point_count', 0) or 0)}，"
        f"覆盖 {int(run_kpis.get('usable_analyzer_count', 0) or 0)}/{int(run_kpis.get('expected_analyzer_count', 0) or 0)}，"
        f"质控 {str(qc_gate.get('status') or '--')}，"
        f"漂移 {str(drift_summary.get('overall_trend') or '--')}，"
        f"控制图 {str(control_chart_summary.get('status') or '--')}，"
        f"健康 {str(analyzer_health_digest.get('overall_status') or '--')}，"
        f"主故障 {str(fault_attribution_summary.get('primary_fault') or '--')}。"
    )
    reviewer_notes = _unique_review_lines(
        [
            summary,
            *list(qc_summary.get("lines") or []),
            *list(analytics_summary.get("lines") or []),
            boundary_note,
        ]
    )
    return {
        **boundary,
        "reviewer_title": "离线审阅摘要",
        "summary": summary,
        "reviewer_notes": reviewer_notes,
        "qc_summary": qc_summary,
        "analytics_summary": analytics_summary,
        "boundary_note": boundary_note,
        "reviewer_sections": [
            {"id": "qc", "title": "质控审阅", "summary": qc_summary["summary"], "lines": list(qc_summary["lines"])},
            {
                "id": "analytics",
                "title": "离线分析审阅",
                "summary": analytics_summary["summary"],
                "lines": list(analytics_summary["lines"]),
            },
            {"id": "boundary", "title": "证据边界", "summary": boundary_note, "lines": [boundary_note]},
        ],
    }


def _status_counts(samples: list[Any], field: str) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for sample in samples:
        value = getattr(sample, field, None)
        text = str(value or "").strip()
        if not text:
            continue
        counts[text] += 1
    return dict(counts)


def _dominant_status(counts: dict[str, int]) -> str:
    if not counts:
        return ""
    return sorted(counts.items(), key=lambda item: (-int(item[1]), item[0]))[0][0]


def build_lineage_summary(
    *,
    run_id: str,
    run_dir: Path,
    output_files: list[str],
    export_statuses: dict[str, dict[str, Any]],
    versions: dict[str, Any],
) -> dict[str, Any]:
    artifacts = []
    for name, payload in sorted(export_statuses.items()):
        artifacts.append(
            {
                "artifact_id": f"{run_id}:{name}",
                "artifact_name": str(name),
                "role": str((payload or {}).get("role", "") or "unclassified"),
                "path": str((payload or {}).get("path", "") or ""),
                "present": bool((payload or {}).get("path")),
                "parent_run_id": None,
                "source_artifact_ids": [f"{run_id}:source_points"],
            }
        )
    return {
        "schema_version": OFFLINE_ARTIFACT_SCHEMA_VERSION,
        "artifact_type": "lineage_summary",
        "run_id": run_id,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "parent_run_id": None,
        "source_artifact_ids": [f"{run_id}:source_points"],
        "config_version": versions.get("config_version"),
        "points_version": versions.get("points_version"),
        "profile_version": versions.get("profile_version"),
        "software_build_id": versions.get("software_build_id"),
        "artifacts": artifacts,
        "known_output_files": sorted({str(item) for item in output_files if str(item).strip()}),
        "expected_artifacts": [
            str(run_dir / ACCEPTANCE_PLAN_FILENAME),
            str(run_dir / ANALYTICS_SUMMARY_FILENAME),
            str(run_dir / TREND_REGISTRY_FILENAME),
            str(run_dir / LINEAGE_SUMMARY_FILENAME),
            str(run_dir / EVIDENCE_REGISTRY_FILENAME),
            str(run_dir / COEFFICIENT_REGISTRY_FILENAME),
        ],
    }


def build_coefficient_registry(
    *,
    run_id: str,
    run_dir: Path,
    samples: list[Any],
    export_statuses: dict[str, dict[str, Any]],
    versions: dict[str, Any],
    acceptance_plan: dict[str, Any],
    analytics_summary: dict[str, Any],
) -> dict[str, Any]:
    coeff_payload = dict(export_statuses.get("coefficient_report") or {})
    coeff_path = str(coeff_payload.get("path") or "")
    analyzer_ids = sorted(
        {
            str(getattr(sample, "analyzer_id", "") or "").strip().upper()
            for sample in samples
            if str(getattr(sample, "analyzer_id", "") or "").strip()
        }
    )
    routes = sorted(
        {
            str(getattr(getattr(sample, "point", None), "route", "") or "").strip().lower()
            for sample in samples
            if str(getattr(getattr(sample, "point", None), "route", "") or "").strip()
        }
    )
    temps = [
        float(getattr(getattr(sample, "point", None), "temperature_c"))
        for sample in samples
        if getattr(getattr(sample, "point", None), "temperature_c", None) is not None
    ]
    co2_values = [
        float(getattr(getattr(sample, "point", None), "co2_ppm"))
        for sample in samples
        if getattr(getattr(sample, "point", None), "co2_ppm", None) is not None
    ]
    entries = [
        {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "analyzer_id": analyzer_id,
            "coefficient_version": _path_version(coeff_path or (run_dir / "calibration_coefficients.xlsx"), prefix="coef"),
            "source_run": run_id,
            "source_artifact_ids": [f"{run_id}:coefficient_report", f"{run_id}:run_summary", f"{run_id}:results_json"],
            "route_range": routes,
            "temperature_range_c": {"min": min(temps) if temps else None, "max": max(temps) if temps else None},
            "source_range": {"co2_ppm_min": min(co2_values) if co2_values else None, "co2_ppm_max": max(co2_values) if co2_values else None},
            "parity_status": analytics_summary.get("summary_parity_status"),
            "quality_status": analytics_summary.get("reference_quality_statistics", {}).get("reference_quality"),
            "config_version": versions.get("config_version"),
            "points_version": versions.get("points_version"),
            "profile_version": versions.get("profile_version"),
            "software_build_id": versions.get("software_build_id"),
            "not_real_acceptance_evidence": acceptance_plan.get("not_real_acceptance_evidence", True),
        }
        for analyzer_id in (analyzer_ids or ["UNSPECIFIED"])
    ]
    return {
        "schema_version": OFFLINE_ARTIFACT_SCHEMA_VERSION,
        "artifact_type": "coefficient_registry",
        "run_id": run_id,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "available": bool(coeff_path),
        "report_path": coeff_path,
        "status": str(coeff_payload.get("status") or "missing"),
        "not_real_acceptance_evidence": acceptance_plan.get("not_real_acceptance_evidence", True),
        "entries": entries,
    }


def build_trend_registry(
    *,
    run_id: str,
    run_dir: Path,
    output_dir: Path,
    samples: list[Any],
    analytics_summary: dict[str, Any],
) -> dict[str, Any]:
    current_entry = _current_history_entry(run_id=run_id, samples=samples, analytics_summary=analytics_summary)
    sibling_entries = []
    if output_dir.exists():
        for child in sorted(output_dir.iterdir()):
            if not child.is_dir() or child == run_dir:
                continue
            analytics_path = child / ANALYTICS_SUMMARY_FILENAME
            if not analytics_path.exists():
                continue
            try:
                payload = json.loads(analytics_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            sibling_entries.append(
                {
                    "run_id": str(payload.get("run_id") or child.name),
                    "start_time": str(payload.get("generated_at") or ""),
                    "coverage_ratio": float(_text_ratio_to_float(((payload.get("analyzer_coverage") or {}).get("coverage_text"))) or 0.0),
                    "usable_frame_ratio": float(((payload.get("frame_quality_statistics") or {}).get("usable_frame_ratio")) or 0.0),
                    "reference_quality_score": _reference_quality_score((payload.get("reference_quality_statistics") or {}).get("reference_quality")),
                }
            )
    history = sibling_entries + [current_entry]
    analyzers = sorted(
        {
            str(getattr(sample, "analyzer_id", "") or "").strip().upper()
            for sample in samples
            if str(getattr(sample, "analyzer_id", "") or "").strip()
        }
    )
    analyzer_entries = []
    for analyzer_id in analyzers or ["UNSPECIFIED"]:
        coverage_series = [float(item.get("coverage_ratio") or 0.0) for item in history]
        usable_series = [float(item.get("usable_frame_ratio") or 0.0) for item in history]
        reference_series = [float(item.get("reference_quality_score") or 0.0) for item in history]
        analyzer_entries.append(
            {
                "analyzer_id": analyzer_id,
                "history": history,
                "drift_indicator": _drift_indicator(usable_series),
                "spc_metric": _spc_metric("usable_frame_ratio", usable_series),
                "control_limit_status": _control_limit_status(usable_series),
                "reference_quality_trend": _trend_label(reference_series),
                "coverage_trend": _trend_label(coverage_series),
            }
        )
    route_groups = []
    grouped = defaultdict(list)
    for sample in samples:
        point = getattr(sample, "point", None)
        route = str(getattr(point, "route", "") or "").strip().lower()
        temp = getattr(point, "temperature_c", None)
        source = (
            f"co2:{float(getattr(point, 'co2_ppm')):g}"
            if getattr(point, "co2_ppm", None) is not None
            else f"h2o:{float(getattr(point, 'humidity_pct')):g}"
            if getattr(point, "humidity_pct", None) is not None
            else "unspecified"
        )
        grouped[(route, temp, source)].append(sample)
    for (route, temp, source), group_samples in sorted(grouped.items(), key=lambda item: (item[0][0], item[0][1] or 0.0, item[0][2])):
        usable_series = [1.0 if bool(getattr(item, "frame_usable", True)) else 0.0 for item in group_samples]
        reference_series = [
            1.0
            if getattr(item, "thermometer_temp_c", None) is not None and getattr(item, "pressure_gauge_hpa", None) is not None
            else 0.0
            for item in group_samples
        ]
        route_groups.append(
            {
                "route": route,
                "temperature_c": temp,
                "source": source,
                "history": [
                    {
                        "run_id": run_id,
                        "start_time": current_entry["start_time"],
                        "usable_frame_ratio": _safe_ratio(sum(usable_series), len(usable_series)),
                        "reference_quality_score": _safe_ratio(sum(reference_series), len(reference_series)),
                    }
                ],
                "drift_indicator": _drift_indicator(usable_series),
                "spc_metric": _spc_metric("usable_frame_ratio", usable_series),
                "control_limit_status": _control_limit_status(usable_series),
                "reference_quality_trend": _trend_label(reference_series),
                "coverage_trend": _trend_label(usable_series),
            }
        )
    return {
        "schema_version": OFFLINE_ARTIFACT_SCHEMA_VERSION,
        "artifact_type": "trend_registry",
        "run_id": run_id,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "history_window": {"run_count": len(history)},
        "analyzers": analyzer_entries,
        "route_temp_source_groups": route_groups,
    }


def build_run_evidence_registry(
    *,
    run_id: str,
    run_dir: Path,
    export_statuses: dict[str, dict[str, Any]],
    versions: dict[str, Any],
    acceptance_plan: dict[str, Any],
    analytics_summary: dict[str, Any],
    coefficient_registry: dict[str, Any],
    trend_registry: dict[str, Any],
    config_safety: dict[str, Any] | None = None,
    config_safety_review: dict[str, Any] | None = None,
) -> dict[str, Any]:
    boundary = build_user_visible_evidence_boundary(
        evidence_source=acceptance_plan.get("evidence_source"),
        not_real_acceptance_evidence=acceptance_plan.get("not_real_acceptance_evidence"),
        acceptance_level=acceptance_plan.get("acceptance_level"),
        promotion_state=acceptance_plan.get("promotion_state"),
    )
    entries = []
    for name, payload in sorted(export_statuses.items()):
        entries.append(
            _registry_entry(
                artifact_id=f"{run_id}:{name}",
                artifact_name=str(name),
                role=str((payload or {}).get("role", "") or "unclassified"),
                path=str((payload or {}).get("path", "") or ""),
                run_id=run_id,
                versions=versions,
                acceptance_plan=acceptance_plan,
                dimensions={
                    "suite": "",
                    "scenario": "",
                    "analyzer": [],
                    "route": list(analytics_summary.get("routes") or []),
                    "temp": [],
                },
                source_artifact_ids=[f"{run_id}:source_points"],
            )
        )
    entries.extend(
        [
            _registry_entry(
                artifact_id=f"{run_id}:acceptance_plan",
                artifact_name="acceptance_plan",
                role="execution_summary",
                path=str(run_dir / ACCEPTANCE_PLAN_FILENAME),
                run_id=run_id,
                versions=versions,
                acceptance_plan=acceptance_plan,
                dimensions={"suite": "", "scenario": "", "analyzer": [], "route": list(analytics_summary.get("routes") or []), "temp": []},
                source_artifact_ids=[f"{run_id}:run_summary", f"{run_id}:manifest"],
            ),
            _registry_entry(
                artifact_id=f"{run_id}:analytics_summary",
                artifact_name="analytics_summary",
                role="diagnostic_analysis",
                path=str(run_dir / ANALYTICS_SUMMARY_FILENAME),
                run_id=run_id,
                versions=versions,
                acceptance_plan=acceptance_plan,
                dimensions={"suite": "", "scenario": "", "analyzer": list((analytics_summary.get("analyzer_coverage") or {}).get("usable_analyzers") or []), "route": list(analytics_summary.get("routes") or []), "temp": []},
                source_artifact_ids=[f"{run_id}:results_json", f"{run_id}:points_readable"],
            ),
            _registry_entry(
                artifact_id=f"{run_id}:trend_registry",
                artifact_name="trend_registry",
                role="diagnostic_analysis",
                path=str(run_dir / TREND_REGISTRY_FILENAME),
                run_id=run_id,
                versions=versions,
                acceptance_plan=acceptance_plan,
                dimensions={"suite": "", "scenario": "", "analyzer": [item.get("analyzer_id") for item in trend_registry.get("analyzers", [])], "route": [item.get("route") for item in trend_registry.get("route_temp_source_groups", [])], "temp": [item.get("temperature_c") for item in trend_registry.get("route_temp_source_groups", [])]},
                source_artifact_ids=[f"{run_id}:analytics_summary"],
            ),
            _registry_entry(
                artifact_id=f"{run_id}:coefficient_registry",
                artifact_name="coefficient_registry",
                role="formal_analysis",
                path=str(run_dir / COEFFICIENT_REGISTRY_FILENAME),
                run_id=run_id,
                versions=versions,
                acceptance_plan=acceptance_plan,
                dimensions={"suite": "", "scenario": "", "analyzer": [item.get("analyzer_id") for item in coefficient_registry.get("entries", [])], "route": list({route for item in coefficient_registry.get("entries", []) for route in list(item.get("route_range") or [])}), "temp": []},
                source_artifact_ids=[f"{run_id}:coefficient_report", f"{run_id}:run_summary"],
            ),
        ]
    )
    payload = {
        "schema_version": OFFLINE_ARTIFACT_SCHEMA_VERSION,
        "artifact_type": "evidence_registry",
        "run_id": run_id,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        **boundary,
        "acceptance_scope": acceptance_plan.get("acceptance_scope"),
        "review_state": acceptance_plan.get("review_state"),
        "approval_state": acceptance_plan.get("approval_state"),
        "ready_for_promotion": acceptance_plan.get("ready_for_promotion"),
        "entries": entries,
        "indexes": build_registry_indexes(entries),
    }
    if isinstance(config_safety, dict) and config_safety:
        payload["config_safety"] = dict(config_safety)
    if isinstance(config_safety_review, dict) and config_safety_review:
        payload["config_safety_review"] = dict(config_safety_review)
    return payload


def build_suite_analytics_summary(summary: dict[str, Any]) -> dict[str, Any]:
    cases = [dict(item) for item in list(summary.get("cases") or [])]
    failure_types = Counter(str(item.get("failure_type") or "none") for item in cases if not bool(item.get("ok", False)))
    failure_phases = Counter(str(item.get("failure_phase") or "--") for item in cases if not bool(item.get("ok", False)))
    total = len(cases)
    passed = sum(1 for item in cases if bool(item.get("ok", False)))
    return {
        "schema_version": OFFLINE_ARTIFACT_SCHEMA_VERSION,
        "artifact_type": "suite_analytics_summary",
        "suite": str(summary.get("suite") or "--"),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "pass_rates": {"suite_pass_rate": _safe_ratio(passed, total), "passed": passed, "failed": max(total - passed, 0), "total": total},
        "case_status_counts": dict(Counter(str(item.get("status") or "--") for item in cases)),
        "case_kind_counts": dict(Counter(str(item.get("kind") or "--") for item in cases)),
        "common_failure_types": dict(failure_types),
        "top_failure_phases": dict(failure_phases),
        "evidence_sources_present": _normalized_review_evidence_sources(
            [item.get("evidence_source") for item in cases]
        ),
        "digest": {
            "summary": f"{passed}/{total} passed | top failures {', '.join(list(failure_types)[:3]) or 'none'}",
            "health": "healthy" if passed == total else "attention",
        },
    }


def build_suite_evidence_registry(
    summary: dict[str, Any],
    *,
    analytics_summary: dict[str, Any],
    acceptance_plan: dict[str, Any],
) -> dict[str, Any]:
    suite = str(summary.get("suite") or "--")
    boundary = build_user_visible_evidence_boundary(
        evidence_source=acceptance_plan.get("evidence_source"),
        not_real_acceptance_evidence=acceptance_plan.get("not_real_acceptance_evidence"),
        acceptance_level=acceptance_plan.get("acceptance_level"),
        promotion_state=acceptance_plan.get("promotion_state"),
    )
    entries = []
    for case in list(summary.get("cases") or []):
        entries.append(
            {
                "artifact_id": f"{suite}:{case.get('name')}",
                "artifact_name": str(case.get("name") or "--"),
                "role": "diagnostic_analysis",
                "path": str(case.get("artifact_dir") or ""),
                "present": bool(case.get("artifact_dir")),
                "run_id": str(case.get("parent_run_id") or suite),
                "parent_run_id": str(case.get("parent_run_id") or ""),
                "evidence_source": _normalize_review_evidence_source(case.get("evidence_source")),
                "evidence_state": str(case.get("evidence_state") or "--"),
                "acceptance_level": "offline_regression",
                "acceptance_scope": "suite_case",
                "promotion_state": "dry_run_only",
                "review_state": "pending",
                "approval_state": "blocked",
                "ready_for_promotion": False,
                "config_version": "",
                "points_version": "",
                "profile_version": "",
                "software_build_id": "",
                "source_artifact_ids": list(case.get("source_artifact_ids") or []),
                "dimensions": {"suite": suite, "scenario": str(case.get("name") or "--"), "analyzer": [], "route": [], "temp": []},
                "details": {"status": str(case.get("status") or "--"), "risk_level": str(case.get("risk_level") or "--"), "failure_type": str(case.get("failure_type") or "--")},
            }
        )
    return {
        "schema_version": OFFLINE_ARTIFACT_SCHEMA_VERSION,
        "artifact_type": "suite_evidence_registry",
        "suite": suite,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        **boundary,
        "acceptance_scope": acceptance_plan.get("acceptance_scope"),
        "review_state": acceptance_plan.get("review_state"),
        "approval_state": acceptance_plan.get("approval_state"),
        "ready_for_promotion": acceptance_plan.get("ready_for_promotion"),
        "entries": entries,
        "indexes": build_registry_indexes(entries),
        "digest": analytics_summary.get("digest", {}),
    }


def build_suite_case_metadata(case: dict[str, Any], *, suite_name: str) -> dict[str, Any]:
    kind = str(case.get("kind") or "")
    details = dict(case.get("details") or {})
    report_json = str(details.get("report_json") or "")
    report_payload = {}
    if report_json:
        try:
            report_payload = json.loads(Path(report_json).read_text(encoding="utf-8"))
        except Exception:
            report_payload = {}
    evidence_source = "diagnostic"
    evidence_state = "collected"
    if kind == "scenario":
        evidence_source = "simulated_protocol"
    elif kind == "replay":
        evidence_source = "replay"
    compare_status = str(case.get("status") or report_payload.get("compare_status") or report_payload.get("status") or "--")
    route_execution = dict(report_payload.get("route_execution_summary") or {})
    reference_quality = str((report_payload.get("reference_quality") or {}).get("reference_quality") or "unknown")
    route_mismatch = relay_mismatch_present(route_execution)
    failure_phase = str(report_payload.get("first_failure_phase") or route_execution.get("first_failure_phase") or case.get("name") or "")
    if evidence_source == "replay" and ("stale" in case.get("name", "") or "stale" in compare_status.lower()):
        evidence_state = "stale"
    if "superseded" in compare_status.lower():
        evidence_state = "superseded"
    parent_run_id = Path(str(case.get("artifact_dir") or "")).name if str(case.get("artifact_dir") or "").strip() else ""
    source_artifact_ids = []
    if parent_run_id:
        source_artifact_ids = [
            f"{parent_run_id}:report_json",
            f"{parent_run_id}:report_markdown",
        ]
    risk_level = "low"
    if not bool(case.get("ok", False)):
        risk_level = "high"
    elif compare_status in {"MISMATCH", "NOT_EXECUTED"} or route_mismatch or reference_quality in {"degraded", "failed"}:
        risk_level = "medium"
    failure_type = "none"
    if route_mismatch:
        failure_type = "route_physical_mismatch"
    elif reference_quality in {"degraded", "failed"}:
        failure_type = "reference_quality"
    elif compare_status in {"MISMATCH", "NOT_EXECUTED", "SNAPSHOT_ONLY"}:
        failure_type = compare_status.lower()
    elif kind == "parity":
        failure_type = "summary_parity"
    elif kind == "resilience":
        failure_type = "export_resilience"
    return {
        "suite": suite_name,
        "evidence_source": evidence_source,
        "evidence_state": evidence_state,
        "acceptance_level": "offline_regression",
        "acceptance_scope": "suite_case",
        "parent_run_id": parent_run_id or None,
        "source_artifact_ids": source_artifact_ids,
        "risk_level": risk_level,
        "failure_type": failure_type,
        "failure_phase": failure_phase,
        "reference_quality": reference_quality,
        "route_physical_mismatch": route_mismatch,
    }


def build_registry_indexes(entries: list[dict[str, Any]]) -> dict[str, Any]:
    index_payload: dict[str, dict[str, list[str]]] = {
        "by_evidence_source": defaultdict(list),
        "by_evidence_state": defaultdict(list),
        "by_suite": defaultdict(list),
        "by_scenario": defaultdict(list),
        "by_analyzer": defaultdict(list),
        "by_route": defaultdict(list),
        "by_temp": defaultdict(list),
        "by_config_version": defaultdict(list),
        "by_points_version": defaultdict(list),
        "by_profile_version": defaultdict(list),
        "by_software_build_id": defaultdict(list),
    }
    for entry in entries:
        artifact_id = str(entry.get("artifact_id") or "")
        dimensions = dict(entry.get("dimensions") or {})
        index_payload["by_evidence_source"][
            _normalize_review_evidence_source(entry.get("evidence_source"))
        ].append(artifact_id)
        index_payload["by_evidence_state"][str(entry.get("evidence_state") or "--")].append(artifact_id)
        index_payload["by_config_version"][str(entry.get("config_version") or "--")].append(artifact_id)
        index_payload["by_points_version"][str(entry.get("points_version") or "--")].append(artifact_id)
        index_payload["by_profile_version"][str(entry.get("profile_version") or "--")].append(artifact_id)
        index_payload["by_software_build_id"][str(entry.get("software_build_id") or "--")].append(artifact_id)
        index_payload["by_suite"][str(dimensions.get("suite") or "--")].append(artifact_id)
        index_payload["by_scenario"][str(dimensions.get("scenario") or "--")].append(artifact_id)
        for analyzer in _ensure_list(dimensions.get("analyzer")) or ["--"]:
            index_payload["by_analyzer"][str(analyzer or "--")].append(artifact_id)
        for route in _ensure_list(dimensions.get("route")) or ["--"]:
            index_payload["by_route"][str(route or "--")].append(artifact_id)
        for temp in _ensure_list(dimensions.get("temp")) or ["--"]:
            index_payload["by_temp"][str(temp)].append(artifact_id)
    return {name: {key: sorted(set(value)) for key, value in mapping.items()} for name, mapping in index_payload.items()}


def _build_skipped_spectral_quality_summary(
    *,
    run_id: str,
    simulation_mode: bool,
    min_samples: int,
    min_duration_s: float,
    low_freq_max_hz: float,
    error: str,
) -> dict[str, Any]:
    return {
        "artifact_type": "spectral_quality_summary",
        "schema_version": OFFLINE_ARTIFACT_SCHEMA_VERSION,
        "run_id": str(run_id or ""),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "skipped",
        "evidence_source": "simulated_protocol" if simulation_mode else "diagnostic",
        "evidence_state": "collected",
        "not_real_acceptance_evidence": True,
        "channel_count": 0,
        "ok_channel_count": 0,
        "overall_score": None,
        "flags": [],
        "status_counts": {"skipped": 1},
        "config": {
            "min_samples": int(min_samples),
            "min_duration_s": float(min_duration_s),
            "low_freq_max_hz": float(low_freq_max_hz),
        },
        "channels": {},
        "diagnostics": {
            "error": str(error or "").strip(),
        },
    }


def _spectral_quality_digest(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": str(payload.get("status") or ""),
        "channel_count": int(payload.get("channel_count", 0) or 0),
        "ok_channel_count": int(payload.get("ok_channel_count", 0) or 0),
        "overall_score": payload.get("overall_score"),
        "flags": [str(item) for item in list(payload.get("flags") or []) if str(item).strip()],
    }


def _artifact_status_payload(role: str, path: Path) -> dict[str, str]:
    return {"role": role, "status": "ok" if path.exists() else "missing", "path": str(path), "error": ""}


def _registry_entry(
    *,
    artifact_id: str,
    artifact_name: str,
    role: str,
    path: str,
    run_id: str,
    versions: dict[str, Any],
    acceptance_plan: dict[str, Any],
    dimensions: dict[str, Any],
    source_artifact_ids: list[str],
) -> dict[str, Any]:
    return {
        "artifact_id": artifact_id,
        "artifact_name": artifact_name,
        "role": role,
        "path": path,
        "present": bool(path and Path(path).exists()),
        "run_id": run_id,
        "evidence_source": acceptance_plan.get("evidence_source"),
        "evidence_state": acceptance_plan.get("evidence_state"),
        "acceptance_level": acceptance_plan.get("acceptance_level"),
        "acceptance_scope": acceptance_plan.get("acceptance_scope"),
        "promotion_state": acceptance_plan.get("promotion_state"),
        "review_state": acceptance_plan.get("review_state"),
        "approval_state": acceptance_plan.get("approval_state"),
        "ready_for_promotion": acceptance_plan.get("ready_for_promotion"),
        "config_version": versions.get("config_version"),
        "points_version": versions.get("points_version"),
        "profile_version": versions.get("profile_version"),
        "software_build_id": versions.get("software_build_id"),
        "source_artifact_ids": source_artifact_ids,
        "dimensions": dimensions,
    }


def _expected_analyzers(session: Any) -> list[str]:
    devices = getattr(getattr(session, "config", None), "devices", None)
    analyzers = getattr(devices, "gas_analyzers", []) if devices is not None else []
    values = []
    for index, item in enumerate(analyzers or []):
        if not bool(getattr(item, "enabled", True)):
            continue
        values.append(str(getattr(item, "id", "") or f"GA{index + 1:02d}").strip().upper())
    return values


def _current_history_entry(*, run_id: str, samples: list[Any], analytics_summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "start_time": datetime.now().isoformat(timespec="seconds"),
        "coverage_ratio": float(_text_ratio_to_float(((analytics_summary.get("analyzer_coverage") or {}).get("coverage_text"))) or 0.0),
        "usable_frame_ratio": float(((analytics_summary.get("frame_quality_statistics") or {}).get("usable_frame_ratio")) or 0.0),
        "reference_quality_score": _reference_quality_score((analytics_summary.get("reference_quality_statistics") or {}).get("reference_quality")),
        "sample_count": len(samples),
    }


def _spc_metric(metric: str, values: list[float]) -> dict[str, Any]:
    if not values:
        return {"metric": metric, "center_line": None, "ucl": None, "lcl": None, "latest": None}
    center = sum(values) / len(values)
    if len(values) < 2:
        return {"metric": metric, "center_line": round(center, 6), "ucl": round(center, 6), "lcl": round(center, 6), "latest": round(values[-1], 6)}
    variance = sum((item - center) ** 2 for item in values) / len(values)
    sigma = variance ** 0.5
    return {"metric": metric, "center_line": round(center, 6), "ucl": round(center + (3.0 * sigma), 6), "lcl": round(center - (3.0 * sigma), 6), "latest": round(values[-1], 6)}


def _control_limit_status(values: list[float]) -> str:
    if len(values) < 2:
        return "insufficient_history"
    metric = _spc_metric("metric", values)
    latest = float(metric.get("latest") or 0.0)
    return "out_of_control" if latest > float(metric.get("ucl") or 0.0) or latest < float(metric.get("lcl") or 0.0) else "in_control"


def _drift_indicator(values: list[float]) -> str:
    if len(values) < 2:
        return "insufficient_history"
    delta = values[-1] - values[0]
    if abs(delta) < 1e-6:
        return "stable"
    return "increasing" if delta > 0 else "decreasing"


def _trend_label(values: list[float]) -> str:
    if len(values) < 2:
        return "insufficient_history"
    if all(abs(values[idx] - values[0]) < 1e-6 for idx in range(1, len(values))):
        return "stable"
    return "improving" if values[-1] >= values[0] else "declining"


def _reference_quality_score(value: Any) -> float:
    normalized = str(value or "").strip().lower()
    if normalized == "healthy":
        return 1.0
    if normalized == "degraded":
        return 0.5
    return 0.0


def _load_named_status(path: Path, *, key: str, default: str) -> str:
    if not path.exists():
        return default
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default
    return str(payload.get(key) or default)


def _text_ratio_to_float(value: Any) -> Optional[float]:
    text = str(value or "").strip()
    if "/" not in text:
        return None
    numerator, denominator = text.split("/", 1)
    try:
        return _safe_ratio(float(numerator), float(denominator))
    except Exception:
        return None


def _path_version(path: str | Path, *, prefix: str) -> str:
    target = Path(path)
    if not target.exists():
        return f"{prefix}-missing"
    stats = target.stat()
    return f"{prefix}-{target.name}-{stats.st_size}-{stats.st_mtime_ns}"


def _ensure_list(value: Any) -> list[Any]:
    if value in (None, ""):
        return []
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return [value]


def _safe_ratio(numerator: float | int, denominator: float | int) -> float:
    if float(denominator) == 0.0:
        return 0.0
    return round(float(numerator) / float(denominator), 6)
