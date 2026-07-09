"""Offline readiness gate for recovering V1.5 route physical blockers.

This gate consumes the route run root-cause audit and an optional reviewed
physical recovery evidence packet. It never opens COM ports, controls pressure,
controls gas/water routes, connects to PostgreSQL, or writes analyzer state.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping


SCHEMA = "v1_5_route_physical_recovery_readiness_v1"

DRY_GAS_CATEGORY = "dry_gas_dewpoint_rebound_or_not_dry_enough"
PACE_VENT_CATEGORY = "pressure_controller_vent_no_response"
PRESSURE_GAUGE_CATEGORY = "pressure_gauge_no_response"

PHYSICAL_BLOCKER_CATEGORIES = {
    DRY_GAS_CATEGORY,
    PACE_VENT_CATEGORY,
    PRESSURE_GAUGE_CATEGORY,
    "instrument_no_response",
    "unclassified_failed_point",
}

SEGMENT_REVIEW_CATEGORIES = {
    "stale_running_manifest_with_completed_point_artifacts",
    "direct_or_retry_point_without_queue_manifest",
    "manual_parameter_or_execution_mode_change",
}

FRESH_QUEUE_REQUIRED_CATEGORIES = {
    "running_manifest_without_completed_point_artifacts",
    "queue_aborted_before_sampling_no_manifest",
    *SEGMENT_REVIEW_CATEGORIES,
}

VALID_PRESSURE_SOURCES = {
    "inl",
    "absolute_inl",
    "sens_pres_inl",
    "sens:pres:inl",
    ":sens:pres:inl?",
    "pace_inl_absolute_pressure",
}


@dataclass(frozen=True)
class RoutePhysicalRecoveryFinding:
    severity: str
    category: str
    requirement: str
    status: str
    reason: str
    required_action: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return {}
    payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if not isinstance(payload, Mapping):
        raise ValueError("JSON payload must be an object")
    return dict(payload)


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _text(value).lower() in {"1", "true", "yes", "y", "pass", "ok", "ready"}


def _number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _category_counts(root_cause_audit: Mapping[str, Any]) -> dict[str, int]:
    manifest = _as_dict(root_cause_audit.get("manifest"))
    counts = manifest.get("category_counts")
    if isinstance(counts, Mapping):
        return {str(key): int(value) for key, value in counts.items()}
    out: dict[str, int] = {}
    for row in root_cause_audit.get("findings") or []:
        if isinstance(row, Mapping):
            category = _text(row.get("category"))
            if category:
                out[category] = out.get(category, 0) + 1
    return out


def _status_is_pass(payload: Mapping[str, Any]) -> bool:
    return _text(payload.get("status")).lower() in {"pass", "ok", "ready", "reviewed_pass"}


def _add(
    findings: list[RoutePhysicalRecoveryFinding],
    *,
    severity: str,
    category: str,
    requirement: str,
    status: str,
    reason: str,
    required_action: str,
) -> None:
    findings.append(
        RoutePhysicalRecoveryFinding(
            severity=severity,
            category=category,
            requirement=requirement,
            status=status,
            reason=reason,
            required_action=required_action,
        )
    )


def _check_dry_gas(
    recovery: Mapping[str, Any],
    findings: list[RoutePhysicalRecoveryFinding],
    category_counts: Mapping[str, int],
) -> bool:
    if DRY_GAS_CATEGORY not in category_counts:
        return True
    packet = _as_dict(recovery.get("dry_gas_dewpoint_recovery"))
    threshold = _number(packet.get("dry_enough_threshold_c"))
    if threshold is None:
        threshold = -28.0
    max_span = _number(packet.get("max_tail_span_c"))
    if max_span is None:
        max_span = 0.5
    max_slope = _number(packet.get("max_tail_slope_abs_c_per_s"))
    if max_slope is None:
        max_slope = 0.01
    dewpoint = _number(packet.get("dewpoint_c"))
    tail_span = _number(packet.get("tail_span_c"))
    tail_slope = _number(packet.get("tail_slope_abs_c_per_s"))
    ok = (
        _status_is_pass(packet)
        and dewpoint is not None
        and dewpoint <= threshold
        and tail_span is not None
        and tail_span <= max_span
        and tail_slope is not None
        and tail_slope <= max_slope
        and _truthy(packet.get("route_or_dryer_checked"))
    )
    if ok:
        _add(
            findings,
            severity="info",
            category=DRY_GAS_CATEGORY,
            requirement="dry_gas_dewpoint_recovery",
            status="pass",
            reason=f"Dry-gas dewpoint recovery reviewed at {dewpoint:g} C with stable tail.",
            required_action="Keep the recovery evidence with the next formal run preflight package.",
        )
        return True
    _add(
        findings,
        severity="blocker",
        category=DRY_GAS_CATEGORY,
        requirement="dry_gas_dewpoint_recovery",
        status="missing_or_failed",
        reason="Dry-gas dewpoint blocker exists but recovery evidence is missing, not dry enough, unstable, or route/dryer check is absent.",
        required_action="Recover the dry-gas source/route, prove dewpoint <= threshold with stable tail, then rerun a clean smoke before a continuous queue.",
    )
    return False


def _check_pace_vent(
    recovery: Mapping[str, Any],
    findings: list[RoutePhysicalRecoveryFinding],
    category_counts: Mapping[str, int],
) -> bool:
    if PACE_VENT_CATEGORY not in category_counts:
        return True
    packet = _as_dict(recovery.get("pace_vent_recovery"))
    ok = (
        _status_is_pass(packet)
        and _truthy(packet.get("vent_on_off_roundtrip_pass"))
        and _truthy(packet.get("no_response_absent"))
    )
    if ok:
        _add(
            findings,
            severity="info",
            category=PACE_VENT_CATEGORY,
            requirement="pace_vent_recovery",
            status="pass",
            reason="PACE vent recovery proves ON/OFF roundtrip and no NO_RESPONSE.",
            required_action="Keep vent recovery evidence with the next formal run preflight package.",
        )
        return True
    _add(
        findings,
        severity="blocker",
        category=PACE_VENT_CATEGORY,
        requirement="pace_vent_recovery",
        status="missing_or_failed",
        reason="PACE vent NO_RESPONSE blocker exists without reviewed vent roundtrip recovery.",
        required_action="Recover PACE vent communication and prove ON/OFF roundtrip before opening a continuous route queue.",
    )
    return False


def _check_pressure_gauge(
    recovery: Mapping[str, Any],
    findings: list[RoutePhysicalRecoveryFinding],
    category_counts: Mapping[str, int],
) -> bool:
    if PRESSURE_GAUGE_CATEGORY not in category_counts:
        return True
    packet = _as_dict(recovery.get("pressure_gauge_recovery"))
    source = _text(packet.get("absolute_pressure_source") or packet.get("pressure_reference_query")).lower()
    source = source.replace("\\", "/")
    ok = (
        _status_is_pass(packet)
        and _text(packet.get("readback_status")).lower() in {"pass", "ok", "ready"}
        and _truthy(packet.get("no_response_absent"))
        and source in VALID_PRESSURE_SOURCES
    )
    if ok:
        _add(
            findings,
            severity="info",
            category=PRESSURE_GAUGE_CATEGORY,
            requirement="pressure_gauge_recovery",
            status="pass",
            reason="Pressure gauge recovery proves INL absolute pressure readback and no NO_RESPONSE.",
            required_action="Keep pressure gauge recovery evidence with the next formal run preflight package.",
        )
        return True
    _add(
        findings,
        severity="blocker",
        category=PRESSURE_GAUGE_CATEGORY,
        requirement="pressure_gauge_recovery",
        status="missing_or_failed",
        reason="Pressure gauge NO_RESPONSE blocker exists without reviewed INL absolute-pressure readback recovery.",
        required_action="Restore pressure gauge readback and prove the mature INL absolute-pressure source before a continuous queue.",
    )
    return False


def _check_generic_physical(
    recovery: Mapping[str, Any],
    findings: list[RoutePhysicalRecoveryFinding],
    category_counts: Mapping[str, int],
) -> bool:
    generic = sorted(
        category
        for category in category_counts
        if category in PHYSICAL_BLOCKER_CATEGORIES
        and category not in {DRY_GAS_CATEGORY, PACE_VENT_CATEGORY, PRESSURE_GAUGE_CATEGORY}
    )
    if not generic:
        return True
    packet = _as_dict(recovery.get("instrument_communication_recovery"))
    ok = _status_is_pass(packet) and _truthy(packet.get("no_response_absent"))
    if ok:
        _add(
            findings,
            severity="info",
            category="instrument_communication_recovery",
            requirement="instrument_communication_recovery",
            status="pass",
            reason="Generic instrument communication recovery reviewed for unclassified NO_RESPONSE/failed-point blockers.",
            required_action="Keep the recovery evidence with the next formal run preflight package.",
        )
        return True
    _add(
        findings,
        severity="blocker",
        category=";".join(generic),
        requirement="instrument_communication_recovery",
        status="missing_or_failed",
        reason="A generic instrument/failed-point blocker exists without reviewed recovery evidence.",
        required_action="Classify and recover the instrument communication failure before opening a continuous queue.",
    )
    return False


def _fresh_queue_pass(recovery: Mapping[str, Any]) -> bool:
    packet = _as_dict(recovery.get("next_run_policy"))
    baseline = _text(packet.get("mature_physical_baseline")).lower()
    return (
        _truthy(packet.get("fresh_canonical_queue"))
        and _truthy(packet.get("forbidden_surfaces_absent"))
        and ("0613" in baseline and "0620" in baseline and "0621" in baseline)
    )


def _accepted_manifest_pass(recovery: Mapping[str, Any]) -> bool:
    packet = _as_dict(recovery.get("accepted_manifest_review"))
    return (
        _status_is_pass(packet)
        and bool(_text(packet.get("accepted_manifest_path")))
        and bool(_text(packet.get("supersedence_review_id")))
    )


def _check_fresh_queue_policy(
    recovery: Mapping[str, Any],
    findings: list[RoutePhysicalRecoveryFinding],
    category_counts: Mapping[str, int],
) -> bool:
    if not category_counts:
        return True
    if _fresh_queue_pass(recovery):
        _add(
            findings,
            severity="info",
            category="fresh_canonical_queue_policy",
            requirement="next_run_policy",
            status="pass",
            reason="Next run is explicitly bound to a fresh canonical 0613/0620/0621 queue with forbidden surfaces absent.",
            required_action="Start the next formal run from this clean queue; do not append to failed segmented evidence.",
        )
        return True
    _add(
        findings,
        severity="blocker",
        category="fresh_canonical_queue_policy",
        requirement="next_run_policy",
        status="missing_or_failed",
        reason="Prior segmented/aborted/direct evidence exists, but the next run policy does not prove a fresh canonical queue.",
        required_action="Open the next CO2/H2O run from the mature 0613/0620/0621 formal queue, not from _handoff, 0624, worker, diagnostic, retry, or root migration surfaces.",
    )
    return False


def _check_segmented_evidence_review(
    recovery: Mapping[str, Any],
    findings: list[RoutePhysicalRecoveryFinding],
    category_counts: Mapping[str, int],
) -> bool:
    if not any(category in category_counts for category in SEGMENT_REVIEW_CATEGORIES):
        return True
    if _accepted_manifest_pass(recovery):
        _add(
            findings,
            severity="info",
            category="accepted_manifest_review",
            requirement="accepted_manifest_review",
            status="pass",
            reason="Segmented/direct/retry evidence has an accepted-manifest supersedence review.",
            required_action="Use only the reviewed accepted manifest for fitting; never use raw segmented manifests directly.",
        )
        return True
    _add(
        findings,
        severity="review",
        category="accepted_manifest_review",
        requirement="accepted_manifest_review",
        status="missing",
        reason="Segmented/direct/retry evidence lacks accepted-manifest supersedence review.",
        required_action="Keep old segmented evidence out of fitting, or bind it through an accepted manifest before any coefficient calculation.",
    )
    return False


def build_v1_5_route_physical_recovery_readiness(
    *,
    root_cause_audit_path: str | Path,
    recovery_evidence_path: str | Path | None = None,
) -> dict[str, Any]:
    root_cause_audit = _load_json(root_cause_audit_path)
    recovery = _load_json(recovery_evidence_path)
    category_counts = _category_counts(root_cause_audit)
    findings: list[RoutePhysicalRecoveryFinding] = []

    physical_ok = all(
        (
            _check_dry_gas(recovery, findings, category_counts),
            _check_pace_vent(recovery, findings, category_counts),
            _check_pressure_gauge(recovery, findings, category_counts),
            _check_generic_physical(recovery, findings, category_counts),
        )
    )
    fresh_queue_ok = _check_fresh_queue_policy(recovery, findings, category_counts)
    accepted_manifest_ok = _check_segmented_evidence_review(recovery, findings, category_counts)

    blocker_count = sum(1 for item in findings if item.severity == "blocker")
    review_count = sum(1 for item in findings if item.severity == "review")
    next_run_allowed = physical_ok and fresh_queue_ok and blocker_count == 0
    segmented_fit_use_allowed = accepted_manifest_ok and not any(
        item.severity == "blocker" and item.category in SEGMENT_REVIEW_CATEGORIES for item in findings
    )
    status = "blocked" if blocker_count else "review_required" if review_count else "pass"

    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "manifest": {
            "status": status,
            "root_cause_audit_path": str(Path(root_cause_audit_path)),
            "recovery_evidence_path": str(Path(recovery_evidence_path)) if recovery_evidence_path else "",
            "root_cause_status": _text(_as_dict(root_cause_audit.get("manifest")).get("status")),
            "category_counts": category_counts,
            "finding_count": len(findings),
            "blocker_count": blocker_count,
            "review_required_count": review_count,
            "physical_recovery_passed": physical_ok,
            "fresh_canonical_queue_policy_passed": fresh_queue_ok,
            "accepted_manifest_review_passed": accepted_manifest_ok,
            "next_continuous_run_allowed": next_run_allowed,
            "segmented_evidence_fit_use_allowed": segmented_fit_use_allowed,
            "opens_com_ports": False,
            "controls_pressure": False,
            "controls_water_or_gas_routes": False,
            "connects_postgresql": False,
            "writes_coefficients": False,
            "writes_sn_or_device_code": False,
            "formal_release_allowed": False,
            "database_import_allowed": False,
            "not_real_acceptance_evidence": True,
        },
        "findings": [item.to_json() for item in findings],
    }


def _markdown(model: Mapping[str, Any]) -> str:
    manifest = _as_dict(model["manifest"])
    lines = [
        "# V1.5 Route Physical Recovery Readiness",
        "",
        f"- schema: `{model['schema']}`",
        f"- status: `{manifest['status']}`",
        f"- next_continuous_run_allowed: `{manifest['next_continuous_run_allowed']}`",
        f"- segmented_evidence_fit_use_allowed: `{manifest['segmented_evidence_fit_use_allowed']}`",
        f"- blocker_count: `{manifest['blocker_count']}`",
        f"- review_required_count: `{manifest['review_required_count']}`",
        "",
        "## Physical Meaning",
        "",
        "- PACE vent or pressure-gauge NO_RESPONSE must be recovered before the next continuous queue.",
        "- Dry-gas dewpoint rebound must be resolved with stable dry evidence before the next CO2 zero-gas point.",
        "- Segmented, direct, retry, or stale-running evidence is not a continuous formal run unless an accepted manifest reviews it.",
        "- A fresh canonical 0613/0620/0621 queue is required before starting the next continuous run after these failures.",
        "",
        "## Findings",
        "",
        "| severity | category | requirement | status | reason | required action |",
        "|---|---|---|---|---|---|",
    ]
    if model.get("findings"):
        for row in model["findings"]:
            lines.append(
                "| `{severity}` | `{category}` | `{requirement}` | `{status}` | {reason} | {action} |".format(
                    severity=row["severity"],
                    category=row["category"],
                    requirement=row["requirement"],
                    status=row["status"],
                    reason=row["reason"],
                    action=row["required_action"],
                )
            )
    else:
        lines.append("| `none` | `none` | `none` | `pass` | No recovery requirements detected. |  |")
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- Offline evidence review only.",
            "- Does not open COM ports, control pressure, control gas/water routes, connect PostgreSQL, or write coefficients/SN.",
            "- This readiness is not formal release, database import, or real acceptance evidence.",
            "",
        ]
    )
    return "\n".join(lines)


def write_v1_5_route_physical_recovery_readiness(
    *,
    root_cause_audit_path: str | Path,
    output_dir: str | Path,
    recovery_evidence_path: str | Path | None = None,
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    model = build_v1_5_route_physical_recovery_readiness(
        root_cause_audit_path=root_cause_audit_path,
        recovery_evidence_path=recovery_evidence_path,
    )
    paths = {
        "manifest": out / "v1_5_route_physical_recovery_readiness.json",
        "findings": out / "v1_5_route_physical_recovery_readiness_findings.csv",
        "markdown": out / "V1_5_ROUTE_PHYSICAL_RECOVERY_READINESS.md",
    }
    paths["manifest"].write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    with paths["findings"].open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=tuple(RoutePhysicalRecoveryFinding.__dataclass_fields__.keys()))
        writer.writeheader()
        writer.writerows(model["findings"])
    paths["markdown"].write_text(_markdown(model), encoding="utf-8")
    return paths


__all__ = [
    "SCHEMA",
    "build_v1_5_route_physical_recovery_readiness",
    "write_v1_5_route_physical_recovery_readiness",
]
