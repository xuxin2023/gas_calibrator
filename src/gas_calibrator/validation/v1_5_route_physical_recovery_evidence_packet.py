"""Validate reviewed V1.5 route physical recovery evidence packets.

This is an offline packet validator for the recovery evidence that feeds the
route physical recovery readiness gate. It does not open COM ports, control
pressure, control gas/water routes, connect PostgreSQL, or write analyzer state.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from .v1_5_route_physical_recovery_readiness import VALID_PRESSURE_SOURCES


SCHEMA = "v1_5_route_physical_recovery_evidence_packet_v1"

FORBIDDEN_REFERENCE_TOKENS = (
    "_handoff",
    "d:/gas_calibrator/src/",
    "formal_queue_migration_20260624",
    "20260624",
    "0624",
    "diagnostic",
    "worker",
    "run_v1_5_formal_open_flow_sampling",
    "run_v1_5_formal_h2o_open_flow_sampling",
)

CANONICAL_QUEUE_TOKENS = (
    "run_v1_5_formal_co2_open_flow_queue",
    "run_v1_5_formal_h2o_open_flow_queue",
)

SIDE_EFFECT_FALSE_KEYS = (
    "opens_com_ports",
    "controls_pressure",
    "controls_water_or_gas_routes",
    "connects_postgresql",
    "writes_coefficients",
    "writes_sn_or_device_code",
    "formal_release_allowed",
    "database_import_allowed",
)


@dataclass(frozen=True)
class RoutePhysicalRecoveryEvidenceFinding:
    severity: str
    requirement: str
    status: str
    reason: str
    required_action: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if not isinstance(payload, Mapping):
        raise ValueError("recovery evidence packet must be a JSON object")
    return dict(payload)


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _text(value).lower() in {"1", "true", "yes", "y", "ok", "pass", "ready", "reviewed_pass"}


def _status_pass(value: Any) -> bool:
    return _text(value).lower() in {"pass", "ok", "ready", "reviewed_pass"}


def _add(
    findings: list[RoutePhysicalRecoveryEvidenceFinding],
    *,
    severity: str,
    requirement: str,
    status: str,
    reason: str,
    required_action: str,
) -> None:
    findings.append(
        RoutePhysicalRecoveryEvidenceFinding(
            severity=severity,
            requirement=requirement,
            status=status,
            reason=reason,
            required_action=required_action,
        )
    )


def _has_reference(value: Any) -> bool:
    if isinstance(value, Mapping):
        return any(_has_reference(item) for item in value.values())
    if isinstance(value, list | tuple):
        return any(_has_reference(item) for item in value)
    return bool(_text(value))


def _packet_flag(packet: Mapping[str, Any], key: str) -> Any:
    manifest = _as_dict(packet.get("manifest"))
    if key in packet:
        return packet.get(key)
    return manifest.get(key)


def _check_side_effect_boundaries(
    packet: Mapping[str, Any],
    findings: list[RoutePhysicalRecoveryEvidenceFinding],
) -> bool:
    ok = True
    for key in SIDE_EFFECT_FALSE_KEYS:
        if _packet_flag(packet, key) is not False:
            ok = False
            _add(
                findings,
                severity="blocker",
                requirement=f"side_effect_boundary.{key}",
                status="not_locked_false",
                reason=f"Recovery packet must keep {key}=false.",
                required_action="Regenerate the recovery packet as offline/no-COM/no-write/no-route evidence.",
            )
    if _packet_flag(packet, "not_real_acceptance_evidence") is not True:
        ok = False
        _add(
            findings,
            severity="blocker",
            requirement="side_effect_boundary.not_real_acceptance_evidence",
            status="not_locked_true",
            reason="Recovery packet must mark not_real_acceptance_evidence=true.",
            required_action="Do not use recovery evidence as formal release, database import, or real acceptance evidence.",
        )
    if ok:
        _add(
            findings,
            severity="info",
            requirement="side_effect_boundary",
            status="pass",
            reason="Packet keeps COM, pressure, route, PostgreSQL, write, release, and import side effects locked.",
            required_action="Keep this packet as offline recovery evidence only.",
        )
    return ok


def _check_dry_gas(
    packet: Mapping[str, Any],
    findings: list[RoutePhysicalRecoveryEvidenceFinding],
) -> bool:
    evidence = _as_dict(packet.get("dry_gas_dewpoint_recovery"))
    threshold = _number(evidence.get("dry_enough_threshold_c"))
    if threshold is None:
        threshold = -28.0
    max_span = _number(evidence.get("max_tail_span_c"))
    if max_span is None:
        max_span = 0.5
    max_slope = _number(evidence.get("max_tail_slope_abs_c_per_s"))
    if max_slope is None:
        max_slope = 0.01
    dewpoint = _number(evidence.get("dewpoint_c"))
    tail_span = _number(evidence.get("tail_span_c"))
    tail_slope = _number(evidence.get("tail_slope_abs_c_per_s"))
    ok = (
        _status_pass(evidence.get("status"))
        and dewpoint is not None
        and dewpoint <= threshold
        and tail_span is not None
        and tail_span <= max_span
        and tail_slope is not None
        and tail_slope <= max_slope
        and _truthy(evidence.get("route_or_dryer_checked"))
        and _has_reference(evidence.get("evidence"))
    )
    if ok:
        _add(
            findings,
            severity="info",
            requirement="dry_gas_dewpoint_recovery",
            status="pass",
            reason=f"Dry-gas dewpoint is {dewpoint:g} C with stable tail and route/dryer evidence.",
            required_action="Use this only as recovery evidence before a fresh canonical queue.",
        )
        return True
    _add(
        findings,
        severity="blocker",
        requirement="dry_gas_dewpoint_recovery",
        status="missing_or_failed",
        reason="Dry-gas recovery must prove dry-enough dewpoint, stable tail, route/dryer check, and evidence reference.",
        required_action="Recover zero-gas dryness and attach dewpoint/route evidence before unlocking a continuous run.",
    )
    return False


def _check_pace_vent(
    packet: Mapping[str, Any],
    findings: list[RoutePhysicalRecoveryEvidenceFinding],
) -> bool:
    evidence = _as_dict(packet.get("pace_vent_recovery"))
    ok = (
        _status_pass(evidence.get("status"))
        and _truthy(evidence.get("vent_on_off_roundtrip_pass"))
        and _truthy(evidence.get("no_response_absent"))
        and _has_reference(evidence.get("evidence"))
    )
    if ok:
        _add(
            findings,
            severity="info",
            requirement="pace_vent_recovery",
            status="pass",
            reason="PACE vent recovery has ON/OFF roundtrip, no NO_RESPONSE, and evidence reference.",
            required_action="Keep this evidence with the next formal run preflight package.",
        )
        return True
    _add(
        findings,
        severity="blocker",
        requirement="pace_vent_recovery",
        status="missing_or_failed",
        reason="PACE vent recovery must prove ON/OFF roundtrip, no NO_RESPONSE, and evidence reference.",
        required_action="Recover PACE vent communication before allowing a continuous route queue.",
    )
    return False


def _check_pressure_gauge(
    packet: Mapping[str, Any],
    findings: list[RoutePhysicalRecoveryEvidenceFinding],
) -> bool:
    evidence = _as_dict(packet.get("pressure_gauge_recovery"))
    source = _text(evidence.get("absolute_pressure_source") or evidence.get("pressure_reference_query")).lower()
    source = source.replace("\\", "/")
    ok = (
        _status_pass(evidence.get("status"))
        and _text(evidence.get("readback_status")).lower() in {"pass", "ok", "ready"}
        and _truthy(evidence.get("no_response_absent"))
        and source in VALID_PRESSURE_SOURCES
        and _has_reference(evidence.get("evidence"))
    )
    if ok:
        _add(
            findings,
            severity="info",
            requirement="pressure_gauge_recovery",
            status="pass",
            reason="Pressure gauge recovery uses the mature INL absolute-pressure source with no NO_RESPONSE.",
            required_action="Keep this evidence with the next formal run preflight package.",
        )
        return True
    _add(
        findings,
        severity="blocker",
        requirement="pressure_gauge_recovery",
        status="missing_or_failed",
        reason="Pressure gauge recovery must prove INL absolute-pressure readback, no NO_RESPONSE, and evidence reference.",
        required_action="Recover COM22/pressure-gauge readback before allowing a continuous route queue.",
    )
    return False


def _forbidden_reference_hits(value: Any) -> list[str]:
    raw = json.dumps(value, ensure_ascii=False).replace("\\", "/").lower()
    return [token for token in FORBIDDEN_REFERENCE_TOKENS if token in raw]


def _canonical_queue_present(value: Any) -> bool:
    raw = json.dumps(value, ensure_ascii=False).replace("\\", "/").lower()
    return any(token in raw for token in CANONICAL_QUEUE_TOKENS)


def _check_next_run_policy(
    packet: Mapping[str, Any],
    findings: list[RoutePhysicalRecoveryEvidenceFinding],
) -> bool:
    policy = _as_dict(packet.get("next_run_policy"))
    baseline = _text(policy.get("mature_physical_baseline")).lower()
    hits = _forbidden_reference_hits(policy)
    ok = (
        _truthy(policy.get("fresh_canonical_queue"))
        and _truthy(policy.get("forbidden_surfaces_absent"))
        and "0613" in baseline
        and "0620" in baseline
        and "0621" in baseline
        and _canonical_queue_present(policy)
        and not hits
    )
    if ok:
        _add(
            findings,
            severity="info",
            requirement="next_run_policy",
            status="pass",
            reason="Next-run policy binds execution to fresh canonical 0613/0620/0621 queue entrypoints.",
            required_action="Open the next continuous run from these canonical queue entrypoints only.",
        )
        return True
    reason = "Next-run policy must prove fresh canonical 0613/0620/0621 queue usage and forbidden surfaces absent."
    if hits:
        reason += f" Forbidden references: {', '.join(sorted(set(hits)))}."
    _add(
        findings,
        severity="blocker",
        requirement="next_run_policy",
        status="missing_or_failed",
        reason=reason,
        required_action="Bind the next run to canonical CO2/H2O queue entrypoints, not _handoff, root migration, 0624, worker, diagnostic, or migration surfaces.",
    )
    return False


def _check_accepted_manifest_review(
    packet: Mapping[str, Any],
    findings: list[RoutePhysicalRecoveryEvidenceFinding],
) -> bool:
    review = _as_dict(packet.get("accepted_manifest_review"))
    if not review:
        _add(
            findings,
            severity="review",
            requirement="accepted_manifest_review",
            status="missing",
            reason="No accepted-manifest supersedence review is attached for segmented/direct/retry evidence.",
            required_action="Keep segmented evidence out of fitting unless an accepted manifest review is attached.",
        )
        return False
    ok = (
        _status_pass(review.get("status"))
        and bool(_text(review.get("accepted_manifest_path")))
        and bool(_text(review.get("supersedence_review_id")))
    )
    if ok:
        _add(
            findings,
            severity="info",
            requirement="accepted_manifest_review",
            status="pass",
            reason="Accepted-manifest supersedence review is present.",
            required_action="Use only the reviewed accepted manifest for fitting, if segmented evidence is later allowed.",
        )
        return True
    _add(
        findings,
        severity="review",
        requirement="accepted_manifest_review",
        status="missing_or_failed",
        reason="Accepted-manifest review must include pass status, accepted_manifest_path, and supersedence_review_id.",
        required_action="Do not use segmented/direct/retry evidence for fitting until this review is complete.",
    )
    return False


def build_v1_5_route_physical_recovery_evidence_packet(
    *,
    recovery_evidence_packet_path: str | Path,
) -> dict[str, Any]:
    packet = _load_json(recovery_evidence_packet_path)
    findings: list[RoutePhysicalRecoveryEvidenceFinding] = []

    side_effect_ok = _check_side_effect_boundaries(packet, findings)
    dry_ok = _check_dry_gas(packet, findings)
    vent_ok = _check_pace_vent(packet, findings)
    gauge_ok = _check_pressure_gauge(packet, findings)
    queue_ok = _check_next_run_policy(packet, findings)
    manifest_ok = _check_accepted_manifest_review(packet, findings)

    blocker_count = sum(1 for item in findings if item.severity == "blocker")
    review_count = sum(1 for item in findings if item.severity == "review")
    status = "blocked" if blocker_count else "review_required" if review_count else "pass"
    readiness_input_ready = (
        side_effect_ok and dry_ok and vent_ok and gauge_ok and queue_ok and blocker_count == 0
    )
    segmented_evidence_review_ready = manifest_ok and blocker_count == 0

    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "manifest": {
            "status": status,
            "source_packet_path": str(Path(recovery_evidence_packet_path)),
            "blocker_count": blocker_count,
            "review_required_count": review_count,
            "readiness_input_ready": readiness_input_ready,
            "segmented_evidence_review_ready": segmented_evidence_review_ready,
            "dry_gas_dewpoint_recovery_passed": dry_ok,
            "pace_vent_recovery_passed": vent_ok,
            "pressure_gauge_recovery_passed": gauge_ok,
            "fresh_canonical_queue_policy_passed": queue_ok,
            "side_effect_boundaries_locked": side_effect_ok,
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
        "validated_recovery_evidence": {
            key: packet.get(key)
            for key in (
                "dry_gas_dewpoint_recovery",
                "pace_vent_recovery",
                "pressure_gauge_recovery",
                "accepted_manifest_review",
                "next_run_policy",
            )
            if key in packet
        },
    }


def _markdown(model: Mapping[str, Any]) -> str:
    manifest = _as_dict(model.get("manifest"))
    lines = [
        "# V1.5 Route Physical Recovery Evidence Packet",
        "",
        f"- schema: `{model['schema']}`",
        f"- status: `{manifest.get('status')}`",
        f"- readiness_input_ready: `{manifest.get('readiness_input_ready')}`",
        f"- segmented_evidence_review_ready: `{manifest.get('segmented_evidence_review_ready')}`",
        f"- blocker_count: `{manifest.get('blocker_count')}`",
        f"- review_required_count: `{manifest.get('review_required_count')}`",
        "",
        "## Physical Meaning",
        "",
        "- Dry-gas recovery must prove the zero-gas route is dry and stable before the next CO2 zero-gas point.",
        "- PACE vent recovery must prove vent communication has recovered before a continuous route queue.",
        "- Pressure-gauge recovery must prove the mature INL absolute-pressure readback is available.",
        "- Next-run policy must bind execution to a fresh canonical 0613/0620/0621 queue, not migration or worker surfaces.",
        "- Segmented evidence remains excluded from fitting unless an accepted-manifest supersedence review exists.",
        "",
        "## Findings",
        "",
        "| severity | requirement | status | reason | required action |",
        "|---|---|---|---|---|",
    ]
    for row in model.get("findings") or []:
        lines.append(
            "| `{severity}` | `{requirement}` | `{status}` | {reason} | {action} |".format(
                severity=row["severity"],
                requirement=row["requirement"],
                status=row["status"],
                reason=row["reason"],
                action=row["required_action"],
            )
        )
    if not model.get("findings"):
        lines.append("| `none` | `none` | `pass` | No findings. |  |")
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- Offline packet validation only.",
            "- Does not open COM ports, control pressure, control gas/water routes, connect PostgreSQL, or write coefficients/SN.",
            "- Passing this packet does not itself start a route run; the route physical recovery readiness gate still consumes the root-cause audit.",
            "",
        ]
    )
    return "\n".join(lines)


def write_v1_5_route_physical_recovery_evidence_packet(
    *,
    recovery_evidence_packet_path: str | Path,
    output_dir: str | Path,
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    model = build_v1_5_route_physical_recovery_evidence_packet(
        recovery_evidence_packet_path=recovery_evidence_packet_path,
    )
    paths = {
        "manifest": out / "v1_5_route_physical_recovery_evidence_packet.json",
        "findings": out / "v1_5_route_physical_recovery_evidence_packet_findings.csv",
        "validated_recovery_evidence": out / "v1_5_validated_route_physical_recovery_evidence.json",
        "markdown": out / "V1_5_ROUTE_PHYSICAL_RECOVERY_EVIDENCE_PACKET.md",
    }
    paths["manifest"].write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    with paths["findings"].open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=tuple(RoutePhysicalRecoveryEvidenceFinding.__dataclass_fields__.keys()),
        )
        writer.writeheader()
        writer.writerows(model["findings"])
    paths["validated_recovery_evidence"].write_text(
        json.dumps(model["validated_recovery_evidence"], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    paths["markdown"].write_text(_markdown(model), encoding="utf-8")
    return paths


__all__ = [
    "SCHEMA",
    "build_v1_5_route_physical_recovery_evidence_packet",
    "write_v1_5_route_physical_recovery_evidence_packet",
]
