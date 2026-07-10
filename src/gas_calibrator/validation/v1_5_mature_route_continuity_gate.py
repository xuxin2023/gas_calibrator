"""Offline gate for mature V1.5 continuous route-run evidence.

This gate reviews an already-generated CO2/H2O queue manifest and optional
route root-cause audit. It does not open COM ports, control gas/water routes,
connect PostgreSQL, or write analyzer state. Its job is to keep segmented,
retry/direct-recovery, empty-manifest, 0624/migration, diagnostic, or worker
evidence from being mistaken for a fresh 0613/0620/0621 mature continuous run.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping


SCHEMA = "v1_5_mature_route_continuity_gate_v1"

EXPECTED_POINT_COUNTS = {
    "co2": 45,
    "h2o": 13,
}

PASS_STATUSES = {"ok", "pass", "passed", "done", "completed", "success"}
BAD_STATUSES = {"", "failed", "fail", "error", "aborted", "abort", "running", "pending", "skipped"}

FORBIDDEN_SURFACE_TOKENS = (
    "_handoff",
    "0624",
    "formal_queue_migration",
    "diagnostic",
    "worker",
    "run_v1_5_formal_open_flow_sampling",
    "run_v1_5_formal_h2o_open_flow_sampling",
)

FORBIDDEN_SEGMENT_TOKENS = (
    "retry",
    "direct",
    "recovery",
    "240purge",
    "manual",
    "segment",
    "segmented",
)

SIDE_EFFECT_FALSE = {
    "opens_com_ports": False,
    "controls_pressure": False,
    "controls_water_or_gas_routes": False,
    "connects_postgresql": False,
    "writes_coefficients": False,
    "writes_sn_or_device_code": False,
    "formal_release_allowed": False,
    "database_import_allowed": False,
}


@dataclass(frozen=True)
class MatureRouteContinuityFinding:
    severity: str
    requirement: str
    status: str
    reason: str
    required_action: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _text(value: Any) -> str:
    return str(value or "").strip()


def _status(value: Any) -> str:
    return _text(value).lower()


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _load_json(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return {}
    payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if not isinstance(payload, Mapping):
        raise ValueError("root-cause audit JSON must contain an object")
    return dict(payload)


def _add(
    findings: list[MatureRouteContinuityFinding],
    *,
    severity: str,
    requirement: str,
    status: str,
    reason: str,
    required_action: str,
) -> None:
    findings.append(
        MatureRouteContinuityFinding(
            severity=severity,
            requirement=requirement,
            status=status,
            reason=reason,
            required_action=required_action,
        )
    )


def _row_text(row: Mapping[str, Any]) -> str:
    return " ".join(f"{key}={value}" for key, value in row.items()).replace("\\", "/").lower()


def _path_text(path: str | Path) -> str:
    return str(path).replace("\\", "/").lower()


def _has_token(text: str, tokens: tuple[str, ...]) -> list[str]:
    return [token for token in tokens if token in text]


def _route_kind(route_kind: str) -> str:
    value = _text(route_kind).lower()
    if value not in EXPECTED_POINT_COUNTS:
        raise ValueError("route_kind must be 'co2' or 'h2o'")
    return value


def _point_key(row: Mapping[str, Any], index: int) -> str:
    for key in ("point_id", "point_run_id", "id", "name"):
        value = _text(row.get(key))
        if value:
            return value
    return f"row_{index:03d}"


def _check_manifest_path(
    *,
    manifest_path: Path,
    findings: list[MatureRouteContinuityFinding],
) -> bool:
    hits = _has_token(_path_text(manifest_path), FORBIDDEN_SURFACE_TOKENS + FORBIDDEN_SEGMENT_TOKENS)
    if hits:
        _add(
            findings,
            severity="blocker",
            requirement="canonical_manifest_path",
            status="forbidden_surface_or_segment",
            reason=f"Queue manifest path contains forbidden token(s): {', '.join(sorted(set(hits)))}.",
            required_action="Use a fresh 0613/0620/0621 mature queue manifest, not _handoff, 0624, diagnostic, worker, retry, direct, or segmented evidence.",
        )
        return False
    _add(
        findings,
        severity="info",
        requirement="canonical_manifest_path",
        status="pass",
        reason="Queue manifest path does not contain forbidden migration/diagnostic/segment tokens.",
        required_action="Keep this manifest path attached to the continuity review.",
    )
    return True


def _check_rows(
    *,
    rows: list[Mapping[str, Any]],
    route_kind: str,
    expected_point_count: int,
    findings: list[MatureRouteContinuityFinding],
) -> bool:
    ok = True
    if not rows:
        _add(
            findings,
            severity="blocker",
            requirement="queue_manifest_non_empty",
            status="empty_manifest",
            reason="Queue manifest has no rows, so the route did not enter usable point sampling evidence.",
            required_action="Rerun from the mature queue; do not use an empty manifest for fitting.",
        )
        return False
    if len(rows) != expected_point_count:
        ok = False
        _add(
            findings,
            severity="blocker",
            requirement="expected_point_count",
            status="count_mismatch",
            reason=f"{route_kind.upper()} manifest has {len(rows)} point rows; expected {expected_point_count}.",
            required_action="Use a complete fresh continuous route run: legacy CO2 45 points or H2O 13 wet points.",
        )
    else:
        _add(
            findings,
            severity="info",
            requirement="expected_point_count",
            status="pass",
            reason=f"{route_kind.upper()} manifest has the expected {expected_point_count} point rows.",
            required_action="Keep point-count evidence with the route continuity review.",
        )

    point_keys: list[str] = []
    bad_statuses: list[str] = []
    forbidden_rows: list[str] = []
    for index, row in enumerate(rows, start=1):
        key = _point_key(row, index)
        point_keys.append(key)
        status = _status(row.get("status") or row.get("source_status") or row.get("risk_level"))
        if status in BAD_STATUSES or status not in PASS_STATUSES:
            bad_statuses.append(f"{key}:{status or 'missing'}")
        row_hits = _has_token(_row_text(row), FORBIDDEN_SEGMENT_TOKENS + FORBIDDEN_SURFACE_TOKENS)
        if row_hits:
            forbidden_rows.append(f"{key}:{','.join(sorted(set(row_hits)))}")

    duplicates = sorted({key for key in point_keys if point_keys.count(key) > 1})
    if duplicates:
        ok = False
        _add(
            findings,
            severity="blocker",
            requirement="unique_point_ids",
            status="duplicate_points",
            reason=f"Duplicate point identifiers: {', '.join(duplicates[:10])}.",
            required_action="Do not fit duplicate manifest rows; regenerate or review the accepted manifest explicitly.",
        )
    if bad_statuses:
        ok = False
        _add(
            findings,
            severity="blocker",
            requirement="all_points_completed",
            status="bad_or_incomplete_status",
            reason=f"Manifest contains non-complete point statuses: {'; '.join(bad_statuses[:12])}.",
            required_action="Rerun or supersede failed/running/aborted points before formal fitting.",
        )
    if forbidden_rows:
        ok = False
        _add(
            findings,
            severity="blocker",
            requirement="no_segmented_retry_direct_rows",
            status="forbidden_point_lineage",
            reason=f"Manifest rows contain segmented/retry/direct/migration lineage: {'; '.join(forbidden_rows[:12])}.",
            required_action="Exclude segmented/direct/retry rows unless a later accepted-manifest review explicitly supersedes them.",
        )
    if ok:
        _add(
            findings,
            severity="info",
            requirement="point_status_and_lineage",
            status="pass",
            reason="All manifest points are complete, unique, and free of segmented/retry/direct lineage tokens.",
            required_action="This manifest can proceed to the next offline fit-input review gate.",
        )
    return ok


def _check_root_cause_audit(
    *,
    root_cause_audit: Mapping[str, Any],
    findings: list[MatureRouteContinuityFinding],
) -> tuple[bool, bool]:
    if not root_cause_audit:
        _add(
            findings,
            severity="review",
            requirement="root_cause_audit_attached",
            status="missing",
            reason="No route root-cause audit is attached, so continuity is based only on queue manifest rows.",
            required_action="Attach the route root-cause audit before final fit/release review.",
        )
        return True, False
    manifest = root_cause_audit.get("manifest") if isinstance(root_cause_audit.get("manifest"), Mapping) else {}
    status = _status(manifest.get("status") or root_cause_audit.get("status"))
    blocker_count = int(manifest.get("blocker_count") or 0)
    review_count = int(manifest.get("review_required_count") or 0)
    if blocker_count or status == "blocked":
        _add(
            findings,
            severity="blocker",
            requirement="root_cause_audit_clear",
            status="blocked",
            reason=f"Attached root-cause audit is blocked (status={status or 'missing'}, blocker_count={blocker_count}).",
            required_action="Resolve physical/run root-cause blockers and bind recovery evidence before treating this as a continuous formal run.",
        )
        return False, False
    if review_count or status == "review_required":
        _add(
            findings,
            severity="review",
            requirement="root_cause_audit_clear",
            status="review_required",
            reason=f"Attached root-cause audit still requires review (status={status or 'missing'}, review_required_count={review_count}).",
            required_action="Complete accepted-manifest/supersedence review before allowing this run into fitting.",
        )
        return True, False
    if status and status != "pass":
        _add(
            findings,
            severity="review",
            requirement="root_cause_audit_clear",
            status="unknown_status",
            reason=f"Attached root-cause audit has unexpected status={status}.",
            required_action="Regenerate the root-cause audit or review it manually before fitting.",
        )
        return True, False
    _add(
        findings,
        severity="info",
        requirement="root_cause_audit_clear",
        status="pass",
        reason="Attached root-cause audit has no blockers or review-required findings.",
        required_action="Keep the audit with the continuous-run manifest review.",
    )
    return True, True


def build_v1_5_mature_route_continuity_gate(
    *,
    route_kind: str,
    queue_manifest_path: str | Path,
    root_cause_audit_path: str | Path | None = None,
    expected_point_count: int | None = None,
) -> dict[str, Any]:
    route = _route_kind(route_kind)
    expected = int(expected_point_count or EXPECTED_POINT_COUNTS[route])
    manifest_path = Path(queue_manifest_path)
    findings: list[MatureRouteContinuityFinding] = []
    root_cause_audit = _load_json(root_cause_audit_path)

    manifest_path_ok = _check_manifest_path(manifest_path=manifest_path, findings=findings)
    if manifest_path.exists():
        rows = _read_csv(manifest_path)
    else:
        rows = []
        _add(
            findings,
            severity="blocker",
            requirement="queue_manifest_exists",
            status="missing",
            reason=f"Queue manifest does not exist: {manifest_path}.",
            required_action="Generate a fresh mature queue manifest before continuity review.",
        )
    rows_ok = _check_rows(
        rows=rows,
        route_kind=route,
        expected_point_count=expected,
        findings=findings,
    )
    root_ok, root_pass = _check_root_cause_audit(root_cause_audit=root_cause_audit, findings=findings)

    blocker_count = sum(1 for item in findings if item.severity == "blocker")
    review_count = sum(1 for item in findings if item.severity == "review")
    fit_eligible = blocker_count == 0 and review_count == 0 and manifest_path_ok and rows_ok and root_ok and root_pass
    status = "blocked" if blocker_count else "review_required" if review_count else "pass"

    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "manifest": {
            "status": status,
            "route_kind": route,
            "queue_manifest_path": str(manifest_path),
            "root_cause_audit_path": str(Path(root_cause_audit_path)) if root_cause_audit_path else "",
            "expected_point_count": expected,
            "observed_point_count": len(rows),
            "blocker_count": blocker_count,
            "review_required_count": review_count,
            "manifest_path_policy_passed": manifest_path_ok,
            "point_row_policy_passed": rows_ok,
            "root_cause_audit_policy_passed": root_ok and root_pass,
            "continuous_route_run_fit_eligible": fit_eligible,
            "mature_physical_baseline": "0613 fitting + 0620/0621 clean-worktree route path",
            **SIDE_EFFECT_FALSE,
            "not_real_acceptance_evidence": True,
        },
        "findings": [item.to_json() for item in findings],
    }


def _markdown(model: Mapping[str, Any]) -> str:
    manifest = model["manifest"]
    lines = [
        "# V1.5 Mature Route Continuity Gate",
        "",
        f"- schema: `{model['schema']}`",
        f"- status: `{manifest['status']}`",
        f"- route_kind: `{manifest['route_kind']}`",
        f"- expected_point_count: `{manifest['expected_point_count']}`",
        f"- observed_point_count: `{manifest['observed_point_count']}`",
        f"- continuous_route_run_fit_eligible: `{manifest['continuous_route_run_fit_eligible']}`",
        f"- mature_physical_baseline: `{manifest['mature_physical_baseline']}`",
        "",
        "## Meaning",
        "",
        "This gate decides whether one mature V1.5 route manifest is eligible to feed the next offline fit-input review.",
        "It blocks segmented, retry, direct-recovery, empty-manifest, 0624/migration, diagnostic, and worker evidence from being treated as a continuous 0613/0620/0621 route run.",
        "",
        "## Findings",
        "",
        "| severity | requirement | status | reason | required_action |",
        "|---|---|---|---|---|",
    ]
    for row in model["findings"]:
        lines.append(
            "| `{severity}` | `{requirement}` | `{status}` | {reason} | {required_action} |".format(
                severity=row["severity"],
                requirement=row["requirement"],
                status=row["status"],
                reason=row["reason"].replace("|", "/"),
                required_action=row["required_action"].replace("|", "/"),
            )
        )
    lines.extend(
        [
            "",
            "## Non-Execution Boundary",
            "",
            "- opens_com_ports: `false`",
            "- controls_pressure: `false`",
            "- controls_water_or_gas_routes: `false`",
            "- connects_postgresql: `false`",
            "- writes_coefficients: `false`",
            "- formal_release_allowed: `false`",
            "- database_import_allowed: `false`",
            "- not_real_acceptance_evidence: `true`",
            "",
        ]
    )
    return "\n".join(lines)


def write_v1_5_mature_route_continuity_gate(
    *,
    route_kind: str,
    queue_manifest_path: str | Path,
    output_dir: str | Path,
    root_cause_audit_path: str | Path | None = None,
    expected_point_count: int | None = None,
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    model = build_v1_5_mature_route_continuity_gate(
        route_kind=route_kind,
        queue_manifest_path=queue_manifest_path,
        root_cause_audit_path=root_cause_audit_path,
        expected_point_count=expected_point_count,
    )
    outputs = {
        "manifest": out / "v1_5_mature_route_continuity_gate.json",
        "findings": out / "v1_5_mature_route_continuity_gate_findings.csv",
        "markdown": out / "V1_5_MATURE_ROUTE_CONTINUITY_GATE.md",
    }
    outputs["manifest"].write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    with outputs["findings"].open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=("severity", "requirement", "status", "reason", "required_action"),
        )
        writer.writeheader()
        writer.writerows(model["findings"])
    outputs["markdown"].write_text(_markdown(model), encoding="utf-8")
    return outputs


__all__ = [
    "EXPECTED_POINT_COUNTS",
    "SCHEMA",
    "build_v1_5_mature_route_continuity_gate",
    "write_v1_5_mature_route_continuity_gate",
]
