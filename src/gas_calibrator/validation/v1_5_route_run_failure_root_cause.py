"""Offline root-cause audit for segmented V1.5 route runs.

This module reads existing CO2/H2O queue artifacts and classifies why a route
run stopped, split, or became unsafe for direct fitting. It never opens COM
ports, controls pressure/routes, connects to PostgreSQL, or writes device state.
"""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping


SCHEMA = "v1_5_route_run_failure_root_cause_audit_v1"


@dataclass(frozen=True)
class RouteRunRootCauseFinding:
    run_dir: str
    route_kind: str
    artifact: str
    point_run_id: str
    point_id: str
    status: str
    severity: str
    category: str
    root_cause: str
    physical_meaning: str
    required_action: str
    fit_eligibility_policy: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_csv(path: Path) -> list[dict[str, str]]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            return list(csv.DictReader(handle))
    except FileNotFoundError:
        return []


def _text(row: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = row.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _route_kind(path: Path) -> str:
    text = str(path).replace("\\", "/").lower()
    if "h2o" in text:
        return "h2o"
    if "co2" in text:
        return "co2"
    return "unknown"


def _point_artifact_complete(run_dir: Path, point_run_id: str) -> bool:
    if not point_run_id:
        return False
    for candidate in run_dir.rglob(point_run_id):
        if candidate.is_dir() and (candidate / "conclusion_summary.csv").exists():
            return True
    return False


def _has_any_completed_point(run_dir: Path) -> bool:
    return any(path.name == "conclusion_summary.csv" for path in run_dir.rglob("conclusion_summary.csv"))


def _relative(path: Path, base: Path) -> str:
    try:
        return str(path.relative_to(base)).replace("\\", "/")
    except ValueError:
        return str(path).replace("\\", "/")


def _point_log_text(row: Mapping[str, str]) -> str:
    log_path = _text(row, "point_log")
    if not log_path:
        return ""
    path = Path(log_path)
    if not path.exists():
        return ""
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def _failure_context(row: Mapping[str, str]) -> str:
    return " | ".join(
        part
        for part in (
            _text(row, "failure_reason", "source_failure_reason", "error", "reason"),
            _point_log_text(row),
        )
        if part
    )


def _add(
    findings: list[RouteRunRootCauseFinding],
    *,
    run_dir: Path,
    route_kind: str,
    artifact: Path,
    point_run_id: str = "",
    point_id: str = "",
    status: str = "",
    severity: str,
    category: str,
    root_cause: str,
    physical_meaning: str,
    required_action: str,
    fit_eligibility_policy: str,
) -> None:
    findings.append(
        RouteRunRootCauseFinding(
            run_dir=str(run_dir),
            route_kind=route_kind,
            artifact=_relative(artifact, run_dir),
            point_run_id=point_run_id,
            point_id=point_id,
            status=status,
            severity=severity,
            category=category,
            root_cause=root_cause,
            physical_meaning=physical_meaning,
            required_action=required_action,
            fit_eligibility_policy=fit_eligibility_policy,
        )
    )


def _classify_failed_row(
    *,
    run_dir: Path,
    route_kind: str,
    artifact: Path,
    row: Mapping[str, str],
    findings: list[RouteRunRootCauseFinding],
) -> None:
    point_run_id = _text(row, "point_run_id")
    point_id = _text(row, "point_id")
    status = _text(row, "status", "source_status")
    reason = _failure_context(row)
    reason_l = reason.lower()
    if "dewpoint_rebound_detected" in reason_l or "dewpoint_tail_reference_not_dry_enough" in reason_l:
        _add(
            findings,
            run_dir=run_dir,
            route_kind=route_kind,
            artifact=artifact,
            point_run_id=point_run_id,
            point_id=point_id,
            status=status,
            severity="blocker",
            category="dry_gas_dewpoint_rebound_or_not_dry_enough",
            root_cause="The dry-gas route did not hold a stable dry dewpoint before sampling.",
            physical_meaning="CO2 zero gas still contained unstable residual water or the dry route/line was re-wetting.",
            required_action="Do not fit this point; dry/purge the route and rerun the point or select a reviewed retry.",
            fit_eligibility_policy="requires_superseded_retry_or_exclusion",
        )
        return
    if "pressure controller vent command failed" in reason_l or "vent on failed" in reason_l:
        _add(
            findings,
            run_dir=run_dir,
            route_kind=route_kind,
            artifact=artifact,
            point_run_id=point_run_id,
            point_id=point_id,
            status=status,
            severity="blocker",
            category="pressure_controller_vent_no_response",
            root_cause="PACE atmosphere vent command returned NO_RESPONSE during startup/pre-point reset.",
            physical_meaning="The route could not prove atmospheric vent-hold before opening the flow path.",
            required_action="Stop the queue, recover PACE communication/vent state, then rerun from a clean queue segment.",
            fit_eligibility_policy="failed_point_not_fit_eligible",
        )
        return
    if "no_response" in reason_l and ("pressure-gauge" in reason_l or "pressure gauge" in reason_l):
        _add(
            findings,
            run_dir=run_dir,
            route_kind=route_kind,
            artifact=artifact,
            point_run_id=point_run_id,
            point_id=point_id,
            status=status,
            severity="blocker",
            category="pressure_gauge_no_response",
            root_cause="Pressure gauge readback returned NO_RESPONSE during pre-seal/open-flow verification.",
            physical_meaning="The atmospheric/open-flow pressure reference for the point was not traceable.",
            required_action="Hold the run, restore pressure gauge communication, and rerun the affected point.",
            fit_eligibility_policy="failed_point_not_fit_eligible",
        )
        return
    if "no_response" in reason_l:
        _add(
            findings,
            run_dir=run_dir,
            route_kind=route_kind,
            artifact=artifact,
            point_run_id=point_run_id,
            point_id=point_id,
            status=status,
            severity="blocker",
            category="instrument_no_response",
            root_cause="A required instrument command/read returned NO_RESPONSE.",
            physical_meaning="The physical route state for this point was not fully observed.",
            required_action="Hold the point and rerun only after instrument communication is recovered.",
            fit_eligibility_policy="failed_point_not_fit_eligible",
        )
        return
    _add(
        findings,
        run_dir=run_dir,
        route_kind=route_kind,
        artifact=artifact,
        point_run_id=point_run_id,
        point_id=point_id,
        status=status,
        severity="blocker",
        category="unclassified_failed_point",
        root_cause=reason or "Queue manifest recorded a failed point without a classified reason.",
        physical_meaning="The point did not complete as formal evidence.",
        required_action="Classify the point failure before allowing it into fitting or accepted manifests.",
        fit_eligibility_policy="failed_point_not_fit_eligible",
    )


def _audit_manifest(run_dir: Path, manifest: Path, findings: list[RouteRunRootCauseFinding]) -> None:
    route_kind = _route_kind(run_dir)
    for row in _read_csv(manifest):
        point_run_id = _text(row, "point_run_id")
        point_id = _text(row, "point_id")
        status = _text(row, "status", "source_status").lower()
        if status == "running":
            if _point_artifact_complete(run_dir, point_run_id):
                _add(
                    findings,
                    run_dir=run_dir,
                    route_kind=route_kind,
                    artifact=manifest,
                    point_run_id=point_run_id,
                    point_id=point_id,
                    status=status,
                    severity="blocker",
                    category="stale_running_manifest_with_completed_point_artifacts",
                    root_cause="Queue manifest was not finalized after point artifacts were written.",
                    physical_meaning="The physical point may be usable, but the queue-level state is stale and cannot prove continuous execution.",
                    required_action="Regenerate or review the accepted manifest; do not treat the original queue as continuous.",
                    fit_eligibility_policy="requires_accepted_manifest_supersedence",
                )
            else:
                _add(
                    findings,
                    run_dir=run_dir,
                    route_kind=route_kind,
                    artifact=manifest,
                    point_run_id=point_run_id,
                    point_id=point_id,
                    status=status,
                    severity="blocker",
                    category="running_manifest_without_completed_point_artifacts",
                    root_cause="Queue stopped or was interrupted before the point completed.",
                    physical_meaning="No closed point-level evidence exists for this manifest row.",
                    required_action="Rerun the point from a clean queue segment.",
                    fit_eligibility_policy="not_fit_eligible",
                )
            continue
        if status == "failed":
            _classify_failed_row(run_dir=run_dir, route_kind=route_kind, artifact=manifest, row=row, findings=findings)


def _audit_abort(run_dir: Path, abort_file: Path, findings: list[RouteRunRootCauseFinding], has_manifest: bool) -> None:
    route_kind = _route_kind(run_dir)
    for row in _read_csv(abort_file):
        point_run_id = _text(row, "point_run_id")
        point_id = _text(row, "point_id")
        source_status = _text(row, "source_status", "status").lower()
        if source_status == "aborted" and not has_manifest:
            _add(
                findings,
                run_dir=run_dir,
                route_kind=route_kind,
                artifact=abort_file,
                point_run_id=point_run_id,
                point_id=point_id,
                status=source_status,
                severity="blocker",
                category="queue_aborted_before_sampling_no_manifest",
                root_cause="The queue planned points but aborted before producing queue_manifest.csv.",
                physical_meaning="No formal point sampling occurred, so ok=0/failed=0 is not success.",
                required_action="Treat the segment as not executed; restart with a valid queue runner and require queue_manifest.csv.",
                fit_eligibility_policy="not_fit_eligible",
            )
        elif source_status == "failed":
            _classify_failed_row(run_dir=run_dir, route_kind=route_kind, artifact=abort_file, row=row, findings=findings)


def _audit_direct_or_parameter_changed(run_dir: Path, findings: list[RouteRunRootCauseFinding], has_manifest: bool) -> None:
    name = run_dir.name.lower()
    route_kind = _route_kind(run_dir)
    has_completed = _has_any_completed_point(run_dir)
    direct_like = any(token in name for token in ("direct", "retry", "remaining"))
    parameter_changed = any(token in name for token in ("notemp", "240purge", "finalparams"))
    if has_completed and not has_manifest and direct_like:
        _add(
            findings,
            run_dir=run_dir,
            route_kind=route_kind,
            artifact=run_dir,
            severity="review",
            category="direct_or_retry_point_without_queue_manifest",
            root_cause="A completed point was produced outside a closed formal queue manifest.",
            physical_meaning="The point can only be considered through explicit supersedence/accepted-manifest review.",
            required_action="Bind it into an accepted manifest with the failed point it supersedes; never call it continuous.",
            fit_eligibility_policy="review_only_until_accepted_manifest",
        )
    if parameter_changed:
        _add(
            findings,
            run_dir=run_dir,
            route_kind=route_kind,
            artifact=run_dir,
            severity="review",
            category="manual_parameter_or_execution_mode_change",
            root_cause="Run naming indicates changed execution parameters such as no-temp wait, 240 s purge, finalparams, direct, or retry.",
            physical_meaning="The physical point may still be valid, but it is no longer one frozen queue execution.",
            required_action="Require parameter-hash review and accepted-manifest selection before fitting.",
            fit_eligibility_policy="requires_parameter_review",
        )


def audit_v1_5_route_run_failure_root_causes(*, run_dirs: Iterable[str | Path]) -> dict[str, Any]:
    dirs = [Path(path) for path in run_dirs]
    findings: list[RouteRunRootCauseFinding] = []
    for run_dir in dirs:
        manifests = list(run_dir.rglob("queue_manifest.csv"))
        aborts = list(run_dir.rglob("queue_abort_exclusion.csv"))
        has_manifest = bool(manifests)
        for manifest in manifests:
            _audit_manifest(run_dir, manifest, findings)
        for abort in aborts:
            _audit_abort(run_dir, abort, findings, has_manifest=has_manifest)
        _audit_direct_or_parameter_changed(run_dir, findings, has_manifest=has_manifest)

    blocker_count = sum(1 for item in findings if item.severity == "blocker")
    review_count = sum(1 for item in findings if item.severity == "review")
    categories: dict[str, int] = {}
    for item in findings:
        categories[item.category] = categories.get(item.category, 0) + 1
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "manifest": {
            "status": "blocked" if blocker_count else "review_required" if review_count else "pass",
            "run_dir_count": len(dirs),
            "finding_count": len(findings),
            "blocker_count": blocker_count,
            "review_required_count": review_count,
            "category_counts": categories,
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
        "run_dirs": [str(path) for path in dirs],
        "findings": [item.to_json() for item in findings],
    }


def _markdown(model: Mapping[str, Any]) -> str:
    manifest = model["manifest"]
    lines = [
        "# V1.5 Route Run Failure Root-Cause Audit",
        "",
        f"- schema: `{model['schema']}`",
        f"- status: `{manifest['status']}`",
        f"- run_dir_count: `{manifest['run_dir_count']}`",
        f"- finding_count: `{manifest['finding_count']}`",
        f"- blocker_count: `{manifest['blocker_count']}`",
        f"- review_required_count: `{manifest['review_required_count']}`",
        "",
        "## Category Counts",
        "",
        "| category | count |",
        "|---|---:|",
    ]
    for key, value in sorted(manifest["category_counts"].items()):
        lines.append(f"| `{key}` | {value} |")
    lines.extend(
        [
            "",
            "## Findings",
            "",
            "| severity | category | run | point | root cause | required action |",
            "|---|---|---|---|---|---|",
        ]
    )
    if model["findings"]:
        for row in model["findings"]:
            lines.append(
                "| `{severity}` | `{category}` | `{run}` | `{point}` | {root} | {action} |".format(
                    severity=row["severity"],
                    category=row["category"],
                    run=Path(row["run_dir"]).name,
                    point=row["point_run_id"] or row["point_id"],
                    root=row["root_cause"],
                    action=row["required_action"],
                )
            )
    else:
        lines.append("| `none` | `none` |  |  | No root-cause findings detected. |  |")
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- Offline evidence review only.",
            "- Does not open COM ports, control pressure, control gas/water routes, connect PostgreSQL, or write coefficients/SN.",
            "- Findings are not real acceptance evidence.",
            "",
        ]
    )
    return "\n".join(lines)


def write_v1_5_route_run_failure_root_cause_audit(
    *,
    run_dirs: Iterable[str | Path],
    output_dir: str | Path,
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    model = audit_v1_5_route_run_failure_root_causes(run_dirs=run_dirs)
    paths = {
        "manifest": out / "v1_5_route_run_failure_root_cause_audit.json",
        "findings": out / "v1_5_route_run_failure_root_cause_audit_findings.csv",
        "markdown": out / "V1_5_ROUTE_RUN_FAILURE_ROOT_CAUSE_AUDIT.md",
    }
    paths["manifest"].write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    with paths["findings"].open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=tuple(RouteRunRootCauseFinding.__dataclass_fields__.keys()))
        writer.writeheader()
        writer.writerows(model["findings"])
    paths["markdown"].write_text(_markdown(model), encoding="utf-8")
    return paths


__all__ = [
    "SCHEMA",
    "audit_v1_5_route_run_failure_root_causes",
    "write_v1_5_route_run_failure_root_cause_audit",
]
