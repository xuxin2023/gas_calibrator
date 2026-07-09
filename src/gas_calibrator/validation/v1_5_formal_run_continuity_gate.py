"""Offline continuity gate for V1.5 formal CO2/H2O route evidence.

This gate reviews a formal route segment ledger. It does not execute queues,
open COM ports, control gas/water routes, connect to PostgreSQL, or write
coefficients. Its purpose is to keep a segmented engineering run from being
silently treated as one continuous production run.
"""

from __future__ import annotations

import csv
import json
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from .v1_5_production_entrypoint_map import (
    LEGACY_CO2_POINT_COUNT,
    LEGACY_H2O_WET_POINT_COUNT,
    MATURE_FITTING_BASELINE,
    MATURE_PHYSICAL_BASELINE,
    NEW_ALGORITHM_CO2_POINT_COUNT,
    NEW_ALGORITHM_H2O_WET_POINT_COUNT,
)


SCHEMA = "v1_5_formal_run_continuity_gate_v1"

CANONICAL_QUEUE_RUNNERS = {
    "co2": "src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py",
    "h2o": "src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_queue.py",
}

WORKER_RUNNERS = {
    "src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py",
    "src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_sampling.py",
}

FORBIDDEN_REFERENCE_TOKENS = (
    "/_handoff/",
    "_handoff/",
    "formal_queue_migration_20260624",
    "20260624",
    "0624",
)

REVIEW_SOURCE_TOKENS = (
    "diagnostic",
    "smoke",
    "reverify",
    "verify",
    "targeted",
)


@dataclass(frozen=True)
class FormalRunContinuitySegment:
    segment_id: str
    parent_formal_run_id: str
    route_kind: str
    algorithm_profile: str
    runner: str
    normalized_runner: str
    queue_csv: str
    config_hash: str
    parameter_hash: str
    selected_points: int
    ok_points: int
    failed_points: int
    running_points: int
    status: str
    segment_reason: str
    supersedes_points: str
    parameter_change_review_id: str
    source_kind: str
    fit_eligible: bool

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FormalRunContinuityFinding:
    severity: str
    status: str
    policy: str
    segment_id: str
    reason: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_slashes(value: str) -> str:
    return str(value or "").replace("\\", "/").strip().strip("'\"")


def _module_to_path(value: str) -> str | None:
    match = re.search(r"gas_calibrator\.tools\.([A-Za-z0-9_]+)", value)
    if not match:
        return None
    return f"src/gas_calibrator/tools/{match.group(1)}.py"


def _normalize_runner(value: str) -> str:
    text = _normalize_slashes(value)
    module_path = _module_to_path(text)
    if module_path:
        return module_path
    path_match = re.search(r"(src/gas_calibrator/[^\s'\";]+?\.py)", text)
    if path_match:
        return _normalize_slashes(path_match.group(1))
    return text


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple, set)):
        return ";".join(str(item) for item in value)
    return str(value)


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "fit", "eligible", "accepted"}


def _int_value(value: Any) -> int:
    if value is None or value == "":
        return 0
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return 0


def _load_ledger(path: str | Path) -> dict[str, Any]:
    ledger_path = Path(path)
    suffix = ledger_path.suffix.lower()
    if suffix == ".json":
        payload = json.loads(ledger_path.read_text(encoding="utf-8-sig"))
        if isinstance(payload, Mapping):
            return dict(payload)
        raise ValueError("JSON ledger must be an object")
    if suffix == ".csv":
        with ledger_path.open("r", encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))
        return {
            "schema": "v1_5_formal_run_segment_ledger_csv",
            "formal_run_id": rows[0].get("parent_formal_run_id", "") if rows else "",
            "route_kind": rows[0].get("route_kind", "") if rows else "",
            "algorithm_profile": rows[0].get("algorithm_profile", "") if rows else "",
            "segments": rows,
        }
    raise ValueError("ledger path must be .json or .csv")


def _first(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row:
            return row.get(key)
    return ""


def _segments(payload: Mapping[str, Any]) -> list[FormalRunContinuitySegment]:
    parent_run = _text(payload.get("formal_run_id") or payload.get("parent_formal_run_id"))
    route_kind = _text(payload.get("route_kind") or payload.get("route")).strip().lower()
    profile = _text(payload.get("algorithm_profile") or payload.get("profile_id") or "legacy_ratio_production")
    out: list[FormalRunContinuitySegment] = []
    for index, raw in enumerate(payload.get("segments") or [], start=1):
        if not isinstance(raw, Mapping):
            continue
        segment_id = _text(_first(raw, "segment_id", "queue_run_id", "run_name", "name")) or f"segment_{index:03d}"
        runner = _text(_first(raw, "runner", "runner_path", "tool", "module", "entrypoint"))
        normalized_runner = _normalize_runner(runner)
        selected = _int_value(_first(raw, "selected_points", "total_points", "planned_points"))
        ok = _int_value(_first(raw, "ok_points", "accepted_points", "completed_points"))
        failed = _int_value(_first(raw, "failed_points", "failure_points"))
        running = _int_value(_first(raw, "running_points", "incomplete_points"))
        out.append(
            FormalRunContinuitySegment(
                segment_id=segment_id,
                parent_formal_run_id=_text(_first(raw, "parent_formal_run_id", "formal_run_id")) or parent_run,
                route_kind=(_text(_first(raw, "route_kind", "route")) or route_kind).strip().lower(),
                algorithm_profile=_text(_first(raw, "algorithm_profile", "profile_id")) or profile,
                runner=runner,
                normalized_runner=normalized_runner,
                queue_csv=_normalize_slashes(_text(_first(raw, "queue_csv", "queue_source", "source_queue"))),
                config_hash=_text(_first(raw, "config_hash", "runtime_config_hash")),
                parameter_hash=_text(_first(raw, "parameter_hash", "command_hash", "run_parameter_hash")),
                selected_points=selected,
                ok_points=ok,
                failed_points=failed,
                running_points=running,
                status=_text(_first(raw, "status", "queue_status")),
                segment_reason=_text(_first(raw, "segment_reason", "reason", "run_reason")),
                supersedes_points=_text(_first(raw, "supersedes_points", "superseded_points", "point_supersedence")),
                parameter_change_review_id=_text(_first(raw, "parameter_change_review_id", "change_review_id")),
                source_kind=_text(_first(raw, "source_kind", "evidence_kind")),
                fit_eligible=_truthy(_first(raw, "fit_eligible", "eligible_for_fit")),
            )
        )
    return out


def _expected_point_count(route_kind: str, algorithm_profile: str, payload: Mapping[str, Any]) -> int:
    explicit = _int_value(payload.get("expected_point_count"))
    if explicit:
        return explicit
    profile = algorithm_profile.strip().lower()
    if route_kind == "co2":
        return NEW_ALGORITHM_CO2_POINT_COUNT if "absorption" in profile or "new" in profile else LEGACY_CO2_POINT_COUNT
    if route_kind == "h2o":
        return NEW_ALGORITHM_H2O_WET_POINT_COUNT if "absorption" in profile or "new" in profile else LEGACY_H2O_WET_POINT_COUNT
    return 0


def _reference_blocker(reference: str) -> str:
    text = _normalize_slashes(reference).lower()
    if not text:
        return ""
    if any(token in text for token in FORBIDDEN_REFERENCE_TOKENS):
        return "handoff_0624_or_scratch_reference"
    if text.startswith("d:/gas_calibrator/") and "/_worktrees/" not in text and "/_p9_" not in text:
        return "root_migration_reference"
    return ""


def _review_source_kind(source_kind: str) -> bool:
    text = source_kind.strip().lower()
    return bool(text) and any(token in text for token in REVIEW_SOURCE_TOKENS)


def _add_finding(
    findings: list[FormalRunContinuityFinding],
    *,
    severity: str,
    status: str,
    policy: str,
    segment_id: str = "",
    reason: str,
) -> None:
    findings.append(
        FormalRunContinuityFinding(
            severity=severity,
            status=status,
            policy=policy,
            segment_id=segment_id,
            reason=reason,
        )
    )


def _accepted_manifest_count(payload: Mapping[str, Any]) -> int:
    return _int_value(
        payload.get("accepted_point_count")
        or payload.get("accepted_points")
        or payload.get("accepted_manifest_point_count")
    )


def build_v1_5_formal_run_continuity_gate(*, ledger_path: str | Path) -> dict[str, Any]:
    ledger_file = Path(ledger_path)
    payload = _load_ledger(ledger_file)
    segments = _segments(payload)
    route_kind = (_text(payload.get("route_kind")) or (segments[0].route_kind if segments else "")).strip().lower()
    algorithm_profile = _text(payload.get("algorithm_profile") or (segments[0].algorithm_profile if segments else "legacy_ratio_production"))
    formal_run_id = _text(payload.get("formal_run_id") or (segments[0].parent_formal_run_id if segments else ""))
    expected_points = _expected_point_count(route_kind, algorithm_profile, payload)
    accepted_points = _accepted_manifest_count(payload)
    accepted_manifest_path = _normalize_slashes(_text(payload.get("accepted_manifest_path") or payload.get("accepted_manifest")))
    findings: list[FormalRunContinuityFinding] = []

    if not segments:
        _add_finding(
            findings,
            severity="blocker",
            status="blocker",
            policy="segment_ledger_empty",
            reason="A formal route continuity gate requires at least one segment.",
        )

    if route_kind not in {"co2", "h2o"}:
        _add_finding(
            findings,
            severity="blocker",
            status="blocker",
            policy="unknown_route_kind",
            reason=f"route_kind must be co2 or h2o, got {route_kind!r}.",
        )

    if expected_points <= 0:
        _add_finding(
            findings,
            severity="blocker",
            status="blocker",
            policy="expected_point_count_missing",
            reason="The ledger must declare or imply the expected formal point count.",
        )

    parameter_hashes = {segment.parameter_hash for segment in segments if segment.parameter_hash}
    queue_sources = {segment.queue_csv for segment in segments if segment.queue_csv}
    fit_eligible_segments = [segment for segment in segments if segment.fit_eligible]

    for segment in segments:
        canonical_runner = CANONICAL_QUEUE_RUNNERS.get(segment.route_kind)
        if not segment.parent_formal_run_id or segment.parent_formal_run_id != formal_run_id:
            _add_finding(
                findings,
                severity="blocker",
                status="blocker",
                policy="segment_parent_run_mismatch",
                segment_id=segment.segment_id,
                reason="Every segment must bind to the same frozen formal_run_id.",
            )
        if segment.route_kind != route_kind:
            _add_finding(
                findings,
                severity="blocker",
                status="blocker",
                policy="segment_route_kind_mismatch",
                segment_id=segment.segment_id,
                reason="CO2 and H2O segments cannot be mixed in one formal route continuity ledger.",
            )
        if segment.normalized_runner in WORKER_RUNNERS:
            _add_finding(
                findings,
                severity="blocker",
                status="blocker",
                policy="sampling_worker_not_formal_queue_segment",
                segment_id=segment.segment_id,
                reason="Per-point workers cannot be used as top-level formal route segments.",
            )
        elif canonical_runner and segment.normalized_runner != canonical_runner:
            _add_finding(
                findings,
                severity="blocker",
                status="blocker",
                policy="noncanonical_route_runner",
                segment_id=segment.segment_id,
                reason=f"Segment must use {canonical_runner}, got {segment.normalized_runner!r}.",
            )
        for reference in (segment.queue_csv, segment.runner):
            policy = _reference_blocker(reference)
            if policy:
                _add_finding(
                    findings,
                    severity="blocker",
                    status="blocker",
                    policy=policy,
                    segment_id=segment.segment_id,
                    reason="Formal route continuity cannot depend on _handoff, 0624, or root migration references.",
                )
        if segment.running_points or segment.status.strip().lower() == "running":
            _add_finding(
                findings,
                severity="blocker",
                status="blocker",
                policy="running_segment_not_fit_eligible",
                segment_id=segment.segment_id,
                reason="Running/incomplete segments cannot be used as closed formal evidence.",
            )
        if segment.fit_eligible and (segment.failed_points or segment.running_points):
            _add_finding(
                findings,
                severity="blocker",
                status="blocker",
                policy="failed_or_running_segment_marked_fit_eligible",
                segment_id=segment.segment_id,
                reason="Segments with failed/running points must be superseded before fit eligibility.",
            )
        if segment.fit_eligible and _review_source_kind(segment.source_kind):
            _add_finding(
                findings,
                severity="blocker",
                status="blocker",
                policy="diagnostic_or_reverify_segment_marked_fit_eligible",
                segment_id=segment.segment_id,
                reason="Diagnostic, smoke, targeted, or reverify evidence cannot be silently used as production fit data.",
            )
        if len(segments) > 1 and not segment.segment_reason:
            _add_finding(
                findings,
                severity="blocker",
                status="blocker",
                policy="segmented_run_missing_reason",
                segment_id=segment.segment_id,
                reason="Every segment in a non-continuous run must explain why the formal run was split.",
            )

    if len(segments) == 1:
        only = segments[0]
        if only.ok_points != expected_points or only.failed_points or only.running_points:
            _add_finding(
                findings,
                severity="blocker",
                status="blocker",
                policy="single_segment_not_complete",
                segment_id=only.segment_id,
                reason=f"Single-segment formal run must close {expected_points} points with no failed/running points.",
            )
    elif segments:
        if not accepted_manifest_path:
            _add_finding(
                findings,
                severity="blocker",
                status="blocker",
                policy="segmented_run_missing_accepted_manifest",
                reason="A segmented formal run needs an accepted/supersedence manifest before fitting.",
            )
        else:
            policy = _reference_blocker(accepted_manifest_path)
            if policy:
                _add_finding(
                    findings,
                    severity="blocker",
                    status="blocker",
                    policy=policy,
                    reason="Accepted manifest cannot live in _handoff, 0624, or root migration scratch paths.",
                )
        if accepted_points != expected_points:
            _add_finding(
                findings,
                severity="blocker",
                status="blocker",
                policy="segmented_run_accepted_point_count_mismatch",
                reason=f"Accepted manifest must close {expected_points} points, got {accepted_points}.",
            )
        if len(parameter_hashes) > 1 and not _text(payload.get("parameter_change_review_id")):
            for segment in segments:
                if not segment.parameter_change_review_id:
                    _add_finding(
                        findings,
                        severity="blocker",
                        status="blocker",
                        policy="parameter_hash_changed_without_review",
                        segment_id=segment.segment_id,
                        reason="Parameter/config changes across segments require explicit review before fitting.",
                    )
        if len(queue_sources) > 1 and not _text(payload.get("queue_source_review_id")):
            _add_finding(
                findings,
                severity="review",
                status="review_required",
                policy="multiple_queue_sources_require_review",
                reason="Segmented runs with more than one queue CSV need explicit source/supersedence review.",
            )
        _add_finding(
            findings,
            severity="review",
            status="review_required",
            policy="segmented_run_requires_reviewer_acceptance",
            reason="A segmented route may be usable, but it is not a continuous formal run and needs reviewer acceptance.",
        )

    blocker_count = sum(1 for finding in findings if finding.severity == "blocker")
    review_required_count = sum(1 for finding in findings if finding.severity == "review")
    status = "blocked" if blocker_count else "review_required" if review_required_count else "pass"

    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "ledger_path": str(ledger_file),
        "formal_run_id": formal_run_id,
        "route_kind": route_kind,
        "algorithm_profile": algorithm_profile,
        "manifest": {
            "status": status,
            "blocker_count": blocker_count,
            "review_required_count": review_required_count,
            "segment_count": len(segments),
            "fit_eligible_segment_count": len(fit_eligible_segments),
            "expected_point_count": expected_points,
            "accepted_point_count": accepted_points,
            "accepted_manifest_path": accepted_manifest_path,
            "parameter_hash_count": len(parameter_hashes),
            "queue_source_count": len(queue_sources),
            "continuous_formal_run": len(segments) == 1 and status == "pass",
            "segmented_run_requires_ledger_review": len(segments) > 1,
            "mature_fitting_baseline": MATURE_FITTING_BASELINE,
            "mature_physical_baseline": MATURE_PHYSICAL_BASELINE,
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
        "segments": [segment.to_json() for segment in segments],
        "findings": [finding.to_json() for finding in findings],
    }


def _markdown(model: Mapping[str, Any]) -> str:
    manifest = model["manifest"]
    lines = [
        "# V1.5 Formal Run Continuity Gate",
        "",
        f"- schema: `{model['schema']}`",
        f"- status: `{manifest['status']}`",
        f"- blocker_count: `{manifest['blocker_count']}`",
        f"- review_required_count: `{manifest['review_required_count']}`",
        f"- formal_run_id: `{model['formal_run_id']}`",
        f"- route_kind: `{model['route_kind']}`",
        f"- algorithm_profile: `{model['algorithm_profile']}`",
        f"- expected_point_count: `{manifest['expected_point_count']}`",
        f"- accepted_point_count: `{manifest['accepted_point_count']}`",
        f"- continuous_formal_run: `{manifest['continuous_formal_run']}`",
        f"- segmented_run_requires_ledger_review: `{manifest['segmented_run_requires_ledger_review']}`",
        f"- mature_physical_baseline: `{manifest['mature_physical_baseline']}`",
        f"- mature_fitting_baseline: `{manifest['mature_fitting_baseline']}`",
        "",
        "## Segments",
        "",
        "| segment | runner | selected | ok | failed | running | fit eligible | reason |",
        "|---|---|---:|---:|---:|---:|---|---|",
    ]
    for row in model["segments"]:
        lines.append(
            "| `{segment_id}` | `{runner}` | {selected_points} | {ok_points} | {failed_points} | {running_points} | `{fit_eligible}` | {segment_reason} |".format(
                **row
            )
        )
    lines.extend(["", "## Findings", "", "| severity | policy | segment | reason |", "|---|---|---|---|"])
    if model["findings"]:
        for row in model["findings"]:
            lines.append(f"| `{row['severity']}` | `{row['policy']}` | `{row['segment_id']}` | {row['reason']} |")
    else:
        lines.append("| `none` | `none` |  | No continuity issues detected. |")
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
            "- writes_sn_or_device_code: `false`",
            "- formal_release_allowed: `false`",
            "- database_import_allowed: `false`",
            "- not_real_acceptance_evidence: `true`",
            "",
            "A segmented route can be reviewed for fitting, but it must not be mislabeled as one continuous formal run.",
            "",
        ]
    )
    return "\n".join(lines)


def write_v1_5_formal_run_continuity_gate(*, ledger_path: str | Path, output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    model = build_v1_5_formal_run_continuity_gate(ledger_path=ledger_path)
    paths = {
        "manifest": out / "v1_5_formal_run_continuity_gate.json",
        "segments": out / "v1_5_formal_run_continuity_gate_segments.csv",
        "findings": out / "v1_5_formal_run_continuity_gate_findings.csv",
        "markdown": out / "V1_5_FORMAL_RUN_CONTINUITY_GATE.md",
    }
    paths["manifest"].write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    with paths["segments"].open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=tuple(FormalRunContinuitySegment.__dataclass_fields__.keys()))
        writer.writeheader()
        writer.writerows(model["segments"])
    with paths["findings"].open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=tuple(FormalRunContinuityFinding.__dataclass_fields__.keys()))
        writer.writeheader()
        writer.writerows(model["findings"])
    paths["markdown"].write_text(_markdown(model), encoding="utf-8")
    return paths


__all__ = [
    "SCHEMA",
    "build_v1_5_formal_run_continuity_gate",
    "write_v1_5_formal_run_continuity_gate",
]
