"""Audit missing-QC historical replay points for bindable evidence.

The historical replay evidence binder intentionally blocks points that lack
point-level QC. This audit explains those gaps without changing fit
eligibility: it searches for same-run queue-manifest reject evidence,
same-point retry quality, and cross-run reference quality, then labels what can
and cannot be bound.
"""

from __future__ import annotations

import csv
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA = "v1_5_historical_replay_qc_gap_audit_v1"
CO2_RE = re.compile(r"^p(?P<index>\d+)_T(?P<temp>m?\d+)_(?P<ppm>\d+)ppm", re.IGNORECASE)
H2O_RE = re.compile(
    r"^p(?P<index>\d+)_T(?P<temp>m?\d+)_HG(?P<hgen>m?\d+)C_(?P<rh>\d+)RH",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class QcGapPoint:
    family_id: str
    route_kind: str
    point_id: str
    temp_c: float | None
    co2_ppm: float | None
    hgen_c: float | None
    rh_pct: float | None
    point_path: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class QcGapCandidate:
    point_id: str
    family_id: str
    route_kind: str
    candidate_type: str
    bind_decision: str
    candidate_path: str
    quality_grade: str
    sample_can_enter_calibration_fit: bool | None
    sample_can_enter_diagnostic_model: bool | None
    reason: str
    same_run: bool
    same_point_id: bool
    same_physical_point: bool

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class QcGapAuditCheck:
    check_id: str
    title: str
    status: str
    reason: str
    expected: str
    observed: str
    physical_meaning: str
    blocks_qc_gap_closure: bool

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _fmt(value: Any) -> str:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _check(
    *,
    check_id: str,
    title: str,
    status: str,
    reason: str,
    expected: Any,
    observed: Any,
    physical_meaning: str,
) -> QcGapAuditCheck:
    return QcGapAuditCheck(
        check_id=check_id,
        title=title,
        status=status,
        reason=reason,
        expected=_fmt(expected),
        observed=_fmt(observed),
        physical_meaning=physical_meaning,
        blocks_qc_gap_closure=status == "blocker",
    )


def _boolish(value: Any) -> bool | None:
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _parse_temp(token: str) -> float:
    return float(-int(token[1:]) if token.lower().startswith("m") else int(token))


def _point_identity(point_id: str) -> dict[str, Any]:
    h2o_match = H2O_RE.match(point_id)
    if h2o_match:
        return {
            "route_kind": "h2o",
            "temp_c": _parse_temp(h2o_match.group("temp")),
            "co2_ppm": None,
            "hgen_c": _parse_temp(h2o_match.group("hgen")),
            "rh_pct": float(h2o_match.group("rh")),
        }
    co2_match = CO2_RE.match(point_id)
    if co2_match:
        return {
            "route_kind": "co2",
            "temp_c": _parse_temp(co2_match.group("temp")),
            "co2_ppm": float(co2_match.group("ppm")),
            "hgen_c": None,
            "rh_pct": None,
        }
    return {
        "route_kind": "unknown",
        "temp_c": None,
        "co2_ppm": None,
        "hgen_c": None,
        "rh_pct": None,
    }


def _same_physical_point(point: QcGapPoint, candidate_point_id: str) -> bool:
    identity = _point_identity(candidate_point_id)
    if point.route_kind != identity.get("route_kind"):
        return False
    if point.route_kind == "co2":
        return point.temp_c == identity.get("temp_c") and point.co2_ppm == identity.get("co2_ppm")
    if point.route_kind == "h2o":
        return (
            point.temp_c == identity.get("temp_c")
            and point.hgen_c == identity.get("hgen_c")
            and point.rh_pct == identity.get("rh_pct")
        )
    return False


def _load_replay_model(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise ValueError("historical replay evidence must be a JSON object")
    return payload


def _missing_qc_points(model: Mapping[str, Any]) -> list[QcGapPoint]:
    points: list[QcGapPoint] = []
    for row in model.get("points", []):
        if row.get("quality_source"):
            continue
        points.append(
            QcGapPoint(
                family_id=str(row.get("family_id") or ""),
                route_kind=str(row.get("route_kind") or ""),
                point_id=str(row.get("point_id") or ""),
                temp_c=row.get("temp_c"),
                co2_ppm=row.get("co2_ppm"),
                hgen_c=row.get("hgen_c"),
                rh_pct=row.get("rh_pct"),
                point_path=str(row.get("point_path") or ""),
            )
        )
    return points


def _ancestor_dirs(path: Path, stop_after: int = 3) -> list[Path]:
    dirs: list[Path] = []
    current = path
    for _ in range(stop_after):
        current = current.parent
        if current in dirs:
            break
        dirs.append(current)
    return dirs


def _candidate_from_manifest_row(point: QcGapPoint, manifest: Path, row: Mapping[str, str]) -> QcGapCandidate:
    can_fit = _boolish(row.get("sample_can_enter_calibration_fit"))
    can_diag = _boolish(row.get("sample_can_enter_diagnostic_model"))
    quality_grade = str(row.get("quality_grade") or "")
    queue_status = str(row.get("status") or row.get("queue_status") or "")
    failure = ";".join(
        item
        for item in (
            str(row.get("failure_category") or row.get("queue_failure_category") or ""),
            str(row.get("failure_reason") or row.get("queue_failure_reason") or ""),
            str(row.get("quality_reason") or ""),
        )
        if item
    )
    if can_fit is True:
        decision = "bindable_calibration_qc_review_required"
    elif quality_grade or queue_status:
        decision = "bindable_reject_only_not_fit"
    else:
        decision = "not_bindable_metadata_only"
    return QcGapCandidate(
        point_id=point.point_id,
        family_id=point.family_id,
        route_kind=point.route_kind,
        candidate_type="same_run_queue_manifest_with_quality",
        bind_decision=decision,
        candidate_path=str(manifest),
        quality_grade=quality_grade,
        sample_can_enter_calibration_fit=can_fit,
        sample_can_enter_diagnostic_model=can_diag,
        reason=failure,
        same_run=True,
        same_point_id=True,
        same_physical_point=True,
    )


def _quality_candidate_from_dir(point: QcGapPoint, candidate_dir: Path, *, same_run: bool) -> QcGapCandidate | None:
    formal_quality = candidate_dir / "formal_open_flow_data_quality_by_analyzer.csv"
    frame_quality = candidate_dir / "frame_quality_summary.csv"
    if formal_quality.exists():
        rows = _read_csv_rows(formal_quality)
        can_fit_count = sum(1 for row in rows if _boolish(row.get("sample_can_enter_calibration_fit")) is True)
        reject_count = len(rows) - can_fit_count
        grades: dict[str, int] = {}
        reasons: list[str] = []
        for row in rows:
            grade = str(row.get("grade") or "unknown")
            grades[grade] = grades.get(grade, 0) + 1
            reason = str(row.get("reason") or "").strip()
            if reason:
                reasons.append(reason)
        same_point_id = candidate_dir.name == point.point_id
        same_physical = _same_physical_point(point, candidate_dir.name)
        if same_run and same_point_id:
            decision = "bindable_point_quality"
        elif same_physical:
            decision = "cross_run_reference_not_direct_bind"
        else:
            decision = "not_bindable_different_point"
        return QcGapCandidate(
            point_id=point.point_id,
            family_id=point.family_id,
            route_kind=point.route_kind,
            candidate_type="formal_open_flow_data_quality_by_analyzer",
            bind_decision=decision,
            candidate_path=str(formal_quality),
            quality_grade=_fmt(grades),
            sample_can_enter_calibration_fit=can_fit_count > 0,
            sample_can_enter_diagnostic_model=None,
            reason=f"eligible_rows={can_fit_count};rejected_rows={reject_count};" + ";".join(sorted(set(reasons)))[:700],
            same_run=same_run,
            same_point_id=same_point_id,
            same_physical_point=same_physical,
        )
    if frame_quality.exists():
        rows = _read_csv_rows(frame_quality)
        same_point_id = candidate_dir.name == point.point_id
        same_physical = _same_physical_point(point, candidate_dir.name)
        return QcGapCandidate(
            point_id=point.point_id,
            family_id=point.family_id,
            route_kind=point.route_kind,
            candidate_type="frame_quality_summary",
            bind_decision="bindable_frame_quality_only_review_required" if same_run and same_point_id else "cross_run_reference_not_direct_bind",
            candidate_path=str(frame_quality),
            quality_grade="frame_quality_only",
            sample_can_enter_calibration_fit=None,
            sample_can_enter_diagnostic_model=None,
            reason=f"frame_quality_rows={len(rows)}",
            same_run=same_run,
            same_point_id=same_point_id,
            same_physical_point=same_physical,
        )
    return None


def _find_same_run_manifest_candidates(point: QcGapPoint) -> list[QcGapCandidate]:
    point_dir = Path(point.point_path)
    candidates: list[QcGapCandidate] = []
    seen_manifests: set[Path] = set()
    for ancestor in _ancestor_dirs(point_dir):
        if not ancestor.exists():
            continue
        manifests = list(ancestor.glob("**/queue_manifest_with_quality.csv"))
        for manifest in manifests:
            if manifest in seen_manifests:
                continue
            seen_manifests.add(manifest)
            for row in _read_csv_rows(manifest):
                if str(row.get("point_run_id") or "") == point.point_id:
                    candidates.append(_candidate_from_manifest_row(point, manifest, row))
    return candidates


def _find_local_raw_evidence(point: QcGapPoint) -> list[QcGapCandidate]:
    point_dir = Path(point.point_path)
    if not point_dir.exists():
        return []
    raw_files = [
        path
        for path in point_dir.iterdir()
        if path.is_file()
        and (
            path.name.startswith("io_")
            or path.name in {"formal_open_flow_route_timing.json", "pressure_transition_trace.csv", "runtime_config_snapshot.json"}
        )
    ]
    if not raw_files:
        return []
    return [
        QcGapCandidate(
            point_id=point.point_id,
            family_id=point.family_id,
            route_kind=point.route_kind,
            candidate_type="raw_sampling_evidence_without_qc",
            bind_decision="not_bindable_raw_only",
            candidate_path=str(point_dir),
            quality_grade="",
            sample_can_enter_calibration_fit=None,
            sample_can_enter_diagnostic_model=None,
            reason=f"raw_files={len(raw_files)};requires QC derivation before fit binding",
            same_run=True,
            same_point_id=True,
            same_physical_point=True,
        )
    ]


def _temp_token(value: float | None) -> str:
    if value is None:
        return ""
    number = int(float(value))
    return f"Tm{abs(number)}" if number < 0 else f"T{number}"


def _candidate_point_globs(point: QcGapPoint) -> list[str]:
    patterns = [point.point_id]
    temp = _temp_token(point.temp_c)
    if point.route_kind == "co2" and temp and point.co2_ppm is not None:
        patterns.append(f"p*_{temp}_{int(float(point.co2_ppm))}ppm*")
    elif point.route_kind == "h2o" and temp and point.hgen_c is not None and point.rh_pct is not None:
        hgen = _temp_token(point.hgen_c).replace("T", "HG", 1)
        patterns.append(f"p*_{temp}_{hgen}C_{int(float(point.rh_pct))}RH*")
    return list(dict.fromkeys(patterns))


def _iter_candidate_point_dirs(point: QcGapPoint, search_root: Path) -> Iterable[Path]:
    if not search_root.exists():
        return []
    seen: set[Path] = set()
    matches: list[Path] = []
    for pattern in _candidate_point_globs(point):
        for path in search_root.rglob(pattern):
            if path.is_dir() and path not in seen:
                seen.add(path)
                matches.append(path)
    return matches


def _find_cross_run_candidates(point: QcGapPoint, search_roots: Sequence[str | Path]) -> list[QcGapCandidate]:
    candidates: list[QcGapCandidate] = []
    original = Path(point.point_path)
    for root in search_roots:
        root_path = Path(root)
        for candidate_dir in _iter_candidate_point_dirs(point, root_path):
            if candidate_dir == original:
                continue
            if candidate_dir.name != point.point_id and not _same_physical_point(point, candidate_dir.name):
                continue
            candidate = _quality_candidate_from_dir(point, candidate_dir, same_run=False)
            if candidate is not None:
                candidates.append(candidate)
    return candidates


def _summarize_point(point: QcGapPoint, candidates: Sequence[QcGapCandidate]) -> dict[str, Any]:
    decisions = [candidate.bind_decision for candidate in candidates]
    if any(decision == "bindable_point_quality" for decision in decisions):
        recommendation = "bind_same_run_point_quality"
    elif any(decision == "bindable_reject_only_not_fit" for decision in decisions):
        recommendation = "bind_same_run_reject_only_quality"
    elif any(decision == "bindable_frame_quality_only_review_required" for decision in decisions):
        recommendation = "bind_frame_quality_with_review"
    elif any(decision == "cross_run_reference_not_direct_bind" for decision in decisions):
        recommendation = "cross_run_reference_only_find_same_run_qc_or_retry"
    elif any(decision == "not_bindable_raw_only" for decision in decisions):
        recommendation = "raw_only_generate_qc_or_rerun_targeted_point"
    else:
        recommendation = "no_candidate_found"
    return {
        **point.to_json(),
        "candidate_count": len(candidates),
        "candidate_decisions": sorted(set(decisions)),
        "recommendation": recommendation,
    }


def build_v1_5_historical_replay_qc_gap_audit(
    *,
    replay_evidence_path: str | Path,
    search_roots: Sequence[str | Path] = (),
) -> dict[str, Any]:
    """Audit missing-QC replay points and propose read-only binding decisions."""

    replay_file = Path(replay_evidence_path).resolve()
    model = _load_replay_model(replay_file)
    points = _missing_qc_points(model)
    candidates: list[QcGapCandidate] = []
    for point in points:
        candidates.extend(_find_same_run_manifest_candidates(point))
        local_quality = _quality_candidate_from_dir(point, Path(point.point_path), same_run=True)
        if local_quality is not None:
            candidates.append(local_quality)
        candidates.extend(_find_local_raw_evidence(point))
        candidates.extend(_find_cross_run_candidates(point, search_roots))

    point_summaries = [_summarize_point(point, [candidate for candidate in candidates if candidate.point_id == point.point_id and candidate.family_id == point.family_id]) for point in points]
    direct_bindable = [
        row
        for row in point_summaries
        if row["recommendation"]
        in {
            "bind_same_run_point_quality",
            "bind_same_run_reject_only_quality",
            "bind_frame_quality_with_review",
        }
    ]
    unresolved = [
        row
        for row in point_summaries
        if row["recommendation"]
        in {
            "cross_run_reference_only_find_same_run_qc_or_retry",
            "raw_only_generate_qc_or_rerun_targeted_point",
            "no_candidate_found",
        }
    ]

    checks = [
        _check(
            check_id="missing_qc_points_loaded",
            title="Missing-QC points are loaded from replay evidence",
            status="pass" if points else "review_required",
            reason="the audit starts from binder-discovered missing QC points",
            expected="one or more missing QC points when replay evidence is blocked on QC",
            observed=[point.to_json() for point in points],
            physical_meaning="The audit must explain the exact replay blocker instead of scanning unrelated historical data.",
        ),
        _check(
            check_id="same_run_reject_only_bindings_identified",
            title="Same-run reject-only QC candidates are separated from fit bindings",
            status="pass" if any(row["recommendation"] == "bind_same_run_reject_only_quality" for row in point_summaries) else "review_required",
            reason="queue-manifest C_reject rows can close missing-QC metadata but cannot enter calibration fit",
            expected="reject-only candidates stay non-fit",
            observed=point_summaries,
            physical_meaning="A failed or C_reject point can be traceable evidence without becoming a calibration point.",
        ),
        _check(
            check_id="cross_run_quality_not_directly_bound",
            title="Cross-run quality remains reference-only",
            status="pass",
            reason="quality from another family/run is reported as reference and never used as direct replacement",
            expected="cross_run_reference_not_direct_bind",
            observed=[candidate.to_json() for candidate in candidates if candidate.bind_decision == "cross_run_reference_not_direct_bind"],
            physical_meaning="Same physical gas point in another run can guide diagnosis, but it does not prove this device/run was stable.",
        ),
        _check(
            check_id="unresolved_gaps_remain_review_required",
            title="Unresolved QC gaps remain review-required",
            status="review_required" if unresolved else "pass",
            reason="points with only raw IO or cross-run reference still need QC derivation, retry evidence, or targeted rerun",
            expected="unresolved gaps are not promoted",
            observed=unresolved,
            physical_meaning="This keeps replay from silently manufacturing fit-ready evidence.",
        ),
        _check(
            check_id="qc_gap_audit_is_read_only",
            title="QC gap audit is read-only",
            status="pass",
            reason="this audit writes only JSON/CSV/Markdown artifacts",
            expected={
                "opens_com_ports": False,
                "connects_postgresql": False,
                "writes_coefficients": False,
                "formal_release_allowed": False,
            },
            observed={
                "opens_com_ports": False,
                "connects_postgresql": False,
                "writes_coefficients": False,
                "formal_release_allowed": False,
            },
            physical_meaning="QC gap closure planning must not become hidden hardware control or release authorization.",
        ),
    ]
    status = "blocked" if any(check.status == "blocker" for check in checks) else (
        "review_required" if any(check.status == "review_required" for check in checks) else "pass"
    )
    manifest = {
        "schema": SCHEMA,
        "created_at": _now(),
        "replay_evidence_path": str(replay_file),
        "status": status,
        "blocker_count": sum(1 for check in checks if check.status == "blocker"),
        "review_required_count": sum(1 for check in checks if check.status == "review_required"),
        "missing_qc_point_count": len(points),
        "candidate_count": len(candidates),
        "direct_bindable_point_count": len(direct_bindable),
        "unresolved_point_count": len(unresolved),
        "no_write": True,
        "opens_com_ports": False,
        "connects_postgresql": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_device_id": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
    }
    return {
        "manifest": manifest,
        "missing_points": point_summaries,
        "candidate_evidence": [candidate.to_json() for candidate in candidates],
        "checks": [check.to_json() for check in checks],
    }


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(str(key))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(
            [
                {
                    key: _fmt(value) if isinstance(value, (dict, list, tuple)) else value
                    for key, value in dict(row).items()
                }
                for row in rows
            ]
        )


def _render_markdown(model: Mapping[str, Any]) -> str:
    manifest = model.get("manifest", {})
    lines = [
        "# V1.5 Historical Replay QC Gap Audit",
        "",
        f"- schema: `{manifest.get('schema')}`",
        f"- status: `{manifest.get('status')}`",
        f"- blocker_count: `{manifest.get('blocker_count')}`",
        f"- review_required_count: `{manifest.get('review_required_count')}`",
        f"- missing_qc_point_count: `{manifest.get('missing_qc_point_count')}`",
        f"- direct_bindable_point_count: `{manifest.get('direct_bindable_point_count')}`",
        f"- unresolved_point_count: `{manifest.get('unresolved_point_count')}`",
        f"- replay_evidence_path: `{manifest.get('replay_evidence_path')}`",
        "",
        "## Physical Boundaries",
        "",
        f"- opens_com_ports: `{manifest.get('opens_com_ports')}`",
        f"- connects_postgresql: `{manifest.get('connects_postgresql')}`",
        f"- controls_water_or_gas_routes: `{manifest.get('controls_water_or_gas_routes')}`",
        f"- writes_coefficients: `{manifest.get('writes_coefficients')}`",
        f"- formal_release_allowed: `{manifest.get('formal_release_allowed')}`",
        f"- database_import_allowed: `{manifest.get('database_import_allowed')}`",
        f"- not_real_acceptance_evidence: `{manifest.get('not_real_acceptance_evidence')}`",
        "",
        "## Missing Points",
        "",
        "| Point | Family | Route | Recommendation | Candidate count |",
        "|---|---|---|---|---:|",
    ]
    for row in model.get("missing_points", []):
        lines.append(
            f"| `{row.get('point_id')}` | `{row.get('family_id')}` | `{row.get('route_kind')}` | "
            f"`{row.get('recommendation')}` | {row.get('candidate_count')} |"
        )

    lines.extend(
        [
            "",
            "## Candidate Evidence",
            "",
            "| Point | Candidate type | Decision | Fit? | Path |",
            "|---|---|---|---|---|",
        ]
    )
    for row in model.get("candidate_evidence", []):
        lines.append(
            f"| `{row.get('point_id')}` | `{row.get('candidate_type')}` | `{row.get('bind_decision')}` | "
            f"`{row.get('sample_can_enter_calibration_fit')}` | `{row.get('candidate_path')}` |"
        )

    lines.extend(
        [
            "",
            "## Checks",
            "",
            "| Check | Status | Reason | Physical meaning |",
            "|---|---|---|---|",
        ]
    )
    for row in model.get("checks", []):
        lines.append(
            f"| `{row.get('check_id')}` | `{row.get('status')}` | {row.get('reason')} | "
            f"{row.get('physical_meaning')} |"
        )
    return "\n".join(lines).rstrip() + "\n"


def write_v1_5_historical_replay_qc_gap_audit(
    *,
    replay_evidence_path: str | Path,
    search_roots: Sequence[str | Path],
    output_dir: str | Path,
) -> dict[str, str]:
    """Write JSON/CSV/Markdown QC gap audit artifacts."""

    model = build_v1_5_historical_replay_qc_gap_audit(
        replay_evidence_path=replay_evidence_path,
        search_roots=search_roots,
    )
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    outputs = {
        "manifest": out / "v1_5_historical_replay_qc_gap_audit.json",
        "missing_points": out / "v1_5_historical_replay_qc_missing_points.csv",
        "candidate_evidence": out / "v1_5_historical_replay_qc_candidate_evidence.csv",
        "checks": out / "v1_5_historical_replay_qc_gap_audit_checks.csv",
        "markdown": out / "V1_5_HISTORICAL_REPLAY_QC_GAP_AUDIT.md",
    }
    outputs["manifest"].write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(outputs["missing_points"], model["missing_points"])
    _write_csv(outputs["candidate_evidence"], model["candidate_evidence"])
    _write_csv(outputs["checks"], model["checks"])
    outputs["markdown"].write_text(_render_markdown(model), encoding="utf-8")
    return {key: str(path) for key, path in outputs.items()}
