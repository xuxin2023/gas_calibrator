"""Audit missing historical replay points for bindable segmented evidence.

The historical replay evidence binder reports missing expected physical points.
This audit explains those missing points without changing fit eligibility: it
looks for matching segmented/retry point directories and separates supplemental
new-algorithm gaps from mature-route gaps.
"""

from __future__ import annotations

import csv
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


SCHEMA = "v1_5_historical_replay_missing_point_audit_v1"
CO2_RE = re.compile(r"^p(?P<index>\d+)_T(?P<temp>m?\d+)_(?P<ppm>\d+)ppm", re.IGNORECASE)
H2O_RE = re.compile(
    r"^p(?P<index>\d+)_T(?P<temp>m?\d+)_HG(?P<hgen>m?\d+)C_(?P<rh>\d+)RH",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class MissingPoint:
    family_id: str
    route_kind: str
    algorithm_profile_id: str
    point_key: str
    temp_c: float | None
    co2_ppm: float | None
    hgen_c: float | None
    rh_pct: float | None
    is_new_algorithm_supplemental: bool
    source_summary_status: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MissingPointCandidate:
    family_id: str
    route_kind: str
    point_key: str
    candidate_type: str
    bind_decision: str
    candidate_path: str
    quality_source: str
    has_quality_evidence: bool
    has_raw_sampling_evidence: bool
    same_family_hint: bool
    same_physical_point: bool
    reason: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MissingPointAuditCheck:
    check_id: str
    title: str
    status: str
    reason: str
    expected: str
    observed: str
    physical_meaning: str
    blocks_missing_point_closure: bool

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
) -> MissingPointAuditCheck:
    return MissingPointAuditCheck(
        check_id=check_id,
        title=title,
        status=status,
        reason=reason,
        expected=_fmt(expected),
        observed=_fmt(observed),
        physical_meaning=physical_meaning,
        blocks_missing_point_closure=status == "blocker",
    )


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _load_json(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def _parse_missing_key(route_kind: str, point_key: str) -> dict[str, float | None]:
    parts = [part for part in str(point_key).split("/") if part != ""]
    if route_kind == "co2" and len(parts) == 2:
        return {"temp_c": float(parts[0]), "co2_ppm": float(parts[1]), "hgen_c": None, "rh_pct": None}
    if route_kind == "h2o" and len(parts) == 3:
        return {"temp_c": float(parts[0]), "co2_ppm": None, "hgen_c": float(parts[1]), "rh_pct": float(parts[2])}
    return {"temp_c": None, "co2_ppm": None, "hgen_c": None, "rh_pct": None}


def _temp_token(value: float | None) -> str:
    if value is None:
        return ""
    number = int(float(value))
    return f"Tm{abs(number)}" if number < 0 else f"T{number}"


def _hgen_token(value: float | None) -> str:
    if value is None:
        return ""
    number = int(float(value))
    return f"HGm{abs(number)}C" if number < 0 else f"HG{number}C"


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
    return {"route_kind": "unknown", "temp_c": None, "co2_ppm": None, "hgen_c": None, "rh_pct": None}


def _parse_temp(token: str) -> float:
    return float(-int(token[1:]) if token.lower().startswith("m") else int(token))


def _same_physical_point(point: MissingPoint, candidate_name: str) -> bool:
    identity = _point_identity(candidate_name)
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


def _profiles_by_id(profile_config: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {
        str(profile.get("profile_id") or ""): profile
        for profile in profile_config.get("profiles", [])
        if isinstance(profile, Mapping)
    }


def _supplemental_keys(profile: Mapping[str, Any], route_kind: str) -> set[str]:
    keys: set[str] = set()
    if route_kind == "co2":
        route = profile.get("co2_route", {}) if isinstance(profile, Mapping) else {}
        policy = route.get("supplement_policy", {}) if isinstance(route, Mapping) else {}
        points = policy.get("required_new_algorithm_supplemental_gas_points", []) if isinstance(policy, Mapping) else []
        for item in points or []:
            if isinstance(item, Mapping):
                keys.add(f"{float(item.get('temperature_c')):g}/{float(item.get('co2_ppm')):g}")
    if route_kind == "h2o":
        route = profile.get("h2o_route", {}) if isinstance(profile, Mapping) else {}
        points = route.get("required_new_algorithm_supplemental_wet_points", []) if isinstance(route, Mapping) else []
        for item in points:
            if not isinstance(item, Mapping):
                continue
            match = re.match(r"HGEN(?P<hgen>m?\d+)C", str(item.get("humidity_generator") or ""), re.IGNORECASE)
            if not match:
                continue
            hgen = _parse_temp(match.group("hgen"))
            keys.add(f"{float(item.get('temperature_c')):g}/{hgen:g}/{float(item.get('relative_humidity_pct')):g}")
    return keys


def _missing_points(replay_model: Mapping[str, Any], profile_config: Mapping[str, Any]) -> list[MissingPoint]:
    profiles = _profiles_by_id(profile_config)
    points: list[MissingPoint] = []
    for summary in replay_model.get("route_summaries", []):
        route_kind = str(summary.get("route_kind") or "")
        profile_id = str(summary.get("algorithm_profile_id") or "")
        supplements = _supplemental_keys(profiles.get(profile_id, {}), route_kind)
        for point_key in summary.get("missing_expected_points") or []:
            identity = _parse_missing_key(route_kind, str(point_key))
            points.append(
                MissingPoint(
                    family_id=str(summary.get("family_id") or ""),
                    route_kind=route_kind,
                    algorithm_profile_id=profile_id,
                    point_key=str(point_key),
                    temp_c=identity["temp_c"],
                    co2_ppm=identity["co2_ppm"],
                    hgen_c=identity["hgen_c"],
                    rh_pct=identity["rh_pct"],
                    is_new_algorithm_supplemental=str(point_key) in supplements,
                    source_summary_status=str(summary.get("status") or ""),
                )
            )
    return points


def _candidate_globs(point: MissingPoint) -> list[str]:
    temp = _temp_token(point.temp_c)
    if point.route_kind == "co2" and temp and point.co2_ppm is not None:
        return [f"p*_{temp}_{int(float(point.co2_ppm))}ppm*"]
    if point.route_kind == "h2o" and temp and point.hgen_c is not None and point.rh_pct is not None:
        return [f"p*_{temp}_{_hgen_token(point.hgen_c)}_{int(float(point.rh_pct))}RH*"]
    return []


def _iter_candidate_dirs(point: MissingPoint, search_roots: Sequence[str | Path]) -> Iterable[Path]:
    seen: set[Path] = set()
    for root in search_roots:
        root_path = Path(root)
        if not root_path.exists():
            continue
        for pattern in _candidate_globs(point):
            for path in root_path.rglob(pattern):
                if path.is_dir() and path not in seen:
                    seen.add(path)
                    yield path


def _candidate_from_dir(point: MissingPoint, candidate_dir: Path) -> MissingPointCandidate:
    formal_quality = candidate_dir / "formal_open_flow_data_quality_by_analyzer.csv"
    frame_quality = candidate_dir / "frame_quality_summary.csv"
    raw_files = [
        path
        for path in candidate_dir.iterdir()
        if path.is_file()
        and (
            path.name.startswith("io_")
            or path.name in {"formal_open_flow_route_timing.json", "pressure_transition_trace.csv", "runtime_config_snapshot.json"}
        )
    ]
    quality_source = ""
    reason = ""
    if formal_quality.exists():
        rows = _read_csv_rows(formal_quality)
        fit_rows = sum(1 for row in rows if str(row.get("sample_can_enter_calibration_fit") or "").lower() == "true")
        quality_source = "formal_open_flow_data_quality_by_analyzer"
        reason = f"formal_quality_rows={len(rows)};fit_rows={fit_rows}"
    elif frame_quality.exists():
        rows = _read_csv_rows(frame_quality)
        quality_source = "frame_quality_summary"
        reason = f"frame_quality_rows={len(rows)}"
    elif raw_files:
        reason = f"raw_files={len(raw_files)};requires QC derivation before replay binding"

    same_physical = _same_physical_point(point, candidate_dir.name)
    same_family_hint = point.family_id.lower().split("_")[0] in str(candidate_dir).lower()
    if quality_source and same_physical and same_family_hint:
        decision = "segmented_quality_candidate_review_bind"
    elif raw_files and same_physical and same_family_hint:
        decision = "segmented_raw_only_qc_derivation_required"
    elif same_physical:
        decision = "cross_family_reference_not_direct_bind"
    else:
        decision = "not_bindable_different_point"
    return MissingPointCandidate(
        family_id=point.family_id,
        route_kind=point.route_kind,
        point_key=point.point_key,
        candidate_type="segmented_or_retry_point_directory",
        bind_decision=decision,
        candidate_path=str(candidate_dir),
        quality_source=quality_source,
        has_quality_evidence=bool(quality_source),
        has_raw_sampling_evidence=bool(raw_files),
        same_family_hint=same_family_hint,
        same_physical_point=same_physical,
        reason=reason,
    )


def _summarize_point(point: MissingPoint, candidates: Sequence[MissingPointCandidate]) -> dict[str, Any]:
    decisions = [candidate.bind_decision for candidate in candidates]
    if any(decision == "segmented_quality_candidate_review_bind" for decision in decisions):
        recommendation = "review_bind_segmented_quality_candidate"
    elif any(decision == "segmented_raw_only_qc_derivation_required" for decision in decisions):
        recommendation = "derive_qc_from_segmented_raw_candidate"
    elif point.is_new_algorithm_supplemental:
        recommendation = "targeted_supplemental_resampling_candidate"
    else:
        recommendation = "find_segmented_evidence_or_targeted_resampling"
    return {
        **point.to_json(),
        "candidate_count": len(candidates),
        "candidate_decisions": sorted(set(decisions)),
        "recommendation": recommendation,
    }


def build_v1_5_historical_replay_missing_point_audit(
    *,
    replay_evidence_path: str | Path,
    profile_path: str | Path,
    search_roots: Sequence[str | Path] = (),
) -> dict[str, Any]:
    """Audit missing replay points and look for segmented/retry evidence."""

    replay_file = Path(replay_evidence_path).resolve()
    profile_file = Path(profile_path).resolve()
    replay_model = _load_json(replay_file)
    profile_config = _load_json(profile_file)
    points = _missing_points(replay_model, profile_config)

    candidates: list[MissingPointCandidate] = []
    for point in points:
        for candidate_dir in _iter_candidate_dirs(point, search_roots):
            candidate = _candidate_from_dir(point, candidate_dir)
            if candidate.same_physical_point:
                candidates.append(candidate)

    summaries = [
        _summarize_point(
            point,
            [candidate for candidate in candidates if candidate.family_id == point.family_id and candidate.point_key == point.point_key],
        )
        for point in points
    ]
    segmented_bindable = [
        row for row in summaries if row["recommendation"] == "review_bind_segmented_quality_candidate"
    ]
    supplemental_unresolved = [
        row for row in summaries if row["recommendation"] == "targeted_supplemental_resampling_candidate"
    ]
    unresolved = [
        row
        for row in summaries
        if row["recommendation"]
        in {
            "targeted_supplemental_resampling_candidate",
            "derive_qc_from_segmented_raw_candidate",
            "find_segmented_evidence_or_targeted_resampling",
        }
    ]
    checks = [
        _check(
            check_id="missing_points_loaded",
            title="Missing expected points are loaded from replay evidence",
            status="pass" if points else "review_required",
            reason="the audit starts from route-summarized missing expected points",
            expected="one or more missing_expected_points when replay route summary is incomplete",
            observed=[point.to_json() for point in points],
            physical_meaning="This separates truly absent physical points from points that only lack QC evidence.",
        ),
        _check(
            check_id="segmented_quality_candidates_found",
            title="Segmented/retry quality candidates are identified",
            status="pass" if segmented_bindable else "review_required",
            reason="split runs can provide missing physical points, but must be reviewed before binding",
            expected="segmented quality candidates stay review-bind, not auto-fit",
            observed=segmented_bindable,
            physical_meaning="A split-run point can close a missing-point gap only after matching physical state and QC are reviewed.",
        ),
        _check(
            check_id="supplemental_points_remain_explicit",
            title="New-algorithm supplemental missing points remain explicit",
            status="review_required" if supplemental_unresolved else "pass",
            reason="new algorithm supplemental points are candidate-specific requirements and must not be hidden by mature 45/13 replay",
            expected="supplemental gaps listed separately",
            observed=supplemental_unresolved,
            physical_meaning="The extra -20/-10 600ppm and 40C/HGEN30C/30RH points are part of the new-algorithm candidate contract.",
        ),
        _check(
            check_id="unresolved_points_not_promoted",
            title="Unresolved missing points are not promoted",
            status="review_required" if unresolved else "pass",
            reason="missing physical points require segmented evidence review, QC derivation, or targeted resampling",
            expected="unresolved missing points stay review-required",
            observed=unresolved,
            physical_meaning="Replay must not manufacture calibration evidence for physical points that were not observed.",
        ),
        _check(
            check_id="missing_point_audit_is_read_only",
            title="Missing-point audit is read-only",
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
            physical_meaning="A replay missing-point audit can plan evidence binding, not operate hardware or release data.",
        ),
    ]
    status = "blocked" if any(check.status == "blocker" for check in checks) else (
        "review_required" if any(check.status == "review_required" for check in checks) else "pass"
    )
    manifest = {
        "schema": SCHEMA,
        "created_at": _now(),
        "replay_evidence_path": str(replay_file),
        "profile_path": str(profile_file),
        "status": status,
        "blocker_count": sum(1 for check in checks if check.status == "blocker"),
        "review_required_count": sum(1 for check in checks if check.status == "review_required"),
        "missing_point_count": len(points),
        "candidate_count": len(candidates),
        "segmented_quality_candidate_count": len(segmented_bindable),
        "supplemental_unresolved_count": len(supplemental_unresolved),
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
        "missing_points": summaries,
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
        "# V1.5 Historical Replay Missing Point Audit",
        "",
        f"- schema: `{manifest.get('schema')}`",
        f"- status: `{manifest.get('status')}`",
        f"- blocker_count: `{manifest.get('blocker_count')}`",
        f"- review_required_count: `{manifest.get('review_required_count')}`",
        f"- missing_point_count: `{manifest.get('missing_point_count')}`",
        f"- segmented_quality_candidate_count: `{manifest.get('segmented_quality_candidate_count')}`",
        f"- supplemental_unresolved_count: `{manifest.get('supplemental_unresolved_count')}`",
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
        "| Point | Family | Route | Supplemental? | Recommendation | Candidates |",
        "|---|---|---|---:|---|---:|",
    ]
    for row in model.get("missing_points", []):
        lines.append(
            f"| `{row.get('point_key')}` | `{row.get('family_id')}` | `{row.get('route_kind')}` | "
            f"`{row.get('is_new_algorithm_supplemental')}` | `{row.get('recommendation')}` | {row.get('candidate_count')} |"
        )

    lines.extend(["", "## Candidate Evidence", "", "| Point | Decision | Quality source | Path |", "|---|---|---|---|"])
    for row in model.get("candidate_evidence", []):
        lines.append(
            f"| `{row.get('point_key')}` | `{row.get('bind_decision')}` | `{row.get('quality_source')}` | "
            f"`{row.get('candidate_path')}` |"
        )

    lines.extend(["", "## Checks", "", "| Check | Status | Reason | Physical meaning |", "|---|---|---|---|"])
    for row in model.get("checks", []):
        lines.append(
            f"| `{row.get('check_id')}` | `{row.get('status')}` | {row.get('reason')} | "
            f"{row.get('physical_meaning')} |"
        )
    return "\n".join(lines).rstrip() + "\n"


def write_v1_5_historical_replay_missing_point_audit(
    *,
    replay_evidence_path: str | Path,
    profile_path: str | Path,
    search_roots: Sequence[str | Path],
    output_dir: str | Path,
) -> dict[str, str]:
    """Write JSON/CSV/Markdown missing-point audit artifacts."""

    model = build_v1_5_historical_replay_missing_point_audit(
        replay_evidence_path=replay_evidence_path,
        profile_path=profile_path,
        search_roots=search_roots,
    )
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    outputs = {
        "manifest": out / "v1_5_historical_replay_missing_point_audit.json",
        "missing_points": out / "v1_5_historical_replay_missing_points.csv",
        "candidate_evidence": out / "v1_5_historical_replay_missing_point_candidates.csv",
        "checks": out / "v1_5_historical_replay_missing_point_audit_checks.csv",
        "markdown": out / "V1_5_HISTORICAL_REPLAY_MISSING_POINT_AUDIT.md",
    }
    outputs["manifest"].write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(outputs["missing_points"], model["missing_points"])
    _write_csv(outputs["candidate_evidence"], model["candidate_evidence"])
    _write_csv(outputs["checks"], model["checks"])
    outputs["markdown"].write_text(_render_markdown(model), encoding="utf-8")
    return {key: str(path) for key, path in outputs.items()}
