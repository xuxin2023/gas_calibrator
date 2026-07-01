"""Read-only V1.5 historical evidence binder.

This module binds existing point-level CSV/JSON evidence into a replay status
model. It is intentionally conservative: it reports missing points, missing QC,
and rejected rows instead of repairing data or reclassifying samples. It never
opens COM ports, connects to PostgreSQL, controls routes, or writes device
state.
"""

from __future__ import annotations

import csv
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .v1_5_historical_replay_contract import EXPECTED_ABSORPTION_FORMULA


SCHEMA = "v1_5_historical_replay_evidence_v1"

POINT_RE = re.compile(r"^p(?P<index>\d+)_T(?P<temp>m?\d+)_")
CO2_RE = re.compile(r"^p(?P<index>\d+)_T(?P<temp>m?\d+)_(?P<ppm>\d+)ppm", re.IGNORECASE)
H2O_RE = re.compile(
    r"^p(?P<index>\d+)_T(?P<temp>m?\d+)_HG(?P<hgen>m?\d+)C_(?P<rh>\d+)RH",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class HistoricalEvidenceRoot:
    family_id: str
    route_kind: str
    root_path: str
    label: str = ""
    algorithm_profile_id: str = ""

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class HistoricalPointEvidence:
    family_id: str
    route_kind: str
    point_id: str
    point_index: int | None
    temp_c: float | None
    co2_ppm: float | None
    hgen_c: float | None
    rh_pct: float | None
    point_path: str
    has_conclusion_summary: bool
    has_frame_quality_summary: bool
    has_formal_quality_by_analyzer: bool
    quality_source: str
    analyzer_rows: int
    fit_eligible_rows: int
    rejected_rows: int
    grade_counts: dict[str, int]
    conclusion_risk_level: str
    status: str
    exclusion_reason: str

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class HistoricalReplayEvidenceCheck:
    check_id: str
    title: str
    status: str
    reason: str
    expected: str
    observed: str
    physical_meaning: str
    blocks_replay_binding: bool

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _fmt(value: Any) -> str:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _status_from_checks(checks: Sequence[HistoricalReplayEvidenceCheck]) -> str:
    if any(check.status == "blocker" for check in checks):
        return "blocked"
    if any(check.status == "review_required" for check in checks):
        return "review_required"
    return "pass"


def _check(
    *,
    check_id: str,
    title: str,
    status: str,
    reason: str,
    expected: Any,
    observed: Any,
    physical_meaning: str,
) -> HistoricalReplayEvidenceCheck:
    return HistoricalReplayEvidenceCheck(
        check_id=check_id,
        title=title,
        status=status,
        reason=reason,
        expected=_fmt(expected),
        observed=_fmt(observed),
        physical_meaning=physical_meaning,
        blocks_replay_binding=status == "blocker",
    )


def _load_profile(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise ValueError("V1.5 algorithm route profile must be a JSON object")
    return payload


def _profiles_by_id(config: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {
        str(profile.get("profile_id") or ""): profile
        for profile in config.get("profiles", [])
        if isinstance(profile, Mapping)
    }


def _parse_temp(token: str) -> float:
    return float(-int(token[1:]) if token.lower().startswith("m") else int(token))


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _read_first_csv_row(path: Path) -> dict[str, str]:
    rows = _read_csv_rows(path)
    return rows[0] if rows else {}


def _boolish(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _point_identity(path: Path, route_hint: str = "") -> dict[str, Any] | None:
    name = path.name
    h2o_match = H2O_RE.match(name)
    if h2o_match:
        return {
            "route_kind": "h2o",
            "point_index": int(h2o_match.group("index")),
            "temp_c": _parse_temp(h2o_match.group("temp")),
            "co2_ppm": None,
            "hgen_c": _parse_temp(h2o_match.group("hgen")),
            "rh_pct": float(h2o_match.group("rh")),
        }
    co2_match = CO2_RE.match(name)
    if co2_match:
        return {
            "route_kind": "co2",
            "point_index": int(co2_match.group("index")),
            "temp_c": _parse_temp(co2_match.group("temp")),
            "co2_ppm": float(co2_match.group("ppm")),
            "hgen_c": None,
            "rh_pct": None,
        }
    generic_match = POINT_RE.match(name)
    if generic_match:
        return {
            "route_kind": route_hint or "unknown",
            "point_index": int(generic_match.group("index")),
            "temp_c": _parse_temp(generic_match.group("temp")),
            "co2_ppm": None,
            "hgen_c": None,
            "rh_pct": None,
        }
    return None


def _quality_from_formal_rows(rows: Sequence[Mapping[str, str]]) -> dict[str, Any]:
    grade_counts: dict[str, int] = {}
    eligible = 0
    rejected = 0
    reasons: list[str] = []
    for row in rows:
        grade = str(row.get("grade") or "").strip() or "unknown"
        grade_counts[grade] = grade_counts.get(grade, 0) + 1
        if _boolish(row.get("sample_can_enter_calibration_fit")):
            eligible += 1
        else:
            rejected += 1
        reason = str(row.get("reason") or "").strip()
        if reason:
            reasons.append(reason)
    return {
        "quality_source": "formal_open_flow_data_quality_by_analyzer",
        "analyzer_rows": len(rows),
        "fit_eligible_rows": eligible,
        "rejected_rows": rejected,
        "grade_counts": grade_counts,
        "exclusion_reason": ";".join(sorted(set(reasons)))[:1000],
    }


def _quality_from_frame_rows(rows: Sequence[Mapping[str, str]]) -> dict[str, Any]:
    reasons: list[str] = []
    valid_rows = 0
    for row in rows:
        if str(row.get("ValidRatio") or "").strip():
            valid_rows += 1
        reason = str(row.get("UnusableReasonTopN") or "").strip()
        if reason:
            reasons.append(reason)
    return {
        "quality_source": "frame_quality_summary",
        "analyzer_rows": len(rows),
        "fit_eligible_rows": 0,
        "rejected_rows": 0,
        "grade_counts": {"frame_quality_only": valid_rows},
        "exclusion_reason": ";".join(sorted(set(reasons)))[:1000],
    }


def _read_point(root: HistoricalEvidenceRoot, point_dir: Path) -> HistoricalPointEvidence | None:
    identity = _point_identity(point_dir, root.route_kind)
    if not identity:
        return None

    formal_quality_path = point_dir / "formal_open_flow_data_quality_by_analyzer.csv"
    frame_quality_path = point_dir / "frame_quality_summary.csv"
    conclusion_path = point_dir / "conclusion_summary.csv"
    formal_rows = _read_csv_rows(formal_quality_path)
    frame_rows = _read_csv_rows(frame_quality_path)
    conclusion = _read_first_csv_row(conclusion_path)

    if formal_rows:
        quality = _quality_from_formal_rows(formal_rows)
    elif frame_rows:
        quality = _quality_from_frame_rows(frame_rows)
    else:
        quality = {
            "quality_source": "",
            "analyzer_rows": 0,
            "fit_eligible_rows": 0,
            "rejected_rows": 0,
            "grade_counts": {},
            "exclusion_reason": "",
        }

    status = str(conclusion.get("risk_level") or conclusion.get("status") or "").strip()
    return HistoricalPointEvidence(
        family_id=root.family_id,
        route_kind=str(identity["route_kind"] or root.route_kind),
        point_id=point_dir.name,
        point_index=identity["point_index"],
        temp_c=identity["temp_c"],
        co2_ppm=identity["co2_ppm"],
        hgen_c=identity["hgen_c"],
        rh_pct=identity["rh_pct"],
        point_path=str(point_dir),
        has_conclusion_summary=conclusion_path.exists(),
        has_frame_quality_summary=frame_quality_path.exists(),
        has_formal_quality_by_analyzer=formal_quality_path.exists(),
        quality_source=str(quality["quality_source"]),
        analyzer_rows=int(quality["analyzer_rows"]),
        fit_eligible_rows=int(quality["fit_eligible_rows"]),
        rejected_rows=int(quality["rejected_rows"]),
        grade_counts=dict(quality["grade_counts"]),
        conclusion_risk_level=status,
        status=status or "missing_conclusion",
        exclusion_reason=str(quality["exclusion_reason"]),
    )


def _discover_points(root: HistoricalEvidenceRoot) -> list[HistoricalPointEvidence]:
    base = Path(root.root_path)
    if not base.exists():
        return []
    points: list[HistoricalPointEvidence] = []
    for child in sorted(base.iterdir()):
        if not child.is_dir() or not child.name.startswith("p"):
            continue
        point = _read_point(root, child)
        if point is not None:
            points.append(point)
    return points


def _co2_expected_points(profile: Mapping[str, Any], profile_id: str) -> set[tuple[float, float]]:
    route = profile.get("co2_route", {}) if isinstance(profile, Mapping) else {}
    plan = route.get("temperature_plan", {}) if isinstance(route, Mapping) else {}
    expected: set[tuple[float, float]] = set()
    for temp, values in plan.items():
        for ppm in values or []:
            expected.add((float(temp), float(ppm)))
    supplement_policy = route.get("supplement_policy", {}) if isinstance(route, Mapping) else {}
    supplemental_points = (
        supplement_policy.get("required_new_algorithm_supplemental_gas_points", [])
        if isinstance(supplement_policy, Mapping)
        else []
    )
    for item in supplemental_points or []:
        if not isinstance(item, Mapping):
            continue
        try:
            expected.add((float(item["temperature_c"]), float(item["co2_ppm"])))
        except (KeyError, TypeError, ValueError):
            continue
    return expected


def _h2o_expected_points(profile: Mapping[str, Any], profile_id: str) -> set[tuple[float, float, float]]:
    route = profile.get("h2o_route", {}) if isinstance(profile, Mapping) else {}
    plan = route.get("temperature_plan") or route.get("wet_temperature_plan") or {}
    expected: set[tuple[float, float, float]] = set()
    for temp, values in plan.items():
        for token in values or []:
            match = re.match(r"HGEN(?P<hgen>m?\d+)C_(?P<rh>\d+)RH", str(token), re.IGNORECASE)
            if match:
                expected.add((float(temp), _parse_temp(match.group("hgen")), float(match.group("rh"))))
    supplemental_points = (
        route.get("required_new_algorithm_supplemental_wet_points", [])
        if isinstance(route, Mapping)
        else []
    )
    for item in supplemental_points or []:
        if not isinstance(item, Mapping):
            continue
        hgen_match = re.match(
            r"HGEN(?P<hgen>m?\d+)C",
            str(item.get("humidity_generator") or ""),
            re.IGNORECASE,
        )
        if not hgen_match:
            continue
        try:
            expected.add(
                (
                    float(item["temperature_c"]),
                    _parse_temp(hgen_match.group("hgen")),
                    float(item["relative_humidity_pct"]),
                )
            )
        except (KeyError, TypeError, ValueError):
            continue
    return expected


def _observed_co2(points: Sequence[HistoricalPointEvidence]) -> set[tuple[float, float]]:
    return {
        (float(point.temp_c), float(point.co2_ppm))
        for point in points
        if point.route_kind == "co2" and point.temp_c is not None and point.co2_ppm is not None
    }


def _observed_h2o(points: Sequence[HistoricalPointEvidence]) -> set[tuple[float, float, float]]:
    return {
        (float(point.temp_c), float(point.hgen_c), float(point.rh_pct))
        for point in points
        if point.route_kind == "h2o"
        and point.temp_c is not None
        and point.hgen_c is not None
        and point.rh_pct is not None
    }


def _point_key(value: tuple[float, ...]) -> str:
    return "/".join(f"{item:g}" for item in value)


def _route_summary_for_root(
    *,
    root: HistoricalEvidenceRoot,
    points: Sequence[HistoricalPointEvidence],
    profile: Mapping[str, Any],
) -> dict[str, Any]:
    route_points = [point for point in points if point.family_id == root.family_id and point.route_kind == root.route_kind]
    if root.route_kind == "co2":
        expected = _co2_expected_points(profile, root.algorithm_profile_id)
        observed = _observed_co2(route_points)
    elif root.route_kind == "h2o":
        expected = _h2o_expected_points(profile, root.algorithm_profile_id)
        observed = _observed_h2o(route_points)
    else:
        expected = set()
        observed = set()
    missing = sorted(expected - observed)
    unexpected = sorted(observed - expected)
    quality_missing = [point.point_id for point in route_points if not point.quality_source]
    total_rejected = sum(point.rejected_rows for point in route_points)
    total_fit_eligible = sum(point.fit_eligible_rows for point in route_points)

    if not Path(root.root_path).exists():
        status = "blocked"
    elif not route_points:
        status = "blocked"
    elif quality_missing or missing or unexpected:
        status = "review_required"
    else:
        status = "pass"

    return {
        "family_id": root.family_id,
        "route_kind": root.route_kind,
        "algorithm_profile_id": root.algorithm_profile_id,
        "label": root.label,
        "root_path": root.root_path,
        "root_exists": Path(root.root_path).exists(),
        "observed_point_count": len(route_points),
        "expected_point_count": len(expected),
        "matched_expected_point_count": len(expected & observed),
        "missing_expected_points": [_point_key(item) for item in missing],
        "unexpected_points": [_point_key(item) for item in unexpected],
        "quality_missing_point_ids": quality_missing,
        "fit_eligible_rows": total_fit_eligible,
        "rejected_rows": total_rejected,
        "status": status,
    }


def _fit_input_for_profile(profile: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "algorithm_mode": profile.get("algorithm_mode"),
        "fit_input": profile.get("fit_input", {}),
        "production_default": profile.get("production_default"),
    }


def _root_from_mapping(item: Mapping[str, Any]) -> HistoricalEvidenceRoot:
    return HistoricalEvidenceRoot(
        family_id=str(item.get("family_id") or ""),
        route_kind=str(item.get("route_kind") or ""),
        root_path=str(item.get("root_path") or ""),
        label=str(item.get("label") or ""),
        algorithm_profile_id=str(item.get("algorithm_profile_id") or item.get("family_id") or ""),
    )


def build_v1_5_historical_replay_evidence(
    *,
    profile_path: str | Path,
    evidence_roots: Sequence[Mapping[str, Any] | HistoricalEvidenceRoot],
) -> dict[str, Any]:
    """Bind historical point directories to a conservative replay status."""

    profile_file = Path(profile_path).resolve()
    config = _load_profile(profile_file)
    profiles = _profiles_by_id(config)
    roots = [item if isinstance(item, HistoricalEvidenceRoot) else _root_from_mapping(item) for item in evidence_roots]

    all_points: list[HistoricalPointEvidence] = []
    for root in roots:
        all_points.extend(_discover_points(root))

    route_summaries = [
        _route_summary_for_root(
            root=root,
            points=all_points,
            profile=profiles.get(root.algorithm_profile_id, {}),
        )
        for root in roots
    ]

    checks = [
        _check(
            check_id="evidence_roots_exist",
            title="Historical evidence roots exist",
            status="pass" if all(summary["root_exists"] for summary in route_summaries) else "blocker",
            reason="every requested evidence root must be present before replay binding can be trusted",
            expected="all roots exist",
            observed={summary["label"] or summary["root_path"]: summary["root_exists"] for summary in route_summaries},
            physical_meaning="Replay binding must point at real historical evidence directories, not guessed locations.",
        ),
        _check(
            check_id="point_directories_discovered",
            title="Point directories are discovered and parsed",
            status="pass" if all(summary["observed_point_count"] > 0 for summary in route_summaries) else "blocker",
            reason="each route root must contain parseable pNNN point directories",
            expected="observed_point_count > 0 for each route",
            observed={summary["label"] or summary["root_path"]: summary["observed_point_count"] for summary in route_summaries},
            physical_meaning="The replay binder operates on point-level physical evidence, not only top-level notes.",
        ),
        _check(
            check_id="point_sequence_matches_profile_or_requires_review",
            title="Observed point sequence is compared to the profile contract",
            status=(
                "pass"
                if all(summary["status"] == "pass" for summary in route_summaries)
                else "review_required"
            ),
            reason="missing or unexpected points are preserved as review gaps instead of being silently filled",
            expected="all expected profile points matched and no unexpected points",
            observed={
                summary["label"] or summary["root_path"]: {
                    "status": summary["status"],
                    "observed": summary["observed_point_count"],
                    "expected": summary["expected_point_count"],
                    "missing": summary["missing_expected_points"],
                    "unexpected": summary["unexpected_points"],
                }
                for summary in route_summaries
            },
            physical_meaning="This is where split runs and retry segments are surfaced for human review before fitting.",
        ),
        _check(
            check_id="quality_evidence_present",
            title="Point-level QC evidence is present",
            status=(
                "pass"
                if all(not summary["quality_missing_point_ids"] for summary in route_summaries)
                else "blocker"
            ),
            reason="replay binding must see QC/quality evidence for every discovered point",
            expected="no quality_missing_point_ids",
            observed={summary["label"] or summary["root_path"]: summary["quality_missing_point_ids"] for summary in route_summaries},
            physical_meaning="A point without quality evidence cannot be safely promoted into fit input review.",
        ),
        _check(
            check_id="rejected_rows_preserved",
            title="Rejected rows remain explicit replay evidence",
            status="pass",
            reason="the binder records rejected rows and exclusion reasons without changing eligibility",
            expected="rejected_rows retained as evidence",
            observed={summary["label"] or summary["root_path"]: summary["rejected_rows"] for summary in route_summaries},
            physical_meaning="Historical replay must keep C/B/rejected analyzer rows visible instead of washing them into fit-ready data.",
        ),
        _check(
            check_id="fit_input_profile_bound",
            title="Fit input profile is bound, not inferred from file names",
            status="pass",
            reason="legacy and new-algorithm fit inputs are read from the profile contract",
            expected={
                "legacy": "R_CO2/R_H2O",
                "new_algorithm": EXPECTED_ABSORPTION_FORMULA,
            },
            observed={profile_id: _fit_input_for_profile(profile) for profile_id, profile in profiles.items()},
            physical_meaning="This prevents a historical directory name from switching legacy data into absorption A or vice versa.",
        ),
        _check(
            check_id="replay_release_blocked",
            title="Replay status does not authorize release",
            status="pass",
            reason="this binder is no-write and cannot authorize archive release or PostgreSQL import",
            expected={
                "formal_release_allowed": False,
                "database_import_allowed": False,
                "not_real_acceptance_evidence": True,
            },
            observed={
                "formal_release_allowed": False,
                "database_import_allowed": False,
                "not_real_acceptance_evidence": True,
            },
            physical_meaning="A historical replay can validate interpretation logic, not today's production release state.",
        ),
    ]

    status = _status_from_checks(checks)
    manifest = {
        "schema": SCHEMA,
        "created_at": _now(),
        "profile_path": str(profile_file),
        "status": status,
        "blocker_count": sum(1 for check in checks if check.status == "blocker"),
        "review_required_count": sum(1 for check in checks if check.status == "review_required"),
        "no_write": True,
        "opens_com_ports": False,
        "connects_postgresql": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_device_id": False,
        "not_real_acceptance_evidence": True,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "replay_scope": {
            "evidence_root_count": len(roots),
            "point_count": len(all_points),
            "route_summary_count": len(route_summaries),
            "purpose": "read_only_historical_evidence_binding",
        },
    }
    return {
        "manifest": manifest,
        "evidence_roots": [root.to_json() for root in roots],
        "route_summaries": route_summaries,
        "points": [point.to_json() for point in sorted(all_points, key=lambda item: (item.family_id, item.route_kind, item.point_index or -1, item.point_id))],
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
        writer.writerows([{key: _fmt(value) if isinstance(value, (dict, list, tuple)) else value for key, value in dict(row).items()} for row in rows])


def _render_markdown(model: Mapping[str, Any]) -> str:
    manifest = model.get("manifest", {})
    lines = [
        "# V1.5 Historical Replay Evidence",
        "",
        f"- schema: `{manifest.get('schema')}`",
        f"- status: `{manifest.get('status')}`",
        f"- blocker_count: `{manifest.get('blocker_count')}`",
        f"- review_required_count: `{manifest.get('review_required_count')}`",
        f"- profile_path: `{manifest.get('profile_path')}`",
        "",
        "## Physical Boundaries",
        "",
        f"- opens_com_ports: `{manifest.get('opens_com_ports')}`",
        f"- connects_postgresql: `{manifest.get('connects_postgresql')}`",
        f"- controls_pressure: `{manifest.get('controls_pressure')}`",
        f"- controls_water_or_gas_routes: `{manifest.get('controls_water_or_gas_routes')}`",
        f"- writes_coefficients: `{manifest.get('writes_coefficients')}`",
        f"- writes_device_id: `{manifest.get('writes_device_id')}`",
        f"- formal_release_allowed: `{manifest.get('formal_release_allowed')}`",
        f"- database_import_allowed: `{manifest.get('database_import_allowed')}`",
        f"- not_real_acceptance_evidence: `{manifest.get('not_real_acceptance_evidence')}`",
        "",
        "## Route Summaries",
        "",
        "| Family | Route | Status | Observed | Expected | Matched | Missing | Unexpected | Rejected rows |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in model.get("route_summaries", []):
        lines.append(
            f"| `{row.get('family_id')}` | `{row.get('route_kind')}` | `{row.get('status')}` | "
            f"{row.get('observed_point_count')} | {row.get('expected_point_count')} | "
            f"{row.get('matched_expected_point_count')} | {len(row.get('missing_expected_points') or [])} | "
            f"{len(row.get('unexpected_points') or [])} | {row.get('rejected_rows')} |"
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


def write_v1_5_historical_replay_evidence(
    *,
    profile_path: str | Path,
    evidence_roots: Sequence[Mapping[str, Any] | HistoricalEvidenceRoot],
    output_dir: str | Path,
) -> dict[str, str]:
    """Write JSON/CSV/Markdown historical replay evidence binding artifacts."""

    model = build_v1_5_historical_replay_evidence(
        profile_path=profile_path,
        evidence_roots=evidence_roots,
    )
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    outputs = {
        "manifest": out / "v1_5_historical_replay_evidence.json",
        "route_summaries": out / "v1_5_historical_replay_route_summaries.csv",
        "points": out / "v1_5_historical_replay_points.csv",
        "checks": out / "v1_5_historical_replay_evidence_checks.csv",
        "markdown": out / "V1_5_HISTORICAL_REPLAY_EVIDENCE.md",
    }
    outputs["manifest"].write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_csv(outputs["route_summaries"], model["route_summaries"])
    _write_csv(outputs["points"], model["points"])
    _write_csv(outputs["checks"], model["checks"])
    outputs["markdown"].write_text(_render_markdown(model), encoding="utf-8")
    return {key: str(path) for key, path in outputs.items()}
