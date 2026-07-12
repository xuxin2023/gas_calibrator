"""Discover historical V1.5 route roots without promoting them.

Discovery is deliberately weaker than attestation: it indexes exact queue
summaries and ranks candidates, but it never authorizes fitting.  A candidate
must still pass ``v1_5_historical_route_attestation_binder`` before use.
"""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_historical_route_attestation_binder import historical_provenance_blockers


SCHEMA = "v1_5_historical_mature_root_discovery_v1"


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return payload


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _number(value: Any) -> float | None:
    try:
        return float(str(value).strip()) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _profile_counts(path: str | Path) -> dict[str, dict[str, int]]:
    payload = _read_json(Path(path).resolve())
    result: dict[str, dict[str, int]] = {}
    for profile in payload.get("profiles", []):
        profile_id = str(profile.get("profile_id") or "")
        co2 = profile.get("co2_route") or {}
        h2o = profile.get("h2o_route") or {}
        co2_count = co2.get("production_candidate_point_count_with_supplements", co2.get("formal_point_count"))
        h2o_count = h2o.get(
            "production_candidate_wet_point_count_with_supplements",
            h2o.get("formal_wet_point_count", h2o.get("formal_point_count")),
        )
        result[profile_id] = {"co2": int(co2_count), "h2o": int(h2o_count)}
    return result


def _route_kind(summary: Mapping[str, Any]) -> str:
    schema = str(summary.get("schema_version") or "")
    if schema == "v1_5_co2_open_flow_queue_v0":
        return "co2"
    if schema == "v1_5_h2o_open_flow_queue_v0":
        return "h2o"
    return ""


def _matching_profiles(
    profile_counts: Mapping[str, Mapping[str, int]], route_kind: str, selected_points: int | None
) -> list[str]:
    if not route_kind or selected_points is None:
        return []
    return sorted(
        profile_id
        for profile_id, counts in profile_counts.items()
        if counts.get(route_kind) == selected_points
    )


def _artifact_counts(root: Path, route_kind: str, point_ids: Sequence[str]) -> dict[str, int]:
    sidecar_name = (
        "formal_open_flow_sidecar_metadata.json"
        if route_kind == "co2"
        else "formal_h2o_open_flow_sidecar_metadata.json"
    )
    counts = {"point_directories": 0, "sidecars": 0, "samples": 0, "component_qc": 0}
    for point_id in point_ids:
        point = root / point_id
        if point.is_dir():
            counts["point_directories"] += 1
        if (point / sidecar_name).is_file():
            counts["sidecars"] += 1
        if (point / "samples_machine_readable.csv").is_file():
            counts["samples"] += 1
        if (point / "formal_open_flow_data_quality_by_analyzer.csv").is_file():
            counts["component_qc"] += 1
    return counts


def build_v1_5_historical_mature_root_discovery(
    *, queue_summary_paths: Sequence[str | Path], algorithm_profile_path: str | Path
) -> dict[str, Any]:
    profile_path = Path(algorithm_profile_path).resolve()
    profile_counts = _profile_counts(profile_path)
    unique_summaries = sorted({Path(path).resolve() for path in queue_summary_paths})
    candidates: list[dict[str, Any]] = []
    output_root_counts: dict[str, int] = {}

    for summary_path in unique_summaries:
        if not summary_path.is_file():
            candidates.append(
                {
                    "summary_path": str(summary_path),
                    "output_root": "",
                    "route_kind": "",
                    "classification": "missing_summary",
                    "eligible_for_attestation_input": False,
                    "blocker_codes": ["queue_summary_missing"],
                    "blocker_count": 1,
                }
            )
            continue
        summary = _read_json(summary_path)
        route_kind = _route_kind(summary)
        output_text = str(summary.get("output_dir") or "").strip()
        output_root = Path(output_text).resolve() if output_text else summary_path.parent.resolve()
        output_key = str(output_root).lower()
        output_root_counts[output_key] = output_root_counts.get(output_key, 0) + 1
        selected_number = _number(summary.get("selected_points"))
        selected_points = int(selected_number) if selected_number is not None and selected_number.is_integer() else None
        matching_profiles = _matching_profiles(profile_counts, route_kind, selected_points)
        queue_source_text = str(summary.get("queue_csv") or "").strip()
        runtime_config_text = str(summary.get("config_path") or "").strip()
        queue_source_path = Path(queue_source_text).resolve() if queue_source_text else Path("__missing_queue_source__").resolve()
        runtime_config_path = Path(runtime_config_text).resolve() if runtime_config_text else Path("__missing_runtime_config__").resolve()
        manifest_path = summary_path.parent / "queue_manifest.csv"
        manifest_rows = _read_csv(manifest_path) if manifest_path.is_file() else []
        point_ids = [str(row.get("point_run_id") or "") for row in manifest_rows if row.get("point_run_id")]
        artifacts = _artifact_counts(output_root, route_kind, point_ids) if route_kind else {
            "point_directories": 0,
            "sidecars": 0,
            "samples": 0,
            "component_qc": 0,
        }
        reasons: list[str] = []
        provenance_codes = historical_provenance_blockers(
            summary_path,
            output_root,
            summary.get("queue_run_id"),
            summary.get("config_path"),
            summary.get("queue_csv"),
        )
        reasons.extend(provenance_codes)
        if not route_kind:
            reasons.append("queue_summary_schema_unknown")
        if summary.get("dry_run") is not False:
            reasons.append("dry_run_not_historical_route_evidence")
        if summary.get("no_write") is not True or summary.get("writes_senco") is not False or summary.get("writes_device_id") is not False:
            reasons.append("queue_no_write_boundary_invalid")
        if not queue_source_text or not queue_source_path.is_file():
            reasons.append("queue_source_missing")
        if not runtime_config_text or not runtime_config_path.is_file():
            reasons.append("runtime_config_missing")
        if not matching_profiles:
            reasons.append("no_profile_matches_selected_point_count")
        elif len(matching_profiles) > 1:
            reasons.append("multiple_profiles_match_selected_point_count")
        if not manifest_path.is_file():
            reasons.append("queue_manifest_missing")
        if selected_points is None or len(manifest_rows) != selected_points:
            reasons.append("manifest_row_count_mismatch")
        if len(point_ids) != len(set(point_ids)):
            reasons.append("manifest_point_id_duplicate")
        if any(row.get("status") != "ok" or str(row.get("returncode") or "") != "0" for row in manifest_rows):
            reasons.append("manifest_contains_non_ok_point")
        ok_points = _number(summary.get("ok_points"))
        failed_points = _number(summary.get("failed_points"))
        if (
            selected_points is None
            or ok_points != float(selected_points)
            or failed_points != 0.0
            or summary.get("hard_failure") is not False
        ):
            reasons.append("queue_summary_not_finalized_clean")
        readiness = summary.get("formal_route_readiness") or {}
        if readiness.get("status") != "pass" or readiness.get("ok") is not True:
            reasons.append("formal_route_readiness_not_pass")
        if not output_text or not output_root.is_dir():
            reasons.append("output_root_missing")
        expected_artifacts = len(point_ids)
        if expected_artifacts and artifacts["point_directories"] != expected_artifacts:
            reasons.append("point_directories_incomplete")
        if expected_artifacts and artifacts["sidecars"] != expected_artifacts:
            reasons.append("point_sidecars_incomplete")
        if expected_artifacts and artifacts["samples"] != expected_artifacts:
            reasons.append("point_samples_incomplete")
        if expected_artifacts and artifacts["component_qc"] != expected_artifacts:
            reasons.append("point_component_qc_incomplete")
        reasons = sorted(set(reasons))
        if provenance_codes:
            classification = "forbidden_source"
        elif "dry_run_not_historical_route_evidence" in reasons:
            classification = "dry_run_only"
        elif not reasons:
            classification = "attestation_input_candidate"
        else:
            classification = "review_required"
        candidates.append(
            {
                "summary_path": str(summary_path),
                "summary_sha256": _sha256(summary_path),
                "manifest_path": str(manifest_path.resolve()) if manifest_path.is_file() else "",
                "manifest_sha256": _sha256(manifest_path) if manifest_path.is_file() else "",
                "queue_source_path": str(queue_source_path) if queue_source_path.is_file() else queue_source_text,
                "queue_source_sha256": _sha256(queue_source_path) if queue_source_path.is_file() else "",
                "runtime_config_path": str(runtime_config_path) if runtime_config_path.is_file() else runtime_config_text,
                "runtime_config_sha256": _sha256(runtime_config_path) if runtime_config_path.is_file() else "",
                "output_root": str(output_root),
                "queue_run_id": str(summary.get("queue_run_id") or ""),
                "route_kind": route_kind,
                "selected_points": selected_points,
                "ok_points": ok_points,
                "failed_points": failed_points,
                "dry_run": summary.get("dry_run"),
                "matching_profile_ids": matching_profiles,
                **artifacts,
                "classification": classification,
                "eligible_for_attestation_input": not reasons,
                "blocker_count": len(reasons),
                "blocker_codes": reasons,
                "historical_fit_allowed": False,
            }
        )

    for candidate in candidates:
        output_key = str(candidate.get("output_root") or "").lower()
        if output_key and output_root_counts.get(output_key, 0) > 1:
            reasons = set(candidate.get("blocker_codes") or [])
            reasons.add("duplicate_queue_summary_for_output_root")
            candidate["blocker_codes"] = sorted(reasons)
            candidate["blocker_count"] = len(reasons)
            candidate["eligible_for_attestation_input"] = False
            if candidate.get("classification") == "attestation_input_candidate":
                candidate["classification"] = "review_required"

    candidates.sort(
        key=lambda row: (
            0 if row.get("eligible_for_attestation_input") else 1,
            {
                "attestation_input_candidate": 0,
                "review_required": 1,
                "dry_run_only": 2,
                "forbidden_source": 3,
                "missing_summary": 4,
            }.get(str(row.get("classification")), 5),
            0 if row.get("matching_profile_ids") else 1,
            row.get("blocker_count", 999),
            -int(row.get("samples") or 0),
            str(row.get("summary_path") or ""),
        )
    )
    eligible = [row for row in candidates if row.get("eligible_for_attestation_input")]
    attestation_roots = [
        {
            "family_id": f"discovered_{row['route_kind']}_{index:03d}",
            "route_kind": row["route_kind"],
            "root_path": row["output_root"],
            "algorithm_profile_id": row["matching_profile_ids"][0],
            "source_queue_summary": row["summary_path"],
        }
        for index, row in enumerate(eligible, start=1)
    ]
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": "ready_for_attestation_review" if eligible else "blocked_no_complete_mature_root",
        "summary_count": len(candidates),
        "attestation_input_candidate_count": len(eligible),
        "classification_counts": {
            classification: sum(1 for row in candidates if row.get("classification") == classification)
            for classification in sorted({str(row.get("classification")) for row in candidates})
        },
        "candidates": candidates,
        "attestation_candidate_replay": {"evidence_roots": attestation_roots, "points": []},
        "source_paths": {
            "algorithm_profile_path": str(profile_path),
            "algorithm_profile_sha256": _sha256(profile_path),
        },
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_sn_or_device_code": False,
        "connects_postgresql": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "historical_fit_allowed": False,
        "not_real_acceptance_evidence": True,
    }


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields: list[str] = []
    flattened: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        for key in ("matching_profile_ids", "blocker_codes"):
            item[key] = json.dumps(item.get(key) or [], ensure_ascii=False)
        flattened.append(item)
        for key in item:
            if key not in fields:
                fields.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields or ["empty"])
        writer.writeheader()
        writer.writerows(flattened)


def write_v1_5_historical_mature_root_discovery(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    outputs = {
        "json": out / "v1_5_historical_mature_root_discovery.json",
        "candidates_csv": out / "v1_5_historical_mature_root_candidates.csv",
        "attestation_candidates_json": out / "v1_5_historical_attestation_candidate_replay.json",
        "markdown": out / "V1_5_HISTORICAL_MATURE_ROOT_DISCOVERY.md",
    }
    outputs["json"].write_text(json.dumps(dict(model), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _write_csv(outputs["candidates_csv"], model.get("candidates") or [])
    outputs["attestation_candidates_json"].write_text(
        json.dumps(model.get("attestation_candidate_replay") or {}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    lines = [
        "# V1.5 Historical Mature-Root Discovery",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- summary_count: `{model.get('summary_count')}`",
        f"- attestation_input_candidate_count: `{model.get('attestation_input_candidate_count')}`",
        "- historical_fit_allowed: `false`",
        "- offline_only: `true`",
        "",
        "Discovery never promotes a route. Every candidate must still pass the historical route attestation binder.",
        "",
        "## Closest Candidates",
        "",
        "| Route | Points | Classification | Blockers | Root |",
        "| --- | ---: | --- | ---: | --- |",
    ]
    for row in list(model.get("candidates") or [])[:20]:
        lines.append(
            f"| `{row.get('route_kind')}` | {row.get('selected_points')} | `{row.get('classification')}` | {row.get('blocker_count')} | `{row.get('output_root')}` |"
        )
    outputs["markdown"].write_text("\n".join(lines) + "\n", encoding="utf-8")
    return outputs


__all__ = [
    "SCHEMA",
    "build_v1_5_historical_mature_root_discovery",
    "write_v1_5_historical_mature_root_discovery",
]
