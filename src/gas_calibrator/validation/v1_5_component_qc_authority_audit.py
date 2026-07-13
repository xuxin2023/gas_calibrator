"""Audit authority for a future V1.5 component-QC generator.

The audit separates mature pre-sample stability gates from the untracked
0624/migration post-sample QC writer. It never derives or writes QC artifacts.
"""

from __future__ import annotations

import csv
import hashlib
import json
import subprocess
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_legacy_historical_evidence_catalog import SCHEMA as CATALOG_SCHEMA
from .v1_5_p2_qc_derivation_design import SCHEMA as P2_SCHEMA


SCHEMA = "v1_5_component_qc_authority_audit_v1"
WRITER_RELATIVE_PATH = Path("src/gas_calibrator/validation/v1_5_open_flow_quality.py")
CO2_SAMPLER_RELATIVE_PATH = Path(
    "src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py"
)
H2O_SAMPLER_RELATIVE_PATH = Path(
    "src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_sampling.py"
)
QUALITY_FILENAME = "formal_open_flow_data_quality_by_analyzer.csv"
REQUIRED_QC_FIELDS = {
    "label",
    "prefix",
    "grade",
    "ratio_key",
    "ratio_span",
    "ratio_tol",
    "ratio_a_tol",
    "frame_count",
    "usable_ratio_count",
    "reason",
    "sample_can_enter_calibration_fit",
    "sample_can_enter_diagnostic_model",
}


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return payload


def _git(repo_root: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(repo_root), *args],
        check=check,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def _git_path_tracked(repo_root: Path, relative_path: Path) -> bool:
    result = _git(
        repo_root,
        "ls-files",
        "--error-unmatch",
        relative_path.as_posix(),
        check=False,
    )
    return result.returncode == 0


def _git_history(repo_root: Path, relative_path: Path) -> list[str]:
    result = _git(
        repo_root,
        "log",
        "--all",
        "--format=%H",
        "--",
        relative_path.as_posix(),
        check=False,
    )
    if result.returncode != 0:
        return []
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def _safety_locks() -> dict[str, bool]:
    return {
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_sn_or_device_code": False,
        "connects_postgresql": False,
        "component_qc_generation_allowed": False,
        "component_qc_backfill_allowed": False,
        "historical_fit_allowed": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
    }


def _upstream_blockers(p2: Mapping[str, Any], catalog: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    if p2.get("schema") != P2_SCHEMA:
        reasons.append("p2_design_schema_mismatch")
    if p2.get("overall_status") != "blocked_missing_reviewed_qc_generator_contract":
        reasons.append("p2_design_status_invalid")
    if catalog.get("schema") != CATALOG_SCHEMA:
        reasons.append("catalog_schema_mismatch")
    if catalog.get("overall_status") != "catalog_complete_diagnostic_only":
        reasons.append("catalog_status_invalid")
    false_locks = (
        "opens_com_ports",
        "controls_pressure",
        "controls_water_or_gas_routes",
        "writes_coefficients",
        "writes_sn_or_device_code",
        "connects_postgresql",
        "historical_fit_allowed",
        "formal_release_allowed",
        "database_import_allowed",
    )
    for source_name, payload in (("p2", p2), ("catalog", catalog)):
        for key in false_locks:
            if payload.get(key) is not False:
                reasons.append(f"{source_name}_{key}_not_false")
        if payload.get("not_real_acceptance_evidence") is not True:
            reasons.append(f"{source_name}_real_acceptance_lock_missing")
    if p2.get("qc_derivation_execution_allowed") is not False:
        reasons.append("p2_qc_derivation_execution_not_locked")
    if p2.get("generated_qc_write_allowed") is not False:
        reasons.append("p2_generated_qc_write_not_locked")
    return sorted(set(reasons))


def _sampler_authority(repo_root: Path, relative_path: Path, component: str) -> dict[str, Any]:
    path = repo_root / relative_path
    text = path.read_text(encoding="utf-8") if path.is_file() else ""
    prefix = "h2o" if component == "h2o" else "co2"
    markers = {
        "preseal_hard_tolerance_marker": f"{prefix}_ratio_f_preseal_tol" in text,
        "preseal_a_grade_tolerance_marker": f"{prefix}_ratio_f_preseal_a_grade_tol" in text,
        "preseal_min_samples_marker": f"{prefix}_ratio_f_preseal_min_samples" in text,
        "component_qc_writer_imported": "apply_open_flow_quality_grades" in text,
        "component_qc_filename_present": QUALITY_FILENAME in text,
    }
    return {
        "authority_role": "mature_pre_sample_stability_contract",
        "component": component,
        "path": str(path.resolve()),
        "tracked_at_head": _git_path_tracked(repo_root, relative_path),
        "sha256": _sha256(path) if path.is_file() else "",
        **markers,
        "is_component_qc_writer_authority": False,
    }


def _runtime_threshold_inventory(p2: Mapping[str, Any]) -> list[dict[str, Any]]:
    counts: Counter[tuple[Any, ...]] = Counter()
    missing_count: Counter[str] = Counter()
    for candidate in p2.get("candidates") or []:
        route = str(candidate.get("route_kind") or "")
        if route not in {"co2", "h2o"}:
            continue
        runtime_path = Path(str(candidate.get("point_dir") or "")) / "runtime_config_snapshot.json"
        if not runtime_path.is_file():
            missing_count[route] += 1
            continue
        cfg = _read_json(runtime_path)
        sensor = (
            cfg.get("workflow", {}).get("stability", {}).get("sensor", {})
            if isinstance(cfg.get("workflow"), Mapping)
            else {}
        )
        counts[
            (
                route,
                sensor.get(f"{route}_ratio_f_preseal_tol"),
                sensor.get(f"{route}_ratio_f_preseal_a_grade_tol"),
                sensor.get(f"{route}_ratio_f_preseal_policy"),
                sensor.get(f"{route}_ratio_f_preseal_min_samples"),
            )
        ] += 1
    rows = [
        {
            "component": key[0],
            "preseal_hard_tol": key[1],
            "preseal_a_grade_tol": key[2],
            "preseal_policy": key[3],
            "preseal_min_samples": key[4],
            "point_count": count,
            "authority_role": "recorded_runtime_pre_sample_gate_not_post_sample_component_qc",
        }
        for key, count in sorted(counts.items(), key=lambda item: str(item[0]))
    ]
    for route, count in sorted(missing_count.items()):
        rows.append(
            {
                "component": route,
                "preseal_hard_tol": "",
                "preseal_a_grade_tol": "",
                "preseal_policy": "",
                "preseal_min_samples": "",
                "point_count": count,
                "authority_role": "runtime_config_missing",
            }
        )
    return rows


def _catalog_qc_inventory(catalog: Mapping[str, Any]) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    mismatch_count = 0
    for point in catalog.get("points") or []:
        if point.get("has_component_qc") is not True:
            continue
        artifact = (point.get("artifacts") or {}).get(QUALITY_FILENAME) or {}
        path = Path(str(artifact.get("path") or ""))
        fields: list[str] = []
        grades: Counter[str] = Counter()
        ratio_tols: set[float] = set()
        ratio_a_tols: set[float] = set()
        sha_matches = False
        if path.is_file():
            sha_matches = _sha256(path) == str(artifact.get("sha256") or "")
            mismatch_count += int(not sha_matches)
            with path.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle)
                fields = list(reader.fieldnames or [])
                for item in reader:
                    grades[str(item.get("grade") or "")] += 1
                    for key, target in (("ratio_tol", ratio_tols), ("ratio_a_tol", ratio_a_tols)):
                        try:
                            target.add(float(str(item.get(key) or "")))
                        except ValueError:
                            pass
        rows.append(
            {
                "point_name": point.get("point_name"),
                "route_kind": point.get("route_kind"),
                "root_classification": point.get("root_classification"),
                "lineage_classification": point.get("lineage_classification"),
                "artifact_path": str(path),
                "artifact_sha256": artifact.get("sha256"),
                "artifact_sha256_matches": sha_matches,
                "schema_has_required_fields": REQUIRED_QC_FIELDS.issubset(fields),
                "grades": dict(sorted(grades.items())),
                "ratio_tols": sorted(ratio_tols),
                "ratio_a_tols": sorted(ratio_a_tols),
                "is_mature_threshold_authority": False,
            }
        )
    return rows, mismatch_count


def _polluted_writer_review(polluted_root: Path) -> dict[str, Any]:
    writer = polluted_root / WRITER_RELATIVE_PATH
    text = writer.read_text(encoding="utf-8") if writer.is_file() else ""
    co2_sampler = polluted_root / CO2_SAMPLER_RELATIVE_PATH
    h2o_sampler = polluted_root / H2O_SAMPLER_RELATIVE_PATH
    co2_text = co2_sampler.read_text(encoding="utf-8") if co2_sampler.is_file() else ""
    h2o_text = h2o_sampler.read_text(encoding="utf-8") if h2o_sampler.is_file() else ""
    markers = {
        "has_grade_tokens": all(
            token in text
            for token in ("A_calibration_eligible", "B_diagnostic_model_only", "C_reject")
        ),
        "writes_expected_csv": QUALITY_FILENAME in text,
        "has_ratio_tol_fields": "ratio_tol" in text and "ratio_a_tol" in text,
        "co2_sampler_imports_writer": "apply_open_flow_quality_grades" in co2_text,
        "h2o_sampler_imports_writer": "apply_open_flow_quality_grades" in h2o_text,
    }
    return {
        "authority_role": "polluted_root_migration_writer_schema_reference_only",
        "path": str(writer.resolve()),
        "exists": writer.is_file(),
        "tracked_in_polluted_root": (
            _git_path_tracked(polluted_root, WRITER_RELATIVE_PATH) if writer.is_file() else False
        ),
        "sha256": _sha256(writer) if writer.is_file() else "",
        **markers,
        "is_mature_threshold_authority": False,
    }


def build_v1_5_component_qc_authority_audit(
    *,
    repo_root: str | Path,
    polluted_root: str | Path,
    p2_design_json_path: str | Path,
    legacy_catalog_json_path: str | Path,
) -> dict[str, Any]:
    repo = Path(repo_root).resolve()
    polluted = Path(polluted_root).resolve()
    p2_path = Path(p2_design_json_path).resolve()
    catalog_path = Path(legacy_catalog_json_path).resolve()
    p2 = _read_json(p2_path)
    catalog = _read_json(catalog_path)
    blockers = _upstream_blockers(p2, catalog)
    if blockers:
        return {
            "schema": SCHEMA,
            "generated_at": _now(),
            "overall_status": "blocked_invalid_upstream_evidence",
            "upstream_blocker_codes": blockers,
            "authority_gap_codes": [],
            **_safety_locks(),
        }
    head = _git(repo, "rev-parse", "HEAD").stdout.strip()
    samplers = [
        _sampler_authority(repo, CO2_SAMPLER_RELATIVE_PATH, "co2"),
        _sampler_authority(repo, H2O_SAMPLER_RELATIVE_PATH, "h2o"),
    ]
    tracked_writer_present = _git_path_tracked(repo, WRITER_RELATIVE_PATH)
    writer_history = _git_history(repo, WRITER_RELATIVE_PATH)
    polluted_writer = _polluted_writer_review(polluted)
    runtime_thresholds = _runtime_threshold_inventory(p2)
    historical_qc, artifact_hash_mismatch_count = _catalog_qc_inventory(catalog)
    root_counts = Counter(str(row["root_classification"]) for row in historical_qc)
    route_counts = Counter(str(row["route_kind"]) for row in historical_qc)
    authority_gaps: list[str] = []
    if tracked_writer_present:
        authority_gaps.append("tracked_component_qc_writer_requires_separate_review")
    else:
        authority_gaps.append("tracked_mature_component_qc_writer_missing")
    if writer_history:
        authority_gaps.append("component_qc_writer_history_requires_separate_review")
    else:
        authority_gaps.append("component_qc_writer_absent_from_git_history")
    if not historical_qc:
        authority_gaps.append("historical_component_qc_artifacts_missing")
    if historical_qc and any(row["root_classification"] != "forbidden_0624_or_migration" for row in historical_qc):
        authority_gaps.append("non_0624_component_qc_artifact_requires_review")
    else:
        authority_gaps.append("all_observed_component_qc_artifacts_are_0624_or_migration")
    if route_counts.get("h2o", 0) == 0:
        authority_gaps.append("h2o_historical_component_qc_examples_missing")
    if artifact_hash_mismatch_count:
        authority_gaps.append("historical_component_qc_artifact_hash_mismatch")
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": "blocked_no_reviewed_mature_component_qc_authority",
        "upstream_blocker_codes": [],
        "authority_gap_codes": sorted(set(authority_gaps)),
        "repo_root": str(repo),
        "repo_head": head,
        "p2_design_path": str(p2_path),
        "p2_design_sha256": _sha256(p2_path),
        "legacy_catalog_path": str(catalog_path),
        "legacy_catalog_sha256": _sha256(catalog_path),
        "p2_candidate_count": p2.get("candidate_count"),
        "p2_manual_gate_review_count": p2.get("manual_gate_review_count"),
        "mature_sampler_reviews": samplers,
        "tracked_component_qc_writer_present": tracked_writer_present,
        "tracked_component_qc_writer_history_commits": writer_history,
        "polluted_writer_review": polluted_writer,
        "runtime_preseal_threshold_inventory": runtime_thresholds,
        "historical_component_qc_artifact_count": len(historical_qc),
        "historical_component_qc_artifact_hash_mismatch_count": artifact_hash_mismatch_count,
        "historical_component_qc_root_classification_counts": dict(sorted(root_counts.items())),
        "historical_component_qc_route_counts": dict(sorted(route_counts.items())),
        "historical_component_qc_inventory": historical_qc,
        "interpretation_contract": {
            "preseal_stability_thresholds_are_component_qc_threshold_authority": False,
            "0624_or_migration_qc_is_schema_reference_only": True,
            "polluted_root_writer_may_be_promoted_without_review": False,
            "co2_zero_gas_and_h2o_dry_gas_roles_remain_distinct": True,
            "next_allowed_step": "reviewed_component_qc_generator_contract_design_only",
        },
        **_safety_locks(),
    }


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields: list[str] = []
    flattened: list[dict[str, Any]] = []
    for source in rows:
        row = dict(source)
        for key, value in list(row.items()):
            if isinstance(value, (dict, list)):
                row[key] = json.dumps(value, ensure_ascii=False, sort_keys=True)
        flattened.append(row)
        for key in row:
            if key not in fields:
                fields.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields or ["empty"])
        writer.writeheader()
        writer.writerows(flattened)


def write_v1_5_component_qc_authority_audit(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    outputs = {
        "json": out / "v1_5_component_qc_authority_audit.json",
        "sources_csv": out / "v1_5_component_qc_authority_sources.csv",
        "thresholds_csv": out / "v1_5_component_qc_runtime_preseal_thresholds.csv",
        "artifacts_csv": out / "v1_5_component_qc_historical_artifacts.csv",
        "summary_csv": out / "v1_5_component_qc_authority_summary.csv",
        "markdown": out / "V1_5_COMPONENT_QC_AUTHORITY_AUDIT.md",
    }
    payload = dict(model)
    payload.pop("historical_component_qc_inventory", None)
    payload["historical_component_qc_inventory_csv"] = outputs["artifacts_csv"].name
    outputs["json"].write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    source_rows = list(model.get("mature_sampler_reviews") or [])
    source_rows.append(dict(model.get("polluted_writer_review") or {}))
    _write_csv(outputs["sources_csv"], source_rows)
    _write_csv(outputs["thresholds_csv"], model.get("runtime_preseal_threshold_inventory") or [])
    _write_csv(outputs["artifacts_csv"], model.get("historical_component_qc_inventory") or [])
    _write_csv(
        outputs["summary_csv"],
        [
            {"metric": "overall_status", "value": model.get("overall_status")},
            {"metric": "p2_candidate_count", "value": model.get("p2_candidate_count")},
            {
                "metric": "p2_manual_gate_review_count",
                "value": model.get("p2_manual_gate_review_count"),
            },
            {
                "metric": "historical_component_qc_artifact_count",
                "value": model.get("historical_component_qc_artifact_count"),
            },
            {
                "metric": "tracked_component_qc_writer_present",
                "value": model.get("tracked_component_qc_writer_present"),
            },
            {
                "metric": "component_qc_generation_allowed",
                "value": model.get("component_qc_generation_allowed"),
            },
        ],
    )
    lines = [
        "# V1.5 Component-QC Authority Audit",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- p2_candidate_count: `{model.get('p2_candidate_count')}`",
        f"- p2_manual_gate_review_count: `{model.get('p2_manual_gate_review_count')}`",
        f"- tracked_component_qc_writer_present: `{str(model.get('tracked_component_qc_writer_present')).lower()}`",
        f"- historical_component_qc_artifact_count: `{model.get('historical_component_qc_artifact_count')}`",
        f"- historical route counts: `{json.dumps(model.get('historical_component_qc_route_counts') or {}, sort_keys=True)}`",
        "- component_qc_generation_allowed: `false`",
        "- component_qc_backfill_allowed: `false`",
        "- offline_only: `true`",
        "",
        "Mature samplers contain pre-sample stability gates, not a reviewed post-sample component-QC writer.",
        "The only observed writer is an untracked polluted-root migration implementation; all cataloged QC artifacts are forbidden 0624/migration evidence.",
    ]
    outputs["markdown"].write_text("\n".join(lines) + "\n", encoding="utf-8")
    return outputs


__all__ = [
    "SCHEMA",
    "build_v1_5_component_qc_authority_audit",
    "write_v1_5_component_qc_authority_audit",
]
