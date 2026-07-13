"""Plan offline legacy V1.5 evidence-gap work without repairing evidence.

The plan consumes the immutable legacy evidence catalog, revalidates every
cataloged artifact, and emits one manual review task per historical point. It
never derives QC, mutates source artifacts, authorizes fitting, or promotes a
segmented/retry/recovery collection to a mature continuous route.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_legacy_historical_evidence_catalog import SCHEMA as CATALOG_SCHEMA


SCHEMA = "v1_5_legacy_evidence_gap_task_plan_v1"
_CO2_POINT_RE = re.compile(r"^p\d+_T(?P<temp>m?\d+)_(?P<ppm>\d+)ppm", re.IGNORECASE)
_H2O_POINT_RE = re.compile(
    r"^p\d+_T(?P<temp>m?\d+)_HG(?P<hgen>m?\d+)C_(?P<rh>\d+)RH",
    re.IGNORECASE,
)


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


def _catalog_blockers(catalog: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    if catalog.get("schema") != CATALOG_SCHEMA:
        reasons.append("catalog_schema_mismatch")
    if catalog.get("overall_status") != "catalog_complete_diagnostic_only":
        reasons.append("catalog_status_not_diagnostic_only")
    false_locks = (
        "opens_com_ports",
        "controls_pressure",
        "controls_water_or_gas_routes",
        "writes_coefficients",
        "writes_sn_or_device_code",
        "connects_postgresql",
        "continuous_route_attestation_allowed",
        "historical_fit_allowed",
        "formal_release_allowed",
        "database_import_allowed",
    )
    for key in false_locks:
        if catalog.get(key) is not False:
            reasons.append(f"catalog_{key}_not_false")
    if catalog.get("not_real_acceptance_evidence") is not True:
        reasons.append("catalog_real_acceptance_lock_missing")
    contract = catalog.get("interpretation_contract") or {}
    if contract.get("co2_zero_and_h2o_dry_anchor_are_interchangeable") is not False:
        reasons.append("co2_zero_h2o_dry_anchor_separation_missing")
    if contract.get("anchor_role_inference_allowed") is not False:
        reasons.append("anchor_role_inference_lock_missing")
    if not isinstance(catalog.get("points"), list):
        reasons.append("catalog_points_missing")
    return sorted(set(reasons))


def _artifact_integrity(point: Mapping[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    gaps: list[str] = []
    artifacts = point.get("artifacts") or {}
    if not isinstance(artifacts, Mapping):
        return rows, ["artifact_inventory_invalid"]
    for role, raw in sorted(artifacts.items()):
        evidence = raw if isinstance(raw, Mapping) else {}
        path = Path(str(evidence.get("path") or ""))
        expected_hash = str(evidence.get("sha256") or "")
        expected_size = evidence.get("size_bytes")
        exists = path.is_file()
        actual_size = path.stat().st_size if exists else None
        actual_hash = _sha256(path) if exists else ""
        status = "match"
        if not exists:
            status = "missing"
            gaps.append("artifact_missing_since_catalog")
        elif actual_size != expected_size:
            status = "size_mismatch"
            gaps.append("artifact_size_mismatch_since_catalog")
        elif actual_hash != expected_hash:
            status = "hash_mismatch"
            gaps.append("artifact_hash_mismatch_since_catalog")
        rows.append(
            {
                "point_dir": str(point.get("point_dir") or ""),
                "route_kind": str(point.get("route_kind") or ""),
                "artifact_role": str(role),
                "artifact_path": str(path),
                "expected_size_bytes": expected_size,
                "actual_size_bytes": actual_size,
                "expected_sha256": expected_hash,
                "actual_sha256": actual_hash,
                "integrity_status": status,
            }
        )
    return rows, gaps


def _physical_key(point: Mapping[str, Any]) -> tuple[str, ...] | None:
    name = str(point.get("point_name") or "")
    root = str(point.get("root_path") or "").casefold()
    h2o = _H2O_POINT_RE.match(name)
    if h2o:
        return (root, "h2o", h2o.group("temp").lower(), h2o.group("hgen").lower(), h2o.group("rh"))
    co2 = _CO2_POINT_RE.match(name)
    if co2:
        return (root, "co2", co2.group("temp").lower(), co2.group("ppm"))
    return None


def _gap_codes(
    point: Mapping[str, Any],
    integrity_gaps: Sequence[str],
    *,
    accepted_alternative_exists: bool,
) -> list[str]:
    gaps = set(integrity_gaps)
    if point.get("has_sidecar") is not True:
        gaps.add("point_sidecar_missing")
    if point.get("has_samples") is not True:
        gaps.add("point_samples_missing")
    if point.get("has_component_qc") is not True:
        gaps.add("component_qc_missing")
    if point.get("has_frame_qc") is not True:
        gaps.add("frame_qc_missing")
    if str(point.get("accepted_manifest_warning") or "").strip():
        gaps.add("accepted_manifest_warning_requires_review")
    if accepted_alternative_exists:
        gaps.add("same_physical_accepted_composite_alternative_exists")
    lineage = str(point.get("lineage_classification") or "")
    if lineage == "forbidden_0624_or_migration":
        gaps.add("forbidden_0624_or_migration_source")
    elif lineage == "segmented_retry_or_recovery":
        gaps.add("retry_or_recovery_lineage_not_continuous")
    elif lineage == "accepted_composite_member_diagnostic_only":
        gaps.add("accepted_composite_lineage_not_continuous")
    else:
        gaps.add("segmented_lineage_not_continuous")
    return sorted(gaps)


def _recommended_actions(point: Mapping[str, Any], gaps: Sequence[str]) -> list[str]:
    codes = set(gaps)
    actions: list[str] = []
    if any(code.startswith("artifact_") for code in codes):
        actions.append("restore_exact_cataloged_artifact_or_regenerate_catalog_from_reviewed_source")
    if "forbidden_0624_or_migration_source" in codes:
        actions.append("retain_for_diagnostic_reference_only_never_promote")
        return actions
    if "same_physical_accepted_composite_alternative_exists" in codes:
        actions.extend(
            [
                "retain_original_attempt_and_reference_accepted_alternative_without_direct_binding",
                "preserve_segment_retry_recovery_lineage_never_claim_continuous_route",
            ]
        )
        return actions
    if "point_sidecar_missing" in codes:
        actions.append("locate_same_point_sidecar_or_mark_point_traceability_only")
    if "point_samples_missing" in codes:
        actions.append("locate_same_point_samples_or_mark_point_unusable")
    if "component_qc_missing" in codes:
        if point.get("has_frame_qc") is True and point.get("has_samples") is True:
            actions.append("review_same_point_samples_and_frame_qc_for_component_qc_backfill")
        else:
            actions.append("locate_same_run_component_qc_or_plan_separate_offline_qc_derivation_review")
    if "frame_qc_missing" in codes:
        actions.append("locate_same_point_frame_qc_or_mark_quality_lineage_incomplete")
    if "accepted_manifest_warning_requires_review" in codes:
        actions.append("review_warning_without_clearing_original_acceptance_status")
    actions.append("preserve_segment_retry_recovery_lineage_never_claim_continuous_route")
    return actions


def _priority(gaps: Sequence[str]) -> str:
    codes = set(gaps)
    if any(code.startswith("artifact_") for code in codes):
        return "P0_integrity"
    if "forbidden_0624_or_migration_source" in codes:
        return "P3_forbidden_reference"
    if "same_physical_accepted_composite_alternative_exists" in codes:
        return "P3_superseded_reference"
    if {"point_sidecar_missing", "point_samples_missing"} & codes:
        return "P1_core_evidence"
    if {
        "component_qc_missing",
        "frame_qc_missing",
        "accepted_manifest_warning_requires_review",
    } & codes:
        return "P2_quality_traceability"
    return "P3_lineage_only"


def _task_status(point: Mapping[str, Any], priority: str) -> str:
    if priority == "P0_integrity":
        return "artifact_integrity_blocker_manual_review_required"
    if str(point.get("lineage_classification") or "") == "forbidden_0624_or_migration":
        return "forbidden_source_retain_diagnostic_only"
    if priority == "P3_superseded_reference":
        return "superseded_attempt_retain_reference_only"
    if priority == "P3_lineage_only":
        return "lineage_review_only_no_repair_promotion"
    return "manual_offline_evidence_review_required"


def build_v1_5_legacy_evidence_gap_task_plan(
    *, catalog_json_path: str | Path
) -> dict[str, Any]:
    catalog_path = Path(catalog_json_path).resolve()
    catalog = _read_json(catalog_path)
    blockers = _catalog_blockers(catalog)
    if blockers:
        return {
            "schema": SCHEMA,
            "generated_at": _now(),
            "overall_status": "blocked_invalid_catalog",
            "catalog_blocker_codes": blockers,
            "task_count": 0,
            "tasks": [],
            "artifact_integrity_rows": [],
            **_safety_locks(),
        }

    tasks: list[dict[str, Any]] = []
    integrity_rows: list[dict[str, Any]] = []
    accepted_physical_keys = {
        key
        for point in catalog.get("points") or []
        if point.get("accepted_manifest_member") is True
        if (key := _physical_key(point)) is not None
    }
    for index, source in enumerate(catalog.get("points") or [], start=1):
        point = dict(source)
        point_integrity, integrity_gaps = _artifact_integrity(point)
        integrity_rows.extend(point_integrity)
        point_key = _physical_key(point)
        accepted_alternative_exists = (
            point.get("accepted_manifest_member") is not True
            and point_key is not None
            and point_key in accepted_physical_keys
        )
        gaps = _gap_codes(
            point,
            integrity_gaps,
            accepted_alternative_exists=accepted_alternative_exists,
        )
        priority = _priority(gaps)
        actions = _recommended_actions(point, gaps)
        forbidden = "forbidden_0624_or_migration_source" in gaps
        tasks.append(
            {
                "task_id": f"legacy_evidence_gap_{index:04d}",
                "priority": priority,
                "task_status": _task_status(point, priority),
                "route_kind": str(point.get("route_kind") or ""),
                "point_name": str(point.get("point_name") or ""),
                "point_dir": str(point.get("point_dir") or ""),
                "root_classification": str(point.get("root_classification") or ""),
                "lineage_classification": str(point.get("lineage_classification") or ""),
                "accepted_manifest_member": point.get("accepted_manifest_member") is True,
                "accepted_manifest_status": str(point.get("accepted_manifest_status") or ""),
                "accepted_manifest_warning": str(point.get("accepted_manifest_warning") or ""),
                "same_physical_accepted_composite_alternative_exists": accepted_alternative_exists,
                "gap_codes": gaps,
                "recommended_actions": actions,
                "same_point_evidence_only": True,
                "cross_run_qc_direct_bind_allowed": False,
                "automatic_qc_derivation_allowed": False,
                "offline_evidence_recovery_review_allowed": not forbidden and not accepted_alternative_exists,
                "continuous_route_attestation_allowed": False,
                "formal_fit_allowed": False,
                "formal_release_allowed": False,
                "database_import_allowed": False,
            }
        )

    priority_counts = Counter(str(row["priority"]) for row in tasks)
    status_counts = Counter(str(row["task_status"]) for row in tasks)
    gap_counts = Counter(code for row in tasks for code in row["gap_codes"])
    integrity_mismatches = sum(
        1 for row in integrity_rows if row["integrity_status"] != "match"
    )
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": (
            "blocked_cataloged_artifact_integrity_mismatch"
            if integrity_mismatches
            else "review_required_manual_offline_evidence_tasks"
        ),
        "catalog_blocker_codes": [],
        "catalog_path": str(catalog_path),
        "catalog_sha256": _sha256(catalog_path),
        "catalog_point_count": int(catalog.get("point_count") or 0),
        "task_count": len(tasks),
        "artifact_integrity_row_count": len(integrity_rows),
        "artifact_integrity_mismatch_count": integrity_mismatches,
        "recoverable_manual_review_task_count": sum(
            1 for row in tasks if row["offline_evidence_recovery_review_allowed"]
        ),
        "forbidden_reference_task_count": sum(
            1 for row in tasks if not row["offline_evidence_recovery_review_allowed"]
        ),
        "priority_counts": dict(sorted(priority_counts.items())),
        "task_status_counts": dict(sorted(status_counts.items())),
        "gap_counts": dict(sorted(gap_counts.items())),
        "tasks": tasks,
        "artifact_integrity_rows": integrity_rows,
        "interpretation_contract": {
            "task_plan_mutates_source_evidence": False,
            "task_plan_derives_component_qc": False,
            "task_plan_authorizes_formal_fit": False,
            "co2_zero_and_h2o_dry_anchor_are_interchangeable": False,
            "anchor_role_inference_allowed": False,
        },
        **_safety_locks(),
    }


def _safety_locks() -> dict[str, bool]:
    return {
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_sn_or_device_code": False,
        "connects_postgresql": False,
        "automatic_repair_allowed": False,
        "continuous_route_attestation_allowed": False,
        "historical_fit_allowed": False,
        "formal_release_allowed": False,
        "database_import_allowed": False,
        "not_real_acceptance_evidence": True,
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


def write_v1_5_legacy_evidence_gap_task_plan(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    outputs = {
        "json": out / "v1_5_legacy_evidence_gap_task_plan.json",
        "tasks_csv": out / "v1_5_legacy_evidence_gap_tasks.csv",
        "integrity_csv": out / "v1_5_legacy_evidence_artifact_integrity.csv",
        "summary_csv": out / "v1_5_legacy_evidence_gap_summary.csv",
        "markdown": out / "V1_5_LEGACY_EVIDENCE_GAP_TASK_PLAN.md",
    }
    json_payload = dict(model)
    json_payload.pop("artifact_integrity_rows", None)
    json_payload["artifact_integrity_rows_omitted_from_json"] = True
    json_payload["artifact_integrity_csv"] = outputs["integrity_csv"].name
    outputs["json"].write_text(
        json.dumps(json_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    _write_csv(outputs["tasks_csv"], model.get("tasks") or [])
    _write_csv(outputs["integrity_csv"], model.get("artifact_integrity_rows") or [])
    summary_rows = [
        {"metric": "overall_status", "value": model.get("overall_status")},
        {"metric": "task_count", "value": model.get("task_count")},
        {
            "metric": "artifact_integrity_mismatch_count",
            "value": model.get("artifact_integrity_mismatch_count"),
        },
    ]
    summary_rows.extend(
        {"metric": f"priority:{key}", "value": value}
        for key, value in (model.get("priority_counts") or {}).items()
    )
    summary_rows.extend(
        {"metric": f"gap:{key}", "value": value}
        for key, value in (model.get("gap_counts") or {}).items()
    )
    _write_csv(outputs["summary_csv"], summary_rows)
    lines = [
        "# V1.5 Legacy Evidence Gap Task Plan",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- task_count: `{model.get('task_count')}`",
        f"- artifact_integrity_mismatch_count: `{model.get('artifact_integrity_mismatch_count')}`",
        f"- recoverable_manual_review_task_count: `{model.get('recoverable_manual_review_task_count')}`",
        f"- forbidden_reference_task_count: `{model.get('forbidden_reference_task_count')}`",
        f"- priority_counts: `{json.dumps(model.get('priority_counts') or {}, sort_keys=True)}`",
        "- automatic_repair_allowed: `false`",
        "- historical_fit_allowed: `false`",
        "- formal_release_allowed: `false`",
        "- database_import_allowed: `false`",
        "- offline_only: `true`",
        "",
        "Tasks describe manual offline evidence work. They do not modify source files, derive QC, or authorize fitting.",
        "Cross-run quality remains reference-only, and CO2 zero gas remains distinct from an H2O dry-gas anchor.",
        "",
        "## Highest-Priority Tasks",
        "",
        "| Priority | Route | Point | Gaps |",
        "| --- | --- | --- | --- |",
    ]
    rank = {
        "P0_integrity": 0,
        "P1_core_evidence": 1,
        "P2_quality_traceability": 2,
        "P3_lineage_only": 3,
        "P3_superseded_reference": 4,
        "P3_forbidden_reference": 5,
    }
    tasks = sorted(
        model.get("tasks") or [],
        key=lambda row: (rank.get(str(row.get("priority")), 9), str(row.get("point_dir"))),
    )
    for row in tasks[:40]:
        lines.append(
            f"| `{row.get('priority')}` | `{row.get('route_kind')}` | `{row.get('point_name')}` | `{','.join(row.get('gap_codes') or [])}` |"
        )
    outputs["markdown"].write_text("\n".join(lines) + "\n", encoding="utf-8")
    return outputs


__all__ = [
    "SCHEMA",
    "build_v1_5_legacy_evidence_gap_task_plan",
    "write_v1_5_legacy_evidence_gap_task_plan",
]
