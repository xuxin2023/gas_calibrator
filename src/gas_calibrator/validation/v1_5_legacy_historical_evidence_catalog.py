"""Catalog legacy V1.5 point evidence without promoting it.

Legacy runs may contain useful point-level samples, sidecars, quality files, or
derived acceptance manifests without a closed continuous queue.  This module
hashes and labels that evidence for diagnostic review only.  It never turns a
segmented/retry/recovery collection into mature-route, fitting, release, or
database-import evidence.
"""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .v1_5_historical_route_attestation_binder import historical_provenance_blockers


SCHEMA = "v1_5_legacy_historical_evidence_catalog_v1"

_SIDECARS = {
    "formal_open_flow_sidecar_metadata.json": "co2",
    "formal_h2o_open_flow_sidecar_metadata.json": "h2o",
}
_POINT_FILES = (
    "samples_machine_readable.csv",
    "formal_open_flow_data_quality_by_analyzer.csv",
    "frame_quality_summary.csv",
    "formal_open_flow_route_timing.json",
    "formal_h2o_open_flow_route_timing.json",
    "runtime_config_snapshot.json",
)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _normal_path(path: str | Path) -> str:
    return str(Path(path).resolve()).casefold()


def _root_classification(root: Path) -> str:
    text = str(root).casefold()
    blockers = historical_provenance_blockers(root)
    if "_gas_calibrator_archive" in text:
        return "legacy_non_v1_5_archive"
    if "0624_source_forbidden" in blockers or "migration_source_forbidden" in blockers:
        return "forbidden_0624_or_migration"
    return "segmented_v1_5_point_evidence"


def _route_kind(point_dir: Path) -> str:
    for filename, kind in _SIDECARS.items():
        if (point_dir / filename).is_file():
            return kind
    name = point_dir.name.casefold()
    return "h2o" if "_hg" in name or "h2o" in name else "co2"


def _point_dirs(root: Path) -> list[Path]:
    points: set[Path] = set()
    for filename in (*_SIDECARS, "samples_machine_readable.csv"):
        for path in root.rglob(filename):
            if path.is_file():
                points.add(path.parent.resolve())
    return sorted(points, key=lambda path: str(path).casefold())


def _artifact(path: Path) -> dict[str, Any]:
    return {
        "path": str(path.resolve()),
        "size_bytes": path.stat().st_size,
        "sha256": _sha256(path),
    }


def _lineage_classification(point_dir: Path, manifest_row: Mapping[str, Any] | None) -> str:
    blockers = historical_provenance_blockers(point_dir, (manifest_row or {}).get("source_kind"))
    if "0624_source_forbidden" in blockers or "migration_source_forbidden" in blockers:
        return "forbidden_0624_or_migration"
    if blockers:
        return "segmented_retry_or_recovery"
    if manifest_row:
        return "accepted_composite_member_diagnostic_only"
    return "segmented_point_evidence"


def _manifest_rows(paths: Sequence[str | Path]) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    by_point: dict[str, dict[str, Any]] = {}
    for raw_path in sorted({Path(path).resolve() for path in paths}, key=lambda path: str(path).casefold()):
        if not raw_path.is_file():
            rows.append(
                {
                    "manifest_path": str(raw_path),
                    "manifest_missing": True,
                    "point_dir": "",
                    "source_kind": "",
                    "acceptance_status": "",
                    "diagnostic_composite_only": True,
                    "continuous_route_attestation_allowed": False,
                    "formal_fit_allowed": False,
                    "formal_release_allowed": False,
                    "database_import_allowed": False,
                }
            )
            continue
        manifest_hash = _sha256(raw_path)
        for source in _read_csv(raw_path):
            point_text = str(source.get("point_dir") or "").strip()
            point = Path(point_text).resolve() if point_text else None
            row: dict[str, Any] = {
                **source,
                "manifest_path": str(raw_path),
                "manifest_sha256": manifest_hash,
                "manifest_missing": False,
                "point_dir_exists": bool(point and point.is_dir()),
                "diagnostic_composite_only": True,
                "continuous_route_attestation_allowed": False,
                "formal_fit_allowed": False,
                "formal_release_allowed": False,
                "database_import_allowed": False,
            }
            rows.append(row)
            if point:
                by_point[_normal_path(point)] = row
    return rows, by_point


def build_v1_5_legacy_historical_evidence_catalog(
    *, search_roots: Sequence[str | Path], accepted_manifest_paths: Sequence[str | Path] = ()
) -> dict[str, Any]:
    """Build a deterministic, offline, diagnostic-only legacy catalog."""
    manifests, manifest_by_point = _manifest_rows(accepted_manifest_paths)
    roots: list[dict[str, Any]] = []
    points: list[dict[str, Any]] = []
    seen_points: set[str] = set()

    for root in sorted({Path(path).resolve() for path in search_roots}, key=lambda path: str(path).casefold()):
        classification = _root_classification(root)
        if not root.is_dir():
            roots.append(
                {
                    "root_path": str(root),
                    "root_exists": False,
                    "classification": "missing_root",
                    "point_count": 0,
                    "co2_point_count": 0,
                    "h2o_point_count": 0,
                    "formal_promotion_allowed": False,
                }
            )
            continue
        root_points = _point_dirs(root)
        root_counts = Counter(_route_kind(point) for point in root_points)
        roots.append(
            {
                "root_path": str(root),
                "root_exists": True,
                "classification": classification,
                "point_count": len(root_points),
                "co2_point_count": root_counts["co2"],
                "h2o_point_count": root_counts["h2o"],
                "formal_promotion_allowed": False,
            }
        )
        for point_dir in root_points:
            key = _normal_path(point_dir)
            if key in seen_points:
                continue
            seen_points.add(key)
            manifest_row = manifest_by_point.get(key)
            route_kind = _route_kind(point_dir)
            sidecar_path = next(
                (point_dir / name for name, kind in _SIDECARS.items() if kind == route_kind and (point_dir / name).is_file()),
                None,
            )
            artifacts: dict[str, Any] = {}
            if sidecar_path:
                artifacts[sidecar_path.name] = _artifact(sidecar_path)
            for filename in _POINT_FILES:
                path = point_dir / filename
                if path.is_file() and filename not in artifacts:
                    artifacts[filename] = _artifact(path)
            sample_path = point_dir / "samples_machine_readable.csv"
            component_qc = point_dir / "formal_open_flow_data_quality_by_analyzer.csv"
            frame_qc = point_dir / "frame_quality_summary.csv"
            provenance = historical_provenance_blockers(
                root, point_dir, (manifest_row or {}).get("source_kind"), (manifest_row or {}).get("warning")
            )
            points.append(
                {
                    "root_path": str(root),
                    "root_classification": classification,
                    "point_dir": str(point_dir),
                    "point_name": point_dir.name,
                    "route_kind": route_kind,
                    "lineage_classification": _lineage_classification(point_dir, manifest_row),
                    "provenance_blocker_codes": provenance,
                    "accepted_manifest_member": manifest_row is not None,
                    "accepted_manifest_source_kind": str((manifest_row or {}).get("source_kind") or ""),
                    "accepted_manifest_status": str((manifest_row or {}).get("acceptance_status") or ""),
                    "accepted_manifest_warning": str((manifest_row or {}).get("warning") or ""),
                    "has_sidecar": sidecar_path is not None,
                    "has_samples": sample_path.is_file(),
                    "has_component_qc": component_qc.is_file(),
                    "has_frame_qc": frame_qc.is_file(),
                    "artifact_count": len(artifacts),
                    "artifacts": artifacts,
                    "diagnostic_data_review_allowed": bool(sidecar_path and sample_path.is_file()),
                    "traceability_review_required": bool(provenance) or not component_qc.is_file(),
                    "continuous_route_attestation_allowed": False,
                    "formal_fit_allowed": False,
                    "formal_release_allowed": False,
                    "database_import_allowed": False,
                }
            )

    points.sort(key=lambda row: (str(row["route_kind"]), str(row["point_dir"]).casefold()))
    classification_counts = Counter(str(row["lineage_classification"]) for row in points)
    accepted_count = sum(1 for row in points if row["accepted_manifest_member"])
    missing_component_qc = Counter(
        str(row["route_kind"]) for row in points if not row["has_component_qc"]
    )
    return {
        "schema": SCHEMA,
        "generated_at": _now(),
        "overall_status": "catalog_complete_diagnostic_only",
        "root_count": len(roots),
        "point_count": len(points),
        "co2_point_count": sum(1 for row in points if row["route_kind"] == "co2"),
        "h2o_point_count": sum(1 for row in points if row["route_kind"] == "h2o"),
        "accepted_composite_manifest_row_count": len(manifests),
        "accepted_composite_member_count": accepted_count,
        "diagnostic_data_review_count": sum(
            1 for row in points if row["diagnostic_data_review_allowed"]
        ),
        "traceability_review_required_count": sum(
            1 for row in points if row["traceability_review_required"]
        ),
        "missing_component_qc_counts": {
            "co2": missing_component_qc["co2"],
            "h2o": missing_component_qc["h2o"],
        },
        "classification_counts": dict(sorted(classification_counts.items())),
        "roots": roots,
        "points": points,
        "accepted_composite_manifest_rows": manifests,
        "interpretation_contract": {
            "accepted_composite_manifest_is_continuous_route_evidence": False,
            "segmented_retry_or_recovery_is_mature_route_evidence": False,
            "co2_zero_and_h2o_dry_anchor_are_interchangeable": False,
            "anchor_role_inference_allowed": False,
        },
        "opens_com_ports": False,
        "controls_pressure": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "writes_sn_or_device_code": False,
        "connects_postgresql": False,
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


def write_v1_5_legacy_historical_evidence_catalog(
    model: Mapping[str, Any], output_dir: str | Path
) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    outputs = {
        "json": out / "v1_5_legacy_historical_evidence_catalog.json",
        "points_csv": out / "v1_5_legacy_historical_points.csv",
        "roots_csv": out / "v1_5_legacy_historical_roots.csv",
        "accepted_rows_csv": out / "v1_5_accepted_composite_manifest_rows.csv",
        "markdown": out / "V1_5_LEGACY_HISTORICAL_EVIDENCE_CATALOG.md",
    }
    outputs["json"].write_text(json.dumps(dict(model), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _write_csv(outputs["points_csv"], model.get("points") or [])
    _write_csv(outputs["roots_csv"], model.get("roots") or [])
    _write_csv(outputs["accepted_rows_csv"], model.get("accepted_composite_manifest_rows") or [])
    lines = [
        "# V1.5 Legacy Historical Evidence Catalog",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- point_count: `{model.get('point_count')}`",
        f"- CO2 / H2O: `{model.get('co2_point_count')} / {model.get('h2o_point_count')}`",
        f"- accepted composite members found: `{model.get('accepted_composite_member_count')}`",
        f"- diagnostic_data_review_count: `{model.get('diagnostic_data_review_count')}`",
        f"- traceability_review_required_count: `{model.get('traceability_review_required_count')}`",
        f"- missing component QC, CO2 / H2O: `{(model.get('missing_component_qc_counts') or {}).get('co2')} / {(model.get('missing_component_qc_counts') or {}).get('h2o')}`",
        "- continuous_route_attestation_allowed: `false`",
        "- historical_fit_allowed: `false`",
        "- formal_release_allowed: `false`",
        "- database_import_allowed: `false`",
        "- offline_only: `true`",
        "",
        "Accepted composite manifests, segmented runs, retries, and direct recoveries remain diagnostic lineage only.",
        "They do not prove one continuous 0613/0620/0621 mature route run and cannot authorize fitting.",
        "CO2 zero-gas evidence and an H2O dry-gas anchor remain separate physical concepts.",
        "",
        "## Roots",
        "",
        "| Classification | CO2 | H2O | Root |",
        "| --- | ---: | ---: | --- |",
    ]
    for row in model.get("roots") or []:
        lines.append(
            f"| `{row.get('classification')}` | {row.get('co2_point_count')} | {row.get('h2o_point_count')} | `{row.get('root_path')}` |"
        )
    outputs["markdown"].write_text("\n".join(lines) + "\n", encoding="utf-8")
    return outputs


__all__ = [
    "SCHEMA",
    "build_v1_5_legacy_historical_evidence_catalog",
    "write_v1_5_legacy_historical_evidence_catalog",
]
