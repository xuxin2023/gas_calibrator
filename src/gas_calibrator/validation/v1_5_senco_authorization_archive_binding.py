"""Bind V1.5 controlled SENCO writes to their reviewed artifact authorization."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping, Sequence

from .v1_5_artifact_hash_binding import sha256_file
from .v1_5_senco_artifact_authorization import validate_senco_artifact_authorization


SCHEMA = "v1_5_senco_authorization_archive_binding_v1"


@dataclass(frozen=True)
class _WriterEvidenceSpec:
    writer_scope: str
    metadata_name: str
    rows_name: str


_WRITER_SPECS = (
    _WriterEvidenceSpec(
        "co2_senco13_pair",
        "co2_senco13_pair_write_meta.json",
        "co2_senco13_pair_write_summary.csv",
    ),
    _WriterEvidenceSpec(
        "h2o_senco24_pair",
        "h2o_senco24_pair_write_meta.json",
        "h2o_senco24_pair_write_summary.csv",
    ),
    _WriterEvidenceSpec(
        "co2_senco5_linear",
        "senco5_linear_write_meta.json",
        "senco5_linear_write_events.csv",
    ),
    _WriterEvidenceSpec(
        "h2o_senco6_linear",
        "senco6_linear_write_meta.json",
        "senco6_linear_write_events.csv",
    ),
)


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True


def _latest_named_artifact(
    root: Path,
    name: str,
    *,
    exclude_dirs: Sequence[Path],
) -> Path | None:
    excluded = tuple(path.resolve() for path in exclude_dirs)
    matches = [
        path.resolve()
        for path in root.rglob(name)
        if path.is_file() and not any(_is_relative_to(path, item) for item in excluded)
    ]
    return max(matches, key=lambda item: item.stat().st_mtime) if matches else None


def _named_artifacts(root: Path, name: str, *, exclude_dirs: Sequence[Path]) -> list[Path]:
    excluded = tuple(path.resolve() for path in exclude_dirs)
    return sorted(
        (
            path.resolve()
            for path in root.rglob(name)
            if path.is_file() and not any(_is_relative_to(path, item) for item in excluded)
        ),
        key=str,
    )


def _load_json(path: Path | None) -> Mapping[str, Any]:
    if path is None or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}
    return payload if isinstance(payload, Mapping) else {}


def _load_rows(path: Path | None) -> list[Dict[str, str]]:
    if path is None or not path.is_file():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    except Exception:
        return []


def _normalized_device_ids(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    values = set()
    for row in rows:
        value = str(row.get("analyzer_device_id") or row.get("device_id") or "").strip()
        if value:
            values.add(value.zfill(3) if value.isdigit() else value)
    return sorted(values)


def _metadata_config(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    nested = payload.get("config_summary")
    return nested if isinstance(nested, Mapping) else payload


def _success_status(value: Any) -> bool:
    return str(value or "").strip().startswith("written_readback_verified")


def build_v1_5_senco_authorization_archive_binding(
    *,
    run_dir: str | Path,
    authorization_json: str | Path | None = None,
    exclude_dirs: Sequence[str | Path] = (),
) -> Dict[str, Any]:
    """Return an offline archive gate over actual controlled-write evidence."""

    root = Path(run_dir).resolve()
    excluded = tuple(Path(path).resolve() for path in exclude_dirs)
    authorization_path = (
        Path(authorization_json).resolve()
        if authorization_json
        else _latest_named_artifact(
            root,
            "main_senco_artifact_authorization.json",
            exclude_dirs=excluded,
        )
    )
    authorization = _load_json(authorization_path)
    authorization_id = str(authorization.get("authorization_id") or "").strip()
    manifest_path_text = str(authorization.get("manifest_path") or "").strip()
    manifest_path = Path(manifest_path_text).resolve() if manifest_path_text else None

    blockers: list[str] = []
    writer_evidence: list[Dict[str, Any]] = []
    all_device_ids: set[str] = set()
    for spec in _WRITER_SPECS:
        metadata_paths = _named_artifacts(root, spec.metadata_name, exclude_dirs=excluded)
        rows_paths = _named_artifacts(root, spec.rows_name, exclude_dirs=excluded)
        evidence_dirs = sorted({path.parent for path in (*metadata_paths, *rows_paths)}, key=str)
        if not evidence_dirs:
            continue
        for evidence_dir in evidence_dirs:
            metadata_path = evidence_dir / spec.metadata_name
            rows_path = evidence_dir / spec.rows_name
            metadata_exists = metadata_path.is_file()
            rows_exists = rows_path.is_file()
            reasons: list[str] = []
            if not metadata_exists:
                reasons.append(f"{spec.writer_scope}:write_metadata_missing")
            if not rows_exists:
                reasons.append(f"{spec.writer_scope}:write_rows_missing")
            metadata = _load_json(metadata_path if metadata_exists else None)
            config = _metadata_config(metadata)
            rows = _load_rows(rows_path if rows_exists else None)
            device_ids = _normalized_device_ids(rows)
            all_device_ids.update(device_ids)
            if metadata_exists and not metadata:
                reasons.append(f"{spec.writer_scope}:write_metadata_invalid")
            if rows_exists and not rows:
                reasons.append(f"{spec.writer_scope}:write_rows_empty_or_invalid")
            failed_rows = [row for row in rows if not _success_status(row.get("status"))]
            if failed_rows:
                reasons.append(f"{spec.writer_scope}:write_readback_not_verified")
            if not device_ids:
                reasons.append(f"{spec.writer_scope}:write_device_ids_missing")

            metadata_authorization_id = str(config.get("artifact_authorization_id") or "").strip()
            if str(config.get("artifact_authorization_status") or "").strip() != "pass":
                reasons.append(f"{spec.writer_scope}:artifact_authorization_status_not_pass")
            if str(config.get("artifact_hash_status") or "").strip() != "pass":
                reasons.append(f"{spec.writer_scope}:artifact_hash_status_not_pass")
            if not authorization_path or not authorization:
                reasons.append(f"{spec.writer_scope}:senco_artifact_authorization_missing_or_invalid")
            elif metadata_authorization_id != authorization_id:
                reasons.append(f"{spec.writer_scope}:artifact_authorization_id_mismatch")
            elif manifest_path is None:
                reasons.append(f"{spec.writer_scope}:artifact_authorization_manifest_path_missing")
            else:
                valid, validation_reasons, _ = validate_senco_artifact_authorization(
                    authorization_path,
                    manifest_path=manifest_path,
                    reviewer=str(config.get("reviewer") or ""),
                    approver=str(config.get("approver") or ""),
                    writer_scope=spec.writer_scope,
                    device_ids=device_ids,
                )
                if not valid:
                    reasons.extend(f"{spec.writer_scope}:{reason}" for reason in validation_reasons)

            reasons = list(dict.fromkeys(reasons))
            blockers.extend(reasons)
            writer_evidence.append(
                {
                    "writer_scope": spec.writer_scope,
                    "evidence_dir": str(evidence_dir),
                    "metadata_path": str(metadata_path) if metadata_exists else "",
                    "metadata_sha256": sha256_file(metadata_path) if metadata_exists else "",
                    "write_rows_path": str(rows_path) if rows_exists else "",
                    "write_rows_sha256": sha256_file(rows_path) if rows_exists else "",
                    "authorization_id": metadata_authorization_id,
                    "device_ids": device_ids,
                    "row_count": len(rows),
                    "verified_row_count": sum(1 for row in rows if _success_status(row.get("status"))),
                    "status": "pass" if not reasons else "blocked",
                    "reasons": reasons,
                }
            )

    write_evidence_present = bool(writer_evidence)
    actual_scopes = sorted({str(row.get("writer_scope") or "") for row in writer_evidence})
    authorized_scopes = sorted(
        {str(item).strip() for item in authorization.get("authorized_writer_scopes") or [] if str(item).strip()}
    )
    authorized_device_ids = sorted(
        {
            text.zfill(3) if text.isdigit() else text
            for item in authorization.get("authorized_device_ids") or []
            if (text := str(item or "").strip())
        }
    )
    if write_evidence_present and authorization:
        if actual_scopes != authorized_scopes:
            blockers.append(
                "artifact_authorization_writer_scope_set_mismatch:"
                f"actual={','.join(actual_scopes)} authorized={','.join(authorized_scopes)}"
            )
        if sorted(all_device_ids) != authorized_device_ids:
            blockers.append(
                "artifact_authorization_device_set_mismatch:"
                f"actual={','.join(sorted(all_device_ids))} authorized={','.join(authorized_device_ids)}"
            )
    blockers = list(dict.fromkeys(blockers))
    if not write_evidence_present:
        overall_status = "not_applicable_no_main_senco_write_evidence"
    elif blockers:
        overall_status = "blocked"
    else:
        overall_status = "ready_for_archive_release"
    return {
        "schema": SCHEMA,
        "overall_status": overall_status,
        "ready_for_archive_release": not blockers,
        "write_evidence_present": write_evidence_present,
        "authorization_path": str(authorization_path) if authorization_path else "",
        "authorization_sha256": (
            sha256_file(authorization_path)
            if authorization_path is not None and authorization_path.is_file()
            else ""
        ),
        "authorization_id": authorization_id,
        "authorized_writer_scopes": authorized_scopes,
        "authorized_device_ids": authorized_device_ids,
        "manifest_path": str(manifest_path) if manifest_path else "",
        "manifest_sha256": (
            sha256_file(manifest_path) if manifest_path is not None and manifest_path.is_file() else ""
        ),
        "writer_scope_count": len({row["writer_scope"] for row in writer_evidence}),
        "write_evidence_set_count": len(writer_evidence),
        "device_count": len(all_device_ids),
        "device_ids": sorted(all_device_ids),
        "writer_evidence": writer_evidence,
        "blocker_count": len(blockers),
        "blockers": blockers,
        "next_action": (
            "carry_forward"
            if not blockers
            else "resolve authorization, writer-scope, device-set, or write-readback evidence mismatch before archive release"
        ),
        "physical_boundaries": {
            "offline_archive_check_only": True,
            "opens_com_ports": False,
            "writes_coefficients": False,
            "connects_postgresql": False,
            "controls_water_or_gas_routes": False,
            "not_real_acceptance_evidence": True,
        },
    }


def render_v1_5_senco_authorization_archive_binding_markdown(model: Mapping[str, Any]) -> str:
    lines = [
        "# V1.5 SENCO write authorization archive binding",
        "",
        f"- overall_status: `{model.get('overall_status')}`",
        f"- ready_for_archive_release: `{model.get('ready_for_archive_release')}`",
        f"- write_evidence_present: `{model.get('write_evidence_present')}`",
        f"- authorization_id: `{model.get('authorization_id')}`",
        f"- writer_scope_count: `{model.get('writer_scope_count')}`",
        f"- device_ids: `{','.join(str(item) for item in model.get('device_ids') or [])}`",
        "",
        "| writer_scope | status | device_ids | verified_rows | metadata | write_rows |",
        "| --- | --- | --- | ---: | --- | --- |",
    ]
    for row in model.get("writer_evidence") or []:
        device_ids = ",".join(str(item) for item in row.get("device_ids") or [])
        lines.append(
            f"| {row.get('writer_scope')} | {row.get('status')} | {device_ids} | "
            f"{row.get('verified_row_count')}/{row.get('row_count')} | "
            f"{row.get('metadata_path')} | {row.get('write_rows_path')} |"
        )
    if model.get("blockers"):
        lines.extend(["", "## Blockers", ""])
        lines.extend(f"- `{reason}`" for reason in model.get("blockers") or [])
    return "\n".join(lines).rstrip() + "\n"


def write_v1_5_senco_authorization_archive_binding_outputs(
    model: Mapping[str, Any],
    output_dir: str | Path,
) -> Dict[str, Path]:
    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    json_path = root / "v1_5_senco_authorization_archive_binding.json"
    csv_path = root / "v1_5_senco_authorization_archive_binding.csv"
    markdown_path = root / "V1_5_SENCO_AUTHORIZATION_ARCHIVE_BINDING.md"
    json_path.write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    rows = list(model.get("writer_evidence") or [])
    fieldnames = [
        "writer_scope",
        "status",
        "authorization_id",
        "device_ids",
        "row_count",
        "verified_row_count",
        "evidence_dir",
        "metadata_path",
        "metadata_sha256",
        "write_rows_path",
        "write_rows_sha256",
        "reasons",
    ]
    with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    **{key: row.get(key, "") for key in fieldnames},
                    "device_ids": ",".join(str(item) for item in row.get("device_ids") or []),
                    "reasons": ";".join(str(item) for item in row.get("reasons") or []),
                }
            )
    markdown_path.write_text(
        render_v1_5_senco_authorization_archive_binding_markdown(model),
        encoding="utf-8-sig",
    )
    return {"json": json_path, "csv": csv_path, "markdown": markdown_path}
