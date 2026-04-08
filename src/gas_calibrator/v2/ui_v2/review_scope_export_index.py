from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from ..core.phase_transition_bridge_reviewer_artifact_entry import (
    PHASE_TRANSITION_BRIDGE_REVIEWER_ARTIFACT_KEY,
    build_phase_transition_bridge_reviewer_artifact_entry,
)
from ..review_surface_formatter import (
    build_review_scope_payload_reviewer_display,
    hydrate_review_scope_reviewer_display,
)
from ..core.stage_admission_review_pack_artifact_entry import (
    STAGE_ADMISSION_REVIEW_PACK_ARTIFACT_KEY,
    build_stage_admission_review_pack_artifact_entry,
)
from ..core.phase_transition_bridge_presenter import build_phase_transition_bridge_panel_payload

INDEX_FILENAME = "index.json"


def build_review_scope_batch_id(
    destination: str | Path,
    *,
    scope: Any,
    generated_at: Any = "",
) -> str:
    directory = Path(destination)
    scope_slug = _slugify(scope) or "all"
    timestamp = _timestamp_slug(generated_at)
    base = f"review_scope_{timestamp}_{scope_slug}"
    candidate = base
    sequence = 2
    while (directory / f"{candidate}.json").exists() or (directory / f"{candidate}.md").exists():
        candidate = f"{base}_{sequence:02d}"
        sequence += 1
    return candidate


def write_review_scope_export_index(
    destination: str | Path,
    *,
    run_dir: Any,
    payload: dict[str, Any],
    batch_id: str,
    exported_files: list[str],
) -> dict[str, Any]:
    directory = Path(destination)
    directory.mkdir(parents=True, exist_ok=True)
    index_path = directory / INDEX_FILENAME
    existing = _load_index(index_path)
    entries = [dict(item) for item in list(existing.get("entries", []) or []) if isinstance(item, dict)]
    entry = build_review_scope_export_entry(
        payload,
        batch_id=batch_id,
        exported_files=exported_files,
    )
    previous = dict(entries[-1]) if entries else None
    entries.append(entry)
    index_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_dir": str(run_dir or ""),
        "entry_count": len(entries),
        "entries": entries,
        "latest": dict(entry),
        "previous": previous,
    }
    index_path.write_text(json.dumps(index_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return index_payload


def build_review_scope_export_entry(
    payload: dict[str, Any],
    *,
    batch_id: str,
    exported_files: list[str],
) -> dict[str, Any]:
    selection_snapshot = dict(payload.get("selection", {}) or {})
    scope_summary = dict(payload.get("scope_summary", {}) or {})
    disclaimer = dict(payload.get("disclaimer", {}) or {})
    reviewer_display = hydrate_review_scope_reviewer_display(
        payload,
        selection=selection_snapshot,
        scope_summary=scope_summary,
    ) or build_review_scope_payload_reviewer_display(
        selection=selection_snapshot,
        scope_summary=scope_summary,
    )
    entry = {
        "batch_id": str(batch_id or ""),
        "generated_at": str(payload.get("generated_at") or ""),
        "scope": str(scope_summary.get("scope") or payload.get("selection", {}).get("scope") or "all"),
        "scope_label": str(scope_summary.get("scope_label") or ""),
        "selection_snapshot": selection_snapshot,
        "summary_counts": {
            "catalog_total_count": int(scope_summary.get("catalog_total_count", 0) or 0),
            "catalog_present_count": int(scope_summary.get("catalog_present_count", 0) or 0),
            "scope_visible_count": int(scope_summary.get("scope_visible_count", 0) or 0),
            "scope_present_count": int(scope_summary.get("scope_present_count", 0) or 0),
            "scope_external_count": int(scope_summary.get("scope_external_count", 0) or 0),
            "scope_missing_count": int(scope_summary.get("scope_missing_count", 0) or 0),
        },
        "exported_files": [str(item) for item in list(exported_files or [])],
        "disclaimer_flags": {
            "offline_review_only": bool(disclaimer.get("offline_review_only", False)),
            "simulated_or_replay_context": bool(disclaimer.get("simulated_or_replay_context", False)),
            "diagnostic_context": bool(disclaimer.get("diagnostic_context", False)),
            "not_real_acceptance_evidence": bool(disclaimer.get("not_real_acceptance_evidence", False)),
        },
        "reviewer_display": reviewer_display,
    }
    spectral_quality = dict(payload.get("spectral_quality", {}) or {})
    if spectral_quality:
        entry["spectral_quality"] = spectral_quality
    phase_transition_bridge_section = _build_phase_transition_bridge_section(payload)
    if phase_transition_bridge_section:
        entry["phase_transition_bridge_section"] = phase_transition_bridge_section
    reviewer_artifact_entry = _build_phase_transition_bridge_reviewer_artifact_entry(payload)
    if reviewer_artifact_entry:
        entry["phase_transition_bridge_reviewer_artifact_entry"] = reviewer_artifact_entry
    stage_admission_review_pack_entry = _build_stage_admission_review_pack_artifact_entry(payload)
    if stage_admission_review_pack_entry:
        entry["stage_admission_review_pack_artifact_entry"] = stage_admission_review_pack_entry
    return entry


def _build_phase_transition_bridge_section(payload: dict[str, Any]) -> dict[str, Any]:
    analytics_summary = dict(payload.get("analytics_summary", {}) or {})
    analytics_detail = dict(analytics_summary.get("detail", {}) or {})
    bridge = (
        dict(payload.get("phase_transition_bridge", {}) or {})
        or dict(analytics_detail.get("phase_transition_bridge", {}) or {})
    )
    bundle = build_phase_transition_bridge_panel_payload(bridge)
    return bundle if bool(bundle.get("available", False)) else {}


def _build_phase_transition_bridge_reviewer_artifact_entry(payload: dict[str, Any]) -> dict[str, Any]:
    direct_entry = dict(payload.get("phase_transition_bridge_reviewer_artifact_entry") or {})
    if direct_entry:
        return direct_entry

    manifest_sections = dict(payload.get("manifest_sections", {}) or {})
    manifest_section = dict(manifest_sections.get(PHASE_TRANSITION_BRIDGE_REVIEWER_ARTIFACT_KEY) or {})
    reviewer_section = dict(manifest_sections.get("phase_transition_bridge_reviewer_section") or {})
    if manifest_section:
        entry = build_phase_transition_bridge_reviewer_artifact_entry(
            artifact_path=manifest_section.get("path"),
            manifest_section=manifest_section,
            reviewer_section=reviewer_section,
        )
        if entry:
            return entry

    for row in list(payload.get("rows", []) or []):
        row_entry = dict(dict(row or {}).get("phase_transition_bridge_reviewer_artifact_entry") or {})
        if row_entry:
            return row_entry

    return {}


def _build_stage_admission_review_pack_artifact_entry(payload: dict[str, Any]) -> dict[str, Any]:
    direct_entry = dict(payload.get("stage_admission_review_pack_artifact_entry") or {})
    if direct_entry:
        return direct_entry

    manifest_sections = dict(payload.get("manifest_sections", {}) or {})
    manifest_section = dict(manifest_sections.get(STAGE_ADMISSION_REVIEW_PACK_ARTIFACT_KEY) or {})
    reviewer_manifest_section = dict(manifest_sections.get("stage_admission_review_pack_reviewer_artifact") or {})
    if manifest_section or reviewer_manifest_section:
        entry = build_stage_admission_review_pack_artifact_entry(
            artifact_path=manifest_section.get("path"),
            reviewer_artifact_path=reviewer_manifest_section.get("path"),
            manifest_section=manifest_section,
            reviewer_manifest_section=reviewer_manifest_section,
        )
        if entry:
            return entry

    for row in list(payload.get("rows", []) or []):
        row_entry = dict(dict(row or {}).get("stage_admission_review_pack_artifact_entry") or {})
        if row_entry:
            return row_entry

    return {}


def _load_index(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _timestamp_slug(value: Any) -> str:
    text = str(value or "").strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S"):
        try:
            parsed = datetime.strptime(text, fmt)
        except ValueError:
            continue
        return parsed.strftime("%Y%m%d_%H%M%S")
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _slugify(value: Any) -> str:
    text = str(value or "").strip().lower()
    chars: list[str] = []
    for char in text:
        if char.isalnum():
            chars.append(char)
            continue
        if char in {"-", "_"}:
            chars.append("_")
            continue
    slug = "".join(chars).strip("_")
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug
