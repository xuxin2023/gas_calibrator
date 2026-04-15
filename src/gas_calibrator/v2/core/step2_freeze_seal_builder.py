"""Step 2 freeze seal builder — final no-drift guardrail that audits the
five core closeout objects for cross-surface consistency.

Audited objects:
  1. step2_closeout_readiness
  2. step2_closeout_package
  3. step2_freeze_audit
  4. step3_admission_dossier
  5. step2_closeout_verification

Audit dimensions:
  - Field existence (consumable fields present on each surface)
  - Boundary marker consistency (all 7 markers match canonical set)
  - Source priority / persisted-vs-fallback behaviour
  - Core status field naming consistency
  - Cross-surface consumability (results / reports / historical / review index / UI)

This is a guardrail layer, NOT a replacement for closeout_package /
freeze_audit / admission_dossier / closeout_verification.

freeze_seal_status only expresses Step 2 seal / no-drift state.
It does NOT express formal release approval.

Step 2 boundary:
  - evidence_source = "simulated"
  - not_real_acceptance_evidence = True
  - not_ready_for_formal_claim = True
  - reviewer_only = True
  - readiness_mapping_only = True
  - primary_evidence_rewritten = False
  - real_acceptance_ready = False
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .step2_closure_schema_registry import get_closure_schema_entry
from .step2_freeze_seal_contracts import (
    AUDITED_OBJECT_KEYS,
    BOUNDARY_MARKER_FIELDS,
    FREEZE_SEAL_STATUS_OK,
    FREEZE_SEAL_STATUS_ATTENTION,
    FREEZE_SEAL_STATUS_BLOCKER,
    FREEZE_SEAL_STATUS_REVIEWER_ONLY,
    FREEZE_SEAL_STEP2_BOUNDARY,
    FREEZE_SEAL_TITLE_ZH,
    FREEZE_SEAL_TITLE_EN,
    FREEZE_SEAL_SUMMARY_ZH,
    FREEZE_SEAL_SUMMARY_EN,
    FREEZE_SEAL_SIMULATION_ONLY_BOUNDARY_ZH,
    FREEZE_SEAL_SIMULATION_ONLY_BOUNDARY_EN,
    resolve_freeze_seal_title,
    resolve_freeze_seal_summary,
    resolve_freeze_seal_status_label,
    resolve_freeze_seal_simulation_only_boundary,
    resolve_freeze_seal_reviewer_only_notice,
    resolve_freeze_seal_non_claim_notice,
    resolve_drift_label,
    resolve_missing_surface_label,
    resolve_source_mismatch_label,
)

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------

FREEZE_SEAL_BUILDER_VERSION: str = "2.26.0"


# ---------------------------------------------------------------------------
# build_step2_freeze_seal — main entry point
# ---------------------------------------------------------------------------

def build_step2_freeze_seal(
    *,
    run_id: str = "",
    # The five core objects
    step2_closeout_readiness: dict[str, Any] | None = None,
    step2_closeout_package: dict[str, Any] | None = None,
    step2_freeze_audit: dict[str, Any] | None = None,
    step3_admission_dossier: dict[str, Any] | None = None,
    step2_closeout_verification: dict[str, Any] | None = None,
    # Surface availability flags
    surface_results: bool = True,
    surface_reports: bool = True,
    surface_historical: bool = True,
    surface_review_index: bool = True,
    surface_ui: bool = True,
    # Config
    lang: str = "zh",
) -> dict[str, Any]:
    """Build the Step 2 freeze seal — final no-drift guardrail.

    Audits the five core closeout objects for cross-surface consistency.
    Does NOT replace any of them.  Does NOT express formal release approval.
    """
    objects = {
        "step2_closeout_readiness": dict(step2_closeout_readiness or {}),
        "step2_closeout_package": dict(step2_closeout_package or {}),
        "step2_freeze_audit": dict(step2_freeze_audit or {}),
        "step3_admission_dossier": dict(step3_admission_dossier or {}),
        "step2_closeout_verification": dict(step2_closeout_verification or {}),
    }

    surfaces = {
        "results": surface_results,
        "reports": surface_reports,
        "historical": surface_historical,
        "review_index": surface_review_index,
        "ui": surface_ui,
    }

    # --- Audit: boundary markers ---
    drift_sections = _audit_boundary_markers(objects, lang=lang)

    # --- Audit: field existence ---
    field_drifts = _audit_field_existence(objects, lang=lang)
    drift_sections.extend(field_drifts)

    # --- Audit: status field naming ---
    status_drifts = _audit_status_field_naming(objects, lang=lang)
    drift_sections.extend(status_drifts)

    # --- Audit: source priority ---
    source_mismatches = _audit_source_priority(objects, lang=lang)

    # --- Audit: missing surfaces ---
    missing_surfaces = _audit_missing_surfaces(objects, surfaces, lang=lang)

    # --- Derive freeze_seal_status ---
    freeze_seal_status = _derive_freeze_seal_status(
        drift_sections=drift_sections,
        source_mismatches=source_mismatches,
        missing_surfaces=missing_surfaces,
    )

    # --- Build reviewer summary line ---
    reviewer_summary_line = _build_reviewer_summary_line(
        freeze_seal_status=freeze_seal_status,
        drift_count=len(drift_sections),
        lang=lang,
    )

    # --- Build reviewer summary lines ---
    reviewer_summary_lines = _build_reviewer_summary_lines(
        freeze_seal_status=freeze_seal_status,
        drift_sections=drift_sections,
        source_mismatches=source_mismatches,
        missing_surfaces=missing_surfaces,
        lang=lang,
    )

    # --- Build audited_objects summary ---
    audited_objects = _build_audited_objects_summary(objects)

    # --- Simulation-only boundary ---
    simulation_only_boundary = (
        FREEZE_SEAL_SIMULATION_ONLY_BOUNDARY_EN if lang == "en"
        else FREEZE_SEAL_SIMULATION_ONLY_BOUNDARY_ZH
    )

    return {
        "schema_version": "1.0",
        "artifact_type": "step2_freeze_seal",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": str(run_id or ""),
        "phase": "step2_freeze_seal",
        "seal_version": FREEZE_SEAL_BUILDER_VERSION,
        "freeze_seal_status": freeze_seal_status,
        "freeze_seal_status_label": resolve_freeze_seal_status_label(freeze_seal_status, lang=lang),
        "reviewer_summary_line": reviewer_summary_line,
        "reviewer_summary_lines": reviewer_summary_lines,
        "drift_sections": drift_sections,
        "missing_surfaces": missing_surfaces,
        "source_mismatches": source_mismatches,
        "audited_objects": audited_objects,
        "simulation_only_boundary": simulation_only_boundary,
        "freeze_seal_source": "rebuilt",
        # Step 2 boundary markers — always enforced
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "not_ready_for_formal_claim": True,
        "reviewer_only": True,
        "readiness_mapping_only": True,
        "primary_evidence_rewritten": False,
        "real_acceptance_ready": False,
    }


# ---------------------------------------------------------------------------
# Audit helpers
# ---------------------------------------------------------------------------

def _audit_boundary_markers(
    objects: dict[str, dict[str, Any]],
    *,
    lang: str,
) -> list[dict[str, Any]]:
    """Check boundary marker consistency across all five objects."""
    drifts: list[dict[str, Any]] = []

    for obj_key in AUDITED_OBJECT_KEYS:
        obj = objects.get(obj_key, {})
        if not obj:
            continue

        for field in BOUNDARY_MARKER_FIELDS:
            expected = FREEZE_SEAL_STEP2_BOUNDARY.get(field)
            actual = obj.get(field)
            if actual is None:
                drifts.append({
                    "object": obj_key,
                    "field": field,
                    "drift_type": "field_missing",
                    "label": resolve_drift_label("field_missing", lang=lang),
                    "expected": expected,
                    "actual": None,
                })
            elif actual != expected:
                drifts.append({
                    "object": obj_key,
                    "field": field,
                    "drift_type": "boundary_marker_mismatch",
                    "label": resolve_drift_label("boundary_marker_mismatch", lang=lang),
                    "expected": expected,
                    "actual": actual,
                })

    return drifts


def _audit_field_existence(
    objects: dict[str, dict[str, Any]],
    *,
    lang: str,
) -> list[dict[str, Any]]:
    """Check that consumable fields exist on each object."""
    drifts: list[dict[str, Any]] = []

    for obj_key in AUDITED_OBJECT_KEYS:
        entry = get_closure_schema_entry(obj_key)
        obj = objects.get(obj_key, {})
        if not obj:
            continue

        for field in entry.required_consumable_fields:
            if field not in obj:
                drifts.append({
                    "object": obj_key,
                    "field": field,
                    "drift_type": "field_missing",
                    "label": resolve_drift_label("field_missing", lang=lang),
                    "expected": "present",
                    "actual": "missing",
                })

    return drifts


def _audit_status_field_naming(
    objects: dict[str, dict[str, Any]],
    *,
    lang: str,
) -> list[dict[str, Any]]:
    """Check that each object uses its canonical status field name."""
    drifts: list[dict[str, Any]] = []

    for obj_key in AUDITED_OBJECT_KEYS:
        entry = get_closure_schema_entry(obj_key)
        obj = objects.get(obj_key, {})
        if not obj:
            continue

        expected_field = entry.status_field
        if expected_field and expected_field not in obj:
            drifts.append({
                "object": obj_key,
                "field": expected_field,
                "drift_type": "field_missing",
                "label": resolve_drift_label("field_missing", lang=lang),
                "expected": "present",
                "actual": "missing",
            })

    return drifts


def _audit_source_priority(
    objects: dict[str, dict[str, Any]],
    *,
    lang: str,
) -> list[dict[str, Any]]:
    """Check source field values are in canonical priority order."""
    mismatches: list[dict[str, Any]] = []

    for obj_key in AUDITED_OBJECT_KEYS:
        entry = get_closure_schema_entry(obj_key)
        obj = objects.get(obj_key, {})
        if not obj:
            continue

        source_field = entry.source_field
        if not source_field:
            continue

        source_value = str(obj.get(source_field) or "")
        if source_value and source_value not in entry.source_priority:
            mismatches.append({
                "object": obj_key,
                "field": source_field,
                "mismatch_type": "unexpected_source",
                "label": resolve_source_mismatch_label("unexpected_source", lang=lang),
                "expected_one_of": list(entry.source_priority),
                "actual": source_value,
            })

    return mismatches


def _audit_missing_surfaces(
    objects: dict[str, dict[str, Any]],
    surfaces: dict[str, bool],
    *,
    lang: str,
) -> list[dict[str, Any]]:
    """Check which surfaces are missing each object."""
    missing: list[dict[str, Any]] = []

    for obj_key in AUDITED_OBJECT_KEYS:
        obj = objects.get(obj_key, {})
        # If the object itself is empty, it's missing from all surfaces
        if not obj:
            for surface_key, available in surfaces.items():
                if available:
                    missing.append({
                        "object": obj_key,
                        "surface": surface_key,
                        "label": resolve_missing_surface_label(surface_key, lang=lang),
                    })

    return missing


# ---------------------------------------------------------------------------
# Derive status
# ---------------------------------------------------------------------------

def _derive_freeze_seal_status(
    *,
    drift_sections: list[dict[str, Any]],
    source_mismatches: list[dict[str, Any]],
    missing_surfaces: list[dict[str, Any]],
) -> str:
    """Derive freeze_seal_status from audit results.

    - blocker: boundary marker mismatch or source mismatch
    - attention: field missing or missing surface
    - ok: no drift at all
    - reviewer_only: no objects to audit
    """
    # Check for boundary marker mismatches (blocker)
    boundary_mismatches = [
        d for d in drift_sections
        if d.get("drift_type") == "boundary_marker_mismatch"
    ]
    if boundary_mismatches or source_mismatches:
        return FREEZE_SEAL_STATUS_BLOCKER

    # Check for field missing or missing surfaces (attention)
    field_missing = [
        d for d in drift_sections
        if d.get("drift_type") == "field_missing"
    ]
    if field_missing or missing_surfaces:
        return FREEZE_SEAL_STATUS_ATTENTION

    # If no drift at all, check if we had any objects
    if not drift_sections and not source_mismatches and not missing_surfaces:
        return FREEZE_SEAL_STATUS_OK

    return FREEZE_SEAL_STATUS_REVIEWER_ONLY


# ---------------------------------------------------------------------------
# Summary helpers
# ---------------------------------------------------------------------------

def _build_reviewer_summary_line(
    *,
    freeze_seal_status: str,
    drift_count: int,
    lang: str,
) -> str:
    if lang == "en":
        if freeze_seal_status == FREEZE_SEAL_STATUS_OK:
            return "Step 2 freeze seal: no drift detected. Not formal release approval."
        if freeze_seal_status == FREEZE_SEAL_STATUS_BLOCKER:
            return f"Step 2 freeze seal: {drift_count} drift blocker(s). Not formal release approval."
        if freeze_seal_status == FREEZE_SEAL_STATUS_ATTENTION:
            return f"Step 2 freeze seal: {drift_count} drift attention item(s). Not formal release approval."
        return "Step 2 freeze seal: reviewer-only observation. Not formal release approval."
    if freeze_seal_status == FREEZE_SEAL_STATUS_OK:
        return "Step 2 封板守护：未检测到漂移。不是正式放行批准。"
    if freeze_seal_status == FREEZE_SEAL_STATUS_BLOCKER:
        return f"Step 2 封板守护：{drift_count} 项漂移阻塞。不是正式放行批准。"
    if freeze_seal_status == FREEZE_SEAL_STATUS_ATTENTION:
        return f"Step 2 封板守护：{drift_count} 项漂移需关注。不是正式放行批准。"
    return "Step 2 封板守护：仅限审阅观察。不是正式放行批准。"


def _build_reviewer_summary_lines(
    *,
    freeze_seal_status: str,
    drift_sections: list[dict[str, Any]],
    source_mismatches: list[dict[str, Any]],
    missing_surfaces: list[dict[str, Any]],
    lang: str,
) -> list[str]:
    lines: list[str] = []

    # Title
    title = FREEZE_SEAL_TITLE_EN if lang == "en" else FREEZE_SEAL_TITLE_ZH
    lines.append(title)

    # Status
    status_label = resolve_freeze_seal_status_label(freeze_seal_status, lang=lang)
    if lang == "en":
        lines.append(f"Status: {status_label}")
    else:
        lines.append(f"状态：{status_label}")

    # Summary
    lines.append(FREEZE_SEAL_SUMMARY_EN if lang == "en" else FREEZE_SEAL_SUMMARY_ZH)

    # Drift sections
    if drift_sections:
        if lang == "en":
            lines.append(f"Drift items ({len(drift_sections)}):")
        else:
            lines.append(f"漂移项（{len(drift_sections)}）：")
        for d in drift_sections:
            obj = d.get("object", "")
            field = d.get("field", "")
            label = d.get("label", "")
            lines.append(f"  - {obj}.{field}: {label}")

    # Source mismatches
    if source_mismatches:
        if lang == "en":
            lines.append(f"Source mismatches ({len(source_mismatches)}):")
        else:
            lines.append(f"source 不一致（{len(source_mismatches)}）：")
        for m in source_mismatches:
            obj = m.get("object", "")
            field = m.get("field", "")
            label = m.get("label", "")
            lines.append(f"  - {obj}.{field}: {label}")

    # Missing surfaces
    if missing_surfaces:
        if lang == "en":
            lines.append(f"Missing surfaces ({len(missing_surfaces)}):")
        else:
            lines.append(f"缺失层（{len(missing_surfaces)}）：")
        for m in missing_surfaces:
            obj = m.get("object", "")
            surface = m.get("surface", "")
            label = m.get("label", "")
            lines.append(f"  - {obj} @ {surface}: {label}")

    # Simulation-only boundary
    lines.append(resolve_freeze_seal_simulation_only_boundary(lang=lang))

    # Reviewer-only notice
    lines.append(resolve_freeze_seal_reviewer_only_notice(lang=lang))

    # Non-claim notice
    lines.append(resolve_freeze_seal_non_claim_notice(lang=lang))

    return lines


def _build_audited_objects_summary(
    objects: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Build summary of which objects were present."""
    summary: list[dict[str, Any]] = []
    for obj_key in AUDITED_OBJECT_KEYS:
        entry = get_closure_schema_entry(obj_key)
        obj = objects.get(obj_key, {})
        summary.append({
            "key": obj_key,
            "present": bool(obj),
            "source": str(obj.get(entry.source_field) or ""),
            "status": str(obj.get(entry.status_field) or ""),
        })
    return summary


# ---------------------------------------------------------------------------
# Fallback
# ---------------------------------------------------------------------------

def build_freeze_seal_fallback(
    *,
    lang: str = "zh",
) -> dict[str, Any]:
    """Build freeze seal fallback default value.

    Guarantees all Step 2 boundary markers.
    Does not modify old run files; generates in-memory only.
    """
    _boundary = FREEZE_SEAL_SIMULATION_ONLY_BOUNDARY_EN if lang == "en" else FREEZE_SEAL_SIMULATION_ONLY_BOUNDARY_ZH
    _status_label = resolve_freeze_seal_status_label(FREEZE_SEAL_STATUS_REVIEWER_ONLY, lang=lang)
    _summary_line = (
        "Step 2 freeze seal: fallback — no persisted data. Not formal release approval."
        if lang == "en"
        else "Step 2 封板守护：fallback — 无持久化数据。不是正式放行批准。"
    )
    _summary_lines = [
        FREEZE_SEAL_TITLE_EN if lang == "en" else FREEZE_SEAL_TITLE_ZH,
        f"Status: {_status_label}" if lang == "en" else f"状态：{_status_label}",
        _summary_line,
        _boundary,
        resolve_freeze_seal_reviewer_only_notice(lang=lang),
        resolve_freeze_seal_non_claim_notice(lang=lang),
    ]

    return {
        "schema_version": "1.0",
        "artifact_type": "step2_freeze_seal_fallback",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": "",
        "phase": "step2_freeze_seal",
        "seal_version": FREEZE_SEAL_BUILDER_VERSION,
        "freeze_seal_status": FREEZE_SEAL_STATUS_REVIEWER_ONLY,
        "freeze_seal_status_label": _status_label,
        "reviewer_summary_line": _summary_line,
        "reviewer_summary_lines": _summary_lines,
        "drift_sections": [],
        "missing_surfaces": [],
        "source_mismatches": [],
        "audited_objects": [
            {"key": k, "present": False, "source": "", "status": ""}
            for k in AUDITED_OBJECT_KEYS
        ],
        "simulation_only_boundary": _boundary,
        "freeze_seal_source": "fallback",
        # Step 2 boundary markers — all enforced
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "not_ready_for_formal_claim": True,
        "reviewer_only": True,
        "readiness_mapping_only": True,
        "primary_evidence_rewritten": False,
        "real_acceptance_ready": False,
    }
