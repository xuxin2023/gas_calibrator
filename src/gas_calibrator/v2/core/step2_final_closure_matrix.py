"""Step 2 final closure matrix.

This is the final Step 2 schema-lock / no-drift guardrail built on top of the
existing closeout objects and freeze seal. It expresses Step 2 closure
consistency only. It is not a formal approval or release decision.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .step2_closure_schema_registry import (
    CANONICAL_BOUNDARY_MARKER_FIELDS,
    CANONICAL_SOURCE_PRIORITY,
    CANONICAL_STEP2_BOUNDARY,
    STEP2_CLOSURE_CORE_OBJECT_KEYS,
    get_closure_schema_entry,
)

FINAL_CLOSURE_MATRIX_VERSION: str = "2.26.0"

CLOSURE_MATRIX_STATUS_OK = "ok"
CLOSURE_MATRIX_STATUS_ATTENTION = "attention"
CLOSURE_MATRIX_STATUS_BLOCKER = "blocker"
CLOSURE_MATRIX_STATUS_REVIEWER_ONLY = "reviewer_only"

FINAL_CLOSURE_MATRIX_TITLE_ZH = "Step 2 最终封板矩阵"
FINAL_CLOSURE_MATRIX_TITLE_EN = "Step 2 Final Closure Matrix"

FINAL_CLOSURE_MATRIX_SUMMARY_ZH = (
    "Step 2 最终封板矩阵：面向 reviewer 与工程维护者，锁定 5 个核心收官对象在 "
    "results / reports / historical / review index 之间的可消费口径。"
    "它只表达 Step 2 封板一致性，不代表正式批准。"
)
FINAL_CLOSURE_MATRIX_SUMMARY_EN = (
    "Step 2 final closure matrix: reviewer-facing schema lock for the five core "
    "closeout objects across results / reports / historical / review index. "
    "It expresses Step 2 closure consistency only, not formal approval."
)

FINAL_CLOSURE_MATRIX_SIMULATION_ONLY_BOUNDARY_ZH = (
    "本最终封板矩阵仅基于仿真/离线/headless 证据，不代表 real acceptance "
    "evidence，不构成正式批准。"
)
FINAL_CLOSURE_MATRIX_SIMULATION_ONLY_BOUNDARY_EN = (
    "This final closure matrix is based on simulation/offline/headless evidence "
    "only. Not real acceptance evidence. Not formal approval."
)

FINAL_CLOSURE_MATRIX_REVIEWER_ONLY_NOTICE_ZH = (
    "本最终封板矩阵仅供 reviewer / 工程维护者审阅，不作为 operator 真实设备操作依据。"
)
FINAL_CLOSURE_MATRIX_REVIEWER_ONLY_NOTICE_EN = (
    "This final closure matrix is for reviewer / engineering maintenance only, "
    "not as operator action basis."
)

FINAL_CLOSURE_MATRIX_NON_CLAIM_NOTICE_ZH = (
    "不形成 formal compliance claim / accreditation claim / real acceptance evidence。"
)
FINAL_CLOSURE_MATRIX_NON_CLAIM_NOTICE_EN = (
    "Does not form formal compliance claim / accreditation claim / real "
    "acceptance evidence."
)

DRIFT_LABELS_ZH: dict[str, str] = {
    "field_missing": "字段缺失",
    "boundary_marker_mismatch": "边界标记不一致",
    "status_field_missing": "状态字段缺失",
}

DRIFT_LABELS_EN: dict[str, str] = {
    "field_missing": "Field missing",
    "boundary_marker_mismatch": "Boundary marker mismatch",
    "status_field_missing": "Status field missing",
}

SOURCE_MISMATCH_LABELS_ZH: dict[str, str] = {
    "unexpected_source": "source 值不在锁定优先级中",
}

SOURCE_MISMATCH_LABELS_EN: dict[str, str] = {
    "unexpected_source": "Source value not in canonical priority",
}

MISSING_SURFACE_LABELS_ZH: dict[str, str] = {
    "results": "results 层缺失",
    "reports": "reports 层缺失",
    "historical": "historical 层缺失",
    "review_index": "review index 层缺失",
}

MISSING_SURFACE_LABELS_EN: dict[str, str] = {
    "results": "Missing from results",
    "reports": "Missing from reports",
    "historical": "Missing from historical",
    "review_index": "Missing from review index",
}


def _extract_compare_from_closure_objects(
    objects: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    for source_object in ("step2_freeze_audit", "step2_closeout_package"):
        payload = dict(objects.get(source_object, {}) or {})
        if not bool(payload.get("compare_available")):
            continue
        return {
            "compare_available": True,
            "compare_source_object": source_object,
            "compare_status": str(payload.get("compare_status") or ""),
            "compare_status_display": str(payload.get("compare_status_display") or ""),
            "compare_summary_line": str(payload.get("compare_summary_line") or ""),
            "compare_summary_lines": list(payload.get("compare_summary_lines") or []),
            "compare_validation_profile": str(payload.get("compare_validation_profile") or ""),
            "compare_target_route": str(payload.get("compare_target_route") or ""),
            "compare_target_route_display": str(payload.get("compare_target_route_display") or ""),
            "compare_first_failure_phase": str(payload.get("compare_first_failure_phase") or ""),
            "compare_first_failure_phase_display": str(payload.get("compare_first_failure_phase_display") or ""),
            "compare_next_check": str(payload.get("compare_next_check") or ""),
            "compare_next_check_display": str(payload.get("compare_next_check_display") or ""),
        }
    return {
        "compare_available": False,
        "compare_source_object": "",
        "compare_status": "",
        "compare_status_display": "",
        "compare_summary_line": "",
        "compare_summary_lines": [],
        "compare_validation_profile": "",
        "compare_target_route": "",
        "compare_target_route_display": "",
        "compare_first_failure_phase": "",
        "compare_first_failure_phase_display": "",
        "compare_next_check": "",
        "compare_next_check_display": "",
    }


def _build_reviewer_summary_line_with_compare(
    *,
    closure_matrix_status: str,
    drift_count: int,
    missing_surface_count: int,
    source_mismatch_count: int,
    compare: dict[str, Any],
    lang: str,
) -> str:
    base = _build_reviewer_summary_line(
        closure_matrix_status=closure_matrix_status,
        drift_count=drift_count,
        missing_surface_count=missing_surface_count,
        source_mismatch_count=source_mismatch_count,
        lang=lang,
    )
    if not bool(compare.get("compare_available")):
        return base
    status = str(compare.get("compare_status_display") or compare.get("compare_status") or "--")
    next_check = str(compare.get("compare_next_check_display") or compare.get("compare_next_check") or "--")
    suffix = (
        f" | Compare: {status} | Next check: {next_check}"
        if lang == "en"
        else f" | 对齐状态：{status} | 下一步检查：{next_check}"
    )
    return base + suffix


def _build_reviewer_summary_lines_with_compare(
    *,
    closure_matrix_status: str,
    audited_surfaces: list[str],
    drift_sections: list[dict[str, Any]],
    missing_surfaces: list[dict[str, Any]],
    source_mismatches: list[dict[str, Any]],
    compare: dict[str, Any],
    lang: str,
) -> list[str]:
    lines = list(
        _build_reviewer_summary_lines(
            closure_matrix_status=closure_matrix_status,
            audited_surfaces=audited_surfaces,
            drift_sections=drift_sections,
            missing_surfaces=missing_surfaces,
            source_mismatches=source_mismatches,
            lang=lang,
        )
    )
    if not bool(compare.get("compare_available")):
        return lines
    compare_summary_line = str(compare.get("compare_summary_line") or "").strip()
    if not compare_summary_line:
        status = str(compare.get("compare_status_display") or compare.get("compare_status") or "--")
        next_check = str(compare.get("compare_next_check_display") or compare.get("compare_next_check") or "--")
        compare_summary_line = (
            f"Compare: {status} | Next check: {next_check}"
            if lang == "en"
            else f"离线对齐：{status} | 下一步检查：{next_check}"
        )
    insert_at = 3 if len(lines) >= 3 else len(lines)
    if compare_summary_line and compare_summary_line not in lines:
        lines.insert(insert_at, compare_summary_line)
    return lines


def _build_audited_objects_summary_with_compare(
    objects: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    summary = _build_audited_objects_summary(objects)
    for index, item in enumerate(summary):
        payload = dict(objects.get(str(item.get("key") or ""), {}) or {})
        if not bool(payload.get("compare_available")):
            continue
        summary[index] = {
            **item,
            "compare_available": True,
            "compare_status": str(payload.get("compare_status") or ""),
            "compare_status_display": str(payload.get("compare_status_display") or ""),
            "compare_summary_line": str(payload.get("compare_summary_line") or ""),
        }
    return summary


def build_step2_final_closure_matrix(
    *,
    run_id: str = "",
    step2_closeout_readiness: dict[str, Any] | None = None,
    step2_closeout_package: dict[str, Any] | None = None,
    step2_freeze_audit: dict[str, Any] | None = None,
    step3_admission_dossier: dict[str, Any] | None = None,
    step2_freeze_seal: dict[str, Any] | None = None,
    surface_results: bool = True,
    surface_reports: bool = True,
    surface_historical: bool = True,
    surface_review_index: bool = True,
    lang: str = "zh",
) -> dict[str, Any]:
    objects = {
        "step2_closeout_readiness": dict(step2_closeout_readiness or {}),
        "step2_closeout_package": dict(step2_closeout_package or {}),
        "step2_freeze_audit": dict(step2_freeze_audit or {}),
        "step3_admission_dossier": dict(step3_admission_dossier or {}),
        "step2_freeze_seal": dict(step2_freeze_seal or {}),
    }
    surfaces = {
        "results": surface_results,
        "reports": surface_reports,
        "historical": surface_historical,
        "review_index": surface_review_index,
    }

    drift_sections = _audit_boundary_markers(objects, lang=lang)
    drift_sections.extend(_audit_field_existence(objects, lang=lang))
    drift_sections.extend(_audit_status_field_naming(objects, lang=lang))
    source_mismatches = _audit_source_priority(objects, lang=lang)
    missing_surfaces = _audit_missing_surfaces(objects, surfaces, lang=lang)
    compare = _extract_compare_from_closure_objects(objects)
    closure_matrix_status = _derive_status(
        drift_sections=drift_sections,
        source_mismatches=source_mismatches,
        missing_surfaces=missing_surfaces,
    )
    audited_surfaces = [surface for surface, available in surfaces.items() if available]
    reviewer_summary_line = _build_reviewer_summary_line_with_compare(
        closure_matrix_status=closure_matrix_status,
        drift_count=len(drift_sections),
        missing_surface_count=len(missing_surfaces),
        source_mismatch_count=len(source_mismatches),
        compare=compare,
        lang=lang,
    )
    reviewer_summary_lines = _build_reviewer_summary_lines_with_compare(
        closure_matrix_status=closure_matrix_status,
        audited_surfaces=audited_surfaces,
        drift_sections=drift_sections,
        missing_surfaces=missing_surfaces,
        source_mismatches=source_mismatches,
        compare=compare,
        lang=lang,
    )

    return {
        "schema_version": "1.0",
        "artifact_type": "step2_final_closure_matrix",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": str(run_id or ""),
        "phase": "step2_final_closure_matrix",
        "closure_matrix_version": FINAL_CLOSURE_MATRIX_VERSION,
        "closure_matrix_status": closure_matrix_status,
        "closure_matrix_status_label": resolve_closure_matrix_status_label(
            closure_matrix_status,
            lang=lang,
        ),
        "audited_objects": _build_audited_objects_summary_with_compare(objects),
        "audited_surfaces": audited_surfaces,
        "drift_sections": drift_sections,
        "missing_surfaces": missing_surfaces,
        "source_mismatches": source_mismatches,
        "reviewer_summary_line": reviewer_summary_line,
        "reviewer_summary_lines": reviewer_summary_lines,
        "simulation_only_boundary": resolve_simulation_only_boundary(lang=lang),
        "closure_matrix_source": "rebuilt",
        **compare,
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "not_ready_for_formal_claim": True,
        "reviewer_only": True,
        "readiness_mapping_only": True,
        "primary_evidence_rewritten": False,
        "real_acceptance_ready": False,
    }


def has_final_closure_matrix_inputs(
    *,
    step2_closeout_readiness: dict[str, Any] | None = None,
    step2_closeout_package: dict[str, Any] | None = None,
    step2_freeze_audit: dict[str, Any] | None = None,
    step3_admission_dossier: dict[str, Any] | None = None,
    step2_freeze_seal: dict[str, Any] | None = None,
) -> bool:
    return any(
        bool(dict(payload or {}))
        for payload in (
            step2_closeout_readiness,
            step2_closeout_package,
            step2_freeze_audit,
            step3_admission_dossier,
            step2_freeze_seal,
        )
    )


def build_step2_final_closure_matrix_surface_payload(
    *,
    run_id: str = "",
    step2_closeout_readiness: dict[str, Any] | None = None,
    step2_closeout_package: dict[str, Any] | None = None,
    step2_freeze_audit: dict[str, Any] | None = None,
    step3_admission_dossier: dict[str, Any] | None = None,
    step2_freeze_seal: dict[str, Any] | None = None,
    surface_results: bool = True,
    surface_reports: bool = True,
    surface_historical: bool = True,
    surface_review_index: bool = True,
    lang: str = "zh",
) -> dict[str, Any]:
    if not has_final_closure_matrix_inputs(
        step2_closeout_readiness=step2_closeout_readiness,
        step2_closeout_package=step2_closeout_package,
        step2_freeze_audit=step2_freeze_audit,
        step3_admission_dossier=step3_admission_dossier,
        step2_freeze_seal=step2_freeze_seal,
    ):
        return build_final_closure_matrix_fallback(lang=lang)
    return build_step2_final_closure_matrix(
        run_id=run_id,
        step2_closeout_readiness=step2_closeout_readiness,
        step2_closeout_package=step2_closeout_package,
        step2_freeze_audit=step2_freeze_audit,
        step3_admission_dossier=step3_admission_dossier,
        step2_freeze_seal=step2_freeze_seal,
        surface_results=surface_results,
        surface_reports=surface_reports,
        surface_historical=surface_historical,
        surface_review_index=surface_review_index,
        lang=lang,
    )


def build_final_closure_matrix_fallback(*, lang: str = "zh") -> dict[str, Any]:
    reviewer_summary_line = (
        "Step 2 final closure matrix: fallback - no persisted data. Not formal approval."
        if lang == "en"
        else "Step 2 最终封板矩阵：fallback - 无持久化数据。不是正式批准。"
    )
    return {
        "schema_version": "1.0",
        "artifact_type": "step2_final_closure_matrix_fallback",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": "",
        "phase": "step2_final_closure_matrix",
        "closure_matrix_version": FINAL_CLOSURE_MATRIX_VERSION,
        "closure_matrix_status": CLOSURE_MATRIX_STATUS_REVIEWER_ONLY,
        "closure_matrix_status_label": resolve_closure_matrix_status_label(
            CLOSURE_MATRIX_STATUS_REVIEWER_ONLY,
            lang=lang,
        ),
        "audited_objects": [
            {
                "key": key,
                "label": resolve_object_label(key, lang=lang),
                "present": False,
                "status": "",
                "source": "",
            }
            for key in STEP2_CLOSURE_CORE_OBJECT_KEYS
        ],
        "audited_surfaces": ["results", "reports", "historical", "review_index"],
        "drift_sections": [],
        "missing_surfaces": [],
        "source_mismatches": [],
        "reviewer_summary_line": reviewer_summary_line,
        "reviewer_summary_lines": [
            resolve_title(lang=lang),
            _build_status_line(
                resolve_closure_matrix_status_label(
                    CLOSURE_MATRIX_STATUS_REVIEWER_ONLY,
                    lang=lang,
                ),
                lang=lang,
            ),
            reviewer_summary_line,
            resolve_simulation_only_boundary(lang=lang),
            resolve_reviewer_only_notice(lang=lang),
            resolve_non_claim_notice(lang=lang),
        ],
        "simulation_only_boundary": resolve_simulation_only_boundary(lang=lang),
        "closure_matrix_source": "fallback",
        **_extract_compare_from_closure_objects({}),
        "evidence_source": "simulated",
        "not_real_acceptance_evidence": True,
        "not_ready_for_formal_claim": True,
        "reviewer_only": True,
        "readiness_mapping_only": True,
        "primary_evidence_rewritten": False,
        "real_acceptance_ready": False,
    }


def resolve_title(*, lang: str = "zh") -> str:
    return FINAL_CLOSURE_MATRIX_TITLE_EN if lang == "en" else FINAL_CLOSURE_MATRIX_TITLE_ZH


def resolve_summary(*, lang: str = "zh") -> str:
    return FINAL_CLOSURE_MATRIX_SUMMARY_EN if lang == "en" else FINAL_CLOSURE_MATRIX_SUMMARY_ZH


def resolve_simulation_only_boundary(*, lang: str = "zh") -> str:
    return (
        FINAL_CLOSURE_MATRIX_SIMULATION_ONLY_BOUNDARY_EN
        if lang == "en"
        else FINAL_CLOSURE_MATRIX_SIMULATION_ONLY_BOUNDARY_ZH
    )


def resolve_reviewer_only_notice(*, lang: str = "zh") -> str:
    return (
        FINAL_CLOSURE_MATRIX_REVIEWER_ONLY_NOTICE_EN
        if lang == "en"
        else FINAL_CLOSURE_MATRIX_REVIEWER_ONLY_NOTICE_ZH
    )


def resolve_non_claim_notice(*, lang: str = "zh") -> str:
    return (
        FINAL_CLOSURE_MATRIX_NON_CLAIM_NOTICE_EN
        if lang == "en"
        else FINAL_CLOSURE_MATRIX_NON_CLAIM_NOTICE_ZH
    )


def resolve_closure_matrix_status_label(status: str, *, lang: str = "zh") -> str:
    labels_zh = {
        CLOSURE_MATRIX_STATUS_OK: "封板一致性已锁定",
        CLOSURE_MATRIX_STATUS_ATTENTION: "存在封板一致性关注项",
        CLOSURE_MATRIX_STATUS_BLOCKER: "存在封板一致性阻塞",
        CLOSURE_MATRIX_STATUS_REVIEWER_ONLY: "仅限审阅观察",
    }
    labels_en = {
        CLOSURE_MATRIX_STATUS_OK: "Closure consistency locked",
        CLOSURE_MATRIX_STATUS_ATTENTION: "Closure consistency attention",
        CLOSURE_MATRIX_STATUS_BLOCKER: "Closure consistency blocker",
        CLOSURE_MATRIX_STATUS_REVIEWER_ONLY: "Reviewer-only observation",
    }
    labels = labels_en if lang == "en" else labels_zh
    return labels.get(status, status)


def resolve_object_label(object_key: str, *, lang: str = "zh") -> str:
    entry = get_closure_schema_entry(object_key)
    return entry.display_label_en if lang == "en" else entry.display_label_zh


def _resolve_drift_label(drift_type: str, *, lang: str = "zh") -> str:
    labels = DRIFT_LABELS_EN if lang == "en" else DRIFT_LABELS_ZH
    return labels.get(drift_type, drift_type)


def _resolve_source_mismatch_label(mismatch_type: str, *, lang: str = "zh") -> str:
    labels = SOURCE_MISMATCH_LABELS_EN if lang == "en" else SOURCE_MISMATCH_LABELS_ZH
    return labels.get(mismatch_type, mismatch_type)


def _resolve_missing_surface_label(surface: str, *, lang: str = "zh") -> str:
    labels = MISSING_SURFACE_LABELS_EN if lang == "en" else MISSING_SURFACE_LABELS_ZH
    return labels.get(surface, surface)


def _audit_boundary_markers(
    objects: dict[str, dict[str, Any]],
    *,
    lang: str,
) -> list[dict[str, Any]]:
    drifts: list[dict[str, Any]] = []
    for object_key in STEP2_CLOSURE_CORE_OBJECT_KEYS:
        entry = get_closure_schema_entry(object_key)
        payload = objects.get(object_key, {})
        if not payload:
            continue
        for field in entry.boundary_marker_fields:
            expected = CANONICAL_STEP2_BOUNDARY[field]
            actual = payload.get(field)
            if actual is None:
                drifts.append(
                    {
                        "object": object_key,
                        "field": field,
                        "drift_type": "field_missing",
                        "label": _resolve_drift_label("field_missing", lang=lang),
                        "expected": expected,
                        "actual": None,
                    }
                )
            elif actual != expected:
                drifts.append(
                    {
                        "object": object_key,
                        "field": field,
                        "drift_type": "boundary_marker_mismatch",
                        "label": _resolve_drift_label(
                            "boundary_marker_mismatch",
                            lang=lang,
                        ),
                        "expected": expected,
                        "actual": actual,
                    }
                )
    return drifts


def _audit_field_existence(
    objects: dict[str, dict[str, Any]],
    *,
    lang: str,
) -> list[dict[str, Any]]:
    drifts: list[dict[str, Any]] = []
    for object_key in STEP2_CLOSURE_CORE_OBJECT_KEYS:
        entry = get_closure_schema_entry(object_key)
        payload = objects.get(object_key, {})
        if not payload:
            continue
        for field in entry.required_consumable_fields:
            if field in payload:
                continue
            drifts.append(
                {
                    "object": object_key,
                    "field": field,
                    "drift_type": "field_missing",
                    "label": _resolve_drift_label("field_missing", lang=lang),
                    "expected": "present",
                    "actual": "missing",
                }
            )
    return drifts


def _audit_status_field_naming(
    objects: dict[str, dict[str, Any]],
    *,
    lang: str,
) -> list[dict[str, Any]]:
    drifts: list[dict[str, Any]] = []
    for object_key in STEP2_CLOSURE_CORE_OBJECT_KEYS:
        entry = get_closure_schema_entry(object_key)
        payload = objects.get(object_key, {})
        if not payload:
            continue
        if entry.status_field in payload:
            continue
        drifts.append(
            {
                "object": object_key,
                "field": entry.status_field,
                "drift_type": "status_field_missing",
                "label": _resolve_drift_label("status_field_missing", lang=lang),
                "expected": "present",
                "actual": "missing",
            }
        )
    return drifts


def _audit_source_priority(
    objects: dict[str, dict[str, Any]],
    *,
    lang: str,
) -> list[dict[str, Any]]:
    mismatches: list[dict[str, Any]] = []
    for object_key in STEP2_CLOSURE_CORE_OBJECT_KEYS:
        entry = get_closure_schema_entry(object_key)
        payload = objects.get(object_key, {})
        if not payload:
            continue
        source_value = str(payload.get(entry.source_field) or "")
        if not source_value:
            continue
        if source_value in CANONICAL_SOURCE_PRIORITY:
            continue
        mismatches.append(
            {
                "object": object_key,
                "field": entry.source_field,
                "mismatch_type": "unexpected_source",
                "label": _resolve_source_mismatch_label("unexpected_source", lang=lang),
                "expected_one_of": list(entry.source_priority),
                "actual": source_value,
            }
        )
    return mismatches


def _audit_missing_surfaces(
    objects: dict[str, dict[str, Any]],
    surfaces: dict[str, bool],
    *,
    lang: str,
) -> list[dict[str, Any]]:
    missing: list[dict[str, Any]] = []
    for object_key in STEP2_CLOSURE_CORE_OBJECT_KEYS:
        payload = objects.get(object_key, {})
        if payload:
            continue
        for surface, available in surfaces.items():
            if not available:
                continue
            missing.append(
                {
                    "object": object_key,
                    "surface": surface,
                    "label": _resolve_missing_surface_label(surface, lang=lang),
                }
            )
    return missing


def _derive_status(
    *,
    drift_sections: list[dict[str, Any]],
    source_mismatches: list[dict[str, Any]],
    missing_surfaces: list[dict[str, Any]],
) -> str:
    if any(item.get("drift_type") == "boundary_marker_mismatch" for item in drift_sections):
        return CLOSURE_MATRIX_STATUS_BLOCKER
    if source_mismatches:
        return CLOSURE_MATRIX_STATUS_BLOCKER
    if drift_sections or missing_surfaces:
        return CLOSURE_MATRIX_STATUS_ATTENTION
    return CLOSURE_MATRIX_STATUS_OK


def _build_status_line(status_label: str, *, lang: str) -> str:
    return f"Status: {status_label}" if lang == "en" else f"状态：{status_label}"


def _build_reviewer_summary_line(
    *,
    closure_matrix_status: str,
    drift_count: int,
    missing_surface_count: int,
    source_mismatch_count: int,
    lang: str,
) -> str:
    if lang == "en":
        if closure_matrix_status == CLOSURE_MATRIX_STATUS_OK:
            return "Step 2 final closure matrix: five core objects are schema-locked across four surfaces. Not formal approval."
        return (
            "Step 2 final closure matrix: "
            f"{drift_count} drift item(s), {missing_surface_count} missing-surface item(s), "
            f"{source_mismatch_count} source mismatch(es). Not formal approval."
        )
    if closure_matrix_status == CLOSURE_MATRIX_STATUS_OK:
        return "Step 2 最终封板矩阵：5 个核心对象在 4 个 surface 上已锁定一致。不是正式批准。"
    return (
        "Step 2 最终封板矩阵："
        f"{drift_count} 项漂移，{missing_surface_count} 项 surface 缺失，"
        f"{source_mismatch_count} 项 source 不一致。不是正式批准。"
    )


def _build_reviewer_summary_lines(
    *,
    closure_matrix_status: str,
    audited_surfaces: list[str],
    drift_sections: list[dict[str, Any]],
    missing_surfaces: list[dict[str, Any]],
    source_mismatches: list[dict[str, Any]],
    lang: str,
) -> list[str]:
    lines = [
        resolve_title(lang=lang),
        _build_status_line(
            resolve_closure_matrix_status_label(closure_matrix_status, lang=lang),
            lang=lang,
        ),
        resolve_summary(lang=lang),
    ]
    if lang == "en":
        lines.append(f"Audited surfaces: {', '.join(audited_surfaces) or 'none'}")
    else:
        lines.append(f"审计 surfaces：{', '.join(audited_surfaces) or '无'}")
    if drift_sections:
        label = "Drift items" if lang == "en" else "漂移项"
        lines.append(f"{label} ({len(drift_sections)}):")
        for item in drift_sections:
            lines.append(f"  - {item.get('object')}.{item.get('field')}: {item.get('label')}")
    if missing_surfaces:
        label = "Missing surfaces" if lang == "en" else "缺失 surfaces"
        lines.append(f"{label} ({len(missing_surfaces)}):")
        for item in missing_surfaces:
            lines.append(f"  - {item.get('object')} @ {item.get('surface')}: {item.get('label')}")
    if source_mismatches:
        label = "Source mismatches" if lang == "en" else "source 不一致"
        lines.append(f"{label} ({len(source_mismatches)}):")
        for item in source_mismatches:
            lines.append(f"  - {item.get('object')}.{item.get('field')}: {item.get('label')}")
    lines.append(resolve_simulation_only_boundary(lang=lang))
    lines.append(resolve_reviewer_only_notice(lang=lang))
    lines.append(resolve_non_claim_notice(lang=lang))
    return lines


def _build_audited_objects_summary(
    objects: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    summary: list[dict[str, Any]] = []
    for object_key in STEP2_CLOSURE_CORE_OBJECT_KEYS:
        entry = get_closure_schema_entry(object_key)
        payload = objects.get(object_key, {})
        summary.append(
            {
                "key": object_key,
                "label_zh": entry.display_label_zh,
                "label_en": entry.display_label_en,
                "present": bool(payload),
                "status_field": entry.status_field,
                "source_field": entry.source_field,
                "status": str(payload.get(entry.status_field) or ""),
                "source": str(payload.get(entry.source_field) or ""),
            }
        )
    return summary
