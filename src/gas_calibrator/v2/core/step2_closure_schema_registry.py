"""Canonical Step 2 closure schema registry.

This registry locks the consumable schema for the Step 2 closeout guardrails so
that freeze-seal audits, final-closure-matrix audits, tests, and reviewer
surfaces all read from the same canonical definition.

It does not introduce new business workflows. It only records the stable schema
that Step 2 surfaces are expected to consume.
"""

from __future__ import annotations

from dataclasses import dataclass

SCHEMA_REGISTRY_VERSION: str = "2.26.0"

CANONICAL_STEP2_BOUNDARY: dict[str, str | bool] = {
    "evidence_source": "simulated",
    "not_real_acceptance_evidence": True,
    "not_ready_for_formal_claim": True,
    "reviewer_only": True,
    "readiness_mapping_only": True,
    "primary_evidence_rewritten": False,
    "real_acceptance_ready": False,
}

CANONICAL_BOUNDARY_MARKER_FIELDS: tuple[str, ...] = (
    "evidence_source",
    "not_real_acceptance_evidence",
    "not_ready_for_formal_claim",
    "reviewer_only",
    "readiness_mapping_only",
    "primary_evidence_rewritten",
    "real_acceptance_ready",
)

CANONICAL_SOURCE_PRIORITY: tuple[str, ...] = ("persisted", "rebuilt", "fallback")

STEP2_CLOSURE_CORE_OBJECT_KEYS: tuple[str, ...] = (
    "step2_closeout_readiness",
    "step2_closeout_package",
    "step2_freeze_audit",
    "step3_admission_dossier",
    "step2_freeze_seal",
)

STEP2_CLOSURE_AUXILIARY_OBJECT_KEYS: tuple[str, ...] = (
    "step2_closeout_verification",
)

STEP2_FREEZE_SEAL_AUDIT_OBJECT_KEYS: tuple[str, ...] = (
    "step2_closeout_readiness",
    "step2_closeout_package",
    "step2_freeze_audit",
    "step3_admission_dossier",
    "step2_closeout_verification",
)

STEP2_CLOSURE_ALL_OBJECT_KEYS: tuple[str, ...] = (
    STEP2_CLOSURE_CORE_OBJECT_KEYS + STEP2_CLOSURE_AUXILIARY_OBJECT_KEYS
)


@dataclass(frozen=True)
class ClosureSchemaEntry:
    object_key: str
    artifact_type: str
    status_field: str
    source_field: str
    boundary_marker_fields: tuple[str, ...]
    required_consumable_fields: tuple[str, ...]
    optional_consumable_fields: tuple[str, ...]
    source_priority: tuple[str, ...]
    display_label_zh: str
    display_label_en: str

    @property
    def consumable_fields(self) -> tuple[str, ...]:
        return self.required_consumable_fields + self.optional_consumable_fields


_BASE_REQUIRED_FIELDS: tuple[str, ...] = (
    "schema_version",
    "artifact_type",
    "generated_at",
    "run_id",
    "phase",
    "reviewer_summary_line",
    "simulation_only_boundary",
)

_BASE_OPTIONAL_FIELDS: tuple[str, ...] = (
    "reviewer_summary_lines",
    "blockers",
    "next_steps",
)


def _required_fields(
    *,
    status_field: str,
    source_field: str,
    extra_required: tuple[str, ...] = (),
) -> tuple[str, ...]:
    return (
        _BASE_REQUIRED_FIELDS
        + (status_field, source_field)
        + CANONICAL_BOUNDARY_MARKER_FIELDS
        + extra_required
    )


REGISTRY: dict[str, ClosureSchemaEntry] = {
    "step2_closeout_readiness": ClosureSchemaEntry(
        object_key="step2_closeout_readiness",
        artifact_type="step2_closeout_readiness",
        status_field="closeout_status",
        source_field="closeout_readiness_source",
        boundary_marker_fields=CANONICAL_BOUNDARY_MARKER_FIELDS,
        required_consumable_fields=_required_fields(
            status_field="closeout_status",
            source_field="closeout_readiness_source",
            extra_required=(
                "closeout_status_label",
                "contributing_sections",
                "rendered_compact_sections",
                "gate_status",
                "gate_summary",
                "closeout_gate_alignment",
            ),
        ),
        optional_consumable_fields=_BASE_OPTIONAL_FIELDS
        + (
            "source_readiness_status",
            "source_blocking_items",
            "source_warning_items",
        ),
        source_priority=CANONICAL_SOURCE_PRIORITY,
        display_label_zh="Step 2 收官就绪度",
        display_label_en="Step 2 Closeout Readiness",
    ),
    "step2_closeout_package": ClosureSchemaEntry(
        object_key="step2_closeout_package",
        artifact_type="step2_closeout_package",
        status_field="package_status",
        source_field="closeout_package_source",
        boundary_marker_fields=CANONICAL_BOUNDARY_MARKER_FIELDS,
        required_consumable_fields=_required_fields(
            status_field="package_status",
            source_field="closeout_package_source",
            extra_required=(
                "package_version",
                "package_status_label",
                "sections",
                "section_order",
                "source_versions",
            ),
        ),
        optional_consumable_fields=_BASE_OPTIONAL_FIELDS,
        source_priority=CANONICAL_SOURCE_PRIORITY,
        display_label_zh="Step 2 收官包",
        display_label_en="Step 2 Closeout Package",
    ),
    "step2_freeze_audit": ClosureSchemaEntry(
        object_key="step2_freeze_audit",
        artifact_type="step2_freeze_audit",
        status_field="audit_status",
        source_field="freeze_audit_source",
        boundary_marker_fields=CANONICAL_BOUNDARY_MARKER_FIELDS,
        required_consumable_fields=_required_fields(
            status_field="audit_status",
            source_field="freeze_audit_source",
            extra_required=(
                "audit_version",
                "audit_status_label",
                "audit_sections",
                "section_order",
                "freeze_candidate",
                "freeze_candidate_notice_zh",
                "freeze_candidate_notice_en",
            ),
        ),
        optional_consumable_fields=_BASE_OPTIONAL_FIELDS,
        source_priority=CANONICAL_SOURCE_PRIORITY,
        display_label_zh="Step 2 冻结审计",
        display_label_en="Step 2 Freeze Audit",
    ),
    "step3_admission_dossier": ClosureSchemaEntry(
        object_key="step3_admission_dossier",
        artifact_type="step3_admission_dossier",
        status_field="dossier_status",
        source_field="admission_dossier_source",
        boundary_marker_fields=CANONICAL_BOUNDARY_MARKER_FIELDS,
        required_consumable_fields=_required_fields(
            status_field="dossier_status",
            source_field="admission_dossier_source",
            extra_required=(
                "dossier_version",
                "dossier_status_label",
                "dossier_sections",
                "section_order",
                "admission_candidate",
                "admission_candidate_notice_zh",
                "admission_candidate_notice_en",
                "source_versions",
            ),
        ),
        optional_consumable_fields=_BASE_OPTIONAL_FIELDS,
        source_priority=CANONICAL_SOURCE_PRIORITY,
        display_label_zh="Step 3 准入材料",
        display_label_en="Step 3 Admission Dossier",
    ),
    "step2_closeout_verification": ClosureSchemaEntry(
        object_key="step2_closeout_verification",
        artifact_type="step2_closeout_verification",
        status_field="verification_status",
        source_field="closeout_verification_source",
        boundary_marker_fields=CANONICAL_BOUNDARY_MARKER_FIELDS,
        required_consumable_fields=_required_fields(
            status_field="verification_status",
            source_field="closeout_verification_source",
            extra_required=(
                "verification_version",
                "missing_for_step3",
                "closeout_readiness_status",
                "closeout_package_status",
                "freeze_audit_status",
                "dossier_status",
                "verification_source",
                "verification_fallback_reason",
            ),
        ),
        optional_consumable_fields=_BASE_OPTIONAL_FIELDS + ("reviewer_summary_lines",),
        source_priority=CANONICAL_SOURCE_PRIORITY,
        display_label_zh="Step 2 收官验证",
        display_label_en="Step 2 Closeout Verification",
    ),
    "step2_freeze_seal": ClosureSchemaEntry(
        object_key="step2_freeze_seal",
        artifact_type="step2_freeze_seal",
        status_field="freeze_seal_status",
        source_field="freeze_seal_source",
        boundary_marker_fields=CANONICAL_BOUNDARY_MARKER_FIELDS,
        required_consumable_fields=_required_fields(
            status_field="freeze_seal_status",
            source_field="freeze_seal_source",
            extra_required=(
                "seal_version",
                "freeze_seal_status_label",
                "drift_sections",
                "missing_surfaces",
                "source_mismatches",
                "audited_objects",
            ),
        ),
        optional_consumable_fields=("reviewer_summary_lines",),
        source_priority=CANONICAL_SOURCE_PRIORITY,
        display_label_zh="Step 2 封板守护",
        display_label_en="Step 2 Freeze Seal",
    ),
}


def get_closure_schema_entry(object_key: str) -> ClosureSchemaEntry:
    return REGISTRY[object_key]


def get_closure_schema_entries(
    object_keys: tuple[str, ...] = STEP2_CLOSURE_ALL_OBJECT_KEYS,
) -> tuple[ClosureSchemaEntry, ...]:
    return tuple(REGISTRY[key] for key in object_keys)


def build_status_field_map(
    object_keys: tuple[str, ...] = STEP2_CLOSURE_ALL_OBJECT_KEYS,
) -> dict[str, str]:
    return {
        entry.object_key: entry.status_field
        for entry in get_closure_schema_entries(object_keys)
    }


def build_source_field_map(
    object_keys: tuple[str, ...] = STEP2_CLOSURE_ALL_OBJECT_KEYS,
) -> dict[str, str]:
    return {
        entry.object_key: entry.source_field
        for entry in get_closure_schema_entries(object_keys)
    }


def build_required_consumable_field_map(
    object_keys: tuple[str, ...] = STEP2_CLOSURE_ALL_OBJECT_KEYS,
) -> dict[str, tuple[str, ...]]:
    return {
        entry.object_key: entry.required_consumable_fields
        for entry in get_closure_schema_entries(object_keys)
    }


def build_optional_consumable_field_map(
    object_keys: tuple[str, ...] = STEP2_CLOSURE_ALL_OBJECT_KEYS,
) -> dict[str, tuple[str, ...]]:
    return {
        entry.object_key: entry.optional_consumable_fields
        for entry in get_closure_schema_entries(object_keys)
    }


def build_consumable_field_union(
    object_keys: tuple[str, ...] = STEP2_CLOSURE_ALL_OBJECT_KEYS,
) -> tuple[str, ...]:
    ordered_fields: list[str] = []
    seen: set[str] = set()
    for entry in get_closure_schema_entries(object_keys):
        for field in entry.consumable_fields:
            if field in seen:
                continue
            seen.add(field)
            ordered_fields.append(field)
    return tuple(ordered_fields)
