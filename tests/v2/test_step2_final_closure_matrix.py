from __future__ import annotations

from gas_calibrator.v2.core.step2_closeout_package_builder import build_step2_closeout_package
from gas_calibrator.v2.core.step2_closeout_readiness_builder import build_step2_closeout_readiness
from gas_calibrator.v2.core.step2_closure_schema_registry import (
    CANONICAL_BOUNDARY_MARKER_FIELDS,
    CANONICAL_SOURCE_PRIORITY,
    SCHEMA_REGISTRY_VERSION,
    STEP2_CLOSURE_CORE_OBJECT_KEYS,
    get_closure_schema_entry,
)
from gas_calibrator.v2.core.step2_final_closure_matrix import (
    FINAL_CLOSURE_MATRIX_VERSION,
    build_step2_final_closure_matrix,
)
from gas_calibrator.v2.core.step2_freeze_audit_builder import build_step2_freeze_audit
from gas_calibrator.v2.core.step2_freeze_seal_builder import build_step2_freeze_seal
from gas_calibrator.v2.core.step2_closeout_verification import build_step2_closeout_verification
from gas_calibrator.v2.core.step3_admission_dossier_builder import build_step3_admission_dossier
from gas_calibrator.v2.core.reviewer_summary_packs import build_control_flow_compare_pack


def _build_closure_chain() -> tuple[dict, dict, dict, dict, dict]:
    readiness = build_step2_closeout_readiness(run_id="matrix-test")
    package = build_step2_closeout_package(
        run_id="matrix-test",
        step2_closeout_readiness=readiness,
    )
    audit = build_step2_freeze_audit(
        run_id="matrix-test",
        step2_closeout_package=package,
        step2_closeout_readiness=readiness,
    )
    dossier = build_step3_admission_dossier(
        run_id="matrix-test",
        step2_freeze_audit=audit,
        step2_closeout_package=package,
        step2_closeout_readiness=readiness,
    )
    verification = build_step2_closeout_verification(
        run_id="matrix-test",
        step2_closeout_readiness=readiness,
        step2_closeout_package=package,
        step2_freeze_audit=audit,
        step3_admission_dossier=dossier,
    )
    seal = build_step2_freeze_seal(
        run_id="matrix-test",
        step2_closeout_readiness=readiness,
        step2_closeout_package=package,
        step2_freeze_audit=audit,
        step3_admission_dossier=dossier,
        step2_closeout_verification=verification,
    )
    return readiness, package, audit, dossier, seal


def test_registry_version_and_core_keys() -> None:
    assert SCHEMA_REGISTRY_VERSION == "2.26.0"
    assert STEP2_CLOSURE_CORE_OBJECT_KEYS == (
        "step2_closeout_readiness",
        "step2_closeout_package",
        "step2_freeze_audit",
        "step3_admission_dossier",
        "step2_freeze_seal",
    )


def test_registry_entries_have_zh_en_labels_and_canonical_fields() -> None:
    for key in STEP2_CLOSURE_CORE_OBJECT_KEYS:
        entry = get_closure_schema_entry(key)
        assert entry.object_key == key
        assert entry.display_label_zh
        assert entry.display_label_en
        assert entry.status_field
        assert entry.source_field
        assert entry.required_consumable_fields
        assert entry.source_priority == CANONICAL_SOURCE_PRIORITY
        assert entry.boundary_marker_fields == CANONICAL_BOUNDARY_MARKER_FIELDS


def test_final_closure_matrix_output_is_stable_for_five_core_objects() -> None:
    readiness, package, audit, dossier, seal = _build_closure_chain()

    matrix = build_step2_final_closure_matrix(
        run_id="matrix-test",
        step2_closeout_readiness=readiness,
        step2_closeout_package=package,
        step2_freeze_audit=audit,
        step3_admission_dossier=dossier,
        step2_freeze_seal=seal,
    )

    assert FINAL_CLOSURE_MATRIX_VERSION == "2.26.0"
    assert matrix["artifact_type"] == "step2_final_closure_matrix"
    assert matrix["closure_matrix_status"] == "ok"
    assert matrix["drift_sections"] == []
    assert matrix["missing_surfaces"] == []
    assert matrix["source_mismatches"] == []
    assert [item["key"] for item in matrix["audited_objects"]] == list(STEP2_CLOSURE_CORE_OBJECT_KEYS)
    assert matrix["audited_surfaces"] == ["results", "reports", "historical", "review_index"]


def test_final_closure_matrix_flags_missing_review_index_surface_without_formal_claims() -> None:
    readiness, package, audit, dossier, seal = _build_closure_chain()

    matrix = build_step2_final_closure_matrix(
        run_id="matrix-test",
        step2_closeout_readiness=readiness,
        step2_closeout_package=package,
        step2_freeze_audit=audit,
        step3_admission_dossier=dossier,
        step2_freeze_seal=seal,
        surface_review_index=False,
    )

    assert "review_index" not in matrix["audited_surfaces"]
    assert matrix["missing_surfaces"] == []
    assert matrix["not_real_acceptance_evidence"] is True
    assert matrix["not_ready_for_formal_claim"] is True
    assert matrix["real_acceptance_ready"] is False
    assert "formal approval" not in matrix["reviewer_summary_line"].lower() or "not" in matrix["reviewer_summary_line"].lower()


def test_final_closure_matrix_surfaces_compare_summary_from_closeout_chain() -> None:
    readiness = build_step2_closeout_readiness(run_id="matrix-compare")
    compare_pack = build_control_flow_compare_pack(
        {
            "latest_control_flow_compare": {
                "compare_status": "MISMATCH",
                "validation_profile": "replacement_skip0_co2_only_simulated",
                "target_route": "co2",
                "first_failure_phase": "sample_end",
                "point_presence_diff": "no_diff",
                "sample_count_diff": "diff_present",
                "route_trace_diff": "diff_present",
                "key_action_mismatches": ["vent"],
                "physical_route_mismatch": "yes",
                "next_check": "inspect sample count diff",
            }
        }
    )
    package = build_step2_closeout_package(
        run_id="matrix-compare",
        step2_closeout_readiness=readiness,
        compact_summary_packs=[compare_pack],
    )
    audit = build_step2_freeze_audit(
        run_id="matrix-compare",
        step2_closeout_package=package,
        step2_closeout_readiness=readiness,
    )
    dossier = build_step3_admission_dossier(
        run_id="matrix-compare",
        step2_freeze_audit=audit,
        step2_closeout_package=package,
        step2_closeout_readiness=readiness,
    )
    verification = build_step2_closeout_verification(
        run_id="matrix-compare",
        step2_closeout_readiness=readiness,
        step2_closeout_package=package,
        step2_freeze_audit=audit,
        step3_admission_dossier=dossier,
    )
    seal = build_step2_freeze_seal(
        run_id="matrix-compare",
        step2_closeout_readiness=readiness,
        step2_closeout_package=package,
        step2_freeze_audit=audit,
        step3_admission_dossier=dossier,
        step2_closeout_verification=verification,
    )

    matrix = build_step2_final_closure_matrix(
        run_id="matrix-compare",
        step2_closeout_readiness=readiness,
        step2_closeout_package=package,
        step2_freeze_audit=audit,
        step3_admission_dossier=dossier,
        step2_freeze_seal=seal,
    )

    closeout_object = next(item for item in matrix["audited_objects"] if item["key"] == "step2_closeout_package")
    assert matrix["compare_available"] is True
    assert matrix["compare_status"] == "MISMATCH"
    assert matrix["compare_source_object"] in {"step2_freeze_audit", "step2_closeout_package"}
    assert any("离线对齐" in line or "Compare" in line for line in matrix["reviewer_summary_lines"])
    assert closeout_object["compare_available"] is True
    assert closeout_object["compare_status"] == "MISMATCH"
