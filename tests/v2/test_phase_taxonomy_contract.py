from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

from gas_calibrator.v2.adapters.results_gateway import ResultsGateway
from gas_calibrator.v2.core.measurement_phase_coverage import (
    MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME,
    MEASUREMENT_PHASE_COVERAGE_REPORT_MARKDOWN_FILENAME,
    build_measurement_phase_coverage_report,
)
from gas_calibrator.v2.core.models import CalibrationPoint, SamplingResult
from gas_calibrator.v2.core.multi_source_stability import (
    MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME,
    SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME,
)
from gas_calibrator.v2.core.phase_taxonomy_contract import (
    GAP_CLASSIFICATION_FAMILY,
    GAP_SEVERITY_FAMILY,
    METHOD_CONFIRMATION_FAMILY,
    TAXONOMY_CONTRACT_VERSION,
    TRACEABILITY_NODE_FAMILY,
    UNCERTAINTY_INPUT_FAMILY,
    normalize_phase_taxonomy_row,
    normalize_taxonomy_key,
    phase_gap_classification_key,
    phase_gap_severity_key,
    phase_reviewer_next_step_template_key,
    taxonomy_display_label,
    taxonomy_i18n_key,
)
from gas_calibrator.v2.core.reviewer_fragments_contract import (
    BLOCKER_FRAGMENT_FAMILY,
    GAP_REASON_FRAGMENT_FAMILY,
    READINESS_IMPACT_FRAGMENT_FAMILY,
    REVIEWER_FRAGMENTS_CONTRACT_VERSION,
    REVIEWER_NEXT_STEP_FRAGMENT_FAMILY,
    build_fragment_row,
    normalize_fragment_key,
)
from gas_calibrator.v2.review_surface_formatter import (
    build_measurement_review_digest_lines,
    build_readiness_review_digest_lines,
)
from gas_calibrator.v2.scripts.build_offline_governance_artifacts import rebuild_run
from gas_calibrator.v2.ui_v2.i18n import display_fragment_value, display_taxonomy_value

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import build_fake_facade


def _point(
    index: int,
    *,
    route: str,
    pressure_hpa: float | None = 1000.0,
    pressure_mode: str = "",
) -> CalibrationPoint:
    return CalibrationPoint(
        index=index,
        temperature_c=25.0,
        co2_ppm=400.0 if route != "h2o" else None,
        humidity_pct=45.0 if route == "h2o" else None,
        pressure_hpa=pressure_hpa,
        route=route,
        pressure_mode=pressure_mode,
        pressure_target_label="ambient" if pressure_mode == "ambient_open" else None,
        pressure_selection_token="ambient" if pressure_mode == "ambient_open" else "",
    )


def _sample(
    point: CalibrationPoint,
    *,
    seconds: int,
    point_phase: str,
    **overrides: float | str | bool | None,
) -> SamplingResult:
    payload = {
        "co2_ppm": 400.0 if point.route != "h2o" else None,
        "h2o_mmol": 0.72 if point.route == "h2o" else None,
        "co2_signal": 4500.0 if point.route != "h2o" else None,
        "h2o_signal": 2400.0 if point.route == "h2o" else None,
        "co2_ratio_f": 1.000 if point.route != "h2o" else None,
        "co2_ratio_raw": 1.001 if point.route != "h2o" else None,
        "h2o_ratio_f": 0.700 if point.route == "h2o" else None,
        "h2o_ratio_raw": 0.699 if point.route == "h2o" else None,
        "ref_signal": 3500.0,
        "temperature_c": 25.0,
        "pressure_hpa": None if point.pressure_mode == "ambient_open" else 1000.0,
        "dew_point_c": 5.2,
        "frame_has_data": True,
        "frame_usable": True,
        "point_phase": point_phase,
        "point_tag": point.pressure_target_label or point.route,
        "sample_index": max(1, seconds // 5 + 1),
        "stability_time_s": float(max(seconds, 1)),
        "total_time_s": 30.0,
    }
    payload.update(overrides)
    return SamplingResult(
        point=point,
        analyzer_id="ga01",
        timestamp=datetime(2026, 4, 9, 9, 0, tzinfo=timezone.utc) + timedelta(seconds=seconds),
        **payload,
    )


def test_taxonomy_contract_normalizes_aliases_and_phase_profiles() -> None:
    assert normalize_taxonomy_key(
        METHOD_CONFIRMATION_FAMILY,
        "Ambient baseline stabilization rule",
    ) == "ambient_baseline_stabilization_rule"
    assert normalize_taxonomy_key(
        UNCERTAINTY_INPUT_FAMILY,
        "preseal pressure term",
    ) == "preseal_pressure_term"
    assert normalize_taxonomy_key(
        TRACEABILITY_NODE_FAMILY,
        "Software event log chain",
    ) == "software_event_log_chain"
    assert taxonomy_i18n_key(
        METHOD_CONFIRMATION_FAMILY,
        "ambient_baseline_stabilization_rule",
    ) == "taxonomy.method_confirmation.ambient_baseline_stabilization_rule"
    assert taxonomy_display_label(
        METHOD_CONFIRMATION_FAMILY,
        "ambient_baseline_stabilization_rule",
        locale="en_US",
    ) == "Ambient baseline stabilization rule"
    assert taxonomy_display_label(
        METHOD_CONFIRMATION_FAMILY,
        "ambient_baseline_stabilization_rule",
        locale="zh_CN",
    ) != "Ambient baseline stabilization rule"
    assert phase_gap_classification_key(
        route_family="water",
        phase_name="preseal",
        coverage_bucket="actual_simulated_run_with_payload_partial",
        payload_completeness="partial",
    ) == "conditioning_window_partial_payload"
    assert phase_gap_severity_key(
        route_family="water",
        phase_name="preseal",
        coverage_bucket="actual_simulated_run_with_payload_partial",
        payload_completeness="partial",
    ) == "high"
    assert phase_reviewer_next_step_template_key(
        route_family="water",
        phase_name="preseal",
        coverage_bucket="actual_simulated_run_with_payload_partial",
        payload_completeness="partial",
    ) == "water_preseal_partial_gap_closeout"

    normalized = normalize_phase_taxonomy_row(
        {
            "route_family": "ambient",
            "phase_name": "ambient_diagnostic",
            "coverage_bucket": "actual_simulated_run_with_payload_complete",
            "payload_completeness": "complete",
            "linked_method_confirmation_items": ["Ambient baseline stabilization rule"],
            "linked_uncertainty_inputs": ["Ambient pressure baseline"],
            "linked_traceability_stub_nodes": ["Ambient environment reference chain"],
            "gap_classification": "ambient_baseline_payload_complete_anchor",
            "gap_severity": "info",
        },
        display_locale="en_US",
    )
    assert normalized["taxonomy_contract_version"] == TAXONOMY_CONTRACT_VERSION
    assert normalized["linked_method_confirmation_item_keys"] == ["ambient_baseline_stabilization_rule"]
    assert normalized["linked_uncertainty_input_keys"] == ["ambient_pressure_baseline"]
    assert normalized["linked_traceability_node_keys"] == ["ambient_environment_reference_chain"]
    assert normalized["reviewer_next_step_template_key"] == "ambient_diagnostic_payload_complete_anchor"


def test_reviewer_fragments_contract_normalizes_aliases_and_labels() -> None:
    assert normalize_fragment_key(
        BLOCKER_FRAGMENT_FAMILY,
        "payload stays partial so reviewer evidence cannot be overstated as phase-complete measurement evidence",
    ) == "partial_payload_not_phase_complete"
    assert normalize_fragment_key(
        GAP_REASON_FRAGMENT_FAMILY,
        "output: conditioning window remains setup evidence until same-route pressure_stable closes full payload-backed output capture",
    ) == "conditioning_window_output_layer_open"
    assert normalize_fragment_key(
        READINESS_IMPACT_FRAGMENT_FAMILY,
        "scope, method confirmation remains open because this phase is still trace-only and not payload-evaluated",
    ) == "trace_only_linkage_open"
    next_step_en = display_fragment_value(
        REVIEWER_NEXT_STEP_FRAGMENT_FAMILY,
        "water_preseal_partial_gap_closeout",
        locale="en_US",
    )
    next_step_zh = display_fragment_value(
        REVIEWER_NEXT_STEP_FRAGMENT_FAMILY,
        "water_preseal_partial_gap_closeout",
        locale="zh_CN",
    )
    assert normalize_fragment_key(REVIEWER_NEXT_STEP_FRAGMENT_FAMILY, next_step_en) == "water_preseal_partial_gap_closeout"
    assert next_step_en
    assert next_step_zh
    assert next_step_zh != next_step_en


def test_taxonomy_contract_preserves_partial_complete_and_payload_backed_phase_differences() -> None:
    ambient_point = _point(10, route="co2", pressure_hpa=None, pressure_mode="ambient_open")
    recovery_point = _point(11, route="", pressure_hpa=1000.0, pressure_mode="")
    water_point = _point(20, route="h2o")
    gas_point = _point(21, route="co2")
    samples = [
        _sample(
            ambient_point,
            seconds=0,
            point_phase="ambient_diagnostic",
            point_tag="synthetic_ambient_diagnostic",
            frame_status="simulation_payload_synthetic",
            pressure_hpa=1001.4,
            co2_signal=None,
            co2_ratio_f=None,
            co2_ratio_raw=None,
        ),
        _sample(
            ambient_point,
            seconds=8,
            point_phase="sample_ready",
            point_tag="synthetic_ambient_sample_ready",
            frame_status="simulation_payload_synthetic",
            pressure_hpa=1001.2,
            co2_signal=None,
            co2_ratio_f=None,
            co2_ratio_raw=None,
        ),
        _sample(
            recovery_point,
            seconds=0,
            point_phase="recovery_retry",
            point_tag="synthetic_recovery_retry",
            frame_status="simulation_payload_synthetic_recovery",
            pressure_hpa=1000.1,
            co2_signal=None,
            co2_ratio_f=None,
            co2_ratio_raw=None,
            frame_usable=False,
        ),
        _sample(
            recovery_point,
            seconds=5,
            point_phase="recovery_retry",
            point_tag="synthetic_recovery_retry",
            frame_status="simulation_payload_synthetic_recovery",
            pressure_hpa=1000.0,
            co2_signal=None,
            co2_ratio_f=None,
            co2_ratio_raw=None,
        ),
        _sample(
            water_point,
            seconds=0,
            point_phase="preseal",
            point_tag="synthetic_water_preseal_partial_payload",
            frame_status="simulation_payload_synthetic_preseal_partial",
            h2o_mmol=None,
            h2o_ratio_f=None,
        ),
        _sample(
            water_point,
            seconds=8,
            point_phase="preseal",
            point_tag="synthetic_water_preseal_partial_payload",
            frame_status="simulation_payload_synthetic_preseal_partial",
            h2o_mmol=None,
            h2o_ratio_f=None,
        ),
        _sample(
            gas_point,
            seconds=0,
            point_phase="pressure_stable",
            point_tag="synthetic_gas_pressure_stable_complete",
            frame_status="simulation_payload_synthetic_pressure_stable",
        ),
        _sample(
            gas_point,
            seconds=8,
            point_phase="pressure_stable",
            point_tag="synthetic_gas_pressure_stable_complete",
            frame_status="simulation_payload_synthetic_pressure_stable",
        ),
    ]

    report = build_measurement_phase_coverage_report(
        run_id="run_taxonomy_contract_phase_differences",
        samples=samples,
        point_summaries=[],
        artifact_paths={
            "measurement_phase_coverage_report": MEASUREMENT_PHASE_COVERAGE_REPORT_FILENAME,
            "measurement_phase_coverage_report_markdown": MEASUREMENT_PHASE_COVERAGE_REPORT_MARKDOWN_FILENAME,
            "multi_source_stability_evidence": MULTI_SOURCE_STABILITY_EVIDENCE_FILENAME,
            "state_transition_evidence": "state_transition_evidence.json",
            "simulation_evidence_sidecar_bundle": SIMULATION_EVIDENCE_SIDECAR_BUNDLE_FILENAME,
        },
        synthetic_trace_provenance={"summary": "measurement_trace_rich_v1 synthetic payload"},
    )

    rows_by_key = {
        str(row.get("phase_route_key") or ""): dict(row)
        for row in list(report.get("raw", {}).get("phase_rows") or [])
    }
    ambient_row = rows_by_key["ambient:ambient_diagnostic"]
    sample_ready_row = rows_by_key["ambient:sample_ready"]
    recovery_row = rows_by_key["system:recovery_retry"]
    water_preseal_row = rows_by_key["water:preseal"]
    gas_pressure_row = rows_by_key["gas:pressure_stable"]

    assert report["raw"]["taxonomy_contract_version"] == TAXONOMY_CONTRACT_VERSION
    assert report["raw"]["reviewer_fragments_contract_version"] == REVIEWER_FRAGMENTS_CONTRACT_VERSION
    assert ambient_row["taxonomy_contract_version"] == TAXONOMY_CONTRACT_VERSION
    assert ambient_row["reviewer_fragments_contract_version"] == REVIEWER_FRAGMENTS_CONTRACT_VERSION
    assert ambient_row["payload_completeness"] == "complete"
    assert ambient_row["gap_classification"] == "ambient_baseline_payload_complete_anchor"
    assert ambient_row["linked_method_confirmation_item_keys"] == [
        "ambient_baseline_stabilization_rule",
        "ambient_diagnostic_decision_threshold",
        "ambient_diagnostic_drift_review",
    ]
    assert sample_ready_row["gap_classification"] == "ambient_sample_ready_payload_complete_anchor"
    assert recovery_row["gap_classification"] == "recovery_retry_payload_complete_anchor"
    assert recovery_row["reviewer_next_step_template_key"] == "recovery_retry_payload_complete_anchor"
    assert recovery_row["reviewer_next_step_fragment_keys"] == ["recovery_retry_payload_complete_anchor"]

    assert water_preseal_row["payload_completeness"] == "partial"
    assert water_preseal_row["gap_classification"] == "conditioning_window_partial_payload"
    assert water_preseal_row["gap_severity"] == "high"
    assert "water_preseal_window_definition" in water_preseal_row["linked_method_confirmation_item_keys"]
    assert "humidity_reference_window" in water_preseal_row["linked_uncertainty_input_keys"]
    assert "humidity_reference_chain" in water_preseal_row["linked_traceability_node_keys"]
    assert water_preseal_row["reviewer_next_step_template_key"] == "water_preseal_partial_gap_closeout"
    assert water_preseal_row["gap_reason_fragment_keys"] == ["conditioning_window_output_layer_open"]
    assert "payload_partial_linkage_open" in list(water_preseal_row.get("readiness_impact_fragment_keys") or [])
    assert "partial_payload_not_phase_complete" in list(water_preseal_row.get("blocker_fragment_keys") or [])
    assert water_preseal_row["reviewer_next_step_fragment_keys"] == ["water_preseal_partial_gap_closeout"]
    assert "analyzer_raw" not in list(water_preseal_row.get("missing_signal_layers") or [])

    assert gas_pressure_row["payload_completeness"] == "complete"
    assert gas_pressure_row["gap_classification"] == "payload_complete_synthetic_reviewer_anchor"
    assert gas_pressure_row["gap_severity"] == "info"
    assert "gas_pressure_stabilization_hold_confirmation" in gas_pressure_row["linked_method_confirmation_item_keys"]
    assert "reference_gas_value" in gas_pressure_row["linked_uncertainty_input_keys"]
    assert "standard_gas_chain" in gas_pressure_row["linked_traceability_node_keys"]
    assert gas_pressure_row["reviewer_next_step_template_key"] == "gas_pressure_stable_payload_complete_anchor"
    assert gas_pressure_row["reviewer_next_step_fragment_keys"] == ["gas_pressure_stable_payload_complete_anchor"]

    linked_gap_rows = list(report["raw"].get("linked_measurement_gaps") or [])
    water_gap = next(item for item in linked_gap_rows if str(item.get("phase_route_key") or "") == "water:preseal")
    assert water_gap["gap_reason_fragment_keys"] == ["conditioning_window_output_layer_open"]
    assert "payload_partial_linkage_open" in list(water_gap.get("readiness_impact_fragment_keys") or [])
    assert "partial_payload_not_phase_complete" in list(water_gap.get("blocker_fragment_keys") or [])
    assert water_gap["reviewer_next_step_fragment_keys"] == ["water_preseal_partial_gap_closeout"]


def test_taxonomy_contract_parity_remains_consistent_across_gateway_and_review_surfaces(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    run_dir = Path(facade.result_store.run_dir)
    rebuild_run(run_dir)
    gateway = ResultsGateway(
        run_dir,
        output_files_provider=facade.service.get_output_files,
    )

    results_payload = gateway.read_results_payload()
    measurement_entry = dict(results_payload["measurement_phase_coverage_report"])
    scope_entry = dict(results_payload["scope_readiness_summary"])
    audit_entry = dict(results_payload["audit_readiness_digest"])

    assert measurement_entry["phase_rows"]
    assert scope_entry["linked_measurement_gaps"]
    assert audit_entry["linked_measurement_gaps"]
    assert scope_entry["taxonomy_contract_version"] == TAXONOMY_CONTRACT_VERSION
    assert audit_entry["taxonomy_contract_version"] == TAXONOMY_CONTRACT_VERSION
    assert scope_entry["reviewer_fragments_contract_version"] == REVIEWER_FRAGMENTS_CONTRACT_VERSION
    assert audit_entry["reviewer_fragments_contract_version"] == REVIEWER_FRAGMENTS_CONTRACT_VERSION
    assert "ambient_baseline_gap" in list(scope_entry.get("linked_gap_classification_keys") or [])
    assert "recovery_retry_test_only_gap" in list(audit_entry.get("linked_gap_classification_keys") or [])
    scope_gap = next(
        item
        for item in list(scope_entry.get("linked_measurement_gaps") or [])
        if str(item.get("route_phase") or "") == "ambient/ambient_diagnostic"
    )
    assert list(scope_gap.get("gap_reason_fragment_keys") or [])
    assert list(scope_gap.get("blocker_fragment_keys") or [])
    assert list(scope_gap.get("reviewer_next_step_fragment_keys") or [])

    measurement_lines = build_measurement_review_digest_lines(measurement_entry)
    scope_lines = build_readiness_review_digest_lines(scope_entry)
    audit_lines = build_readiness_review_digest_lines(audit_entry)

    method_label = display_taxonomy_value(
        METHOD_CONFIRMATION_FAMILY,
        "ambient_baseline_stabilization_rule",
    )
    gap_label = display_taxonomy_value(
        GAP_CLASSIFICATION_FAMILY,
        "ambient_baseline_gap",
    )
    audit_gap_label = display_taxonomy_value(
        GAP_CLASSIFICATION_FAMILY,
        "recovery_retry_test_only_gap",
    )
    blocker_label = display_fragment_value(
        BLOCKER_FRAGMENT_FAMILY,
        "linked_method_items_open",
        params={"items": "Ambient baseline stabilization rule"},
    )
    scope_joined = "\n".join(scope_lines["detail_lines"])
    measurement_joined = "\n".join(measurement_lines["detail_lines"])
    audit_joined = "\n".join(audit_lines["detail_lines"])

    assert method_label in measurement_joined
    assert method_label in scope_joined
    assert gap_label in measurement_joined
    assert gap_label in scope_joined
    assert audit_gap_label in audit_joined
    assert blocker_label in measurement_joined or blocker_label in scope_joined
    assert "ambient_baseline_stabilization_rule" not in measurement_joined
    assert "ambient_baseline_gap" not in scope_joined
    assert "linked method confirmation items remain open" not in scope_joined
    assert any("关联方法确认条目" in line for line in scope_lines["detail_lines"])
    assert any("差距分类" in line for line in measurement_lines["detail_lines"])
