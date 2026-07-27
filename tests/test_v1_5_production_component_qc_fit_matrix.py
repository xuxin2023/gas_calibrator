import csv
import hashlib
import json
from pathlib import Path

import pytest

from gas_calibrator.tools.export_v1_5_candidate_model_selection_review import (
    MODEL_FAMILIES as EXISTING_MODEL_FAMILIES,
)
from gas_calibrator.tools.export_v1_5_production_component_qc_fit_matrix import main
from gas_calibrator.tools.run_v1_5_formal_open_flow_sampling import (
    _write_formal_evidence_bundle_manifest,
)
from gas_calibrator.validation.v1_5_component_qc_generator_contract import (
    FORMAL_REFERENCE_SOURCE_RECORD_SCHEMA,
)
from gas_calibrator.validation.v1_5_production_component_qc_fit_matrix import (
    MODEL_FAMILIES,
    build_v1_5_production_component_qc_fit_matrix,
    write_v1_5_production_component_qc_fit_matrix,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
CONTRACT = REPO_ROOT / "configs" / "v1_5_component_qc_generator_contract.json"


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _seed_packet(
    tmp_path,
    *,
    component="co2",
    purge_below_minimum=False,
    sample_times=None,
    sampling_interval_s=1.0,
    strict_bundle=False,
):
    point = tmp_path / f"{component}_point"
    rows = []
    timestamps = sample_times or [float(index) for index in range(10)]
    for index in range(10):
        seconds = float(timestamps[index])
        row = {
            "sample_ts": f"2026-07-13T00:00:{seconds:06.3f}",
            "run_id": point.name,
            "co2_ppm_target": "0" if component == "co2" else "",
            "h2o_mmol_target": "" if component == "co2" else "8.2",
            "point_quality_blocked": "False",
            "ga01_analyzer_device_id": "001",
            "ga01_frame_usable": "True",
            "ga01_co2_ratio_f": f"{1.2 + index * 0.00002:.6f}",
            "ga01_h2o_ratio_f": f"{0.8 + index * 0.00003:.6f}",
            "ga02_analyzer_device_id": "002",
            "ga02_frame_usable": "True",
            "ga02_co2_ratio_f": f"{1.3 + index * 0.0002:.6f}",
            "ga02_h2o_ratio_f": f"{0.9 + index * 0.0002:.6f}",
        }
        rows.append(row)
    samples = point / "samples_machine_readable.csv"
    frame_qc = point / "frame_quality_summary.csv"
    runtime = point / "runtime_config_snapshot.json"
    if component == "co2":
        sidecar = point / "formal_open_flow_sidecar_metadata.json"
        route_timing = point / "formal_open_flow_route_timing.json"
        humidity = None
        hgen_flow = None
    else:
        sidecar = point / "formal_h2o_open_flow_sidecar_metadata.json"
        route_timing = None
        humidity = point / "h2o_humidity_reference_review.json"
        hgen_flow = point / "formal_h2o_open_flow_hgen_flow_set.json"
    point_timing = point / "point_timing_summary.csv"
    _write_csv(samples, rows)
    _write_csv(frame_qc, [{"Analyzer": "GA01", "ValidFrames": 10}])
    runtime_payload = {
        "workflow": {
            "sampling": (
                {
                    "interval_s": sampling_interval_s,
                    f"{component}_interval_s": sampling_interval_s,
                }
                if sampling_interval_s is not None
                else {}
            ),
            "stability": {
                "sensor": {f"{component}_ratio_f_preseal_min_samples": 10}
            },
        },
        "metadata": (
            {
                "run_id": point.name,
                "evidence_identity_contract": (
                    "immutable_claim_runtime_run_id_and_sha256_bundle"
                ),
            }
            if strict_bundle
            else {}
        ),
    }
    _write_json(runtime, runtime_payload)
    _write_json(
        sidecar,
        {
            "run_id": point.name,
            "route_open_until_sample_end": True,
            "co2_source_ppm": 0 if component == "co2" else None,
            "certificate_h2o_mmol": 8.2 if component == "h2o" else None,
        },
    )
    if route_timing:
        _write_json(
            route_timing,
            {
                "run_id": point.name,
                "route_opened": True,
                "sampling_before_route_close": True,
            },
        )
    if humidity:
        _write_json(
            humidity,
            {"humidity_reference_check": {"status": "pass", "hard_block": False}},
        )
    if hgen_flow:
        _write_json(hgen_flow, {"flow_control_role": "evidence_only"})
    _write_csv(point_timing, [{"point_phase": component, "sampling_end_ts": "2026-07-13T00:00:10"}])
    if strict_bundle:
        _write_json(
            point / "run_directory_claim.json",
            {
                "schema_version": "immutable_run_directory_claim_v1",
                "run_id": point.name,
                "policy": "create_once_no_overwrite",
            },
        )
        if component == "co2":
            reference_payload = {
                "schema_version": FORMAL_REFERENCE_SOURCE_RECORD_SCHEMA,
                "run_id": point.name,
                "route_kind": "co2",
                "reference_source_status": "pass",
                "selected_asset": {
                    "asset_id": "co2-dry-air-zero-test",
                    "nominal_co2_ppm": 0.0,
                    "reference_value_source": "operator_confirmed_previous_calibration",
                    "co2_value_directly_certified": False,
                },
                "documents_verified": [{"page_role": "certificate", "verified": True}],
                "not_real_acceptance_evidence": True,
            }
        else:
            reference_payload = {
                "schema_version": FORMAL_REFERENCE_SOURCE_RECORD_SCHEMA,
                "run_id": point.name,
                "route_kind": "h2o",
                "reference_source_status": "pass",
                "reference_asset_id": "dynamic_h2o_dewpoint_pressure_reference",
                "reference_value_source": "measured_dewpoint_plus_measured_pressure",
                "h2o_concentration_reference": {
                    "primary_quantities": [
                        "actual_dewpoint_meter_measurement",
                        "actual_pressure_measurement_bound_in_samples",
                    ]
                },
                "humidity_generator_flow": {
                    "role": "source_state_evidence_only"
                },
                "route_flow_evidence": {
                    "role": "route_and_process_evidence_only",
                    "source": "dewpoint_meter_output",
                    "observed_flow_lpm": 1.58,
                },
                "not_real_acceptance_evidence": True,
            }
        _write_json(point / "formal_reference_source_record.json", reference_payload)

    artifacts = {
        "samples": samples,
        "frame_qc": frame_qc,
        "runtime_config": runtime,
        "sidecar": sidecar,
    }
    if route_timing:
        artifacts["route_timing"] = route_timing
    if humidity:
        artifacts["humidity_reference_review"] = humidity
    if hgen_flow:
        artifacts["hgen_flow_set"] = hgen_flow
    artifacts["point_timing_summary"] = point_timing
    if strict_bundle:
        _write_formal_evidence_bundle_manifest(
            point,
            run_id=point.name,
            route_kind=component,
        )
    checks = [
        {
            "point_dir": str(point),
            "role": role,
            "path": str(path),
            "recorded_sha256": _sha(path),
            "status": "pass",
        }
        for role, path in artifacts.items()
    ]
    candidate = {
        "source_role": "test_point",
        "route_kind": component,
        "point_name": point.name,
        "point_dir": str(point),
        "preflight_ready": True,
        "purge_below_declared_minimum": purge_below_minimum,
        "formal_fit_allowed": False,
    }
    preflight = tmp_path / "preflight.json"
    _write_json(
        preflight,
        {
            "overall_status": "ready_for_historical_component_qc_generator_preflight_manual_review",
            "candidates": [candidate],
            "artifact_checks": checks,
        },
    )
    catalog = tmp_path / "catalog.json"
    _write_json(
        catalog,
        {
            "accepted_composite_manifest_rows": (
                [{"point_dir": str(point)}] if component == "co2" else []
            ),
            "continuous_route_attestation_allowed": False,
            "historical_fit_allowed": False,
        },
    )
    discovery = tmp_path / "discovery.json"
    _write_json(discovery, {"overall_status": "blocked_no_complete_mature_root"})
    return preflight, catalog, discovery, point


def _build(tmp_path, **kwargs):
    preflight, catalog, discovery, point = _seed_packet(tmp_path, **kwargs)
    model = build_v1_5_production_component_qc_fit_matrix(
        preflight_json=preflight,
        contract_json=CONTRACT,
        legacy_catalog_json=catalog,
        mature_root_discovery_json=discovery,
    )
    return model, point


def _build_packet(preflight, catalog, discovery):
    return build_v1_5_production_component_qc_fit_matrix(
        preflight_json=preflight,
        contract_json=CONTRACT,
        legacy_catalog_json=catalog,
        mature_root_discovery_json=discovery,
    )


def _refresh_preflight_hash(preflight, *, role, path):
    payload = json.loads(preflight.read_text(encoding="utf-8"))
    row = next(item for item in payload["artifact_checks"] if item["role"] == role)
    row["recorded_sha256"] = _sha(path)
    _write_json(preflight, payload)


def test_model_families_remain_identical_to_existing_0613_review():
    assert MODEL_FAMILIES == EXISTING_MODEL_FAMILIES


def test_component_qc_is_per_analyzer_and_fit_remains_blocked(tmp_path):
    model, point = _build(tmp_path)

    assert model["production_component_qc_evaluation_complete"] is True
    assert model["production_component_qc_evaluator_available"] is True
    assert model["canonical_0613_strategy_matrix_available"] is True
    assert model["no_write_fit_evaluation_allowed"] is False
    assert model["production_fit_allowed"] is False
    rows = {row["prefix"]: row for row in model["analyzer_qc"]}
    assert rows["ga01"]["grade"] == "A_calibration_eligible"
    assert rows["ga02"]["grade"] == "C_reject"
    assert rows["ga01"]["sample_can_enter_calibration_fit"] is True
    assert rows["ga02"]["sample_can_enter_calibration_fit"] is False
    assert rows["ga01"]["timestamps_strictly_increasing"] is True
    assert rows["ga01"]["actual_window_duration_s"] == pytest.approx(9.0)
    assert rows["ga01"]["minimum_window_duration_s"] == pytest.approx(8.1)
    assert rows["ga01"]["cadence_warning"] is False
    assert rows["ga01"]["evidence_identity_status"] == "pass"
    assert rows["ga01"]["evidence_identity_mode"] == "legacy_run_id_consensus"
    assert rows["ga01"]["evidence_bundle_manifest_verified"] is False
    assert rows["ga01"]["reference_source_status"] == "legacy_not_recorded"
    assert rows["ga01"]["reference_source_record_present"] is False
    assert rows["ga01"]["reference_source_record_valid"] is False
    assert "reference_source_invalid" not in model["points"][0]["hard_blockers"]
    assert len(rows["ga01"]["evidence_bundle_sha256"]) == 64
    assert len(rows["ga01"]["contract_sha256"]) == 64
    assert all(row["fit_executed"] is False for row in model["fit_strategy_matrix"])
    assert all(
        "continuous_mature_route_attestation_missing" in row["blocker_codes"]
        for row in model["fit_strategy_matrix"]
    )
    assert not (point / "formal_open_flow_data_quality_by_analyzer.csv").exists()


def test_strict_claim_runtime_and_bundle_identity_is_verified(tmp_path):
    model, _ = _build(tmp_path, strict_bundle=True)
    rows = {row["prefix"]: row for row in model["analyzer_qc"]}

    assert rows["ga01"]["evidence_identity_status"] == "pass"
    assert rows["ga01"]["evidence_identity_mode"] == "strict_claim_runtime_bundle"
    assert rows["ga01"]["evidence_bundle_manifest_verified"] is True
    assert rows["ga01"]["evidence_bundle_member_count"] == 8
    assert rows["ga01"]["grade"] == "A_calibration_eligible"
    assert rows["ga01"]["reference_source_record_valid"] is True


def test_strict_bundle_rejects_reference_record_replacement(tmp_path):
    preflight, catalog, discovery, point = _seed_packet(tmp_path, strict_bundle=True)
    reference = point / "formal_reference_source_record.json"
    payload = json.loads(reference.read_text(encoding="utf-8"))
    payload["selected_asset"]["asset_id"] = "cross_run_replacement"
    _write_json(reference, payload)

    model = _build_packet(preflight, catalog, discovery)

    assert {row["grade"] for row in model["analyzer_qc"]} == {"C_reject"}
    assert all(row["evidence_bundle_manifest_verified"] is False for row in model["analyzer_qc"])
    assert all(
        "bundle_manifest_sha_mismatch:reference_source"
        in row["evidence_identity_reason_codes"]
        for row in model["analyzer_qc"]
    )


def test_cross_run_sidecar_is_rejected_even_when_inventory_hash_is_refreshed(tmp_path):
    preflight, catalog, discovery, point = _seed_packet(tmp_path)
    sidecar = point / "formal_open_flow_sidecar_metadata.json"
    payload = json.loads(sidecar.read_text(encoding="utf-8"))
    payload["run_id"] = "different_run"
    _write_json(sidecar, payload)
    _refresh_preflight_hash(preflight, role="sidecar", path=sidecar)

    model = _build_packet(preflight, catalog, discovery)

    assert {row["grade"] for row in model["analyzer_qc"]} == {"C_reject"}
    assert all(row["evidence_identity_status"] == "fail" for row in model["analyzer_qc"])
    assert all(
        "sidecar_run_id_mismatch" in row["evidence_identity_reason_codes"]
        for row in model["analyzer_qc"]
    )


def test_mixed_sample_run_ids_are_rejected(tmp_path):
    preflight, catalog, discovery, point = _seed_packet(tmp_path)
    samples = point / "samples_machine_readable.csv"
    with samples.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    rows[-1]["run_id"] = "different_run"
    _write_csv(samples, rows)
    _refresh_preflight_hash(preflight, role="samples", path=samples)

    model = _build_packet(preflight, catalog, discovery)

    assert {row["grade"] for row in model["analyzer_qc"]} == {"C_reject"}
    assert all(
        "sample_run_id_missing_or_changes" in row["evidence_identity_reason_codes"]
        for row in model["analyzer_qc"]
    )


def test_strict_bundle_detects_identityless_h2o_artifact_replacement(tmp_path):
    preflight, catalog, discovery, point = _seed_packet(
        tmp_path,
        component="h2o",
        strict_bundle=True,
    )
    hgen_flow = point / "formal_h2o_open_flow_hgen_flow_set.json"
    _write_json(hgen_flow, {"flow_control_role": "cross_run_replacement"})
    _refresh_preflight_hash(preflight, role="hgen_flow_set", path=hgen_flow)

    model = _build_packet(preflight, catalog, discovery)

    assert {row["grade"] for row in model["analyzer_qc"]} == {"C_reject"}
    assert all(row["evidence_bundle_manifest_verified"] is False for row in model["analyzer_qc"])
    assert all(
        "bundle_manifest_sha_mismatch:hgen_flow_set"
        in row["evidence_identity_reason_codes"]
        for row in model["analyzer_qc"]
    )


@pytest.mark.parametrize(
    ("sample_times", "reason"),
    [
        ([index * 0.1 for index in range(10)], "sample_window_duration_below_minimum"),
        ([0, 1, 2, 3, 4, 5, 6, 6, 8, 9], "timestamps_not_strictly_increasing"),
    ],
)
def test_temporal_window_hard_failures_reject_all_analyzers(
    tmp_path, sample_times, reason
):
    model, _ = _build(tmp_path, sample_times=sample_times)

    assert {row["grade"] for row in model["analyzer_qc"]} == {"C_reject"}
    assert all(row["temporal_window_complete"] is False for row in model["analyzer_qc"])
    assert all(f"temporal:{reason}" in row["reason"] for row in model["analyzer_qc"])


def test_irregular_but_long_enough_cadence_caps_only_eligible_analyzer_at_b(tmp_path):
    model, _ = _build(
        tmp_path,
        sample_times=[0, 1, 2, 3, 4, 5, 6, 7, 8, 10.1],
    )
    rows = {row["prefix"]: row for row in model["analyzer_qc"]}

    assert rows["ga01"]["temporal_window_complete"] is True
    assert rows["ga01"]["cadence_warning"] is True
    assert rows["ga01"]["maximum_observed_interval_s"] == pytest.approx(2.1)
    assert rows["ga01"]["grade"] == "B_diagnostic_model_only"
    assert "cadence_warning_grade_capped_at_b" in rows["ga01"]["reason"]
    assert rows["ga02"]["grade"] == "C_reject"


def test_missing_runtime_sampling_interval_rejects_window(tmp_path):
    model, _ = _build(tmp_path, sampling_interval_s=None)

    assert {row["grade"] for row in model["analyzer_qc"]} == {"C_reject"}
    assert all(
        "temporal:expected_sample_interval_missing_or_invalid" in row["reason"]
        for row in model["analyzer_qc"]
    )


def test_co2_zero_and_h2o_wet_roles_are_not_collapsed(tmp_path):
    co2, _ = _build(tmp_path / "co2", component="co2")
    h2o, _ = _build(tmp_path / "h2o", component="h2o")

    assert {row["anchor_role"] for row in co2["fit_input_decisions"]} == {
        "co2_zero_gas_low_concentration_anchor"
    }
    assert {row["anchor_role"] for row in h2o["fit_input_decisions"]} == {
        "h2o_wet_calibration_point"
    }
    assert all(
        "h2o_dry_gas_anchor_missing_from_current_point_packet" in row["blocker_codes"]
        for row in h2o["fit_strategy_matrix"]
    )


def test_declared_h2o_purge_shortfall_is_a_point_wide_reject(tmp_path):
    model, _ = _build(tmp_path, component="h2o", purge_below_minimum=True)

    assert {row["grade"] for row in model["analyzer_qc"]} == {"C_reject"}
    assert all(
        "point_wide_hard_blocker:route_specific_physical_reference_invalid"
        in row["reason"]
        for row in model["analyzer_qc"]
    )


def test_changed_source_artifact_blocks_evaluation(tmp_path):
    preflight, catalog, discovery, point = _seed_packet(tmp_path)
    with (point / "samples_machine_readable.csv").open("a", encoding="utf-8") as handle:
        handle.write("\n")

    model = build_v1_5_production_component_qc_fit_matrix(
        preflight_json=preflight,
        contract_json=CONTRACT,
        legacy_catalog_json=catalog,
        mature_root_discovery_json=discovery,
    )

    assert model["production_fit_allowed"] is False
    assert any("source_artifact_sha_mismatch:samples" in row["evaluation_reasons"] for row in model["points"])
    assert model["grade_counts"] == {}


def test_writer_is_confined_to_review_directory(tmp_path):
    model, _ = _build(tmp_path / "packet")
    with pytest.raises(ValueError, match="output_dir_must_be"):
        write_v1_5_production_component_qc_fit_matrix(model, tmp_path / "elsewhere")


def test_cli_writes_central_review_only(tmp_path):
    preflight, catalog, discovery, point = _seed_packet(tmp_path / "packet")
    output = (
        tmp_path
        / "docs"
        / "v1_5_flow_contract"
        / "production_component_qc_fit_matrix"
    )
    rc = main(
        [
            "--preflight-json",
            str(preflight),
            "--contract-json",
            str(CONTRACT),
            "--legacy-catalog-json",
            str(catalog),
            "--mature-root-discovery-json",
            str(discovery),
            "--output-dir",
            str(output),
        ]
    )

    assert rc == 0
    payload = json.loads(
        (output / "v1_5_production_component_qc_fit_matrix.json").read_text(
            encoding="utf-8"
        )
    )
    assert payload["opens_com_ports"] is False
    assert payload["writes_coefficients"] is False
    assert payload["connects_postgresql"] is False
    assert payload["not_real_acceptance_evidence"] is True
    assert not (point / "formal_open_flow_data_quality_by_analyzer.csv").exists()
