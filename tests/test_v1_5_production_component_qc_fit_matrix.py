import csv
import hashlib
import json
from pathlib import Path

import pytest

from gas_calibrator.tools.export_v1_5_candidate_model_selection_review import (
    MODEL_FAMILIES as EXISTING_MODEL_FAMILIES,
)
from gas_calibrator.tools.export_v1_5_production_component_qc_fit_matrix import main
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


def _seed_packet(tmp_path, *, component="co2", purge_below_minimum=False):
    point = tmp_path / f"{component}_point"
    rows = []
    for index in range(10):
        row = {
            "sample_ts": f"2026-07-13T00:00:{index:02d}.000",
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
    else:
        sidecar = point / "formal_h2o_open_flow_sidecar_metadata.json"
        route_timing = None
        humidity = point / "h2o_humidity_reference_review.json"
    _write_csv(samples, rows)
    _write_csv(frame_qc, [{"Analyzer": "GA01", "ValidFrames": 10}])
    _write_json(
        runtime,
        {
            "workflow": {
                "stability": {
                    "sensor": {f"{component}_ratio_f_preseal_min_samples": 10}
                }
            }
        },
    )
    _write_json(
        sidecar,
        {
            "route_open_until_sample_end": True,
            "co2_source_ppm": 0 if component == "co2" else None,
            "certificate_h2o_mmol": 8.2 if component == "h2o" else None,
        },
    )
    if route_timing:
        _write_json(route_timing, {"route_opened": True, "sampling_before_route_close": True})
    if humidity:
        _write_json(
            humidity,
            {"humidity_reference_check": {"status": "pass", "hard_block": False}},
        )

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
    assert len(rows["ga01"]["contract_sha256"]) == 64
    assert all(row["fit_executed"] is False for row in model["fit_strategy_matrix"])
    assert all(
        "continuous_mature_route_attestation_missing" in row["blocker_codes"]
        for row in model["fit_strategy_matrix"]
    )
    assert not (point / "formal_open_flow_data_quality_by_analyzer.csv").exists()


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
