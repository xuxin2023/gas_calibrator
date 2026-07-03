import csv
import json

from gas_calibrator.storage.v1_5_evidence.bundle import bundle_summary, bundle_traceability_summary
from gas_calibrator.storage.v1_5_evidence.pressure_completion_bundle import (
    build_pressure_channel_completion_evidence_bundle,
)
from gas_calibrator.tools.import_v1_5_pressure_channel_completion_package import main as import_main
from gas_calibrator.validation.pressure_channel_completion import write_pressure_channel_completion_report


def _write_csv(path, rows):
    keys = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def _make_completion_package(tmp_path):
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    certificate_pdf = inputs / "FRGsz25038057.pdf"
    certificate_pdf.write_bytes(b"%PDF-1.4\npressure certificate fixture\n")
    pressure_reference = {
        "device_id": "118288",
        "device_name": "Digital pressure gauge",
        "model": "745-23A",
        "manufacturer": "PAROSCIENTIFIC,INC.",
        "certificate_id": "FRGsz25038057",
        "certificate_uncertainty": 0.55,
        "valid_until": "2027-05-21",
        "certificate_hash": "pressure-cert-hash",
        "certificate_file": str(certificate_pdf),
        "unit": "hPa",
    }
    pressure_reference_path = inputs / "pressure_reference.json"
    pressure_reference_path.write_text(json.dumps(pressure_reference, ensure_ascii=False), encoding="utf-8")
    write_summary = inputs / "senco9_write_summary.csv"
    _write_csv(
        write_summary,
        [
            {
                "analyzer_prefix": "ga01",
                "analyzer_device_id": "023",
                "port": "COM35",
                "candidate_offset_kpa": "0.7047",
                "old_senco9_c0": "0.0",
                "target_senco9_c0": "0.7047",
                "target_senco9_values": "[0.7047,1.0,0.0,0.0]",
                "candidate_offset_mode": "add-to-current-c0",
                "candidate_residual_max_abs_hpa": "0.3",
                "status": "written_readback_verified",
                "write_applied": "True",
                "readback_verified": "True",
                "rollback_attempted": "False",
                "identity_before": "023",
                "identity_after": "023",
                "controls_water_or_gas_routes": "False",
                "writes_device_id": "False",
                "writes_senco9": "True",
                "reviewer": "codex_pressure_review",
                "approver": "user_authorized_20260525",
            },
            {
                "analyzer_prefix": "ga02",
                "analyzer_device_id": "030",
                "port": "COM36",
                "candidate_offset_kpa": "-0.2188",
                "old_senco9_c0": "0.0",
                "target_senco9_c0": "-0.2188",
                "target_senco9_values": "[-0.2188,1.0,0.0,0.0]",
                "candidate_offset_mode": "add-to-current-c0",
                "candidate_residual_max_abs_hpa": "0.28",
                "status": "written_readback_verified",
                "write_applied": "True",
                "readback_verified": "True",
                "rollback_attempted": "False",
                "identity_before": "030",
                "identity_after": "030",
                "controls_water_or_gas_routes": "False",
                "writes_device_id": "False",
                "writes_senco9": "True",
                "reviewer": "codex_pressure_review",
                "approver": "user_authorized_20260525",
            },
        ],
    )
    fit_summary = inputs / "pressure_fit_summary.csv"
    _write_csv(
        fit_summary,
        [
            {
                "analyzer_prefix": "ga01",
                "analyzer_device_id": "023",
                "status": "pass",
                "offset_only_offset_kpa": "0.0065",
                "offset_only_residual_max_abs_hpa": "0.226",
                "linear_slope_bias": "-0.0003",
                "valid_pair_count": "96",
                "distinct_pressure_points": "8",
                "reference_span_hpa": "599.4",
            },
            {
                "analyzer_prefix": "ga02",
                "analyzer_device_id": "030",
                "status": "pass",
                "offset_only_offset_kpa": "0.0050",
                "offset_only_residual_max_abs_hpa": "0.289",
                "linear_slope_bias": "-0.0006",
                "valid_pair_count": "96",
                "distinct_pressure_points": "8",
                "reference_span_hpa": "599.4",
            },
        ],
    )
    traceability = inputs / "pressure_reference_traceability.csv"
    _write_csv(
        traceability,
        [
            {
                "status": "pass",
                "validation_level": "formal_pressure_validation",
                "device_id": "118288",
                "certificate_id": "FRGsz25038057",
                "certificate_hash": "pressure-cert-hash",
                "valid_until": "2027-05-21",
                "uncertainty_hpa": "0.55",
            }
        ],
    )
    old_getco = {
        "023": {
            "analyzer_prefix": "ga01",
            "port": "COM35",
            "GETCO9_before": [0.0, 1.0, 0.0, 0.0],
            "candidate_values": [0.7047, 1.0, 0.0, 0.0],
            "readback": [0.7047, 1.0, 0.0, 0.0],
            "candidate_offset_mode": "add-to-current-c0",
        },
        "030": {
            "analyzer_prefix": "ga02",
            "port": "COM36",
            "GETCO9_before": [0.0, 1.0, 0.0, 0.0],
            "candidate_values": [-0.2188, 1.0, 0.0, 0.0],
            "readback": [-0.2188, 1.0, 0.0, 0.0],
            "candidate_offset_mode": "add-to-current-c0",
        },
    }
    old_getco_path = inputs / "old_getco9_snapshot.json"
    old_getco_path.write_text(json.dumps(old_getco, ensure_ascii=False), encoding="utf-8")
    completion_dir = tmp_path / "pressure_channel_completion_20260525_2ch"
    write_pressure_channel_completion_report(
        output_dir=completion_dir,
        senco9_write_summary_path=write_summary,
        post_write_fit_summary_path=fit_summary,
        pressure_reference_path=pressure_reference_path,
        pressure_reference_traceability_path=traceability,
        old_getco_snapshot_path=old_getco_path,
        today="2026-05-25",
    )
    return completion_dir


def test_pressure_completion_bundle_indexes_devices_writes_and_traceability(tmp_path):
    completion_dir = _make_completion_package(tmp_path)

    bundle = build_pressure_channel_completion_evidence_bundle(completion_dir=completion_dir)
    summary = bundle_summary(bundle)
    traceability = bundle_traceability_summary(bundle)
    tables = bundle["tables"]
    devices = {row["serial_number"]: row for row in tables["devices"] if row["device_type"] == "gas_analyzer"}
    roles = {row["artifact_role"] for row in tables["sample_files"]}

    assert summary["evidence_status"] == "ready_for_open_flow_sampling"
    assert summary["package_status"] == "ready_for_open_flow_main_calibration"
    assert set(devices) == {"023", "030"}
    assert devices["023"]["metadata"]["analyzer_prefix"] == "ga01"
    assert devices["023"]["metadata"]["acquisition_channel_only"] is True
    assert roles >= {
        "pressure_channel_completion_summary",
        "pressure_channel_device_readiness",
        "pressure_reference_certificate_pdf",
        "pressure_senco9_write_summary",
        "pressure_senco9_old_getco_snapshot",
    }
    assert tables["standard_gases"] == []
    assert tables["reference_certificates"][0]["certificate_id"] == "FRGsz25038057"
    assert len(tables["coefficient_snapshots"]) == 2
    assert len(tables["coefficient_write_events"]) == 2
    assert {row["status"] for row in tables["coefficient_write_events"]} == {"written_readback_verified"}
    assert all(row["metadata"]["controls_water_or_gas_routes"] is False for row in tables["coefficient_write_events"])
    assert all(row["metadata"]["writes_device_id"] is False for row in tables["coefficient_write_events"])
    assert all(row["status"] == "pass" for row in tables["evidence_integrity_checks"])
    assert traceability["physical_boundaries"]["writes_coefficients"] is True
    assert traceability["traceability_checks"]["no_coefficient_write_attempted"] is False


def test_pressure_completion_import_cli_dry_run_writes_bundle_json(tmp_path):
    completion_dir = _make_completion_package(tmp_path)
    bundle_path = tmp_path / "bundle.json"
    summary_path = tmp_path / "summary.json"

    rc = import_main(
        [
            "--completion-dir",
            str(completion_dir),
            "--output-json",
            str(bundle_path),
            "--summary-json",
            str(summary_path),
            "--dry-run",
        ]
    )

    assert rc == 0
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert bundle["schema"] == "v1_5_evidence_registry"
    assert summary["database_imported"] is False
    assert summary["table_counts"]["coefficient_write_events"] == 2
