import csv
import json

from gas_calibrator.validation.co2_senco_algorithm_audit import (
    build_co2_senco_algorithm_audit_tables,
    direct_ratio_model_prediction,
    write_co2_senco_algorithm_audit,
)


def _write_csv(path, rows):
    header = []
    for row in rows:
        for key in row:
            if key not in header:
                header.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def _getco_snapshot(path, device_id, rows):
    _write_csv(
        path,
        [
            {
                "analyzer_device_id": device_id,
                "getco_group": group,
                "getco_target": device_id,
                "coefficient_valid": "True",
                "coefficient_values_json": json.dumps(values),
            }
            for group, values in rows
        ],
    )


def _candidate_dir(tmp_path):
    root = tmp_path / "candidate"
    _write_csv(
        root / "candidate_coefficients.csv",
        [
            {"component": "co2", "analyzer_device_id": "030", "term": "intercept", "coefficient": "1.0"},
            {"component": "co2", "analyzer_device_id": "030", "term": "R", "coefficient": "2.0"},
            {"component": "co2", "analyzer_device_id": "030", "term": "R2", "coefficient": "3.0"},
            {"component": "co2", "analyzer_device_id": "030", "term": "R3", "coefficient": "4.0"},
            {"component": "co2", "analyzer_device_id": "030", "term": "T", "coefficient": "5.0"},
            {"component": "co2", "analyzer_device_id": "030", "term": "T2", "coefficient": "0.0"},
            {"component": "co2", "analyzer_device_id": "030", "term": "RT", "coefficient": "6.0"},
        ],
    )
    return root


def test_direct_ratio_model_prediction_uses_kelvin_temperature():
    prediction = direct_ratio_model_prediction(
        {"intercept": 1.0, "R": 2.0, "R2": 3.0, "R3": 4.0, "T": 5.0, "T2": 0.0, "RT": 6.0},
        ratio=2.0,
        temperature_c=20.0,
    )

    assert prediction == 1.0 + 2.0 * 2.0 + 3.0 * 4.0 + 4.0 * 8.0 + 5.0 * 293.15 + 6.0 * 2.0 * 293.15


def test_algorithm_audit_flags_payload_truncation_and_formula_block(tmp_path):
    candidate_dir = _candidate_dir(tmp_path)
    verification = tmp_path / "verification.csv"
    write_dir = tmp_path / "write"
    _write_csv(
        verification,
        [
            {
                "device_id": "030",
                "certificate_co2_ppm": "700.0",
                "co2_mean_ppm": "3000.0",
                "co2_ratio_f_mean": "2.0",
                "chamber_temp_mean_c": "20.0",
                "pressure_mean_kpa": "101.3",
                "h2o_mean_mmol_mol": "1.0",
            }
        ],
    )
    _write_csv(
        write_dir / "co2_senco13_pair_write_summary.csv",
        [
            {
                "analyzer_device_id": "030",
                "target_senco1_values": json.dumps([1, 2, 3, 4]),
                "target_senco3_values": json.dumps([5, 6, 7, 8]),
            }
        ],
    )

    tables = build_co2_senco_algorithm_audit_tables(
        candidate_dir=candidate_dir,
        verification_summary_csv=verification,
        write_dir=write_dir,
    )

    row = tables["co2_senco_algorithm_audit_devices"][0]
    assert row["payload_status"] == "fail_payload_length_senco1_4_senco3_4"
    assert row["likely_root_cause"] == "writer_payload_or_device_group_width_incompatible"
    assert row["formula_contract_status"] == "senco13_raw_ppm_then_h2o_dry_basis_final_ppm"
    contract = {row["topic"]: row for row in tables["co2_senco_algorithm_contract"]}
    assert (
        contract["manual_co2_coefficient_scope"]["status"]
        == "senco1_senco3_main_co2_chain_senco5_not_authorized_by_current_open_flow_fit"
    )


def test_algorithm_audit_uses_getco_snapshot_to_block_output_chain_faults(tmp_path):
    candidate_dir = _candidate_dir(tmp_path)
    verification = tmp_path / "verification.csv"
    write_dir = tmp_path / "write"
    getco = tmp_path / "getco_snapshot.csv"
    _write_csv(
        verification,
        [
            {
                "device_id": "030",
                "certificate_co2_ppm": "5032.55",
                "co2_mean_ppm": "5161.59",
                "co2_ratio_f_mean": "2.0",
                "chamber_temp_mean_c": "20.0",
                "pressure_mean_kpa": "101.3",
                "h2o_mean_mmol_mol": "25.0",
            }
        ],
    )
    _write_csv(
        write_dir / "co2_senco13_pair_write_summary.csv",
        [
            {
                "analyzer_device_id": "030",
                "target_senco1_values": json.dumps([1, 2, 3, 4, 0, 0]),
                "target_senco3_values": json.dumps([5, 0, 6, 0, 0, 0]),
            }
        ],
    )
    _getco_snapshot(
        getco,
        "030",
        [
            (5, [0.0, 1.0]),
            (6, [24.4, 1.0]),
            (7, [0.0, 1.0, 0.0, 0.0]),
            (8, [0.0, 1.0, 0.0, 0.0]),
            (9, [-1.0, 1.0, 0.0, 0.0]),
        ],
    )

    tables = build_co2_senco_algorithm_audit_tables(
        candidate_dir=candidate_dir,
        verification_summary_csv=verification,
        write_dir=write_dir,
        getco_snapshot_csv=getco,
    )

    row = tables["co2_senco_algorithm_audit_devices"][0]
    assert row["payload_status"] == "pass_6_value_payload"
    assert row["getco5_status"] == "neutral"
    assert row["getco6_status"] == "non_neutral"
    assert row["firmware_model_agreement_status"] == "firmware_model_reproduced_but_acceptance_failed"
    assert row["likely_root_cause"] == "h2o_channel_bias_explains_co2_final_output_shift"
    assert row["output_chain_gate"] == "blocked_h2o_channel_before_co2_acceptance"
    assert row["output_chain_write_blocker"] is True
    summary = tables["co2_senco_algorithm_audit_summary"][0]
    assert summary["audit_status"] == "blocked_output_chain_isolation_required"
    assert summary["output_chain_blocked_count"] == 1
    assert summary["firmware_model_reproduced_count"] == 1


def test_algorithm_audit_writes_artifacts(tmp_path):
    candidate_dir = _candidate_dir(tmp_path)
    verification = tmp_path / "verification.csv"
    _write_csv(
        verification,
        [
            {
                "device_id": "030",
                "certificate_co2_ppm": "700.0",
                "co2_mean_ppm": "701.0",
                "co2_ratio_f_mean": "2.0",
                "chamber_temp_mean_c": "20.0",
            }
        ],
    )

    outputs = write_co2_senco_algorithm_audit(
        candidate_dir=candidate_dir,
        verification_summary_csv=verification,
        output_dir=tmp_path / "audit",
    )

    assert outputs["markdown"].exists()
    assert outputs["co2_senco_algorithm_audit_devices_csv"].exists()
