import csv
import json

from gas_calibrator.tools.export_v1_5_pressure_channel_completion import main as completion_main
from gas_calibrator.validation.pressure_channel_completion import build_pressure_channel_completion_tables


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


def _read_csv(path):
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_inputs(tmp_path, *, expired=False, residual="0.22"):
    write_summary = tmp_path / "write" / "senco9_write_summary.csv"
    fit_summary = tmp_path / "fit" / "pressure_fit_summary.csv"
    traceability = tmp_path / "fit" / "pressure_reference_traceability.csv"
    reference = tmp_path / "pressure_reference.json"
    old_getco = tmp_path / "old_getco9_snapshot.json"
    _write_csv(
        write_summary,
        [
            {
                "analyzer_prefix": "ga01",
                "analyzer_device_id": "023",
                "old_senco9_c0": "0.0",
                "candidate_offset_kpa": "0.704736",
                "target_senco9_c0": "0.704736",
                "status": "written_readback_verified",
                "write_applied": "True",
                "readback_verified": "True",
                "identity_before": "023",
                "identity_after": "023",
            }
        ],
    )
    _write_csv(
        fit_summary,
        [
            {
                "analyzer_prefix": "ga01",
                "analyzer_device_id": "023",
                "status": "pass",
                "valid_pair_count": "96",
                "distinct_pressure_points": "8",
                "reference_span_hpa": "599.4",
                "offset_only_offset_kpa": "0.0065",
                "offset_only_residual_max_abs_hpa": residual,
                "linear_slope_bias": "-0.0003",
            }
        ],
    )
    _write_csv(
        traceability,
        [
            {
                "analyzer_prefix": "ga01",
                "status": "pass" if not expired else "fail",
                "validation_level": "formal_pressure_validation" if not expired else "engineering_diagnostic",
                "reasons": "[]" if not expired else '["certificate_expired"]',
                "device_id": "118288",
                "certificate_id": "FRGsz25038057",
                "certificate_hash": "hash-a",
                "valid_until": "2027-05-21" if not expired else "2026-05-21",
                "uncertainty_hpa": "0.55",
            }
        ],
    )
    reference.write_text(
        json.dumps(
            {
                "device_id": "118288",
                "certificate_id": "FRGsz25038057",
                "certificate_uncertainty": 0.55,
                "valid_until": "2027-05-21" if not expired else "2026-05-21",
                "certificate_hash": "hash-a",
            }
        ),
        encoding="utf-8",
    )
    old_getco.write_text(json.dumps({"023": {"GETCO9_before": [0.0, 1.0, 0.0, 0.0]}}), encoding="utf-8")
    return write_summary, fit_summary, traceability, reference, old_getco


def _append_csv(path, rows):
    existing = _read_csv(path)
    _write_csv(path, existing + rows)


def test_pressure_channel_completion_marks_ready_when_all_evidence_passes(tmp_path):
    write_summary, fit_summary, traceability, reference, old_getco = _write_inputs(tmp_path)

    tables = build_pressure_channel_completion_tables(
        senco9_write_summary_path=write_summary,
        post_write_fit_summary_path=fit_summary,
        pressure_reference_path=reference,
        pressure_reference_traceability_path=traceability,
        old_getco_snapshot_path=old_getco,
        today="2026-05-25",
    )

    summary = tables["pressure_channel_completion_summary"][0]
    device = tables["pressure_channel_device_readiness"][0]
    assert summary["overall_status"] == "ready_for_open_flow_main_calibration"
    assert summary["ready_for_open_flow_sampling"] is True
    assert summary["controls_water_or_gas_routes"] is False
    assert device["can_enter_open_flow_main_calibration"] is True
    assert device["can_write_co2_h2o_coefficients"] is False
    assert device["not_co2_h2o_fit_evidence"] is True


def test_pressure_channel_completion_can_scope_selected_devices_and_record_limitations(tmp_path):
    write_summary, fit_summary, traceability, reference, old_getco = _write_inputs(tmp_path)
    _append_csv(
        fit_summary,
        [
            {
                "analyzer_prefix": "ga06",
                "analyzer_device_id": "090",
                "status": "insufficient_evidence",
                "valid_pair_count": "0",
                "distinct_pressure_points": "0",
                "reference_span_hpa": "0",
            }
        ],
    )

    tables = build_pressure_channel_completion_tables(
        senco9_write_summary_path=write_summary,
        post_write_fit_summary_path=fit_summary,
        pressure_reference_path=reference,
        pressure_reference_traceability_path=traceability,
        old_getco_snapshot_path=old_getco,
        selected_device_ids=["023"],
        known_limitations=[
            {
                "limitation_id": "500hpa_low_pressure_micro_leak",
                "reason": "excluded_from_formal_pressure_fit",
                "impact": "500 hPa remains engineering diagnostic until the leak is repaired.",
            }
        ],
        today="2026-05-25",
    )

    summary = tables["pressure_channel_completion_summary"][0]
    gate = tables["pressure_channel_readiness_gate"][0]
    assert summary["overall_status"] == "ready_for_open_flow_main_calibration"
    assert summary["completion_scope_device_ids"] == "023"
    assert summary["excluded_device_count"] == 1
    assert gate["status"] == "pass"
    assert tables["pressure_channel_excluded_devices"][0]["analyzer_device_id"] == "090"
    assert "post_write_pressure_fit_not_pass" in tables["pressure_channel_excluded_devices"][0]["exclusion_reasons"]
    assert tables["pressure_channel_known_limitations"][0]["limitation_id"] == "500hpa_low_pressure_micro_leak"


def test_pressure_channel_completion_records_acceptance_policy_note(tmp_path):
    write_summary, fit_summary, traceability, reference, old_getco = _write_inputs(tmp_path)

    tables = build_pressure_channel_completion_tables(
        senco9_write_summary_path=write_summary,
        post_write_fit_summary_path=fit_summary,
        pressure_reference_path=reference,
        pressure_reference_traceability_path=traceability,
        old_getco_snapshot_path=old_getco,
        max_abs_offset_kpa=0.08,
        acceptance_policy_note="Near-atmospheric open-flow component calibration precondition.",
        today="2026-05-25",
    )

    policy = tables["pressure_channel_acceptance_policy"][0]
    summary = tables["pressure_channel_completion_summary"][0]
    assert policy["max_abs_offset_kpa_limit"] == 0.08
    assert policy["not_pressure_compensation_acceptance"] is True
    assert "Near-atmospheric" in policy["note"]
    assert summary["max_abs_offset_kpa_limit"] == 0.08


def test_pressure_channel_completion_blocks_expired_reference(tmp_path):
    write_summary, fit_summary, traceability, reference, old_getco = _write_inputs(tmp_path, expired=True)

    tables = build_pressure_channel_completion_tables(
        senco9_write_summary_path=write_summary,
        post_write_fit_summary_path=fit_summary,
        pressure_reference_path=reference,
        pressure_reference_traceability_path=traceability,
        old_getco_snapshot_path=old_getco,
        today="2026-05-25",
    )

    assert tables["pressure_channel_completion_summary"][0]["overall_status"] == "blocked"
    device = tables["pressure_channel_device_readiness"][0]
    assert "pressure_reference_traceability_not_pass" in device["readiness_reasons"]


def test_pressure_channel_completion_cli_writes_report(tmp_path):
    write_summary, fit_summary, traceability, reference, old_getco = _write_inputs(tmp_path)
    out_dir = tmp_path / "out"

    rc = completion_main(
        [
            "--senco9-write-summary",
            str(write_summary),
            "--post-write-fit-summary",
            str(fit_summary),
            "--pressure-reference-json",
            str(reference),
            "--pressure-reference-traceability",
            str(traceability),
            "--old-getco-json",
            str(old_getco),
            "--output-dir",
            str(out_dir),
            "--device-id",
            "023",
            "--known-limitation",
            "500hpa_low_pressure_micro_leak|excluded_from_formal_pressure_fit|500 hPa remains diagnostic until repaired",
            "--acceptance-policy-note",
            "CLI policy note",
            "--today",
            "2026-05-25",
        ]
    )

    assert rc == 0
    rows = _read_csv(out_dir / "pressure_channel_completion_summary.csv")
    assert rows[0]["overall_status"] == "ready_for_open_flow_main_calibration"
    assert rows[0]["completion_scope_device_ids"] == "023"
    policy_rows = _read_csv(out_dir / "pressure_channel_acceptance_policy.csv")
    assert policy_rows[0]["note"] == "CLI policy note"
    assert (out_dir / "pressure_channel_completion_report.md").exists()
    assert (out_dir / "pressure_channel_completion.xlsx").exists()
