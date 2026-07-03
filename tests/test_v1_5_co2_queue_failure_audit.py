import csv
import json

from gas_calibrator.validation.v1_5_co2_queue_failure_audit import (
    analyze_point_log,
    audit_and_write,
    audit_queue_manifest,
    classify_point_failure_from_log,
)


def test_analyze_point_log_extracts_dewpoint_physical_summary(tmp_path):
    log_path = tmp_path / "point.log"
    log_path.write_text(
        "\n".join(
            [
                "CO2 route precondition dewpoint gate waiting: row=1 dewpoint=-33.4 time_to_gate=10.0s tail_span_60s=None tail_slope_60s=None reason=flush_duration_below_min",
                "CO2 route precondition dewpoint gate waiting: row=1 dewpoint=-18.8 time_to_gate=1800.0s tail_span_60s=1.2 tail_slope_60s=0.02 reason=dewpoint_tail_reference_not_dry_enough;dewpoint_rebound_detected;max_total_wait_exceeded",
            ]
        ),
        encoding="utf-8",
    )

    result = analyze_point_log(log_path)

    assert result["failure_category"] == "dewpoint_rebound"
    assert result["wait_sample_count"] == 2
    assert result["first_dewpoint_c"] == -33.4
    assert result["last_dewpoint_c"] == -18.8
    assert result["min_dewpoint_c"] == -33.4
    assert result["last_tail_slope_60s_c_per_min"] == 1.2
    assert "露点曾经变干但随后回升" in result["physical_interpretation_zh"]


def test_classify_point_failure_from_log_identifies_not_dry_enough(tmp_path):
    log_path = tmp_path / "point.log"
    log_path.write_text(
        "CO2 route precondition failed: reason=dewpoint_tail_reference_not_dry_enough;max_total_wait_exceeded\n",
        encoding="utf-8",
    )

    result = classify_point_failure_from_log(log_path)

    assert result["failure_category"] == "dewpoint_not_dry_enough"
    assert "dewpoint_tail_reference_not_dry_enough" in result["failure_reason"]


def test_audit_queue_manifest_and_write_outputs(tmp_path):
    point_log = tmp_path / "point.log"
    point_log.write_text(
        "Analyzer startup config failed: ga05 err=MODE2 not ready (stream) last=\n",
        encoding="utf-8",
    )
    manifest = tmp_path / "queue_manifest.csv"
    with manifest.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["point_run_id", "status", "returncode", "point_log"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "point_run_id": "p001_T20_100ppm_fit",
                "status": "failed",
                "returncode": "1",
                "point_log": str(point_log),
            }
        )

    audit = audit_queue_manifest(manifest)

    assert audit["total_points"] == 1
    assert audit["failure_category_counts"] == {"analyzer_startup_mode2": 1}

    written = audit_and_write(manifest, tmp_path / "audit")
    outputs = written["outputs"]
    assert outputs["csv"].endswith("co2_queue_failure_audit.csv")
    assert outputs["queue_failure_json"].endswith("queue_failure_audit.json")
    payload = json.loads((tmp_path / "audit" / "co2_queue_failure_audit.json").read_text(encoding="utf-8"))
    assert payload["rows"][0]["failure_category"] == "analyzer_startup_mode2"
    alias_payload = json.loads((tmp_path / "audit" / "queue_failure_audit.json").read_text(encoding="utf-8"))
    assert alias_payload["rows"][0]["point_run_id"] == "p001_T20_100ppm_fit"
