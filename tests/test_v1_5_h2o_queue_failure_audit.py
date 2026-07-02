import csv
import json

from gas_calibrator.tools.export_v1_5_h2o_queue_failure_audit import main as export_h2o_audit_main
from gas_calibrator.validation.v1_5_h2o_queue_failure_audit import (
    analyze_point_log,
    audit_and_write,
    classify_point_failure_from_log,
)


def test_analyze_h2o_point_log_extracts_dewpoint_physical_summary(tmp_path):
    log_path = tmp_path / "point.log"
    log_path.write_text(
        "\n".join(
            [
                "H2O route precondition dewpoint gate waiting: row=1 dewpoint=-6.0 "
                "time_to_gate=10.0s tail_span_60s=None tail_slope_60s=None reason=waiting",
                "H2O route precondition failed: dewpoint gate timeout after open-route alignment; "
                "row=1 gate_wait_s=1800.0 reason=dewpoint_tail_span_too_large;max_total_wait_exceeded",
                "H2O route precondition dewpoint gate waiting: row=1 dewpoint=8.2 "
                "time_to_gate=1800.0s tail_span_60s=0.8 tail_slope_60s=0.01 "
                "reason=dewpoint_tail_span_too_large;max_total_wait_exceeded",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = analyze_point_log(log_path)

    assert result["failure_category"] == "dewpoint_unstable"
    assert result["wait_sample_count"] == 2
    assert result["first_dewpoint_c"] == -6.0
    assert result["last_dewpoint_c"] == 8.2
    assert result["min_dewpoint_c"] == -6.0
    assert result["last_tail_slope_60s_c_per_min"] == 0.6
    assert "水汽状态仍在变化" in result["physical_interpretation_zh"]


def test_classify_h2o_humidity_generator_gate_failure(tmp_path):
    log_path = tmp_path / "hgen_gate_failed.log"
    log_path.write_text(
        "H2O open-flow humidity-generator gate failed.\n"
        "reason=humidity_generator_gate_failed;target_not_ready\n",
        encoding="utf-8",
    )

    result = classify_point_failure_from_log(log_path)

    assert result["failure_category"] == "hgen_not_ready"
    assert "humidity_generator_gate_failed" in result["failure_reason"]


def test_h2o_queue_failure_audit_writes_route_specific_outputs(tmp_path):
    queue_dir = tmp_path / "h2o_open_flow" / "queue"
    queue_dir.mkdir(parents=True)
    point_log = queue_dir / "point_logs" / "p001.log"
    point_log.parent.mkdir()
    point_log.write_text(
        "H2O precondition analyzer stability gate failed before open-route sampling/seal: "
        "row=1 tol=0.001 window_s=120\n",
        encoding="utf-8",
    )
    manifest_path = queue_dir / "queue_manifest.csv"
    with manifest_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "point_run_id",
                "point_id",
                "status",
                "point_log",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "point_run_id": "p001_T20_HG20C_50RH_h2o",
                "point_id": "h2o_T20_HG20C_50RH",
                "status": "failed",
                "point_log": str(point_log),
            }
        )

    audit = audit_and_write(manifest_path, queue_dir / "queue_failure_audit")

    assert audit["failure_category_counts"] == {"h2o_ratio_unstable": 1}
    outputs = audit["outputs"]
    assert outputs["json"].endswith("h2o_queue_failure_audit.json")
    assert outputs["queue_failure_json"].endswith("queue_failure_audit.json")
    payload = json.loads((queue_dir / "queue_failure_audit" / "h2o_queue_failure_audit.json").read_text(encoding="utf-8"))
    markdown = (queue_dir / "queue_failure_audit" / "h2o_queue_failure_audit_zh.md").read_text(encoding="utf-8")
    assert payload["rows"][0]["failure_category"] == "h2o_ratio_unstable"
    assert "H2O 队列失败离线审计" in markdown


def test_export_h2o_queue_failure_audit_cli(tmp_path, capsys):
    queue_dir = tmp_path / "h2o_open_flow" / "queue"
    queue_dir.mkdir(parents=True)
    point_log = queue_dir / "failed.log"
    point_log.write_text("H2O open-flow dewpoint meter ready check failed.\n", encoding="utf-8")
    manifest_path = queue_dir / "queue_manifest.csv"
    with manifest_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["point_run_id", "status", "point_log"])
        writer.writeheader()
        writer.writerow({"point_run_id": "p001", "status": "failed", "point_log": str(point_log)})

    rc = export_h2o_audit_main(
        [
            "--manifest",
            str(manifest_path),
            "--output-dir",
            str(queue_dir / "audit"),
        ]
    )
    result = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert result["status"] == "ok"
    assert result["failure_category_counts"] == {"dewpoint_reference_unavailable": 1}
    assert (queue_dir / "audit" / "queue_failure_audit.json").exists()
