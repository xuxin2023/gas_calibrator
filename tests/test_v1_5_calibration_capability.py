import csv
import json

import pytest

from gas_calibrator.tools.export_v1_5_calibration_capability import main as capability_main
from gas_calibrator.validation.v1_5_calibration_capability import (
    build_v1_5_calibration_capability,
    render_v1_5_calibration_capability_markdown,
)


pytestmark = pytest.mark.v1_5_formal_gate


def _status_payload(*, blocked=False, h2o_traceability=True):
    def stage(stage_id, status="pass"):
        return {
            "stage_id": stage_id,
            "title": stage_id,
            "status": status,
            "reason": status,
            "artifact_roles": [],
            "artifact_count": 1 if status == "pass" else 0,
            "physical_meaning": stage_id,
        }

    contract_status = "blocked" if blocked else "pass"
    return {
        "schema": "v1_5_run_evidence_status_v1",
        "overall_status": "blocked" if blocked else "incomplete",
        "contract_status": contract_status,
        "component": "both",
        "stage_statuses": [
            stage("full_flow_contract_gate", "blocked" if blocked else "pass"),
            stage("plan_traceability"),
            stage("identity_getco_epoch0", "missing"),
            stage("pressure_quick_check"),
            stage("co2_open_flow"),
            stage("h2o_open_flow"),
            stage("candidate_review"),
            stage("controlled_write_events", "not_attempted"),
            stage("post_write_reverification", "not_attempted"),
            stage("evidence_bundle"),
            stage("database_import", "not_attempted"),
            stage("reports", "pass"),
        ],
        "traceability_checks": {
            "has_water_route_traceability": h2o_traceability,
            "has_h2o_raw_signal_fields": h2o_traceability,
            "has_post_write_reverification": False,
        },
    }


def _write_csv(path, rows):
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return path


def test_calibration_capability_reports_demonstrated_scope_but_not_formal_release(tmp_path):
    verification_csv = _write_csv(
        tmp_path / "verification.csv",
        [
            {
                "point": "400ppm_after_s5",
                "device_id": "100",
                "target_ppm": "399.56",
                "mean_ppm": "400.63",
                "error_ppm": "1.07",
                "error_pct": "0.27",
                "ratio_f_range": "0.0001",
                "dewpoint_mean_c": "-21.4",
                "qc_status": "pass",
            },
            {
                "point": "800ppm_after_s5",
                "device_id": "100",
                "target_ppm": "800.59",
                "mean_ppm": "803.10",
                "error_ppm": "2.51",
                "error_pct": "0.31",
                "ratio_f_range": "0.0001",
                "dewpoint_mean_c": "-23.2",
                "qc_status": "pass",
            },
        ],
    )
    candidate_csv = _write_csv(
        tmp_path / "candidate.csv",
        [
            {
                "device_id": "100",
                "candidate_status": "review_ready",
                "payload_max_abs_error_pct": "0.012",
                "payload_max_abs_error_ppm": "0.108",
                "blocked_reasons": "",
            }
        ],
    )

    assessment = build_v1_5_calibration_capability(
        run_status=_status_payload(),
        verification_csvs=[verification_csv],
        candidate_csvs=[candidate_csv],
        component="both",
    )

    assert assessment["capability_status"] == "demonstrated_calibratable_for_verified_scope"
    assert assessment["method_backbone_ready"] is True
    assert assessment["formal_release_ready"] is False
    assert "identity_getco_epoch0" in assessment["formal_release_blockers"]
    assert assessment["verification_rollup"]["device_ids"] == ("100",)
    assert assessment["verification_rollup"]["max_abs_error_pct"] == 0.31
    assert not any(issue["severity"] == "P0" for issue in assessment["issues"])
    assert any(issue["code"] == "release_stage_not_closed:post_write_reverification" for issue in assessment["issues"])


def test_calibration_capability_keeps_h2o_traceability_as_physical_p1():
    assessment = build_v1_5_calibration_capability(
        run_status=_status_payload(h2o_traceability=False),
        component="both",
    )

    assert assessment["capability_status"] == "conditionally_calibratable_needs_release_closure"
    codes = {issue["code"] for issue in assessment["issues"]}
    assert "h2o_traceability_incomplete" in codes
    assert "h2o_raw_signal_fields_missing" in codes
    assert assessment["method_backbone_ready"] is True


def test_calibration_capability_blocks_on_contract_p0():
    assessment = build_v1_5_calibration_capability(
        run_status=_status_payload(blocked=True),
        component="both",
    )

    assert assessment["capability_status"] == "not_calibratable_until_p0_resolved"
    assert assessment["method_backbone_ready"] is False
    assert any(issue["code"] == "contract_blocked" for issue in assessment["issues"])


def test_calibration_capability_cli_writes_utf8_markdown(tmp_path, capsys):
    status_path = tmp_path / "v1_5_run_evidence_status.json"
    status_path.write_text(json.dumps(_status_payload(), ensure_ascii=False, indent=2), encoding="utf-8")
    verification_csv = _write_csv(
        tmp_path / "verification.csv",
        [
            {
                "point": "900ppm",
                "device_id": "100",
                "error_pct": "0.3",
                "error_ppm": "2.7",
                "ratio_f_range": "0.0001",
                "dewpoint_mean_c": "-22",
                "qc_status": "pass",
            }
        ],
    )
    output_dir = tmp_path / "out"

    rc = capability_main(
        [
            "--run-status-json",
            str(status_path),
            "--verification-csv",
            str(verification_csv),
            "--output-dir",
            str(output_dir),
        ]
    )
    cli_result = json.loads(capsys.readouterr().out)
    markdown = (output_dir / "v1_5_calibration_capability.md").read_text(encoding="utf-8")

    assert rc == 0
    assert cli_result["method_backbone_ready"] is True
    assert "V1.5 校准能力离线评估" in markdown
    assert "方法骨架可校准" in markdown
    assert "`100`" in markdown


def test_calibration_capability_markdown_names_physical_boundary():
    assessment = build_v1_5_calibration_capability(
        run_status=_status_payload(),
        component="co2",
    )
    markdown = render_v1_5_calibration_capability_markdown(assessment)

    assert "不打开串口" in markdown
    assert "压力" in markdown
