from __future__ import annotations

import json
from pathlib import Path

from gas_calibrator.v2.core.run001_a1_dry_run import (
    RUN001_FAIL,
    RUN001_PASS,
    build_run001_a1_evidence_payload,
    write_run001_a1_artifacts,
)


def _base_raw_config() -> dict:
    return {
        "run001_a1": {
            "run_id": "Run-001/A1",
            "scenario": "Run-001/A1 CO2-only skip0 no-write real-machine dry-run",
            "mode": "real_machine_dry_run",
            "no_write": True,
            "co2_only": True,
            "skip_co2_ppm": [0],
            "single_route": True,
            "single_temperature_group": True,
            "allow_real_route": True,
            "allow_real_pressure": True,
            "allow_real_wait": True,
            "allow_real_sample": True,
            "allow_artifact": True,
            "allow_write_coefficients": False,
            "allow_write_zero": False,
            "allow_write_span": False,
            "allow_write_calibration_parameters": False,
            "default_cutover_to_v2": False,
            "disable_v1": False,
            "full_h2o_co2_group": False,
        },
        "workflow": {
            "route_mode": "co2_only",
            "selected_temps_c": [20.0],
            "skip_co2_ppm": [0],
            "sampling": {"count": 10, "stable_count": 10, "interval_s": 1.0},
        },
        "paths": {},
        "features": {"use_v2": True, "simulation_mode": False},
    }


def _co2_points() -> list[dict]:
    return [
        {"index": 1, "temperature_c": 20.0, "pressure_hpa": 1100.0, "route": "co2", "co2_ppm": 100.0},
    ]


def test_device_id_mismatch_hard_stop_generates_terminal_artifacts(tmp_path: Path) -> None:
    (tmp_path / "summary.json").write_text("{}", encoding="utf-8")
    (tmp_path / "manifest.json").write_text("{}", encoding="utf-8")
    (tmp_path / "route_trace.jsonl").write_text(
        json.dumps(
            {
                "action": "sensor_precheck_analyzer",
                "result": "fail",
                "message": "Sensor precheck device_id_mismatch (analyzer_0): expected=091 observed=001",
                "target": {"analyzer": "analyzer_0", "expected_device_id": "091"},
                "actual": {"observed_device_id": "001", "last_valid": "YGAS,001,..."},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    payload = build_run001_a1_evidence_payload(
        _base_raw_config(),
        point_rows=_co2_points(),
        run_dir=tmp_path,
        artifact_paths={
            "summary": tmp_path / "summary.json",
            "manifest": tmp_path / "manifest.json",
            "trace": tmp_path / "route_trace.jsonl",
        },
        require_runtime_artifacts=True,
        service_summary={
            "points_completed": 0,
            "completed_points": 0,
            "status": {
                "phase": "error",
                "completed_points": 0,
                "message": "Calibration failed: Sensor precheck device_id_mismatch",
                "error": "Sensor precheck device_id_mismatch",
            },
            "stats": {"sample_count": 0},
        },
    )
    written = write_run001_a1_artifacts(tmp_path / "run001", payload)

    assert set(written) == {"summary", "no_write_guard", "readiness", "trace", "manifest", "report"}
    for path in written.values():
        assert Path(path).exists()
    summary = json.loads(Path(written["summary"]).read_text(encoding="utf-8"))
    guard = json.loads(Path(written["no_write_guard"]).read_text(encoding="utf-8"))
    readiness = json.loads(Path(written["readiness"]).read_text(encoding="utf-8"))
    report = Path(written["report"]).read_text(encoding="utf-8")

    assert summary["readiness_result"] == RUN001_PASS
    assert summary["final_decision"] == RUN001_FAIL
    assert summary["a1_final_decision"] == RUN001_FAIL
    assert summary["a1_fail_reason"] == "device_id_mismatch"
    assert summary["fail_reason"] == "device_id_mismatch"
    assert summary["points_completed"] == 0
    assert summary["sample_count"] == 0
    assert summary["attempted_write_count"] == 0
    assert summary["blocked_write_events"] == []
    assert summary["no_write"] is True
    assert summary["v1_fallback_status"]["status"] == "available"
    assert guard["attempted_write_count"] == 0
    assert guard["final_decision"] == RUN001_FAIL
    assert readiness["a1_final_decision"] == RUN001_FAIL
    assert readiness["final_decision"] == RUN001_FAIL
    assert readiness["a1_fail_reason"] == "device_id_mismatch"
    assert "not real acceptance evidence" in report.lower()
