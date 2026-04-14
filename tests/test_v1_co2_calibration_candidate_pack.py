from __future__ import annotations

import csv
import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger, _field_label
from gas_calibrator.tools.build_v1_co2_calibration_candidate_pack import main as build_pack_main
from gas_calibrator.workflow.co2_calibration_candidate_pack import (
    build_co2_calibration_candidate_pack,
)
from gas_calibrator.workflow.runner import CalibrationRunner


def _candidate_row(
    *,
    title: str,
    point_no: int,
    target: float,
    temp_c: float,
    pressure_label: str,
    measured: float,
    suitability: str,
    recommended: bool,
    hard_blocked: bool,
    weight: float,
    evidence_score: float,
    weight_reason: str,
    reason_chain: str,
    measured_source: str = "co2_steady_state_window",
    route: str = "gas",
) -> dict[str, object]:
    return {
        "point_title": title,
        "point_no": point_no,
        "point_tag": f"row-{point_no}",
        "point_row": str(point_no),
        "route": route,
        "pressure_target_label": pressure_label,
        "co2_ppm_target": target,
        "temp_chamber_c": temp_c,
        "measured_value": measured,
        "measured_value_source": measured_source,
        "co2_point_suitability_status": suitability,
        "co2_calibration_candidate_status": suitability,
        "co2_calibration_candidate_recommended": recommended,
        "co2_calibration_candidate_hard_blocked": hard_blocked,
        "co2_calibration_weight_recommended": weight,
        "co2_calibration_weight_reason": weight_reason,
        "co2_calibration_reason_chain": reason_chain,
        "co2_evidence_score": evidence_score,
        "co2_decision_waterfall_status": "fail" if hard_blocked else "pass",
        "co2_decision_selected_stage_path": "phase:pass>source_segment:pass>temporal:pass>quarantine:pass>source_trust:pass>steady_state:pass",
        "co2_source_selected": "primary",
        "co2_source_switch_reason": "" if suitability == "fit" else "source_fallback",
        "co2_temporal_contract_status": "pass" if suitability != "unfit" else "fail",
        "co2_temporal_contract_reason": "" if suitability != "unfit" else "timestamp_rollback_detected",
        "co2_point_evidence_budget_summary": f"total={evidence_score:.1f}/100",
    }


def test_build_candidate_pack_groups_fit_advisory_and_unfit_points() -> None:
    rows = [
        _candidate_row(
            title="clean",
            point_no=1,
            target=500.0,
            temp_c=20.0,
            pressure_label="ambient",
            measured=500.0,
            suitability="fit",
            recommended=True,
            hard_blocked=False,
            weight=1.0,
            evidence_score=100.0,
            weight_reason="trusted_steady_state",
            reason_chain="waterfall=pass;steady_state_window",
        ),
        _candidate_row(
            title="fallback",
            point_no=2,
            target=500.0,
            temp_c=20.0,
            pressure_label="ambient",
            measured=503.0,
            suitability="advisory",
            recommended=True,
            hard_blocked=False,
            weight=0.62,
            evidence_score=62.0,
            weight_reason="fallback_but_usable",
            reason_chain="waterfall=warn;source_fallback",
            measured_source="co2_trailing_window_fallback",
        ),
        _candidate_row(
            title="blocked",
            point_no=3,
            target=800.0,
            temp_c=20.0,
            pressure_label="ambient",
            measured=810.0,
            suitability="unfit",
            recommended=False,
            hard_blocked=True,
            weight=0.0,
            evidence_score=0.0,
            weight_reason="hard_blocked_or_untrusted",
            reason_chain="waterfall=fail;no_trusted_source",
            measured_source="co2_no_trusted_source",
        ),
    ]

    pack = build_co2_calibration_candidate_pack(rows)

    assert pack["summary"]["point_count_total"] == 3
    assert pack["summary"]["point_count_fit"] == 1
    assert pack["summary"]["point_count_advisory"] == 1
    assert pack["summary"]["point_count_unfit"] == 1
    assert pack["summary"]["point_count_recommended"] == 2
    assert pack["summary"]["point_count_hard_blocked"] == 1

    groups = {row["calibration_group_key"]: row for row in pack["groups"]}
    ambient_500 = groups["co2|route=gas|target=500.000|temp=20.000|pressure=ambient"]
    assert ambient_500["point_count_total"] == 2
    assert ambient_500["point_count_fit"] == 1
    assert ambient_500["point_count_advisory"] == 1
    assert ambient_500["point_count_unfit"] == 0
    assert pytest.approx(float(ambient_500["weight_sum"]), abs=1e-9) == 1.62
    assert pytest.approx(float(ambient_500["weighted_mean_measured_value"]), abs=1e-6) == ((1.0 * 500.0) + (0.62 * 503.0)) / 1.62
    assert pytest.approx(float(ambient_500["unweighted_mean_measured_value"]), abs=1e-9) == 501.5
    assert ambient_500["group_recommended_for_fit"] is True

    blocked_group = groups["co2|route=gas|target=800.000|temp=20.000|pressure=ambient"]
    assert blocked_group["group_recommended_for_fit"] is False
    assert pytest.approx(float(blocked_group["weight_sum"]), abs=1e-9) == 0.0


def test_build_candidate_pack_accepts_chinese_labels_and_missing_fields() -> None:
    rows = [
        {
            "点位标题": "中文点",
            "点位编号": "7",
            "目标二氧化碳浓度ppm": "400",
            "温箱目标温度C": "20",
            "压力目标标签": "ambient",
            "测量值": "401.2",
            "测量值来源": "co2_steady_state_window",
            "气路点适用性": "advisory",
            "气路校准候选推荐": "True",
            "气路校准候选硬阻断": "False",
            "气路推荐校准权重": "0.58",
            "气路证据分": "58",
            "气路点适用性原因链": "waterfall=warn;source_fallback",
            "气路决策瀑布结果": "warn",
        }
    ]

    pack = build_co2_calibration_candidate_pack(rows)
    point = pack["points"][0]

    assert point["co2_calibration_candidate_status"] == "advisory"
    assert point["co2_calibration_candidate_recommended"] is True
    assert point["co2_calibration_candidate_hard_blocked"] is False
    assert pytest.approx(float(point["co2_calibration_weight_recommended"]), abs=1e-9) == 0.58
    assert point["co2_calibration_group_key"] == "co2|route=unknown|target=400.000|temp=20.000|pressure=ambient"


def test_candidate_pack_tool_writes_csv_json_and_markdown(tmp_path: Path) -> None:
    points_csv = tmp_path / "points_demo.csv"
    output_dir = tmp_path / "pack_out"
    rows = [
        _candidate_row(
            title="clean",
            point_no=1,
            target=500.0,
            temp_c=20.0,
            pressure_label="ambient",
            measured=500.0,
            suitability="fit",
            recommended=True,
            hard_blocked=False,
            weight=1.0,
            evidence_score=100.0,
            weight_reason="trusted_steady_state",
            reason_chain="waterfall=pass;steady_state_window",
        ),
        _candidate_row(
            title="blocked",
            point_no=2,
            target=800.0,
            temp_c=20.0,
            pressure_label="ambient",
            measured=805.0,
            suitability="unfit",
            recommended=False,
            hard_blocked=True,
            weight=0.0,
            evidence_score=0.0,
            weight_reason="hard_blocked_or_untrusted",
            reason_chain="waterfall=fail;no_trusted_source",
            measured_source="co2_no_trusted_source",
        ),
    ]
    with points_csv.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    assert build_pack_main(["--points-csv", str(points_csv), "--output-dir", str(output_dir)]) == 0
    assert (output_dir / "calibration_candidate_points.csv").exists()
    assert (output_dir / "calibration_candidate_groups.csv").exists()
    assert (output_dir / "calibration_candidate_pack.json").exists()
    assert (output_dir / "report.md").exists()

    payload = json.loads((output_dir / "calibration_candidate_pack.json").read_text(encoding="utf-8"))
    assert payload["summary"]["point_count_total"] == 2
    assert payload["summary"]["not_real_acceptance_evidence"] is True
    report = (output_dir / "report.md").read_text(encoding="utf-8")
    assert "replay evidence only" in report
    assert "not real acceptance evidence" in report


def _point_co2_low_pressure() -> CalibrationPoint:
    return CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=500.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=700.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
        co2_group="B",
    )


def _co2_sampling_rows(values: list[float]) -> list[dict]:
    start = datetime(2026, 4, 4, 10, 0, 0)
    rows: list[dict] = []
    for idx, value in enumerate(values):
        ts = start + timedelta(seconds=idx)
        rows.append(
            {
                "sample_ts": ts.isoformat(timespec="milliseconds"),
                "sample_start_ts": ts.isoformat(timespec="milliseconds"),
                "sample_end_ts": (ts + timedelta(milliseconds=100)).isoformat(timespec="milliseconds"),
                "co2_ppm": value,
                "frame_usable": True,
                "id": "086",
            }
        )
    return rows


def test_runner_exports_candidate_fields_to_samples_and_readable_points(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    cfg = {
        "workflow": {
            "sampling": {
                "interval_s": 1.0,
                "co2_interval_s": 1.0,
                "quality": {
                    "co2_steady_state_enabled": True,
                    "co2_steady_state_policy": "warn",
                    "co2_steady_state_min_samples": 4,
                    "co2_steady_state_fallback_samples": 4,
                    "co2_steady_state_max_std_ppm": 0.2,
                    "co2_steady_state_max_range_ppm": 0.4,
                    "co2_steady_state_max_abs_slope_ppm_per_s": 0.2,
                },
            }
        }
    }
    runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)
    point = _point_co2_low_pressure()
    rows = _co2_sampling_rows([500.0, 500.1, 499.9, 500.0, 500.1, 499.9])

    result = runner._evaluate_co2_steady_state_window_qc(point, phase="co2", samples=rows)
    runner._copy_point_runtime_exports_into_samples(point, phase="co2", samples=rows)
    logger.close()

    assert result["co2_calibration_candidate_status"] == "fit"
    assert result["co2_calibration_weight_reason"] == "steady_state_window;fit_for_calibration"
    assert "waterfall=pass" in result["co2_calibration_reason_chain"]
    assert rows[0]["co2_calibration_candidate_status"] == "fit"
    assert rows[0]["co2_calibration_weight_reason"] == "steady_state_window;fit_for_calibration"
    assert _field_label("co2_calibration_candidate_status") == "气路校准候选状态"
    assert _field_label("co2_calibration_weight_reason") == "气路校准权重原因"
