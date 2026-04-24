from __future__ import annotations

import csv
import json
from pathlib import Path

from gas_calibrator.v2.scripts import route_trace_diff


def _write_trace(path: Path, lines: list[str]) -> None:
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    fieldnames = list(rows[0].keys()) if rows else []
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_compare_route_traces_detects_missing_extra_and_order(tmp_path: Path) -> None:
    v1_path = tmp_path / "v1.jsonl"
    v2_path = tmp_path / "v2.jsonl"
    _write_trace(
        v1_path,
        [
            '{"route":"h2o","action":"set_h2o_path","point_tag":"h2o_20c_50rh_1000hpa"}',
            '{"route":"h2o","action":"wait_dewpoint","point_tag":"h2o_20c_50rh_1000hpa"}',
            '{"route":"h2o","action":"seal_route","point_tag":"h2o_20c_50rh_1000hpa"}',
            '{"route":"co2","action":"route_baseline","point_tag":"co2_groupa_400ppm_1000hpa"}',
            '{"route":"co2","action":"set_co2_valves","point_tag":"co2_groupa_400ppm_1000hpa"}',
        ],
    )
    _write_trace(
        v2_path,
        [
            '{"route":"h2o","action":"set_h2o_path","point_tag":"h2o_20c_50rh_1000hpa"}',
            '{"route":"h2o","action":"seal_route","point_tag":"h2o_20c_50rh_1000hpa"}',
            '{"route":"co2","action":"set_co2_valves","point_tag":"co2_groupa_400ppm_1000hpa"}',
            '{"route":"co2","action":"route_baseline","point_tag":"co2_groupa_400ppm_1000hpa"}',
            '{"route":"co2","action":"cleanup","point_tag":"co2_groupa_400ppm_1000hpa"}',
        ],
    )

    summaries = route_trace_diff.compare_route_traces(
        route_trace_diff.load_route_trace(v1_path),
        route_trace_diff.load_route_trace(v2_path),
    )
    h2o = next(summary for summary in summaries if summary.route == "h2o")
    co2 = next(summary for summary in summaries if summary.route == "co2")

    assert "wait_dewpoint@h2o_20c_50rh_1000hpa" in h2o.missing_in_v2
    assert h2o.order_mismatches
    assert "cleanup@co2_groupa_400ppm_1000hpa" in co2.extra_in_v2
    assert co2.order_mismatches


def test_route_trace_diff_main_prints_readable_report(capsys, tmp_path: Path) -> None:
    v1_path = tmp_path / "v1.jsonl"
    v2_path = tmp_path / "v2.jsonl"
    _write_trace(
        v1_path,
        ['{"route":"co2","action":"route_baseline","point_tag":"co2_groupa_400ppm_1000hpa"}'],
    )
    _write_trace(
        v2_path,
        ['{"route":"co2","action":"set_co2_valves","point_tag":"co2_groupa_400ppm_1000hpa"}'],
    )

    exit_code = route_trace_diff.main(["--v1-trace", str(v1_path), "--v2-trace", str(v2_path)])
    captured = capsys.readouterr().out

    assert exit_code == 1
    assert "Route Trace Diff" in captured
    assert "Overall status: MISMATCH" in captured
    assert "[CO2]" in captured
    assert "Review stages:" in captured
    assert "baseline_restore" in captured
    assert "source_valve_selection" in captured
    assert "Missing in V2" in captured


def test_diagnose_compare_report_detects_bias_and_analyzer_scope(tmp_path: Path) -> None:
    v2_run_dir = tmp_path / "run"
    v2_run_dir.mkdir()
    v1_source = tmp_path / "points_readable.csv"
    _write_csv(
        v1_source,
        [
            {
                "\u70b9\u4f4d\u6807\u7b7e": "h2o_pt",
                "\u8bbe\u5907ID_\u5e73\u5747\u503c": "10.0",
            },
            {
                "\u70b9\u4f4d\u6807\u7b7e": "co2_pt",
                "\u8bbe\u5907ID_\u5e73\u5747\u503c": "10.0",
            },
        ],
    )
    (v2_run_dir / "results.json").write_text(
        json.dumps(
            {
                "samples": [
                    {"point_tag": "h2o_pt", "analyzer_id": "ga07", "h2o_mmol": 0.71, "h2o_ratio_f": 0.71, "co2_ppm": 1.2, "co2_ratio_f": 1.2, "pressure_hpa": 1100.0},
                    {"point_tag": "h2o_pt", "analyzer_id": "ga08", "h2o_mmol": 0.705, "h2o_ratio_f": 0.705, "co2_ppm": 1.19, "co2_ratio_f": 1.19, "pressure_hpa": 1100.0},
                    {"point_tag": "co2_pt", "analyzer_id": "ga07", "h2o_mmol": 0.73, "h2o_ratio_f": 0.73, "co2_ppm": 1.005, "co2_ratio_f": 1.005, "pressure_hpa": 1100.0},
                    {"point_tag": "co2_pt", "analyzer_id": "ga08", "h2o_mmol": 0.681, "h2o_ratio_f": 0.681, "co2_ppm": 0.95, "co2_ratio_f": 0.95, "pressure_hpa": 1100.0},
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    _write_csv(
        v2_run_dir / "samples_runtime.csv",
        [
            {
                "point_tag": "h2o_pt",
                "ga07_id": "029",
                "ga08_id": "004",
            }
        ],
    )
    report_path = tmp_path / "compare_report.json"
    report_path.write_text(
        json.dumps(
            {
                "v1_source": str(v1_source),
                "v2_run_dir": str(v2_run_dir),
                "thresholds": {
                    "co2_ppm_mean_rel_err": 0.01,
                    "h2o_mmol_mean_rel_err": 0.01,
                    "pressure_hpa_mean_rel_err": 0.005,
                    "co2_ratio_f_mean_rel_err": 0.01,
                    "h2o_ratio_f_mean_rel_err": 0.01,
                },
                "point_results": [
                    {
                        "point_tag": "h2o_pt",
                        "route": "h2o",
                        "metrics": {
                            "h2o_mmol": {"v1_mean": 0.66, "v2_mean": 0.7075, "passed": False},
                            "h2o_ratio_f": {"v1_mean": 0.66, "v2_mean": 0.7075, "passed": False},
                            "co2_ppm": {"v1_mean": 1.19, "v2_mean": 1.195, "passed": True},
                            "co2_ratio_f": {"v1_mean": 1.19, "v2_mean": 1.195, "passed": True},
                            "pressure_hpa": {"v1_mean": 1100.0, "v2_mean": 1100.0, "passed": True},
                        },
                    },
                    {
                        "point_tag": "co2_pt",
                        "route": "co2",
                        "metrics": {
                            "h2o_mmol": {"v1_mean": 0.68, "v2_mean": 0.74, "passed": False},
                            "h2o_ratio_f": {"v1_mean": 0.68, "v2_mean": 0.74, "passed": False},
                            "co2_ppm": {"v1_mean": 1.0, "v2_mean": 0.98, "passed": False},
                            "co2_ratio_f": {"v1_mean": 1.0, "v2_mean": 0.98, "passed": False},
                            "pressure_hpa": {"v1_mean": 1100.0, "v2_mean": 1100.0, "passed": True},
                        },
                    },
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    diagnosis = route_trace_diff.diagnose_compare_report(report_path)
    rendered = route_trace_diff.format_compare_report_diagnosis(diagnosis)

    assert diagnosis.metric_summaries
    assert diagnosis.v1_primary_analyzer_ids == ["10.0"]
    assert diagnosis.v2_runtime_analyzer_ids == {"ga07": "029", "ga08": "004"}
    assert diagnosis.analyzer_matches[0].analyzer_id == "ga08"
    assert diagnosis.best_metric_analyzers["h2o_mmol"] == "ga08"
    assert diagnosis.best_metric_analyzers["co2_ppm"] == "ga07"
    assert "Metric Bias Summary" in rendered
    assert "avg_delta=+0.053750" in rendered
    assert "V1 primary analyzer ids: 10.0" in rendered
    assert "V2 runtime analyzer ids: ga07=029, ga08=004" in rendered
    assert "Best analyzer by metric: co2_ppm=ga07" in rendered
    assert "different physical analyzers" in rendered


def test_route_trace_diff_main_supports_compare_report_mode(capsys, tmp_path: Path) -> None:
    report_path = tmp_path / "compare_report.json"
    report_path.write_text(json.dumps({"point_results": []}, ensure_ascii=False), encoding="utf-8")

    exit_code = route_trace_diff.main(["--compare-report", str(report_path)])
    captured = capsys.readouterr().out

    assert exit_code == 0
    assert "Compare Report Diagnosis" in captured
